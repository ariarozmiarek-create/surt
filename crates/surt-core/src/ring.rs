//! Single-producer / single-consumer ring over caller-provided shared memory.
//!
//! Memory layout (one ring):
//!
//! ```text
//! +---------------------------+ offset 0
//! | SurtRingHeader (192 B)    |  3 cache lines: const | producer | consumer
//! +---------------------------+ offset 192
//! | entries[queue_len]        |  E = SurtSqe or SurtCqe
//! +---------------------------+
//! ```
//!
//! ## Concurrency model
//!
//! The producer owns `tail`; the consumer owns `head`. Both are monotonically
//! increasing `u32` counters that wrap; the slot index is `counter & mask`
//! where `mask == queue_len - 1` (queue_len is a power of two). Capacity is
//! `tail.wrapping_sub(head)`; the ring is empty when that is `0` and full when
//! it is `queue_len`.
//!
//! Ordering: the producer writes the entry, then publishes `tail` with
//! `Release`; the consumer reads `tail` with `Acquire`, then reads the entry.
//! That release/acquire pair establishes happens-before for the entry payload,
//! so producer and consumer never touch the same slot concurrently and there
//! is no data race. Each side caches the peer's cursor to avoid an atomic load
//! on the fast path.
//!
//! ## Untrusted peers
//!
//! Shared memory is treated as mutable by the peer (spec §16). `queue_len`,
//! `entry_size` and the entries base are validated once at attach time and
//! then cached *locally* — never re-read from shared memory on the hot path,
//! so a peer cannot enlarge the ring under us. Cursor movement is bounded:
//! the consumer rejects a `tail` more than `queue_len` ahead of `head`
//! (`PopError::PeerFaulted`), and the producer treats an implausible `head`
//! conservatively (it can only ever refuse to push).

use core::marker::PhantomData;
use core::ptr::NonNull;
use core::sync::atomic::{fence, AtomicU32, Ordering};

use surt_abi::{ring_flags, ring_state, SurtCqe, SurtRingHeader, SurtSqe};

use crate::error::{PopError, PushError, RingError};
use crate::notify::{Notify, NotifyError, WaitDecision};

/// Marker for a plain-old-data type that may be projected as a ring entry from
/// shared memory.
///
/// # Safety
/// Implementors must be `#[repr(C)]`, `Copy`, contain no padding with
/// uninitialised semantics, and be valid for any bit pattern (so a snapshot of
/// peer-written bytes is always a sound value to read). The ABI descriptor
/// types satisfy this.
pub unsafe trait RingEntry: Copy + 'static {}

// SAFETY: SurtSqe is #[repr(C)], Copy, all-integer fields, valid for any bits.
unsafe impl RingEntry for SurtSqe {}
// SAFETY: SurtCqe is #[repr(C)], Copy, all-integer fields, valid for any bits.
unsafe impl RingEntry for SurtCqe {}

/// Bytes required to back a ring of `queue_len` entries of type `E`.
#[inline]
pub const fn ring_bytes<E>(queue_len: u32) -> usize {
    core::mem::size_of::<SurtRingHeader>() + (queue_len as usize) * core::mem::size_of::<E>()
}

/// Configuration for initialising a fresh ring header.
#[derive(Debug, Clone, Copy)]
pub struct RingConfig {
    /// Number of entries; must be a power of two in `1..=2^31`.
    pub queue_len: u32,
    /// Ring identity stored in the header.
    pub ring_id: u64,
    /// Negotiated feature bits stored in the header.
    pub feature_flags: u64,
    /// Owner role ([`surt_abi::role`]).
    pub role: u16,
}

/// Largest permitted `queue_len` (spec §8.2: keep wrap unambiguous).
const MAX_QUEUE_LEN: u32 = 1 << 31;
const ENTRY_OFFSET: usize = core::mem::size_of::<SurtRingHeader>();

#[inline]
fn validate_queue_len(queue_len: u32) -> Result<(), RingError> {
    if queue_len == 0 || queue_len > MAX_QUEUE_LEN || !queue_len.is_power_of_two() {
        return Err(RingError::BadQueueLen { found: queue_len });
    }
    Ok(())
}

/// Validate a backing region's geometry and return the entry count it can hold.
fn validate_region<E>(ptr: *mut u8, len: usize, queue_len: u32) -> Result<(), RingError> {
    if ptr.is_null() {
        return Err(RingError::NullPointer);
    }
    let align = core::mem::align_of::<SurtRingHeader>();
    if (ptr as usize) % align != 0 {
        return Err(RingError::Misaligned { required: align });
    }
    let needed = ring_bytes::<E>(queue_len);
    if len < needed {
        return Err(RingError::TooSmall { needed, got: len });
    }
    Ok(())
}

/// Initialise a fresh ring header in `ptr..ptr+len` and mark it `READY`.
///
/// The memory authority calls this once before either peer attaches. Sets
/// `head = tail = 0`.
///
/// # Safety
/// `ptr` must point to at least [`ring_bytes::<E>`]`(cfg.queue_len)` bytes of
/// writable memory, aligned to `align_of::<SurtRingHeader>()`, that is not
/// concurrently accessed during initialisation.
pub unsafe fn init_ring<E: RingEntry>(
    ptr: *mut u8,
    len: usize,
    cfg: &RingConfig,
) -> Result<(), RingError> {
    validate_queue_len(cfg.queue_len)?;
    validate_region::<E>(ptr, len, cfg.queue_len)?;

    let header = ptr as *mut SurtRingHeader;
    let init = SurtRingHeader::new(
        cfg.role,
        core::mem::size_of::<E>() as u16,
        cfg.queue_len,
        cfg.feature_flags,
        cfg.ring_id,
        ring_state::READY,
    );
    // SAFETY: validated above — `header` is non-null, aligned, and `len` covers
    // a full SurtRingHeader. We hold exclusive access during init (caller
    // contract), so the write is sound.
    unsafe { core::ptr::write(header, init) };
    Ok(())
}

/// Validated, locally-cached projection of a ring's shared memory.
///
/// Holds raw pointers (the memory is shared/aliased across address spaces, so a
/// Rust borrow cannot model it) plus the *locally cached* `queue_len`/`mask`
/// that the hot path trusts instead of the shared header.
struct RingView<E> {
    header: NonNull<SurtRingHeader>,
    entries: NonNull<E>,
    queue_len: u32,
    mask: u32,
    _marker: PhantomData<E>,
}

impl<E: RingEntry> RingView<E> {
    /// # Safety
    /// `ptr` must remain a valid mapping of an initialised ring (header +
    /// `queue_len` entries) for the lifetime of the returned view, and must be
    /// projected as the agreed entry type `E`.
    unsafe fn attach(ptr: *mut u8, len: usize) -> Result<Self, RingError> {
        if ptr.is_null() {
            return Err(RingError::NullPointer);
        }
        let align = core::mem::align_of::<SurtRingHeader>();
        if (ptr as usize) % align != 0 {
            return Err(RingError::Misaligned { required: align });
        }
        if len < ENTRY_OFFSET {
            return Err(RingError::TooSmall {
                needed: ENTRY_OFFSET,
                got: len,
            });
        }

        let header_ptr = ptr as *mut SurtRingHeader;
        // SAFETY: non-null, aligned, and `len >= size_of::<SurtRingHeader>()`,
        // so reading the header fields is in-bounds. We only read here.
        let header = unsafe { &*header_ptr };

        if header.magic != surt_abi::SURT_MAGIC {
            return Err(RingError::BadMagic {
                found: header.magic,
            });
        }
        if header.abi_major != surt_abi::SURT_ABI_MAJOR {
            return Err(RingError::BadAbiMajor {
                found: header.abi_major,
            });
        }
        let expected_entry = core::mem::size_of::<E>() as u16;
        if header.entry_size != expected_entry {
            return Err(RingError::BadEntrySize {
                expected: expected_entry,
                found: header.entry_size,
            });
        }
        let queue_len = header.queue_len;
        validate_queue_len(queue_len)?;
        let state = header.state.load(Ordering::Acquire);
        if state == ring_state::UNINIT {
            return Err(RingError::BadState { found: state });
        }
        validate_region::<E>(ptr, len, queue_len)?;

        // SAFETY: `ptr + ENTRY_OFFSET` is in-bounds (validate_region ensured
        // `len >= ENTRY_OFFSET + queue_len*size_of::<E>()`) and ENTRY_OFFSET is
        // a multiple of align_of::<E>() when the base is 64-aligned.
        let entries = unsafe { ptr.add(ENTRY_OFFSET) } as *mut E;

        Ok(Self {
            // SAFETY: ptr is non-null (checked).
            header: unsafe { NonNull::new_unchecked(header_ptr) },
            // SAFETY: derived from a non-null base plus a fixed offset.
            entries: unsafe { NonNull::new_unchecked(entries) },
            queue_len,
            mask: queue_len - 1,
            _marker: PhantomData,
        })
    }

    #[inline]
    fn header(&self) -> &SurtRingHeader {
        // SAFETY: `header` points to a live, initialised SurtRingHeader for the
        // lifetime of `self` (attach contract). Only atomic fields are mutated
        // (by us or the peer); reading the reference is sound.
        unsafe { self.header.as_ref() }
    }

    #[inline]
    fn head(&self) -> &AtomicU32 {
        &self.header().head
    }

    #[inline]
    fn tail(&self) -> &AtomicU32 {
        &self.header().tail
    }

    #[inline]
    fn state(&self) -> &AtomicU32 {
        &self.header().state
    }

    #[inline]
    fn consumer_flags(&self) -> &AtomicU32 {
        &self.header().consumer_flags
    }

    /// Write entry `e` into slot `idx` (`idx < queue_len`).
    ///
    /// # Safety
    /// `idx < queue_len`, and the slot must currently be free (the caller, the
    /// sole producer, has confirmed the consumer advanced past it). The peer
    /// does not concurrently write this slot under the SPSC protocol.
    #[inline]
    unsafe fn write_entry(&self, idx: u32, e: E) {
        // SAFETY: idx < queue_len (caller), so the offset is in-bounds of the
        // entries array. `write_volatile` keeps the store from being elided or
        // reordered past the subsequent Release publish.
        unsafe {
            self.entries.as_ptr().add(idx as usize).write_volatile(e);
        }
    }

    /// Read a snapshot of slot `idx` (`idx < queue_len`).
    ///
    /// # Safety
    /// `idx < queue_len`, and the producer must have published this slot
    /// (`head < tail`), so it is not concurrently written.
    #[inline]
    unsafe fn read_entry(&self, idx: u32) -> E {
        // SAFETY: idx < queue_len (caller); the slot was published before the
        // Acquire load of `tail` that gated this read. `read_volatile` takes a
        // snapshot so a later peer mutation cannot retroactively change it.
        unsafe { self.entries.as_ptr().add(idx as usize).read_volatile() }
    }
}

/// The producing end of a ring (writes entries, advances `tail`).
pub struct Producer<E: RingEntry> {
    view: RingView<E>,
    cached_tail: u32,
    cached_head: u32,
}

// SAFETY: a Producer is the *sole* owner of the producer role. Its shared-memory
// interaction is the `tail` atomic (it writes) and the `head` atomic (it reads,
// Acquire); entry slots it touches are provably not touched by the consumer
// (SPSC discipline). Moving it to another thread is sound as long as it stays
// single-owner, which `&mut self` methods enforce. It is intentionally not Sync.
unsafe impl<E: RingEntry> Send for Producer<E> {}

impl<E: RingEntry> Producer<E> {
    /// Attach a producer to an already-initialised ring.
    ///
    /// # Safety
    /// See [`RingView::attach`]: `ptr` must remain a valid mapping of the ring
    /// for the lifetime of the returned producer, and this must be the only
    /// producer for the ring.
    pub unsafe fn attach(ptr: *mut u8, len: usize) -> Result<Self, RingError> {
        // SAFETY: forwarded contract.
        let view = unsafe { RingView::attach(ptr, len)? };
        let cached_tail = view.tail().load(Ordering::Relaxed);
        let cached_head = view.head().load(Ordering::Acquire);
        Ok(Self {
            view,
            cached_tail,
            cached_head,
        })
    }

    /// Initialise a fresh ring and attach a producer to it in one step.
    ///
    /// # Safety
    /// As [`init_ring`] and [`Producer::attach`].
    pub unsafe fn create(ptr: *mut u8, len: usize, cfg: &RingConfig) -> Result<Self, RingError> {
        // SAFETY: forwarded contract.
        unsafe {
            init_ring::<E>(ptr, len, cfg)?;
            Self::attach(ptr, len)
        }
    }

    /// Total number of entries the ring can hold.
    #[inline]
    pub fn capacity(&self) -> u32 {
        self.view.queue_len
    }

    /// Free slots available right now (refreshes the peer's `head`).
    #[inline]
    pub fn available_capacity(&mut self) -> u32 {
        self.cached_head = self.view.head().load(Ordering::Acquire);
        let used = self
            .cached_tail
            .wrapping_sub(self.cached_head)
            .min(self.view.queue_len);
        self.view.queue_len - used
    }

    #[inline]
    fn full_or_closed(&self) -> PushError {
        if self.view.state().load(Ordering::Acquire) != ring_state::READY {
            PushError::Closed
        } else {
            PushError::Full
        }
    }

    /// Push a single entry. Returns [`PushError::Full`] if there is no room, or
    /// [`PushError::Closed`] if the ring is no longer accepting submissions.
    pub fn try_push(&mut self, e: E) -> Result<(), PushError> {
        let mut used = self.cached_tail.wrapping_sub(self.cached_head);
        if used >= self.view.queue_len {
            // Fast-path capacity estimate said full; refresh the real head.
            self.cached_head = self.view.head().load(Ordering::Acquire);
            used = self.cached_tail.wrapping_sub(self.cached_head);
            if used >= self.view.queue_len {
                return Err(self.full_or_closed());
            }
        }
        let idx = self.cached_tail & self.view.mask;
        // SAFETY: idx < queue_len; `used < queue_len` proves the slot is free.
        unsafe { self.view.write_entry(idx, e) };
        self.cached_tail = self.cached_tail.wrapping_add(1);
        self.view.tail().store(self.cached_tail, Ordering::Release);
        Ok(())
    }

    /// Push as many of `entries` as fit, publishing `tail` once. Returns the
    /// number pushed (may be fewer than requested; spec §8.5). `Err` only if
    /// the ring is closed and nothing could be pushed.
    pub fn push_batch(&mut self, entries: &[E]) -> Result<usize, PushError> {
        if entries.is_empty() {
            return Ok(0);
        }
        self.cached_head = self.view.head().load(Ordering::Acquire);
        let used = self
            .cached_tail
            .wrapping_sub(self.cached_head)
            .min(self.view.queue_len);
        let free = (self.view.queue_len - used) as usize;
        let n = free.min(entries.len());
        if n == 0 {
            return Err(self.full_or_closed());
        }
        for (i, e) in entries[..n].iter().enumerate() {
            let idx = self.cached_tail.wrapping_add(i as u32) & self.view.mask;
            // SAFETY: i < n <= free, so each idx names a currently-free slot.
            unsafe { self.view.write_entry(idx, *e) };
        }
        self.cached_tail = self.cached_tail.wrapping_add(n as u32);
        self.view.tail().store(self.cached_tail, Ordering::Release);
        Ok(n)
    }

    /// Wake the consumer via `notify` iff it has armed `NEEDS_WAKE`, then clear
    /// the flag so further publishes don't re-wake until it re-arms (coalescing,
    /// spec §9.2). Call this after a publish (`try_push`/`push_batch`).
    ///
    /// The `SeqCst` fence pairs with the one in
    /// [`Consumer::prepare_wait`]: between the `Release` publish of `tail` and
    /// this load of the flag, it guarantees that either the consumer's recheck
    /// observes the new `tail` or this load observes `NEEDS_WAKE` — so a
    /// blocking consumer is never missed. Returns whether a wake was issued.
    pub fn notify_consumer<N: Notify>(&self, notify: &N) -> Result<bool, NotifyError> {
        fence(Ordering::SeqCst);
        let flags = self.view.consumer_flags().load(Ordering::Relaxed);
        if flags & ring_flags::NEEDS_WAKE != 0 {
            self.view
                .consumer_flags()
                .fetch_and(!ring_flags::NEEDS_WAKE, Ordering::Relaxed);
            notify.wake()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// The consuming end of a ring (reads entries, advances `head`).
pub struct Consumer<E: RingEntry> {
    view: RingView<E>,
    cached_head: u32,
    cached_tail: u32,
}

// SAFETY: mirror of `Producer` — the Consumer is the sole owner of the consumer
// role (`head` writes, `tail` Acquire reads, reads only published slots). Sound
// to move across threads; intentionally not Sync.
unsafe impl<E: RingEntry> Send for Consumer<E> {}

impl<E: RingEntry> Consumer<E> {
    /// Attach a consumer to an already-initialised ring.
    ///
    /// # Safety
    /// As [`Producer::attach`], but this must be the only consumer.
    pub unsafe fn attach(ptr: *mut u8, len: usize) -> Result<Self, RingError> {
        // SAFETY: forwarded contract.
        let view = unsafe { RingView::attach(ptr, len)? };
        let cached_head = view.head().load(Ordering::Relaxed);
        let cached_tail = view.tail().load(Ordering::Acquire);
        Ok(Self {
            view,
            cached_head,
            cached_tail,
        })
    }

    /// Total number of entries the ring can hold.
    #[inline]
    pub fn capacity(&self) -> u32 {
        self.view.queue_len
    }

    /// Refresh and return the number of entries available to pop. Rejects an
    /// implausible peer `tail` with [`PopError::PeerFaulted`].
    pub fn available_entries(&mut self) -> Result<u32, PopError> {
        let t = self.view.tail().load(Ordering::Acquire);
        if t.wrapping_sub(self.cached_head) > self.view.queue_len {
            return Err(PopError::PeerFaulted);
        }
        self.cached_tail = t;
        Ok(self.cached_tail.wrapping_sub(self.cached_head))
    }

    /// Pop a single entry. `Ok(None)` means empty; `Err` means the peer faulted.
    pub fn try_pop(&mut self) -> Result<Option<E>, PopError> {
        if self.cached_head == self.cached_tail {
            let t = self.view.tail().load(Ordering::Acquire);
            if t.wrapping_sub(self.cached_head) > self.view.queue_len {
                return Err(PopError::PeerFaulted);
            }
            self.cached_tail = t;
            if self.cached_head == self.cached_tail {
                if self.view.state().load(Ordering::Acquire) == ring_state::FAULTED {
                    return Err(PopError::PeerFaulted);
                }
                return Ok(None);
            }
        }
        let idx = self.cached_head & self.view.mask;
        // SAFETY: idx < queue_len; head != tail proves the producer published
        // this slot, so it is not concurrently written.
        let e = unsafe { self.view.read_entry(idx) };
        self.cached_head = self.cached_head.wrapping_add(1);
        self.view.head().store(self.cached_head, Ordering::Release);
        Ok(Some(e))
    }

    /// Pop up to `out.len()` entries, advancing `head` once. Returns the number
    /// popped. `Err` means the peer faulted.
    pub fn pop_batch(&mut self, out: &mut [E]) -> Result<usize, PopError> {
        if out.is_empty() {
            return Ok(0);
        }
        let t = self.view.tail().load(Ordering::Acquire);
        if t.wrapping_sub(self.cached_head) > self.view.queue_len {
            return Err(PopError::PeerFaulted);
        }
        self.cached_tail = t;
        let avail = self.cached_tail.wrapping_sub(self.cached_head) as usize;
        let n = avail.min(out.len());
        if n == 0 {
            return Ok(0);
        }
        for (i, slot) in out[..n].iter_mut().enumerate() {
            let idx = self.cached_head.wrapping_add(i as u32) & self.view.mask;
            // SAFETY: i < n <= avail, so each idx names a published slot.
            *slot = unsafe { self.view.read_entry(idx) };
        }
        self.cached_head = self.cached_head.wrapping_add(n as u32);
        self.view.head().store(self.cached_head, Ordering::Release);
        Ok(n)
    }

    /// Arm-and-recheck before blocking (spec §9.3). Sets `NEEDS_WAKE`, then —
    /// with a `SeqCst` fence in between (pairing with
    /// [`Producer::notify_consumer`]) — rechecks for work. Returns
    /// [`WaitDecision::Ready`] if an entry appeared (the flag is cleared again),
    /// or [`WaitDecision::Block`] if the ring is confirmed empty and the caller
    /// may now block on its notification backend without risking a lost wakeup.
    pub fn prepare_wait(&mut self) -> Result<WaitDecision, PopError> {
        if self.cached_head != self.cached_tail {
            return Ok(WaitDecision::Ready);
        }
        self.view
            .consumer_flags()
            .fetch_or(ring_flags::NEEDS_WAKE, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        let t = self.view.tail().load(Ordering::Acquire);
        if t.wrapping_sub(self.cached_head) > self.view.queue_len {
            return Err(PopError::PeerFaulted);
        }
        self.cached_tail = t;
        if self.cached_head != self.cached_tail {
            self.disarm_wait();
            Ok(WaitDecision::Ready)
        } else {
            Ok(WaitDecision::Block)
        }
    }

    /// Clear the `NEEDS_WAKE` flag (e.g. after deciding not to block).
    #[inline]
    pub fn disarm_wait(&self) {
        self.view
            .consumer_flags()
            .fetch_and(!ring_flags::NEEDS_WAKE, Ordering::Relaxed);
    }
}

/// Convenience bundle for one side of a bidirectional connection: a submission
/// ring we produce into and a completion ring we consume from.
pub struct QueuePair {
    /// Submission ring (we produce).
    pub sq: Producer<SurtSqe>,
    /// Completion ring (we consume).
    pub cq: Consumer<SurtCqe>,
}

/// The peer's view of a [`QueuePair`]: consume submissions, produce completions.
pub struct QueuePairPeer {
    /// Submission ring (we consume).
    pub sq: Consumer<SurtSqe>,
    /// Completion ring (we produce).
    pub cq: Producer<SurtCqe>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // A 64-byte-aligned stack buffer big enough for the tests' largest ring.
    #[repr(C, align(64))]
    struct Aligned([u8; 4096]);

    fn fresh(queue_len: u32) -> Aligned {
        let mut buf = Aligned([0; 4096]);
        let cfg = RingConfig {
            queue_len,
            ring_id: 7,
            feature_flags: surt_abi::feature::REQUIRED_V0_1,
            role: surt_abi::role::PRODUCER,
        };
        // SAFETY: buffer is 64-aligned and 4096 bytes, far larger than any test
        // ring; not concurrently accessed here.
        unsafe { init_ring::<SurtSqe>(buf.0.as_mut_ptr(), buf.0.len(), &cfg).unwrap() };
        buf
    }

    fn sqe(n: u64) -> SurtSqe {
        SurtSqe {
            request_id: n,
            user_data: n.wrapping_mul(3),
            ..Default::default()
        }
    }

    fn pair(buf: &mut Aligned) -> (Producer<SurtSqe>, Consumer<SurtSqe>) {
        let ptr = buf.0.as_mut_ptr();
        let len = buf.0.len();
        // SAFETY: single producer over an initialised ring that outlives the
        // test.
        let p = unsafe { Producer::<SurtSqe>::attach(ptr, len).unwrap() };
        // SAFETY: the matching sole consumer over the same ring.
        let c = unsafe { Consumer::<SurtSqe>::attach(ptr, len).unwrap() };
        (p, c)
    }

    #[test]
    fn init_rejects_bad_queue_len() {
        let mut buf = Aligned([0; 4096]);
        for bad in [0u32, 3, 6, 100, MAX_QUEUE_LEN + 1] {
            let cfg = RingConfig {
                queue_len: bad,
                ring_id: 0,
                feature_flags: 0,
                role: 0,
            };
            // SAFETY: aligned, large buffer.
            let r = unsafe { init_ring::<SurtSqe>(buf.0.as_mut_ptr(), buf.0.len(), &cfg) };
            assert_eq!(r, Err(RingError::BadQueueLen { found: bad }));
        }
    }

    #[test]
    fn attach_rejects_bad_geometry() {
        let mut buf = fresh(8);
        let ptr = buf.0.as_mut_ptr();
        // Too small to even hold the header.
        // SAFETY: ptr valid; we deliberately pass a short len.
        let r = unsafe { Consumer::<SurtSqe>::attach(ptr, 8) };
        assert!(matches!(r, Err(RingError::TooSmall { .. })));
        // Misaligned base.
        // SAFETY: ptr+1 is within the buffer; deliberately misaligned.
        let r = unsafe { Consumer::<SurtSqe>::attach(ptr.wrapping_add(1), 4000) };
        assert!(matches!(r, Err(RingError::Misaligned { .. })));
        // Wrong entry type (SurtCqe is 48 B, header says 80).
        // SAFETY: valid region, but entry size mismatches the header.
        let r = unsafe { Consumer::<SurtCqe>::attach(ptr, buf.0.len()) };
        assert!(matches!(r, Err(RingError::BadEntrySize { .. })));
    }

    #[test]
    fn empty_then_single_roundtrip() {
        let mut buf = fresh(8);
        let (mut p, mut c) = pair(&mut buf);
        assert_eq!(c.try_pop(), Ok(None));
        assert_eq!(p.capacity(), 8);
        assert_eq!(p.available_capacity(), 8);

        p.try_push(sqe(1)).unwrap();
        assert_eq!(c.try_pop(), Ok(Some(sqe(1))));
        assert_eq!(c.try_pop(), Ok(None));
    }

    #[test]
    fn fills_and_reports_full() {
        let mut buf = fresh(4);
        let (mut p, mut c) = pair(&mut buf);
        for i in 0..4 {
            p.try_push(sqe(i)).unwrap();
        }
        assert_eq!(p.available_capacity(), 0);
        assert_eq!(p.try_push(sqe(99)), Err(PushError::Full));
        // Drain one, room for one.
        assert_eq!(c.try_pop(), Ok(Some(sqe(0))));
        p.try_push(sqe(4)).unwrap();
        assert_eq!(p.try_push(sqe(5)), Err(PushError::Full));
    }

    #[test]
    fn wraps_around_many_times() {
        let mut buf = fresh(4);
        let (mut p, mut c) = pair(&mut buf);
        // Push/pop 1000 entries through a 4-slot ring: exercises wraparound.
        for i in 0..1000u64 {
            p.try_push(sqe(i)).unwrap();
            assert_eq!(c.try_pop(), Ok(Some(sqe(i))));
        }
        assert_eq!(c.try_pop(), Ok(None));
    }

    #[test]
    fn batch_push_pop() {
        let mut buf = fresh(8);
        let (mut p, mut c) = pair(&mut buf);
        let batch: [SurtSqe; 6] = core::array::from_fn(|i| sqe(i as u64));
        assert_eq!(p.push_batch(&batch), Ok(6));
        // Only 2 slots remain.
        let more: [SurtSqe; 6] = core::array::from_fn(|i| sqe(100 + i as u64));
        assert_eq!(p.push_batch(&more), Ok(2));
        assert_eq!(p.push_batch(&more), Err(PushError::Full));

        let mut out = [SurtSqe::default(); 10];
        assert_eq!(c.pop_batch(&mut out), Ok(8));
        assert_eq!(out[0], sqe(0));
        assert_eq!(out[5], sqe(5));
        assert_eq!(out[6], sqe(100));
        assert_eq!(out[7], sqe(101));
        assert_eq!(c.pop_batch(&mut out), Ok(0));
    }

    #[test]
    fn detects_implausible_peer_tail() {
        let mut buf = fresh(8);
        let (_p, mut c) = pair(&mut buf);
        // Corrupt the shared tail to a value far beyond head (malicious peer).
        let header = buf.0.as_mut_ptr() as *mut SurtRingHeader;
        // SAFETY: header points to the initialised ring header.
        unsafe { (*header).tail.store(1_000_000, Ordering::Release) };
        assert_eq!(c.try_pop(), Err(PopError::PeerFaulted));
        assert_eq!(c.available_entries(), Err(PopError::PeerFaulted));
    }

    #[test]
    fn ring_bytes_matches_layout() {
        assert_eq!(ring_bytes::<SurtSqe>(8), 192 + 8 * 80);
        assert_eq!(ring_bytes::<SurtCqe>(16), 192 + 16 * 48);
    }

    struct CountNotify(AtomicU32);
    impl Notify for CountNotify {
        fn wake(&self) -> Result<(), NotifyError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn arm(&self) -> Result<(), NotifyError> {
            Ok(())
        }
        fn disarm(&self) -> Result<(), NotifyError> {
            Ok(())
        }
    }

    #[test]
    fn wakeup_arm_recheck_and_coalesce() {
        let mut buf = fresh(4);
        let (mut p, mut c) = pair(&mut buf);
        let n = CountNotify(AtomicU32::new(0));

        // Empty ring: the consumer arms and is told to block.
        assert_eq!(c.prepare_wait(), Ok(WaitDecision::Block));
        // Producer publishes, sees the armed flag, wakes once and clears it.
        p.try_push(sqe(1)).unwrap();
        assert_eq!(p.notify_consumer(&n), Ok(true));
        assert_eq!(n.0.load(Ordering::Relaxed), 1);
        // A further publish must NOT re-wake (coalesced) until re-arm.
        p.try_push(sqe(2)).unwrap();
        assert_eq!(p.notify_consumer(&n), Ok(false));
        assert_eq!(n.0.load(Ordering::Relaxed), 1);

        // With work present, prepare_wait returns Ready without arming.
        assert_eq!(c.try_pop(), Ok(Some(sqe(1))));
        assert_eq!(c.prepare_wait(), Ok(WaitDecision::Ready));
        assert_eq!(c.try_pop(), Ok(Some(sqe(2))));

        // Race the recheck: producer publishes, THEN the consumer prepares to
        // wait — the recheck observes the new tail and returns Ready (no block),
        // re-disarming the flag.
        p.try_push(sqe(3)).unwrap();
        assert_eq!(c.prepare_wait(), Ok(WaitDecision::Ready));
        assert_eq!(p.notify_consumer(&n), Ok(false)); // flag was disarmed
        assert_eq!(n.0.load(Ordering::Relaxed), 1);
    }
}
