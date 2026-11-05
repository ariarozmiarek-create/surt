//! # `surt-sel4` — the seL4 binding for SURT
//!
//! [`surt-core`](surt_core) is the platform-agnostic ring transport: it projects
//! a single-producer / single-consumer ring over caller-provided shared memory
//! and defines the coalesced, lost-wakeup-free notification protocol — but it
//! deliberately knows nothing about seL4. This crate is the thin seL4 glue:
//!
//! - [`Sel4Env`] — the *only* platform contract the fast path needs: the two
//!   syscalls `seL4_Signal` and `seL4_Wait` on a Notification.
//! - [`Sel4Notify`] — a [`surt_core::Notify`] backed by a Notification signalled
//!   through an [`Sel4Env`], plus the blocking `wait` the core omits.
//! - [`drain_blocking`] — a ready-made consumer wait-loop that drains the ring
//!   and blocks only when genuinely idle (coalesced via `prepare_wait`).
//!
//! ## Portability
//!
//! Everything here is generic over [`Sel4Env`], so the same code runs on:
//! - **upstream seL4** — implement [`Sel4Env`] with the `sel4` crate
//!   (`sel4::Notification::signal` / `sel4::Notification::wait`);
//! - **any other seL4-compatible kernel** — implement it with that kernel's ABI.
//!
//! The **control path** (retyping/sharing frames, transferring the frame and
//! notification caps) is intentionally *not* abstracted here. The HELLO /
//! HELLO_ACK codec lives in `surt_abi::hello` (pure, no seL4 calls); the
//! cap-transfer sequence that follows it is issued by the application at setup.

//! design, and the rust-micro rootserver's `surt_ring_cap_transfer` microtest
//! for a complete working reference (two isolated components, own CSpaces,
//! frame + notification caps transferred over IPC).
//!
//! ```no_run
//! use surt_sel4::{Sel4Env, Sel4Notify, CPtr, drain_blocking};
//! use surt_sel4::surt_core::{Consumer, surt_abi::SurtSqe};
//!
//! struct MyKernel; // your platform
//! impl Sel4Env for MyKernel {
//!     fn signal(&self, ntfn: CPtr) { /* seL4_Signal(ntfn) */ }
//!     fn wait(&self, ntfn: CPtr)   { /* seL4_Wait(ntfn, _) */ }
//! }
//!
//! # fn run(mut c: Consumer<SurtSqe>, ntfn: CPtr) {
//! let env = MyKernel;
//! let wake = Sel4Notify::new(&env, ntfn);
//! drain_blocking(&mut c, &wake, |sqe: &SurtSqe| {
//!     // handle one submission; return false to stop.
//!     let _ = sqe;
//!     true
//! });
//! # }
//! ```

#![no_std]

pub use surt_core;
pub use surt_core::surt_abi;

use surt_core::{Consumer, Notify, NotifyError, RingEntry, WaitDecision};

/// A capability pointer (seL4 `CPtr` / `seL4_CPtr`). Opaque to this crate — the
/// [`Sel4Env`] implementation interprets it against whatever CSpace the caller
/// runs in.
pub type CPtr = u64;

/// The seL4 operations SURT's fast-path wakeup needs from the host platform —
/// just the two Notification syscalls. Implement this over the upstream `sel4`
/// crate or your own kernel's ABI.
pub trait Sel4Env {
    /// `seL4_Signal(ntfn)` — wake a peer blocked on the notification. Idempotent;
    /// seL4 notifications latch, so signalling with no waiter is safe.
    fn signal(&self, ntfn: CPtr);
    /// `seL4_Wait(ntfn, _)` — block until the notification is signalled.
    fn wait(&self, ntfn: CPtr);
}

/// A [`surt_core::Notify`] backed by a seL4 Notification signalled through an
/// [`Sel4Env`]. seL4 notifications latch a signal that precedes the wait, so
/// `arm`/`disarm` are no-ops — exactly what the coalesced-wakeup protocol wants.
///
/// The blocking [`wait`](Sel4Notify::wait) is a separate method (not part of the
/// [`Notify`] trait, which is send-only by design): the consumer's wait-loop
/// calls it after [`Consumer::prepare_wait`] returns [`WaitDecision::Block`].
pub struct Sel4Notify<'e, E: Sel4Env> {
    env: &'e E,
    ntfn: CPtr,
}

impl<'e, E: Sel4Env> Sel4Notify<'e, E> {
    /// Wrap the notification `ntfn`, signalled/waited through `env`.
    pub fn new(env: &'e E, ntfn: CPtr) -> Self {
        Self { env, ntfn }
    }

    /// Block until the notification is signalled (the wait half `Notify` omits).
    pub fn wait(&self) {
        self.env.wait(self.ntfn);
    }
}

impl<E: Sel4Env> Notify for Sel4Notify<'_, E> {
    fn wake(&self) -> Result<(), NotifyError> {
        self.env.signal(self.ntfn);
        Ok(())
    }
    fn arm(&self) -> Result<(), NotifyError> {
        Ok(())
    }
    fn disarm(&self) -> Result<(), NotifyError> {
        Ok(())
    }
}

/// Why [`drain_blocking`] returned.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DrainExit {
    /// `handler` asked to stop (returned `false`).
    Stopped,
    /// The peer misbehaved (an out-of-range cursor was detected).
    PeerFaulted,
}

/// Consumer-side blocking drain: pop entries, hand each to `handler`, and block
/// on `wake` only when the ring is genuinely empty — using [`Consumer::prepare_wait`]
/// so a publish racing the sleep is never lost (the M4 coalescing protocol).
///
/// Returns [`DrainExit::Stopped`] when `handler` returns `false`, or
/// [`DrainExit::PeerFaulted`] if the peer corrupts the ring cursors.
pub fn drain_blocking<E, Env, H>(
    consumer: &mut Consumer<E>,
    wake: &Sel4Notify<Env>,
    mut handler: H,
) -> DrainExit
where
    E: RingEntry,
    Env: Sel4Env,
    H: FnMut(&E) -> bool,
{
    loop {
        match consumer.try_pop() {
            Ok(Some(entry)) => {
                if !handler(&entry) {
                    return DrainExit::Stopped;
                }
            }
            // Empty: arm + block, unless a publish already raced in.
            Ok(None) => match consumer.prepare_wait() {
                Ok(WaitDecision::Block) => wake.wait(),
                Ok(WaitDecision::Ready) => {}
                Err(_) => return DrainExit::PeerFaulted,
            },
            Err(_) => return DrainExit::PeerFaulted,
        }
    }
}

/// Consumer-side **busy-poll** drain: pop entries and hand each to `handler`,
/// spinning on an empty ring and **never blocking** — so in steady state the
/// data path issues *zero syscalls* on this side (the SURT equivalent of
/// io_uring's SQPOLL). It needs no [`Sel4Env`]: it never waits. Pair it with a
/// producer whose coalesced `notify_consumer` becomes a no-op because this side
/// never arms — giving a fully syscall-free channel.
///
/// Returns [`DrainExit::Stopped`] when `handler` returns `false`, or
/// [`DrainExit::PeerFaulted`] if the peer corrupts the ring.
///
/// **Deploy on a dedicated core.** A spinning poller starves any peer sharing
/// its core; on a single core use [`drain_adaptive`] or [`drain_blocking`].
pub fn drain_polling<E, H>(consumer: &mut Consumer<E>, mut handler: H) -> DrainExit
where
    E: RingEntry,
    H: FnMut(&E) -> bool,
{
    loop {
        match consumer.try_pop() {
            Ok(Some(entry)) => {
                if !handler(&entry) {
                    return DrainExit::Stopped;
                }
            }
            Ok(None) => core::hint::spin_loop(),
            Err(_) => return DrainExit::PeerFaulted,
        }
    }
}

/// Consumer-side **adaptive** drain: busy-poll while work is flowing, then fall
/// back to a coalesced block once the ring has been empty for `spin_budget`
/// consecutive polls (io_uring's SQPOLL idle-timeout, under your control).
/// Spends *zero syscalls while busy* and only one `wait` after a quiet spell;
/// the producer's `notify_consumer` wakes it via the lost-wakeup-free protocol.
///
/// `spin_budget == 0` is equivalent to [`drain_blocking`] (block immediately on
/// empty). A large budget approaches [`drain_polling`] but still yields the CPU
/// (one `wait`) if the ring ever stays empty past the budget — safe to run on a
/// shared core, unlike [`drain_polling`].
///
/// Returns [`DrainExit::Stopped`] when `handler` returns `false`, or
/// [`DrainExit::PeerFaulted`] if the peer corrupts the ring.
pub fn drain_adaptive<E, Env, H>(
    consumer: &mut Consumer<E>,
    wake: &Sel4Notify<Env>,
    spin_budget: u32,
    mut handler: H,
) -> DrainExit
where
    E: RingEntry,
    Env: Sel4Env,
    H: FnMut(&E) -> bool,
{
    let mut idle: u32 = 0;
    loop {
        match consumer.try_pop() {
            Ok(Some(entry)) => {
                idle = 0;
                if !handler(&entry) {
                    return DrainExit::Stopped;
                }
            }
            Ok(None) => {
                if idle < spin_budget {
                    idle += 1;
                    core::hint::spin_loop();
                } else {
                    // Quiet long enough: arm + block (coalesced, lost-wakeup-free).
                    match consumer.prepare_wait() {
                        Ok(WaitDecision::Block) => wake.wait(),
                        Ok(WaitDecision::Ready) => {}
                        Err(_) => return DrainExit::PeerFaulted,
                    }
                    idle = 0;
                }
            }
            Err(_) => return DrainExit::PeerFaulted,
        }
    }
}
