//! # `surt-abi` — SURT shared-memory ABI
//!
//! Stable, domain-neutral definitions for the wire format shared between two
//! SURT peers across a shared-memory mapping: identifiers, status codes, the
//! submission/completion descriptors, the ring header, the registered-buffer
//! descriptor, opcodes and feature flags.
//!
//! This crate is the contract. It is `#![no_std]`, allocation-free, and has
//! no dependency on seL4 or on any higher-level protocol. Everything here is
//! plain `#[repr(C)]` data with fixed-width fields — no Rust references, raw
//! pointers, `usize`/`isize`, or `bool` ever crosses the shared ABI (see the
//! SURT spec §6.1). The layout is pinned by the compile-time assertions at the
//! bottom of this file; changing a struct without updating them is a build
//! error.
//!
//! ## Versioning
//! A ring carries [`SURT_MAGIC`], [`SURT_ABI_MAJOR`]/[`SURT_ABI_MINOR`], a
//! feature-flag word, `entry_size` and `queue_len`. A major mismatch is
//! incompatible; a minor mismatch is allowed only when feature negotiation
//! succeeds. Unknown *required* feature flags reject setup; unknown *optional*
//! flags are ignored.

#![cfg_attr(not(test), no_std)]

use core::sync::atomic::{AtomicU32, AtomicU64};

// ===========================================================================
// Magic & version
// ===========================================================================

/// Ring magic, ASCII `"SURT"` (big-endian byte order of the four chars).
pub const SURT_MAGIC: u32 = 0x5355_5254;

/// ABI major version. A mismatch is unconditionally incompatible.
pub const SURT_ABI_MAJOR: u16 = 0;

/// ABI minor version. A mismatch is tolerated only via feature negotiation.
pub const SURT_ABI_MINOR: u16 = 1;

/// Cache line assumed for producer/consumer false-sharing avoidance.
pub const SURT_CACHE_LINE: usize = 64;

// ===========================================================================
// Identifiers (§7.1)
// ===========================================================================
//
// Every cross-domain identifier is a transparent `u64`. Where an identifier
// names a reusable slot (buffers especially) it carries a generation in its
// high bits so a stale reference can be rejected after the slot is recycled.
//
// Split (implementation-defined but fixed here and documented in the spec):
//   * low  40 bits: index
//   * high 24 bits: generation
// The remaining top bits are zero and reserved.

/// Number of low bits used for the index portion of a generationed id.
pub const SURT_ID_INDEX_BITS: u32 = 40;
/// Mask selecting the index portion.
pub const SURT_ID_INDEX_MASK: u64 = (1u64 << SURT_ID_INDEX_BITS) - 1;
/// Number of bits used for the generation portion of a generationed id.
pub const SURT_ID_GENERATION_BITS: u32 = 24;
/// Mask selecting the generation portion (before shifting into place).
pub const SURT_ID_GENERATION_MASK: u64 = (1u64 << SURT_ID_GENERATION_BITS) - 1;

macro_rules! define_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[repr(transparent)]
        #[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
        pub struct $name(pub u64);

        impl $name {
            /// The reserved "no id" value.
            pub const NULL: Self = Self(0);

            /// Wrap a raw 64-bit value.
            #[inline]
            pub const fn new(raw: u64) -> Self { Self(raw) }

            /// The raw 64-bit value.
            #[inline]
            pub const fn raw(self) -> u64 { self.0 }

            /// True if this is the reserved null id (`0`).
            #[inline]
            pub const fn is_null(self) -> bool { self.0 == 0 }

            /// Compose an id from an `index` and a `generation` using the
            /// documented 24/40 split. Bits outside each field are masked off.
            #[inline]
            pub const fn from_parts(index: u64, generation: u32) -> Self {
                let g = (generation as u64) & SURT_ID_GENERATION_MASK;
                Self((g << SURT_ID_INDEX_BITS) | (index & SURT_ID_INDEX_MASK))
            }

            /// The index portion (low 40 bits).
            #[inline]
            pub const fn index(self) -> u64 { self.0 & SURT_ID_INDEX_MASK }

            /// The generation portion (high 24 bits).
            #[inline]
            pub const fn generation(self) -> u32 {
                ((self.0 >> SURT_ID_INDEX_BITS) & SURT_ID_GENERATION_MASK) as u32
            }
        }

        impl core::fmt::Debug for $name {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                write!(f, concat!(stringify!($name), "(0x{:016x})"), self.0)
            }
        }
    };
}

define_id!(/// Identifies a single submitted request through to its completion.
    RequestId);
define_id!(/// Identifies a registered shared buffer (carries a generation).
    BufferId);
define_id!(/// Identifies a ring within a connection.
    RingId);
define_id!(/// Identifies a peer of a connection.
    PeerId);
define_id!(/// Protocol-defined target object/service for a request.
    ObjectId);
define_id!(/// Opaque caller cookie, returned unchanged in the completion.
    UserData);

// ===========================================================================
// Status (§7.2)
// ===========================================================================

/// Wire-visible status code. Non-negative values are non-error outcomes
/// (`OK`, `PENDING`, `CANCELLED`); negative values are errors. Higher-level
/// protocols may map this onto their own status space.
#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SurtStatus(pub i32);

impl SurtStatus {
    /// Operation succeeded.
    pub const OK: Self = Self(0);
    /// Operation accepted and still in flight.
    pub const PENDING: Self = Self(1);
    /// Operation was cancelled before completing.
    pub const CANCELLED: Self = Self(2);

    /// A supplied argument was invalid.
    pub const INVALID_ARGUMENT: Self = Self(-1);
    /// The ring/connection was in the wrong state for the operation.
    pub const INVALID_STATE: Self = Self(-2);
    /// A descriptor was malformed.
    pub const INVALID_DESCRIPTOR: Self = Self(-3);
    /// The submission queue was full.
    pub const QUEUE_FULL: Self = Self(-4);
    /// The queue was empty.
    pub const QUEUE_EMPTY: Self = Self(-5);
    /// A referenced buffer was invalid (bounds/rights/unknown).
    pub const BAD_BUFFER: Self = Self(-6);
    /// A buffer id referred to a recycled slot (generation mismatch).
    pub const BAD_GENERATION: Self = Self(-7);
    /// The peer closed the connection.
    pub const PEER_CLOSED: Self = Self(-8);
    /// The operation/feature is not supported.
    pub const UNSUPPORTED: Self = Self(-9);
    /// Out of memory during setup.
    pub const NO_MEMORY: Self = Self(-10);
    /// The peer faulted.
    pub const FAULTED: Self = Self(-11);

    /// The raw `i32` code.
    #[inline]
    pub const fn raw(self) -> i32 {
        self.0
    }

    /// True for non-error outcomes (`>= 0`).
    #[inline]
    pub const fn is_ok(self) -> bool {
        self.0 >= 0
    }

    /// True for error outcomes (`< 0`).
    #[inline]
    pub const fn is_err(self) -> bool {
        self.0 < 0
    }
}

impl core::fmt::Debug for SurtStatus {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let name = match *self {
            Self::OK => "OK",
            Self::PENDING => "PENDING",
            Self::CANCELLED => "CANCELLED",
            Self::INVALID_ARGUMENT => "INVALID_ARGUMENT",
            Self::INVALID_STATE => "INVALID_STATE",
            Self::INVALID_DESCRIPTOR => "INVALID_DESCRIPTOR",
            Self::QUEUE_FULL => "QUEUE_FULL",
            Self::QUEUE_EMPTY => "QUEUE_EMPTY",
            Self::BAD_BUFFER => "BAD_BUFFER",
            Self::BAD_GENERATION => "BAD_GENERATION",
            Self::PEER_CLOSED => "PEER_CLOSED",
            Self::UNSUPPORTED => "UNSUPPORTED",
            Self::NO_MEMORY => "NO_MEMORY",
            Self::FAULTED => "FAULTED",
            _ => return write!(f, "SurtStatus({})", self.0),
        };
        write!(f, "SurtStatus::{name}")
    }
}

// ===========================================================================
// Opcodes (§7.3)
// ===========================================================================

/// Transport-reserved and range-boundary opcodes. Domain protocols choose
/// their own opcodes from [`opcode::DOMAIN_MIN`]..=[`opcode::DOMAIN_MAX`];
/// local/experimental use takes [`opcode::EXPERIMENTAL_MIN`]..=`0xffff`.
pub mod opcode {
    /// No-op.
    pub const NOP: u16 = 0x0000;
    /// Liveness ping.
    pub const PING: u16 = 0x0001;
    /// Begin orderly shutdown.
    pub const SHUTDOWN: u16 = 0x0002;
    /// Cancel an in-flight request (target in `request_id`).
    pub const CANCEL: u16 = 0x0003;
    /// Register a shared buffer.
    pub const REGISTER_BUFFER: u16 = 0x0004;
    /// Unregister a shared buffer.
    pub const UNREGISTER_BUFFER: u16 = 0x0005;
    /// Ordering barrier within a single ring.
    pub const BARRIER: u16 = 0x0006;
    /// Query negotiated features.
    pub const FEATURE_QUERY: u16 = 0x0007;

    /// First opcode reserved for domain protocols (inclusive).
    pub const DOMAIN_MIN: u16 = 0x1000;
    /// Last opcode reserved for domain protocols (inclusive).
    pub const DOMAIN_MAX: u16 = 0xefff;
    /// First opcode reserved for experimental/local use (inclusive).
    pub const EXPERIMENTAL_MIN: u16 = 0xf000;

    /// True if `op` is a transport-reserved opcode (`0x0000..=0x0007`).
    #[inline]
    pub const fn is_transport(op: u16) -> bool {
        op <= FEATURE_QUERY
    }

    /// True if `op` falls in the domain-protocol range.
    #[inline]
    pub const fn is_domain(op: u16) -> bool {
        op >= DOMAIN_MIN && op <= DOMAIN_MAX
    }

    /// True if `op` falls in the experimental/local range.
    #[inline]
    pub const fn is_experimental(op: u16) -> bool {
        op >= EXPERIMENTAL_MIN
    }
}

// ===========================================================================
// Feature flags (§12)
// ===========================================================================

/// Feature-flag bits negotiated at setup. Unknown *required* bits reject the
/// connection; unknown *optional* bits are ignored.
pub mod feature {
    /// Batched submit/complete operations.
    pub const BATCH: u64 = 1 << 0;
    /// Registered (pre-mapped) buffers referenced by id.
    pub const REGISTERED_BUFFERS: u64 = 1 << 1;
    /// Coalesced wakeups via the needs-wake flag.
    pub const COALESCED_NOTIFY: u64 = 1 << 2;
    /// Per-ring ordering barrier opcode.
    pub const BARRIER: u64 = 1 << 3;
    /// Ring reset support.
    pub const RESET: u64 = 1 << 4;
    /// Trace event emission.
    pub const TRACE: u64 = 1 << 5;
    /// 64-bit head/tail counters (vs 32-bit).
    pub const SIXTYFOUR_BIT_COUNTERS: u64 = 1 << 6;
    /// Shared completion ring across multiple submit rings.
    pub const SHARED_COMPLETION: u64 = 1 << 7;

    /// Features every v0.1 peer must implement.
    pub const REQUIRED_V0_1: u64 = BATCH | REGISTERED_BUFFERS | COALESCED_NOTIFY;

    /// All bits defined by this ABI version. Bits outside this mask are
    /// "unknown" for negotiation purposes.
    pub const KNOWN: u64 = BATCH
        | REGISTERED_BUFFERS
        | COALESCED_NOTIFY
        | BARRIER
        | RESET
        | TRACE
        | SIXTYFOUR_BIT_COUNTERS
        | SHARED_COMPLETION;
}

// ===========================================================================
// Buffer rights (§11.2)
// ===========================================================================

/// Access-rights bits for a registered buffer.
pub mod rights {
    /// Readable by the peer.
    pub const READ: u32 = 1 << 0;
    /// Writable by the peer.
    pub const WRITE: u32 = 1 << 1;
    /// Eligible for DMA (policy belongs to a higher layer — no IOMMU
    /// semantics are implied by this bit).
    pub const DMA: u32 = 1 << 2;
    /// Persistently shared (not consumed by a single request).
    pub const SHARED: u32 = 1 << 3;
    /// Valid for a single use, then auto-invalidated.
    pub const ONE_SHOT: u32 = 1 << 4;
    /// Visible/mappable by the peer at all.
    pub const PEER_VISIBLE: u32 = 1 << 5;

    /// Bits required by v0.1.
    pub const REQUIRED_V0_1: u32 = READ | WRITE | SHARED;
}

// ===========================================================================
// Ring role / state / flags (§7.5, §8, §9)
// ===========================================================================

/// The role a ring header plays for its local owner.
pub mod role {
    /// Local owner produces into this ring.
    pub const PRODUCER: u16 = 0;
    /// Local owner consumes from this ring.
    pub const CONSUMER: u16 = 1;
}

/// Lifecycle state stored in `SurtRingHeader::state`.
pub mod ring_state {
    /// Memory not yet initialised.
    pub const UNINIT: u32 = 0;
    /// Setup in progress (handshake / mapping).
    pub const SETUP: u32 = 1;
    /// Ready for traffic.
    pub const READY: u32 = 2;
    /// Draining in-flight work before close.
    pub const DRAINING: u32 = 3;
    /// Closed; no further traffic.
    pub const CLOSED: u32 = 4;
    /// A peer faulted; recover via the control path.
    pub const FAULTED: u32 = 5;
}

/// Producer/consumer side flags stored in the per-side header words.
pub mod ring_flags {
    /// Set by the consumer to request a wakeup; observed by the producer
    /// after publishing entries (the lost-wakeup-avoidance protocol, §9.3).
    pub const NEEDS_WAKE: u32 = 1 << 0;
}

/// Transport-defined bits within `SurtSqe::flags`. The remaining bits are
/// protocol-defined.
pub mod sqe_flags {
    /// Two low bits encode the buffer-ownership mode for the request (§11.5).
    pub const OWNERSHIP_MASK: u16 = 0b11;
    /// Peer may read the referenced buffer but must not modify it.
    pub const OWN_BORROWED_READ: u16 = 0;
    /// Peer may write the referenced buffer.
    pub const OWN_BORROWED_WRITE: u16 = 1;
    /// The buffer is owned by the request until completion (the submitter must
    /// not touch it meanwhile).
    pub const OWN_UNTIL_COMPLETION: u16 = 2;
    /// The buffer is persistently shared across requests.
    pub const OWN_SHARED_PERSISTENT: u16 = 3;
}

// ===========================================================================
// Descriptors (§7.3, §7.4, §7.2 buffer)
// ===========================================================================

/// Submission Queue Entry — a domain-neutral request descriptor.
///
/// `opcode`/`object_id`/`arg0..3` are interpreted by the protocol layer; the
/// transport only validates structural fields (opcode range if configured,
/// `buffer_id`/`offset`/`len` against the buffer registry). `user_data` is an
/// opaque cookie echoed verbatim in the matching [`SurtCqe`].
#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct SurtSqe {
    /// Protocol-defined operation (see [`opcode`] for transport-reserved ones).
    pub opcode: u16,
    /// Transport and protocol flags.
    pub flags: u16,
    /// Byte length associated with `buffer_id`/`offset`, if any.
    pub len: u32,
    /// Stable request identity, echoed in the completion.
    pub request_id: u64,
    /// Protocol-defined target object/service.
    pub object_id: u64,
    /// Registered buffer identity, or `0` if unused.
    pub buffer_id: u64,
    /// Byte offset within the registered buffer (or protocol-defined offset).
    pub offset: u64,
    /// Opaque caller cookie, returned unchanged in the completion.
    pub user_data: u64,
    /// Protocol-defined fixed argument 0.
    pub arg0: u64,
    /// Protocol-defined fixed argument 1.
    pub arg1: u64,
    /// Protocol-defined fixed argument 2.
    pub arg2: u64,
    /// Protocol-defined fixed argument 3.
    pub arg3: u64,
}

/// Completion Queue Entry — the result of a submitted request.
#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct SurtCqe {
    /// The request being completed.
    pub request_id: u64,
    /// Copied from the SQE unless the protocol overrides it.
    pub user_data: u64,
    /// [`SurtStatus`] (or a protocol-mapped status), stored raw.
    pub status: i32,
    /// Completion flags.
    pub flags: u32,
    /// Protocol-defined result length/count/value.
    pub information: u64,
    /// Protocol-defined detail 0.
    pub detail0: u64,
    /// Protocol-defined detail 1.
    pub detail1: u64,
}

/// Registered-buffer descriptor (§11.2).
#[repr(C)]
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct SurtBufferDesc {
    /// Buffer identity (carries a generation; see [`BufferId`]).
    pub buffer_id: u64,
    /// Generation counter; bumped on each re-registration of the slot.
    pub generation: u32,
    /// [`rights`] bits.
    pub flags: u32,
    /// Buffer length in bytes.
    pub len: u64,
    /// Owning peer.
    pub owner_peer_id: u64,
    /// [`rights`] granted to the peer.
    pub rights: u32,
    /// Reserved; must be zero.
    pub reserved: u32,
}

/// Ring header (§7.5).
///
/// Laid out as three 64-byte cache lines so the producer's `tail` and the
/// consumer's `head` never share a line (false-sharing avoidance): a constant
/// header line, a producer-owned line, then a consumer-owned line. The exact
/// offsets are pinned by the assertions at the bottom of this file.
#[repr(C, align(64))]
pub struct SurtRingHeader {
    // --- Header cache line (0..64), constant after setup ---
    /// [`SURT_MAGIC`].
    pub magic: u32,
    /// ABI major (`SURT_ABI_MAJOR`).
    pub abi_major: u16,
    /// ABI minor (`SURT_ABI_MINOR`).
    pub abi_minor: u16,
    /// [`role`] of the owner that produces/consumes this ring.
    pub role: u16,
    /// Size in bytes of each ring entry that follows the header.
    pub entry_size: u16,
    /// Number of entries (power of two).
    pub queue_len: u32,
    /// Negotiated [`feature`] bits.
    pub feature_flags: u64,
    /// [`RingId`] raw value.
    pub ring_id: u64,
    /// [`ring_state`] lifecycle value.
    pub state: AtomicU32,
    /// Reserved; must be zero.
    pub reserved0: u32,
    _pad_header: [u8; 24],

    // --- Producer-owned cache line (64..128) ---
    /// Producer write cursor (monotonic, wraps via `& (queue_len - 1)`).
    pub tail: AtomicU32,
    /// Producer-side [`ring_flags`].
    pub producer_flags: AtomicU32,
    /// Producer sequence (diagnostics / 64-bit-counter feature).
    pub producer_seq: AtomicU64,
    _pad_producer: [u8; 48],

    // --- Consumer-owned cache line (128..192) ---
    /// Consumer read cursor (monotonic, wraps via `& (queue_len - 1)`).
    pub head: AtomicU32,
    /// Consumer-side [`ring_flags`] (e.g. [`ring_flags::NEEDS_WAKE`]).
    pub consumer_flags: AtomicU32,
    /// Consumer sequence (diagnostics / 64-bit-counter feature).
    pub consumer_seq: AtomicU64,
    _pad_consumer: [u8; 48],
}

impl SurtRingHeader {
    /// Total size of the header in bytes (three cache lines).
    pub const SIZE: usize = core::mem::size_of::<Self>();

    /// Build an initialised header with `head`/`tail`/sequence counters and
    /// side flags zeroed and `magic`/version stamped. `state` is one of
    /// [`ring_state`]. Padding is zeroed. This is the only way to construct the
    /// header outside this crate (the `_pad_*` fields are private).
    pub const fn new(
        role: u16,
        entry_size: u16,
        queue_len: u32,
        feature_flags: u64,
        ring_id: u64,
        state: u32,
    ) -> Self {
        Self {
            magic: SURT_MAGIC,
            abi_major: SURT_ABI_MAJOR,
            abi_minor: SURT_ABI_MINOR,
            role,
            entry_size,
            queue_len,
            feature_flags,
            ring_id,
            state: AtomicU32::new(state),
            reserved0: 0,
            _pad_header: [0; 24],
            tail: AtomicU32::new(0),
            producer_flags: AtomicU32::new(0),
            producer_seq: AtomicU64::new(0),
            _pad_producer: [0; 48],
            head: AtomicU32::new(0),
            consumer_flags: AtomicU32::new(0),
            consumer_seq: AtomicU64::new(0),
            _pad_consumer: [0; 48],
        }
    }
}

// ===========================================================================
// Compile-time layout assertions (§3.3, §6, §7.5)
// ===========================================================================
//
// These pin the wire format. A field reorder, type change, or padding shift
// that alters the layout breaks the build here rather than silently corrupting
// cross-peer communication.

const _: () = {
    use core::mem::{align_of, offset_of, size_of};

    // Identifiers and status are transparent 64/32-bit values.
    assert!(size_of::<RequestId>() == 8 && align_of::<RequestId>() == 8);
    assert!(size_of::<BufferId>() == 8);
    assert!(size_of::<RingId>() == 8);
    assert!(size_of::<PeerId>() == 8);
    assert!(size_of::<ObjectId>() == 8);
    assert!(size_of::<UserData>() == 8);
    assert!(size_of::<SurtStatus>() == 4 && align_of::<SurtStatus>() == 4);

    // SQE: 2 + 2 + 4 + 9*8 = 80 bytes, 8-aligned, no tail padding.
    assert!(size_of::<SurtSqe>() == 80);
    assert!(align_of::<SurtSqe>() == 8);
    assert!(offset_of!(SurtSqe, opcode) == 0);
    assert!(offset_of!(SurtSqe, flags) == 2);
    assert!(offset_of!(SurtSqe, len) == 4);
    assert!(offset_of!(SurtSqe, request_id) == 8);
    assert!(offset_of!(SurtSqe, arg3) == 72);

    // CQE: 8 + 8 + 4 + 4 + 3*8 = 48 bytes, 8-aligned.
    assert!(size_of::<SurtCqe>() == 48);
    assert!(align_of::<SurtCqe>() == 8);
    assert!(offset_of!(SurtCqe, status) == 16);
    assert!(offset_of!(SurtCqe, flags) == 20);
    assert!(offset_of!(SurtCqe, information) == 24);

    // Buffer descriptor: 8 + 4 + 4 + 8 + 8 + 4 + 4 = 40 bytes.
    assert!(size_of::<SurtBufferDesc>() == 40);
    assert!(align_of::<SurtBufferDesc>() == 8);

    // Ring header: three 64-byte cache lines, producer/consumer separated.
    assert!(size_of::<SurtRingHeader>() == 192);
    assert!(align_of::<SurtRingHeader>() == 64);
    assert!(offset_of!(SurtRingHeader, magic) == 0);
    assert!(offset_of!(SurtRingHeader, tail) == 64);
    assert!(offset_of!(SurtRingHeader, head) == 128);
    // `head` (consumer) and `tail` (producer) must be on different lines.
    assert!(offset_of!(SurtRingHeader, head) - offset_of!(SurtRingHeader, tail) == SURT_CACHE_LINE);

    // Generation split sanity.
    assert!(SURT_ID_INDEX_BITS + SURT_ID_GENERATION_BITS == 64);

    // Required features are a subset of known features.
    assert!(feature::REQUIRED_V0_1 & !feature::KNOWN == 0);
};

// ===========================================================================
// Tests (run on the host with `cargo test -p surt-abi`)
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_generation_roundtrip() {
        let idx = 0x01_2345_6789u64; // fits in 40 bits
        let gen = 0x00ab_cdefu32 & (SURT_ID_GENERATION_MASK as u32); // 24 bits
        let id = BufferId::from_parts(idx, gen);
        assert_eq!(id.index(), idx);
        assert_eq!(id.generation(), gen);
        assert!(!id.is_null());
        assert_eq!(BufferId::NULL.raw(), 0);
        assert!(BufferId::NULL.is_null());
    }

    #[test]
    fn id_parts_are_masked() {
        // Index over 40 bits and generation over 24 bits are truncated, not
        // allowed to bleed into neighbouring fields.
        let id = RequestId::from_parts(u64::MAX, u32::MAX);
        assert_eq!(id.index(), SURT_ID_INDEX_MASK);
        assert_eq!(id.generation() as u64, SURT_ID_GENERATION_MASK);
        // The two fields together fill the whole 64-bit word.
        assert_eq!(id.raw(), u64::MAX);
    }

    #[test]
    fn status_ok_vs_err() {
        assert!(SurtStatus::OK.is_ok());
        assert!(SurtStatus::PENDING.is_ok());
        assert!(SurtStatus::CANCELLED.is_ok());
        assert!(SurtStatus::QUEUE_FULL.is_err());
        assert!(SurtStatus::BAD_GENERATION.is_err());
        assert_eq!(SurtStatus::OK.raw(), 0);
    }

    #[test]
    fn opcode_ranges_are_disjoint() {
        assert!(opcode::is_transport(opcode::NOP));
        assert!(opcode::is_transport(opcode::FEATURE_QUERY));
        assert!(!opcode::is_transport(opcode::DOMAIN_MIN));
        assert!(opcode::is_domain(opcode::DOMAIN_MIN));
        assert!(opcode::is_domain(opcode::DOMAIN_MAX));
        assert!(!opcode::is_domain(opcode::EXPERIMENTAL_MIN));
        assert!(opcode::is_experimental(opcode::EXPERIMENTAL_MIN));
        assert!(opcode::is_experimental(0xffff));
    }

    #[test]
    fn feature_required_subset_of_known() {
        assert_eq!(feature::REQUIRED_V0_1 & !feature::KNOWN, 0);
        assert_ne!(feature::REQUIRED_V0_1 & feature::BATCH, 0);
        assert_ne!(feature::REQUIRED_V0_1 & feature::REGISTERED_BUFFERS, 0);
        assert_ne!(feature::REQUIRED_V0_1 & feature::COALESCED_NOTIFY, 0);
        // An optional feature is not required.
        assert_eq!(feature::REQUIRED_V0_1 & feature::RESET, 0);
    }

    #[test]
    fn descriptors_default_to_zero() {
        let sqe = SurtSqe::default();
        assert_eq!(sqe.opcode, 0);
        assert_eq!(sqe.arg3, 0);
        let cqe = SurtCqe::default();
        assert_eq!(cqe.status, 0);
        let buf = SurtBufferDesc::default();
        assert_eq!(buf.reserved, 0);
    }

    #[test]
    fn ring_header_cache_lines_separated() {
        // Re-checked at runtime as a guard against accidental edits.
        assert_eq!(core::mem::size_of::<SurtRingHeader>(), 192);
        assert_eq!(core::mem::align_of::<SurtRingHeader>(), 64);
        assert_eq!(core::mem::offset_of!(SurtRingHeader, tail), 64);
        assert_eq!(core::mem::offset_of!(SurtRingHeader, head), 128);
        assert_eq!(SurtRingHeader::SIZE, 192);
    }
}
