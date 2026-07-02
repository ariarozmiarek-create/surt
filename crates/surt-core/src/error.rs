//! Error types for ring construction and operation.

use core::fmt;

/// Failure attaching to or initialising a ring projection over shared bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RingError {
    /// The backing pointer was null.
    NullPointer,
    /// The backing pointer was not aligned to the required boundary.
    Misaligned {
        /// Required alignment in bytes.
        required: usize,
    },
    /// The backing region was too small for the header + entries.
    TooSmall {
        /// Bytes required.
        needed: usize,
        /// Bytes available.
        got: usize,
    },
    /// The header magic did not match [`surt_abi::SURT_MAGIC`].
    BadMagic {
        /// The value found in the header.
        found: u32,
    },
    /// The header ABI major version was incompatible.
    BadAbiMajor {
        /// The value found in the header.
        found: u16,
    },
    /// The header `entry_size` did not match the entry type in use.
    BadEntrySize {
        /// Expected entry size (`size_of::<E>()`).
        expected: u16,
        /// The value found in the header.
        found: u16,
    },
    /// The header `queue_len` was not a power of two in `1..=2^31`.
    BadQueueLen {
        /// The value found in the header.
        found: u32,
    },
    /// The header was in a state that forbids attaching (e.g. `UNINIT`).
    BadState {
        /// The value found in the header.
        found: u32,
    },
}

impl fmt::Display for RingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NullPointer => write!(f, "ring backing pointer is null"),
            Self::Misaligned { required } => {
                write!(f, "ring backing pointer is not {required}-byte aligned")
            }
            Self::TooSmall { needed, got } => {
                write!(f, "ring backing region too small: need {needed}, got {got}")
            }
            Self::BadMagic { found } => write!(f, "bad ring magic: 0x{found:08x}"),
            Self::BadAbiMajor { found } => write!(f, "incompatible ABI major: {found}"),
            Self::BadEntrySize { expected, found } => {
                write!(f, "entry size mismatch: expected {expected}, found {found}")
            }
            Self::BadQueueLen { found } => write!(f, "bad queue_len: {found}"),
            Self::BadState { found } => write!(f, "ring in unusable state: {found}"),
        }
    }
}

impl core::error::Error for RingError {}

/// Failure pushing into a submission ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushError {
    /// No free slots; the consumer has not caught up.
    Full,
    /// The ring is no longer accepting submissions (closed/draining/faulted).
    Closed,
}

impl fmt::Display for PushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => write!(f, "ring full"),
            Self::Closed => write!(f, "ring closed"),
        }
    }
}

impl core::error::Error for PushError {}

/// Failure popping from a ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopError {
    /// The peer advanced a cursor implausibly or marked the ring faulted —
    /// the connection must be torn down via the control path.
    PeerFaulted,
}

impl fmt::Display for PopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PeerFaulted => write!(f, "peer faulted (implausible cursor or fault state)"),
        }
    }
}

impl core::error::Error for PopError {}

/// Failure registering, releasing, or looking up a buffer in the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferError {
    /// No free registry slot.
    RegistryFull,
    /// The id's slot index is out of range or unoccupied.
    UnknownId,
    /// The id named a recycled slot (generation mismatch) — spec §11.3.
    BadGeneration,
    /// `offset + len` exceeded the registered buffer length — spec §11.4.
    OutOfBounds {
        /// Requested offset.
        offset: u64,
        /// Requested length.
        len: u64,
        /// The buffer's registered length.
        buffer_len: u64,
    },
    /// The requested access rights were not all granted to the buffer.
    RightsViolation {
        /// Rights requested by the access.
        requested: u32,
        /// Rights granted at registration.
        granted: u32,
    },
    /// A zero-length buffer cannot be registered.
    ZeroLength,
}

impl fmt::Display for BufferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RegistryFull => write!(f, "buffer registry full"),
            Self::UnknownId => write!(f, "unknown buffer id"),
            Self::BadGeneration => write!(f, "buffer id generation mismatch (stale)"),
            Self::OutOfBounds {
                offset,
                len,
                buffer_len,
            } => write!(
                f,
                "buffer access out of bounds: offset {offset} + len {len} > {buffer_len}"
            ),
            Self::RightsViolation { requested, granted } => write!(
                f,
                "buffer rights violation: requested 0x{requested:x}, granted 0x{granted:x}"
            ),
            Self::ZeroLength => write!(f, "cannot register a zero-length buffer"),
        }
    }
}

impl core::error::Error for BufferError {}
