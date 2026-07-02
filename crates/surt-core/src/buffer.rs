//! Registered-buffer registry and descriptor validation (spec §11, §16.2).
//!
//! A peer pre-registers shared buffers so descriptors can refer to payload by
//! [`BufferId`] + `offset`/`len` instead of by raw pointer (spec §3.3, §11.1).
//! Each registry slot carries a **generation** counter: unregistering a buffer
//! bumps the generation, so any descriptor still holding the old id is rejected
//! with [`BufferError::BadGeneration`] rather than silently aliasing a recycled
//! slot (§11.3).
//!
//! The registry is fixed-capacity (`const CAP`), so it needs no allocation, and
//! [`BufferRegistry::lookup`] (the hot-path, per-request check) is a bounded
//! array index plus a few comparisons. Buffer *base addresses* are stored as
//! `u64` tokens — the registry validates id/generation/bounds/rights and hands
//! back a validated [`BufferRegion`]; actually dereferencing that region is the
//! protocol layer's concern, with provenance from the real mapping.

use surt_abi::{opcode, sqe_flags, BufferId, SurtSqe, SurtStatus, SURT_ID_GENERATION_MASK};

use crate::error::BufferError;

/// A validated sub-region of a registered buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferRegion {
    /// Base address of the sub-region (`buffer base + offset`).
    pub addr: u64,
    /// Length in bytes.
    pub len: u64,
    /// Rights granted to the whole buffer at registration.
    pub rights: u32,
}

/// Buffer-ownership mode for a single request (spec §11.5), decoded from the
/// low bits of `SurtSqe::flags`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ownership {
    /// Peer reads the buffer; the submitter must not assume it can modify it.
    BorrowedRead,
    /// Peer writes the buffer.
    BorrowedWrite,
    /// Owned by the request until completion (submitter must not touch it).
    OwnedUntilCompletion,
    /// Persistently shared across requests.
    SharedPersistent,
}

impl Ownership {
    /// Decode the ownership mode from a `SurtSqe::flags` word.
    #[inline]
    pub const fn from_flags(flags: u16) -> Self {
        match flags & sqe_flags::OWNERSHIP_MASK {
            0 => Self::BorrowedRead,
            1 => Self::BorrowedWrite,
            2 => Self::OwnedUntilCompletion,
            _ => Self::SharedPersistent,
        }
    }

    /// Decode the ownership mode of a submission entry.
    #[inline]
    pub const fn from_sqe(sqe: &SurtSqe) -> Self {
        Self::from_flags(sqe.flags)
    }

    /// True if, under this mode, the submitter must treat the buffer as
    /// off-limits until the request completes.
    #[inline]
    pub const fn locked_until_completion(self) -> bool {
        matches!(self, Self::OwnedUntilCompletion)
    }
}

#[derive(Clone, Copy)]
struct Slot {
    occupied: bool,
    /// Always in `1..=SURT_ID_GENERATION_MASK` so a live buffer's id is never 0.
    generation: u32,
    base: u64,
    len: u64,
    rights: u32,
    owner: u64,
}

impl Slot {
    const EMPTY: Self = Self {
        occupied: false,
        generation: 1,
        base: 0,
        len: 0,
        rights: 0,
        owner: 0,
    };
}

/// Advance a slot generation within the 24-bit id field, never landing on `0`.
#[inline]
fn bump_generation(g: u32) -> u32 {
    let mask = SURT_ID_GENERATION_MASK as u32;
    let next = g.wrapping_add(1) & mask;
    if next == 0 {
        1
    } else {
        next
    }
}

/// Fixed-capacity table of registered shared buffers.
pub struct BufferRegistry<const CAP: usize> {
    slots: [Slot; CAP],
    occupied: usize,
}

impl<const CAP: usize> Default for BufferRegistry<CAP> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const CAP: usize> BufferRegistry<CAP> {
    /// Create an empty registry.
    pub const fn new() -> Self {
        Self {
            slots: [Slot::EMPTY; CAP],
            occupied: 0,
        }
    }

    /// Maximum number of simultaneously-registered buffers.
    #[inline]
    pub const fn capacity(&self) -> usize {
        CAP
    }

    /// Number of currently-registered buffers.
    #[inline]
    pub const fn len(&self) -> usize {
        self.occupied
    }

    /// True if no buffers are registered.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.occupied == 0
    }

    /// Register a shared buffer `[base, base+len)` with `rights` (see
    /// [`surt_abi::rights`]) owned by `owner`. Returns the [`BufferId`] (which
    /// embeds the slot's current generation). `O(CAP)` — a setup-path call.
    pub fn register(
        &mut self,
        base: u64,
        len: u64,
        owner: u64,
        rights: u32,
    ) -> Result<BufferId, BufferError> {
        if len == 0 {
            return Err(BufferError::ZeroLength);
        }
        for (i, slot) in self.slots.iter_mut().enumerate() {
            if !slot.occupied {
                slot.occupied = true;
                slot.base = base;
                slot.len = len;
                slot.owner = owner;
                slot.rights = rights;
                self.occupied += 1;
                return Ok(BufferId::from_parts(i as u64, slot.generation));
            }
        }
        Err(BufferError::RegistryFull)
    }

    /// Validate `id` and return its slot index, or a [`BufferError`].
    ///
    /// The generation is checked *before* occupancy so that an id whose slot
    /// was unregistered (which bumps the generation) fails with
    /// [`BufferError::BadGeneration`] rather than `UnknownId` — the stale-id
    /// behaviour the spec mandates (§11.3). A fabricated id whose generation
    /// happens to match an unoccupied slot still falls through to `UnknownId`.
    fn index_of(&self, id: BufferId) -> Result<usize, BufferError> {
        let idx = id.index() as usize;
        if idx >= CAP {
            return Err(BufferError::UnknownId);
        }
        let slot = &self.slots[idx];
        if slot.generation != id.generation() {
            return Err(BufferError::BadGeneration);
        }
        if !slot.occupied {
            return Err(BufferError::UnknownId);
        }
        Ok(idx)
    }

    /// Unregister the buffer named by `id`, bumping its slot generation so the
    /// id (and any descriptor still referencing it) becomes stale (§11.3).
    pub fn unregister(&mut self, id: BufferId) -> Result<(), BufferError> {
        let idx = self.index_of(id)?;
        let slot = &mut self.slots[idx];
        slot.occupied = false;
        slot.base = 0;
        slot.len = 0;
        slot.rights = 0;
        slot.owner = 0;
        slot.generation = bump_generation(slot.generation);
        self.occupied -= 1;
        Ok(())
    }

    /// The owner recorded for a registered buffer.
    pub fn owner(&self, id: BufferId) -> Result<u64, BufferError> {
        let idx = self.index_of(id)?;
        Ok(self.slots[idx].owner)
    }

    /// Validate an access to `[offset, offset+len)` of buffer `id` requiring
    /// `required_rights` (a subset of [`surt_abi::rights`]; pass `0` to check
    /// existence + bounds only). Returns the validated [`BufferRegion`].
    pub fn lookup(
        &self,
        id: BufferId,
        offset: u64,
        len: u64,
        required_rights: u32,
    ) -> Result<BufferRegion, BufferError> {
        let idx = self.index_of(id)?;
        let slot = &self.slots[idx];
        let end = offset.checked_add(len).ok_or(BufferError::OutOfBounds {
            offset,
            len,
            buffer_len: slot.len,
        })?;
        if end > slot.len {
            return Err(BufferError::OutOfBounds {
                offset,
                len,
                buffer_len: slot.len,
            });
        }
        if required_rights & !slot.rights != 0 {
            return Err(BufferError::RightsViolation {
                requested: required_rights,
                granted: slot.rights,
            });
        }
        Ok(BufferRegion {
            // `base + offset` cannot overflow: `offset <= end <= slot.len`, and
            // a registered `[base, base+len)` is a real mapping.
            addr: slot.base.wrapping_add(offset),
            len,
            rights: slot.rights,
        })
    }
}

/// Structurally validate a submission entry against `registry` (the validation
/// deferred from M2). Checks the opcode is in a defined range and, if the entry
/// references a buffer, that the buffer exists and the `[offset, offset+len)`
/// access is in bounds. Rights are protocol-specific, so they are *not* checked
/// here — use [`BufferRegistry::lookup`] with the operation's required rights.
///
/// Returns a [`SurtStatus`] error suitable for a completion's `status` field.
pub fn validate_sqe<const CAP: usize>(
    sqe: &SurtSqe,
    registry: &BufferRegistry<CAP>,
) -> Result<(), SurtStatus> {
    // Opcode: reject the reserved gap between the transport range
    // (`..=FEATURE_QUERY`) and the domain range (`DOMAIN_MIN..`).
    let op = sqe.opcode;
    let valid_op = opcode::is_transport(op) || opcode::is_domain(op) || opcode::is_experimental(op);
    if !valid_op {
        return Err(SurtStatus::INVALID_DESCRIPTOR);
    }

    // Buffer: only checked when one is referenced (`buffer_id != 0`).
    if sqe.buffer_id != 0 {
        match registry.lookup(BufferId::new(sqe.buffer_id), sqe.offset, sqe.len as u64, 0) {
            Ok(_) => {}
            Err(BufferError::BadGeneration) => return Err(SurtStatus::BAD_GENERATION),
            Err(_) => return Err(SurtStatus::BAD_BUFFER),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use surt_abi::rights;

    #[test]
    fn register_lookup_unregister() {
        let mut reg = BufferRegistry::<4>::new();
        assert!(reg.is_empty());
        let id = reg
            .register(0x1_0000, 4096, 42, rights::READ | rights::WRITE)
            .unwrap();
        assert_eq!(reg.len(), 1);
        assert!(!id.is_null());
        assert_eq!(reg.owner(id), Ok(42));

        let r = reg.lookup(id, 256, 512, rights::READ).unwrap();
        assert_eq!(r.addr, 0x1_0000 + 256);
        assert_eq!(r.len, 512);

        reg.unregister(id).unwrap();
        assert!(reg.is_empty());
        // The old id is now stale.
        assert_eq!(reg.lookup(id, 0, 1, 0), Err(BufferError::BadGeneration));
        assert_eq!(reg.unregister(id), Err(BufferError::BadGeneration));
    }

    #[test]
    fn generation_invalidates_recycled_slot() {
        let mut reg = BufferRegistry::<1>::new();
        let a = reg.register(0x2000, 64, 0, rights::READ).unwrap();
        reg.unregister(a).unwrap();
        // Re-registering reuses slot 0 but with a bumped generation.
        let b = reg.register(0x3000, 64, 0, rights::READ).unwrap();
        assert_eq!(a.index(), b.index());
        assert_ne!(a.generation(), b.generation());
        // Old id rejected; new id works and sees the new base.
        assert_eq!(reg.lookup(a, 0, 1, 0), Err(BufferError::BadGeneration));
        assert_eq!(reg.lookup(b, 0, 64, rights::READ).unwrap().addr, 0x3000);
    }

    #[test]
    fn bounds_are_enforced() {
        let mut reg = BufferRegistry::<2>::new();
        let id = reg.register(0x4000, 100, 0, rights::READ).unwrap();
        assert!(reg.lookup(id, 0, 100, 0).is_ok());
        assert!(reg.lookup(id, 100, 0, 0).is_ok()); // empty slice at the end
        assert_eq!(
            reg.lookup(id, 50, 51, 0),
            Err(BufferError::OutOfBounds {
                offset: 50,
                len: 51,
                buffer_len: 100
            })
        );
        // Overflowing offset+len is rejected, not wrapped.
        assert!(matches!(
            reg.lookup(id, u64::MAX, 2, 0),
            Err(BufferError::OutOfBounds { .. })
        ));
    }

    #[test]
    fn rights_are_enforced() {
        let mut reg = BufferRegistry::<2>::new();
        let ro = reg.register(0x5000, 32, 0, rights::READ).unwrap();
        assert!(reg.lookup(ro, 0, 32, rights::READ).is_ok());
        assert_eq!(
            reg.lookup(ro, 0, 32, rights::WRITE),
            Err(BufferError::RightsViolation {
                requested: rights::WRITE,
                granted: rights::READ
            })
        );
        // Requesting no rights always passes the rights check.
        assert!(reg.lookup(ro, 0, 32, 0).is_ok());
    }

    #[test]
    fn registry_full_and_zero_length() {
        let mut reg = BufferRegistry::<2>::new();
        let _ = reg.register(1, 8, 0, 0).unwrap();
        let _ = reg.register(2, 8, 0, 0).unwrap();
        assert_eq!(reg.register(3, 8, 0, 0), Err(BufferError::RegistryFull));
        assert_eq!(reg.register(4, 0, 0, 0), Err(BufferError::ZeroLength));
    }

    #[test]
    fn validate_sqe_checks_opcode_and_buffer() {
        let mut reg = BufferRegistry::<2>::new();
        let id = reg.register(0x6000, 64, 0, rights::READ).unwrap();

        // A domain opcode with a valid buffer reference passes.
        let good = SurtSqe {
            opcode: opcode::DOMAIN_MIN,
            buffer_id: id.raw(),
            offset: 0,
            len: 64,
            ..Default::default()
        };
        assert_eq!(validate_sqe(&good, &reg), Ok(()));

        // Opcode in the reserved gap is rejected.
        let bad_op = SurtSqe {
            opcode: 0x0008,
            ..Default::default()
        };
        assert_eq!(
            validate_sqe(&bad_op, &reg),
            Err(SurtStatus::INVALID_DESCRIPTOR)
        );

        // Out-of-bounds buffer access -> BAD_BUFFER.
        let oob = SurtSqe {
            opcode: opcode::DOMAIN_MIN,
            buffer_id: id.raw(),
            offset: 0,
            len: 65,
            ..Default::default()
        };
        assert_eq!(validate_sqe(&oob, &reg), Err(SurtStatus::BAD_BUFFER));

        // No buffer referenced (buffer_id == 0): buffer check skipped.
        let no_buf = SurtSqe {
            opcode: opcode::NOP,
            buffer_id: 0,
            ..Default::default()
        };
        assert_eq!(validate_sqe(&no_buf, &reg), Ok(()));

        // After unregister the same id is stale -> BAD_GENERATION.
        reg.unregister(id).unwrap();
        assert_eq!(validate_sqe(&good, &reg), Err(SurtStatus::BAD_GENERATION));
    }

    #[test]
    fn ownership_decodes() {
        use sqe_flags::*;
        assert_eq!(
            Ownership::from_flags(OWN_BORROWED_READ),
            Ownership::BorrowedRead
        );
        assert_eq!(
            Ownership::from_flags(OWN_BORROWED_WRITE),
            Ownership::BorrowedWrite
        );
        assert_eq!(
            Ownership::from_flags(OWN_UNTIL_COMPLETION),
            Ownership::OwnedUntilCompletion
        );
        assert_eq!(
            Ownership::from_flags(OWN_SHARED_PERSISTENT),
            Ownership::SharedPersistent
        );
        // High protocol bits don't disturb the ownership decode.
        assert_eq!(
            Ownership::from_flags(0xfffc | OWN_BORROWED_WRITE),
            Ownership::BorrowedWrite
        );
        assert!(Ownership::OwnedUntilCompletion.locked_until_completion());
        assert!(!Ownership::BorrowedRead.locked_until_completion());
    }
}
