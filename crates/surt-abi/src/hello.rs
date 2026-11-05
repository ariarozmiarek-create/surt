//! HELLO / HELLO_ACK codec for the SURT control path (spec §6.1).
//!
//! The codec is pure: it maps the message registers to and from typed
//! values and performs no seL4 calls. Message registers are 64-bit words.

use crate::{feature, SurtStatus, SURT_ABI_MAJOR, SURT_ABI_MINOR, SURT_MAGIC};

/// Directionality of a connection (HELLO MR4).
pub mod direction {
    /// One-way ring: the initiator only produces.
    pub const ONE_WAY: u64 = 0;
    /// Queue pair: submit and completion rings.
    pub const QUEUE_PAIR: u64 = 1;
}

/// Initiator request (HELLO).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hello {
    pub abi_major: u16,
    pub abi_minor: u16,
    pub features: u64,
    pub queue_len: u32,
    pub direction: u64,
}

/// Acceptor reply (HELLO_ACK).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HelloAck {
    pub status: SurtStatus,
    pub abi_major: u16,
    pub abi_minor: u16,
    pub accepted_features: u64,
    pub queue_len: u32,
    pub peer_id_base: u64,
}

impl Hello {
    /// The default request for this build.
    pub const fn local(queue_len: u32, direction: u64) -> Self {
        Self {
            abi_major: SURT_ABI_MAJOR,
            abi_minor: SURT_ABI_MINOR,
            features: feature::REQUIRED_V0_1,
            queue_len,
            direction,
        }
    }

    /// Encodes into MR0..MR4.
    pub const fn encode(&self) -> [u64; 5] {
        [
            SURT_MAGIC as u64,
            ((self.abi_major as u64) << 16) | self.abi_minor as u64,
            self.features,
            self.queue_len as u64,
            self.direction,
        ]
    }

    /// Decodes MR0..MR4. Rejects a wrong magic or a non-canonical word.
    pub const fn decode(mr: [u64; 5]) -> Result<Self, SurtStatus> {
        if mr[0] != SURT_MAGIC as u64 {
            return Err(SurtStatus::INVALID_ARGUMENT);
        }
        if mr[1] > 0xFFFF_FFFF || mr[3] > u32::MAX as u64 {
            return Err(SurtStatus::INVALID_ARGUMENT);
        }
        if mr[4] > direction::QUEUE_PAIR {
            return Err(SurtStatus::INVALID_ARGUMENT);
        }
        Ok(Self {
            abi_major: (mr[1] >> 16) as u16,
            abi_minor: (mr[1] & 0xFFFF) as u16,
            features: mr[2],
            queue_len: mr[3] as u32,
            direction: mr[4],
        })
    }
}

impl HelloAck {
    /// Encodes into MR0..MR4.
    pub const fn encode(&self) -> [u64; 5] {
        [
            self.status.raw() as i64 as u64,
            ((self.abi_major as u64) << 16) | self.abi_minor as u64,
            self.accepted_features,
            self.queue_len as u64,
            self.peer_id_base,
        ]
    }

    /// Decodes MR0..MR4.
    pub const fn decode(mr: [u64; 5]) -> Result<Self, SurtStatus> {
        let status = SurtStatus(mr[0] as i64 as i32);
        if mr[1] > 0xFFFF_FFFF || mr[3] > u32::MAX as u64 {
            return Err(SurtStatus::INVALID_ARGUMENT);
        }
        Ok(Self {
            status,
            abi_major: (mr[1] >> 16) as u16,
            abi_minor: (mr[1] & 0xFFFF) as u16,
            accepted_features: mr[2],
            queue_len: mr[3] as u32,
            peer_id_base: mr[4],
        })
    }
}

/// Acceptor-side decision for a received HELLO.
///
/// `max_queue_len` is the largest ring the acceptor can map. The acceptor may
/// shrink `queue_len` but not grow it.
pub fn accept(req: &Hello, supported: u64, max_queue_len: u32, peer_id_base: u64) -> HelloAck {
    let reject = |status| HelloAck {
        status,
        abi_major: SURT_ABI_MAJOR,
        abi_minor: SURT_ABI_MINOR,
        accepted_features: 0,
        queue_len: 0,
        peer_id_base: 0,
    };

    if req.abi_major != SURT_ABI_MAJOR {
        return reject(SurtStatus::UNSUPPORTED);
    }
    if req.queue_len == 0 {
        return reject(SurtStatus::INVALID_ARGUMENT);
    }
    if req.direction > direction::QUEUE_PAIR {
        return reject(SurtStatus::INVALID_ARGUMENT);
    }

    let accepted = req.features & supported;
    if accepted & feature::REQUIRED_V0_1 != feature::REQUIRED_V0_1 {
        return reject(SurtStatus::UNSUPPORTED);
    }

    HelloAck {
        status: SurtStatus::OK,
        abi_major: SURT_ABI_MAJOR,
        abi_minor: req.abi_minor.min(SURT_ABI_MINOR),
        accepted_features: accepted,
        queue_len: req.queue_len.min(max_queue_len),
        peer_id_base,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_hello() {
        let h = Hello::local(256, direction::QUEUE_PAIR);
        assert_eq!(Hello::decode(h.encode()), Ok(h));
    }

    #[test]
    fn round_trip_ack() {
        let a = HelloAck {
            status: SurtStatus::OK,
            abi_major: 0,
            abi_minor: 1,
            accepted_features: feature::REQUIRED_V0_1,
            queue_len: 128,
            peer_id_base: 7,
        };
        assert_eq!(HelloAck::decode(a.encode()), Ok(a));
    }

    #[test]
    fn bad_magic_rejected() {
        let mut mr = Hello::local(8, direction::ONE_WAY).encode();
        mr[0] ^= 1;
        assert_eq!(Hello::decode(mr), Err(SurtStatus::INVALID_ARGUMENT));
    }

    #[test]
    fn accept_shrinks_queue_and_masks_features() {
        let req = Hello::local(1024, direction::QUEUE_PAIR);
        let ack = accept(&req, feature::KNOWN, 256, 3);
        assert_eq!(ack.status, SurtStatus::OK);
        assert_eq!(ack.queue_len, 256);
        assert_eq!(ack.accepted_features, feature::REQUIRED_V0_1);
        assert_eq!(ack.peer_id_base, 3);
    }

    #[test]
    fn major_mismatch_is_unsupported() {
        let mut req = Hello::local(8, direction::ONE_WAY);
        req.abi_major = SURT_ABI_MAJOR + 1;
        assert_eq!(accept(&req, feature::KNOWN, 8, 0).status, SurtStatus::UNSUPPORTED);
    }

    #[test]
    fn missing_required_feature_is_rejected() {
        let req = Hello::local(8, direction::ONE_WAY);
        let ack = accept(&req, feature::BATCH, 8, 0);
        assert_eq!(ack.status, SurtStatus::UNSUPPORTED);
    }
}
