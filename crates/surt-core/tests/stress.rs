//! Multi-threaded SPSC stress: two host threads sharing one ring, verifying
//! no loss, no duplication, and strict FIFO order under real contention. This
//! is what exercises the release/acquire ordering — a torn entry or a missed
//! publish shows up as an out-of-order or wrong-payload assertion.

use surt_core::surt_abi::{feature, role, SurtSqe};
use surt_core::{init_ring, ring_bytes, Consumer, Producer, PushError, RingConfig};

/// 64-byte-aligned heap storage for a ring (so the header's `align(64)` and the
/// producer/consumer cache-line separation are honoured).
#[repr(C, align(64))]
struct Block([u8; 64]);

struct Aligned {
    blocks: Vec<Block>,
}

impl Aligned {
    fn new(bytes: usize) -> Self {
        let nblocks = bytes.div_ceil(64);
        let mut blocks = Vec::with_capacity(nblocks);
        for _ in 0..nblocks {
            blocks.push(Block([0; 64]));
        }
        Self { blocks }
    }
    fn ptr(&mut self) -> *mut u8 {
        self.blocks.as_mut_ptr() as *mut u8
    }
    fn len(&self) -> usize {
        self.blocks.len() * 64
    }
}

fn sqe(n: u64) -> SurtSqe {
    SurtSqe {
        request_id: n,
        user_data: n.wrapping_mul(0x9e37_79b9_7f4a_7c15),
        object_id: !n,
        ..Default::default()
    }
}

fn check(e: &SurtSqe, expect: u64) {
    assert_eq!(e.request_id, expect, "out of order / lost entry");
    assert_eq!(
        e.user_data,
        expect.wrapping_mul(0x9e37_79b9_7f4a_7c15),
        "torn payload (user_data) at {expect}"
    );
    assert_eq!(e.object_id, !expect, "torn payload (object_id) at {expect}");
}

/// Tiny deterministic PRNG (SplitMix64) so batch sizes vary without a dep.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn in_range(&mut self, lo: usize, hi: usize) -> usize {
        lo + (self.next() as usize) % (hi - lo + 1)
    }
}

fn make_ring(queue_len: u32) -> Aligned {
    let bytes = ring_bytes::<SurtSqe>(queue_len);
    let mut mem = Aligned::new(bytes);
    let cfg = RingConfig {
        queue_len,
        ring_id: 1,
        feature_flags: feature::REQUIRED_V0_1,
        role: role::PRODUCER,
    };
    // SAFETY: freshly-allocated, 64-aligned, large-enough region; nothing else
    // touches it during initialisation.
    unsafe { init_ring::<SurtSqe>(mem.ptr(), mem.len(), &cfg).unwrap() };
    mem
}

#[test]
fn spsc_single_no_loss_in_order() {
    const N: u64 = 2_000_000;
    const QLEN: u32 = 1024;

    let mut mem = make_ring(QLEN);
    let (ptr, len) = (mem.ptr(), mem.len());
    // SAFETY: the sole producer over the initialised ring; `mem` outlives the
    // thread scope below, so the pointer stays valid.
    let mut producer = unsafe { Producer::<SurtSqe>::attach(ptr, len).unwrap() };
    // SAFETY: the sole consumer over the same ring, valid for the scope.
    let mut consumer = unsafe { Consumer::<SurtSqe>::attach(ptr, len).unwrap() };

    std::thread::scope(|s| {
        s.spawn(move || {
            for i in 0..N {
                let e = sqe(i);
                loop {
                    match producer.try_push(e) {
                        Ok(()) => break,
                        Err(PushError::Full) => std::hint::spin_loop(),
                        Err(PushError::Closed) => panic!("unexpected close"),
                    }
                }
            }
        });
        s.spawn(move || {
            let mut expect = 0u64;
            while expect < N {
                match consumer.try_pop().expect("peer faulted") {
                    Some(e) => {
                        check(&e, expect);
                        expect += 1;
                    }
                    None => std::hint::spin_loop(),
                }
            }
            assert_eq!(consumer.try_pop(), Ok(None), "extra entries after N");
        });
    });
}

#[test]
fn spsc_batch_random_no_loss_in_order() {
    const N: u64 = 1_000_000;
    const QLEN: u32 = 256;

    let mut mem = make_ring(QLEN);
    let (ptr, len) = (mem.ptr(), mem.len());
    // SAFETY: the sole producer over a longer-lived ring (as above).
    let mut producer = unsafe { Producer::<SurtSqe>::attach(ptr, len).unwrap() };
    // SAFETY: the sole consumer over the same ring, valid for the scope.
    let mut consumer = unsafe { Consumer::<SurtSqe>::attach(ptr, len).unwrap() };

    std::thread::scope(|s| {
        s.spawn(move || {
            let mut rng = Rng(0xdead_beef);
            let mut next = 0u64;
            let mut buf = [SurtSqe::default(); 64];
            while next < N {
                let want = rng.in_range(1, 64).min((N - next) as usize);
                for (k, slot) in buf[..want].iter_mut().enumerate() {
                    *slot = sqe(next + k as u64);
                }
                let mut off = 0;
                while off < want {
                    match producer.push_batch(&buf[off..want]) {
                        Ok(0) => std::hint::spin_loop(),
                        Ok(pushed) => off += pushed,
                        Err(PushError::Closed) => panic!("unexpected close"),
                        Err(PushError::Full) => std::hint::spin_loop(),
                    }
                }
                next += want as u64;
                if rng.next() & 0x3f == 0 {
                    std::thread::yield_now();
                }
            }
        });
        s.spawn(move || {
            let mut rng = Rng(0x1234_5678);
            let mut expect = 0u64;
            let mut out = [SurtSqe::default(); 48];
            while expect < N {
                let cap = rng.in_range(1, 48);
                match consumer.pop_batch(&mut out[..cap]).expect("peer faulted") {
                    0 => std::hint::spin_loop(),
                    n => {
                        for e in &out[..n] {
                            check(e, expect);
                            expect += 1;
                        }
                    }
                }
                if rng.next() & 0x1f == 0 {
                    std::thread::yield_now();
                }
            }
        });
    });
}
