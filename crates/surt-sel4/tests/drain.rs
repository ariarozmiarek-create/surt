//! Host tests for the drain helpers, driving the real `surt-core` ring through a
//! fake `Sel4Env` whose `signal`/`wait` latch like a seL4 Notification (a signal
//! before a wait is remembered, so wakeups can't be lost).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use surt_sel4::surt_core::surt_abi::{feature, role, SurtSqe};
use surt_sel4::surt_core::{
    init_ring, ring_bytes, Consumer, NotifyingProducer, Producer, RingConfig,
};
use surt_sel4::{drain_adaptive, drain_polling, CPtr, DrainExit, Sel4Env, Sel4Notify};

/// A latching wait/signal backend, keyed on a single channel (the `ntfn` value
/// is ignored — one notification suffices for these tests). Mirrors a seL4
/// Notification: `signal` before `wait` is remembered.
struct TestEnv {
    signaled: Mutex<bool>,
    cv: Condvar,
    wakes: AtomicU64,
}

impl TestEnv {
    fn new() -> Self {
        Self {
            signaled: Mutex::new(false),
            cv: Condvar::new(),
            wakes: AtomicU64::new(0),
        }
    }
}

impl Sel4Env for TestEnv {
    fn signal(&self, _ntfn: CPtr) {
        self.wakes.fetch_add(1, Ordering::Relaxed);
        let mut g = self.signaled.lock().unwrap();
        *g = true;
        self.cv.notify_one();
    }
    fn wait(&self, _ntfn: CPtr) {
        let mut g = self.signaled.lock().unwrap();
        while !*g {
            g = self.cv.wait(g).unwrap();
        }
        *g = false;
    }
}

#[repr(C, align(64))]
struct Block([u8; 64]);

fn sqe(n: u64) -> SurtSqe {
    SurtSqe {
        request_id: n,
        user_data: !n,
        ..Default::default()
    }
}

/// Allocate + initialise a ring of `qlen` slots; returns the backing storage
/// (keep it alive) plus the (ptr, len) to attach over.
fn make_ring(qlen: u32) -> (Vec<Block>, *mut u8, usize) {
    let bytes = ring_bytes::<SurtSqe>(qlen);
    let nblocks = bytes.div_ceil(64);
    let mut storage: Vec<Block> = (0..nblocks).map(|_| Block([0; 64])).collect();
    let ptr = storage.as_mut_ptr() as *mut u8;
    let len = nblocks * 64;
    let cfg = RingConfig {
        queue_len: qlen,
        ring_id: 1,
        feature_flags: feature::REQUIRED_V0_1,
        role: role::PRODUCER,
    };
    // SAFETY: fresh 64-aligned region, large enough, init'd before any attach.
    unsafe { init_ring::<SurtSqe>(ptr, len, &cfg).unwrap() };
    (storage, ptr, len)
}

/// `drain_polling` drains a pre-filled ring with no blocking at all — the
/// handler stops before the ring empties, so it never spins on empty.
#[test]
fn polling_drains_prefilled() {
    const N: u64 = 500;
    let (_storage, ptr, len) = make_ring(1024);
    // SAFETY: single producer + consumer over the ring; storage outlives them.
    let mut p = unsafe { Producer::<SurtSqe>::attach(ptr, len).unwrap() };
    // SAFETY: the sole consumer over the same ring.
    let mut c = unsafe { Consumer::<SurtSqe>::attach(ptr, len).unwrap() };
    for i in 0..N {
        p.try_push(sqe(i)).unwrap();
    }

    let mut expect = 0u64;
    let exit = drain_polling(&mut c, |e: &SurtSqe| {
        assert_eq!(e.request_id, expect, "out of order / lost");
        assert_eq!(e.user_data, !expect, "torn payload");
        expect += 1;
        expect < N // stop once we've seen all N
    });
    assert_eq!(exit, DrainExit::Stopped);
    assert_eq!(expect, N);
}

/// `drain_adaptive` over a pre-filled ring never has to block (the handler stops
/// before empty), so no wakeup is issued.
#[test]
fn adaptive_drains_prefilled_without_blocking() {
    const N: u64 = 500;
    let (_storage, ptr, len) = make_ring(1024);
    // SAFETY: single producer + consumer over the ring.
    let mut p = unsafe { Producer::<SurtSqe>::attach(ptr, len).unwrap() };
    // SAFETY: the sole consumer over the same ring.
    let mut c = unsafe { Consumer::<SurtSqe>::attach(ptr, len).unwrap() };
    for i in 0..N {
        p.try_push(sqe(i)).unwrap();
    }

    let env = TestEnv::new();
    let notify = Sel4Notify::new(&env, 0);
    let mut expect = 0u64;
    let exit = drain_adaptive(&mut c, &notify, 8, |e: &SurtSqe| {
        assert_eq!(e.request_id, expect);
        assert_eq!(e.user_data, !expect);
        expect += 1;
        expect < N
    });
    assert_eq!(exit, DrainExit::Stopped);
    assert_eq!(expect, N);
    assert_eq!(
        env.wakes.load(Ordering::Relaxed),
        0,
        "should not have blocked"
    );
}

/// `drain_adaptive`'s block/wake fallback: a deliberately slow producer leaves
/// the ring empty past the spin budget so the consumer arms + blocks, and is
/// woken via the coalesced protocol. If a wakeup were lost this would hang.
#[test]
fn adaptive_blocks_then_wakes() {
    const N: u64 = 20_000;
    const QLEN: u32 = 16;
    let (_storage, ptr, len) = make_ring(QLEN);
    // SAFETY: single producer + single consumer over the ring; storage outlives
    // the scope below.
    let producer = unsafe { Producer::<SurtSqe>::attach(ptr, len).unwrap() };
    // SAFETY: the sole consumer over the same ring.
    let mut consumer = unsafe { Consumer::<SurtSqe>::attach(ptr, len).unwrap() };

    let env = TestEnv::new();

    std::thread::scope(|s| {
        s.spawn(|| {
            let notify = Sel4Notify::new(&env, 0);
            let mut np = NotifyingProducer::new(producer, &notify);
            for i in 0..N {
                while np.try_push(sqe(i)).is_err() {
                    std::hint::spin_loop();
                }
                if i % 64 == 0 {
                    std::thread::sleep(Duration::from_micros(50));
                }
            }
        });
        s.spawn(|| {
            let notify = Sel4Notify::new(&env, 0);
            let mut expect = 0u64;
            let exit = drain_adaptive(&mut consumer, &notify, 4, |e: &SurtSqe| {
                assert_eq!(e.request_id, expect, "out of order / lost");
                assert_eq!(e.user_data, !expect, "torn payload");
                expect += 1;
                expect < N
            });
            assert_eq!(exit, DrainExit::Stopped);
        });
    });

    let wakes = env.wakes.load(Ordering::Relaxed);
    assert!(wakes > 0, "block/wake path was never exercised");
    assert!(wakes <= N, "wakeups not coalesced: {wakes} > {N}");
}
