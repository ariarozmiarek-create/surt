//! No-lost-wakeup stress: a deliberately slow producer leaves the ring empty so
//! the consumer blocks on a real condvar backend, woken via the coalesced
//! protocol. If a wakeup were ever lost the consumer would block forever and
//! this test would hang (and the harness would time it out) — completion is the
//! proof. We also assert FIFO/payload integrity and that wakeups were both
//! exercised (`> 0`) and coalesced (`<= N`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use surt_core::surt_abi::{feature, role, SurtSqe};
use surt_core::{
    init_ring, ring_bytes, Consumer, Notify, NotifyError, NotifyingProducer, Producer, WaitDecision,
};

/// A latching condvar backend: a `wake` before a `wait` is remembered, so the
/// wait cannot miss it.
struct CondNotify {
    signaled: Mutex<bool>,
    cv: Condvar,
    wakes: AtomicU64,
}

impl CondNotify {
    fn new() -> Self {
        Self {
            signaled: Mutex::new(false),
            cv: Condvar::new(),
            wakes: AtomicU64::new(0),
        }
    }
    fn wait(&self) {
        let mut g = self.signaled.lock().unwrap();
        while !*g {
            g = self.cv.wait(g).unwrap();
        }
        *g = false;
    }
}

impl Notify for CondNotify {
    fn wake(&self) -> Result<(), NotifyError> {
        self.wakes.fetch_add(1, Ordering::Relaxed);
        let mut g = self.signaled.lock().unwrap();
        *g = true;
        self.cv.notify_one();
        Ok(())
    }
    fn arm(&self) -> Result<(), NotifyError> {
        Ok(())
    }
    fn disarm(&self) -> Result<(), NotifyError> {
        Ok(())
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

#[test]
fn no_lost_wakeups_under_blocking() {
    const N: u64 = 20_000;
    const QLEN: u32 = 16;

    let bytes = ring_bytes::<SurtSqe>(QLEN);
    let nblocks = bytes.div_ceil(64);
    let mut storage: Vec<Block> = (0..nblocks).map(|_| Block([0; 64])).collect();
    let ptr = storage.as_mut_ptr() as *mut u8;
    let len = nblocks * 64;

    let cfg = surt_core::RingConfig {
        queue_len: QLEN,
        ring_id: 1,
        feature_flags: feature::REQUIRED_V0_1,
        role: role::PRODUCER,
    };
    // SAFETY: fresh 64-aligned region, large enough, init'd before any attach.
    unsafe { init_ring::<SurtSqe>(ptr, len, &cfg).unwrap() };
    // SAFETY: single producer + single consumer over the ring; `storage`
    // outlives the thread scope below.
    let producer = unsafe { Producer::<SurtSqe>::attach(ptr, len).unwrap() };
    // SAFETY: the sole consumer over the same ring.
    let mut consumer = unsafe { Consumer::<SurtSqe>::attach(ptr, len).unwrap() };

    let cond = CondNotify::new();

    std::thread::scope(|s| {
        // Producer: push N, pausing periodically so the consumer drains the
        // ring and actually blocks (exercising the wakeup path).
        s.spawn(|| {
            let mut np = NotifyingProducer::new(producer, &cond);
            for i in 0..N {
                while np.try_push(sqe(i)).is_err() {
                    std::hint::spin_loop();
                }
                if i % 64 == 0 {
                    std::thread::sleep(Duration::from_micros(50));
                }
            }
        });
        // Consumer: pop N, blocking via the coalesced protocol when empty.
        s.spawn(|| {
            let mut expect = 0u64;
            while expect < N {
                match consumer.try_pop().expect("peer faulted") {
                    Some(e) => {
                        assert_eq!(e.request_id, expect, "out of order / lost");
                        assert_eq!(e.user_data, !expect, "torn payload");
                        expect += 1;
                    }
                    None => match consumer.prepare_wait().expect("peer faulted") {
                        WaitDecision::Block => cond.wait(),
                        WaitDecision::Ready => {}
                    },
                }
            }
        });
    });

    let wakes = cond.wakes.load(Ordering::Relaxed);
    assert!(wakes > 0, "wakeup path was never exercised");
    assert!(wakes <= N, "wakeups not coalesced: {wakes} > {N}");
}
