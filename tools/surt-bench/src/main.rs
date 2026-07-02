//! Host benchmarks for the SURT ring transport.
//!
//! All measurements run between two OS threads sharing one or two rings — the
//! same `surt-core` code paths that run between two seL4 components. The seL4
//! comparisons (vs plain IPC, vs a notification mailbox) and hardware counters
//! from spec §19 require the binding and are reported there; here we cover the
//! host-measurable parts: one-way throughput (batch / queue-size sweep),
//! request/completion round-trip latency (percentiles), and the
//! coalesced-vs-naive notification-policy comparison.
//!
//! Run with `cargo run -p surt-bench --release`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use surt_core::surt_abi::{feature, role, SurtCqe, SurtSqe};
use surt_core::{
    init_ring, ring_bytes, Consumer, Notify, NotifyError, Producer, PushError, RingConfig,
    RingEntry, WaitDecision,
};

// ---------------------------------------------------------------------------
// 64-byte-aligned backing memory for a ring.
// ---------------------------------------------------------------------------

#[repr(C, align(64))]
struct Block([u8; 64]);

struct Mem {
    blocks: Vec<Block>,
}

impl Mem {
    fn for_ring<E>(queue_len: u32) -> Self {
        let n = ring_bytes::<E>(queue_len).div_ceil(64);
        Self {
            blocks: (0..n).map(|_| Block([0; 64])).collect(),
        }
    }
    fn ptr(&mut self) -> *mut u8 {
        self.blocks.as_mut_ptr() as *mut u8
    }
    fn len(&self) -> usize {
        self.blocks.len() * 64
    }
}

fn make<E: RingEntry>(queue_len: u32) -> Mem {
    let mut mem = Mem::for_ring::<E>(queue_len);
    let cfg = RingConfig {
        queue_len,
        ring_id: 1,
        feature_flags: feature::REQUIRED_V0_1,
        role: role::PRODUCER,
    };
    // SAFETY: freshly-allocated, 64-aligned, large-enough region.
    unsafe { init_ring::<E>(mem.ptr(), mem.len(), &cfg).unwrap() };
    mem
}

// ---------------------------------------------------------------------------
// Formatting helpers.
// ---------------------------------------------------------------------------

fn group(n: u64) -> String {
    let s = n.to_string();
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in b.iter().enumerate() {
        if i > 0 && (b.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(*c as char);
    }
    out
}

fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// ---------------------------------------------------------------------------
// Latching condvar backend (same shape as the seL4 notification we'll bind).
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// 1. One-way throughput.
// ---------------------------------------------------------------------------

fn run_throughput(queue_len: u32, batch: usize, n: u64) -> (f64, f64) {
    let mut mem = make::<SurtSqe>(queue_len);
    let (ptr, len) = (mem.ptr(), mem.len());
    // SAFETY: one producer + one consumer over the ring; `mem` outlives scope.
    let mut prod = unsafe { Producer::<SurtSqe>::attach(ptr, len).unwrap() };
    // SAFETY: the sole consumer over the same ring.
    let mut cons = unsafe { Consumer::<SurtSqe>::attach(ptr, len).unwrap() };
    let src = vec![SurtSqe::default(); batch];

    let t0 = Instant::now();
    std::thread::scope(|s| {
        s.spawn(move || {
            let mut sent = 0u64;
            while sent < n {
                let want = batch.min((n - sent) as usize);
                let mut off = 0;
                while off < want {
                    match prod.push_batch(&src[off..want]) {
                        Ok(0) | Err(PushError::Full) => std::hint::spin_loop(),
                        Ok(k) => off += k,
                        Err(PushError::Closed) => return,
                    }
                }
                sent += want as u64;
            }
        });
        s.spawn(move || {
            let mut out = vec![SurtSqe::default(); batch];
            let mut recv = 0u64;
            while recv < n {
                match cons.pop_batch(&mut out) {
                    Ok(0) => std::hint::spin_loop(),
                    Ok(k) => recv += k as u64,
                    Err(_) => break,
                }
            }
        });
    });
    let secs = t0.elapsed().as_secs_f64();
    ((n as f64 / secs) / 1e6, secs * 1e9 / n as f64)
}

fn bench_throughput() {
    const N: u64 = 2_000_000;
    println!("\n== One-way throughput ({} entries/run) ==", group(N));
    println!(
        "  {:>9}  {:>5}  {:>9}  {:>8}",
        "queue_len", "batch", "Mops/s", "ns/op"
    );
    for &qlen in &[256u32, 1024, 4096] {
        for &batch in &[1usize, 8, 64, 256] {
            let (mops, nsop) = run_throughput(qlen, batch, N);
            println!("  {qlen:>9}  {batch:>5}  {mops:>9.1}  {nsop:>8.2}");
        }
    }
}

// ---------------------------------------------------------------------------
// 2. Request/completion round-trip latency (strict ping-pong).
// ---------------------------------------------------------------------------

fn bench_roundtrip() {
    const ROUNDS: usize = 100_000;
    let qlen = 256u32;

    let mut sub = make::<SurtSqe>(qlen);
    let mut com = make::<SurtCqe>(qlen);
    let (sp, sl) = (sub.ptr(), sub.len());
    let (cp, cl) = (com.ptr(), com.len());
    // SAFETY: each ring has exactly one producer and one consumer across the
    // client/server split; both buffers outlive the scope.
    let mut client_sq = unsafe { Producer::<SurtSqe>::attach(sp, sl).unwrap() };
    // SAFETY: see above.
    let mut client_cq = unsafe { Consumer::<SurtCqe>::attach(cp, cl).unwrap() };
    // SAFETY: see above.
    let mut server_sq = unsafe { Consumer::<SurtSqe>::attach(sp, sl).unwrap() };
    // SAFETY: see above.
    let mut server_cq = unsafe { Producer::<SurtCqe>::attach(cp, cl).unwrap() };

    let mut lat = Vec::with_capacity(ROUNDS);
    std::thread::scope(|s| {
        s.spawn(move || {
            let mut done = 0;
            while done < ROUNDS {
                match server_sq.try_pop() {
                    Ok(Some(sqe)) => {
                        let cqe = SurtCqe {
                            request_id: sqe.request_id,
                            ..Default::default()
                        };
                        while server_cq.try_push(cqe).is_err() {
                            std::hint::spin_loop();
                        }
                        done += 1;
                    }
                    Ok(None) => std::hint::spin_loop(),
                    Err(_) => break,
                }
            }
        });
        // Client runs on this thread so it can fill `lat`.
        for r in 0..ROUNDS {
            let t0 = Instant::now();
            let sqe = SurtSqe {
                request_id: r as u64,
                ..Default::default()
            };
            while client_sq.try_push(sqe).is_err() {
                std::hint::spin_loop();
            }
            loop {
                match client_cq.try_pop() {
                    Ok(Some(_)) => break,
                    Ok(None) => std::hint::spin_loop(),
                    Err(_) => break,
                }
            }
            lat.push(t0.elapsed().as_nanos() as u64);
        }
    });
    lat.sort_unstable();
    println!(
        "\n== Round-trip latency ({} ping-pongs, queue_len {}) ==",
        group(ROUNDS as u64),
        qlen
    );
    println!(
        "  p50 {} ns   p95 {} ns   p99 {} ns   p99.9 {} ns   max {} ns",
        pct(&lat, 50.0),
        pct(&lat, 95.0),
        pct(&lat, 99.0),
        pct(&lat, 99.9),
        lat.last().copied().unwrap_or(0),
    );
}

// ---------------------------------------------------------------------------
// 3. Notification policy: coalesced vs wake-every-item, under bursty load.
// ---------------------------------------------------------------------------

fn run_notify(coalesced: bool, n: u64, burst: u64) -> (u64, f64) {
    let qlen = 64u32;
    let mut mem = make::<SurtSqe>(qlen);
    let (ptr, len) = (mem.ptr(), mem.len());
    // SAFETY: single producer + single consumer over the ring for the scope.
    let producer = unsafe { Producer::<SurtSqe>::attach(ptr, len).unwrap() };
    // SAFETY: the sole consumer over the same ring.
    let mut consumer = unsafe { Consumer::<SurtSqe>::attach(ptr, len).unwrap() };
    let cond = CondNotify::new();

    let t0 = Instant::now();
    std::thread::scope(|s| {
        s.spawn(|| {
            let mut p = producer;
            for i in 0..n {
                let e = SurtSqe {
                    request_id: i,
                    ..Default::default()
                };
                while p.try_push(e).is_err() {
                    std::hint::spin_loop();
                }
                if coalesced {
                    let _ = p.notify_consumer(&cond);
                } else {
                    let _ = cond.wake(); // naive: wake on every publish
                }
                if i % burst == 0 {
                    std::thread::sleep(Duration::from_micros(20));
                }
            }
        });
        s.spawn(|| {
            let mut recv = 0u64;
            while recv < n {
                match consumer.try_pop() {
                    Ok(Some(_)) => recv += 1,
                    Ok(None) => match consumer.prepare_wait() {
                        Ok(WaitDecision::Block) => cond.wait(),
                        Ok(WaitDecision::Ready) => {}
                        Err(_) => break,
                    },
                    Err(_) => break,
                }
            }
        });
    });
    (
        cond.wakes.load(Ordering::Relaxed),
        t0.elapsed().as_secs_f64(),
    )
}

fn bench_notify_policy() {
    const N: u64 = 500_000;
    const BURST: u64 = 256;
    println!(
        "\n== Notification policy (bursty load, {} entries, burst {}) ==",
        group(N),
        BURST
    );
    println!(
        "  {:>10}  {:>12}  {:>10}  {:>8}",
        "policy", "wakeups", "wakes/op", "Mops/s"
    );
    let (cw, ct) = run_notify(true, N, BURST);
    let (nw, nt) = run_notify(false, N, BURST);
    println!(
        "  {:>10}  {:>12}  {:>10.4}  {:>8.1}",
        "coalesced",
        group(cw),
        cw as f64 / N as f64,
        (N as f64 / ct) / 1e6
    );
    println!(
        "  {:>10}  {:>12}  {:>10.4}  {:>8.1}",
        "naive",
        group(nw),
        nw as f64 / N as f64,
        (N as f64 / nt) / 1e6
    );
    println!(
        "  -> coalescing issued {:.0}x fewer wakeups",
        nw as f64 / cw.max(1) as f64
    );
}

fn main() {
    println!("SURT host benchmarks  (two threads, shared rings)");
    bench_throughput();
    bench_roundtrip();
    bench_notify_policy();
    println!();
}
