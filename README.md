# SURT — seL4 User-space Ring Transport

An **io_uring / virtqueue-inspired shared-memory ring transport** for fast,
asynchronous communication between isolated seL4 user-space components.

SURT needs no kernel support beyond ordinary seL4 mechanisms (endpoints,
notifications, capability transfer, shared frames) — so it runs on **upstream
seL4** or any seL4-compatible microkernel. It is domain-neutral: no
driver/filesystem/network/device semantics live in the transport; higher-level
protocols layer their own opcodes on top. The transport core is `#![no_std]`,
has **no external dependencies**, and does **no allocation on the hot path**.

## Design pillars

- **Policy out of the kernel** — all queue management is user-space code.
- **Shared memory for data, capabilities for authority** — fast-path submission
  and completion entries move through shared rings; frame/notification grants and
  teardown move through seL4 IPC + capability transfer.
- **Descriptors, not pointers** — cross-domain entries use ids and offsets, never
  raw pointers, references, or `usize`, so the two peers may map the ring at
  different virtual addresses.
- **No hot-path allocation** — setup may allocate; push/pop must not.
- **Untrusted-peer safe** — each peer validates the other's queue metadata,
  descriptor lengths, buffer ids, and generation counters; a misbehaving peer
  can cause a validation failure, never memory unsafety.
- **Small, audited `unsafe` surface** — confined to shared-memory projection and
  atomic ring manipulation; every `unsafe` block carries a `SAFETY:` comment
  (enforced by `clippy::undocumented_unsafe_blocks`).
- **Lost-wakeup-free coalescing** — a two-`SeqCst`-fence protocol
  (`Consumer::prepare_wait` ↔ `Producer::notify_consumer`) blocks only when
  genuinely idle and never misses a publish that races the sleep.

## Workspace layout

```
crates/
  surt-abi/    stable shared-memory ABI — SQE/CQE, ring header, ids   (no_std, no deps)
  surt-core/   SPSC ring + buffer registry + coalesced wakeups        (no_std, host-testable)
  surt-sel4/   seL4 binding — the Sel4Env platform trait + wait-loop   (no_std)
tools/
  surt-bench/  host benchmarks (throughput / round-trip latency / wakeup policy)
```

- **`surt-core` is platform-agnostic** — it projects the ring over caller-provided
  memory and knows nothing about seL4, so the exact code that runs between two
  seL4 components is exercised on the host by ordinary threads (`cargo test`).
- **`surt-sel4` is the thin glue** — it defines [`Sel4Env`], the two-syscall
  platform contract (`seL4_Signal` / `seL4_Wait`), a `Notify` adapter, and a
  ready-made coalesced `drain_blocking` consumer loop. You implement `Sel4Env`
  once for your platform.

## Using it on seL4

Implement the platform contract with your seL4 API, then drive the ring:

```rust
use surt_sel4::{Sel4Env, Sel4Notify, CPtr, drain_blocking};
use surt_sel4::surt_core::{Consumer, surt_abi::SurtSqe};

struct Kernel;
impl Sel4Env for Kernel {
    fn signal(&self, ntfn: CPtr) { /* seL4_Signal(ntfn)  — or sel4::Notification */ }
    fn wait(&self, ntfn: CPtr)   { /* seL4_Wait(ntfn, _) */ }
}

fn consumer(mut ring: Consumer<SurtSqe>, ntfn: CPtr) {
    let wake = Sel4Notify::new(&Kernel, ntfn);
    drain_blocking(&mut ring, &wake, |sqe| { /* handle */ let _ = sqe; true });
}
```

- On **upstream seL4**, implement `Sel4Env` over the [`sel4`](https://crates.io/crates/sel4)
  crate (`Notification::signal` / `Notification::wait`).
- The **control path** — retyping/sharing the ring frame, transferring the frame
  and notification caps, and the HELLO/HELLO_ACK handshake — is a handful of
  ordinary seL4 invocations the application issues once at setup. It is documented
  in [`docs/architecture/surt_sel4_binding.md`](docs/architecture/surt_sel4_binding.md),
  with a complete working reference (two fully-isolated components, own CSpaces,
  frame + notification caps transferred over IPC) in the
  [rust-micro](https://github.com/stakach/rust-micro) rootserver's
  `surt_ring_cap_transfer` microtest.

## Building / testing

```sh
cargo test  --workspace --all-features                 # unit + stress tests (host)
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build -p surt-abi -p surt-core -p surt-sel4 --target x86_64-unknown-none   # no_std
cargo run   -p surt-bench --release                    # host benchmarks
```

Sample host benchmark (Apple Silicon): batch=64 **~410 Mops/s**, round-trip p50
**~125 ns**, coalescing **~59×** fewer wakeups than naive signalling.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. "seL4" is a trademark of the seL4
Foundation; this project is an independent library and is not affiliated with or
endorsed by the Foundation.
