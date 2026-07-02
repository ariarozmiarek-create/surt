# `surt-sel4` binding — design

Status: design (Milestone 6). Implements: spec §10 (control path), §14 (seL4
binding). Target: the rust-micro kernel's seL4 API (the same ABI sel4test runs
against — `seL4_Call`/`Recv`/`ReplyRecv`/`NBSend`, `seL4_Signal`/`Wait`/`Poll`,
CNode ops, `Untyped_Retype`, page mapping).

This document scopes `crates/surt-sel4` before implementation (M7). It does not
introduce any new ABI; the wire format and all queue logic stay in `surt-abi` /
`surt-core`. `surt-sel4` is glue: it turns seL4 frames into a `RingMem`, backs
`Notify` with a seL4 notification, and runs the control-path handshake that
sets a connection up and tears it down.

> **Implementation status (M7, partial).** On the rust-micro kernel the seL4 ABI
> is hand-rolled inline in the rootserver (no shareable bindings crate), so the
> first integration lives in `rootserver/src/microtest.rs` rather than a separate
> `surt-sel4` crate. Done: `surt-core` builds under `-Z build-std=core`; a
> **single-thread ring microtest passes in QEMU** (the transport runs unchanged
> on the kernel); `Sel4Notify` is implemented (`wake`=Signal, `wait`=Recv). The
> integration also fixed three real kernel-side bugs (an `ep_recv` register-
> clobber miscompile, a spawned-child stack-alignment requirement, and the
> rootserver linker script lacking large-code-model section rules). The
> two-thread cross-thread test is written but disabled pending an unresolved
> rootserver child-spawn issue (a child spawned after the parent's surt-core
> setup is never scheduled — even a minimal child — while the existing
> `child_send` test works; independent of surt-core's correctness).

---

## 1. Responsibilities (spec §14.1 / §14.2)

`surt-sel4` **provides**:

- projecting one or more **shared frames** into a `surt-core` ring
  (`Producer`/`Consumer`/`QueuePair`);
- a **`Notify`** implementation backed by a seL4 Notification capability;
- the **control-endpoint handshake** (HELLO / HELLO_ACK + feature negotiation);
- **capability-transfer** helpers (frames + notifications over IPC);
- **peer lifecycle** tracking (READY → DRAINING → CLOSED) and **fault**
  integration;
- a non-prescriptive **wait-loop** helper that drives the M4 coalesced-wakeup
  protocol against `seL4_Wait`.

`surt-sel4` **must not** provide: protocol opcodes, device/filesystem/network
semantics, a driver framework, a DMA/IOMMU manager, a scheduler, or an async
runtime (§14.2, §14.5). Those belong to layers above.

---

## 2. What a SURT component holds

A component that participates in a connection is configured (at construction or
via its bootinfo) with capabilities in its CSpace:

```
control endpoint cap     seL4 Endpoint — setup/teardown RPC with the peer
wake notification cap     seL4 Notification — WE signal it to wake the PEER
wait notification cap     seL4 Notification — the PEER signals it to wake US
frame caps               seL4 Frame(s) backing the ring(s) (post-handshake)
vspace / vspace-root     to map the frames
untyped (authority only) to retype into frames + notifications
cnode + receive slots    to receive transferred caps
optional fault endpoint  where the kernel delivers this component's faults
optional bootstrap cfg   queue sizes, directionality, peer identity
```

The **memory authority** (often the initiator, but either side may serve, §10.2)
additionally holds untyped to mint the frames and notifications; the peer only
needs empty receive slots to accept them.

---

## 3. Crate structure

```
crates/surt-sel4/            #![no_std]
  src/
    lib.rs        re-exports; Connection / ConnectionConfig
    sys.rs        thin seL4 syscall wrappers used here (Call/Recv/Signal/Wait,
                  CNode_Copy/Mint, *Page_Map, SetCapReceivePath)
    frames.rs     SharedFrames: map frame caps → vaddr; project to Ring*
    notify.rs     Sel4Notify (Notify impl) + the blocking wait()
    control.rs    HELLO/HELLO_ACK message codec; setup state machine
    captx.rs      cap-transfer helpers (send/recv frame + ntfn caps)
    lifecycle.rs  state transitions, drain, teardown, fault handling
    waitloop.rs   server/run helper integrating prepare_wait ↔ Wait
```

`surt-sel4` depends on `surt-core` (and transitively `surt-abi`). It links the
kernel's libsel4-equivalent bindings for the raw invocations (`sys.rs` keeps that
surface tiny and in one place).

---

## 4. Shared frames → `RingMem`

A connection's rings live in physical frames mapped into **both** peers' VSpaces
(at possibly different virtual addresses — descriptors use ids/offsets, never
pointers, so that's fine; spec §3.3).

```
authority                                   peer
  Untyped_Retype  -> Frame caps  --IPC-->     (receive Frame caps)
  *Page_Map(frame, vaddr_A)                   *Page_Map(frame, vaddr_B)
  RingMem @ vaddr_A  <==== same physical frames ====>  RingMem @ vaddr_B
```

`SharedFrames` records the local `vaddr`, byte length, and the frame caps (kept
for unmap/teardown). Alignment: the first frame must be 64-byte aligned for
`SurtRingHeader` — seL4 frames are page-aligned (≥4 KiB), so this is automatic.
A ring larger than one frame uses physically-contiguous or
sequentially-mapped frames so the `[header | entries]` layout is linear in each
peer's address space.

```rust
pub struct SharedFrames { vaddr: usize, len: usize, frames: FrameCapList }

impl SharedFrames {
    /// # Safety: this peer is the sole producer (resp. consumer) of the ring,
    /// the frames stay mapped for the returned view's life, and the peer agreed
    /// on entry type `E`.
    pub unsafe fn producer<E: RingEntry>(&self) -> Result<Producer<E>, RingError>;
    pub unsafe fn consumer<E: RingEntry>(&self) -> Result<Consumer<E>, RingError>;
}
```

`init_ring` is called exactly once, by the authority, before either side
attaches (the header's `state` starts `UNINIT`; `attach` rejects that, so a peer
that races ahead fails cleanly and retries after HELLO_ACK).

---

## 5. `Sel4Notify` — the notification backend

Each wakeup direction is one seL4 Notification object. `wake` = `seL4_Signal`;
the **blocking wait is *not* part of `Notify`** (spec §9.4) — it is a separate
method the wait loop calls.

```rust
pub struct Sel4Notify { ntfn: CPtr }      // a Notification cap

impl Notify for Sel4Notify {
    fn wake(&self) -> Result<(), NotifyError> { sys::signal(self.ntfn); Ok(()) }
    fn arm(&self)  -> Result<(), NotifyError> { Ok(()) }   // always armed
    fn disarm(&self)-> Result<(), NotifyError> { Ok(()) }
}
impl Sel4Notify {
    pub fn wait(&self) { sys::wait(self.ntfn); }           // blocks; NOT in the trait
    pub fn poll(&self) -> bool { sys::poll(self.ntfn) }     // non-blocking drain
}
```

**Why this is correct with M4.** The coalescing protocol requires `wake` to
*latch* against a later wait. seL4 notifications are exactly that: `Signal`
before `Wait` sets the pending bit, so the subsequent `Wait` returns
immediately. Combined with `Consumer::prepare_wait`'s `SeqCst`-fenced recheck
and `Producer::notify_consumer`'s fenced flag-check, a producer's signal is
never lost even if it races the consumer's decision to sleep. `arm`/`disarm`
are no-ops because the notification is always able to receive a signal.

---

## 6. Control-path handshake (spec §10.2)

The control path is a seL4 **Endpoint** used for the operations that need
authority and ordering: version/feature negotiation, frame + notification cap
transfer, and the READY/SHUTDOWN transitions. Fast-path SQE/CQE traffic never
touches it.

### 6.1 Messages

Each message is `seL4_Call`/`ReplyRecv` with a small message-register payload
and (where noted) transferred caps. Message tag's label selects the type.

```
HELLO        (initiator -> acceptor)
  MR0 = SURT_MAGIC
  MR1 = (abi_major<<16) | abi_minor
  MR2 = requested_features
  MR3 = requested queue_len (entries) and entry kind (sqe/cqe sizes implied)
  MR4 = directionality (one-way | queue-pair)

HELLO_ACK    (acceptor -> initiator, as the Reply)
  MR0 = status (0 = OK, else SurtStatus)
  MR1 = accepted abi
  MR2 = accepted_features            (subset of requested ∩ supported)
  MR3 = actual queue_len             (acceptor may shrink)
  MR4 = assigned peer_id / ring_id base

MAP_FRAMES   (authority -> peer)     caps: [submit frame(s), completion frame(s)]
  MR0 = frame count + per-ring layout

NTFN_EXCHANGE (each -> each)         caps: [the notification the SENDER will
                                            signal to wake the RECEIVER]
READY        (each -> each)          rings move to READY; traffic may begin
SHUTDOWN / CLOSED                    see §8
```

Negotiation (§6.3 of the spec): a major-version mismatch in HELLO ⇒ `HELLO_ACK`
status `UNSUPPORTED`. `accepted_features = requested & supported`; if that drops
a **required** bit (`feature::REQUIRED_V0_1`), the acceptor rejects. Unknown
optional bits are ignored.

### 6.2 Sequence

```
initiator                         acceptor (memory authority here)
   |        Call(HELLO) ------------>|  validate magic/abi/features
   |<------ Reply(HELLO_ACK) --------|  pick queue_len, peer_id
   |                                 |  Untyped_Retype -> frames + notifications
   |                                 |  init_ring() on each ring (authority only)
   |<------ Call(MAP_FRAMES,caps)----|  transfer frame caps
   |  map frames; SetCapReceivePath  |
   |        Reply() --------------->  |
   |<====== NTFN_EXCHANGE (caps) ====>|  swap the two notification caps
   |        Call(READY) ------------>|
   |<------ Reply(READY) ------------|
   |   both attach Producer/Consumer; rings READY; fast path begins
```

Either side may be the authority (§10.2); the roles above just fix one concrete
ordering. The initiator designates a **receive slot** with
`seL4_SetCapReceivePath` before any message that carries caps, so transferred
frame/notification caps land in known CSpace slots.

---

## 7. Capability transfer (spec §3.2)

Authority moves through caps, data through shared memory. seL4 copies caps as
part of IPC: the sender stages caps in its IPC buffer (`caps[]` + the message
info's `extraCaps`), the receiver names a destination via
`seL4_SetCapReceivePath(cnode, slot, depth)` and finds the copied cap there
after the receive.

- **Frame caps** are `CNode_Copy`'d (not moved) to the peer so the authority
  retains its mapping; the peer maps its copy with `*Page_Map`.
- **Notification caps**: each side `CNode_Mint`s a **badged** send-right to *its*
  notification and hands that to the peer; the peer signals the badged cap, the
  owner waits on the unbadged original. Badging lets one notification multiplex
  several rings if `SHARED_COMPLETION` is negotiated.
- All transferred caps are validated on receipt (cap type, rights) before use;
  a wrong cap type aborts setup with `INVALID_DESCRIPTOR` rather than faulting.

`captx.rs` wraps the stage/receive dance so `control.rs` stays readable.

---

## 8. Teardown and faults (spec §10.3, §15.3, §16.4)

### 8.1 Graceful teardown

```
SHUTDOWN over the control endpoint
  -> producer stops accepting new submissions (state = DRAINING)
  -> in-flight requests are completed or cancelled
  -> buffers unregistered (BufferRegistry::unregister bumps generations)
  -> notification caps revoked/deleted
  -> frames unmapped (*Page_Unmap) and frame caps deleted
  -> rings move to CLOSED
```

The ring header `state` (`READY`→`DRAINING`→`CLOSED`) is the cross-peer signal;
`Producer::try_push` already returns `PushError::Closed` once `state != READY`,
and `Consumer::try_pop` surfaces `FAULTED`.

### 8.2 Faults

A SURT component registers a **fault endpoint** with the kernel (`TCB_Configure`
fault_ep). If a peer faults (e.g. a bad access), the kernel delivers the fault
there instead of to the faulting thread. The connection's supervisor then:

- marks the ring `state = FAULTED` (so the surviving peer's next `try_pop`
  returns `PopError::PeerFaulted` and its `try_push` returns `Closed`);
- enumerates in-flight `request_id`s for the protocol layer to fail/recover
  (`SurtStatus::PEER_CLOSED` / `FAULTED`);
- runs the teardown above, skipping the graceful drain.

Because every peer treats shared memory as hostile (spec §16.1), a faulted or
malicious peer cannot escalate past `PeerFaulted`: geometry is cached locally at
attach, cursor movement is bounded, and descriptors are validated before use.
A peer can never access frames it was not granted — that's enforced by seL4
capabilities, not by SURT.

---

## 9. The wait loop (spec §14.4)

The binding offers a non-prescriptive server loop that ties the M4 protocol to
`seL4_Wait`. No executor, no async (§14.5).

```rust
impl Connection {
    /// Drain submissions, hand each to `handler`, publish completions, and
    /// block on the seL4 notification only when there is genuinely no work —
    /// using prepare_wait() so a publish racing the sleep is never lost.
    pub fn serve<H: FnMut(&SurtSqe) -> SurtCqe>(&mut self, mut handler: H) {
        loop {
            // 1. drain a batch of submissions, produce completions
            let mut got = false;
            while let Ok(Some(sqe)) = self.sq.try_pop() {
                got = true;
                if validate_sqe(&sqe, &self.registry).is_err() { /* complete err */ }
                let cqe = handler(&sqe);
                while self.cq.try_push(cqe).is_err() { /* full: wake peer, spin */ }
            }
            // 2. wake the client if it armed (coalesced)
            if got { let _ = self.cq_producer.notify_consumer(&self.peer_wake); }
            // 3. block iff truly idle
            match self.sq.prepare_wait() {
                Ok(WaitDecision::Block) => self.wait_ntfn.wait(),
                Ok(WaitDecision::Ready) => {}             // work appeared; loop
                Err(_) => break,                          // peer faulted
            }
        }
    }
}
```

The client side is symmetric: submit, then `prepare_wait` + `wait_ntfn.wait()`
on the completion ring while outstanding. Microkit integration (optional, §14.1)
maps this loop onto a protection-domain `notified()` entrypoint instead of an
explicit `Wait`.

---

## 10. Memory model across address spaces

- Control words (`head`/`tail`/`*_flags`/`state`) are `AtomicU32`/`AtomicU64` in
  the shared frame; both peers access them with the orderings `surt-core`
  already uses (Release publish / Acquire observe; `SeqCst` fences for the
  wakeup handshake). seL4 maps the frames cacheable and coherent on the target,
  so atomics work cross-AS exactly as cross-thread on the host.
- Entry payloads are ordered by the `tail` Release/Acquire; `surt-core` reads
  them via `read_volatile` snapshots, so a hostile peer mutating a slot after
  publish cannot cause UB — only a validation failure.
- No cache-maintenance ops are needed on x86_64 (coherent DMA-free shared RAM).
  An architecture requiring explicit cache flushes for shared memory would add a
  barrier in `frames.rs`; that is out of scope for the x86_64 target.

---

## 11. Public API sketch (for M7)

```rust
pub struct ConnectionConfig {
    pub control_ep: CPtr,
    pub vspace: CPtr,
    pub recv_cnode: CPtr, pub recv_depth: u8,   // where transferred caps land
    pub untyped: Option<CPtr>,                   // Some(_) ⇒ we are the authority
    pub queue_len: u32,
    pub features: u64,                           // requested
    pub role: ConnRole,                          // Initiator | Acceptor
}

pub struct Connection { /* QueuePair, notifications, frames, registry, state */ }

pub fn establish(cfg: &ConnectionConfig) -> Result<Connection, SetupError>;

impl Connection {
    pub fn submit(&mut self, sqe: SurtSqe) -> Result<(), PushError>;   // + wake
    pub fn poll_completion(&mut self) -> Result<Option<SurtCqe>, PopError>;
    pub fn serve<H: FnMut(&SurtSqe) -> SurtCqe>(&mut self, handler: H);
    pub fn shutdown(&mut self) -> Result<(), SetupError>;
    pub fn register_buffer(&mut self, /* frame(s), rights */) -> Result<BufferId, BufferError>;
}

#[derive(Debug)] pub enum SetupError {
    Unsupported, FeatureRejected, BadCap, MapFailed, PeerRefused, Timeout,
}
```

---

## 12. Open decisions (carried from spec §22)

1. 32- vs 64-bit head/tail counters — start 32-bit (`surt-core` today); revisit
   under `SIXTYFOUR_BIT_COUNTERS` if wrap-window analysis demands it.
2. Queue-pair memory: one frame holding both rings vs two frames — start two
   frames (independent unmap, simpler `SHARED_COMPLETION` later).
3. Multi-shot completions / shared completion rings — deferred past v0.1.
4. `RESET` — deferred past v0.1 (teardown + re-establish instead).
5. Microkit notification arming pattern — evaluate once a PD entrypoint exists.
6. Buffer registration cap model — frames registered with `register_buffer`
   carry their own frame cap so the peer maps on demand; rights mirror the cap's
   rights.

---

## 13. M7 milestones (implementation order)

1. `sys.rs` + `Sel4Notify` (+ a kernel spec exercising Signal/Wait latching).
2. `SharedFrames` map/project over caller-supplied frame caps; a two-thread-in-
   two-PD ring spec.
3. `control.rs` HELLO/HELLO_ACK + feature negotiation (no caps yet).
4. `captx.rs` frame + notification transfer; full `establish`.
5. `serve`/client wait loops; the end-to-end "two components over shared rings"
   spec (the M7 success criterion).
6. fault/teardown spec: kill a peer, assert the survivor gets `PeerFaulted` and
   tears down cleanly.
```
