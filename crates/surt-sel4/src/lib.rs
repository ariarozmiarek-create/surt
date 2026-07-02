//! # `surt-sel4` — the seL4 binding for SURT
//!
//! [`surt-core`](surt_core) is the platform-agnostic ring transport: it projects
//! a single-producer / single-consumer ring over caller-provided shared memory
//! and defines the coalesced, lost-wakeup-free notification protocol — but it
//! deliberately knows nothing about seL4. This crate is the thin seL4 glue:
//!
//! - [`Sel4Env`] — the *only* platform contract the fast path needs: the two
//!   syscalls `seL4_Signal` and `seL4_Wait` on a Notification.
//! - [`Sel4Notify`] — a [`surt_core::Notify`] backed by a Notification signalled
//!   through an [`Sel4Env`], plus the blocking `wait` the core omits.
//! - [`drain_blocking`] — a ready-made consumer wait-loop that drains the ring
//!   and blocks only when genuinely idle (coalesced via `prepare_wait`).
//!
//! ## Portability
//!
//! Everything here is generic over [`Sel4Env`], so the same code runs on:
//! - **upstream seL4** — implement [`Sel4Env`] with the `sel4` crate
//!   (`sel4::Notification::signal` / `sel4::Notification::wait`);
//! - **any other seL4-compatible kernel** — implement it with that kernel's ABI.
//!
//! The **control path** (retyping/sharing frames, transferring the frame and
//! notification caps, the HELLO/HELLO_ACK handshake) is intentionally *not*
//! abstracted here — it is a handful of ordinary seL4 invocations the
//! application issues once at setup, and it varies more between platforms than
//! the fast path. See `docs/architecture/surt_sel4_binding.md` for the full
//! design, and the rust-micro rootserver's `surt_ring_cap_transfer` microtest
//! for a complete working reference (two isolated components, own CSpaces,
//! frame + notification caps transferred over IPC).
//!
//! ```no_run
//! use surt_sel4::{Sel4Env, Sel4Notify, CPtr, drain_blocking};
//! use surt_sel4::surt_core::{Consumer, surt_abi::SurtSqe};
//!
//! struct MyKernel; // your platform
//! impl Sel4Env for MyKernel {
//!     fn signal(&self, ntfn: CPtr) { /* seL4_Signal(ntfn) */ }
//!     fn wait(&self, ntfn: CPtr)   { /* seL4_Wait(ntfn, _) */ }
//! }
//!
//! # fn run(mut c: Consumer<SurtSqe>, ntfn: CPtr) {
//! let env = MyKernel;
//! let wake = Sel4Notify::new(&env, ntfn);
//! drain_blocking(&mut c, &wake, |sqe: &SurtSqe| {
//!     // handle one submission; return false to stop.
//!     let _ = sqe;
//!     true
//! });
//! # }
//! ```

#![no_std]

pub use surt_core;
pub use surt_core::surt_abi;

use surt_core::{Consumer, Notify, NotifyError, RingEntry, WaitDecision};

/// A capability pointer (seL4 `CPtr` / `seL4_CPtr`). Opaque to this crate — the
/// [`Sel4Env`] implementation interprets it against whatever CSpace the caller
/// runs in.
pub type CPtr = u64;

/// The seL4 operations SURT's fast-path wakeup needs from the host platform —
/// just the two Notification syscalls. Implement this over the upstream `sel4`
/// crate or your own kernel's ABI.
pub trait Sel4Env {
    /// `seL4_Signal(ntfn)` — wake a peer blocked on the notification. Idempotent;
    /// seL4 notifications latch, so signalling with no waiter is safe.
    fn signal(&self, ntfn: CPtr);
    /// `seL4_Wait(ntfn, _)` — block until the notification is signalled.
    fn wait(&self, ntfn: CPtr);
}

/// A [`surt_core::Notify`] backed by a seL4 Notification signalled through an
/// [`Sel4Env`]. seL4 notifications latch a signal that precedes the wait, so
/// `arm`/`disarm` are no-ops — exactly what the coalesced-wakeup protocol wants.
///
/// The blocking [`wait`](Sel4Notify::wait) is a separate method (not part of the
/// [`Notify`] trait, which is send-only by design): the consumer's wait-loop
/// calls it after [`Consumer::prepare_wait`] returns [`WaitDecision::Block`].
pub struct Sel4Notify<'e, E: Sel4Env> {
    env: &'e E,
    ntfn: CPtr,
}

impl<'e, E: Sel4Env> Sel4Notify<'e, E> {
    /// Wrap the notification `ntfn`, signalled/waited through `env`.
    pub fn new(env: &'e E, ntfn: CPtr) -> Self {
        Self { env, ntfn }
    }

    /// Block until the notification is signalled (the wait half `Notify` omits).
    pub fn wait(&self) {
        self.env.wait(self.ntfn);
    }
}

impl<E: Sel4Env> Notify for Sel4Notify<'_, E> {
    fn wake(&self) -> Result<(), NotifyError> {
        self.env.signal(self.ntfn);
        Ok(())
    }
    fn arm(&self) -> Result<(), NotifyError> {
        Ok(())
    }
    fn disarm(&self) -> Result<(), NotifyError> {
        Ok(())
    }
}

/// Why [`drain_blocking`] returned.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DrainExit {
    /// `handler` asked to stop (returned `false`).
    Stopped,
    /// The peer misbehaved (an out-of-range cursor was detected).
    PeerFaulted,
}

/// Consumer-side blocking drain: pop entries, hand each to `handler`, and block
/// on `wake` only when the ring is genuinely empty — using [`Consumer::prepare_wait`]
/// so a publish racing the sleep is never lost (the M4 coalescing protocol).
///
/// Returns [`DrainExit::Stopped`] when `handler` returns `false`, or
/// [`DrainExit::PeerFaulted`] if the peer corrupts the ring cursors.
pub fn drain_blocking<E, Env, H>(
    consumer: &mut Consumer<E>,
    wake: &Sel4Notify<Env>,
    mut handler: H,
) -> DrainExit
where
    E: RingEntry,
    Env: Sel4Env,
    H: FnMut(&E) -> bool,
{
    loop {
        match consumer.try_pop() {
            Ok(Some(entry)) => {
                if !handler(&entry) {
                    return DrainExit::Stopped;
                }
            }
            // Empty: arm + block, unless a publish already raced in.
            Ok(None) => match consumer.prepare_wait() {
                Ok(WaitDecision::Block) => wake.wait(),
                Ok(WaitDecision::Ready) => {}
                Err(_) => return DrainExit::PeerFaulted,
            },
            Err(_) => return DrainExit::PeerFaulted,
        }
    }
}
