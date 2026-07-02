//! # `surt-core` — host-testable SPSC ring transport
//!
//! The mechanical heart of SURT: a single-producer / single-consumer ring
//! projected over caller-provided shared memory, plus the producer/consumer
//! views and batch operations. It is `#![no_std]`, performs **no allocation on
//! the hot path**, and has no dependency on seL4 — so the exact same code that
//! runs between two seL4 components is exercised here by ordinary host threads
//! (`cargo test`).
//!
//! ## What lives here
//! - [`init_ring`] / [`ring_bytes`] — lay out and initialise a ring's header.
//! - [`Producer`] / [`Consumer`] — the two ends, with `try_push`/`push_batch`
//!   and `try_pop`/`pop_batch`, cached cursors, and release/acquire ordering.
//! - [`QueuePair`] / [`QueuePairPeer`] — convenience bundles of a submit +
//!   completion ring for the two sides of a bidirectional connection.
//! - [`RingEntry`] — the marker for projectable POD entry types (`SurtSqe`,
//!   `SurtCqe`).
//! - [`BufferRegistry`] / [`validate_sqe`] / [`Ownership`] — registered buffers
//!   with generation counters and bounds/rights checks, plus submission-entry
//!   validation against the registry.
//! - [`Notify`] / [`NoNotify`] / [`NotifyingProducer`] / [`WaitDecision`] —
//!   the backend-neutral wakeup trait and the lost-wakeup-free coalescing
//!   protocol (`Consumer::prepare_wait` ↔ `Producer::notify_consumer`).
//!
//! This crate owns the ring data path, its memory ordering, buffer validation,
//! and the wakeup protocol. The actual *blocking* wait and the concrete seL4
//! backend live in the binding crate.
//!
//! ## Safety model
//! Shared memory is treated as mutable by an untrusted peer. Geometry is
//! validated once at attach and cached locally; cursor movement is bounded so a
//! misbehaving peer can never make us read or overwrite out of range. Every
//! `unsafe` block carries a `SAFETY:` comment (enforced by
//! `clippy::undocumented_unsafe_blocks`).

#![cfg_attr(not(any(test, feature = "std")), no_std)]

mod buffer;
mod error;
mod notify;
mod ring;

pub use buffer::{validate_sqe, BufferRegion, BufferRegistry, Ownership};
pub use error::{BufferError, PopError, PushError, RingError};
pub use notify::{NoNotify, Notify, NotifyError, NotifyingProducer, WaitDecision};
pub use ring::{
    init_ring, ring_bytes, Consumer, Producer, QueuePair, QueuePairPeer, RingConfig, RingEntry,
};

/// Re-export of the shared ABI so downstream code can depend on a single crate.
pub use surt_abi;
