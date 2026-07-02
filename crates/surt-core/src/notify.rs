//! Wakeup notification backend and the lost-wakeup-free coalescing protocol
//! (spec §9).
//!
//! The transport never wakes once per entry. Instead the consumer sets a
//! `NEEDS_WAKE` flag in the ring header only when it is about to block, and the
//! producer wakes it at most once per arming. The hard part is avoiding a
//! *lost wakeup* — the consumer deciding to sleep at the exact moment the
//! producer publishes. We use the textbook arm-and-recheck (§9.3) made correct
//! with a `SeqCst` fence on each side:
//!
//! ```text
//! consumer ([`Consumer::prepare_wait`])   producer ([`Producer::notify_consumer`])
//!   set NEEDS_WAKE                           publish tail (Release, in try_push)
//!   fence(SeqCst)                            fence(SeqCst)
//!   re-check tail                            load NEEDS_WAKE
//!   if work: don't block                     if set: clear + wake
//!   else: safe to block on the backend
//! ```
//!
//! Two `SeqCst` fences give the Dekker guarantee: at least one of "consumer
//! sees the new tail" or "producer sees NEEDS_WAKE" holds, so the consumer
//! never sleeps through an item. The producer clears the flag when it wakes, so
//! further publishes don't re-wake until the consumer re-arms (coalescing).
//!
//! [`Notify`] is non-blocking and backend-neutral (§9.4): the actual *blocking*
//! wait lives in the host/seL4 backends, not here.

use core::fmt;

use crate::error::PushError;
use crate::ring::{Producer, RingEntry};

/// A notification backend failed to deliver or arm a wakeup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotifyError {
    /// The underlying backend rejected the operation (e.g. an invalid seL4
    /// notification capability).
    Backend,
}

impl fmt::Display for NotifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend => write!(f, "notification backend failed"),
        }
    }
}

impl core::error::Error for NotifyError {}

/// A wakeup backend. **Non-blocking** — the blocking wait is provided
/// separately by the host/seL4 binding (spec §9.4).
///
/// Implementations must make [`wake`](Notify::wake) *latch* with respect to a
/// subsequent backend wait: a wake that happens before the peer blocks must not
/// be lost (a binary semaphore, an seL4 notification, or `Thread::unpark` all
/// satisfy this). The ring-level protocol relies on that latching.
pub trait Notify {
    /// Signal the peer. Idempotent and safe to call when no one is waiting.
    fn wake(&self) -> Result<(), NotifyError>;
    /// Prime the backend to receive a wake (a no-op for always-armed backends
    /// like seL4 notifications).
    fn arm(&self) -> Result<(), NotifyError>;
    /// Cancel a prior [`arm`](Notify::arm).
    fn disarm(&self) -> Result<(), NotifyError>;
}

/// A shared reference to a backend is itself a backend, so the producer (which
/// wakes) and the consumer (which waits) can hold the *same* backend.
impl<T: Notify + ?Sized> Notify for &T {
    #[inline]
    fn wake(&self) -> Result<(), NotifyError> {
        (**self).wake()
    }
    #[inline]
    fn arm(&self) -> Result<(), NotifyError> {
        (**self).arm()
    }
    #[inline]
    fn disarm(&self) -> Result<(), NotifyError> {
        (**self).disarm()
    }
}

/// No-op backend for pure polling — every method succeeds and does nothing.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoNotify;

impl Notify for NoNotify {
    #[inline]
    fn wake(&self) -> Result<(), NotifyError> {
        Ok(())
    }
    #[inline]
    fn arm(&self) -> Result<(), NotifyError> {
        Ok(())
    }
    #[inline]
    fn disarm(&self) -> Result<(), NotifyError> {
        Ok(())
    }
}

/// Outcome of [`Consumer::prepare_wait`](crate::Consumer::prepare_wait).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitDecision {
    /// Work appeared during the recheck — do not block, go consume.
    Ready,
    /// The ring is confirmed empty and `NEEDS_WAKE` is armed — the caller may
    /// now safely block on its notification backend.
    Block,
}

/// A [`Producer`] paired with a [`Notify`] backend that wakes the consumer
/// after a successful push (coalesced — see the module docs).
pub struct NotifyingProducer<E: RingEntry, N: Notify> {
    /// The underlying producer.
    pub producer: Producer<E>,
    /// The backend used to wake the consumer.
    pub notify: N,
}

impl<E: RingEntry, N: Notify> NotifyingProducer<E, N> {
    /// Bundle a producer with its wakeup backend.
    pub fn new(producer: Producer<E>, notify: N) -> Self {
        Self { producer, notify }
    }

    /// Push one entry, then wake the consumer if it is armed. The wake is
    /// best-effort: the push has already succeeded, so a backend error is
    /// dropped (handle it explicitly via
    /// [`Producer::notify_consumer`] if you need the result).
    pub fn try_push(&mut self, entry: E) -> Result<(), PushError> {
        self.producer.try_push(entry)?;
        let _ = self.producer.notify_consumer(&self.notify);
        Ok(())
    }

    /// Push a batch, then wake the consumer once if anything was pushed.
    pub fn push_batch(&mut self, entries: &[E]) -> Result<usize, PushError> {
        let n = self.producer.push_batch(entries)?;
        if n > 0 {
            let _ = self.producer.notify_consumer(&self.notify);
        }
        Ok(n)
    }
}
