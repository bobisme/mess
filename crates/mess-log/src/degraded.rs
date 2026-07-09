//! D8 store poisoning and degraded-read state (`bn-25e`;
//! `notes/mess-research/12_convergence.md` D8, constrained by
//! [`docs/spec/03-durability.md`] §2.6).
//!
//! # The policy this module encodes
//!
//! A durability barrier (`fdatasync`) that **fails** — with `EIO`, `ENOSPC`,
//! or anything else — leaves the store in a state no in-process bookkeeping
//! can reconstruct: the kernel is free to have written some, all, or none of
//! the dirty pages the barrier was meant to flush, and (the fsyncgate case,
//! §2.6) on `EIO` Linux may have *dropped* those dirty pages entirely, so the
//! very next `fdatasync` can return `Ok` while the bytes are gone. The only
//! safe response is to treat the whole store as **poisoned**:
//!
//! 1. **Fail-fast writes.** Every subsequent append fails with a typed
//!    `StorePoisoned` — the committer never writes atop an indeterminate
//!    durable state.
//! 2. **Degraded reads.** Reads remain *allowed*, but the durable watermark
//!    is frozen at the last known-durable position (the committer stops
//!    advancing it), so a reader can only ever serve the pre-poison committed
//!    prefix. A reader holding this [`Degraded`] handle can additionally
//!    *observe* that it is reading a degraded store (`is_poisoned`) rather
//!    than a live one.
//! 3. **Sticky for the store's lifetime.** The flag is set once and never
//!    cleared. There is no reset API. **The only exit is process restart +
//!    recovery** ([`docs/spec/02-recovery.md`]): a fresh open re-scans the
//!    segment, re-establishes the committed prefix from what actually reached
//!    the device, and starts from a known state.
//! 4. **Never retry the barrier.** The failed barrier's group is
//!    `Indeterminate`, and the committer never re-issues `fdatasync` on a
//!    poisoned store — retrying `fdatasync` after `EIO` is the classic
//!    fsyncgate corruption (§2.6), so it is structurally forbidden, not merely
//!    discouraged.
//!
//! This module is *just* the shared, cheap-to-clone flag plus its cause
//! classification; the committer ([`crate::committer`]) owns wiring it into
//! the write/barrier path, and the reader ([`crate::reader`]) reads a
//! watermark the committer has frozen. All three parties share one
//! [`Degraded`] value, so what the committer poisons, every reader sees.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

/// Why a store's durability barrier failed and poisoned it. Every cause maps
/// to the *same* policy (permanent poison, §2.6) — the cause is retained only
/// for operator diagnostics and to make the fsyncgate `EIO` case legible in
/// logs and tests. It is emphatically **not** a branch point: an `Enospc`
/// poison is exactly as permanent and as sticky as an `Eio` poison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoisonCause {
    /// `fdatasync` returned `ENOSPC` — disk full at the barrier. The device
    /// could not complete the flush; which bytes landed is unknowable.
    Enospc,
    /// `fdatasync` returned `EIO` — the fsyncgate case. On Linux the kernel
    /// may have cleared the failed dirty pages, so a *retry* `fdatasync` can
    /// return `Ok` having flushed nothing. This is the canonical reason the
    /// barrier is never retried (§2.6).
    Eio,
    /// Any other barrier failure. The durable state is equally unknowable, so
    /// the response is identical: poison.
    Other,
}

impl PoisonCause {
    /// Classify a barrier (`fdatasync`) [`io::Error`] into its poison cause.
    /// Matches on `raw_os_error()` so a sim-injected fault (which carries the
    /// same `raw_os_error()` as a real one) classifies identically to the
    /// kernel's.
    pub fn classify(e: &io::Error) -> PoisonCause {
        match e.raw_os_error() {
            Some(c) if c == libc::ENOSPC => PoisonCause::Enospc,
            Some(c) if c == libc::EIO => PoisonCause::Eio,
            _ => PoisonCause::Other,
        }
    }

    /// The non-zero `AtomicU8` code this cause is stored as (`0` is reserved
    /// for the healthy state).
    fn code(self) -> u8 {
        match self {
            PoisonCause::Enospc => 1,
            PoisonCause::Eio => 2,
            PoisonCause::Other => 3,
        }
    }

    fn from_code(code: u8) -> Option<PoisonCause> {
        match code {
            1 => Some(PoisonCause::Enospc),
            2 => Some(PoisonCause::Eio),
            3 => Some(PoisonCause::Other),
            _ => None,
        }
    }
}

/// A cheap-to-clone, `Send + Sync` handle to one store's poison/degraded
/// state. The committer holds it (and is the only writer via [`poison`]);
/// appenders and readers hold clones and query it ([`is_poisoned`],
/// [`cause`]).
///
/// The state is a single `AtomicU8`: `0` is healthy, and any non-zero value
/// is the [`PoisonCause`] code of the barrier failure that poisoned it. It is
/// set exactly once — the **first** barrier failure wins via compare-exchange
/// — and never cleared, so a reader that observes `is_poisoned() == true` can
/// never later observe it flip back (matching the watermark's own monotonicity
/// while the store is live). Restart is the only exit.
///
/// [`poison`]: Degraded::poison
/// [`is_poisoned`]: Degraded::is_poisoned
/// [`cause`]: Degraded::cause
#[derive(Clone, Default)]
pub struct Degraded {
    /// `0` = healthy; otherwise `1 + `[`PoisonCause`] discriminant. Written
    /// once (CAS from `0`), read many.
    state: Arc<AtomicU8>,
}

impl Degraded {
    /// A fresh, healthy state.
    pub fn new() -> Self {
        Degraded::default()
    }

    /// Whether a barrier has poisoned this store. Once `true`, always `true`
    /// (sticky for the store's lifetime; the only exit is restart + recovery).
    pub fn is_poisoned(&self) -> bool {
        self.state.load(Ordering::Acquire) != 0
    }

    /// The cause of the poison, or `None` while healthy. Every cause carries
    /// the same permanent policy — this is diagnostics, not a branch.
    pub fn cause(&self) -> Option<PoisonCause> {
        PoisonCause::from_code(self.state.load(Ordering::Acquire))
    }

    /// Poison the store with `cause`. Sticky and idempotent: the **first**
    /// call to fire wins (barrier failures are only ever reported by the
    /// single committer thread, but the first-wins CAS makes the invariant
    /// hold regardless), later calls are no-ops that preserve the original
    /// cause. Returns `true` iff this call is the one that transitioned the
    /// store from healthy to poisoned.
    pub fn poison(&self, cause: PoisonCause) -> bool {
        self.state
            .compare_exchange(0, cause.code(), Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_maps_raw_os_errors() {
        assert_eq!(
            PoisonCause::classify(&io::Error::from_raw_os_error(libc::ENOSPC)),
            PoisonCause::Enospc
        );
        assert_eq!(
            PoisonCause::classify(&io::Error::from_raw_os_error(libc::EIO)),
            PoisonCause::Eio
        );
        // An error with no raw os code (e.g. a synthetic `Interrupted`) is
        // still a barrier failure → still poison, classified `Other`.
        assert_eq!(
            PoisonCause::classify(&io::Error::new(io::ErrorKind::Interrupted, "x")),
            PoisonCause::Other
        );
        // An unusual errno that is neither ENOSPC nor EIO also falls to Other.
        assert_eq!(
            PoisonCause::classify(&io::Error::from_raw_os_error(libc::EACCES)),
            PoisonCause::Other
        );
    }

    #[test]
    fn healthy_by_default() {
        let d = Degraded::new();
        assert!(!d.is_poisoned());
        assert_eq!(d.cause(), None);
    }

    #[test]
    fn poison_is_sticky_and_first_cause_wins() {
        let d = Degraded::new();
        assert!(d.poison(PoisonCause::Eio), "first poison transitions the store");
        assert!(d.is_poisoned());
        assert_eq!(d.cause(), Some(PoisonCause::Eio));

        // A second poison (even a different cause) is a no-op: the store is
        // already poisoned and the original cause is preserved. No reset.
        assert!(!d.poison(PoisonCause::Enospc), "second poison does not re-transition");
        assert_eq!(d.cause(), Some(PoisonCause::Eio), "first cause is retained");
        assert!(d.is_poisoned());
    }

    #[test]
    fn clones_share_one_state() {
        let a = Degraded::new();
        let b = a.clone();
        assert!(!b.is_poisoned());
        a.poison(PoisonCause::Enospc);
        // What one clone poisons, every clone sees — this is the property that
        // lets a reader holding a clone observe the committer's poison.
        assert!(b.is_poisoned());
        assert_eq!(b.cause(), Some(PoisonCause::Enospc));
    }
}
