//! [`ProjectionAnomalies`]: a small, reusable anomaly-counter surface for
//! read-model projections (`bn-3uu`).
//!
//! # The dogfood finding this answers
//!
//! A projection that folds a *slice* of the global log — the common
//! shape, see `examples/social/src/projections.rs` — cannot decode or route
//! every record it is handed: the log is wider than the slice of it any one
//! projection models. Silently skipping a record it does not understand is
//! the *correct* liveness policy (a strict projection that panics or halts on
//! an unrecognized stream turns "someone shipped an unrelated feature" into
//! an outage), but a silent skip is also silently **invisible**: schema
//! drift, a stream-naming typo, or a bug in the projection's own decode path
//! all look identical to "nothing happened."
//!
//! [`ProjectionAnomalies`] is the missing middle ground: keep skipping (the
//! liveness policy does not change), but count what got skipped, cheaply
//! enough to call on every folded record, so an operator — or a metrics
//! scrape, or a log line — can tell "I am silently dropping records" apart
//! from "the log genuinely has nothing else in it for me."
//!
//! # Shape
//!
//! Three counters, one per failure mode a decode-and-route fold typically
//! distinguishes ([`AnomalyKind`]):
//!
//! - [`undecodable_payload`](ProjectionAnomalies::undecodable_payload) — the
//!   stream/category was recognized but the payload bytes failed to decode for
//!   the message type (e.g. a schema change the projection has not caught up
//!   to).
//! - [`unroutable_stream`](ProjectionAnomalies::unroutable_stream) — the
//!   record's stream (category and/or entity-id suffix) does not match anything
//!   this projection folds.
//! - [`unknown_event_kind`](ProjectionAnomalies::unknown_event_kind) — the
//!   stream was recognized but the stored message type was not.
//!
//! Each counter is a total count plus the global position of the
//! most-recently-recorded occurrence ([`AnomalyCounterSnapshot`]), backed by
//! plain atomics — cheap enough for the fold's hot path, no lock, no
//! allocation. [`ProjectionAnomalies::snapshot`] renders a point-in-time
//! [`ProjectionAnomaliesSnapshot`] for logging or a metrics/status endpoint;
//! both it and [`ProjectionAnomalies`] implement `Display` for a
//! log-friendly one-liner.
//!
//! # A library surface, not an engine hook
//!
//! This type has **no** connection to [`crate::Backend`] or
//! [`crate::EventStore`] — it does not sit on any hot append/replay path this
//! crate owns. It exists so an **application's** projection can opt in by
//! holding one as a field and calling `record_*` at its own skip sites (see
//! `examples/social/src/projections.rs`), exactly the way it would hold any
//! other piece of its own read-model state. The counter's `record_*` methods
//! return whether the call was the first ever occurrence, so a caller with
//! log/tracing context of its own can choose to log a warning line the
//! moment a count first goes nonzero, without this crate imposing a logging
//! dependency or a fixed message format.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// One kind of skip a decode-and-route projection fold can hit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AnomalyKind {
    /// The stream was recognized but the payload failed to decode.
    UndecodablePayload,
    /// The record's stream did not match anything this projection folds.
    UnroutableStream,
    /// The stream was recognized but the stored message type was not.
    UnknownEventKind,
}

impl AnomalyKind {
    /// A short, log-friendly label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            AnomalyKind::UndecodablePayload => "undecodable_payload",
            AnomalyKind::UnroutableStream => "unroutable_stream",
            AnomalyKind::UnknownEventKind => "unknown_event_kind",
        }
    }
}

impl fmt::Display for AnomalyKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// No occurrence has ever been recorded — the sentinel `last_position` value.
///
/// `u64::MAX` rather than `0`: `0` is a valid global position (the very first
/// record in the log), so it cannot double as "never observed."
const NEVER: u64 = u64::MAX;

/// A single lock-free anomaly counter: how many times something happened, and
/// the global position of the most recent occurrence.
///
/// `record` is one or two `fetch_add`/`store`s on plain atomics — safe to call
/// from the hot fold path of a projection with no lock and no allocation.
/// Concurrent recorders may interleave such that `last_position` transiently
/// does not correspond to the very latest `count` — this type is an
/// observability surface, not a linearizable log, and that race is harmless
/// for its purpose (rendering "roughly how many, roughly how recent").
#[derive(Debug)]
pub struct AnomalyCounter {
    count:         AtomicU64,
    last_position: AtomicU64,
}

// NOT `#[derive(Default)]`: a derived `Default` would default-construct
// `last_position` as `AtomicU64::default()` (`0`), which is a valid global
// position, silently breaking the `NEVER` sentinel `new()` relies on for
// "no occurrence yet". Delegate to `new()` instead.
impl Default for AnomalyCounter {
    fn default() -> Self { Self::new() }
}

impl AnomalyCounter {
    /// A fresh, zeroed counter.
    #[must_use]
    pub fn new() -> Self {
        AnomalyCounter {
            count:         AtomicU64::new(0),
            last_position: AtomicU64::new(NEVER),
        }
    }

    /// Record one occurrence at `global_position`.
    ///
    /// Returns `true` iff this call was the **first ever** occurrence (the
    /// count's `0 -> 1` transition) — the moment a caller with its own
    /// logging setup typically wants to emit a one-time warning that this
    /// counter has gone nonzero.
    pub fn record(&self, global_position: u64) -> bool {
        self.last_position.store(global_position, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed) == 0
    }

    /// The total number of occurrences recorded so far.
    #[must_use]
    pub fn count(&self) -> u64 { self.count.load(Ordering::Relaxed) }

    /// The global position of the most recent occurrence, or `None` if
    /// [`record`](Self::record) has never been called.
    #[must_use]
    pub fn last_position(&self) -> Option<u64> {
        match self.last_position.load(Ordering::Relaxed) {
            NEVER => None,
            p => Some(p),
        }
    }

    /// A point-in-time snapshot of this counter.
    #[must_use]
    pub fn snapshot(&self) -> AnomalyCounterSnapshot {
        AnomalyCounterSnapshot {
            count:         self.count(),
            last_position: self.last_position(),
        }
    }
}

/// A point-in-time snapshot of one [`AnomalyCounter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AnomalyCounterSnapshot {
    /// The total number of occurrences recorded as of the snapshot.
    pub count:         u64,
    /// The global position of the most recent occurrence, or `None` if there
    /// were none.
    pub last_position: Option<u64>,
}

impl fmt::Display for AnomalyCounterSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.last_position {
            Some(p) => write!(f, "{} (last @ {p})", self.count),
            None => write!(f, "0"),
        }
    }
}

/// A reusable anomaly-counter surface for a projection that decodes and
/// routes records off the global log: one [`AnomalyCounter`] per
/// [`AnomalyKind`]. See the module docs for the full rationale.
///
/// Cheap to hold by value inside a projection's own state (it is `Sync` and
/// every method takes `&self`), so a projection under a shared `RwLock` or an
/// `Arc` can record anomalies without taking a write lock.
#[derive(Debug, Default)]
pub struct ProjectionAnomalies {
    undecodable_payload: AnomalyCounter,
    unroutable_stream:   AnomalyCounter,
    unknown_event_kind:  AnomalyCounter,
}

impl ProjectionAnomalies {
    /// A fresh set of zeroed counters.
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// The counter for [`AnomalyKind::UndecodablePayload`].
    #[must_use]
    pub fn undecodable_payload(&self) -> &AnomalyCounter {
        &self.undecodable_payload
    }

    /// The counter for [`AnomalyKind::UnroutableStream`].
    #[must_use]
    pub fn unroutable_stream(&self) -> &AnomalyCounter {
        &self.unroutable_stream
    }

    /// The counter for [`AnomalyKind::UnknownEventKind`].
    #[must_use]
    pub fn unknown_event_kind(&self) -> &AnomalyCounter {
        &self.unknown_event_kind
    }

    /// The counter for `kind`.
    #[must_use]
    pub fn counter(&self, kind: AnomalyKind) -> &AnomalyCounter {
        match kind {
            AnomalyKind::UndecodablePayload => &self.undecodable_payload,
            AnomalyKind::UnroutableStream => &self.unroutable_stream,
            AnomalyKind::UnknownEventKind => &self.unknown_event_kind,
        }
    }

    /// Record one occurrence of `kind` at `global_position`. Returns `true`
    /// iff this was `kind`'s first-ever occurrence — see
    /// [`AnomalyCounter::record`].
    pub fn record(&self, kind: AnomalyKind, global_position: u64) -> bool {
        self.counter(kind).record(global_position)
    }

    /// The sum of every counter's total count.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.undecodable_payload.count()
            + self.unroutable_stream.count()
            + self.unknown_event_kind.count()
    }

    /// A point-in-time snapshot of all three counters, cheap to render or log.
    #[must_use]
    pub fn snapshot(&self) -> ProjectionAnomaliesSnapshot {
        ProjectionAnomaliesSnapshot {
            undecodable_payload: self.undecodable_payload.snapshot(),
            unroutable_stream:   self.unroutable_stream.snapshot(),
            unknown_event_kind:  self.unknown_event_kind.snapshot(),
        }
    }
}

impl fmt::Display for ProjectionAnomalies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.snapshot())
    }
}

/// A point-in-time snapshot of a [`ProjectionAnomalies`], suitable for
/// logging or serving from a status/metrics endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ProjectionAnomaliesSnapshot {
    /// Snapshot of [`AnomalyKind::UndecodablePayload`].
    pub undecodable_payload: AnomalyCounterSnapshot,
    /// Snapshot of [`AnomalyKind::UnroutableStream`].
    pub unroutable_stream:   AnomalyCounterSnapshot,
    /// Snapshot of [`AnomalyKind::UnknownEventKind`].
    pub unknown_event_kind:  AnomalyCounterSnapshot,
}

impl ProjectionAnomaliesSnapshot {
    /// The sum of every counter's total count.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.undecodable_payload.count
            + self.unroutable_stream.count
            + self.unknown_event_kind.count
    }
}

impl fmt::Display for ProjectionAnomaliesSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "projection anomalies: undecodable_payload={}, \
             unroutable_stream={}, unknown_event_kind={}",
            self.undecodable_payload,
            self.unroutable_stream,
            self.unknown_event_kind
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_counter_is_zero_and_has_no_last_position() {
        let c = AnomalyCounter::new();
        assert_eq!(c.count(), 0);
        assert_eq!(c.last_position(), None);
        assert_eq!(c.snapshot(), AnomalyCounterSnapshot::default());
    }

    #[test]
    fn record_increments_count_and_tracks_last_position() {
        let c = AnomalyCounter::new();
        assert!(c.record(10)); // first occurrence
        assert_eq!(c.count(), 1);
        assert_eq!(c.last_position(), Some(10));

        assert!(!c.record(42)); // not the first occurrence anymore
        assert_eq!(c.count(), 2);
        assert_eq!(c.last_position(), Some(42));
    }

    #[test]
    fn record_returns_true_only_on_zero_to_one_transition() {
        let c = AnomalyCounter::new();
        let firsts: Vec<bool> = (0..5).map(|i| c.record(i)).collect();
        assert_eq!(firsts, [true, false, false, false, false]);
    }

    #[test]
    fn projection_anomalies_kinds_are_independent() {
        let a = ProjectionAnomalies::new();
        a.record(AnomalyKind::UndecodablePayload, 1);
        a.record(AnomalyKind::UndecodablePayload, 2);
        a.record(AnomalyKind::UnroutableStream, 5);

        assert_eq!(a.undecodable_payload().count(), 2);
        assert_eq!(a.undecodable_payload().last_position(), Some(2));
        assert_eq!(a.unroutable_stream().count(), 1);
        assert_eq!(a.unroutable_stream().last_position(), Some(5));
        assert_eq!(a.unknown_event_kind().count(), 0);
        assert_eq!(a.unknown_event_kind().last_position(), None);
        assert_eq!(a.total(), 3);
    }

    #[test]
    fn counter_accessor_matches_kind() {
        let a = ProjectionAnomalies::new();
        a.record(AnomalyKind::UnknownEventKind, 7);
        assert_eq!(a.counter(AnomalyKind::UnknownEventKind).count(), 1);
        assert_eq!(a.counter(AnomalyKind::UndecodablePayload).count(), 0);
    }

    #[test]
    fn snapshot_is_a_stable_point_in_time_copy() {
        let a = ProjectionAnomalies::new();
        a.record(AnomalyKind::UnroutableStream, 3);
        let snap = a.snapshot();
        a.record(AnomalyKind::UnroutableStream, 99);
        // The earlier snapshot does not see the later record.
        assert_eq!(snap.unroutable_stream.count, 1);
        assert_eq!(snap.unroutable_stream.last_position, Some(3));
        assert_eq!(a.unroutable_stream().count(), 2);
        assert_eq!(snap.total(), 1);
    }

    #[test]
    fn display_renders_zero_counters_without_position() {
        let a = ProjectionAnomalies::new();
        let s = a.to_string();
        assert!(s.contains("undecodable_payload=0"));
        assert!(s.contains("unroutable_stream=0"));
        assert!(s.contains("unknown_event_kind=0"));
        assert!(!s.contains("last @"));
    }

    #[test]
    fn display_renders_nonzero_counter_with_last_position() {
        let a = ProjectionAnomalies::new();
        a.record(AnomalyKind::UndecodablePayload, 123);
        let s = a.to_string();
        assert!(s.contains("undecodable_payload=1 (last @ 123)"));
    }

    #[test]
    fn kind_label_and_display_agree() {
        for kind in [
            AnomalyKind::UndecodablePayload,
            AnomalyKind::UnroutableStream,
            AnomalyKind::UnknownEventKind,
        ] {
            assert_eq!(kind.to_string(), kind.label());
        }
    }
}
