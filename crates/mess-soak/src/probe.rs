//! The invariant probes, as **pure functions** returning `Result<(),
//! Violation>`.
//!
//! Keeping every probe a pure function of (observed engine state, expected
//! shadow state) is what makes the acceptance criterion "each probe
//! demonstrably able to fire" a unit test: feed the function a doctored input
//! and assert it returns the matching [`Violation`]. The driver is the only
//! thing that supplies *real* engine reads; the decision logic lives here and
//! is tested in isolation.

use std::collections::HashMap;
use std::time::Duration;

use mess_store::Version;
use mess_store::backend::StoredRecord;

use crate::shadow::ShadowEvent;

/// A detected invariant violation. Any one of these is fatal: the driver dumps
/// full state and aborts (`03`/`05` say a durable store must never present any
/// of these, so continuing would only pile corruption on corruption).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    /// A stream's positions were not a dense `0..n` sequence: a gap or a
    /// duplicate stream position (violates the per-stream density invariant).
    Density { stream: String, detail: String },
    /// A read-back record disagreed with the shadow model at a known position
    /// (index/log divergence — the read tier lied about durable history).
    IndexMismatch {
        stream:     String,
        stream_pos: u64,
        field:      &'static str,
        expected:   String,
        got:        String,
    },
    /// The engine's head for a stream disagreed with the shadow head.
    HeadMismatch { stream: String, expected: Version, got: Version },
    /// A subscriber did not see every global position exactly once in order.
    SubscriptionGap { subscriber: String, expected: u64, got: u64 },
    /// After a crash+reopen the engine lost a globally-durable event, or grew
    /// a gap in the recovered prefix.
    RecoveryLoss { detail: String },
    /// Resident set size crossed the configured ceiling (a leak, per the
    /// "RSS plateau" exit criterion).
    Rss { rss_bytes: u64, ceiling_bytes: u64 },
    /// Open file descriptors crossed the ceiling (an fd leak — aging seals /
    /// subscriptions not releasing handles).
    Fd { count: usize, ceiling: usize },
    /// fsync p99 crossed the ceiling (device/durability degradation — the
    /// round-4 50x finding).
    FsyncP99 { p99: Duration, ceiling: Duration },
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Violation::Density { stream, detail } => {
                write!(f, "DENSITY[{stream}]: {detail}")
            }
            Violation::IndexMismatch {
                stream,
                stream_pos,
                field,
                expected,
                got,
            } => write!(
                f,
                "INDEX!=LOG[{stream}@{stream_pos}] {field}: expected \
                 {expected}, got {got}"
            ),
            Violation::HeadMismatch { stream, expected, got } => {
                write!(f, "HEAD[{stream}]: expected {expected:?}, got {got:?}")
            }
            Violation::SubscriptionGap { subscriber, expected, got } => write!(
                f,
                "SUBSCRIPTION[{subscriber}]: expected global {expected}, got \
                 {got}"
            ),
            Violation::RecoveryLoss { detail } => {
                write!(f, "RECOVERY: {detail}")
            }
            Violation::Rss { rss_bytes, ceiling_bytes } => {
                write!(f, "RSS: {rss_bytes} bytes > ceiling {ceiling_bytes}")
            }
            Violation::Fd { count, ceiling } => {
                write!(f, "FD: {count} open > ceiling {ceiling}")
            }
            Violation::FsyncP99 { p99, ceiling } => {
                write!(f, "FSYNC_P99: {p99:?} > ceiling {ceiling:?}")
            }
        }
    }
}

impl std::error::Error for Violation {}

/// Per-stream density: the `stream_position`s the engine returned for a stream,
/// read in ascending order, must be exactly `0, 1, ..., n-1` — no gap, no
/// duplicate. `positions` is whatever `read_stream(stream, NoStream, ..)`
/// yielded, in order.
pub fn check_density(stream: &str, positions: &[u64]) -> Result<(), Violation> {
    for (i, &pos) in positions.iter().enumerate() {
        let want = i as u64;
        if pos != want {
            let detail = if pos > want {
                format!("gap: expected position {want} at index {i}, saw {pos}")
            } else {
                format!(
                    "duplicate/out-of-order: expected position {want} at \
                     index {i}, saw {pos}"
                )
            };
            return Err(Violation::Density {
                stream: stream.to_string(),
                detail,
            });
        }
    }
    Ok(())
}

/// Compare one read-back record against the shadow's expected event at that
/// position. Checks stream id, message type, and payload bytes.
pub fn check_record(
    stream: &str,
    stream_pos: u64,
    expected: &ShadowEvent,
    got: &StoredRecord,
) -> Result<(), Violation> {
    if got.stream_id != stream {
        return Err(Violation::IndexMismatch {
            stream: stream.to_string(),
            stream_pos,
            field: "stream_id",
            expected: stream.to_string(),
            got: got.stream_id.clone(),
        });
    }
    if got.stream_position != stream_pos {
        return Err(Violation::IndexMismatch {
            stream: stream.to_string(),
            stream_pos,
            field: "stream_position",
            expected: stream_pos.to_string(),
            got: got.stream_position.to_string(),
        });
    }
    if got.message_type != expected.message_type {
        return Err(Violation::IndexMismatch {
            stream: stream.to_string(),
            stream_pos,
            field: "message_type",
            expected: expected.message_type.clone(),
            got: got.message_type.clone(),
        });
    }
    if got.data != expected.data {
        return Err(Violation::IndexMismatch {
            stream: stream.to_string(),
            stream_pos,
            field: "data",
            expected: hex_preview(&expected.data),
            got: hex_preview(&got.data),
        });
    }
    Ok(())
}

/// Compare an engine head to the shadow head.
pub fn check_head(
    stream: &str,
    expected: Version,
    got: Version,
) -> Result<(), Violation> {
    if expected == got {
        Ok(())
    } else {
        Err(Violation::HeadMismatch {
            stream: stream.to_string(),
            expected,
            got,
        })
    }
}

/// A subscriber's cursor: it must observe every global position exactly once,
/// strictly ascending with no gaps — the subscription-delivery contract
/// (`06`). Construct at the position the subscriber joined *after* (so a fresh
/// full subscriber starts with `next_expected == 0`).
#[derive(Debug, Clone)]
pub struct SubCursor {
    pub name:          String,
    pub next_expected: u64,
}

impl SubCursor {
    #[must_use]
    pub fn joining_from(name: impl Into<String>, first_global: u64) -> Self {
        SubCursor { name: name.into(), next_expected: first_global }
    }

    /// Observe the next delivered global position. Must equal `next_expected`;
    /// a higher value is a dropped event, a lower/equal value is a
    /// re-delivery — both are gaps in the exactly-once-in-order contract.
    pub fn observe(&mut self, global_pos: u64) -> Result<(), Violation> {
        if global_pos != self.next_expected {
            return Err(Violation::SubscriptionGap {
                subscriber: self.name.clone(),
                expected:   self.next_expected,
                got:        global_pos,
            });
        }
        self.next_expected += 1;
        Ok(())
    }
}

/// How recovery surfaced an event that is NOT at a shadow-acked position — the
/// spec-02 A6 classification of a post-reopen "extra".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtraKind {
    /// Byte-matches a distinct submitted-but-unacked append (an [`crate`]-level
    /// A6 candidate), consumed here. **Legal** (spec 02 §6): recovery MAY
    /// surface a complete, unacknowledged batch.
    SubmittedUnacked,
    /// Byte-matches an event that is ALSO present at its own acked position:
    /// the same acked event surfaced a second time — a recovery
    /// **double-replay** bug (Z1-family), never legal.
    DuplicateOfAcked,
    /// Byte-matches no submitted append at all — a **fabricated** event
    /// recovery invented from nowhere (or resurfaced a conflicted,
    /// never-durable write).
    Fabricated,
}

/// Running tally of the [`ExtraKind`]s over all extras after a reopen.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExtraCounts {
    pub submitted_unacked:  u64,
    pub duplicate_of_acked: u64,
    pub fabricated:         u64,
}

impl ExtraCounts {
    /// The count that makes recovery **wrong**: duplicates + fabrications. Zero
    /// iff every extra was a legal A6 candidate.
    #[must_use]
    pub fn illegal(&self) -> u64 { self.duplicate_of_acked + self.fabricated }

    pub(crate) fn record(&mut self, kind: ExtraKind) {
        match kind {
            ExtraKind::SubmittedUnacked => self.submitted_unacked += 1,
            ExtraKind::DuplicateOfAcked => self.duplicate_of_acked += 1,
            ExtraKind::Fabricated => self.fabricated += 1,
        }
    }
}

/// Classify one post-reopen extra (an engine event at a position the shadow
/// never acked) against the acked-payload index and the submitted-but-unacked
/// candidate multiset. Consumes one candidate on a
/// [`SubmittedUnacked`](ExtraKind::SubmittedUnacked) match so no candidate
/// excuses two extras. Because every driver payload embeds a unique write
/// nonce, a payload that appears in `acked` is necessarily a duplicate of that
/// acked event (spec-02 A6 forbids duplicating an *acked* batch), and a payload
/// in neither set was never submitted at all.
pub fn classify_extra(
    payload: &[u8],
    acked: &HashMap<Vec<u8>, Vec<u64>>,
    candidates: &mut HashMap<Vec<u8>, u32>,
) -> ExtraKind {
    if acked.contains_key(payload) {
        ExtraKind::DuplicateOfAcked
    } else if let Some(n) = candidates.get_mut(payload).filter(|n| **n > 0) {
        *n -= 1;
        ExtraKind::SubmittedUnacked
    } else {
        ExtraKind::Fabricated
    }
}

/// RSS ceiling. `ceiling == 0` disables the check.
pub fn check_rss(rss_bytes: u64, ceiling_bytes: u64) -> Result<(), Violation> {
    if ceiling_bytes != 0 && rss_bytes > ceiling_bytes {
        Err(Violation::Rss { rss_bytes, ceiling_bytes })
    } else {
        Ok(())
    }
}

/// fd ceiling. `ceiling == 0` disables the check.
pub fn check_fd(count: usize, ceiling: usize) -> Result<(), Violation> {
    if ceiling != 0 && count > ceiling {
        Err(Violation::Fd { count, ceiling })
    } else {
        Ok(())
    }
}

/// fsync p99 ceiling. `ceiling == ZERO` disables the check.
pub fn check_fsync_p99(
    p99: Duration,
    ceiling: Duration,
) -> Result<(), Violation> {
    if ceiling != Duration::ZERO && p99 > ceiling {
        Err(Violation::FsyncP99 { p99, ceiling })
    } else {
        Ok(())
    }
}

fn hex_preview(bytes: &[u8]) -> String {
    let mut s = String::new();
    for b in bytes.iter().take(16) {
        s.push_str(&format!("{b:02x}"));
    }
    if bytes.len() > 16 {
        s.push_str("...");
    }
    format!("{}B[{s}]", bytes.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(t: &str, d: &[u8]) -> ShadowEvent {
        ShadowEvent { message_type: t.into(), data: d.into() }
    }
    fn rec(stream: &str, pos: u64, t: &str, d: &[u8]) -> StoredRecord {
        StoredRecord {
            stream_id:       stream.into(),
            message_type:    t.into(),
            data:            d.into(),
            stream_position: pos,
            global_position: 0,
        }
    }

    // ---- density ----
    #[test]
    fn density_accepts_dense() {
        assert!(check_density("s", &[0, 1, 2, 3]).is_ok());
        assert!(check_density("s", &[]).is_ok());
    }
    #[test]
    fn density_fires_on_gap() {
        let v = check_density("s", &[0, 1, 3]).unwrap_err();
        assert!(matches!(v, Violation::Density { .. }), "{v}");
        assert!(v.to_string().contains("gap"));
    }
    #[test]
    fn density_fires_on_dupe() {
        let v = check_density("s", &[0, 1, 1]).unwrap_err();
        assert!(matches!(v, Violation::Density { .. }), "{v}");
    }

    // ---- index == log ----
    #[test]
    fn record_matches() {
        assert!(
            check_record("s", 2, &ev("t", b"abc"), &rec("s", 2, "t", b"abc"))
                .is_ok()
        );
    }
    #[test]
    fn record_fires_on_payload_divergence() {
        let v =
            check_record("s", 2, &ev("t", b"abc"), &rec("s", 2, "t", b"XYZ"))
                .unwrap_err();
        assert!(
            matches!(v, Violation::IndexMismatch { field: "data", .. }),
            "{v}"
        );
    }
    #[test]
    fn record_fires_on_type_divergence() {
        let v = check_record(
            "s",
            0,
            &ev("opened", b""),
            &rec("s", 0, "closed", b""),
        )
        .unwrap_err();
        assert!(
            matches!(v, Violation::IndexMismatch { field: "message_type", .. }),
            "{v}"
        );
    }
    #[test]
    fn record_fires_on_position_divergence() {
        let v = check_record("s", 5, &ev("t", b""), &rec("s", 6, "t", b""))
            .unwrap_err();
        assert!(
            matches!(
                v,
                Violation::IndexMismatch { field: "stream_position", .. }
            ),
            "{v}"
        );
    }

    // ---- head ----
    #[test]
    fn head_matches_and_fires() {
        assert!(check_head("s", Version::At(3), Version::At(3)).is_ok());
        let v = check_head("s", Version::At(3), Version::At(2)).unwrap_err();
        assert!(matches!(v, Violation::HeadMismatch { .. }), "{v}");
    }

    // ---- subscription ----
    #[test]
    fn subscription_accepts_contiguous() {
        let mut c = SubCursor::joining_from("sub", 0);
        for gp in 0..10 {
            c.observe(gp).unwrap();
        }
    }
    #[test]
    fn subscription_fires_on_skip() {
        let mut c = SubCursor::joining_from("sub", 0);
        c.observe(0).unwrap();
        c.observe(1).unwrap();
        // Doctored sequence skips global 2.
        let v = c.observe(3).unwrap_err();
        assert!(
            matches!(v, Violation::SubscriptionGap { expected: 2, got: 3, .. }),
            "{v}"
        );
    }
    #[test]
    fn subscription_fires_on_redelivery() {
        let mut c = SubCursor::joining_from("sub", 0);
        c.observe(0).unwrap();
        c.observe(1).unwrap();
        let v = c.observe(1).unwrap_err();
        assert!(
            matches!(v, Violation::SubscriptionGap { expected: 2, got: 1, .. }),
            "{v}"
        );
    }

    // ---- A6 extra classification ----
    fn acked_of(pairs: &[(&[u8], u64)]) -> HashMap<Vec<u8>, Vec<u64>> {
        let mut m: HashMap<Vec<u8>, Vec<u64>> = HashMap::new();
        for (p, gp) in pairs {
            m.entry(p.to_vec()).or_default().push(*gp);
        }
        m
    }
    fn cands(pairs: &[(&[u8], u32)]) -> HashMap<Vec<u8>, u32> {
        pairs.iter().map(|(p, n)| (p.to_vec(), *n)).collect()
    }

    #[test]
    fn extra_submitted_unacked_is_legal_and_consumes_one_candidate() {
        let acked = acked_of(&[(b"acked", 0)]);
        let mut c = cands(&[(b"inflight", 1)]);
        assert_eq!(
            classify_extra(b"inflight", &acked, &mut c),
            ExtraKind::SubmittedUnacked
        );
        // The candidate is now spent: a *second* extra with the same payload is
        // no longer excused (a candidate can back at most one surfaced event).
        assert_eq!(
            classify_extra(b"inflight", &acked, &mut c),
            ExtraKind::Fabricated
        );
    }

    #[test]
    fn extra_duplicate_of_acked_is_a_double_replay_bug() {
        let acked = acked_of(&[(b"acked", 3)]);
        let mut c = cands(&[]);
        assert_eq!(
            classify_extra(b"acked", &acked, &mut c),
            ExtraKind::DuplicateOfAcked,
            "a payload already present at its acked position surfacing again \
             is a duplicate"
        );
    }

    #[test]
    fn extra_matching_nothing_is_fabricated() {
        let acked = acked_of(&[(b"acked", 0)]);
        let mut c = cands(&[(b"inflight", 1)]);
        assert_eq!(
            classify_extra(b"ghost", &acked, &mut c),
            ExtraKind::Fabricated
        );
    }

    #[test]
    fn extra_counts_illegal_is_dupes_plus_fabricated_only() {
        let mut e = ExtraCounts::default();
        e.record(ExtraKind::SubmittedUnacked);
        e.record(ExtraKind::SubmittedUnacked);
        assert_eq!(
            e.illegal(),
            0,
            "legal A6 candidates never count as illegal"
        );
        e.record(ExtraKind::DuplicateOfAcked);
        e.record(ExtraKind::Fabricated);
        assert_eq!(e.illegal(), 2);
        assert_eq!(e.submitted_unacked, 2);
        assert_eq!(e.duplicate_of_acked, 1);
        assert_eq!(e.fabricated, 1);
    }

    // ---- ceilings ----
    #[test]
    fn rss_ceiling() {
        assert!(check_rss(100, 0).is_ok()); // disabled
        assert!(check_rss(100, 200).is_ok());
        assert!(matches!(
            check_rss(300, 200).unwrap_err(),
            Violation::Rss { .. }
        ));
    }
    #[test]
    fn fd_ceiling() {
        assert!(check_fd(10, 0).is_ok());
        assert!(check_fd(10, 20).is_ok());
        assert!(matches!(check_fd(30, 20).unwrap_err(), Violation::Fd { .. }));
    }
    #[test]
    fn fsync_ceiling() {
        assert!(
            check_fsync_p99(Duration::from_millis(5), Duration::ZERO).is_ok()
        );
        assert!(
            check_fsync_p99(
                Duration::from_millis(5),
                Duration::from_millis(10)
            )
            .is_ok()
        );
        let v = check_fsync_p99(
            Duration::from_millis(50),
            Duration::from_millis(10),
        )
        .unwrap_err();
        assert!(matches!(v, Violation::FsyncP99 { .. }), "{v}");
    }
}
