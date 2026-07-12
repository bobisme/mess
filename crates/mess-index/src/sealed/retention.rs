//! The retention-blocking rule (`bn-2ug`): a sealed segment MUST NOT be
//! deleted while it holds a **certification frame** — frame `v` or frame
//! `v+1` — of any live snapshot, unless a durable `SnapshotAnchor` (Path C,
//! `docs/spec/05-fold-certificates.md` §7.1 / §8.2) already discharges that
//! snapshot's prefix claim without needing either frame.
//!
//! # Placement decision (why this crate, why this module)
//!
//! Doc 05 §8.2 states the normative rule; `bn-2ug` asks for the *mechanism*.
//! Three crates touch this: `mess-log` owns the physical segment (the
//! deletion unit) and the `SnapshotAnchor`/`StreamHeadTable` footer types;
//! `mess-index` owns the **per-stream frame-range structure of a sealed
//! segment** ([`crate::sealed::segment::SealedSegmentIndex`]'s directory:
//! `stream_id -> first_version..=last_version`); `mess-store` owns the live
//! `SnapshotRef`s themselves (`crates/mess-store/src/snapshot.rs`).
//!
//! No retention *executor* exists yet in this codebase (v1 retention —
//! whole-segment deletion — is unbuilt; there is no `delete_segment`/candidate
//! path anywhere in `mess-log`, `mess-index`, or `mess-store` as of this
//! bone). So this module places the rule at the layer that **already has the
//! exact data the rule needs to consult without inventing new plumbing**:
//! `mess-index` is the only crate that knows a sealed segment's per-stream
//! frame coverage (`SealedSegmentIndex::stream_range`), and it already
//! depends on `mess-log` (so it can consume real `SnapshotAnchor` values
//! directly, not a stand-in). [`segment_retention_decision`] is a pure
//! function decoupled even from `SealedSegmentIndex`'s byte format — it takes
//! a plain per-stream span list — so it is unit-testable in isolation and
//! reusable from any future executor regardless of where segment metadata
//! ultimately lives. [`decide_segment`] is the wiring: the adapter from a
//! real `SealedSegmentIndex` to the pure function.
//!
//! **Integration seam.** There is no retention executor to call this from
//! yet. [`decide_segment`] (or [`segment_retention_decision`] directly, for a
//! caller that already has spans from elsewhere) is the documented gate: *any
//! future code path that unlinks a sealed segment's files MUST call this
//! first and MUST NOT proceed on [`RetentionDecision::Blocked`]*. That
//! integration point is `crate::sealed::store::SealedStore` (or its future
//! deletion counterpart) plus whatever assembles the live-snapshot and
//! durable-anchor inputs (`mess-store`'s snapshot registry and the union of
//! every retained segment's `SnapshotAnchorList`, per §8.2 rule 2).
//!
//! # The conservative reading of §8.2 rule 1
//!
//! Doc 05 §8.2 rule 1, precisely: compaction MUST NOT delete a frame that is
//! the *sole* certification frame of a live snapshot (frame `v` when `v+1` is
//! also gone, or vice versa) without first recording a `SnapshotAnchor`. Read
//! literally, that permits deleting `v` alone as long as `v+1` survives
//! elsewhere. But v1 retention deletes **whole segments only**
//! (`docs/spec/05-fold-certificates.md` §8.2, bn-2ug's brief), and a
//! whole-segment-granularity decision about segment `S` cannot cheaply know
//! whether "the other frame" survives in some *other* retained segment
//! without first walking the whole retained set. bn-2ug's brief resolves this
//! by specifying the simpler, strictly more conservative rule directly: **a
//! segment is retention-blocked while any live `SnapshotRef`'s certification
//! frames live in it** — full stop, not "...and no other segment has the
//! other frame." This never violates rule 1 (it only blocks strictly more
//! often) and needs no cross-segment reasoning. A future v2 that wants the
//! less conservative rule can compute "does some *other* retained segment
//! cover the sibling frame" as a strict refinement layered on top of this
//! function's per-segment verdicts.

use mess_log::footer_ext::SnapshotAnchor;

use crate::sealed::segment::SealedSegmentIndex;

/// One stream's committed frame span within a segment: `first_version
/// ..= last_version`, inclusive, 0-based — mirrors
/// [`SealedSegmentIndex::stream_range`]. Deliberately decoupled from the
/// sidecar byte format so [`segment_retention_decision`] is testable without
/// building a real sidecar and reusable by any future segment-metadata
/// representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentStreamSpan {
    pub stream_id:     u64,
    pub first_version: u64,
    pub last_version:  u64,
}

impl SegmentStreamSpan {
    fn contains(&self, version: u64) -> bool {
        version >= self.first_version && version <= self.last_version
    }
}

/// A live snapshot's prefix-certification requirement (doc 05 §8.2): as long
/// as this snapshot is live, its certification frames — frame `v` (Path A)
/// and frame `v+1` (Path B) of `stream_id` — must remain resolvable by *some*
/// retained segment, unless a durable Path-C `SnapshotAnchor` already
/// discharges the prefix claim (§7.1 Path C).
///
/// A `covers_empty_prefix` snapshot (§4.2) has **no** certification frames —
/// its prefix claim is `event_prefix_hash == genesis`, checked without
/// reading any frame — so callers MUST NOT construct one of these for it;
/// filter such snapshots out before calling [`segment_retention_decision`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveSnapshotRef {
    /// Interned stream id (D3).
    pub stream_id: u64,
    /// `v`, the snapshot's `stream_version` (§4.1, 0-based last-index).
    pub version:   u64,
}

/// Which certification frame of a [`LiveSnapshotRef`] a segment holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertFrame {
    /// Frame `v` — Path A's input (recompute `h[v]` from the payload).
    V,
    /// Frame `v+1` — Path B's input (read the stored chain value).
    VPlus1,
}

/// One reason a segment is retention-blocked: a live snapshot's
/// certification frame lives in it, and no durable `SnapshotAnchor` (Path C)
/// discharges the requirement instead. This is the fact `retention explain`
/// (bn-2ug's acceptance criterion) surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockingReason {
    pub stream_id: u64,
    pub version:   u64,
    pub frame:     CertFrame,
}

impl std::fmt::Display for BlockingReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let frame = match self.frame {
            CertFrame::V => "v",
            CertFrame::VPlus1 => "v+1",
        };
        write!(
            f,
            "blocked by live snapshot stream_id={} version={} (frame {frame})",
            self.stream_id, self.version
        )
    }
}

/// The retention verdict for one segment (doc 05 §8.2, `bn-2ug`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionDecision {
    /// No live snapshot's certification frame lives in this segment (or every
    /// one that would is discharged by a durable `SnapshotAnchor`, Path C).
    /// Safe to delete under the v1 whole-segment-deletion model.
    Deletable,
    /// At least one live snapshot's certification frame lives in this
    /// segment; deleting it would strand that snapshot's prefix proof.
    Blocked(Vec<BlockingReason>),
}

impl RetentionDecision {
    #[must_use]
    pub fn is_blocked(&self) -> bool {
        matches!(self, RetentionDecision::Blocked(_))
    }

    /// Human-readable explanation naming every blocking snapshot — the
    /// `retention explain` acceptance criterion. Empty for `Deletable`.
    #[must_use]
    pub fn explain(&self) -> Vec<String> {
        match self {
            RetentionDecision::Deletable => Vec::new(),
            RetentionDecision::Blocked(reasons) => {
                reasons.iter().map(ToString::to_string).collect()
            }
        }
    }
}

/// **The retention-blocking rule.** A segment is retention-blocked while any
/// live snapshot's certification frame — `v` or `v+1` — lives in it, unless a
/// durable `SnapshotAnchor` (Path C) already certifies that snapshot's prefix
/// without needing either frame (§7.1 Path C, §8.2 rule 2). See the module
/// docs for why this is the deliberately conservative reading of §8.2 rule 1
/// under v1's whole-segment-deletion model.
///
/// - `spans` — this segment's per-stream frame coverage (one entry per stream
///   present in the segment; a stream absent from `spans` has none of its
///   frames here).
/// - `live` — every currently-live snapshot's certification requirement. Empty
///   (or a requirement whose snapshot was superseded/dropped and thus omitted)
///   never blocks.
/// - `anchors` — every durable `SnapshotAnchor` known across the retained log
///   (§8.2 rule 2); a `(stream_id, version)` match discharges that requirement
///   via Path C regardless of which segment recorded the anchor.
#[must_use]
pub fn segment_retention_decision(
    spans: &[SegmentStreamSpan],
    live: &[LiveSnapshotRef],
    anchors: &[SnapshotAnchor],
) -> RetentionDecision {
    let mut reasons = Vec::new();
    for req in live {
        let anchored = anchors
            .iter()
            .any(|a| a.stream_id == req.stream_id && a.version == req.version);
        if anchored {
            continue; // Path C discharges this requirement; no frame needed.
        }
        let Some(span) = spans.iter().find(|s| s.stream_id == req.stream_id)
        else {
            continue; // this segment holds none of the stream's frames
        };
        if span.contains(req.version) {
            reasons.push(BlockingReason {
                stream_id: req.stream_id,
                version:   req.version,
                frame:     CertFrame::V,
            });
        }
        if let Some(v_plus_1) = req.version.checked_add(1)
            && span.contains(v_plus_1)
        {
            reasons.push(BlockingReason {
                stream_id: req.stream_id,
                version:   req.version,
                frame:     CertFrame::VPlus1,
            });
        }
    }
    if reasons.is_empty() {
        RetentionDecision::Deletable
    } else {
        RetentionDecision::Blocked(reasons)
    }
}

/// Build a segment's [`SegmentStreamSpan`] list from its sealed pointer index
/// — the wiring seam between the real on-disk structure and the pure
/// decision function above.
#[must_use]
pub fn spans_for_segment(seg: &SealedSegmentIndex) -> Vec<SegmentStreamSpan> {
    seg.stream_ids()
        .iter()
        .filter_map(|&stream_id| {
            seg.stream_range(stream_id).map(|(first_version, last_version)| {
                SegmentStreamSpan { stream_id, first_version, last_version }
            })
        })
        .collect()
}

/// Convenience wrapper over [`segment_retention_decision`]: decide whether
/// `seg` is retention-blocked, deriving its per-stream spans directly from
/// the sealed index. This is the call any future whole-segment retention
/// executor makes per candidate segment.
#[must_use]
pub fn decide_segment(
    seg: &SealedSegmentIndex,
    live: &[LiveSnapshotRef],
    anchors: &[SnapshotAnchor],
) -> RetentionDecision {
    segment_retention_decision(&spans_for_segment(seg), live, anchors)
}

/// An **active backup lease** (`bn-2ln`, doc 07 §5): while a backup is copying
/// a consistent cut, every segment id in the cut must be pinned so retention
/// (v1 whole-segment deletion) cannot delete a segment out from under the copy.
/// A lease pins the inclusive segment-id range `[protect_min_segment_id,
/// protect_max_segment_id]` — the range of segments present in the cut.
///
/// The lease's *liveness* (a TTL so a crashed backup cannot leak the pin
/// forever, doc 07 §5.2) is enforced by the caller that reads leases off disk:
/// only **active** (unexpired) leases are ever passed to the pure predicate
/// here. This type is deliberately free of time/filesystem/pid concerns so it
/// is unit-testable and reusable by any future retention executor, exactly as
/// [`segment_retention_decision`] is for the snapshot rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupLease {
    /// The backup that holds this lease (its `BACKUP_MANIFEST` / lease-file
    /// id).
    pub backup_id:              String,
    /// Lowest segment id the cut protects (inclusive).
    pub protect_min_segment_id: u64,
    /// Highest segment id the cut protects (inclusive).
    pub protect_max_segment_id: u64,
}

impl BackupLease {
    /// Whether this lease pins `segment_id` against deletion.
    #[must_use]
    pub fn pins(&self, segment_id: u64) -> bool {
        segment_id >= self.protect_min_segment_id
            && segment_id <= self.protect_max_segment_id
    }
}

/// Every active backup lease that pins `segment_id` (doc 07 §5.1). Non-empty
/// ⇒ the segment is retention-blocked by a running backup regardless of the
/// snapshot decision. The retention executor MUST NOT unlink a segment while
/// this returns any lease; `mess retention explain` surfaces each as a
/// `lease-hold` blocker.
#[must_use]
pub fn lease_holds(
    segment_id: u64,
    leases: &[BackupLease],
) -> Vec<&BackupLease> {
    leases.iter().filter(|l| l.pins(segment_id)).collect()
}

/// The full retention gate a whole-segment deletion executor calls per
/// candidate: a segment is deletable iff the snapshot decision is
/// [`RetentionDecision::Deletable`] **and** no active backup lease pins it
/// (doc 07 §5.1). This composes the `bn-2ug` snapshot rule with the `bn-2ln`
/// backup lease into the single "may I unlink this segment?" question.
#[must_use]
pub fn segment_deletable(
    seg: &SealedSegmentIndex,
    live: &[LiveSnapshotRef],
    anchors: &[SnapshotAnchor],
    leases: &[BackupLease],
) -> bool {
    !decide_segment(seg, live, anchors).is_blocked()
        && lease_holds(seg.segment_id(), leases).is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sealed::segment::{
        SealBatch, SealInput, SealStream, encode_sidecar,
    };

    fn span(stream_id: u64, first: u64, last: u64) -> SegmentStreamSpan {
        SegmentStreamSpan {
            stream_id,
            first_version: first,
            last_version: last,
        }
    }

    fn live(stream_id: u64, version: u64) -> LiveSnapshotRef {
        LiveSnapshotRef { stream_id, version }
    }

    fn anchor(stream_id: u64, version: u64) -> SnapshotAnchor {
        SnapshotAnchor { stream_id, version, chain_hash: [0u8; 32] }
    }

    /// Shape 1: a live snapshot's certification frames (v and, here, v+1) both
    /// live inside one segment -> blocked, reasons name the stream/version.
    #[test]
    fn snapshot_cert_frames_inside_segment_blocked() {
        let spans = [span(1, 0, 9)];
        let live_refs = [live(1, 5)];
        let decision = segment_retention_decision(&spans, &live_refs, &[]);
        assert!(decision.is_blocked());
        let RetentionDecision::Blocked(reasons) = &decision else {
            unreachable!()
        };
        assert!(reasons.contains(&BlockingReason {
            stream_id: 1,
            version:   5,
            frame:     CertFrame::V,
        }));
        assert!(reasons.contains(&BlockingReason {
            stream_id: 1,
            version:   5,
            frame:     CertFrame::VPlus1,
        }));
        assert!(!decision.explain().is_empty());
    }

    /// Shape 2: once the snapshot is superseded/dropped (no longer in `live`),
    /// the same segment becomes deletable.
    #[test]
    fn segment_deletable_after_snapshot_superseded_or_dropped() {
        let spans = [span(1, 0, 9)];
        let blocked = segment_retention_decision(&spans, &[live(1, 5)], &[]);
        assert!(blocked.is_blocked());

        // The snapshot at v=5 was superseded by a newer one (or dropped
        // entirely) — it no longer appears in the live set.
        let after = segment_retention_decision(&spans, &[], &[]);
        assert_eq!(after, RetentionDecision::Deletable);
        assert!(after.explain().is_empty());
    }

    /// Shape 3: frame v is the last frame of segment A, frame v+1 is the
    /// first frame of segment B (a boundary straddle) -> BOTH segments are
    /// individually blocked.
    #[test]
    fn straddling_boundary_blocks_both_segments() {
        let segment_a_spans = [span(1, 0, 5)]; // holds v=5 (segment A's last frame)
        let segment_b_spans = [span(1, 6, 10)]; // holds v+1=6 (segment B's first frame)
        let live_refs = [live(1, 5)];

        let decision_a =
            segment_retention_decision(&segment_a_spans, &live_refs, &[]);
        let decision_b =
            segment_retention_decision(&segment_b_spans, &live_refs, &[]);
        assert!(decision_a.is_blocked(), "segment A holds frame v");
        assert!(decision_b.is_blocked(), "segment B holds frame v+1");

        let RetentionDecision::Blocked(reasons_a) = &decision_a else {
            unreachable!()
        };
        assert_eq!(
            reasons_a,
            &[BlockingReason {
                stream_id: 1,
                version:   5,
                frame:     CertFrame::V,
            }]
        );
        let RetentionDecision::Blocked(reasons_b) = &decision_b else {
            unreachable!()
        };
        assert_eq!(
            reasons_b,
            &[BlockingReason {
                stream_id: 1,
                version:   5,
                frame:     CertFrame::VPlus1,
            }]
        );
    }

    /// Shape 4: a durable Path-C `SnapshotAnchor` for (stream, v) discharges
    /// the requirement even though both cert frames physically live in the
    /// segment — the anchor makes the segment deletable.
    #[test]
    fn path_c_anchor_unblocks_segment() {
        let spans = [span(1, 0, 9)];
        let live_refs = [live(1, 5)];

        let without_anchor =
            segment_retention_decision(&spans, &live_refs, &[]);
        assert!(without_anchor.is_blocked());

        let anchors = [anchor(1, 5)];
        let with_anchor =
            segment_retention_decision(&spans, &live_refs, &anchors);
        assert_eq!(with_anchor, RetentionDecision::Deletable);

        // An anchor for a *different* version doesn't discharge this one.
        let wrong_version = [anchor(1, 4)];
        assert!(
            segment_retention_decision(&spans, &live_refs, &wrong_version)
                .is_blocked()
        );
    }

    /// A live snapshot whose stream is entirely absent from the segment never
    /// blocks it (the segment has none of that stream's frames at all).
    #[test]
    fn unrelated_stream_never_blocks() {
        let spans = [span(2, 0, 9)];
        let decision = segment_retention_decision(&spans, &[live(1, 5)], &[]);
        assert_eq!(decision, RetentionDecision::Deletable);
    }

    fn seal_stream(id: u64, batches: &[(u64, u32, u64, u64)]) -> SealStream {
        SealStream {
            stream_id: id,
            batches:   batches
                .iter()
                .map(|&(v, fc, g, off)| SealBatch {
                    first_version:    v,
                    frame_count:      fc,
                    first_global_pos: g,
                    offset:           off,
                })
                .collect(),
        }
    }

    /// Wiring test: the adapter over a *real* `SealedSegmentIndex` (built the
    /// same way the sealer does) reaches the same verdict as the pure
    /// function, confirming `spans_for_segment`/`decide_segment` correctly
    /// bridge the sidecar's directory into [`SegmentStreamSpan`]s.
    #[test]
    fn decide_segment_matches_pure_function_over_real_sidecar() {
        let input = SealInput {
            segment_id:     7,
            base_pos:       1000,
            streams:        vec![
                seal_stream(10, &[(0, 3, 1000, 4096), (3, 2, 1003, 8192)]), /* versions 0..=4 */
                seal_stream(20, &[(0, 1, 1005, 12288)]), // version 0..=0
            ],
            payloads:       None,
            event_type_ids: None,
        };
        let bytes = encode_sidecar(&input);
        let seg = SealedSegmentIndex::from_bytes(bytes).unwrap();

        // Live snapshot at v=2 (mid stream 10's range) -> blocked (both v and
        // v+1 = 3 are inside 0..=4).
        let live_refs = [live(10, 2)];
        let decision = decide_segment(&seg, &live_refs, &[]);
        assert!(decision.is_blocked());
        assert_eq!(
            decision,
            segment_retention_decision(
                &spans_for_segment(&seg),
                &live_refs,
                &[]
            )
        );

        // A snapshot on a stream not present in this segment never blocks.
        let unrelated = [live(999, 0)];
        assert_eq!(
            decide_segment(&seg, &unrelated, &[]),
            RetentionDecision::Deletable
        );
    }

    fn lease(id: &str, min: u64, max: u64) -> BackupLease {
        BackupLease {
            backup_id:              id.to_string(),
            protect_min_segment_id: min,
            protect_max_segment_id: max,
        }
    }

    /// A backup lease pins exactly the inclusive segment-id range of its cut.
    #[test]
    fn lease_pins_its_range_inclusive() {
        let l = lease("b1", 3, 7);
        assert!(!l.pins(2));
        assert!(l.pins(3));
        assert!(l.pins(5));
        assert!(l.pins(7));
        assert!(!l.pins(8));
    }

    /// `lease_holds` names every active lease covering a segment; empty ⇒ not
    /// lease-blocked.
    #[test]
    fn lease_holds_reports_every_covering_lease() {
        let leases = [lease("b1", 1, 4), lease("b2", 3, 9)];
        // segment 3 is inside both cuts.
        let held = lease_holds(3, &leases);
        assert_eq!(held.len(), 2);
        // segment 6 only in b2.
        assert_eq!(
            lease_holds(6, &leases)
                .iter()
                .map(|l| l.backup_id.as_str())
                .collect::<Vec<_>>(),
            ["b2"]
        );
        // segment 12 in neither.
        assert!(lease_holds(12, &leases).is_empty());
    }

    /// `segment_deletable` composes the snapshot rule AND the lease pin: a
    /// snapshot-deletable segment becomes undeletable while a lease pins it,
    /// and deletable again once the lease is gone.
    #[test]
    fn segment_deletable_respects_active_lease() {
        let input = SealInput {
            segment_id:     5,
            base_pos:       0,
            streams:        vec![seal_stream(10, &[(0, 1, 0, 4096)])],
            payloads:       None,
            event_type_ids: None,
        };
        let seg =
            SealedSegmentIndex::from_bytes(encode_sidecar(&input)).unwrap();

        // No snapshots, no leases -> deletable.
        assert!(segment_deletable(&seg, &[], &[], &[]));
        // A lease covering segment 5 pins it -> not deletable.
        assert!(!segment_deletable(&seg, &[], &[], &[lease("b1", 4, 6)]));
        // A lease that does not cover segment 5 -> still deletable.
        assert!(segment_deletable(&seg, &[], &[], &[lease("b1", 1, 3)]));
    }
}
