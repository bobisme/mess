//! `mess retention explain <dir>` — name each sealed segment's retention
//! verdict and its blockers, via the bn-2ug decision function
//! ([`mess_index::sealed::retention`]).
//!
//! A segment is retention-blocked while any live snapshot's certification
//! frame (`v` or `v+1`, spec 05 §8.2) lives in it and no durable
//! `SnapshotAnchor` (Path C) discharges the requirement. The verdict here is
//! exactly `decide_segment`'s — this command is a read-only explainer over the
//! same function a retention executor would call.
//!
//! # Which segments are "sealed" (bn-3of, bn-30u)
//!
//! A sealed segment carries a consolidated `.seal` pack **or** a legacy `.pidx`
//! sidecar, and [`store::SegmentFile::sealed_artifact`] picks the one a reader
//! would use. Both shapes get a verdict, from the same `decide_segment` fed the
//! same live set — the sealed index is an index either way, and retention is a
//! statement about the segment, not about the file format its index happens to
//! use. A segment whose only candidate is quarantined (`*.refuted`) has no
//! sealed index at all and therefore no verdict; that is reported explicitly,
//! because a missing row must never be read as "deletable".

use std::path::Path;

use mess_index::sealed::retention::{
    BackupLease, BlockingReason, CertFrame, LiveSnapshotRef, RetentionDecision,
    decide_segment, lease_holds,
};
use mess_index::sealed::segment::SealedSegmentIndex;
use mess_log::footer_ext::{SnapshotAnchor, decode_extension};
use mess_log::runtime::real::RealFs;
use mess_log::sealer::read_trailer;
use serde_json::json;

use crate::lease;
use crate::lockprobe;
use crate::metaread;
use crate::report::{Finding, Report, Severity};
use crate::store;

/// Explain the retention verdict for every sealed segment at `dir`.
pub fn run(dir: &Path) -> Report {
    let mut report = Report::new("retention", "verdicts");
    report.set("dir", json!(dir.display().to_string()));

    let lock = lockprobe::probe(dir);
    if lock.is_held() {
        report.advise(
            "store-locked",
            "store is locked by a live writer; explaining read-only",
        );
    }

    // Live snapshots drive the decision. A §4.2 empty-prefix snapshot has no
    // certification frames, so it is filtered out before the decision.
    //
    // bn-fj34: `metaread::read` is the lock-free pack-sidecar reader and is
    // infallible, so the old `snapshots-unavailable` advisory (a locked or
    // unreadable metadata store => explain against an empty live set, flagged)
    // is gone with the store that produced it. An empty set here means the
    // store genuinely has no live snapshots.
    //
    // # The id spaces must be joined, not assumed equal
    //
    // `decide_segment` matches `LiveSnapshotRef::stream_id` against the SEALED
    // INDEX's stream ids, which are the engine's **interned dense** ids (the
    // sealer writes `AcceptedBatch::stream_id` straight into the sidecar).
    // A snapshot's own `stream_id` is the **interim FNV** hash of its stream
    // name — a different space entirely, which can only collide with a dense
    // id by accident. Feeding FNV ids to `decide_segment` therefore matches
    // nothing and reports every segment `deletable`: silently vacuous, in the
    // dangerous direction, for an explainer that fronts a destructive
    // operation.
    //
    // So resolve properly: the pack record is self-describing and carries its
    // stream NAME (ADR 0002 §1), and the log's `$registry` is the authority for
    // name -> dense id (`bn-2di`). `registryfold::fold` is the same offline,
    // lock-free fold `inspect` uses, so this join needs no writer lock and no
    // second metadata copy. A snapshot whose name the registry does not know
    // cannot have certification frames in this log's segments, so it drops out.
    let facts = metaread::read(dir);
    let folded = crate::registryfold::fold(dir);
    let mut unresolved = 0usize;
    let live: Vec<LiveSnapshotRef> = match &folded {
        Ok(state) => facts
            .snapshots
            .iter()
            .filter(|s| !s.covers_empty_prefix)
            .filter_map(|s| match state.stream_id(&s.stream) {
                Some(dense) => Some(LiveSnapshotRef {
                    stream_id: dense,
                    version:   s.version,
                }),
                None => {
                    unresolved += 1;
                    None
                }
            })
            .collect(),
        Err(reason) => {
            // Without the bijection we cannot place any snapshot in the id
            // space the decision function speaks. Explaining against an empty
            // live set would under-report blockers, so say so loudly.
            report.push_finding(Finding::new(
                Severity::Warn,
                "retention",
                "registry-unfoldable",
                format!(
                    "the $registry does not fold out of this log, so snapshot \
                     blockers cannot be resolved to stream ids and the \
                     verdicts below account for LEASES ONLY — do not delete \
                     on this report: {reason}"
                ),
            ));
            Vec::new()
        }
    };
    if unresolved > 0 {
        report.advise(
            "snapshot-stream-unregistered",
            &format!(
                "{unresolved} live snapshot(s) name a stream this log's \
                 $registry does not know; they cannot have certification \
                 frames here and were not counted as blockers"
            ),
        );
    }
    report.set(
        "live_snapshots",
        json!(
            live.iter()
                .map(|s| json!({
                    "stream_id": s.stream_id, "version": s.version
                }))
                .collect::<Vec<_>>()
        ),
    );

    // Durable Path-C anchors, gathered across every sealed segment's footer
    // extension (§3.3.2). Empty in Phase 3, but honoured when present.
    let anchors = gather_anchors(dir);
    report.set(
        "anchors",
        json!(
            anchors
                .iter()
                .map(|a| json!({
                    "stream_id": a.stream_id, "version": a.version
                }))
                .collect::<Vec<_>>()
        ),
    );

    // Active backup leases (bn-2ln, doc 07 §5) pin their cut's segment ids
    // against deletion for the backup's duration; an expired (crashed-backup)
    // lease is filtered out here so it never blocks.
    let leases = lease::active_leases(dir, lease::now_unix());
    report.set(
        "active_leases",
        json!(
            leases
                .iter()
                .map(|l| json!({
                    "backup_id": l.backup_id,
                    "protect_min_segment_id": l.protect_min_segment_id,
                    "protect_max_segment_id": l.protect_max_segment_id,
                }))
                .collect::<Vec<_>>()
        ),
    );

    let quarantined_ids: Vec<u64> = store::discover_quarantined(dir)
        .iter()
        .filter(|q| q.is_primary)
        .filter_map(|q| q.segment_id)
        .collect();

    let segments = store::discover_segments(dir);
    let mut sealed_seen = 0usize;
    for seg in &segments {
        // Retention is a whole-*sealed*-segment decision, and a segment is
        // sealed in EITHER shape (bn-3of): a consolidated `.seal` pack or a
        // legacy `.pidx`. Asking `has_pidx` skipped every segment of a
        // `seal_pack` store, so `retention explain` reported "no sealed
        // segments to evaluate" over a store full of them — an explainer that
        // fronts a destructive operation, silently vacuous.
        let artifact = seg.sealed_artifact();
        let Some(path) = seg.sealed_artifact_path() else {
            // bn-30u: a segment whose only candidate was refuted has no sealed
            // index to decide over, so it gets NO verdict — and an operator
            // reading a verdict list must not read the absence as "deletable".
            if quarantined_ids.contains(&seg.segment_id) {
                report.push_finding(
                    Finding::new(
                        Severity::Info,
                        "retention",
                        "quarantined-no-verdict",
                        format!(
                            "segment {}: its sealed candidate is quarantined \
                             (*.refuted, pending re-seal), so there is no \
                             sealed index to decide retention over and this \
                             segment gets no verdict — absence here is NOT \
                             deletability",
                            seg.segment_id
                        ),
                    )
                    .with("segment_id", seg.segment_id),
                );
            }
            continue;
        };
        sealed_seen += 1;
        // The pack is opened the way engine open opens it (`open_pack`,
        // bn-dbz): the decision reads the stream directory and per-stream
        // version spans, which the lazy attach fully materialises and verifies.
        let opened = match artifact {
            store::SealedArtifact::Pack => SealedSegmentIndex::open_pack(path),
            _ => SealedSegmentIndex::open(path),
        };
        let idx = match opened {
            Ok(i) => i,
            Err(e) => {
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "retention",
                        "sidecar-unreadable",
                        format!(
                            "segment {}: sealed index ({}) unreadable, cannot \
                             decide retention: {e}",
                            seg.segment_id,
                            path.display()
                        ),
                    )
                    .with("segment_id", seg.segment_id)
                    .with("artifact", artifact.as_str())
                    .with("path", path.display().to_string()),
                );
                continue;
            }
        };

        let decision = decide_segment(&idx, &live, &anchors);
        let (snapshot_verdict, blockers) = match &decision {
            RetentionDecision::Deletable => ("deletable", Vec::new()),
            RetentionDecision::Blocked(reasons) => ("blocked", reasons.clone()),
        };
        // A backup lease pins this segment regardless of the snapshot verdict
        // (doc 07 §5.1): the segment is blocked while any active lease covers
        // it.
        let lease_blockers: Vec<&BackupLease> =
            lease_holds(idx.segment_id(), &leases);
        let blocked = decision.is_blocked() || !lease_blockers.is_empty();
        let verdict = if blocked { "blocked" } else { snapshot_verdict };

        report.push_row(json!({
            "segment_id": idx.segment_id(),
            "artifact": artifact.as_str(),
            "base_pos": idx.base_pos(),
            "streams": idx.stream_count(),
            "event_count": idx.event_count(),
            "verdict": verdict,
            "blockers": blockers.iter().map(blocker_json).collect::<Vec<_>>(),
            "lease_blockers": lease_blockers.iter().map(|l| json!({
                "backup_id": l.backup_id,
                "protect_max_segment_id": l.protect_max_segment_id,
            })).collect::<Vec<_>>(),
        }));

        for l in &lease_blockers {
            report.push_finding(
                Finding::new(
                    Severity::Info,
                    "retention",
                    "lease-hold",
                    format!(
                        "segment {} pinned by active backup lease {}",
                        idx.segment_id(),
                        l.backup_id
                    ),
                )
                .with("segment_id", idx.segment_id())
                .with("backup_id", l.backup_id.clone()),
            );
        }

        if decision.is_blocked() {
            report.push_finding(
                Finding::new(
                    Severity::Info,
                    "retention",
                    "retention-blocked",
                    format!(
                        "segment {} blocked: {}",
                        idx.segment_id(),
                        decision.explain().join("; ")
                    ),
                )
                .with("segment_id", idx.segment_id())
                .with(
                    "blockers",
                    json!(
                        blockers.iter().map(blocker_json).collect::<Vec<_>>()
                    ),
                ),
            );
        } else if !blocked {
            report.push_finding(
                Finding::new(
                    Severity::Ok,
                    "retention",
                    "retention-deletable",
                    format!("segment {} is deletable", idx.segment_id()),
                )
                .with("segment_id", idx.segment_id()),
            );
        }
    }

    if sealed_seen == 0 {
        report.push_finding(Finding::new(
            Severity::Info,
            "retention",
            "no-sealed-segments",
            "no sealed segments to evaluate",
        ));
    }
    report
}

fn blocker_json(r: &BlockingReason) -> serde_json::Value {
    json!({
        "stream_id": r.stream_id,
        "version": r.version,
        "frame": match r.frame { CertFrame::V => "v", CertFrame::VPlus1 => "v+1" },
    })
}

/// Read every sealed segment's footer extension and collect its
/// `SnapshotAnchorList` (Path-C certificates). Best-effort: a segment with no
/// extension (Phase 3) contributes nothing.
fn gather_anchors(dir: &Path) -> Vec<SnapshotAnchor> {
    let fs = RealFs;
    let mut anchors = Vec::new();
    for seg in store::discover_segments(dir) {
        let Ok(Some(trailer)) = read_trailer(&fs, &seg.log_path) else {
            continue
        };
        if trailer.ext_len == 0 {
            continue;
        }
        let Ok(bytes) = std::fs::read(&seg.log_path) else { continue };
        let start = trailer.ext_offset as usize;
        let end = start.saturating_add(trailer.ext_len as usize);
        if end <= bytes.len() {
            let parsed = decode_extension(&bytes[start..end]);
            anchors.extend(parsed.anchors);
        }
    }
    anchors
}
