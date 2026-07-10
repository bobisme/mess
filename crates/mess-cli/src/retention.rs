//! `mess retention explain <dir>` — name each sealed segment's retention
//! verdict and its blockers, via the bn-2ug decision function
//! ([`mess_index::sealed::retention`]).
//!
//! A segment is retention-blocked while any live snapshot's certification
//! frame (`v` or `v+1`, spec 05 §8.2) lives in it and no durable
//! `SnapshotAnchor` (Path C) discharges the requirement. The verdict here is
//! exactly `decide_segment`'s — this command is a read-only explainer over the
//! same function a retention executor would call.

use std::path::Path;

use mess_index::sealed::retention::{
    BlockingReason, CertFrame, LiveSnapshotRef, RetentionDecision, decide_segment,
};
use mess_index::sealed::segment::SealedSegmentIndex;
use mess_log::footer_ext::{SnapshotAnchor, decode_extension};
use mess_log::runtime::real::RealFs;
use mess_log::sealer::read_trailer;
use serde_json::json;

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
        report.advise("store-locked", "store is locked by a live writer; explaining read-only");
    }

    // Live snapshots drive the decision. A §4.2 empty-prefix snapshot has no
    // certification frames, so it is filtered out before the decision.
    let live: Vec<LiveSnapshotRef> = match metaread::read(dir) {
        Ok(facts) => facts
            .snapshots
            .iter()
            .filter(|s| !s.covers_empty_prefix)
            .map(|s| LiveSnapshotRef { stream_id: s.stream_id, version: s.version })
            .collect(),
        Err(reason) => {
            // Without the live snapshot set we cannot compute blockers; report
            // it as an advisory and treat the live set as empty (everything
            // deletable), which is the safe-to-explain fallback but flagged.
            report.advise("snapshots-unavailable", &reason);
            Vec::new()
        }
    };
    report.set("live_snapshots", json!(live.iter().map(|s| json!({
        "stream_id": s.stream_id, "version": s.version
    })).collect::<Vec<_>>()));

    // Durable Path-C anchors, gathered across every sealed segment's footer
    // extension (§3.3.2). Empty in Phase 3, but honoured when present.
    let anchors = gather_anchors(dir);
    report.set("anchors", json!(anchors.iter().map(|a| json!({
        "stream_id": a.stream_id, "version": a.version
    })).collect::<Vec<_>>()));

    let segments = store::discover_segments(dir);
    let mut sealed_seen = 0usize;
    for seg in &segments {
        if !seg.has_pidx {
            continue; // retention is a whole-*sealed*-segment decision
        }
        sealed_seen += 1;
        let idx = match SealedSegmentIndex::open(&seg.pidx_path) {
            Ok(i) => i,
            Err(e) => {
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "retention",
                        "sidecar-unreadable",
                        format!("segment {}: sidecar unreadable, cannot decide retention: {e}", seg.segment_id),
                    )
                    .with("segment_id", seg.segment_id),
                );
                continue;
            }
        };

        let decision = decide_segment(&idx, &live, &anchors);
        let (verdict, blockers) = match &decision {
            RetentionDecision::Deletable => ("deletable", Vec::new()),
            RetentionDecision::Blocked(reasons) => ("blocked", reasons.clone()),
        };

        report.push_row(json!({
            "segment_id": idx.segment_id(),
            "base_pos": idx.base_pos(),
            "streams": idx.stream_count(),
            "event_count": idx.event_count(),
            "verdict": verdict,
            "blockers": blockers.iter().map(blocker_json).collect::<Vec<_>>(),
        }));

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
                .with("blockers", json!(blockers.iter().map(blocker_json).collect::<Vec<_>>())),
            );
        } else {
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
        let Ok(Some(trailer)) = read_trailer(&fs, &seg.log_path) else { continue };
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
