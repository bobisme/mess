//! `mess inspect <dir> [--segment N] [--stream ID]` — a read-only overview of
//! the segment chain, stream head versions, and the registry.

use std::collections::BTreeMap;
use std::path::Path;

use serde_json::json;

use crate::lockprobe::{self, LockState};
use crate::metaread;
use crate::report::{Finding, Report, Severity};
use crate::scan::scan_segment;
use crate::store;

/// Options for [`run`].
#[derive(Debug, Default, Clone)]
pub struct InspectOptions {
    /// Restrict the segment overview to this segment id.
    pub segment: Option<u64>,
    /// Restrict the stream-head overview to this stream id.
    pub stream: Option<u64>,
}

/// Inspect the store at `dir`. Read-only; works against a live-locked store.
pub fn run(dir: &Path, opts: &InspectOptions) -> Report {
    let mut report = Report::new("inspect", "segments");
    report.set("dir", json!(dir.display().to_string()));

    let lock = lockprobe::probe(dir);
    report.set("lock", lock_json(&lock));

    // Segment chain overview.
    let segments = store::discover_segments(dir);
    let mut stream_heads: BTreeMap<u64, u64> = BTreeMap::new();
    for seg in &segments {
        if let Some(want) = opts.segment
            && seg.segment_id != want
        {
            continue;
        }
        match scan_segment(seg.segment_id, &seg.log_path) {
            Ok(scan) => {
                for (&sid, &v) in &scan.recovery.stream_heads {
                    stream_heads
                        .entry(sid)
                        .and_modify(|cur| *cur = (*cur).max(v))
                        .or_insert(v);
                }
                report.push_row(json!({
                    "segment_id": seg.segment_id,
                    "epoch": scan.epoch(),
                    "base_pos": scan.base_pos(),
                    "size_bytes": scan.file_len,
                    "sealed": scan.is_sealed(),
                    "batch_count": scan.batch_count(),
                    "event_count": scan.event_count(),
                    "has_pidx": seg.has_pidx,
                    "has_pcol": seg.has_pcol,
                    "safe_offset": scan.recovery.safe_offset,
                    "next_pos": scan.recovery.next_pos,
                }));
            }
            Err(e) => {
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "segment-read",
                        "segment-io",
                        format!("segment {} unreadable: {e}", seg.segment_id),
                    )
                    .with("segment_id", seg.segment_id),
                );
            }
        }
    }

    // Stream heads and registry — from the durable metadata store when it can
    // be opened (a live writer holds fjall's lock, so degrade to the
    // recovered-from-log heads otherwise).
    let meta = metaread::read(dir);
    let mut names: BTreeMap<u64, String> = BTreeMap::new();
    match &meta {
        Ok(facts) => {
            for (id, name) in &facts.stream_names {
                names.insert(*id, name.clone());
            }
            report.set("registry_source", json!("meta"));
            report.set(
                "registry",
                json!({
                    "stream_names": facts.stream_names.iter()
                        .map(|(id, n)| json!({ "stream_id": id, "name": n }))
                        .collect::<Vec<_>>(),
                    "type_names": facts.type_names.iter()
                        .map(|(id, n)| json!({ "event_type_id": id, "name": n }))
                        .collect::<Vec<_>>(),
                    "snapshots": facts.snapshots.iter()
                        .map(|s| json!({
                            "stream_id": s.stream_id,
                            "version": s.version,
                            "fold_version": s.fold_version,
                            "covers_empty_prefix": s.covers_empty_prefix,
                        }))
                        .collect::<Vec<_>>(),
                }),
            );
        }
        Err(reason) => {
            report.set("registry_source", json!("recovered"));
            report.advise("registry-unavailable", reason);
        }
    }

    let heads: Vec<_> = stream_heads
        .iter()
        .filter(|(sid, _)| opts.stream.is_none_or(|want| **sid == want))
        .map(|(sid, v)| {
            json!({
                "stream_id": sid,
                "name": names.get(sid),
                "head_version": v,
            })
        })
        .collect();
    report.set("stream_heads", json!(heads));

    report
}

fn lock_json(lock: &LockState) -> serde_json::Value {
    match lock {
        LockState::Free => json!({ "state": "free" }),
        LockState::Held { pid } => json!({ "state": "held", "pid": pid }),
        LockState::Unknown { reason } => json!({ "state": "unknown", "reason": reason }),
    }
}
