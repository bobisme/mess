//! `mess verify <dir> [--full]` — the user-facing recovery scanner.
//!
//! Runs the acceptance-kernel scan ([`mess_log::scanner`]) over every segment,
//! cross-checks sealed segments against their durable trailers, and validates
//! every sealed sidecar's CRC. `--full` additionally reassembles the `.pcol`
//! payload blocks (byte-integrity of the columnar tier). Fold-chain bytes are
//! covered transitively by the batch CRC; explicit chain-linkage verification
//! (recomputing the chain across batches, catching CRC-repaired tampers) is
//! not yet implemented here — use `certificates::load_verified` for that.
//!
//! Detection contract (the acceptance test): every corruption class the
//! existing harnesses inject surfaces as an `Error` finding with a stable
//! `kind` and forces a non-zero exit.

use std::path::Path;

use mess_index::sealed::payload::{NoDicts, SealedPayloadIndex};
use mess_index::sealed::segment::SealedSegmentIndex;
use mess_log::scanner::ScanStop;
use serde_json::json;

use crate::lockprobe;
use crate::report::{Finding, Report, Severity};
use crate::scan::{SegmentScan, scan_segment, scan_stop_kind};
use crate::store;

/// Options for [`run`].
#[derive(Debug, Default, Clone)]
pub struct VerifyOptions {
    /// Run the full byte-integrity pass (payload reassembly), not just the
    /// structural scan.
    pub full: bool,
}

/// Verify the store at `dir`. Read-only; works against a live-locked store.
pub fn run(dir: &Path, opts: &VerifyOptions) -> Report {
    let mut report = Report::new("verify", "segments");
    report.set("dir", json!(dir.display().to_string()));
    report.set("full", json!(opts.full));

    let lock = lockprobe::probe(dir);
    if lock.is_held() {
        report.advise("store-locked", "store is locked by a live writer; verifying read-only");
    }

    let segments = store::discover_segments(dir);
    if segments.is_empty() {
        report.push_finding(Finding::new(
            Severity::Warn,
            "discover",
            "no-segments",
            "no seg-*.log segments found under dir",
        ));
    }

    for seg in &segments {
        let scan = match scan_segment(seg.segment_id, &seg.log_path) {
            Ok(s) => s,
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
                continue;
            }
        };
        verify_segment(&mut report, &scan);

        // Sidecar integrity for sealed segments (or any segment with sidecars).
        if seg.has_pidx {
            verify_pidx(&mut report, seg.segment_id, &seg.pidx_path);
        }
        if seg.has_pcol {
            verify_pcol(&mut report, seg.segment_id, &seg.pcol_path, opts.full);
        }
    }

    // Sealed sidecars whose `.log` is gone (or renamed) still get CRC-checked.
    for path in store::discover_sidecars(dir) {
        let already = segments.iter().any(|s| s.pidx_path == path);
        if !already {
            report.push_finding(
                Finding::new(
                    Severity::Warn,
                    "sidecar",
                    "orphan-sidecar",
                    format!("sidecar {} has no matching .log segment", path.display()),
                )
                .with("path", path.display().to_string()),
            );
            verify_pidx(&mut report, u64::MAX, &path);
        }
    }

    let clean = report.worst() < Severity::Error;
    report.set("verified", json!(clean));
    report
}

/// Structural verification of one segment: header integrity, and — for a
/// sealed segment — cross-check the recovered prefix against the trailer's
/// ground truth (batch/event counts and content length). Any shortfall means
/// a batch went missing or was rejected, i.e. a corruption in the body.
fn verify_segment(report: &mut Report, scan: &SegmentScan) {
    let id = scan.segment_id;

    // A missing/invalid segment header on a non-empty file is corruption of
    // the committed structure itself (A11).
    if scan.recovery.header.is_none() && scan.file_len > 0 {
        report.push_finding(
            Finding::new(
                Severity::Error,
                "segment-scan",
                "segment-header-corrupt",
                format!("segment {id}: segment header failed to validate"),
            )
            .with("segment_id", id),
        );
        return;
    }

    match &scan.trailer {
        Some(trailer) => {
            // Sealed: the trailer is checksummed ground truth. Compare the
            // scanned prefix against it. A body corruption makes the scan stop
            // early, so accepted < batch_count and/or safe_offset < content len.
            let mut mismatch = Vec::new();
            if scan.batch_count() as u64 != trailer.batch_count {
                mismatch.push(format!(
                    "batch_count {} != trailer {}",
                    scan.batch_count(),
                    trailer.batch_count
                ));
            }
            if scan.event_count() != trailer.event_count {
                mismatch.push(format!(
                    "event_count {} != trailer {}",
                    scan.event_count(),
                    trailer.event_count
                ));
            }
            // `ext_offset` is the first byte after the last commit marker: the
            // segment's content length. The safe offset must land exactly there.
            if scan.recovery.safe_offset != trailer.ext_offset {
                mismatch.push(format!(
                    "safe_offset {} != content_len {}",
                    scan.recovery.safe_offset, trailer.ext_offset
                ));
            }
            if scan.epoch() != Some(trailer.epoch) {
                mismatch.push(format!("header epoch {:?} != trailer {}", scan.epoch(), trailer.epoch));
            }

            if mismatch.is_empty() {
                report.push_finding(
                    Finding::new(
                        Severity::Ok,
                        "segment-scan",
                        "sealed-segment-verified",
                        format!("segment {id}: sealed body matches trailer ({} batches)", trailer.batch_count),
                    )
                    .with("segment_id", id),
                );
            } else {
                // The stop reason names the corruption class that halted the scan.
                let kind = if scan.recovery.stop == ScanStop::EndOfSegment {
                    "sealed-body-shortfall"
                } else {
                    scan_stop_kind(scan.recovery.stop)
                };
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "segment-scan",
                        kind,
                        format!("segment {id}: sealed body disagrees with trailer: {}", mismatch.join("; ")),
                    )
                    .with("segment_id", id)
                    .with("stop", scan_stop_kind(scan.recovery.stop))
                    .with("accepted_batches", scan.batch_count() as u64)
                    .with("trailer_batches", trailer.batch_count)
                    .with("safe_offset", scan.recovery.safe_offset)
                    .with("content_len", trailer.ext_offset),
                );
            }
        }
        None => {
            // Unsealed (active/rolled head): a torn tail at the write frontier
            // is normal and indistinguishable from legitimate content, so we
            // do NOT flag batch-level stops here — recovery truncates them
            // safely. The header check above is the only hard failure.
            report.push_finding(
                Finding::new(
                    Severity::Ok,
                    "segment-scan",
                    "unsealed-segment-scanned",
                    format!(
                        "segment {id}: unsealed, {} committed batch(es), tail stop = {}",
                        scan.batch_count(),
                        scan_stop_kind(scan.recovery.stop)
                    ),
                )
                .with("segment_id", id)
                .with("committed_batches", scan.batch_count() as u64),
            );
        }
    }
}

/// Validate a sealed pointer sidecar's magic/version/CRC and structural spans.
fn verify_pidx(report: &mut Report, segment_id: u64, path: &Path) {
    match SealedSegmentIndex::open(path) {
        Ok(idx) => {
            if segment_id != u64::MAX && idx.segment_id() != segment_id {
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "sidecar",
                        "sidecar-segment-mismatch",
                        format!(
                            "sidecar {} claims segment {} but is named for {segment_id}",
                            path.display(),
                            idx.segment_id()
                        ),
                    )
                    .with("segment_id", segment_id)
                    .with("sidecar_segment_id", idx.segment_id()),
                );
            } else {
                report.push_finding(
                    Finding::new(
                        Severity::Ok,
                        "sidecar",
                        "pidx-verified",
                        format!("sidecar {} verified ({} streams)", path.display(), idx.stream_count()),
                    )
                    .with("segment_id", idx.segment_id()),
                );
            }
        }
        Err(e) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "sidecar",
                    "pidx-corrupt",
                    format!("sidecar {} failed to open: {e}", path.display()),
                )
                .with("segment_id", segment_id)
                .with("path", path.display().to_string()),
            );
        }
    }
}

/// Validate a payload sidecar's CRC (always) and, under `--full`, reassemble
/// every block to prove byte-integrity of the columnar tier.
fn verify_pcol(report: &mut Report, segment_id: u64, path: &Path, full: bool) {
    let parsed = match SealedPayloadIndex::open(path) {
        Ok(inner) => inner,
        Err(e) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "payload",
                    "pcol-io",
                    format!("payload sidecar {} unreadable: {e}", path.display()),
                )
                .with("segment_id", segment_id),
            );
            return;
        }
    };
    let index = match parsed {
        Ok(idx) => idx,
        Err(e) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "payload",
                    "pcol-corrupt",
                    format!("payload sidecar {} failed CRC/parse: {e}", path.display()),
                )
                .with("segment_id", segment_id)
                .with("path", path.display().to_string()),
            );
            return;
        }
    };

    if full {
        let mut out = Vec::new();
        let mut offs = Vec::new();
        match index.reassemble_all(&NoDicts, &mut out, &mut offs) {
            Ok(()) => {
                report.push_finding(
                    Finding::new(
                        Severity::Ok,
                        "payload",
                        "pcol-reassembled",
                        format!(
                            "payload sidecar {} reassembled {} event(s)",
                            path.display(),
                            index.event_count()
                        ),
                    )
                    .with("segment_id", index.segment_id()),
                );
            }
            Err(e) => {
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "payload",
                        "pcol-reassembly-failed",
                        format!("payload sidecar {} failed to reassemble: {e}", path.display()),
                    )
                    .with("segment_id", index.segment_id())
                    .with("path", path.display().to_string()),
                );
            }
        }
    } else {
        report.push_finding(
            Finding::new(
                Severity::Ok,
                "payload",
                "pcol-verified",
                format!("payload sidecar {} CRC verified", path.display()),
            )
            .with("segment_id", index.segment_id()),
        );
    }
}
