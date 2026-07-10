//! `mess verify <dir> [--full]` — the user-facing recovery scanner.
//!
//! Runs the acceptance-kernel scan ([`mess_log::scanner`]) over every segment,
//! cross-checks sealed segments against their durable trailers, and validates
//! every sealed sidecar's CRC. `--full` additionally reassembles the `.pcol`
//! payload blocks (byte-integrity of the columnar tier) AND recomputes the
//! fold chain (spec 05 §3, §6) across every chain-enabled batch, catching a
//! CRC-repaired payload tamper the structural scan and batch CRC alone cannot
//! see (the attack [`crash_verify.rs`] in `mess-log` proves the chain, not the
//! CRC, exists to catch). Streams/batches that never opted into
//! `crypto_chain` (flags bit 0 unset) are skipped silently — the chain is
//! opt-in.
//!
//! [`crash_verify.rs`]: ../../../mess-log/tests/crash_verify.rs
//!
//! Detection contract (the acceptance test): every corruption class the
//! existing harnesses inject surfaces as an `Error` finding with a stable
//! `kind` and forces a non-zero exit.

use std::collections::BTreeMap;
use std::path::Path;

use mess_index::sealed::payload::{NoDicts, SealedPayloadIndex};
use mess_index::sealed::segment::SealedSegmentIndex;
use mess_log::fold_chain::{self, Hash as ChainHash};
use mess_log::format::{CHAIN_LEN, HEADER_LEN};
use mess_log::runtime::real::RealFs;
use mess_log::scanner::{AcceptedBatch, ScanStop, recover_segment_with_image};
use serde_json::json;

use crate::lockprobe;
use crate::report::{Finding, Report, Severity};
use crate::scan::{SegmentScan, scan_segment, scan_stop_kind};
use crate::store;

/// Lowercase-hex render of a 32-byte chain value, for finding messages/fields.
fn hex(h: &ChainHash) -> String {
    h.iter().map(|b| format!("{b:02x}")).collect()
}

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
        if opts.full {
            verify_fold_chain(&mut report, seg.segment_id, &seg.log_path);
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

/// Under `--full`, recompute the fold chain (spec 05 §3, §6) across every
/// chain-enabled batch of a segment and compare against the stored
/// `crypto_chain` (the 32 bytes at [`HEADER_LEN`], i.e. offset 72, §4.4).
/// Batches carry `stream_id` in their header, so this groups accepted batches
/// per stream (ascending `first_stream_version`) and checks two independent
/// things:
///
/// 1. The stream's very first batch (`first_stream_version == 0`) must carry
///    the stream's [`fold_chain::genesis`] as its stored `crypto_chain`.
/// 2. Each subsequent batch's stored `crypto_chain` must equal the batch
///    **exit head** folded out of the *previous* batch's frames — recomputed
///    via [`fold_chain::recompute_batch`] by walking that batch's payload
///    bytes on disk (never trusted from storage).
///
/// A payload edited at rest with a recompute-repaired `batch_crc` (in both
/// the header and the marker echo — exactly the attack `crash_verify.rs`
/// proves the fold chain, not the CRC, exists to catch) changes the
/// recomputed exit head, so it surfaces here as a `fold-chain-break` finding
/// even though the batch CRC and the structural scan both pass clean.
///
/// Scoped to one segment: this does not stitch chain continuity across a
/// segment boundary. A stream whose chain began in an earlier segment has its
/// first batch *in this segment* taken as a trusted starting point for the
/// continuity checks that follow within this segment only. Batches without
/// the `crypto_chain` flag (bit 0) are skipped — the chain is opt-in (spec
/// 05, D-FMT).
fn verify_fold_chain(report: &mut Report, segment_id: u64, log_path: &Path) {
    let (recovery, image) = match recover_segment_with_image(&RealFs, log_path) {
        Ok(v) => v,
        Err(e) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "fold-chain",
                    "fold-chain-io",
                    format!("segment {segment_id}: failed to re-read for fold-chain verification: {e}"),
                )
                .with("segment_id", segment_id),
            );
            return;
        }
    };

    let mut by_stream: BTreeMap<u64, Vec<&AcceptedBatch>> = BTreeMap::new();
    for b in &recovery.accepted {
        if b.has_crypto_chain {
            by_stream.entry(b.stream_id).or_default().push(b);
        }
    }
    if by_stream.is_empty() {
        return;
    }

    let stream_count = by_stream.len() as u64;
    let mut batches_checked = 0u64;
    let mut had_error = false;

    for (stream_id, mut batches) in by_stream {
        batches.sort_by_key(|b| b.first_stream_version);
        let mut expected_entry: Option<ChainHash> = None;

        for b in batches {
            let off = b.offset as usize;
            let Some(stored_slice) = image.get(off + HEADER_LEN..off + HEADER_LEN + CHAIN_LEN)
            else {
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "fold-chain",
                        "fold-chain-io",
                        format!(
                            "segment {segment_id}: batch at {off} claims crypto_chain but its \
                             slot falls outside the recovered image"
                        ),
                    )
                    .with("segment_id", segment_id)
                    .with("stream_id", stream_id)
                    .with("batch_base_pos", b.offset),
                );
                had_error = true;
                break;
            };
            let stored: ChainHash = stored_slice.try_into().expect("slice is CHAIN_LEN bytes");

            if b.first_stream_version == 0 {
                let want = fold_chain::genesis(stream_id);
                if stored != want {
                    report.push_finding(
                        Finding::new(
                            Severity::Error,
                            "fold-chain",
                            "fold-chain-break",
                            format!(
                                "segment {segment_id}: stream {stream_id} batch at {} carries a \
                                 crypto_chain that does not match the stream genesis",
                                b.offset
                            ),
                        )
                        .with("segment_id", segment_id)
                        .with("stream_id", stream_id)
                        .with("batch_base_pos", b.offset)
                        .with("expected", hex(&want))
                        .with("found", hex(&stored)),
                    );
                    had_error = true;
                    break;
                }
            } else if let Some(want) = expected_entry
                && stored != want
            {
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "fold-chain",
                        "fold-chain-break",
                        format!(
                            "segment {segment_id}: stream {stream_id} batch at {} carries a \
                             crypto_chain that does not match the prior batch's exit head",
                            b.offset
                        ),
                    )
                    .with("segment_id", segment_id)
                    .with("stream_id", stream_id)
                    .with("batch_base_pos", b.offset)
                    .with("expected", hex(&want))
                    .with("found", hex(&stored)),
                );
                had_error = true;
                break;
            }

            let Ok(frames) = b.frames(&image) else {
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "fold-chain",
                        "fold-chain-io",
                        format!(
                            "segment {segment_id}: batch at {} failed to re-materialize its \
                             frames for fold-chain recompute",
                            b.offset
                        ),
                    )
                    .with("segment_id", segment_id)
                    .with("stream_id", stream_id)
                    .with("batch_base_pos", b.offset),
                );
                had_error = true;
                break;
            };
            let payloads: Vec<&[u8]> = frames.map(|f| f.payload).collect();
            let exit = fold_chain::recompute_batch(
                &stored,
                b.first_stream_version,
                payloads,
                |_frame_chain| {},
            );
            expected_entry = Some(exit);
            batches_checked += 1;
        }
    }

    if !had_error {
        report.push_finding(
            Finding::new(
                Severity::Ok,
                "fold-chain",
                "fold-chain-verified",
                format!(
                    "segment {segment_id}: fold chain verified ({batches_checked} chain-enabled \
                     batch(es) across {stream_count} stream(s))"
                ),
            )
            .with("segment_id", segment_id),
        );
    }
}
