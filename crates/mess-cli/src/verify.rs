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

use mess_index::sealed::parity::{ParityError, ParitySidecar};
use mess_index::sealed::payload::{NoDicts, SealedPayloadIndex};
use mess_index::sealed::regdelta::{RegDeltaError, RegistryDelta};
use mess_index::sealed::segment::{SealedSegmentIndex, SidecarError};
use mess_log::fold_chain::{self, Hash as ChainHash};
use mess_log::footer_ext::{SealPackIdentity, decode_extension};
use mess_log::format::{CHAIN_LEN, HEADER_LEN};
use mess_log::runtime::real::RealFs;
use mess_log::scanner::{
    AcceptedBatch, ScanStop, recover_segment_with_image, scan_image,
};
use mess_log::sealer::{SegmentCatalogEntry, read_extension};
use mess_store::registry::REGISTRY_STREAM_ID;
use serde_json::{Value, json};

use crate::lockprobe;
use crate::report::{Finding, Report, Severity};
use crate::scan::{SegmentScan, scan_segment, scan_stop_kind};
use crate::store::{self, SealedArtifact};

/// Lowercase-hex render of a 32-byte chain value, for finding messages/fields.
fn hex(h: &ChainHash) -> String {
    h.iter().map(|b| format!("{b:02x}")).collect()
}

/// Options for [`run`].
#[derive(Debug, Default, Clone)]
pub struct VerifyOptions {
    /// Run the full byte-integrity pass (payload reassembly), not just the
    /// structural scan.
    pub full:   bool,
    /// Attempt Reed-Solomon repair of damaged sealed segments from their
    /// `.par` parity sidecars (bn-2za). Reconstructs damaged byte blocks,
    /// re-verifies them against the segment's own batch CRCs (+ fold
    /// chain) BEFORE writing, keeps the damaged original as
    /// `.damaged-<ts>`, and reports exactly which blocks were repaired.
    /// Refuses if the parity is itself damaged or the damage exceeds the
    /// correction budget.
    pub repair: bool,
}

/// Verify the store at `dir`. Read-only; works against a live-locked store.
pub fn run(dir: &Path, opts: &VerifyOptions) -> Report {
    let mut report = Report::new("verify", "segments");
    report.set("dir", json!(dir.display().to_string()));
    report.set("full", json!(opts.full));
    report.set("repair", json!(opts.repair));

    let lock = lockprobe::probe(dir);
    if lock.is_held() {
        report.advise(
            "store-locked",
            "store is locked by a live writer; verifying read-only",
        );
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

        // bn-3of/bn-11g: the consolidated `.seal` pack — its own trailer hash
        // and mandatory-section checksums, then whether the segment footer
        // NAMES it and the pack on disk is that exact one. One open feeds both
        // checks.
        verify_seal_pack(&mut report, seg, &scan);

        // Sidecar integrity for sealed segments (or any segment with sidecars).
        if seg.has_pidx {
            verify_pidx(&mut report, seg.segment_id, &seg.pidx_path);
        }
        if seg.has_pcol {
            verify_pcol(&mut report, seg.segment_id, &seg.pcol_path, opts.full);
        }
        // bn-26pp/bn-w5my: the registry delta, when the segment carries one as
        // a sibling file. A pack-sealed segment carries its delta as a pack
        // section instead (bn-3h64) and so has no sibling at all — that
        // section's bytes are already inside the whole-image trailer hash and
        // the per-section checksums `verify_seal_pack`'s eager open
        // recomputes, so it is covered there and its absence here is the
        // design, not damage.
        if seg.has_reg {
            verify_reg(&mut report, seg);
        }
        if opts.full {
            verify_fold_chain(&mut report, seg.segment_id, &seg.log_path);
        }

        // bn-2za: offline Reed-Solomon repair from the `.par` parity sidecar.
        // Only attempted for a sealed segment carrying a parity sidecar.
        if opts.repair && seg.has_par && scan.is_sealed() {
            attempt_repair(&mut report, seg, &scan);
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
                    format!(
                        "sidecar {} has no matching .log segment",
                        path.display()
                    ),
                )
                .with("path", path.display().to_string()),
            );
            verify_pidx(&mut report, u64::MAX, &path);
        }
    }

    // The same sweep for consolidated packs (bn-3of): a `.seal` whose `.log`
    // is gone is the pack-shaped orphan, and the engine refutes it as
    // `orphan` at the next open.
    for path in store::discover_seal_packs(dir) {
        if segments.iter().any(|s| s.seal_path == path) {
            continue;
        }
        report.push_finding(
            Finding::new(
                Severity::Warn,
                "seal-pack",
                "orphan-seal-pack",
                format!(
                    "SealPack {} has no matching .log segment",
                    path.display()
                ),
            )
            .with("path", path.display().to_string()),
        );
        match SealedSegmentIndex::open_pack_eager(&path) {
            Ok(idx) => report.push_finding(
                Finding::new(
                    Severity::Ok,
                    "seal-pack",
                    "seal-pack-verified",
                    format!(
                        "SealPack {} verified ({} stream(s))",
                        path.display(),
                        idx.stream_count()
                    ),
                )
                .with("segment_id", idx.segment_id()),
            ),
            Err(e) => report.push_finding(
                Finding::new(
                    Severity::Error,
                    "seal-pack",
                    "seal-pack-corrupt",
                    format!("SealPack {} failed to open: {e}", path.display()),
                )
                .with("path", path.display().to_string()),
            ),
        }
    }

    // bn-30u: quarantined candidates are neither orphans nor unknown files.
    // `verify` names them because a thrown-away seal is exactly the kind of
    // evidence this command exists to surface; `mess doctor` reports their
    // re-seal state. Never an error — the raw log is authority and nothing
    // committed is at risk (D1).
    for q in store::discover_quarantined(dir) {
        if !q.is_primary {
            continue; // a derived sibling dragged along; doctor accounts for it
        }
        let mut f = Finding::new(
            Severity::Info,
            "quarantine",
            "quarantined-candidate",
            format!(
                "{} is a quarantined .{} candidate a store open refuted \
                 (bn-30u); its segment is served from the raw log and is owed \
                 a re-seal. Preserved evidence — do not delete it.",
                q.path.display(),
                q.original_ext
            ),
        )
        .with("path", q.path.display().to_string())
        .with("artifact", q.original_ext.clone())
        .with("fallback", "raw-segment-scan");
        if let Some(id) = q.segment_id {
            f = f.with("segment_id", id);
        }
        report.push_finding(f);
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
            // early, so accepted < batch_count and/or safe_offset < content
            // len.
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
            // segment's content length. The safe offset must land exactly
            // there.
            if scan.recovery.safe_offset != trailer.ext_offset {
                mismatch.push(format!(
                    "safe_offset {} != content_len {}",
                    scan.recovery.safe_offset, trailer.ext_offset
                ));
            }
            if scan.epoch() != Some(trailer.epoch) {
                mismatch.push(format!(
                    "header epoch {:?} != trailer {}",
                    scan.epoch(),
                    trailer.epoch
                ));
            }

            if mismatch.is_empty() {
                report.push_finding(
                    Finding::new(
                        Severity::Ok,
                        "segment-scan",
                        "sealed-segment-verified",
                        format!(
                            "segment {id}: sealed body matches trailer ({} \
                             batches)",
                            trailer.batch_count
                        ),
                    )
                    .with("segment_id", id),
                );
            } else {
                // The stop reason names the corruption class that halted the
                // scan.
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
                        format!(
                            "segment {id}: sealed body disagrees with \
                             trailer: {}",
                            mismatch.join("; ")
                        ),
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
                        "segment {id}: unsealed, {} committed batch(es), tail \
                         stop = {}",
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

/// The consolidated `.seal` pack (bn-3of): its own integrity, then bn-11g's
/// footer-identity binding — both off **one** open.
///
/// Two independent questions, in the order an operator needs them answered:
///
/// 1. *Are these bytes a readable pack?* `open_pack_eager` recomputes the
///    whole-image trailer hash and every section checksum, so a corrupt pack is
///    named here whether or not any footer points at it. This is the check that
///    was entirely missing before bn-1w4h: a `.seal`-sealed store had no pack
///    integrity check at all, because `verify` only ever opened `.pidx`es.
/// 2. *Is it the pack this segment was sealed with?* —
///    [`verify_seal_identity`], spec 01 §3.3.3.
///
/// The findings are mutually exclusive by construction: a pack that fails to
/// open reports `seal-pack-unreadable` when the footer names one (bn-11g's
/// finding, carrying the expected identity) and `seal-pack-corrupt` when it
/// does not, never both.
fn verify_seal_pack(
    report: &mut Report,
    seg: &store::SegmentFile,
    scan: &SegmentScan,
) {
    let id = seg.segment_id;
    // `open_pack_eager`, not `open_pack`: engine open attaches the
    // payload/event-type sections lazily (bn-dbz), but an offline verifier
    // wants the strongest check the format offers and is not latency- or
    // residency-bound. The identity is identical either way — it is the
    // header+directory hash both paths recompute.
    let opened = seg
        .has_seal
        .then(|| SealedSegmentIndex::open_pack_eager(&seg.seal_path));

    let names_pack =
        scan.trailer.as_ref().is_some_and(SegmentCatalogEntry::names_seal_pack);

    match &opened {
        Some(Ok(idx)) if idx.segment_id() != id => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "seal-pack",
                    "seal-pack-segment-mismatch",
                    format!(
                        "segment {id}: {} claims segment {} — a pack from a \
                         different segment cannot serve this one",
                        seg.seal_path.display(),
                        idx.segment_id()
                    ),
                )
                .with("segment_id", id)
                .with("pack_segment_id", idx.segment_id())
                .with("fallback", "raw-segment-scan")
                .with("path", seg.seal_path.display().to_string()),
            );
        }
        Some(Ok(idx)) => {
            report.push_finding(
                Finding::new(
                    Severity::Ok,
                    "seal-pack",
                    "seal-pack-verified",
                    format!(
                        "segment {id}: {} verified — trailer hash and every \
                         section checksum ({} stream(s))",
                        seg.seal_path.display(),
                        idx.stream_count()
                    ),
                )
                .with("segment_id", id)
                .with("streams", idx.stream_count() as u64),
            );
        }
        Some(Err(e)) if !names_pack => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "seal-pack",
                    "seal-pack-corrupt",
                    format!(
                        "segment {id}: {} failed to open: {e}. The next open \
                         refutes and quarantines it (bn-30u); the segment is \
                         served from the raw log, the only authority (D1)",
                        seg.seal_path.display()
                    ),
                )
                .with("segment_id", id)
                .with("fallback", "raw-segment-scan")
                .with("path", seg.seal_path.display().to_string()),
            );
        }
        // A named-but-unreadable pack is bn-11g's `seal-pack-unreadable`,
        // reported below with the identity the footer expected.
        Some(Err(_)) | None => {}
    }

    verify_seal_identity(report, seg, scan, opened.as_ref());
}

/// bn-11g: check the segment footer's **SealPack identity** binding
/// (spec 01 §3.3.3) — expected (what the footer names) against observed (what
/// the pack on disk hashes to), plus the reason a reader would fall back to the
/// raw segment.
///
/// This mirrors the engine's own admission decision in
/// `LogEngine::load_sealed`, deliberately: `verify` exists so an operator can
/// learn *offline, before a reopen*, that a segment is about to lose its cold
/// tier and why. Every `Error` here corresponds to a candidate the next open
/// would refuse (and quarantine); the `fallback` field names what a reader does
/// instead, which is always the same thing — read the canonical raw segment
/// bytes, the only authority (D1). No finding here means data loss.
///
/// `opened` is the pack open [`verify_seal_pack`] already performed (`None`
/// when no `.seal` exists), so the identity check re-uses those bytes instead
/// of hashing the pack a second time.
fn verify_seal_identity(
    report: &mut Report,
    seg: &store::SegmentFile,
    scan: &SegmentScan,
    opened: Option<&Result<SealedSegmentIndex, SidecarError>>,
) {
    let id = seg.segment_id;
    let Some(trailer) = &scan.trailer else {
        return; // unsealed: no footer, so no installation record to check.
    };

    if !trailer.names_seal_pack() {
        // The documented legacy compatibility policy (D-FMT-10): a footer
        // written before bn-11g, or by a sidecar-mode seal, names no pack. Only
        // worth a line when a pack IS present and is therefore being trusted on
        // coverage alone — that is the residual exposure an operator can close
        // by re-sealing the segment.
        if seg.has_seal {
            report.push_finding(
                Finding::new(
                    Severity::Warn,
                    "seal-pack",
                    "seal-pack-unnamed",
                    format!(
                        "segment {id}: footer does not name a SealPack, so {} \
                         is trusted on coverage alone (legacy footer policy, \
                         spec 01 D-FMT-10); re-seal the segment to bind it",
                        seg.seal_path.display()
                    ),
                )
                .with("segment_id", id)
                .with("path", seg.seal_path.display().to_string()),
            );
        }
        return;
    }

    // The footer names a pack. Resolve the name.
    let named = read_named_identity(seg, trailer);
    let Some(named) = named else {
        report.push_finding(
            Finding::new(
                Severity::Error,
                "seal-pack",
                "seal-pack-identity-unresolvable",
                format!(
                    "segment {id}: footer sets SEAL_PACK_IDENTITY but the \
                     identity cannot be read (bad ext_crc, missing/duplicate \
                     SealPackIdentity section, wrong segment, or an unknown \
                     identity_kind); no pack may be installed"
                ),
            )
            .with("segment_id", id)
            .with("fallback", "raw-segment-scan")
            .with("ext_offset", trailer.ext_offset)
            .with("ext_len", trailer.ext_len),
        );
        return;
    };
    let expected = named.hex();

    if !seg.has_seal {
        report.push_finding(
            Finding::new(
                Severity::Error,
                "seal-pack",
                "seal-pack-missing",
                format!(
                    "segment {id}: footer names SealPack {expected} but {} \
                     does not exist",
                    seg.seal_path.display()
                ),
            )
            .with("segment_id", id)
            .with("expected_identity", expected.clone())
            .with("observed_identity", Value::Null)
            .with("fallback", "raw-segment-scan")
            .with("path", seg.seal_path.display().to_string()),
        );
        return;
    }

    let index = match opened.expect("has_seal implies an open attempt") {
        Ok(index) => index,
        Err(e) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "seal-pack",
                    "seal-pack-unreadable",
                    format!(
                        "segment {id}: footer names SealPack {expected} but \
                         {} failed to open: {e}",
                        seg.seal_path.display()
                    ),
                )
                .with("segment_id", id)
                .with("expected_identity", expected)
                .with("observed_identity", Value::Null)
                .with("fallback", "raw-segment-scan")
                .with("path", seg.seal_path.display().to_string()),
            );
            return;
        }
    };
    let observed = index.pack_identity().map(|i| i.hex());

    if observed.as_deref() != Some(expected.as_str()) {
        report.push_finding(
            Finding::new(
                Severity::Error,
                "seal-pack",
                "seal-pack-identity-mismatch",
                format!(
                    "segment {id}: footer names SealPack {expected} but {} is \
                     {} — a stale, copied, or substituted pack; it matches \
                     the segment's coverage and is still NOT the accepted pack",
                    seg.seal_path.display(),
                    observed.clone().unwrap_or_else(|| "(none)".to_string()),
                ),
            )
            .with("segment_id", id)
            .with("expected_identity", expected)
            .with(
                "observed_identity",
                observed.map_or(Value::Null, Value::from),
            )
            .with("fallback", "raw-segment-scan")
            .with("path", seg.seal_path.display().to_string()),
        );
        return;
    }

    report.push_finding(
        Finding::new(
            Severity::Ok,
            "seal-pack",
            "seal-pack-identity-verified",
            format!("segment {id}: footer names SealPack {expected}, matched"),
        )
        .with("segment_id", id)
        .with("expected_identity", expected.clone())
        .with("observed_identity", expected),
    );
}

/// The `SealPackIdentity` a segment footer names, or `None` if it cannot be
/// resolved — the same four rejections the engine makes (spec 01 §3.3.3 reader
/// rule 2): the extension fails `ext_crc`, carries no (or more than one)
/// kind-`3` section, names another segment, or uses an unknown `identity_kind`.
pub(crate) fn read_named_identity(
    seg: &store::SegmentFile,
    trailer: &SegmentCatalogEntry,
) -> Option<SealPackIdentity> {
    let ext = read_extension(&RealFs, &seg.log_path, trailer).ok()??;
    let named = decode_extension(&ext).pack_identity?;
    (named.kind_is_known() && named.segment_id == trailer.segment_id)
        .then_some(named)
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
                            "sidecar {} claims segment {} but is named for \
                             {segment_id}",
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
                        format!(
                            "sidecar {} verified ({} streams)",
                            path.display(),
                            idx.stream_count()
                        ),
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
    // `open_eager`, not `open`: engine open attaches `.pcol`s lazily (bn-bka2)
    // and so validates structure + the block index rather than the whole-image
    // `content_crc`. An offline verifier has the opposite trade — it exists to
    // read every byte — so it keeps the strongest check the format offers.
    let parsed = match SealedPayloadIndex::open_eager(path) {
        Ok(inner) => inner,
        Err(e) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "payload",
                    "pcol-io",
                    format!(
                        "payload sidecar {} unreadable: {e}",
                        path.display()
                    ),
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
                    format!(
                        "payload sidecar {} failed CRC/parse: {e}",
                        path.display()
                    ),
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
                        format!(
                            "payload sidecar {} failed to reassemble: {e}",
                            path.display()
                        ),
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

/// Validate a registry-delta sidecar (`.reg`, bn-26pp): its own
/// magic/version/flags and content CRC, then the **layout cross-check** the
/// engine itself applies before admitting one.
///
/// # Absence is silence, corruption is a report the store survives
///
/// A `.reg` is discardable acceleration in the exact mould of the `.pcol`: it
/// copies the segment's `$registry` payload bytes so recovery folds them
/// sequentially instead of chasing one cold `pread` per registration. Absent,
/// truncated, corrupt, or refused, engine open point-reads the same batches out
/// of the `.log` and folds an identical registry (D1) — so this pass is only
/// ever reached for a `.reg` that *exists* ([`store::SegmentFile::has_reg`]),
/// and a segment with none is not a finding of any severity. A pack-sealed
/// segment never has one (bn-3h64); nor does a segment that registered no name.
///
/// What *is* worth a report is a sibling that exists and is wrong, for the same
/// reason a damaged `.pcol` is: nothing is at risk, but a permanently refused
/// accelerator is a silent, unbounded slowdown at every open, and `verify` is
/// where an operator learns that offline. The severity therefore matches
/// `pcol-corrupt` exactly — `Error`, non-zero exit — and means the same thing:
/// delete the file (or re-seal the segment) and the store is whole and fast
/// again.
///
/// # The cross-check is the engine's own, not a re-derivation
///
/// [`SealedSegmentIndex::accepts_registry_delta`] is the predicate engine
/// recovery gates the fast path on, so it is what runs here: the delta's
/// `segment_id`/`base_pos`/`event_count` must match the sidecar it would ride
/// on, and its `(first_global_pos, frame_count)` list must be **exactly** the
/// list that sidecar's pointer directory yields for the stream. The index it is
/// asked against is the one a reader would actually serve this segment from
/// ([`store::SegmentFile::sealed_artifact`] — the pack wins over a shadowed
/// `.pidx`, as in `LogEngine::load_sealed`), and it is validated independently
/// of the delta: `verify_pidx`/`verify_seal_pack` have already reported it if
/// it does not open at all, in which case there is no directory to compare
/// against and the layout step is skipped rather than blamed on the `.reg`.
fn verify_reg(report: &mut Report, seg: &store::SegmentFile) {
    let id = seg.segment_id;
    let path = &seg.reg_path;

    let delta = match RegistryDelta::open(path) {
        Ok(d) => d,
        Err(RegDeltaError::Io(e)) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "registry-delta",
                    "reg-io",
                    format!(
                        "registry delta {} unreadable: {e}",
                        path.display()
                    ),
                )
                .with("segment_id", id)
                .with("fallback", "registry-point-read")
                .with("path", path.display().to_string()),
            );
            return;
        }
        Err(e) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "registry-delta",
                    "reg-corrupt",
                    format!(
                        "registry delta {} failed CRC/parse: {e}. Open drops \
                         it and point-reads this segment's $registry batches \
                         from the log, the only authority (D1); delete it or \
                         re-seal the segment to restore the fast path",
                        path.display()
                    ),
                )
                .with("segment_id", id)
                .with("fallback", "registry-point-read")
                .with("path", path.display().to_string()),
            );
            return;
        }
    };

    // The pointer directory a reader would serve this segment from. A lazy pack
    // open is enough: only the (already checksum-verified) stream directory and
    // pointer blocks are consulted, and `verify_seal_pack` above has separately
    // recomputed the pack's whole-image hash.
    let index = match seg.sealed_artifact() {
        SealedArtifact::Pack => SealedSegmentIndex::open_pack(&seg.seal_path),
        SealedArtifact::Sidecar => SealedSegmentIndex::open(&seg.pidx_path),
        SealedArtifact::None => {
            // No sealed index at all, so no reader ever consults this delta and
            // there is no directory to cross-check it against. Its own bytes
            // are sound, which is all that can be said here.
            report.push_finding(
                Finding::new(
                    Severity::Ok,
                    "registry-delta",
                    "reg-verified",
                    format!(
                        "registry delta {} CRC verified ({} batch(es), {} \
                         record(s)); no sealed index on this segment, so the \
                         layout cross-check was not run",
                        path.display(),
                        delta.batch_count(),
                        delta.record_count()
                    ),
                )
                .with("segment_id", id)
                .with("cross_checked", false),
            );
            return;
        }
    };
    let Ok(index) = index else {
        // The serving artifact does not open — already reported as
        // `pidx-corrupt` / `seal-pack-corrupt`. Not the delta's fault, and not
        // reported twice.
        report.push_finding(
            Finding::new(
                Severity::Ok,
                "registry-delta",
                "reg-verified",
                format!(
                    "registry delta {} CRC verified ({} batch(es), {} \
                     record(s)); its segment's sealed index does not open, so \
                     the layout cross-check was not run",
                    path.display(),
                    delta.batch_count(),
                    delta.record_count()
                ),
            )
            .with("segment_id", id)
            .with("cross_checked", false),
        );
        return;
    };

    // The two reasons engine open refuses a parseable delta: it is not the
    // `$registry`'s, or the pointer directory disagrees with its batch layout.
    let refusal = if delta.stream_id() != REGISTRY_STREAM_ID {
        Some(format!(
            "it carries stream {}, not the $registry stream \
             {REGISTRY_STREAM_ID}",
            delta.stream_id()
        ))
    } else if !index.accepts_registry_delta(&delta) {
        Some(format!(
            "the segment's pointer directory does not vouch for it (delta \
             segment {} base_pos {} event_count {} with {} batch(es) vs index \
             segment {} base_pos {} event_count {})",
            delta.segment_id(),
            delta.base_pos(),
            delta.event_count(),
            delta.batch_count(),
            index.segment_id(),
            index.base_pos(),
            index.event_count(),
        ))
    } else {
        None
    };

    if let Some(reason) = refusal {
        report.push_finding(
            Finding::new(
                Severity::Error,
                "registry-delta",
                "reg-layout-mismatch",
                format!(
                    "registry delta {} parses but open would refuse it: \
                     {reason}. This segment's $registry batches are \
                     point-read from the log, the only authority (D1); delete \
                     it or re-seal the segment to restore the fast path",
                    path.display()
                ),
            )
            .with("segment_id", id)
            .with("fallback", "registry-point-read")
            .with("delta_segment_id", delta.segment_id())
            .with("delta_batches", delta.batch_count() as u64)
            .with("path", path.display().to_string()),
        );
        return;
    }

    report.push_finding(
        Finding::new(
            Severity::Ok,
            "registry-delta",
            "reg-verified",
            format!(
                "registry delta {} verified: CRC clean and {} batch(es) / {} \
                 record(s) match the segment's pointer directory",
                path.display(),
                delta.batch_count(),
                delta.record_count()
            ),
        )
        .with("segment_id", id)
        .with("cross_checked", true),
    );
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
/// 2. Each subsequent batch's stored `crypto_chain` must equal the batch **exit
///    head** folded out of the *previous* batch's frames — recomputed via
///    [`fold_chain::recompute_batch`] by walking that batch's payload bytes on
///    disk (never trusted from storage).
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
    let (recovery, image) = match recover_segment_with_image(&RealFs, log_path)
    {
        Ok(v) => v,
        Err(e) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "fold-chain",
                    "fold-chain-io",
                    format!(
                        "segment {segment_id}: failed to re-read for \
                         fold-chain verification: {e}"
                    ),
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
            let Some(stored_slice) =
                image.get(off + HEADER_LEN..off + HEADER_LEN + CHAIN_LEN)
            else {
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "fold-chain",
                        "fold-chain-io",
                        format!(
                            "segment {segment_id}: batch at {off} claims \
                             crypto_chain but its slot falls outside the \
                             recovered image"
                        ),
                    )
                    .with("segment_id", segment_id)
                    .with("stream_id", stream_id)
                    .with("batch_base_pos", b.offset),
                );
                had_error = true;
                break;
            };
            let stored: ChainHash =
                stored_slice.try_into().expect("slice is CHAIN_LEN bytes");

            if b.first_stream_version == 0 {
                let want = fold_chain::genesis(stream_id);
                if stored != want {
                    report.push_finding(
                        Finding::new(
                            Severity::Error,
                            "fold-chain",
                            "fold-chain-break",
                            format!(
                                "segment {segment_id}: stream {stream_id} \
                                 batch at {} carries a crypto_chain that does \
                                 not match the stream genesis",
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
                            "segment {segment_id}: stream {stream_id} batch \
                             at {} carries a crypto_chain that does not match \
                             the prior batch's exit head",
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
                            "segment {segment_id}: batch at {} failed to \
                             re-materialize its frames for fold-chain \
                             recompute",
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
                    "segment {segment_id}: fold chain verified \
                     ({batches_checked} chain-enabled batch(es) across \
                     {stream_count} stream(s))"
                ),
            )
            .with("segment_id", segment_id),
        );
    }
}

// ---------------------------------------------------------------------------
// bn-2za: Reed-Solomon repair from the `.par` parity sidecar
// ---------------------------------------------------------------------------

/// Attempt to repair a damaged sealed segment from its parity sidecar.
///
/// Flow (all read-only until the very last step):
/// 1. Open + CRC-validate the `.par`. A parity that fails its own content CRC
///    is *itself* damaged — reported, no repair attempted.
/// 2. Localize damaged byte blocks (per-shard CRC) and reconstruct them via RS.
///    Refuse (typed finding, originals untouched) if any group's damage exceeds
///    its parity budget, or the segment length no longer matches the sidecar.
/// 3. **Re-verify the reconstructed image against the segment's own batch CRCs
///    and fold chain BEFORE writing anything** — parity proves erasure
///    recovery, the batch CRC / chain prove the bytes are the committed bytes.
/// 4. Only on a clean re-verify: keep the damaged original as `.damaged-<ts>`
///    and install the repaired image via temp → fsync → rename. Report exactly
///    which blocks were repaired.
fn attempt_repair(
    report: &mut Report,
    seg: &crate::store::SegmentFile,
    scan: &SegmentScan,
) {
    let segment_id = seg.segment_id;

    let current = match std::fs::read(&seg.log_path) {
        Ok(b) => b,
        Err(e) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "repair",
                    "repair-io",
                    format!(
                        "segment {segment_id}: failed to read for repair: {e}"
                    ),
                )
                .with("segment_id", segment_id),
            );
            return;
        }
    };

    // 1. Parity sidecar integrity.
    let sidecar = match ParitySidecar::open(&seg.par_path) {
        Ok(Ok(s)) => s,
        Ok(Err(ParityError::ParityCorrupt)) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "repair",
                    "par-corrupt",
                    format!(
                        "segment {segment_id}: parity sidecar {} is itself \
                         damaged (content CRC mismatch); no repair attempted",
                        seg.par_path.display()
                    ),
                )
                .with("segment_id", segment_id)
                .with("path", seg.par_path.display().to_string()),
            );
            return;
        }
        Ok(Err(e)) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "repair",
                    "par-invalid",
                    format!(
                        "segment {segment_id}: parity sidecar {} failed to \
                         parse: {e}",
                        seg.par_path.display()
                    ),
                )
                .with("segment_id", segment_id),
            );
            return;
        }
        Err(e) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "repair",
                    "par-io",
                    format!(
                        "segment {segment_id}: parity sidecar unreadable: {e}"
                    ),
                )
                .with("segment_id", segment_id),
            );
            return;
        }
    };

    if sidecar.segment_id() != segment_id {
        report.push_finding(
            Finding::new(
                Severity::Error,
                "repair",
                "par-segment-mismatch",
                format!(
                    "segment {segment_id}: parity sidecar claims segment {}",
                    sidecar.segment_id()
                ),
            )
            .with("segment_id", segment_id)
            .with("par_segment_id", sidecar.segment_id()),
        );
        return;
    }

    // 2. Plan the reconstruction (no writes).
    let plan = match sidecar.plan_repair(&current) {
        Ok(p) => p,
        Err(ParityError::BeyondTolerance {
            group,
            damaged,
            tolerance,
            beyond_groups,
        }) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "repair",
                    "repair-beyond-tolerance",
                    format!(
                        "segment {segment_id}: damage exceeds parity budget \
                         (group {group} lost {damaged} shards, budget \
                         {tolerance}; {beyond_groups} group(s) beyond \
                         tolerance); refusing repair, originals untouched"
                    ),
                )
                .with("segment_id", segment_id)
                .with("group", group)
                .with("damaged", damaged as u64)
                .with("tolerance", tolerance as u64)
                .with("groups_beyond_tolerance", beyond_groups as u64),
            );
            return;
        }
        Err(ParityError::LengthMismatch { expected, actual }) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "repair",
                    "repair-length-mismatch",
                    format!(
                        "segment {segment_id}: current length {actual} != \
                         parity source length {expected}; RS block repair \
                         does not cover truncation/extension"
                    ),
                )
                .with("segment_id", segment_id)
                .with("expected_len", expected)
                .with("actual_len", actual),
            );
            return;
        }
        Err(e) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "repair",
                    "repair-failed",
                    format!("segment {segment_id}: reconstruction failed: {e}"),
                )
                .with("segment_id", segment_id),
            );
            return;
        }
    };

    if plan.repaired_blocks.is_empty() {
        report.push_finding(
            Finding::new(
                Severity::Ok,
                "repair",
                "repair-clean",
                format!(
                    "segment {segment_id}: parity localized no damaged \
                     blocks; nothing to repair"
                ),
            )
            .with("segment_id", segment_id),
        );
        return;
    }

    // 3. Prove the reconstruction restored the committed bytes BEFORE writing.
    if let Err(reasons) = reverify_repaired_image(segment_id, &plan.image, scan)
    {
        report.push_finding(
            Finding::new(
                Severity::Error,
                "repair",
                "repair-reverify-failed",
                format!(
                    "segment {segment_id}: reconstructed image failed \
                     batch-CRC/fold-chain re-verification; refusing to write \
                     (originals untouched): {reasons}"
                ),
            )
            .with("segment_id", segment_id),
        );
        return;
    }

    // 4. Install: keep the damaged original, then atomically swap in the
    //    repair.
    match install_repaired(&seg.log_path, &plan.image) {
        Ok(damaged_path) => {
            report.push_finding(
                Finding::new(
                    Severity::Ok,
                    "repair",
                    "repaired",
                    format!(
                        "segment {segment_id}: repaired {} block(s) {:?} from \
                         parity and re-verified against batch CRCs + fold \
                         chain; damaged original kept at {}",
                        plan.repaired_blocks.len(),
                        plan.repaired_blocks,
                        damaged_path.display()
                    ),
                )
                .with("segment_id", segment_id)
                .with("repaired_block_count", plan.repaired_blocks.len() as u64)
                .with("repaired_blocks", json!(plan.repaired_blocks))
                .with("damaged_original", damaged_path.display().to_string()),
            );
        }
        Err(e) => {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "repair",
                    "repair-write-failed",
                    format!(
                        "segment {segment_id}: repaired image re-verified but \
                         write failed: {e}"
                    ),
                )
                .with("segment_id", segment_id),
            );
        }
    }
}

/// Re-verify a reconstructed segment image against the segment's own committed
/// structure: the structural/byte scan (every batch CRC) must land clean at the
/// sealed content length and match the trailer's batch/event counts, and every
/// chain-enabled batch's fold linkage must recompute. Returns `Err` describing
/// the first failure. This is the gate that proves RS erasure recovery restored
/// the *committed* bytes, not merely *some* bytes that satisfy the parity.
fn reverify_repaired_image(
    segment_id: u64,
    image: &[u8],
    scan: &SegmentScan,
) -> Result<(), String> {
    let recovery = scan_image(image, None);

    // The recovery scanner is a prefix model: it accepts committed batches
    // until it can go no further. On a *sealed* segment it naturally halts
    // at the checksummed trailer (which is not a batch header), so the stop
    // reason is not `EndOfSegment` — the authoritative "clean" proof is
    // that the accepted prefix reaches the sealed content length and
    // matches the trailer's batch/event counts. A mid-body batch-CRC
    // failure would stop the scan early, short of `ext_offset`, and fail
    // this cross-check.
    let trailer = scan.trailer.as_ref().ok_or_else(|| {
        "segment is not sealed (no checksummed trailer to prove against)"
            .to_string()
    })?;
    let batches = recovery.accepted.len() as u64;
    let events: u64 =
        recovery.accepted.iter().map(|b| u64::from(b.frame_count)).sum();
    if batches != trailer.batch_count {
        return Err(format!(
            "batch_count {batches} != trailer {}",
            trailer.batch_count
        ));
    }
    if events != trailer.event_count {
        return Err(format!(
            "event_count {events} != trailer {}",
            trailer.event_count
        ));
    }
    if recovery.safe_offset != trailer.ext_offset {
        return Err(format!(
            "recovered content length {} != trailer content length {} (batch \
             CRC broke mid-body)",
            recovery.safe_offset, trailer.ext_offset
        ));
    }

    // Fold-chain linkage over every chain-enabled batch (spec 05 §3, §6).
    let mut by_stream: BTreeMap<u64, Vec<&AcceptedBatch>> = BTreeMap::new();
    for b in &recovery.accepted {
        if b.has_crypto_chain {
            by_stream.entry(b.stream_id).or_default().push(b);
        }
    }
    for (stream_id, mut batches) in by_stream {
        batches.sort_by_key(|b| b.first_stream_version);
        let mut expected_entry: Option<ChainHash> = None;
        for b in batches {
            let off = b.offset as usize;
            let stored: ChainHash = image
                .get(off + HEADER_LEN..off + HEADER_LEN + CHAIN_LEN)
                .ok_or_else(|| {
                    format!("segment {segment_id}: chain slot outside image")
                })?
                .try_into()
                .expect("slice is CHAIN_LEN bytes");

            if b.first_stream_version == 0 {
                let want = fold_chain::genesis(stream_id);
                if stored != want {
                    return Err(format!(
                        "stream {stream_id} genesis chain mismatch at offset \
                         {off}"
                    ));
                }
            } else if let Some(want) = expected_entry
                && stored != want
            {
                return Err(format!(
                    "stream {stream_id} chain break at offset {off} (want {}, \
                     found {})",
                    hex(&want),
                    hex(&stored)
                ));
            }

            let frames = b.frames(image).map_err(|_| {
                format!(
                    "stream {stream_id} batch at {off} failed to \
                     re-materialize"
                )
            })?;
            let payloads: Vec<&[u8]> = frames.map(|f| f.payload).collect();
            expected_entry = Some(fold_chain::recompute_batch(
                &stored,
                b.first_stream_version,
                payloads,
                |_| {},
            ));
        }
    }

    Ok(())
}

/// Install a re-verified repaired image: rename the damaged original aside to
/// `<log>.damaged-<unix_nanos>` (preserved for forensics), then write the
/// repaired bytes via temp → fsync → rename → dir-fsync. Returns the path the
/// damaged original was preserved at.
fn install_repaired(
    log_path: &Path,
    image: &[u8],
) -> std::io::Result<std::path::PathBuf> {
    use std::io::Write;

    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut damaged_name =
        log_path.file_name().unwrap_or_default().to_os_string();
    damaged_name.push(format!(".damaged-{ts}"));
    let damaged_path = log_path.with_file_name(damaged_name);

    // Preserve the damaged original first (atomic rename off the live name).
    std::fs::rename(log_path, &damaged_path)?;

    // Write the repaired image durably into the live name.
    let mut tmp_name = log_path.file_name().unwrap_or_default().to_os_string();
    tmp_name.push(".repair-tmp");
    let tmp = log_path.with_file_name(tmp_name);
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(image)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, log_path)?;
    if let Some(parent) = log_path.parent()
        && let Ok(dir) = std::fs::File::open(parent)
    {
        let _ = dir.sync_all();
    }
    Ok(damaged_path)
}
