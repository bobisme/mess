//! `mess doctor <dir>` — operational health checks.
//!
//! Checks: lock state, epoch sanity across the segment chain, footer/trailer
//! presence per sealed segment, sidecar presence/CRC, an fsync health probe
//! (writable + `fdatasync` on a probe file), and `fold_version` drift across
//! the snapshot set. Read-only w.r.t. committed data; reports the lock holder
//! instead of failing when a live writer holds the store.

use std::collections::BTreeSet;
use std::path::Path;

use mess_index::sealed::segment::SealedSegmentIndex;
use serde_json::json;

use crate::lockprobe::{self, LockState};
use crate::metaread;
use crate::report::{Finding, Report, Severity};
use crate::scan::scan_segment;
use crate::store;

/// Options for [`run`].
#[derive(Debug, Default, Clone)]
pub struct DoctorOptions {
    /// The `fold_version` the operator expects every live snapshot to carry.
    /// When set, any snapshot at a different fold is flagged as drift.
    pub expect_fold_version: Option<u32>,
}

/// Run the doctor checks on the store at `dir`.
pub fn run(dir: &Path, opts: &DoctorOptions) -> Report {
    let mut report = Report::new("doctor", "checks");
    report.set("dir", json!(dir.display().to_string()));

    check_lock(&mut report, dir);
    check_segments(&mut report, dir);
    check_fsync(&mut report, dir);
    check_fold_version(&mut report, dir, opts);

    report
}

fn check_lock(report: &mut Report, dir: &Path) {
    let lock = lockprobe::probe(dir);
    let (sev, kind, msg, extra) = match &lock {
        LockState::Free => (Severity::Ok, "lock-free", "store is not locked by a live writer".to_string(), json!({})),
        LockState::Held { pid } => (
            Severity::Info,
            "lock-held",
            match pid {
                Some(p) => format!("store is locked by a live writer (pid {p})"),
                None => "store is locked by a live writer (pid unknown)".to_string(),
            },
            json!({ "pid": pid }),
        ),
        LockState::Unknown { reason } => (
            Severity::Warn,
            "lock-unknown",
            format!("could not determine lock state: {reason}"),
            json!({}),
        ),
    };
    let mut f = Finding::new(sev, "lock", kind, msg);
    if let Some(obj) = extra.as_object() {
        for (k, v) in obj {
            f = f.with(k, v.clone());
        }
    }
    report.push_finding(f);
}

/// Epoch sanity + footer/trailer + sidecar presence across the segment chain.
fn check_segments(report: &mut Report, dir: &Path) {
    let segments = store::discover_segments(dir);
    if segments.is_empty() {
        report.push_finding(Finding::new(
            Severity::Warn,
            "segments",
            "no-segments",
            "no seg-*.log segments found",
        ));
        return;
    }

    let mut prev_epoch: Option<u64> = None;
    let mut prev_end_pos: Option<u64> = None;
    let mut epochs_seen: BTreeSet<u64> = BTreeSet::new();

    for seg in &segments {
        let scan = match scan_segment(seg.segment_id, &seg.log_path) {
            Ok(s) => s,
            Err(e) => {
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "segments",
                        "segment-io",
                        format!("segment {} unreadable: {e}", seg.segment_id),
                    )
                    .with("segment_id", seg.segment_id),
                );
                continue;
            }
        };

        // Header/epoch sanity.
        match scan.epoch() {
            None => report.push_finding(
                Finding::new(
                    Severity::Error,
                    "epoch",
                    "segment-header-corrupt",
                    format!("segment {}: header did not validate", seg.segment_id),
                )
                .with("segment_id", seg.segment_id),
            ),
            Some(epoch) => {
                epochs_seen.insert(epoch);
                if epoch == 0 {
                    report.push_finding(
                        Finding::new(
                            Severity::Error,
                            "epoch",
                            "epoch-zero",
                            format!("segment {}: epoch is 0", seg.segment_id),
                        )
                        .with("segment_id", seg.segment_id),
                    );
                }
                // A later segment (higher id) must not carry an OLDER epoch — a
                // resurrected prior generation (A11/§5).
                if let Some(prev) = prev_epoch
                    && epoch < prev
                {
                    report.push_finding(
                        Finding::new(
                            Severity::Error,
                            "epoch",
                            "epoch-regression",
                            format!(
                                "segment {}: epoch {epoch} < previous segment epoch {prev}",
                                seg.segment_id
                            ),
                        )
                        .with("segment_id", seg.segment_id)
                        .with("epoch", epoch)
                        .with("prev_epoch", prev),
                    );
                }
                prev_epoch = Some(epoch);
            }
        }

        // base_pos continuity across the chain (§8.1): segment N+1.base_pos ==
        // segment N.end_pos.
        if let (Some(prev_end), Some(base)) = (prev_end_pos, scan.base_pos())
            && base != prev_end
        {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "chain",
                    "base-pos-gap",
                    format!(
                        "segment {}: base_pos {base} != previous segment end_pos {prev_end}",
                        seg.segment_id
                    ),
                )
                .with("segment_id", seg.segment_id),
            );
        }

        // Footer/trailer + sidecar presence.
        match &scan.trailer {
            Some(trailer) => {
                prev_end_pos = Some(trailer.end_pos);
                if scan.epoch() != Some(trailer.epoch) {
                    report.push_finding(
                        Finding::new(
                            Severity::Error,
                            "trailer",
                            "trailer-epoch-mismatch",
                            format!(
                                "segment {}: header epoch {:?} != trailer epoch {}",
                                seg.segment_id,
                                scan.epoch(),
                                trailer.epoch
                            ),
                        )
                        .with("segment_id", seg.segment_id),
                    );
                }
                // A sealed segment SHOULD have a pointer sidecar.
                if !seg.has_pidx {
                    report.push_finding(
                        Finding::new(
                            Severity::Warn,
                            "sidecar",
                            "sidecar-missing",
                            format!("segment {}: sealed but no .pidx sidecar", seg.segment_id),
                        )
                        .with("segment_id", seg.segment_id),
                    );
                } else {
                    check_sidecar_crc(report, seg.segment_id, &seg.pidx_path);
                }
            }
            None => {
                // Unsealed head: no trailer expected. Its base_pos still seeds
                // the next segment if it is the last one.
                prev_end_pos = Some(scan.recovery.next_pos);
                report.push_finding(
                    Finding::new(
                        Severity::Ok,
                        "trailer",
                        "unsealed-head",
                        format!("segment {}: unsealed active/rolled head (no trailer)", seg.segment_id),
                    )
                    .with("segment_id", seg.segment_id),
                );
            }
        }

        // Validate any pointer sidecar that is present regardless of whether
        // the `.log` itself carries a trailer (the composed engine seals the
        // index sidecar while keeping the segment live for appends).
        if seg.has_pidx && scan.trailer.is_none() {
            check_sidecar_crc(report, seg.segment_id, &seg.pidx_path);
        }
    }

    report.set(
        "epochs_seen",
        json!(epochs_seen.iter().copied().collect::<Vec<_>>()),
    );
}

fn check_sidecar_crc(report: &mut Report, segment_id: u64, path: &Path) {
    match SealedSegmentIndex::open(path) {
        Ok(_) => report.push_finding(
            Finding::new(
                Severity::Ok,
                "sidecar",
                "sidecar-ok",
                format!("segment {segment_id}: .pidx CRC verified"),
            )
            .with("segment_id", segment_id),
        ),
        Err(e) => report.push_finding(
            Finding::new(
                Severity::Error,
                "sidecar",
                "sidecar-corrupt",
                format!("segment {segment_id}: .pidx failed to open: {e}"),
            )
            .with("segment_id", segment_id),
        ),
    }
}

/// Writable + `fdatasync` health probe: create a uniquely-named probe file,
/// write to it, `sync_data`, then remove it. A failure here means the store
/// directory cannot be durably written — a serious operational fault.
fn check_fsync(report: &mut Report, dir: &Path) {
    use std::io::Write;
    let probe = dir.join(format!(".mess-doctor-probe-{}", std::process::id()));
    let result = (|| -> std::io::Result<()> {
        let mut f = std::fs::File::create(&probe)?;
        f.write_all(b"mess doctor fsync probe\n")?;
        f.sync_data()?; // fdatasync
        Ok(())
    })();
    let _ = std::fs::remove_file(&probe);
    match result {
        Ok(()) => report.push_finding(Finding::new(
            Severity::Ok,
            "fsync",
            "fsync-ok",
            "store directory is writable and fdatasync succeeded",
        )),
        Err(e) => report.push_finding(Finding::new(
            Severity::Error,
            "fsync",
            "fsync-failed",
            format!("store directory failed the writable+fdatasync probe: {e}"),
        )),
    }
}

/// `fold_version` drift: decode each live snapshot's fold and flag when the
/// set spans more than one version (stale snapshots pending re-fold), or when
/// any differs from an operator-supplied `--expect-fold-version`.
fn check_fold_version(report: &mut Report, dir: &Path, opts: &DoctorOptions) {
    let facts = match metaread::read(dir) {
        Ok(f) => f,
        Err(reason) => {
            report.push_finding(Finding::new(
                Severity::Info,
                "fold-version",
                "registry-unavailable",
                format!("could not read snapshot metadata for fold-version check: {reason}"),
            ));
            return;
        }
    };
    let folds: BTreeSet<u32> = facts.snapshots.iter().map(|s| s.fold_version).collect();
    report.set("fold_versions", json!(folds.iter().copied().collect::<Vec<_>>()));

    if facts.snapshots.is_empty() {
        report.push_finding(Finding::new(
            Severity::Ok,
            "fold-version",
            "no-snapshots",
            "no live snapshots to check for fold drift",
        ));
        return;
    }

    if let Some(expected) = opts.expect_fold_version {
        let drifted: Vec<_> = facts
            .snapshots
            .iter()
            .filter(|s| s.fold_version != expected)
            .map(|s| json!({ "stream_id": s.stream_id, "fold_version": s.fold_version }))
            .collect();
        if drifted.is_empty() {
            report.push_finding(Finding::new(
                Severity::Ok,
                "fold-version",
                "fold-version-current",
                format!("all live snapshots carry the expected fold_version {expected}"),
            ));
        } else {
            report.push_finding(
                Finding::new(
                    Severity::Warn,
                    "fold-version",
                    "fold-version-drift",
                    format!("{} snapshot(s) do not carry fold_version {expected}", drifted.len()),
                )
                .with("expected", expected)
                .with("drifted", json!(drifted)),
            );
        }
    } else if folds.len() > 1 {
        report.push_finding(
            Finding::new(
                Severity::Warn,
                "fold-version",
                "fold-version-drift",
                format!(
                    "live snapshots span multiple fold_versions {:?}; stale snapshots pending re-fold",
                    folds.iter().copied().collect::<Vec<_>>()
                ),
            )
            .with("fold_versions", json!(folds.iter().copied().collect::<Vec<_>>())),
        );
    } else {
        report.push_finding(
            Finding::new(
                Severity::Ok,
                "fold-version",
                "fold-version-consistent",
                format!("all live snapshots carry a single fold_version {:?}", folds.iter().next()),
            ),
        );
    }
}
