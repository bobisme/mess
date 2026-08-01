//! `mess doctor <dir>` — operational health checks.
//!
//! Checks: lock state, epoch sanity across the segment chain, footer/trailer
//! presence per sealed segment, sidecar presence/CRC, an fsync health probe
//! (writable + `fdatasync` on a probe file), and `fold_version` drift across
//! the snapshot set. Read-only w.r.t. committed data; reports the lock holder
//! instead of failing when a live writer holds the store.
//!
//! # What the fold-version check sees
//!
//! The persisted snapshots this check inspects are the app's, written by a
//! [`PackSnapshotBackend`](mess_store::PackSnapshotBackend) into the snapshot
//! sidecar (`<dir>/.snapshots.packs`). [`metaread::read`] reads that
//! sidecar read-only, so the check is
//! non-vacuous on any store that actually persists snapshots (an app opts into
//! that with `mess_store::SnapshotPolicy`). A store that persists none — e.g.
//! one whose warm path is a pure in-memory cache — correctly reports the
//! `no-snapshots` OK finding: there is genuinely nothing to drift.
//!
//! `doctor` never writes to the sidecar and never repairs it: the reader takes
//! no lock and creates, truncates, renames, and deletes nothing. A head it
//! cannot resolve is simply not reported (a miss, which the app answers by
//! replaying) — it is never repaired and never invented.
//!
//! # bn-fj34: every check is now live-writer safe
//!
//! `doctor` used to carry one exception. The `fold_version` check had a second,
//! *legacy* source — a `<dir>/meta` key-value store whose open took an
//! exclusive directory lock with no read-only mode — so against a live writer
//! that one check could not run and degraded to an `info`-severity
//! `meta-store-locked` finding (bn-ve0). `bn-3l8n` moved the app sidecar off
//! that engine and `bn-fj34` deleted it, so [`metaread::read`] is now
//! lock-free and infallible. Every check here reads off the segment files, the
//! pack sidecar, or the D9 store-lock probe; none of them needs exclusive
//! access, and the degraded finding is gone rather than merely unreachable.

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

    // Probed once and shared: `check_registry` needs the engine (hence the
    // store lock) and reuses this state rather than probing again.
    let lock = lockprobe::probe(dir);
    check_lock(&mut report, &lock);
    check_segments(&mut report, dir);
    check_fsync(&mut report, dir);
    check_fold_version(&mut report, dir, opts);
    check_registry(&mut report, dir, &lock);

    report
}

/// bn-2di: fold the `$registry` out of the log and report what it names.
///
/// The registry IS the log now — there is no second copy of the `id -> name`
/// bijection anywhere in the store, so there is nothing to diff it against.
/// What this check buys is that the fold is *exercised* on demand, off the open
/// path: it re-derives the whole registry from the durable bytes, enforcing
/// every REG-rule (`RegistryState::apply` — REG14 double registration, REG16
/// name rebinding, REG12 dangling references), and fails loudly if the log's
/// own registry does not fold. A store whose registry does not fold is a store
/// whose ids have no meaning, so an operator wants to hear about it from
/// `doctor` rather than from a failed open.
///
/// Needs the engine (hence the D9 store lock), so against a live writer it
/// degrades to an info finding rather than failing the command. bn-fj34: this
/// is now the *only* check that degrades under a live writer — the
/// fold-version check reads the lock-free pack sidecar and no longer does.
fn check_registry(report: &mut Report, dir: &Path, lock: &LockState) {
    if matches!(lock, LockState::Held { .. }) {
        report.push_finding(Finding::new(
            Severity::Info,
            "registry",
            "registry-store-locked",
            "a live writer holds this store, so the $registry fold cannot \
             run. Run against a stopped writer (or a `mess backup` copy) for \
             the full check.",
        ));
        return;
    }
    // Run on a dedicated thread with its own runtime. `doctor::run` is a
    // synchronous API that callers legitimately invoke from *inside* an async
    // context (its own test suite does), and `block_on` from within a runtime
    // panics — so we cannot simply build one here.
    let dir = dir.to_path_buf();
    let findings = std::thread::spawn(move || {
        let mut sub = Report::new("doctor", "checks");
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                sub.push_finding(Finding::new(
                    Severity::Warn,
                    "registry",
                    "registry-runtime",
                    format!("could not build a runtime for the check: {e}"),
                ));
                return sub;
            }
        };
        let engine = match mess_store::LogEngine::open(&dir) {
            Ok(e) => e,
            Err(e) => {
                let msg = e.to_string();
                let locked = msg.contains("Locked") || msg.contains("lock");
                sub.push_finding(Finding::new(
                    if locked { Severity::Info } else { Severity::Warn },
                    "registry",
                    if locked {
                        "registry-store-locked"
                    } else {
                        "registry-open"
                    },
                    if locked {
                        "a live writer holds this store, so the $registry fold \
                         cannot run."
                            .to_string()
                    } else {
                        format!("could not open the store for the check: {e}")
                    },
                ));
                return sub;
            }
        };
        match rt.block_on(engine.fold_registry()) {
            Ok(state) => sub.push_finding(
                Finding::new(
                    Severity::Ok,
                    "registry",
                    "registry-folds",
                    format!(
                        "the $registry folds cleanly from the log: {} stream \
                         name(s), {} event-type name(s). The log is the sole \
                         source of truth for the id->name bijection — there \
                         is no second copy to drift from.",
                        state.stream_high_water_mark(),
                        state.event_type_high_water_mark()
                    ),
                )
                .with("streams", state.stream_high_water_mark())
                .with("event_types", state.event_type_high_water_mark()),
            ),
            Err(e) => sub.push_finding(Finding::new(
                Severity::Error,
                "registry",
                "registry-fold-failed",
                format!(
                    "the log's $registry does NOT fold: {e}. Every interned \
                     id in this store is uninterpretable until this is \
                     resolved."
                ),
            )),
        }
        sub
    })
    .join();

    match findings {
        Ok(sub) => {
            for f in sub.findings {
                report.push_finding(f);
            }
        }
        Err(_) => report.push_finding(Finding::new(
            Severity::Warn,
            "registry",
            "registry-panic",
            "the registry check panicked",
        )),
    }
}

fn check_lock(report: &mut Report, lock: &LockState) {
    let (sev, kind, msg, extra) = match lock {
        LockState::Free => (
            Severity::Ok,
            "lock-free",
            "store is not locked by a live writer".to_string(),
            json!({}),
        ),
        LockState::Held { pid } => (
            Severity::Info,
            "lock-held",
            match pid {
                Some(p) => {
                    format!("store is locked by a live writer (pid {p})")
                }
                None => {
                    "store is locked by a live writer (pid unknown)".to_string()
                }
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
                    format!(
                        "segment {}: header did not validate",
                        seg.segment_id
                    ),
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
                                "segment {}: epoch {epoch} < previous segment \
                                 epoch {prev}",
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
                        "segment {}: base_pos {base} != previous segment \
                         end_pos {prev_end}",
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
                                "segment {}: header epoch {:?} != trailer \
                                 epoch {}",
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
                            format!(
                                "segment {}: sealed but no .pidx sidecar",
                                seg.segment_id
                            ),
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
                        format!(
                            "segment {}: unsealed active/rolled head (no \
                             trailer)",
                            seg.segment_id
                        ),
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
///
/// bn-fj34: [`metaread::read`] is infallible and lock-free, so this check runs
/// identically against a live store and a stopped one. It has no degraded
/// path — an empty live set is the honest `no-snapshots` OK finding, never a
/// "could not read" advisory.
fn check_fold_version(report: &mut Report, dir: &Path, opts: &DoctorOptions) {
    // Set before the read so the JSON envelope's top-level keys are the same
    // whatever the sidecar holds (mirrors bn-1yz's fix for `inspect`'s
    // `registry`): `fold_versions` is always present, just empty when there
    // are no snapshots.
    report.set("fold_versions", json!(Vec::<u32>::new()));

    let facts = metaread::read(dir);
    let folds: BTreeSet<u32> =
        facts.snapshots.iter().map(|s| s.fold_version).collect();
    report
        .set("fold_versions", json!(folds.iter().copied().collect::<Vec<_>>()));

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
                format!(
                    "all live snapshots carry the expected fold_version \
                     {expected}"
                ),
            ));
        } else {
            report.push_finding(
                Finding::new(
                    Severity::Warn,
                    "fold-version",
                    "fold-version-drift",
                    format!(
                        "{} snapshot(s) do not carry fold_version {expected}",
                        drifted.len()
                    ),
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
                    "live snapshots span multiple fold_versions {:?}; stale \
                     snapshots pending re-fold",
                    folds.iter().copied().collect::<Vec<_>>()
                ),
            )
            .with(
                "fold_versions",
                json!(folds.iter().copied().collect::<Vec<_>>()),
            ),
        );
    } else {
        report.push_finding(Finding::new(
            Severity::Ok,
            "fold-version",
            "fold-version-consistent",
            format!(
                "all live snapshots carry a single fold_version {:?}",
                folds.iter().next()
            ),
        ));
    }
}
