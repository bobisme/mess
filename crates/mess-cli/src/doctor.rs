//! `mess doctor <dir>` — operational health checks.
//!
//! Checks: lock state, epoch sanity across the segment chain, footer/trailer
//! presence per sealed segment, sealed-index presence/integrity, quarantined
//! candidates, an fsync health probe (writable + `fdatasync` on a probe file),
//! and `fold_version` drift across the snapshot set. Read-only w.r.t. committed
//! data; reports the lock holder instead of failing when a live writer holds
//! the store.
//!
//! # What "sealed" looks like on disk (bn-3of, bn-30u)
//!
//! A sealed segment carries a consolidated `.seal` pack OR the legacy
//! `.pidx`+`.pcol`+`.filter` trio; [`store::SegmentFile::sealed_artifact`]
//! answers which, using the engine's own dual-read preference. A pack-sealed
//! segment is **healthy** and is reported as such — before bn-1w4h this check
//! knew only `.pidx` and so warned "sealed but no .pidx sidecar" for every
//! healthy segment of a `seal_pack` store, i.e. it misdiagnosed the entire
//! store. A refuted candidate quarantined as `*.refuted` (bn-30u) is reported
//! as quarantined-with-state, never deleted or repaired: it is preserved
//! evidence and the durable trigger for the segment's re-seal.
//!
//! bn-3m62: that marker is also what tells `sidecar-missing` whether a repair
//! is actually coming. The re-seal trigger is the durable slot, never the
//! absence of an artifact, so a sealed segment whose index was *deleted* is
//! owed nothing and this check must not say otherwise — see
//! [`sidecar_missing_finding`]. `mess verify` splits the narrower
//! footer-names-an-absent-pack case on the same bit
//! (`seal-pack-reseal-pending` vs `seal-pack-missing`); the two tools describe
//! one store with one vocabulary.
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

use crate::authority;
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
    // bn-11ba: the authority view — canonical vs discardable, per-segment
    // accelerator inventory, and the fallback the engine would take. Runs
    // after the segment walk so its summary reads against the same on-disk
    // state the per-segment findings above describe.
    authority::check(&mut report, dir);
    check_quarantine(&mut report, dir);
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
    // bn-3m62: the segments whose durable re-seal request is already on disk.
    // `sidecar-missing` below used to promise every artifact-less sealed
    // segment a re-seal; only these actually get one.
    let owed_reseal: std::collections::BTreeSet<u64> =
        store::discover_quarantined(dir)
            .iter()
            .filter_map(|q| q.segment_id)
            .collect();
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
                // A sealed segment SHOULD have a sealed index — in EITHER
                // shape (bn-3of): a consolidated `.seal` pack or the legacy
                // `.pidx`. Missing means missing both.
                match seg.sealed_artifact() {
                    store::SealedArtifact::None => {
                        report.push_finding(sidecar_missing_finding(
                            seg.segment_id,
                            owed_reseal.contains(&seg.segment_id),
                        ))
                    }
                    artifact => check_sealed_artifact(report, seg, artifact),
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

        // Validate any sealed index that is present regardless of whether the
        // `.log` itself carries a trailer (the composed engine seals the index
        // while keeping the segment live for appends — `seal_active` writes a
        // `.seal`/`.pidx` over a footerless head).
        if scan.trailer.is_none() && seg.sealed_artifact().is_present() {
            check_sealed_artifact(report, seg, seg.sealed_artifact());
        }
    }

    report.set(
        "epochs_seen",
        json!(epochs_seen.iter().copied().collect::<Vec<_>>()),
    );
}

/// bn-3m62: a **sealed** segment carrying no sealed index at all — and whether
/// anything is actually going to fix that.
///
/// This finding used to tell every such operator that the segment "is owed a
/// re-seal", unconditionally. That is true only when the segment's quarantine
/// slot under `sealed/` is occupied: the re-seal trigger is the durable
/// `*.refuted` marker, never the absence of an artifact, so a pack that was
/// simply *deleted* leaves nothing to trigger on and the engine never re-queues
/// the segment. bn-3qh0 pinned that as
/// `never_reseals_a_deleted_pack_on_its_own` — three consecutive reopens,
/// nothing refuted, nothing pending, the pack never returns. Promising a repair
/// that will not happen is the one thing an operational report must not do.
///
/// Both states stay `Warn` here — reads are correct off the log either way, and
/// `doctor`'s severity scale is about operational health, not exit codes. The
/// split lives in the message, the `state` field, and the remedy. `mess verify`
/// makes the same split on the same bit and adds the exit-code judgement
/// (`seal-pack-reseal-pending` Warn vs `seal-pack-missing` Error) for the
/// narrower case where the footer *names* the absent pack.
fn sidecar_missing_finding(segment_id: u64, owed_reseal: bool) -> Finding {
    let f = Finding::new(
        Severity::Warn,
        "sidecar",
        "sidecar-missing",
        format!(
            "segment {segment_id}: sealed but no sealed index artifact \
             (neither a .seal pack nor a .pidx sidecar); the segment is \
             served from the raw log (D1) and {}",
            if owed_reseal {
                "is owed a re-seal — its quarantine slot under sealed/ is \
                 occupied, so the next engine open rebuilds the index"
            } else {
                "nothing on disk requests a re-seal, so the engine will not \
                 rebuild the index on its own; run mess rebuild-index"
            },
        ),
    )
    .with("segment_id", segment_id)
    .with("state", if owed_reseal { "pending-reseal" } else { "lost" });
    if owed_reseal {
        f.with("converges", "next-open")
    } else {
        f.with("remedy", "mess rebuild-index")
    }
}

/// Integrity of the segment's load-bearing sealed index, whichever shape it is
/// (bn-3of).
///
/// A pack-sealed segment is **healthy**: `doctor` says so with a `seal-pack-ok`
/// finding rather than the historical "sealed but no .pidx sidecar" warning,
/// which on a `seal_pack` store fired for every single healthy segment. Only a
/// segment with neither artifact is missing one.
///
/// The pack is opened with `open_pack` — the same lazy attach engine open uses
/// (bn-dbz) — so `doctor` verifies exactly what a reopen would: header, section
/// directory, trailer hash, and every mandatory section's checksum. `verify` is
/// the surface that additionally pulls the optional sections and re-hashes the
/// whole image (`open_pack_eager`); `doctor` is the operational health check,
/// and its job is to answer "would the next open install this?".
fn check_sealed_artifact(
    report: &mut Report,
    seg: &store::SegmentFile,
    artifact: store::SealedArtifact,
) {
    let segment_id = seg.segment_id;
    if seg.pidx_shadowed_by_pack() {
        // bn-3of dual read: the pack wins and the `.pidx` is never even opened
        // for judgement. Worth one line, because "this segment has a .pidx" is
        // a fact an operator will otherwise misread as the serving artifact.
        report.push_finding(
            Finding::new(
                Severity::Info,
                "sidecar",
                "pidx-shadowed-by-pack",
                format!(
                    "segment {segment_id}: both a .seal pack and a legacy \
                     .pidx are present; the pack serves and the sidecar is \
                     inert (bn-3of dual read)"
                ),
            )
            .with("segment_id", segment_id)
            .with("path", seg.pidx_path.display().to_string()),
        );
    }
    match artifact {
        store::SealedArtifact::Pack => {
            match SealedSegmentIndex::open_pack(&seg.seal_path) {
                Ok(idx) if idx.segment_id() != segment_id => report
                    .push_finding(
                        Finding::new(
                            Severity::Error,
                            "sidecar",
                            "seal-pack-segment-mismatch",
                            format!(
                                "segment {segment_id}: .seal pack claims \
                                 segment {}",
                                idx.segment_id()
                            ),
                        )
                        .with("segment_id", segment_id)
                        .with("pack_segment_id", idx.segment_id()),
                    ),
                Ok(idx) => report.push_finding(
                    Finding::new(
                        Severity::Ok,
                        "sidecar",
                        "seal-pack-ok",
                        format!(
                            "segment {segment_id}: .seal pack verified ({} \
                             stream(s))",
                            idx.stream_count()
                        ),
                    )
                    .with("segment_id", segment_id)
                    .with("streams", idx.stream_count() as u64),
                ),
                Err(e) => report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "sidecar",
                        "seal-pack-corrupt",
                        format!(
                            "segment {segment_id}: .seal pack failed to open: \
                             {e}. The next open will refute and quarantine \
                             it; the segment is served from the raw log (D1)"
                        ),
                    )
                    .with("segment_id", segment_id)
                    .with("path", seg.seal_path.display().to_string()),
                ),
            }
        }
        store::SealedArtifact::Sidecar => {
            check_sidecar_crc(report, segment_id, &seg.pidx_path);
        }
        store::SealedArtifact::None => {}
    }
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

/// bn-30u quarantine surface: every `sealed/*.refuted` artifact, what it was,
/// and whether its segment has since regained a sealed index.
///
/// **Diagnosis only — `doctor` never touches these files.** A quarantine marker
/// is the preserved evidence of a seal that was thrown away *and* the durable
/// trigger that re-queues the segment for a fresh seal at the next open; the
/// engine converges the store, not the CLI. Deleting one here would destroy the
/// only witness of a possible sealer bug and cancel the pending re-seal.
///
/// The refutation *reason* is not durable — it is logged loudly by the open
/// that made it (`SealedCandidateHealth::refute`) — so this check reports the
/// one reason class it can honestly re-derive offline: whether the quarantined
/// bytes parse at all. Bytes that do not parse were refuted as `unparsable`;
/// bytes that do parse were refuted for a coverage or identity reason
/// (`coverage-unproven`, `identity-mismatch`, `pack-identity-mismatch`,
/// `pack-identity-unresolvable`, `orphan`), which `mess verify` names exactly.
fn check_quarantine(report: &mut Report, dir: &Path) {
    let quarantined = store::discover_quarantined(dir);
    report.set("quarantined", json!(quarantined.len() as u64));
    if quarantined.is_empty() {
        report.push_finding(Finding::new(
            Severity::Ok,
            "quarantine",
            "no-quarantined-artifacts",
            "no refuted sealed-index candidates are quarantined under sealed/",
        ));
        return;
    }

    // A segment whose quarantined candidate has NOT been replaced is owed a
    // re-seal; one that has been is a marker left behind as evidence.
    let segments = store::discover_segments(dir);
    for q in &quarantined {
        if !q.is_primary {
            // A derived sibling (`.filter`/`.pcol`/`.reg`) dragged along by a
            // `.pidx` quarantine. Reported at info so the file is accounted
            // for, never as a problem of its own.
            report.push_finding(
                Finding::new(
                    Severity::Info,
                    "quarantine",
                    "quarantined-sibling",
                    format!(
                        "{}: a derived .{} sibling moved with its quarantined \
                         .pidx (bn-30u)",
                        q.path.display(),
                        q.original_ext
                    ),
                )
                .with("path", q.path.display().to_string())
                .with("artifact", q.original_ext.clone()),
            );
            continue;
        }
        let resealed = q.segment_id.is_some_and(|id| {
            segments
                .iter()
                .any(|s| s.segment_id == id && s.sealed_artifact().is_present())
        });
        let parses = quarantined_parses(q);
        let state = if resealed { "resealed" } else { "pending-reseal" };
        let mut f = Finding::new(
            Severity::Warn,
            "quarantine",
            "quarantined-candidate",
            format!(
                "segment {}: a refuted .{} candidate is quarantined at {} \
                 ({}). It is preserved evidence and the durable re-seal \
                 trigger — do not delete it; the segment is served from the \
                 raw log (D1) and {}",
                q.segment_id
                    .map_or_else(|| "?".to_string(), |id| id.to_string()),
                q.original_ext,
                q.path.display(),
                if parses {
                    "the bytes still parse, so it was refuted on coverage or \
                     identity — `mess verify` names which"
                } else {
                    "the bytes do not parse (refuted as unparsable)"
                },
                if resealed {
                    "has since regained a sealed index"
                } else {
                    "is queued for a fresh seal at the next open"
                },
            ),
        )
        .with("path", q.path.display().to_string())
        .with("artifact", q.original_ext.clone())
        .with("state", state)
        .with("parses", parses);
        if let Some(id) = q.segment_id {
            f = f.with("segment_id", id);
        }
        report.push_finding(f);
    }
}

/// Whether a quarantined candidate's bytes still parse — the one refutation
/// class re-derivable from the file alone. Read-only: opens the `*.refuted`
/// path itself, never the candidate name it used to hold.
fn quarantined_parses(q: &store::QuarantinedArtifact) -> bool {
    match q.original_ext.as_str() {
        "seal" => SealedSegmentIndex::open_pack(&q.path).is_ok(),
        "pidx" => SealedSegmentIndex::open(&q.path).is_ok(),
        _ => false,
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
