//! `mess rebuild-index <dir>` — I5 made executable.
//!
//! From bare `.log` segments, restore each segment's sealed index. There are
//! two shapes and this command has a path for each:
//!
//! - a **legacy loose sidecar** (`.pidx`) is rebuilt in place, offline. The
//!   bytes come from the exact [`encode_sidecar`] the sealer uses, so a rebuild
//!   from an intact segment is **byte-equal** to the original sidecar (the
//!   acceptance test). The `.pidx` is the only authoritative artifact rebuilt
//!   here; the payload (`.pcol`) and filter (`.filter`) siblings are advisory
//!   and left to the sealer.
//! - a **consolidated `.seal` pack** is restored by *requesting a re-seal*
//!   rather than by encoding one offline — see below.
//!
//! # bn-1w4h: why a pack-sealed segment never gets a rebuilt `.pidx`
//!
//! A `.pidx` written for a pack-sealed segment is not merely redundant:
//! bn-3of's dual read prefers the pack and never opens the sidecar, and
//! bn-11g's footer identity makes a legacy sidecar offered in a named pack's
//! place a `PackIdentityMismatch` refutation — the next open would quarantine
//! the very file this command wrote. That refusal is still in force and is
//! still correct.
//!
//! **bn-ccx1 turned that refusal into a capability regression.** With
//! `EngineOptions::seal_pack` ON by default, every segment a current engine
//! seals is pack-sealed, so `rebuild-index` became a no-op on freshly written
//! stores: it reported `pack-sealed-segment-skipped` for each and wrote
//! nothing. bn-1yor's default-on matrix called it the one real capability loss
//! of the flip and admitted it.
//!
//! # bn-3qh0: the pack path, and why it is a re-seal request
//!
//! The regression is closed here, and deliberately **not** with an offline pack
//! encoder. Rebuilding a pack offline would mean duplicating the pack encoder
//! and the footer-extension encoder outside the engine, keeping both in
//! lockstep with `PACK_FORMAT_VERSION` forever, and — because bn-11g binds the
//! segment footer to the exact pack that sealed it — rewriting the footer,
//! which lives inside the `.log`: the sole authority, mutated by an offline
//! tool. And it would not even escape the alternative. A crash between writing
//! the new pack and rewriting the footer leaves a pack the footer does not
//! name, which is exactly `PackIdentityMismatch`, which converges only through
//! bn-30u's quarantine-and-re-seal. The offline encoder needs the re-seal
//! machinery for its own crash windows *and* adds a second copy of the most
//! safety-critical byte surface in the project.
//!
//! So the pack path uses that machinery directly:
//! [`mess_store::withdraw_sealed_index`] takes the segment's sealed index out
//! of the candidate namespace and leaves the durable re-seal request in its
//! quarantine slot. The engine's own roll-sealer rebuilds the pack from the raw
//! log at the next open and re-finalizes the footer to name it — the same code
//! path, the same encoder, the same identity discipline as any other seal. The
//! rebuilt pack is by construction what a fresh seal produces, which is
//! strictly more than an offline encoder could promise.
//!
//! The gap this closes is precise, and it is the *absent* artifact. A pack that
//! is corrupt, stale, or substituted is a candidate the engine judges, refutes,
//! quarantines and re-seals on its own. A pack that was **deleted** — an
//! operator cleaned `sealed/`, a restore dropped it, a disk lost it — is no
//! candidate at all: nothing is refuted, no marker is written, `owed_reseal`
//! stays empty and the segment is never re-queued. It reads correctly from the
//! log forever and never regains its cold tier. That is the segment
//! `rebuild-index` exists for, and the one the engine cannot fix by itself.
//!
//! ## What the operator sees
//!
//! - A **healthy** pack (it opens, and it is the pack the footer names) is
//!   still refused, unchanged: `pack-sealed-segment-skipped`, nothing written.
//!   `--force` withdraws it anyway, for an operator who wants it rebuilt from
//!   the log regardless.
//! - A **broken** pack path — named but absent, unreadable, identity mismatch,
//!   identity unresolvable — is repaired without `--force`. That is what a
//!   repair tool is for, and the engine would quarantine three of those four
//!   itself at the next open in any case.
//! - By default the withdrawal is all that happens offline and the command says
//!   so (`reseal-pending`): **the rebuild completes at the next engine open.**
//!   `--reseal` opens the store once and drops it, which drains the sealer, so
//!   the command exits with a real footer-bound pack back on disk.
//!
//! Between the withdrawal and the re-seal the segment is served from the raw
//! log — the only authority — and its footer goes on naming a pack that is not
//! there. That is a miss, not an error: with no candidate to judge, spec 01
//! §3.3.3's fail-closed identity rule has nothing to fail on. (`mess verify`
//! does report `seal-pack-missing` in that window. It is the same thing it
//! reported before this command ran, and `--reseal` closes the window inside
//! the one invocation.)
//!
//! # bn-fj34: `--meta` is gone
//!
//! This command used to take a `--meta` flag that additionally opened a
//! `<dir>/meta` key-value store and wrote a derived `stream_heads` table into
//! it. Nothing ever read that table: the engine's per-stream heads live in its
//! resident `Book`, rebuilt on open from batch headers and sealed directory
//! summaries, and it ignored `<dir>/meta` entirely even when the directory was
//! deleted wholesale (`mess-store`'s `engine_name_durability` suite proves
//! exactly that). Worse, the generic commit-group write advanced *snapshot* and
//! *dedupe* high-water marks it had written nothing to. The flag created a
//! store the product does not have, so it was removed with the storage engine
//! behind it rather than kept as a no-op.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use mess_index::sealed::SealedSegmentIndex;
use mess_index::sealed::segment::{
    SealBatch, SealInput, SealStream, encode_sidecar,
};
use mess_log::sealer::SegmentCatalogEntry;
use serde_json::json;

use crate::lockprobe;
use crate::report::{Finding, Report, Severity};
use crate::scan::{SegmentScan, scan_segment};
use crate::store;

/// Options for [`run`].
#[derive(Debug, Default, Clone)]
pub struct RebuildOptions {
    /// Show what would be written without writing anything.
    pub dry_run: bool,
    /// Withdraw a **healthy** pack too (bn-3qh0). Off by default: bn-1w4h's
    /// refusal to touch a pack that opens and matches the identity its footer
    /// names is correct, and a needless re-seal costs an open's worth of work
    /// for no gain. Never required to repair a broken one.
    pub force:   bool,
    /// After the withdrawals, open the store once so the engine's roll-sealer
    /// rebuilds every owed pack before this command exits (bn-3qh0). Off by
    /// default because opening the engine is a heavier act than an offline
    /// tool should perform implicitly — it takes the writer lock and mutates
    /// the store — but it is what makes `rebuild-index` leave a rebuilt index
    /// behind, the way it always has for the loose-sidecar shape.
    pub reseal:  bool,
}

/// Build the byte-image of a segment's pointer sidecar from its recovered,
/// accepted batches — the pure, deterministic core the byte-equality test
/// pins. Streams are ordered ascending by id and each stream's batches
/// ascending by version, exactly as [`SealInput::from_snapshot`] would.
#[must_use]
pub fn rebuild_sidecar_bytes(
    segment_id: u64,
    base_pos: u64,
    accepted: &[mess_log::scanner::AcceptedBatch],
) -> Vec<u8> {
    let mut by_stream: BTreeMap<u64, Vec<SealBatch>> = BTreeMap::new();
    for b in accepted {
        by_stream.entry(b.stream_id).or_default().push(SealBatch {
            first_version:    b.first_stream_version,
            frame_count:      b.frame_count,
            first_global_pos: b.first_global_pos,
            // bn-2ib: the sealer persists the batch's REAL byte offset (the
            // block-native read path dereferences sealed `EventPtr`s straight
            // into the `.log`), so a byte-equal rebuild reproduces exactly
            // that. (Pre-bn-2ib sidecars carried the global position as a
            // pseudo offset; the engine's locate-by-scan fallback still
            // serves those, but a rebuild upgrades them to real pointers.)
            offset:           b.offset,
        });
    }
    let streams: Vec<SealStream> = by_stream
        .into_iter()
        .map(|(stream_id, mut batches)| {
            batches.sort_by_key(|b| b.first_version);
            SealStream { stream_id, batches }
        })
        .collect();
    let input = SealInput {
        segment_id,
        base_pos,
        streams,
        payloads: None,
        event_type_ids: None,
    };
    encode_sidecar(&input)
}

/// The state of a segment's consolidated sealed index, as the *engine's* own
/// admission rules would judge it (bn-3of dual read + bn-11g identity).
///
/// Only the first variant is a reason to leave the segment alone; every other
/// one means the segment either has already lost its cold tier or is about to
/// at the next open.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PackState {
    /// The pack opens and is the one the footer names (or the footer names
    /// none, the legacy D-FMT-10 policy, and the pack is trusted on coverage).
    Healthy,
    /// The footer names a SealPack and there is no `.seal` on disk. **The
    /// engine cannot see this**: with no candidate, nothing is refuted, so
    /// nothing ever re-queues the seal.
    Absent,
    /// A `.seal` is present and does not parse.
    Unreadable(String),
    /// The pack on disk is not the pack the footer names.
    IdentityMismatch { expected: String, observed: String },
    /// The footer names a pack but the name cannot be read (damaged extension
    /// region, wrong segment, unknown identity kind) — spec 01 §3.3.3 reader
    /// rule 2 fails closed, so no pack may be installed whatever is on disk.
    IdentityUnresolvable,
}

impl PackState {
    /// The stable machine token, reused as the withdrawal's `reason`.
    fn as_str(&self) -> &'static str {
        match self {
            PackState::Healthy => "pack-healthy",
            PackState::Absent => "pack-named-but-absent",
            PackState::Unreadable(_) => "pack-unreadable",
            PackState::IdentityMismatch { .. } => "pack-identity-mismatch",
            PackState::IdentityUnresolvable => "pack-identity-unresolvable",
        }
    }

    /// The one-line operator explanation of why a rebuild is owed.
    fn why(&self) -> String {
        match self {
            PackState::Healthy => {
                "the pack opens and matches its footer".into()
            }
            PackState::Absent => "the footer names a SealPack that is not on \
                                  disk, and with no candidate to refute the \
                                  engine never re-queues the seal by itself"
                .into(),
            PackState::Unreadable(e) => format!("the pack does not parse: {e}"),
            PackState::IdentityMismatch { expected, observed } => format!(
                "the footer names {expected} but the pack on disk is \
                 {observed}"
            ),
            PackState::IdentityUnresolvable => {
                "the footer sets SEAL_PACK_IDENTITY but the identity cannot be \
                 read, so no pack may be installed (spec 01 section 3.3.3 \
                 reader rule 2)"
                    .into()
            }
        }
    }
}

/// Judge a segment's consolidated sealed index exactly as `load_sealed` would.
fn classify_pack(seg: &store::SegmentFile, scan: &SegmentScan) -> PackState {
    let named = scan
        .trailer
        .as_ref()
        .filter(|t| t.names_seal_pack())
        .map(|t| crate::verify::read_named_identity(seg, t));

    if !seg.has_seal {
        // No pack on disk. Only a *named* one is missing; a segment that names
        // none and has none is not pack-sealed and never reaches here.
        return match named {
            Some(Some(_)) => PackState::Absent,
            Some(None) => PackState::IdentityUnresolvable,
            None => PackState::Healthy,
        };
    }

    let idx = match SealedSegmentIndex::open_pack_eager(&seg.seal_path) {
        Ok(idx) => idx,
        Err(e) => return PackState::Unreadable(e.to_string()),
    };
    match named {
        // Legacy footer policy (D-FMT-10): nothing to bind against, so the
        // pack is trusted on coverage — the same call the engine makes.
        None => PackState::Healthy,
        Some(None) => PackState::IdentityUnresolvable,
        Some(Some(name)) => {
            let observed = idx.pack_identity();
            let matches = observed
                .is_some_and(|o| o.as_bytes() == &name.identity)
                && idx.pack_format_version() == Some(name.pack_format_version);
            if matches {
                PackState::Healthy
            } else {
                PackState::IdentityMismatch {
                    expected: name.hex(),
                    observed: observed
                        .map_or_else(|| "none".to_string(), |o| o.hex()),
                }
            }
        }
    }
}

/// Rebuild the store's sealed indexes at `dir`.
pub fn run(dir: &Path, opts: &RebuildOptions) -> Report {
    let mut report = Report::new("rebuild-index", "rebuilt");
    report.set("dir", json!(dir.display().to_string()));
    report.set("dry_run", json!(opts.dry_run));
    report.set("force", json!(opts.force));
    report.set("reseal", json!(opts.reseal));

    // Rebuilding mutates the sealed dir: require exclusive access.
    let lock = lockprobe::probe(dir);
    if lock.is_held() && !opts.dry_run {
        report.push_finding(Finding::new(
            Severity::Error,
            "lock",
            "store-locked",
            "store is locked by a live writer; rebuild-index needs exclusive \
             access (use --dry-run to preview)",
        ));
        return report;
    }

    let segments = store::discover_segments(dir);
    let sealed_dir = dir.join("sealed");
    // Segments whose sealed index this run withdrew: the re-seal is owed for
    // exactly these, and `--reseal` reports on exactly these.
    let mut owed: Vec<u64> = Vec::new();

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

        // The pack path (bn-1w4h classification, bn-3qh0 repair). A segment is
        // on it if it carries a `.seal` OR if its footer names one — the
        // second disjunct is the deleted-pack case, and it is the whole point.
        if seg.has_seal
            || scan
                .trailer
                .as_ref()
                .is_some_and(SegmentCatalogEntry::names_seal_pack)
        {
            rebuild_pack_segment(
                &mut report,
                &sealed_dir,
                seg,
                &scan,
                opts,
                &mut owed,
            );
            continue;
        }

        let Some(header) = &scan.recovery.header else {
            report.push_finding(
                Finding::new(
                    Severity::Warn,
                    "rebuild",
                    "no-header",
                    format!(
                        "segment {} has no valid header; skipping",
                        seg.segment_id
                    ),
                )
                .with("segment_id", seg.segment_id),
            );
            continue;
        };

        // The rebuilt pointer sidecar image.
        let bytes = rebuild_sidecar_bytes(
            header.segment_id,
            header.base_pos,
            &scan.recovery.accepted,
        );
        let path = store::pidx_path(dir, seg.segment_id);

        let identical =
            std::fs::read(&path).map(|old| old == bytes).unwrap_or(false);
        if opts.dry_run {
            report.push_row(json!({
                "segment_id": seg.segment_id,
                "shape": "pidx",
                "pidx": path.display().to_string(),
                "bytes": bytes.len(),
                "streams": scan.recovery.stream_heads.len(),
                "identical_to_existing": identical,
                "written": false,
            }));
            continue;
        }

        if let Err(e) = write_atomic(&sealed_dir, &path, &bytes) {
            report.push_finding(
                Finding::new(
                    Severity::Error,
                    "rebuild",
                    "sidecar-write",
                    format!(
                        "segment {}: failed to write {}: {e}",
                        seg.segment_id,
                        path.display()
                    ),
                )
                .with("segment_id", seg.segment_id),
            );
            continue;
        }
        report.push_row(json!({
            "segment_id": seg.segment_id,
            "shape": "pidx",
            "pidx": path.display().to_string(),
            "bytes": bytes.len(),
            "streams": scan.recovery.stream_heads.len(),
            "identical_to_existing": identical,
            "written": true,
        }));
    }

    if opts.reseal && !opts.dry_run && !owed.is_empty() {
        drive_reseal(&mut report, dir, &owed);
    } else if !owed.is_empty() {
        report.advise(
            "next-step",
            "the rebuild completes at the next engine open, which re-seals \
             each withdrawn segment from the log and re-finalizes its footer \
             to name the fresh pack; pass --reseal to do that now",
        );
    }

    report.set("segments", json!(segments.len()));
    report.set("reseal_owed", json!(owed.len()));
    report
}

/// The pack path for one segment: classify, then either refuse (healthy, no
/// `--force`) or withdraw the sealed index and record the re-seal request.
fn rebuild_pack_segment(
    report: &mut Report,
    sealed_dir: &Path,
    seg: &store::SegmentFile,
    scan: &SegmentScan,
    opts: &RebuildOptions,
    owed: &mut Vec<u64>,
) {
    let id = seg.segment_id;
    let state = classify_pack(seg, scan);

    // bn-1w4h, preserved: a healthy pack is left alone. Writing a legacy
    // `.pidx` over it would be inert at best (bn-3of's dual read prefers the
    // `.seal`) and refutable at worst (bn-11g), and throwing away a pack that
    // works in order to re-seal an equivalent one buys nothing.
    if state == PackState::Healthy && !opts.force {
        report.push_finding(
            Finding::new(
                Severity::Warn,
                "rebuild",
                "pack-sealed-segment-skipped",
                format!(
                    "segment {id}: pack-sealed (bn-3of) and healthy, so \
                     nothing is rebuilt — a legacy sidecar would be shadowed \
                     by the .seal, and refuted outright if the footer names \
                     one. Re-seal through the engine to regenerate its pack \
                     (--force withdraws this pack so the next open rebuilds \
                     it)."
                ),
            )
            .with("segment_id", id)
            .with("pack_state", state.as_str())
            .with("path", seg.seal_path.display().to_string()),
        );
        return;
    }

    let reason =
        if state == PackState::Healthy { "forced" } else { state.as_str() };

    if opts.dry_run {
        report.push_row(json!({
            "segment_id": id,
            "shape": "seal-pack",
            "pack_state": state.as_str(),
            "reason": reason,
            "action": "withdraw-sealed-index",
            "written": false,
        }));
        owed.push(id);
        return;
    }

    let withdrawal =
        match mess_store::withdraw_sealed_index(sealed_dir, id, reason) {
            Ok(w) => w,
            Err(e) => {
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "rebuild",
                        "reseal-request-failed",
                        format!(
                            "segment {id}: could not record the re-seal \
                             request under {}: {e}. Nothing was lost — the \
                             log is authority and no .log byte was touched — \
                             but the segment keeps its current sealed state.",
                            sealed_dir.display()
                        ),
                    )
                    .with("segment_id", id)
                    .with("pack_state", state.as_str()),
                );
                return;
            }
        };

    // A `.pidx` for the same segment must go with the pack, and this is not
    // tidiness. While the pack served, the sidecar was inert (bn-3of's dual
    // read never even opened it); the moment the pack leaves the candidate
    // namespace the sidecar becomes the primary candidate. If the footer names
    // a pack it is refuted and quarantined at the next open anyway — noise for
    // an outcome already decided. If the footer names none (a legacy D-FMT-10
    // store mid-migration) it is *admitted*, which puts the segment in
    // `sealed_ids`, which is precisely the condition that suppresses the
    // re-seal: the withdrawal would silently accomplish nothing. Either way the
    // sidecar goes, and its derived `.filter`/`.pcol`/`.reg` siblings go with
    // it (the quarantine family already knows that).
    //
    // A crash between the two quarantines is survivable and self-announcing:
    // the re-seal request is already durable, so the next open either refutes
    // the sidecar (footer names a pack — converges) or admits it and skips the
    // re-seal (footer names none — the segment keeps a valid legacy index, and
    // re-running this command finishes the job).
    let pidx_quarantined = if seg.has_pidx {
        match mess_store::sealed_candidate::quarantine(&seg.pidx_path) {
            Ok(moved) => moved > 0,
            Err(e) => {
                report.push_finding(
                    Finding::new(
                        Severity::Warn,
                        "rebuild",
                        "sidecar-quarantine-failed",
                        format!(
                            "segment {id}: the pack was withdrawn but its \
                             legacy sidecar {} could not be: {e}. The next \
                             open may admit the sidecar instead of \
                             re-sealing; re-run once the filesystem allows it.",
                            seg.pidx_path.display()
                        ),
                    )
                    .with("segment_id", id),
                );
                false
            }
        }
    } else {
        false
    };

    owed.push(id);
    report.push_row(json!({
        "segment_id": id,
        "shape": "seal-pack",
        "pack_state": state.as_str(),
        "reason": reason,
        "action": "withdraw-sealed-index",
        "withdrawal": withdrawal.as_str(),
        "marker": withdrawal.marker.display().to_string(),
        "quarantined": withdrawal
            .quarantined
            .as_ref()
            .map(|p| p.display().to_string()),
        "pidx_quarantined": pidx_quarantined,
        "written": true,
    }));
    // Under `--reseal` the outcome is decided later in this same run, by
    // `drive_reseal`'s `pack-rebuilt` / `reseal-incomplete` / the open failure.
    // Announcing a pending re-seal here as well would contradict it.
    if opts.reseal {
        return;
    }
    report.push_finding(
        Finding::new(
            Severity::Warn,
            "rebuild",
            "reseal-pending",
            format!(
                "segment {id}: sealed index withdrawn ({}) because {}. The \
                 rebuild completes at the NEXT ENGINE OPEN, which re-seals \
                 the segment from the log and re-finalizes its footer to name \
                 the fresh pack; pass --reseal to do that now. Reads are \
                 served from the raw log — the only authority — meanwhile.",
                withdrawal.as_str(),
                state.why(),
            ),
        )
        .with("segment_id", id)
        .with("pack_state", state.as_str())
        .with("marker", withdrawal.marker.display().to_string())
        .with("fallback", "raw-segment-scan"),
    );
}

/// `--reseal`: open the store once and drop it, which drains the engine's
/// roll-sealer, then report what each owed segment ended up with.
///
/// Opening the real engine is the entire mechanism — there is no second sealer
/// here, deliberately. `LogEngine`'s `Drop` joins the seal thread, so by the
/// time this returns every queued re-seal has written its pack and fsynced its
/// footer, or the engine has counted it as skipped and the durable request is
/// still on disk for the next open.
fn drive_reseal(report: &mut Report, dir: &Path, owed: &[u64]) {
    let opts = mess_store::EngineOptions {
        // Whatever the store was created with. `SegmentWriter::resume`
        // re-reserves the active segment's blocks at `segment_size`, so
        // passing the crate default (256 MiB) to a store built with small
        // segments would fallocate a quarter of a gigabyte as a side effect of
        // a repair command. The active `.log` is preallocated to exactly the
        // segment size, so its own length is the store's answer.
        segment_size: detect_segment_size(dir),
        // The rebuilt shape is the current default shape.
        seal_pack: true,
        // Generous: this bounds only the wait for the read watermark to cover
        // a queued segment, which is already satisfied at open (recovery seeds
        // it), and a repair command would rather wait than silently leave a
        // segment unsealed.
        shutdown_seal_budget: Duration::from_secs(60),
        ..mess_store::EngineOptions::default()
    };
    match mess_store::LogEngine::open_with(dir, opts) {
        Ok(engine) => {
            let queued =
                engine.sealed_candidate_health().pending_reseal.clone();
            drop(engine); // joins the sealer: every queued pack is durable
            report.set("reseal_queued", json!(queued.len()));
            for &id in owed {
                let path = store::seal_path(dir, id);
                let identity = SealedSegmentIndex::open_pack_eager(&path)
                    .ok()
                    .and_then(|idx| idx.pack_identity().map(|i| i.hex()));
                match identity {
                    Some(hex) => report.push_finding(
                        Finding::new(
                            Severity::Ok,
                            "rebuild",
                            "pack-rebuilt",
                            format!(
                                "segment {id}: re-sealed — {} is back with \
                                 identity {hex}, and the segment footer names \
                                 it",
                                path.display()
                            ),
                        )
                        .with("segment_id", id)
                        .with("identity", hex)
                        .with("path", path.display().to_string()),
                    ),
                    None => report.push_finding(
                        Finding::new(
                            Severity::Warn,
                            "rebuild",
                            "reseal-incomplete",
                            format!(
                                "segment {id}: the re-seal did not leave a \
                                 readable pack at {}. The request is still on \
                                 disk, so the next open retries it; the \
                                 segment is served from the raw log meanwhile.",
                                path.display()
                            ),
                        )
                        .with("segment_id", id)
                        .with("fallback", "raw-segment-scan"),
                    ),
                }
            }
        }
        Err(e) => report.push_finding(
            Finding::new(
                Severity::Error,
                "rebuild",
                "reseal-open-failed",
                format!(
                    "--reseal could not open the store: {e}. The re-seal \
                     requests are durable, so the next engine open still \
                     completes the rebuild."
                ),
            )
            .with("dir", dir.display().to_string()),
        ),
    }
}

/// The store's own segment size: the byte length of the active (highest-id)
/// `.log`, which the writer preallocates to exactly that. Falls back to the
/// engine default for a store with no segments.
fn detect_segment_size(dir: &Path) -> u64 {
    let default = mess_store::EngineOptions::default().segment_size;
    store::discover_segments(dir)
        .last()
        .and_then(|seg| std::fs::metadata(&seg.log_path).ok())
        .map_or(default, |m| m.len().max(mess_log::format::HEADER_LEN as u64))
}

/// Crash-safe sidecar write: temp file in the same dir, then rename (mirrors
/// the sealer's temp→fsync→rename discipline; the dir fsync the sealer adds is
/// not reproduced here since rebuild is an offline recovery tool).
fn write_atomic(
    sealed_dir: &Path,
    final_path: &Path,
    bytes: &[u8],
) -> std::io::Result<()> {
    std::fs::create_dir_all(sealed_dir)?;
    let tmp = final_path.with_extension("pidx.tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, final_path)?;
    Ok(())
}
