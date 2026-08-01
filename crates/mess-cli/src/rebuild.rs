//! `mess rebuild-index <dir>` — I5 made executable.
//!
//! From bare `.log` segments, rebuild the sealed pointer sidecars (`.pidx`).
//! The sidecar bytes are
//! produced by the exact [`encode_sidecar`] the sealer uses, so a rebuild from
//! an intact segment is **byte-equal** to the original sidecar (the acceptance
//! test). The `.pidx` is the only authoritative artifact rebuilt here; the
//! payload (`.pcol`) and filter (`.filter`) siblings are advisory and left to
//! the sealer.
//!
//! # bn-1w4h: pack-sealed segments are skipped, not rebuilt
//!
//! This command emits the **legacy sidecar shape only**. There is no offline
//! encoder for a consolidated `.seal` pack (bn-3of), and a `.pidx` written for
//! a pack-sealed segment is not merely redundant: bn-3of's dual read prefers
//! the pack and never opens the sidecar, and bn-11g's footer identity makes a
//! legacy sidecar offered in a named pack's place a `PackIdentityMismatch`
//! refutation — the next open would quarantine the very file this command
//! wrote. So those segments are reported and left alone; regenerating a pack is
//! the engine's re-seal path. That is a documented gap, not a silent one.
//!
//! **bn-ccx1 widened that gap deliberately.** With `EngineOptions::seal_pack`
//! now ON by default, every segment a current engine seals is pack-sealed, so
//! `rebuild-index` is a no-op on freshly written stores — it reports
//! `pack-sealed-segment-skipped` for each and writes nothing. It remains fully
//! functional for legacy loose-sidecar stores and for stores run in the
//! compatibility mode (`seal_pack: false`). bn-1yor's default-on matrix called
//! this the one real capability regression of the flip and admitted it;
//! bn-3qh0 tracks the offline pack encoder (plus the footer-refinalize
//! question bn-11g's identity rule raises) that would close it.
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

use mess_index::sealed::segment::{
    SealBatch, SealInput, SealStream, encode_sidecar,
};
use serde_json::json;

use crate::lockprobe;
use crate::report::{Finding, Report, Severity};
use crate::scan::scan_segment;
use crate::store;

/// Options for [`run`].
#[derive(Debug, Default, Clone)]
pub struct RebuildOptions {
    /// Show what would be written without writing anything.
    pub dry_run: bool,
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

/// Rebuild the store's pointer sidecars at `dir`.
pub fn run(dir: &Path, opts: &RebuildOptions) -> Report {
    let mut report = Report::new("rebuild-index", "rebuilt");
    report.set("dir", json!(dir.display().to_string()));
    report.set("dry_run", json!(opts.dry_run));

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
        // bn-1w4h: a pack-sealed segment gets no rebuilt `.pidx`. Writing one
        // would be inert at best (bn-3of's dual read prefers the `.seal`, so
        // the sidecar is never even opened for judgement) and actively
        // refutable at worst: when the footer NAMES a SealPack (bn-11g), a
        // legacy `.pidx` offered in its place is exactly the
        // `PackIdentityMismatch` refutation, so the next open would quarantine
        // the file this command just wrote. Rebuilding the pack shape is
        // engine work (the pack encoder is not exposed as an offline rebuild),
        // so the honest answer is to say so and touch nothing.
        if seg.has_seal
            || scan.trailer.as_ref().is_some_and(|t| t.names_seal_pack())
        {
            report.push_finding(
                Finding::new(
                    Severity::Warn,
                    "rebuild",
                    "pack-sealed-segment-skipped",
                    format!(
                        "segment {}: pack-sealed (bn-3of), so no .pidx is \
                         rebuilt — a legacy sidecar would be shadowed by the \
                         .seal, and refuted outright if the footer names one. \
                         Re-seal through the engine to regenerate its pack.",
                        seg.segment_id
                    ),
                )
                .with("segment_id", seg.segment_id)
                .with("path", seg.seal_path.display().to_string()),
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
            "pidx": path.display().to_string(),
            "bytes": bytes.len(),
            "streams": scan.recovery.stream_heads.len(),
            "identical_to_existing": identical,
            "written": true,
        }));
    }

    report.set("segments", json!(segments.len()));
    report
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
