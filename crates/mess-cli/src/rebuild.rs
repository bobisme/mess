//! `mess rebuild-index <dir>` — I5 made executable.
//!
//! From bare `.log` segments, rebuild the sealed pointer sidecars (`.pidx`)
//! and the derivable metadata tables (stream heads). The sidecar bytes are
//! produced by the exact [`encode_sidecar`] the sealer uses, so a rebuild from
//! an intact segment is **byte-equal** to the original sidecar (the acceptance
//! test). The `.pidx` is the only authoritative artifact rebuilt here; the
//! payload (`.pcol`) and filter (`.filter`) siblings are advisory and left to
//! the sealer.

use std::collections::BTreeMap;
use std::path::Path;

use mess_index::meta::{CommitGroup, Head, MetaStore, StreamId};
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
    /// Also rebuild the derivable metadata tables (stream heads). Off by
    /// default so a plain `rebuild-index` never touches the metadata store.
    pub meta:    bool,
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
    let input = SealInput { segment_id, base_pos, streams, payloads: None };
    encode_sidecar(&input)
}

/// Rebuild the store's pointer sidecars (and, with `--meta`, the stream-head
/// table) at `dir`.
pub fn run(dir: &Path, opts: &RebuildOptions) -> Report {
    let mut report = Report::new("rebuild-index", "rebuilt");
    report.set("dir", json!(dir.display().to_string()));
    report.set("dry_run", json!(opts.dry_run));

    // Rebuilding mutates the sealed dir + metadata: require exclusive access.
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

    // Aggregate stream heads across all segments (max version per stream) for
    // the optional metadata rebuild.
    let mut stream_heads: BTreeMap<u64, Head> = BTreeMap::new();
    let mut end_position: u64 = 0;

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
        end_position = end_position.max(scan.recovery.next_pos);

        // Track per-stream head version + head global position for meta.
        for b in &scan.recovery.accepted {
            let head_version = b.last_stream_version();
            let head_gpos = b.first_global_pos + u64::from(b.frame_count) - 1;
            stream_heads
                .entry(b.stream_id)
                .and_modify(|h| {
                    if head_version >= h.version {
                        h.version = head_version;
                        h.global_position = head_gpos;
                    }
                })
                .or_insert(Head {
                    version:         head_version,
                    global_position: head_gpos,
                });
        }

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

    if opts.meta && !opts.dry_run {
        rebuild_meta(&mut report, dir, &stream_heads, end_position);
    } else if opts.meta {
        report
            .set("meta_stream_heads_would_rebuild", json!(stream_heads.len()));
    }

    report.set("segments", json!(segments.len()));
    report
}

/// Rebuild the stream-head metadata table from the recovered heads (idempotent
/// per I5: rewriting the same group rewrites the same bytes).
fn rebuild_meta(
    report: &mut Report,
    dir: &Path,
    heads: &BTreeMap<u64, Head>,
    end_position: u64,
) {
    let meta = match MetaStore::open(store::meta_dir(dir)) {
        Ok(m) => m,
        Err(e) => {
            report.push_finding(Finding::new(
                Severity::Warn,
                "meta",
                "meta-open",
                format!("could not open metadata store to rebuild heads: {e}"),
            ));
            return;
        }
    };
    let mut group = CommitGroup::new(end_position);
    for (&id, &head) in heads {
        group.stream_heads.push((StreamId(id), head));
    }
    match meta.apply_group(&group) {
        Ok(()) => report.set("meta_stream_heads_rebuilt", json!(heads.len())),
        Err(e) => report.push_finding(Finding::new(
            Severity::Warn,
            "meta",
            "meta-apply",
            format!("failed to rebuild stream heads: {e}"),
        )),
    }
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
