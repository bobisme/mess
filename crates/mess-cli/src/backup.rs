//! `mess backup <dir> --to <dest> [--incremental]` — copy a consistent cut of
//! a live store (doc 07 §2).
//!
//! The cut ([`compute_cut`]) is: every sealed segment whole + the active
//! segment's committed prefix `[0, safe_offset)` + a `BACKUP_MANIFEST` written
//! **last** (its presence proves completeness, doc 07 §4). A retention lease
//! ([`crate::lease`]) is held for the copy so compaction cannot delete a
//! sealed segment mid-run. Every file is copied temp + fsync + rename so a
//! crashed backup leaves no half-file — only a missing manifest, which restore
//! detects.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use mess_log::crc::crc32c_two;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::lease;
use crate::report::{Finding, Report, Severity};
use crate::scan::scan_segment;
use crate::store;

/// The `BACKUP_MANIFEST` file name (written last; gates restore, doc 07 §4).
pub const BACKUP_MANIFEST: &str = "BACKUP_MANIFEST";

/// The `mess-backup` manifest format tag.
pub const BACKUP_FORMAT: &str = "mess-backup-v1";

/// Options for [`run`].
#[derive(Debug, Default, Clone)]
pub struct BackupOptions {
    /// Skip sealed files already present at the destination with a matching
    /// length + CRC (doc 07 §2 step 3).
    pub incremental: bool,
}

/// One file in the backup manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupFileEntry {
    /// Destination-relative path (e.g. `seg-00000001.log`, `sealed/...pidx`).
    pub path: String,
    /// The number of bytes that make up this file in the backup.
    pub len: u64,
    /// CRC32C over exactly those `len` bytes.
    pub crc32c: u32,
    /// `sealed` | `active` | `sidecar` | `meta`.
    pub role: String,
    /// Bytes copied from the source (for the active segment, the cut
    /// `safe_offset`; for a whole file, equal to `len`).
    pub copied_len: u64,
}

/// The `BACKUP_MANIFEST` payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    /// Format tag ([`BACKUP_FORMAT`]).
    pub format: String,
    /// Wall-clock creation time (unix seconds).
    pub created_unix: u64,
    /// The cut's durable watermark (doc 07 §1.3).
    pub watermark: u64,
    /// Whether this run was incremental.
    pub incremental: bool,
    /// Every file in the backup.
    pub files: Vec<BackupFileEntry>,
}

/// One member of the consistent cut, resolved from the source store.
#[derive(Debug, Clone)]
pub struct CutFile {
    /// Absolute source path.
    pub src: PathBuf,
    /// Destination-relative path.
    pub rel: String,
    /// Bytes to copy (whole file, or the active prefix `safe_offset`).
    pub copied_len: u64,
    /// `sealed` | `active` | `sidecar` | `meta`.
    pub role: &'static str,
    /// Content-stable (immutable) files can be skipped on an incremental run;
    /// the active prefix cannot.
    pub content_stable: bool,
}

/// The computed consistent cut of a store: the files to copy plus the cut's
/// durable watermark and protected segment-id range (doc 07 §1).
#[derive(Debug, Clone, Default)]
pub struct Cut {
    /// Files that make up the cut, in copy order (sealed first, active last).
    pub files: Vec<CutFile>,
    /// The cut's durable watermark (doc 07 §1.3).
    pub watermark: u64,
    /// Lowest segment id in the cut (for the retention lease).
    pub min_segment_id: u64,
    /// Highest segment id in the cut (for the retention lease).
    pub max_segment_id: u64,
    /// Non-fatal problems encountered computing the cut (e.g. a segment that
    /// failed to scan). Surfaced as findings by the caller.
    pub problems: Vec<String>,
}

/// Compute the consistent cut of the store at `dir` (doc 07 §1). Pure with
/// respect to the store (reads files, never writes; does not take the D9
/// lock). Sealed segments contribute their whole `.log` + sidecars; the active
/// segment contributes its committed prefix `[0, safe_offset)`.
#[must_use]
pub fn compute_cut(dir: &Path) -> Cut {
    let mut cut = Cut::default();
    let segments = store::discover_segments(dir);
    let mut min_seg = u64::MAX;
    let mut max_seg = 0u64;

    // Sealed segments and the active segment (sorted ascending by id already).
    // Emit sealed .log + sidecars first; defer the active .log to the end so a
    // plain-rsync-shaped copy order matches doc 07 §2.1.
    let mut active: Option<CutFile> = None;

    for seg in &segments {
        let scan = match scan_segment(seg.segment_id, &seg.log_path) {
            Ok(s) => s,
            Err(e) => {
                cut.problems.push(format!("segment {} unreadable: {e}", seg.segment_id));
                continue;
            }
        };
        min_seg = min_seg.min(seg.segment_id);
        max_seg = max_seg.max(seg.segment_id);

        let rel = seg
            .log_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        match &scan.trailer {
            Some(trailer) => {
                // Sealed: copy the whole immutable file; watermark tracks its end.
                cut.watermark = cut.watermark.max(trailer.end_pos);
                cut.files.push(CutFile {
                    src: seg.log_path.clone(),
                    rel,
                    copied_len: scan.file_len,
                    role: "sealed",
                    content_stable: true,
                });
            }
            None => {
                // Active/unsealed: copy the committed prefix only. Watermark is
                // its base_pos + accepted events (the exclusive durable end).
                let safe = scan.recovery.safe_offset;
                let end = scan.base_pos().unwrap_or(0) + scan.event_count();
                cut.watermark = cut.watermark.max(end);
                active = Some(CutFile {
                    src: seg.log_path.clone(),
                    rel,
                    copied_len: safe,
                    role: "active",
                    content_stable: false,
                });
            }
        }

        // Sidecars (content-stable) travel with their segment when present.
        for (path, present) in [
            (&seg.pidx_path, seg.has_pidx),
            (&seg.pcol_path, seg.has_pcol),
            (&seg.filter_path, seg.filter_path.exists()),
        ] {
            if present && let Ok(meta) = std::fs::metadata(path) {
                cut.files.push(CutFile {
                    src: path.clone(),
                    rel: format!("sealed/{}", path.file_name().and_then(|n| n.to_str()).unwrap_or("")),
                    copied_len: meta.len(),
                    role: "sidecar",
                    content_stable: true,
                });
            }
        }
    }

    if let Some(active) = active {
        cut.files.push(active);
    }

    // The `meta/` interner tables (`stream_names`/`type_names`) are the durable
    // source of truth for the name↔id bijection and are NOT rebuildable from
    // the log (bn-20b / bn-150; unlike every other meta table). A restored
    // store cannot resolve stream/type names without them, so the whole `meta/`
    // dir travels with the cut. bn-150 fsyncs a newly-interned name's row
    // durable *before* its covering append becomes durable, so every stream in
    // the committed cut already has its name durable in `meta/` at cut time
    // (doc 07 §1.2). It is copied after the log cut to minimise skew.
    collect_meta(dir, &mut cut.files);

    cut.min_segment_id = if min_seg == u64::MAX { 0 } else { min_seg };
    cut.max_segment_id = max_seg;
    cut
}

/// Recursively enumerate the `meta/` directory into cut files (role `meta`).
/// Meta is small and its LSM files mutate, so it is never skipped on an
/// incremental run.
fn collect_meta(dir: &Path, files: &mut Vec<CutFile>) {
    let meta_root = store::meta_dir(dir);
    let mut stack = vec![meta_root.clone()];
    while let Some(cur) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&cur) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(ft) = entry.file_type() else { continue };
            if ft.is_dir() {
                stack.push(path);
            } else if ft.is_file()
                && let Ok(meta) = std::fs::metadata(&path)
                && let Ok(rel) = path.strip_prefix(dir)
                && let Some(rel) = rel.to_str()
            {
                files.push(CutFile {
                    src: path.clone(),
                    rel: rel.replace('\\', "/"),
                    copied_len: meta.len(),
                    role: "meta",
                    content_stable: false,
                });
            }
        }
    }
}

/// Read `[0, copied_len)` of `src`, returning the bytes and their CRC32C.
fn read_prefix(src: &Path, copied_len: u64) -> io::Result<(Vec<u8>, u32)> {
    let mut f = std::fs::File::open(src)?;
    let mut buf = vec![0u8; copied_len as usize];
    f.read_exact(&mut buf)?;
    let crc = crc32c_two(&buf, &[]);
    Ok((buf, crc))
}

/// Atomically write `bytes` to `dest_rel` under `dest`: temp + fsync + rename.
fn write_atomic(dest: &Path, rel: &str, bytes: &[u8]) -> io::Result<()> {
    let final_path = dest.join(rel);
    if let Some(parent) = final_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = final_path.with_extension("mbk.tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &final_path)?;
    Ok(())
}

/// CRC32C over exactly `len` bytes of a destination file (for incremental
/// skip: verify by size + CRC, not name alone, doc 07 §2 step 3).
fn dest_matches(dest: &Path, rel: &str, len: u64, crc: u32) -> bool {
    let path = dest.join(rel);
    let Ok(meta) = std::fs::metadata(&path) else {
        return false;
    };
    if meta.len() != len {
        return false;
    }
    let Ok(bytes) = std::fs::read(&path) else {
        return false;
    };
    crc32c_two(&bytes, &[]) == crc
}

/// Run a backup of `dir` into `dest` (doc 07 §2).
pub fn run(dir: &Path, dest: &Path, opts: &BackupOptions) -> Report {
    let mut report = Report::new("backup", "files");
    report.set("dir", json!(dir.display().to_string()));
    report.set("dest", json!(dest.display().to_string()));
    report.set("incremental", json!(opts.incremental));

    // 1. Compute the cut.
    let cut = compute_cut(dir);
    for problem in &cut.problems {
        report.push_finding(Finding::new(
            Severity::Error,
            "cut",
            "segment-unreadable",
            problem.clone(),
        ));
    }
    report.set("watermark", json!(cut.watermark));

    if cut.files.is_empty() {
        report.push_finding(Finding::new(
            Severity::Warn,
            "cut",
            "empty-store",
            "no segments found to back up",
        ));
        return report;
    }

    if let Err(e) = std::fs::create_dir_all(dest) {
        report.push_finding(Finding::new(
            Severity::Error,
            "backup",
            "dest-create-failed",
            format!("could not create destination {}: {e}", dest.display()),
        ));
        return report;
    }

    // 2. Register the retention lease BEFORE copying (doc 07 §2 step 1).
    let backup_id = format!("bkp-{}-{}", std::process::id(), lease::now_unix());
    let _guard = match lease::acquire(
        dir,
        backup_id.clone(),
        cut.min_segment_id,
        cut.max_segment_id,
        cut.watermark,
        lease::DEFAULT_TTL_SECS,
    ) {
        Ok(g) => Some(g),
        Err(e) => {
            // A lease we cannot register is a hard problem: without it,
            // retention could race the copy. Refuse rather than copy unsafely.
            report.push_finding(Finding::new(
                Severity::Error,
                "lease",
                "lease-acquire-failed",
                format!("could not register retention lease: {e}"),
            ));
            return report;
        }
    };
    report.set("lease_id", json!(backup_id));
    report.advise("retention-lease-held", "segments in the cut are pinned against retention for the copy");

    // 3. Copy each cut file (temp + rename). Incremental skips content-stable
    //    files already present with matching size + CRC.
    let mut manifest_files = Vec::new();
    let mut copied = 0u64;
    let mut skipped = 0u64;
    let mut copied_bytes = 0u64;

    for file in &cut.files {
        let (bytes, crc) = match read_prefix(&file.src, file.copied_len) {
            Ok(v) => v,
            Err(e) => {
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "copy",
                        "source-read-failed",
                        format!("failed to read {}: {e}", file.src.display()),
                    )
                    .with("path", file.rel.clone()),
                );
                return report;
            }
        };
        let len = bytes.len() as u64;

        let will_skip = opts.incremental
            && file.content_stable
            && dest_matches(dest, &file.rel, len, crc);

        if will_skip {
            skipped += 1;
        } else {
            if let Err(e) = write_atomic(dest, &file.rel, &bytes) {
                report.push_finding(
                    Finding::new(
                        Severity::Error,
                        "copy",
                        "dest-write-failed",
                        format!("failed to write {}: {e}", file.rel),
                    )
                    .with("path", file.rel.clone()),
                );
                return report;
            }
            copied += 1;
            copied_bytes += len;
        }

        manifest_files.push(BackupFileEntry {
            path: file.rel.clone(),
            len,
            crc32c: crc,
            role: file.role.to_string(),
            copied_len: file.copied_len,
        });
        report.push_row(json!({
            "path": file.rel,
            "role": file.role,
            "len": len,
            "crc32c": crc,
            "action": if will_skip { "skipped" } else { "copied" },
        }));
    }

    // 4. Write the manifest LAST (its presence proves completeness).
    let manifest = BackupManifest {
        format: BACKUP_FORMAT.to_string(),
        created_unix: lease::now_unix(),
        watermark: cut.watermark,
        incremental: opts.incremental,
        files: manifest_files,
    };
    let manifest_bytes = match serde_json::to_vec_pretty(&manifest) {
        Ok(b) => b,
        Err(e) => {
            report.push_finding(Finding::new(
                Severity::Error,
                "manifest",
                "manifest-encode-failed",
                format!("could not encode backup manifest: {e}"),
            ));
            return report;
        }
    };
    if let Err(e) = write_atomic(dest, BACKUP_MANIFEST, &manifest_bytes) {
        report.push_finding(Finding::new(
            Severity::Error,
            "manifest",
            "manifest-write-failed",
            format!("could not write backup manifest: {e}"),
        ));
        return report;
    }

    // 5. Lease released on guard drop here.
    report.set("copied_files", json!(copied));
    report.set("skipped_files", json!(skipped));
    report.set("copied_bytes", json!(copied_bytes));
    report.push_finding(
        Finding::new(
            Severity::Ok,
            "backup",
            "backup-complete",
            format!(
                "backup complete: {copied} file(s) copied, {skipped} skipped, watermark {}",
                cut.watermark
            ),
        )
        .with("watermark", cut.watermark)
        .with("copied_files", copied)
        .with("skipped_files", skipped),
    );
    report
}
