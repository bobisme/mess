//! On-disk layout knowledge shared by every subcommand.
//!
//! The store directory laid down by [`mess_store::LogEngine`] is:
//!
//! ```text
//! <dir>/
//!   LOCK                       # D9 single-writer OS advisory lock
//!   seg-00000001.log           # active/rolled segments (id, 8-wide, ".log")
//!   sealed/
//!     seg-00000000000000000001.pidx   # pointer sidecar (id, 20-wide)
//!     seg-...........................pcol   # payload sidecar
//!     seg-...........................filter # membership filter
//!   meta/                      # fjall metadata (stream/snapshot heads, ...)
//! ```
//!
//! The two id widths (`:08` for `.log`, `:020` for the sidecars) mirror the
//! engine's own `segment_path` and `SealDriver::sidecar_path`; this module is
//! the single place the CLI encodes that.

use std::path::{Path, PathBuf};

/// The single-writer lock file name (mirrors `mess_log::lock::LOCK_FILE_NAME`).
pub const LOCK_FILE_NAME: &str = "LOCK";

/// A discovered segment on disk.
#[derive(Debug, Clone)]
pub struct SegmentFile {
    pub segment_id: u64,
    pub log_path: PathBuf,
    /// The sealed pointer sidecar path (may or may not exist).
    pub pidx_path: PathBuf,
    /// The payload sidecar path (may or may not exist).
    pub pcol_path: PathBuf,
    /// The membership-filter sidecar path (may or may not exist).
    pub filter_path: PathBuf,
    /// Whether the `.pidx` sidecar exists on disk.
    pub has_pidx: bool,
    /// Whether the `.pcol` sidecar exists on disk.
    pub has_pcol: bool,
}

/// The `.log` path for a segment id (`seg-<id:08>.log`).
#[must_use]
pub fn log_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("seg-{segment_id:08}.log"))
}

/// The sealed pointer-sidecar path for a segment id (`sealed/seg-<id:020>.pidx`).
#[must_use]
pub fn pidx_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join("sealed").join(format!("seg-{segment_id:020}.pidx"))
}

/// The lock-file path.
#[must_use]
pub fn lock_path(dir: &Path) -> PathBuf {
    dir.join(LOCK_FILE_NAME)
}

/// The metadata directory.
#[must_use]
pub fn meta_dir(dir: &Path) -> PathBuf {
    dir.join("meta")
}

/// Enumerate every `seg-<id>.log` under `dir`, ascending by id, resolving
/// each one's sidecar paths and presence. Files that do not match the naming
/// scheme are ignored (mirrors the engine's `enumerate_segment_ids`).
#[must_use]
pub fn discover_segments(dir: &Path) -> Vec<SegmentFile> {
    let mut ids = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(rest) = name.strip_prefix("seg-")
                && let Some(num) = rest.strip_suffix(".log")
                && let Ok(id) = num.parse::<u64>()
            {
                ids.push(id);
            }
        }
    }
    ids.sort_unstable();
    ids.into_iter()
        .map(|segment_id| {
            let pidx = pidx_path(dir, segment_id);
            let pcol = pidx.with_extension("pcol");
            let filter = pidx.with_extension("filter");
            SegmentFile {
                segment_id,
                log_path: log_path(dir, segment_id),
                has_pidx: pidx.exists(),
                has_pcol: pcol.exists(),
                pidx_path: pidx,
                pcol_path: pcol,
                filter_path: filter,
            }
        })
        .collect()
}

/// Enumerate the sealed pointer sidecars (`sealed/*.pidx`) present on disk,
/// ascending by segment id. Skips `.pidx.tmp` husks (crash-mid-seal temp
/// files) and the `.pcol`/`.filter` siblings.
#[must_use]
pub fn discover_sidecars(dir: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir.join("sealed")) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("pidx") {
                paths.push(path);
            }
        }
    }
    paths.sort();
    paths
}
