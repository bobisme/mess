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
//! ```
//!
//! That is the whole engine layout: **log segments and their derived sealed
//! sidecars, and nothing else.** There is no metadata directory. `bn-2di` moved
//! the name↔id bijection into the log as the `$registry` stream and `bn-fj34`
//! deleted the leftover `<dir>/meta` key-value store, so every durable fact
//! about a store is now in the segment bytes or derived from them.
//!
//! An **application** may additionally co-locate a discardable snapshot sidecar
//! at `<dir>/.snapshots.packs/` ([`snapshot_pack_dir`]). It is not part of the
//! engine's layout — the engine neither writes nor requires it — but the CLI
//! knows the convention so `doctor`/`inspect` can read it.
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
    pub segment_id:  u64,
    pub log_path:    PathBuf,
    /// The sealed pointer sidecar path (may or may not exist).
    pub pidx_path:   PathBuf,
    /// The payload sidecar path (may or may not exist).
    pub pcol_path:   PathBuf,
    /// The membership-filter sidecar path (may or may not exist).
    pub filter_path: PathBuf,
    /// The Reed-Solomon parity sidecar path (bn-2za; may or may not exist).
    pub par_path:    PathBuf,
    /// The consolidated SealPack path (bn-3of; may or may not exist).
    pub seal_path:   PathBuf,
    /// Whether the `.pidx` sidecar exists on disk.
    pub has_pidx:    bool,
    /// Whether the `.pcol` sidecar exists on disk.
    pub has_pcol:    bool,
    /// Whether the `.par` parity sidecar exists on disk (bn-2za).
    pub has_par:     bool,
    /// Whether the consolidated `.seal` pack exists on disk (bn-3of).
    pub has_seal:    bool,
}

/// The `.log` path for a segment id (`seg-<id:08>.log`).
#[must_use]
pub fn log_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("seg-{segment_id:08}.log"))
}

/// The sealed pointer-sidecar path for a segment id
/// (`sealed/seg-<id:020>.pidx`).
#[must_use]
pub fn pidx_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join("sealed").join(format!("seg-{segment_id:020}.pidx"))
}

/// The Reed-Solomon parity-sidecar path for a segment id
/// (`sealed/seg-<id:020>.par`, bn-2za).
#[must_use]
pub fn par_path(dir: &Path, segment_id: u64) -> PathBuf {
    pidx_path(dir, segment_id).with_extension("par")
}

/// The consolidated SealPack path for a segment id
/// (`sealed/seg-<id:020>.seal`, bn-3of) — the same naming
/// [`mess_index::sealed::seal_pack_path`] owns, expressed in the CLI's layout
/// module so `verify`/`inspect` resolve it exactly as the engine does.
#[must_use]
pub fn seal_path(dir: &Path, segment_id: u64) -> PathBuf {
    pidx_path(dir, segment_id).with_extension("seal")
}

/// The lock-file path.
#[must_use]
pub fn lock_path(dir: &Path) -> PathBuf { dir.join(LOCK_FILE_NAME) }

/// The **app snapshot sidecar** directory: `<dir>/.snapshots.packs`.
///
/// An app that wraps its [`LogEngine`](mess_store::LogEngine) in a
/// [`PackSnapshotBackend`](mess_store::PackSnapshotBackend) persists its
/// snapshots under a co-located sidecar (the convention the `examples/social`
/// store uses, so a single `--dir` names the whole store). The sidecar is a
/// flat directory of immutable packs plus a discovery root — self-describing
/// records that carry the stream *name*, so no reverse `id -> name` side map is
/// needed. `doctor`'s fold-version check reads this location, so the check can
/// actually see app-persisted snapshots. Absent (a store with no snapshot
/// sidecar) is normal and simply means "no app snapshots here".
///
/// # Why `.snapshots.packs` and not `.snapshots`
///
/// bn-3l8n moved the sidecar from the fjall head table + blob dir
/// (`<dir>/.snapshots/{meta,blobs}`) to packs. The pack root is a **new,
/// distinct directory** so the two layouts never share a root: a store written
/// before the move keeps its `.snapshots/` bytes untouched and simply has no
/// pack sidecar, which every reader answers with "no snapshots" and every
/// writer answers by replaying — the discardable-acceleration law working as
/// designed. Nothing in mess reads, migrates, or deletes an old `.snapshots/`;
/// an operator who wants the space back removes it by hand.
#[must_use]
pub fn snapshot_pack_dir(dir: &Path) -> PathBuf { dir.join(".snapshots.packs") }

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
            let par = pidx.with_extension("par");
            let seal = pidx.with_extension("seal");
            SegmentFile {
                segment_id,
                log_path: log_path(dir, segment_id),
                has_pidx: pidx.exists(),
                has_pcol: pcol.exists(),
                has_par: par.exists(),
                has_seal: seal.exists(),
                pidx_path: pidx,
                pcol_path: pcol,
                filter_path: filter,
                par_path: par,
                seal_path: seal,
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
