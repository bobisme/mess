//! On-disk layout knowledge shared by every subcommand.
//!
//! The store directory laid down by [`mess_store::LogEngine`] is:
//!
//! ```text
//! <dir>/
//!   LOCK                       # D9 single-writer OS advisory lock
//!   seg-00000001.log           # active/rolled segments (id, 8-wide, ".log")
//!   sealed/
//!     seg-00000000000000000001.seal   # consolidated SealPack (id, 20-wide)
//!     seg-...........................pidx   # legacy pointer sidecar
//!     seg-...........................pcol   # payload sidecar   (.pidx family)
//!     seg-...........................filter # membership filter (.pidx family)
//!     seg-...........................reg    # registry delta    (.pidx family)
//!     seg-...........................par    # Reed-Solomon parity over the .log
//!     seg-.....................pidx.refuted # quarantined candidate (bn-30u)
//!     seg-......................seal.tmp    # crash-mid-seal husk (ignored)
//! ```
//!
//! That is the whole engine layout: **log segments and their derived sealed
//! artifacts, and nothing else.** There is no metadata directory. `bn-2di`
//! moved the name↔id bijection into the log as the `$registry` stream and
//! `bn-fj34` deleted the leftover `<dir>/meta` key-value store, so every
//! durable fact about a store is now in the segment bytes or derived from them.
//!
//! # The two sealed shapes, and the one that wins
//!
//! A sealed segment carries **either** a consolidated `.seal` pack (bn-3of) or
//! the legacy `.pidx` + `.pcol` + `.filter` trio — or, mid-migration, both.
//! [`SegmentFile::sealed_artifact`] answers which one is load-bearing using the
//! engine's own dual-read rule (`LogEngine::load_sealed`): the pack wins, and a
//! `.pidx` shadowed by a pack is inert. Every CLI surface that judges "is this
//! segment sealed?" must ask that question, not `has_pidx` — on a pack-sealed
//! store the `.pidx` simply does not exist.
//!
//! # Quarantined candidates are not unknown files
//!
//! bn-3qh0 puts one more thing in that same slot: `mess rebuild-index`'s
//! offline **re-seal request** for a pack-sealed segment. When there were
//! refuted bytes to preserve the slot holds them as always; when the pack was
//! simply gone — the state that has no candidate and so no refutation — it
//! holds a short self-describing note instead. Both mean the same thing to the
//! engine and to an operator: this segment's sealed index was withdrawn and a
//! fresh seal is owed.
//!
//! bn-30u renames a refuted candidate to `<name>.refuted`, which is exactly
//! what makes it invisible to the structural name parses above (its extension
//! is no longer `seal`/`pidx`). That invisibility is deliberate for *readers*
//! and wrong for *operators*: the file is preserved evidence of a seal that was
//! thrown away, and the marker is what re-queues the segment for a fresh seal.
//! [`discover_quarantined`] enumerates them so `doctor`/`verify`/`inspect` can
//! report them as what they are instead of ignoring them.
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
    /// The registry-delta sidecar path (bn-26pp; may or may not exist).
    pub reg_path:    PathBuf,
    /// The Reed-Solomon parity sidecar path (bn-2za; may or may not exist).
    pub par_path:    PathBuf,
    /// The consolidated SealPack path (bn-3of; may or may not exist).
    pub seal_path:   PathBuf,
    /// Whether the `.pidx` sidecar exists on disk.
    pub has_pidx:    bool,
    /// Whether the `.pcol` sidecar exists on disk.
    pub has_pcol:    bool,
    /// Whether the `.reg` registry-delta sidecar exists on disk (bn-26pp). A
    /// pack-sealed segment normally has none — its delta rides the pack's
    /// `REGISTRY_DELTA` section instead (bn-3h64) — and a segment that
    /// registered no name has none in either shape.
    pub has_reg:     bool,
    /// Whether the `.par` parity sidecar exists on disk (bn-2za).
    pub has_par:     bool,
    /// Whether the consolidated `.seal` pack exists on disk (bn-3of).
    pub has_seal:    bool,
}

/// Which sealed-index artifact is load-bearing for a segment, in the engine's
/// own preference order.
///
/// bn-3of's dual read (`LogEngine::load_sealed`) parses every `.seal` first and
/// folds in a `.pidx` only for segments no pack covered, so a store carrying
/// both for one segment serves the pack and leaves the sidecar entirely
/// unclassified. The CLI mirrors that here rather than inventing a second
/// notion of "sealed", so a surface can never disagree with the engine about
/// which bytes a reader would actually use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealedArtifact {
    /// A consolidated `.seal` pack (bn-3of) — wins over any sibling `.pidx`.
    Pack,
    /// A legacy `.pidx` pointer sidecar and its `.pcol`/`.filter` family.
    Sidecar,
    /// Neither: the segment has no sealed index (unsealed head, a segment
    /// whose seal never completed, or one whose candidate was quarantined).
    None,
}

impl SealedArtifact {
    /// The stable machine token for reports (`seal-pack`, `pidx`, `none`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            SealedArtifact::Pack => "seal-pack",
            SealedArtifact::Sidecar => "pidx",
            SealedArtifact::None => "none",
        }
    }

    /// Whether a sealed index artifact is present at all.
    #[must_use]
    pub fn is_present(self) -> bool { self != SealedArtifact::None }
}

impl SegmentFile {
    /// The sealed-index artifact a reader would use for this segment (the
    /// engine's dual-read preference: `.seal` over `.pidx`).
    #[must_use]
    pub fn sealed_artifact(&self) -> SealedArtifact {
        if self.has_seal {
            SealedArtifact::Pack
        } else if self.has_pidx {
            SealedArtifact::Sidecar
        } else {
            SealedArtifact::None
        }
    }

    /// The path of the artifact [`Self::sealed_artifact`] names, or `None`
    /// when the segment carries no sealed index.
    #[must_use]
    pub fn sealed_artifact_path(&self) -> Option<&Path> {
        match self.sealed_artifact() {
            SealedArtifact::Pack => Some(&self.seal_path),
            SealedArtifact::Sidecar => Some(&self.pidx_path),
            SealedArtifact::None => None,
        }
    }

    /// Whether a `.pidx` is present but shadowed by a `.seal` for the same
    /// segment — the mid-migration state bn-3of's dual read defines. The
    /// sidecar is inert while the pack serves; it becomes the primary candidate
    /// only if the pack is ever refuted and quarantined.
    #[must_use]
    pub fn pidx_shadowed_by_pack(&self) -> bool {
        self.has_seal && self.has_pidx
    }
}

/// A quarantined sealed-index artifact (bn-30u): a candidate some store open
/// refuted and renamed out of the candidate namespace.
#[derive(Debug, Clone)]
pub struct QuarantinedArtifact {
    /// The `*.refuted` file itself.
    pub path:         PathBuf,
    /// The segment it named, parsed from the file name (`None` when the name
    /// does not follow `seg-<id:020>.<ext>.refuted`).
    pub segment_id:   Option<u64>,
    /// The extension the file carried before quarantine (`seal`, `pidx`, or a
    /// derived sibling: `filter`, `pcol`, `reg`).
    pub original_ext: String,
    /// Whether this was the **primary** candidate (the artifact classification
    /// judged and refuted) rather than one of its derived siblings, which are
    /// dragged along by a `.pidx` quarantine.
    pub is_primary:   bool,
}

impl QuarantinedArtifact {
    /// The path the artifact would occupy if it were still a live candidate —
    /// the quarantine name with [`mess_store::QUARANTINE_SUFFIX`] stripped.
    #[must_use]
    pub fn candidate_path(&self) -> PathBuf {
        let name = self.path.as_os_str().to_string_lossy();
        PathBuf::from(
            name.strip_suffix(mess_store::QUARANTINE_SUFFIX)
                .unwrap_or(&name)
                .to_string(),
        )
    }
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

/// The registry-delta sidecar path for a segment id
/// (`sealed/seg-<id:020>.reg`, bn-26pp) — the same naming
/// [`mess_index::sealed::regdelta::reg_path`] owns, expressed in the CLI's
/// layout module so `backup`/`verify` resolve it exactly as the engine does.
#[must_use]
pub fn reg_path(dir: &Path, segment_id: u64) -> PathBuf {
    pidx_path(dir, segment_id).with_extension("reg")
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
            let reg = pidx.with_extension("reg");
            let par = pidx.with_extension("par");
            let seal = pidx.with_extension("seal");
            SegmentFile {
                segment_id,
                log_path: log_path(dir, segment_id),
                has_pidx: pidx.exists(),
                has_pcol: pcol.exists(),
                has_reg: reg.exists(),
                has_par: par.exists(),
                has_seal: seal.exists(),
                pidx_path: pidx,
                pcol_path: pcol,
                filter_path: filter,
                reg_path: reg,
                par_path: par,
                seal_path: seal,
            }
        })
        .collect()
}

/// Enumerate the sealed pointer sidecars (`sealed/*.pidx`) present on disk,
/// ascending by segment id. Skips `.pidx.tmp` husks (crash-mid-seal temp
/// files), `.pidx.refuted` quarantine markers (bn-30u — see
/// [`discover_quarantined`]), and the `.pcol`/`.filter` siblings.
#[must_use]
pub fn discover_sidecars(dir: &Path) -> Vec<PathBuf> {
    sealed_files_with_extension(dir, "pidx")
}

/// Enumerate the consolidated SealPacks (`sealed/*.seal`) present on disk,
/// ascending by segment id (bn-3of). The pack counterpart of
/// [`discover_sidecars`], and skipping the same husks and quarantine markers
/// for the same structural reason: their extension is `tmp`/`refuted`, not
/// `seal`.
#[must_use]
pub fn discover_seal_packs(dir: &Path) -> Vec<PathBuf> {
    sealed_files_with_extension(dir, "seal")
}

fn sealed_files_with_extension(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir.join("sealed")) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some(ext) {
                paths.push(path);
            }
        }
    }
    paths.sort();
    paths
}

/// Enumerate the quarantined sealed-index artifacts (`sealed/*.refuted`,
/// bn-30u), ascending by path.
///
/// **Read-only.** A quarantine marker is preserved evidence — either a crash
/// landed in a survivable window or the sealer has a bug, and the bytes are the
/// only witness — and it is also the durable trigger that re-queues the
/// segment's seal at the next open. The CLI therefore reports these and never
/// deletes, renames, or "repairs" one; the engine's own next open is what
/// converges the segment back to an admitted seal.
///
/// The name parse mirrors the engine's
/// (`sealed_candidate::segment_id_from_name` plus `PRIMARY_EXTENSIONS`), so
/// `mess` and `LogEngine` agree on which segment a marker names and whether it
/// was a primary candidate.
#[must_use]
pub fn discover_quarantined(dir: &Path) -> Vec<QuarantinedArtifact> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir.join("sealed")) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("refuted") {
            continue;
        }
        // `seg-<id:020>.<ext>.refuted` -> the extension under the suffix.
        let original_ext = path
            .file_stem()
            .and_then(|s| Path::new(s).extension())
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_string();
        out.push(QuarantinedArtifact {
            segment_id: mess_store::sealed_candidate::segment_id_from_name(
                &path,
            ),
            is_primary: mess_store::sealed_candidate::PRIMARY_EXTENSIONS
                .contains(&original_ext.as_str()),
            original_ext,
            path,
        });
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    out
}
