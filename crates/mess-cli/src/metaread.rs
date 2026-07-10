//! Read-only access to the durable metadata (`<dir>/meta`): the live snapshot
//! set (for `retention explain`), the per-snapshot `fold_version` (for
//! `doctor`'s drift check), and the stream/type name registry (for `inspect`).
//!
//! Opening the fjall metadata store takes fjall's own directory lock, so under
//! a live writer this open fails — every caller treats that as best-effort and
//! degrades to an advisory rather than a hard error.
//!
//! # bn-ve0: why there is no read-only fallback
//!
//! The obvious fix for the fjall-lock failure mode is "open read-only /
//! secondary instead of falling over" — investigated for `doctor`'s
//! fold-version check and rejected for fjall 3.1.6 (the version pinned in
//! `crates/mess-index/Cargo.toml`), for two independent reasons:
//!
//! 1. **No such API exists.** `fjall::Database::open` (via the `Openable`
//!    trait, `fjall-3.1.6/src/tx/single_writer/mod.rs`) dispatches to either
//!    `Database::create_new` or `Database::recover` (`fjall-3.1.6/src/db.rs`)
//!    depending on whether the directory already has a version marker. **Both**
//!    unconditionally call `LockedFileGuard::create_new`/`try_acquire`
//!    (`fjall-3.1.6/src/locked_file.rs`), which take an exclusive
//!    `std::fs::File::try_lock` on the directory's `LOCK` file — there is no
//!    read-only, shared-lock, or secondary/replica open mode in the public API,
//!    no builder flag for it, and no `#[doc(hidden)]` escape hatch either
//!    (checked every `pub`/`pub(crate)` fn in `db.rs`, `builder.rs`,
//!    `locked_file.rs`). Forking or patching fjall to add one is out of scope
//!    for a CLI-side bone.
//!
//! 2. **Even bypassing the lock would not be safe.** fjall's default durability
//!    here is journal-buffered (`MetaStore`'s own doc: "writes land in fjall's
//!    journal buffer and are not fsynced per commit"). A just-written
//!    `snapshot_heads` row — exactly the data `doctor`'s fold-version check
//!    wants — can sit in the live writer's in-memory journal for a while before
//!    it is ever flushed to an SST file on disk. Reading the on-disk
//!    SST/journal files directly (skipping `Database::open`'s lock, and with it
//!    fjall's own journal-recovery logic in `recovery.rs`) would silently
//!    return *stale or incomplete* data rather than failing loudly — worse for
//!    a health-check tool than today's honest "could not read" degradation,
//!    because a stale read could report `fold-version-consistent` while a real
//!    drift sits unflushed in the writer's journal.
//!
//! Given both, `doctor` keeps the existing "try exclusive, degrade to an
//! advisory finding on failure" shape (item 1/2 of bn-ve0: better wording +
//! docs for the caveat) rather than a forced read-only path. If fjall ever
//! ships a documented read-only/secondary mode, revisit this.

use mess_index::meta::{MetaStore, StreamId};

/// One live snapshot decoded from the `snapshot_heads` table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveSnapshot {
    pub stream_id:           u64,
    /// The stream version this snapshot folds up to (its `covered_version`).
    pub version:             u64,
    /// The fold used to produce it (from the opaque snapshot ref, §9).
    pub fold_version:        u32,
    /// A §4.2 empty-prefix snapshot has no certification frames.
    pub covers_empty_prefix: bool,
}

/// Everything `retention`/`doctor`/`inspect` read out of the metadata store.
#[derive(Debug, Default)]
pub struct MetaFacts {
    /// Live snapshots (one per stream that has a snapshot head).
    pub snapshots:    Vec<LiveSnapshot>,
    /// `(stream_id, name)` interned pairs.
    pub stream_names: Vec<(u64, String)>,
    /// `(event_type_id, name)` interned pairs.
    pub type_names:   Vec<(u32, String)>,
}

/// The opaque snapshot-ref v1 layout (mirrors `mess_store::fjall_snapshot`):
/// `tag(1) || fold_version(4 LE) || flags(1) || snapshot_ptr(8 LE)`; flags bit
/// 0 = `covers_empty_prefix`. Returns `None` for any record this binary does
/// not understand (legacy / future / garbled) — treated as no usable snapshot.
fn decode_ref(bytes: &[u8]) -> Option<(u32, bool)> {
    const REF_FORMAT_V1: u8 = 0x01;
    const REF_V1_LEN: usize = 1 + 4 + 1 + 8;
    if bytes.len() != REF_V1_LEN || bytes[0] != REF_FORMAT_V1 {
        return None;
    }
    let fold_version = u32::from_le_bytes(bytes[1..5].try_into().ok()?);
    let covers_empty_prefix = bytes[5] != 0;
    Some((fold_version, covers_empty_prefix))
}

/// True when a [`read`] error reason came from fjall's own exclusive
/// directory lock being held (`fjall::Error::Locked`, almost always a live
/// writer) rather than some other failure (fresh/absent store, corruption,
/// I/O). `read` collapses every error to a display string before returning
/// it (`mess-cli` has no direct `fjall` dependency to match the typed error
/// variant against), so this matches fjall's own `Display` text for that
/// variant — `"FjallError: Locked"`, from `#[error(transparent)]` on
/// [`mess_index::meta::MetaError::Fjall`] forwarding straight to
/// `fjall::Error`'s `Display` impl. `fjall::Error::Locked` is a stable
/// documented unit variant (`fjall-3.1.6/src/error.rs`), so this string is
/// tied to fjall's public API surface, not an implementation detail that
/// shifts under us.
#[must_use]
pub fn is_locked_error(reason: &str) -> bool {
    reason.contains("FjallError: Locked")
}

/// Open the metadata store read-only and pull the facts. Returns an error
/// string (never panics) when the store can't be opened — e.g. it is locked by
/// a live writer, or absent on a fresh directory.
pub fn read(dir: &std::path::Path) -> Result<MetaFacts, String> {
    let meta_path = crate::store::meta_dir(dir);
    if !meta_path.exists() {
        return Err(format!("no metadata store at {}", meta_path.display()));
    }
    let meta = MetaStore::open(&meta_path).map_err(|e| e.to_string())?;
    let stream_names = meta.stream_names().map_err(|e| e.to_string())?;
    let type_names = meta.type_names().map_err(|e| e.to_string())?;

    let mut snapshots = Vec::new();
    for (id, _name) in &stream_names {
        if let Ok(Some(head)) = meta.snapshot_head(StreamId(*id)) {
            let (fold_version, covers_empty_prefix) =
                decode_ref(&head.snapshot_ref).unwrap_or((0, false));
            snapshots.push(LiveSnapshot {
                stream_id: *id,
                version: head.covered_version,
                fold_version,
                covers_empty_prefix,
            });
        }
    }
    snapshots.sort_by_key(|s| s.stream_id);
    Ok(MetaFacts { snapshots, stream_names, type_names })
}
