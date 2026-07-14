//! Read-only access to the durable metadata: the live snapshot set (for
//! `retention explain`), the per-snapshot `fold_version` (for `doctor`'s drift
//! check), and the stream/type name registry (for `inspect`).
//!
//! Opening the fjall metadata store takes fjall's own directory lock, so under
//! a live writer this open fails — every caller treats that as best-effort and
//! degrades to an advisory rather than a hard error.
//!
//! # Two snapshot id spaces, unified here
//!
//! Snapshots live in **two** places, and this module reads both:
//!
//! 1. the optional legacy `<dir>/meta` `snapshot_heads` table, keyed by the
//!    engine interner's dense `stream_id` (the ids `stream_names` maps). The
//!    flat append owner does not create this store, so in practice this set is
//!    absent — reading it is for compatibility and tests that inject heads
//!    directly.
//! 2. the **app snapshot sidecar** `<dir>/.snapshots/meta`
//!    ([`store::snapshot_meta_dir`](crate::store::snapshot_meta_dir)), where a
//!    [`FjallSnapshotBackend`](mess_store::FjallSnapshotBackend) actually
//!    persists app snapshots — keyed by the *interim FNV* `stream_id` and now
//!    (post-bone) carrying the stream name as a side map so the heads can be
//!    joined back to names.
//!
//! Historically `doctor` read only (1) and correlated by registry id, while
//! apps wrote only (2) under FNV ids: the id spaces never intersected, so the
//! fold-version drift check was structurally vacuous. Reading (2) here and
//! joining each store's `snapshot_heads` against its own `stream_names` is what
//! makes the check fire on a real app store.
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
    pub snapshots: Vec<LiveSnapshot>,
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

/// Open the optional metadata stores and pull the facts. An absent legacy
/// engine store is the normal flat-owner state and contributes no snapshots.
/// Returns an error string (never panics) when a store that does exist cannot
/// be opened — e.g. it is locked by a live writer.
pub fn read(dir: &std::path::Path) -> Result<MetaFacts, String> {
    let meta_path = crate::store::meta_dir(dir);
    let mut snapshots = Vec::new();
    if meta_path.exists() {
        let meta = MetaStore::open(&meta_path).map_err(|e| e.to_string())?;
        snapshots.extend(snapshots_of(&meta));
    }

    // `bn-2di`: names no longer come from here at all — fjall has no name
    // keyspace. The engine's `id -> name` bijection lives in the log's
    // `$registry` (see `crate::registryfold`). What is still here is the
    // *snapshot* side map, which the sidecar writes so its FNV-keyed heads can
    // be joined back to stream names.
    //
    // Unify the two snapshot id spaces (see the module doc): the engine's own
    // `<dir>/meta` heads (historically empty) PLUS the app snapshot sidecar's
    // heads, each joined against its own side map.
    if let Some(app) = open_snapshot_sidecar(dir)? {
        snapshots.extend(snapshots_of(&app));
    }
    snapshots.sort_by_key(|s| s.stream_id);
    Ok(MetaFacts { snapshots })
}

/// Join a meta store's `snapshot_heads` against its `stream_names` interner:
/// one [`LiveSnapshot`] per stream that has both a persisted name and a
/// snapshot head. A head is only surfaced once a name is on record for its id,
/// which the engine interner and (post-bone) the snapshot sidecar both
/// guarantee — so this same join serves both id spaces.
fn snapshots_of(meta: &MetaStore) -> Vec<LiveSnapshot> {
    let Ok(names) = meta.snapshot_stream_names() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (id, _name) in &names {
        if let Ok(Some(head)) = meta.snapshot_head(StreamId(*id)) {
            let (fold_version, covers_empty_prefix) =
                decode_ref(&head.snapshot_ref).unwrap_or((0, false));
            out.push(LiveSnapshot {
                stream_id: *id,
                version: head.covered_version,
                fold_version,
                covers_empty_prefix,
            });
        }
    }
    out
}

/// Open the app snapshot sidecar meta store
/// ([`store::snapshot_meta_dir`](crate::store::snapshot_meta_dir)) if it
/// exists.
///
/// `Ok(None)` means there is no sidecar — a store with no app snapshots, which
/// is normal and never an error. A sidecar that EXISTS but cannot be opened
/// (locked by a live writer, or corrupt) is a hard `Err(reason)`, so `doctor`
/// degrades to an honest "could not read" advisory rather than silently
/// reporting `no snapshots` while real drift sits unread behind the lock — the
/// same reasoning bn-ve0 applies to the engine meta lock. The `exists` guard
/// also avoids *creating* an empty sidecar during a read-only run.
fn open_snapshot_sidecar(
    dir: &std::path::Path,
) -> Result<Option<MetaStore>, String> {
    let path = crate::store::snapshot_meta_dir(dir);
    if !path.exists() {
        return Ok(None);
    }
    MetaStore::open(&path).map(Some).map_err(|e| e.to_string())
}
