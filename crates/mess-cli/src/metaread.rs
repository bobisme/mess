//! Read-only access to the live snapshot set: the per-snapshot `fold_version`
//! (for `doctor`'s drift check), the covered version and empty-prefix flag (for
//! `retention explain`), and the set itself (for `inspect`).
//!
//! # One source, and it cannot fail
//!
//! Snapshots live in exactly one place: the **app snapshot sidecar**
//! `<dir>/.snapshots.packs`
//! ([`store::snapshot_pack_dir`](crate::store::snapshot_pack_dir)), where a
//! [`PackSnapshotBackend`](mess_store::PackSnapshotBackend) persists them as a
//! flat directory of immutable packs plus a discovery root. The records are
//! self-describing and carry the stream *name*, so no reverse `id -> name` side
//! map is needed to report one.
//!
//! [`mess_store::Sidecar::open_reader`] takes **no lock**, never creates,
//! truncates, repairs, renames, or deletes anything, and cannot fail: a missing
//! directory, a foreign/unknown layout, a truncated root, or a deleted pack all
//! resolve to *zero heads*. Enumeration walks the already-resolved discovery
//! root ([`stream_names`](mess_store::Sidecar::stream_names) +
//! [`load`](mess_store::Sidecar::load)) — bounded by the published head count,
//! O(1) per head, with no repair and no writer interference.
//!
//! `LiveSnapshot::stream_id` is the *interim FNV* id
//! ([`mess_store::interim_stream_id`] of the record's stream name); the pack
//! record is what makes recovering it from a name free.
//!
//! # bn-fj34: why [`read`] no longer returns a `Result`
//!
//! There used to be a *second* source — a legacy `<dir>/meta` key-value store
//! whose `snapshot_heads` table was keyed by the engine interner's dense
//! `stream_id`, joined against a `snapshot_stream_names` side map. It was the
//! only fallible part of this module and the only reason `doctor` had a
//! degraded path: opening that store took an exclusive directory lock with no
//! read-only or secondary mode, so under a live writer the open failed and
//! every caller degraded to an advisory (`meta-store-locked`, bn-ve0).
//!
//! The flat-append owner never created that store, `bn-3l8n` moved the app
//! sidecar off it, and `bn-fj34` deleted the storage engine underneath it. With
//! it goes the whole failure mode: this module reads one lock-free source that
//! answers "no snapshots" for everything it does not understand, so [`read`]
//! returns [`MetaFacts`] directly. `doctor`'s fold-version check, `inspect`'s
//! snapshot half, and `retention explain`'s live set are now available against
//! a **live** store, not just a stopped one or a `mess backup` copy.

use mess_store::interim_stream_id;
use mess_store::pack_snapshot::Sidecar;

/// One live snapshot decoded from the app snapshot sidecar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveSnapshot {
    /// The **interim FNV** id ([`mess_store::interim_stream_id`]) of `stream`.
    ///
    /// This is the identity the snapshot subsystem uses internally and the one
    /// `inspect` reports. It is deliberately **not** the engine's interned
    /// dense `stream_id`, so it must never be joined against anything keyed by
    /// those — see [`stream`](Self::stream), which is what a join uses.
    pub stream_id:           u64,
    /// The stream **name** the pack record names itself with.
    ///
    /// `bn-fj34`: the whole point of a self-describing record (ADR 0002 §1) is
    /// that this is available without a reverse side map, so a consumer that
    /// needs the engine's dense id can resolve one through the log's own
    /// `$registry` (`crate::registryfold`) instead of guessing.
    pub stream:              String,
    /// The stream version this snapshot folds up to (its `covered_version`).
    pub version:             u64,
    /// The fold used to produce it (from the record's own header, §9).
    pub fold_version:        u32,
    /// A §4.2 empty-prefix snapshot has no certification frames.
    pub covers_empty_prefix: bool,
}

/// Everything `retention`/`doctor`/`inspect` read out of the snapshot sidecar.
#[derive(Debug, Default)]
pub struct MetaFacts {
    /// Live snapshots (one per stream that has a published snapshot head).
    pub snapshots: Vec<LiveSnapshot>,
}

/// Read the app snapshot sidecar's live snapshot set, ascending by stream id.
///
/// Infallible by construction (see the module doc): an absent sidecar, an
/// unknown layout, or an unresolvable root all mean "this store has no app
/// snapshots", which is a legitimate answer and not a failure. A head that
/// cannot be resolved to a valid record (deleted pack, corrupt frame, a format
/// this binary does not understand) is silently skipped — a miss, never an
/// error, never a fabricated entry.
#[must_use]
pub fn read(dir: &std::path::Path) -> MetaFacts {
    let sidecar = Sidecar::open_reader(crate::store::snapshot_pack_dir(dir));
    let mut snapshots: Vec<LiveSnapshot> = sidecar
        .stream_names()
        .iter()
        .filter_map(|name| {
            let rec = sidecar.load(name)?;
            Some(LiveSnapshot {
                stream_id:           interim_stream_id(name),
                stream:              name.clone(),
                version:             rec.stream_version,
                fold_version:        rec.fold_version,
                covers_empty_prefix: rec.covers_empty_prefix,
            })
        })
        .collect();
    snapshots.sort_by_key(|s| s.stream_id);
    MetaFacts { snapshots }
}
