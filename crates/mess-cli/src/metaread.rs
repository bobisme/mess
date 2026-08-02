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
//! resolve to *zero heads*.
//!
//! # bn-2gns: enumeration is a pinned, paged scan
//!
//! Enumeration pins one validated discovery root
//! ([`pin_root`](mess_store::Sidecar::pin_root)) and pages through it in capped
//! chunks ([`scan`](mess_store::Sidecar::scan)) rather than materializing every
//! stream name and then resolving each one. Three things follow, all of which
//! `doctor`/`inspect`/`retention explain` want at scale:
//!
//! - peak memory is one page, not one entry per published head;
//! - every page is bound to **one** root generation, so the inventory is a
//!   consistent view rather than a smear across concurrent publications — a
//!   cursor from a superseded root is rejected and the read restarts;
//! - a head that does not validate is skipped *and counted*
//!   ([`degraded`](MetaFacts::degraded)), so a partial inventory announces
//!   itself instead of looking like a smaller store.
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

use std::num::NonZeroU32;

use mess_store::pack_snapshot::Sidecar;
use mess_store::{SnapshotScanDiagnostic, interim_stream_id};

/// How many heads one scan page carries.
///
/// Bounded memory is the point, so this is a page size and not a total: the
/// loop below keeps paging until the pinned root is exhausted. Well under
/// [`mess_store::MAX_SNAPSHOT_SCAN_LIMIT`], because an offline tool would
/// rather make a few more cheap calls than hold a large page.
const SCAN_PAGE: u32 = 512;

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
    /// Live snapshots (one per published head).
    pub snapshots: Vec<LiveSnapshot>,
    /// Published heads that did not validate and were therefore **not**
    /// reported: a deleted pack, a corrupt frame, a format this binary does
    /// not understand.
    ///
    /// Nonzero means the inventory above is a *degraded* view of a larger set.
    /// It is never an error — the store still replays — but a caller that
    /// would act destructively on this inventory must fail closed.
    pub degraded:  u64,
}

/// Read the app snapshot sidecar's live snapshot set, ascending by stream id.
///
/// Infallible by construction (see the module doc): an absent sidecar, an
/// unknown layout, or an unresolvable root all mean "this store has no app
/// snapshots", which is a legitimate answer and not a failure. A head that
/// cannot be resolved to a valid record is skipped and counted in
/// [`degraded`](MetaFacts::degraded) — a miss, never an error, never a
/// fabricated entry.
#[must_use]
pub fn read(dir: &std::path::Path) -> MetaFacts {
    let sidecar = Sidecar::open_reader(crate::store::snapshot_pack_dir(dir));
    let limit = NonZeroU32::new(SCAN_PAGE).expect("SCAN_PAGE is nonzero");
    let mut snapshots = Vec::new();
    let mut degraded = 0u64;

    // Re-pin on a rejected page: the writer published a newer generation
    // mid-walk, so the partial read is discarded and the inventory is taken
    // from one consistent root. Bounded attempts, because a store under
    // continuous save pressure must not spin an offline tool forever.
    'attempt: for _ in 0..8 {
        let Some(pin) = sidecar.pin_root() else {
            return MetaFacts::default();
        };
        snapshots.clear();
        degraded = 0;
        let mut cursor = None;
        loop {
            let page = sidecar.scan(&pin, cursor.as_ref(), limit);
            match page.diagnostic {
                SnapshotScanDiagnostic::Rejected => continue 'attempt,
                SnapshotScanDiagnostic::Partial { unresolved } => {
                    degraded += unresolved;
                }
                _ => {}
            }
            snapshots.extend(page.entries.into_iter().map(|e| LiveSnapshot {
                stream_id:           interim_stream_id(&e.key.stream_id),
                stream:              e.key.stream_id,
                version:             e.coverage.covered_version().unwrap_or(0),
                fold_version:        e.key.compatibility.fold_version,
                covers_empty_prefix: e.coverage.covered_version().is_none(),
            }));
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        break;
    }

    snapshots.sort_by_key(|s| s.stream_id);
    MetaFacts { snapshots, degraded }
}
