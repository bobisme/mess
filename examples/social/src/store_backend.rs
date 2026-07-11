//! Type aliases + open helpers naming the concrete on-disk backend the demo
//! binaries use — now the **warm-write** production path.
//!
//! `Store` pins the concrete backend the demo binaries write through:
//! [`mess_store::EventStore`] over a
//! [`FjallSnapshotBackend`](mess_store::FjallSnapshotBackend) wrapping a
//! [`mess_store::LogEngine`]. Wrapping the log in the snapshot backend is what
//! makes it a [`SnapshotStore`](mess_store::SnapshotStore), which is exactly
//! the
//! bound [`EventStore::command_cached`](mess_store::EventStore::command_cached)
//! (the hot-aggregate write-through cache + snapshot-accelerated cold load)
//! requires — and [`crate::contracts::WriteOps`] now routes every write through
//! `command_cached`. `FjallSnapshotBackend` also forwards
//! [`SubscribeBackend`](mess_store::SubscribeBackend) straight to the wrapped
//! log, so `StoreProjections` is [`Projections`] over the **same** [`Store`]
//! backend the warm-write path uses — ONE `EventStore` serves both writes and
//! subscriptions; there is no second handle over a cloned engine.

use std::path::{Path, PathBuf};

use mess_store::{EventStore, FjallSnapshotBackend, LogEngine};

use crate::projections::Projections;

/// The on-disk **warm-write** store the demo binaries write through:
/// [`EventStore`](mess_store::EventStore) over
/// [`FjallSnapshotBackend<LogEngine>`](mess_store::FjallSnapshotBackend). Being
/// a [`SnapshotStore`](mess_store::SnapshotStore) is what unlocks the
/// `command_cached` warm path in [`crate::contracts::WriteOps`]; being a
/// [`SubscribeBackend`](mess_store::SubscribeBackend) (forwarded straight to
/// the wrapped [`LogEngine`]) is what lets [`StoreProjections`] tail the very
/// same handle.
pub type Store = EventStore<FjallSnapshotBackend<LogEngine>>;

/// The on-disk backend's rebuildable read model: [`Projections`] over the
/// same [`Store`] backend, subscribing directly — no second handle over a
/// cloned log needed now that [`FjallSnapshotBackend`] forwards
/// [`SubscribeBackend`](mess_store::SubscribeBackend).
pub type StoreProjections = Projections<FjallSnapshotBackend<LogEngine>>;

/// Failure opening the on-disk store (either half — the event log or the
/// snapshot sidecar). Rendered to a `String` at the seam so this crate needs no
/// dependency on `mess-index` (whose `MetaError` the snapshot open returns).
#[derive(Debug)]
pub enum OpenError {
    /// The event-log engine failed to open.
    Log(String),
    /// The snapshot sidecar (fjall heads + blob dir) failed to open.
    Snapshot(String),
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpenError::Log(e) => write!(f, "opening event log: {e}"),
            OpenError::Snapshot(e) => write!(f, "opening snapshot store: {e}"),
        }
    }
}

impl std::error::Error for OpenError {}

/// The snapshot sidecar directory kept **inside** the store dir:
/// `<dir>/.snapshots` (fjall heads under `meta/`, state blobs under `blobs/`).
///
/// Co-located so a single `--dir` names the whole store, and so `--force`'s
/// `remove_dir_all` wipes log and snapshots together. Seeding writes no
/// snapshots (the warm path's write-through cache is in-memory;
/// `command_cached` only persists a snapshot on an explicit `save_snapshot`),
/// so this dir stays skeletal after a seed — it is the *read/serve* side that
/// populates it if snapshots are ever saved.
#[must_use]
pub fn snapshot_root(dir: &Path) -> PathBuf { dir.join(".snapshots") }

/// The projection checkpoint sidecar kept next to the store dir:
/// `<dir>/.social-projections.ckpt`. The one place this path is written down,
/// so the serve binary, the `--rebuild` proof ([`crate::rebuild`]), and any
/// test agree on it. See
/// [`Projections::with_checkpoint`](crate::Projections::with_checkpoint)
/// for the atomic write-rename and why a sidecar (not the log).
#[must_use]
pub fn checkpoint_path(dir: &Path) -> PathBuf {
    dir.join(".social-projections.ckpt")
}

/// Open the warm-write [`Store`] over `dir`: a [`LogEngine`] on `dir` wrapped
/// in a [`FjallSnapshotBackend`] rooted at [`snapshot_root`]. The returned
/// store's hot-aggregate cache is enabled so the `command_cached` warm path
/// actually caches — see
/// [`with_cache_capacity`](mess_store::EventStore::with_cache_capacity).
pub fn open_store(dir: &Path) -> Result<Store, OpenError> {
    let engine =
        LogEngine::open(dir).map_err(|e| OpenError::Log(e.to_string()))?;
    let backend = FjallSnapshotBackend::open(engine, snapshot_root(dir))
        .map_err(|e| OpenError::Snapshot(e.to_string()))?;
    Ok(EventStore::new(backend).with_cache_capacity(DEFAULT_CACHE_CAPACITY))
}

/// Default hot-aggregate cache capacity for the on-disk warm-write [`Store`].
/// Bounds resident cached aggregates; the relationship streams that dominate a
/// social workload are shallow, so a modest window covers the genuinely hot
/// entities (a viral post, a celebrity user) without unbounded growth.
const DEFAULT_CACHE_CAPACITY: usize = 4096;
