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
//! `command_cached`. `StoreProjections` is the rebuildable read model,
//! [`Projections`] over the **log** engine.
//!
//! # The `SubscribeBackend` vs `SnapshotStore` reconciliation (dogfood gap)
//!
//! The read model ([`Projections`]) tails the log via
//! [`SubscribeBackend`](mess_store::SubscribeBackend); the warm write path
//! needs [`SnapshotStore`](mess_store::SnapshotStore). Those are two different
//! capabilities and — as of this bone — **no single backend type has both**:
//!
//! - [`LogEngine`](mess_store::LogEngine) is a `SubscribeBackend` but not a
//!   `SnapshotStore`.
//! - [`FjallSnapshotBackend<LogEngine>`](mess_store::FjallSnapshotBackend) is a
//!   `SnapshotStore` but **does not forward `SubscribeBackend`** — it wraps a
//!   `Backend` and delegates the base log ops (`head`/`read_stream`/
//!   `read_global`/`append_batch`), but it does not re-expose the wrapped
//!   engine's `watermark`/`await_watermark_past`. That is a **`mess-store`
//!   gap**, reported in this crate's concerns as dogfood (we do not edit
//!   `mess-store` from here).
//!
//! The workaround is clean because [`LogEngine`](mess_store::LogEngine) is
//! `Arc`-backed: a clone shares the same underlying log, watermark, and
//! commit-notification list. So the warm-write [`Store`] writes through the
//! snapshot backend, and the read model tails a *second* [`EventStore`] over a
//! **clone of the very same engine** — [`read_handle`]. Every commit the write
//! store makes is visible to the read handle's `read_global`/`subscribe` and
//! wakes its `wait_for` waiters, because both are views of one `Arc<Inner>`.
//! When `FjallSnapshotBackend` grows a `SubscribeBackend` forward (or the Phase
//! 4 engine makes snapshots always-on), this second handle collapses back into
//! one type with no change to callers.

use std::path::{Path, PathBuf};

use mess_store::{EventStore, FjallSnapshotBackend, LogEngine};

use crate::projections::Projections;

/// The on-disk **warm-write** store the demo binaries write through:
/// [`EventStore`](mess_store::EventStore) over
/// [`FjallSnapshotBackend<LogEngine>`](mess_store::FjallSnapshotBackend). Being
/// a [`SnapshotStore`](mess_store::SnapshotStore) is what unlocks the
/// `command_cached` warm path in [`crate::contracts::WriteOps`].
pub type Store = EventStore<FjallSnapshotBackend<LogEngine>>;

/// The on-disk backend's rebuildable read model: [`Projections`] over the
/// [`LogEngine`](mess_store::LogEngine) (a
/// [`SubscribeBackend`](mess_store::SubscribeBackend)), built from a
/// [`read_handle`] over the same log the [`Store`] writes to.
pub type StoreProjections = Projections<LogEngine>;

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

/// A subscribe-capable read handle over the same log `store` writes to — the
/// [`SubscribeBackend`](mess_store::SubscribeBackend) workaround for
/// `FjallSnapshotBackend` not forwarding it (see the module docs). The returned
/// [`EventStore`] wraps a **clone of the wrapped [`LogEngine`]**, which shares
/// the store's `Arc<Inner>` (and thus its watermark + commit notifications), so
/// a [`Projections`] built over it observes every commit made through `store`.
#[must_use]
pub fn read_handle(store: &Store) -> EventStore<LogEngine> {
    EventStore::new(store.backend().inner().clone())
}

/// Default hot-aggregate cache capacity for the on-disk warm-write [`Store`].
/// Bounds resident cached aggregates; the relationship streams that dominate a
/// social workload are shallow, so a modest window covers the genuinely hot
/// entities (a viral post, a celebrity user) without unbounded growth.
const DEFAULT_CACHE_CAPACITY: usize = 4096;
