//! Type aliases + open helpers naming the concrete on-disk backend the demo
//! binaries use — now the **warm-write** production path.
//!
//! `Store` pins the concrete backend the demo binaries write through:
//! [`mess_store::EventStore`] over a
//! [`PackSnapshotBackend`](mess_store::PackSnapshotBackend) wrapping a
//! [`mess_store::LogEngine`]. Wrapping the log in the snapshot backend is what
//! makes it a [`SnapshotStore`](mess_store::SnapshotStore), which is exactly
//! the
//! bound [`EventStore::command_cached`](mess_store::EventStore::command_cached)
//! (the hot-aggregate write-through cache + snapshot-accelerated cold load)
//! requires — and [`crate::contracts::WriteOps`] now routes every write through
//! `command_cached`. `PackSnapshotBackend` also forwards
//! [`SubscribeBackend`](mess_store::SubscribeBackend) straight to the wrapped
//! log, so `StoreProjections` is [`Projections`] over the **same** [`Store`]
//! backend the warm-write path uses — ONE `EventStore` serves both writes and
//! subscriptions; there is no second handle over a cloned engine.
//!
//! # The sidecar is discardable, and that is a feature you can see
//!
//! bn-3l8n moved the sidecar from a fjall head table + blob dir
//! (`<dir>/.snapshots/`) to the pack sidecar at [`snapshot_root`]
//! (`<dir>/.snapshots.packs/`). Nothing migrates: a store seeded before the
//! move keeps its old `.snapshots/` bytes, this code never reads or deletes
//! them, and the new sidecar starts empty — so the first load of every
//! aggregate is a **miss that replays from the log** and re-warms. That is the
//! whole contract on display: the events are the truth, the sidecar is
//! acceleration you are allowed to throw away. Deleting
//! `<dir>/.snapshots.packs/` at any time (writer stopped) is a supported
//! operation with no effect other than a colder next start; `--rebuild`'s
//! byte-compare proof passes either way.

use std::path::{Path, PathBuf};

use mess_store::{EventStore, LogEngine, PackSnapshotBackend};

use crate::projections::Projections;

/// The on-disk **warm-write** store the demo binaries write through:
/// [`EventStore`](mess_store::EventStore) over
/// [`PackSnapshotBackend<LogEngine>`](mess_store::PackSnapshotBackend). Being
/// a [`SnapshotStore`](mess_store::SnapshotStore) is what unlocks the
/// `command_cached` warm path in [`crate::contracts::WriteOps`]; being a
/// [`SubscribeBackend`](mess_store::SubscribeBackend) (forwarded straight to
/// the wrapped [`LogEngine`]) is what lets [`StoreProjections`] tail the very
/// same handle.
pub type Store = EventStore<PackSnapshotBackend<LogEngine>>;

/// The on-disk backend's rebuildable read model: [`Projections`] over the
/// same [`Store`] backend, subscribing directly — no second handle over a
/// cloned log needed now that [`PackSnapshotBackend`] forwards
/// [`SubscribeBackend`](mess_store::SubscribeBackend).
pub type StoreProjections = Projections<PackSnapshotBackend<LogEngine>>;

/// Failure opening the on-disk store (either half — the event log or the
/// snapshot sidecar). Rendered to a `String` at the seam so the demo's error
/// type does not name either half's concrete error.
#[derive(Debug)]
pub enum OpenError {
    /// The event-log engine failed to open.
    Log(String),
    /// The snapshot sidecar failed to open. In practice this means another
    /// live process already holds its writer lock — a corrupt or foreign
    /// sidecar does not fail, it degrades to replay-on-miss.
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
/// `<dir>/.snapshots.packs` — immutable packs plus a discovery root. The name
/// matches `mess_cli::store::snapshot_pack_dir`, so `mess doctor` and `mess
/// inspect` find the same sidecar this app writes.
///
/// Co-located so a single `--dir` names the whole store, and so `--force`'s
/// `remove_dir_all` wipes log and snapshots together. Seeding writes no
/// snapshots (the warm path's write-through cache is in-memory;
/// `command_cached` only persists a snapshot on an explicit `save_snapshot`),
/// so this dir stays skeletal after a seed — it is the *read/serve* side that
/// populates it if snapshots are ever saved.
///
/// A store created before bn-3l8n has a `<dir>/.snapshots` fjall directory
/// instead. It is deliberately left alone — never read, never migrated, never
/// deleted — and the new sidecar simply starts empty, so every load replays.
/// See the module doc.
#[must_use]
pub fn snapshot_root(dir: &Path) -> PathBuf { dir.join(".snapshots.packs") }

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
/// in a [`PackSnapshotBackend`] rooted at [`snapshot_root`]. The returned
/// store's hot-aggregate cache is enabled so the `command_cached` warm path
/// actually caches — see
/// [`with_cache_capacity`](mess_store::EventStore::with_cache_capacity).
///
/// The sidecar opens in the default
/// [`SaveMode::Buffered`](mess_store::SaveMode) — a discardable cache write,
/// the same promise the old fjall head buffer made. The demo exposes no
/// durability knob for the sidecar because it has nothing to protect: losing a
/// buffered snapshot costs a replay, never a fact.
pub fn open_store(dir: &Path) -> Result<Store, OpenError> {
    let engine =
        LogEngine::open(dir).map_err(|e| OpenError::Log(e.to_string()))?;
    let backend = PackSnapshotBackend::open(engine, snapshot_root(dir))
        .map_err(|e| OpenError::Snapshot(e.to_string()))?;
    Ok(EventStore::new(backend).with_cache_capacity(DEFAULT_CACHE_CAPACITY))
}

/// Default hot-aggregate cache capacity for the on-disk warm-write [`Store`].
/// Bounds resident cached aggregates; the relationship streams that dominate a
/// social workload are shallow, so a modest window covers the genuinely hot
/// entities (a viral post, a celebrity user) without unbounded growth.
const DEFAULT_CACHE_CAPACITY: usize = 4096;
