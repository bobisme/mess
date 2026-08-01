//! Shared test support for the bn-20b engine swap: a thin backend wrapper that
//! bundles a [`LogEngine`] (or a snapshot-capable `PackSnapshotBackend` over
//! one) with the temp directory it lives in, so the existing Phase 1/2 suites
//! run **unchanged** against the composed production engine.
//!
//! bn-3l8n: the snapshot half is the **pack sidecar**
//! ([`PackSnapshotBackend`]), not the retired fjall head table — these types
//! are infrastructure for testing the facade, so they follow the production
//! composition. The fjall backend's own suite (`tests/fjall_snapshot.rs`) and
//! the pack/fjall differential (`tests/pack_snapshot.rs`) still name it
//! directly; they retire with it.
//!
//! The wrapper is [`Clone`] and shares one `Arc`-held temp dir + engine handle,
//! matching the interim `MockBackend`'s reuse-the-handle semantics (a facade
//! reopen over `backend.clone()` sees the same durable state). Dropping the
//! last clone cleans the temp dir.
#![allow(dead_code)]

use std::future::Future;
use std::sync::Arc;

use mess_store::backend::{
    AppendError, Appended, Backend, OwnedAppendBatch, RecordToAppend,
    StoredRecord,
};
use mess_store::snapshot::{SnapshotStore, StoredSnapshot};
use mess_store::{LogEngine, MockBackend, PackSnapshotBackend, Version};
use mess_testkit::{SweepingTempDir, sweeping_temp_dir};

/// Simulate a full process restart over a backend's own durable directory
/// (bn-20b). `self` is consumed — every in-process handle, OS lock, and cache
/// is released — and a fresh handle over the **same** durable state is
/// returned. The differential harness calls this on `Op::CrashReopen` so the
/// composed engine's reopen is a GENUINE fresh open (drop the engine, release
/// the D9 `StoreLock`, `LogEngine::open` the same dir, rehydrate the book from
/// the log), not a shared-`Arc` handle clone that would keep the in-process
/// book alive and make the "crash" vacuous.
pub trait Reopen: Sized {
    /// Consume this handle and re-open the same durable state.
    fn reopen(self) -> Self;
}

impl Reopen for MockBackend {
    /// The in-memory mock has no durable directory to reopen and no OS lock to
    /// release: its state *is* the handle. Reopening is the identity (the
    /// caller drops the old `EventStore`'s hot cache separately, modelling the
    /// process restart's lost in-process cache). Kept deliberately unchanged —
    /// the mock crash-reopen fidelity note lives in `differential_support`.
    fn reopen(self) -> Self { self }
}

/// A backend `B` paired with the temp dir it is rooted in.
pub struct Tmp<B> {
    backend: B,
    _dir:    Arc<SweepingTempDir>,
}

impl<B: Clone> Clone for Tmp<B> {
    fn clone(&self) -> Self {
        Tmp { backend: self.backend.clone(), _dir: Arc::clone(&self._dir) }
    }
}

/// The composed engine as a drop-in test backend (`MockBackend` replacement).
pub type TestBackend = Tmp<LogEngine>;

/// The snapshot-capable composed backend: the production
/// `PackSnapshotBackend` over the composed engine.
pub type TestSnapshotBackend = Tmp<PackSnapshotBackend<LogEngine>>;

impl TestBackend {
    /// A fresh composed engine on its own temp dir.
    #[must_use]
    pub fn new() -> Self {
        let dir = sweeping_temp_dir("mess-store-common-backend");
        let engine =
            LogEngine::open(dir.path().join("store")).expect("open engine");
        Tmp { backend: engine, _dir: Arc::new(dir) }
    }
}

impl Default for TestBackend {
    fn default() -> Self { Self::new() }
}

impl TestSnapshotBackend {
    /// A fresh snapshot-capable composed engine on its own temp dir.
    #[must_use]
    pub fn new() -> Self {
        let dir = sweeping_temp_dir("mess-store-common-snapshot-backend");
        Self::open_at(dir)
    }

    fn open_at(dir: SweepingTempDir) -> Self {
        let engine =
            LogEngine::open(dir.path().join("store")).expect("open engine");
        let backend =
            PackSnapshotBackend::open(engine, dir.path().join("snap"))
                .expect("open snapshot backend");
        Tmp { backend, _dir: Arc::new(dir) }
    }
}

impl Default for TestSnapshotBackend {
    fn default() -> Self { Self::new() }
}

impl Reopen for TestSnapshotBackend {
    /// A genuine process restart: drop the live engine + snapshot backend
    /// FIRST (releasing the D9 `StoreLock` and the sidecar's own writer lock),
    /// then `LogEngine::open` / `PackSnapshotBackend::open` the SAME
    /// directories fresh — so recovery rehydrates the record book from the
    /// durable log with no shared in-process state carried over, and the
    /// sidecar re-selects its published root off disk. The temp dir (the
    /// durable bytes) survives via the retained `Arc<SweepingTempDir>`.
    fn reopen(self) -> Self {
        let Tmp { backend, _dir } = self;
        // Release every handle to the old engine before re-acquiring its lock.
        drop(backend);
        let root = _dir.path().to_path_buf();
        let engine =
            LogEngine::open(root.join("store")).expect("reopen engine");
        let backend = PackSnapshotBackend::open(engine, root.join("snap"))
            .expect("reopen snapshot backend");
        Tmp { backend, _dir }
    }
}

impl Reopen for TestBackend {
    /// As [`TestSnapshotBackend::reopen`], for the bare-engine backend.
    fn reopen(self) -> Self {
        let Tmp { backend, _dir } = self;
        drop(backend);
        let root = _dir.path().to_path_buf();
        let engine =
            LogEngine::open(root.join("store")).expect("reopen engine");
        Tmp { backend: engine, _dir }
    }
}

/// Open a snapshot-capable composed backend rooted at a caller-supplied dir,
/// for the tests that assert reopen behaviour against a fixed path.
#[must_use]
pub fn open_snapshot(root: &std::path::Path) -> PackSnapshotBackend<LogEngine> {
    std::fs::create_dir_all(root).expect("create root");
    let engine = LogEngine::open(root.join("store")).expect("open engine");
    PackSnapshotBackend::open(engine, root.join("snap"))
        .expect("open snapshot backend")
}

impl<B: Backend + Clone> Backend for Tmp<B> {
    type Error = B::Error;

    fn head(
        &self,
        stream_id: &str,
    ) -> impl Future<Output = Result<Version, Self::Error>> + Send {
        self.backend.head(stream_id)
    }

    fn read_stream(
        &self,
        stream_id: &str,
        after: Version,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<StoredRecord>, Self::Error>> + Send
    {
        self.backend.read_stream(stream_id, after, limit)
    }

    fn read_global(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<StoredRecord>, Self::Error>> + Send
    {
        self.backend.read_global(after, limit)
    }

    fn append_batch(
        &self,
        stream_id: &str,
        expected: Version,
        records: &[RecordToAppend],
    ) -> impl Future<Output = Result<Appended, AppendError<Self::Error>>> + Send
    {
        self.backend.append_batch(stream_id, expected, records)
    }

    fn append_batch_owned<'a>(
        &'a self,
        stream_id: &'a str,
        expected: Version,
        batch: OwnedAppendBatch,
    ) -> impl Future<Output = Result<Appended, AppendError<Self::Error>>> + Send + 'a
    {
        self.backend.append_batch_owned(stream_id, expected, batch)
    }
}

impl<B: SnapshotStore + Clone> SnapshotStore for Tmp<B> {
    fn save_snapshot(
        &self,
        stream_id: &str,
        snapshot: StoredSnapshot,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.backend.save_snapshot(stream_id, snapshot)
    }

    fn load_snapshot(
        &self,
        stream_id: &str,
    ) -> impl Future<Output = Result<Option<StoredSnapshot>, Self::Error>> + Send
    {
        self.backend.load_snapshot(stream_id)
    }
}
