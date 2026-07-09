//! The background sealer driver (D5): turns a rolled, unsealed segment's slice
//! of the active index into a durable sealed sidecar **off the append hot
//! path**, then hands readers over to it gaplessly.
//!
//! `spikes/perf_append` measured the cost of doing this inline: rolling a full
//! segment stalled the append path ~1.4 s. Moving the seal to a dedicated thread
//! removes that stall entirely — the committer's `apply_committed` never waits
//! on a seal. This module is that thread plus the synchronous
//! [`SealDriver::seal`] it runs per segment.
//!
//! # Seal steps (per segment)
//!
//! 1. **Encode** the sidecar bytes from the [`SealInput`]
//!    ([`crate::sealed::segment::encode_sidecar`]).
//! 2. **Durably write** them: temp file → fsync → rename → fsync the directory,
//!    so a crash leaves either no sidecar or a complete one (never a torn one).
//! 3. **Finalize the footer**: call back into mess-log's
//!    `SegmentWriter::seal` (the single seal `fdatasync`, §6) so recovery's R2
//!    fast path can trust the segment.
//! 4. **Install** the sealed index into the [`SealedStore`] (publish to
//!    readers) and then **evict** the segment's active entries — in that order,
//!    so the sealed-or-active invariant never gaps ([`crate::sealed::store`]).
//!
//! The finalize step is a caller-supplied closure returning [`io::Result`] so
//! this crate need not construct a `SegmentWriter` (which is generic over
//! mess-log's `Fs`); the caller passes `|| writer.seal().map(drop).map_err(..)`.
//!
//! # Measured (bn-20e, `tests/sealed_scale.rs`, release, this machine)
//!
//! Full-scale seal of a 256-MiB-shaped segment slice — 1,000,000 events,
//! 100,000 batches, 10,000 streams:
//!
//! | metric | value |
//! |---|---|
//! | background seal wall time | **~6.4 ms** (pointer index only; Phase-5 payload recompression adds the `seal_pipeline` ~1.4 s pass off-path) |
//! | append hot path during the seal, p50 / p99 / max | **70 ns / 1.48 µs / 67 µs** — no stall (an inline roll cost ~1.4 s, `perf_append`) |
//! | sealed point read, cached, p50 / p99 | **160 ns / 290 ns** (skip table; target <2 µs, `perf_replay` 1.67 µs / 0.79 µs) |
//! | stream replay (100k events), active walk / sealed | **252 µs / 329 µs** — competitive (both pure pointer decodes) |
//! | sidecar size | **1.47 B/event** (packed blocks ~1.0 + skip 0.16 + 56 B/stream directory) |
//!
//! The append path never stalling is the whole D5 point: the committer's
//! `apply_committed` shares nothing with the sealer except the store, which is
//! write-locked once, briefly, at install.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::thread::JoinHandle;

use crate::sealed::segment::{
    SealInput, SealedSegmentIndex, SealedSegmentRef, SidecarError, encode_sidecar,
};
use crate::sealed::store::SealedStore;

/// Why a seal failed.
#[derive(Debug, thiserror::Error)]
pub enum SealError {
    /// Encoding/parsing the sidecar, or writing it durably, failed.
    #[error("sidecar: {0}")]
    Sidecar(#[from] SidecarError),
    /// Durably writing the sidecar file failed.
    #[error("sidecar write: {0}")]
    Write(io::Error),
    /// The caller's footer-finalize step (mess-log seal) failed.
    #[error("footer finalize: {0}")]
    Finalize(io::Error),
}

/// Seal orchestration for one store + sidecar directory. Cheap to clone
/// (`Arc` inside); share it with the background thread.
#[derive(Clone)]
pub struct SealDriver {
    store: Arc<SealedStore>,
    dir: Arc<PathBuf>,
}

impl SealDriver {
    /// A driver writing sidecars into `dir` and publishing into `store`.
    pub fn new(store: Arc<SealedStore>, dir: impl Into<PathBuf>) -> Self {
        SealDriver { store, dir: Arc::new(dir.into()) }
    }

    /// The shared sealed store.
    pub fn store(&self) -> &Arc<SealedStore> {
        &self.store
    }

    /// The sidecar path for `segment_id`: `<dir>/seg-<id>.pidx`.
    pub fn sidecar_path(&self, segment_id: u64) -> PathBuf {
        self.dir.join(format!("seg-{segment_id:020}.pidx"))
    }

    /// Seal one segment synchronously (the four steps in the module docs).
    /// `finalize` finalizes the segment footer (mess-log's seal). On success
    /// the sealed index is installed and the segment's active entries are
    /// marked evicted; returns the installed index. **Ordering guarantee:**
    /// install precedes eviction, so readers using
    /// [`crate::sealed::store::resolve`] never see a gap.
    pub fn seal<Fin>(&self, input: SealInput, finalize: Fin) -> Result<SealedSegmentRef, SealError>
    where
        Fin: FnOnce() -> io::Result<()>,
    {
        let segment_id = input.segment_id;

        // 1 + 2: encode and durably write the sidecar.
        let bytes = encode_sidecar(&input);
        let path = self.sidecar_path(segment_id);
        write_durable(&path, &bytes).map_err(SealError::Write)?;

        // 3: finalize the footer (mess-log's single seal fsync).
        finalize().map_err(SealError::Finalize)?;

        // Parse back the bytes we just wrote (validates our own encoding; the
        // reader owns the same bytes without a re-read).
        let index: SealedSegmentRef = Arc::new(SealedSegmentIndex::from_bytes(bytes)?);

        // 4: install (publish) THEN evict — the gapless handoff.
        self.store.install(index.clone());
        self.store.mark_active_evicted(segment_id);
        Ok(index)
    }
}

/// Write `bytes` to `path` crash-atomically: temp file → fsync → rename →
/// fsync the parent directory. A crash leaves either the old/no file or the
/// complete new one, never a torn sidecar.
fn write_durable(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let tmp = path.with_extension("pidx.tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        // Directory fsync makes the rename durable. Best-effort: some
        // filesystems reject O_RDONLY dir fsync, which is not fatal to the
        // rename's atomicity on mainstream Linux fs.
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Background sealer thread
// ---------------------------------------------------------------------------

/// A finalize action shipped to the background thread. Boxed so callers can
/// close over a `SegmentWriter`.
pub type FinalizeFn = Box<dyn FnOnce() -> io::Result<()> + Send>;

enum Msg {
    Seal { input: SealInput, finalize: FinalizeFn, done: SyncSender<Result<SealedSegmentRef, SealError>> },
}

/// A dedicated background thread that seals segments off the append path. Drop
/// (or [`shutdown`](Self::shutdown)) joins the thread after draining queued
/// requests.
pub struct BackgroundSealer {
    tx: Option<std::sync::mpsc::Sender<Msg>>,
    handle: Option<JoinHandle<()>>,
}

impl BackgroundSealer {
    /// Spawn the sealer thread around `driver`.
    pub fn spawn(driver: SealDriver) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<Msg>();
        let handle = std::thread::Builder::new()
            .name("mess-sealer".into())
            .spawn(move || run(driver, rx))
            .expect("spawn sealer thread");
        BackgroundSealer { tx: Some(tx), handle: Some(handle) }
    }

    /// Queue a segment to seal. Returns a receiver that yields the seal result
    /// when the background thread finishes it. The append path does not block
    /// on this — that is the whole point (D5 / `perf_append`).
    pub fn submit<Fin>(
        &self,
        input: SealInput,
        finalize: Fin,
    ) -> Receiver<Result<SealedSegmentRef, SealError>>
    where
        Fin: FnOnce() -> io::Result<()> + Send + 'static,
    {
        let (done, rx) = sync_channel(1);
        let msg = Msg::Seal { input, finalize: Box::new(finalize), done };
        if let Some(tx) = &self.tx {
            // If the worker is gone the receiver simply never fires; callers
            // that care select with a timeout.
            let _ = tx.send(msg);
        }
        rx
    }

    /// Stop accepting work and join the thread, draining what is queued.
    pub fn shutdown(mut self) {
        self.join_inner();
    }

    fn join_inner(&mut self) {
        drop(self.tx.take());
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for BackgroundSealer {
    fn drop(&mut self) {
        self.join_inner();
    }
}

fn run(driver: SealDriver, rx: std::sync::mpsc::Receiver<Msg>) {
    for msg in rx {
        match msg {
            Msg::Seal { input, finalize, done } => {
                let result = driver.seal(input, finalize);
                let _ = done.send(result);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sealed::segment::{SealBatch, SealStream};
    use crate::sealed::store::resolve;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn input(segment_id: u64) -> SealInput {
        SealInput {
            segment_id,
            base_pos: 0,
            streams: vec![SealStream {
                stream_id: 10,
                batches: vec![SealBatch {
                    first_version: 0,
                    frame_count: 3,
                    first_global_pos: 0,
                    offset: 4096,
                }],
            }],
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn seal_writes_sidecar_finalizes_and_installs() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SealedStore::new());
        let driver = SealDriver::new(store.clone(), dir.path());

        let finalized = Arc::new(AtomicUsize::new(0));
        let f = finalized.clone();
        let idx = driver.seal(input(7), move || {
            f.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        let idx = idx.unwrap();

        assert_eq!(finalized.load(Ordering::SeqCst), 1, "finalize ran once");
        assert!(driver.sidecar_path(7).exists(), "sidecar written");
        assert_eq!(idx.resolve(10, 1).unwrap().unwrap().offset, 4096);
        assert!(store.get(7).is_some(), "installed");
        assert!(store.is_evicted(7), "evicted after install");

        // Reopen from disk round-trips.
        let reopened = SealedSegmentIndex::open(&driver.sidecar_path(7)).unwrap();
        assert_eq!(reopened.resolve(10, 2).unwrap().unwrap().offset, 4096);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn finalize_failure_aborts_before_install() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SealedStore::new());
        let driver = SealDriver::new(store.clone(), dir.path());
        let r = driver.seal(input(7), || Err(io::Error::other("boom")));
        assert!(matches!(r, Err(SealError::Finalize(_))));
        assert!(store.get(7).is_none(), "not installed on finalize failure");
        assert!(!store.is_evicted(7));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn background_sealer_seals_off_thread() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SealedStore::new());
        let driver = SealDriver::new(store.clone(), dir.path());
        let active = crate::ActiveIndex::new();
        let sealer = BackgroundSealer::spawn(driver);

        let rx = sealer.submit(input(3), || Ok(()));
        let idx = rx.recv().unwrap().unwrap();
        assert_eq!(idx.segment_id(), 3);
        assert_eq!(resolve(&active, &store, 10, 1).unwrap().offset, 4096);
        sealer.shutdown();
    }
}
