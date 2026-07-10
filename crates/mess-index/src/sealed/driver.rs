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

use crate::sealed::filter::SegmentFilter;
use crate::sealed::payload::{
    self, PayloadError, PayloadSealOpts, SealedPayloadIndex,
};
use crate::sealed::segment::{
    SealInput, SealedSegmentIndex, SealedSegmentRef, SidecarError, encode_sidecar, filter_path_for,
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
    /// Encoding the payload sidecar failed — including a **verify-on-seal**
    /// byte-exactness mismatch ([`PayloadError::VerifyMismatch`]), which aborts
    /// the seal rather than write an unverifiable payload.
    #[error("payload sidecar: {0}")]
    Payload(#[from] PayloadError),
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

    /// The seal-time membership filter path for `segment_id` (bn-1i7):
    /// `<dir>/seg-<id>.filter`, paired with [`Self::sidecar_path`] via
    /// [`filter_path_for`].
    pub fn filter_path(&self, segment_id: u64) -> PathBuf {
        filter_path_for(&self.sidecar_path(segment_id))
    }

    /// The payload-block sidecar path for `segment_id` (bn-zge / D6):
    /// `<dir>/seg-<id>.pcol`, a sibling of [`Self::sidecar_path`]. Delegates to
    /// [`payload::pcol_path`] — the single source of truth for the `.pcol`
    /// naming, shared with the offline archive re-block (bn-382) so the two can
    /// never target different files.
    pub fn payload_sidecar_path(&self, segment_id: u64) -> PathBuf {
        payload::pcol_path(&self.dir, segment_id)
    }

    /// Seal a segment's **payloads** into the D6 payload-block sidecar
    /// (`.pcol`): columnar by default, row fallback where the codec cannot
    /// shred, an optional row-fallback dictionary tier. `events` are the raw
    /// payloads in stored (global-position) order.
    ///
    /// [`encode_payload_sidecar`](payload::encode_payload_sidecar) runs the
    /// **permanent verify-on-seal**: every block is reassembled and byte-compared
    /// against `events` before the bytes are written; a mismatch returns
    /// [`SealError::Payload`] and writes nothing. On success the sidecar is
    /// written crash-atomically and the parsed index returned.
    ///
    /// This is a separate artifact from the pointer sidecar
    /// ([`Self::seal`]); a caller that has the payload bytes in hand seals both.
    pub fn seal_payload(
        &self,
        segment_id: u64,
        events: &[&[u8]],
        opts: &PayloadSealOpts,
    ) -> Result<SealedPayloadIndex, SealError> {
        self.encode_and_write_payload(segment_id, events, opts)
    }

    /// Encode the D6 payload sidecar (running verify-on-seal), durably write it
    /// to the segment's `.pcol` path, and return the parsed index. Shared by
    /// [`Self::seal_payload`] and [`Self::seal`] (the run-loop path that emits
    /// `.pcol` when [`SealInput::payloads`](crate::sealed::segment::SealInput::payloads)
    /// is present). A verify-on-seal mismatch returns [`SealError::Payload`] and
    /// writes nothing.
    fn encode_and_write_payload(
        &self,
        segment_id: u64,
        events: &[&[u8]],
        opts: &PayloadSealOpts,
    ) -> Result<SealedPayloadIndex, SealError> {
        let bytes = payload::encode_payload_sidecar(segment_id, events, opts)?;
        let path = self.payload_sidecar_path(segment_id);
        write_durable(&path, &bytes).map_err(SealError::Write)?;
        Ok(SealedPayloadIndex::from_bytes(bytes)?)
    }

    /// Seal one segment synchronously (the four steps in the module docs,
    /// plus the bn-1i7 filter build folded into step 1–2). `finalize`
    /// finalizes the segment footer (mess-log's seal). On success the sealed
    /// index is installed and the segment's active entries are marked
    /// evicted; returns the installed index. **Ordering guarantee:** install
    /// precedes eviction, so readers using [`crate::sealed::store::resolve`]
    /// never see a gap.
    pub fn seal<Fin>(&self, input: SealInput, finalize: Fin) -> Result<SealedSegmentRef, SealError>
    where
        Fin: FnOnce() -> io::Result<()>,
    {
        let segment_id = input.segment_id;

        // 1 + 2: encode and durably write the sidecar.
        let bytes = encode_sidecar(&input);
        let path = self.sidecar_path(segment_id);
        write_durable(&path, &bytes).map_err(SealError::Write)?;

        // bn-1i7: build the seal-time stream-id membership filter and write
        // it durably too. Best-effort by design (I5): a build or write
        // failure here must never fail the seal itself — the segment is
        // still fully durable and correct without a filter, just without the
        // skip-ahead optimization (`might_contain_stream` degrades to
        // always-`true`). Built over the segment's *distinct* stream ids —
        // one key per `SealStream`, already deduplicated by construction.
        let stream_ids: Vec<u64> = input.streams.iter().map(|s| s.stream_id).collect();
        let filter = SegmentFilter::build(segment_id, &stream_ids);
        if let Some(f) = &filter {
            let _ = write_durable(&self.filter_path(segment_id), &f.to_bytes());
        }

        // bn-zge / D6: if the caller handed us the segment's payloads, emit the
        // columnar-by-default payload-block sidecar (`.pcol`) alongside the
        // pointer sidecar and attach it to the installed index, so the sealed
        // read path reassembles payloads from it. This runs the PERMANENT
        // verify-on-seal inside `encode_payload_sidecar`: a byte-exactness
        // mismatch returns `SealError::Payload` and aborts the whole seal
        // (before finalize/install) rather than ship an unverifiable payload.
        // `None` payloads seal the pointer sidecar only.
        let payload_index = match &input.payloads {
            Some(payloads) => {
                let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
                Some(self.encode_and_write_payload(
                    segment_id,
                    &refs,
                    &PayloadSealOpts::default(),
                )?)
            }
            None => None,
        };

        // 3: finalize the footer (mess-log's single seal fsync).
        finalize().map_err(SealError::Finalize)?;

        // Parse back the bytes we just wrote (validates our own encoding; the
        // reader owns the same bytes without a re-read).
        let mut index = SealedSegmentIndex::from_bytes(bytes)?;
        if let Some(f) = filter {
            index.attach_filter(f);
        }
        if let Some(p) = payload_index {
            index.attach_payload(p);
        }
        let index: SealedSegmentRef = Arc::new(index);

        // 4: install (publish) THEN evict — the gapless handoff.
        self.store.install(index.clone());
        self.store.mark_active_evicted(segment_id);
        Ok(index)
    }
}

/// Write `bytes` to `path` crash-atomically: temp file → fsync → rename →
/// fsync the parent directory. A crash leaves either the old/no file or the
/// complete new one, never a torn sidecar.
///
/// `pub(crate)` so the offline archive re-block ([`payload::archive_reblock`],
/// bn-382) rewrites a segment's `.pcol` under the exact same seal-commit
/// discipline: a crash mid-reblock leaves the OLD `.pcol` intact and serving.
pub(crate) fn write_durable(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;
    // Temp name derived from the real file name (not a fixed `.pidx.tmp`), so
    // the pointer (`.pidx`) and payload (`.pcol`) sidecars never share a temp.
    let mut tmp_name = path.file_name().unwrap_or_default().to_os_string();
    tmp_name.push(".tmp");
    let tmp = path.with_file_name(tmp_name);
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
            payloads: None,
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

    /// bn-1i7: `seal` builds and durably writes a `.filter` file alongside the
    /// sidecar, attaches it to the in-memory index, and a fresh `open` from
    /// disk re-attaches it too.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn seal_builds_and_persists_filter() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SealedStore::new());
        let driver = SealDriver::new(store.clone(), dir.path());

        let idx = driver.seal(input(7), || Ok(())).unwrap();

        assert!(driver.filter_path(7).exists(), "filter file written");
        assert!(idx.might_contain_stream(10), "present stream must never be a false negative");

        let reopened = SealedSegmentIndex::open(&driver.sidecar_path(7)).unwrap();
        assert!(
            reopened.might_contain_stream(10),
            "reopened index must re-attach the filter and answer correctly"
        );
    }

    /// bn-1i7 / I5: a missing or corrupt `.filter` file must never break
    /// opening the sidecar, and `resolve` must still be exactly correct —
    /// the filter degrades to always-`true` (unfiltered), never a wrong
    /// answer.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn missing_or_corrupt_filter_falls_back_to_unfiltered() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SealedStore::new());
        let driver = SealDriver::new(store.clone(), dir.path());
        driver.seal(input(7), || Ok(())).unwrap();

        // Missing filter file.
        std::fs::remove_file(driver.filter_path(7)).unwrap();
        let reopened = SealedSegmentIndex::open(&driver.sidecar_path(7)).unwrap();
        assert!(reopened.might_contain_stream(10), "missing filter degrades to always-maybe");
        assert_eq!(reopened.resolve(10, 1).unwrap().unwrap().offset, 4096, "resolve still correct");
        assert_eq!(reopened.resolve(10, 99).unwrap(), None, "absent version still correctly absent");

        // Corrupt filter file (seal again to recreate it, then flip a byte).
        driver.seal(input(8), || Ok(())).unwrap();
        let fp = driver.filter_path(8);
        let mut bytes = std::fs::read(&fp).unwrap();
        bytes[0] ^= 0xff;
        std::fs::write(&fp, &bytes).unwrap();
        let reopened8 = SealedSegmentIndex::open(&driver.sidecar_path(8)).unwrap();
        assert!(reopened8.might_contain_stream(10), "corrupt filter degrades to always-maybe");
        assert_eq!(reopened8.resolve(10, 1).unwrap().unwrap().offset, 4096, "resolve still correct");
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

    /// bn-zge / D6: `seal_payload` writes a `.pcol` sidecar next to the `.pidx`,
    /// runs verify-on-seal, and the reopened index reassembles byte-exact across
    /// a mixed (columnar + row-fallback) segment.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn seal_payload_writes_verified_pcol_sidecar() {
        use crate::columnar::{emit_int, emit_str};
        use crate::sealed::payload::{BlockKind, NoDicts, SealedPayloadIndex};

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SealedStore::new());
        let driver = SealDriver::new(store.clone(), dir.path());

        // Mixed corpus: shreddable msgpack maps interleaved with binary blobs.
        let mut evs: Vec<Vec<u8>> = Vec::new();
        for i in 0..64u64 {
            let mut m = vec![0x82];
            emit_str(&mut m, b"seq");
            emit_int(&mut m, i as i64);
            emit_str(&mut m, b"kind");
            emit_str(&mut m, b"demo");
            evs.push(m);
        }
        for i in 0..40u8 {
            evs.push(vec![0xff, 0x00, i, 0xca, 0x99]); // unshreddable
        }
        let refs: Vec<&[u8]> = evs.iter().map(Vec::as_slice).collect();

        let opts = PayloadSealOpts { block_events: 16, ..Default::default() };
        let idx = driver.seal_payload(9, &refs, &opts).unwrap();
        assert!(driver.payload_sidecar_path(9).exists(), "pcol written");
        assert_eq!(idx.event_count() as usize, evs.len());
        let kinds: Vec<BlockKind> = idx.blocks().iter().map(|b| b.kind).collect();
        assert!(kinds.contains(&BlockKind::Columnar) && kinds.contains(&BlockKind::Row));

        // Reopen from disk and reassemble byte-exact.
        let reopened =
            SealedPayloadIndex::open(&driver.payload_sidecar_path(9)).unwrap().unwrap();
        for (i, ev) in evs.iter().enumerate() {
            assert_eq!(&reopened.reassemble_event(i as u64, &NoDicts).unwrap(), ev);
        }
    }

    /// bn-zge / D6 pipeline wiring: a **real** `driver.seal()` (the run-loop
    /// path, not `seal_payload` directly) emits the `.pcol` sidecar when the
    /// `SealInput` carries payloads, attaches the parsed payload index to the
    /// installed segment, and the installed segment reassembles a mixed
    /// (columnar + row-fallback) corpus byte-exact — proving the columnar
    /// payload path is connected to the live seal, not just standalone.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn seal_emits_and_attaches_pcol_when_payloads_present() {
        use crate::columnar::{emit_int, emit_str};
        use crate::sealed::payload::{BlockKind, NoDicts};
        use crate::sealed::segment::SealStream;

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SealedStore::new());
        let driver = SealDriver::new(store.clone(), dir.path());

        // 260 events at global positions 0..260 in one stream: 130 shreddable
        // msgpack maps then 130 unshreddable blobs. Since one unshreddable event
        // routes a whole 128-event block to raw, this clustered layout yields a
        // MIX under the default block size — the first block columnar, the later
        // blocks row-fallback — exercising both reassembly paths through the
        // installed segment.
        let mut payloads: Vec<Vec<u8>> = Vec::new();
        for i in 0..130u64 {
            let mut m = vec![0x82];
            emit_str(&mut m, b"seq");
            emit_int(&mut m, i as i64);
            emit_str(&mut m, b"kind");
            emit_str(&mut m, b"demo");
            payloads.push(m);
        }
        for i in 0..130u16 {
            payloads.push(vec![0xff, 0x00, i as u8, (i >> 8) as u8, 0xca, 0x99]);
        }
        let input = SealInput {
            segment_id: 5,
            base_pos: 0,
            streams: vec![SealStream {
                stream_id: 1,
                batches: vec![SealBatch {
                    first_version: 0,
                    frame_count: payloads.len() as u32,
                    first_global_pos: 0,
                    offset: 4096,
                }],
            }],
            payloads: Some(payloads.clone()),
        };

        // Seal through the normal live path (`driver.seal`, the same call
        // `BackgroundSealer::run` makes).
        let idx = driver.seal(input, || Ok(())).unwrap();

        // The `.pcol` sidecar was written next to the `.pidx`.
        assert!(driver.payload_sidecar_path(5).exists(), ".pcol emitted by seal()");
        // The installed segment carries the attached payload index.
        assert!(idx.has_payload(), "seal attached the payload index");
        let pidx = idx.payload_index().unwrap();
        assert_eq!(pidx.event_count() as usize, payloads.len());
        let kinds: Vec<BlockKind> = pidx.blocks().iter().map(|b| b.kind).collect();
        assert!(kinds.contains(&BlockKind::Columnar), "expected a columnar block");
        assert!(kinds.contains(&BlockKind::Row), "expected a row-fallback block");

        // The installed segment reassembles every payload byte-exact.
        for (i, ev) in payloads.iter().enumerate() {
            assert_eq!(
                idx.reassemble_payload(i as u64, &NoDicts).unwrap().as_deref(),
                Some(ev.as_slice()),
                "payload {i} mismatch through installed segment",
            );
        }

        // A fresh reopen from disk re-attaches the sibling `.pcol`.
        let reopened = SealedSegmentIndex::open(&driver.sidecar_path(5)).unwrap();
        assert!(reopened.has_payload(), "open() re-attached the .pcol");
        assert_eq!(
            reopened.reassemble_payload(0, &NoDicts).unwrap().as_deref(),
            Some(payloads[0].as_slice()),
        );
    }

    /// A pointer-only seal (no payloads) emits no `.pcol` and attaches none —
    /// the additive nature of the payload sidecar.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn seal_without_payloads_emits_no_pcol() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SealedStore::new());
        let driver = SealDriver::new(store.clone(), dir.path());
        let idx = driver.seal(input(7), || Ok(())).unwrap();
        assert!(!driver.payload_sidecar_path(7).exists(), "no .pcol for pointer-only seal");
        assert!(!idx.has_payload());
    }

    /// The background thread path (`BackgroundSealer::run` → `driver.seal`) also
    /// emits and attaches the `.pcol` when payloads are present.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn background_sealer_emits_pcol() {
        use crate::sealed::payload::NoDicts;
        use crate::sealed::segment::SealStream;

        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(SealedStore::new());
        let driver = SealDriver::new(store.clone(), dir.path());
        let sealer = BackgroundSealer::spawn(driver.clone());

        let payloads: Vec<Vec<u8>> = (0..8u8).map(|i| vec![0xde, 0xad, i]).collect();
        let input = SealInput {
            segment_id: 4,
            base_pos: 0,
            streams: vec![SealStream {
                stream_id: 1,
                batches: vec![SealBatch {
                    first_version: 0,
                    frame_count: 8,
                    first_global_pos: 0,
                    offset: 4096,
                }],
            }],
            payloads: Some(payloads.clone()),
        };
        let rx = sealer.submit(input, || Ok(()));
        let idx = rx.recv().unwrap().unwrap();
        assert!(driver.payload_sidecar_path(4).exists(), "bg seal emitted .pcol");
        assert!(idx.has_payload());
        assert_eq!(
            idx.reassemble_payload(3, &NoDicts).unwrap().as_deref(),
            Some(payloads[3].as_slice()),
        );
        sealer.shutdown();
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
