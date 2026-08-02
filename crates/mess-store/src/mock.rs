//! [`MockBackend`]: an in-memory [`Backend`] with **real** expected-version
//! conflict semantics, for unit tests and the concurrency harness.
//!
//! This is not a stub. All state lives behind one lock, and
//! [`append_batch`](Backend::append_batch) checks the expected version against
//! the stream's actual head *inside* that lock before committing — so two
//! writers that loaded the same version genuinely race, and exactly one wins.
//! That is what lets [`EventStore::command`](crate::EventStore::command)'s
//! retry machinery be exercised for real: run enough concurrent writers on one
//! stream and the losers observe [`AppendError::Conflict`] and retry.
//!
//! The RocksDB-backed [`Backend`] is a separate Phase 2 bone; nothing here
//! depends on `mess_db`.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use mess_log::watermark::Watermark;

use crate::backend::{
    AppendError, Appended, Backend, RecordToAppend, StoredRecord,
    SubscribeBackend,
};
use crate::snapshot::{
    CurrentHead, SnapshotCompatibility, SnapshotLookup, SnapshotMiss,
    SnapshotSaveOutcome, SnapshotStore, StoredSnapshot, publication_decision,
};
use crate::version::Version;

#[derive(Default)]
struct Inner {
    /// Events per stream, in append order (index == stream position).
    streams:   HashMap<String, Vec<StoredRecord>>,
    /// Every event across all streams, in global order.
    global:    Vec<StoredRecord>,
    /// The throwaway snapshot keyspace, keyed by the same complete
    /// `(stream, compatibility)` key the durable sidecar uses — a test double
    /// that keyed by stream alone would let a fold bump silently overwrite an
    /// older identity's head, which the real store forbids. Explicitly *not*
    /// part of the commit authority; wiped whenever this backend is dropped.
    snapshots: HashMap<(String, SnapshotCompatibility), StoredSnapshot>,
}

impl Inner {
    fn head(&self, stream_id: &str) -> Version {
        match self.streams.get(stream_id) {
            Some(events) if !events.is_empty() => {
                Version::At((events.len() - 1) as u64)
            }
            _ => Version::NoStream,
        }
    }
}

/// An in-memory event store backend. Cheap to clone — every clone shares the
/// same underlying state.
#[derive(Clone)]
pub struct MockBackend {
    inner:     Arc<Mutex<Inner>>,
    /// The committed global watermark (the count of committed events), a
    /// `mess-log` durable [`Watermark`] advanced under the same critical
    /// section as each append so a live tail can wake event-bounded rather
    /// than by polling. Shared across clones (it is `Arc`-backed internally).
    watermark: Watermark,
}

impl Default for MockBackend {
    fn default() -> Self {
        MockBackend {
            inner:     Arc::new(Mutex::new(Inner::default())),
            watermark: Watermark::new(0),
        }
    }
}

impl MockBackend {
    /// A fresh, empty backend.
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// Total number of events stored across all streams (test helper).
    #[must_use]
    pub fn total_events(&self) -> usize {
        self.inner.lock().expect("mock lock poisoned").global.len()
    }
}

impl Backend for MockBackend {
    // The in-memory backend never fails at the engine level; only version
    // conflicts (a distinct, non-`Error` outcome) arise.
    type Error = Infallible;

    async fn head(&self, stream_id: &str) -> Result<Version, Self::Error> {
        Ok(self.inner.lock().expect("mock lock poisoned").head(stream_id))
    }

    async fn read_stream(
        &self,
        stream_id: &str,
        after: Version,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        let start = after.next_position() as usize;
        let page = {
            let inner = self.inner.lock().expect("mock lock poisoned");
            match inner.streams.get(stream_id) {
                Some(events) => events
                    .get(start..)
                    .unwrap_or(&[])
                    .iter()
                    .take(limit)
                    .cloned()
                    .collect(),
                None => Vec::new(),
            }
        };
        // Yield with the lock released, *after* a caller has loaded but
        // *before* it appends. This deliberately widens the load→append
        // window so concurrent writers on one stream genuinely contend — the
        // whole point of a mock that exercises the retry machinery for real.
        tokio::task::yield_now().await;
        Ok(page)
    }

    async fn read_global(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        let inner = self.inner.lock().expect("mock lock poisoned");
        // Global positions are assigned densely from 0, so `after` maps
        // straight to a slice offset. An unrepresentable successor (including
        // `u64::MAX`) is the terminal cursor and therefore an empty read.
        let start = match after {
            None => Some(0),
            Some(p) => p.checked_add(1).and_then(|n| usize::try_from(n).ok()),
        };
        Ok(start
            .and_then(|start| inner.global.get(start..))
            .unwrap_or(&[])
            .iter()
            .take(limit)
            .cloned()
            .collect())
    }

    async fn append_batch(
        &self,
        stream_id: &str,
        expected: Version,
        records: &[RecordToAppend],
    ) -> Result<Appended, AppendError<Self::Error>> {
        let mut inner = self.inner.lock().expect("mock lock poisoned");

        // The expected-version check and the write happen under one lock —
        // this is the real optimistic-concurrency gate.
        let actual = inner.head(stream_id);
        if actual != expected {
            return Err(AppendError::Conflict { expected, actual });
        }

        // Build the whole batch, then commit it in one shot, so a
        // partially-built batch never lands.
        let first_stream_pos = expected.next_position();
        let first_global = inner.global.len() as u64;
        let committed: Vec<StoredRecord> = records
            .iter()
            .enumerate()
            .map(|(i, rec)| StoredRecord {
                stream_id:       stream_id.to_string(),
                message_type:    rec.message_type.clone(),
                data:            rec.data.clone(),
                stream_position: first_stream_pos + i as u64,
                global_position: first_global + i as u64,
            })
            .collect();

        let last_global = committed.last().map_or(0, |r| r.global_position);
        let version = committed
            .last()
            .map_or(expected, |r| Version::At(r.stream_position));

        let stream = inner.streams.entry(stream_id.to_string()).or_default();
        stream.extend(committed.iter().cloned());
        inner.global.extend(committed);
        let new_watermark = inner.global.len() as u64;
        // Advance the watermark AFTER the events are visible in `global` (the
        // slice `read_global` serves) and with the state lock dropped, so a
        // woken live-tail subscriber that immediately reads `read_global`
        // observes exactly the positions the watermark now covers.
        drop(inner);
        self.watermark.advance(new_watermark);

        Ok(Appended { version, last_global_position: last_global })
    }
}

impl SubscribeBackend for MockBackend {
    async fn watermark(&self) -> Result<u64, Self::Error> {
        Ok(self.watermark.get())
    }

    async fn await_watermark_past(&self, pos: u64) -> Result<(), Self::Error> {
        self.watermark.await_past(pos).await;
        Ok(())
    }
}

impl SnapshotStore for MockBackend {
    /// Applies the same [`publication_decision`] rule as the durable sidecar,
    /// so a test that passes against the mock is testing the real contract and
    /// not a permissive stand-in. The canonical identity bytes here are the
    /// state blob: the mock has no frame to encode.
    async fn save_snapshot(
        &self,
        stream_id: &str,
        snapshot: StoredSnapshot,
    ) -> Result<SnapshotSaveOutcome, Self::Error> {
        let key = (stream_id.to_string(), snapshot.snapshot_ref.compatibility);
        let mut inner = self.inner.lock().expect("mock lock poisoned");
        let current =
            inner.snapshots.get(&key).map_or(CurrentHead::Vacant, |cur| {
                CurrentHead::Published(cur.snapshot_ref.coverage)
            });
        let outcome = publication_decision(
            current,
            snapshot.snapshot_ref.coverage,
            &snapshot.state_blob,
            || inner.snapshots.get(&key).map(|c| c.state_blob.clone()),
        );
        if matches!(
            outcome,
            SnapshotSaveOutcome::Published | SnapshotSaveOutcome::Repaired
        ) {
            inner.snapshots.insert(key, snapshot);
        }
        Ok(outcome)
    }

    async fn load_snapshot(
        &self,
        stream_id: &str,
        compatibility: SnapshotCompatibility,
    ) -> Result<SnapshotLookup, Self::Error> {
        let inner = self.inner.lock().expect("mock lock poisoned");
        let key = (stream_id.to_string(), compatibility);
        if let Some(found) = inner.snapshots.get(&key) {
            return Ok(SnapshotLookup::Hit(found.clone()));
        }
        // Same reporting the durable store gives: a head under another
        // identity is "incompatible", which is what makes the deploy wave
        // observable.
        let foreign = inner
            .snapshots
            .iter()
            .filter(|((s, c), _)| s == stream_id && *c != compatibility)
            .max_by_key(|(_, v)| v.snapshot_ref.coverage)
            .map(|((_, c), _)| *c);
        Ok(SnapshotLookup::Miss(match foreign {
            Some(stored) => SnapshotMiss::Incompatible { stored },
            None => SnapshotMiss::Absent,
        }))
    }
}
