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

use crate::backend::{
    AppendError, Appended, Backend, RecordToAppend, StoredRecord,
};
use crate::version::Version;

#[derive(Default)]
struct Inner {
    /// Events per stream, in append order (index == stream position).
    streams: HashMap<String, Vec<StoredRecord>>,
    /// Every event across all streams, in global order.
    global: Vec<StoredRecord>,
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
#[derive(Clone, Default)]
pub struct MockBackend {
    inner: Arc<Mutex<Inner>>,
}

impl MockBackend {
    /// A fresh, empty backend.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

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
        // straight to a slice offset.
        let start = after.map_or(0, |p| p as usize + 1);
        Ok(inner
            .global
            .get(start..)
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
                stream_id: stream_id.to_string(),
                message_type: rec.message_type.clone(),
                data: rec.data.clone(),
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

        Ok(Appended { version, last_global_position: last_global })
    }
}
