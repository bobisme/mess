//! Engine B — RocksDB baseline in the current mess_db shape: two column
//! families ("global", "stream"), full payload duplicated into both records
//! (record shapes mirror mess_db/src/rocks/record.rs), N events batched into
//! one WriteBatch.

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use rocksdb::{
    ColumnFamilyDescriptor, Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB,
};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::seglog::ReplayStats;

#[derive(Clone, Copy, Debug)]
pub enum BDurability {
    /// WriteOptions::set_sync(false): WAL written, not synced.
    NoSync,
    /// WriteOptions::set_sync(true): WAL fsync before ack (RocksDB's own
    /// internal group commit applies across concurrent writers).
    SyncPerBatch,
    /// set_sync(false) writes + a dedicated task calling flush_wal(true) that
    /// acks all batches covered by the sync (same window shape as Engine A).
    Group(Duration),
}

// Mirrors mess_db/src/rocks/record.rs GlobalRecord.
#[derive(Serialize, Deserialize)]
struct GlobalRecord<'a> {
    #[serde(borrow)]
    id: Cow<'a, str>,
    #[serde(borrow)]
    stream_name: Cow<'a, str>,
    stream_position: u64,
    #[serde(borrow)]
    message_type: Cow<'a, str>,
    #[serde(borrow)]
    data: Cow<'a, [u8]>,
    #[serde(borrow)]
    metadata: Cow<'a, [u8]>,
    ord: u64,
}

// Mirrors mess_db/src/rocks/record.rs StreamRecord.
#[derive(Serialize, Deserialize)]
struct StreamRecord<'a> {
    global_position: u64,
    #[serde(borrow)]
    id: Cow<'a, str>,
    #[serde(borrow)]
    message_type: Cow<'a, str>,
    #[serde(borrow)]
    data: Cow<'a, [u8]>,
    #[serde(borrow)]
    metadata: Cow<'a, [u8]>,
    ord: u64,
}

struct Inner {
    next_global: u64,
    heads: HashMap<u64, u64>,
}

pub struct RocksEngine {
    db: Arc<DB>,
    inner: Mutex<Inner>,
    durability: BDurability,
    sync_tx: Option<mpsc::Sender<oneshot::Sender<()>>>,
}

pub fn stream_name(stream: u64) -> String {
    format!("stream-{:05}", stream)
}

fn stream_key(name: &str, version: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(name.len() + 9);
    k.extend_from_slice(name.as_bytes());
    k.push(0);
    k.extend_from_slice(&version.to_be_bytes());
    k
}

fn group_sync_loop(rx: mpsc::Receiver<oneshot::Sender<()>>, db: Arc<DB>, max_delay: Duration) {
    loop {
        let first = match rx.recv() {
            Ok(a) => a,
            Err(_) => return,
        };
        let deadline = Instant::now() + max_delay;
        let mut acks = vec![first];
        loop {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match rx.recv_timeout(deadline - now) {
                Ok(a) => acks.push(a),
                Err(_) => break,
            }
        }
        db.flush_wal(true).unwrap();
        for a in acks {
            let _ = a.send(());
        }
    }
}

// Same option shape as mess_db/src/rocks/db.rs.
fn opts() -> Options {
    let mut opts = Options::default();
    opts.create_missing_column_families(true);
    opts.create_if_missing(true);
    let cores = std::thread::available_parallelism().unwrap().get();
    opts.increase_parallelism(cores as i32);
    opts
}

impl RocksEngine {
    pub fn open(dir: &Path, durability: BDurability) -> Self {
        let db = DB::open_cf_descriptors(
            &opts(),
            dir,
            vec![
                ColumnFamilyDescriptor::new("global", opts()),
                ColumnFamilyDescriptor::new("stream", opts()),
            ],
        )
        .unwrap();
        let db = Arc::new(db);

        let sync_tx = if let BDurability::Group(d) = durability {
            let (tx, rx) = mpsc::channel();
            let dbc = db.clone();
            std::thread::spawn(move || group_sync_loop(rx, dbc, d));
            Some(tx)
        } else {
            None
        };

        RocksEngine {
            db,
            inner: Mutex::new(Inner { next_global: 0, heads: HashMap::new() }),
            durability,
            sync_tx,
        }
    }

    pub fn head(&self, stream: u64) -> Option<u64> {
        self.inner.lock().unwrap().heads.get(&stream).copied()
    }

    pub async fn append_batch(
        &self,
        stream: u64,
        expected: Option<u64>,
        payloads: &[Vec<u8>],
    ) -> u64 {
        let (first_global, first_version) = {
            let mut inner = self.inner.lock().unwrap();
            let head = inner.heads.get(&stream).copied();
            assert_eq!(head, expected, "version conflict on stream {stream}");
            let fv = head.map_or(0, |h| h + 1);
            let fg = inner.next_global;
            inner.next_global += payloads.len() as u64;
            inner.heads.insert(stream, fv + payloads.len() as u64 - 1);
            (fg, fv)
        };

        let name = stream_name(stream);
        let g_cf = self.db.cf_handle("global").unwrap();
        let s_cf = self.db.cf_handle("stream").unwrap();
        let mut wb = WriteBatch::default();
        for (i, data) in payloads.iter().enumerate() {
            let gp = first_global + i as u64;
            let ver = first_version + i as u64;
            let id = format!("{:026x}", gp); // stands in for mess_db's 26-char ULID
            let g = GlobalRecord {
                id: Cow::Borrowed(id.as_str()),
                stream_name: Cow::Borrowed(name.as_str()),
                stream_position: ver,
                message_type: Cow::Borrowed("AccountCredited"),
                data: Cow::Borrowed(data.as_slice()),
                metadata: Cow::Borrowed(&[]),
                ord: 0,
            };
            wb.put_cf(g_cf, gp.to_be_bytes(), postcard::to_allocvec(&g).unwrap());
            let s = StreamRecord {
                global_position: gp,
                id: Cow::Borrowed(id.as_str()),
                message_type: Cow::Borrowed("AccountCredited"),
                data: Cow::Borrowed(data.as_slice()),
                metadata: Cow::Borrowed(&[]),
                ord: 0,
            };
            wb.put_cf(s_cf, stream_key(&name, ver), postcard::to_allocvec(&s).unwrap());
        }

        let mut wo = WriteOptions::default();
        wo.set_sync(matches!(self.durability, BDurability::SyncPerBatch));
        self.db.write_opt(wb, &wo).unwrap();

        if let BDurability::Group(_) = self.durability {
            let (tx, rx) = oneshot::channel();
            self.sync_tx.as_ref().unwrap().send(tx).unwrap();
            let _ = rx.await;
        }
        first_global
    }

    /// Global CF iterator scan.
    pub fn read_global(&self, from: u64, limit: u64) -> ReplayStats {
        let cf = self.db.cf_handle("global").unwrap();
        let mut st = ReplayStats::default();
        let from_key = from.to_be_bytes();
        for item in self
            .db
            .iterator_cf(cf, IteratorMode::From(&from_key, Direction::Forward))
        {
            let (k, v) = item.unwrap();
            let rec: GlobalRecord = postcard::from_bytes(&v).unwrap();
            st.events += 1;
            st.bytes += (k.len() + v.len()) as u64;
            st.checksum = st
                .checksum
                .wrapping_add(rec.data.first().copied().unwrap_or(0) as u64 + rec.data.len() as u64);
            if st.events >= limit {
                break;
            }
        }
        st
    }

    /// Stream CF prefix scan.
    pub fn read_stream(&self, stream: u64, from: u64, limit: u64) -> ReplayStats {
        let cf = self.db.cf_handle("stream").unwrap();
        let name = stream_name(stream);
        let mut prefix = name.clone().into_bytes();
        prefix.push(0);
        let start = stream_key(&name, from);
        let mut st = ReplayStats::default();
        for item in self
            .db
            .iterator_cf(cf, IteratorMode::From(&start, Direction::Forward))
        {
            let (k, v) = item.unwrap();
            if !k.starts_with(&prefix) {
                break;
            }
            let rec: StreamRecord = postcard::from_bytes(&v).unwrap();
            st.events += 1;
            st.bytes += (k.len() + v.len()) as u64;
            st.checksum = st
                .checksum
                .wrapping_add(rec.data.first().copied().unwrap_or(0) as u64 + rec.data.len() as u64);
            if st.events >= limit {
                break;
            }
        }
        st
    }

    /// Untimed: sync the WAL and flush memtables to SSTs so `du` reflects the
    /// LSM representation.
    pub fn finalize(&self) {
        self.db.flush_wal(true).unwrap();
        for cf in ["global", "stream"] {
            self.db.flush_cf(self.db.cf_handle(cf).unwrap()).unwrap();
        }
    }

    /// Untimed: full manual compaction (secondary disk-size datapoint).
    pub fn compact(&self) {
        for cf in ["global", "stream"] {
            self.db.compact_range_cf(
                self.db.cf_handle(cf).unwrap(),
                None::<&[u8]>,
                None::<&[u8]>,
            );
        }
    }
}
