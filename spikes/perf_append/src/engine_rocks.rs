//! RocksDB baseline in the current mess_db shape (copy of vertical_slice
//! Engine B, trimmed to the buffered path): two column families, full payload
//! duplicated into both records, N events per WriteBatch, WAL not synced.
//! This is the 532k ev/s target being reproduced on this machine.

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use rocksdb::{ColumnFamilyDescriptor, Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB};
use serde::{Deserialize, Serialize};

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
    db: DB,
    inner: Mutex<Inner>,
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

fn opts() -> Options {
    let mut opts = Options::default();
    opts.create_missing_column_families(true);
    opts.create_if_missing(true);
    let cores = std::thread::available_parallelism().unwrap().get();
    opts.increase_parallelism(cores as i32);
    opts
}

impl RocksEngine {
    pub fn open(dir: &Path) -> Self {
        let db = DB::open_cf_descriptors(
            &opts(),
            dir,
            vec![
                ColumnFamilyDescriptor::new("global", opts()),
                ColumnFamilyDescriptor::new("stream", opts()),
            ],
        )
        .unwrap();
        RocksEngine {
            db,
            inner: Mutex::new(Inner { next_global: 0, heads: HashMap::new() }),
        }
    }

    pub fn append_batch(&self, stream: u64, expected: Option<u64>, payloads: &[Vec<u8>]) -> u64 {
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
            let id = format!("{:026x}", gp);
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
        wo.set_sync(false);
        self.db.write_opt(wb, &wo).unwrap();
        first_global
    }

    /// (event count, commutative checksum) over the global CF.
    pub fn global_check(&self) -> (u64, u64) {
        let cf = self.db.cf_handle("global").unwrap();
        let mut events = 0u64;
        let mut ck = 0u64;
        for item in self.db.iterator_cf(cf, IteratorMode::Start) {
            let (_k, v) = item.unwrap();
            let rec: GlobalRecord = postcard::from_bytes(&v).unwrap();
            events += 1;
            ck = ck.wrapping_add(
                rec.data.first().copied().unwrap_or(0) as u64 + rec.data.len() as u64,
            );
        }
        (events, ck)
    }

    /// (count, fnv-of-payloads-in-version-order) for one stream.
    pub fn stream_fnv(&self, stream: u64) -> (u64, u64) {
        let cf = self.db.cf_handle("stream").unwrap();
        let name = stream_name(stream);
        let mut prefix = name.clone().into_bytes();
        prefix.push(0);
        let start = stream_key(&name, 0);
        let mut count = 0u64;
        let mut fnv = crate::verify::FNV_SEED;
        for item in self
            .db
            .iterator_cf(cf, IteratorMode::From(&start, Direction::Forward))
        {
            let (k, v) = item.unwrap();
            if !k.starts_with(&prefix) {
                break;
            }
            let rec: StreamRecord = postcard::from_bytes(&v).unwrap();
            count += 1;
            fnv = crate::verify::fnv1a(fnv, &rec.data);
        }
        (count, fnv)
    }

    pub fn finalize(&self) {
        self.db.flush_wal(true).unwrap();
        for cf in ["global", "stream"] {
            self.db.flush_cf(self.db.cf_handle(cf).unwrap()).unwrap();
        }
    }
}
