use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    path::Path,
    sync::{Mutex, atomic::AtomicU64},
};

use rocksdb::{ColumnFamilyDescriptor, ColumnFamilyRef, Options};
use tracing::debug;

use crate::error::Result;

pub struct DB {
    db:                           ::rocksdb::DB,
    /// Last written global position. 0 = unknown; lazily filled by scanning
    /// the global CF, advanced on every successful write.
    pub(crate) cached_global:     AtomicU64,
    /// Authoritative in-memory last-written stream position per stream, keyed
    /// by stream name. The actor is the sole writer for this DB's lifetime, so
    /// this map is complete for every stream this process has written to. It
    /// lets `ExpectedVersion::Any` appends assign the next stream position
    /// without a disk head read (dx_api friction #3). Absence means "no event
    /// written to this stream yet" (empty stream).
    pub(crate) stream_heads:      Mutex<HashMap<String, u64>>,
    /// Count of disk stream-head reads (`get_last_stream_position`). Real
    /// instrumentation, not a comment: the write-path tests assert an
    /// `ExpectedVersion::Any` append performs zero of these.
    pub(crate) stream_head_reads: AtomicU64,
}

fn opts() -> Options {
    let mut opts = Options::default();
    opts.create_missing_column_families(true);
    opts.create_if_missing(true);
    let cores = std::thread::available_parallelism().unwrap().get();
    opts.increase_parallelism(cores as i32);
    opts
}

fn new_cf(name: &str) -> ColumnFamilyDescriptor {
    ColumnFamilyDescriptor::new(name, opts())
}

impl DB {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        debug!(path = %path.as_ref().to_string_lossy(), "opened db");

        let db_opts = opts();
        // let cf_opts = Options::default();
        let db = rocksdb::DB::open_cf_descriptors(
            &db_opts,
            path,
            vec![new_cf("global"), new_cf("stream")],
        )?;
        Ok(Self {
            db,
            cached_global: AtomicU64::new(0),
            stream_heads: Mutex::new(HashMap::new()),
            stream_head_reads: AtomicU64::new(0),
        })
    }

    #[must_use]
    pub fn global(&self) -> ColumnFamilyRef<'_> {
        self.db.cf_handle("global").expect("no global column family")
    }

    #[must_use]
    pub fn stream(&self) -> ColumnFamilyRef<'_> {
        self.db.cf_handle("stream").expect("no stream column family")
    }

    /// Last-written stream position for `stream` from the in-memory cache, or
    /// `None` if this process has not written to it (treated as empty).
    /// Performs no disk I/O.
    pub(crate) fn cached_stream_head(&self, stream: &str) -> Option<u64> {
        self.stream_heads.lock().unwrap().get(stream).copied()
    }

    /// Record `pos` as the stream's last-written position. Monotonic: never
    /// moves a stream's head backwards.
    pub(crate) fn set_stream_head(&self, stream: &str, pos: u64) {
        let mut heads = self.stream_heads.lock().unwrap();
        heads
            .entry(stream.to_string())
            .and_modify(|h| *h = (*h).max(pos))
            .or_insert(pos);
    }

    /// Number of disk stream-head reads performed so far (test
    /// instrumentation).
    #[cfg(test)]
    pub(crate) fn stream_head_reads(&self) -> u64 {
        self.stream_head_reads.load(std::sync::atomic::Ordering::Acquire)
    }
}

impl Deref for DB {
    type Target = ::rocksdb::DB;

    fn deref(&self) -> &Self::Target { &self.db }
}

impl DerefMut for DB {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.db }
}

#[cfg(test)]
pub(crate) mod test {
    use ident::Id;

    use super::DB;

    pub(crate) struct SelfDestructingDB(Option<DB>);

    impl SelfDestructingDB {
        pub(crate) fn new_tmp() -> Self {
            let path = std::env::temp_dir();
            let path = path.join(Id::new().to_string());
            SelfDestructingDB(Some(DB::new(path).unwrap()))
        }
    }

    impl std::ops::Deref for SelfDestructingDB {
        type Target = DB;

        fn deref(&self) -> &Self::Target { self.0.as_ref().unwrap() }
    }

    impl std::ops::DerefMut for SelfDestructingDB {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.0.as_mut().unwrap()
        }
    }

    impl Drop for SelfDestructingDB {
        fn drop(&mut self) {
            let path = self.path().to_owned();
            drop(std::mem::take(&mut self.0));
            ::rocksdb::DB::destroy(&rocksdb::Options::default(), path).unwrap();
        }
    }
}
