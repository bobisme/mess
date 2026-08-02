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
    use std::{
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use ident::Id;

    use super::DB;

    /// Mint a fresh, **collision-proof** temp-directory path for a test
    /// RocksDB, and create the directory with `O_EXCL` semantics.
    ///
    /// bn-2lir. The previous name was just `temp_dir()/<ident::Id>`, on the
    /// assumption that an `Id` collision across the ~29 test processes
    /// nextest spawns needs ~2^-64 luck. It does not. `ident::Id::new()` is
    /// `(31.25ms-bucket << 64) | (fastrand::u128(..) & 2^64-1)`, and
    /// `fastrand` 2.5's thread-local RNG is seeded by `random_seed()` =
    /// `DefaultHasher::new()` — SipHash-1-3 with **fixed, all-zero keys**,
    /// not `RandomState` — over exactly two inputs: `Instant::now()`
    /// (`CLOCK_MONOTONIC`) and `thread::current().id()`. In a freshly
    /// `exec`'d nextest child the thread id is a constant, so the whole
    /// 64-bit "random" half of the `Id` is a pure function of one
    /// monotonic-clock *nanosecond* reading. Two processes that read the
    /// same nanosecond mint the same `Id` — and, being in the same
    /// nanosecond, necessarily the same 31.25ms bucket, so the same full
    /// path. Measured on this host, not argued: burst-spawning 400_000
    /// processes produced 14 duplicate `Id`s, and every one of them came
    /// with a duplicated `fastrand::get_seed()`. Matched to this suite —
    /// 4000 bursts of 29 simultaneous processes, the number of tests here
    /// that mint a DB — 2 bursts collided, i.e. ~5e-4 per suite run, some
    /// 16 orders of magnitude above the 2^-64 the old name assumed. Rare
    /// enough to be seen once and never reproduced; nowhere near impossible.
    ///
    /// Two live processes on the same path is exactly the observed failure:
    /// the first holds `fcntl(F_SETLK, F_WRLCK)` on `<path>/LOCK` and the
    /// second gets `EAGAIN` -> "While lock file: <path>/LOCK: Resource
    /// temporarily unavailable" (rocksdb 8.1.1 `env/fs_posix.cc:785-786`).
    /// Note that this can only ever be a *cross-process* conflict: POSIX
    /// record locks are per-(process, inode) and never self-conflict, and
    /// RocksDB's own same-process guard is a path-keyed `locked_files` map
    /// that reports a different error entirely (`ENOLCK`, "lock hold by
    /// current process", `fs_posix.cc:761-773`).
    ///
    /// So the fix is to make the name unique among *live processes* rather
    /// than merely improbable: `<pid>` cannot be shared by two live
    /// processes, and `<seq>` cannot repeat within one. The `Id` is kept
    /// only so that a *recycled* pid cannot land on a directory leaked by a
    /// long-dead run, and so the names stay time-sortable by eye. Deliberately
    /// no retry/sleep anywhere: uniqueness here is structural.
    pub(crate) fn tmp_db_path() -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir()
            .join(format!("mess_db-{pid}-{seq}-{}", Id::new()));
        // `create_dir` is exclusive (`O_EXCL`-equivalent): it turns any
        // residual name reuse into this crisp panic instead of a RocksDB
        // lock error or, worse, a silent open of somebody else's data.
        // Given pid+seq this is unreachable; if it ever fires, the report
        // below says who and what is there.
        if let Err(e) = std::fs::create_dir(&path) {
            panic!("{}", tmp_db_report(&path, &format!("create_dir: {e}")));
        }
        path
    }

    /// bn-2lir: everything needed to decide the mechanism from ONE future
    /// occurrence, rendered into the panic message — because the 2026-07-31
    /// report had none of it and cost a full investigation.
    pub(crate) fn tmp_db_report(path: &Path, err: &str) -> String {
        use std::fmt::Write as _;

        let mut s = String::new();
        let _ = write!(
            s,
            "mess_db tmp RocksDB failed at {}: {err}",
            path.display()
        );
        let _ = write!(
            s,
            "\n  pid={} thread={:?} name={:?}",
            std::process::id(),
            std::thread::current().id(),
            std::thread::current().name(),
        );
        match std::fs::read_dir(path) {
            Ok(entries) => {
                let mut names: Vec<String> = entries
                    .filter_map(|e| {
                        Some(e.ok()?.file_name().to_string_lossy().into_owned())
                    })
                    .collect();
                names.sort();
                let _ = write!(
                    s,
                    "\n  dir exists, {} entries: {names:?}",
                    names.len()
                );
            }
            Err(e) => {
                let _ = write!(s, "\n  dir unreadable: {e}");
            }
        }
        let open_fds = std::fs::read_dir("/proc/self/fd").map(Iterator::count);
        let _ = write!(s, "\n  open fds: {open_fds:?}");
        // Any sibling temp dir minted by *this* pid. A LOCK conflict on a
        // path carrying our own pid would mean the conflict is NOT a name
        // collision, which is the single most valuable bit to know next time.
        let mine = format!("mess_db-{}-", std::process::id());
        let siblings: Vec<String> = std::fs::read_dir(std::env::temp_dir())
            .into_iter()
            .flatten()
            .filter_map(|e| {
                Some(e.ok()?.file_name().to_string_lossy().into_owned())
            })
            .filter(|n| n.starts_with(&mine))
            .collect();
        let _ = write!(s, "\n  live tmp dirs for this pid: {siblings:?}");
        s
    }

    pub(crate) struct SelfDestructingDB(Option<DB>);

    impl SelfDestructingDB {
        pub(crate) fn new_tmp() -> Self {
            let path = tmp_db_path();
            let db = DB::new(&path).unwrap_or_else(|e| {
                panic!("{}", tmp_db_report(&path, &format!("DB::new: {e}")))
            });
            SelfDestructingDB(Some(db))
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
            // Close the handle — releasing its `fcntl` lock on `<path>/LOCK`
            // — strictly BEFORE anything touches the directory.
            drop(std::mem::take(&mut self.0));
            if let Err(e) =
                ::rocksdb::DB::destroy(&rocksdb::Options::default(), &path)
            {
                // Always reclaim the directory; `/tmp` is shared with every
                // other concurrent run.
                let _ = std::fs::remove_dir_all(&path);
                // bn-2lir: the old code was a bare `.unwrap()` here. Panicking
                // inside `drop` while another panic is already unwinding
                // aborts the process, replacing the test's real failure with
                // an abort and no message — exactly the wrong thing for a
                // flake investigation. Report only when we are not already
                // unwinding.
                if !std::thread::panicking() {
                    panic!(
                        "{}",
                        tmp_db_report(&path, &format!("DB::destroy: {e}"))
                    );
                }
            }
        }
    }

    #[cfg(test)]
    mod tmp_path_tests {
        use super::*;

        /// bn-2lir, the mechanism itself, pinned: two `fastrand` states that
        /// agree — the real-world case being two processes whose thread-local
        /// seed `hash(Instant::now(), ThreadId)` collided on one
        /// monotonic-clock nanosecond — draw the *identical* randomness, so
        /// they mint the identical `ident::Id`.
        #[test]
        fn equal_fastrand_seeds_draw_equal_id_randomness() {
            const SEED: u64 = 0x2117;
            fastrand::seed(SEED);
            let a = fastrand::u128(..);
            fastrand::seed(SEED);
            let b = fastrand::u128(..);
            assert_eq!(
                a, b,
                "ident::Id's 64 random bits are a pure function of the \
                 fastrand seed; a seed collision is an Id collision"
            );
        }

        /// …and the temp path must survive that anyway, because its
        /// uniqueness comes from pid+seq, not from the `Id`.
        #[test]
        fn tmp_db_paths_differ_under_a_pinned_rng() {
            const SEED: u64 = 0x2117;
            fastrand::seed(SEED);
            let a = tmp_db_path();
            fastrand::seed(SEED);
            let b = tmp_db_path();
            let cleanup = |p: &std::path::Path| {
                let _ = std::fs::remove_dir(p);
            };
            cleanup(&a);
            cleanup(&b);
            assert_ne!(
                a, b,
                "two identically-seeded RNGs must still mint distinct temp \
                 paths (pre-bn-2lir these were byte-identical)"
            );
            let pid_tag = format!("mess_db-{}-", std::process::id());
            for p in [&a, &b] {
                let name =
                    p.file_name().unwrap().to_string_lossy().into_owned();
                assert!(
                    name.starts_with(&pid_tag),
                    "temp dir {name} must carry this pid so no other live \
                     process can mint it"
                );
            }
        }
    }
}
