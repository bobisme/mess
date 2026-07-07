//! Mutex-sequenced engine: the vertical_slice Engine A shape (global mutex
//! around position assignment + log append), with the index write strategy as
//! a knob:
//!
//! - `PerEvent`  — baseline: one synchronous fjall insert per event + one head
//!                 insert per batch (exactly vertical_slice Engine A).
//! - `None`      — log only (reproduces the ~224k ev/s "log alone" number).
//! - `Batched`   — F1 fix: all index inserts for a commit go into one fjall
//!                 WriteBatch (one journal write per commit).
//! - `InMem`     — D5 endgame upper bound: in-memory index, entries persisted
//!                 to fjall only at seal (segment roll); tail is rebuilt from
//!                 the last segment on recovery.
//!
//! Durability: BUFFERED only (the spike's workload).

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use crate::seglog::{list_segments, scan, EventPtr, SegmentLog, StopReason};
use crate::workload::STREAMS;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexMode {
    PerEvent,
    None,
    Batched,
    InMem,
}

pub fn key16(stream: u64, version: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&stream.to_be_bytes());
    k[8..].copy_from_slice(&version.to_be_bytes());
    k
}

pub fn be64(b: &[u8]) -> u64 {
    u64::from_be_bytes(b[..8].try_into().unwrap())
}

struct Inner {
    log: SegmentLog,
    heads: HashMap<u64, u64>, // stream -> last version
    // InMem mode state:
    mem: Vec<Vec<EventPtr>>, // stream -> ptrs (version = index)
    journal: Vec<(u64, u64, EventPtr)>, // (stream, version, ptr) not yet sealed
}

#[derive(Debug, Default, Clone, Copy)]
pub struct OpenStats {
    pub scanned_batches: usize,
    pub scanned_bytes: u64,
    pub repaired_batches: usize,
    pub truncated_bytes: u64,
    pub next_global_pos: u64,
    pub heads_loaded: usize,
    pub stop: Option<StopReason>,
}

pub struct MutexEngine {
    inner: Mutex<Inner>,
    pub db: fjall::Database,
    pub ptrs: fjall::Keyspace,
    pub headks: fjall::Keyspace,
    mode: IndexMode,
    pub files: RwLock<HashMap<u64, Arc<File>>>,
    pub log_dir: PathBuf,
}

impl MutexEngine {
    /// Open (or create) — identical recovery semantics to vertical_slice
    /// Engine A: load heads from the index, scan ONLY the last segment, let
    /// the log overrule the index (repair), truncate any torn tail.
    pub fn open(dir: &Path, mode: IndexMode) -> (Self, OpenStats) {
        fs::create_dir_all(dir).unwrap();
        let log_dir = dir.join("log");
        fs::create_dir_all(&log_dir).unwrap();

        let db = fjall::Database::open(fjall::Config::new(&dir.join("index"))).unwrap();
        let ptrs = db
            .keyspace("ptrs", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let headks = db
            .keyspace("heads", fjall::KeyspaceCreateOptions::default)
            .unwrap();

        let mut heads: HashMap<u64, u64> = HashMap::new();
        for guard in headks.prefix([0u8; 0]) {
            let (k, v) = guard.into_inner().unwrap();
            heads.insert(be64(&k), be64(&v));
        }

        let mut stats = OpenStats { heads_loaded: heads.len(), ..Default::default() };

        let segs = list_segments(&log_dir);
        let mut files: HashMap<u64, Arc<File>> = HashMap::new();
        let log = if segs.is_empty() {
            let log = SegmentLog::create(&log_dir);
            files.insert(0, log.file.clone());
            log
        } else {
            for s in &segs[..segs.len() - 1] {
                files.insert(s.id, Arc::new(File::open(&s.path).unwrap()));
            }
            let last = segs.last().unwrap();
            let data = fs::read(&last.path).unwrap();
            let rec = scan(&data, 0, last.base_pos);
            stats.scanned_bytes = data.len() as u64;
            stats.scanned_batches = rec.batches.len();
            stats.truncated_bytes = data.len() as u64 - rec.safe_offset;
            stats.stop = Some(rec.stop);
            stats.next_global_pos = rec.next_global_pos;

            for b in &rec.batches {
                let n = b.payloads.len() as u64;
                let last_ver = b.first_stream_version + n - 1;
                heads.insert(b.stream_id, last_ver);
                let (off, len) = *b.payloads.last().unwrap();
                let want = EventPtr { segment_id: last.id, offset: off, len }.encode();
                let have = ptrs.get(key16(b.stream_id, last_ver)).unwrap();
                if have.as_deref() != Some(&want[..]) {
                    for (i, &(o, l)) in b.payloads.iter().enumerate() {
                        let ptr = EventPtr { segment_id: last.id, offset: o, len: l };
                        ptrs.insert(
                            key16(b.stream_id, b.first_stream_version + i as u64),
                            ptr.encode(),
                        )
                        .unwrap();
                    }
                    stats.repaired_batches += 1;
                }
                headks
                    .insert(b.stream_id.to_be_bytes(), last_ver.to_be_bytes())
                    .unwrap();
            }

            let file = OpenOptions::new()
                .read(true)
                .append(true)
                .open(&last.path)
                .unwrap();
            file.set_len(rec.safe_offset).unwrap();
            let file = Arc::new(file);
            files.insert(last.id, file.clone());
            SegmentLog {
                dir: log_dir.clone(),
                seg_id: last.id,
                seg_base: last.base_pos,
                file,
                seg_len: rec.safe_offset,
                next_batch_id: rec.next_batch_id,
                next_global_pos: rec.next_global_pos,
            }
        };

        (
            MutexEngine {
                inner: Mutex::new(Inner {
                    log,
                    heads,
                    mem: vec![Vec::new(); STREAMS as usize],
                    journal: Vec::new(),
                }),
                db,
                ptrs,
                headks,
                mode,
                files: RwLock::new(files),
                log_dir,
            },
            stats,
        )
    }

    pub fn head(&self, stream: u64) -> Option<u64> {
        self.inner.lock().unwrap().heads.get(&stream).copied()
    }

    /// Append one batch (BUFFERED: ack after the buffered write + index apply).
    pub fn append_batch(&self, stream: u64, expected: Option<u64>, payloads: &[Vec<u8>]) -> u64 {
        let (out, first_version) = {
            let mut inner = self.inner.lock().unwrap();
            let head = inner.heads.get(&stream).copied();
            assert_eq!(head, expected, "version conflict on stream {stream}");
            let first_version = head.map_or(0, |h| h + 1);
            let out = inner.log.append(stream, first_version, payloads);
            inner
                .heads
                .insert(stream, first_version + payloads.len() as u64 - 1);

            if self.mode == IndexMode::InMem {
                if out.rolled.is_some() {
                    // Seal: persist every not-yet-sealed index entry in one
                    // fjall write batch, durably. (Held under the engine lock;
                    // happens once per 256 MiB segment — untimed-path cost,
                    // noted in the report.)
                    let mut wb = fjall::OwnedWriteBatch::with_capacity(
                        self.db.clone(),
                        inner.journal.len() + inner.heads.len(),
                    );
                    for &(s, v, p) in &inner.journal {
                        wb.insert(&self.ptrs, key16(s, v), p.encode());
                    }
                    for (&s, &h) in inner.heads.iter() {
                        wb.insert(&self.headks, s.to_be_bytes(), h.to_be_bytes());
                    }
                    wb.commit().unwrap();
                    self.db.persist(fjall::PersistMode::SyncAll).unwrap();
                    inner.journal.clear();
                }
                for (i, ptr) in out.ptrs.iter().enumerate() {
                    let v = first_version + i as u64;
                    debug_assert_eq!(inner.mem[stream as usize].len() as u64, v);
                    inner.mem[stream as usize].push(*ptr);
                    inner.journal.push((stream, v, *ptr));
                }
            }
            (out, first_version)
        };

        if let Some((id, f)) = &out.rolled {
            self.files.write().unwrap().insert(*id, f.clone());
            if matches!(self.mode, IndexMode::PerEvent | IndexMode::Batched) {
                // Seal point: make the index durable through the previous
                // segment so recovery only ever needs to scan the last one.
                self.db.persist(fjall::PersistMode::SyncAll).unwrap();
            }
        }

        // Index AFTER the log append (D1). Journal-buffered: no fsync.
        match self.mode {
            IndexMode::PerEvent => {
                for (i, ptr) in out.ptrs.iter().enumerate() {
                    self.ptrs
                        .insert(key16(stream, first_version + i as u64), ptr.encode())
                        .unwrap();
                }
                self.headks
                    .insert(
                        stream.to_be_bytes(),
                        (first_version + out.ptrs.len() as u64 - 1).to_be_bytes(),
                    )
                    .unwrap();
            }
            IndexMode::Batched => {
                let mut wb =
                    fjall::OwnedWriteBatch::with_capacity(self.db.clone(), out.ptrs.len() + 1);
                for (i, ptr) in out.ptrs.iter().enumerate() {
                    wb.insert(&self.ptrs, key16(stream, first_version + i as u64), ptr.encode());
                }
                wb.insert(
                    &self.headks,
                    stream.to_be_bytes(),
                    (first_version + out.ptrs.len() as u64 - 1).to_be_bytes(),
                );
                wb.commit().unwrap();
            }
            IndexMode::None | IndexMode::InMem => {}
        }

        out.first_global_pos
    }

    pub fn next_global_pos(&self) -> u64 {
        self.inner.lock().unwrap().log.next_global_pos
    }

    /// Pointer list for one stream, in version order (verification read path).
    pub fn stream_ptrs(&self, stream: u64) -> Option<Vec<EventPtr>> {
        match self.mode {
            IndexMode::None => None,
            IndexMode::InMem => Some(self.inner.lock().unwrap().mem[stream as usize].clone()),
            IndexMode::PerEvent | IndexMode::Batched => {
                let mut out = Vec::new();
                for guard in self.ptrs.prefix(stream.to_be_bytes()) {
                    let (k, v) = guard.into_inner().unwrap();
                    let ver = be64(&k[8..16]);
                    assert_eq!(ver, out.len() as u64, "index gap on stream {stream}");
                    out.push(EventPtr::decode(&v));
                }
                Some(out)
            }
        }
    }

    pub fn seg_files(&self) -> HashMap<u64, Arc<File>> {
        self.files.read().unwrap().clone()
    }

    /// Untimed: durable log + index, memtables rotated so `du` reflects LSM.
    pub fn finalize(&self) {
        let file = self.inner.lock().unwrap().log.file.clone();
        file.sync_data().unwrap();
        self.db.persist(fjall::PersistMode::SyncAll).unwrap();
        self.ptrs.rotate_memtable_and_wait().unwrap();
        self.headks.rotate_memtable_and_wait().unwrap();
    }
}
