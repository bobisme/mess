//! Engine A — "Meridian slice": custom segment log (payload bytes stored once)
//! + fjall pointer index per D5. Log is commit authority (D1); the index is a
//! rebuildable cache written after the log append, at fjall's default
//! journal-buffered durability.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::seglog::{
    list_segments, scan, EventPtr, ReplayStats, SegmentLog, StopReason,
};

#[derive(Clone, Copy, Debug)]
pub enum Durability {
    /// No fsync: page cache accepted (survives process crash only).
    Buffered,
    /// fdatasync after each batch, before ack.
    SyncPerBatch,
    /// Dedicated fsync task; writers await an ack fired after the covering
    /// fdatasync. The task collects requests for up to `max_delay` after the
    /// first pending one, then syncs once for the whole group.
    Group(Duration),
}

struct SyncReq {
    seg_id: u64,
    file: Arc<File>,
    ack: oneshot::Sender<()>,
}

struct Inner {
    log: SegmentLog,
    heads: HashMap<u64, u64>, // stream -> last version
}

pub struct MeridianEngine {
    inner: Mutex<Inner>,
    db: fjall::Database,
    ptrs: fjall::Keyspace,
    headks: fjall::Keyspace,
    durability: Durability,
    sync_tx: Option<mpsc::Sender<SyncReq>>,
    files: RwLock<HashMap<u64, Arc<File>>>,
    log_dir: PathBuf,
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

fn key16(stream: u64, version: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&stream.to_be_bytes());
    k[8..].copy_from_slice(&version.to_be_bytes());
    k
}

fn be64(b: &[u8]) -> u64 {
    u64::from_be_bytes(b[..8].try_into().unwrap())
}

fn group_sync_loop(rx: mpsc::Receiver<SyncReq>, max_delay: Duration) {
    loop {
        let first = match rx.recv() {
            Ok(r) => r,
            Err(_) => return,
        };
        let deadline = Instant::now() + max_delay;
        let mut pending = vec![first];
        loop {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match rx.recv_timeout(deadline - now) {
                Ok(r) => pending.push(r),
                Err(_) => break,
            }
        }
        // One fdatasync per distinct segment file covers every pending batch.
        let mut synced: Vec<u64> = Vec::new();
        for req in &pending {
            if !synced.contains(&req.seg_id) {
                req.file.sync_data().unwrap();
                synced.push(req.seg_id);
            }
        }
        for req in pending {
            let _ = req.ack.send(());
        }
    }
}

impl MeridianEngine {
    pub fn open(dir: &Path, durability: Durability) -> (Self, OpenStats) {
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

        // Load stream heads from the index...
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
            // Segments before the last were sealed with an fdatasync and the
            // index was persisted at each roll; scan only the last segment
            // (A7: checkpoints advisory, segment boundaries align with batches).
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

            // ...then let the log overrule the index for everything the scan
            // saw (D1: log is the sole authority; index entries are rebuilt).
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

            // Truncate the torn tail (all-or-nothing per batch) and resume.
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
                split_writes: false,
            }
        };

        let sync_tx = if let Durability::Group(d) = durability {
            let (tx, rx) = mpsc::channel();
            std::thread::spawn(move || group_sync_loop(rx, d));
            Some(tx)
        } else {
            None
        };

        (
            MeridianEngine {
                inner: Mutex::new(Inner { log, heads }),
                db,
                ptrs,
                headks,
                durability,
                sync_tx,
                files: RwLock::new(files),
                log_dir,
            },
            stats,
        )
    }

    pub fn set_split_writes(&self, v: bool) {
        self.inner.lock().unwrap().log.split_writes = v;
    }

    pub fn head(&self, stream: u64) -> Option<u64> {
        self.inner.lock().unwrap().heads.get(&stream).copied()
    }

    pub fn heads_snapshot(&self) -> Vec<(u64, u64)> {
        let inner = self.inner.lock().unwrap();
        inner.heads.iter().map(|(&k, &v)| (k, v)).collect()
    }

    pub fn next_global_pos(&self) -> u64 {
        self.inner.lock().unwrap().log.next_global_pos
    }

    /// Append one batch of events to `stream`. Returns the first global
    /// position assigned. Ack semantics depend on the durability mode.
    pub async fn append_batch(
        &self,
        stream: u64,
        expected: Option<u64>,
        payloads: &[Vec<u8>],
    ) -> u64 {
        let (out, first_version) = {
            let mut inner = self.inner.lock().unwrap();
            let head = inner.heads.get(&stream).copied();
            assert_eq!(head, expected, "version conflict on stream {stream}");
            let first_version = head.map_or(0, |h| h + 1);
            let out = inner.log.append(stream, first_version, payloads);
            inner
                .heads
                .insert(stream, first_version + payloads.len() as u64 - 1);
            (out, first_version)
        };

        if let Some((id, f)) = &out.rolled {
            self.files.write().unwrap().insert(*id, f.clone());
            // Seal point: make the index durable through the previous segment
            // so recovery only ever needs to scan the last segment.
            self.db.persist(fjall::PersistMode::SyncAll).unwrap();
        }

        // Index AFTER the log append (D1). Journal-buffered: no fsync.
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

        match self.durability {
            Durability::Buffered => {}
            Durability::SyncPerBatch => out.file.sync_data().unwrap(),
            Durability::Group(_) => {
                let (tx, rx) = oneshot::channel();
                self.sync_tx
                    .as_ref()
                    .unwrap()
                    .send(SyncReq { seg_id: out.seg_id, file: out.file.clone(), ack: tx })
                    .unwrap();
                let _ = rx.await;
            }
        }
        out.first_global_pos
    }

    /// Sequential segment scan (the D1 recovery path doubles as global replay).
    pub fn read_global(&self, from: u64, limit: u64) -> ReplayStats {
        let mut st = ReplayStats::default();
        for seg in list_segments(&self.log_dir) {
            let data = fs::read(&seg.path).unwrap();
            st.bytes += data.len() as u64;
            let rec = scan(&data, 0, seg.base_pos);
            for b in &rec.batches {
                for (j, &(o, l)) in b.payloads.iter().enumerate() {
                    let pos = b.first_global_pos + j as u64;
                    if pos < from {
                        continue;
                    }
                    st.events += 1;
                    st.checksum = st
                        .checksum
                        .wrapping_add(data[o as usize] as u64 + l as u64);
                    if st.events >= limit {
                        return st;
                    }
                }
            }
        }
        st
    }

    /// Index lookup -> pointer reads into the segment files.
    pub fn read_stream(&self, stream: u64, from: u64, limit: u64) -> ReplayStats {
        let mut st = ReplayStats::default();
        let mut buf = Vec::new();
        for guard in self.ptrs.prefix(stream.to_be_bytes()) {
            let (k, v) = guard.into_inner().unwrap();
            let ver = be64(&k[8..16]);
            if ver < from {
                continue;
            }
            let ptr = EventPtr::decode(&v);
            let file = self.seg_file(ptr.segment_id);
            buf.resize(ptr.len as usize, 0);
            file.read_exact_at(&mut buf, ptr.offset).unwrap();
            st.events += 1;
            st.bytes += ptr.len as u64;
            st.checksum = st.checksum.wrapping_add(buf[0] as u64 + ptr.len as u64);
            if st.events >= limit {
                break;
            }
        }
        st
    }

    fn seg_file(&self, id: u64) -> Arc<File> {
        if let Some(f) = self.files.read().unwrap().get(&id) {
            return f.clone();
        }
        let seg = list_segments(&self.log_dir)
            .into_iter()
            .find(|s| s.id == id)
            .unwrap_or_else(|| panic!("dangling EventPtr to segment {id}"));
        let f = Arc::new(File::open(&seg.path).unwrap());
        self.files.write().unwrap().insert(id, f.clone());
        f
    }

    /// Untimed: make everything durable and flush the index memtables so the
    /// on-disk footprint reflects the LSM representation.
    pub fn finalize(&self) {
        let file = self.inner.lock().unwrap().log.file.clone();
        file.sync_data().unwrap();
        self.db.persist(fjall::PersistMode::SyncAll).unwrap();
        self.ptrs.rotate_memtable_and_wait().unwrap();
        self.headks.rotate_memtable_and_wait().unwrap();
    }
}
