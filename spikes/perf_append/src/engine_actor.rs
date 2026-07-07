//! Actor engine (F8a): a dedicated appender thread owns position assignment,
//! stream heads, the segment log, and the index — writers hand off batch
//! requests over an MPSC channel and block on a per-writer reply channel
//! (depth 1 in flight per writer, same ack semantics as the baseline).
//! This mirrors the mess_db actor design.
//!
//! Knobs:
//! - `WriteMode::PerBatch` — allocate + encode + write() one batch at a time
//!   (baseline encode path, only the sequencing changed).
//! - `WriteMode::Group`    — encode every drained request into one reusable
//!   contiguous buffer and issue a single write() for the whole group
//!   (hypothesis 3: kill per-event allocations + amortize syscalls).
//! - `ActorIndex::FjallBatch` — one fjall WriteBatch per drained group (F1).
//! - `ActorIndex::InMem`      — in-memory index; fjall written only at seal
//!   (D5 endgame), tail rebuilt from the last segment on recovery.
//! - `async_seal` — seal work (old-segment fdatasync + index dump + persist)
//!   runs on a background sealer thread instead of stalling the appender
//!   (the seal_pipeline design). Caveat: until the sealer finishes, a crash
//!   would require scanning one extra segment on recovery (the F6 watermark
//!   question); the appender never blocks on it.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::{mpsc, Arc};

use crate::engine_mutex::key16;
use crate::seglog::{batch_len, encode_batch_into, EventPtr, SegmentLog, SEGMENT_SIZE};
use crate::workload::{BatchSpec, STREAMS};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    PerBatch,
    Group,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActorIndex {
    FjallBatch,
    InMem,
}

const MAX_GROUP: usize = 256;

struct Req {
    idx: u32,
    writer: u8,
}

/// State handed back by the actor thread at shutdown, used for reads/verify.
pub struct FinalState {
    pub files: HashMap<u64, Arc<File>>,
    pub mem: Vec<Vec<EventPtr>>, // InMem mode; empty otherwise
    pub last_file: Arc<File>,
    pub next_global_pos: u64,
}

struct Actor {
    master: Arc<Vec<BatchSpec>>,
    log: SegmentLog,
    heads: HashMap<u64, u64>,
    files: HashMap<u64, Arc<File>>,
    buf: Vec<u8>,
    // (stream, version, ptr) entries produced since the last index apply.
    scratch: Vec<(u64, u64, EventPtr)>,
    // Per-request head updates for the current group: (writer, stream, last_version, pos).
    acks: Vec<(u8, u64, u64, u64)>,
    write_mode: WriteMode,
    index_mode: ActorIndex,
    db: fjall::Database,
    ptrs: fjall::Keyspace,
    headks: fjall::Keyspace,
    // InMem state:
    mem: Vec<Vec<EventPtr>>,
    journal: Vec<(u64, u64, EventPtr)>,
    replies: Vec<mpsc::Sender<u64>>,
    // Async seal pipeline (None = seal inline).
    seal_tx: Option<mpsc::Sender<SealReq>>,
    sealer: Option<std::thread::JoinHandle<()>>,
}

/// Work shipped to the background sealer thread.
struct SealReq {
    old_file: Arc<File>,
    journal: Vec<(u64, u64, EventPtr)>,
    heads: Vec<(u64, u64)>,
}

fn sealer_loop(
    rx: mpsc::Receiver<SealReq>,
    db: fjall::Database,
    ptrs: fjall::Keyspace,
    headks: fjall::Keyspace,
) {
    while let Ok(req) = rx.recv() {
        // Same ordering as the inline seal: old segment durable first, then
        // its index entries, then a durable index persist.
        req.old_file.sync_data().unwrap();
        let mut wb = fjall::OwnedWriteBatch::with_capacity(
            db.clone(),
            req.journal.len() + req.heads.len(),
        );
        for &(s, v, p) in &req.journal {
            wb.insert(&ptrs, key16(s, v), p.encode());
        }
        for &(s, h) in &req.heads {
            wb.insert(&headks, s.to_be_bytes(), h.to_be_bytes());
        }
        wb.commit().unwrap();
        db.persist(fjall::PersistMode::SyncAll).unwrap();
    }
}

impl Actor {
    fn flush_buf(&mut self) {
        if !self.buf.is_empty() {
            (&*self.log.file).write_all(&self.buf).unwrap();
            self.log.seg_len += self.buf.len() as u64;
            self.buf.clear();
        }
    }

    /// Apply pending scratch index entries (they all correspond to bytes
    /// already handed to write()).
    fn apply_index(&mut self) {
        if self.scratch.is_empty() {
            return;
        }
        match self.index_mode {
            ActorIndex::FjallBatch => {
                let mut wb = fjall::OwnedWriteBatch::with_capacity(
                    self.db.clone(),
                    self.scratch.len() + self.acks.len(),
                );
                for &(s, v, p) in &self.scratch {
                    wb.insert(&self.ptrs, key16(s, v), p.encode());
                }
                for &(_, s, last_ver, _) in &self.acks {
                    wb.insert(&self.headks, s.to_be_bytes(), last_ver.to_be_bytes());
                }
                wb.commit().unwrap();
            }
            ActorIndex::InMem => {
                for &(s, v, p) in &self.scratch {
                    debug_assert_eq!(self.mem[s as usize].len() as u64, v);
                    self.mem[s as usize].push(p);
                    self.journal.push((s, v, p));
                }
            }
        }
        self.scratch.clear();
    }

    /// Roll into a new segment: flush, index-apply, seal durably, roll.
    /// With `seal_tx` set, the seal (old-segment fdatasync + index dump +
    /// persist) is shipped to the background sealer instead.
    fn seal_and_roll(&mut self) {
        self.flush_buf();
        self.apply_index();
        if let Some(tx) = &self.seal_tx {
            // Async path (InMem index only).
            let journal = std::mem::take(&mut self.journal);
            let heads: Vec<(u64, u64)> = self.heads.iter().map(|(&s, &h)| (s, h)).collect();
            let (old, id, f) = self.log.roll_nosync();
            tx.send(SealReq { old_file: old, journal, heads }).unwrap();
            self.files.insert(id, f);
            return;
        }
        if self.index_mode == ActorIndex::InMem {
            let mut wb = fjall::OwnedWriteBatch::with_capacity(
                self.db.clone(),
                self.journal.len() + self.heads.len(),
            );
            for &(s, v, p) in &self.journal {
                wb.insert(&self.ptrs, key16(s, v), p.encode());
            }
            for (&s, &h) in self.heads.iter() {
                wb.insert(&self.headks, s.to_be_bytes(), h.to_be_bytes());
            }
            wb.commit().unwrap();
            self.journal.clear();
        }
        let (id, f) = self.log.roll(); // fdatasyncs the old segment
        self.db.persist(fjall::PersistMode::SyncAll).unwrap();
        self.files.insert(id, f);
    }

    fn process_group(&mut self, reqs: &[Req]) {
        self.acks.clear();
        let master = self.master.clone(); // one Arc clone per group, frees &mut self
        for req in reqs {
            let b = &master[req.idx as usize];
            let head = self.heads.get(&b.stream).copied();
            assert_eq!(head, b.expected, "version conflict on stream {}", b.stream);
            let first_version = head.map_or(0, |h| h + 1);
            let n = b.payloads.len() as u64;
            let blen = batch_len(&b.payloads) as u64;

            let virt = self.log.seg_len + self.buf.len() as u64;
            if virt + blen > SEGMENT_SIZE && virt > 0 {
                self.seal_and_roll();
            }

            match self.write_mode {
                WriteMode::Group => {
                    let base = self.log.seg_len + self.buf.len() as u64;
                    let batch_start = self.buf.len() as u64;
                    let seg_id = self.log.seg_id;
                    let stream = b.stream;
                    let scratch = &mut self.scratch;
                    let mut k = 0u64;
                    encode_batch_into(
                        &mut self.buf,
                        self.log.next_batch_id,
                        self.log.next_global_pos,
                        stream,
                        first_version,
                        &b.payloads,
                        |off, len| {
                            scratch.push((
                                stream,
                                first_version + k,
                                EventPtr {
                                    segment_id: seg_id,
                                    offset: base + (off as u64 - batch_start),
                                    len,
                                },
                            ));
                            k += 1;
                        },
                    );
                }
                WriteMode::PerBatch => {
                    // Baseline encode path: allocate, encode, write() per batch.
                    let out = self.log.append(b.stream, first_version, &b.payloads);
                    debug_assert!(out.rolled.is_none()); // roll handled above
                    for (i, p) in out.ptrs.iter().enumerate() {
                        self.scratch.push((b.stream, first_version + i as u64, *p));
                    }
                }
            }

            let pos = if self.write_mode == WriteMode::Group {
                let pos = self.log.next_global_pos;
                self.log.next_batch_id += 1;
                self.log.next_global_pos += n;
                pos
            } else {
                self.log.next_global_pos - n
            };
            self.heads.insert(b.stream, first_version + n - 1);
            self.acks.push((req.writer, b.stream, first_version + n - 1, pos));
        }

        // Log first (single write for the whole group), then index, then acks.
        self.flush_buf();
        self.apply_index();
        for i in 0..self.acks.len() {
            let (w, _, _, pos) = self.acks[i];
            self.replies[w as usize].send(pos).unwrap();
        }
    }

    fn run(mut self, rx: crossbeam_channel::Receiver<Req>) -> FinalState {
        let mut reqs: Vec<Req> = Vec::with_capacity(MAX_GROUP);
        loop {
            let first = match rx.recv() {
                Ok(r) => r,
                Err(_) => break,
            };
            reqs.clear();
            reqs.push(first);
            while reqs.len() < MAX_GROUP {
                match rx.try_recv() {
                    Ok(r) => reqs.push(r),
                    Err(_) => break,
                }
            }
            self.process_group(&reqs);
        }
        // Drain the seal pipeline before reporting final state.
        drop(self.seal_tx.take());
        if let Some(h) = self.sealer.take() {
            h.join().unwrap();
        }
        FinalState {
            files: self.files,
            mem: self.mem,
            last_file: self.log.file.clone(),
            next_global_pos: self.log.next_global_pos,
        }
    }
}

pub struct ActorEngine {
    tx: Option<crossbeam_channel::Sender<Req>>,
    join: Option<std::thread::JoinHandle<FinalState>>,
    pub db: fjall::Database,
    pub ptrs: fjall::Keyspace,
    pub headks: fjall::Keyspace,
    index_mode: ActorIndex,
    pub final_state: Option<FinalState>,
}

/// Per-writer handle: submit + block for ack (depth 1, like the baseline).
pub struct WriterHandle {
    tx: crossbeam_channel::Sender<Req>,
    rx: mpsc::Receiver<u64>,
    writer: u8,
}

impl WriterHandle {
    pub fn append(&mut self, idx: u32) -> u64 {
        self.tx.send(Req { idx, writer: self.writer }).unwrap();
        self.rx.recv().unwrap()
    }
}

impl ActorEngine {
    pub fn new(
        dir: &Path,
        master: Arc<Vec<BatchSpec>>,
        writers: usize,
        write_mode: WriteMode,
        index_mode: ActorIndex,
        async_seal: bool,
    ) -> (Self, Vec<WriterHandle>) {
        assert!(!async_seal || index_mode == ActorIndex::InMem);
        fs::create_dir_all(dir).unwrap();
        let log_dir = dir.join("log");
        let db = fjall::Database::open(fjall::Config::new(&dir.join("index"))).unwrap();
        let ptrs = db
            .keyspace("ptrs", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let headks = db
            .keyspace("heads", fjall::KeyspaceCreateOptions::default)
            .unwrap();

        let (tx, rx) = crossbeam_channel::bounded::<Req>(4096);
        let mut handles = Vec::with_capacity(writers);
        let mut replies = Vec::with_capacity(writers);
        for w in 0..writers {
            let (rtx, rrx) = mpsc::channel();
            replies.push(rtx);
            handles.push(WriterHandle { tx: tx.clone(), rx: rrx, writer: w as u8 });
        }

        let (seal_tx, sealer) = if async_seal {
            let (stx, srx) = mpsc::channel::<SealReq>();
            let (dbc, pc, hc) = (db.clone(), ptrs.clone(), headks.clone());
            let h = std::thread::Builder::new()
                .name("sealer".into())
                .spawn(move || sealer_loop(srx, dbc, pc, hc))
                .unwrap();
            (Some(stx), Some(h))
        } else {
            (None, None)
        };

        let log = SegmentLog::create(&log_dir);
        let mut files = HashMap::new();
        files.insert(0, log.file.clone());
        let actor = Actor {
            master,
            log,
            heads: HashMap::new(),
            files,
            buf: Vec::with_capacity(4 << 20),
            scratch: Vec::with_capacity(MAX_GROUP * 16),
            acks: Vec::with_capacity(MAX_GROUP),
            write_mode,
            index_mode,
            db: db.clone(),
            ptrs: ptrs.clone(),
            headks: headks.clone(),
            mem: if index_mode == ActorIndex::InMem {
                vec![Vec::new(); STREAMS as usize]
            } else {
                Vec::new()
            },
            journal: Vec::new(),
            replies,
            seal_tx,
            sealer,
        };
        let join = std::thread::Builder::new()
            .name("appender".into())
            .spawn(move || actor.run(rx))
            .unwrap();

        (
            ActorEngine {
                tx: Some(tx),
                join: Some(join),
                db,
                ptrs,
                headks,
                index_mode,
                final_state: None,
            },
            handles,
        )
    }

    /// Shut the actor down (writer handles must be dropped first), make the
    /// log + index durable, rotate memtables for disk accounting.
    pub fn finalize(&mut self) {
        drop(self.tx.take());
        let st = self.join.take().unwrap().join().unwrap();
        st.last_file.sync_data().unwrap();
        self.db.persist(fjall::PersistMode::SyncAll).unwrap();
        self.ptrs.rotate_memtable_and_wait().unwrap();
        self.headks.rotate_memtable_and_wait().unwrap();
        self.final_state = Some(st);
    }

    pub fn next_global_pos(&self) -> u64 {
        self.final_state.as_ref().unwrap().next_global_pos
    }

    pub fn stream_ptrs(&self, stream: u64) -> Option<Vec<EventPtr>> {
        match self.index_mode {
            ActorIndex::InMem => {
                Some(self.final_state.as_ref().unwrap().mem[stream as usize].clone())
            }
            ActorIndex::FjallBatch => {
                let mut out = Vec::new();
                for guard in self.ptrs.prefix(stream.to_be_bytes()) {
                    let (k, v) = guard.into_inner().unwrap();
                    let ver = crate::engine_mutex::be64(&k[8..16]);
                    assert_eq!(ver, out.len() as u64, "index gap on stream {stream}");
                    out.push(EventPtr::decode(&v));
                }
                Some(out)
            }
        }
    }

    pub fn seg_files(&self) -> HashMap<u64, Arc<File>> {
        self.final_state.as_ref().unwrap().files.clone()
    }
}
