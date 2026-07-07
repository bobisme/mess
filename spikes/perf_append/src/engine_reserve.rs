//! Reservation engine (F8b): writers reserve (global position range, segment
//! offset, batch id) under a micro-critical-section, then encode + CRC the
//! batch IN PARALLEL on their own thread, then hand the finished bytes to a
//! sequencer thread that reorders by (segment, offset) and issues the writes
//! in exactly reserved order — the on-disk log stays byte-identical in shape
//! to the actor engine's (A1 contiguity preserved).
//!
//! Index: in-memory, sharded by writer (streams are writer-owned in this
//! workload, so shard updates are uncontended); persisted to fjall at seal by
//! the sequencer, like the actor engine's InMem mode.
//!
//! Semantics caveat (recorded in REPORT.md): the shard index entry is inserted
//! before the log write completes (reversed D1 order). Fine for a BUFFERED
//! spike; a real implementation would publish index entries post-write.
//!
//! Version conflict checks are writer-local: streams are partitioned by
//! writer, so each writer owns its streams' heads.

use std::collections::{BinaryHeap, HashMap};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};

use crate::engine_mutex::key16;
use crate::seglog::{
    batch_len, encode_batch_into, open_segment_file, segment_file_name, EventPtr, SEGMENT_SIZE,
};
use crate::workload::{BatchSpec, STREAMS};

struct SeqState {
    seg_id: u64,
    seg_off: u64,
    next_batch_id: u64,
    next_global_pos: u64,
    file: Arc<File>,
}

struct Item {
    seg_id: u64,
    off: u64,
    pos: u64,
    writer: u8,
    bytes: Vec<u8>,
    /// Set when this batch opens a new segment: (new_file, prev_segment_len).
    new_seg: Option<(Arc<File>, u64)>,
}

impl PartialEq for Item {
    fn eq(&self, other: &Self) -> bool {
        (self.seg_id, self.off) == (other.seg_id, other.off)
    }
}
impl Eq for Item {}
impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Item {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse: BinaryHeap is a max-heap, we want the smallest (seg, off).
        (other.seg_id, other.off).cmp(&(self.seg_id, self.off))
    }
}

pub struct Shard {
    pub mem: Vec<Vec<EventPtr>>, // stream -> ptrs (only this shard's streams filled)
    pub journal: Vec<(u64, u64, EventPtr, u64)>, // (stream, version, ptr, seg_id)
}

pub struct FinalState {
    pub files: HashMap<u64, Arc<File>>,
    pub next_global_pos: u64,
    pub last_file: Arc<File>,
}

struct Sequencer {
    heap: BinaryHeap<Item>,
    expect_seg: u64,
    expect_off: u64,
    file: Arc<File>,
    files: HashMap<u64, Arc<File>>,
    shards: Arc<Vec<Mutex<Shard>>>,
    db: fjall::Database,
    ptrs: fjall::Keyspace,
    headks: fjall::Keyspace,
    replies: Vec<mpsc::Sender<(u64, Vec<u8>)>>,
    written_pos: u64,
    // Async seal pipeline (None = seal inline on the sequencer thread).
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

impl Sequencer {
    /// Seal `sealed_seg`: dump all shard journal entries for segments
    /// <= sealed_seg into fjall, durably. Called after the old segment's last
    /// byte is written. Draining the shard journals is cheap and happens
    /// here; with `seal_tx` set, the expensive part (old-segment fdatasync +
    /// fjall writes + persist) moves to the background sealer.
    fn seal(&mut self, sealed_seg: u64, old_file: Arc<File>) {
        let mut journal: Vec<(u64, u64, EventPtr)> = Vec::new();
        let mut heads: Vec<(u64, u64)> = Vec::new();
        for shard in self.shards.iter() {
            let mut sh = shard.lock().unwrap();
            let mut kept = Vec::with_capacity(sh.journal.len());
            for &(s, v, p, seg) in &sh.journal {
                if seg <= sealed_seg {
                    journal.push((s, v, p));
                } else {
                    kept.push((s, v, p, seg));
                }
            }
            sh.journal = kept;
            // Heads for this shard's streams (last version = mem len - 1).
            for (s, ptrs) in sh.mem.iter().enumerate() {
                if !ptrs.is_empty() {
                    heads.push((s as u64, ptrs.len() as u64 - 1));
                }
            }
        }
        if let Some(tx) = &self.seal_tx {
            tx.send(SealReq { old_file, journal, heads }).unwrap();
            return;
        }
        old_file.sync_data().unwrap();
        let mut wb = fjall::OwnedWriteBatch::with_capacity(
            self.db.clone(),
            journal.len() + heads.len(),
        );
        for &(s, v, p) in &journal {
            wb.insert(&self.ptrs, key16(s, v), p.encode());
        }
        for &(s, h) in &heads {
            wb.insert(&self.headks, s.to_be_bytes(), h.to_be_bytes());
        }
        wb.commit().unwrap();
        self.db.persist(fjall::PersistMode::SyncAll).unwrap();
    }

    fn pump(&mut self) {
        loop {
            let ready = match self.heap.peek() {
                Some(top) => {
                    (top.seg_id == self.expect_seg && top.off == self.expect_off)
                        || (top.seg_id == self.expect_seg + 1
                            && top.off == 0
                            && top
                                .new_seg
                                .as_ref()
                                .is_some_and(|&(_, prev)| prev == self.expect_off))
                }
                None => false,
            };
            if !ready {
                return;
            }
            let mut item = self.heap.pop().unwrap();
            if let Some((new_file, _prev_len)) = item.new_seg.take() {
                // Old segment complete: seal it durably, then switch.
                let old = self.file.clone();
                self.seal(self.expect_seg, old);
                self.expect_seg += 1;
                self.expect_off = 0;
                self.files.insert(self.expect_seg, new_file.clone());
                self.file = new_file;
            }
            (&*self.file).write_all(&item.bytes).unwrap();
            self.expect_off += item.bytes.len() as u64;
            self.written_pos = item.pos;
            let mut bytes = item.bytes;
            bytes.clear();
            self.replies[item.writer as usize]
                .send((item.pos, bytes))
                .unwrap();
        }
    }

    fn run(mut self, rx: crossbeam_channel::Receiver<Item>) -> FinalState {
        while let Ok(item) = rx.recv() {
            self.heap.push(item);
            self.pump();
        }
        assert!(self.heap.is_empty(), "sequencer shut down with holes");
        // Drain the seal pipeline before reporting final state.
        drop(self.seal_tx.take());
        if let Some(h) = self.sealer.take() {
            h.join().unwrap();
        }
        FinalState {
            last_file: self.file.clone(),
            files: self.files,
            next_global_pos: 0, // filled by engine from seq state
        }
    }
}

pub struct ReserveEngine {
    seq: Arc<Mutex<SeqState>>,
    tx: Option<crossbeam_channel::Sender<Item>>,
    join: Option<std::thread::JoinHandle<FinalState>>,
    shards: Arc<Vec<Mutex<Shard>>>,
    pub db: fjall::Database,
    pub ptrs: fjall::Keyspace,
    pub headks: fjall::Keyspace,
    writers: usize,
    pub final_state: Option<FinalState>,
}

pub struct WriterHandle {
    master: Arc<Vec<BatchSpec>>,
    seq: Arc<Mutex<SeqState>>,
    tx: crossbeam_channel::Sender<Item>,
    rx: mpsc::Receiver<(u64, Vec<u8>)>,
    shards: Arc<Vec<Mutex<Shard>>>,
    log_dir: std::path::PathBuf,
    writer: u8,
    buf: Vec<u8>,
    heads: Vec<i64>, // writer-local heads for owned streams (-1 = none)
}

impl WriterHandle {
    pub fn append(&mut self, idx: u32) -> u64 {
        let b = &self.master[idx as usize];
        let head = self.heads[b.stream as usize];
        let expected = if head < 0 { None } else { Some(head as u64) };
        assert_eq!(expected, b.expected, "version conflict on stream {}", b.stream);
        let first_version = (head + 1) as u64;
        let n = b.payloads.len() as u64;
        let blen = batch_len(&b.payloads) as u64;

        // --- micro critical section: reserve (segment, offset, batch, pos) ---
        let (seg_id, off, batch_id, pos, new_seg) = {
            let mut st = self.seq.lock().unwrap();
            let mut new_seg = None;
            if st.seg_off + blen > SEGMENT_SIZE && st.seg_off > 0 {
                let prev_len = st.seg_off;
                st.seg_id += 1;
                let path = self
                    .log_dir
                    .join(segment_file_name(st.seg_id, st.next_global_pos));
                st.file = Arc::new(open_segment_file(&path));
                st.seg_off = 0;
                st.next_batch_id = 0;
                new_seg = Some((st.file.clone(), prev_len));
            }
            let r = (st.seg_id, st.seg_off, st.next_batch_id, st.next_global_pos, new_seg);
            st.seg_off += blen;
            st.next_batch_id += 1;
            st.next_global_pos += n;
            r
        };

        // --- parallel: encode + CRC on this writer's thread ---
        let mut buf = std::mem::take(&mut self.buf);
        buf.clear();
        {
            let mut shard = self.shards[self.writer as usize].lock().unwrap();
            let stream = b.stream;
            let mut k = 0u64;
            let sh = &mut *shard;
            encode_batch_into(
                &mut buf,
                batch_id,
                pos,
                stream,
                first_version,
                &b.payloads,
                |o, len| {
                    let ptr = EventPtr { segment_id: seg_id, offset: off + o as u64, len };
                    debug_assert_eq!(sh.mem[stream as usize].len() as u64, first_version + k);
                    sh.mem[stream as usize].push(ptr);
                    sh.journal.push((stream, first_version + k, ptr, seg_id));
                    k += 1;
                },
            );
        }
        self.heads[b.stream as usize] = (first_version + n - 1) as i64;

        // --- hand off to the sequencer; block for the ack (depth 1) ---
        self.tx
            .send(Item { seg_id, off, pos, writer: self.writer, bytes: buf, new_seg })
            .unwrap();
        let (acked_pos, buf) = self.rx.recv().unwrap();
        self.buf = buf; // buffer returned: zero steady-state allocation
        debug_assert_eq!(acked_pos, pos);
        pos
    }
}

impl ReserveEngine {
    pub fn new(
        dir: &Path,
        master: Arc<Vec<BatchSpec>>,
        writers: usize,
        async_seal: bool,
    ) -> (Self, Vec<WriterHandle>) {
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

        let first = Arc::new(open_segment_file(&log_dir.join(segment_file_name(0, 0))));
        let seq = Arc::new(Mutex::new(SeqState {
            seg_id: 0,
            seg_off: 0,
            next_batch_id: 0,
            next_global_pos: 0,
            file: first.clone(),
        }));

        let shards: Arc<Vec<Mutex<Shard>>> = Arc::new(
            (0..writers)
                .map(|_| {
                    Mutex::new(Shard {
                        mem: vec![Vec::new(); STREAMS as usize],
                        journal: Vec::new(),
                    })
                })
                .collect(),
        );

        let (tx, rx) = crossbeam_channel::bounded::<Item>(4096);
        let mut handles = Vec::with_capacity(writers);
        let mut replies = Vec::with_capacity(writers);
        for w in 0..writers {
            let (rtx, rrx) = mpsc::channel();
            replies.push(rtx);
            handles.push(WriterHandle {
                master: master.clone(),
                seq: seq.clone(),
                tx: tx.clone(),
                rx: rrx,
                shards: shards.clone(),
                log_dir: log_dir.clone(),
                writer: w as u8,
                buf: Vec::with_capacity(1 << 20),
                heads: vec![-1; STREAMS as usize],
            });
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

        let mut files = HashMap::new();
        files.insert(0, first.clone());
        let sequencer = Sequencer {
            heap: BinaryHeap::new(),
            expect_seg: 0,
            expect_off: 0,
            file: first,
            files,
            shards: shards.clone(),
            db: db.clone(),
            ptrs: ptrs.clone(),
            headks: headks.clone(),
            replies,
            written_pos: 0,
            seal_tx,
            sealer,
        };
        let join = std::thread::Builder::new()
            .name("sequencer".into())
            .spawn(move || sequencer.run(rx))
            .unwrap();

        (
            ReserveEngine {
                seq,
                tx: Some(tx),
                join: Some(join),
                shards,
                db,
                ptrs,
                headks,
                writers,
                final_state: None,
            },
            handles,
        )
    }

    pub fn finalize(&mut self) {
        drop(self.tx.take());
        let mut st = self.join.take().unwrap().join().unwrap();
        st.next_global_pos = self.seq.lock().unwrap().next_global_pos;
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
        let shard = self.shards[(stream % self.writers as u64) as usize]
            .lock()
            .unwrap();
        Some(shard.mem[stream as usize].clone())
    }

    pub fn seg_files(&self) -> HashMap<u64, Arc<File>> {
        self.final_state.as_ref().unwrap().files.clone()
    }
}
