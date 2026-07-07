//! Durable-append engine variants under test. Invariant shared by ALL of
//! them: `append()` returns strictly after the covering fdatasync (or
//! O_DSYNC write) has returned — an ack is never issued from page cache.
//!
//! Variants (see REPORT.md for hypotheses):
//!
//! - `SyncPerBatch` — writer does its own fdatasync after its own write.
//! - `GroupEngine { Locked }` — vertical_slice Group mode + the D7
//!   amendment: the window closes early when every in-flight writer's batch
//!   is already pending; `max_delay` is a cap, not a target
//!   (`GroupCfg::early_close = false` reproduces the old fixed-window mode).
//! - `GroupEngine { Pwrite }` — positions AND file ranges reserved under a
//!   short lock; encode + pwrite happen outside it, in parallel across
//!   writers. A per-stripe write-completion watermark gates fsync so a hole
//!   (reserved-but-unwritten range) below any pending batch is impossible at
//!   fsync time.
//! - `GroupEngine { stripes: k > 1 }` — batches striped round-robin (by
//!   batch sequence) across k segment files, each with its own fsync
//!   pipeline. Global positions are assigned centrally; a batch is acked
//!   only when the GLOBAL durable watermark passes it (all earlier batches
//!   on all stripes durable), so recovery's merge rule can never discard an
//!   acked batch.
//! - `CommitterEngine` — writers hand payloads to a single committer thread
//!   that assigns positions, encodes everything into one buffer and issues
//!   ONE big write per group. Modes: inline fdatasync; pipelined fdatasync
//!   (a syncer thread syncs group N while the committer encodes+writes
//!   group N+1); O_DSYNC (the write itself is the durability barrier).

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::seglog::{batch_total_len, encode_batch_into, SegmentLog, SEGMENT_SIZE};

pub trait Engine: Send + Sync {
    /// Durable append; returns the batch's first global position.
    fn append(&self, stream: u64, first_version: u64, events: &Arc<Vec<Vec<u8>>>) -> u64;
    fn finalize(&self);
    /// Number of durability barriers issued (fdatasync calls, or O_DSYNC
    /// group writes for the dsync committer).
    fn fsyncs(&self) -> u64;
}

#[derive(Clone, Copy, Debug)]
pub struct GroupCfg {
    pub max_delay: Duration,
    pub max_bytes: u64,
    /// D7 amendment: close the window as soon as no in-flight writer is
    /// still producing (all are pending). false = vertical_slice fixed window.
    pub early_close: bool,
}

pub fn d7_cfg() -> GroupCfg {
    GroupCfg { max_delay: Duration::from_millis(1), max_bytes: 64 << 20, early_close: true }
}

fn fixed_cfg(ms: u64) -> GroupCfg {
    GroupCfg { max_delay: Duration::from_millis(ms), max_bytes: 64 << 20, early_close: false }
}

// ---------------------------------------------------------------------------
// Contiguous-watermark tracker (durable acks + pwrite completion gating)
// ---------------------------------------------------------------------------

pub struct ContigTracker {
    st: Mutex<Track>,
    cv: Condvar,
}
struct Track {
    next: u64, // everything < next is marked
    done: BinaryHeap<Reverse<u64>>,
}

impl ContigTracker {
    pub fn new() -> Self {
        ContigTracker { st: Mutex::new(Track { next: 0, done: BinaryHeap::new() }), cv: Condvar::new() }
    }
    pub fn mark(&self, seq: u64) {
        self.mark_many(std::iter::once(seq));
    }
    pub fn mark_many(&self, seqs: impl IntoIterator<Item = u64>) {
        let mut st = self.st.lock().unwrap();
        for s in seqs {
            st.done.push(Reverse(s));
        }
        let mut advanced = false;
        while st.done.peek().map_or(false, |r| r.0 == st.next) {
            st.done.pop();
            st.next += 1;
            advanced = true;
        }
        if advanced {
            self.cv.notify_all();
        }
    }
    /// Block until every sequence <= seq is marked.
    pub fn wait_for(&self, seq: u64) {
        let mut st = self.st.lock().unwrap();
        while st.next <= seq {
            st = self.cv.wait(st).unwrap();
        }
    }
}

// ---------------------------------------------------------------------------
// Group gathering (shared by stripe syncers and the committer)
// ---------------------------------------------------------------------------

/// Collect a commit group: drain what's pending, then wait until either
/// `max_bytes` is reached, the D7 early-close fires (`active == 0`: every
/// in-flight writer has already submitted), or `max_delay` expires.
///
/// `active` counts writers between append() entry and their submission (they
/// decrement right after sending), so `active == 0` means nothing more can
/// arrive until someone is acked.
fn gather<T>(
    rx: &Receiver<T>,
    first: T,
    cfg: &GroupCfg,
    active: &AtomicUsize,
    bytes_of: impl Fn(&T) -> u64,
) -> Vec<T> {
    const POLL: Duration = Duration::from_micros(50);
    let mut pending = vec![first];
    let mut bytes = bytes_of(&pending[0]);
    let deadline = Instant::now() + cfg.max_delay;
    loop {
        while let Ok(r) = rx.try_recv() {
            bytes += bytes_of(&r);
            pending.push(r);
        }
        if bytes >= cfg.max_bytes {
            break;
        }
        // Poll: submission and the active-decrement are not atomic together,
        // so a short timeout re-checks the close condition.
        if cfg.early_close && active.load(Ordering::Relaxed) == 0 {
            break;
        }
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let step = if cfg.early_close { POLL.min(deadline - now) } else { deadline - now };
        match rx.recv_timeout(step) {
            Ok(r) => {
                bytes += bytes_of(&r);
                pending.push(r);
            }
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    pending
}

// ---------------------------------------------------------------------------
// SyncPerBatch
// ---------------------------------------------------------------------------

pub struct SyncPerBatch {
    log: Mutex<SegmentLog>,
    fsyncs: AtomicU64,
}

impl SyncPerBatch {
    pub fn new(dir: &Path) -> Self {
        SyncPerBatch { log: Mutex::new(SegmentLog::create(dir, 0, false)), fsyncs: AtomicU64::new(0) }
    }
}

impl Engine for SyncPerBatch {
    fn append(&self, stream: u64, first_version: u64, events: &Arc<Vec<Vec<u8>>>) -> u64 {
        let out = self.log.lock().unwrap().append(stream, first_version, events);
        out.file.sync_data().unwrap();
        self.fsyncs.fetch_add(1, Ordering::Relaxed);
        out.first_global_pos
    }
    fn finalize(&self) {
        self.log.lock().unwrap().file.sync_data().unwrap();
    }
    fn fsyncs(&self) -> u64 {
        self.fsyncs.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// GroupEngine: Locked / Pwrite writes, 1..k stripes, D7 window
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    /// Encode + write() under the stripe lock (vertical_slice shape).
    Locked,
    /// Reserve under the lock; encode + pwrite outside it.
    Pwrite,
}

struct SReq {
    seq: u64,     // global batch sequence (durable-watermark key)
    res_seq: u64, // per-stripe reservation sequence (write-completion key)
    bytes: u64,
    seg_id: u64,
    file: Arc<File>,
}

struct StripeState {
    sl: SegmentLog,
    next_res: u64,
}

struct Stripe {
    state: Mutex<StripeState>,
    tx: Sender<SReq>,
    active: Arc<AtomicUsize>,
    wtracker: Arc<ContigTracker>,
}

pub struct GroupEngine {
    global: Mutex<(u64, u64)>, // (next_batch_seq, next_global_pos)
    stripes: Vec<Stripe>,
    dtracker: Arc<ContigTracker>,
    fsyncs: Arc<AtomicU64>,
    mode: WriteMode,
}

fn stripe_syncer(
    rx: Receiver<SReq>,
    cfg: GroupCfg,
    active: Arc<AtomicUsize>,
    wtracker: Arc<ContigTracker>,
    dtracker: Arc<ContigTracker>,
    fsyncs: Arc<AtomicU64>,
) {
    while let Ok(first) = rx.recv() {
        let pending = gather(&rx, first, &cfg, &active, |r| r.bytes);
        // Pwrite hole gate: every reservation at or below the highest pending
        // one must have completed its write, so no hole can exist below any
        // pending batch when the fsync runs. (Trivially satisfied in Locked
        // mode: writers mark before submitting.)
        let max_res = pending.iter().map(|r| r.res_seq).max().unwrap();
        wtracker.wait_for(max_res);
        let mut seen: Vec<u64> = Vec::new();
        for r in &pending {
            if !seen.contains(&r.seg_id) {
                r.file.sync_data().unwrap();
                fsyncs.fetch_add(1, Ordering::Relaxed);
                seen.push(r.seg_id);
            }
        }
        dtracker.mark_many(pending.iter().map(|r| r.seq));
    }
}

impl GroupEngine {
    pub fn new(dir: &Path, k: u32, mode: WriteMode, cfg: GroupCfg) -> Self {
        let dtracker = Arc::new(ContigTracker::new());
        let fsyncs = Arc::new(AtomicU64::new(0));
        let stripes = (0..k)
            .map(|i| {
                let sl = SegmentLog::create(dir, i, false);
                let (tx, rx) = mpsc::channel::<SReq>();
                let active = Arc::new(AtomicUsize::new(0));
                let wtracker = Arc::new(ContigTracker::new());
                {
                    let (a, w, d, f) = (active.clone(), wtracker.clone(), dtracker.clone(), fsyncs.clone());
                    thread::spawn(move || stripe_syncer(rx, cfg, a, w, d, f));
                }
                Stripe { state: Mutex::new(StripeState { sl, next_res: 0 }), tx, active, wtracker }
            })
            .collect();
        GroupEngine { global: Mutex::new((0, 0)), stripes, dtracker, fsyncs, mode }
    }
}

impl Engine for GroupEngine {
    fn append(&self, stream: u64, first_version: u64, events: &Arc<Vec<Vec<u8>>>) -> u64 {
        let n = events.len() as u64;
        // Central position assignment: global batch sequence + global position.
        let (seq, gpos) = {
            let mut g = self.global.lock().unwrap();
            let out = (g.0, g.1);
            g.0 += 1;
            g.1 += n;
            out
        };
        let stripe = &self.stripes[(seq % self.stripes.len() as u64) as usize];
        stripe.active.fetch_add(1, Ordering::Relaxed);

        let req = match self.mode {
            WriteMode::Locked => {
                let mut st = stripe.state.lock().unwrap();
                st.sl.next_global_pos = gpos;
                let out = st.sl.append(stream, first_version, events);
                let res_seq = st.next_res;
                st.next_res += 1;
                drop(st);
                stripe.wtracker.mark(res_seq);
                SReq { seq, res_seq, bytes: out.bytes, seg_id: out.seg_id, file: out.file }
            }
            WriteMode::Pwrite => {
                let tl = batch_total_len(events);
                let (file, off, batch_id, seg_id, res_seq) = {
                    let mut st = stripe.state.lock().unwrap();
                    if st.sl.seg_len + tl as u64 > SEGMENT_SIZE && st.sl.seg_len > 0 {
                        // No inline sync: outstanding pwrites may still target
                        // the old file; its covering fsyncs come from pending
                        // SReqs that hold the old Arc<File>.
                        st.sl.roll(false);
                    }
                    let file = st.sl.file.clone();
                    let off = st.sl.seg_len;
                    let batch_id = st.sl.next_batch_id;
                    st.sl.seg_len += tl as u64;
                    st.sl.next_batch_id += 1;
                    let res_seq = st.next_res;
                    st.next_res += 1;
                    (file, off, batch_id, st.sl.seg_id, res_seq)
                };
                let mut buf = Vec::with_capacity(tl);
                encode_batch_into(&mut buf, batch_id, gpos, stream, first_version, events);
                file.write_all_at(&buf, off).unwrap();
                stripe.wtracker.mark(res_seq);
                SReq { seq, res_seq, bytes: tl as u64, seg_id, file }
            }
        };
        stripe.tx.send(req).unwrap();
        // Submitted: this writer is no longer "in flight" for window purposes.
        stripe.active.fetch_sub(1, Ordering::Relaxed);
        // Ack = GLOBAL durable watermark passes this batch: its own stripe's
        // covering fdatasync returned AND every earlier batch (any stripe) is
        // durable. This is what keeps the recovery merge from ever discarding
        // an acked batch.
        self.dtracker.wait_for(seq);
        gpos
    }

    fn finalize(&self) {
        for s in &self.stripes {
            s.state.lock().unwrap().sl.file.sync_data().unwrap();
        }
    }
    fn fsyncs(&self) -> u64 {
        self.fsyncs.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// CommitterEngine
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitMode {
    Fsync { piped: bool },
    Dsync,
}

struct CReq {
    stream: u64,
    first_version: u64,
    events: Arc<Vec<Vec<u8>>>,
    ack: SyncSender<u64>,
}

pub struct CommitterEngine {
    tx: Sender<CReq>,
    active: Arc<AtomicUsize>,
    fsyncs: Arc<AtomicU64>,
}

fn committer_loop(
    rx: Receiver<CReq>,
    dir: PathBuf,
    mode: CommitMode,
    cfg: GroupCfg,
    active: Arc<AtomicUsize>,
    fsyncs: Arc<AtomicU64>,
) {
    let dsync = mode == CommitMode::Dsync;
    let mut sl = SegmentLog::create(&dir, 0, dsync);

    type PipedGroup = (Arc<File>, Vec<(SyncSender<u64>, u64)>);
    let piped_tx: Option<SyncSender<PipedGroup>> = if mode == (CommitMode::Fsync { piped: true }) {
        // Bounded depth: the committer may encode+write group N+1 while the
        // syncer fsyncs group N (double buffering).
        let (tx, prx) = mpsc::sync_channel::<PipedGroup>(1);
        let f = fsyncs.clone();
        thread::spawn(move || {
            for (file, acks) in prx {
                file.sync_data().unwrap();
                f.fetch_add(1, Ordering::Relaxed);
                for (ack, gpos) in acks {
                    let _ = ack.send(gpos);
                }
            }
        });
        Some(tx)
    } else {
        None
    };

    let mut buf: Vec<u8> = Vec::with_capacity(1 << 20);
    while let Ok(first) = rx.recv() {
        let pending = gather(&rx, first, &cfg, &active, |r| batch_total_len(&r.events) as u64);
        let group_bytes: u64 = pending.iter().map(|r| batch_total_len(&r.events) as u64).sum();
        if sl.seg_len + group_bytes > SEGMENT_SIZE && sl.seg_len > 0 {
            // Whole group into one segment (A8). Everything already in the
            // old segment was synced before its acks, so no sync needed here
            // for Fsync modes; O_DSYNC writes are durable by definition.
            sl.roll(false);
        }
        buf.clear();
        let mut acks: Vec<(SyncSender<u64>, u64)> = Vec::with_capacity(pending.len());
        for r in &pending {
            let gpos = sl.next_global_pos;
            encode_batch_into(&mut buf, sl.next_batch_id, gpos, r.stream, r.first_version, &r.events);
            sl.next_batch_id += 1;
            sl.next_global_pos += r.events.len() as u64;
            acks.push((r.ack.clone(), gpos));
        }
        {
            use std::io::Write;
            (&*sl.file).write_all(&buf).unwrap();
        }
        sl.seg_len += buf.len() as u64;
        match mode {
            CommitMode::Dsync => {
                // The O_DSYNC write IS the durability barrier.
                fsyncs.fetch_add(1, Ordering::Relaxed);
                for (ack, gpos) in acks {
                    let _ = ack.send(gpos);
                }
            }
            CommitMode::Fsync { piped: false } => {
                sl.file.sync_data().unwrap();
                fsyncs.fetch_add(1, Ordering::Relaxed);
                for (ack, gpos) in acks {
                    let _ = ack.send(gpos);
                }
            }
            CommitMode::Fsync { piped: true } => {
                piped_tx.as_ref().unwrap().send((sl.file.clone(), acks)).unwrap();
            }
        }
    }
}

impl CommitterEngine {
    pub fn new(dir: &Path, mode: CommitMode, cfg: GroupCfg) -> Self {
        let (tx, rx) = mpsc::channel::<CReq>();
        let active = Arc::new(AtomicUsize::new(0));
        let fsyncs = Arc::new(AtomicU64::new(0));
        {
            let (dir, a, f) = (dir.to_path_buf(), active.clone(), fsyncs.clone());
            thread::spawn(move || committer_loop(rx, dir, mode, cfg, a, f));
        }
        CommitterEngine { tx, active, fsyncs }
    }
}

impl Engine for CommitterEngine {
    fn append(&self, stream: u64, first_version: u64, events: &Arc<Vec<Vec<u8>>>) -> u64 {
        self.active.fetch_add(1, Ordering::Relaxed);
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        self.tx
            .send(CReq { stream, first_version, events: events.clone(), ack: ack_tx })
            .unwrap();
        self.active.fetch_sub(1, Ordering::Relaxed);
        ack_rx.recv().unwrap()
    }
    fn finalize(&self) {} // every ack already implies durability
    fn fsyncs(&self) -> u64 {
        self.fsyncs.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

pub fn make_engine(design: &str, dir: &Path) -> Arc<dyn Engine> {
    match design {
        "spb" => Arc::new(SyncPerBatch::new(dir)),
        "v0-1ms" => Arc::new(GroupEngine::new(dir, 1, WriteMode::Locked, fixed_cfg(1))),
        "v0-5ms" => Arc::new(GroupEngine::new(dir, 1, WriteMode::Locked, fixed_cfg(5))),
        "v0-25ms" => Arc::new(GroupEngine::new(dir, 1, WriteMode::Locked, fixed_cfg(25))),
        "d7" => Arc::new(GroupEngine::new(dir, 1, WriteMode::Locked, d7_cfg())),
        "d7-pwrite" => Arc::new(GroupEngine::new(dir, 1, WriteMode::Pwrite, d7_cfg())),
        "striped2" => Arc::new(GroupEngine::new(dir, 2, WriteMode::Locked, d7_cfg())),
        "striped4" => Arc::new(GroupEngine::new(dir, 4, WriteMode::Locked, d7_cfg())),
        "striped2-pwrite" => Arc::new(GroupEngine::new(dir, 2, WriteMode::Pwrite, d7_cfg())),
        "striped4-pwrite" => Arc::new(GroupEngine::new(dir, 4, WriteMode::Pwrite, d7_cfg())),
        "commit-fsync" => Arc::new(CommitterEngine::new(dir, CommitMode::Fsync { piped: false }, d7_cfg())),
        "commit-piped" => Arc::new(CommitterEngine::new(dir, CommitMode::Fsync { piped: true }, d7_cfg())),
        "commit-dsync" => Arc::new(CommitterEngine::new(dir, CommitMode::Dsync, d7_cfg())),
        other => panic!("unknown design {other}"),
    }
}
