//! Spike B (bn-28g): **FlatEngine** — a single-owner flat-combined
//! append/publish prototype over the REAL `mess-log` committer.
//!
//! What it keeps: v3 on-disk bytes, `SegmentWriter`, `Committer`,
//! `Appender` — untouched.
//!
//! What it replaces (vs `mess_store::LogEngine`'s append path):
//!
//! | current engine                         | FlatEngine                       |
//! |----------------------------------------|----------------------------------|
//! | per-stream `AppendGate` (async mutex)  | owner validates in dequeue order |
//! | one `spawn_blocking` task per append   | zero blocking-pool tasks         |
//! | `PublishSequencer` (condvar turns)     | owner publishes in order for free|
//! | global `Book` mutex on the append path | owner-owned `HashMap` shadow heads|
//! | post-commit Fjall `CommitGroup` write  | nothing (heads recovered from log)|
//!
//! Architecture (design.md §6): async producers push an `Intent` into a
//! bounded MPSC ring (bounded by BYTES via a semaphore and by COUNT via the
//! channel capacity; producers await space) and await a oneshot completion.
//! ONE owner thread drains a group of intents, validates expected-version
//! against shadow heads in deterministic dequeue order with a speculative
//! group overlay (two same-stream `Exact(v)` intents in one group: first
//! wins, second conflicts), submits accepted appends to the real
//! [`Appender`], awaits the acks (the committer acks in position order),
//! applies head effects, publishes a group watermark (single `AtomicU64`,
//! Release), and completes waiters. Conflicts/duplicates/empty batches
//! complete WITHOUT writing anything. A dropped caller future only drops its
//! oneshot receiver: the owner runs every accepted intent to a terminal
//! state and ignores completion-send failures, so a committed append always
//! publishes.
//!
//! Variants (review 11 §D1):
//! * **B0** — the owner awaits the durability ack inline before starting the
//!   next group.
//! * **B1** — while group N's ack is pending, the owner validates/encodes
//!   group N+1 against speculative state (shadow heads already advanced at
//!   accept time) and submits it, then retires group N. Note the
//!   `perf_group_commit` H2a precedent: naive pipelining was a no-win — B1
//!   is measured, not assumed.

use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;


use mess_log::committer::{
    AppendError as LogAppendError, AppendOutcome, AppendRequest, Appender,
    Committer, Durability, EventInput,
};
use mess_log::encode::Subframe;
use mess_log::runtime::real::RealFs;
use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};
pub use mess_store::Version;
pub use mess_store::backend::{AppendError, Appended, RecordToAppend};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

/// The spike appends everything under one category (matches the current
/// engine's `CATEGORY_ID`).
pub const CATEGORY_ID: u64 = 0;
/// Single active segment, no roll (matches the committer benches; a roll adds
/// one clean barrier per segment and changes nothing structural).
pub const SEGMENT_ID: u64 = 1;

/// FlatEngine's backend-level error (the `E` of `AppendError<E>`).
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum FlatError {
    /// A durability barrier failed (or an append came back
    /// `Indeterminate`): the store is poisoned, writes fail fast.
    #[error("store poisoned: {0}")]
    Poisoned(String),
    /// The engine was closed.
    #[error("engine closed")]
    Closed,
}

/// Which owner loop to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Variant {
    /// Owner submits to the REAL committer thread and awaits the group's
    /// durability ack inline.
    B0,
    /// Owner validates/submits group N+1 to the committer while group N's
    /// ack is pending.
    B1,
    /// Owner owns the `SegmentWriter` DIRECTLY (design.md §6.3: "the owner
    /// performs one coalesced write and one durability barrier"): validate
    /// → `SegmentWriter::append` per accepted batch → one `sync()` barrier
    /// per group (Group/Os) → publish → complete. Same v3 bytes through the
    /// same writer/encoder — the committer's separate thread and its two
    /// wake hops are what disappear.
    B0Direct,
}

/// Open-time knobs.
#[derive(Debug, Clone)]
pub struct FlatConfig {
    pub durability:        Durability,
    pub segment_size:      u64,
    /// Intent-ring COUNT bound (channel capacity).
    pub ring_intents:      usize,
    /// Intent-ring BYTE bound. Producers `await` space on a semaphore; an
    /// intent's permits are released when it reaches a terminal state, so
    /// this bounds queued + in-flight intent memory.
    pub ring_bytes:        usize,
    /// Owner drain cap per group (intents).
    pub max_group_intents: usize,
    /// Owner drain cap per group (bytes).
    pub max_group_bytes:   usize,
    pub variant:           Variant,
    /// Bounded busy-poll before the owner parks waiting for the next
    /// intent (µs). Under sustained load the next intent arrives within a
    /// few µs of the last completion; spinning skips a futex round trip on
    /// the producer→owner hop. `0` = always park.
    pub spin_intake_us:    u64,
    /// Bounded busy-poll after the first intent of a group, letting the
    /// rest of a convoy join the same group (µs). `0` = drain-only.
    pub spin_gather_us:    u64,
    /// TEST-ONLY: sleep this long before validating each group, so a test
    /// can deterministically observe producers blocking on the byte-bounded
    /// ring while the owner is busy. `None` in every benchmark.
    pub group_stall:       Option<Duration>,
}

impl Default for FlatConfig {
    fn default() -> Self {
        // Spin knobs are env-overridable for spike tuning sweeps
        // (`FLAT_SPIN_INTAKE_US` / `FLAT_SPIN_GATHER_US`); the committed
        // defaults are the measured winners on the reference machine.
        let env_us = |k: &str, d: u64| {
            std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
        };
        FlatConfig {
            durability:        Durability::Process,
            segment_size:      256 * 1024 * 1024,
            ring_intents:      1024,
            ring_bytes:        32 * 1024 * 1024,
            max_group_intents: 512,
            max_group_bytes:   8 * 1024 * 1024,
            variant:           Variant::B0,
            spin_intake_us:    env_us("FLAT_SPIN_INTAKE_US", 40),
            spin_gather_us:    env_us("FLAT_SPIN_GATHER_US", 3),
            group_stall:       None,
        }
    }
}

/// Owner counters, returned by [`FlatEngine::close`].
#[derive(Debug, Default, Clone)]
pub struct OwnerStats {
    pub groups:              u64,
    pub intents:             u64,
    pub accepted_batches:    u64,
    pub conflicts:           u64,
    pub empties:             u64,
    pub events:              u64,
    pub max_group_intents:   u64,
    /// Committer barriers issued (from the real committer's metrics).
    pub fsyncs:              u64,
    /// Mean barrier latency, nanoseconds (0 if no barrier).
    pub mean_fsync_nanos:    u64,
    /// Owner-predicted positions that disagreed with the committer's acked
    /// positions. MUST be 0 — nonzero means the shadow state diverged.
    pub position_mismatches: u64,
}

/// Everything the owner knew at shutdown — the shadow state the reopen test
/// compares against a standard recovery scan.
#[derive(Debug, Clone)]
pub struct OwnerExit {
    pub stats:       OwnerStats,
    /// `stream id -> last stream version` (the shadow heads).
    pub heads:       HashMap<u64, u64>,
    /// `stream name -> stream id` (owner-local interner).
    pub stream_ids:  HashMap<String, u64>,
    /// The next global position (== total committed events).
    pub next_global: u64,
}

// ---------------------------------------------------------------------------
// Intent ring
// ---------------------------------------------------------------------------

struct Intent {
    stream:   String,
    expected: Version,
    records:  Vec<RecordToAppend>,
    /// Byte-ring reservation; dropped (released) when the intent reaches a
    /// terminal state.
    permit:   OwnedSemaphorePermit,
    cost:     u64,
    done:     oneshot::Sender<Result<Appended, AppendError<FlatError>>>,
}

/// The byte cost an intent charges against the ring (payloads + small fixed
/// overhead per event and per intent — a bound, not an exact size).
fn intent_cost(stream: &str, records: &[RecordToAppend]) -> u64 {
    let payloads: u64 = records
        .iter()
        .map(|r| (r.data.len() + r.message_type.len() + 64) as u64)
        .sum();
    payloads + stream.len() as u64 + 128
}

// ---------------------------------------------------------------------------
// FlatEngine (producer side)
// ---------------------------------------------------------------------------

pub struct FlatEngine {
    tx:        Option<mpsc::Sender<Intent>>,
    bytes:     Arc<Semaphore>,
    byte_cap:  u32,
    watermark: Arc<AtomicU64>,
    poisoned:  Arc<AtomicBool>,
    /// Producers between `append_batch` entry and their intent's send —
    /// the D7 early-close signal (`perf_group_commit` H1): under a barrier
    /// mode the owner holds its gather window open while this is nonzero
    /// (capped by the Group `max_delay`), so a convoy is not split across
    /// two barriers just because the owner woke first.
    inflight:  Arc<AtomicUsize>,
    owner:     Option<std::thread::JoinHandle<OwnerExit>>,
}

/// Decrements the in-flight count on drop — including when a caller's
/// future is dropped while awaiting ring space, so an abandoned producer
/// can never wedge the owner's gather window open past its cap.
struct InflightGuard(Arc<AtomicUsize>);

impl Drop for InflightGuard {
    fn drop(&mut self) { self.0.fetch_sub(1, Ordering::Relaxed); }
}

impl FlatEngine {
    pub fn open(dir: impl AsRef<Path>, cfg: FlatConfig) -> std::io::Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let rt = RealRuntime::new();
        let fs = rt.fs();
        let path = dir.join(format!("flat-seg-{SEGMENT_ID}.log"));
        let mut params = SegmentParams::new(SEGMENT_ID, 0, 1, 0);
        params.segment_size = cfg.segment_size;
        let writer = SegmentWriter::create(&fs, &path, params)
            .map_err(|e| std::io::Error::other(format!("segment create: {e}")))?;
        // B0/B1 hand the writer to the REAL committer thread; B0Direct
        // keeps it on the owner (design.md §6.3).
        let core = match cfg.variant {
            Variant::B0 | Variant::B1 => {
                let committer = Committer::spawn(&rt, writer, cfg.durability);
                let appender = committer.appender();
                Core::Committer { committer, appender }
            }
            Variant::B0Direct => Core::Direct { writer },
        };

        let (tx, rx) = mpsc::channel::<Intent>(cfg.ring_intents.max(1));
        let bytes = Arc::new(Semaphore::new(cfg.ring_bytes));
        let watermark = Arc::new(AtomicU64::new(0));
        let poisoned = Arc::new(AtomicBool::new(false));
        let inflight = Arc::new(AtomicUsize::new(0));
        let byte_cap = cfg.ring_bytes.min(u32::MAX as usize) as u32;

        let owner = {
            let watermark = Arc::clone(&watermark);
            let poisoned = Arc::clone(&poisoned);
            let inflight = Arc::clone(&inflight);
            std::thread::Builder::new().name("flat-owner".into()).spawn(
                move || {
                    owner_loop(rt, core, rx, cfg, watermark, poisoned, inflight)
                },
            )?
        };

        Ok(FlatEngine {
            tx: Some(tx),
            bytes,
            byte_cap,
            watermark,
            poisoned,
            inflight,
            owner: Some(owner),
        })
    }

    /// The published group watermark: the exclusive end of the committed,
    /// head-applied global-position sequence. Single `AtomicU64`, Acquire
    /// load (the owner Release-stores it after each group publishes).
    pub fn watermark(&self) -> u64 { self.watermark.load(Ordering::Acquire) }

    pub fn is_poisoned(&self) -> bool { self.poisoned.load(Ordering::Acquire) }

    /// Append `records` to `stream` iff it is exactly at `expected` —
    /// the same contract as `Backend::append_batch` on the current engine.
    ///
    /// Producer side: reserve ring bytes (await space), send the intent
    /// (await ring count slot), await the oneshot completion. NO
    /// spawn_blocking, NO per-stream lock, NO publish turn — the owner does
    /// validation/ordering/publication for everyone.
    pub async fn append_batch(
        &self,
        stream: &str,
        expected: Version,
        records: &[RecordToAppend],
    ) -> Result<Appended, AppendError<FlatError>> {
        self.append_batch_owned(stream, expected, records.to_vec()).await
    }

    /// [`append_batch`](Self::append_batch) without the defensive copy: the
    /// caller hands the records over. The borrowed entry point exists to
    /// mirror the current `Backend::append_batch(&[RecordToAppend])`
    /// signature for matched benchmarks; this owned one is what a v4 API
    /// would actually expose (the intent ring wants owned payloads anyway),
    /// and is the fair single-copy comparison against the bare log's
    /// `EventInput` handoff.
    pub async fn append_batch_owned(
        &self,
        stream: &str,
        expected: Version,
        records: Vec<RecordToAppend>,
    ) -> Result<Appended, AppendError<FlatError>> {
        if self.is_poisoned() {
            return Err(AppendError::Backend(FlatError::Poisoned(
                "poisoned before submit".into(),
            )));
        }
        // In-flight from entry until the intent is IN the ring (D7
        // early-close accounting, matching the committer's own gate) — the
        // owner's barrier-mode gather window stays open while producers are
        // still on their way. Guard-dropped even if this future is dropped
        // mid-await.
        self.inflight.fetch_add(1, Ordering::Relaxed);
        let inflight_guard = InflightGuard(Arc::clone(&self.inflight));
        let cost = intent_cost(stream, &records);
        // Clamp: a single oversized intent may charge at most the whole
        // ring (it can never acquire more permits than exist).
        let charge = cost.min(u64::from(self.byte_cap)) as u32;
        let permit = Arc::clone(&self.bytes)
            .acquire_many_owned(charge)
            .await
            .map_err(|_| AppendError::Backend(FlatError::Closed))?;
        let (done_tx, done_rx) = oneshot::channel();
        let intent = Intent {
            stream: stream.to_string(),
            expected,
            records,
            permit,
            cost,
            done: done_tx,
        };
        self.tx
            .as_ref()
            .expect("engine is live")
            .send(intent)
            .await
            .map_err(|_| AppendError::Backend(FlatError::Closed))?;
        // Submitted: no longer "in flight" for the gather window.
        drop(inflight_guard);
        match done_rx.await {
            Ok(res) => res,
            Err(_) => Err(AppendError::Backend(FlatError::Poisoned(
                "owner dropped completion".into(),
            ))),
        }
    }

    /// Clean shutdown: close the ring, let the owner drain every queued
    /// intent to a terminal state, shut the committer down, return the
    /// owner's exit state (stats + shadow heads).
    pub fn close(mut self) -> OwnerExit {
        drop(self.tx.take());
        self.owner
            .take()
            .expect("owner is live")
            .join()
            .expect("owner thread panicked")
    }
}

impl Drop for FlatEngine {
    fn drop(&mut self) {
        drop(self.tx.take());
        if let Some(h) = self.owner.take() {
            let _ = h.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Owner thread
// ---------------------------------------------------------------------------

type AckFut =
    Pin<Box<dyn Future<Output = Result<AppendOutcome, LogAppendError>> + Send>>;

/// An accepted intent, submitted to the committer, awaiting its ack.
struct Accepted {
    done:            oneshot::Sender<Result<Appended, AppendError<FlatError>>>,
    /// Kept until the terminal completion so the ring's byte bound covers
    /// in-flight (not just queued) intents.
    _permit:         OwnedSemaphorePermit,
    last_stream_pos: u64,
    pred_first:      u64,
    pred_last:       u64,
}

/// A validated, submitted group whose acks may still be in flight.
struct Prepared {
    accepted:  Vec<Accepted>,
    /// One ack future per accepted intent, in submission (== position)
    /// order. Each was polled once at submit time (which performs the send
    /// into the committer's gather channel); its slot in `results` is
    /// `Some` if that first poll already resolved it.
    futs:      Vec<AckFut>,
    results:   Vec<Option<Result<AppendOutcome, LogAppendError>>>,
    /// Exclusive end of THIS group's global positions (the watermark value
    /// to publish once the group's acks land).
    group_end: u64,
}

struct Owner {
    rt:         RealRuntime,
    /// `Some` for the committer-backed variants (B0/B1); `None` for
    /// B0Direct (the owner holds the writer instead).
    appender:   Option<Appender>,
    cfg:        FlatConfig,
    stream_ids: HashMap<String, u64>,
    type_ids:   HashMap<String, u32>,
    /// Speculative frontier: advanced at intent-accept time; the validation
    /// source. Equals the published frontier between groups in B0; may run
    /// one group ahead in B1.
    heads:      HashMap<u64, u64>,
    /// Memo for the (overwhelmingly common) single-type batch: skips the
    /// per-record hash lookup in `intern_type`.
    last_type:  Option<(String, u32)>,
    next_global: u64,
    watermark:  Arc<AtomicU64>,
    poisoned:   Arc<AtomicBool>,
    /// Producers between append entry and their send (the D7 early-close
    /// signal; see [`FlatEngine::inflight`]).
    inflight:   Arc<AtomicUsize>,
    stats:      OwnerStats,
    /// B0Direct only: cumulative barrier time, for `mean_fsync_nanos`.
    fsync_nanos: u64,
    /// Expected convoy width for the barrier-mode gather window — the last
    /// group's size, seeded at 1 (the committer's own anti-convoy-split
    /// mechanism: gate-zero closes the window only once the convoy has
    /// reformed to this size; `max_delay` remains a strict cap).
    gather_target: usize,
}

/// The result of validating one intent against the shadow heads.
enum Validated {
    /// Conflict/empty/poisoned: already completed inline, wrote nothing.
    Skip,
    /// Accepted: caller writes it and advances the speculative frontier.
    Accept {
        sid:      u64,
        first_sp: u64,
        last_sp:  u64,
        records:  Vec<RecordToAppend>,
        permit:   OwnedSemaphorePermit,
        done:     oneshot::Sender<Result<Appended, AppendError<FlatError>>>,
    },
}

impl Owner {
    fn intern_stream(&mut self, name: &str) -> u64 {
        if let Some(&id) = self.stream_ids.get(name) {
            return id;
        }
        let id = self.stream_ids.len() as u64 + 1;
        self.stream_ids.insert(name.to_string(), id);
        id
    }

    fn intern_type(&mut self, name: &str) -> u32 {
        if let Some((last, id)) = &self.last_type
            && last == name
        {
            return *id;
        }
        let id = match self.type_ids.get(name) {
            Some(&id) => id,
            None => {
                let id = self.type_ids.len() as u32 + 1;
                self.type_ids.insert(name.to_string(), id);
                id
            }
        };
        self.last_type = Some((name.to_string(), id));
        id
    }

    /// Validate one intent in dequeue order against the speculative shadow
    /// heads (which double as the group overlay: an accept advances them —
    /// via the caller — so a second same-stream `Exact(v)` in the same
    /// group conflicts). Conflicts/empties/poisoned complete inline WITHOUT
    /// writing anything; completion-send failure (dropped caller) is
    /// ignored.
    fn precheck(&mut self, intent: Intent) -> Validated {
        self.stats.intents += 1;
        let Intent { stream, expected, records, permit, cost: _, done } =
            intent;
        if self.poisoned.load(Ordering::Acquire) {
            let _ = done.send(Err(AppendError::Backend(FlatError::Poisoned(
                "poisoned".into(),
            ))));
            return Validated::Skip;
        }
        let sid = self.intern_stream(&stream);
        let actual = match self.heads.get(&sid) {
            Some(&v) => Version::At(v),
            None => Version::NoStream,
        };
        if actual != expected {
            self.stats.conflicts += 1;
            let _ = done.send(Err(AppendError::Conflict { expected, actual }));
            return Validated::Skip;
        }
        if records.is_empty() {
            self.stats.empties += 1;
            // Empty batch: a no-op that still validated `expected` (same
            // semantics as the current engine).
            let last_global = self.next_global.saturating_sub(1);
            let _ = done.send(Ok(Appended {
                version:              expected,
                last_global_position: last_global,
            }));
            return Validated::Skip;
        }
        let n = records.len() as u64;
        let first_sp = expected.next_position();
        Validated::Accept {
            sid,
            first_sp,
            last_sp: first_sp + n - 1,
            records,
            permit,
            done,
        }
    }

    /// Advance the speculative frontier past an accepted batch.
    fn accept_effects(&mut self, sid: u64, last_sp: u64, n: u64) {
        self.heads.insert(sid, last_sp);
        self.next_global += n;
        self.stats.accepted_batches += 1;
        self.stats.events += n;
    }

    /// Validate a drained group in deterministic dequeue order, complete
    /// conflicts/empties inline, submit accepted batches to the real
    /// committer, and predict their positions.
    fn validate_and_submit(&mut self, group: Vec<Intent>) -> Option<Prepared> {
        if let Some(stall) = self.cfg.group_stall {
            std::thread::sleep(stall);
        }
        let mut accepted: Vec<Accepted> = Vec::with_capacity(group.len());
        let mut futs: Vec<AckFut> = Vec::with_capacity(group.len());
        for intent in group {
            let Validated::Accept { sid, first_sp, last_sp, records, permit, done } =
                self.precheck(intent)
            else {
                continue;
            };
            let n = records.len() as u64;
            let events: Vec<EventInput> = records
                .into_iter()
                .map(|r| {
                    let tid = self.intern_type(&r.message_type);
                    EventInput::plain(tid, 0, 0, r.data)
                })
                .collect();
            let req = AppendRequest {
                stream_id: sid,
                category_id: CATEGORY_ID,
                first_stream_version: first_sp,
                events,
            };
            let ap =
                self.appender.as_ref().expect("committer-backed core").clone();
            futs.push(Box::pin(async move { ap.append(req).await }));
            accepted.push(Accepted {
                done,
                _permit: permit,
                last_stream_pos: last_sp,
                pred_first: self.next_global,
                pred_last: self.next_global + n - 1,
            });
            // Speculative overlay: advance immediately so the next intent
            // in this group validates against the post-accept head.
            self.accept_effects(sid, last_sp, n);
        }
        if accepted.is_empty() {
            return None;
        }
        let mut p = Prepared {
            results: (0..futs.len()).map(|_| None).collect(),
            accepted,
            futs,
            group_end: self.next_global,
        };
        // Submit NOW: the first poll of each `Appender::append` future
        // synchronously computes the encoded length and SENDS the request
        // into the committer's gather channel — no `.await` precedes the
        // send — so after this loop the whole group is submitted, in
        // dequeue (== position) order. In B1 this happens while the
        // previous group's barrier is still in flight (the overlap).
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        for (i, fut) in p.futs.iter_mut().enumerate() {
            if let Poll::Ready(out) = fut.as_mut().poll(&mut cx) {
                p.results[i] = Some(out);
            }
        }
        Some(p)
    }

    /// Await the group's acks (position-ordered from the committer), verify
    /// them against the owner's predicted positions, apply/publish (the
    /// heads were speculatively applied at accept; publication is the
    /// watermark Release-store), and complete every waiter. Every accepted
    /// intent reaches a terminal state here regardless of whether its
    /// caller still exists.
    ///
    /// The owner parks on the group's LAST ack first: the committer
    /// fulfills acks in submission (position) order, so once the tail is
    /// acked every earlier future resolves on its next poll without
    /// parking — one owner wake per group instead of one per batch.
    fn retire(&mut self, p: Prepared) {
        let Prepared { accepted, mut futs, mut results, group_end } = p;
        for i in (0..futs.len()).rev() {
            if results[i].is_none() {
                results[i] = Some(self.rt.block_on(&mut futs[i]));
            }
        }
        let outcomes: Vec<_> =
            results.into_iter().map(|r| r.expect("resolved")).collect();
        let mut all_acked = true;
        for (acc, out) in accepted.into_iter().zip(outcomes) {
            match out {
                Ok(AppendOutcome::Acked { first_position, last_position }) => {
                    if first_position != acc.pred_first
                        || last_position != acc.pred_last
                    {
                        self.stats.position_mismatches += 1;
                    }
                    let _ = acc.done.send(Ok(Appended {
                        version: Version::At(acc.last_stream_pos),
                        last_global_position: last_position,
                    }));
                }
                Ok(AppendOutcome::Indeterminate) => {
                    all_acked = false;
                    self.poisoned.store(true, Ordering::Release);
                    let _ = acc.done.send(Err(AppendError::Backend(
                        FlatError::Poisoned("indeterminate durability".into()),
                    )));
                }
                Err(e) => {
                    all_acked = false;
                    self.poisoned.store(true, Ordering::Release);
                    let _ = acc.done.send(Err(AppendError::Backend(
                        FlatError::Poisoned(e.to_string()),
                    )));
                }
            }
        }
        if all_acked {
            // Publish: single Release store. Readers/subscribers load
            // Acquire and are guaranteed every head effect at or below
            // `group_end` is applied.
            self.watermark.store(group_end, Ordering::Release);
            self.stats.groups += 1;
        }
    }

    /// Receive the next intent: bounded busy-poll first (skipping a futex
    /// round trip under sustained load), then park. `None` = ring closed
    /// and drained.
    fn recv_intake(&self, rx: &mut mpsc::Receiver<Intent>) -> Option<Intent> {
        if self.cfg.spin_intake_us > 0 {
            let spin = Duration::from_micros(self.cfg.spin_intake_us);
            let start = std::time::Instant::now();
            loop {
                match rx.try_recv() {
                    Ok(i) => return Some(i),
                    Err(TryRecvError::Disconnected) => return None,
                    Err(TryRecvError::Empty) => {
                        if start.elapsed() >= spin {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            }
        }
        rx.blocking_recv()
    }

    fn gather(
        &mut self,
        rx: &mut mpsc::Receiver<Intent>,
        first: Intent,
    ) -> Vec<Intent> {
        let mut bytes = first.cost;
        let mut group = vec![first];
        // Under a barrier mode, hold the gather window open while any
        // producer is still between append-entry and its send (the D7
        // early-close rule, exactly the committer's own gate) — otherwise a
        // convoy racing the owner's wake is split across two multi-ms
        // barriers (`perf_group_commit` H1's convoy-split). Capped by the
        // Group `max_delay` (1 ms default). Under `Process` there is no
        // barrier to amortize, so only the (µs-scale) spin grace applies.
        let start = std::time::Instant::now();
        let cap = match self.cfg.durability {
            Durability::Group { max_delay, .. } => Some(max_delay),
            Durability::Os | Durability::Process => {
                (self.cfg.spin_gather_us > 0)
                    .then(|| Duration::from_micros(self.cfg.spin_gather_us))
            }
        };
        let deadline = cap.map(|c| start + c);
        let wait_inflight =
            matches!(self.cfg.durability, Durability::Group { .. });
        // Barrier-mode minimum gather grace: a producer released by the
        // previous group's completion spends tens of µs waking and BUILDING
        // its next batch before it re-enters `append_batch` — entirely
        // outside the in-flight signal. Without a floor, the `target` rule
        // collapses (first group of 1 → target 1 → every subsequent window
        // closes on a momentary in-flight zero) and the convoy splits
        // across barriers (observed: 2 batches/fsync vs bare's ~4 at
        // 4 writers). 200 µs against a multi-ms barrier is ≤10% latency
        // for ~2× amortization; `max_delay` stays the strict cap.
        let grace = start + Duration::from_micros(200);
        while group.len() < self.cfg.max_group_intents
            && bytes < self.cfg.max_group_bytes as u64
        {
            match rx.try_recv() {
                Ok(i) => {
                    bytes += i.cost;
                    group.push(i);
                }
                Err(_) => {
                    let open = match deadline {
                        Some(d) => std::time::Instant::now() < d,
                        None => false,
                    };
                    if !open {
                        break;
                    }
                    // Barrier mode: close early once no producer is on its
                    // way (in-flight == 0) AND the convoy has reformed to
                    // the last group's width — a momentary zero right after
                    // a group completes must not split the convoy across
                    // two barriers (`perf_group_commit` H1 note; the real
                    // committer uses the same `target` rule). `max_delay`
                    // is strictly a cap.
                    if wait_inflight
                        && self.inflight.load(Ordering::Relaxed) == 0
                        && group.len() >= self.gather_target
                        && std::time::Instant::now() >= grace
                    {
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }
        self.gather_target = group.len().max(1);
        self.stats.max_group_intents =
            self.stats.max_group_intents.max(group.len() as u64);
        group
    }
}

/// What the owner drives: the real committer thread (B0/B1) or the real
/// segment writer held directly (B0Direct, design.md §6.3).
enum Core {
    Committer {
        committer: Committer<RealRuntime>,
        appender:  Appender,
    },
    Direct {
        writer: SegmentWriter<RealFs>,
    },
}

fn owner_loop(
    rt: RealRuntime,
    core: Core,
    mut rx: mpsc::Receiver<Intent>,
    cfg: FlatConfig,
    watermark: Arc<AtomicU64>,
    poisoned: Arc<AtomicBool>,
    inflight: Arc<AtomicUsize>,
) -> OwnerExit {
    let variant = cfg.variant;
    let mut owner = Owner {
        rt,
        appender: None,
        cfg,
        stream_ids: HashMap::new(),
        type_ids: HashMap::new(),
        heads: HashMap::new(),
        last_type: None,
        next_global: 0,
        watermark,
        poisoned,
        inflight,
        stats: OwnerStats::default(),
        fsync_nanos: 0,
        gather_target: 1,
    };

    match core {
        Core::Committer { committer, appender } => {
            owner.appender = Some(appender);
            match variant {
                Variant::B0 => run_b0(&mut owner, &mut rx),
                Variant::B1 => run_b1(&mut owner, &mut rx),
                Variant::B0Direct => unreachable!("direct core"),
            }
            let m = committer.metrics();
            owner.stats.fsyncs = m.fsync.count;
            owner.stats.mean_fsync_nanos = m.fsync.mean_nanos;
            let rt = owner.rt.clone();
            rt.block_on(committer.shutdown());
        }
        Core::Direct { mut writer } => {
            run_b0_direct(&mut owner, &mut rx, &mut writer);
            if owner.stats.fsyncs > 0 {
                owner.stats.mean_fsync_nanos =
                    owner.fsync_nanos / owner.stats.fsyncs;
            }
        }
    }

    OwnerExit {
        stats:       owner.stats,
        heads:       owner.heads,
        stream_ids:  owner.stream_ids,
        next_global: owner.next_global,
    }
}

/// B0Direct: the owner IS the committer (design.md §6.3). Per group:
/// validate in dequeue order → `SegmentWriter::append` each accepted batch
/// (the REAL v3 encoder + positioned write) → ONE `sync()` barrier for the
/// whole group under `Os`/`Group` (none under `Process`) → publish the
/// watermark → complete waiters. No second thread, no ack round trip.
fn run_b0_direct(
    owner: &mut Owner,
    rx: &mut mpsc::Receiver<Intent>,
    writer: &mut SegmentWriter<RealFs>,
) {
    let barrier = matches!(
        owner.cfg.durability,
        Durability::Os | Durability::Group { .. }
    );
    while let Some(first) = owner.recv_intake(rx) {
        let group = owner.gather(rx, first);
        if let Some(stall) = owner.cfg.group_stall {
            std::thread::sleep(stall);
        }
        type Done =
            oneshot::Sender<Result<Appended, AppendError<FlatError>>>;
        let mut completions: Vec<(Done, OwnedSemaphorePermit, u64, u64)> =
            Vec::with_capacity(group.len());
        for intent in group {
            let Validated::Accept { sid, first_sp, last_sp, records, permit, done } =
                owner.precheck(intent)
            else {
                continue;
            };
            let n = records.len() as u64;
            // Real v3 bytes through the real encoder/writer; subframes
            // borrow the record payloads (zero copy on the owner).
            let tids: Vec<u32> = records
                .iter()
                .map(|r| owner.intern_type(&r.message_type))
                .collect();
            let subs: Vec<Subframe> = records
                .iter()
                .zip(&tids)
                .map(|(r, &tid)| Subframe::plain(tid, 0, 0, &r.data))
                .collect();
            let spec = BatchSpec {
                stream_id:            sid,
                category_id:          CATEGORY_ID,
                first_stream_version: first_sp,
                crypto_chain:         None,
                subframes:            &subs,
            };
            match writer.append(&spec) {
                Ok(receipt) => {
                    if receipt.first_global_pos != owner.next_global {
                        owner.stats.position_mismatches += 1;
                    }
                    owner.accept_effects(sid, last_sp, n);
                    completions.push((
                        done,
                        permit,
                        last_sp,
                        owner.next_global - 1,
                    ));
                }
                Err(e) => {
                    owner.poisoned.store(true, Ordering::Release);
                    let _ = done.send(Err(AppendError::Backend(
                        FlatError::Poisoned(e.to_string()),
                    )));
                }
            }
        }
        if completions.is_empty() {
            continue;
        }
        if barrier {
            let t = std::time::Instant::now();
            match writer.sync() {
                Ok(()) => {
                    owner.stats.fsyncs += 1;
                    owner.fsync_nanos += t.elapsed().as_nanos() as u64;
                }
                Err(e) => {
                    owner.poisoned.store(true, Ordering::Release);
                    for (done, _permit, _, _) in completions {
                        let _ = done.send(Err(AppendError::Backend(
                            FlatError::Poisoned(format!("barrier: {e}")),
                        )));
                    }
                    continue;
                }
            }
        }
        // Publish, then complete: no committed group acks before its
        // reader state (the watermark) is publishable — K3.
        owner.watermark.store(owner.next_global, Ordering::Release);
        owner.stats.groups += 1;
        for (done, _permit, last_sp, last_global) in completions {
            let _ = done.send(Ok(Appended {
                version:              Version::At(last_sp),
                last_global_position: last_global,
            }));
        }
    }
}

/// B0: drain a group → validate → submit → await acks inline → publish.
fn run_b0(owner: &mut Owner, rx: &mut mpsc::Receiver<Intent>) {
    while let Some(first) = owner.recv_intake(rx) {
        let group = owner.gather(rx, first);
        if let Some(p) = owner.validate_and_submit(group) {
            owner.retire(p);
        }
    }
}

/// B1: while group N's ack is pending, drain/validate/submit group N+1
/// against speculative state, then retire group N. Depth-2 pipeline: at
/// most one group's acks outstanding while one more is being built.
fn run_b1(owner: &mut Owner, rx: &mut mpsc::Receiver<Intent>) {
    let mut pending: Option<Prepared> = None;
    loop {
        let first = if pending.is_some() {
            match rx.try_recv() {
                Ok(i) => i,
                Err(TryRecvError::Empty) => {
                    // Nothing to overlap with — retire the in-flight group.
                    let p = pending.take().expect("pending is some");
                    owner.retire(p);
                    continue;
                }
                Err(TryRecvError::Disconnected) => {
                    let p = pending.take().expect("pending is some");
                    owner.retire(p);
                    return;
                }
            }
        } else {
            match owner.recv_intake(rx) {
                Some(i) => i,
                None => return,
            }
        };
        let group = owner.gather(rx, first);
        // `validate_and_submit` performs the channel sends before
        // returning, so group N+1's encode/gather overlaps group N's
        // barrier…
        if let Some(next) = owner.validate_and_submit(group) {
            // …then retire group N (its acks are position-ordered ahead of
            // N+1's, so this await cannot deadlock).
            if let Some(p) = pending.take() {
                owner.retire(p);
            }
            pending = Some(next);
        }
        // A group that was all conflicts/empties submitted nothing; any
        // pending group stays pending and the loop continues draining.
    }
}
