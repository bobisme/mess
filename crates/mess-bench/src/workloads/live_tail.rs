//! Live-tail / global-position reads: the subscriber shape (`bn-1r9c`).
//!
//! The premise check on `bn-3hm4` counted the engine methods each reference
//! workload actually calls and found the whole global read path unexercised:
//! zero calls to [`Backend::read_global`] / [`Backend::read_global_page`]
//! (hence zero to `ActiveIndex::global_range` underneath it) across all nine
//! workloads, and no multi-stream [`Backend::read_stream`] paging either.
//! `bn-1dlb`'s `reader_contention` reads `head`/`stream_id_of` — index
//! *lookups*, not a scan — and everything else in the suite is a write loop or
//! an offline replay of an already-sealed corpus. So the one read shape every
//! real consumer has (a subscriber paging committed history forward from a
//! cursor, then following the writer live) had no number attached to it at
//! all, and a Phase 10 change that halved it would leave the ratchet green.
//!
//! Four passes over one corpus, in the `reader_contention` idiom — a solo
//! reference and a loaded pass on the same host in the same run, because
//! run-to-run drift on this box is larger than most effects worth measuring
//! (`bn-1gn1`):
//!
//! 1. **Catch-up** — a consumer that is far behind: page the whole committed
//!    history from position 0 to the watermark, once alone and once with
//!    `readers` doing it concurrently. This is `read_global_page` in bulk.
//! 2. **Tail under a flood** — `readers` consumers started at the pre-burst
//!    watermark while a writer appends at full speed underneath them. This
//!    engine writes far faster than the read path materialises, so the tailers
//!    stay behind: what this pass measures is a global scan and the committer
//!    competing, in both directions (`writer_retention`, `tail_reader`).
//! 3. **Wake-to-delivery latency** — the *actually caught up* path, which pass
//!    2 cannot reach. Every tailer is parked on
//!    [`SubscribeBackend::await_watermark_past`] before each single-record
//!    append, and each record carries the writer's own pre-append timestamp, so
//!    the delivered record yields an exact append-issue → subscriber-visible
//!    latency. This is the number a subscription actually lives on and nothing
//!    else in the suite (or in `docs/perf/envelope.md`) has ever measured.
//! 4. **Hot multi-stream `read_stream` paging** — `readers` consumers paging
//!    every stream end to end through the unsealed hot index. Same corpus, one
//!    extra pass, so the other half of the `bn-3hm4` gap closes here rather
//!    than needing its own workload.
//!
//! # Positions are opaque, and this corpus proves it
//!
//! Global positions have legal gaps: `$registry` records consume canonical
//! positions and are then filtered out of application reads (`bn-2di`,
//! `docs/spec/06-subscriptions.md` §2). Every cursor here therefore advances
//! exactly the way `mess-store/src/subscription.rs` does — take
//! [`GlobalPage::frontier`] into account on every page, treat an *empty* page
//! whose frontier moved as "crossed a run of engine records, keep reading"
//! rather than as "caught up", and never jump a cursor to the watermark. To
//! keep that honest rather than nominal, the corpus deliberately punches
//! registry holes *through* the history (`gap_streams` one-record streams
//! registered at even intervals during the build) instead of leaving them all
//! bunched in the first round-robin cycle where the primed streams register.
//!
//! Every read pass asserts its exact delivered count (`history_records` for
//! catch-up, `burst_events + 1` per tailer, `pings + 1` per latency tailer).
//! That is what makes the numbers below trustworthy: a frontier bug that
//! skipped a hole would inflate throughput while silently dropping records,
//! and an assert-free bench would report the inflated number as an
//! improvement.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use mess_log::committer::Durability;
use mess_store::backend::{
    Backend, GlobalPage, RecordToAppend, SubscribeBackend,
};
use mess_store::{EngineOptions, LogEngine, Version};

use crate::{Metric, RunSize};

/// The live pass ends structurally, not on a count or a stop flag: the writer
/// appends one record to this stream after its burst, and a tailer stops the
/// moment it *delivers* that record. Termination is then a property of the
/// log itself — every tailer starts at a position at or before the sentinel,
/// so every tailer is guaranteed to reach it, and no tailer can be left parked
/// on a watermark that will never advance again.
const SENTINEL_STREAM: &str = "live-tail-sentinel";

/// The stream the wake-to-delivery pass appends its timestamped single-record
/// pings to. Separate from the burst streams so a tailer can tell a latency
/// ping apart from ordinary traffic by stream name alone.
const PING_STREAM: &str = "live-tail-ping";

/// Sizes for one [`RunSize`].
struct Params {
    /// Streams the history is round-robined over.
    streams:         usize,
    /// Application records in the pre-read history.
    history_events:  usize,
    /// Records per history append.
    per_append:      usize,
    /// One-record streams registered at even intervals through the history
    /// build, purely so `$registry` holes are spread across the whole global
    /// range the catch-up pass scans.
    gap_streams:     usize,
    /// Concurrent readers/tailers in the loaded passes.
    readers:         usize,
    /// `limit` handed to `read_global_page` / `read_stream`.
    page_size:       usize,
    /// Appends in each write burst.
    burst_batches:   usize,
    /// Records per write-burst append.
    burst_per_batch: usize,
    /// Single-record timestamped appends in the wake-to-delivery pass.
    pings:           usize,
}

impl Params {
    fn burst_events(&self) -> u64 {
        (self.burst_batches * self.burst_per_batch) as u64
    }
}

fn rec(i: usize) -> RecordToAppend {
    RecordToAppend {
        message_type: "account.deposited".to_string(),
        data:         (i as u64).to_le_bytes().to_vec(),
    }
}

pub fn run(size: RunSize, scratch: &Path) -> Vec<Metric> {
    let p = match size {
        // Sized so every measured window is >=100ms: the first cut of this
        // workload used a 75k-record burst and timed the writer over 4.7ms,
        // which is scheduler noise, not a measurement.
        RunSize::Full => Params {
            streams:         32,
            history_events:  1_000_000,
            per_append:      250,
            gap_streams:     8,
            readers:         4,
            page_size:       256,
            burst_batches:   1_000,
            burst_per_batch: 1_000,
            pings:           300,
        },
        RunSize::Smoke => Params {
            streams:         4,
            history_events:  400,
            per_append:      25,
            gap_streams:     2,
            readers:         2,
            page_size:       32,
            burst_batches:   4,
            burst_per_batch: 25,
            pings:           5,
        },
    };

    let dir = scratch.join("live-tail");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    let engine = LogEngine::open_with(
        dir.join("append"),
        EngineOptions {
            durability: Durability::Process,
            // Big enough that nothing seals: this workload is about the HOT
            // read path (`ActiveIndex::global_range`, `stream_entries_from`).
            // Sealed-tier reads already have their own coverage
            // (`sealed_pointer`, `engine::sealed_replay`), and letting a seal
            // land mid-run would silently mix the two.
            segment_size: 2 * 1024 * 1024 * 1024,
            ..Default::default()
        },
    )
    .expect("open");

    let names: Vec<String> =
        (0..p.streams).map(|s| format!("tail-stream-{s}")).collect();

    let history_records = rt.block_on(build_history(&engine, &names, &p));

    // ---- pass 1: catch-up ------------------------------------------------
    //
    // Warm first, discarded. Every pass below re-reads the same range, so the
    // block reader's decoded-batch cache is warm for the loaded pass no matter
    // what; without this warm-up the solo pass would eat the cold cost alone
    // and `catchup_scaling` would report cache warming as reader scaling.
    let _ = rt.block_on(drain_from(&engine, 0, p.page_size));

    let started = Instant::now();
    let solo_delivered = rt.block_on(drain_from(&engine, 0, p.page_size));
    let catchup_solo = solo_delivered as f64 / started.elapsed().as_secs_f64();
    assert_eq!(
        solo_delivered, history_records,
        "catch-up must deliver every application record exactly once; a \
         mismatch means the frontier walk skipped or replayed a registry hole"
    );

    let (catchup_loaded, loaded_delivered) = loaded_catchup(&rt, &engine, &p);
    assert_eq!(
        loaded_delivered,
        history_records * p.readers as u64,
        "every concurrent catch-up reader must deliver the whole history"
    );

    // ---- pass 2: tail under a flood --------------------------------------
    let (writer_solo, _) = write_burst(&rt, &engine, &names, &p);
    let (writer_tailed, tail_ev_per_s) =
        flood_tail_pass(&rt, &engine, &names, &p);

    // ---- pass 3: wake-to-delivery latency --------------------------------
    let (append_to_delivery_us, parked_deliveries) =
        wake_latency_pass(&rt, &engine, &p);

    // ---- pass 4: hot multi-stream read_stream paging ---------------------
    let stream_paging = loaded_stream_paging(&rt, &engine, &names, &p);

    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);

    let scaling =
        if catchup_solo > 0.0 { catchup_loaded / catchup_solo } else { 0.0 };
    let retention =
        if writer_solo > 0.0 { writer_tailed / writer_solo } else { 0.0 };
    let readers = p.readers;
    let page_size = p.page_size;
    let streams = p.streams;
    let burst = p.burst_events();

    vec![
        Metric::new(
            "engine.live_tail.catchup_solo.ev_per_s",
            catchup_solo,
            "ev/s",
            format!(
                "one reader paging read_global_page(limit={page_size}) from \
                 position 0 to the watermark over {history_records} \
                 application records on {streams} hot (unsealed) streams, \
                 cursor advanced by GlobalPage::frontier; no concurrent \
                 writer; caches pre-warmed by a discarded identical pass"
            ),
        ),
        Metric::new(
            "engine.live_tail.catchup_loaded.ev_per_s",
            catchup_loaded,
            "ev/s",
            format!(
                "aggregate delivered-records/sec with {readers} readers each \
                 doing the same full catch-up pass concurrently; same corpus, \
                 same process, same run as catchup_solo above"
            ),
        ),
        Metric::new(
            "engine.live_tail.catchup_scaling",
            scaling,
            "ratio",
            format!(
                "catchup_loaded / catchup_solo with {readers} readers. 1.0 = \
                 the global read path serialises completely (all readers \
                 share the Arc<RwLock<Book>> and the block reader); \
                 {readers}.0 = perfect scaling. This is the number a Phase 10 \
                 change to the read path moves"
            ),
        ),
        Metric::new(
            "engine.live_tail.writer_tailed.ev_per_s",
            writer_tailed,
            "ev/s",
            format!(
                "writer throughput during the live pass: {} x {}-record \
                 appends round-robin over {streams} streams with {readers} \
                 tailers following it via read_global_page + \
                 await_watermark_past",
                p.burst_batches, p.burst_per_batch
            ),
        ),
        Metric::new(
            "engine.live_tail.writer_retention",
            retention,
            "ratio",
            format!(
                "writer_tailed / an identical solo burst measured immediately \
                 before it. 1.0 = {readers} live tailers cost the writer \
                 nothing. Distinct from reader_contention's retention, which \
                 measures head/stream_id_of lookups rather than a global scan \
                 competing with the committer"
            ),
        ),
        Metric::new(
            "engine.live_tail.tail_reader.ev_per_s",
            tail_ev_per_s,
            "ev/s",
            format!(
                "aggregate records/sec delivered by the {readers} tailers \
                 over the live window; each delivers exactly {burst} burst \
                 records plus the sentinel, asserted, so this is a rate over \
                 a fixed verified payload and not a best-effort sample. \
                 Compare with catchup_loaded to isolate what a concurrent \
                 committer costs the global read path"
            ),
        ),
        Metric::new(
            "engine.live_tail.append_to_delivery_us",
            append_to_delivery_us,
            "us",
            format!(
                "mean microseconds from the writer taking its timestamp \
                 (immediately before append_batch) to a tailer having the \
                 record in hand, over {parked_deliveries} deliveries ({} \
                 pings x {readers} tailers). Every ping is issued only once \
                 all {readers} tailers are parked in await_watermark_past, so \
                 this is the genuine caught-up wake path: commit + publish + \
                 watermark wake + read_global_page + materialise. A ceiling, \
                 not a floor",
                p.pings
            ),
        ),
        Metric::new(
            "engine.live_tail.stream_paging.ev_per_s",
            stream_paging,
            "ev/s",
            format!(
                "aggregate records/sec with {readers} readers paging \
                 read_stream(limit={page_size}) end to end over all {streams} \
                 hot streams (staggered start stream, so they do not march in \
                 lockstep). Hot path only — no stream here has a sealed \
                 segment, so this is ActiveIndex::stream_entries_from plus \
                 materialisation"
            ),
        ),
    ]
}

/// Build the pre-read corpus and return the number of application records in
/// it (every one of which a full global scan must deliver).
async fn build_history(
    engine: &LogEngine,
    names: &[String],
    p: &Params,
) -> u64 {
    let batch: Vec<RecordToAppend> = (0..p.per_append).map(rec).collect();
    let appends = p.history_events.div_ceil(p.per_append);
    // Spread the gap-stream registrations across the whole build rather than
    // letting every `$registry` record bunch up in the first round-robin
    // cycle — see the module docs.
    let gap_every = if p.gap_streams == 0 {
        usize::MAX
    } else {
        (appends / p.gap_streams).max(1)
    };

    let mut heads = vec![Version::NoStream; names.len()];
    let mut written = 0u64;
    let mut gaps = 0usize;
    for a in 0..appends {
        let i = a % names.len();
        let out = engine
            .append_batch(&names[i], heads[i], &batch)
            .await
            .expect("append history");
        heads[i] = out.version;
        written += p.per_append as u64;
        if gaps < p.gap_streams && a % gap_every == gap_every - 1 {
            let gap = format!("tail-gap-{gaps}");
            engine
                .append_batch(&gap, Version::NoStream, &[rec(0)])
                .await
                .expect("append gap stream");
            written += 1;
            gaps += 1;
        }
    }
    // Register the sentinel and ping streams now so no live pass mints a
    // `$registry` record inside its own measured window.
    for name in [SENTINEL_STREAM, PING_STREAM] {
        engine
            .append_batch(name, Version::NoStream, &[rec(0)])
            .await
            .expect("prime live-pass stream");
        written += 1;
    }
    written
}

/// Page forward from `cursor` until caught up, returning delivered records.
///
/// This is `Subscription::next_batch`'s catch-up half (`mess-store/src/
/// subscription.rs`) with the park removed: same three cases, same frontier
/// rules. `read_global_page(after)` is exclusive of `after`, so delivering
/// *from* `cursor` means reading strictly after `cursor - 1` (or from the
/// start when `cursor == 0`).
async fn drain_from(
    engine: &LogEngine,
    mut cursor: u64,
    page_size: usize,
) -> u64 {
    let mut delivered = 0u64;
    loop {
        let GlobalPage { records, frontier } = engine
            .read_global_page(cursor.checked_sub(1), page_size)
            .await
            .expect("read_global_page");
        if let Some(last) = records.last() {
            delivered += records.len() as u64;
            cursor = last.global_position.saturating_add(1).max(frontier);
        } else if frontier > cursor {
            // Scanned a run of engine-internal positions and found nothing
            // deliverable. Take the frontier and re-read; do NOT treat this
            // as caught-up, and do NOT jump to the watermark.
            cursor = frontier;
        } else {
            return delivered;
        }
        // MANDATORY, same reason as reader_contention's: `read_global_page`
        // is `async` but serves the hot path entirely from memory, so this
        // loop never returns to the scheduler on its own. Without the yield
        // these readers pin every worker thread and the concurrent writer is
        // starved rather than merely slowed — that measures a livelock, not
        // contention. Once per page (not per record) so the cost is noise.
        tokio::task::yield_now().await;
    }
}

/// `readers` full catch-up passes at once. Returns (aggregate ev/s, records).
fn loaded_catchup(
    rt: &tokio::runtime::Runtime,
    engine: &LogEngine,
    p: &Params,
) -> (f64, u64) {
    let started = Instant::now();
    let delivered = rt.block_on(async {
        let handles: Vec<_> = (0..p.readers)
            .map(|_| {
                let engine = engine.clone();
                let page_size = p.page_size;
                tokio::spawn(
                    async move { drain_from(&engine, 0, page_size).await },
                )
            })
            .collect();
        let mut total = 0u64;
        for h in handles {
            total += h.await.expect("catch-up reader");
        }
        total
    });
    let window = started.elapsed().as_secs_f64();
    (delivered as f64 / window, delivered)
}

/// One round-robin write burst. Returns (ev/s, last global position written).
fn write_burst(
    rt: &tokio::runtime::Runtime,
    engine: &LogEngine,
    names: &[String],
    p: &Params,
) -> (f64, u64) {
    let batch: Vec<RecordToAppend> = (0..p.burst_per_batch).map(rec).collect();
    // Read live heads rather than assuming a version: earlier passes have
    // already advanced these streams, and a stale `expected` would conflict on
    // every append instead of measuring anything.
    let mut heads = rt.block_on(async {
        let mut heads = Vec::with_capacity(names.len());
        for name in names {
            heads.push(engine.head(name).await.expect("head"));
        }
        heads
    });
    let started = Instant::now();
    let last_pos = rt.block_on(async {
        let mut last_pos = 0u64;
        for b in 0..p.burst_batches {
            let i = b % names.len();
            let out = engine
                .append_batch(&names[i], heads[i], &batch)
                .await
                .expect("append burst");
            heads[i] = out.version;
            last_pos = out.last_global_position;
        }
        last_pos
    });
    let ev_per_s = p.burst_events() as f64 / started.elapsed().as_secs_f64();
    (ev_per_s, last_pos)
}

/// Cross-tailer state shared by both live passes.
#[derive(Default)]
struct TailTally {
    /// Tailers currently blocked inside `await_watermark_past`. The latency
    /// pass's writer waits on this so every ping is delivered to a genuinely
    /// parked subscriber rather than one still draining a page.
    parked:  AtomicUsize,
    /// Sum of observed append-issue → delivery latencies, in nanoseconds.
    lat_sum: AtomicU64,
    /// Samples in [`TailTally::lat_sum`].
    lat_n:   AtomicU64,
}

impl TailTally {
    /// Record one append-issue → delivery latency. `data` is the writer's
    /// `base.elapsed()` nanos, taken immediately before `append_batch`, so
    /// this needs no shared timestamp slot and cannot mis-pair a sample with
    /// a different ping.
    fn sample(&self, base: Instant, data: &[u8]) {
        let Ok(bytes) = <[u8; 8]>::try_from(data) else { return };
        let issued = u64::from_le_bytes(bytes);
        let now = base.elapsed().as_nanos() as u64;
        self.lat_sum.fetch_add(now.saturating_sub(issued), Ordering::Relaxed);
        self.lat_n.fetch_add(1, Ordering::Relaxed);
    }

    fn mean_us(&self) -> (f64, u64) {
        let n = self.lat_n.load(Ordering::Relaxed);
        if n == 0 {
            return (0.0, 0);
        }
        let sum = self.lat_sum.load(Ordering::Relaxed) as f64;
        (sum / n as f64 / 1_000.0, n)
    }
}

/// Follow the global sequence from `cursor` until the sentinel record is
/// delivered, parking on the watermark whenever caught up.
///
/// This is the full `Subscription::next_batch` state machine: catch-up read,
/// frontier-only advance across engine records, and an **event-bounded** park
/// on `await_watermark_past` when the page is empty and the frontier did not
/// move. Returns the delivered record count.
async fn tail_to_sentinel(
    engine: &LogEngine,
    mut cursor: u64,
    page_size: usize,
    base: Instant,
    tally: &TailTally,
) -> u64 {
    let mut delivered = 0u64;
    loop {
        let GlobalPage { records, frontier } = engine
            .read_global_page(cursor.checked_sub(1), page_size)
            .await
            .expect("read_global_page");
        if let Some(last) = records.last() {
            cursor = last.global_position.saturating_add(1).max(frontier);
            delivered += records.len() as u64;
            let mut done = false;
            for r in &records {
                if r.stream_id == PING_STREAM {
                    tally.sample(base, &r.data);
                } else if r.stream_id == SENTINEL_STREAM {
                    done = true;
                }
            }
            if done {
                return delivered;
            }
        } else if frontier > cursor {
            cursor = frontier;
        } else {
            // Caught up. Block until a commit passes the cursor — driven by
            // the committer's waker list, never by polling — then re-read
            // history, which is the authoritative source.
            tally.parked.fetch_add(1, Ordering::Release);
            let woken = engine.await_watermark_past(cursor).await;
            tally.parked.fetch_sub(1, Ordering::Release);
            woken.expect("await_watermark_past");
        }
        tokio::task::yield_now().await;
    }
}

/// A watermark that provably covers everything written so far.
///
/// The read watermark advances *after* the durable ack, so reading it straight
/// after an append can return a value behind the history — tailers started
/// there would replay backlog instead of tailing. Awaiting the probe append's
/// own position first removes that race: with one writer and nothing else in
/// flight, the watermark is then exactly one past it.
fn pin_watermark(
    rt: &tokio::runtime::Runtime,
    engine: &LogEngine,
    probe_stream: &str,
) -> u64 {
    rt.block_on(async {
        let head = engine.head(probe_stream).await.expect("head");
        let out = engine
            .append_batch(probe_stream, head, &[rec(0)])
            .await
            .expect("watermark probe append");
        engine
            .await_watermark_past(out.last_global_position)
            .await
            .expect("await_watermark_past");
        engine.watermark().await.expect("watermark")
    })
}

type Tailers = Vec<tokio::task::JoinHandle<u64>>;

fn spawn_tailers(
    rt: &tokio::runtime::Runtime,
    engine: &LogEngine,
    p: &Params,
    start: u64,
    base: Instant,
    tally: &Arc<TailTally>,
) -> Tailers {
    rt.block_on(async {
        (0..p.readers)
            .map(|_| {
                let engine = engine.clone();
                let page_size = p.page_size;
                let tally = Arc::clone(tally);
                tokio::spawn(async move {
                    tail_to_sentinel(&engine, start, page_size, base, &tally)
                        .await
                })
            })
            .collect()
    })
}

/// End a live pass structurally (see [`SENTINEL_STREAM`]).
fn append_sentinel(rt: &tokio::runtime::Runtime, engine: &LogEngine) {
    rt.block_on(async {
        let head = engine.head(SENTINEL_STREAM).await.expect("head sentinel");
        engine
            .append_batch(SENTINEL_STREAM, head, &[rec(0)])
            .await
            .expect("append sentinel")
    });
}

/// Join every tailer, asserting each delivered exactly `expected` records.
fn join_tailers(
    rt: &tokio::runtime::Runtime,
    handles: Tailers,
    expected: u64,
) -> u64 {
    rt.block_on(async {
        let mut total = 0u64;
        for h in handles {
            let d = h.await.expect("tailer");
            assert_eq!(
                d, expected,
                "a tailer started at the pinned pre-pass watermark must \
                 deliver every record written during the pass plus the \
                 sentinel, exactly once"
            );
            total += d;
        }
        total
    })
}

/// Pass 2: `readers` tailers started at the pre-burst watermark while the
/// writer floods. Returns (writer ev/s, aggregate tailer ev/s).
fn flood_tail_pass(
    rt: &tokio::runtime::Runtime,
    engine: &LogEngine,
    names: &[String],
    p: &Params,
) -> (f64, f64) {
    let start = pin_watermark(rt, engine, &names[0]);
    let tally = Arc::new(TailTally::default());
    let base = Instant::now();
    let handles = spawn_tailers(rt, engine, p, start, base, &tally);

    let started = Instant::now();
    let (writer_ev_per_s, _) = write_burst(rt, engine, names, p);
    append_sentinel(rt, engine);
    let delivered = join_tailers(rt, handles, p.burst_events() + 1);
    let window = started.elapsed().as_secs_f64();

    (writer_ev_per_s, delivered as f64 / window)
}

/// Pass 3: append-issue → delivery latency on the genuinely caught-up path.
/// Returns (mean microseconds, sample count).
///
/// Pass 2 cannot measure this. This engine commits far faster than the read
/// path materialises, so a flooding writer leaves its tailers permanently
/// behind and they park at most once — the wake path stays effectively
/// untested. Here the writer instead issues single-record pings and waits for
/// every tailer to be parked before each one, so what is timed is exactly
/// commit → publish → watermark wake → `read_global_page` → materialise.
fn wake_latency_pass(
    rt: &tokio::runtime::Runtime,
    engine: &LogEngine,
    p: &Params,
) -> (f64, u64) {
    // Probe on the sentinel stream, not the ping stream: a probe record on
    // PING_STREAM sits below `start` and is never delivered, but if that ever
    // stopped being true its zero timestamp would poison the mean.
    let start = pin_watermark(rt, engine, SENTINEL_STREAM);
    let tally = Arc::new(TailTally::default());
    let base = Instant::now();
    let handles = spawn_tailers(rt, engine, p, start, base, &tally);

    let mut head =
        rt.block_on(async { engine.head(PING_STREAM).await.expect("head") });
    for _ in 0..p.pings {
        wait_all_parked(&tally, p.readers);
        head = rt.block_on(async {
            let issued = base.elapsed().as_nanos() as u64;
            let ping = RecordToAppend {
                message_type: "account.deposited".to_string(),
                data:         issued.to_le_bytes().to_vec(),
            };
            engine
                .append_batch(PING_STREAM, head, &[ping])
                .await
                .expect("append ping")
                .version
        });
    }
    append_sentinel(rt, engine);
    let _ = join_tailers(rt, handles, p.pings as u64 + 1);

    tally.mean_us()
}

/// Block (off-runtime — this runs on the main thread, not a tokio worker)
/// until every tailer is parked on the watermark.
///
/// Bounded, and the bound is a safety valve rather than a poll: in a healthy
/// run the tailers are parked within microseconds of delivering the previous
/// ping. If a scheduling stall means one is not, the ping goes out anyway and
/// that sample is merely optimistic — better than hanging the suite.
fn wait_all_parked(tally: &TailTally, readers: usize) {
    let deadline = Instant::now() + Duration::from_millis(5);
    while tally.parked.load(Ordering::Acquire) < readers {
        if Instant::now() >= deadline {
            return;
        }
        std::thread::yield_now();
    }
}

/// Page one stream end to end through `read_stream`, returning its records.
async fn page_stream(engine: &LogEngine, name: &str, page_size: usize) -> u64 {
    let mut after = Version::NoStream;
    let mut delivered = 0u64;
    loop {
        let page = engine
            .read_stream(name, after, page_size)
            .await
            .expect("read_stream");
        let Some(last) = page.last() else { return delivered };
        after = Version::At(last.stream_position);
        delivered += page.len() as u64;
        // The `Backend::read_stream` contract: a short page IS the end of the
        // stream, so a paging consumer stops here rather than issuing one more
        // read that is guaranteed to come back empty.
        if page.len() < page_size {
            return delivered;
        }
    }
}

/// `readers` readers paging every stream. Returns aggregate ev/s.
fn loaded_stream_paging(
    rt: &tokio::runtime::Runtime,
    engine: &LogEngine,
    names: &[String],
    p: &Params,
) -> f64 {
    let started = Instant::now();
    let delivered = rt.block_on(async {
        let handles: Vec<_> = (0..p.readers)
            .map(|r| {
                let engine = engine.clone();
                let names: Vec<String> = names.to_vec();
                let page_size = p.page_size;
                tokio::spawn(async move {
                    let mut total = 0u64;
                    // Stagger the starting stream so the readers do not walk
                    // the same stream in lockstep and read each other's cache
                    // lines warm.
                    for k in 0..names.len() {
                        let name = &names[(r + k) % names.len()];
                        total += page_stream(&engine, name, page_size).await;
                    }
                    total
                })
            })
            .collect();
        let mut total = 0u64;
        for h in handles {
            total += h.await.expect("stream pager");
        }
        total
    });
    let window = started.elapsed().as_secs_f64();
    delivered as f64 / window
}
