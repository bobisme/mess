//! # Seed-throughput profile spike (`bn-1jg`)
//!
//! D10 measurement spike: **why is the social seeder ~2.3 ms/command** (large
//! tier, 54,680 events / 126 s pipelined) **when the hot-post bench warm path
//! is ~53 us/command — a ~40x gap?**
//!
//! The workload difference the spike interrogates: the seeder makes almost
//! every action a **brand-new stream** (each relationship / like / post is its
//! own stream), so nearly every command is a *cold miss that creates a new
//! stream*; the hot-post bench hammers ONE warm stream. This harness reproduces
//! both shapes over the real production backend
//! (`EventStore<FjallSnapshotBackend<LogEngine>>`, `Durability::Process`, the
//! seeder's exact construction) and decomposes the per-command latency into its
//! phases so the dominant cost is *measured*, not argued.
//!
//! ## Two instruments (METHOD §1)
//!
//! 1. [`ProfilingBackend<B>`] — a `Backend + SnapshotStore + SubscribeBackend`
//!    wrapper that delegates every call and records its latency into a
//!    per-call-kind histogram (`append_batch`, `read_stream`, `head`,
//!    `read_global`, `load_snapshot`, `save_snapshot`). It lives **entirely in
//!    this harness** — no `mess-*` production path is touched.
//! 2. A stopwatch-instrumented **copy of the `command_cached` cold-miss loop**
//!    ([`timed_command`]) that times each phase — cache probe, `load_cached`,
//!    decide, encode+append, write-through fold + cache put — built only from
//!    the public `EventStore` surface, so the phases add up to the measured
//!    total (the residue is reported as its own row).
//!
//! ## Workloads (METHOD §3), all release, all deterministic
//!
//! - **(a) all-new-streams** — the seeder shape: every command creates a new
//!   stream and appends one event.
//! - **(b) all-existing-streams warm** — the bench shape (control): every
//!   command hits ONE cached stream.
//! - **(c) all-existing-streams, cold cache** — fill N streams, then measure a
//!   first-touch command on each over a *fresh* cache: a cache miss + real
//!   snapshot-lookup + tail load, but the append is to an **existing** stream
//!   (no new-stream registry write). Separates first-touch load cost from the
//!   new-stream cost.
//! - **(d) all-new-streams over plain `LogEngine`** — H3: the same new-stream
//!   append over `EventStore<LogEngine>` (no `FjallSnapshotBackend` wrapper),
//!   via the base `command` path (the plain engine is not a `SnapshotStore`).
//! - **(e) all-new-streams sequential vs pipelined** — H4: wall-clock/n and the
//!   per-command latency *inside* the k=32 pipeline.
//!
//! ## Running it
//!
//! The full matrix is `#[ignore]`d (it seeds tens of thousands of real-fs
//! commands). Run it release, with the real-fs TMPDIR:
//!
//! ```text
//! CLANG_PATH=/usr/bin/clang TMPDIR=$HOME/.cache/mess-test-tmp \
//!   cargo test -p social --release --test seed_profile -- --ignored --nocapture
//! ```
//!
//! Sizes are overridable via env vars (`SEED_PROFILE_NEW`, `SEED_PROFILE_WARM`,
//! `SEED_PROFILE_COLD`, `SEED_PROFILE_PLAIN`, `SEED_PROFILE_PIPE`,
//! `SEED_PROFILE_PIPE_SEQ`, `SEED_PROFILE_K`). A tiny non-ignored smoke variant
//! ([`seed_profile_smoke`]) runs in the normal suite and asserts only the
//! *shape* (new-stream append ≫ warm append), never absolute timings.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mess_core::Decide;
use mess_derive::{Aggregate, Event};
use mess_store::{
    AppendError, Appended, Backend, EventStore, FjallSnapshotBackend,
    LogEngine, OwnedAppendBatch, RecordToAppend, SnapshotStore, Snapshottable,
    StateCodecError, StoredRecord, StoredSnapshot, SubscribeBackend, Version,
};
use mess_testkit::{SweepingTempDir, sweeping_temp_dir};
use tokio::task::JoinSet;

// ===========================================================================
// The measured aggregate: a one-event relationship stream.
// ===========================================================================
//
// Each command appends exactly one event to its stream — the seeder's
// relationship / like shape, where the interesting cost is per-command
// plumbing, not fold depth. `decide` always emits (no rejection path) so a
// stream can be touched repeatedly (workload (c) appends a second event to an
// existing stream). One tiny payload keeps encode/append cost representative of
// a real payload-light relationship event.

#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "rel-touch", version = 1)]
enum RelEvent {
    Touched { seq: u32 },
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Aggregate)]
#[aggregate(event = RelEvent)]
struct Rel {
    count: u32,
}

impl Rel {
    fn apply(&mut self, _event: &RelEvent) { self.count += 1; }
}

/// The command: touch the stream once. Always emits exactly one event.
#[derive(Debug, Clone, Copy)]
struct Touch;

impl Decide<Touch> for Rel {
    type Rejection = std::convert::Infallible;

    fn decide(&self, _cmd: Touch) -> Result<Vec<RelEvent>, Self::Rejection> {
        Ok(vec![RelEvent::Touched { seq: self.count }])
    }
}

impl Snapshottable for Rel {
    const FOLD_VERSION: u32 = 1;

    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        Ok(self.count.to_le_bytes().to_vec())
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        let arr: [u8; 4] = bytes
            .try_into()
            .map_err(|_| StateCodecError("rel blob len".to_string()))?;
        Ok(Rel { count: u32::from_le_bytes(arr) })
    }
}

// ===========================================================================
// Latency histograms.
// ===========================================================================

/// Min / p50 / mean / p99 / max over a sample of durations.
#[derive(Clone, Copy)]
struct Stats {
    n:    usize,
    min:  Duration,
    p50:  Duration,
    mean: Duration,
    p99:  Duration,
    max:  Duration,
}

impl Stats {
    fn of(mut samples: Vec<Duration>) -> Option<Stats> {
        if samples.is_empty() {
            return None;
        }
        samples.sort_unstable();
        let n = samples.len();
        let sum: Duration = samples.iter().sum();
        let idx99 = ((n * 99) / 100).min(n - 1);
        Some(Stats {
            n,
            min: samples[0],
            p50: samples[n / 2],
            mean: sum / n as u32,
            p99: samples[idx99],
            max: samples[n - 1],
        })
    }
}

/// A thread-safe map of `call-kind -> latencies`, shared behind an `Arc` so a
/// cloned [`ProfilingBackend`] (the pipelined workload clones the store)
/// records into the same histograms.
#[derive(Clone, Default)]
struct Hist {
    inner: Arc<Mutex<HashMap<&'static str, Vec<Duration>>>>,
}

impl Hist {
    fn record(&self, key: &'static str, d: Duration) {
        self.inner.lock().expect("hist lock").entry(key).or_default().push(d);
    }

    fn reset(&self) { self.inner.lock().expect("hist lock").clear(); }

    fn stats(&self, key: &str) -> Option<Stats> {
        let guard = self.inner.lock().expect("hist lock");
        Stats::of(guard.get(key)?.clone())
    }
}

// ===========================================================================
// ProfilingBackend<B>: delegating wrapper that times every backend call.
// ===========================================================================

/// A `Backend` (and `SnapshotStore` / `SubscribeBackend`) wrapper that
/// delegates every operation to `inner` and records the call latency into a
/// shared [`Hist`]. Adds one `Instant::now()` + one `Vec` push per call; the
/// guard is never held across the delegated `await`, so the futures stay
/// `Send`.
#[derive(Clone)]
struct ProfilingBackend<B> {
    inner: B,
    hist:  Hist,
}

impl<B> ProfilingBackend<B> {
    fn new(inner: B, hist: Hist) -> Self { Self { inner, hist } }
}

impl<B: Backend> Backend for ProfilingBackend<B> {
    type Error = B::Error;

    async fn head(&self, stream_id: &str) -> Result<Version, Self::Error> {
        let t = Instant::now();
        let r = self.inner.head(stream_id).await;
        self.hist.record("head", t.elapsed());
        r
    }

    async fn read_stream(
        &self,
        stream_id: &str,
        after: Version,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        let t = Instant::now();
        let r = self.inner.read_stream(stream_id, after, limit).await;
        self.hist.record("read_stream", t.elapsed());
        r
    }

    async fn read_global(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        let t = Instant::now();
        let r = self.inner.read_global(after, limit).await;
        self.hist.record("read_global", t.elapsed());
        r
    }

    async fn append_batch(
        &self,
        stream_id: &str,
        expected: Version,
        records: &[RecordToAppend],
    ) -> Result<Appended, AppendError<Self::Error>> {
        let t = Instant::now();
        let r = self.inner.append_batch(stream_id, expected, records).await;
        self.hist.record("append_batch", t.elapsed());
        r
    }

    async fn append_batch_owned(
        &self,
        stream_id: &str,
        expected: Version,
        batch: OwnedAppendBatch,
    ) -> Result<Appended, AppendError<Self::Error>> {
        let t = Instant::now();
        let r = self.inner.append_batch_owned(stream_id, expected, batch).await;
        self.hist.record("append_batch", t.elapsed());
        r
    }
}

impl<B: SnapshotStore> SnapshotStore for ProfilingBackend<B> {
    async fn save_snapshot(
        &self,
        stream_id: &str,
        snapshot: StoredSnapshot,
    ) -> Result<(), Self::Error> {
        let t = Instant::now();
        let r = self.inner.save_snapshot(stream_id, snapshot).await;
        self.hist.record("save_snapshot", t.elapsed());
        r
    }

    async fn load_snapshot(
        &self,
        stream_id: &str,
    ) -> Result<Option<StoredSnapshot>, Self::Error> {
        let t = Instant::now();
        let r = self.inner.load_snapshot(stream_id).await;
        self.hist.record("load_snapshot", t.elapsed());
        r
    }
}

impl<B: SubscribeBackend> SubscribeBackend for ProfilingBackend<B> {
    async fn watermark(&self) -> Result<u64, Self::Error> {
        self.inner.watermark().await
    }

    async fn await_watermark_past(&self, pos: u64) -> Result<(), Self::Error> {
        self.inner.await_watermark_past(pos).await
    }
}

// ===========================================================================
// Store construction — the seeder's exact production shape.
// ===========================================================================

type SnapBackend = FjallSnapshotBackend<LogEngine>;
type ProfSnap = ProfilingBackend<SnapBackend>;

/// Open a fresh `FjallSnapshotBackend<LogEngine>` on a self-sweeping temp dir
/// (the real-fs TMPDIR rule), wrapped in a [`ProfilingBackend`]. `Durability`
/// is the engine default (`Process`) — the seeder's exact construction
/// (`open_store` → `LogEngine::open` → default options).
fn fresh_prof_backend(tag: &str) -> (ProfSnap, Hist, SweepingTempDir) {
    let dir = sweeping_temp_dir(tag);
    let engine = LogEngine::open(dir.path().join("log")).expect("open engine");
    let backend = FjallSnapshotBackend::open(engine, dir.path().join("snap"))
        .expect("open snapshot backend");
    let hist = Hist::default();
    (ProfilingBackend::new(backend, hist.clone()), hist, dir)
}

const CACHE_CAP: usize = 4096;

fn stream_name(i: usize) -> String { format!("rel-{i:08x}") }

// ===========================================================================
// The instrumented command_cached cold-miss loop (METHOD §1.2).
// ===========================================================================

/// Phase-attributed per-command spans, keyed and ordered for the report table.
#[derive(Default)]
struct Spans {
    probe:     Vec<Duration>,
    load:      Vec<Duration>,
    decide:    Vec<Duration>,
    append:    Vec<Duration>,
    cache_put: Vec<Duration>,
    total:     Vec<Duration>,
}

/// A faithful copy of `EventStore::command_cached`'s cold-miss path, built from
/// the public surface only, with a stopwatch around each phase. No conflict /
/// retry path (the workloads never contend within one stream). Returns nothing;
/// records into `spans`.
///
/// This mirrors `store.rs`: cache probe → (miss) `load_cached` → `decide` →
/// encode+`append` → write-through fold + `cache.put`.
async fn timed_command(
    store: &EventStore<ProfSnap>,
    stream: &str,
    spans: &mut Spans,
) {
    let outer = Instant::now();

    let t = Instant::now();
    let cached = store.cache().get::<Rel>(stream);
    spans.probe.push(t.elapsed());

    let (version, state) = match cached {
        Some(warm) => warm,
        None => {
            let t = Instant::now();
            let loaded =
                store.load_cached::<Rel>(stream).await.expect("load_cached");
            spans.load.push(t.elapsed());
            (loaded.version, loaded.state)
        }
    };

    let t = Instant::now();
    let events = state.decide(Touch).expect("decide (infallible)");
    spans.decide.push(t.elapsed());

    // encode + append: `append` re-encodes internally, so this span is
    // encode+append_batch. The ProfilingBackend's `append_batch` histogram
    // isolates the backend component within it.
    let t = Instant::now();
    let commit = store
        .append::<RelEvent>(stream, version, &events)
        .await
        .expect("append");
    spans.append.push(t.elapsed());

    let t = Instant::now();
    let mut folded = state;
    for e in &events {
        folded.apply(e);
    }
    store.cache().put::<Rel>(stream, commit.version, folded);
    spans.cache_put.push(t.elapsed());

    spans.total.push(outer.elapsed());
}

// ===========================================================================
// Report rendering.
// ===========================================================================

fn ms(d: Duration) -> String { format!("{:>9.4}", d.as_secs_f64() * 1e3) }

fn stat_row(label: &str, s: Option<Stats>) {
    match s {
        Some(s) => println!(
            "  {label:<26} n={:<6} min={} p50={} mean={} p99={} max={} (ms)",
            s.n,
            ms(s.min),
            ms(s.p50),
            ms(s.mean),
            ms(s.p99),
            ms(s.max),
        ),
        None => println!("  {label:<26} (no samples)"),
    }
}

fn phase_table(title: &str, spans: &Spans) {
    println!(
        "--- {title}: per-phase attribution (instrumented command loop) ---"
    );
    let probe = Stats::of(spans.probe.clone());
    let load = Stats::of(spans.load.clone());
    let decide = Stats::of(spans.decide.clone());
    let append = Stats::of(spans.append.clone());
    let cache_put = Stats::of(spans.cache_put.clone());
    let total = Stats::of(spans.total.clone());
    stat_row("1 cache probe", probe);
    stat_row("2 load_cached (miss)", load);
    stat_row("3 decide", decide);
    stat_row("4 encode+append", append);
    stat_row("5 write-through+put", cache_put);
    stat_row("= measured total", total);

    // Attribution sum vs total, with an explicit residue row (METHOD §4).
    let mean_of = |s: Option<Stats>| s.map_or(Duration::ZERO, |x| x.mean);
    let attributed = mean_of(probe)
        + mean_of(load)
        + mean_of(decide)
        + mean_of(append)
        + mean_of(cache_put);
    let total_mean = mean_of(total);
    let residue = total_mean.saturating_sub(attributed);
    println!(
        "    mean attributed = {} ms; mean total = {} ms; residue = {} ms",
        ms(attributed),
        ms(total_mean),
        ms(residue),
    );
}

fn backend_table(title: &str, hist: &Hist) {
    println!("--- {title}: backend call latency (ProfilingBackend) ---");
    for key in [
        "load_snapshot",
        "read_stream",
        "head",
        "append_batch",
        "read_global",
        "save_snapshot",
    ] {
        if let Some(s) = hist.stats(key) {
            stat_row(key, Some(s));
        }
    }
}

// ===========================================================================
// Workload runners.
// ===========================================================================

/// (a) all-new-streams, sequential: each command creates a brand-new stream.
async fn run_new_streams_seq(n: usize) -> (Spans, Hist, Duration) {
    let (backend, hist, _dir) = fresh_prof_backend("seedprof-new");
    let store = EventStore::new(backend).with_cache_capacity(CACHE_CAP);
    let mut spans = Spans::default();
    let wall = Instant::now();
    for i in 0..n {
        timed_command(&store, &stream_name(i), &mut spans).await;
    }
    let wall = wall.elapsed();
    (spans, hist, wall)
}

/// (b) all-existing-streams warm, sequential: every command hits ONE cached
/// stream (the hot-post-bench shape). First command primes the cache (not
/// measured into the warm spans — a separate priming pass).
async fn run_warm_one_stream(n: usize) -> (Spans, Hist, Duration) {
    let (backend, hist, _dir) = fresh_prof_backend("seedprof-warm");
    let store = EventStore::new(backend).with_cache_capacity(CACHE_CAP);
    let stream = stream_name(0);
    // Prime: the first miss pays a full cold load + new-stream append.
    let mut prime = Spans::default();
    timed_command(&store, &stream, &mut prime).await;
    hist.reset(); // discard the priming call's backend samples
    let mut spans = Spans::default();
    let wall = Instant::now();
    for _ in 0..n {
        timed_command(&store, &stream, &mut spans).await;
    }
    let wall = wall.elapsed();
    (spans, hist, wall)
}

/// (c) all-existing-streams, cold cache: fill `n` streams (setup, not
/// measured), then measure a first-touch command on each over a fresh cache —
/// a cache miss + snapshot lookup + 1-event tail load, appending a *second*
/// event to an already-interned stream (no new-stream registry write).
async fn run_cold_existing(n: usize) -> (Spans, Hist, Duration) {
    let (backend, hist, _dir) = fresh_prof_backend("seedprof-cold");
    // Fill: create each stream once (this pays the new-stream cost — setup).
    {
        let warm =
            EventStore::new(backend.clone()).with_cache_capacity(CACHE_CAP);
        for i in 0..n {
            warm.command_cached::<Rel, _>(&stream_name(i), Touch)
                .await
                .expect("fill");
        }
    }
    // Fresh cache over the same backend: every stream is now a cache miss but
    // an existing (interned) stream.
    hist.reset();
    let store = EventStore::new(backend).with_cache_capacity(CACHE_CAP);
    let mut spans = Spans::default();
    let wall = Instant::now();
    for i in 0..n {
        timed_command(&store, &stream_name(i), &mut spans).await;
    }
    let wall = wall.elapsed();
    (spans, hist, wall)
}

/// (d) all-new-streams over plain `LogEngine` (H3): same new-stream append with
/// NO `FjallSnapshotBackend` wrapper, via the base `command` path (the plain
/// engine is not a `SnapshotStore`, so `command_cached` is unavailable). Times
/// each command's wall latency and the backend calls.
async fn run_new_streams_plain(n: usize) -> (Vec<Duration>, Hist, Duration) {
    let dir = sweeping_temp_dir("seedprof-plain");
    let engine = LogEngine::open(dir.path().join("log")).expect("open engine");
    let hist = Hist::default();
    let store = EventStore::new(ProfilingBackend::new(engine, hist.clone()));
    let mut per_cmd = Vec::with_capacity(n);
    let wall = Instant::now();
    for i in 0..n {
        let t = Instant::now();
        store.command::<Rel, _>(&stream_name(i), Touch).await.expect("command");
        per_cmd.push(t.elapsed());
    }
    let wall = wall.elapsed();
    drop(store);
    drop(dir);
    (per_cmd, hist, wall)
}

/// (e) all-new-streams, pipelined k-at-a-time (H4): bounded-concurrency
/// `command_cached` over distinct new streams (the seeder's `drive` pattern).
/// Each task times its own in-pipeline command latency; the wall clock gives
/// throughput (wall/n). Returns `(in_pipeline_latencies, wall)`.
async fn run_new_streams_pipelined(
    n: usize,
    k: usize,
) -> (Vec<Duration>, Duration) {
    let (backend, _hist, _dir) = fresh_prof_backend("seedprof-pipe");
    let store = EventStore::new(backend).with_cache_capacity(CACHE_CAP);
    let lat: Arc<Mutex<Vec<Duration>>> =
        Arc::new(Mutex::new(Vec::with_capacity(n)));

    let spawn_one = |set: &mut JoinSet<()>, i: usize| {
        let s = store.clone();
        let lat = lat.clone();
        set.spawn(async move {
            let stream = stream_name(i);
            let t = Instant::now();
            s.command_cached::<Rel, _>(&stream, Touch).await.expect("pipe cmd");
            lat.lock().expect("lat lock").push(t.elapsed());
        });
    };

    let wall = Instant::now();
    let mut set: JoinSet<()> = JoinSet::new();
    let mut next = 0usize;
    for _ in 0..k.min(n) {
        spawn_one(&mut set, next);
        next += 1;
    }
    while let Some(joined) = set.join_next().await {
        joined.expect("pipe task panicked");
        if next < n {
            spawn_one(&mut set, next);
            next += 1;
        }
    }
    let wall = wall.elapsed();
    let lat = Arc::try_unwrap(lat).expect("sole owner").into_inner().unwrap();
    (lat, wall)
}

/// (e-baseline) all-new-streams, sequential command_cached — the wall/n
/// baseline the pipelined run is compared against.
async fn run_new_streams_pipe_seq(n: usize) -> (Vec<Duration>, Duration) {
    let (backend, _hist, _dir) = fresh_prof_backend("seedprof-pipeseq");
    let store = EventStore::new(backend).with_cache_capacity(CACHE_CAP);
    let mut lat = Vec::with_capacity(n);
    let wall = Instant::now();
    for i in 0..n {
        let t = Instant::now();
        store
            .command_cached::<Rel, _>(&stream_name(i), Touch)
            .await
            .expect("seq cmd");
        lat.push(t.elapsed());
    }
    let wall = wall.elapsed();
    (lat, wall)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn throughput_line(label: &str, n: usize, wall: Duration) {
    println!(
        "  {label:<34} n={n:<6} wall={:>9.3} ms  wall/n={} ms/cmd",
        wall.as_secs_f64() * 1e3,
        ms(wall / n as u32),
    );
}

// ===========================================================================
// The full matrix (#[ignore]d).
// ===========================================================================

#[ignore = "heavy seed-throughput profile; run explicitly with --release \
            --ignored --nocapture"]
#[tokio::test(flavor = "multi_thread")]
async fn seed_profile() {
    let n_new = env_usize("SEED_PROFILE_NEW", 4_000);
    let n_warm = env_usize("SEED_PROFILE_WARM", 20_000);
    let n_cold = env_usize("SEED_PROFILE_COLD", 4_000);
    let n_plain = env_usize("SEED_PROFILE_PLAIN", 4_000);
    let n_pipe = env_usize("SEED_PROFILE_PIPE", 8_000);
    let n_pipe_seq = env_usize("SEED_PROFILE_PIPE_SEQ", 2_000);
    let k = env_usize("SEED_PROFILE_K", 32);

    println!(
        "\n================ seed-throughput profile (bn-1jg) ================"
    );
    println!(
        "store: EventStore<FjallSnapshotBackend<LogEngine>>, \
         Durability::Process, real fs"
    );
    println!("aggregate: Rel (one event/command), cache cap={CACHE_CAP}");

    // (a) all-new-streams (the seeder shape).
    let (new_spans, new_hist, new_wall) = run_new_streams_seq(n_new).await;
    println!("\n### (a) all-new-streams, sequential [seeder shape]");
    throughput_line("all-new-streams seq", n_new, new_wall);
    phase_table("(a) all-new-streams", &new_spans);
    backend_table("(a) all-new-streams", &new_hist);

    // (b) warm one stream (the bench shape / control).
    let (warm_spans, warm_hist, warm_wall) = run_warm_one_stream(n_warm).await;
    println!("\n### (b) all-existing warm, one stream [bench control]");
    throughput_line("warm one-stream", n_warm, warm_wall);
    phase_table("(b) warm one-stream", &warm_spans);
    backend_table("(b) warm one-stream", &warm_hist);

    // (c) cold cache, existing streams.
    let (cold_spans, cold_hist, cold_wall) = run_cold_existing(n_cold).await;
    println!("\n### (c) all-existing, cold cache, first-touch");
    throughput_line("cold existing first-touch", n_cold, cold_wall);
    phase_table("(c) cold existing", &cold_spans);
    backend_table("(c) cold existing", &cold_hist);

    // (d) plain LogEngine, new streams (H3).
    let (plain_lat, plain_hist, plain_wall) =
        run_new_streams_plain(n_plain).await;
    println!(
        "\n### (d) all-new-streams over plain LogEngine [H3 wrapper isolation]"
    );
    throughput_line("plain-engine new-streams", n_plain, plain_wall);
    stat_row("per-command (command)", Stats::of(plain_lat));
    backend_table("(d) plain-engine", &plain_hist);

    // (e) sequential vs pipelined (H4).
    let (pseq_lat, pseq_wall) = run_new_streams_pipe_seq(n_pipe_seq).await;
    let (ppipe_lat, ppipe_wall) = run_new_streams_pipelined(n_pipe, k).await;
    println!("\n### (e) new-streams sequential vs pipelined (k={k}) [H4]");
    throughput_line("new-streams seq (command_cached)", n_pipe_seq, pseq_wall);
    stat_row("  seq per-command", Stats::of(pseq_lat));
    throughput_line("new-streams pipelined", n_pipe, ppipe_wall);
    stat_row("  in-pipeline per-command", Stats::of(ppipe_lat));

    println!(
        "\n================================================================="
    );
    println!(
        "Read: compare (a).append_batch vs (b)/(c).append_batch — the delta \
         is the per-new-stream registry-name durability flush. Compare \
         (c).load vs (b) (cache hit) for the cold-load cost. (e) shows \
         pipelining amortizes the same per-command latency into a lower \
         wall/n."
    );
    println!();
}

/// Non-ignored smoke: tiny sizes, runs in the normal suite, asserts only the
/// *shape* (a new-stream append is materially costlier than a warm append) —
/// never an absolute timing. Also gives clippy/coverage of the whole harness.
#[tokio::test(flavor = "multi_thread")]
async fn seed_profile_smoke() {
    let (new_spans, new_hist, _) = run_new_streams_seq(64).await;
    let (warm_spans, _warm_hist, _) = run_warm_one_stream(256).await;

    let new_append = Stats::of(new_spans.append.clone()).expect("new append");
    let warm_append =
        Stats::of(warm_spans.append.clone()).expect("warm append");

    // A brand-new-stream append persists a new registry name; a warm append to
    // an existing stream does not. The former must be the costlier phase. This
    // holds by a wide margin (a durability flush vs an in-memory append), so it
    // is a safe shape assertion, not an absolute-latency claim.
    assert!(
        new_append.p50 > warm_append.p50,
        "new-stream append p50 ({:?}) should exceed warm append p50 ({:?})",
        new_append.p50,
        warm_append.p50,
    );

    // The ProfilingBackend must have observed the new-stream appends.
    assert!(
        new_hist.stats("append_batch").is_some(),
        "profiling backend recorded no append_batch calls"
    );
    // And the cold-miss path must consult the snapshot store at least once.
    assert!(
        new_hist.stats("load_snapshot").is_some(),
        "cold-miss path should have called load_snapshot"
    );
}

// Silence dead-code analysis for the Aggregate-derived plumbing the harness
// exercises only through the store (kept explicit for clarity).
#[allow(dead_code)]
fn _assert_traits() {
    fn is_send<T: Send>() {}
    is_send::<ProfilingBackend<SnapBackend>>();
}
