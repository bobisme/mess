//! # The retired anti-pattern, as a *measured* artifact
//!
//! Before `bn-jes`, a post's likers were folded into the post aggregate itself:
//! a `likes: HashSet<Id>` in the fold state, `Liked`/`Unliked` events on the
//! post's own stream. That is **unbounded** aggregate state — a viral post with
//! 100k like/unlike events replays the whole crowd on *every* command. The
//! bounded remodel moved each like onto its own tiny `like-<post>_<user>`
//! stream, so a real `Post` command is O(1) forever.
//!
//! This file resurrects the unbounded shape as a **private, benchmark-only**
//! [`MegaPost`] aggregate (never exported from the crate) and measures, on ONE
//! post stream seeded with ~100k like/unlike events, exactly what the remodel
//! bought — all through the **same** `EventStore` APIs the app uses (no private
//! engine access):
//!
//! - **(a)** plain [`EventStore::command`] per like at depth — a full replay
//!   per command (sampled, per-command latency reported, not a fabricated
//!   throughput);
//! - **(b)** the warm [`EventStore::command_cached`] path at the same depth —
//!   steady-state per-command latency (zero event reads on a hit);
//! - **(c)** snapshot-assisted cold start — [`EventStore::load_cached`]
//!   (snapshot + tail) vs a full replay from a fresh store;
//! - **(d)** snapshot size in bytes vs replayed-event count — the
//!   churn-compaction story (the folded set holds tens of thousands of ids
//!   while history holds 100k events).
//!
//! ## Running it
//!
//! The full 100k run is `#[ignore]`d so it never runs in normal `cargo test`:
//!
//! ```text
//! TMPDIR=$HOME/.cache/mess-test-tmp \
//!   cargo test -p social --test hot_post_bench -- --ignored --nocapture
//! ```
//!
//! Override the shape with env vars: `HOT_POST_BENCH_EVENTS` (default 100000),
//! `HOT_POST_BENCH_UNLIKE_FRAC` (default 0.30), `HOT_POST_BENCH_SEED`. A short
//! 5k-event variant ([`hot_post_bench_short`]) runs in normal `cargo test` as a
//! smoke check and prints its own (smaller) table.
//!
//! No measured numbers are asserted as absolutes or committed to docs — timings
//! are machine-dependent; `bn-o9z` captures a canonical run into the README.
//! The short variant only asserts the *shape* of the result (warm ≪ cold,
//! snapshot ≪ full replay), which holds by orders of magnitude.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use ident::Id;
use mess_core::Decide;
use mess_derive::{Aggregate, Event};
use mess_store::{
    EventStore, FjallSnapshotBackend, LogEngine, Snapshottable,
    StateCodecError, Version,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

// ===========================================================================
// MegaPost — the pre-bn-jes unbounded shape, PRIVATE to this benchmark.
// ===========================================================================

/// The unbounded like events: each carries the liker id, because the crowd is
/// aggregate state here (contrast the bounded `LikeEvent`, which is
/// payload-free because the ids live in its stream key).
#[derive(Debug, Clone, PartialEq, Eq, Event)]
#[event(name = "megapost", version = 1)]
enum MegaPostEvent {
    Liked { user: Id },
    Unliked { user: Id },
}

/// The retired shape: the whole liker crowd folded into one aggregate's state.
#[derive(Debug, Default, Clone, PartialEq, Eq, Aggregate)]
#[aggregate(event = MegaPostEvent)]
struct MegaPost {
    likes: HashSet<Id>,
}

impl MegaPost {
    fn apply(&mut self, event: &MegaPostEvent) {
        match event {
            MegaPostEvent::Liked { user } => {
                self.likes.insert(*user);
            }
            MegaPostEvent::Unliked { user } => {
                self.likes.remove(user);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MegaPostError {
    AlreadyLiked,
    NotLiked,
}

impl std::fmt::Display for MegaPostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MegaPostError::AlreadyLiked => write!(f, "already liked"),
            MegaPostError::NotLiked => write!(f, "not liked"),
        }
    }
}
impl std::error::Error for MegaPostError {}

#[derive(Debug, Clone, Copy)]
struct MegaLike {
    user: Id,
}
#[derive(Debug, Clone, Copy)]
struct MegaUnlike {
    user: Id,
}

impl Decide<MegaLike> for MegaPost {
    type Rejection = MegaPostError;

    fn decide(
        &self,
        cmd: MegaLike,
    ) -> Result<Vec<MegaPostEvent>, MegaPostError> {
        if self.likes.contains(&cmd.user) {
            return Err(MegaPostError::AlreadyLiked);
        }
        Ok(vec![MegaPostEvent::Liked { user: cmd.user }])
    }
}

impl Decide<MegaUnlike> for MegaPost {
    type Rejection = MegaPostError;

    fn decide(
        &self,
        cmd: MegaUnlike,
    ) -> Result<Vec<MegaPostEvent>, MegaPostError> {
        if !self.likes.contains(&cmd.user) {
            return Err(MegaPostError::NotLiked);
        }
        Ok(vec![MegaPostEvent::Unliked { user: cmd.user }])
    }
}

/// Snapshot of the unbounded state: a `u64` count then each id as a
/// length-prefixed `Display` string. Deliberately hand-rolled and O(set) — the
/// *point* of measurement (d) is that this blob grows with the crowd, unlike a
/// bounded aggregate's fixed-size snapshot.
impl Snapshottable for MegaPost {
    const FOLD_VERSION: u32 = 1;

    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        let mut out = Vec::with_capacity(8 + self.likes.len() * 24);
        out.extend_from_slice(&(self.likes.len() as u64).to_le_bytes());
        for id in &self.likes {
            let s = id.to_string();
            out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        Ok(out)
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        let err = |m: &str| StateCodecError(m.to_string());
        let mut pos = 0usize;
        let mut take = |n: usize| -> Result<&[u8], StateCodecError> {
            let end =
                pos.checked_add(n).ok_or_else(|| err("length overflow"))?;
            let s = bytes.get(pos..end).ok_or_else(|| err("blob too short"))?;
            pos = end;
            Ok(s)
        };
        let count =
            u64::from_le_bytes(take(8)?.try_into().map_err(|_| err("count"))?);
        let mut likes = HashSet::with_capacity(count as usize);
        for _ in 0..count {
            let len = u32::from_le_bytes(
                take(4)?.try_into().map_err(|_| err("len"))?,
            ) as usize;
            let raw = take(len)?;
            let id: Id = std::str::from_utf8(raw)
                .map_err(|_| err("utf8"))?
                .parse()
                .map_err(|_| err("id"))?;
            likes.insert(id);
        }
        Ok(MegaPost { likes })
    }
}

// ===========================================================================
// The measured run.
// ===========================================================================

/// A snapshot-capable real-fs store, plus the temp dir it lives in (dropped
/// last so the store closes before its files are swept).
struct BenchStore {
    backend: FjallSnapshotBackend<LogEngine>,
    _dir:    mess_testkit::SweepingTempDir,
}

impl BenchStore {
    fn fresh() -> Self {
        // Real fs under the TMPDIR rule, via the shared self-sweeping helper.
        let dir = mess_testkit::sweeping_temp_dir("hot-post-bench");
        let engine =
            LogEngine::open(dir.path().join("log")).expect("open engine");
        let backend =
            FjallSnapshotBackend::open(engine, dir.path().join("snap"))
                .expect("open snapshot backend");
        BenchStore { backend, _dir: dir }
    }

    /// A cache-off store over the shared backend (the "cold" path).
    fn cold(&self) -> EventStore<FjallSnapshotBackend<LogEngine>> {
        EventStore::new(self.backend.clone())
    }

    /// A cache-on store over the shared backend (the "warm" path).
    fn warm(
        &self,
        capacity: usize,
    ) -> EventStore<FjallSnapshotBackend<LogEngine>> {
        EventStore::new(self.backend.clone()).with_cache_capacity(capacity)
    }
}

struct Params {
    events:           usize,
    unlike_frac:      f64,
    seed:             u64,
    /// Samples for the (expensive) plain-command-per-like measurement.
    cold_cmd_samples: usize,
    /// Samples for the warm steady-state measurement.
    warm_cmd_samples: usize,
}

struct Report {
    seed_depth:        u64,
    active_ids:        usize,
    // (a)
    cold_cmd:          Stats,
    // (b)
    warm_cmd:          Stats,
    depth_after_warm:  u64,
    // (c)
    full_replay_load:  Duration,
    snapshot_load:     Duration,
    first_warm_miss:   Duration,
    snapshot_tail_len: usize,
    // (d)
    snapshot_bytes:    usize,
    event_count:       u64,
}

/// Min / median / mean over a sample of per-command durations.
struct Stats {
    n:      usize,
    min:    Duration,
    median: Duration,
    mean:   Duration,
}

impl Stats {
    fn of(mut samples: Vec<Duration>) -> Self {
        assert!(!samples.is_empty());
        samples.sort_unstable();
        let n = samples.len();
        let sum: Duration = samples.iter().sum();
        Stats {
            n,
            min: samples[0],
            median: samples[n / 2],
            mean: sum / n as u32,
        }
    }
}

/// Deterministic-churn seeding: drive a seeded RNG to choose, at each step, an
/// unlike of a currently-active liker (probability `unlike_frac`, when any are
/// active) or a like of a **fresh** id. Returns the event history and the set
/// of ids still active at the end — the exact set a fold of the history yields.
///
/// The RNG makes the *churn pattern* reproducible run to run; the fresh liker
/// ids come from `Id::new()` (their concrete values do not affect any latency
/// measured here, only the blob's byte length, which is reported honestly).
fn generate(events: usize, unlike_frac: f64, seed: u64) -> Vec<MegaPostEvent> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut active: Vec<Id> = Vec::new();
    let mut out: Vec<MegaPostEvent> = Vec::with_capacity(events);
    for _ in 0..events {
        let unlike = !active.is_empty() && rng.random::<f64>() < unlike_frac;
        if unlike {
            let idx = rng.random_range(0..active.len());
            let user = active.swap_remove(idx);
            out.push(MegaPostEvent::Unliked { user });
        } else {
            let user = Id::new();
            active.push(user);
            out.push(MegaPostEvent::Liked { user });
        }
    }
    out
}

async fn run_bench(p: &Params) -> Report {
    let store = BenchStore::fresh();
    let stream = "megapost-hot";

    // --- seed the deep stream via raw appends (not per-command) -------------
    let history = generate(p.events, p.unlike_frac, p.seed);
    let seeder = store.cold();
    let mut version = Version::NoStream;
    for chunk in history.chunks(5_000) {
        let commit = seeder
            .append::<MegaPostEvent>(stream, version, chunk)
            .await
            .expect("seed append");
        version = commit.version;
    }
    let seed_depth = version.position().map_or(0, |v| v + 1);
    let active_ids =
        seeder.load::<MegaPost>(stream).await.unwrap().state.likes.len();

    // --- (a) plain command per like at depth --------------------------------
    // Each command full-replays the whole stream. Sample a handful.
    let cold = store.cold();
    let mut cold_samples = Vec::with_capacity(p.cold_cmd_samples);
    for _ in 0..p.cold_cmd_samples {
        let user = Id::new();
        let t = Instant::now();
        cold.command::<MegaPost, _>(stream, MegaLike { user })
            .await
            .expect("cold like");
        cold_samples.push(t.elapsed());
    }
    let cold_cmd = Stats::of(cold_samples);

    // --- (b) warm command_cached steady-state -------------------------------
    let warm = store.warm(8);
    // Prime the cache (this first miss pays a full load; not measured here).
    warm.command_cached::<MegaPost, _>(stream, MegaLike { user: Id::new() })
        .await
        .expect("warm prime");
    let mut warm_samples = Vec::with_capacity(p.warm_cmd_samples);
    for _ in 0..p.warm_cmd_samples {
        let user = Id::new();
        let t = Instant::now();
        warm.command_cached::<MegaPost, _>(stream, MegaLike { user })
            .await
            .expect("warm like");
        warm_samples.push(t.elapsed());
    }
    let warm_cmd = Stats::of(warm_samples);
    let depth_after_warm = warm
        .load::<MegaPost>(stream)
        .await
        .unwrap()
        .version
        .position()
        .unwrap()
        + 1;

    // --- (c) snapshot-assisted cold start -----------------------------------
    // Write a snapshot at the current head.
    store
        .cold()
        .save_snapshot::<MegaPost>(stream)
        .await
        .expect("save snapshot");

    // Full replay from a fresh cache-off store.
    let cold1 = store.cold();
    let t = Instant::now();
    let full = cold1.load::<MegaPost>(stream).await.expect("full replay");
    let full_replay_load = t.elapsed();

    // Snapshot + tail from a fresh cache-off store.
    let cold2 = store.cold();
    let t = Instant::now();
    let via_snap =
        cold2.load_cached::<MegaPost>(stream).await.expect("snapshot load");
    let snapshot_load = t.elapsed();
    let snapshot_tail_len = via_snap.events_replayed;
    assert_eq!(full.state, via_snap.state, "snapshot load must equal replay");

    // First command on a fresh warm store: a miss that goes through the
    // snapshot-accelerated load, then decides + appends.
    let cold3 = store.warm(8);
    let t = Instant::now();
    cold3
        .command_cached::<MegaPost, _>(stream, MegaLike { user: Id::new() })
        .await
        .expect("first warm-miss command");
    let first_warm_miss = t.elapsed();

    // --- (d) snapshot size vs event count -----------------------------------
    let snapshot_bytes = full.state.encode_state().unwrap().len();
    let event_count = cold1
        .load::<MegaPost>(stream)
        .await
        .unwrap()
        .version
        .position()
        .unwrap()
        + 1;

    Report {
        seed_depth,
        active_ids,
        cold_cmd,
        warm_cmd,
        depth_after_warm,
        full_replay_load,
        snapshot_load,
        first_warm_miss,
        snapshot_tail_len,
        snapshot_bytes,
        event_count,
    }
}

fn micros(d: Duration) -> String {
    format!("{:>12.3} ms", d.as_secs_f64() * 1e3)
}

fn print_report(p: &Params, r: &Report) {
    let speedup = r.cold_cmd.median.as_secs_f64()
        / r.warm_cmd.median.as_secs_f64().max(f64::MIN_POSITIVE);
    let snap_speedup = r.full_replay_load.as_secs_f64()
        / r.snapshot_load.as_secs_f64().max(f64::MIN_POSITIVE);
    let bytes_per_event = r.snapshot_bytes as f64 / r.event_count as f64;

    println!();
    println!("================ hot-post benchmark ================");
    println!(
        "params: events={} unlike_frac={:.2} seed={} page_size=1000 (default)",
        p.events, p.unlike_frac, p.seed
    );
    println!(
        "seeded: depth={} events, active likers in fold={} ids",
        r.seed_depth, r.active_ids
    );
    println!(
        "store:  EventStore over FjallSnapshotBackend<LogEngine>, real fs"
    );
    println!("---------------------------------------------------");
    println!("(a) plain command per like @depth (full replay each)");
    println!(
        "      n={:<3} min={} median={} mean={}",
        r.cold_cmd.n,
        micros(r.cold_cmd.min),
        micros(r.cold_cmd.median),
        micros(r.cold_cmd.mean),
    );
    println!("(b) command_cached warm steady-state (0 event reads/hit)");
    println!(
        "      n={:<3} min={} median={} mean={}   (depth≈{})",
        r.warm_cmd.n,
        micros(r.warm_cmd.min),
        micros(r.warm_cmd.median),
        micros(r.warm_cmd.mean),
        r.depth_after_warm,
    );
    println!("      => warm median speedup vs cold command: {speedup:>10.1}x");
    println!("(c) cold start @depth={}", r.event_count);
    println!("      full replay  load        {}", micros(r.full_replay_load));
    println!(
        "      snapshot+tail load_cached {}   (tail={} events)",
        micros(r.snapshot_load),
        r.snapshot_tail_len
    );
    println!("      first warm-miss command   {}", micros(r.first_warm_miss));
    println!(
        "      => snapshot load speedup vs full replay: {snap_speedup:>7.1}x"
    );
    println!("(d) churn compaction");
    println!(
        "      snapshot blob = {} bytes for {} events ({:.2} B/event)",
        r.snapshot_bytes, r.event_count, bytes_per_event
    );
    println!("===================================================");
    println!(
        "Interpretation: at this depth a plain `command` pays a full replay \
         of every event on the stream, so its per-command latency scales with \
         history and lands in the millisecond range (a). The warm \
         `command_cached` path proves the stream's version by the append \
         itself and folds the one event it wrote into the cached state, \
         reading zero events — so it is flat regardless of depth and \
         ~{speedup:.0}x faster here (b). When the cache is cold (a fresh \
         process), a snapshot turns the same load from a full {ec}-event \
         replay into a single blob decode plus a {tail}-event tail, \
         ~{snap:.0}x faster (c). That blob is {sb} bytes summarizing {ec} \
         events ({bpe:.1} B/event) — the churn story: the fold holds only the \
         {act} still-active ids, so unlikes compact away and the snapshot \
         never carries the retracted likes the log still records. This is \
         exactly the cost the bn-jes bounded remodel removes from the *real* \
         Post aggregate, whose commands are O(1) and whose snapshot is a \
         handful of bytes at any like count.",
        speedup = speedup,
        ec = r.event_count,
        tail = r.snapshot_tail_len,
        snap = snap_speedup,
        sb = r.snapshot_bytes,
        bpe = bytes_per_event,
        act = r.active_ids,
    );
    println!();
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}
fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// The full ~100k-event benchmark. `#[ignore]`d: run on demand with
/// `--ignored --nocapture` (see the module docs). Overridable via env vars.
#[ignore = "heavy hot-post benchmark; run explicitly with --ignored --nocapture"]
#[tokio::test]
async fn hot_post_bench_full() {
    let p = Params {
        events:           env_usize("HOT_POST_BENCH_EVENTS", 100_000),
        unlike_frac:      env_f64("HOT_POST_BENCH_UNLIKE_FRAC", 0.30),
        seed:             env_u64("HOT_POST_BENCH_SEED", 0x0B00_B1E5),
        cold_cmd_samples: env_usize("HOT_POST_BENCH_COLD_SAMPLES", 5),
        warm_cmd_samples: env_usize("HOT_POST_BENCH_WARM_SAMPLES", 300),
    };
    let report = run_bench(&p).await;
    print_report(&p, &report);
}

/// The short (5k-event) smoke variant — runs in normal `cargo test`, bounded to
/// a couple of seconds. Prints its own table and asserts only the *shape* of
/// the result (which holds by orders of magnitude), never absolute timings.
#[tokio::test]
async fn hot_post_bench_short() {
    let p = Params {
        events:           5_000,
        unlike_frac:      0.30,
        seed:             0x0B00_B1E5,
        cold_cmd_samples: 3,
        warm_cmd_samples: 100,
    };
    let report = run_bench(&p).await;
    print_report(&p, &report);

    // Shape assertions (robust at this depth; not absolute-latency claims):
    assert!(
        report.warm_cmd.median < report.cold_cmd.median,
        "warm command_cached must beat a full-replay command"
    );
    assert!(
        report.snapshot_load < report.full_replay_load,
        "snapshot+tail load must beat a full replay"
    );
    assert_eq!(
        report.snapshot_tail_len, 0,
        "a head snapshot leaves an empty tail"
    );
    assert!(report.snapshot_bytes > 0 && report.event_count >= 5_000);
    assert!(
        report.active_ids > 0
            && (report.active_ids as u64) < report.event_count
    );
}
