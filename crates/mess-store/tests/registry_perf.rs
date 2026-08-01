//! bn-2di perf gate: **new-stream append latency, before vs after**.
//!
//! The bone's hard requirement is that moving the registry into the log must
//! not make a new-stream append slower — and the expectation is that it makes
//! it much faster, because the co-durable fjall name barrier (bn-150 / bn-2cj /
//! bn-34o, plus the second serialized `SyncAll` worth ~953 µs/new-stream that
//! Spike J found and the `commit.fsync` counter could not see) disappears
//! entirely: the registration becomes a log batch in the same commit group as
//! the append that first uses the name.
//!
//! This harness deliberately uses **only public API** (`LogEngine::open_with`,
//! `append_batch`), so the identical file compiles and runs against the
//! pre-bn-2di tree — which is how the "before" column is produced. It is
//! `#[ignore]`d: it is a measurement, not an assertion, and it is only
//! meaningful on a quiet machine.
//!
//! Run: `cargo test -p mess-store --test registry_perf -- --ignored
//! --nocapture`

use std::time::{Duration, Instant};

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{Durability, EngineOptions, LogEngine, Version};

fn rec(message_type: &str, data: &[u8]) -> RecordToAppend {
    RecordToAppend {
        message_type: message_type.to_string(),
        data:         data.to_vec(),
    }
}

/// 1-minute load average — recorded rather than asserted, so a reader can judge
/// whether the numbers were taken under a quiet machine (this host idles around
/// 4-5, so a fixed floor would be a lie).
fn load1() -> f64 {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| s.split_whitespace().next()?.parse().ok())
        .unwrap_or(f64::NAN)
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let i = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[i]
}

/// One measurement: `n` appends, each to a BRAND-NEW stream with a brand-new
/// event type — i.e. every single one mints two registry records. This is the
/// worst case for this bone and the exact shape the old barrier taxed.
async fn new_stream_appends(
    durability: Durability,
    n: usize,
    tag: &str,
) -> Vec<Duration> {
    let dir = mess_testkit::sweeping_temp_dir("bn2di-perf");
    let engine = LogEngine::open_with(
        dir.path().join("store"),
        EngineOptions { durability, ..EngineOptions::default() },
    )
    .expect("open");

    // Warm: the first append pays one-time costs (segment prealloc, committer
    // spin-up) that are not what we are measuring.
    engine
        .append_batch("warm", Version::NoStream, &[rec("warm.t", b"w")])
        .await
        .expect("warm");

    let mut samples = Vec::with_capacity(n);
    for i in 0..n {
        let t = Instant::now();
        engine
            .append_batch(
                &format!("{tag}-stream-{i}"),
                Version::NoStream,
                &[rec(&format!("{tag}.type-{i}"), &(i as u64).to_le_bytes())],
            )
            .await
            .expect("new-stream append");
        samples.push(t.elapsed());
    }
    samples.sort_unstable();
    samples
}

/// The existing-stream floor, for context: an append to an ALREADY-registered
/// stream and type mints nothing, so it should be identical before and after.
/// If this moved, the bone did something it should not have.
async fn hot_appends(durability: Durability, n: usize) -> Vec<Duration> {
    let dir = mess_testkit::sweeping_temp_dir("bn2di-perf-hot");
    let engine = LogEngine::open_with(
        dir.path().join("store"),
        EngineOptions { durability, ..EngineOptions::default() },
    )
    .expect("open");
    engine
        .append_batch("hot", Version::NoStream, &[rec("hot.t", b"0")])
        .await
        .expect("prime");

    let mut samples = Vec::with_capacity(n);
    let mut expected = Version::At(0);
    for i in 0..n {
        let t = Instant::now();
        let out = engine
            .append_batch(
                "hot",
                expected,
                &[rec("hot.t", &(i as u64).to_le_bytes())],
            )
            .await
            .expect("hot append");
        samples.push(t.elapsed());
        expected = out.version;
    }
    samples.sort_unstable();
    samples
}

fn report(label: &str, s: &[Duration]) {
    eprintln!(
        "{label:<28} p50={:>10.1?}  p90={:>10.1?}  p99={:>10.1?}  \
         mean={:>10.1?}",
        pct(s, 0.50),
        pct(s, 0.90),
        pct(s, 0.99),
        s.iter().sum::<Duration>() / s.len() as u32,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, not an assertion: run explicitly on a quiet machine"]
async fn bn2di_new_stream_append_latency() {
    eprintln!("=== bn-2di new-stream append latency ===");
    eprintln!("load1 at start: {:.2}", load1());

    const N: usize = 300;

    let s = new_stream_appends(Durability::Process, N, "proc").await;
    report("NEW-STREAM  Process", &s);

    let s = hot_appends(Durability::Process, N).await;
    report("  hot floor Process", &s);

    let group = Durability::Group {
        max_delay: Duration::from_micros(200),
        max_bytes: 4 * 1024 * 1024,
    };
    let s = new_stream_appends(group, N, "grp").await;
    report("NEW-STREAM  Group", &s);

    let s = hot_appends(group, N).await;
    report("  hot floor Group", &s);

    let s = new_stream_appends(Durability::Os, N, "os").await;
    report("NEW-STREAM  Os", &s);

    eprintln!("load1 at end:   {:.2}", load1());
}
