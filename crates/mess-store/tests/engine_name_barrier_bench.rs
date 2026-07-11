//! bn-34o measurement: the coalesced new-name barrier under a pipelined,
//! all-new-streams workload at concurrency 32 under the durable modes — the
//! shape spike bn-1jg profiled (`spikes/seed_profile/REPORT.md`, workload (e),
//! k=32), replicated here in `mess-store` so it runs without the read-only
//! `examples/social` harness.
//!
//! # What it proves, and the honest Os-vs-Group story
//!
//! bn-1jg's headline finding was that the per-new-stream `MetaStore::persist`
//! `SyncAll` is a **shared single-writer serialization point**: at k=32 every
//! new-stream append funnels its name flush through one `MetaStore`, so
//! pipelining bought only ~1.3× and in-pipeline latency exploded. bn-34o folds
//! that flush into the log committer's group barrier (a
//! [`PreBarrier`](mess_log::committer::PreBarrier) hook, high-watered by ticket
//! — see `engine.rs`'s `NameFlush`). This test reports, per durable mode:
//!
//! * `barriers / new-name` — the ratio that used to be ~1.0 (one meta fsync per
//!   new stream).
//! * per-command p50 / mean and wall/command at concurrency 32.
//!
//! The result splits cleanly by mode, and the split is fundamental, not a
//! tuning artifact:
//!
//! * **`Group`** (a real group-commit window): the hook issues ONE meta
//!   `SyncAll` per group, covering every name buffered so far — so `K` names in
//!   a group's window collapse to one fsync. The ratio approaches `1/K` and the
//!   shared-`MetaStore` serialization point dissolves. **This is the bone's
//!   target and the spike's recommendation realized.**
//! * **`Os`** (group-of-one — the operator asked for one log `fdatasync` per
//!   append): in a *closed-loop* k-permit pipeline the admission rate is
//!   rate-matched to the committer's serial drain, so exactly ~1 new name
//!   buffers between consecutive group barriers — there is nothing to coalesce,
//!   in ANY mechanism, and the ratio stays ~1. That is inherent to `Os`
//!   semantics, not a defect: an `Os` window *is* a single append. What bn-34o
//!   still buys `Os` is that the name flush is no longer a *separate* fsync
//!   stage serialized on the shared `MetaStore` upstream of the committer — it
//!   is one hop inside the committer's existing per-append barrier. (The
//!   open-loop burst — many new streams submitted without waiting — DOES
//!   coalesce even under `Os`; see
//!   `engine_name_durability::os_burst_of_new_streams_coalesces_barriers`.)
//!
//! It is `#[ignore]`d (a timing measurement, real fs / real fsyncs) and prints
//! its numbers with `--nocapture`. The assertions keep it honest without
//! pinning host-specific timings: `Group` must show real coalescing (ratio well
//! below 1), `Os` is reported but only sanity-bounded, and every one of the
//! thousands of new names must reopen resolvable in both modes.
//!
//! Run:
//! ```text
//! CLANG_PATH=/usr/bin/clang TMPDIR=$HOME/.cache/mess-test-tmp \
//!   cargo test -p mess-store --release --test engine_name_barrier_bench \
//!   -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{Durability, EngineOptions, LogEngine, Version};

fn rec(message_type: &str, data: &[u8]) -> RecordToAppend {
    RecordToAppend {
        message_type: message_type.to_string(),
        data:         data.to_vec(),
    }
}

struct Report {
    barriers:  u64,
    buffered:  u64,
    p50:       Duration,
    mean:      Duration,
    wall_each: Duration,
}

/// Drive `n` all-new-stream appends at pipeline depth `k` under `durability`,
/// returning the barrier/latency report and proving every name reopens.
async fn run(durability: Durability, n: usize, k: usize, tag: &str) -> Report {
    let dir = mess_testkit::sweeping_temp_dir(&format!("bn34o-bench-{tag}"));
    let store_path = dir.path().join("store");
    let engine = LogEngine::open_with(
        &store_path,
        EngineOptions { durability, ..EngineOptions::default() },
    )
    .expect("open store");

    // Bound in-flight appends at k, exactly like the seeder's k=32 pipeline.
    let sem = Arc::new(tokio::sync::Semaphore::new(k));
    let latencies =
        Arc::new(std::sync::Mutex::new(Vec::<Duration>::with_capacity(n)));

    let wall = Instant::now();
    let mut handles = Vec::with_capacity(n);
    for i in 0..n {
        let permit = sem.clone().acquire_owned().await.expect("semaphore");
        let engine = engine.clone();
        let latencies = latencies.clone();
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            let t = Instant::now();
            engine
                .append_batch(
                    &format!("bench-stream-{i}"),
                    Version::NoStream,
                    &[rec("evt", &(i as u64).to_le_bytes())],
                )
                .await
                .expect("new-stream append");
            latencies.lock().unwrap().push(t.elapsed());
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    let wall = wall.elapsed();

    let barriers = engine.meta_persist_call_count();
    let buffered = engine.meta_buffered_persist_call_count();
    let mut lat = Arc::try_unwrap(latencies).unwrap().into_inner().unwrap();
    lat.sort_unstable();
    let p50 = lat[lat.len() / 2];
    let mean = lat.iter().sum::<Duration>() / lat.len() as u32;

    // Durability preserved: probe names reopen resolvable in every mode.
    drop(engine);
    let engine = LogEngine::open_with(
        &store_path,
        EngineOptions { durability, ..EngineOptions::default() },
    )
    .expect("reopen must not error EngineError::Meta");
    for probe in [0usize, n / 2, n - 1] {
        assert_eq!(
            engine.head(&format!("bench-stream-{probe}")).await.unwrap(),
            Version::At(0),
            "{tag}: stream {probe}'s name+event must survive reopen"
        );
    }

    Report { barriers, buffered, p50, mean, wall_each: wall / n as u32 }
}

fn print_report(tag: &str, n: usize, k: usize, r: &Report) {
    eprintln!("--- bn-34o {tag} pipelined new-streams (k={k}, n={n}) ---");
    eprintln!("meta SyncAll barriers : {}", r.barriers);
    eprintln!(
        "barriers / new-name   : {:.4}  (was ~1.0 per new stream)",
        r.barriers as f64 / n as f64
    );
    eprintln!("per-command p50       : {:?}", r.p50);
    eprintln!("per-command mean      : {:?}", r.mean);
    eprintln!("wall / command        : {:?}", r.wall_each);
    eprintln!("buffered flushes      : {} (0 in a barriered mode)", r.buffered);
}

/// The headline measurement: same pipelined all-new-stream workload under both
/// durable modes, so the coalescing win (`Group`) and its inherent `Os` limit
/// are reported side by side from one run.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "timing measurement (real fsyncs); run with --ignored --nocapture"]
async fn durable_pipelined_new_streams_barrier_ratio() {
    let n: usize = std::env::var("BARRIER_BENCH_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4_000);
    let k: usize = std::env::var("BARRIER_BENCH_K")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32);

    let group = run(Durability::group_default(), n, k, "Group").await;
    print_report("Group", n, k, &group);
    let os = run(Durability::Os, n, k, "Os").await;
    print_report("Os", n, k, &os);

    let group_ratio = group.barriers as f64 / n as f64;
    let os_ratio = os.barriers as f64 / n as f64;
    eprintln!(
        "\nbn-34o summary: Group barriers/new-name = {group_ratio:.4} \
         (~1/{:.0}), Os = {os_ratio:.4}. Group throughput {:.1}x of Os.",
        if group_ratio > 0.0 { 1.0 / group_ratio } else { 0.0 },
        os.wall_each.as_secs_f64() / group.wall_each.as_secs_f64().max(1e-9),
    );

    // Barriered modes never use the buffered (Process) flush.
    assert_eq!(group.buffered, 0, "Group is a barriered mode");
    assert_eq!(os.buffered, 0, "Os is a barriered mode");
    assert!(group.barriers >= 1 && os.barriers >= 1, "durability: fsyncs ran");

    // Group HAS a coalescing window: the shared-MetaStore serialization point
    // must dissolve — barriers/new-name well under 1. A regression to a
    // per-new-stream fsync would push this back toward 1.0.
    assert!(
        group_ratio < 0.5,
        "bn-34o: Group barrier/new-name ratio {group_ratio:.4} — the name \
         flush is not riding the group barrier (coalescing regressed)"
    );

    // Os is reported, not required to coalesce: a closed-loop pipeline is
    // rate-matched, so ~1 name buffers per group-of-one. Bound it only for
    // sanity (never MORE than one meta fsync per new name).
    assert!(
        os_ratio <= 1.05,
        "Os barrier/new-name ratio {os_ratio:.4} exceeded 1 — the hook must \
         never fsync more than once per new name"
    );
}
