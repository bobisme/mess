//! `bn-e2y`: the engine's runtime-metrics surface moves under a real workload,
//! and the mandatory §2.6 fsync-degradation flag latches.
#![cfg(not(miri))]

use std::time::Duration;

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};

fn rec(t: &str, d: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: t.to_string(), data: d.to_vec() }
}

/// Under a durable (`Group`) engine, appends move the throughput counters and
/// the barrier histogram; a fresh store is not degraded.
#[tokio::test]
async fn metrics_move_under_durable_workload() {
    let dir = mess_testkit::sweeping_temp_dir(
        "engine-metrics-metrics-move-under-durable",
    );
    let opts = EngineOptions {
        durability: mess_log::committer::Durability::group_default(),
        ..EngineOptions::default()
    };
    let engine = LogEngine::open_with(dir.path(), opts).expect("open");

    // Baseline: nothing committed, no barriers, not degraded.
    let m0 = engine.metrics();
    assert_eq!(m0.total_events, 0);
    assert_eq!(m0.commit.events, 0);
    assert!(!m0.commit.fsync_degraded, "a fresh store is not degraded");
    assert_eq!(m0.durable_watermark, 0);

    // Commit three batches across two streams.
    engine
        .append_batch(
            "acct-1",
            Version::NoStream,
            &[rec("Opened", b"x"), rec("Deposited", b"5")],
        )
        .await
        .unwrap();
    engine
        .append_batch("acct-1", Version::At(1), &[rec("Withdrew", b"2")])
        .await
        .unwrap();
    engine
        .append_batch("acct-2", Version::NoStream, &[rec("Opened", b"y")])
        .await
        .unwrap();

    let m = engine.metrics();
    assert_eq!(m.total_events, 4, "record book counts every committed event");
    assert_eq!(m.commit.events, 4, "committer counts every durable event");
    assert_eq!(m.commit.batches, 3);
    assert!(m.commit.groups >= 1, "at least one barriered group");
    assert_eq!(m.commit.fsync.count, m.commit.groups, "one barrier per group");
    assert!(m.commit.bytes > 0, "durable bytes counted");
    assert_eq!(
        m.durable_watermark, 4,
        "watermark past the last global position"
    );
    // fsync percentiles are populated (>= mean is meaningless on tmpfs, but the
    // count proves the histogram recorded).
    assert!(m.commit.fsync.count > 0);
    // The degradation threshold is the 50 ms default.
    assert_eq!(
        m.commit.fsync_threshold_nanos,
        Duration::from_millis(50).as_nanos() as u64
    );
    assert!(
        !m.commit.fsync_degraded,
        "healthy barriers must not trip the alarm"
    );

    // The cache is enabled by default now, but this workload never reads a
    // sealed stream, so no block lookup has happened yet — hit/miss are still
    // 0. (The `cache_and_seal_metrics_move_under_workload` test below
    // drives them.)
    assert_eq!(m.cache_hits + m.cache_misses, 0);
    assert!(m.active_segment_age_secs >= 0.0);
    // No seal has run in this workload.
    assert_eq!(m.seals, 0);
    assert_eq!(m.seal_fsync.count, 0);
    assert!(!m.seal_fsync_degraded);
    assert_eq!(
        m.seals_skipped, 0,
        "bn-u6o: nothing skipped absent a stalled roll"
    );
}

/// `bn-e2y` (SCOPE 5): with the block cache enabled (the default now) and the
/// seal path instrumented, both the **cache hit/miss** and the **seal**
/// (duration + fsync-barrier) metrics MOVE under a real workload — the
/// requirement that was previously unsatisfiable (cache hardwired off, seal
/// metrics absent).
#[tokio::test]
async fn cache_and_seal_metrics_move_under_workload() {
    let dir = mess_testkit::sweeping_temp_dir(
        "engine-metrics-cache-and-seal-metrics",
    );
    let opts = EngineOptions {
        durability: mess_log::committer::Durability::group_default(),
        block_cache_budget_bytes: 4 * 1024 * 1024,
        ..EngineOptions::default()
    };
    let engine = LogEngine::open_with(dir.path(), opts).expect("open");

    // Commit a stream, then seal the active segment into the cold tier.
    engine
        .append_batch(
            "acct-1",
            Version::NoStream,
            &[rec("Opened", b"x"), rec("Deposited", b"5")],
        )
        .await
        .unwrap();
    engine
        .append_batch("acct-1", Version::At(1), &[rec("Withdrew", b"2")])
        .await
        .unwrap();

    engine.seal_active().expect("seal active segment");

    // Seal metrics moved: a seal ran, its duration was timed, and it issued
    // real seal-path fsync barriers (the sidecar + directory fsyncs).
    let after_seal = engine.metrics();
    assert!(after_seal.seals >= 1, "seal count moved");
    assert!(after_seal.seal_duration.count >= 1, "seal duration recorded");
    assert!(
        after_seal.seal_fsync.count >= 1,
        "seal-path fsync barriers recorded"
    );
    assert!(
        !after_seal.seal_fsync_degraded,
        "healthy seal barriers do not trip the alarm"
    );
    // bn-u6o: a healthy on-demand seal never hits the bounded-wait skip path.
    assert_eq!(
        after_seal.seals_skipped, 0,
        "a healthy seal is never counted as skipped"
    );

    // First sealed read decodes the block → a cache MISS.
    let r1 =
        engine.read_stream("acct-1", Version::NoStream, 100).await.unwrap();
    assert_eq!(r1.len(), 3, "sealed stream replays all three events");
    let after_first = engine.metrics();
    assert!(
        after_first.cache_misses >= 1,
        "first sealed read misses the cache"
    );

    // Second sealed read is served from the warmed cache → a cache HIT.
    let r2 =
        engine.read_stream("acct-1", Version::NoStream, 100).await.unwrap();
    assert_eq!(r2.len(), 3);
    let after_second = engine.metrics();
    assert!(
        after_second.cache_hits >= 1,
        "second sealed read hits the warmed cache (hits={}, misses={})",
        after_second.cache_hits,
        after_second.cache_misses,
    );
    assert!(after_second.cache_hit_rate > 0.0, "hit rate moved off zero");
    assert!(after_second.cache_entries >= 1, "a block is resident");
}
