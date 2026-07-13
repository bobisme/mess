//! `bn-e2y`: the engine's runtime-metrics surface moves under a real workload,
//! and the mandatory §2.6 fsync-degradation flag latches.
//!
//! `bn-11n`: [`metrics_move_under_durable_workload`] used to also assert the
//! degradation flag stays *false* after a healthy three-batch workload. That
//! assertion is a real-wall-clock comparison against the alarm's fixed 50 ms
//! threshold (`DEFAULT_FSYNC_THRESHOLD`, mess-log/src/metrics.rs) — a
//! property of the *shared disk*, not of this store's wiring. Recurrence data
//! (2 incidents, 2026-07-10) showed it tripping for real: once immediately
//! after the 462s differential suite, once during a 10-crate parallel sweep;
//! standalone it always passes in ~2s. Reproduced directly in this bone by
//! running the compiled test binary in a loop while `differential_full_profile`
//! (`--release`) and a full `cargo test --workspace --release` sweep ran
//! concurrently: barriers were observed at 102–155 ms, comfortably crossing
//! the 50 ms threshold on an otherwise-healthy store. The fix splits the
//! promise in two: this test now asserts the flag stays *internally
//! consistent* with its own trip counter (a wiring property, independent of
//! host load) plus the unconditional counter-movement assertions below;
//! [`metrics_fsync_healthy_on_uncontended_disk`] keeps the strict
//! never-degraded check, `#[ignore]`d for on-demand/quiet-disk runs. The
//! alarm's trip-on-genuinely-slow-barrier behavior itself is covered
//! deterministically (no real disk timing involved) by
//! `degradation_alarm_fires_on_slow_barrier` in mess-log's committer tests.
#![cfg(not(miri))]

use std::time::Duration;

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};

fn rec(t: &str, d: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: t.to_string(), data: d.to_vec() }
}

/// Under a durable (`Group`) engine, appends move the throughput counters and
/// the barrier histogram; a fresh store is not degraded before any barrier
/// runs. (The stronger "stays healthy under this workload" claim is a
/// host-disk-speed property, not a store-wiring one — see the module doc and
/// [`metrics_fsync_healthy_on_uncontended_disk`].)
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

    // `bn-2di`: these three appends also minted FIVE names, each of which is a
    // `$registry` record — a real, committed, durable log event:
    //   append 1: stream `acct-1` + types `Opened`, `Deposited`  -> 3 records
    //   append 2: type `Withdrew`                                -> 1 record
    //   append 3: stream `acct-2` (`Opened` already known)       -> 1 record
    // Each append's registrations go in ONE batch, pushed ahead of the batch
    // that uses them. So the committer sees 6 batches and 9 events, of which 4
    // are the user's. These counters describe the LOG, and the registry is in
    // the log now — that is the whole point of the bone.
    const REGISTRY_EVENTS: u64 = 5;
    const REGISTRY_BATCHES: u64 = 3;
    let m = engine.metrics();
    assert_eq!(
        m.total_events as u64,
        4 + REGISTRY_EVENTS,
        "the log holds every committed event, registrations included"
    );
    assert_eq!(
        m.commit.events,
        4 + REGISTRY_EVENTS,
        "committer counts every durable event"
    );
    assert_eq!(m.commit.batches, 3 + REGISTRY_BATCHES);
    assert!(m.commit.groups >= 1, "at least one barriered group");
    assert_eq!(m.commit.fsync.count, m.commit.groups, "one barrier per group");
    assert!(m.commit.bytes > 0, "durable bytes counted");
    assert_eq!(
        m.durable_watermark,
        4 + REGISTRY_EVENTS,
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
    // bn-11n: NOT `assert!(!m.commit.fsync_degraded, ...)` — whether a real
    // barrier crosses the fixed 50 ms threshold depends on host disk
    // contention (a concurrent build/test sweep), not on this store's
    // wiring; see the module doc. What IS a wiring property, independent of
    // host load, is that the sticky flag and its trip counter always agree —
    // a dead alarm (e.g. `observe` wired to a no-op) that lets `trips`
    // advance without ever latching `fsync_degraded`, or latches without
    // ever counting a trip, would fail this regardless of disk speed.
    assert_eq!(
        m.commit.fsync_degraded,
        m.commit.fsync_degraded_trips > 0,
        "the degradation flag and its trip counter must agree (flag={}, \
         trips={})",
        m.commit.fsync_degraded,
        m.commit.fsync_degraded_trips,
    );
    if m.commit.fsync_degraded {
        eprintln!(
            "note: fsync alarm tripped during this run (trips={}, p99={} ns) \
             — expected under host disk contention, not a regression; see \
             `metrics_fsync_healthy_on_uncontended_disk` (#[ignore]d) for the \
             strict standalone check",
            m.commit.fsync_degraded_trips, m.commit.fsync.p99_nanos,
        );
    }

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
    // bn-11n: same host-disk-contention caveat as the commit-path alarm
    // above (module doc) — observed live in this bone's own validation run
    // (a concurrent sibling workspace's `cargo test` tripped this exact
    // assertion at a 78.8 ms seal-fsync latency). Assert wiring consistency,
    // not real-wall-clock health.
    assert_eq!(
        after_seal.seal_fsync_degraded,
        after_seal.seal_fsync_degraded_trips > 0,
        "the seal-fsync degradation flag and its trip counter must agree \
         (flag={}, trips={})",
        after_seal.seal_fsync_degraded,
        after_seal.seal_fsync_degraded_trips,
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

/// `bn-11n`: the strict, timing-sensitive half of
/// [`metrics_move_under_durable_workload`] and
/// [`cache_and_seal_metrics_move_under_workload`] — that on a healthy,
/// uncontended disk this tiny workload's real commit AND seal barriers never
/// cross the §2.6 alarms' 50 ms thresholds. `#[ignore]`d because that claim
/// only holds when nothing else on the host is contending for the same disk
/// (see the module doc for the recurrence data and two independent
/// reproductions — one under a concurrent differential-suite +
/// workspace-sweep load, one live against a sibling workspace's own `cargo
/// test`). Run on demand: `cargo test -p mess-store --test engine_metrics --
/// --ignored metrics_fsync_healthy_on_uncontended_disk`.
#[tokio::test]
#[ignore = "timing-sensitive: assumes an uncontended disk; run standalone, not \
            under parallel host load (bn-11n)"]
async fn metrics_fsync_healthy_on_uncontended_disk() {
    let dir = mess_testkit::sweeping_temp_dir(
        "engine-metrics-fsync-healthy-uncontended",
    );
    let opts = EngineOptions {
        durability: mess_log::committer::Durability::group_default(),
        block_cache_budget_bytes: 4 * 1024 * 1024,
        ..EngineOptions::default()
    };
    let engine = LogEngine::open_with(dir.path(), opts).expect("open");

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
    assert!(
        !m.commit.fsync_degraded,
        "healthy barriers on an uncontended disk must not trip the alarm \
         (p99={} ns, threshold={} ns) — if this trips, either re-run on a \
         quieter disk or the committer/alarm wiring genuinely regressed",
        m.commit.fsync.p99_nanos, m.commit.fsync_threshold_nanos,
    );

    engine.seal_active().expect("seal active segment");
    let after_seal = engine.metrics();
    assert!(
        !after_seal.seal_fsync_degraded,
        "healthy seal barriers on an uncontended disk must not trip the alarm \
         (p99={} ns, threshold={} ns default) — if this trips, either re-run \
         on a quieter disk or the seal-path alarm wiring genuinely regressed",
        after_seal.seal_fsync.p99_nanos,
        mess_log::committer::DEFAULT_FSYNC_THRESHOLD.as_nanos(),
    );
}
