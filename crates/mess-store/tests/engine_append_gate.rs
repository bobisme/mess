//! bn-1s0: per-stream append gate.
//!
//! `LogEngine::append_batch` used to serialise the whole check-head → append
//! → apply critical section behind ONE store-wide `tokio::sync::Mutex<()>`,
//! so appends to two completely unrelated streams queued behind each other.
//! This bone replaces it with a fixed-shard, per-stream gate
//! (`engine::AppendGate`). Two properties must survive the swap:
//!
//! 1. The `ExpectedVersion` check-and-reserve is still atomic PER STREAM: N
//!    tasks racing `Exact(v)` against the SAME stream must yield exactly one
//!    winner.
//! 2. Appends to DIFFERENT streams genuinely overlap in wall-clock time under a
//!    real (non-tmpfs) durable commit path — the thing the old store-wide gate
//!    made impossible.
#![cfg(not(miri))]

use mess_log::committer::Durability;
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::engine::EngineOptions;
use mess_store::{AppendError, LogEngine, Version};

fn rec(t: &str, d: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: t.to_string(), data: d.to_vec() }
}

/// Requirement 2: N tasks race `Exact(v)` appends to the SAME stream.
/// Exactly one must win; the rest must observe the version conflict with the
/// correct `expected`/`actual` pair, and the stream's final head must be
/// exactly one version past the raced-on version (no lost or double writes).
#[tokio::test]
async fn same_stream_exact_version_race_has_exactly_one_winner() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = LogEngine::open(dir.path()).expect("open");

    // Establish the stream at version 0.
    engine
        .append_batch(
            "race-stream",
            Version::NoStream,
            &[rec("Opened", b"seed")],
        )
        .await
        .expect("seed append");
    assert_eq!(engine.head("race-stream").await.unwrap(), Version::At(0));

    const N: usize = 32;
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let engine = engine.clone();
        handles.push(tokio::spawn(async move {
            engine
                .append_batch(
                    "race-stream",
                    Version::At(0),
                    &[rec("Claim", i.to_string().as_bytes())],
                )
                .await
        }));
    }

    let mut wins = 0usize;
    let mut conflicts = 0usize;
    for h in handles {
        match h.await.expect("task panicked") {
            Ok(appended) => {
                wins += 1;
                assert_eq!(
                    appended.version,
                    Version::At(1),
                    "winner lands at version 1"
                );
            }
            Err(AppendError::Conflict { expected, actual }) => {
                assert_eq!(expected, Version::At(0));
                assert_eq!(
                    actual,
                    Version::At(1),
                    "conflict must see the winner's head"
                );
                conflicts += 1;
            }
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }

    assert_eq!(
        wins, 1,
        "exactly one Exact(0) append must win the race, got {wins}"
    );
    assert_eq!(conflicts, N - 1);
    assert_eq!(engine.head("race-stream").await.unwrap(), Version::At(1));

    // No double/lost writes: exactly 2 events total on the stream (seed +
    // the single winner), and read_stream agrees with the head.
    let page = engine
        .read_stream("race-stream", Version::NoStream, 1000)
        .await
        .unwrap();
    assert_eq!(page.len(), 2);
}

/// Requirement 3: appends to K DISTINCT streams, run under a real
/// (non-tmpfs) durability path with a real synchronous `fdatasync` barrier,
/// must genuinely overlap — proof that the per-stream gate no longer forces
/// different streams' appends to queue behind one lock the way the old
/// store-wide mutex did.
///
/// The probe is timing, not a sleep, and it is chosen to be sensitive to the
/// OLD bug specifically: `Durability::Group` lets `mess-log`'s committer
/// coalesce multiple *concurrently in-flight* requests into a single
/// `fdatasync` (its "gate" tracks writers between `append()` entry and
/// submission — see `mess-log`'s `committer::gather`). Under the OLD
/// store-wide `append_gate`, only one task could EVER be inside the
/// check-and-submit section at a time, so every group was permanently
/// pinned at size 1 — concurrent submission was structurally impossible, no
/// matter how generous the coalescing window. Under the per-stream gate, K
/// distinct-stream tasks can all reach submission together, so the committer
/// coalesces them into far fewer `fdatasync` calls. That difference shows up
/// directly in wall-clock time: total time for K concurrent distinct-stream
/// appends should be a small fraction of K sequential appends' total time,
/// not roughly equal to it.
#[tokio::test]
async fn distinct_streams_overlap_under_durable_commit_path() {
    // A real (non-`tmpfs`) scratch dir: `fdatasync` is a no-op on `tmpfs`
    // and would make the "slow commit path" this test relies on dishonestly
    // fast (or make `open_with` itself fail with EDQUOT/os 122 on some
    // `tmpfs` configs). `sweeping_temp_dir` resolves `TMPDIR` (falling back
    // to `$HOME/.cache/mess-test-tmp`, the same real device this test used
    // to hardcode directly) and — bn-cxr — sweeps stale sibling dirs so
    // this real-fs suite stops accumulating leaked segment dirs there.
    let dir = mess_testkit::sweeping_temp_dir("engine-append-gate-overlap");
    let engine = LogEngine::open_with(
        dir.path(),
        EngineOptions {
            // `Group`: ack after the covering group's single `fdatasync`
            // returns. A real, non-negligible synchronous disk barrier per
            // group — the "slow/durable commit path" — with a coalescing
            // window generous enough that a genuine convoy of concurrently
            // in-flight requests gets gathered into one fsync, while a
            // single request still closes (early-close) without waiting out
            // the full cap.
            durability: Durability::Group {
                max_delay: std::time::Duration::from_millis(25),
                max_bytes: 64 * 1024 * 1024,
            },
            ..EngineOptions::default()
        },
    )
    .expect("open");

    // Warm up the segment writer / blocking thread pool / page cache with a
    // few throwaway sequential appends so one-time setup costs don't leak
    // into either timed phase below.
    for i in 0..4 {
        engine
            .append_batch(
                &format!("warmup-{i}"),
                Version::NoStream,
                &[rec("Warmed", b"x")],
            )
            .await
            .expect("warmup append");
    }

    const N: usize = 16;

    // Prime every stream this test times, sequentially, OUTSIDE either timed
    // phase (bn-150). A newly-interned stream name now forces a real,
    // synchronous meta-store `fsync` strictly before its covering append —
    // a correct and deliberate durability cost (bn-150: name persistence
    // must be co-durable with the covering append), but a DIFFERENT concern
    // from what this test measures (per-stream gate / committer coalescing
    // concurrency). Left un-primed, every one of the 32 distinct brand-new
    // stream names below would pay that extra fsync in BOTH phases, and
    // — being a second, independent durable barrier outside the committer's
    // own group-commit coalescing — it does not coalesce the way the
    // covering append does, which swamps the very effect this test exists
    // to observe. Priming first means both timed phases below only ever see
    // already-interned streams and the already-interned "Opened" type: the
    // hot, no-new-name path, which is unconditionally zero-added-cost
    // (bn-150) and so cannot confound the measurement.
    for i in 0..N {
        engine
            .append_batch(
                &format!("serial-{i}"),
                Version::NoStream,
                &[rec("Opened", b"seed")],
            )
            .await
            .expect("prime serial stream");
        engine
            .append_batch(
                &format!("concurrent-{i}"),
                Version::NoStream,
                &[rec("Opened", b"seed")],
            )
            .await
            .expect("prime concurrent stream");
    }

    // Phase 1 — SERIAL baseline: N distinct (already-primed) streams, one
    // append fully awaited before the next starts. No two requests are ever
    // concurrently in flight, so every group is pinned at size 1 regardless
    // of gate design; this is the "no coalescing possible" reference point.
    let serial_start = std::time::Instant::now();
    for i in 0..N {
        engine
            .append_batch(
                &format!("serial-{i}"),
                Version::At(0),
                &[rec("Opened", b"payload")],
            )
            .await
            .expect("serial append");
    }
    let serial_total = serial_start.elapsed();

    // Phase 2 — CONCURRENT: the same N appends, to N other distinct
    // (already-primed) streams, all submitted at once. Under the per-stream
    // gate these can all reach the committer together and coalesce; under
    // the old store-wide gate they'd be admitted one at a time and behave
    // just like phase 1.
    let concurrent_start = std::time::Instant::now();
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let engine = engine.clone();
        handles.push(tokio::spawn(async move {
            engine
                .append_batch(
                    &format!("concurrent-{i}"),
                    Version::At(0),
                    &[rec("Opened", b"payload")],
                )
                .await
                .expect("concurrent append")
        }));
    }
    for h in handles {
        h.await.expect("task panicked");
    }
    let concurrent_total = concurrent_start.elapsed();

    eprintln!(
        "distinct_streams_overlap_under_durable_commit_path: serial {N}x = \
         {serial_total:?}, concurrent {N}x = {concurrent_total:?} (speedup \
         {:.2}x)",
        serial_total.as_secs_f64() / concurrent_total.as_secs_f64().max(1e-9)
    );

    assert!(
        concurrent_total < serial_total / 2,
        "expected concurrent distinct-stream appends to be at least 2x faster \
         than the serial baseline (per-stream gate should let the committer \
         coalesce concurrently in-flight requests into fewer fdatasync \
         calls); got serial={serial_total:?} concurrent={concurrent_total:?} \
         — looks like appends are still effectively serialised"
    );

    // Sanity: every stream actually landed both its priming event (version 0)
    // and its timed event (version 1).
    for i in 0..N {
        assert_eq!(
            engine.head(&format!("serial-{i}")).await.unwrap(),
            Version::At(1)
        );
        assert_eq!(
            engine.head(&format!("concurrent-{i}")).await.unwrap(),
            Version::At(1)
        );
    }
}
