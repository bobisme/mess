//! Flat-owner append ordering and convoy formation.
//!
//! `LogEngine::append_batch` used to serialise the whole check-head → append
//! → apply critical section behind ONE store-wide `tokio::sync::Mutex<()>`,
//! so appends to two completely unrelated streams queued behind each other.
//! The flat-combined owner replaces those gates entirely. Two properties must
//! survive the swap:
//!
//! 1. The `ExpectedVersion` check-and-reserve is still atomic PER STREAM: N
//!    tasks racing `Exact(v)` against the SAME stream must yield exactly one
//!    winner.
//! 2. Appends to DIFFERENT streams can occupy one owner cohort and one durable
//!    group. That proof needs a private, test-only post-admission owner
//!    rendezvous, so it lives in `engine::append_gate_tests` rather than this
//!    integration-test crate.
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
    let dir = mess_testkit::sweeping_temp_dir("engine-append-gate-race");
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
    let start = std::sync::Arc::new(tokio::sync::Barrier::new(N + 1));
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let engine = engine.clone();
        let start = start.clone();
        handles.push(tokio::spawn(async move {
            start.wait().await;
            engine
                .append_batch(
                    "race-stream",
                    Version::At(0),
                    &[rec("Claim", i.to_string().as_bytes())],
                )
                .await
        }));
    }
    // Every contender has reached the same pre-submit boundary before any is
    // released, so this exercises a real Exact(0) race rather than merely
    // creating tasks that the runtime may poll one at a time.
    start.wait().await;

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

/// `Os` durability is intentionally one fdatasync per written batch, even
/// though the flat owner can see multiple producer intents at once. A new-name
/// append writes a registry batch plus its domain batch, so it earns two.
#[tokio::test]
async fn os_durability_remains_sync_per_batch() {
    let dir = mess_testkit::sweeping_temp_dir("flat-owner-os-singleton");
    let engine = LogEngine::open_with(
        dir.path(),
        EngineOptions {
            durability: Durability::Os,
            ..EngineOptions::default()
        },
    )
    .expect("open");

    engine
        .append_batch("os-stream", Version::NoStream, &[rec("Opened", b"x")])
        .await
        .expect("new-name append");
    assert_eq!(engine.metrics().commit.fsync.count, 2);

    engine
        .append_batch("os-stream", Version::At(0), &[rec("Opened", b"y")])
        .await
        .expect("hot append");
    assert_eq!(engine.metrics().commit.fsync.count, 3);
}

/// Regression for bn-3pz: after each covering barrier wakes the producers,
/// they build and re-enter at slightly different times. The owner must not
/// close on the momentary empty-ring/in-flight-zero gap and split a stable
/// four-writer convoy into roughly two batches per fsync.
#[tokio::test]
async fn repeated_four_writer_convoy_reforms_before_early_close() {
    const WRITERS: usize = 4;
    const ROUNDS: u64 = 100;
    let dir = mess_testkit::sweeping_temp_dir("flat-owner-d7-convoy");
    let engine = LogEngine::open_with(
        dir.path(),
        EngineOptions {
            durability: Durability::group_default(),
            ..EngineOptions::default()
        },
    )
    .expect("open");

    for writer in 0..WRITERS {
        engine
            .append_batch(
                &format!("convoy-{writer}"),
                Version::NoStream,
                &[rec("Tick", b"seed")],
            )
            .await
            .expect("prime stream and names");
    }

    let before = engine.metrics().commit.fsync.count;
    let rendezvous = std::sync::Arc::new(tokio::sync::Barrier::new(WRITERS));
    let mut tasks = Vec::new();
    for writer in 0..WRITERS {
        let engine = engine.clone();
        let rendezvous = rendezvous.clone();
        tasks.push(tokio::spawn(async move {
            rendezvous.wait().await;
            for round in 0..ROUNDS {
                engine
                    .append_batch(
                        &format!("convoy-{writer}"),
                        Version::At(round),
                        &[rec("Tick", &round.to_le_bytes())],
                    )
                    .await
                    .expect("convoy append");
            }
        }));
    }
    for task in tasks {
        task.await.expect("writer task");
    }
    let fsyncs = engine.metrics().commit.fsync.count - before;
    let batches = WRITERS as u64 * ROUNDS;
    eprintln!(
        "repeated_four_writer_convoy: batches={batches} fsyncs={fsyncs} \
         batches/fsync={:.2}",
        batches as f64 / fsyncs as f64,
    );
    assert!(
        fsyncs <= 150,
        "D7 convoy split: expected substantially better than the known ~200 \
         fsync defect for {batches} batches, got {fsyncs}"
    );
}
