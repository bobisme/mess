//! Deterministic owner-cohort regression for distinct-stream append overlap.
//!
//! This lives beside the engine, rather than in `tests/`, so the production
//! engine can expose a completely private `cfg(test)` admission rendezvous.

use super::*;
use crate::backend::{Backend, RecordToAppend};

fn rec(t: &str, d: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: t.to_string(), data: d.to_vec() }
}

/// Dropping an incomplete generation must wake the parked owner, allowing the
/// admitted append to finish and `Inner::drop` to join the owner thread.
#[tokio::test]
async fn abandoned_owner_cohort_guard_unblocks_append_and_shutdown() {
    let dir = mess_testkit::sweeping_temp_dir("engine-owner-cohort-abandon");
    let engine = LogEngine::open_with(
        dir.path(),
        EngineOptions {
            durability: Durability::Group {
                max_delay: Duration::from_millis(25),
                max_bytes: 64 * 1024 * 1024,
            },
            ..EngineOptions::default()
        },
    )
    .expect("open");
    engine
        .append_batch(
            "abandoned-cohort",
            Version::NoStream,
            &[rec("Opened", b"seed")],
        )
        .await
        .expect("prime stream");

    let gate = Arc::clone(&engine.inner.owner.cohort_gate);
    let cohort = gate.arm(2);
    let producer = {
        let engine = engine.clone();
        tokio::spawn(async move {
            engine
                .append_batch(
                    "abandoned-cohort",
                    Version::At(0),
                    &[rec("Opened", b"payload")],
                )
                .await
        })
    };

    let admitted = tokio::task::spawn_blocking(move || {
        gate.wait_until_admitted(1);
    });
    tokio::time::timeout(Duration::from_secs(10), admitted)
        .await
        .expect("producer admission timed out")
        .expect("admission waiter panicked");

    // Only one of the required two intents exists. The append cannot finish
    // until this incomplete generation is explicitly abandoned.
    assert!(!producer.is_finished());
    drop(cohort);
    tokio::time::timeout(Duration::from_secs(10), producer)
        .await
        .expect("append stayed parked after guard drop")
        .expect("producer panicked")
        .expect("admitted append failed");

    tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || drop(engine)),
    )
    .await
    .expect("engine shutdown stayed parked after guard drop")
    .expect("shutdown task panicked");
}

/// N hot appends admitted to different streams are one owner-visible cohort:
/// the owner gathers all N intents into one direct-committer call, and Group
/// durability covers every batch with exactly one successful `fdatasync`.
#[tokio::test]
async fn distinct_streams_overlap_under_durable_commit_path() {
    const N: usize = 16;
    assert!(
        N <= OWNER_RING_CAPACITY + 1,
        "the parked owner holds one intent and its channel holds the rest"
    );

    let dir = mess_testkit::sweeping_temp_dir("engine-owner-admission-cohort");
    let engine = LogEngine::open_with(
        dir.path(),
        EngineOptions {
            durability: Durability::Group {
                max_delay: Duration::from_millis(25),
                max_bytes: 64 * 1024 * 1024,
            },
            ..EngineOptions::default()
        },
    )
    .expect("open");

    // Prime both measured populations. New names add `$registry` batches;
    // hot appends do not, so every measured append below is exactly one
    // domain batch with the same tiny shape.
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
                &format!("cohort-{i}"),
                Version::NoStream,
                &[rec("Opened", b"seed")],
            )
            .await
            .expect("prime cohort stream");
    }

    // Fully awaited serial control: no second intent can be owner-visible,
    // hence every batch forms its own group and covering barrier.
    let before_serial = engine.metrics().commit;
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
    let after_serial = engine.metrics().commit;

    // Arm after the serial control is fully acknowledged. The owner consumes
    // the first subsequent intent, parks before `gather`, and wakes only once
    // all N sends own their channel slots. N=16 is deliberately well below
    // the boundary's capacity: one received plus 1,024 channel-resident.
    let cohort = engine.inner.owner.cohort_gate.arm(N);
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let engine = engine.clone();
        handles.push(tokio::spawn(async move {
            engine
                .append_batch(
                    &format!("cohort-{i}"),
                    Version::At(0),
                    &[rec("Opened", b"payload")],
                )
                .await
        }));
    }

    // This timeout detects a broken test seam; it is not evidence for the
    // grouping assertion. Dropping the guard first disarms/notifies the owner
    // before any unfinished producers are aborted.
    let joined = tokio::time::timeout(Duration::from_secs(30), async {
        for handle in &mut handles {
            let result = handle
                .await
                .map_err(|error| format!("cohort task panicked: {error}"))?;
            result.map_err(|error| format!("cohort append failed: {error}"))?;
        }
        Ok::<(), String>(())
    })
    .await;
    match joined {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            drop(cohort);
            for handle in &handles {
                handle.abort();
            }
            panic!("{error}");
        }
        Err(_) => {
            drop(cohort);
            for handle in &handles {
                handle.abort();
            }
            panic!("owner admission cohort timed out");
        }
    }
    drop(cohort);
    let after_cohort = engine.metrics().commit;

    let serial_batches = after_serial.batches - before_serial.batches;
    let serial_events = after_serial.events - before_serial.events;
    let serial_groups = after_serial.groups - before_serial.groups;
    let serial_fsyncs = after_serial.fsync.count - before_serial.fsync.count;
    let cohort_batches = after_cohort.batches - after_serial.batches;
    let cohort_events = after_cohort.events - after_serial.events;
    let cohort_groups = after_cohort.groups - after_serial.groups;
    let cohort_fsyncs = after_cohort.fsync.count - after_serial.fsync.count;

    eprintln!(
        "distinct_streams_overlap_under_durable_commit_path: serial \
         batches/groups/fsyncs={serial_batches}/{serial_groups}/\
         {serial_fsyncs}, \
         cohort={cohort_batches}/{cohort_groups}/{cohort_fsyncs}",
    );

    assert_eq!(serial_batches, N as u64, "one hot batch per serial append");
    assert_eq!(serial_events, N as u64, "one event per serial append");
    assert_eq!(serial_groups, N as u64, "serial appends are singleton groups");
    assert_eq!(
        serial_fsyncs, N as u64,
        "serial Group appends each issue one covering barrier"
    );

    assert_eq!(cohort_batches, N as u64, "one hot batch per cohort intent");
    assert_eq!(cohort_events, N as u64, "one event per cohort intent");
    assert_eq!(
        cohort_groups, 1,
        "one owner-visible cohort must be one commit group"
    );
    assert_eq!(
        cohort_fsyncs, 1,
        "one commit group must issue one covering barrier"
    );

    for i in 0..N {
        assert_eq!(
            engine.head(&format!("serial-{i}")).await.unwrap(),
            Version::At(1)
        );
        assert_eq!(
            engine.head(&format!("cohort-{i}")).await.unwrap(),
            Version::At(1)
        );
    }
}
