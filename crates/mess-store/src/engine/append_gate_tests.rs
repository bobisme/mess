//! Deterministic owner-cohort regression for distinct-stream append overlap.
//!
//! This lives beside the engine, rather than in `tests/`, so the production
//! engine can expose a completely private `cfg(test)` admission rendezvous.

use super::*;
use crate::backend::{Backend, OwnedAppendBatch, RecordToAppend};

fn rec(t: &str, d: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: t.to_string(), data: d.to_vec() }
}

async fn append_one(
    engine: &LogEngine,
    owned: bool,
    stream: &str,
    expected: Version,
    data: &[u8],
) -> Result<Appended, AppendError<EngineError>> {
    let record = rec("Opened", data);
    if owned {
        engine
            .append_batch_owned(
                stream,
                expected,
                OwnedAppendBatch::from_records(vec![record]),
            )
            .await
    } else {
        engine.append_batch(stream, expected, &[record]).await
    }
}

async fn append_homogeneous(
    engine: &LogEngine,
    owned: bool,
    stream: &str,
    expected: Version,
    message_type: &str,
    count: usize,
) -> Result<Appended, AppendError<EngineError>> {
    let records: Vec<RecordToAppend> = (0..count)
        .map(|index| RecordToAppend {
            message_type: message_type.to_owned(),
            data:         vec![index as u8],
        })
        .collect();
    if owned {
        engine
            .append_batch_owned(
                stream,
                expected,
                OwnedAppendBatch::from_records(records),
            )
            .await
    } else {
        engine.append_batch(stream, expected, &records).await
    }
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
    prove_distinct_streams_overlap(false).await;
    prove_distinct_streams_overlap(true).await;
}

/// The Group byte ceiling is defined over the ordinary borrowed records,
/// including every repeated message-type string. An owned API call under
/// Group deliberately falls back to that exact representation and must not
/// inherit the Process-only compact-name cost.
#[tokio::test]
async fn group_max_bytes_preserves_borrowed_cost_for_owned_fallback() {
    prove_group_max_bytes_uses_borrowed_cost(false).await;
    prove_group_max_bytes_uses_borrowed_cost(true).await;
}

async fn prove_group_max_bytes_uses_borrowed_cost(owned: bool) {
    const RECORDS: usize = 4;
    const MESSAGE_TYPE: &str = "RepeatedEvent";
    const STREAMS: [&str; 2] = ["limit-a", "limit-b"];

    let compact_cost = STREAMS[0].len() + MESSAGE_TYPE.len() + RECORDS;
    let borrowed_cost = STREAMS[0].len() + RECORDS * (MESSAGE_TYPE.len() + 1);
    let max_bytes = (compact_cost + borrowed_cost) / 2;
    assert!(compact_cost < max_bytes);
    assert!(max_bytes < borrowed_cost);
    assert_eq!(STREAMS[0].len(), STREAMS[1].len());

    let mode = if owned { "owned-fallback" } else { "borrowed" };
    let dir = mess_testkit::sweeping_temp_dir(&format!(
        "engine-owner-max-bytes-{mode}"
    ));
    let engine = LogEngine::open_with(
        dir.path(),
        EngineOptions {
            durability: Durability::Group {
                max_delay: Duration::from_millis(25),
                max_bytes: max_bytes as u64,
            },
            ..EngineOptions::default()
        },
    )
    .expect("open");

    // Fully acknowledge setup so both measured intents are hot domain-only
    // batches. Setup is borrowed in both variants and excluded by snapshots.
    for stream in STREAMS {
        engine
            .append_batch(
                stream,
                Version::NoStream,
                &[rec(MESSAGE_TYPE, b"seed")],
            )
            .await
            .expect("prime stream and event type");
    }
    let before_commit = engine.metrics().commit;
    let before_input = engine.append_input_metrics();

    // The private gate, not scheduling or elapsed time, proves both intents
    // are owner-visible before gather evaluates the first intent's cost.
    let cohort = engine.inner.owner.cohort_gate.arm(2);
    let mut handles = Vec::with_capacity(2);
    for stream in STREAMS {
        let engine = engine.clone();
        handles.push(tokio::spawn(async move {
            append_homogeneous(
                &engine,
                owned,
                stream,
                Version::At(0),
                MESSAGE_TYPE,
                RECORDS,
            )
            .await
        }));
    }
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
            panic!("owner byte-limit cohort timed out");
        }
    }
    drop(cohort);

    let after_commit = engine.metrics().commit;
    let after_input = engine.append_input_metrics();
    assert_eq!(after_commit.batches - before_commit.batches, 2);
    assert_eq!(
        after_commit.events - before_commit.events,
        (2 * RECORDS) as u64,
    );
    assert_eq!(
        after_commit.groups - before_commit.groups,
        2,
        "{mode}: each above-threshold intent must be its own group",
    );
    assert_eq!(
        after_commit.fsync.count - before_commit.fsync.count,
        2,
        "{mode}: each singleton Group requires its own barrier",
    );
    assert_eq!(after_input.owned_batches - before_input.owned_batches, 0);
    assert_eq!(after_input.borrowed_batches - before_input.borrowed_batches, 2);
    assert_eq!(
        after_input.borrowed_records - before_input.borrowed_records,
        (2 * RECORDS) as u64,
    );
    assert_eq!(
        after_input.copied_records - before_input.copied_records,
        (2 * RECORDS) as u64,
    );
    assert_eq!(
        after_input.copied_bytes - before_input.copied_bytes,
        (2 * RECORDS * (MESSAGE_TYPE.len() + 1)) as u64,
    );
}

async fn prove_distinct_streams_overlap(owned: bool) {
    const N: usize = 16;
    assert!(
        N <= OWNER_RING_CAPACITY + 1,
        "the parked owner holds one intent and its channel holds the rest"
    );

    let mode = if owned { "owned" } else { "borrowed" };
    let dir = mess_testkit::sweeping_temp_dir(&format!(
        "engine-owner-admission-cohort-{mode}"
    ));
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
        append_one(
            &engine,
            owned,
            &format!("serial-{i}"),
            Version::NoStream,
            b"seed",
        )
        .await
        .expect("prime serial stream");
        append_one(
            &engine,
            owned,
            &format!("cohort-{i}"),
            Version::NoStream,
            b"seed",
        )
        .await
        .expect("prime cohort stream");
    }

    // Fully awaited serial control: no second intent can be owner-visible,
    // hence every batch forms its own group and covering barrier.
    let before_serial = engine.metrics().commit;
    for i in 0..N {
        append_one(
            &engine,
            owned,
            &format!("serial-{i}"),
            Version::At(0),
            b"payload",
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
            append_one(
                &engine,
                owned,
                &format!("cohort-{i}"),
                Version::At(0),
                b"payload",
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
        "distinct_streams_overlap_under_durable_commit_path[{mode}]: serial \
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

    // Group deliberately materializes even an owned API submission into the
    // borrowed compatibility path. Both populations must therefore have the
    // same input-path counters as well as the exact structural group/barrier
    // result above.
    let input = engine.append_input_metrics();
    let total_appends = (4 * N) as u64;
    assert_eq!(input.owned_batches, 0, "{mode}: Process fast path is disabled");
    assert_eq!(input.owned_records, 0, "{mode}: no owned records retained");
    assert_eq!(input.borrowed_batches, total_appends);
    assert_eq!(input.borrowed_records, total_appends);
    assert_eq!(input.copied_records, total_appends);
    let copied_bytes = (2 * N * ("Opened".len() + b"seed".len())
        + 2 * N * ("Opened".len() + b"payload".len()))
        as u64;
    assert_eq!(input.copied_bytes, copied_bytes);

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
