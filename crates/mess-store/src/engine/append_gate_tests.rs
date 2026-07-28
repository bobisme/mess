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

/// The production occupancy metrics must move while intents own bounded
/// channel/byte resources and return exactly to zero after the complete
/// cohort retires. A zero-only implementation would not satisfy this test.
#[tokio::test]
async fn owner_reservation_occupancy_moves_and_returns_to_zero() {
    let dir = mess_testkit::sweeping_temp_dir("engine-owner-occupancy");
    let engine = LogEngine::open(dir.path()).expect("open");
    for stream in ["occupancy-a", "occupancy-b"] {
        engine
            .append_batch(stream, Version::NoStream, &[rec("Opened", b"seed")])
            .await
            .expect("prime stream");
    }

    // Keep the owner parked after consuming one intent. The second admitted
    // intent remains channel-resident, so both occupancy surfaces must move.
    let gate = Arc::clone(&engine.inner.owner.cohort_gate);
    let cohort = gate.arm(3);
    let mut producers = Vec::new();
    for stream in ["occupancy-a", "occupancy-b"] {
        let engine = engine.clone();
        producers.push(tokio::spawn(async move {
            engine
                .append_batch(
                    stream,
                    Version::At(0),
                    &[rec("Opened", b"payload")],
                )
                .await
        }));
    }
    let admitted_gate = Arc::clone(&engine.inner.owner.cohort_gate);
    tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || {
            admitted_gate.wait_until_admitted(2)
        }),
    )
    .await
    .expect("producer admission timed out")
    .expect("admission waiter panicked");

    let occupied = engine.metrics();
    assert!(occupied.owner_intent_slots_in_use > 0);
    assert!(occupied.owner_intent_bytes_in_use > 0);

    drop(cohort);
    for producer in producers {
        producer
            .await
            .expect("producer panicked")
            .expect("admitted append failed");
    }
    let quiescent = engine.metrics();
    assert_eq!(quiescent.owner_intent_slots_in_use, 0);
    assert_eq!(quiescent.owner_intent_bytes_in_use, 0);
}

/// Conflict, no-op, and abandoned-receiver completion paths obey the same
/// ownership boundary as a successful commit: no reservation may remain once
/// a caller can observe a terminal result (or a later sentinel completion).
#[tokio::test]
async fn owner_completion_releases_reservations_on_every_terminal_path() {
    let dir = mess_testkit::sweeping_temp_dir("engine-owner-completion");
    let engine = LogEngine::open(dir.path()).expect("open");
    for stream in ["completion-a", "completion-b"] {
        engine
            .append_batch(stream, Version::NoStream, &[rec("Opened", b"seed")])
            .await
            .expect("prime stream");
    }

    let conflict = engine
        .append_batch(
            "completion-a",
            Version::NoStream,
            &[rec("Opened", b"conflict")],
        )
        .await;
    assert!(matches!(conflict, Err(AppendError::Conflict { .. })));
    let after_conflict = engine.metrics();
    assert_eq!(after_conflict.owner_intent_slots_in_use, 0);
    assert_eq!(after_conflict.owner_intent_bytes_in_use, 0);

    engine
        .append_batch("completion-a", Version::At(0), &[])
        .await
        .expect("empty append");
    let after_empty = engine.metrics();
    assert_eq!(after_empty.owner_intent_slots_in_use, 0);
    assert_eq!(after_empty.owner_intent_bytes_in_use, 0);

    // Park the owner with one admitted intent, then abandon that intent's
    // receiver. A second admitted append releases the rendezvous and acts as a
    // terminal sentinel: after it wakes, its reservation and the cancelled
    // caller's earlier reservation must both be gone.
    let gate = Arc::clone(&engine.inner.owner.cohort_gate);
    let cohort = gate.arm(2);
    let cancelled = {
        let engine = engine.clone();
        tokio::spawn(async move {
            engine
                .append_batch(
                    "completion-a",
                    Version::At(0),
                    &[rec("Opened", b"cancelled")],
                )
                .await
        })
    };
    let admitted_gate = Arc::clone(&engine.inner.owner.cohort_gate);
    tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || {
            admitted_gate.wait_until_admitted(1)
        }),
    )
    .await
    .expect("cancelled producer admission timed out")
    .expect("admission waiter panicked");
    cancelled.abort();
    assert!(
        cancelled
            .await
            .expect_err("cancelled producer completed")
            .is_cancelled()
    );

    engine
        .append_batch(
            "completion-b",
            Version::At(0),
            &[rec("Opened", b"sentinel")],
        )
        .await
        .expect("sentinel append");
    drop(cohort);

    let after_cancel = engine.metrics();
    assert_eq!(after_cancel.owner_intent_slots_in_use, 0);
    assert_eq!(after_cancel.owner_intent_bytes_in_use, 0);
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

// ---------------------------------------------------------------------------
// bn-1gn1: bounded prepared construction memory
// ---------------------------------------------------------------------------

/// Replicates the admission `cost` the prepared paths compute *after*
/// `PreparedBatch::encode`, so the reservation taken *before* it can be
/// compared against the real thing rather than against a restatement of the
/// same estimate.
fn actual_prepared_cost(
    stream_id: &str,
    payloads: &[Vec<u8>],
    type_names: &[String],
    chain: bool,
) -> usize {
    let subframes: Vec<Subframe<'_>> =
        payloads.iter().map(|p| Subframe::plain(0, 0, 0, p)).collect();
    let zero_chain = [0u8; CHAIN_LEN];
    let prepared = PreparedBatch::encode(&BatchInput {
        segment_epoch:        0,
        batch_id:             0,
        first_global_pos:     0,
        stream_id:            0,
        category_id:          0,
        first_stream_version: 0,
        crypto_chain:         chain.then_some(&zero_chain),
        subframes:            &subframes,
    })
    .expect("representable prepared batch");
    stream_id
        .len()
        .saturating_add(prepared.total_len() as usize)
        .saturating_add(
            type_names
                .iter()
                .map(String::len)
                .fold(0usize, usize::saturating_add),
        )
        .saturating_add(payloads.len() * 4)
}

/// The whole safety argument rests on the pre-preparation reservation being an
/// over-estimate: `reconcile_owner_bytes` treats a shortfall as a bug (it
/// `debug_assert!`s, then releases and re-acquires rather than holding and
/// waiting). Pin that over-estimate across the shapes the acceptance matrix
/// names — 1/10/100/1000 frames, tiny and large payloads, homogeneous and
/// pathologically long heterogeneous type names, chain on and off.
#[test]
fn prepare_build_peak_never_under_estimates_prepared_cost() {
    let stream_id = "bound-accounting";
    for &frames in &[1usize, 10, 100, 1000] {
        for &payload_len in &[1usize, 64, 4096] {
            for &chain in &[false, true] {
                for &name_len in &[1usize, 200] {
                    let payloads: Vec<Vec<u8>> =
                        (0..frames).map(|_| vec![7u8; payload_len]).collect();
                    // Worst case for the reservation: every frame contributes a
                    // distinct name, so nothing dedupes away.
                    let type_names: Vec<String> = (0..frames)
                        .map(|i| format!("{i:0width$}", width = name_len))
                        .collect();
                    let encoded_estimate = HEADER_LEN
                        + MARKER_LEN
                        + usize::from(chain) * CHAIN_LEN
                        + frames * SUBFRAME_HDR_LEN
                        + frames * payload_len;
                    let type_name_bytes = type_names
                        .iter()
                        .map(String::len)
                        .fold(0usize, usize::saturating_add);
                    let reserved = prepare_build_peak(
                        stream_id.len(),
                        frames,
                        encoded_estimate,
                        type_name_bytes,
                    );
                    let actual = actual_prepared_cost(
                        stream_id,
                        &payloads,
                        &type_names,
                        chain,
                    );
                    assert!(
                        reserved >= actual,
                        "reservation {reserved} under-estimated cost {actual} \
                         (frames={frames} payload_len={payload_len} \
                         chain={chain} name_len={name_len})"
                    );
                }
            }
        }
    }
}

/// Preparation-sized appends on BOTH public submission paths must return every
/// reserved byte. A reservation leaked on the prepared path would show up here
/// as a non-zero quiescent occupancy, and would eventually wedge the ring.
#[tokio::test]
async fn prepared_appends_on_both_paths_release_every_reserved_byte() {
    for owned in [false, true] {
        let dir = mess_testkit::sweeping_temp_dir("engine-prepared-release");
        let engine = LogEngine::open(dir.path()).expect("open");
        let stream = if owned { "prepared-owned" } else { "prepared-borrowed" };

        // Comfortably over PREPARE_MIN_ENCODED_BYTES so the prepared branch,
        // and therefore the build reservation, is the one exercised.
        let big = vec![9u8; 8 * 1024];
        let records: Vec<RecordToAppend> =
            (0..8).map(|_| rec("Opened", &big)).collect();

        let appended = if owned {
            engine
                .append_batch_owned(
                    stream,
                    Version::NoStream,
                    OwnedAppendBatch::from_records(records),
                )
                .await
        } else {
            engine.append_batch(stream, Version::NoStream, &records).await
        }
        .expect("prepared append");
        assert_eq!(appended.version, Version::At(7));

        let quiescent = engine.metrics();
        assert_eq!(
            quiescent.owner_intent_bytes_in_use, 0,
            "prepared append leaked build reservation (owned={owned})"
        );
        assert_eq!(quiescent.owner_intent_slots_in_use, 0);
    }
}

/// A caller that goes away mid-append must not strand its build reservation.
/// The reservation is taken before preparation, so the cancellation window it
/// opens is strictly wider than the pre-bn-1gn1 one — drop the future inside
/// that window and require the ring back.
#[tokio::test]
async fn cancelled_prepared_append_releases_its_build_reservation() {
    let dir = mess_testkit::sweeping_temp_dir("engine-prepared-cancel");
    let engine = LogEngine::open(dir.path()).expect("open");
    engine
        .append_batch("cancel-me", Version::NoStream, &[rec("Opened", b"seed")])
        .await
        .expect("prime stream");

    let big = vec![3u8; 8 * 1024];
    let records: Vec<RecordToAppend> =
        (0..8).map(|_| rec("Opened", &big)).collect();

    // Arm for two admissions but submit one, so the cohort never fills and the
    // owner stays parked with the intent channel-resident. Then abandon the
    // future while its reservation is still outstanding.
    let gate = Arc::clone(&engine.inner.owner.cohort_gate);
    let cohort = gate.arm(2);
    {
        let mut pending = Box::pin(engine.append_batch(
            "cancel-me",
            Version::At(0),
            &records,
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(250), &mut pending)
                .await
                .is_err(),
            "append should still be parked behind the armed cohort"
        );
        // `pending` dropped here: cancellation mid-flight.
    }
    drop(cohort);

    // Let any admitted intent drain before sampling the leak check.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let quiescent = engine.metrics();
    assert_eq!(
        quiescent.owner_intent_bytes_in_use, 0,
        "cancelled prepared append stranded its build reservation"
    );
}
