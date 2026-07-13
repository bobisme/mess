//! Integration tests for the app-facing subscription / live-tail API
//! (`EventStore::subscribe`, `watermark`, `await_past`) over the real composed
//! [`LogEngine`] — the durable committer + record book + `mess-log` watermark.
//!
//! THE point of this bone: a read model no longer hand-rolls a 1ms polling
//! adapter over `read_global`. So these tests deliberately contain **no polling
//! sleeps in the consumer logic** — the subscriber blocks on the watermark and
//! is woken by commits.
#![cfg(not(miri))]

use std::time::Duration;

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EventStore, LogEngine, Version};

fn rec(t: &str, d: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: t.to_string(), data: d.to_vec() }
}

/// `bn-2di`: the `$registry` records the very first append writes — one naming
/// the stream, one naming the event type `"E"` — and nothing after that. They
/// are ordinary log events on the reserved stream 0: they CONSUME the first two
/// global positions (so the watermark counts them) but are never DELIVERED
/// (stream 0 is filtered out of every user-facing read). So a subscriber's
/// first record sits at global position `REG`, and the delivered sequence is
/// gap-free from there.
const REG: u64 = 2;

/// Append `n` single-event batches to one stream, one per stream version,
/// starting at stream version `from` (== the current head's next position).
async fn append_n(engine: &LogEngine, stream: &str, from: u64, n: u64) {
    for v in from..from + n {
        let expected =
            if v == 0 { Version::NoStream } else { Version::At(v - 1) };
        engine
            .append_batch(stream, expected, &[rec("E", &v.to_le_bytes())])
            .await
            .expect("append");
    }
}

// ---------------------------------------------------------------------------
// (a) subscribe from 0: replay everything, then go live and see a concurrent
//     writer's commits — without any polling sleep in the consumer.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_from_zero_replays_then_tails_live() {
    let dir = mess_testkit::sweeping_temp_dir(
        "subscription-subscribe-from-zero-replays",
    );
    let engine = LogEngine::open(dir.path()).expect("open");
    let store = EventStore::new(engine.clone());

    const PRE: u64 = 40;
    const LIVE: u64 = 60;
    const TOTAL: u64 = PRE + LIVE;

    // Pre-populate committed history before anyone subscribes.
    append_n(&engine, "s", 0, PRE).await;
    assert_eq!(store.watermark().await.unwrap(), PRE + REG);

    // Consumer: drain exactly TOTAL positions. It only ever blocks on
    // `next()` (which parks on the watermark) — no sleeps here.
    let sub_store = store.clone();
    let consumer = tokio::spawn(async move {
        let mut sub = sub_store.subscribe(Some(0));
        let mut got = Vec::with_capacity(TOTAL as usize);
        while (got.len() as u64) < TOTAL {
            let r = sub.next().await.expect("next");
            got.push(r.global_position);
        }
        got
    });

    // Concurrent writer: commit LIVE more events while the consumer tails.
    let writer = tokio::spawn(async move {
        append_n(&engine, "s", PRE, LIVE).await;
    });
    writer.await.unwrap();

    let got = consumer.await.unwrap();
    let expected: Vec<u64> = (REG..REG + TOTAL).collect();
    assert_eq!(
        got, expected,
        "gap-free, in-order, exactly-once global delivery"
    );
}

// ---------------------------------------------------------------------------
// (b) await_past wakes promptly once a commit passes the position.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn await_past_wakes_after_commit_passes_position() {
    let dir =
        mess_testkit::sweeping_temp_dir("subscription-await-past-wakes-after");
    let engine = LogEngine::open(dir.path()).expect("open");
    let store = EventStore::new(engine.clone());

    // Seed 5 events (globals 2..7 behind the two registrations, watermark 7).
    append_n(&engine, "s", 0, 5).await;
    assert_eq!(store.watermark().await.unwrap(), 5 + REG);

    // Already-past positions resolve immediately.
    store.await_past(4).await.unwrap(); // watermark 7 > 4

    // Park a waiter on position 9 (needs watermark > 9, i.e. >= 10).
    let waiter_store = store.clone();
    let waiter = tokio::spawn(async move {
        waiter_store.await_past(9).await.unwrap();
    });

    // Commit past position 9 (5 more events -> watermark 12). The waiter must
    // wake; bound it so a wedged notification fails instead of hanging.
    append_n(&engine, "s", 5, 5).await;
    assert_eq!(store.watermark().await.unwrap(), 10 + REG);

    tokio::time::timeout(Duration::from_secs(10), waiter)
        .await
        .expect("await_past did not wake after the commit passed the position")
        .expect("waiter task");
}

// ---------------------------------------------------------------------------
// (c) subscribe from a mid-log position replays only the suffix.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_from_midlog_replays_only_the_suffix() {
    let dir = mess_testkit::sweeping_temp_dir(
        "subscription-subscribe-from-midlog-replays",
    );
    let engine = LogEngine::open(dir.path()).expect("open");
    let store = EventStore::new(engine.clone());

    const PRE: u64 = 30;
    /// A mid-log GLOBAL position (the events occupy `REG..REG + PRE`).
    const FROM: u64 = 10;
    const SUFFIX: u64 = REG + PRE - FROM;
    append_n(&engine, "s", 0, PRE).await;

    let mut sub = store.subscribe(Some(FROM));

    // First batch must begin exactly at FROM, never earlier.
    let first = sub.next_batch().await.unwrap();
    assert_eq!(
        first[0].global_position, FROM,
        "suffix starts at the requested cursor"
    );

    // Drain the rest of the suffix [FROM, REG + PRE). No writer, so once we
    // have the
    // suffix the next call would block — we stop exactly at the suffix length.
    let mut got: Vec<u64> = first.iter().map(|r| r.global_position).collect();
    while (got.len() as u64) < SUFFIX {
        let batch = sub.next_batch().await.unwrap();
        got.extend(batch.iter().map(|r| r.global_position));
    }
    let expected: Vec<u64> = (FROM..REG + PRE).collect();
    assert_eq!(
        got, expected,
        "replays exactly the suffix, nothing before FROM"
    );
}

// ---------------------------------------------------------------------------
// (d) dropping a subscription (even one parked on the live tail) does not wedge
//     the committer: subsequent appends still commit and advance the watermark.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_a_subscription_does_not_wedge_the_committer() {
    let dir = mess_testkit::sweeping_temp_dir(
        "subscription-dropping-a-subscription-does",
    );
    let engine = LogEngine::open(dir.path()).expect("open");
    let store = EventStore::new(engine.clone());

    append_n(&engine, "s", 0, 5).await;

    // Spawn several subscribers that catch up and then park on the live tail
    // (blocked in `await_watermark_past`), then abort them — dropping their
    // parked watermark waiters mid-flight.
    let mut handles = Vec::new();
    for _ in 0..8 {
        let s = store.clone();
        handles.push(tokio::spawn(async move {
            let mut sub = s.subscribe(Some(0));
            loop {
                // Drains 0..5 then blocks forever on the watermark.
                let _ = sub.next().await;
            }
        }));
    }
    // Also drop a subscription synchronously without ever polling it to end.
    {
        let _sub = store.subscribe(None);
    }
    // Let the spawned subscribers reach and park on the watermark, then abort.
    tokio::task::yield_now().await;
    for h in &handles {
        h.abort();
    }

    // The committer must be perfectly healthy: 20 more appends all commit and
    // the watermark advances to cover them. Bound it so a wedge fails loudly.
    tokio::time::timeout(
        Duration::from_secs(20),
        append_n(&engine, "s", 5, 20),
    )
    .await
    .expect("appends wedged after subscriptions were dropped");
    assert_eq!(store.watermark().await.unwrap(), 25 + REG);

    // And a fresh subscription still delivers the whole (gap-free) log.
    let mut sub = store.subscribe(Some(0));
    let mut got = Vec::new();
    while (got.len() as u64) < 25 {
        got.push(sub.next().await.unwrap().global_position);
    }
    assert_eq!(got, (REG..REG + 25).collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// Extra: the MockBackend supports the same API (generic over SubscribeBackend).
// ---------------------------------------------------------------------------

#[cfg(feature = "mock")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_backend_subscribe_catches_up_and_tails() {
    use mess_store::MockBackend;

    let backend = MockBackend::new();
    let store = EventStore::new(backend.clone());

    // Pre-populate via the backend seam directly (no typed events needed).
    for v in 0..10u64 {
        let expected =
            if v == 0 { Version::NoStream } else { Version::At(v - 1) };
        backend
            .append_batch("s", expected, &[rec("E", &v.to_le_bytes())])
            .await
            .unwrap();
    }
    assert_eq!(store.watermark().await.unwrap(), 10);

    let sub_store = store.clone();
    let consumer = tokio::spawn(async move {
        let mut sub = sub_store.subscribe(Some(0));
        let mut got = Vec::new();
        while (got.len() as u64) < 25 {
            got.push(sub.next().await.unwrap().global_position);
        }
        got
    });

    for v in 10..25u64 {
        backend
            .append_batch(
                "s",
                Version::At(v - 1),
                &[rec("E", &v.to_le_bytes())],
            )
            .await
            .unwrap();
    }

    let got = consumer.await.unwrap();
    assert_eq!(got, (0..25).collect::<Vec<_>>());
}
