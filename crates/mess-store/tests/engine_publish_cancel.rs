//! bn-3nz: a dropped/cancelled `append_batch` future must never gap the
//! global position sequence.
//!
//! The durable committer assigns each accepted batch a permanent, dense
//! global position range. Before this bone the engine assigned that range on
//! a `spawn_blocking` task but then performed the position-ordered publish
//! (record book + active index + meta head, gated by `PublishSequencer`) back
//! on the *async caller's future*, awaiting the append and then the sequencer
//! turn as two separate cancellation points. A caller that dropped its append
//! future between those points — e.g. a `tokio::select!`/timeout that fires
//! mid-commit — left a committed-but-never-published position, and
//! `PublishSequencer::turn`'s strict `== next` wait then stalled every
//! higher-positioned publish *forever*: one cancelled future permanently
//! wedged the whole store.
//!
//! The fix moves the assign → turn → publish sequence entirely inside the
//! single non-cancellable `spawn_blocking` task (a `spawn_blocking` closure
//! always runs to completion even if its `JoinHandle` is dropped), and carries
//! the per-stream gate as an owned guard into that task. So a cancelled append
//! still fully publishes its (already-durable) events and advances the
//! sequencer; it can neither strand a slot nor tear a publish, nor release the
//! per-stream gate early.
//!
//! These tests reproduce the hazard deterministically by polling an append
//! future exactly once — enough to launch its detached commit task — and then
//! dropping it, so the committed batch's publish can only happen on that
//! detached blocking task. Under the pre-fix code the stranded position wedges
//! every later append; here they must all still make progress, the store must
//! stay dense and untorn, and a same-stream retry must not double-write.
#![cfg(not(miri))]

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{LogEngine, Version};

fn rec(t: &str, d: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: t.to_string(), data: d.to_vec() }
}

/// Poll `fut` exactly once and then drop it, returning `true` iff it was still
/// `Pending` (i.e. it really was dropped in flight, not already finished).
///
/// For an `append_batch` to an already-interned ("primed") stream whose event
/// type is also already interned, one poll runs synchronously through the
/// (uncontended) gate acquire and the version pre-check — with no new-name
/// persist flush to await first — launches the durable-commit `spawn_blocking`
/// task, and parks awaiting its `JoinHandle`: exactly the vulnerable point.
/// Dropping now leaves that commit task detached; it finishes committing (and,
/// post-fix, publishing) on its own. A no-op waker is fine: we never want it
/// re-scheduled — we drive exactly one poll and drop.
fn poll_once_then_drop<F: Future>(fut: F) -> bool {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut fut = pin!(fut);
    matches!(fut.as_mut().poll(&mut cx), Poll::Pending)
}

/// Spin until `engine.total_events()` reaches `want`, or panic on timeout. Used
/// to reach quiescence after detached commit tasks: once the book holds every
/// expected event, no publish is in flight, so subsequent reads are stable.
async fn await_total(engine: &LogEngine, want: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if engine.total_events() >= want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "store never reached {want} events (got {}) — a stranded/stalled \
             publish (bn-3nz)",
            engine.total_events()
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// Assert the whole store is dense and internally consistent: global order has
/// no gaps, and each stream's `read_stream` agrees with its `head`. Call only
/// at quiescence (no append in flight).
async fn assert_consistent(engine: &LogEngine, streams: &[String]) {
    let total = engine.total_events();
    let all =
        engine.read_global(None, total + 1000).await.expect("read_global");
    assert_eq!(all.len(), total, "read_global must expose every booked event");
    for (i, r) in all.iter().enumerate() {
        assert_eq!(
            r.global_position, i as u64,
            "global positions must be dense 0..N"
        );
    }
    for s in streams {
        let head = engine.head(s).await.expect("head");
        let page = engine
            .read_stream(s, Version::NoStream, total + 1000)
            .await
            .expect("read");
        match head {
            Version::NoStream => {
                assert!(page.is_empty(), "NoStream head must read empty")
            }
            Version::At(v) => {
                assert_eq!(
                    page.len() as u64,
                    v + 1,
                    "stream {s} head At({v}) must expose exactly v+1 events \
                     (no torn publish)"
                );
                for (i, r) in page.iter().enumerate() {
                    assert_eq!(
                        r.stream_position, i as u64,
                        "stream positions dense"
                    );
                }
            }
        }
    }
}

/// The core regression: drop several in-flight appends (after their commit
/// tasks are launched), then prove a large batch of subsequent appends across
/// many streams still all publish — no permanent stall — and the store stays
/// dense and untorn, with the cancelled batches fully (never partially)
/// visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_append_future_does_not_gap_the_position_sequence() {
    let dir = mess_testkit::sweeping_temp_dir(
        "pub-cancel-dropped-append-future-does",
    );
    let engine = LogEngine::open(dir.path()).expect("open");

    // Prime the streams we will cancel appends on, using the SAME event type
    // the cancelled appends use, so both the stream name AND the type are
    // already interned: one poll then reaches the durable-commit await rather
    // than parking on a new-name persist flush first.
    const CANCELS: usize = 6;
    let cancel_streams: Vec<String> =
        (0..CANCELS).map(|i| format!("cancel-{i}")).collect();
    for s in &cancel_streams {
        engine
            .append_batch(s, Version::NoStream, &[rec("Ev", b"seed")])
            .await
            .expect("prime");
    }

    // Drop an in-flight append on each primed cancel-stream. Each one, once
    // polled, has launched a detached commit task holding a global position;
    // pre-fix, none of them ever publish, and the first later append that
    // lands above them wedges forever. Two events per batch, so a torn publish
    // (some but not all events visible) would be detectable.
    let mut dropped_in_flight = 0usize;
    for s in &cancel_streams {
        let batch = [rec("Ev", b"a"), rec("Ev", b"b")];
        if poll_once_then_drop(engine.append_batch(s, Version::At(0), &batch)) {
            dropped_in_flight += 1;
        }
    }
    assert_eq!(
        dropped_in_flight, CANCELS,
        "every cancel-stream append should have been dropped while in flight"
    );

    // Hammer the store with many appends to fresh streams. Every one lands at
    // a global position ABOVE the stranded cancelled ones, so pre-fix at least
    // one waits on the sequencer forever. Wrapped in a timeout: a stall shows
    // up as this timing out, not as a hang.
    const FOLLOWERS: usize = 40;
    let follow_streams: Vec<String> =
        (0..FOLLOWERS).map(|i| format!("follow-{i}")).collect();
    let progressed = tokio::time::timeout(Duration::from_secs(30), async {
        let mut handles = Vec::new();
        for s in follow_streams.clone() {
            let engine = engine.clone();
            handles.push(tokio::spawn(async move {
                engine
                    .append_batch(&s, Version::NoStream, &[rec("Follow", b"x")])
                    .await
                    .expect("follower append")
            }));
        }
        for h in handles {
            h.await.expect("follower task panicked");
        }
    })
    .await;
    assert!(
        progressed.is_ok(),
        "subsequent appends stalled after dropped in-flight appends — a \
         gapped position sequence (the bn-3nz bug)"
    );

    // Reach quiescence: prime (CANCELS) + cancelled (CANCELS*2) + followers +
    // barrier below. Every append eventually publishes post-fix.
    let expected = CANCELS + CANCELS * 2 + FOLLOWERS + 1;
    engine
        .append_batch("barrier", Version::NoStream, &[rec("Barrier", b"z")])
        .await
        .expect("barrier append");
    await_total(&engine, expected).await;
    assert_eq!(
        engine.total_events(),
        expected,
        "no extra or missing events at quiescence"
    );

    // Consistency: dense global order, and no torn stream.
    let mut all_streams = cancel_streams.clone();
    all_streams.extend(follow_streams.clone());
    all_streams.push("barrier".to_string());
    assert_consistent(&engine, &all_streams).await;

    // The cancelled appends' events are already durable, so the only
    // consistent outcome is FULL visibility: each cancel-stream is at At(2)
    // (primed + the two cancelled events), never a partial At(1).
    for s in &cancel_streams {
        let head = engine.head(s).await.expect("head");
        assert_eq!(
            head,
            Version::At(2),
            "cancelled append must publish fully (untorn) post-fix"
        );
    }
}

/// A cancelled append followed by a same-stream retry must not double-write
/// the stream version: the per-stream gate has to stay held across the
/// (detached) commit+publish, not be released early by the dropped future.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_then_same_stream_retry_never_double_writes_version() {
    let dir =
        mess_testkit::sweeping_temp_dir("pub-cancel-cancel-then-same-stream");
    let engine = LogEngine::open(dir.path()).expect("open");

    // Prime stream + event type so the cancelled append is pure hot-path.
    engine
        .append_batch("s", Version::NoStream, &[rec("Ev", b"seed")])
        .await
        .expect("prime");

    // Drop an in-flight append at Version::At(0). Its detached commit task will
    // (post-fix) publish, advancing the head to At(1), while still holding the
    // per-stream gate until that publish completes. If the gate were released
    // by the dropped future BEFORE the publish, a racing retry could pass its
    // own At(0) check against the still-stale head and both commits would claim
    // stream version 1.
    let dropped = poll_once_then_drop(engine.append_batch(
        "s",
        Version::At(0),
        &[rec("Ev", b"a")],
    ));
    assert!(dropped, "append must be dropped in flight");

    // Retry the same logical write, same expected version. Post-fix the gate
    // serialises it strictly after the cancelled publish, so it must observe
    // the head at At(1) and conflict — never win a second time at version 1.
    let result =
        engine.append_batch("s", Version::At(0), &[rec("Retry", b"b")]).await;
    match result {
        Err(mess_store::AppendError::Conflict { expected, actual }) => {
            assert_eq!(expected, Version::At(0));
            assert_eq!(
                actual,
                Version::At(1),
                "retry must see the cancelled publish's head"
            );
        }
        Ok(a) => panic!(
            "retry unexpectedly won at {:?} — the cancelled append \
             double-wrote",
            a.version
        ),
        Err(other) => panic!("unexpected error: {other:?}"),
    }

    // Stream `s` must have EXACTLY prime@0 + one winner@1 — never two events
    // sharing version 1 (a double-write from an early-released gate).
    await_total(&engine, 2).await;
    let head = engine.head("s").await.expect("head");
    assert_eq!(head, Version::At(1), "head must be exactly one past the prime");
    let page =
        engine.read_stream("s", Version::NoStream, 100).await.expect("read");
    assert_eq!(
        page.len(),
        2,
        "exactly prime + one winner; no duplicate at version 1"
    );
    assert_eq!(page[0].stream_position, 0);
    assert_eq!(page[1].stream_position, 1);
    // Global order dense across the whole store.
    let all = engine.read_global(None, 100).await.expect("read_global");
    for (i, r) in all.iter().enumerate() {
        assert_eq!(r.global_position, i as u64, "dense global order");
    }
}
