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
//! These tests reproduce the hazard by polling an append future exactly once —
//! enough to launch its detached commit task — and then dropping it, so the
//! committed batch's publish can only happen on that detached task. Under the
//! pre-fix code the stranded position wedges every later append; here they
//! must all still make progress, the store must stay dense and untorn, and a
//! same-stream retry must not double-write.
//!
//! `bn-3c6a`: whether one poll is enough for the append to *finish* is pure
//! scheduling, not a property of the engine, so in-flight-ness is a retried
//! *precondition* here, never an asserted outcome. [`drop_in_flight`] re-runs
//! the setup — fresh append, one poll, drop — folding each attempt that simply
//! succeeded into the expected head/event counts, until an attempt is provably
//! dropped while still `Pending`. The properties below are then asserted
//! exactly, parameterised on the attempt count.
#![cfg(not(miri))]

use std::fmt;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{LogEngine, Version};

fn rec(t: &str, d: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: t.to_string(), data: d.to_vec() }
}

/// How many setup attempts [`drop_in_flight`] makes before giving up. Each
/// attempt is one ordinary append, so the bound is cheap; exhausting it means
/// the hot path stopped parking at all, which is a behaviour change worth
/// failing loudly on rather than host-load noise worth retrying past.
const MAX_DROP_ATTEMPTS: usize = 50;

/// Poll `fut` exactly once and then drop it. Returns `None` iff it was still
/// `Pending` — i.e. it really was dropped in flight — and `Some(output)` when
/// the whole append instead resolved inside that single poll, in which case
/// nothing was cancelled and the caller must account for a plain successful
/// append.
///
/// For an `append_batch` to an already-interned ("primed") stream whose event
/// type is also already interned, one poll runs synchronously through the
/// admission reserve and the batch preparation — with no new-name persist
/// flush to await first — submits the unit to the owner, and parks awaiting
/// its completion: exactly the vulnerable point. Dropping now leaves that
/// submitted unit detached; it finishes committing (and, post-fix, publishing)
/// on its own. A no-op waker is fine: we never want it re-scheduled — we drive
/// exactly one poll and drop.
///
/// Reaching that park within poll 1 is *likely*, never guaranteed: under
/// favourable scheduling the commit and publish can already be done by the
/// time the poll looks, and the future returns `Ready`. Callers must therefore
/// go through [`drop_in_flight`] rather than assert on one attempt.
fn poll_once_then_drop<F: Future>(fut: F) -> Option<F::Output> {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut fut = pin!(fut);
    match fut.as_mut().poll(&mut cx) {
        Poll::Pending => None,
        Poll::Ready(output) => Some(output),
    }
}

/// Repeat the cancellation setup until it provably *is* a cancellation, and
/// return how many attempts that took (`>= 1`).
///
/// `mk_fut(completed)` must build a FRESH append future for the next attempt,
/// where `completed` counts the earlier attempts that resolved inside their
/// first poll. Those attempts were not cancellations at all: they appended
/// normally, so they advanced the head, published their events and counted
/// their metrics, and `mk_fut` has to expect the version they left behind.
///
/// On return, exactly one attempt — the last, numbered `completed + 1` — was
/// dropped while `Pending`, so the caller's cancellation property still holds
/// exactly once; every other effect is the `attempts - 1` ordinary appends
/// that preceded it, which callers fold into their expected counts.
fn drop_in_flight<Mk, Fut, T, E>(mut mk_fut: Mk) -> usize
where
    Mk: FnMut(usize) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: fmt::Debug,
{
    for completed in 0..MAX_DROP_ATTEMPTS {
        match poll_once_then_drop(mk_fut(completed)) {
            None => return completed + 1,
            // A completed attempt must be a clean append: anything else means
            // the retry is appending at the wrong expected version (or the
            // engine is rejecting it), and silently retrying would turn a real
            // failure into a confusing "never parked" panic below.
            Some(Ok(_)) => {}
            Some(Err(e)) => panic!(
                "setup attempt {} finished inside its first poll with an \
                 error instead of appending: {e:?}",
                completed + 1
            ),
        }
    }
    panic!(
        "append resolved inside its first poll on all {MAX_DROP_ATTEMPTS} \
         setup attempts — the hot path no longer parks, so there is no \
         in-flight window left to cancel (a real behaviour change, not \
         host-load noise)"
    )
}

/// Spin until `engine.total_events()` reaches `want`, or panic on timeout. Used
/// to reach quiescence after detached commit tasks: once the book holds every
/// expected event, no publish is in flight, so subsequent reads are stable.
/// How many USER events `read_global` currently delivers.
///
/// `bn-2di`: `total_events()` counts the LOG, which now includes the
/// `$registry` records the engine writes when it first sees a stream or type
/// name. This suite is about publish/cancellation of *user* appends, so it
/// counts what a user can actually see — a strictly tighter thing to assert,
/// since a stranded publish would show up here just the same.
async fn delivered(engine: &LogEngine) -> usize {
    engine
        .read_global(None, engine.total_events() + 1000)
        .await
        .expect("read_global")
        .len()
}

async fn await_total(engine: &LogEngine, want: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let got = delivered(engine).await;
        if got >= want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "store never reached {want} delivered events (got {got}) — a \
             stranded/stalled publish (bn-3nz)"
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
    // `bn-2di`: the delivered global order is strictly ascending and
    // duplicate-free, but not dense — `$registry` records consume positions and
    // are never delivered. A stranded or double publish (the bn-3nz bug this
    // suite exists for) still shows up as an out-of-order or repeated position.
    let mut prev: Option<u64> = None;
    for r in &all {
        if let Some(p) = prev {
            assert!(
                r.global_position > p,
                "global positions must be strictly ascending and unique: {} \
                 after {p}",
                r.global_position
            );
        }
        assert!(
            (r.global_position as usize) < total,
            "delivered position {} is past the log's own event count {total}",
            r.global_position
        );
        prev = Some(r.global_position);
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
    //
    // `bn-3c6a`: a setup attempt that instead finishes inside its first poll
    // is simply a successful two-event append, which advances that stream's
    // head by two — so the next attempt expects `At(2 * completed)`, and
    // `attempts[i]` records how many appends stream `i` received in total (the
    // last of those being the one actually dropped in flight). Every count
    // below is derived from that vector, so retrying costs exactness nothing.
    let batch = [rec("Ev", b"a"), rec("Ev", b"b")];
    let attempts: Vec<usize> = cancel_streams
        .iter()
        .map(|s| {
            drop_in_flight(|completed| {
                engine.append_batch(
                    s,
                    Version::At(2 * completed as u64),
                    &batch,
                )
            })
        })
        .collect();
    // Both the completed attempts and the dropped one publish two events.
    let cancel_events: usize = attempts.iter().sum::<usize>() * 2;

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

    // Reach quiescence: prime (CANCELS) + every cancel-stream append attempt
    // (two events each) + followers + barrier below. Every append eventually
    // publishes post-fix.
    let expected = CANCELS + cancel_events + FOLLOWERS + 1;
    engine
        .append_batch("barrier", Version::NoStream, &[rec("Barrier", b"z")])
        .await
        .expect("barrier append");
    await_total(&engine, expected).await;
    assert_eq!(
        delivered(&engine).await,
        expected,
        "no extra or missing events at quiescence"
    );

    // Consistency: dense global order, and no torn stream.
    let mut all_streams = cancel_streams.clone();
    all_streams.extend(follow_streams.clone());
    all_streams.push("barrier".to_string());
    assert_consistent(&engine, &all_streams).await;

    // The cancelled appends' events are already durable, so the only
    // consistent outcome is FULL visibility: each cancel-stream is at
    // `At(2 * attempts)` — primed at version 0, then two events for every
    // append attempt it took to land a dropped-in-flight one — never a partial
    // odd version, which is exactly what a torn publish would leave.
    for (s, attempts) in cancel_streams.iter().zip(&attempts) {
        let head = engine.head(s).await.expect("head");
        assert_eq!(
            head,
            Version::At(2 * *attempts as u64),
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

    // Drop an in-flight append at the stream's current head. Its detached
    // commit task will (post-fix) publish, advancing the head by one, while
    // still holding the per-stream gate until that publish completes. If the
    // gate were released by the dropped future BEFORE the publish, a racing
    // retry could pass its own check against the still-stale head and both
    // commits would claim the same stream version.
    //
    // `bn-3c6a`: an attempt that finishes inside its first poll cancelled
    // nothing — it appended one event and advanced the head — so the next
    // attempt is made at `At(completed)`, and the whole assertion body below
    // is parameterised on where the head ended up rather than on At(0)/At(1).
    let cancelled_batch = [rec("Ev", b"a")];
    let attempts = drop_in_flight(|completed| {
        engine.append_batch(
            "s",
            Version::At(completed as u64),
            &cancelled_batch,
        )
    });
    // The version the dropped append was submitted against (still the visible
    // head while its publish is detached), and the head its publish must
    // leave behind.
    let before_drop = Version::At(attempts as u64 - 1);
    let after_drop = Version::At(attempts as u64);

    // Retry the same logical write, same expected version as the dropped one.
    // Post-fix the gate serialises it strictly after the cancelled publish, so
    // it must observe `after_drop` and conflict — never win a second time at
    // the version the cancelled append already claimed.
    let result =
        engine.append_batch("s", before_drop, &[rec("Retry", b"b")]).await;
    match result {
        Err(mess_store::AppendError::Conflict { expected, actual }) => {
            assert_eq!(expected, before_drop);
            assert_eq!(
                actual, after_drop,
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

    // Stream `s` must have EXACTLY prime@0 + one event per setup attempt —
    // never two events sharing a version (a double-write from an
    // early-released gate).
    await_total(&engine, attempts + 1).await;
    let head = engine.head("s").await.expect("head");
    assert_eq!(
        head, after_drop,
        "head must be exactly one past the last completed setup append"
    );
    let page =
        engine.read_stream("s", Version::NoStream, 100).await.expect("read");
    assert_eq!(
        page.len(),
        attempts + 1,
        "exactly prime + one winner per attempt; no duplicate at \
         {after_drop:?}"
    );
    for (i, r) in page.iter().enumerate() {
        assert_eq!(r.stream_position, i as u64, "stream positions dense");
    }
    // Global order: strictly ascending, no duplicate (bn-2di — not dense, since
    // `$registry` records take positions but are never delivered; a double
    // publish would still repeat a position).
    let all = engine.read_global(None, 100).await.expect("read_global");
    let mut prev: Option<u64> = None;
    for r in &all {
        if let Some(p) = prev {
            assert!(
                r.global_position > p,
                "global order must be strictly ascending: {} after {p}",
                r.global_position
            );
        }
        prev = Some(r.global_position);
    }
}
