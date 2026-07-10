//! bn-1vu — live segment auto-roll + background seal under a live committer.
//!
//! The engine's committer owns one segment and, when it fills, rolls to a fresh
//! segment IN PLACE (continuing the A1/epoch chain, preserving `segment_size`)
//! without losing or reordering appends. Each rolled, still-unsealed segment is
//! handed to a background sealer that builds the pointer + payload sidecars and
//! finalizes the footer OFF the append path (D5). These tests pin: (1) rolls
//! happen under concurrent load and every event stays readable, contiguous, and
//! duplicate-free; (2) a rolled segment whose sidecar never materialised (crash
//! mid-seal) is still served from the durable log after reopen; (3) a clean
//! reopen rehydrates the whole multi-segment chain and keeps appending densely.

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};

fn rec(message_type: &str, data: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: message_type.to_string(), data: data.to_vec() }
}

/// Options that force rolls quickly: a tiny active segment so a few hundred
/// small batches span many segments.
fn rolling_opts() -> EngineOptions {
    EngineOptions { segment_size: 16 * 1024, ..EngineOptions::default() }
}

/// Deterministic payload for stream `s`, event `i` — lets a read verify no
/// reorder / duplication by exact bytes.
fn payload(s: usize, i: usize) -> Vec<u8> {
    format!("s{s:03}-e{i:04}").into_bytes()
}

/// Append `events_per` single-event batches to `stream`, awaiting each so the
/// stream's version chain is exact.
async fn fill_stream(engine: &LogEngine, stream: &str, s: usize, events_per: usize) {
    let mut expected = Version::NoStream;
    for i in 0..events_per {
        let out = engine
            .append_batch(stream, expected, &[rec("ev", &payload(s, i))])
            .await
            .expect("append");
        expected = out.version;
    }
}

/// bn-1vu (item 1 + item 4): auto-roll under concurrent load. Many streams
/// append concurrently against a tiny segment, forcing repeated rolls under a
/// live committer. Afterwards every event is readable via all three read paths,
/// global positions tile `[0, N)` densely (no gaps, no duplicates, no reorder),
/// and every stream's versions are contiguous with byte-exact payloads.
// bn-u6o item 4: multi_thread so the concurrent appenders genuinely run on
// separate OS threads — a single-threaded flavor cooperatively interleaves
// tasks at `.await` points only, which cannot exercise the API-side races
// (distinct tokio worker threads racing the append gate / publish sequencer /
// committer concurrently) this test's docstring claims to cover.
#[tokio::test(flavor = "multi_thread")]
async fn auto_roll_under_concurrent_load_preserves_every_event() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_path = dir.path().join("store");
    let engine = LogEngine::open_with(&store_path, rolling_opts()).expect("open");

    const STREAMS: usize = 24;
    const PER: usize = 25; // 600 tiny batches over a 16 KiB segment ⇒ many rolls

    // Concurrent appenders, one stream each (distinct streams never conflict on
    // the exact-version gate; the API-level concurrency still interleaves their
    // batches through the committer and across roll boundaries).
    let mut joins = Vec::new();
    for s in 0..STREAMS {
        let engine = engine.clone();
        joins.push(tokio::spawn(async move {
            fill_stream(&engine, &format!("stream-{s}"), s, PER).await;
        }));
    }
    for j in joins {
        j.await.expect("appender task");
    }

    let total = STREAMS * PER;
    assert_eq!(engine.total_events(), total, "every event committed to the book");

    // read_global tiles [0, total) densely with unique positions (no dup/gap).
    let g = engine.read_global(None, total * 2).await.unwrap();
    assert_eq!(g.len(), total, "global read returns exactly every event");
    for (i, r) in g.iter().enumerate() {
        assert_eq!(r.global_position, i as u64, "dense, in-order global positions");
    }

    // Per-stream: contiguous versions, byte-exact payloads, correct head.
    for s in 0..STREAMS {
        let name = format!("stream-{s}");
        let evs = engine.read_stream(&name, Version::NoStream, total).await.unwrap();
        assert_eq!(evs.len(), PER, "stream {s}: all events readable after rolls");
        for (i, e) in evs.iter().enumerate() {
            assert_eq!(e.stream_position, i as u64, "stream {s}: contiguous versions");
            assert_eq!(e.data, payload(s, i), "stream {s} event {i}: byte-exact, no reorder");
            assert_eq!(e.stream_id, name);
        }
        assert_eq!(
            engine.head(&name).await.unwrap(),
            Version::At((PER - 1) as u64),
            "stream {s}: head after rolls"
        );
    }

    // Rolls actually happened: after a clean drop (which drains the background
    // sealer) the reopened engine finds durable sealed sidecars.
    drop(engine);
    let reopened = LogEngine::open_with(&store_path, rolling_opts()).expect("reopen");
    assert!(
        reopened.sealed_segment_count() > 0,
        "the active segment must have rolled and sealed at least once under load"
    );
    assert_eq!(reopened.total_events(), total, "reopen rehydrates every event across the chain");
}

/// bn-1vu (item 2 + item 3): crash mid-seal leaves a recoverable state. Force
/// rolls, drop the engine, then DELETE every sealed sidecar under `dir/sealed`
/// — the shape of a crash that rolled segments but never finished (or lost)
/// their sidecars. On reopen those segments are NOT in the cold tier, so their
/// batches are re-seeded into the hot index and served from the durable log:
/// every event comes back byte-exact, none lost.
#[tokio::test]
async fn crash_mid_roll_seal_is_served_from_log_after_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_path = dir.path().join("store");

    const STREAMS: usize = 12;
    const PER: usize = 25;
    let total = STREAMS * PER;

    {
        let engine = LogEngine::open_with(&store_path, rolling_opts()).expect("open");
        for s in 0..STREAMS {
            fill_stream(&engine, &format!("stream-{s}"), s, PER).await;
        }
        // Drop drains the sealer so sidecars + footers are durable...
    }

    // ...then simulate the sidecars never having materialised (crash mid-seal):
    // remove every sealed-tier artifact. The segment `.log` files stay (some
    // with footers, some unsealed) — the durable log is the authority.
    let sealed_dir = store_path.join("sealed");
    let mut removed = 0;
    for entry in std::fs::read_dir(&sealed_dir).expect("sealed dir").flatten() {
        std::fs::remove_file(entry.path()).expect("remove sidecar");
        removed += 1;
    }
    assert!(removed > 0, "rolls must have produced sealed sidecars to remove");

    // Reopen: no cold tier, everything recovered from the log.
    let engine = LogEngine::open_with(&store_path, rolling_opts()).expect("reopen after crash");
    assert_eq!(engine.sealed_segment_count(), 0, "no sidecars ⇒ nothing served cold");
    assert_eq!(engine.total_events(), total, "every event recovered from the durable log");

    let g = engine.read_global(None, total * 2).await.unwrap();
    assert_eq!(g.len(), total, "global read complete after crash mid-seal");
    for (i, r) in g.iter().enumerate() {
        assert_eq!(r.global_position, i as u64, "dense positions after log recovery");
    }
    for s in 0..STREAMS {
        let name = format!("stream-{s}");
        let evs = engine.read_stream(&name, Version::NoStream, total).await.unwrap();
        assert_eq!(evs.len(), PER, "stream {s}: fully served from the log");
        for (i, e) in evs.iter().enumerate() {
            assert_eq!(e.data, payload(s, i), "stream {s} event {i}: byte-exact from the log");
        }
    }

    // And it keeps appending densely on top of the recovered chain.
    let out = engine
        .append_batch("stream-0", Version::At((PER - 1) as u64), &[rec("ev", b"resumed")])
        .await
        .expect("append after crash recovery");
    assert_eq!(out.last_global_position, total as u64, "next append continues the dense order");
}

/// bn-1vu (item 3): a clean reopen after rolls rehydrates the whole segment
/// chain — sealed segments served cold (sidecars reloaded), the live head hot —
/// and every read path returns the exact pre-restart data, then keeps appending.
#[tokio::test]
async fn clean_reopen_after_rolls_serves_cold_and_hot_tiers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_path = dir.path().join("store");

    const STREAMS: usize = 16;
    const PER: usize = 25;
    let total = STREAMS * PER;

    {
        let engine = LogEngine::open_with(&store_path, rolling_opts()).expect("open");
        for s in 0..STREAMS {
            fill_stream(&engine, &format!("stream-{s}"), s, PER).await;
        }
    }

    let engine = LogEngine::open_with(&store_path, rolling_opts()).expect("reopen");
    assert!(engine.sealed_segment_count() > 0, "rolled segments reload into the cold tier");
    assert_eq!(engine.total_events(), total, "book dense across the whole reopened chain");

    let g = engine.read_global(None, total * 2).await.unwrap();
    assert_eq!(g.len(), total);
    for (i, r) in g.iter().enumerate() {
        assert_eq!(r.global_position, i as u64, "dense global order across cold+hot tiers");
    }
    for s in 0..STREAMS {
        let name = format!("stream-{s}");
        let evs = engine.read_stream(&name, Version::NoStream, total).await.unwrap();
        assert_eq!(evs.len(), PER, "stream {s}: all events across cold/hot after reopen");
        for (i, e) in evs.iter().enumerate() {
            assert_eq!(e.stream_position, i as u64);
            assert_eq!(e.data, payload(s, i), "stream {s} event {i}: byte-exact after reopen");
        }
        assert_eq!(engine.head(&name).await.unwrap(), Version::At((PER - 1) as u64));
    }

    // Keep appending on the reopened, rolled store — positions stay dense and a
    // stale expected version still conflicts correctly.
    let conflict = engine
        .append_batch("stream-0", Version::At(0), &[rec("ev", b"stale")])
        .await;
    assert!(conflict.is_err(), "exact-version gate still enforced after reopen");
    let out = engine
        .append_batch("stream-0", Version::At((PER - 1) as u64), &[rec("ev", b"more")])
        .await
        .expect("append after reopen");
    assert_eq!(out.last_global_position, total as u64, "dense append continues after reopen");
}

/// bn-u6o item 2: a batch bigger than a whole EMPTY segment can never fit no
/// matter how many times the committer rolls. Before this bone, the
/// committer's `SegmentFull` branch always rolled first and retried, so an
/// oversized batch wasted a near-empty segment (and a background-sealer
/// notification for it) before re-failing with the identical typed error —
/// and a caller that retried the same oversized batch would proliferate one
/// such wasted segment per attempt. The fix checks the batch against the
/// capacity of an EMPTY segment before rolling at all: the typed error comes
/// back immediately, segment count unchanged.
#[tokio::test]
async fn oversized_batch_fails_fast_without_wasting_a_roll() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_path = dir.path().join("store");
    // A tiny segment: no realistic batch fits, let alone this test's 2000-byte
    // oversized one.
    let opts = EngineOptions { segment_size: 512, ..EngineOptions::default() };
    let engine = LogEngine::open_with(&store_path, opts).expect("open");

    let segment_count = || {
        std::fs::read_dir(&store_path)
            .expect("read store dir")
            .flatten()
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("log"))
            .count()
    };
    assert_eq!(segment_count(), 1, "one active segment at open");

    // A single event whose payload alone (2000 bytes) is well past the whole
    // 512-byte segment_size — cannot fit no matter how many times it rolls.
    let big = rec("Big", &vec![0xABu8; 2000]);
    let err = engine
        .append_batch("stream-oversized", Version::NoStream, &[big])
        .await
        .expect_err("an oversized batch must fail, not succeed");
    let msg = err.to_string();
    assert!(
        msg.contains("segment full"),
        "must surface the typed SegmentFull error, got: {msg}"
    );

    assert_eq!(segment_count(), 1, "no wasted roll: segment count must be unchanged");
    assert_eq!(engine.total_events(), 0, "the failed oversized append committed nothing");

    // The store must still be usable afterwards — the failed pre-check must
    // not have poisoned or otherwise wedged the committer.
    let out = engine
        .append_batch("stream-ok", Version::NoStream, &[rec("ev", b"small")])
        .await
        .expect("a normal-sized append after the rejected oversized one must still work");
    assert_eq!(out.last_global_position, 0);
}
