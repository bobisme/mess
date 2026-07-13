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
    RecordToAppend {
        message_type: message_type.to_string(),
        data:         data.to_vec(),
    }
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
async fn fill_stream(
    engine: &LogEngine,
    stream: &str,
    s: usize,
    events_per: usize,
) {
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
/// `bn-2di`: how many events the LOG holds for a corpus of `user_events` user
/// events across `streams` streams, each using `types_per_stream` event types.
///
/// The log-derived `$registry` writes one record the first time it sees a
/// stream name and one the first time it sees an event-type name. Those are
/// real, committed log events — they consume global positions like anything
/// else — so `LogEngine::total_events()` (which counts the LOG) exceeds the
/// user-event count by exactly the number of names ever registered.
/// `read_global` never delivers them, so the DELIVERED count is still exactly
/// the user-event count.
fn log_events(user_events: usize, registrations: usize) -> usize {
    user_events + registrations
}

/// Every `fill_stream` corpus uses ONE event type (`"ev"`) across all its
/// streams, so it registers `streams + 1` names in total.
fn registrations(streams: usize) -> usize { streams + 1 }

/// Assert a `read_global` page is the user corpus, in order: strictly
/// ascending, duplicate-free positions (bn-2di — NOT dense, since `$registry`
/// records take positions and are never delivered; a gap or a dupe still
/// fails).
fn assert_ascending(g: &[mess_store::backend::StoredRecord]) {
    let mut prev: Option<u64> = None;
    for r in g {
        if let Some(p) = prev {
            assert!(
                r.global_position > p,
                "global positions must be strictly ascending and unique: {} \
                 after {p}",
                r.global_position
            );
        }
        prev = Some(r.global_position);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_roll_under_concurrent_load_preserves_every_event() {
    let dir = mess_testkit::sweeping_temp_dir(
        "engine-roll-auto-roll-under-concurrent",
    );
    let store_path = dir.path().join("store");
    let engine =
        LogEngine::open_with(&store_path, rolling_opts()).expect("open");

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
    assert_eq!(
        engine.total_events(),
        log_events(total, registrations(STREAMS)),
        "every event committed to the log (user events + $registry records)"
    );

    // read_global delivers exactly the user corpus, in ascending order with no
    // duplicate (bn-2di: not dense — `$registry` records take positions between
    // them and are never delivered).
    let g = engine.read_global(None, total * 2).await.unwrap();
    assert_eq!(g.len(), total, "global read returns exactly every user event");
    assert_ascending(&g);

    // Per-stream: contiguous versions, byte-exact payloads, correct head.
    for s in 0..STREAMS {
        let name = format!("stream-{s}");
        let evs =
            engine.read_stream(&name, Version::NoStream, total).await.unwrap();
        assert_eq!(
            evs.len(),
            PER,
            "stream {s}: all events readable after rolls"
        );
        for (i, e) in evs.iter().enumerate() {
            assert_eq!(
                e.stream_position, i as u64,
                "stream {s}: contiguous versions"
            );
            assert_eq!(
                e.data,
                payload(s, i),
                "stream {s} event {i}: byte-exact, no reorder"
            );
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
    let reopened =
        LogEngine::open_with(&store_path, rolling_opts()).expect("reopen");
    assert!(
        reopened.sealed_segment_count() > 0,
        "the active segment must have rolled and sealed at least once under \
         load"
    );
    assert_eq!(
        reopened.total_events(),
        log_events(total, registrations(STREAMS)),
        "reopen rehydrates every event across the chain"
    );
}

/// bn-1vu (item 2 + item 3): crash mid-seal leaves a recoverable state. Force
/// rolls, drop the engine, then DELETE every sealed sidecar under `dir/sealed`
/// — the shape of a crash that rolled segments but never finished (or lost)
/// their sidecars. On reopen those segments are NOT in the cold tier, so their
/// batches are re-seeded into the hot index and served from the durable log:
/// every event comes back byte-exact, none lost.
#[tokio::test]
async fn crash_mid_roll_seal_is_served_from_log_after_reopen() {
    let dir =
        mess_testkit::sweeping_temp_dir("engine-roll-crash-mid-roll-seal");
    let store_path = dir.path().join("store");

    const STREAMS: usize = 12;
    const PER: usize = 25;
    let total = STREAMS * PER;

    {
        let engine =
            LogEngine::open_with(&store_path, rolling_opts()).expect("open");
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
    let engine = LogEngine::open_with(&store_path, rolling_opts())
        .expect("reopen after crash");
    assert_eq!(
        engine.sealed_segment_count(),
        0,
        "no sidecars ⇒ nothing served cold"
    );
    assert_eq!(
        engine.total_events(),
        log_events(total, registrations(STREAMS)),
        "every event recovered from the durable log"
    );

    let g = engine.read_global(None, total * 2).await.unwrap();
    assert_eq!(g.len(), total, "global read complete after crash mid-seal");
    assert_ascending(&g);
    for s in 0..STREAMS {
        let name = format!("stream-{s}");
        let evs =
            engine.read_stream(&name, Version::NoStream, total).await.unwrap();
        assert_eq!(evs.len(), PER, "stream {s}: fully served from the log");
        for (i, e) in evs.iter().enumerate() {
            assert_eq!(
                e.data,
                payload(s, i),
                "stream {s} event {i}: byte-exact from the log"
            );
        }
    }

    // And it keeps appending densely on top of the recovered chain.
    let out = engine
        .append_batch(
            "stream-0",
            Version::At((PER - 1) as u64),
            &[rec("ev", b"resumed")],
        )
        .await
        .expect("append after crash recovery");
    // The next append continues the global order right where the log ended —
    // which is past the `$registry` records too (bn-2di), not merely past the
    // user events. It mints no new name (stream-0 and type `ev` are both
    // already registered), so it takes exactly one position.
    assert_eq!(
        out.last_global_position,
        log_events(total, registrations(STREAMS)) as u64,
        "the next append continues the global order"
    );
}

/// bn-1vu (item 3): a clean reopen after rolls rehydrates the whole segment
/// chain — sealed segments served cold (sidecars reloaded), the live head hot —
/// and every read path returns the exact pre-restart data, then keeps
/// appending.
#[tokio::test]
async fn clean_reopen_after_rolls_serves_cold_and_hot_tiers() {
    let dir =
        mess_testkit::sweeping_temp_dir("engine-roll-clean-reopen-after-rolls");
    let store_path = dir.path().join("store");

    const STREAMS: usize = 16;
    const PER: usize = 25;
    let total = STREAMS * PER;

    {
        let engine =
            LogEngine::open_with(&store_path, rolling_opts()).expect("open");
        for s in 0..STREAMS {
            fill_stream(&engine, &format!("stream-{s}"), s, PER).await;
        }
    }

    let engine =
        LogEngine::open_with(&store_path, rolling_opts()).expect("reopen");
    assert!(
        engine.sealed_segment_count() > 0,
        "rolled segments reload into the cold tier"
    );
    assert_eq!(
        engine.total_events(),
        log_events(total, registrations(STREAMS)),
        "the whole reopened chain is intact (user events + $registry records)"
    );

    let g = engine.read_global(None, total * 2).await.unwrap();
    assert_eq!(g.len(), total, "every user event is delivered");
    assert_ascending(&g);
    for s in 0..STREAMS {
        let name = format!("stream-{s}");
        let evs =
            engine.read_stream(&name, Version::NoStream, total).await.unwrap();
        assert_eq!(
            evs.len(),
            PER,
            "stream {s}: all events across cold/hot after reopen"
        );
        for (i, e) in evs.iter().enumerate() {
            assert_eq!(e.stream_position, i as u64);
            assert_eq!(
                e.data,
                payload(s, i),
                "stream {s} event {i}: byte-exact after reopen"
            );
        }
        assert_eq!(
            engine.head(&name).await.unwrap(),
            Version::At((PER - 1) as u64)
        );
    }

    // Keep appending on the reopened, rolled store — positions stay dense and a
    // stale expected version still conflicts correctly.
    let conflict = engine
        .append_batch("stream-0", Version::At(0), &[rec("ev", b"stale")])
        .await;
    assert!(
        conflict.is_err(),
        "exact-version gate still enforced after reopen"
    );
    let out = engine
        .append_batch(
            "stream-0",
            Version::At((PER - 1) as u64),
            &[rec("ev", b"more")],
        )
        .await
        .expect("append after reopen");
    assert_eq!(
        out.last_global_position,
        log_events(total, registrations(STREAMS)) as u64,
        "the append continues the global order after reopen"
    );
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
    let dir = mess_testkit::sweeping_temp_dir(
        "engine-roll-oversized-batch-fails-fast",
    );
    let store_path = dir.path().join("store");
    // A tiny segment: no realistic batch fits, let alone this test's 2000-byte
    // oversized one.
    let opts = EngineOptions { segment_size: 512, ..EngineOptions::default() };
    let engine = LogEngine::open_with(&store_path, opts).expect("open");

    let segment_count = || {
        std::fs::read_dir(&store_path)
            .expect("read store dir")
            .flatten()
            .filter(|e| {
                e.path().extension().and_then(|x| x.to_str()) == Some("log")
            })
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

    assert_eq!(
        segment_count(),
        1,
        "no wasted roll: segment count must be unchanged"
    );
    // `bn-2di`: the failed append committed no USER event — but its two
    // `$registry` records (the stream name and the event-type name it minted)
    // ARE durable, and must be.
    //
    // This is not new behaviour, only newly *visible*. The pre-bn-2di engine
    // did exactly the same thing: it wrote the name to fjall BEFORE
    // submitting the append, so a failed append left the name persisted
    // there too — it just did not show up in `total_events()`, because
    // fjall was not the log. The reason is unchanged and unavoidable:
    // interning is irreversible (the id was handed out in-process and the
    // interner never rewinds), so the registration has to be durable on the
    // path that minted it or a later successful append to the same stream
    // would reference an id nothing ever registered.
    //
    // An orphan registration is harmless: `$registry` is append-only and tiny
    // by construction (REG4), and a retry with a batch that fits reuses the
    // same ids and registers nothing new.
    assert_eq!(
        engine.read_global(None, 100).await.unwrap().len(),
        0,
        "the failed oversized append committed no USER event"
    );
    assert_eq!(
        engine.total_events(),
        2,
        "...but its stream-name and type-name registrations are durable"
    );

    // The store must still be usable afterwards — the failed pre-check must
    // not have poisoned or otherwise wedged the committer.
    let out = engine
        .append_batch("stream-ok", Version::NoStream, &[rec("ev", b"small")])
        .await
        .expect(
            "a normal-sized append after the rejected oversized one must \
             still work",
        );
    // Globals 0-1 are the failed oversized append's registrations (the stream
    // name `stream-oversized` and the type name `Big`); 2-3 are `stream-ok`'s
    // own (its stream name and the type name `ev`); and 4 is the event itself —
    // the first and only USER event in this store.
    assert_eq!(out.last_global_position, 4);
}
