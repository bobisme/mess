//! Conformance for the **production** snapshot path
//! ([`FjallSnapshotBackend`]): the same snapshot law the interim in-memory
//! store obeys, plus the capabilities the interim store never had —
//! O(1)-ish head lookup with a tail-only load (no prefix scan), snapshots that
//! survive a process restart, and the I5 self-heal (a wiped meta dir or a
//! corrupt blob still loads the *correct* state by falling back to full
//! replay).
//!
//! These tests hit the real filesystem and fjall, so they are `miri`-ignored.

#![cfg(not(miri))]

use std::time::{Duration, Instant};

use mess_core::{Aggregate, CodecError, Event};
use mess_store::{
    EventStore, FjallSnapshotBackend, Loaded, LogEngine, SnapshotStore,
    Snapshottable, StateCodecError, Version,
};

// ---------------------------------------------------------------------------
// A tiny order-sensitive counter aggregate (same shape as the snapshot-law
// test, kept self-contained here).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum CounterEvent {
    Added(i64),
    Scaled(i64),
}

impl Event for CounterEvent {
    fn name(&self) -> &'static str {
        match self {
            CounterEvent::Added(_) => "counter.added",
            CounterEvent::Scaled(_) => "counter.scaled",
        }
    }

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let (CounterEvent::Added(n) | CounterEvent::Scaled(n)) = self;
        Ok(n.to_le_bytes().to_vec())
    }

    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError> {
        let bytes: [u8; 8] =
            data.try_into().map_err(|_| CodecError::Decode {
                event_name: name.to_string(),
                source:     format!("expected 8 bytes, got {}", data.len()),
            })?;
        let n = i64::from_le_bytes(bytes);
        match name {
            "counter.added" => Ok(CounterEvent::Added(n)),
            "counter.scaled" => Ok(CounterEvent::Scaled(n)),
            other => Err(CodecError::UnknownEventName(other.to_string())),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Counter {
    total: i64,
}

impl Aggregate for Counter {
    type Event = CounterEvent;

    fn apply(&mut self, event: &CounterEvent) {
        match event {
            CounterEvent::Added(n) => self.total = self.total.wrapping_add(*n),
            CounterEvent::Scaled(n) => self.total = self.total.wrapping_mul(*n),
        }
    }
}

impl Snapshottable for Counter {
    const FOLD_VERSION: u32 = 1;

    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        Ok(self.total.to_le_bytes().to_vec())
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        let b: [u8; 8] = bytes.try_into().map_err(|_| {
            StateCodecError(format!("expected 8 bytes, got {}", bytes.len()))
        })?;
        Ok(Counter { total: i64::from_le_bytes(b) })
    }
}

/// v2 of the SAME aggregate over the SAME blob layout — a `fold_version` bump
/// with an unchanged fold, so a v1 snapshot is decodable by v2 and the only
/// thing that rejects it is the version check. Used for the persisted
/// invalidation-on-deploy round-trip.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct CounterV2(Counter);

impl Aggregate for CounterV2 {
    type Event = CounterEvent;

    fn apply(&mut self, event: &CounterEvent) { self.0.apply(event); }
}

impl Snapshottable for CounterV2 {
    const FOLD_VERSION: u32 = 2;

    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        self.0.encode_state()
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        Counter::decode_state(bytes).map(CounterV2)
    }
}

type Store = EventStore<FjallSnapshotBackend<LogEngine>>;

fn fold(events: &[CounterEvent]) -> Counter {
    let mut s = Counter::default();
    for e in events {
        s.apply(e);
    }
    s
}

fn open(root: &std::path::Path) -> Store {
    // The composed engine's event log is intentionally given a *fresh* dir on
    // every open — this suite exercises snapshot-head persistence across a
    // reopen while the event log is rehydrated by hand (see `rehydrate` below;
    // "the durable log's job, out of this bone's scope"). The production
    // snapshot heads + blobs persist at the fixed `root`.
    let events = mess_testkit::sweeping_temp_dir("fjall-snap-events");
    let engine = LogEngine::open(events.path()).expect("open engine");
    std::mem::forget(events); // keep the fresh event dir for this store's life
    let backend = FjallSnapshotBackend::open(engine, root).expect("open");
    EventStore::new(backend)
}

/// Re-hydrate an event log (the durable log's job, out of this bone's scope):
/// append `events` back into a freshly-opened store's in-memory backend.
async fn rehydrate(store: &Store, stream: &str, events: &[CounterEvent]) {
    if !events.is_empty() {
        store.append(stream, Version::NoStream, events).await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// The law, on the real path.
// ---------------------------------------------------------------------------

/// `fold(s0, all) == fold(snapshot.state, tail)` across many random prefixes,
/// now driven through the fjall-heads + blob-dir store rather than the mock.
#[tokio::test]
async fn snapshot_plus_tail_equals_full_replay_on_fjall() {
    let dir =
        mess_testkit::sweeping_temp_dir("fjall-snap-snapshot-plus-tail-equals");
    let store = open(dir.path());

    // Seeded xorshift, dependency-free (mirrors the snapshot-law test).
    let mut state: u64 = 0x5EED_C0FF_EE01_1CFF;
    let mut next = || {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    };

    for iter in 0..300u32 {
        let n = (next() % 21) as usize; // 0..=20 events
        let events: Vec<CounterEvent> = (0..n)
            .map(|_| {
                let m = (next() % 13) as i64 - 6;
                if next() % 2 == 0 {
                    CounterEvent::Added(m)
                } else {
                    CounterEvent::Scaled(m)
                }
            })
            .collect();
        let p = (next() % (n as u64 + 1)) as usize; // snapshot after p events
        let stream = format!("law-{iter}");
        let expected = fold(&events);

        if p > 0 {
            store
                .append(&stream, Version::NoStream, &events[..p])
                .await
                .unwrap();
        }
        let snap = store.save_snapshot::<Counter>(&stream).await.unwrap();
        if p == 0 {
            assert!(snap.covers_empty_prefix, "iter {iter}: empty prefix flag");
        } else {
            assert_eq!(snap.stream_version, (p - 1) as u64, "iter {iter}");
        }
        if p < n {
            let expect = if p == 0 {
                Version::NoStream
            } else {
                Version::At((p - 1) as u64)
            };
            store.append(&stream, expect, &events[p..]).await.unwrap();
        }

        let cached: Loaded<Counter> =
            store.load_cached::<Counter>(&stream).await.unwrap();
        let full = store.load::<Counter>(&stream).await.unwrap();
        assert_eq!(cached.state, expected, "iter {iter}: snapshot+tail wrong");
        assert_eq!(cached.state, full.state, "iter {iter}: != full replay");
        assert_eq!(cached.version, full.version, "iter {iter}: version");
        if p >= 1 {
            assert_eq!(
                cached.events_replayed,
                n - p,
                "iter {iter}: accelerated load must fold ONLY the tail"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// O(1)-ish load: head lookup + blob + tail, no prefix scan.
// ---------------------------------------------------------------------------

/// A long stream snapshotted near its head loads by reading only the short
/// tail — `events_replayed` is the tail length, independent of how big the
/// summarized prefix is. And the `snapshot_head` lookup itself stays flat as
/// the number of stored snapshots grows (it is a fjall point read, not a scan).
#[tokio::test]
async fn load_is_head_plus_blob_plus_tail_no_scan() {
    let dir = mess_testkit::sweeping_temp_dir("fjall-snap-load-is-head-plus");
    let store = open(dir.path());

    // Snapshot many *different* streams so the snapshot-heads table is
    // populated; the lookup for any one must not scan the others.
    const STREAMS: usize = 400;
    const PREFIX: usize = 500; // big summarized prefix per stream
    const TAIL: usize = 3; // tiny tail

    // Build one big stream and snapshot it near the head.
    for s in 0..STREAMS {
        let stream = format!("hot-{s}");
        let prefix: Vec<CounterEvent> =
            (0..PREFIX).map(|i| CounterEvent::Added(i as i64)).collect();
        store.append(&stream, Version::NoStream, &prefix).await.unwrap();
        store.save_snapshot::<Counter>(&stream).await.unwrap();
        let tail: Vec<CounterEvent> =
            (0..TAIL).map(|_| CounterEvent::Added(1)).collect();
        store
            .append(&stream, Version::At((PREFIX - 1) as u64), &tail)
            .await
            .unwrap();
    }

    // Accelerated load of the *first* stream: only the 3-event tail is folded,
    // even though 500 events precede the snapshot and 399 other streams exist.
    let target = "hot-0";
    let loaded = store.load_cached::<Counter>(target).await.unwrap();
    assert_eq!(
        loaded.events_replayed, TAIL,
        "load must read only the tail (head + blob + tail), never the prefix"
    );
    // Correctness cross-check against full replay.
    let full = store.load::<Counter>(target).await.unwrap();
    assert_eq!(loaded.state, full.state);
    assert_eq!(full.events_replayed, PREFIX + TAIL);

    // Head-lookup timing stays flat regardless of table size: time a batch of
    // load_cached calls and assert a generous per-op ceiling (never flaky).
    let iters = 200u32;
    let start = Instant::now();
    for _ in 0..iters {
        let _ = store.load_cached::<Counter>(target).await.unwrap();
    }
    let per_op = start.elapsed() / iters;
    println!(
        "load_cached (head lookup + blob + {TAIL}-event tail) over {STREAMS} \
         snapshots: {per_op:?}/op"
    );
    assert!(
        per_op < std::time::Duration::from_millis(50),
        "head lookup is a point read, not a scan; got {per_op:?}/op"
    );
}

// ---------------------------------------------------------------------------
// New capability #1: snapshots survive a process restart.
// ---------------------------------------------------------------------------

/// Save a snapshot at a partial prefix, drop the whole backend, reopen the same
/// root (with a re-hydrated log), and confirm the *persisted* snapshot still
/// accelerates the load — folding only the tail. The interim in-memory store
/// lost everything on drop; this one does not.
#[tokio::test]
async fn snapshots_survive_reopen() {
    let dir =
        mess_testkit::sweeping_temp_dir("fjall-snap-snapshots-survive-reopen");
    let stream = "reopen";
    let events: Vec<CounterEvent> = (0..10)
        .map(|i| {
            if i % 3 == 0 {
                CounterEvent::Scaled(2)
            } else {
                CounterEvent::Added(i)
            }
        })
        .collect();
    let p = 6; // snapshot after 6 events
    let expected = fold(&events);

    {
        let store = open(dir.path());
        store.append(stream, Version::NoStream, &events[..p]).await.unwrap();
        store.save_snapshot::<Counter>(stream).await.unwrap();
        store
            .append(stream, Version::At((p - 1) as u64), &events[p..])
            .await
            .unwrap();
        // Flush fjall so the head is durable across the reopen.
        store.backend().persist().unwrap();
    } // backend dropped: in-memory log gone, fjall heads + blobs remain on disk.

    let store = open(dir.path());
    // The durable log is recovered independently (out of scope); re-supply it.
    rehydrate(&store, stream, &events).await;

    let loaded = store.load_cached::<Counter>(stream).await.unwrap();
    assert_eq!(loaded.state, expected, "reopened snapshot yields wrong state");
    assert_eq!(
        loaded.events_replayed,
        events.len() - p,
        "reopened load must use the PERSISTED snapshot and fold only the tail"
    );
}

// ---------------------------------------------------------------------------
// New capability #2 (I5): a wiped meta dir still loads the correct state.
// ---------------------------------------------------------------------------

/// Wipe the fjall meta directory (the snapshot-heads table) entirely, then
/// reopen: the head is gone, so `load_cached` finds no snapshot and rebuilds
/// the *correct* state by full replay. A lost index is never a wrong answer.
#[tokio::test]
async fn wiped_meta_dir_falls_back_to_full_replay() {
    let dir =
        mess_testkit::sweeping_temp_dir("fjall-snap-wiped-meta-dir-falls");
    let stream = "wipe-meta";
    let events: Vec<CounterEvent> = (1..=8).map(CounterEvent::Added).collect();
    let expected = fold(&events);

    {
        let store = open(dir.path());
        store.append(stream, Version::NoStream, &events).await.unwrap();
        store.save_snapshot::<Counter>(stream).await.unwrap();
        store.backend().persist().unwrap();
    }

    // Blow away the snapshot-heads table (simulate a lost/rebuilt index).
    std::fs::remove_dir_all(dir.path().join("meta")).unwrap();

    let store = open(dir.path());
    rehydrate(&store, stream, &events).await;

    let loaded = store.load_cached::<Counter>(stream).await.unwrap();
    assert_eq!(loaded.state, expected, "wiped meta must still load correctly");
    assert_eq!(
        loaded.events_replayed,
        events.len(),
        "no head => full replay of the whole stream"
    );
}

// ---------------------------------------------------------------------------
// New capability #3 (I5): a corrupt blob self-heals to full replay.
// ---------------------------------------------------------------------------

/// Corrupt the on-disk state blob after saving. The head still points at it,
/// but the blob's checksum no longer verifies, so `load_snapshot` reports "no
/// usable snapshot" and the load rebuilds the correct state by full replay —
/// never trusting the garbled bytes.
#[tokio::test]
async fn corrupt_blob_falls_back_to_full_replay() {
    let dir =
        mess_testkit::sweeping_temp_dir("fjall-snap-corrupt-blob-falls-back");
    let stream = "corrupt-blob";
    let events: Vec<CounterEvent> = (1..=6).map(CounterEvent::Scaled).collect();
    let expected = fold(&events);

    let store = open(dir.path());
    store.append(stream, Version::NoStream, &events).await.unwrap();
    store.save_snapshot::<Counter>(stream).await.unwrap();

    // Corrupt every blob file under <root>/blobs.
    corrupt_all_blobs(&dir.path().join("blobs"));

    let loaded = store.load_cached::<Counter>(stream).await.unwrap();
    assert_eq!(loaded.state, expected, "corrupt blob must not poison the load");
    assert_eq!(
        loaded.events_replayed,
        events.len(),
        "corrupt blob => fall back to full replay"
    );
}

// ---------------------------------------------------------------------------
// Invalidation on deploy (§9), on the PERSISTED path: a persisted v1 snapshot
// is invalidated by a v2 load, rebuilt by full replay, counted once, and the
// on-disk head is replaced with a v2 snapshot — a genuine fjall round-trip.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fold_version_bump_invalidates_and_replaces_persisted_snapshot() {
    let dir = mess_testkit::sweeping_temp_dir(
        "fjall-snap-fold-version-bump-invalidates",
    );
    let store = open(dir.path());
    let stream = "deploy-fjall";

    let events: Vec<CounterEvent> = (1..=6).map(CounterEvent::Added).collect();
    let expected = fold(&events);

    // Old binary (v1): snapshot the whole stream and flush it to disk.
    store.append(stream, Version::NoStream, &events).await.unwrap();
    let v1 = store.save_snapshot::<Counter>(stream).await.unwrap();
    assert_eq!(v1.fold_version, 1);
    store.backend().persist().unwrap();
    assert_eq!(store.snapshot_metrics().invalidated(), 0);

    // Deploy: load the same stream as v2. The persisted v1 head must be
    // invalidated (never used) and rebuilt by a full replay off the log.
    let loaded = store.load_cached::<CounterV2>(stream).await.unwrap();
    assert_eq!(
        loaded.state.0, expected,
        "rebuild must yield the correct state"
    );
    assert_eq!(
        loaded.events_replayed,
        events.len(),
        "persisted stale snapshot must be skipped: full replay"
    );
    assert_eq!(
        store.snapshot_metrics().invalidated(),
        1,
        "the persisted v1 snapshot is invalidated exactly once"
    );

    // The on-disk head was replaced with a v2 snapshot (round-trips through
    // fjall: encode_ref → head → decode_ref).
    let replaced = store
        .backend()
        .load_snapshot(stream)
        .await
        .unwrap()
        .expect("head still present after rebuild");
    assert_eq!(
        replaced.snapshot_ref.fold_version, 2,
        "the replacement persisted snapshot carries the new fold_version"
    );

    // A following v2 load uses the replaced snapshot: append a tail and confirm
    // only the tail is folded, and the counter does not climb again.
    let tail: Vec<CounterEvent> = vec![CounterEvent::Added(100)];
    store
        .append(stream, Version::At((events.len() - 1) as u64), &tail)
        .await
        .unwrap();
    let again = store.load_cached::<CounterV2>(stream).await.unwrap();
    assert_eq!(
        again.events_replayed,
        tail.len(),
        "the replacement v2 snapshot accelerates the next load"
    );
    assert_eq!(
        store.snapshot_metrics().invalidated(),
        1,
        "rebuild happened exactly once across the deploy"
    );
}

// ---------------------------------------------------------------------------
// New capability #4 (bn-o9z dogfood): `FjallSnapshotBackend` forwards
// `SubscribeBackend` by pure delegation, so ONE store serves both the warm
// write path (SnapshotStore) and live projections (subscribe) — no second
// EventStore over a cloned log just to subscribe.
// ---------------------------------------------------------------------------

/// A subscription opened over `EventStore<FjallSnapshotBackend<LogEngine>>`
/// replays pre-existing history and then tails a concurrent writer's commits
/// — proving the forwarded `watermark` / `await_watermark_past` reach the
/// wrapped `LogEngine` unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_over_fjall_snapshot_backend_sees_live_commits() {
    let dir = mess_testkit::sweeping_temp_dir("store-fjall-subscribe");
    let store = open(dir.path());

    const PRE: u64 = 5;
    const LIVE: u64 = 5;
    const TOTAL: usize = (PRE + LIVE) as usize;
    /// `bn-2di`: the first append registers two names — the stream `s` and the
    /// one event type these events carry — as `$registry` records, which are
    /// ordinary log events and so CONSUME the first two global positions. They
    /// are never delivered (stream 0 is filtered out of every user-facing
    /// read), so the subscription's first record is at position `REG`, and
    /// the watermark counts them. Nothing else registers after that.
    const REG: u64 = 2;

    // Pre-populate committed history before anyone subscribes.
    let pre: Vec<CounterEvent> =
        (0..PRE).map(|i| CounterEvent::Added(i as i64)).collect();
    store.append("s", Version::NoStream, &pre).await.unwrap();
    assert_eq!(store.watermark().await.unwrap(), PRE + REG);

    // Consumer: drain exactly TOTAL positions, blocking only on the
    // watermark — no polling sleeps.
    let sub_store = store.clone();
    let consumer = tokio::spawn(async move {
        let mut sub = sub_store.subscribe(Some(0));
        let mut got = Vec::with_capacity(TOTAL);
        while got.len() < TOTAL {
            let r = sub.next().await.expect("next");
            got.push(r.global_position);
        }
        got
    });

    // Concurrent writer: commit LIVE more events while the consumer tails.
    let live: Vec<CounterEvent> =
        (0..LIVE).map(|i| CounterEvent::Added(i as i64)).collect();
    let writer_store = store.clone();
    let writer = tokio::spawn(async move {
        writer_store.append("s", Version::At(PRE - 1), &live).await.unwrap();
    });
    writer.await.unwrap();

    let got = tokio::time::timeout(Duration::from_secs(10), consumer)
        .await
        .expect(
            "subscription over FjallSnapshotBackend must see live commits, \
             not just history",
        )
        .unwrap();
    let expected: Vec<u64> = (REG..REG + TOTAL as u64).collect();
    assert_eq!(
        got, expected,
        "gap-free, in-order delivery through the forwarding wrapper (past the \
         two $registry positions the first append consumed)"
    );
}

/// Flip a byte in every `*.blob` file found beneath `dir`.
fn corrupt_all_blobs(dir: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            corrupt_all_blobs(&path);
        } else if path.extension().and_then(|e| e.to_str()) == Some("blob") {
            let mut bytes = std::fs::read(&path).unwrap();
            if let Some(last) = bytes.last_mut() {
                *last ^= 0xFF;
            } else {
                bytes.push(0xFF);
            }
            std::fs::write(&path, &bytes).unwrap();
        }
    }
}
