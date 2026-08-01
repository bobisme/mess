//! Conformance for [`PackSnapshotBackend`], the pack-based snapshot sidecar
//! (`bn-ozi5`, ADR 0002 §1) and — since `bn-fj34` — the *only* snapshot
//! backend.
//!
//! It covers the snapshot law, tail-only accelerated load, survival across a
//! reopen, the I5 self-heals, invalidation on deploy, and `SubscribeBackend`
//! delegation, plus a **byte-compare of replay vs snapshot-accelerated load**
//! over hundreds of random prefixes.
//!
//! # `bn-fj34`: what happened to the differential
//!
//! Until this bone a `pack_and_fjall_backends_return_byte_identical_snapshots`
//! test ran one workload through both this backend and the retired
//! key-value-backed one, byte-comparing every `StoredSnapshot`. That test
//! existed to license the migration and it did its job: `bn-3l8n` moved every
//! production, example, and tooling path onto packs, and `bn-fj34` deleted the
//! other engine. With one implementation left there is nothing to difference
//! *against*.
//!
//! The corpus did not die with it. `pack_snapshots_match_their_replay_derived_
//! expectation` keeps the same seeded 120-iteration workload and every
//! pack-side assertion, re-anchored on the only oracle that was ever
//! authoritative: **the events themselves**. A backend-to-backend comparison
//! can only ever prove two implementations agree — replay-derived expectations
//! prove this one is *right*, which is strictly the stronger claim and the one
//! [`snapshot_plus_tail_equals_full_replay_on_packs`] already makes at scale.
//!
//! Real filesystem, so `miri`-ignored.

#![cfg(not(miri))]

use std::time::Duration;

use mess_core::{Aggregate, CodecError, Event};
use mess_store::pack_snapshot::{SaveMode, SidecarOptions};
use mess_store::{
    EventStore, Loaded, LogEngine, PackSnapshotBackend, SnapshotStore,
    Snapshottable, StateCodecError, StoredSnapshot, Version,
};

// ---------------------------------------------------------------------------
// An order-sensitive counter aggregate: `Scaled` after `Added` is not the same
// state as the reverse, so a snapshot that folds the wrong prefix cannot pass
// by luck.
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
        let b: [u8; 8] = bytes
            .try_into()
            .map_err(|_| StateCodecError("expected 8 bytes".into()))?;
        Ok(Counter { total: i64::from_le_bytes(b) })
    }
}

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

type Store = EventStore<PackSnapshotBackend<LogEngine>>;

fn fold(events: &[CounterEvent]) -> Counter {
    let mut s = Counter::default();
    for e in events {
        s.apply(e);
    }
    s
}

/// Open a store whose event log is a *fresh* dir every time (this suite
/// exercises snapshot persistence across a reopen while the log is rehydrated
/// by hand) and whose sidecar lives at the fixed `root`.
fn open(root: &std::path::Path) -> Store { open_with(root, durable()) }

fn open_with(root: &std::path::Path, options: SidecarOptions) -> Store {
    let events = mess_testkit::sweeping_temp_dir("pack-snap-events");
    let engine = LogEngine::open(events.path()).expect("open engine");
    std::mem::forget(events); // keep the log dir alive for this store
    let backend =
        PackSnapshotBackend::open_with(engine, root, options).expect("open");
    EventStore::new(backend)
}

/// Durable publication, so a reopen inside a test observes exactly what a
/// power-cut survivor would (no reliance on the page cache).
fn durable() -> SidecarOptions {
    SidecarOptions { mode: SaveMode::Durable, ..Default::default() }
}

async fn rehydrate(store: &Store, stream: &str, events: &[CounterEvent]) {
    if !events.is_empty() {
        store.append(stream, Version::NoStream, events).await.unwrap();
    }
}

/// Seeded xorshift — dependency-free, byte-identical between runs.
fn rng(seed: u64) -> impl FnMut() -> u64 {
    let mut state = seed;
    move || {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

// ---------------------------------------------------------------------------
// The law, on the pack path.
// ---------------------------------------------------------------------------

/// `fold(s0, all) == fold(snapshot.state, tail)` across many random prefixes.
#[tokio::test]
async fn snapshot_plus_tail_equals_full_replay_on_packs() {
    let dir = mess_testkit::sweeping_temp_dir("pack-snap-law");
    let store = open(dir.path());
    let mut next = rng(0x5EED_C0FF_EE01_1CFF);

    for iter in 0..300u32 {
        let n = (next() % 21) as usize;
        let events: Vec<CounterEvent> = (0..n)
            .map(|_| {
                let m = (next() % 13) as i64 - 6;
                if next().is_multiple_of(2) {
                    CounterEvent::Added(m)
                } else {
                    CounterEvent::Scaled(m)
                }
            })
            .collect();
        let p = (next() % (n as u64 + 1)) as usize;
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
        // The bone's byte-compare requirement, taken literally: the state a
        // snapshot-accelerated load produces must serialize to the same bytes
        // as the one a pure replay produces.
        assert_eq!(
            cached.state.encode_state().unwrap(),
            full.state.encode_state().unwrap(),
            "iter {iter}: byte-compare replay vs snapshot"
        );
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
// Self-consistency of the saved snapshot against replay (bn-fj34: the corpus
// that used to be differenced against the retired backend).
// ---------------------------------------------------------------------------

/// The same seeded workload the pack-vs-fjall differential used to drive, now
/// checked against **replay-derived expectations** instead of a second
/// implementation's answers.
///
/// Per iteration the corpus picks a random event sequence and a random split
/// point `p`, appends the prefix, saves a snapshot, appends the tail, and then
/// pins — from the events alone, with no reference to what the sidecar chose
/// to write:
///
/// - the returned `SnapshotRef`'s covered version and empty-prefix flag;
/// - that the persisted [`StoredSnapshot`]'s state blob decodes, byte for byte,
///   to `fold(prefix)` — the blob is what a later load actually resumes from;
/// - that the ref the load hands back equals the ref the save returned;
/// - that the accelerated load reproduces `fold(all)` at the full-replay
///   version, and folds **only** the `n - p` tail events to get there.
///
/// Every assertion here is one the differential also made, minus the arm that
/// needed a second backend to state it.
#[tokio::test]
async fn pack_snapshots_match_their_replay_derived_expectation() {
    let pack_dir = mess_testkit::sweeping_temp_dir("pack-snap-diff-pack");
    let pack = open(pack_dir.path());
    let mut next = rng(0x0DDB_A11B_ADC0_FFEE);

    for iter in 0..120u32 {
        let n = (next() % 12) as usize;
        let events: Vec<CounterEvent> = (0..n)
            .map(|_| CounterEvent::Added((next() % 17) as i64 - 8))
            .collect();
        let p = (next() % (n as u64 + 1)) as usize;
        let stream = format!("diff-{iter}");

        if p > 0 {
            pack.append(&stream, Version::NoStream, &events[..p])
                .await
                .unwrap();
        }

        // 1. The save's own answer, against the prefix that produced it.
        let saved = pack.save_snapshot::<Counter>(&stream).await.unwrap();
        if p == 0 {
            assert!(
                saved.covers_empty_prefix,
                "iter {iter}: a snapshot of an empty prefix must say so"
            );
        } else {
            assert!(!saved.covers_empty_prefix, "iter {iter}: prefix is v0..");
            assert_eq!(
                saved.stream_version,
                (p - 1) as u64,
                "iter {iter}: snapshot must cover exactly the appended prefix"
            );
        }
        assert_eq!(
            saved.fold_version,
            Counter::FOLD_VERSION,
            "iter {iter}: snapshot carries the fold that produced it"
        );

        // 2. The PERSISTED blob — what a later load resumes from — must decode
        //    to the prefix fold, byte for byte.
        let stored: Option<StoredSnapshot> =
            pack.backend().load_snapshot(&stream).await.unwrap();
        let stored = stored.unwrap_or_else(|| {
            panic!("iter {iter}: a saved snapshot must load")
        });
        assert_eq!(
            stored.snapshot_ref, saved,
            "iter {iter}: loaded ref must equal the ref save returned"
        );
        let expected_prefix = fold(&events[..p]);
        assert_eq!(
            stored.state_blob,
            expected_prefix.encode_state().unwrap(),
            "iter {iter}: state blob must be the replay-derived prefix fold"
        );
        assert_eq!(
            Counter::decode_state(&stored.state_blob).unwrap(),
            expected_prefix,
            "iter {iter}: state blob must decode back to the prefix state"
        );

        // 3. Append the tail; the accelerated load must equal full replay and
        //    fold only the tail to get there.
        if p < n {
            let expect = if p == 0 {
                Version::NoStream
            } else {
                Version::At((p - 1) as u64)
            };
            pack.append(&stream, expect, &events[p..]).await.unwrap();
        }
        let loaded = pack.load_cached::<Counter>(&stream).await.unwrap();
        let full = pack.load::<Counter>(&stream).await.unwrap();
        assert_eq!(
            loaded.state,
            fold(&events),
            "iter {iter}: accelerated load must equal the replay-derived state"
        );
        assert_eq!(loaded.state, full.state, "iter {iter}: != full replay");
        assert_eq!(loaded.version, full.version, "iter {iter}: version");
        assert_eq!(
            loaded.events_replayed,
            n - p,
            "iter {iter}: accelerated load must fold ONLY the tail"
        );
        assert_eq!(
            full.events_replayed, n,
            "iter {iter}: the unaccelerated load folds everything"
        );
    }
}

// ---------------------------------------------------------------------------
// Tail-only accelerated load over many heads.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn load_is_root_lookup_plus_record_plus_tail_no_scan() {
    let dir = mess_testkit::sweeping_temp_dir("pack-snap-no-scan");
    // Buffered here: this test is about work per load, not durability, and
    // Buffered is the default production mode.
    let store = open_with(dir.path(), SidecarOptions::default());

    const STREAMS: usize = 200;
    const PREFIX: usize = 200;
    const TAIL: usize = 3;

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

    let target = "hot-0";
    let loaded = store.load_cached::<Counter>(target).await.unwrap();
    assert_eq!(
        loaded.events_replayed, TAIL,
        "load must read only the tail (root leaf + record + tail)"
    );
    let full = store.load::<Counter>(target).await.unwrap();
    assert_eq!(loaded.state, full.state);
    assert_eq!(full.events_replayed, PREFIX + TAIL);

    // The head lookup is an in-memory map hit plus one bounded pread; it does
    // not scan the other 199 heads.
    let iters = 200u32;
    let start = std::time::Instant::now();
    for _ in 0..iters {
        let _ = store.load_cached::<Counter>(target).await.unwrap();
    }
    let per_op = start.elapsed() / iters;
    println!("pack load_cached over {STREAMS} snapshots: {per_op:?}/op");
    assert!(
        per_op < Duration::from_millis(50),
        "head lookup is a point read, not a scan; got {per_op:?}/op"
    );
}

// ---------------------------------------------------------------------------
// Survival across a reopen.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn snapshots_survive_reopen() {
    let dir = mess_testkit::sweeping_temp_dir("pack-snap-reopen");
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
    let p = 6;
    let expected = fold(&events);

    {
        let store = open(dir.path());
        store.append(stream, Version::NoStream, &events[..p]).await.unwrap();
        store.save_snapshot::<Counter>(stream).await.unwrap();
        store
            .append(stream, Version::At((p - 1) as u64), &events[p..])
            .await
            .unwrap();
        // No `persist()` call: a Durable save is already acknowledged only
        // after its closure and the root's directory entry are synced.
    }

    let store = open(dir.path());
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
// I5 self-heals: any missing or corrupt sidecar state degrades to replay.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_wiped_sidecar_falls_back_to_full_replay() {
    let dir = mess_testkit::sweeping_temp_dir("pack-snap-wiped");
    let root = dir.path().join("snapshots");
    let stream = "wipe";
    let events: Vec<CounterEvent> = (1..=8).map(CounterEvent::Added).collect();
    let expected = fold(&events);

    {
        let store = open(&root);
        store.append(stream, Version::NoStream, &events).await.unwrap();
        store.save_snapshot::<Counter>(stream).await.unwrap();
    }
    std::fs::remove_dir_all(&root).expect("wipe the whole sidecar");

    let store = open(&root);
    rehydrate(&store, stream, &events).await;
    let loaded = store.load_cached::<Counter>(stream).await.unwrap();
    assert_eq!(loaded.state, expected, "a wiped sidecar must still be correct");
    assert_eq!(
        loaded.events_replayed,
        events.len(),
        "no sidecar => full replay of the whole stream"
    );
}

#[tokio::test]
async fn every_root_deleted_falls_back_to_full_replay() {
    let dir = mess_testkit::sweeping_temp_dir("pack-snap-no-roots");
    let stream = "no-roots";
    let events: Vec<CounterEvent> = (1..=8).map(CounterEvent::Added).collect();
    let expected = fold(&events);
    {
        let store = open(dir.path());
        store.append(stream, Version::NoStream, &events).await.unwrap();
        store.save_snapshot::<Counter>(stream).await.unwrap();
    }
    // Delete every discovery root but leave the packs: the records are all
    // still there, and none of them is reachable.
    for entry in std::fs::read_dir(dir.path()).unwrap().flatten() {
        if entry.file_name().to_string_lossy().ends_with(".root") {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }

    let store = open(dir.path());
    rehydrate(&store, stream, &events).await;
    let loaded = store.load_cached::<Counter>(stream).await.unwrap();
    assert_eq!(loaded.state, expected);
    assert_eq!(loaded.events_replayed, events.len(), "unreachable => replay");
}

#[tokio::test]
async fn a_corrupt_record_falls_back_to_full_replay() {
    let dir = mess_testkit::sweeping_temp_dir("pack-snap-corrupt-record");
    let stream = "corrupt";
    let events: Vec<CounterEvent> = (1..=6).map(CounterEvent::Scaled).collect();
    let expected = fold(&events);

    let store = open(dir.path());
    store.append(stream, Version::NoStream, &events).await.unwrap();
    store.save_snapshot::<Counter>(stream).await.unwrap();
    drop(store);

    // Flip a byte inside every pack — the CRC over each record body catches
    // it, so the head is a miss instead of a wrong answer.
    for entry in std::fs::read_dir(dir.path()).unwrap().flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".open") || name.ends_with(".pack") {
            let mut raw = std::fs::read(entry.path()).unwrap();
            let n = raw.len();
            raw[n - 12] ^= 0xFF;
            std::fs::write(entry.path(), &raw).unwrap();
        }
    }

    let store = open(dir.path());
    rehydrate(&store, stream, &events).await;
    let loaded = store.load_cached::<Counter>(stream).await.unwrap();
    assert_eq!(loaded.state, expected, "corrupt record must not poison a load");
    assert_eq!(
        loaded.events_replayed,
        events.len(),
        "corrupt record => fall back to full replay"
    );
}

#[tokio::test]
async fn an_unknown_format_is_a_miss_not_a_failure() {
    let dir = mess_testkit::sweeping_temp_dir("pack-snap-unknown-format");
    let stream = "future";
    let events: Vec<CounterEvent> = (1..=4).map(CounterEvent::Added).collect();
    let expected = fold(&events);
    {
        let store = open(dir.path());
        store.append(stream, Version::NoStream, &events).await.unwrap();
        store.save_snapshot::<Counter>(stream).await.unwrap();
    }
    // Rewrite every root descriptor's format word as a version from the
    // future. A binary that cannot understand a descriptor must not guess.
    for entry in std::fs::read_dir(dir.path()).unwrap().flatten() {
        if entry.file_name().to_string_lossy().ends_with(".root") {
            let mut raw = std::fs::read(entry.path()).unwrap();
            raw[4..6].copy_from_slice(&999u16.to_le_bytes());
            std::fs::write(entry.path(), &raw).unwrap();
        }
    }

    let store = open(dir.path());
    rehydrate(&store, stream, &events).await;
    let loaded = store.load_cached::<Counter>(stream).await.unwrap();
    assert_eq!(loaded.state, expected);
    assert_eq!(loaded.events_replayed, events.len());
}

// ---------------------------------------------------------------------------
// Invalidation on deploy (§9), on the persisted pack path.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fold_version_bump_invalidates_and_replaces_persisted_snapshot() {
    let dir = mess_testkit::sweeping_temp_dir("pack-snap-deploy");
    let store = open(dir.path());
    let stream = "deploy-pack";

    let events: Vec<CounterEvent> = (1..=6).map(CounterEvent::Added).collect();
    let expected = fold(&events);

    store.append(stream, Version::NoStream, &events).await.unwrap();
    let v1 = store.save_snapshot::<Counter>(stream).await.unwrap();
    assert_eq!(v1.fold_version, 1);
    assert_eq!(store.snapshot_metrics().invalidated(), 0);

    let loaded = store.load_cached::<CounterV2>(stream).await.unwrap();
    assert_eq!(loaded.state.0, expected);
    assert_eq!(
        loaded.events_replayed,
        events.len(),
        "persisted stale snapshot must be skipped: full replay"
    );
    assert_eq!(store.snapshot_metrics().invalidated(), 1);

    let replaced = store
        .backend()
        .load_snapshot(stream)
        .await
        .unwrap()
        .expect("head still present after rebuild");
    assert_eq!(
        replaced.snapshot_ref.fold_version, 2,
        "the replacement persisted record carries the new fold_version"
    );

    let tail: Vec<CounterEvent> = vec![CounterEvent::Added(100)];
    store
        .append(stream, Version::At((events.len() - 1) as u64), &tail)
        .await
        .unwrap();
    let again = store.load_cached::<CounterV2>(stream).await.unwrap();
    assert_eq!(again.events_replayed, tail.len());
    assert_eq!(store.snapshot_metrics().invalidated(), 1);
}

// ---------------------------------------------------------------------------
// Offline reader.
// ---------------------------------------------------------------------------

/// A read-only handle over a live writer's sidecar serves heads, refuses
/// writes, and enumerates streams by name without any reverse-id side map.
#[tokio::test]
async fn an_offline_reader_can_inspect_a_live_sidecar() {
    let dir = mess_testkit::sweeping_temp_dir("pack-snap-offline-reader");
    let store = open(dir.path());
    for i in 0..5u64 {
        let stream = format!("s{i}");
        store
            .append(
                &stream,
                Version::NoStream,
                &[CounterEvent::Added(i as i64)],
            )
            .await
            .unwrap();
        store.save_snapshot::<Counter>(&stream).await.unwrap();
    }

    let events = mess_testkit::sweeping_temp_dir("pack-snap-offline-log");
    let engine = LogEngine::open(events.path()).expect("open engine");
    let reader = PackSnapshotBackend::open_read_only(engine, dir.path());
    assert_eq!(
        reader.sidecar().stream_names(),
        vec![
            "s0".to_string(),
            "s1".to_string(),
            "s2".to_string(),
            "s3".to_string(),
            "s4".to_string(),
        ]
    );
    let got = reader.load_snapshot("s3").await.unwrap().expect("head");
    let want = store.backend().load_snapshot("s3").await.unwrap().unwrap();
    assert_eq!(got, want, "an offline reader sees the same bytes");

    // …and cannot write.
    let err = reader
        .save_snapshot("s3", want)
        .await
        .expect_err("a read-only handle must refuse to write");
    assert!(format!("{err}").contains("read-only"), "got {err}");
}

// ---------------------------------------------------------------------------
// `SubscribeBackend` delegation (bn-o9z dogfood), unchanged by the swap.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_over_pack_snapshot_backend_sees_live_commits() {
    let dir = mess_testkit::sweeping_temp_dir("pack-snap-subscribe");
    let store = open(dir.path());

    const PRE: u64 = 5;
    const LIVE: u64 = 5;
    const TOTAL: usize = (PRE + LIVE) as usize;
    /// The first append registers the stream and the one event type as
    /// `$registry` records, consuming the first two global positions.
    const REG: u64 = 2;

    let pre: Vec<CounterEvent> =
        (0..PRE).map(|i| CounterEvent::Added(i as i64)).collect();
    store.append("s", Version::NoStream, &pre).await.unwrap();
    assert_eq!(store.watermark().await.unwrap(), PRE + REG);

    let sub_store = store.clone();
    let consumer = tokio::spawn(async move {
        let mut sub = sub_store.subscribe(Some(0));
        let mut got = Vec::with_capacity(TOTAL);
        while got.len() < TOTAL {
            got.push(sub.next().await.expect("next").global_position);
        }
        got
    });

    let live: Vec<CounterEvent> =
        (0..LIVE).map(|i| CounterEvent::Added(i as i64)).collect();
    let writer_store = store.clone();
    tokio::spawn(async move {
        writer_store.append("s", Version::At(PRE - 1), &live).await.unwrap();
    })
    .await
    .unwrap();

    let got = tokio::time::timeout(Duration::from_secs(10), consumer)
        .await
        .expect("subscription must see live commits")
        .unwrap();
    assert_eq!(got, (REG..REG + TOTAL as u64).collect::<Vec<u64>>());
}

// ---------------------------------------------------------------------------
// Clones share one writer owner.
// ---------------------------------------------------------------------------

/// Cloning the backend (which `EventStore::clone` does) must not create a
/// second writer: every clone shares the one serialized owner, so concurrent
/// saves interleave safely and all of them are readable afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cloned_stores_share_one_writer_and_all_saves_land() {
    let dir = mess_testkit::sweeping_temp_dir("pack-snap-clones");
    let store = open_with(dir.path(), SidecarOptions::default());

    let mut tasks = Vec::new();
    for t in 0..4u64 {
        let s = store.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..25u64 {
                let stream = format!("t{t}-{i}");
                s.append(
                    &stream,
                    Version::NoStream,
                    &[CounterEvent::Added(i as i64)],
                )
                .await
                .unwrap();
                s.save_snapshot::<Counter>(&stream).await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(store.backend().sidecar().head_count(), 100);
    for t in 0..4u64 {
        for i in 0..25u64 {
            let stream = format!("t{t}-{i}");
            let snap =
                store.backend().load_snapshot(&stream).await.unwrap().unwrap();
            assert_eq!(
                snap.state_blob,
                (i as i64).to_le_bytes().to_vec(),
                "{stream} must be readable"
            );
        }
    }
}
