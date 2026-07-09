//! THE LAW for snapshots: a snapshot taken at an arbitrary prefix, folded
//! forward over the remaining tail, must land on **exactly** the state a full
//! replay produces. Plus the corollary: a snapshot whose `fold_version` no
//! longer matches the aggregate is invalidated and the load silently falls
//! back to full replay.
//!
//! ```text
//! fold(s0, all_events)  ==  fold(snapshot.state, tail)
//! ```
//!
//! # Why a hand-rolled seeded generator instead of `proptest`
//!
//! `mess-store` deliberately carries a minimal dependency surface (see its
//! `Cargo.toml`), and `proptest` is not a workspace dependency. `retry.rs`
//! already hand-rolls an xorshift PRNG for the same "keep deps minimal"
//! reason, so this test follows suit: a seeded xorshift64* generator drives
//! thousands of randomized (event-sequence, prefix-point) pairs. The seed is
//! printed so any failure is reproducible.

use mess_core::{Aggregate, CodecError, Event};
use mess_store::{
    EventStore, Loaded, MockBackend, SnapshotStore, Snapshottable,
    StateCodecError, StoredSnapshot, Version,
};

// ---------------------------------------------------------------------------
// A small aggregate whose fold is order-sensitive, so a wrong tail split shows.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum CounterEvent {
    /// Add `n` to the running total.
    Added(i64),
    /// Multiply the running total by `n` (order-sensitive on purpose).
    Scaled(i64),
    /// A no-op event type that v1 ignores — used to show a `fold_version` bump
    /// that starts handling a previously-ignored type (§9).
    Marked,
}

impl Event for CounterEvent {
    fn name(&self) -> &'static str {
        match self {
            CounterEvent::Added(_) => "counter.added",
            CounterEvent::Scaled(_) => "counter.scaled",
            CounterEvent::Marked => "counter.marked",
        }
    }

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(match self {
            CounterEvent::Added(n) | CounterEvent::Scaled(n) => {
                n.to_le_bytes().to_vec()
            }
            CounterEvent::Marked => Vec::new(),
        })
    }

    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError> {
        let int = || -> Result<i64, CodecError> {
            let bytes: [u8; 8] =
                data.try_into().map_err(|_| CodecError::Decode {
                    event_name: name.to_string(),
                    source: format!("expected 8 bytes, got {}", data.len()),
                })?;
            Ok(i64::from_le_bytes(bytes))
        };
        match name {
            "counter.added" => Ok(CounterEvent::Added(int()?)),
            "counter.scaled" => Ok(CounterEvent::Scaled(int()?)),
            "counter.marked" => Ok(CounterEvent::Marked),
            other => Err(CodecError::UnknownEventName(other.to_string())),
        }
    }
}

/// v1 fold: `Added`/`Scaled` mutate the total; `Marked` is ignored.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Counter {
    total: i64,
    marks: i64,
}

impl Aggregate for Counter {
    type Event = CounterEvent;

    fn apply(&mut self, event: &CounterEvent) {
        match event {
            CounterEvent::Added(n) => self.total = self.total.wrapping_add(*n),
            CounterEvent::Scaled(n) => self.total = self.total.wrapping_mul(*n),
            CounterEvent::Marked => {} // ignored in v1
        }
    }
}

impl Snapshottable for Counter {
    const FOLD_VERSION: u32 = 1;

    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        let mut out = Vec::with_capacity(16);
        out.extend_from_slice(&self.total.to_le_bytes());
        out.extend_from_slice(&self.marks.to_le_bytes());
        Ok(out)
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        if bytes.len() != 16 {
            return Err(StateCodecError(format!(
                "expected 16 state bytes, got {}",
                bytes.len()
            )));
        }
        let total = i64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let marks = i64::from_le_bytes(bytes[8..16].try_into().unwrap());
        Ok(Counter { total, marks })
    }
}

/// v2 of the SAME aggregate, folding the SAME event stream, but now handling
/// `Marked` (a semantic change that §9 says MUST bump `FOLD_VERSION`). It reads
/// the identical stored `state_blob` layout, so a v1 snapshot is *decodable* by
/// v2 — the only thing that must reject it is the `fold_version` check.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct CounterV2(Counter);

impl Aggregate for CounterV2 {
    type Event = CounterEvent;

    fn apply(&mut self, event: &CounterEvent) {
        match event {
            CounterEvent::Marked => self.0.marks += 1, // newly handled in v2
            other => self.0.apply(other),
        }
    }
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

// ---------------------------------------------------------------------------
// Seeded xorshift64* generator — reproducible, dependency-free.
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    fn small_int(&mut self) -> i64 {
        // Keep magnitudes modest so wrapping arithmetic stays intuitive.
        (self.next_u64() % 19) as i64 - 9
    }

    fn event(&mut self) -> CounterEvent {
        match self.next_u64() % 3 {
            0 => CounterEvent::Added(self.small_int()),
            1 => CounterEvent::Scaled(self.small_int()),
            _ => CounterEvent::Marked,
        }
    }

    fn sequence(&mut self, max_len: usize) -> Vec<CounterEvent> {
        let len = self.below(max_len + 1);
        (0..len).map(|_| self.event()).collect()
    }
}

/// The reference fold — the ground truth `fold(s0, events)`.
fn fold<A: Aggregate>(events: &[A::Event]) -> A {
    let mut state = A::default();
    for e in events {
        state.apply(e);
    }
    state
}

async fn append_all(
    store: &EventStore<MockBackend>,
    stream: &str,
    events: &[CounterEvent],
) {
    if !events.is_empty() {
        store.append(stream, Version::NoStream, events).await.unwrap();
    }
}

/// The expected version for appending the tail after `p` prefix events.
fn tail_expected_version(p: usize) -> Version {
    if p == 0 { Version::NoStream } else { Version::At((p - 1) as u64) }
}

// ---------------------------------------------------------------------------
// THE LAW.
// ---------------------------------------------------------------------------

/// `fold(s0, all) == fold(snapshot.state, tail)` across thousands of random
/// event sequences with the snapshot taken at a random prefix point.
#[tokio::test]
async fn snapshot_plus_tail_equals_full_replay() {
    const ITERATIONS: usize = 3_000;
    const MAX_EVENTS: usize = 40;
    let seed: u64 = 0x5EED_C0FF_EE01_1CFF;
    let mut rng = Rng::new(seed);

    for iter in 0..ITERATIONS {
        let events = rng.sequence(MAX_EVENTS);
        let n = events.len();
        // Snapshot after the first `p` events (0..=n). p==0 exercises the
        // empty-prefix snapshot; p==n exercises an empty tail.
        let p = rng.below(n + 1);
        let stream = format!("counter-{iter}");

        // Ground truth: fold the whole sequence directly.
        let expected = fold::<Counter>(&events);

        // Append the prefix, snapshot it, append the tail.
        let store = EventStore::new(MockBackend::new());
        append_all(&store, &stream, &events[..p]).await;
        let snap_ref = store.save_snapshot::<Counter>(&stream).await.unwrap();
        // The snapshot summarizes exactly the prefix we appended.
        if p == 0 {
            assert!(
                snap_ref.covers_empty_prefix,
                "iter {iter}: empty prefix must set covers_empty_prefix"
            );
        } else {
            assert_eq!(
                snap_ref.stream_version,
                (p - 1) as u64,
                "iter {iter}: stream_version must be the last summarized index"
            );
        }
        if !events[p..].is_empty() {
            store
                .append(&stream, tail_expected_version(p), &events[p..])
                .await
                .unwrap();
        }

        // Accelerated load: snapshot + tail.
        let cached: Loaded<Counter> =
            store.load_cached::<Counter>(&stream).await.unwrap();
        // Plain load: full replay. Both must agree with the ground truth.
        let full = store.load::<Counter>(&stream).await.unwrap();

        assert_eq!(
            cached.state, expected,
            "iter {iter} (seed {seed}, n={n}, p={p}): snapshot+tail mismatch"
        );
        assert_eq!(
            cached.state, full.state,
            "iter {iter}: snapshot+tail != full replay"
        );
        assert_eq!(
            cached.version, full.version,
            "iter {iter}: version differs"
        );

        // Prove the snapshot path was actually taken (not a silent fallback):
        // when p>=1 the accelerated load must replay ONLY the tail, i.e. fewer
        // events than a full replay would.
        if p >= 1 {
            assert_eq!(
                cached.events_replayed,
                n - p,
                "iter {iter}: accelerated load must fold only the tail"
            );
        }
    }
    println!("snapshot law held for {ITERATIONS} prefixes (seed {seed})");
}

// ---------------------------------------------------------------------------
// fold_version mismatch -> invalidate -> full replay.
// ---------------------------------------------------------------------------

/// A snapshot whose `fold_version` differs from the aggregate's current
/// `FOLD_VERSION` must be ignored, and the load must fall back to a full
/// replay that still yields the correct state — even when the stale snapshot's
/// blob would have decoded to a WRONG state.
#[tokio::test]
async fn stale_fold_version_falls_back_to_full_replay() {
    let store = EventStore::new(MockBackend::new());
    let stream = "counter-versioned";

    // A stream that includes `Marked` events — v1 ignores them, v2 counts them.
    let events = vec![
        CounterEvent::Added(5),
        CounterEvent::Marked,
        CounterEvent::Scaled(3),
        CounterEvent::Marked,
        CounterEvent::Added(2),
    ];
    store.append(stream, Version::NoStream, &events).await.unwrap();

    // Correct v2 state (marks are counted): total = (0+5)*3+2 = 17, marks = 2.
    let expected_v2 = fold::<CounterV2>(&events);
    assert_eq!(expected_v2, CounterV2(Counter { total: 17, marks: 2 }));

    // Hand-craft a STALE snapshot: fold_version = 1 (v1), and a deliberately
    // WRONG state blob. If load_cached wrongly trusted it, the result would be
    // this garbage; the fold_version mismatch must reject it instead.
    let mut wrong = expected_v2.clone();
    wrong.0.total = -999_999;
    wrong.0.marks = -999_999;
    let mut stale = StoredSnapshot {
        snapshot_ref: store.save_snapshot::<CounterV2>(stream).await.unwrap(),
        state_blob: wrong.encode_state().unwrap(),
    };
    // Downgrade the stored ref to the old fold version and poison the blob.
    stale.snapshot_ref.fold_version = 1;
    store.backend().save_snapshot(stream, stale).await.unwrap();

    // Sanity: a snapshot IS present (so a non-fallback would use it).
    assert!(store.backend().load_snapshot(stream).await.unwrap().is_some());

    // load_cached as v2 must ignore the fold_version-1 snapshot and full-replay.
    let loaded = store.load_cached::<CounterV2>(stream).await.unwrap();
    assert_eq!(
        loaded.state, expected_v2,
        "stale fold_version must be rejected and rebuilt by full replay"
    );
    // Full replay means ALL events were folded, not a short tail.
    assert_eq!(
        loaded.events_replayed,
        events.len(),
        "fallback must replay the whole stream, proving the snapshot was skipped"
    );
}

/// The positive control: a snapshot with a MATCHING fold_version is actually
/// used (short tail), so the mismatch test above is meaningful.
#[tokio::test]
async fn matching_fold_version_uses_snapshot() {
    let store = EventStore::new(MockBackend::new());
    let stream = "counter-fresh";

    let head = [CounterEvent::Added(10), CounterEvent::Scaled(2)];
    store.append(stream, Version::NoStream, &head).await.unwrap();
    let snap = store.save_snapshot::<CounterV2>(stream).await.unwrap();
    assert_eq!(snap.fold_version, 2);
    assert_eq!(snap.stream_version, 1);

    let tail = [CounterEvent::Added(1), CounterEvent::Marked];
    store.append(stream, Version::At(1), &tail).await.unwrap();

    let loaded = store.load_cached::<CounterV2>(stream).await.unwrap();
    // (0+10)*2 = 20, +1 = 21; one mark.
    assert_eq!(loaded.state, CounterV2(Counter { total: 21, marks: 1 }));
    // Snapshot supplied the 2-event prefix; only the 2 tail events replayed.
    assert_eq!(loaded.events_replayed, 2);
    assert_eq!(loaded.version, Version::At(3));
}

/// `load_cached` on a stream that never had a snapshot saved must still work,
/// by full replay.
#[tokio::test]
async fn load_cached_without_snapshot_is_full_replay() {
    let store = EventStore::new(MockBackend::new());
    let stream = "counter-nosnap";
    let events = [CounterEvent::Added(3), CounterEvent::Scaled(4)];
    store.append(stream, Version::NoStream, &events).await.unwrap();

    let loaded = store.load_cached::<Counter>(stream).await.unwrap();
    assert_eq!(loaded.state, Counter { total: 12, marks: 0 });
    assert_eq!(loaded.events_replayed, 2);
}
