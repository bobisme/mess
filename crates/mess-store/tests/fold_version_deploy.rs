//! The **invalidation-on-deploy** story (`docs/spec/05-fold-certificates.md`
//! §9, decision D4), end to end on the in-memory snapshot store.
//!
//! A deploy that bumps `fold_version` renders every snapshot written by the old
//! binary stale. This suite proves the three properties the bone asks for on
//! the *next* load of such a stream:
//!
//! - **(a) the stale snapshot is never used** — the load full-replays, so its
//!   result reflects the *new* fold even though the stored blob was built by the
//!   old one (and would decode to a wrong state if trusted);
//! - **(b) the rebuild happens exactly once** — the invalidation counter climbs
//!   by one and then stops, because
//! - **(c) a new snapshot is persisted with the new `fold_version`** — so the
//!   following load of the same stream is snapshot-accelerated again.

use mess_core::{Aggregate, CodecError, Event};
use mess_store::{
    EventStore, MockBackend, SnapshotStore, Snapshottable, StateCodecError,
    Version,
};

// ---------------------------------------------------------------------------
// One aggregate, two fold versions over the SAME event stream and the SAME
// state-blob layout — so a v1 snapshot is *decodable* by v2 and the ONLY thing
// that may reject it is the `fold_version` check (not a codec accident).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum CounterEvent {
    Added(i64),
    /// A type v1 ignores and v2 counts — the §9 "newly handled event type"
    /// change that MUST bump `fold_version`.
    Marked,
}

impl Event for CounterEvent {
    fn name(&self) -> &'static str {
        match self {
            CounterEvent::Added(_) => "counter.added",
            CounterEvent::Marked => "counter.marked",
        }
    }

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(match self {
            CounterEvent::Added(n) => n.to_le_bytes().to_vec(),
            CounterEvent::Marked => Vec::new(),
        })
    }

    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError> {
        match name {
            "counter.added" => {
                let b: [u8; 8] =
                    data.try_into().map_err(|_| CodecError::Decode {
                        event_name: name.to_string(),
                        source: format!("expected 8 bytes, got {}", data.len()),
                    })?;
                Ok(CounterEvent::Added(i64::from_le_bytes(b)))
            }
            "counter.marked" => Ok(CounterEvent::Marked),
            other => Err(CodecError::UnknownEventName(other.to_string())),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Counter {
    total: i64,
    marks: i64,
}

impl Counter {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16);
        out.extend_from_slice(&self.total.to_le_bytes());
        out.extend_from_slice(&self.marks.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self, StateCodecError> {
        if bytes.len() != 16 {
            return Err(StateCodecError(format!(
                "expected 16 state bytes, got {}",
                bytes.len()
            )));
        }
        Ok(Counter {
            total: i64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            marks: i64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        })
    }
}

/// v1: `Marked` is ignored.
impl Aggregate for Counter {
    type Event = CounterEvent;
    fn apply(&mut self, event: &CounterEvent) {
        if let CounterEvent::Added(n) = event {
            self.total = self.total.wrapping_add(*n);
        }
    }
}

impl Snapshottable for Counter {
    const FOLD_VERSION: u32 = 1;
    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        Ok(self.encode())
    }
    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        Counter::decode(bytes)
    }
}

/// v2 of the SAME aggregate: now counts `Marked`. Reads the identical blob.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct CounterV2(Counter);

impl Aggregate for CounterV2 {
    type Event = CounterEvent;
    fn apply(&mut self, event: &CounterEvent) {
        match event {
            CounterEvent::Marked => self.0.marks += 1,
            other => self.0.apply(other),
        }
    }
}

impl Snapshottable for CounterV2 {
    const FOLD_VERSION: u32 = 2;
    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        Ok(self.0.encode())
    }
    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        Counter::decode(bytes).map(CounterV2)
    }
}

fn fold_v2(events: &[CounterEvent]) -> CounterV2 {
    let mut s = CounterV2::default();
    for e in events {
        s.apply(e);
    }
    s
}

// ---------------------------------------------------------------------------
// The deploy story.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn deploy_bump_invalidates_rebuilds_once_and_replaces() {
    let store = EventStore::new(MockBackend::new());
    let stream = "acct-deploy";

    // Old binary (v1): a stream with Marked events, snapshotted at the head.
    let events = vec![
        CounterEvent::Added(5),
        CounterEvent::Marked,
        CounterEvent::Added(3),
        CounterEvent::Marked,
    ];
    store.append(stream, Version::NoStream, &events).await.unwrap();
    let v1_ref = store.save_snapshot::<Counter>(stream).await.unwrap();
    assert_eq!(v1_ref.fold_version, 1, "old snapshot carries the v1 fold");

    // The stored (v1) blob summarizes total=8, marks=0 — WRONG for v2, whose
    // correct answer counts the two marks.
    let expected_v2 = fold_v2(&events);
    assert_eq!(expected_v2, CounterV2(Counter { total: 8, marks: 2 }));

    // Precondition: nothing invalidated yet.
    assert_eq!(store.snapshot_metrics().invalidated(), 0);

    // Deploy: the same binary now loads the stream as v2.
    let loaded = store.load_cached::<CounterV2>(stream).await.unwrap();

    // (a) the stale v1 snapshot was NOT used: the answer reflects the v2 fold,
    //     and the load full-replayed every event (not a short tail on top of
    //     the stale blob).
    assert_eq!(
        loaded.state, expected_v2,
        "stale snapshot must be rebuilt by full replay, not trusted"
    );
    assert_eq!(
        loaded.events_replayed,
        events.len(),
        "invalidation rebuilds by FULL replay, proving the snapshot was skipped"
    );

    // (b) counted exactly once.
    assert_eq!(
        store.snapshot_metrics().invalidated(),
        1,
        "the fold_version bump invalidates exactly one snapshot"
    );

    // (c) the stale snapshot was REPLACED with a fresh v2 one.
    let replaced = store
        .backend()
        .load_snapshot(stream)
        .await
        .unwrap()
        .expect("a snapshot is still present after the rebuild");
    assert_eq!(
        replaced.snapshot_ref.fold_version, 2,
        "the replacement snapshot carries the new fold_version"
    );

    // A following load of the same stream is snapshot-accelerated again, and the
    // invalidation counter does NOT climb a second time (rebuild-exactly-once).
    // Append a tail so "used the snapshot" is observable as a short replay.
    let tail = vec![CounterEvent::Added(10), CounterEvent::Marked];
    store.append(stream, Version::At(3), &tail).await.unwrap();

    let again = store.load_cached::<CounterV2>(stream).await.unwrap();
    assert_eq!(again.state, CounterV2(Counter { total: 18, marks: 3 }));
    assert_eq!(
        again.events_replayed,
        tail.len(),
        "the replacement v2 snapshot is used: only the tail is folded"
    );
    assert_eq!(
        store.snapshot_metrics().invalidated(),
        1,
        "no second invalidation — the rebuild happened exactly once"
    );
}

/// A matching-`fold_version` load must NOT touch the invalidation counter — the
/// negative control that keeps the metric meaningful.
#[tokio::test]
async fn matching_version_does_not_invalidate() {
    let store = EventStore::new(MockBackend::new());
    let stream = "acct-match";

    store
        .append(stream, Version::NoStream, &[CounterEvent::Added(1)])
        .await
        .unwrap();
    store.save_snapshot::<CounterV2>(stream).await.unwrap();

    let _ = store.load_cached::<CounterV2>(stream).await.unwrap();
    assert_eq!(
        store.snapshot_metrics().invalidated(),
        0,
        "a fresh, matching snapshot is used, never invalidated"
    );
}

/// The metric is shared across `EventStore` clones (clone-is-share), so an
/// operator reading it on any handle sees the whole process's invalidations.
#[tokio::test]
async fn invalidation_metric_is_shared_across_clones() {
    let store = EventStore::new(MockBackend::new());
    let stream = "acct-clone";

    store
        .append(
            stream,
            Version::NoStream,
            &[CounterEvent::Added(2), CounterEvent::Marked],
        )
        .await
        .unwrap();
    store.save_snapshot::<Counter>(stream).await.unwrap(); // v1

    let clone = store.clone();
    let _ = clone.load_cached::<CounterV2>(stream).await.unwrap(); // deploy on clone
    assert_eq!(
        store.snapshot_metrics().invalidated(),
        1,
        "the invalidation recorded on the clone is visible on the original"
    );
}
