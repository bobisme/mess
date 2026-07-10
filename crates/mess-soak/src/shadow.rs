//! The in-driver shadow model: the source of truth the invariant probes check
//! the real engine against.
//!
//! Every successful append the driver observes is recorded here at the exact
//! `(stream_position, global_position)` the engine assigned it (both come back
//! in the [`Appended`](mess_store::Appended) result, so the shadow's global
//! order is the engine's, not a guess). The probes then sample the engine and
//! demand it agrees with this model byte-for-byte.

use std::collections::BTreeMap;
use std::collections::HashMap;

use mess_store::Version;

/// One event as the driver wrote it — the authoritative `(type, payload)` the
/// engine must return unchanged from every read path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowEvent {
    pub message_type: String,
    pub data:         Vec<u8>,
}

/// Where a global position maps in stream space.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalRef {
    pub stream:     String,
    pub stream_pos: u64,
}

/// The shadow of the whole store.
#[derive(Debug, Default)]
pub struct Shadow {
    /// `stream name → events in stream order` (index == stream position).
    streams:   HashMap<String, Vec<ShadowEvent>>,
    /// `global position → (stream, stream position)`. A `BTreeMap` so
    /// `max_global` and dense-prefix checks are cheap and ordered.
    by_global: BTreeMap<u64, GlobalRef>,
}

impl Shadow {
    #[must_use]
    pub fn new() -> Self { Shadow::default() }

    /// Record a committed batch. `first_stream_pos` MUST equal the stream's
    /// current shadow length (exact-version discipline); `first_global` is the
    /// engine-assigned global position of the batch's first event. Panics on a
    /// stream-position discontinuity — that would be a driver bug, not an
    /// engine bug, and must never be silently absorbed into the model.
    pub fn record_append(
        &mut self,
        stream: &str,
        first_stream_pos: u64,
        first_global: u64,
        events: &[ShadowEvent],
    ) {
        let entry = self.streams.entry(stream.to_string()).or_default();
        assert_eq!(
            entry.len() as u64,
            first_stream_pos,
            "shadow stream-position discontinuity on {stream}: have {}, batch \
             starts at {first_stream_pos}",
            entry.len(),
        );
        for (i, ev) in events.iter().enumerate() {
            let gp = first_global + i as u64;
            let sp = first_stream_pos + i as u64;
            entry.push(ev.clone());
            let prev = self.by_global.insert(
                gp,
                GlobalRef { stream: stream.to_string(), stream_pos: sp },
            );
            assert!(
                prev.is_none(),
                "shadow global-position {gp} written twice"
            );
        }
    }

    /// The shadow head (last position) of `stream`.
    #[must_use]
    pub fn head(&self, stream: &str) -> Version {
        match self.streams.get(stream) {
            Some(v) if !v.is_empty() => Version::At(v.len() as u64 - 1),
            _ => Version::NoStream,
        }
    }

    #[must_use]
    pub fn stream_events(&self, stream: &str) -> &[ShadowEvent] {
        self.streams.get(stream).map(Vec::as_slice).unwrap_or(&[])
    }

    #[must_use]
    pub fn global_ref(&self, gp: u64) -> Option<&GlobalRef> {
        self.by_global.get(&gp)
    }

    /// The `(type, payload)` the engine must return at global position `gp`.
    #[must_use]
    pub fn event_at_global(&self, gp: u64) -> Option<&ShadowEvent> {
        let r = self.by_global.get(&gp)?;
        self.streams.get(&r.stream)?.get(r.stream_pos as usize)
    }

    /// Total events recorded (== next global position on a dense store).
    #[must_use]
    pub fn total(&self) -> u64 { self.by_global.len() as u64 }

    /// The highest global position recorded, or `None` if empty.
    #[must_use]
    pub fn max_global(&self) -> Option<u64> {
        self.by_global.keys().next_back().copied()
    }

    /// Names of every stream the shadow has seen (for random sampling).
    #[must_use]
    pub fn stream_names(&self) -> Vec<String> {
        self.streams.keys().cloned().collect()
    }

    /// Build a `payload bytes → acked global positions` index over every
    /// recorded (acked) event. Each driver payload embeds a unique monotonic
    /// `write_nonce`, so the payload bytes are a globally unique fingerprint of
    /// the append that produced them — an engine "extra" whose payload is a key
    /// here is a byte-exact duplicate of an acked event (a double-replay), not
    /// a fresh unacked write. A `Vec` value (not a scalar) makes a
    /// payload-appearing-at-two-positions duplicate detectable directly.
    #[must_use]
    pub fn acked_payload_index(&self) -> HashMap<Vec<u8>, Vec<u64>> {
        let mut idx: HashMap<Vec<u8>, Vec<u64>> = HashMap::new();
        for (&gp, r) in &self.by_global {
            if let Some(ev) = self
                .streams
                .get(&r.stream)
                .and_then(|s| s.get(r.stream_pos as usize))
            {
                idx.entry(ev.data.clone()).or_default().push(gp);
            }
        }
        idx
    }

    /// Is the shadow's global space a dense `0..total` prefix? (It always
    /// should be — the driver only records engine-assigned dense positions —
    /// but the reconciliation step asserts it to catch a driver-side bug before
    /// blaming the engine.)
    #[must_use]
    pub fn is_dense(&self) -> bool {
        self.by_global.keys().copied().enumerate().all(|(i, gp)| i as u64 == gp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(t: &str, d: &[u8]) -> ShadowEvent {
        ShadowEvent { message_type: t.into(), data: d.into() }
    }

    #[test]
    fn records_and_reads_back() {
        let mut s = Shadow::new();
        s.record_append("a", 0, 0, &[ev("x", b"1"), ev("y", b"2")]);
        s.record_append("b", 0, 2, &[ev("z", b"3")]);
        s.record_append("a", 2, 3, &[ev("x", b"4")]);

        assert_eq!(s.head("a"), Version::At(2));
        assert_eq!(s.head("b"), Version::At(0));
        assert_eq!(s.head("missing"), Version::NoStream);
        assert_eq!(s.total(), 4);
        assert_eq!(s.max_global(), Some(3));
        assert!(s.is_dense());

        assert_eq!(s.event_at_global(0).unwrap().data, b"1");
        assert_eq!(s.event_at_global(2).unwrap().data, b"3");
        assert_eq!(s.event_at_global(3).unwrap(), &ev("x", b"4"));
        assert_eq!(s.global_ref(3).unwrap().stream, "a");
        assert_eq!(s.global_ref(3).unwrap().stream_pos, 2);
    }

    #[test]
    #[should_panic(expected = "discontinuity")]
    fn rejects_stream_gap() {
        let mut s = Shadow::new();
        s.record_append("a", 0, 0, &[ev("x", b"1")]);
        // Skips stream position 1 → must panic (driver bug).
        s.record_append("a", 2, 1, &[ev("x", b"2")]);
    }
}
