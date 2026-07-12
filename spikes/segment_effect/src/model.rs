//! Logical capsule model.
//!
//! v3-shaped user batches (stream, first_stream_version, event_count, first
//! global position — exactly the `AcceptedBatch` header fields the production
//! scanner returns) PLUS synthetic control records standing in for v4
//! controls, so the full product algebra `K = H × S × P × R × D × A` is
//! exercised. Controls do not consume user global positions (design §5.2).
//!
//! Capsules carry NO event payloads at all — the "no payload decode for
//! metadata recovery" gate is structural in this model, and the
//! real-segment integration test keeps it structural against production
//! bytes by never calling `AcceptedBatch::frames`.

/// One accepted commit capsule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Capsule {
    /// A v3 user batch: `first_version -> first_version + event_count` on
    /// one stream (path arrow, research/02 §3).
    UserBatch {
        stream_id: u64,
        first_version: u64,
        event_count: u32,
        first_global_pos: u64,
    },
    /// v4 stand-in: immutable name→id registration (conflict-union algebra).
    StreamRegistered { name: u64, stream_id: u64 },
    /// v4 stand-in: latest-usable-snapshot slot (right-biased).
    SnapshotInstalled { stream_id: u64, version: u64, snapshot_ref: u64 },
    /// v4 stand-in: sharded projection frontier (pointwise max join).
    ProjectionCheckpoint { projection_id: u32, shard: u32, position: u64 },
    /// v4 stand-in: exact position-window dedupe entry, emitted at the
    /// current watermark.
    DedupeKey { fingerprint: u64, position: u64 },
    /// v4 stand-in: monotone allocator slot (right-biased latest value with
    /// a monotonicity validity rule — exercises the `A` component).
    AllocatorSet { slot: u32, value: u64 },
}

impl Capsule {
    /// User global positions this capsule consumes (controls: none, §5.2).
    pub fn consumes(&self) -> u64 {
        match self {
            Capsule::UserBatch { event_count, .. } => u64::from(*event_count),
            _ => 0,
        }
    }

    /// Canonical identity bytes for the accepted-prefix anchor chain.
    pub fn write_canonical(&self, out: &mut Vec<u8>) {
        match *self {
            Capsule::UserBatch {
                stream_id,
                first_version,
                event_count,
                first_global_pos,
            } => {
                out.push(1);
                out.extend_from_slice(&stream_id.to_le_bytes());
                out.extend_from_slice(&first_version.to_le_bytes());
                out.extend_from_slice(&event_count.to_le_bytes());
                out.extend_from_slice(&first_global_pos.to_le_bytes());
            }
            Capsule::StreamRegistered { name, stream_id } => {
                out.push(2);
                out.extend_from_slice(&name.to_le_bytes());
                out.extend_from_slice(&stream_id.to_le_bytes());
            }
            Capsule::SnapshotInstalled { stream_id, version, snapshot_ref } => {
                out.push(3);
                out.extend_from_slice(&stream_id.to_le_bytes());
                out.extend_from_slice(&version.to_le_bytes());
                out.extend_from_slice(&snapshot_ref.to_le_bytes());
            }
            Capsule::ProjectionCheckpoint { projection_id, shard, position } => {
                out.push(4);
                out.extend_from_slice(&projection_id.to_le_bytes());
                out.extend_from_slice(&shard.to_le_bytes());
                out.extend_from_slice(&position.to_le_bytes());
            }
            Capsule::DedupeKey { fingerprint, position } => {
                out.push(5);
                out.extend_from_slice(&fingerprint.to_le_bytes());
                out.extend_from_slice(&position.to_le_bytes());
            }
            Capsule::AllocatorSet { slot, value } => {
                out.push(6);
                out.extend_from_slice(&slot.to_le_bytes());
                out.extend_from_slice(&value.to_le_bytes());
            }
        }
    }
}

/// Why a capsule transition is undefined (`δ(c)` partiality, research/02
/// §2): semantic invariant violations on a physically intact capsule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Invalid {
    /// Stream-version continuity: `first_version != prior event count`
    /// (gap or overlap).
    HeadGap { stream_id: u64, expected: u64, got: u64 },
    /// `first_global_pos` is not the current watermark.
    PositionMismatch { expected: u64, got: u64 },
    /// Same name → different id, or same id → different name.
    RegistryConflict { name: u64, id: u64 },
    /// Snapshot version 0 or beyond the stream's current head count.
    SnapshotInvalid { stream_id: u64, version: u64 },
    /// Allocator moved backwards.
    AllocatorRegression { slot: u32, current: u64, got: u64 },
    /// Dedupe entry position is not the current watermark.
    DedupePosition { expected: u64, got: u64 },
    /// Zero-event user batch (A5-shaped).
    EmptyBatch,
}

/// Commit cursor: capsule index (all capsules, controls included) + next
/// user global position (the watermark).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Cursor {
    pub idx: u64,
    pub pos: u64,
}

/// Extend the accepted-prefix anchor chain by one capsule.
pub fn chain_anchor(prev: &[u8; 32], c: &Capsule) -> [u8; 32] {
    let mut buf = Vec::with_capacity(40);
    c.write_canonical(&mut buf);
    let mut h = blake3::Hasher::new();
    h.update(prev);
    h.update(&buf);
    *h.finalize().as_bytes()
}

/// Genesis anchor.
pub const GENESIS_ANCHOR: [u8; 32] = [0u8; 32];

/// Sealed-segment metadata: cursor + anchor boundaries (the model's stand-in
/// for a sealed footer / fold anchor).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentMeta {
    pub segment_id: u64,
    pub epoch: u64,
    pub start: Cursor,
    pub end: Cursor,
    pub start_anchor: [u8; 32],
    pub end_anchor: [u8; 32],
    /// Capsule index range `[lo, hi)` into `Log::capsules`.
    pub lo: usize,
    pub hi: usize,
}

/// A sealed capsule log: the history plus segment boundaries and their
/// footer anchors.
#[derive(Clone, Debug)]
pub struct Log {
    pub capsules: Vec<Capsule>,
    pub segments: Vec<SegmentMeta>,
    /// Exact dedupe window span `W` (positions).
    pub dedupe_span: u64,
    /// Dedupe epoch granularity (positions), `W/8` by convention.
    pub epoch_span: u64,
}

impl Log {
    /// Seal a capsule sequence into segments at the given boundaries
    /// (`boundaries` are capsule-index split points, strictly increasing,
    /// exclusive of 0 and the end). Computes cursors and footer anchors in
    /// one pass. Performs NO semantic validation — invalid histories seal
    /// fine; rejection is the fold/effect layer's job.
    pub fn seal(
        capsules: Vec<Capsule>,
        boundaries: &[usize],
        dedupe_span: u64,
    ) -> Log {
        let mut segments = Vec::with_capacity(boundaries.len() + 1);
        let mut cursor = Cursor::default();
        let mut anchor = GENESIS_ANCHOR;
        let mut lo = 0usize;
        let mut seg_id = 0u64;
        let mut cuts: Vec<usize> = boundaries.to_vec();
        cuts.push(capsules.len());
        for hi in cuts {
            assert!(hi >= lo && hi <= capsules.len());
            if hi == lo && hi != capsules.len() {
                continue; // skip empty interior segments
            }
            let start = cursor;
            let start_anchor = anchor;
            for c in &capsules[lo..hi] {
                anchor = chain_anchor(&anchor, c);
                cursor.idx += 1;
                cursor.pos += c.consumes();
            }
            segments.push(SegmentMeta {
                segment_id: seg_id,
                epoch: seg_id + 1,
                start,
                end: cursor,
                start_anchor,
                end_anchor: anchor,
                lo,
                hi,
            });
            seg_id += 1;
            lo = hi;
        }
        Log { capsules, segments, dedupe_span, epoch_span: (dedupe_span / 8).max(1) }
    }

    pub fn end_cursor(&self) -> Cursor {
        self.segments.last().map(|s| s.end).unwrap_or_default()
    }

    pub fn end_anchor(&self) -> [u8; 32] {
        self.segments
            .last()
            .map(|s| s.end_anchor)
            .unwrap_or(GENESIS_ANCHOR)
    }

    /// The sealed-footer anchor at a segment-boundary capsule index, if
    /// `idx` IS a boundary (checkpoint anchors validate against these,
    /// design §10.4).
    pub fn boundary_anchor(&self, idx: u64) -> Option<([u8; 32], Cursor)> {
        if idx == 0 {
            return Some((GENESIS_ANCHOR, Cursor::default()));
        }
        self.segments
            .iter()
            .find(|s| s.end.idx == idx)
            .map(|s| (s.end_anchor, s.end))
    }

    /// Capsule slice of segment `i`.
    pub fn segment_capsules(&self, i: usize) -> &[Capsule] {
        let s = &self.segments[i];
        &self.capsules[s.lo..s.hi]
    }
}
