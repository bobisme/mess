//! SegmentEffect construction (research/02 §9): one pass over a sealed
//! segment's capsules, mutable temporary accumulators, emit net values and
//! continuity boundaries. Validates intra-segment invariants (position
//! continuity, per-stream path continuity, registry bijectivity, snapshot
//! prefix, allocator monotonicity) against SEGMENT-LOCAL state only — a
//! stream first touched mid-segment records the capsule's own
//! `first_version` as the incoming boundary, and cross-segment continuity
//! is validated at compose/apply time.

use std::collections::BTreeMap;

use crate::effect::{AllocTransition, HeadTransition, SegmentEffect};
use crate::kernel::FrozenEpoch;
use crate::model::{Capsule, Invalid, SegmentMeta, chain_anchor};

/// Build failure: the capsule ordinal (within the segment) plus the reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BuildErr {
    pub ordinal: usize,
    pub why: Invalid,
}

/// Build one segment's effect from its capsules. `meta` supplies the sealed
/// boundaries (start cursor/anchor); the builder recomputes the end anchor
/// from the bytes it saw.
pub fn build_effect(
    capsules: &[Capsule],
    meta: &SegmentMeta,
    dedupe_span: u64,
    epoch_span: u64,
) -> Result<SegmentEffect, BuildErr> {
    let epoch_span = epoch_span.max(1);
    let mut heads: BTreeMap<u64, HeadTransition> = BTreeMap::new();
    let mut snapshots: BTreeMap<u64, (u64, u64)> = BTreeMap::new();
    let mut frontiers: BTreeMap<(u32, u32), u64> = BTreeMap::new();
    let mut registry: BTreeMap<u64, u64> = BTreeMap::new();
    let mut registry_rev: BTreeMap<u64, u64> = BTreeMap::new();
    let mut alloc: BTreeMap<u32, AllocTransition> = BTreeMap::new();
    let mut dedupe: Vec<FrozenEpoch> = Vec::new();
    let mut active: Vec<(u64, u64)> = Vec::new();
    let mut active_idx = 0u64;
    let mut pos = meta.start.pos;
    let mut anchor = meta.start_anchor;

    let fail = |ordinal: usize, why: Invalid| BuildErr { ordinal, why };

    for (ord, c) in capsules.iter().enumerate() {
        match *c {
            Capsule::UserBatch {
                stream_id,
                first_version,
                event_count,
                first_global_pos,
            } => {
                if event_count == 0 {
                    return Err(fail(ord, Invalid::EmptyBatch));
                }
                if first_global_pos != pos {
                    return Err(fail(
                        ord,
                        Invalid::PositionMismatch {
                            expected: pos,
                            got: first_global_pos,
                        },
                    ));
                }
                match heads.get_mut(&stream_id) {
                    Some(t) => {
                        if first_version != t.last_head {
                            return Err(fail(
                                ord,
                                Invalid::HeadGap {
                                    stream_id,
                                    expected: t.last_head,
                                    got: first_version,
                                },
                            ));
                        }
                        t.last_head = first_version + u64::from(event_count);
                    }
                    None => {
                        // First touch: the capsule's own claim becomes the
                        // incoming boundary; apply/compose validates it.
                        heads.insert(
                            stream_id,
                            HeadTransition {
                                first_prior: first_version,
                                last_head: first_version
                                    + u64::from(event_count),
                            },
                        );
                    }
                }
                pos += u64::from(event_count);
            }
            Capsule::StreamRegistered { name, stream_id } => {
                if let Some(&id) = registry.get(&name)
                    && id != stream_id
                {
                    return Err(fail(
                        ord,
                        Invalid::RegistryConflict { name, id: stream_id },
                    ));
                }
                if let Some(&n) = registry_rev.get(&stream_id)
                    && n != name
                {
                    return Err(fail(
                        ord,
                        Invalid::RegistryConflict { name, id: stream_id },
                    ));
                }
                registry.insert(name, stream_id);
                registry_rev.insert(stream_id, name);
            }
            Capsule::SnapshotInstalled { stream_id, version, snapshot_ref } => {
                if version == 0 {
                    return Err(fail(
                        ord,
                        Invalid::SnapshotInvalid { stream_id, version },
                    ));
                }
                // Prefix validity against the segment-local head when the
                // stream was touched in-segment; otherwise trusted (it was
                // validated at commit time).
                if let Some(t) = heads.get(&stream_id)
                    && version > t.last_head
                {
                    return Err(fail(
                        ord,
                        Invalid::SnapshotInvalid { stream_id, version },
                    ));
                }
                snapshots.insert(stream_id, (version, snapshot_ref));
            }
            Capsule::ProjectionCheckpoint { projection_id, shard, position } => {
                // Frontier bottom: 0 == absent (see oracle.rs / REPORT).
                if position > 0 {
                    let e = frontiers.entry((projection_id, shard)).or_insert(0);
                    *e = (*e).max(position);
                }
            }
            Capsule::DedupeKey { fingerprint, position } => {
                if position != pos {
                    return Err(fail(
                        ord,
                        Invalid::DedupePosition { expected: pos, got: position },
                    ));
                }
                let idx = position / epoch_span;
                if !active.is_empty() && idx != active_idx {
                    active.sort_unstable();
                    let max_pos =
                        active.iter().map(|&(_, p)| p).max().unwrap_or(0);
                    dedupe.push(FrozenEpoch {
                        max_pos,
                        entries: std::mem::take(&mut active),
                    });
                }
                active_idx = idx;
                active.push((fingerprint, position));
            }
            Capsule::AllocatorSet { slot, value } => match alloc.get_mut(&slot) {
                Some(t) => {
                    if value < t.last {
                        return Err(fail(
                            ord,
                            Invalid::AllocatorRegression {
                                slot,
                                current: t.last,
                                got: value,
                            },
                        ));
                    }
                    t.last = value;
                }
                None => {
                    alloc.insert(slot, AllocTransition { first: value, last: value });
                }
            },
        }
        anchor = chain_anchor(&anchor, c);
    }
    if !active.is_empty() {
        active.sort_unstable();
        let max_pos = active.iter().map(|&(_, p)| p).max().unwrap_or(0);
        dedupe.push(FrozenEpoch { max_pos, entries: active });
    }
    // Whole-epoch expiry relative to the segment-end watermark.
    let floor = pos.saturating_sub(dedupe_span);
    dedupe.retain(|e| e.max_pos >= floor);

    Ok(SegmentEffect {
        first_segment: meta.segment_id,
        last_segment: meta.segment_id,
        epoch: meta.epoch,
        first: meta.start,
        last: crate::model::Cursor {
            idx: meta.start.idx + capsules.len() as u64,
            pos,
        },
        first_anchor: meta.start_anchor,
        last_anchor: anchor,
        dedupe_span,
        heads,
        snapshots,
        frontiers,
        registry,
        alloc,
        dedupe,
    })
}
