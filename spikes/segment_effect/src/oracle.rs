//! The oracle: a deliberately boring sequential capsule fold over
//! `BTreeMap`/`VecDeque` structures (research/02 §14 — the oracle must not
//! share implementation bugs with the optimized kernel).
//!
//! Dedupe here is the dumbest exact thing possible: one `VecDeque` of
//! `(fingerprint, position)` pruned by the exact window predicate on every
//! watermark advance. No epochs, no slack.

use std::collections::{BTreeMap, VecDeque};

use crate::codec::DigestBuilder;
use crate::model::{Capsule, Cursor, Invalid, chain_anchor, GENESIS_ANCHOR};

#[derive(Clone, Debug, Default)]
pub struct OracleState {
    pub cursor: Cursor,
    pub anchor: [u8; 32],
    /// stream -> event count (head). Absent = 0 events.
    pub heads: BTreeMap<u64, u64>,
    /// stream -> (version, snapshot_ref), right-biased latest.
    pub snapshots: BTreeMap<u64, (u64, u64)>,
    /// (projection, shard) -> position, pointwise max.
    pub frontiers: BTreeMap<(u32, u32), u64>,
    /// name -> id (with reverse uniqueness enforced).
    pub registry: BTreeMap<u64, u64>,
    /// id -> name (conflict detection mirror).
    pub registry_rev: BTreeMap<u64, u64>,
    /// slot -> value, monotone right-biased.
    pub alloc: BTreeMap<u32, u64>,
    /// Exact live dedupe window, pruned every advance.
    pub dedupe: VecDeque<(u64, u64)>,
    pub dedupe_span: u64,
}

impl OracleState {
    pub fn new(dedupe_span: u64) -> Self {
        Self {
            anchor: GENESIS_ANCHOR,
            dedupe_span,
            ..Default::default()
        }
    }

    fn prune_dedupe(&mut self) {
        let floor = self.cursor.pos.saturating_sub(self.dedupe_span);
        // Entries were inserted at monotonically nondecreasing positions, so
        // the deque front holds the oldest.
        while let Some(&(_, p)) = self.dedupe.front() {
            if p < floor {
                self.dedupe.pop_front();
            } else {
                break;
            }
        }
    }

    /// One deterministic partial transition `δ(c)` (research/02 §2).
    pub fn apply(&mut self, c: &Capsule) -> Result<(), Invalid> {
        match *c {
            Capsule::UserBatch {
                stream_id,
                first_version,
                event_count,
                first_global_pos,
            } => {
                if event_count == 0 {
                    return Err(Invalid::EmptyBatch);
                }
                if first_global_pos != self.cursor.pos {
                    return Err(Invalid::PositionMismatch {
                        expected: self.cursor.pos,
                        got: first_global_pos,
                    });
                }
                let prior = self.heads.get(&stream_id).copied().unwrap_or(0);
                if first_version != prior {
                    return Err(Invalid::HeadGap {
                        stream_id,
                        expected: prior,
                        got: first_version,
                    });
                }
                self.heads
                    .insert(stream_id, prior + u64::from(event_count));
            }
            Capsule::StreamRegistered { name, stream_id } => {
                match self.registry.get(&name) {
                    Some(&id) if id != stream_id => {
                        return Err(Invalid::RegistryConflict {
                            name,
                            id: stream_id,
                        });
                    }
                    _ => {}
                }
                match self.registry_rev.get(&stream_id) {
                    Some(&n) if n != name => {
                        return Err(Invalid::RegistryConflict {
                            name,
                            id: stream_id,
                        });
                    }
                    _ => {}
                }
                // Identical re-registration is an idempotent no-op.
                self.registry.insert(name, stream_id);
                self.registry_rev.insert(stream_id, name);
            }
            Capsule::SnapshotInstalled { stream_id, version, snapshot_ref } => {
                let head = self.heads.get(&stream_id).copied().unwrap_or(0);
                if version == 0 || version > head {
                    return Err(Invalid::SnapshotInvalid { stream_id, version });
                }
                self.snapshots.insert(stream_id, (version, snapshot_ref));
            }
            Capsule::ProjectionCheckpoint { projection_id, shard, position } => {
                // Frontier bottom: position 0 (genesis, nothing processed)
                // IS "absent". Creating an explicit 0 entry would be a
                // digest-visible state change with no value change — which
                // latest-value dirty tracking can never see (bug found by
                // the corpus at seed 22280; see REPORT).
                if position > 0 {
                    let e = self
                        .frontiers
                        .entry((projection_id, shard))
                        .or_insert(0);
                    *e = (*e).max(position);
                }
            }
            Capsule::DedupeKey { fingerprint, position } => {
                if position != self.cursor.pos {
                    return Err(Invalid::DedupePosition {
                        expected: self.cursor.pos,
                        got: position,
                    });
                }
                self.dedupe.push_back((fingerprint, position));
            }
            Capsule::AllocatorSet { slot, value } => {
                let cur = self.alloc.get(&slot).copied().unwrap_or(0);
                if value < cur {
                    return Err(Invalid::AllocatorRegression {
                        slot,
                        current: cur,
                        got: value,
                    });
                }
                self.alloc.insert(slot, value);
            }
        }
        self.anchor = chain_anchor(&self.anchor, c);
        self.cursor.idx += 1;
        self.cursor.pos += c.consumes();
        self.prune_dedupe();
        Ok(())
    }

    /// Fold a whole capsule sequence from this state.
    pub fn fold(&mut self, capsules: &[Capsule]) -> Result<(), (usize, Invalid)> {
        for (i, c) in capsules.iter().enumerate() {
            self.apply(c).map_err(|e| (i, e))?;
        }
        Ok(())
    }

    /// Canonical state digest (shared spec, see [`DigestBuilder`]).
    pub fn digest(&self) -> [u8; 32] {
        let mut d =
            DigestBuilder::new(self.cursor.idx, self.cursor.pos, &self.anchor);
        for (&s, &c) in &self.heads {
            d.head(s, c);
        }
        for (&s, &(v, r)) in &self.snapshots {
            d.snapshot(s, v, r);
        }
        for (&(p, sh), &pos) in &self.frontiers {
            d.frontier(p, sh, pos);
        }
        for (&n, &i) in &self.registry {
            d.registration(n, i);
        }
        for (&s, &v) in &self.alloc {
            d.alloc(s, v);
        }
        let floor = self.cursor.pos.saturating_sub(self.dedupe_span);
        let mut live: Vec<(u64, u64)> = self
            .dedupe
            .iter()
            .copied()
            .filter(|&(_, p)| p >= floor)
            .collect();
        live.sort_unstable();
        for (f, p) in live {
            d.dedupe(f, p);
        }
        d.finish()
    }
}
