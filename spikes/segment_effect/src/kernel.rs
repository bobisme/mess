//! The kernel: dense-array state (the shape design §7 resident state
//! implies) with an INDEPENDENT implementation of the same transition
//! semantics as the oracle. Dedupe uses position-window epochs (whole-epoch
//! reclamation, exact predicate at query/digest time — design §9.6/§13).
//!
//! Tracks dirty checkpoint pages (design §10.5) as it mutates.

use hashbrown::{HashMap, HashSet};

use crate::PAGE_CELLS;
use crate::codec::DigestBuilder;
use crate::model::{Capsule, Cursor, Invalid, chain_anchor, GENESIS_ANCHOR};

/// A frozen dedupe epoch: a coarse position range's entries plus the max
/// position (whole-epoch expiry key).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrozenEpoch {
    pub max_pos: u64,
    /// `(fingerprint, position)`, sorted at freeze for canonical encoding.
    pub entries: Vec<(u64, u64)>,
}

#[derive(Clone, Debug)]
pub struct KernelState {
    pub cursor: Cursor,
    pub anchor: [u8; 32],
    /// Dense heads: `heads[stream_id]` = event count, 0 = absent.
    pub heads: Vec<u64>,
    /// Dense snapshots: `(version, ref)`, version 0 = absent.
    pub snapshots: Vec<(u64, u64)>,
    pub frontiers: HashMap<(u32, u32), u64>,
    pub registry: HashMap<u64, u64>,
    pub registry_rev: HashMap<u64, u64>,
    pub alloc: HashMap<u32, u64>,
    pub dedupe_span: u64,
    pub epoch_span: u64,
    pub epochs: std::collections::VecDeque<FrozenEpoch>,
    pub active: Vec<(u64, u64)>,
    pub active_epoch_index: u64,
    // -- dirty tracking for incremental checkpoints (design §10.5) --
    pub dirty_head_pages: HashSet<u64>,
    pub dirty_snap_pages: HashSet<u64>,
    pub registry_dirty: bool,
    pub frontier_dirty: bool,
    pub alloc_dirty: bool,
    pub dedupe_dirty: bool,
}

impl KernelState {
    pub fn new(dedupe_span: u64, epoch_span: u64) -> Self {
        Self {
            cursor: Cursor::default(),
            anchor: GENESIS_ANCHOR,
            heads: Vec::new(),
            snapshots: Vec::new(),
            frontiers: HashMap::new(),
            registry: HashMap::new(),
            registry_rev: HashMap::new(),
            alloc: HashMap::new(),
            dedupe_span,
            epoch_span: epoch_span.max(1),
            epochs: std::collections::VecDeque::new(),
            active: Vec::new(),
            active_epoch_index: 0,
            dirty_head_pages: HashSet::new(),
            dirty_snap_pages: HashSet::new(),
            registry_dirty: false,
            frontier_dirty: false,
            alloc_dirty: false,
            dedupe_dirty: false,
        }
    }

    pub fn head_count(&self, stream_id: u64) -> u64 {
        self.heads.get(stream_id as usize).copied().unwrap_or(0)
    }

    pub fn set_head(&mut self, stream_id: u64, count: u64) {
        let i = stream_id as usize;
        if i >= self.heads.len() {
            self.heads.resize(i + 1, 0);
        }
        self.heads[i] = count;
        self.dirty_head_pages.insert(stream_id / PAGE_CELLS);
    }

    pub fn set_snapshot(&mut self, stream_id: u64, version: u64, sref: u64) {
        let i = stream_id as usize;
        if i >= self.snapshots.len() {
            self.snapshots.resize(i + 1, (0, 0));
        }
        self.snapshots[i] = (version, sref);
        self.dirty_snap_pages.insert(stream_id / PAGE_CELLS);
    }

    pub fn insert_dedupe(&mut self, fingerprint: u64, position: u64) {
        let idx = position / self.epoch_span;
        if !self.active.is_empty() && idx != self.active_epoch_index {
            self.freeze_active();
        }
        self.active_epoch_index = idx;
        self.active.push((fingerprint, position));
        self.dedupe_dirty = true;
    }

    fn freeze_active(&mut self) {
        if self.active.is_empty() {
            return;
        }
        let mut entries = std::mem::take(&mut self.active);
        entries.sort_unstable();
        let max_pos = entries.iter().map(|&(_, p)| p).max().unwrap_or(0);
        self.epochs.push_back(FrozenEpoch { max_pos, entries });
        self.dedupe_dirty = true;
    }

    /// Whole-epoch reclamation: drop epochs entirely below the exact floor.
    pub fn expire_dedupe(&mut self) {
        let floor = self.cursor.pos.saturating_sub(self.dedupe_span);
        while let Some(e) = self.epochs.front() {
            if e.max_pos < floor {
                self.epochs.pop_front();
                self.dedupe_dirty = true;
            } else {
                break;
            }
        }
    }

    /// One deterministic partial transition — independent implementation of
    /// the oracle's semantics.
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
                let prior = self.head_count(stream_id);
                if first_version != prior {
                    return Err(Invalid::HeadGap {
                        stream_id,
                        expected: prior,
                        got: first_version,
                    });
                }
                self.set_head(stream_id, prior + u64::from(event_count));
            }
            Capsule::StreamRegistered { name, stream_id } => {
                if let Some(&id) = self.registry.get(&name)
                    && id != stream_id
                {
                    return Err(Invalid::RegistryConflict { name, id: stream_id });
                }
                if let Some(&n) = self.registry_rev.get(&stream_id)
                    && n != name
                {
                    return Err(Invalid::RegistryConflict { name, id: stream_id });
                }
                self.registry.insert(name, stream_id);
                self.registry_rev.insert(stream_id, name);
                self.registry_dirty = true;
            }
            Capsule::SnapshotInstalled { stream_id, version, snapshot_ref } => {
                let head = self.head_count(stream_id);
                if version == 0 || version > head {
                    return Err(Invalid::SnapshotInvalid { stream_id, version });
                }
                self.set_snapshot(stream_id, version, snapshot_ref);
            }
            Capsule::ProjectionCheckpoint { projection_id, shard, position } => {
                // Frontier bottom: 0 == absent (see oracle.rs / REPORT).
                if position > 0 {
                    let e =
                        self.frontiers.entry((projection_id, shard)).or_insert(0);
                    if position > *e {
                        *e = position;
                        self.frontier_dirty = true;
                    }
                }
            }
            Capsule::DedupeKey { fingerprint, position } => {
                if position != self.cursor.pos {
                    return Err(Invalid::DedupePosition {
                        expected: self.cursor.pos,
                        got: position,
                    });
                }
                self.insert_dedupe(fingerprint, position);
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
                self.alloc_dirty = true;
            }
        }
        self.anchor = chain_anchor(&self.anchor, c);
        self.cursor.idx += 1;
        self.cursor.pos += c.consumes();
        self.expire_dedupe();
        Ok(())
    }

    /// Fold a capsule sequence from this state.
    pub fn fold(&mut self, capsules: &[Capsule]) -> Result<(), (usize, Invalid)> {
        for (i, c) in capsules.iter().enumerate() {
            self.apply(c).map_err(|e| (i, e))?;
        }
        Ok(())
    }

    /// Live dedupe entries under the EXACT window predicate, sorted.
    pub fn live_dedupe(&self) -> Vec<(u64, u64)> {
        let floor = self.cursor.pos.saturating_sub(self.dedupe_span);
        let mut live: Vec<(u64, u64)> = self
            .epochs
            .iter()
            .flat_map(|e| e.entries.iter())
            .chain(self.active.iter())
            .copied()
            .filter(|&(_, p)| p >= floor)
            .collect();
        live.sort_unstable();
        live
    }

    /// Canonical state digest — same spec as the oracle's.
    pub fn digest(&self) -> [u8; 32] {
        let mut d =
            DigestBuilder::new(self.cursor.idx, self.cursor.pos, &self.anchor);
        for (i, &c) in self.heads.iter().enumerate() {
            if c != 0 {
                d.head(i as u64, c);
            }
        }
        for (i, &(v, r)) in self.snapshots.iter().enumerate() {
            if v != 0 {
                d.snapshot(i as u64, v, r);
            }
        }
        let mut fr: Vec<((u32, u32), u64)> =
            self.frontiers.iter().map(|(&k, &v)| (k, v)).collect();
        fr.sort_unstable();
        for ((p, sh), pos) in fr {
            d.frontier(p, sh, pos);
        }
        let mut reg: Vec<(u64, u64)> =
            self.registry.iter().map(|(&n, &i)| (n, i)).collect();
        reg.sort_unstable();
        for (n, i) in reg {
            d.registration(n, i);
        }
        let mut al: Vec<(u32, u64)> =
            self.alloc.iter().map(|(&s, &v)| (s, v)).collect();
        al.sort_unstable();
        for (s, v) in al {
            d.alloc(s, v);
        }
        for (f, p) in self.live_dedupe() {
            d.dedupe(f, p);
        }
        d.finish()
    }

    /// Clear dirty tracking (a checkpoint was taken).
    pub fn clear_dirty(&mut self) {
        self.dirty_head_pages.clear();
        self.dirty_snap_pages.clear();
        self.registry_dirty = false;
        self.frontier_dirty = false;
        self.alloc_dirty = false;
        self.dedupe_dirty = false;
    }
}
