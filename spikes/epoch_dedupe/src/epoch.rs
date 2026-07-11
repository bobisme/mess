//! Epoch dedupe candidates G1/G2/G3 (design.md §13, research/03 §7).
//!
//! Positions are divided into fixed epochs of `span/8` positions. The active
//! epoch is a mutable in-memory table; when an insert crosses into the next
//! epoch range the active table is FROZEN: entries are sorted by
//! (fingerprint, position) into flat arrays (binary-searchable), an optional
//! BinaryFuse16 negative filter is built, and the whole thing is pushed onto
//! a deque. Whole epochs are dropped when `max_position < w - span` — there
//! is NO per-key delete anywhere: no tombstones, no FIFO order rows, no
//! read-before-write. An epoch straddling the window boundary is retained;
//! its stale entries are rejected by the exact position check at query time.
//!
//! Exactness: the fingerprint is only a router. Every equal-fingerprint
//! candidate is retained (smallvec chains in the active table, equal runs in
//! the sorted arrays) and every hit verifies full key bytes from the arena.

use std::collections::VecDeque;
use std::hash::{BuildHasherDefault, Hasher};
use std::time::Instant;

use hashbrown::HashMap;
use smallvec::SmallVec;
use xorf::{BinaryFuse16, DmaSerializable, Filter};

use crate::arena::Arena;
use crate::{DedupeIndex, Fingerprinter, Scope, fp64};

/// Identity hasher over the (already keyed-hash-quality) fingerprint fold.
/// Under the test fingerprinter's masked widths this degrades to heavy
/// bucket collisions, which hashbrown handles — exactness never depends on
/// hash quality.
#[derive(Default)]
pub struct FpIdentityHasher(u64);

impl Hasher for FpIdentityHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // Generic fallback (u128's Hash may route here on some std
        // versions): fold 8-byte words.
        for c in bytes.chunks(8) {
            let mut w = [0u8; 8];
            w[..c.len()].copy_from_slice(c);
            self.0 ^= u64::from_le_bytes(w);
        }
    }
    #[inline]
    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }
    #[inline]
    fn write_u128(&mut self, v: u128) {
        self.0 = fp64(v);
    }
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

type FpBuildHasher = BuildHasherDefault<FpIdentityHasher>;

/// One (position, arena ptr) candidate under a fingerprint.
pub type Slot = (u64, u64);

/// Active-table abstraction: insert-only during an epoch (no per-row delete,
/// by construction), drained wholesale at freeze.
pub trait ActiveTable {
    fn with_capacity(expected_keys: usize) -> Self;
    fn insert(&mut self, fp: u128, pos: u64, ptr: u64);
    /// Visit every slot whose fingerprint equals `fp` (all collision
    /// candidates — never just the first).
    fn for_each_match(&self, fp: u128, f: impl FnMut(u64, u64));
    /// All entries, sorted by (fp, pos), leaving the table empty.
    fn drain_sorted(&mut self) -> Vec<(u128, u64, u64)>;
    fn len(&self) -> usize;
    fn resident_bytes(&self) -> u64;
}

// ---------------------------------------------------------------- G1/G2

/// hashbrown active table: fp -> smallvec of slots (inline 1: distinct
/// fingerprints sharing a value are rare at 128 bits, but ALL are kept).
pub struct HashActive {
    map: HashMap<u128, SmallVec<[Slot; 1]>, FpBuildHasher>,
    entries: usize,
}

impl ActiveTable for HashActive {
    fn with_capacity(expected_keys: usize) -> Self {
        HashActive {
            map: HashMap::with_capacity_and_hasher(expected_keys, FpBuildHasher::default()),
            entries: 0,
        }
    }

    #[inline]
    fn insert(&mut self, fp: u128, pos: u64, ptr: u64) {
        self.map.entry(fp).or_default().push((pos, ptr));
        self.entries += 1;
    }

    #[inline]
    fn for_each_match(&self, fp: u128, mut f: impl FnMut(u64, u64)) {
        if let Some(slots) = self.map.get(&fp) {
            for &(pos, ptr) in slots {
                f(pos, ptr);
            }
        }
    }

    fn drain_sorted(&mut self) -> Vec<(u128, u64, u64)> {
        let mut out = Vec::with_capacity(self.entries);
        for (fp, slots) in self.map.drain() {
            for (pos, ptr) in slots {
                out.push((fp, pos, ptr));
            }
        }
        self.entries = 0;
        out.sort_unstable_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        out
    }

    fn len(&self) -> usize {
        self.entries
    }

    fn resident_bytes(&self) -> u64 {
        // hashbrown SwissTable: 1 ctrl byte + one (K, V) pair per slot.
        let slot = size_of::<(u128, SmallVec<[Slot; 1]>)>() as u64 + 1;
        self.map.capacity() as u64 * slot
    }
}

// ------------------------------------------------------------------- G3

/// Iceberg-style low-associativity active table: fixed 8-way bins addressed
/// by fingerprint bits, SoA within the bin (one or two cache lines of
/// fingerprints scanned per probe), plus a rare overflow map for bins that
/// fill. No per-row delete exists; the whole table is drained at freeze.
pub struct IcebergActive {
    assoc: usize,
    nbins: usize,
    fps: Vec<u128>,
    slots: Vec<Slot>,
    lens: Vec<u8>,
    spill: HashMap<u128, SmallVec<[Slot; 1]>, FpBuildHasher>,
    entries: usize,
    spilled: usize,
}

const ASSOC: usize = 8;

impl IcebergActive {
    #[inline]
    fn bin(&self, fp: u128) -> usize {
        (fp64(fp) as usize) & (self.nbins - 1)
    }
}

impl ActiveTable for IcebergActive {
    fn with_capacity(expected_keys: usize) -> Self {
        // Target ~75% load in the bins; spill absorbs the tail.
        let want_bins = (expected_keys.max(ASSOC) * 4) / (3 * ASSOC);
        let nbins = want_bins.next_power_of_two();
        IcebergActive {
            assoc: ASSOC,
            nbins,
            fps: vec![0u128; nbins * ASSOC],
            slots: vec![(0, 0); nbins * ASSOC],
            lens: vec![0u8; nbins],
            spill: HashMap::with_hasher(FpBuildHasher::default()),
            entries: 0,
            spilled: 0,
        }
    }

    #[inline]
    fn insert(&mut self, fp: u128, pos: u64, ptr: u64) {
        let b = self.bin(fp);
        let len = self.lens[b] as usize;
        if len < self.assoc {
            let i = b * self.assoc + len;
            self.fps[i] = fp;
            self.slots[i] = (pos, ptr);
            self.lens[b] = (len + 1) as u8;
        } else {
            self.spill.entry(fp).or_default().push((pos, ptr));
            self.spilled += 1;
        }
        self.entries += 1;
    }

    #[inline]
    fn for_each_match(&self, fp: u128, mut f: impl FnMut(u64, u64)) {
        let b = self.bin(fp);
        let len = self.lens[b] as usize;
        let base = b * self.assoc;
        for i in 0..len {
            if self.fps[base + i] == fp {
                let (pos, ptr) = self.slots[base + i];
                f(pos, ptr);
            }
        }
        // A full bin MAY have spilled; the spill map keeps exactness.
        if len == self.assoc
            && let Some(slots) = self.spill.get(&fp)
        {
            for &(pos, ptr) in slots {
                f(pos, ptr);
            }
        }
    }

    fn drain_sorted(&mut self) -> Vec<(u128, u64, u64)> {
        let mut out = Vec::with_capacity(self.entries);
        for b in 0..self.nbins {
            let len = self.lens[b] as usize;
            for i in 0..len {
                let idx = b * self.assoc + i;
                out.push((self.fps[idx], self.slots[idx].0, self.slots[idx].1));
            }
            self.lens[b] = 0;
        }
        for (fp, slots) in self.spill.drain() {
            for (pos, ptr) in slots {
                out.push((fp, pos, ptr));
            }
        }
        self.entries = 0;
        self.spilled = 0;
        out.sort_unstable_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        out
    }

    fn len(&self) -> usize {
        self.entries
    }

    fn resident_bytes(&self) -> u64 {
        (self.fps.capacity() * size_of::<u128>()
            + self.slots.capacity() * size_of::<Slot>()
            + self.lens.capacity()) as u64
            + self.spill.capacity() as u64
                * (size_of::<(u128, SmallVec<[Slot; 1]>)>() as u64 + 1)
    }
}

// ---------------------------------------------------------- frozen epoch

/// An immutable closed epoch: (fp, pos, ptr) sorted by (fp, pos) in SoA
/// arrays + optional BinaryFuse16 negative filter over the deduplicated
/// 64-bit fingerprint folds.
pub struct FrozenEpoch {
    pub min_pos: u64,
    pub max_pos: u64,
    fps: Vec<u128>,
    pos: Vec<u64>,
    ptrs: Vec<u64>,
    filter: Option<BinaryFuse16>,
}

impl FrozenEpoch {
    fn build(sorted: Vec<(u128, u64, u64)>, with_filter: bool) -> Self {
        debug_assert!(!sorted.is_empty());
        let n = sorted.len();
        let mut fps = Vec::with_capacity(n);
        let mut pos = Vec::with_capacity(n);
        let mut ptrs = Vec::with_capacity(n);
        let mut min_pos = u64::MAX;
        let mut max_pos = 0u64;
        for (f, p, t) in sorted {
            fps.push(f);
            pos.push(p);
            ptrs.push(t);
            min_pos = min_pos.min(p);
            max_pos = max_pos.max(p);
        }
        let filter = if with_filter {
            let mut keys: Vec<u64> = fps.iter().map(|&f| fp64(f)).collect();
            keys.sort_unstable();
            keys.dedup();
            // Construction can fail (xorf documents rare unlucky seeds);
            // None degrades to "always maybe" — exactness unaffected.
            BinaryFuse16::try_from(&keys).ok()
        } else {
            None
        };
        FrozenEpoch { min_pos, max_pos, fps, pos, ptrs, filter }
    }

    /// Latest live position in this epoch matching (scope, key) exactly.
    #[inline]
    fn lookup_max(
        &self,
        fp: u128,
        lo: u64,
        scope: Scope,
        key: &[u8],
        arena: &Arena,
    ) -> Option<u64> {
        if let Some(f) = &self.filter
            && !f.contains(&fp64(fp))
        {
            return None;
        }
        let start = self.fps.partition_point(|&x| x < fp);
        if start == self.fps.len() || self.fps[start] != fp {
            return None;
        }
        let end = start + self.fps[start..].partition_point(|&x| x == fp);
        // Positions ascend within the equal-fingerprint run: scan backwards
        // so the first full-key match is the latest.
        for i in (start..end).rev() {
            if self.pos[i] < lo {
                break; // ascending: everything earlier is older
            }
            if arena.matches(self.ptrs[i], scope, key) {
                return Some(self.pos[i]);
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.fps.len()
    }

    /// Bytes this epoch serializes to (entries + filter fingerprints +
    /// descriptor); also its resident size (flat arrays, mmap-shaped).
    pub fn bytes(&self) -> u64 {
        let entries = (self.fps.len() * (16 + 8 + 8)) as u64;
        let filter = self
            .filter
            .as_ref()
            .map(|f| f.len() as u64 * 2 + BinaryFuse16::DESCRIPTOR_LEN as u64)
            .unwrap_or(0);
        entries + filter + 32 // header: min/max/len/flags
    }
}

// ------------------------------------------------------------ EpochDedupe

pub struct EpochDedupe<A: ActiveTable> {
    pub fpr: Fingerprinter,
    span: u64,
    epoch_span: u64,
    active: A,
    /// Which epoch range the active table currently covers.
    active_epoch: u64,
    with_filter: bool,
    /// Oldest at the front. Reclamation = pop_front of WHOLE epochs.
    frozen: VecDeque<FrozenEpoch>,
    /// Per-freeze cost in ns (bench reporting).
    pub freeze_ns: Vec<u64>,
    epochs_dropped: u64,
}

impl<A: ActiveTable> EpochDedupe<A> {
    /// `span` = W (positions). Epochs are `span/8` positions each.
    pub fn new(span: u64, fpr: Fingerprinter, with_filter: bool) -> Self {
        let epoch_span = (span / 8).max(1);
        EpochDedupe {
            fpr,
            span,
            epoch_span,
            active: A::with_capacity(epoch_span.min(1 << 22) as usize),
            active_epoch: 0,
            with_filter,
            frozen: VecDeque::new(),
            freeze_ns: Vec::new(),
            epochs_dropped: 0,
        }
    }

    fn freeze_active(&mut self) {
        if self.active.len() == 0 {
            return;
        }
        let t0 = Instant::now();
        let sorted = self.active.drain_sorted();
        let epoch = FrozenEpoch::build(sorted, self.with_filter);
        self.frozen.push_back(epoch);
        self.freeze_ns.push(t0.elapsed().as_nanos() as u64);
    }

    /// Drop whole expired epochs: `max_position < w - span`. An epoch
    /// straddling the boundary stays; its stale entries are rejected by the
    /// position check in `lookup_max`.
    fn evict(&mut self, w: u64) {
        let lo = w.saturating_sub(self.span);
        while let Some(front) = self.frozen.front() {
            if front.max_pos >= lo {
                break;
            }
            self.frozen.pop_front();
            self.epochs_dropped += 1;
        }
    }

    /// Probe ONLY the active table (bench: active-miss path).
    #[inline]
    pub fn check_active_only(
        &self,
        scope: Scope,
        key: &[u8],
        w: u64,
        arena: &Arena,
    ) -> Option<u64> {
        let fp = self.fpr.fp(scope, key);
        let lo = w.saturating_sub(self.span);
        let mut best: Option<u64> = None;
        self.active.for_each_match(fp, |pos, ptr| {
            if pos >= lo && best.is_none_or(|b| pos > b) && arena.matches(ptr, scope, key) {
                best = Some(pos);
            }
        });
        best
    }

    pub fn frozen_epochs(&self) -> usize {
        self.frozen.len()
    }

    pub fn epochs_dropped(&self) -> u64 {
        self.epochs_dropped
    }

    pub fn oldest_live_epoch_range(&self) -> Option<(u64, u64)> {
        self.frozen.front().map(|e| (e.min_pos, e.max_pos))
    }

    /// Rebuild from the canonical arena suffix (checkpoint-loss recovery,
    /// design.md §13.5): every record with `position >= w - span` is
    /// re-inserted in order. Requires the D3 retention invariant — the
    /// arena (log) must still hold that suffix.
    pub fn rebuild_from_arena(
        arena: &Arena,
        w: u64,
        span: u64,
        fpr: Fingerprinter,
        with_filter: bool,
    ) -> Self {
        let mut s = Self::new(span, fpr, with_filter);
        let lo = w.saturating_sub(span);
        for (ptr, pos, scope, key) in arena.iter() {
            if pos >= lo {
                s.insert(scope, key, pos, ptr);
            }
        }
        s
    }
}

impl<A: ActiveTable> DedupeIndex for EpochDedupe<A> {
    fn check(&mut self, scope: Scope, key: &[u8], w: u64, arena: &Arena) -> Option<u64> {
        let fp = self.fpr.fp(scope, key);
        let lo = w.saturating_sub(self.span);

        // Active first: its positions are the newest.
        let mut best: Option<u64> = None;
        self.active.for_each_match(fp, |pos, ptr| {
            if pos >= lo && best.is_none_or(|b| pos > b) && arena.matches(ptr, scope, key) {
                best = Some(pos);
            }
        });
        if best.is_some() {
            return best;
        }

        // Frozen epochs newest -> oldest; the first exact match is the
        // global latest because epochs partition the position axis.
        for epoch in self.frozen.iter().rev() {
            if epoch.max_pos < lo {
                break;
            }
            if let Some(p) = epoch.lookup_max(fp, lo, scope, key, arena) {
                return Some(p);
            }
        }
        None
    }

    fn insert(&mut self, scope: Scope, key: &[u8], position: u64, ptr: u64) {
        let epoch = position / self.epoch_span;
        if epoch != self.active_epoch {
            self.freeze_active();
            self.active_epoch = epoch;
        }
        let fp = self.fpr.fp(scope, key);
        self.active.insert(fp, position, ptr);
        self.evict(position);
    }

    // No flush: everything is immediately visible.
    // No deletes_issued override: structurally zero — neither ActiveTable
    // impl has a remove operation and reclamation is whole-epoch pop_front.

    fn resident_bytes(&self) -> u64 {
        self.active.resident_bytes() + self.frozen.iter().map(|e| e.bytes()).sum::<u64>()
    }

    fn serialized_bytes(&self) -> u64 {
        // Frozen epochs persist as-is; the active epoch rides the kernel
        // checkpoint as raw (fp, pos, ptr) entries.
        self.frozen.iter().map(|e| e.bytes()).sum::<u64>() + (self.active.len() * 32) as u64
    }
}

/// G1: hashbrown active + plain sorted frozen epochs (no filter).
pub type G1 = EpochDedupe<HashActive>;
/// G2 = G1 + BinaryFuse16 negative filter per frozen epoch: `EpochDedupe::<HashActive>::new(span, fpr, true)`.
pub type G2 = EpochDedupe<HashActive>;
/// G3: Iceberg-style active + filtered frozen epochs.
pub type G3 = EpochDedupe<IcebergActive>;
