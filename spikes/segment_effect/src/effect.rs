//! `SegmentEffect` — the algebraic segment summary (design §9.7) — plus
//! ordered composition `E1 ⊗ E2` (research/02 §8) and atomic application to
//! a kernel state.
//!
//! Head transitions are PATHS, not last-writes: each touched stream records
//! its incoming prior-count (FIRST boundary) and final count (LAST
//! boundary), so composition and application validate continuity without
//! replaying intermediate events. Snapshots/allocators are right-biased
//! (allocators additionally carry a first/last pair for the monotonicity
//! boundary), frontiers join pointwise-max, registry deltas union with
//! conflict `⊥`, dedupe epochs concatenate with whole-epoch expiry.
//!
//! Canonical encoding: sorted logical entries, delta+LEB128 varints, body
//! CRC32C; `effect_hash = BLAKE3(body)`.

use std::collections::BTreeMap;

use crate::codec::{get_varint, put_varint};
use crate::kernel::{FrozenEpoch, KernelState};
use crate::model::Cursor;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeadTransition {
    /// Incoming prior event count (the FIRST boundary).
    pub first_prior: u64,
    /// Final event count after the segment (the LAST boundary).
    pub last_head: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AllocTransition {
    /// First value assigned in the span (monotonicity boundary).
    pub first: u64,
    /// Final value.
    pub last: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentEffect {
    pub first_segment: u64,
    pub last_segment: u64,
    pub epoch: u64,
    pub first: Cursor,
    pub last: Cursor,
    pub first_anchor: [u8; 32],
    pub last_anchor: [u8; 32],
    pub dedupe_span: u64,
    pub heads: BTreeMap<u64, HeadTransition>,
    pub snapshots: BTreeMap<u64, (u64, u64)>,
    pub frontiers: BTreeMap<(u32, u32), u64>,
    /// name -> id immutable additions (internally bijective).
    pub registry: BTreeMap<u64, u64>,
    pub alloc: BTreeMap<u32, AllocTransition>,
    pub dedupe: Vec<FrozenEpoch>,
}

/// `⊥` reasons for `E1 ⊗ E2`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComposeErr {
    /// Cursors/anchors do not meet: effects are not consecutive.
    NotAdjacent,
    SpanMismatch,
    /// End of first != start of second for a stream both touch.
    HeadPath { stream_id: u64, end_of_first: u64, start_of_second: u64 },
    RegistryConflict { name: u64, id: u64 },
    AllocRegression { slot: u32 },
}

/// `⊥` reasons for applying an effect to a state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyErr {
    CursorMismatch { expected: Cursor, got: Cursor },
    AnchorMismatch,
    SpanMismatch,
    HeadPath { stream_id: u64, state_head: u64, effect_first: u64 },
    RegistryConflict { name: u64, id: u64 },
    AllocRegression { slot: u32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeErr {
    Truncated,
    BadMagic,
    BadCrc,
    NonCanonical,
}

const MAGIC: &[u8; 4] = b"SEF1";

impl SegmentEffect {
    /// The empty effect at a cursor: the identity of `⊗`.
    pub fn empty(at: Cursor, anchor: [u8; 32], span: u64) -> Self {
        SegmentEffect {
            first_segment: 0,
            last_segment: 0,
            epoch: 0,
            first: at,
            last: at,
            first_anchor: anchor,
            last_anchor: anchor,
            dedupe_span: span,
            heads: BTreeMap::new(),
            snapshots: BTreeMap::new(),
            frontiers: BTreeMap::new(),
            registry: BTreeMap::new(),
            alloc: BTreeMap::new(),
            dedupe: Vec::new(),
        }
    }

    /// Canonical body bytes (everything but the CRC frame).
    fn body(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64 + self.heads.len() * 8);
        out.extend_from_slice(MAGIC);
        put_varint(&mut out, self.first_segment);
        put_varint(&mut out, self.last_segment);
        put_varint(&mut out, self.epoch);
        put_varint(&mut out, self.first.idx);
        put_varint(&mut out, self.first.pos);
        put_varint(&mut out, self.last.idx);
        put_varint(&mut out, self.last.pos);
        put_varint(&mut out, self.dedupe_span);
        out.extend_from_slice(&self.first_anchor);
        out.extend_from_slice(&self.last_anchor);
        // heads: sorted ascending, delta-encoded stream ids.
        put_varint(&mut out, self.heads.len() as u64);
        let mut prev = 0u64;
        for (&sid, t) in &self.heads {
            put_varint(&mut out, sid - prev);
            put_varint(&mut out, t.first_prior);
            put_varint(&mut out, t.last_head - t.first_prior);
            prev = sid;
        }
        // snapshots
        put_varint(&mut out, self.snapshots.len() as u64);
        let mut prev = 0u64;
        for (&sid, &(v, r)) in &self.snapshots {
            put_varint(&mut out, sid - prev);
            put_varint(&mut out, v);
            put_varint(&mut out, r);
            prev = sid;
        }
        // frontiers
        put_varint(&mut out, self.frontiers.len() as u64);
        for (&(p, s), &pos) in &self.frontiers {
            put_varint(&mut out, u64::from(p));
            put_varint(&mut out, u64::from(s));
            put_varint(&mut out, pos);
        }
        // registry
        put_varint(&mut out, self.registry.len() as u64);
        let mut prev = 0u64;
        for (&n, &i) in &self.registry {
            put_varint(&mut out, n - prev);
            put_varint(&mut out, i);
            prev = n;
        }
        // alloc
        put_varint(&mut out, self.alloc.len() as u64);
        for (&s, t) in &self.alloc {
            put_varint(&mut out, u64::from(s));
            put_varint(&mut out, t.first);
            put_varint(&mut out, t.last);
        }
        // dedupe epochs
        put_varint(&mut out, self.dedupe.len() as u64);
        for e in &self.dedupe {
            put_varint(&mut out, e.max_pos);
            put_varint(&mut out, e.entries.len() as u64);
            for &(f, p) in &e.entries {
                out.extend_from_slice(&f.to_le_bytes());
                put_varint(&mut out, p);
            }
        }
        out
    }

    /// Serialize: canonical body || CRC32C(body).
    pub fn encode(&self) -> Vec<u8> {
        let mut body = self.body();
        let crc = crc32c::crc32c(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        body
    }

    /// The effect hash (BLAKE3 over the canonical body).
    pub fn hash(&self) -> [u8; 32] {
        *blake3::hash(&self.body()).as_bytes()
    }

    /// Bytes of the head section alone (the ≤16 B/touched-stream gate).
    pub fn head_section_bytes(&self) -> usize {
        let mut out = Vec::new();
        put_varint(&mut out, self.heads.len() as u64);
        let mut prev = 0u64;
        for (&sid, t) in &self.heads {
            put_varint(&mut out, sid - prev);
            put_varint(&mut out, t.first_prior);
            put_varint(&mut out, t.last_head - t.first_prior);
            prev = sid;
        }
        out.len()
    }

    /// Deserialize + CRC check. A flipped byte anywhere fails here.
    pub fn decode(bytes: &[u8]) -> Result<SegmentEffect, DecodeErr> {
        if bytes.len() < 4 + 4 {
            return Err(DecodeErr::Truncated);
        }
        let (body, crc_bytes) = bytes.split_at(bytes.len() - 4);
        let want = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        if crc32c::crc32c(body) != want {
            return Err(DecodeErr::BadCrc);
        }
        if &body[..4] != MAGIC {
            return Err(DecodeErr::BadMagic);
        }
        let mut at = 4usize;
        let v = |at: &mut usize| -> Result<u64, DecodeErr> {
            let (x, n) = get_varint(body, *at).ok_or(DecodeErr::Truncated)?;
            *at += n;
            Ok(x)
        };
        let first_segment = v(&mut at)?;
        let last_segment = v(&mut at)?;
        let epoch = v(&mut at)?;
        let first = Cursor { idx: v(&mut at)?, pos: v(&mut at)? };
        let last = Cursor { idx: v(&mut at)?, pos: v(&mut at)? };
        let dedupe_span = v(&mut at)?;
        if body.len() < at + 64 {
            return Err(DecodeErr::Truncated);
        }
        let mut first_anchor = [0u8; 32];
        first_anchor.copy_from_slice(&body[at..at + 32]);
        at += 32;
        let mut last_anchor = [0u8; 32];
        last_anchor.copy_from_slice(&body[at..at + 32]);
        at += 32;

        let n = v(&mut at)?;
        let mut heads = BTreeMap::new();
        let mut prev = 0u64;
        for i in 0..n {
            let d = v(&mut at)?;
            if i > 0 && d == 0 {
                return Err(DecodeErr::NonCanonical);
            }
            let sid = prev + d;
            let first_prior = v(&mut at)?;
            let added = v(&mut at)?;
            heads.insert(
                sid,
                HeadTransition { first_prior, last_head: first_prior + added },
            );
            prev = sid;
        }
        let n = v(&mut at)?;
        let mut snapshots = BTreeMap::new();
        let mut prev = 0u64;
        for i in 0..n {
            let d = v(&mut at)?;
            if i > 0 && d == 0 {
                return Err(DecodeErr::NonCanonical);
            }
            let sid = prev + d;
            let ver = v(&mut at)?;
            let r = v(&mut at)?;
            snapshots.insert(sid, (ver, r));
            prev = sid;
        }
        let n = v(&mut at)?;
        let mut frontiers = BTreeMap::new();
        for _ in 0..n {
            let p = v(&mut at)? as u32;
            let s = v(&mut at)? as u32;
            let pos = v(&mut at)?;
            frontiers.insert((p, s), pos);
        }
        let n = v(&mut at)?;
        let mut registry = BTreeMap::new();
        let mut prev = 0u64;
        for i in 0..n {
            let d = v(&mut at)?;
            if i > 0 && d == 0 {
                return Err(DecodeErr::NonCanonical);
            }
            let name = prev + d;
            let id = v(&mut at)?;
            registry.insert(name, id);
            prev = name;
        }
        let n = v(&mut at)?;
        let mut alloc = BTreeMap::new();
        for _ in 0..n {
            let s = v(&mut at)? as u32;
            let first = v(&mut at)?;
            let last = v(&mut at)?;
            alloc.insert(s, AllocTransition { first, last });
        }
        let n = v(&mut at)?;
        let mut dedupe = Vec::new();
        for _ in 0..n {
            let max_pos = v(&mut at)?;
            let m = v(&mut at)?;
            let mut entries = Vec::with_capacity(m as usize);
            for _ in 0..m {
                if body.len() < at + 8 {
                    return Err(DecodeErr::Truncated);
                }
                let f =
                    u64::from_le_bytes(body[at..at + 8].try_into().unwrap());
                at += 8;
                let p = v(&mut at)?;
                entries.push((f, p));
            }
            dedupe.push(FrozenEpoch { max_pos, entries });
        }
        if at != body.len() {
            return Err(DecodeErr::NonCanonical);
        }
        Ok(SegmentEffect {
            first_segment,
            last_segment,
            epoch,
            first,
            last,
            first_anchor,
            last_anchor,
            dedupe_span,
            heads,
            snapshots,
            frontiers,
            registry,
            alloc,
            dedupe,
        })
    }
}

/// Ordered composition `a ⊗ b` (research/02 §8). Componentwise; the head
/// component is PATH-AWARE right override: the end of `a` must equal the
/// start of `b` for every stream both touch, else `⊥`. Associative on valid
/// ordered histories; NEVER commutative.
pub fn compose(
    a: &SegmentEffect,
    b: &SegmentEffect,
) -> Result<SegmentEffect, ComposeErr> {
    if a.dedupe_span != b.dedupe_span {
        return Err(ComposeErr::SpanMismatch);
    }
    if a.last != b.first || a.last_anchor != b.first_anchor {
        return Err(ComposeErr::NotAdjacent);
    }
    // Heads: path composition.
    let mut heads = a.heads.clone();
    for (&sid, tb) in &b.heads {
        match heads.get_mut(&sid) {
            Some(ta) => {
                if ta.last_head != tb.first_prior {
                    return Err(ComposeErr::HeadPath {
                        stream_id: sid,
                        end_of_first: ta.last_head,
                        start_of_second: tb.first_prior,
                    });
                }
                ta.last_head = tb.last_head;
            }
            None => {
                heads.insert(sid, *tb);
            }
        }
    }
    // Snapshots: right-biased override.
    let mut snapshots = a.snapshots.clone();
    for (&sid, &sv) in &b.snapshots {
        snapshots.insert(sid, sv);
    }
    // Frontiers: pointwise max join.
    let mut frontiers = a.frontiers.clone();
    for (&k, &pos) in &b.frontiers {
        let e = frontiers.entry(k).or_insert(0);
        *e = (*e).max(pos);
    }
    // Registry: conflict-detecting union (immutable partial bijection).
    let mut registry = a.registry.clone();
    let mut rev: BTreeMap<u64, u64> =
        registry.iter().map(|(&n, &i)| (i, n)).collect();
    for (&n, &i) in &b.registry {
        if let Some(&i0) = registry.get(&n)
            && i0 != i
        {
            return Err(ComposeErr::RegistryConflict { name: n, id: i });
        }
        if let Some(&n0) = rev.get(&i)
            && n0 != n
        {
            return Err(ComposeErr::RegistryConflict { name: n, id: i });
        }
        registry.insert(n, i);
        rev.insert(i, n);
    }
    // Allocators: right-biased with the monotone boundary check.
    let mut alloc = a.alloc.clone();
    for (&s, tb) in &b.alloc {
        match alloc.get_mut(&s) {
            Some(ta) => {
                if tb.first < ta.last {
                    return Err(ComposeErr::AllocRegression { slot: s });
                }
                ta.last = tb.last;
            }
            None => {
                alloc.insert(s, *tb);
            }
        }
    }
    // Dedupe: ordered concatenation, dropping whole epochs that are
    // entirely below the exact floor at the composed end watermark.
    let floor = b.last.pos.saturating_sub(b.dedupe_span);
    let mut dedupe: Vec<FrozenEpoch> = Vec::new();
    for e in a.dedupe.iter().chain(b.dedupe.iter()) {
        if e.max_pos >= floor {
            dedupe.push(e.clone());
        }
    }
    Ok(SegmentEffect {
        first_segment: a.first_segment,
        last_segment: b.last_segment,
        epoch: b.epoch,
        first: a.first,
        last: b.last,
        first_anchor: a.first_anchor,
        last_anchor: b.last_anchor,
        dedupe_span: a.dedupe_span,
        heads,
        snapshots,
        frontiers,
        registry,
        alloc,
        dedupe,
    })
}

impl KernelState {
    /// Apply a segment effect ATOMICALLY: validate everything (cursor,
    /// anchor, head continuity, registry conflicts, allocator monotonicity)
    /// before mutating anything, so `⊥` never leaves a half-applied state.
    ///
    /// Cost is proportional to touched keys, not event count (research/02
    /// §10.3).
    pub fn apply_effect(&mut self, e: &SegmentEffect) -> Result<(), ApplyErr> {
        if e.dedupe_span != self.dedupe_span {
            return Err(ApplyErr::SpanMismatch);
        }
        if e.first != self.cursor {
            return Err(ApplyErr::CursorMismatch {
                expected: self.cursor,
                got: e.first,
            });
        }
        if e.first_anchor != self.anchor {
            return Err(ApplyErr::AnchorMismatch);
        }
        // -- validate --
        for (&sid, t) in &e.heads {
            let cur = self.head_count(sid);
            if cur != t.first_prior {
                return Err(ApplyErr::HeadPath {
                    stream_id: sid,
                    state_head: cur,
                    effect_first: t.first_prior,
                });
            }
        }
        for (&n, &i) in &e.registry {
            if let Some(&i0) = self.registry.get(&n)
                && i0 != i
            {
                return Err(ApplyErr::RegistryConflict { name: n, id: i });
            }
            if let Some(&n0) = self.registry_rev.get(&i)
                && n0 != n
            {
                return Err(ApplyErr::RegistryConflict { name: n, id: i });
            }
        }
        for (&s, t) in &e.alloc {
            let cur = self.alloc.get(&s).copied().unwrap_or(0);
            if t.first < cur {
                return Err(ApplyErr::AllocRegression { slot: s });
            }
        }
        // -- mutate --
        for (&sid, t) in &e.heads {
            self.set_head(sid, t.last_head);
        }
        for (&sid, &(v, r)) in &e.snapshots {
            self.set_snapshot(sid, v, r);
        }
        for (&(p, s), &pos) in &e.frontiers {
            // Frontier bottom: 0 == absent — never CREATE an entry for it
            // (entry creation is digest-visible but value-invisible to
            // dirty tracking; see oracle.rs / REPORT).
            if pos == 0 {
                continue;
            }
            let slot = self.frontiers.entry((p, s)).or_insert(0);
            if pos > *slot {
                *slot = pos;
                self.frontier_dirty = true;
            }
        }
        for (&n, &i) in &e.registry {
            self.registry.insert(n, i);
            self.registry_rev.insert(i, n);
            self.registry_dirty = true;
        }
        for (&s, t) in &e.alloc {
            self.alloc.insert(s, t.last);
            self.alloc_dirty = true;
        }
        if !e.dedupe.is_empty() {
            // Keep the epoch deque position-ordered: anything currently
            // active predates the effect's entries.
            let mut act = std::mem::take(&mut self.active);
            if !act.is_empty() {
                act.sort_unstable();
                let max_pos = act.iter().map(|&(_, p)| p).max().unwrap_or(0);
                self.epochs.push_back(FrozenEpoch { max_pos, entries: act });
            }
            for ep in &e.dedupe {
                self.epochs.push_back(ep.clone());
            }
            self.dedupe_dirty = true;
        }
        self.cursor = e.last;
        self.anchor = e.last_anchor;
        self.expire_dedupe();
        Ok(())
    }
}
