//! The microblock arena (design §8, research/03 §3): candidates F1/F2/F3.
//!
//! One `MicroIndex` is one *generation* (one active segment's worth of
//! pointer state). Structure:
//!
//! - **Arena**: append-only slabs (chunks) of fixed 32-entry
//!   [`PtrMicroblock`]s, addressed by a `u32` BlockId. Blocks never move and
//!   never mutate below their `published` count.
//! - **Tail table**: a direct dense `stream_id -> tail BlockId` table
//!   (chunked, grow-only, `AtomicU32` cells) — no hashing, no shard lock.
//! - **Entries**: 16-B delta-encoded [`PackedEntry`] (research/03 §3.1
//!   `BatchPtr`) against per-block base values, with full-width **escape
//!   entries** in an overflow arena when a delta does not fit (or the batch
//!   belongs to a different segment than this generation's).
//! - **Publication** (research/03 §3.2): the single writer writes the entry
//!   bytes, then release-stores `published = n+1`; a new block is built
//!   privately (header incl. `previous`/`skip` links), then its id is
//!   release-stored into the stream's tail cell. Readers acquire-load tails /
//!   `published` and only ever dereference slots below the count.
//! - **Search** (research/03 §3.3): within a block, linear scan (F1) or
//!   binary search (F2); across blocks, walk `previous`, optionally hopping
//!   the per-stream skip chain anchored every [`SKIP_EVERY`] blocks (F3).
//! - **Watermark**: identical D7 clamp semantics to the incumbent
//!   `ActiveIndex` — a single monotone `applied_end` published with
//!   `Release` after the group's entries, and every read clamps to it.
//! - **Global** (design §8.4): a sparse batch-offset checkpoint array for
//!   position→offset seeks, replacing the incumbent's full
//!   `Vec<GlobalEntry>` (in the composed design the segment itself is the
//!   canonical global order; the array only seeds the seek).
//!
//! Reclamation (design §8.5 / research/03 §3.4) is *generational*: the whole
//! `MicroIndex` is dropped as a unit once its segment seals and reader
//! leases drain (`Arc` generation handles — see `tests/reclaim.rs` and the
//! reclaim bench). The `canary` field lets every read assert
//! no-use-after-free.

use crate::shim::{AtomicU32, AtomicU64, OnceCell, Ordering::*};
use mess_index::{BatchEntry, EventPtr, StreamEntry};

pub const NONE: u32 = u32::MAX;
/// Skip-chain anchor period (design §8.3: "every 8 microblocks").
pub const SKIP_EVERY: u32 = 8;

// Loom needs a tiny state space; production sizes otherwise.
#[cfg(not(loom))]
pub const ENTRIES: usize = 32;
#[cfg(loom)]
pub const ENTRIES: usize = 2;

#[cfg(not(loom))]
const CHUNK_BLOCKS: usize = 1024;
#[cfg(loom)]
const CHUNK_BLOCKS: usize = 2;
#[cfg(not(loom))]
const MAX_CHUNKS: usize = 16_384; // 16.7M blocks = ~537M batches
#[cfg(loom)]
const MAX_CHUNKS: usize = 4;

#[cfg(not(loom))]
const TAIL_CHUNK: usize = 8192;
#[cfg(loom)]
const TAIL_CHUNK: usize = 4;
#[cfg(not(loom))]
const MAX_TAIL_CHUNKS: usize = 4096; // 33.5M streams
#[cfg(loom)]
const MAX_TAIL_CHUNKS: usize = 2;

#[cfg(not(loom))]
const OVF_CHUNK: usize = 4096;
#[cfg(loom)]
const OVF_CHUNK: usize = 2;
#[cfg(not(loom))]
const MAX_OVF_CHUNKS: usize = 8192;
#[cfg(loom)]
const MAX_OVF_CHUNKS: usize = 2;

#[cfg(not(loom))]
const GLB_CHUNK: usize = 8192;
#[cfg(loom)]
const GLB_CHUNK: usize = 4;
#[cfg(not(loom))]
const MAX_GLB_CHUNKS: usize = 8192;
#[cfg(loom)]
const MAX_GLB_CHUNKS: usize = 2;

/// The 16-byte delta entry (research/03 §3.1 `BatchPtr`). An **escape**
/// entry has `frame_count == u32::MAX` and `first_version_delta` holding the
/// overflow-arena index of the full-width [`StreamEntry`].
#[derive(Debug, Clone, Copy, Default)]
pub struct PackedEntry {
    first_version_delta: u32,
    frame_count:         u32,
    first_global_delta:  u32,
    offset_delta:        u32,
}

const ESCAPE: u32 = u32::MAX;

/// Immutable block header: written by the single writer *before* the block
/// id is published (via a tail store or a successor's `previous`/`skip`
/// link), never touched again.
#[derive(Debug, Clone, Copy)]
pub struct BlockHeader {
    pub stream_id:    u64,
    pub base_version: u64,
    pub base_global:  u64,
    pub base_offset:  u64,
    pub previous:     u32,
    /// Nearest ancestor block whose `depth % SKIP_EVERY == 0` (the skip
    /// chain — stride ≈ SKIP_EVERY blocks once past the first anchor).
    pub skip:         u32,
    pub depth:        u32,
}

/// Fixed-capacity per-stream pointer block (design §8.2).
pub struct PtrMicroblock {
    header:    OnceCell<BlockHeader>,
    /// Count of readable entries. Release-stored after each entry write;
    /// acquire-loaded by readers. Monotone within a block's life.
    published: AtomicU32,
    entries:   [OnceCell<PackedEntry>; ENTRIES],
}

impl PtrMicroblock {
    fn new() -> Self {
        PtrMicroblock {
            header:    OnceCell::new(BlockHeader {
                stream_id:    0,
                base_version: 0,
                base_global:  0,
                base_offset:  0,
                previous:     NONE,
                skip:         NONE,
                depth:        0,
            }),
            published: AtomicU32::new(0),
            entries:   std::array::from_fn(|_| OnceCell::new(PackedEntry::default())),
        }
    }
}

/// Grow-only chunked storage with a preallocated directory of atomically
/// published chunk pointers. Readers acquire-load a chunk pointer; the
/// writer allocates chunks on demand and release-stores them. Chunks never
/// move (stable addresses for the life of the generation).
struct ChunkDir<T> {
    dir:        Box<[crate::shim::AtomicPtr<T>]>,
    chunk_len:  usize,
    /// Bytes allocated in chunks (writer-maintained, Relaxed) — the
    /// analytic-memory accounting.
    alloc_bytes: AtomicU64,
}

impl<T> ChunkDir<T> {
    fn new(max_chunks: usize, chunk_len: usize) -> Self {
        ChunkDir {
            dir:         (0..max_chunks)
                .map(|_| crate::shim::AtomicPtr::new(std::ptr::null_mut()))
                .collect(),
            chunk_len,
            alloc_bytes: AtomicU64::new(0),
        }
    }

    /// Writer: chunk base pointer for `chunk_idx`, allocating (with `init`)
    /// if absent.
    fn get_or_alloc(&self, chunk_idx: usize, init: impl Fn() -> T) -> *mut T {
        assert!(chunk_idx < self.dir.len(), "chunk directory exhausted");
        let p = self.dir[chunk_idx].load(Acquire);
        if !p.is_null() {
            return p;
        }
        let chunk: Box<[T]> = (0..self.chunk_len).map(|_| init()).collect();
        let base = Box::into_raw(chunk) as *mut T;
        self.alloc_bytes.fetch_add(
            (self.chunk_len * std::mem::size_of::<T>()) as u64,
            Relaxed,
        );
        // Publish the chunk before any element in it is ever referenced.
        self.dir[chunk_idx].store(base, Release);
        base
    }

    /// Reader: chunk base pointer, or null if never allocated.
    #[inline(always)]
    fn get(&self, chunk_idx: usize) -> *mut T {
        self.dir[chunk_idx].load(Acquire)
    }

    fn alloc_bytes(&self) -> u64 {
        self.alloc_bytes.load(Relaxed) + (self.dir.len() * std::mem::size_of::<crate::shim::AtomicPtr<T>>()) as u64
    }
}

impl<T> Drop for ChunkDir<T> {
    fn drop(&mut self) {
        for slot in self.dir.iter() {
            let p = slot.load(Relaxed);
            if !p.is_null() {
                unsafe {
                    drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                        p,
                        self.chunk_len,
                    )));
                }
            }
        }
    }
}

/// Aggregate stats for the report (escape rate, block occupancy, footprint).
#[derive(Debug, Clone, Copy, Default)]
pub struct MicroStats {
    pub batches:     u64,
    pub blocks:      u64,
    pub escapes:     u64,
    pub checkpoints: u64,
    pub alloc_bytes: u64,
}

const CANARY_ALIVE: u64 = 0xC0FF_EE00_C0FF_EE00;
const CANARY_DEAD: u64 = 0xDEAD_DEAD_DEAD_DEAD;

/// One generation of the microblock active index. Single writer
/// (`apply_committed`), any number of concurrent readers. See module doc.
pub struct MicroIndex {
    blocks:   ChunkDir<PtrMicroblock>,
    /// Writer-only bump allocator for BlockIds.
    next_blk: AtomicU32,
    tails:    ChunkDir<AtomicU32>,
    overflow: ChunkDir<OnceCell<StreamEntry>>,
    next_ovf: AtomicU32,
    /// Sparse global batch-offset checkpoints (design §8.4): every
    /// `stride`-th batch records `(first_global_pos, offset)`.
    global:      ChunkDir<OnceCell<(u64, u64)>>,
    glb_count:   AtomicU32,
    stride:      u32,
    batches_seen: AtomicU64,
    escapes:     AtomicU64,
    /// This generation's segment: batches pointing elsewhere use escapes.
    segment_id:  u64,
    /// D7 watermark, identical semantics to `ActiveIndex::applied_end`.
    applied_end: AtomicU64,
    /// Use-after-free tripwire: poisoned in `Drop` before the slabs free.
    canary:      AtomicU64,
}

impl MicroIndex {
    pub fn new(segment_id: u64, global_stride: u32) -> Self {
        assert!(global_stride >= 1);
        MicroIndex {
            blocks:       ChunkDir::new(MAX_CHUNKS, CHUNK_BLOCKS),
            next_blk:     AtomicU32::new(0),
            tails:        ChunkDir::new(MAX_TAIL_CHUNKS, TAIL_CHUNK),
            overflow:     ChunkDir::new(MAX_OVF_CHUNKS, OVF_CHUNK),
            next_ovf:     AtomicU32::new(0),
            global:       ChunkDir::new(MAX_GLB_CHUNKS, GLB_CHUNK),
            glb_count:    AtomicU32::new(0),
            stride:       global_stride,
            batches_seen: AtomicU64::new(0),
            escapes:      AtomicU64::new(0),
            segment_id,
            applied_end:  AtomicU64::new(0),
            canary:       AtomicU64::new(CANARY_ALIVE),
        }
    }

    /// The reclamation invariant, checked by readers in the reclaim
    /// tests/bench: this generation's slabs are still alive.
    #[inline(always)]
    pub fn check_canary(&self) {
        assert_eq!(
            self.canary.load(Acquire),
            CANARY_ALIVE,
            "use-after-free: generation slab read after retirement"
        );
    }

    #[inline(always)]
    fn block(&self, id: u32) -> &PtrMicroblock {
        let chunk = self.blocks.get(id as usize / CHUNK_BLOCKS);
        debug_assert!(!chunk.is_null());
        unsafe { &*chunk.add(id as usize % CHUNK_BLOCKS) }
    }

    #[inline]
    pub fn applied_end(&self) -> u64 {
        self.applied_end.load(Acquire)
    }

    pub fn stats(&self) -> MicroStats {
        MicroStats {
            batches:     self.batches_seen.load(Relaxed),
            blocks:      u64::from(self.next_blk.load(Relaxed)),
            escapes:     self.escapes.load(Relaxed),
            checkpoints: u64::from(self.glb_count.load(Relaxed)),
            alloc_bytes: self.blocks.alloc_bytes()
                + self.tails.alloc_bytes()
                + self.overflow.alloc_bytes()
                + self.global.alloc_bytes(),
        }
    }

    // ------------------------------------------------------------------
    // Writer side (single writer — the committer).
    // ------------------------------------------------------------------

    /// Allocate + privately initialize a new block whose first entry is `b`.
    /// Returns the new BlockId; the caller publishes it (tail store).
    fn new_block(&self, b: &BatchEntry, previous: u32) -> u32 {
        let id = self.next_blk.load(Relaxed);
        let chunk = self
            .blocks
            .get_or_alloc(id as usize / CHUNK_BLOCKS, PtrMicroblock::new);
        self.next_blk.store(id + 1, Relaxed);
        let blk = unsafe { &*chunk.add(id as usize % CHUNK_BLOCKS) };

        let (skip, depth) = if previous == NONE {
            (NONE, 0)
        } else {
            let ph = unsafe { self.block(previous).header.read() };
            let skip = if ph.depth % SKIP_EVERY == 0 { previous } else { ph.skip };
            (skip, ph.depth + 1)
        };
        // Private init: nothing can reach `id` until the caller's release
        // store of the tail (or a successor's header) publishes it.
        unsafe {
            blk.header.write(BlockHeader {
                stream_id:    b.stream_id,
                base_version: b.first_stream_version,
                base_global:  b.first_global_pos,
                base_offset:  b.ptr.offset,
                previous,
                skip,
                depth,
            });
            blk.entries[0].write(self.encode(blk, b));
        }
        blk.published.store(1, Release);
        id
    }

    /// Delta-encode `b` against `blk`'s bases, or spill a full-width escape
    /// entry into the overflow arena.
    fn encode(&self, blk: &PtrMicroblock, b: &BatchEntry) -> PackedEntry {
        let hdr = unsafe { blk.header.read() };
        let vd = b.first_stream_version.wrapping_sub(hdr.base_version);
        let gd = b.first_global_pos.wrapping_sub(hdr.base_global);
        let od = b.ptr.offset.wrapping_sub(hdr.base_offset);
        let fits = b.ptr.segment_id == self.segment_id
            && b.frame_count < ESCAPE
            && b.first_stream_version >= hdr.base_version
            && vd <= u32::MAX as u64
            && b.first_global_pos >= hdr.base_global
            && gd <= u32::MAX as u64
            && b.ptr.offset >= hdr.base_offset
            && od <= u32::MAX as u64;
        if fits {
            return PackedEntry {
                first_version_delta: vd as u32,
                frame_count:         b.frame_count,
                first_global_delta:  gd as u32,
                offset_delta:        od as u32,
            };
        }
        // Escape: full-width copy in the overflow arena. Visibility rides
        // the block's `published` release store (the slot is written before
        // the entry referencing it is published).
        let idx = self.next_ovf.load(Relaxed);
        let chunk = self.overflow.get_or_alloc(idx as usize / OVF_CHUNK, || {
            OnceCell::new(StreamEntry {
                first_version:    0,
                frame_count:      0,
                first_global_pos: 0,
                ptr:              EventPtr { segment_id: 0, offset: 0 },
            })
        });
        unsafe {
            (*chunk.add(idx as usize % OVF_CHUNK)).write(StreamEntry {
                first_version:    b.first_stream_version,
                frame_count:      b.frame_count,
                first_global_pos: b.first_global_pos,
                ptr:              b.ptr,
            });
        }
        self.next_ovf.store(idx + 1, Relaxed);
        self.escapes.fetch_add(1, Relaxed);
        PackedEntry {
            first_version_delta: idx,
            frame_count:         ESCAPE,
            first_global_delta:  0,
            offset_delta:        0,
        }
    }

    /// Writer: tail cell for `sid`, allocating the tail chunk on demand.
    fn tail_cell_or_alloc(&self, sid: u64) -> &AtomicU32 {
        let chunk = self
            .tails
            .get_or_alloc(sid as usize / TAIL_CHUNK, || AtomicU32::new(NONE));
        unsafe { &*chunk.add(sid as usize % TAIL_CHUNK) }
    }

    /// Reader: tail cell for `sid`, or None if the chunk was never touched.
    #[inline(always)]
    fn tail_cell(&self, sid: u64) -> Option<&AtomicU32> {
        let ci = sid as usize / TAIL_CHUNK;
        if ci >= MAX_TAIL_CHUNKS {
            return None;
        }
        let chunk = self.tails.get(ci);
        if chunk.is_null() {
            return None;
        }
        Some(unsafe { &*chunk.add(sid as usize % TAIL_CHUNK) })
    }

    /// **Apply one group of committed batches** — same contract, same D7
    /// discipline, and the same defensive watermark skip as
    /// `ActiveIndex::apply_committed`.
    pub fn apply_committed(&self, watermark: u64, batches: &[BatchEntry]) {
        for b in batches {
            debug_assert!(
                b.end_pos() <= watermark,
                "D7: batch ending at {} above watermark {}",
                b.end_pos(),
                watermark
            );
            if b.end_pos() > watermark {
                continue; // defensive, mirrors the incumbent
            }

            // Global sparse checkpoint (design §8.4).
            let seen = self.batches_seen.load(Relaxed);
            if seen % u64::from(self.stride) == 0 {
                let n = self.glb_count.load(Relaxed);
                let chunk = self
                    .global
                    .get_or_alloc(n as usize / GLB_CHUNK, || OnceCell::new((0, 0)));
                unsafe {
                    (*chunk.add(n as usize % GLB_CHUNK))
                        .write((b.first_global_pos, b.ptr.offset));
                }
                self.glb_count.store(n + 1, Release);
            }
            self.batches_seen.store(seen + 1, Relaxed);

            // Stream side.
            let cell = self.tail_cell_or_alloc(b.stream_id);
            let tail = cell.load(Relaxed); // single writer owns tail stores
            if tail == NONE {
                let id = self.new_block(b, NONE);
                cell.store(id, Release);
                continue;
            }
            let blk = self.block(tail);
            let n = blk.published.load(Relaxed) as usize; // writer-owned
            debug_assert!({
                let hdr = unsafe { blk.header.read() };
                hdr.stream_id == b.stream_id
            });
            if n < ENTRIES {
                unsafe { blk.entries[n].write(self.encode(blk, b)) };
                blk.published.store(n as u32 + 1, Release);
            } else {
                let id = self.new_block(b, tail);
                cell.store(id, Release);
            }
        }
        // Publish LAST (Release): readers that observe the new watermark see
        // every entry above. Single writer, so load+store max is race-free.
        let cur = self.applied_end.load(Relaxed);
        if watermark > cur {
            self.applied_end.store(watermark, Release);
        }
    }

    // ------------------------------------------------------------------
    // Reader side.
    // ------------------------------------------------------------------

    /// Decode published entry `i` of `blk` (i < published, acquired).
    #[inline(always)]
    fn decode(&self, hdr: &BlockHeader, e: PackedEntry) -> StreamEntry {
        if e.frame_count == ESCAPE {
            let chunk = self.overflow.get(e.first_version_delta as usize / OVF_CHUNK);
            debug_assert!(!chunk.is_null());
            return unsafe {
                (*chunk.add(e.first_version_delta as usize % OVF_CHUNK)).read()
            };
        }
        StreamEntry {
            first_version:    hdr.base_version + u64::from(e.first_version_delta),
            frame_count:      e.frame_count,
            first_global_pos: hdr.base_global + u64::from(e.first_global_delta),
            ptr:              EventPtr {
                segment_id: self.segment_id,
                offset:     hdr.base_offset + u64::from(e.offset_delta),
            },
        }
    }

    /// The committed head version of `sid` (clamped to `applied_end`) —
    /// mirror of `ActiveIndex::stream_head`.
    pub fn stream_head(&self, sid: u64) -> Option<u64> {
        let w = self.applied_end.load(Acquire);
        let mut id = self.tail_cell(sid)?.load(Acquire);
        if id == NONE {
            return None;
        }
        loop {
            let blk = self.block(id);
            let hdr = unsafe { blk.header.read() };
            let n = blk.published.load(Acquire) as usize;
            for i in (0..n).rev() {
                let e = self.decode(&hdr, unsafe { blk.entries[i].read() });
                if e.end_pos() <= w {
                    return Some(e.last_version());
                }
            }
            if hdr.previous == NONE {
                return None;
            }
            id = hdr.previous;
        }
    }

    /// Resolve `(sid, version)` to its batch's [`EventPtr`] iff committed —
    /// mirror of `ActiveIndex::resolve`. `BINARY` selects binary search
    /// within a block (F2); `SKIP` walks the skip chain across blocks (F3).
    #[inline(always)]
    fn resolve_impl<const BINARY: bool, const SKIP: bool>(
        &self,
        sid: u64,
        version: u64,
    ) -> Option<EventPtr> {
        let w = self.applied_end.load(Acquire);
        let mut id = self.tail_cell(sid)?.load(Acquire);
        if id == NONE {
            return None;
        }
        loop {
            let blk = self.block(id);
            let hdr = unsafe { blk.header.read() };
            if version >= hdr.base_version {
                // Candidate block: the last published entry with
                // first_version <= version is the candidate batch (the same
                // partition-point rule as the incumbent).
                let n = blk.published.load(Acquire) as usize;
                debug_assert!(n >= 1, "reachable block has >= 1 entry");
                let e = if BINARY {
                    // Largest i with first_version(i) <= version.
                    let (mut lo, mut hi) = (0usize, n); // invariant: fv(lo-1) <= version < fv(hi)
                    while lo < hi {
                        let mid = (lo + hi) / 2;
                        let e = self.decode(&hdr, unsafe { blk.entries[mid].read() });
                        if e.first_version <= version {
                            lo = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                    // lo >= 1 because entry 0 has first_version == base <= version.
                    self.decode(&hdr, unsafe { blk.entries[lo - 1].read() })
                } else {
                    let mut found = None;
                    for i in (0..n).rev() {
                        let e = self.decode(&hdr, unsafe { blk.entries[i].read() });
                        if e.first_version <= version {
                            found = Some(e);
                            break;
                        }
                    }
                    found?
                };
                return if e.contains_version(version) && e.end_pos() <= w {
                    Some(e.ptr)
                } else {
                    None
                };
            }
            // Descend. Skip chain: hop anchors while they are still above
            // the target (never overshoots — a hop lands on an ancestor
            // whose base is still > version, so the containing block is
            // further down).
            if SKIP && hdr.skip != NONE {
                let s = self.block(hdr.skip);
                let sh = unsafe { s.header.read() };
                if sh.base_version > version {
                    id = hdr.skip;
                    continue;
                }
            }
            if hdr.previous == NONE {
                return None;
            }
            id = hdr.previous;
        }
    }

    pub fn resolve_f1(&self, sid: u64, version: u64) -> Option<EventPtr> {
        self.resolve_impl::<false, false>(sid, version)
    }
    pub fn resolve_f2(&self, sid: u64, version: u64) -> Option<EventPtr> {
        self.resolve_impl::<true, false>(sid, version)
    }
    pub fn resolve_f3(&self, sid: u64, version: u64) -> Option<EventPtr> {
        self.resolve_impl::<false, true>(sid, version)
    }

    /// Paged committed entries at/after `from_version` — mirror of
    /// `ActiveIndex::stream_entries_from` (bn-2ib page semantics, identical
    /// covered-events accounting). `SKIP` accelerates the descent past
    /// blocks wholly above the page.
    pub fn stream_entries_from<const SKIP: bool>(
        &self,
        sid: u64,
        from_version: u64,
        max_events: usize,
    ) -> Vec<StreamEntry> {
        let w = self.applied_end.load(Acquire);
        let Some(cell) = self.tail_cell(sid) else {
            return Vec::new();
        };
        let mut id = cell.load(Acquire);
        if id == NONE {
            return Vec::new();
        }
        let page_end = from_version.saturating_add(max_events as u64);

        // Descend from the tail, recording blocks that can intersect the
        // page [from_version, page_end). Blocks with base >= page_end are
        // above the page (versions are dense per stream, so coverage below
        // them already fills the page); blocks below the one containing
        // `from_version` are wholly before it.
        let mut ids: Vec<u32> = Vec::new();
        loop {
            let blk = self.block(id);
            let hdr = unsafe { blk.header.read() };
            if hdr.base_version >= page_end && page_end > from_version {
                // Wholly above the page: hop past without recording.
                if SKIP && hdr.skip != NONE {
                    let sh = unsafe { self.block(hdr.skip).header.read() };
                    if sh.base_version >= page_end {
                        id = hdr.skip;
                        continue;
                    }
                }
                if hdr.previous == NONE {
                    break;
                }
                id = hdr.previous;
                continue;
            }
            ids.push(id);
            if hdr.base_version <= from_version || hdr.previous == NONE {
                break;
            }
            id = hdr.previous;
        }

        // Emit in ascending order with the incumbent's exact accounting.
        let mut out = Vec::new();
        let mut covered = 0usize;
        'outer: for &bid in ids.iter().rev() {
            let blk = self.block(bid);
            let hdr = unsafe { blk.header.read() };
            let n = blk.published.load(Acquire) as usize;
            for i in 0..n {
                let e = self.decode(&hdr, unsafe { blk.entries[i].read() });
                if e.end_pos() > w {
                    break 'outer; // uncommitted suffix (end_pos ascends)
                }
                if e.last_version() < from_version {
                    continue;
                }
                if covered >= max_events {
                    break 'outer;
                }
                out.push(e);
                let lo = from_version.max(e.first_version);
                covered += (e.last_version() - lo + 1) as usize;
            }
        }
        out
    }

    /// All committed entries of `sid` (differential-test convenience).
    pub fn stream_entries(&self, sid: u64) -> Vec<StreamEntry> {
        self.stream_entries_from::<false>(sid, 0, usize::MAX)
    }

    /// Global seek (design §8.4): the last checkpoint at or below `pos`,
    /// as `(first_global_pos, offset)`. Caller must clamp `pos` below
    /// [`applied_end`] (dense global order means every batch starting at or
    /// below a committed position is itself committed — groups commit whole
    /// batches). With `stride == 1` this is exactly the containing batch;
    /// with a sparse stride the composed engine scans forward at most
    /// `stride - 1` batch headers in the (page-cached) segment itself.
    pub fn global_seek(&self, pos: u64) -> Option<(u64, u64)> {
        let n = self.glb_count.load(Acquire) as usize;
        if n == 0 {
            return None;
        }
        let read = |i: usize| -> (u64, u64) {
            let chunk = self.global.get(i / GLB_CHUNK);
            debug_assert!(!chunk.is_null());
            unsafe { (*chunk.add(i % GLB_CHUNK)).read() }
        };
        // Largest i with ckpt(i).pos <= pos.
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if read(mid).0 <= pos {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 { None } else { Some(read(lo - 1)) }
    }
}

impl Drop for MicroIndex {
    fn drop(&mut self) {
        // Poison before the ChunkDir fields free their slabs: a racing
        // reader that (incorrectly) still holds a reference trips the canary
        // rather than silently reading freed memory.
        self.canary.store(CANARY_DEAD, Release);
    }
}
