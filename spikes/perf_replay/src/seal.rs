//! The seal pass (D5/D6) and the sealed-segment reader — BASELINE code,
//! copied from spikes/seal_pipeline/src/seal.rs. Optimized variants live in
//! opt.rs so baseline vs optimized stays an explicit side-by-side.
//!
//! Two deliberate deltas vs seal_pipeline (both inert for baseline timings):
//!
//! 1. File format v2 adds an INTRA-BLOCK SKIP TABLE section for pointer
//!    blocks: every SKIP_K-th eslot is stored absolutely (eslot u64 +
//!    varint-stream byte offset u32) so a point read seeks in O(SKIP_K)
//!    instead of O(version). The baseline `point_read` below IGNORES it and
//!    still does the linear varint seek; only `opt::point_read_skip` uses it.
//!    The skip section sits between ptrs and dir, so block/ptr layout and all
//!    baseline read paths are byte-for-byte what seal_pipeline produced.
//! 2. `SealedReader` additionally mmaps the file (used only by opt.rs paths;
//!    baseline paths keep using preads).
//!
//! Sealed file layout (v2):
//!
//! ```text
//! magic u64
//! blocks:      concatenated zstd-compressed blocks (dict per category)
//!              uncompressed block = (count+1) x u32 LE offsets ++ payloads
//! ptrs:        per-stream varint delta-encoded eslot lists
//! skips:       per-stream skip entries: eslot u64, ptr byte off u32
//!              (entry j covers eslot index (j+1)*SKIP_K)
//! stream dir:  u32 count, then per stream: stream_id u64, count u32,
//!              ptr_off u32, ptr_len u32, skip_off u32, n_skips u32
//! block index: u32 count, then per block: file_off u64, comp_len u32,
//!              uncomp_len u32, count u16, dict_id u8      (offset table)
//! dicts:       u32 count, then per dict: u32 len + bytes
//! filters:     4 x (u32 len + postcard bytes): stream BinaryFuse8,
//!              stream BinaryFuse16, category BinaryFuse8, category BinaryFuse16
//! footer:      10 x u64: ptrs_off, skips_off, dir_off, bidx_off, dicts_off,
//!              filters_off, n_events, n_streams, block_events, magic
//! ```

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::num::NonZeroUsize;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lru::LruCache;
use memmap2::Mmap;
use xorf::{BinaryFuse16, BinaryFuse8};

use crate::seglog::{rd_u16, rd_u32, rd_u64, ReplayStats};
use crate::workload::{category_of, CATEGORIES};

pub const BLOCK_EVENTS: usize = 128;
pub const DICT_MAX_BYTES: usize = 16 * 1024;
pub const DICT_SAMPLES: usize = 5_000;
pub const ZSTD_LEVEL: i32 = 3;
pub const FILE_MAGIC: u64 = 0x5EA1_ED00_0000_0002;
pub const SKIP_K: usize = 64;
const FOOTER_LEN: u64 = 10 * 8;
/// Coalesce block preads when the file gap between them is at most this.
const COALESCE_GAP: u64 = 64 * 1024;
/// Cap a single coalesced pread.
const MAX_READ: u64 = 8 << 20;
/// Blocks fetched per coalescing round during stream replay.
const FETCH_CHUNK: usize = 64;

// ---------------------------------------------------------------------------
// Active-index input
// ---------------------------------------------------------------------------

/// Per-event active index value: global position + payload location in the
/// active segment.
#[derive(Clone, Copy, Debug)]
pub struct EvRef {
    pub pos: u32,
    pub off: u64,
    pub len: u32,
}

/// The per-event active index: (stream, version) -> EvRef.
pub type ActiveIndex = BTreeMap<(u64, u64), EvRef>;

pub struct StreamEvents {
    pub stream: u64,
    pub evs: Vec<EvRef>, // version order (== log order per stream)
}

/// Seal stage (a): consolidate per-event entries into per-stream runs.
pub fn consolidate(index: &ActiveIndex) -> Vec<StreamEvents> {
    let mut out: Vec<StreamEvents> = Vec::new();
    for (&(s, _v), &ev) in index {
        match out.last_mut() {
            Some(se) if se.stream == s => se.evs.push(ev),
            _ => out.push(StreamEvents { stream: s, evs: vec![ev] }),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Varint
// ---------------------------------------------------------------------------

pub fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        if v < 0x80 {
            out.push(v as u8);
            return;
        }
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
}

#[inline]
pub fn read_varint(d: &[u8], p: &mut usize) -> u64 {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let b = d[*p];
        *p += 1;
        v |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return v;
        }
        shift += 7;
    }
}

// ---------------------------------------------------------------------------
// Seal stage (b): filters
// ---------------------------------------------------------------------------

pub struct Filters {
    pub s8: BinaryFuse8,
    pub s16: BinaryFuse16,
    pub c8: BinaryFuse8,
    pub c16: BinaryFuse16,
}

// ---------------------------------------------------------------------------
// Seal stage (c): dictionary training + recompression
// ---------------------------------------------------------------------------

pub struct DictOut {
    pub dicts: Vec<Vec<u8>>,
    pub train_times: Vec<Duration>,
}

pub fn train_dicts(seg: &[u8], streams: &[StreamEvents]) -> DictOut {
    let mut dicts = Vec::with_capacity(CATEGORIES);
    let mut train_times = Vec::with_capacity(CATEGORIES);
    for cat in 0..CATEGORIES {
        let mut refs: Vec<&[u8]> = Vec::new();
        for se in streams.iter().filter(|se| category_of(se.stream) == cat) {
            for ev in &se.evs {
                refs.push(&seg[ev.off as usize..(ev.off + ev.len as u64) as usize]);
            }
        }
        // Evenly strided 5k samples across the category.
        let stride = (refs.len() / DICT_SAMPLES).max(1);
        let samples: Vec<&[u8]> = refs.iter().step_by(stride).take(DICT_SAMPLES).copied().collect();
        let t = Instant::now();
        let dict = zstd::dict::from_samples(&samples, DICT_MAX_BYTES).expect("dict training");
        train_times.push(t.elapsed());
        dicts.push(dict);
    }
    DictOut { dicts, train_times }
}

#[derive(Clone, Copy, Debug)]
pub struct BlockMeta {
    pub file_off: u64,
    pub comp_len: u32,
    pub uncomp_len: u32,
    pub count: u16,
    pub dict_id: u8,
}

pub struct RecompressOut {
    pub blocks: Vec<BlockMeta>,
    /// Parallel to the consolidated `streams` slice: sealed slots per event.
    pub stream_eslots: Vec<Vec<u64>>,
    pub cat_raw: [u64; CATEGORIES],
    pub cat_comp: [u64; CATEGORIES],
    pub cat_blocks: [u64; CATEGORIES],
}

#[allow(clippy::too_many_arguments)]
fn flush_block(
    offsets: &mut Vec<u32>,
    payloads: &mut Vec<u8>,
    cat: usize,
    comp: &mut zstd::bulk::Compressor,
    w: &mut BufWriter<File>,
    file_off: &mut u64,
    blocks: &mut Vec<BlockMeta>,
    cat_comp: &mut [u64; CATEGORIES],
    cat_blocks: &mut [u64; CATEGORIES],
) {
    let count = offsets.len() - 1;
    let mut raw = Vec::with_capacity(4 * offsets.len() + payloads.len());
    for &o in offsets.iter() {
        raw.extend_from_slice(&o.to_le_bytes());
    }
    raw.extend_from_slice(payloads);
    let cbytes = comp.compress(&raw).unwrap();
    w.write_all(&cbytes).unwrap();
    blocks.push(BlockMeta {
        file_off: *file_off,
        comp_len: cbytes.len() as u32,
        uncomp_len: raw.len() as u32,
        count: count as u16,
        dict_id: cat as u8,
    });
    *file_off += cbytes.len() as u64;
    cat_comp[cat] += cbytes.len() as u64;
    cat_blocks[cat] += 1;
    offsets.clear();
    offsets.push(0);
    payloads.clear();
}

/// Rewrite payloads into ~128-event category-homogeneous compressed blocks,
/// streaming them into `w` (which is positioned at `file_off0`). BASELINE:
/// single-threaded, one reused compressor per category.
pub fn recompress(
    seg: &[u8],
    streams: &[StreamEvents],
    dicts: &[Vec<u8>],
    w: &mut BufWriter<File>,
    file_off0: u64,
) -> RecompressOut {
    let mut comps: Vec<zstd::bulk::Compressor> = dicts
        .iter()
        .map(|d| zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, d).unwrap())
        .collect();
    let mut blocks: Vec<BlockMeta> = Vec::new();
    let mut stream_eslots: Vec<Vec<u64>> =
        streams.iter().map(|se| Vec::with_capacity(se.evs.len())).collect();
    let mut cat_raw = [0u64; CATEGORIES];
    let mut cat_comp = [0u64; CATEGORIES];
    let mut cat_blocks = [0u64; CATEGORIES];
    let mut file_off = file_off0;

    let mut offsets: Vec<u32> = Vec::with_capacity(BLOCK_EVENTS + 1);
    let mut payloads: Vec<u8> = Vec::with_capacity(BLOCK_EVENTS * 320);
    for cat in 0..CATEGORIES {
        offsets.clear();
        offsets.push(0);
        payloads.clear();
        for (si, se) in streams.iter().enumerate() {
            if category_of(se.stream) != cat {
                continue;
            }
            for ev in &se.evs {
                if offsets.len() - 1 == BLOCK_EVENTS {
                    flush_block(
                        &mut offsets, &mut payloads, cat, &mut comps[cat], w, &mut file_off,
                        &mut blocks, &mut cat_comp, &mut cat_blocks,
                    );
                }
                let eslot =
                    blocks.len() as u64 * BLOCK_EVENTS as u64 + (offsets.len() - 1) as u64;
                stream_eslots[si].push(eslot);
                payloads.extend_from_slice(
                    &seg[ev.off as usize..(ev.off + ev.len as u64) as usize],
                );
                offsets.push(payloads.len() as u32);
                cat_raw[cat] += ev.len as u64;
            }
        }
        if offsets.len() > 1 {
            flush_block(
                &mut offsets, &mut payloads, cat, &mut comps[cat], w, &mut file_off,
                &mut blocks, &mut cat_comp, &mut cat_blocks,
            );
        }
    }
    RecompressOut { blocks, stream_eslots, cat_raw, cat_comp, cat_blocks }
}

// ---------------------------------------------------------------------------
// Seal stage (d): metadata sections + footer
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone, Copy)]
pub struct Sections {
    pub blocks: u64,
    pub ptrs: u64,
    pub skips: u64,
    pub dir: u64,
    pub bidx: u64,
    pub dicts: u64,
    pub filters: u64,
    pub footer: u64,
    pub total: u64,
}

pub fn write_metadata(
    w: &mut BufWriter<File>,
    blocks_end: u64, // file offset right after the last block
    streams: &[StreamEvents],
    stream_eslots: &[Vec<u64>],
    blocks: &[BlockMeta],
    dicts: &[Vec<u8>],
    filters: &Filters,
    n_events: u64,
) -> Sections {
    let mut sections = Sections { blocks: blocks_end - 8, ..Default::default() };

    // Pointer blocks + skip tables (v2: every SKIP_K-th eslot absolute).
    let mut ptr_data: Vec<u8> = Vec::new();
    let mut skip_data: Vec<u8> = Vec::new();
    let mut dir_entries: Vec<(u64, u32, u32, u32, u32, u32)> = Vec::with_capacity(streams.len());
    for (si, se) in streams.iter().enumerate() {
        let start = ptr_data.len();
        let skip_start = skip_data.len();
        let es = &stream_eslots[si];
        write_varint(&mut ptr_data, es[0]);
        for i in 1..es.len() {
            write_varint(&mut ptr_data, es[i] - es[i - 1]);
            if i % SKIP_K == 0 {
                // After decoding varint #i the cursor is at (ptr_data.len() -
                // start); store (eslot value at index i, that byte offset).
                skip_data.extend(es[i].to_le_bytes());
                skip_data.extend(((ptr_data.len() - start) as u32).to_le_bytes());
            }
        }
        let n_skips = ((skip_data.len() - skip_start) / 12) as u32;
        dir_entries.push((
            se.stream,
            es.len() as u32,
            start as u32,
            (ptr_data.len() - start) as u32,
            skip_start as u32,
            n_skips,
        ));
    }
    let ptrs_off = blocks_end;
    w.write_all(&ptr_data).unwrap();
    sections.ptrs = ptr_data.len() as u64;

    // Skip section.
    let skips_off = ptrs_off + sections.ptrs;
    w.write_all(&skip_data).unwrap();
    sections.skips = skip_data.len() as u64;

    // Stream directory.
    let dir_off = skips_off + sections.skips;
    let mut buf: Vec<u8> = Vec::with_capacity(4 + dir_entries.len() * 28);
    buf.extend((dir_entries.len() as u32).to_le_bytes());
    for &(s, count, off, len, skip_off, n_skips) in &dir_entries {
        buf.extend(s.to_le_bytes());
        buf.extend(count.to_le_bytes());
        buf.extend(off.to_le_bytes());
        buf.extend(len.to_le_bytes());
        buf.extend(skip_off.to_le_bytes());
        buf.extend(n_skips.to_le_bytes());
    }
    w.write_all(&buf).unwrap();
    sections.dir = buf.len() as u64;

    // Block index (the block -> file offset table).
    let bidx_off = dir_off + sections.dir;
    let mut buf: Vec<u8> = Vec::with_capacity(4 + blocks.len() * 19);
    buf.extend((blocks.len() as u32).to_le_bytes());
    for b in blocks {
        buf.extend(b.file_off.to_le_bytes());
        buf.extend(b.comp_len.to_le_bytes());
        buf.extend(b.uncomp_len.to_le_bytes());
        buf.extend(b.count.to_le_bytes());
        buf.push(b.dict_id);
    }
    w.write_all(&buf).unwrap();
    sections.bidx = buf.len() as u64;

    // Dictionaries.
    let dicts_off = bidx_off + sections.bidx;
    let mut buf: Vec<u8> = Vec::new();
    buf.extend((dicts.len() as u32).to_le_bytes());
    for d in dicts {
        buf.extend((d.len() as u32).to_le_bytes());
        buf.extend_from_slice(d);
    }
    w.write_all(&buf).unwrap();
    sections.dicts = buf.len() as u64;

    // Filters (postcard-serialized xorf structs).
    let filters_off = dicts_off + sections.dicts;
    let mut buf: Vec<u8> = Vec::new();
    for ser in [
        postcard::to_allocvec(&filters.s8).unwrap(),
        postcard::to_allocvec(&filters.s16).unwrap(),
        postcard::to_allocvec(&filters.c8).unwrap(),
        postcard::to_allocvec(&filters.c16).unwrap(),
    ] {
        buf.extend((ser.len() as u32).to_le_bytes());
        buf.extend_from_slice(&ser);
    }
    w.write_all(&buf).unwrap();
    sections.filters = buf.len() as u64;

    // Footer.
    let mut buf: Vec<u8> = Vec::with_capacity(FOOTER_LEN as usize);
    for v in [
        ptrs_off,
        skips_off,
        dir_off,
        bidx_off,
        dicts_off,
        filters_off,
        n_events,
        streams.len() as u64,
        BLOCK_EVENTS as u64,
        FILE_MAGIC,
    ] {
        buf.extend(v.to_le_bytes());
    }
    w.write_all(&buf).unwrap();
    sections.footer = FOOTER_LEN;

    w.flush().unwrap();
    w.get_ref().sync_all().unwrap();
    sections.total = filters_off + sections.filters + FOOTER_LEN;
    sections
}

// ---------------------------------------------------------------------------
// Sealed reader
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct DirEntry {
    pub count: u32,
    pub off: u32,
    pub len: u32,
    pub skip_off: u32,
    pub n_skips: u32,
}

pub struct SealedReader {
    pub file: File,
    /// Whole-file mmap; used only by opt.rs read paths.
    pub mmap: Mmap,
    pub blocks: Vec<BlockMeta>,
    pub dir: HashMap<u64, DirEntry>,
    pub stream_ids: Vec<u64>,
    pub ptr_data: Vec<u8>,
    pub skip_data: Vec<u8>,
    pub dicts: Vec<Vec<u8>>,
    pub s8: BinaryFuse8,
    pub s16: BinaryFuse16,
    pub c8: BinaryFuse8,
    pub c16: BinaryFuse16,
    pub n_events: u64,
}

impl SealedReader {
    pub fn open(path: &Path) -> Self {
        let file = File::open(path).unwrap();
        let mmap = unsafe { Mmap::map(&file).unwrap() };
        let flen = file.metadata().unwrap().len();
        let mut foot = vec![0u8; FOOTER_LEN as usize];
        file.read_exact_at(&mut foot, flen - FOOTER_LEN).unwrap();
        let ptrs_off = rd_u64(&foot, 0);
        let skips_off = rd_u64(&foot, 8);
        let dir_off = rd_u64(&foot, 16);
        let bidx_off = rd_u64(&foot, 24);
        let dicts_off = rd_u64(&foot, 32);
        let filters_off = rd_u64(&foot, 40);
        let n_events = rd_u64(&foot, 48);
        assert_eq!(rd_u64(&foot, 64), BLOCK_EVENTS as u64);
        assert_eq!(rd_u64(&foot, 72), FILE_MAGIC);

        let mut meta = vec![0u8; (flen - FOOTER_LEN - ptrs_off) as usize];
        file.read_exact_at(&mut meta, ptrs_off).unwrap();
        let at = |abs: u64| (abs - ptrs_off) as usize;

        let ptr_data = meta[..at(skips_off)].to_vec();
        let skip_data = meta[at(skips_off)..at(dir_off)].to_vec();

        let mut p = at(dir_off);
        let n_streams = rd_u32(&meta, p) as usize;
        p += 4;
        let mut dir = HashMap::with_capacity(n_streams);
        let mut stream_ids = Vec::with_capacity(n_streams);
        for _ in 0..n_streams {
            let s = rd_u64(&meta, p);
            let count = rd_u32(&meta, p + 8);
            let off = rd_u32(&meta, p + 12);
            let len = rd_u32(&meta, p + 16);
            let skip_off = rd_u32(&meta, p + 20);
            let n_skips = rd_u32(&meta, p + 24);
            p += 28;
            dir.insert(s, DirEntry { count, off, len, skip_off, n_skips });
            stream_ids.push(s);
        }

        let mut p = at(bidx_off);
        let n_blocks = rd_u32(&meta, p) as usize;
        p += 4;
        let mut blocks = Vec::with_capacity(n_blocks);
        for _ in 0..n_blocks {
            blocks.push(BlockMeta {
                file_off: rd_u64(&meta, p),
                comp_len: rd_u32(&meta, p + 8),
                uncomp_len: rd_u32(&meta, p + 12),
                count: rd_u16(&meta, p + 16),
                dict_id: meta[p + 18],
            });
            p += 19;
        }

        let mut p = at(dicts_off);
        let n_dicts = rd_u32(&meta, p) as usize;
        p += 4;
        let mut dicts = Vec::with_capacity(n_dicts);
        for _ in 0..n_dicts {
            let len = rd_u32(&meta, p) as usize;
            p += 4;
            dicts.push(meta[p..p + len].to_vec());
            p += len;
        }

        let mut p = at(filters_off);
        let next = |meta: &[u8], p: &mut usize| -> Vec<u8> {
            let len = rd_u32(meta, *p) as usize;
            *p += 4;
            let out = meta[*p..*p + len].to_vec();
            *p += len;
            out
        };
        let s8: BinaryFuse8 = postcard::from_bytes(&next(&meta, &mut p)).unwrap();
        let s16: BinaryFuse16 = postcard::from_bytes(&next(&meta, &mut p)).unwrap();
        let c8: BinaryFuse8 = postcard::from_bytes(&next(&meta, &mut p)).unwrap();
        let c16: BinaryFuse16 = postcard::from_bytes(&next(&meta, &mut p)).unwrap();

        SealedReader {
            file,
            mmap,
            blocks,
            dir,
            stream_ids,
            ptr_data,
            skip_data,
            dicts,
            s8,
            s16,
            c8,
            c16,
            n_events,
        }
    }
}

// ---------------------------------------------------------------------------
// Read context: per-category decompressors + decompressed-block LRU cache
// ---------------------------------------------------------------------------

pub struct ReadCtx<'d> {
    pub decs: Vec<zstd::bulk::Decompressor<'d>>,
    pub cache: Option<LruCache<u32, Arc<Vec<u8>>>>,
    pub hits: u64,
    pub misses: u64,
    pub preads: u64,
    pub pread_bytes: u64,
}

impl<'d> ReadCtx<'d> {
    pub fn new(dicts: &'d [Vec<u8>], cache_blocks: usize) -> Self {
        ReadCtx {
            decs: dicts
                .iter()
                .map(|d| zstd::bulk::Decompressor::with_dictionary(d).unwrap())
                .collect(),
            cache: NonZeroUsize::new(cache_blocks).map(LruCache::new),
            hits: 0,
            misses: 0,
            preads: 0,
            pread_bytes: 0,
        }
    }
}

pub fn fetch_one(r: &SealedReader, ctx: &mut ReadCtx, id: u32) -> Arc<Vec<u8>> {
    if let Some(c) = ctx.cache.as_mut() {
        if let Some(b) = c.get(&id) {
            ctx.hits += 1;
            return b.clone();
        }
    }
    ctx.misses += 1;
    let bm = r.blocks[id as usize];
    let mut buf = vec![0u8; bm.comp_len as usize];
    r.file.read_exact_at(&mut buf, bm.file_off).unwrap();
    ctx.preads += 1;
    ctx.pread_bytes += bm.comp_len as u64;
    let raw = ctx.decs[bm.dict_id as usize]
        .decompress(&buf, bm.uncomp_len as usize)
        .unwrap();
    let arc = Arc::new(raw);
    if let Some(c) = ctx.cache.as_mut() {
        c.put(id, arc.clone());
    }
    arc
}

/// Fetch a sorted, deduplicated set of blocks with segment-order coalesced
/// preads (gaps up to COALESCE_GAP read over; single read capped at MAX_READ).
pub fn fetch_many(
    r: &SealedReader,
    ctx: &mut ReadCtx,
    ids: &[u32],
) -> HashMap<u32, Arc<Vec<u8>>> {
    let mut out = HashMap::with_capacity(ids.len());
    let mut missing: Vec<u32> = Vec::new();
    for &id in ids {
        if let Some(c) = ctx.cache.as_mut() {
            if let Some(b) = c.get(&id) {
                ctx.hits += 1;
                out.insert(id, b.clone());
                continue;
            }
        }
        ctx.misses += 1;
        missing.push(id);
    }
    let mut i = 0;
    while i < missing.len() {
        let first = r.blocks[missing[i] as usize];
        let start_off = first.file_off;
        let mut end_off = start_off + first.comp_len as u64;
        let mut j = i + 1;
        while j < missing.len() {
            let bm = r.blocks[missing[j] as usize];
            let new_end = bm.file_off + bm.comp_len as u64;
            if bm.file_off.saturating_sub(end_off) <= COALESCE_GAP && new_end - start_off <= MAX_READ
            {
                end_off = new_end;
                j += 1;
            } else {
                break;
            }
        }
        let mut buf = vec![0u8; (end_off - start_off) as usize];
        r.file.read_exact_at(&mut buf, start_off).unwrap();
        ctx.preads += 1;
        ctx.pread_bytes += buf.len() as u64;
        for &id in &missing[i..j] {
            let bm = r.blocks[id as usize];
            let s = (bm.file_off - start_off) as usize;
            let raw = ctx.decs[bm.dict_id as usize]
                .decompress(&buf[s..s + bm.comp_len as usize], bm.uncomp_len as usize)
                .unwrap();
            let arc = Arc::new(raw);
            if let Some(c) = ctx.cache.as_mut() {
                c.put(id, arc.clone());
            }
            out.insert(id, arc);
        }
        i = j;
    }
    out
}

#[inline]
pub fn event_slice(block: &[u8], count: u16, idx: u32) -> &[u8] {
    let base = 4 * (count as usize + 1);
    let o0 = rd_u32(block, 4 * idx as usize) as usize;
    let o1 = rd_u32(block, 4 * (idx as usize + 1)) as usize;
    &block[base + o0..base + o1]
}

pub fn decode_eslots(r: &SealedReader, stream: u64) -> Option<Vec<u64>> {
    let e = *r.dir.get(&stream)?;
    let d = &r.ptr_data[e.off as usize..(e.off + e.len) as usize];
    let mut p = 0usize;
    let mut out = Vec::with_capacity(e.count as usize);
    let mut cur = read_varint(d, &mut p);
    out.push(cur);
    for _ in 1..e.count {
        cur += read_varint(d, &mut p);
        out.push(cur);
    }
    Some(out)
}

/// Stream replay through the sealed path: packed pointer block -> coalesced
/// block preads -> dictionary block decompress (LRU-cached) -> events in
/// version order. BASELINE.
pub fn replay_stream(r: &SealedReader, ctx: &mut ReadCtx, stream: u64) -> ReplayStats {
    let mut st = ReplayStats::default();
    let Some(eslots) = decode_eslots(r, stream) else {
        return st;
    };
    // Group into per-block index runs (eslots are sorted, blocks never recur).
    let mut runs: Vec<(u32, Vec<u32>)> = Vec::new();
    for &es in &eslots {
        let b = (es / BLOCK_EVENTS as u64) as u32;
        let idx = (es % BLOCK_EVENTS as u64) as u32;
        match runs.last_mut() {
            Some((rb, v)) if *rb == b => v.push(idx),
            _ => runs.push((b, vec![idx])),
        }
    }
    for chunk in runs.chunks(FETCH_CHUNK) {
        let ids: Vec<u32> = chunk.iter().map(|(b, _)| *b).collect();
        let map = fetch_many(r, ctx, &ids);
        for (b, idxs) in chunk {
            let data = &map[b];
            let count = r.blocks[*b as usize].count;
            for &idx in idxs {
                let ev = event_slice(data, count, idx);
                st.events += 1;
                st.bytes += ev.len() as u64;
                st.checksum = st.checksum.wrapping_add(ev[0] as u64 + ev.len() as u64);
            }
        }
    }
    st
}

/// Same walk, but crc32 over (len_le ++ payload) per event — for the
/// byte-identity verification against the active segment.
pub fn replay_stream_crc(r: &SealedReader, ctx: &mut ReadCtx, stream: u64) -> (u32, u64) {
    let mut h = crc32fast::Hasher::new();
    let mut n = 0u64;
    let Some(eslots) = decode_eslots(r, stream) else {
        return (h.finalize(), 0);
    };
    let mut runs: Vec<(u32, Vec<u32>)> = Vec::new();
    for &es in &eslots {
        let b = (es / BLOCK_EVENTS as u64) as u32;
        let idx = (es % BLOCK_EVENTS as u64) as u32;
        match runs.last_mut() {
            Some((rb, v)) if *rb == b => v.push(idx),
            _ => runs.push((b, vec![idx])),
        }
    }
    for chunk in runs.chunks(FETCH_CHUNK) {
        let ids: Vec<u32> = chunk.iter().map(|(b, _)| *b).collect();
        let map = fetch_many(r, ctx, &ids);
        for (b, idxs) in chunk {
            let data = &map[b];
            let count = r.blocks[*b as usize].count;
            for &idx in idxs {
                let ev = event_slice(data, count, idx);
                h.update(&(ev.len() as u32).to_le_bytes());
                h.update(ev);
                n += 1;
            }
        }
    }
    (h.finalize(), n)
}

/// Point read of (stream, version): linear varint scan to the version'th
/// eslot, then one block fetch (cache-aware). BASELINE (ignores skip table).
pub fn point_read(r: &SealedReader, ctx: &mut ReadCtx, stream: u64, version: u64) -> u64 {
    let e = r.dir[&stream];
    debug_assert!(version < e.count as u64);
    let d = &r.ptr_data[e.off as usize..(e.off + e.len) as usize];
    let mut p = 0usize;
    let mut cur = read_varint(d, &mut p);
    for _ in 0..version {
        cur += read_varint(d, &mut p);
    }
    let b = (cur / BLOCK_EVENTS as u64) as u32;
    let idx = (cur % BLOCK_EVENTS as u64) as u32;
    let data = fetch_one(r, ctx, b);
    let ev = event_slice(&data, r.blocks[b as usize].count, idx);
    ev[0] as u64 + ev.len() as u64
}

/// Global replay: sequential block decompress scan over the blocks region
/// (large coalesced preads, no cache). Returns (stats, compressed bytes read).
/// BASELINE.
pub fn global_replay(r: &SealedReader, ctx: &mut ReadCtx) -> (ReplayStats, u64) {
    let mut st = ReplayStats::default();
    let mut comp_total = 0u64;
    let mut i = 0usize;
    while i < r.blocks.len() {
        let start = r.blocks[i].file_off;
        let mut j = i;
        let mut end = start;
        while j < r.blocks.len() {
            let new_end = r.blocks[j].file_off + r.blocks[j].comp_len as u64;
            if new_end - start > MAX_READ && j > i {
                break;
            }
            end = new_end;
            j += 1;
        }
        let mut buf = vec![0u8; (end - start) as usize];
        r.file.read_exact_at(&mut buf, start).unwrap();
        comp_total += buf.len() as u64;
        for k in i..j {
            let bm = r.blocks[k];
            let s = (bm.file_off - start) as usize;
            let raw = ctx.decs[bm.dict_id as usize]
                .decompress(&buf[s..s + bm.comp_len as usize], bm.uncomp_len as usize)
                .unwrap();
            for idx in 0..bm.count as u32 {
                let ev = event_slice(&raw, bm.count, idx);
                st.events += 1;
                st.bytes += ev.len() as u64;
                st.checksum = st.checksum.wrapping_add(ev[0] as u64 + ev.len() as u64);
            }
        }
        i = j;
    }
    (st, comp_total)
}
