//! Block store: pack/compress/write sealed payload blocks, and the read
//! paths measured against them. Block formats, clustering, coalesced preads,
//! LRU and replay methodology mirror spikes/seal_pipeline/src/seal.rs so the
//! numbers are comparable to the round-3 figures.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::num::NonZeroUsize;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use lru::LruCache;
use rayon::prelude::*;

use crate::columnar::{self, varint_len, CatShred};
use crate::workload::{category_of, Corpus, CATEGORIES};

pub const COALESCE_GAP: u64 = 64 * 1024;
pub const MAX_READ: u64 = 8 << 20;
pub const FETCH_CHUNK: usize = 64;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Fmt {
    Row,
    Col,
}

#[derive(Clone, Copy, Debug)]
pub enum Codec {
    Zstd(i32),
    Lz4,
}

pub struct RawBlock {
    pub count: u16,
    pub buf: Vec<u8>,
}

#[derive(Clone, Copy)]
pub struct BMeta {
    pub off: u64,
    pub clen: u32,
    pub ulen: u32,
    pub count: u16,
    pub cat: u8,
}

// ---------------------------------------------------------------------------
// Packing (row) — offsets header + payloads, as in seal_pipeline
// ---------------------------------------------------------------------------

pub fn row_pack(c: &Corpus, be: usize) -> ([Vec<RawBlock>; CATEGORIES], Duration) {
    let t = Instant::now();
    let mut out: [Vec<RawBlock>; CATEGORIES] = Default::default();
    for cat in 0..CATEGORIES {
        let evs = &c.evs[c.cat_start[cat]..c.cat_start[cat + 1]];
        for chunk in evs.chunks(be) {
            let psize: usize = chunk.iter().map(|e| e.len as usize).sum();
            let mut buf = Vec::with_capacity(4 * (chunk.len() + 1) + psize);
            let mut off = 0u32;
            buf.extend_from_slice(&off.to_le_bytes());
            for e in chunk {
                off += e.len;
                buf.extend_from_slice(&off.to_le_bytes());
            }
            for e in chunk {
                buf.extend_from_slice(&c.arena[e.off as usize..(e.off + e.len) as usize]);
            }
            out[cat].push(RawBlock { count: chunk.len() as u16, buf });
        }
    }
    (out, t.elapsed())
}

#[inline]
pub fn event_slice(block: &[u8], count: u16, idx: u32) -> &[u8] {
    let base = 4 * (count as usize + 1);
    let o0 = u32::from_le_bytes(block[4 * idx as usize..4 * idx as usize + 4].try_into().unwrap())
        as usize;
    let i1 = 4 * (idx as usize + 1);
    let o1 = u32::from_le_bytes(block[i1..i1 + 4].try_into().unwrap()) as usize;
    &block[base + o0..base + o1]
}

// ---------------------------------------------------------------------------
// Compress + write
// ---------------------------------------------------------------------------

pub struct Built {
    #[allow(dead_code)]
    pub path: PathBuf,
    pub file: File,
    pub blocks: Vec<BMeta>,
    pub cat_block_base: [u32; CATEGORIES],
    pub be: usize,
    pub fmts: [Fmt; CATEGORIES],
    pub codec: Codec,
    pub dicts: [Option<Vec<u8>>; CATEGORIES],
    pub comp_cat: [u64; CATEGORIES],
    pub comp_total: u64,
    /// Single-thread-equivalent CPU time spent compressing blocks.
    pub comp_cpu: Duration,
    /// Wall time for compress (rayon, all cores) + file write + fsync.
    pub wall: Duration,
}

pub fn compress_write(
    per_cat: &[Vec<RawBlock>; CATEGORIES],
    fmts: [Fmt; CATEGORIES],
    codec: Codec,
    dicts: [Option<Vec<u8>>; CATEGORIES],
    be: usize,
    path: &Path,
) -> Built {
    let t0 = Instant::now();
    let jobs: Vec<(usize, &RawBlock)> =
        (0..CATEGORIES).flat_map(|c| per_cat[c].iter().map(move |b| (c, b))).collect();
    let comp: Vec<(Vec<u8>, Duration)> = jobs
        .par_iter()
        .map_init(
            <[Option<zstd::bulk::Compressor>; CATEGORIES]>::default,
            |st, &(cat, rb)| match codec {
                Codec::Zstd(lvl) => {
                    if st[cat].is_none() {
                        st[cat] = Some(match &dicts[cat] {
                            Some(d) => {
                                zstd::bulk::Compressor::with_dictionary(lvl, d).unwrap()
                            }
                            None => zstd::bulk::Compressor::new(lvl).unwrap(),
                        });
                    }
                    let c = st[cat].as_mut().unwrap();
                    let t = Instant::now();
                    let out = c.compress(&rb.buf).unwrap();
                    (out, t.elapsed())
                }
                Codec::Lz4 => {
                    let t = Instant::now();
                    let out = lz4_flex::block::compress(&rb.buf);
                    (out, t.elapsed())
                }
            },
        )
        .collect();
    let comp_cpu: Duration = comp.iter().map(|(_, d)| *d).sum();

    let f = File::create(path).unwrap();
    let mut w = BufWriter::with_capacity(8 << 20, f);
    let mut blocks = Vec::with_capacity(jobs.len());
    let mut cat_block_base = [0u32; CATEGORIES];
    let mut comp_cat = [0u64; CATEGORIES];
    let mut off = 0u64;
    let mut k = 0usize;
    for cat in 0..CATEGORIES {
        cat_block_base[cat] = blocks.len() as u32;
        for rb in &per_cat[cat] {
            let (cb, _) = &comp[k];
            k += 1;
            w.write_all(cb).unwrap();
            blocks.push(BMeta {
                off,
                clen: cb.len() as u32,
                ulen: rb.buf.len() as u32,
                count: rb.count,
                cat: cat as u8,
            });
            off += cb.len() as u64;
            comp_cat[cat] += cb.len() as u64;
        }
    }
    w.flush().unwrap();
    w.get_ref().sync_all().unwrap();
    drop(w);
    let wall = t0.elapsed();
    let file = File::open(path).unwrap();
    Built {
        path: path.to_path_buf(),
        file,
        blocks,
        cat_block_base,
        be,
        fmts,
        codec,
        dicts,
        comp_cat,
        comp_total: off,
        comp_cpu,
        wall,
    }
}

// ---------------------------------------------------------------------------
// Read context
// ---------------------------------------------------------------------------

enum Dec<'d> {
    Zstd(zstd::bulk::Decompressor<'d>),
    Lz4,
}

pub struct ReadCtx<'d> {
    decs: Vec<Dec<'d>>,
    pub cache: Option<LruCache<u32, Arc<Vec<u8>>>>,
    pub hits: u64,
    pub misses: u64,
    pub preads: u64,
    pub pread_bytes: u64,
    // reassembly scratch
    out: Vec<u8>,
    offs: Vec<u32>,
}

impl<'d> ReadCtx<'d> {
    pub fn new(b: &'d Built, cache_blocks: usize) -> Self {
        let decs = (0..CATEGORIES)
            .map(|c| match b.codec {
                Codec::Lz4 => Dec::Lz4,
                Codec::Zstd(_) => Dec::Zstd(match &b.dicts[c] {
                    Some(d) => zstd::bulk::Decompressor::with_dictionary(d).unwrap(),
                    None => zstd::bulk::Decompressor::new().unwrap(),
                }),
            })
            .collect();
        ReadCtx {
            decs,
            cache: NonZeroUsize::new(cache_blocks).map(LruCache::new),
            hits: 0,
            misses: 0,
            preads: 0,
            pread_bytes: 0,
            out: Vec::new(),
            offs: Vec::new(),
        }
    }

    fn decode(&mut self, cat: usize, comp: &[u8], ulen: usize) -> Vec<u8> {
        match &mut self.decs[cat] {
            Dec::Zstd(d) => d.decompress(comp, ulen).unwrap(),
            Dec::Lz4 => lz4_flex::block::decompress(comp, ulen).unwrap(),
        }
    }
}

/// LRU size rule: equal decompressed-byte budget across block sizes,
/// anchored at seal_pipeline's 256 x 128-event blocks.
pub fn cache_blocks_for(be: usize) -> usize {
    (256 * 128 / be).max(8)
}

fn fetch_one(b: &Built, ctx: &mut ReadCtx, id: u32) -> Arc<Vec<u8>> {
    if let Some(c) = ctx.cache.as_mut() {
        if let Some(x) = c.get(&id) {
            ctx.hits += 1;
            return x.clone();
        }
    }
    ctx.misses += 1;
    let bm = b.blocks[id as usize];
    let mut buf = vec![0u8; bm.clen as usize];
    b.file.read_exact_at(&mut buf, bm.off).unwrap();
    ctx.preads += 1;
    ctx.pread_bytes += bm.clen as u64;
    let raw = ctx.decode(bm.cat as usize, &buf, bm.ulen as usize);
    let arc = Arc::new(raw);
    if let Some(c) = ctx.cache.as_mut() {
        c.put(id, arc.clone());
    }
    arc
}

/// Coalesced fetch of sorted block ids (gap <= COALESCE_GAP merged,
/// single pread capped at MAX_READ) — same as seal_pipeline.
fn fetch_many(
    b: &Built,
    ctx: &mut ReadCtx,
    ids: &[u32],
    out: &mut Vec<Option<Arc<Vec<u8>>>>,
) {
    out.clear();
    out.resize(ids.len(), None);
    let mut missing: Vec<(usize, u32)> = Vec::new();
    for (k, &id) in ids.iter().enumerate() {
        if let Some(c) = ctx.cache.as_mut() {
            if let Some(x) = c.get(&id) {
                ctx.hits += 1;
                out[k] = Some(x.clone());
                continue;
            }
        }
        ctx.misses += 1;
        missing.push((k, id));
    }
    let mut i = 0;
    while i < missing.len() {
        let first = b.blocks[missing[i].1 as usize];
        let start = first.off;
        let mut end = start + first.clen as u64;
        let mut j = i + 1;
        while j < missing.len() {
            let bm = b.blocks[missing[j].1 as usize];
            let ne = bm.off + bm.clen as u64;
            if bm.off.saturating_sub(end) <= COALESCE_GAP && ne - start <= MAX_READ {
                end = ne;
                j += 1;
            } else {
                break;
            }
        }
        let mut buf = vec![0u8; (end - start) as usize];
        b.file.read_exact_at(&mut buf, start).unwrap();
        ctx.preads += 1;
        ctx.pread_bytes += buf.len() as u64;
        for &(k, id) in &missing[i..j] {
            let bm = b.blocks[id as usize];
            let s = (bm.off - start) as usize;
            let raw = ctx.decode(bm.cat as usize, &buf[s..s + bm.clen as usize], bm.ulen as usize);
            let arc = Arc::new(raw);
            if let Some(c) = ctx.cache.as_mut() {
                c.put(id, arc.clone());
            }
            out[k] = Some(arc);
        }
        i = j;
    }
}

// ---------------------------------------------------------------------------
// Read paths
// ---------------------------------------------------------------------------

#[derive(Default, Clone, Copy)]
pub struct Stats {
    pub events: u64,
    pub bytes: u64,
    pub checksum: u64,
}

/// (block id, first idx, last idx inclusive) runs for one stream.
fn stream_runs(b: &Built, c: &Corpus, stream: u32, runs: &mut Vec<(u32, u32, u32)>) {
    runs.clear();
    let Some(&(start, len)) = c.stream_range.get(&stream) else { return };
    let cat = category_of(stream);
    let local0 = start as usize - c.cat_start[cat];
    let mut i = 0usize;
    while i < len as usize {
        let l = local0 + i;
        let blk = b.cat_block_base[cat] + (l / b.be) as u32;
        let lo = (l % b.be) as u32;
        let n = (b.be - l % b.be).min(len as usize - i);
        runs.push((blk, lo, lo + n as u32 - 1));
        i += n;
    }
}

/// Stream replay through the sealed path: block runs -> coalesced preads ->
/// block decompress (LRU-cached) -> event slices (row) or byte-exact
/// reassembly of the stream's contiguous range (columnar).
pub fn replay_streams(
    b: &Built,
    c: &Corpus,
    shreds: &[CatShred],
    ctx: &mut ReadCtx,
    sample: &[u32],
) -> Stats {
    let mut st = Stats::default();
    let mut runs: Vec<(u32, u32, u32)> = Vec::new();
    let mut fetched: Vec<Option<Arc<Vec<u8>>>> = Vec::new();
    let mut out = std::mem::take(&mut ctx.out);
    let mut offs = std::mem::take(&mut ctx.offs);
    for &s in sample {
        stream_runs(b, c, s, &mut runs);
        for chunk in runs.chunks(FETCH_CHUNK) {
            let ids: Vec<u32> = chunk.iter().map(|r| r.0).collect();
            fetch_many(b, ctx, &ids, &mut fetched);
            for (k, &(blk, lo, hi)) in chunk.iter().enumerate() {
                let data = fetched[k].as_ref().unwrap();
                let bm = &b.blocks[blk as usize];
                match b.fmts[bm.cat as usize] {
                    Fmt::Row => {
                        for idx in lo..=hi {
                            let ev = event_slice(data, bm.count, idx);
                            st.events += 1;
                            st.bytes += ev.len() as u64;
                            st.checksum =
                                st.checksum.wrapping_add(ev[0] as u64 + ev.len() as u64);
                        }
                    }
                    Fmt::Col => {
                        out.clear();
                        offs.clear();
                        columnar::reassemble_range(
                            &shreds[bm.cat as usize],
                            data,
                            lo as usize,
                            hi as usize + 1,
                            &mut out,
                            &mut offs,
                        );
                        for w in offs.windows(2) {
                            let ev = &out[w[0] as usize..w[1] as usize];
                            st.events += 1;
                            st.bytes += ev.len() as u64;
                            st.checksum =
                                st.checksum.wrapping_add(ev[0] as u64 + ev.len() as u64);
                        }
                    }
                }
            }
        }
    }
    ctx.out = out;
    ctx.offs = offs;
    st
}

/// Point read of (stream, version): one block fetch + slice / single-event
/// reassembly.
pub fn point_read(
    b: &Built,
    c: &Corpus,
    shreds: &[CatShred],
    ctx: &mut ReadCtx,
    stream: u32,
    ver: u32,
) -> u64 {
    let &(start, _len) = c.stream_range.get(&stream).unwrap();
    let cat = category_of(stream);
    let local = start as usize - c.cat_start[cat] + ver as usize;
    let blk = b.cat_block_base[cat] + (local / b.be) as u32;
    let idx = (local % b.be) as u32;
    let data = fetch_one(b, ctx, blk);
    let bm = &b.blocks[blk as usize];
    match b.fmts[bm.cat as usize] {
        Fmt::Row => {
            let ev = event_slice(&data, bm.count, idx);
            ev[0] as u64 + ev.len() as u64
        }
        Fmt::Col => {
            let mut out = std::mem::take(&mut ctx.out);
            let mut offs = std::mem::take(&mut ctx.offs);
            out.clear();
            offs.clear();
            columnar::reassemble_range(
                &shreds[bm.cat as usize],
                &data,
                idx as usize,
                idx as usize + 1,
                &mut out,
                &mut offs,
            );
            let r = out[0] as u64 + out.len() as u64;
            ctx.out = out;
            ctx.offs = offs;
            r
        }
    }
}

/// Sequential full scan: big coalesced preads, decompress every block,
/// touch every event. With `verify` it memcmp's every event against the
/// corpus arena (clustered order == block order) and counts mismatches.
pub fn global_scan(
    b: &Built,
    c: &Corpus,
    shreds: &[CatShred],
    ctx: &mut ReadCtx,
    verify: bool,
) -> (Stats, u64) {
    let mut st = Stats::default();
    let mut mismatches = 0u64;
    let mut ev_i = 0usize;
    let mut out: Vec<u8> = Vec::new();
    let mut offs: Vec<u32> = Vec::new();
    let mut i = 0usize;
    while i < b.blocks.len() {
        let start = b.blocks[i].off;
        let mut j = i;
        let mut end = start;
        while j < b.blocks.len() {
            let ne = b.blocks[j].off + b.blocks[j].clen as u64;
            if ne - start > MAX_READ && j > i {
                break;
            }
            end = ne;
            j += 1;
        }
        let mut buf = vec![0u8; (end - start) as usize];
        b.file.read_exact_at(&mut buf, start).unwrap();
        for k in i..j {
            let bm = b.blocks[k];
            let s = (bm.off - start) as usize;
            let raw = ctx.decode(bm.cat as usize, &buf[s..s + bm.clen as usize], bm.ulen as usize);
            match b.fmts[bm.cat as usize] {
                Fmt::Row => {
                    for idx in 0..bm.count as u32 {
                        let ev = event_slice(&raw, bm.count, idx);
                        st.events += 1;
                        st.bytes += ev.len() as u64;
                        st.checksum = st.checksum.wrapping_add(ev[0] as u64 + ev.len() as u64);
                        if verify && ev != c.bytes_of(ev_i) {
                            mismatches += 1;
                        }
                        ev_i += 1;
                    }
                }
                Fmt::Col => {
                    out.clear();
                    offs.clear();
                    columnar::reassemble_range(
                        &shreds[bm.cat as usize],
                        &raw,
                        0,
                        bm.count as usize,
                        &mut out,
                        &mut offs,
                    );
                    for w in offs.windows(2) {
                        let ev = &out[w[0] as usize..w[1] as usize];
                        st.events += 1;
                        st.bytes += ev.len() as u64;
                        st.checksum = st.checksum.wrapping_add(ev[0] as u64 + ev.len() as u64);
                        if verify && ev != c.bytes_of(ev_i) {
                            mismatches += 1;
                        }
                        ev_i += 1;
                    }
                }
            }
        }
        i = j;
    }
    (st, mismatches)
}

// ---------------------------------------------------------------------------
// All-in size estimate (payload blocks + the sealed metadata the
// seal_pipeline format carries: pointer blocks, stream dir, block index,
// dicts, BinaryFuse16 filter, plus columnar skeleton/path tables).
// ---------------------------------------------------------------------------

pub fn allin_bytes(b: &Built, c: &Corpus, shreds: &[CatShred]) -> u64 {
    let mut ptr = 0u64;
    for (&s, &(start, len)) in &c.stream_range {
        let cat = category_of(s);
        let local = start as usize - c.cat_start[cat];
        let first_eslot =
            (b.cat_block_base[cat] as usize + local / b.be) * b.be + local % b.be;
        // clustered layout -> all eslot deltas are 1 (1 varint byte each)
        ptr += varint_len(first_eslot as u64) as u64 + (len as u64 - 1);
    }
    let dir = 20 * c.stream_range.len() as u64;
    let bidx = 19 * b.blocks.len() as u64;
    let dicts: u64 = b.dicts.iter().flatten().map(|d| d.len() as u64).sum();
    let filters = (3.61 * c.stream_range.len() as f64) as u64 + 100; // BinaryFuse16, measured 3.61 B/key
    let shred_meta: u64 = (0..CATEGORIES)
        .filter(|&cat| b.fmts[cat] == Fmt::Col)
        .map(|cat| shreds[cat].meta_bytes())
        .sum();
    b.comp_total + ptr + dir + bidx + dicts + filters + shred_meta
}
