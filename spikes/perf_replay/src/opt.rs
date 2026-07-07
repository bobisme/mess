//! Optimized read/seal paths, each paired with a baseline in seal.rs.
//!
//! Every function here must produce results identical to its baseline
//! counterpart (asserted by the driver):
//!   - replay paths: same ReplayStats (events/bytes/checksum; the checksum is
//!     an order-independent wrapping sum, so parallel reduction is exact);
//!   - point reads: same per-read value;
//!   - parallel seal: byte-identical sealed FILE (crc32 of the whole file).

use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::time::{Duration, Instant};

use rayon::prelude::*;

use crate::seal::{
    decode_eslots, event_slice, read_varint, BlockMeta, DictOut, DirEntry, ReadCtx,
    RecompressOut, SealedReader, StreamEvents, BLOCK_EVENTS, DICT_MAX_BYTES, DICT_SAMPLES,
    SKIP_K, ZSTD_LEVEL,
};
use crate::seglog::{rd_u32, rd_u64, ReplayStats};
use crate::workload::{category_of, CATEGORIES};

fn make_decs<'d>(dicts: &'d [Vec<u8>]) -> Vec<zstd::bulk::Decompressor<'d>> {
    dicts
        .iter()
        .map(|d| zstd::bulk::Decompressor::with_dictionary(d).unwrap())
        .collect()
}

fn merge(a: ReplayStats, b: ReplayStats) -> ReplayStats {
    ReplayStats {
        events: a.events + b.events,
        bytes: a.bytes + b.bytes,
        checksum: a.checksum.wrapping_add(b.checksum),
    }
}

// ---------------------------------------------------------------------------
// 1. Parallel global replay: rayon over sealed blocks, mmap'd source,
//    per-thread decompressors, reused decompress buffer.
// ---------------------------------------------------------------------------

/// Blocks per rayon work item (amortizes the per-split Decompressor init).
const GLOBAL_CHUNK: usize = 64;

pub fn global_replay_par(r: &SealedReader) -> (ReplayStats, u64) {
    let comp_total: u64 = r.blocks.iter().map(|b| b.comp_len as u64).sum();
    let data = &r.mmap[..];
    let st = r
        .blocks
        .par_chunks(GLOBAL_CHUNK)
        .map_init(
            || (make_decs(&r.dicts), Vec::<u8>::new()),
            |(decs, buf), chunk| {
                let mut st = ReplayStats::default();
                for bm in chunk {
                    let s = bm.file_off as usize;
                    let src = &data[s..s + bm.comp_len as usize];
                    buf.clear();
                    buf.reserve(bm.uncomp_len as usize);
                    let n = decs[bm.dict_id as usize]
                        .decompress_to_buffer(src, &mut *buf)
                        .unwrap();
                    debug_assert_eq!(n, bm.uncomp_len as usize);
                    for idx in 0..bm.count as u32 {
                        let ev = event_slice(buf, bm.count, idx);
                        st.events += 1;
                        st.bytes += ev.len() as u64;
                        st.checksum = st.checksum.wrapping_add(ev[0] as u64 + ev.len() as u64);
                    }
                }
                st
            },
        )
        .reduce(ReplayStats::default, merge);
    (st, comp_total)
}

// ---------------------------------------------------------------------------
// 2. Parallel stream replay
// ---------------------------------------------------------------------------

/// Variant A: parallelize the baseline per-stream loop across a thread pool.
/// Each rayon worker gets its own ReadCtx (pread + coalescing + small LRU),
/// so this measures "just add threads" against the baseline path.
pub fn stream_replay_par_preads(r: &SealedReader, streams: &[u64], lru: usize) -> ReplayStats {
    streams
        .par_iter()
        .with_min_len(8)
        .map_init(
            || ReadCtx::new(&r.dicts, lru),
            |ctx, &s| crate::seal::replay_stream(r, ctx, s),
        )
        .reduce(ReplayStats::default, merge)
}

/// Variant B: batch + coalesce. Decode all pointer blocks (parallel), take
/// the union of touched blocks, decompress each unique block exactly once
/// (parallel, mmap-backed so the "pread" is a page-cache read with no
/// syscall), then assemble per-stream stats in parallel. No LRU, no Arc
/// churn, no duplicate decompression across streams.
pub struct BatchOut {
    pub stats: ReplayStats,
    pub unique_blocks: usize,
    pub comp_bytes: u64,
}

pub fn stream_replay_batch(r: &SealedReader, streams: &[u64]) -> BatchOut {
    // Per-stream block runs: (block id, first idx, count) — with seal-time
    // clustering a stream's events are consecutive eslots, so a run is a
    // dense range; keep (block, idx list) grouped as ranges to avoid Vec-per-
    // block allocs.
    let runs_per: Vec<Vec<(u32, u32, u32)>> = streams
        .par_iter()
        .with_min_len(8)
        .map(|&s| {
            let Some(eslots) = decode_eslots(r, s) else {
                return Vec::new();
            };
            let mut runs: Vec<(u32, u32, u32)> = Vec::new();
            for &es in &eslots {
                let b = (es / BLOCK_EVENTS as u64) as u32;
                let idx = (es % BLOCK_EVENTS as u64) as u32;
                match runs.last_mut() {
                    Some((rb, first, n)) if *rb == b && idx == *first + *n => *n += 1,
                    _ => runs.push((b, idx, 1)),
                }
            }
            runs
        })
        .collect();

    // Unique touched blocks.
    let mut ids: Vec<u32> = runs_per
        .iter()
        .flat_map(|rs| rs.iter().map(|&(b, _, _)| b))
        .collect();
    ids.sort_unstable();
    ids.dedup();

    // Decompress each unique block once, in parallel, from the mmap.
    let data = &r.mmap[..];
    let blocks: Vec<Vec<u8>> = ids
        .par_iter()
        .with_min_len(16)
        .map_init(
            || make_decs(&r.dicts),
            |decs, &id| {
                let bm = r.blocks[id as usize];
                let s = bm.file_off as usize;
                decs[bm.dict_id as usize]
                    .decompress(&data[s..s + bm.comp_len as usize], bm.uncomp_len as usize)
                    .unwrap()
            },
        )
        .collect();
    let comp_bytes: u64 = ids.iter().map(|&id| r.blocks[id as usize].comp_len as u64).sum();

    // Per-stream assembly.
    let stats = runs_per
        .par_iter()
        .with_min_len(8)
        .map(|rs| {
            let mut st = ReplayStats::default();
            for &(b, first, n) in rs {
                let k = ids.binary_search(&b).unwrap();
                let block = &blocks[k];
                let count = r.blocks[b as usize].count;
                for idx in first..first + n {
                    let ev = event_slice(block, count, idx);
                    st.events += 1;
                    st.bytes += ev.len() as u64;
                    st.checksum = st.checksum.wrapping_add(ev[0] as u64 + ev.len() as u64);
                }
            }
            st
        })
        .reduce(ReplayStats::default, merge);

    BatchOut { stats, unique_blocks: ids.len(), comp_bytes }
}

// ---------------------------------------------------------------------------
// 3. Point read via the intra-block skip table
// ---------------------------------------------------------------------------

/// Skip-table point read: O(SKIP_K) varint decodes instead of O(version).
pub fn point_read_skip(r: &SealedReader, ctx: &mut ReadCtx, stream: u64, version: u64) -> u64 {
    let e: DirEntry = r.dir[&stream];
    debug_assert!(version < e.count as u64);
    let d = &r.ptr_data[e.off as usize..(e.off + e.len) as usize];
    let j = (version as usize) / SKIP_K;
    let mut cur;
    let mut p;
    let rem;
    if j == 0 {
        p = 0;
        cur = read_varint(d, &mut p);
        rem = version;
    } else {
        debug_assert!((j - 1) < e.n_skips as usize);
        let sd = &r.skip_data[e.skip_off as usize + (j - 1) * 12..];
        cur = rd_u64(sd, 0);
        p = rd_u32(sd, 8) as usize;
        rem = version - (j * SKIP_K) as u64;
    }
    for _ in 0..rem {
        cur += read_varint(d, &mut p);
    }
    let b = (cur / BLOCK_EVENTS as u64) as u32;
    let idx = (cur % BLOCK_EVENTS as u64) as u32;
    let block = crate::seal::fetch_one(r, ctx, b);
    let ev = event_slice(&block, r.blocks[b as usize].count, idx);
    ev[0] as u64 + ev.len() as u64
}

// ---------------------------------------------------------------------------
// 4. Parallel seal: per-category dict training + per-block recompression
// ---------------------------------------------------------------------------

pub fn train_dicts_par(seg: &[u8], streams: &[StreamEvents]) -> DictOut {
    let out: Vec<(Vec<u8>, Duration)> = (0..CATEGORIES)
        .into_par_iter()
        .map(|cat| {
            let mut refs: Vec<&[u8]> = Vec::new();
            for se in streams.iter().filter(|se| category_of(se.stream) == cat) {
                for ev in &se.evs {
                    refs.push(&seg[ev.off as usize..(ev.off + ev.len as u64) as usize]);
                }
            }
            let stride = (refs.len() / DICT_SAMPLES).max(1);
            let samples: Vec<&[u8]> =
                refs.iter().step_by(stride).take(DICT_SAMPLES).copied().collect();
            let t = Instant::now();
            let dict = zstd::dict::from_samples(&samples, DICT_MAX_BYTES).expect("dict training");
            (dict, t.elapsed())
        })
        .collect();
    let mut dicts = Vec::with_capacity(CATEGORIES);
    let mut train_times = Vec::with_capacity(CATEGORIES);
    for (d, t) in out {
        dicts.push(d);
        train_times.push(t);
    }
    DictOut { dicts, train_times }
}

/// Parallel recompression. Reproduces the baseline block assignment exactly
/// (category-major, stream order within category, 128-event blocks, short
/// block at each category boundary), compresses blocks in parallel, then
/// writes them in order — so the output bytes are identical to the baseline.
pub fn recompress_par(
    seg: &[u8],
    streams: &[StreamEvents],
    dicts: &[Vec<u8>],
    w: &mut BufWriter<File>,
    file_off0: u64,
) -> RecompressOut {
    use crate::seal::EvRef;

    // Per-category event lists in the baseline iteration order.
    let mut cat_events: Vec<Vec<EvRef>> = vec![Vec::new(); CATEGORIES];
    let mut cat_streams: Vec<Vec<(usize, usize)>> = vec![Vec::new(); CATEGORIES]; // (si, n)
    for (si, se) in streams.iter().enumerate() {
        let c = category_of(se.stream);
        cat_events[c].extend_from_slice(&se.evs);
        cat_streams[c].push((si, se.evs.len()));
    }

    // Global block numbering: category-major, matching the baseline.
    let mut block_base = [0usize; CATEGORIES];
    let mut nb = 0usize;
    for c in 0..CATEGORIES {
        block_base[c] = nb;
        nb += cat_events[c].len().div_ceil(BLOCK_EVENTS);
    }

    // Sealed slots per stream (pure arithmetic once block bases are known).
    let mut stream_eslots: Vec<Vec<u64>> =
        streams.iter().map(|se| Vec::with_capacity(se.evs.len())).collect();
    for c in 0..CATEGORIES {
        let mut k = 0usize;
        for &(si, n) in &cat_streams[c] {
            let v = &mut stream_eslots[si];
            for _ in 0..n {
                let eslot =
                    ((block_base[c] + k / BLOCK_EVENTS) * BLOCK_EVENTS + k % BLOCK_EVENTS) as u64;
                v.push(eslot);
                k += 1;
            }
        }
    }

    // Block descriptors in file order, then compress in parallel.
    let descs: Vec<(usize, &[EvRef])> = (0..CATEGORIES)
        .flat_map(|c| cat_events[c].chunks(BLOCK_EVENTS).map(move |ch| (c, ch)))
        .collect();
    let comp: Vec<(Vec<u8>, u32, u16, u8)> = descs
        .par_iter()
        .with_min_len(8)
        .map_init(
            || {
                dicts
                    .iter()
                    .map(|d| zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, d).unwrap())
                    .collect::<Vec<_>>()
            },
            |comps, &(c, evs)| {
                let payload_len: usize = evs.iter().map(|e| e.len as usize).sum();
                let mut raw = Vec::with_capacity(4 * (evs.len() + 1) + payload_len);
                let mut o = 0u32;
                raw.extend_from_slice(&o.to_le_bytes());
                for ev in evs {
                    o += ev.len;
                    raw.extend_from_slice(&o.to_le_bytes());
                }
                for ev in evs {
                    raw.extend_from_slice(&seg[ev.off as usize..(ev.off + ev.len as u64) as usize]);
                }
                let cbytes = comps[c].compress(&raw).unwrap();
                (cbytes, raw.len() as u32, evs.len() as u16, c as u8)
            },
        )
        .collect();

    // Ordered write + metadata accumulation.
    use std::io::Write;
    let mut blocks: Vec<BlockMeta> = Vec::with_capacity(comp.len());
    let mut cat_raw = [0u64; CATEGORIES];
    let mut cat_comp = [0u64; CATEGORIES];
    let mut cat_blocks = [0u64; CATEGORIES];
    let mut file_off = file_off0;
    for (cbytes, uncomp_len, count, dict_id) in &comp {
        w.write_all(cbytes).unwrap();
        blocks.push(BlockMeta {
            file_off,
            comp_len: cbytes.len() as u32,
            uncomp_len: *uncomp_len,
            count: *count,
            dict_id: *dict_id,
        });
        file_off += cbytes.len() as u64;
        let c = *dict_id as usize;
        cat_raw[c] += *uncomp_len as u64 - 4 * (*count as u64 + 1);
        cat_comp[c] += cbytes.len() as u64;
        cat_blocks[c] += 1;
    }
    RecompressOut { blocks, stream_eslots, cat_raw, cat_comp, cat_blocks }
}

/// Pipelined dict training + recompression: each category trains its
/// dictionary and then immediately compresses its own blocks (nested rayon),
/// so early-finishing categories' block compression overlaps late
/// dictionaries' training instead of waiting on a global barrier. Output
/// bytes are identical to the baseline (same samples -> same dicts; same
/// block assignment; ordered write).
pub fn dicts_and_recompress_pipelined(
    seg: &[u8],
    streams: &[StreamEvents],
    w: &mut BufWriter<File>,
    file_off0: u64,
) -> (DictOut, RecompressOut) {
    use crate::seal::EvRef;

    let mut cat_events: Vec<Vec<EvRef>> = vec![Vec::new(); CATEGORIES];
    let mut cat_streams: Vec<Vec<(usize, usize)>> = vec![Vec::new(); CATEGORIES];
    for (si, se) in streams.iter().enumerate() {
        let c = category_of(se.stream);
        cat_events[c].extend_from_slice(&se.evs);
        cat_streams[c].push((si, se.evs.len()));
    }
    let mut block_base = [0usize; CATEGORIES];
    let mut nb = 0usize;
    for c in 0..CATEGORIES {
        block_base[c] = nb;
        nb += cat_events[c].len().div_ceil(BLOCK_EVENTS);
    }
    let mut stream_eslots: Vec<Vec<u64>> =
        streams.iter().map(|se| Vec::with_capacity(se.evs.len())).collect();
    for c in 0..CATEGORIES {
        let mut k = 0usize;
        for &(si, n) in &cat_streams[c] {
            let v = &mut stream_eslots[si];
            for _ in 0..n {
                let eslot =
                    ((block_base[c] + k / BLOCK_EVENTS) * BLOCK_EVENTS + k % BLOCK_EVENTS) as u64;
                v.push(eslot);
                k += 1;
            }
        }
    }

    // Per category: train dict (identical sampling to seal::train_dicts),
    // then compress this category's blocks with nested parallelism.
    type CatOut = (Vec<u8>, Duration, Vec<(Vec<u8>, u32, u16)>);
    let cat_out: Vec<CatOut> = (0..CATEGORIES)
        .into_par_iter()
        .map(|c| {
            let evs = &cat_events[c];
            let stride = (evs.len() / DICT_SAMPLES).max(1);
            let samples: Vec<&[u8]> = evs
                .iter()
                .step_by(stride)
                .take(DICT_SAMPLES)
                .map(|ev| &seg[ev.off as usize..(ev.off + ev.len as u64) as usize])
                .collect();
            let t = Instant::now();
            let dict = zstd::dict::from_samples(&samples, DICT_MAX_BYTES).expect("dict training");
            let d_train = t.elapsed();
            let comp: Vec<(Vec<u8>, u32, u16)> = evs
                .par_chunks(BLOCK_EVENTS)
                .with_min_len(8)
                .map_init(
                    || zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, &dict).unwrap(),
                    |comp, evs| {
                        let payload_len: usize = evs.iter().map(|e| e.len as usize).sum();
                        let mut raw = Vec::with_capacity(4 * (evs.len() + 1) + payload_len);
                        let mut o = 0u32;
                        raw.extend_from_slice(&o.to_le_bytes());
                        for ev in evs {
                            o += ev.len;
                            raw.extend_from_slice(&o.to_le_bytes());
                        }
                        for ev in evs {
                            raw.extend_from_slice(
                                &seg[ev.off as usize..(ev.off + ev.len as u64) as usize],
                            );
                        }
                        let cbytes = comp.compress(&raw).unwrap();
                        (cbytes, raw.len() as u32, evs.len() as u16)
                    },
                )
                .collect();
            (dict, d_train, comp)
        })
        .collect();

    // Ordered write, category-major (baseline block order).
    use std::io::Write;
    let mut blocks: Vec<BlockMeta> = Vec::with_capacity(nb);
    let mut cat_raw = [0u64; CATEGORIES];
    let mut cat_comp = [0u64; CATEGORIES];
    let mut cat_blocks = [0u64; CATEGORIES];
    let mut file_off = file_off0;
    let mut dicts = Vec::with_capacity(CATEGORIES);
    let mut train_times = Vec::with_capacity(CATEGORIES);
    for (c, (dict, d_train, comp)) in cat_out.into_iter().enumerate() {
        for (cbytes, uncomp_len, count) in &comp {
            w.write_all(cbytes).unwrap();
            blocks.push(BlockMeta {
                file_off,
                comp_len: cbytes.len() as u32,
                uncomp_len: *uncomp_len,
                count: *count,
                dict_id: c as u8,
            });
            file_off += cbytes.len() as u64;
            cat_raw[c] += *uncomp_len as u64 - 4 * (*count as u64 + 1);
            cat_comp[c] += cbytes.len() as u64;
            cat_blocks[c] += 1;
        }
        dicts.push(dict);
        train_times.push(d_train);
    }
    (
        DictOut { dicts, train_times },
        RecompressOut { blocks, stream_eslots, cat_raw, cat_comp, cat_blocks },
    )
}

// ---------------------------------------------------------------------------
// Ground truth helper: per-stream ReplayStats from the ACTIVE segment scan.
// ---------------------------------------------------------------------------

pub fn active_ground_truth(seg: &[u8]) -> HashMap<u64, ReplayStats> {
    let rec = crate::seglog::scan(seg, 0, 0);
    let mut per: HashMap<u64, ReplayStats> = HashMap::new();
    for b in &rec.batches {
        let st = per.entry(b.stream_id).or_default();
        for &(o, l) in &b.payloads {
            st.events += 1;
            st.bytes += l as u64;
            st.checksum = st.checksum.wrapping_add(seg[o as usize] as u64 + l as u64);
        }
    }
    per
}
