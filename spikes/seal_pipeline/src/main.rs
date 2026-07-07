//! seal_pipeline spike: prototype the D5/D6 seal pass on one ~256 MiB active
//! segment and measure (1) seal cost, (2) disk before/after, (3) stream
//! replay before/after vs. the vertical-slice figures, (4) global replay,
//! (5) filter FPR + size, (6) point-read latency, (7) byte-identity of every
//! stream's replay across the seal.

mod seal;
mod seglog;
mod workload;

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::BufWriter;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rand_distr::{Distribution, Zipf};
use xorf::{BinaryFuse16, BinaryFuse8, Filter};

use seal::{
    consolidate, global_replay, point_read, recompress, replay_stream, replay_stream_crc,
    train_dicts, ActiveIndex, EvRef, Filters, ReadCtx, SealedReader, StreamEvents, BLOCK_EVENTS,
};
use seglog::{scan, ReplayStats, SegmentWriter, StopReason};
use workload::{payload, BATCH, CATEGORIES, CATEGORY_NAMES, STREAMS, ZIPF_S};

const N_EVENTS: usize = 1_000_000;
const FILL_SEED: u64 = 7;
const SAMPLE_SEED: u64 = 99;
const POINT_SEED: u64 = 123;
const N_SAMPLE_STREAMS: usize = 1_000;
const N_POINT_READS: usize = 10_000;
const N_ABSENT: usize = 100_000;
const LRU_BLOCKS: usize = 256;

// Vertical-slice reference figures (same machine, same workload shape) —
// spikes/vertical_slice/REPORT.md.
const VS_ROCKS_STREAM_EVS: f64 = 1_753_626.0;
const VS_SLICE_STREAM_EVS: f64 = 1_051_405.0;
const VS_ROCKS_DISK_B_PER_EV: f64 = 232.6;
const VS_SLICE_DISK_B_PER_EV: f64 = 305.5;
const VS_SLICE_GLOBAL_EVS: f64 = 3_892_049.0;

fn timed<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let t = Instant::now();
    let out = f();
    (out, t.elapsed())
}

fn vm() -> (u64, u64) {
    // (VmRSS kB, VmHWM kB)
    let s = fs::read_to_string("/proc/self/status").unwrap();
    let grab = |k: &str| {
        s.lines()
            .find(|l| l.starts_with(k))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };
    (grab("VmRSS:"), grab("VmHWM:"))
}

fn mb(b: u64) -> f64 {
    b as f64 / (1024.0 * 1024.0)
}

fn pct(durs: &mut [Duration], p: f64) -> Duration {
    durs.sort_unstable();
    durs[((durs.len() as f64 * p) as usize).min(durs.len() - 1)]
}

fn evs(events: u64, d: Duration) -> f64 {
    events as f64 / d.as_secs_f64()
}

fn main() {
    let keep = std::env::args().any(|a| a == "--keep");
    let root = Path::new("bench_data");
    let _ = fs::remove_dir_all(root);
    fs::create_dir_all(root).unwrap();
    let log_dir = root.join("log");
    let sealed_path = root.join("seg-000000.seal");

    println!("== seal_pipeline spike ==");
    println!(
        "workload: {} events, batches of {}, {} streams Zipf({}), {} categories, seeds fill={} sample={}",
        N_EVENTS, BATCH, STREAMS, ZIPF_S, CATEGORIES, FILL_SEED, SAMPLE_SEED
    );

    // -----------------------------------------------------------------
    // 1. Fill one active segment + per-event active index
    // -----------------------------------------------------------------
    let mut log = SegmentWriter::create(&log_dir);
    let mut index: ActiveIndex = ActiveIndex::new();
    let mut counts = vec![0u64; STREAMS as usize];
    let mut posmap: Vec<(u32, u32)> = Vec::with_capacity(N_EVENTS); // pos -> (stream, version)
    let mut payload_bytes = 0u64;

    let (_, d_fill) = timed(|| {
        let mut rng = StdRng::seed_from_u64(FILL_SEED);
        let zipf = Zipf::new(STREAMS as f64, ZIPF_S).unwrap();
        for _ in 0..N_EVENTS / BATCH {
            let s = (zipf.sample(&mut rng) as u64).clamp(1, STREAMS) - 1;
            let first = counts[s as usize];
            let payloads: Vec<Vec<u8>> =
                (0..BATCH as u64).map(|i| payload(&mut rng, s, first + i)).collect();
            let (first_pos, ptrs) = log.append(s, first, &payloads);
            for (i, &(off, len)) in ptrs.iter().enumerate() {
                let ver = first + i as u64;
                let pos = first_pos + i as u64;
                index.insert((s, ver), EvRef { pos: pos as u32, off, len });
                posmap.push((s as u32, ver as u32));
                payload_bytes += len as u64;
            }
            counts[s as usize] += BATCH as u64;
        }
        log.file.sync_data().unwrap();
    });

    let active_bytes = log.seg_len;
    let present: Vec<u64> = (0..STREAMS).filter(|&s| counts[s as usize] > 0).collect();
    let hottest = (0..STREAMS).max_by_key(|&s| counts[s as usize]).unwrap();
    println!("\n-- active segment --");
    println!(
        "size: {:.1} MiB ({:.1} B/event framed; avg payload {:.1} B); streams present: {}; hottest stream {} with {} events",
        mb(active_bytes),
        active_bytes as f64 / N_EVENTS as f64,
        payload_bytes as f64 / N_EVENTS as f64,
        present.len(),
        hottest,
        counts[hottest as usize]
    );
    println!(
        "fill: {:.2}s ({:.0}k ev/s buffered, one fdatasync at end); active index: {} per-event entries (in-memory BTreeMap stand-in)",
        d_fill.as_secs_f64(),
        evs(N_EVENTS as u64, d_fill) / 1000.0,
        index.len()
    );
    let (rss, hwm) = vm();
    println!("mem after fill: RSS {:.0} MiB, HWM {:.0} MiB", rss as f64 / 1024.0, hwm as f64 / 1024.0);

    // -----------------------------------------------------------------
    // 2. Pre-seal baselines on the SAME data
    // -----------------------------------------------------------------
    // 2a. Active global replay (sequential scan + frame parse) — also warms
    // the page cache so every replay comparison below is cache-warm.
    let ((seg_data, n_scanned), d_active_global) = timed(|| {
        let data = fs::read(&log.path).unwrap();
        let rec = scan(&data, 0, 0);
        assert_eq!(rec.stop, StopReason::EndOfLog);
        let mut st = ReplayStats::default();
        for b in &rec.batches {
            for &(o, l) in &b.payloads {
                st.events += 1;
                st.bytes += l as u64;
                st.checksum = st.checksum.wrapping_add(data[o as usize] as u64 + l as u64);
            }
        }
        assert_eq!(st.events, N_EVENTS as u64);
        (data, st.events)
    });
    println!("\n-- pre-seal baselines (page-cache warm) --");
    println!(
        "active global replay: {:.2}M ev/s, {:.0} MB/s file bytes  (vertical-slice slice figure: {:.2}M ev/s)",
        evs(n_scanned, d_active_global) / 1e6,
        mb(active_bytes) / d_active_global.as_secs_f64(),
        VS_SLICE_GLOBAL_EVS / 1e6
    );

    // Per-stream crc32 of (len ++ payload) from the active segment = the
    // ground truth for post-seal byte-identity verification.
    let active_crcs: HashMap<u64, (u32, u64)> = {
        let rec = scan(&seg_data, 0, 0);
        let mut hashers: HashMap<u64, (crc32fast::Hasher, u64)> = HashMap::new();
        for b in &rec.batches {
            let e = hashers.entry(b.stream_id).or_default();
            for &(o, l) in &b.payloads {
                e.0.update(&l.to_le_bytes());
                e.0.update(&seg_data[o as usize..(o + l as u64) as usize]);
                e.1 += 1;
            }
        }
        hashers.into_iter().map(|(s, (h, n))| (s, (h.finalize(), n))).collect()
    };
    drop(seg_data);

    // 2b. Stream sample: 1,000 distinct random present streams (excluding the
    // hottest) + the hottest stream, benchmarked separately.
    let sample: Vec<u64> = {
        let mut rng = StdRng::seed_from_u64(SAMPLE_SEED);
        let mut set = HashSet::new();
        let mut out = Vec::with_capacity(N_SAMPLE_STREAMS);
        while out.len() < N_SAMPLE_STREAMS {
            let s = present[rng.random_range(0..present.len())];
            if s != hottest && set.insert(s) {
                out.push(s);
            }
        }
        out
    };

    // 2c. Per-event pointer chase (the vertical-slice loser): one index range
    // walk + one pread per event.
    let chase = |streams: &[u64]| -> ReplayStats {
        let file = File::open(&log.path).unwrap();
        let mut st = ReplayStats::default();
        let mut buf = Vec::new();
        for &s in streams {
            for (_, ev) in index.range((s, 0)..=(s, u64::MAX)) {
                buf.resize(ev.len as usize, 0);
                file.read_exact_at(&mut buf, ev.off).unwrap();
                st.events += 1;
                st.bytes += ev.len as u64;
                st.checksum = st.checksum.wrapping_add(buf[0] as u64 + ev.len as u64);
            }
        }
        st
    };
    let (chase_rand, d_chase_rand) = timed(|| chase(&sample));
    let (chase_hot, d_chase_hot) = timed(|| chase(&[hottest]));
    println!(
        "pre-seal pointer-chase replay, 1000 random streams: {} events, {:.2}M ev/s",
        chase_rand.events,
        evs(chase_rand.events, d_chase_rand) / 1e6
    );
    println!(
        "pre-seal pointer-chase replay, hottest stream: {} events, {:.2}M ev/s",
        chase_hot.events,
        evs(chase_hot.events, d_chase_hot) / 1e6
    );
    println!(
        "  (vertical-slice: slice pointer-chase {:.2}M ev/s, RocksDB prefix scan {:.2}M ev/s)",
        VS_SLICE_STREAM_EVS / 1e6,
        VS_ROCKS_STREAM_EVS / 1e6
    );

    // -----------------------------------------------------------------
    // 3. THE SEAL PASS — staged and individually timed
    // -----------------------------------------------------------------
    println!("\n-- seal pass --");
    let t_seal = Instant::now();

    // (a) consolidate per-event entries -> per-stream packed runs.
    let (streams, d_consolidate) = timed(|| consolidate(&index));
    assert_eq!(streams.len(), present.len());

    // (b) membership filters (BinaryFuse8 and BinaryFuse16, streams + categories).
    let stream_keys: Vec<u64> = streams.iter().map(|se| se.stream).collect();
    let cat_keys: Vec<u64> = (0..CATEGORIES as u64).collect();
    let (s8, d_f8) = timed(|| BinaryFuse8::try_from(stream_keys.as_slice()).unwrap());
    let (s16, d_f16) = timed(|| BinaryFuse16::try_from(stream_keys.as_slice()).unwrap());
    let (c8, d_c8) = timed(|| BinaryFuse8::try_from(cat_keys.as_slice()).unwrap());
    let (c16, d_c16) = timed(|| BinaryFuse16::try_from(cat_keys.as_slice()).unwrap());
    let filters = Filters { s8, s16, c8, c16 };

    // (c1) read the active segment back (seal input I/O).
    let (seg_data, d_seg_read) = timed(|| fs::read(&log.path).unwrap());

    // (c2) train one 16 KiB dictionary per category on 5k strided samples.
    let (dict_out, d_dicts) = timed(|| train_dicts(&seg_data, &streams));

    // (c3) recompress payloads into ~128-event category-homogeneous blocks,
    // streaming them into the sealed file.
    let out_file = File::create(&sealed_path).unwrap();
    let mut w = BufWriter::with_capacity(8 << 20, out_file);
    use std::io::Write;
    w.write_all(&seal::FILE_MAGIC.to_le_bytes()).unwrap();
    let (rc, d_recompress) = timed(|| recompress(&seg_data, &streams, &dict_out.dicts, &mut w, 8));
    drop(seg_data);
    let blocks_end = 8 + rc.blocks.iter().map(|b| b.comp_len as u64).sum::<u64>();

    // (d) write pointer blocks, stream dir, block index (offset table),
    // dicts, filters, footer; fsync.
    let (sections, d_write) = timed(|| {
        seal::write_metadata(
            &mut w,
            blocks_end,
            &streams,
            &rc.stream_eslots,
            &rc.blocks,
            &dict_out.dicts,
            &filters,
            N_EVENTS as u64,
        )
    });
    drop(w);

    // (e) delete the per-event entries (D5: "delete the per-event entries").
    let (_, d_drop) = timed(|| drop(index));

    let d_seal_total = t_seal.elapsed();
    let d_filters = d_f8 + d_f16 + d_c8 + d_c16;
    let (rss2, hwm2) = vm();

    println!("stage                      time");
    println!("  a consolidate index      {:>8.1} ms", d_consolidate.as_secs_f64() * 1e3);
    println!(
        "  b filters                {:>8.1} ms  (BinaryFuse8 {:.1} ms, BinaryFuse16 {:.1} ms, category pair {:.1} ms)",
        d_filters.as_secs_f64() * 1e3,
        d_f8.as_secs_f64() * 1e3,
        d_f16.as_secs_f64() * 1e3,
        (d_c8 + d_c16).as_secs_f64() * 1e3
    );
    println!("  c1 segment read          {:>8.1} ms", d_seg_read.as_secs_f64() * 1e3);
    print!("  c2 dict training         {:>8.1} ms  (", d_dicts.as_secs_f64() * 1e3);
    for (i, t) in dict_out.train_times.iter().enumerate() {
        print!(
            "{}{} {:.0} ms/{} B",
            if i > 0 { ", " } else { "" },
            CATEGORY_NAMES[i],
            t.as_secs_f64() * 1e3,
            dict_out.dicts[i].len()
        );
    }
    println!(")");
    println!(
        "  c3 recompress+write blks {:>8.1} ms  ({:.0} MB/s raw payload in)",
        d_recompress.as_secs_f64() * 1e3,
        mb(rc.cat_raw.iter().sum()) / d_recompress.as_secs_f64()
    );
    println!("  d  metadata+fsync        {:>8.1} ms", d_write.as_secs_f64() * 1e3);
    println!("  e  drop per-event index  {:>8.1} ms", d_drop.as_secs_f64() * 1e3);
    println!(
        "  TOTAL                    {:>8.2} s   ({:.1} MB/s of active segment)",
        d_seal_total.as_secs_f64(),
        mb(active_bytes) / d_seal_total.as_secs_f64()
    );
    println!(
        "mem after seal: RSS {:.0} MiB, HWM {:.0} MiB (HWM after fill was {:.0} MiB)",
        rss2 as f64 / 1024.0,
        hwm2 as f64 / 1024.0,
        hwm as f64 / 1024.0
    );

    // -----------------------------------------------------------------
    // 4. Sizes
    // -----------------------------------------------------------------
    let sealed_bytes = fs::metadata(&sealed_path).unwrap().len();
    assert_eq!(sealed_bytes, sections.total, "section accounting must match the file");
    println!("\n-- disk --");
    println!(
        "active segment: {:.1} MiB = {:.1} B/event (payloads {:.1} B/ev + framing {:.1} B/ev)",
        mb(active_bytes),
        active_bytes as f64 / N_EVENTS as f64,
        payload_bytes as f64 / N_EVENTS as f64,
        (active_bytes - payload_bytes) as f64 / N_EVENTS as f64
    );
    println!(
        "sealed total:   {:.1} MiB = {:.2} B/event  ({:.2}x smaller than active; RocksDB figure {:.1} B/ev, slice active {:.1} B/ev)",
        mb(sealed_bytes),
        sealed_bytes as f64 / N_EVENTS as f64,
        active_bytes as f64 / sealed_bytes as f64,
        VS_ROCKS_DISK_B_PER_EV,
        VS_SLICE_DISK_B_PER_EV
    );
    println!("  blocks      {:>12.2} MiB  ({:.2} B/ev)", mb(sections.blocks), sections.blocks as f64 / N_EVENTS as f64);
    println!("  ptrs        {:>12.3} MiB  ({:.3} B/ev)", mb(sections.ptrs), sections.ptrs as f64 / N_EVENTS as f64);
    println!("  stream dir  {:>12.3} MiB  ({:.3} B/ev)", mb(sections.dir), sections.dir as f64 / N_EVENTS as f64);
    println!("  block index {:>12.3} MiB  ({:.3} B/ev)", mb(sections.bidx), sections.bidx as f64 / N_EVENTS as f64);
    println!("  dicts       {:>12.3} MiB", mb(sections.dicts));
    println!("  filters     {:>12.4} MiB", mb(sections.filters));
    println!("blocks: {} of ~{} events", rc.blocks.len(), BLOCK_EVENTS);
    for c in 0..CATEGORIES {
        println!(
            "  {}: raw {:.1} MiB -> {:.1} MiB ({:.2}x) in {} blocks",
            CATEGORY_NAMES[c],
            mb(rc.cat_raw[c]),
            mb(rc.cat_comp[c]),
            rc.cat_raw[c] as f64 / rc.cat_comp[c] as f64,
            rc.cat_blocks[c]
        );
    }
    println!(
        "payload compression overall: {:.1} MiB -> {:.1} MiB ({:.2}x)",
        mb(rc.cat_raw.iter().sum()),
        mb(rc.cat_comp.iter().sum()),
        rc.cat_raw.iter().sum::<u64>() as f64 / rc.cat_comp.iter().sum::<u64>() as f64
    );

    // -----------------------------------------------------------------
    // 5. Post-seal reads
    // -----------------------------------------------------------------
    let reader = SealedReader::open(&sealed_path);
    assert_eq!(reader.n_events, N_EVENTS as u64);

    // 5a. Intermediate datapoint: packed pointers + coalesced preads on the
    // UNCOMPRESSED active segment (what packing alone buys, without seal-time
    // recompression/clustering).
    let by_stream: HashMap<u64, &StreamEvents> =
        streams.iter().map(|se| (se.stream, se)).collect();
    let packed_active = |sel: &[u64]| -> ReplayStats {
        let file = File::open(&log.path).unwrap();
        let mut st = ReplayStats::default();
        let mut buf: Vec<u8> = Vec::new();
        for &s in sel {
            let se = by_stream[&s];
            let mut i = 0;
            while i < se.evs.len() {
                let start = se.evs[i].off;
                let mut end = start + se.evs[i].len as u64;
                let mut j = i + 1;
                while j < se.evs.len() {
                    let e = &se.evs[j];
                    if e.off.saturating_sub(end) <= 4096 && e.off + e.len as u64 - start <= 1 << 20
                    {
                        end = e.off + e.len as u64;
                        j += 1;
                    } else {
                        break;
                    }
                }
                buf.resize((end - start) as usize, 0);
                file.read_exact_at(&mut buf, start).unwrap();
                for e in &se.evs[i..j] {
                    let o = (e.off - start) as usize;
                    st.events += 1;
                    st.bytes += e.len as u64;
                    st.checksum = st.checksum.wrapping_add(buf[o] as u64 + e.len as u64);
                }
                i = j;
            }
        }
        st
    };
    let (pk_rand, d_pk_rand) = timed(|| packed_active(&sample));
    let (pk_hot, d_pk_hot) = timed(|| packed_active(&[hottest]));
    assert_eq!(pk_rand.checksum, chase_rand.checksum);
    assert_eq!(pk_hot.checksum, chase_hot.checksum);

    // 5b. Sealed stream replay (packed blocks + coalesced preads + LRU).
    let mut ctx = ReadCtx::new(&reader.dicts, LRU_BLOCKS);
    let (sr_rand, d_sr_rand) = timed(|| {
        let mut st = ReplayStats::default();
        for &s in &sample {
            let r = replay_stream(&reader, &mut ctx, s);
            st.events += r.events;
            st.bytes += r.bytes;
            st.checksum = st.checksum.wrapping_add(r.checksum);
        }
        st
    });
    let (rand_hits, rand_misses, rand_preads, rand_pread_bytes) =
        (ctx.hits, ctx.misses, ctx.preads, ctx.pread_bytes);
    let mut ctx_hot = ReadCtx::new(&reader.dicts, LRU_BLOCKS);
    let (sr_hot, d_sr_hot) = timed(|| replay_stream(&reader, &mut ctx_hot, hottest));
    assert_eq!(sr_rand.checksum, chase_rand.checksum, "sealed random-stream replay diverges");
    assert_eq!(sr_hot.checksum, chase_hot.checksum, "sealed hottest replay diverges");

    println!("\n-- stream replay (1,000 random streams / hottest stream) --");
    println!(
        "pre-seal per-event chase:      {:>8.2}M ev/s  /  {:>8.2}M ev/s",
        evs(chase_rand.events, d_chase_rand) / 1e6,
        evs(chase_hot.events, d_chase_hot) / 1e6
    );
    println!(
        "packed+coalesced (uncompressed): {:>6.2}M ev/s  /  {:>8.2}M ev/s   [intermediate: packing alone]",
        evs(pk_rand.events, d_pk_rand) / 1e6,
        evs(pk_hot.events, d_pk_hot) / 1e6
    );
    println!(
        "sealed blocks (+LRU {}):      {:>8.2}M ev/s  /  {:>8.2}M ev/s",
        LRU_BLOCKS,
        evs(sr_rand.events, d_sr_rand) / 1e6,
        evs(sr_hot.events, d_sr_hot) / 1e6
    );
    println!(
        "  sealed random-stream detail: {} events, {} preads ({:.1} MiB compressed), LRU {} hits / {} misses",
        sr_rand.events, rand_preads, mb(rand_pread_bytes), rand_hits, rand_misses
    );
    println!(
        "targets: RocksDB prefix scan {:.2}M ev/s; pre-seal slice chase measured here {:.2}M ev/s",
        VS_ROCKS_STREAM_EVS / 1e6,
        evs(chase_rand.events, d_chase_rand) / 1e6
    );

    // 5c. Global replay after seal.
    let mut gctx = ReadCtx::new(&reader.dicts, 0);
    let ((g_st, g_comp), d_global) = timed(|| global_replay(&reader, &mut gctx));
    assert_eq!(g_st.events, N_EVENTS as u64);
    println!("\n-- global replay after seal (sequential block decompress scan) --");
    println!(
        "{:.2}M ev/s; {:.0} MB/s compressed file bytes, {:.0} MB/s uncompressed payload bytes",
        evs(g_st.events, d_global) / 1e6,
        mb(g_comp) / d_global.as_secs_f64(),
        mb(g_st.bytes) / d_global.as_secs_f64()
    );
    println!(
        "(active-segment scan measured above: {:.2}M ev/s, {:.0} MB/s; vertical-slice figure {:.2}M ev/s)",
        evs(n_scanned, d_active_global) / 1e6,
        mb(active_bytes) / d_active_global.as_secs_f64(),
        VS_SLICE_GLOBAL_EVS / 1e6
    );

    // 5d. Filters: empirical FPR on absent keys + size.
    println!("\n-- filters --");
    let absent_streams: Vec<u64> = (0..N_ABSENT as u64).map(|i| 10_000_000 + i).collect();
    let absent_cats: Vec<u64> = (0..N_ABSENT as u64).map(|i| CATEGORIES as u64 + i).collect();
    let fpr = |hits: usize| hits as f64 / N_ABSENT as f64 * 100.0;
    let (fp8, d_q8) = timed(|| absent_streams.iter().filter(|k| reader.s8.contains(k)).count());
    let (fp16, d_q16) = timed(|| absent_streams.iter().filter(|k| reader.s16.contains(k)).count());
    let (fpc8, _) = timed(|| absent_cats.iter().filter(|k| reader.c8.contains(k)).count());
    let (fpc16, _) = timed(|| absent_cats.iter().filter(|k| reader.c16.contains(k)).count());
    let miss8 = stream_keys.iter().filter(|k| !reader.s8.contains(k)).count();
    let miss16 = stream_keys.iter().filter(|k| !reader.s16.contains(k)).count();
    assert_eq!(miss8 + miss16, 0, "membership filters must have no false negatives");
    let s8_bytes = postcard::to_allocvec(&reader.s8).unwrap().len();
    let s16_bytes = postcard::to_allocvec(&reader.s16).unwrap().len();
    let c8_bytes = postcard::to_allocvec(&reader.c8).unwrap().len();
    println!(
        "stream BinaryFuse8:  {} keys, {} B ({:.3} B/key, {:.2} bits/key), FPR {}/{} = {:.3}%, {:.0} ns/query",
        stream_keys.len(),
        s8_bytes,
        s8_bytes as f64 / stream_keys.len() as f64,
        s8_bytes as f64 * 8.0 / stream_keys.len() as f64,
        fp8,
        N_ABSENT,
        fpr(fp8),
        d_q8.as_nanos() as f64 / N_ABSENT as f64
    );
    println!(
        "stream BinaryFuse16: {} keys, {} B ({:.3} B/key, {:.2} bits/key), FPR {}/{} = {:.4}%, {:.0} ns/query",
        stream_keys.len(),
        s16_bytes,
        s16_bytes as f64 / stream_keys.len() as f64,
        s16_bytes as f64 * 8.0 / stream_keys.len() as f64,
        fp16,
        N_ABSENT,
        fpr(fp16),
        d_q16.as_nanos() as f64 / N_ABSENT as f64
    );
    println!(
        "category BinaryFuse8: {} keys, {} B, FPR {:.3}%; BinaryFuse16 FPR {:.4}%",
        CATEGORIES,
        c8_bytes,
        fpr(fpc8),
        fpr(fpc16)
    );

    // 5e. Point reads through the sealed path.
    println!("\n-- point reads (single event by (stream, version)) --");
    let mut rng = StdRng::seed_from_u64(POINT_SEED);
    let point_sample: Vec<(u64, u64)> = (0..N_POINT_READS)
        .map(|_| {
            let (s, v) = posmap[rng.random_range(0..posmap.len())];
            (s as u64, v as u64)
        })
        .collect();

    // Cold: no cache — every read pays pread + whole-block decompress.
    let mut cctx = ReadCtx::new(&reader.dicts, 0);
    let mut durs: Vec<Duration> = Vec::with_capacity(N_POINT_READS);
    let mut sink = 0u64;
    for &(s, v) in &point_sample {
        let t = Instant::now();
        sink = sink.wrapping_add(point_read(&reader, &mut cctx, s, v));
        durs.push(t.elapsed());
    }
    let cold_mean = durs.iter().sum::<Duration>() / durs.len() as u32;
    let (cp50, cp99) = (pct(&mut durs, 0.50), pct(&mut durs, 0.99));

    // Cached: same sample with a cache big enough to hold every touched
    // block; second pass is pure cache-hit + varint-seek + slice.
    let mut wctx = ReadCtx::new(&reader.dicts, 8192);
    for &(s, v) in &point_sample {
        sink = sink.wrapping_add(point_read(&reader, &mut wctx, s, v));
    }
    let mut durs2: Vec<Duration> = Vec::with_capacity(N_POINT_READS);
    for &(s, v) in &point_sample {
        let t = Instant::now();
        sink = sink.wrapping_add(point_read(&reader, &mut wctx, s, v));
        durs2.push(t.elapsed());
    }
    let hot_mean = durs2.iter().sum::<Duration>() / durs2.len() as u32;
    let (hp50, hp99) = (pct(&mut durs2, 0.50), pct(&mut durs2, 0.99));

    // Realistic mix: Zipf-stream point reads through the small LRU.
    let zipf = Zipf::new(STREAMS as f64, ZIPF_S).unwrap();
    let zipf_sample: Vec<(u64, u64)> = (0..N_POINT_READS)
        .map(|_| loop {
            let s = (zipf.sample(&mut rng) as u64).clamp(1, STREAMS) - 1;
            if counts[s as usize] > 0 {
                break (s, rng.random_range(0..counts[s as usize]));
            }
        })
        .collect();
    let mut zctx = ReadCtx::new(&reader.dicts, LRU_BLOCKS);
    let (zsink, d_zipf) = timed(|| {
        let mut acc = 0u64;
        for &(s, v) in &zipf_sample {
            acc = acc.wrapping_add(point_read(&reader, &mut zctx, s, v));
        }
        acc
    });
    sink = sink.wrapping_add(zsink);
    println!(
        "cold (no cache, {}x uniform-random event): mean {:.1} us, p50 {:.1} us, p99 {:.1} us",
        N_POINT_READS,
        cold_mean.as_nanos() as f64 / 1e3,
        cp50.as_nanos() as f64 / 1e3,
        cp99.as_nanos() as f64 / 1e3
    );
    println!(
        "cached (same sample, all blocks resident):  mean {:.1} us, p50 {:.1} us, p99 {:.1} us",
        hot_mean.as_nanos() as f64 / 1e3,
        hp50.as_nanos() as f64 / 1e3,
        hp99.as_nanos() as f64 / 1e3
    );
    println!(
        "zipf mix through LRU {}: mean {:.1} us/read, hit rate {:.1}% ({} hits / {} misses)",
        LRU_BLOCKS,
        d_zipf.as_nanos() as f64 / N_POINT_READS as f64 / 1e3,
        zctx.hits as f64 / (zctx.hits + zctx.misses) as f64 * 100.0,
        zctx.hits,
        zctx.misses
    );
    std::hint::black_box(sink);

    // -----------------------------------------------------------------
    // 6. Correctness: byte-identity of EVERY stream across the seal
    // -----------------------------------------------------------------
    let mut vctx = ReadCtx::new(&reader.dicts, LRU_BLOCKS);
    let (mismatches, d_verify) = timed(|| {
        let mut bad = 0usize;
        let mut total = 0u64;
        for &s in &present {
            let (crc, n) = replay_stream_crc(&reader, &mut vctx, s);
            let &(want_crc, want_n) = active_crcs.get(&s).unwrap();
            if crc != want_crc || n != want_n || n != counts[s as usize] {
                bad += 1;
            }
            total += n;
        }
        assert_eq!(total, N_EVENTS as u64);
        bad
    });
    println!("\n-- correctness --");
    println!(
        "verified {} streams / {} events: sealed replay crc32(len++payload) vs active segment -> {} mismatches ({:.2}s, LRU hit rate {:.1}%)",
        present.len(),
        N_EVENTS,
        mismatches,
        d_verify.as_secs_f64(),
        vctx.hits as f64 / (vctx.hits + vctx.misses) as f64 * 100.0
    );
    assert_eq!(mismatches, 0, "seal must preserve every stream byte-for-byte");

    let (rss3, hwm3) = vm();
    println!(
        "\nmem at end: RSS {:.0} MiB, HWM {:.0} MiB",
        rss3 as f64 / 1024.0,
        hwm3 as f64 / 1024.0
    );

    // -----------------------------------------------------------------
    // 7. Cleanup (unlinking open files is fine on Linux)
    // -----------------------------------------------------------------
    if keep {
        println!("\n--keep: leaving bench_data in place");
    } else {
        fs::remove_file(&log.path).unwrap();
        fs::remove_file(&sealed_path).unwrap();
        let _ = fs::remove_dir(&log_dir);
        let _ = fs::remove_dir(root);
        println!("\ncleanup: removed bench_data files");
    }
    println!("\ndone.");
}
