//! perf_compress spike: how far can seal-time payload compression go
//! (target >= 10x) without dropping sealed stream replay below the round-3
//! figure of 2.72M ev/s?
//!
//! Method: locked baseline (the shipped seal config: row blocks, zstd-3,
//! 128 events, 16 KiB per-category dicts) -> measured sweeps -> keep wins.
//!
//!   1. zstd sweep: level {3,9,19,22} x block {128,512,2048} x dict
//!      {none, 16Ki, 64Ki, 110Ki}, dict training timed.
//!   2. columnar shredding prototype with byte-exact reassembly proof.
//!   3. hybrid columnar/row split by category.
//!   4. hot-tier codecs (lz4, zstd-1) for the two-tier story.
//!
//! Every config reports: ratio, compress cost (single-thread-equivalent
//! CPU + parallel wall), sequential decompress MB/s + ev/s, sealed stream
//! replay ev/s (1,000 random streams, coalesced preads + LRU — the
//! methodology behind the 2.72M ev/s number), and point-read latency.

mod columnar;
mod store;
mod workload;

use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use columnar::CatShred;
use store::{
    allin_bytes, cache_blocks_for, compress_write, global_scan, point_read, replay_streams,
    row_pack, Built, Codec, Fmt, RawBlock, ReadCtx,
};
use workload::{Corpus, CATEGORIES, CATEGORY_NAMES};

const N_EVENTS: usize = 1_000_000;
const FILL_SEED: u64 = 7;
const SAMPLE_SEED: u64 = 99;
const POINT_SEED: u64 = 123;
const N_SAMPLE_STREAMS: usize = 1_000;
const N_POINT_READS: usize = 5_000;

/// Round-3 sealed replay reference (spikes/seal_pipeline, this machine).
const REF_REPLAY_EVS: f64 = 2_720_000.0;

fn timed<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let t = Instant::now();
    let out = f();
    (out, t.elapsed())
}

fn mb(b: u64) -> f64 {
    b as f64 / (1024.0 * 1024.0)
}

struct Bench {
    n_events: usize,
    raw_payload: u64,
    json_bytes: u64,
    sample: Vec<u32>,
    points: Vec<(u32, u32)>,
}

#[allow(clippy::too_many_arguments)]
fn bench_config(
    tag: &str,
    b: &Built,
    c: &Corpus,
    shreds: &[CatShred],
    bench: &Bench,
    pack_cpu: Duration,
    train_ms: f64,
    verify: bool,
    extra: &str,
) -> (f64, f64) {
    // Sequential full scan (single-threaded decompress + slice/reassemble).
    // Two runs (verify on the first), min time kept — page cache warm both.
    let mut ctx = ReadCtx::new(b, 0);
    let ((gst, mism), d_global0) = timed(|| global_scan(b, c, shreds, &mut ctx, verify));
    let (_, d_global1) = timed(|| global_scan(b, c, shreds, &mut ctx, false));
    let d_global = d_global0.min(d_global1);
    assert_eq!(gst.events, bench.n_events as u64);
    if verify {
        assert_eq!(mism, 0, "byte-exact verification FAILED: {mism} mismatching events");
    }

    // Stream replay, seal_pipeline methodology. Two runs, min kept.
    let mut ctx = ReadCtx::new(b, cache_blocks_for(b.be));
    let (rst, d_replay0) = timed(|| replay_streams(b, c, shreds, &mut ctx, &bench.sample));
    let mut ctx = ReadCtx::new(b, cache_blocks_for(b.be));
    let (rst1, d_replay1) = timed(|| replay_streams(b, c, shreds, &mut ctx, &bench.sample));
    assert_eq!(rst.checksum, rst1.checksum);
    let d_replay = d_replay0.min(d_replay1);

    // Point reads.
    let mut ctx = ReadCtx::new(b, cache_blocks_for(b.be));
    let (psink, d_point) = timed(|| {
        let mut sink = 0u64;
        for &(s, v) in &bench.points {
            sink = sink.wrapping_add(point_read(b, c, shreds, &mut ctx, s, v));
        }
        sink
    });

    let ratio = bench.raw_payload as f64 / b.comp_total as f64;
    let jratio = bench.json_bytes as f64 / b.comp_total as f64;
    let bpe = b.comp_total as f64 / bench.n_events as f64;
    let allin = allin_bytes(b, c, shreds) as f64 / bench.n_events as f64;
    let comp_st_mbps = mb(bench.raw_payload) / b.comp_cpu.as_secs_f64();
    let global_mevs = gst.events as f64 / d_global.as_secs_f64() / 1e6;
    let global_mbps = mb(gst.bytes) / d_global.as_secs_f64();
    let replay_mevs = rst.events as f64 / d_replay.as_secs_f64() / 1e6;
    let point_us = d_point.as_secs_f64() * 1e6 / bench.points.len() as f64;
    let percat: Vec<String> = (0..CATEGORIES)
        .map(|cat| {
            let lo = c.cat_start[cat];
            let hi = c.cat_start[cat + 1];
            let raw: u64 = c.evs[lo..hi].iter().map(|e| e.len as u64).sum();
            format!("{:.2}", raw as f64 / b.comp_cat[cat] as f64)
        })
        .collect();
    println!(
        "RESULT kind={tag} be={} dict={} lvl={} comp_MiB={:.1} ratio={ratio:.2} jratio={jratio:.2} bpe={bpe:.1} allin_bpe={allin:.1} \
         train_ms={train_ms:.0} pack_s={:.2} comp_cpu_s={:.2} comp_st_MBps={comp_st_mbps:.0} comp_wall_s={:.2} \
         global_Mevs={global_mevs:.2} global_MBps={global_mbps:.0} replay_Mevs={replay_mevs:.2} replay_ev={} \
         point_us={point_us:.1} verify={} psink={psink} ck={}{extra} percat={}",
        b.be,
        b.dicts[0].as_ref().map_or(0, |d| d.len()),
        match b.codec {
            Codec::Zstd(l) => l.to_string(),
            Codec::Lz4 => "lz4".into(),
        },
        mb(b.comp_total),
        pack_cpu.as_secs_f64(),
        b.comp_cpu.as_secs_f64(),
        b.wall.as_secs_f64(),
        rst.events,
        if verify { format!("OK({} ev, 0 mismatch)", gst.events) } else { "-".into() },
        gst.checksum,
        percat.join(":"),
    );
    (ratio, replay_mevs)
}

fn train_dicts(
    c: &Corpus,
    max_bytes: usize,
    n_samples: usize,
) -> ([Vec<u8>; CATEGORIES], Duration) {
    let t = Instant::now();
    let mut out: [Vec<u8>; CATEGORIES] = Default::default();
    for cat in 0..CATEGORIES {
        let lo = c.cat_start[cat];
        let hi = c.cat_start[cat + 1];
        let stride = ((hi - lo) / n_samples).max(1);
        let samples: Vec<&[u8]> =
            (lo..hi).step_by(stride).take(n_samples).map(|i| c.bytes_of(i)).collect();
        out[cat] = zstd::dict::from_samples(&samples, max_bytes).expect("dict training");
    }
    (out, t.elapsed())
}

fn some_dicts(d: &[Vec<u8>; CATEGORIES]) -> [Option<Vec<u8>>; CATEGORIES] {
    let mut out: [Option<Vec<u8>>; CATEGORIES] = Default::default();
    for (i, v) in d.iter().enumerate() {
        out[i] = Some(v.clone());
    }
    out
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let quick = args.iter().any(|a| a == "--quick");
    let keep = args.iter().any(|a| a == "--keep");
    let n_events = if quick { 100_000 } else { N_EVENTS };

    let root = Path::new("bench_data");
    let _ = fs::remove_dir_all(root);
    fs::create_dir_all(root).unwrap();
    let bpath = root.join("blocks.bin");

    let cpu = fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| {
            s.lines().find(|l| l.starts_with("model name")).map(|l| {
                l.split(':').nth(1).unwrap_or("").trim().to_string()
            })
        })
        .unwrap_or_default();
    println!("== perf_compress spike ==");
    println!("machine: {cpu}, {} threads (rayon for seal-side compression only; all read paths single-threaded)", rayon::current_num_threads());
    println!(
        "workload: {} events, msgpack-named (codec_id 1), {} streams Zipf({}), {} categories, batches of {}, seed {}",
        n_events,
        workload::STREAMS,
        workload::ZIPF_S,
        CATEGORIES,
        workload::BATCH,
        FILL_SEED
    );

    // -----------------------------------------------------------------
    // Corpus
    // -----------------------------------------------------------------
    let (c, d_gen) = timed(|| workload::generate(n_events, FILL_SEED));
    let raw_payload = c.arena.len() as u64;
    println!(
        "corpus: {:.1} MiB msgpack-named payloads, {:.1} B/event avg (JSON-equivalent {:.1} B/event -> codec alone banks {:.2}x), {} streams present, hottest stream {} ({} events), generated+clustered in {:.1}s",
        mb(raw_payload),
        raw_payload as f64 / n_events as f64,
        c.json_bytes as f64 / n_events as f64,
        c.json_bytes as f64 / raw_payload as f64,
        c.present.len(),
        c.hottest,
        c.stream_range.get(&c.hottest).map_or(0, |r| r.1),
        d_gen.as_secs_f64()
    );
    for cat in 0..CATEGORIES {
        let lo = c.cat_start[cat];
        let hi = c.cat_start[cat + 1];
        let raw: u64 = c.evs[lo..hi].iter().map(|e| e.len as u64).sum();
        println!(
            "  {}: {} events, {:.1} B/event avg",
            CATEGORY_NAMES[cat],
            hi - lo,
            raw as f64 / (hi - lo) as f64
        );
    }

    // Samples (methodology of seal_pipeline: 1,000 distinct random present
    // streams excluding the hottest; 5,000 Zipf point reads).
    let sample: Vec<u32> = {
        let mut rng = StdRng::seed_from_u64(SAMPLE_SEED);
        let mut set = std::collections::HashSet::new();
        let mut out = Vec::with_capacity(N_SAMPLE_STREAMS);
        while out.len() < N_SAMPLE_STREAMS.min(c.present.len() - 1) {
            let s = c.present[rng.random_range(0..c.present.len())];
            if s != c.hottest && set.insert(s) {
                out.push(s);
            }
        }
        out
    };
    let points: Vec<(u32, u32)> = {
        let mut rng = StdRng::seed_from_u64(POINT_SEED);
        let zipf = rand_distr::Zipf::new(workload::STREAMS as f64, workload::ZIPF_S).unwrap();
        let mut out = Vec::with_capacity(N_POINT_READS);
        while out.len() < N_POINT_READS {
            use rand_distr::Distribution;
            let s = (zipf.sample(&mut rng) as u64).clamp(1, workload::STREAMS) as u32 - 1;
            if let Some(&(_, len)) = c.stream_range.get(&s) {
                out.push((s, rng.random_range(0..len)));
            }
        }
        out
    };
    let bench = Bench { n_events, raw_payload, json_bytes: c.json_bytes, sample, points };

    // -----------------------------------------------------------------
    // Dictionary training (timed per size)
    // -----------------------------------------------------------------
    let specs = [(16 * 1024, 5_000usize), (64 * 1024, 20_000), (110 * 1024, 20_000)];
    let mut dictsets: Vec<[Vec<u8>; CATEGORIES]> = Vec::new();
    let mut dict_train_ms: Vec<f64> = Vec::new();
    for &(sz, ns) in &specs {
        let (ds, d) = train_dicts(&c, sz, ns);
        println!(
            "DICT max={} samples={} train_s={:.2} sizes={}",
            sz,
            ns,
            d.as_secs_f64(),
            ds.iter().map(|d| d.len().to_string()).collect::<Vec<_>>().join(":")
        );
        dict_train_ms.push(d.as_secs_f64() * 1e3);
        dictsets.push(ds);
    }

    let row_fmts = [Fmt::Row; CATEGORIES];
    let no_dicts: [Option<Vec<u8>>; CATEGORIES] = Default::default();
    let no_shreds: Vec<CatShred> = Vec::new();

    // -----------------------------------------------------------------
    // LOCKED BASELINE: row, zstd-3, 128-event blocks, 16 KiB dicts
    // -----------------------------------------------------------------
    println!("\n== locked baseline (seal_pipeline shipped config) ==");
    {
        let (raw, pack_cpu) = row_pack(&c, 128);
        let b = compress_write(&raw, row_fmts, Codec::Zstd(3), some_dicts(&dictsets[0]), 128, &bpath);
        bench_config("baseline", &b, &c, &no_shreds, &bench, pack_cpu, dict_train_ms[0], true, "");
        println!(
            "reference: round-3 sealed replay 2.72M ev/s (JSON corpus); replay floor for this spike = the baseline row above"
        );
    }

    // -----------------------------------------------------------------
    // Sweep 1: zstd level x block size x dict
    // -----------------------------------------------------------------
    println!("\n== sweep 1: row blocks, zstd level x block x dict ==");
    for &be in &[128usize, 512, 2048] {
        let (raw, pack_cpu) = row_pack(&c, be);
        for (di, dict) in [None, Some(0usize), Some(1), Some(2)].iter().enumerate() {
            for &lvl in &[3i32, 9, 19, 22] {
                let (dicts, tms) = match dict {
                    None => (no_dicts.clone(), 0.0),
                    Some(i) => (some_dicts(&dictsets[*i]), dict_train_ms[*i]),
                };
                let b = compress_write(&raw, row_fmts, Codec::Zstd(lvl), dicts, be, &bpath);
                let verify = lvl == 3 && di == 0; // one verify per block size
                bench_config("row", &b, &c, &no_shreds, &bench, pack_cpu, tms, verify, "");
            }
        }
    }

    // -----------------------------------------------------------------
    // Sweep 2: columnar shredding
    // -----------------------------------------------------------------
    println!("\n== sweep 2: columnar shredding (byte-exact) ==");
    for &be in &[128usize, 512, 2048] {
        let (raw, shreds, shred_cpu) = columnar::shred_all(&c, be);
        let raw_col: u64 = raw.iter().flatten().map(|b| b.buf.len() as u64).sum();
        println!(
            "SHRED be={be} shred_s={:.2} shred_Mevs={:.2} col_raw_MiB={:.1} (vs payload {:.1} MiB) skels={} cols={}",
            shred_cpu.as_secs_f64(),
            n_events as f64 / shred_cpu.as_secs_f64() / 1e6,
            mb(raw_col),
            mb(raw_payload),
            shreds.iter().map(|s| s.skels.len().to_string()).collect::<Vec<_>>().join(":"),
            shreds.iter().map(|s| s.col_kind.len().to_string()).collect::<Vec<_>>().join(":"),
        );
        for &lvl in &[3i32, 9, 19, 22] {
            let b = compress_write(&raw, [Fmt::Col; CATEGORIES], Codec::Zstd(lvl), no_dicts.clone(), be, &bpath);
            let verify = lvl == 3; // full byte-exact proof once per block size
            bench_config("col", &b, &c, &shreds, &bench, shred_cpu, 0.0, verify, "");
        }
    }

    // -----------------------------------------------------------------
    // Sweep 3: hybrid (columnar where it pays, row + dict for the rest)
    // -----------------------------------------------------------------
    println!("\n== sweep 3: hybrid columnar/row ==");
    // Highest-volume categories get columnar; tail categories row+dict.
    // Category volume = event count (cat 0 holds the hottest streams).
    {
        let be = 512usize;
        let lvl = 19i32;
        let (rraw, rpack) = row_pack(&c, be);
        let (craw, shreds, shred_cpu) = columnar::shred_all(&c, be);
        // Variants: (a) highest-volume category columnar (the prompt's
        // heuristic; account holds the hottest streams), (b) the categories
        // where columnar's measured per-cat win is largest (numeric-heavy
        // order+sensor), (c) all but account.
        let variants: [&[usize]; 3] = [&[0], &[1, 3], &[1, 2, 3]];
        for mask in variants {
            let mut fmts = [Fmt::Row; CATEGORIES];
            let mut per_cat: [Vec<RawBlock>; CATEGORIES] = Default::default();
            let mut dicts = some_dicts(&dictsets[2]);
            for &cat in mask {
                fmts[cat] = Fmt::Col;
                dicts[cat] = None;
            }
            for cat in 0..CATEGORIES {
                let src = match fmts[cat] {
                    Fmt::Col => &craw[cat],
                    Fmt::Row => &rraw[cat],
                };
                per_cat[cat] = src
                    .iter()
                    .map(|b| RawBlock { count: b.count, buf: b.buf.clone() })
                    .collect();
            }
            let b = compress_write(&per_cat, fmts, Codec::Zstd(lvl), dicts, be, &bpath);
            let cols: Vec<&str> = (0..CATEGORIES)
                .filter(|&i| fmts[i] == Fmt::Col)
                .map(|i| CATEGORY_NAMES[i])
                .collect();
            bench_config(
                "hybrid",
                &b,
                &c,
                &shreds,
                &bench,
                rpack + shred_cpu,
                dict_train_ms[2],
                true,
                &format!(" colcats={}", cols.join("+")),
            );
        }
    }

    // -----------------------------------------------------------------
    // Sweep 4: hot-tier codecs (recently-sealed tier)
    // -----------------------------------------------------------------
    println!("\n== sweep 4: hot-tier codecs (lz4 / zstd-1) ==");
    for &be in &[128usize, 512] {
        let (raw, pack_cpu) = row_pack(&c, be);
        let b = compress_write(&raw, row_fmts, Codec::Lz4, no_dicts.clone(), be, &bpath);
        bench_config("hot", &b, &c, &no_shreds, &bench, pack_cpu, 0.0, false, "");
        let b = compress_write(&raw, row_fmts, Codec::Zstd(1), some_dicts(&dictsets[0]), be, &bpath);
        bench_config("hot", &b, &c, &no_shreds, &bench, pack_cpu, dict_train_ms[0], false, "");
    }

    let _ = REF_REPLAY_EVS;
    if !keep {
        let _ = fs::remove_dir_all(root);
        println!("\nbench_data cleaned");
    }
    println!("done");
}
