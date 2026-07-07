//! perf_replay spike: baseline-vs-optimized performance work on the sealed
//! segment paths measured in spikes/seal_pipeline and spikes/recovery_scale.
//!
//! Subcommands (bench_data lives next to Cargo.toml):
//!   gen                     fill the 256 MiB / 1M-event active segment
//!   seal base|par           seal it (baseline / parallel); `base` writes the
//!                           canonical sealed file and verifies byte-identity
//!                           vs the active segment; `par` asserts its output
//!                           file is byte-identical to the canonical one
//!   bench replay            warm global + stream replay, baseline vs optimized
//!   bench point             point-read latency, linear seek vs skip table
//!   bench cold              cold-cache (fadvise DONTNEED) global/stream replay
//!   prof <name> [reps]      hot loops for `perf stat` / `perf record`
//!   recovery [gib]          multi-segment recovery scan, 1 thread vs rayon
//!   clean                   delete bench_data

mod opt;
mod recovery;
mod seal;
mod seglog;
mod workload;

use std::collections::HashSet;
use std::env;
use std::fs::{self, File};
use std::io::BufWriter;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rand_distr::{Distribution, Zipf};
use xorf::{BinaryFuse16, BinaryFuse8};

use opt::{
    active_ground_truth, global_replay_par, point_read_skip, recompress_par,
    stream_replay_batch, stream_replay_par_preads, train_dicts_par,
};
use seal::{
    consolidate, global_replay, point_read, recompress, replay_stream, replay_stream_crc,
    train_dicts, ActiveIndex, EvRef, Filters, ReadCtx, SealedReader,
};
use seglog::{scan, ReplayStats, SegmentWriter, StopReason};
use workload::{payload, BATCH, STREAMS, ZIPF_S};

const N_EVENTS: usize = 1_000_000;
const FILL_SEED: u64 = 7;
const SAMPLE_SEED: u64 = 99;
const POINT_SEED: u64 = 123;
const N_SAMPLE_STREAMS: usize = 1_000;
const N_POINT_READS: usize = 10_000;
const LRU_BLOCKS: usize = 256;

fn data_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bench_data")
}

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

fn evs(events: u64, d: Duration) -> f64 {
    events as f64 / d.as_secs_f64()
}

fn pct(durs: &mut [Duration], p: f64) -> Duration {
    durs.sort_unstable();
    durs[((durs.len() as f64 * p) as usize).min(durs.len() - 1)]
}

fn us(d: Duration) -> f64 {
    d.as_nanos() as f64 / 1e3
}

fn crc_file(path: &Path) -> u32 {
    let data = fs::read(path).unwrap();
    let mut h = crc32fast::Hasher::new();
    h.update(&data);
    h.finalize()
}

fn evict_path(path: &Path) {
    if let Ok(f) = File::open(path) {
        unsafe {
            libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
        }
    }
    std::thread::sleep(Duration::from_millis(200));
}

fn active_log_path() -> PathBuf {
    data_root().join("log").join("seg-000000.log")
}

fn sealed_path() -> PathBuf {
    data_root().join("seg-000000.seal")
}

// ---------------------------------------------------------------------------
// gen — fill the active segment (identical workload shape to seal_pipeline:
// 1M events, batches of 10, 10k streams Zipf(1.1), 4 categories, seed 7)
// ---------------------------------------------------------------------------

fn cmd_gen() {
    let root = data_root();
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let mut log = SegmentWriter::create(&root.join("log"));
    let mut counts = vec![0u64; STREAMS as usize];
    let (_, d_fill) = timed(|| {
        let mut rng = StdRng::seed_from_u64(FILL_SEED);
        let zipf = Zipf::new(STREAMS as f64, ZIPF_S).unwrap();
        for _ in 0..N_EVENTS / BATCH {
            let s = (zipf.sample(&mut rng) as u64).clamp(1, STREAMS) - 1;
            let first = counts[s as usize];
            let payloads: Vec<Vec<u8>> =
                (0..BATCH as u64).map(|i| payload(&mut rng, s, first + i)).collect();
            log.append(s, first, &payloads);
            counts[s as usize] += BATCH as u64;
        }
        log.file.sync_data().unwrap();
    });
    let present = counts.iter().filter(|&&c| c > 0).count();
    println!(
        "gen: {} events -> {:.1} MiB active segment in {:.2}s; {} streams present",
        N_EVENTS,
        mb(log.seg_len),
        d_fill.as_secs_f64(),
        present
    );
}

// ---------------------------------------------------------------------------
// seal — baseline vs parallel
// ---------------------------------------------------------------------------

fn load_index(seg: &[u8]) -> ActiveIndex {
    let rec = scan(seg, 0, 0);
    assert_eq!(rec.stop, StopReason::EndOfLog);
    let mut index = ActiveIndex::new();
    let mut pos = 0u32;
    for b in &rec.batches {
        for (i, &(o, l)) in b.payloads.iter().enumerate() {
            index.insert(
                (b.stream_id, b.first_stream_version + i as u64),
                EvRef { pos, off: o, len: l },
            );
            pos += 1;
        }
    }
    index
}

fn cmd_seal(mode: &str) {
    let par = match mode {
        "base" => false,
        "par1" | "par" => true, // par1 = barrier between dicts and recompress
        _ => panic!("seal mode must be base|par1|par"),
    };
    let pipelined = mode == "par";
    let seg_path = active_log_path();
    let setup = fs::read(&seg_path).unwrap();
    let active_bytes = setup.len() as u64;
    let index = load_index(&setup);
    assert_eq!(index.len(), N_EVENTS);
    drop(setup);
    let out_path = if par { data_root().join("reseal-par.seal") } else { sealed_path() };
    if par {
        // rayon pool spin-up outside the timed region (a running system's
        // pool already exists; first-use cost is ~1 ms of thread spawns).
        use rayon::prelude::*;
        let _: u64 = (0..1_000u64).into_par_iter().sum();
    }

    let t_total = Instant::now();

    // (a) consolidate
    let (streams, d_a) = timed(|| consolidate(&index));

    // (e') optimized modes drop the per-event index on a background thread —
    // it is dead after consolidate, so its teardown is off the critical path.
    // NOTE: a "background drop" of the index (std::thread::spawn(move ||
    // drop(index)) after consolidate) was tried and REJECTED: freeing 1M
    // BTreeMap nodes cross-thread contends the glibc allocator with ZDICT's
    // workspace allocations and slowed per-category dict training 2.5-3x
    // (125 ms -> ~290 ms), costing far more than the ~18 ms it saved.
    let mut index = Some(index);
    let drop_handle: Option<std::thread::JoinHandle<()>> = None;

    // (b) filters
    let (filters, d_b) = timed(|| {
        let stream_keys: Vec<u64> = streams.iter().map(|se| se.stream).collect();
        let cat_keys: Vec<u64> = (0..workload::CATEGORIES as u64).collect();
        Filters {
            s8: BinaryFuse8::try_from(stream_keys.as_slice()).unwrap(),
            s16: BinaryFuse16::try_from(stream_keys.as_slice()).unwrap(),
            c8: BinaryFuse8::try_from(cat_keys.as_slice()).unwrap(),
            c16: BinaryFuse16::try_from(cat_keys.as_slice()).unwrap(),
        }
    });

    // (c1) seal input: baseline re-reads the segment into a buffer; the
    // optimized pass mmaps it (zero-copy; fault cost lands in c2/c3).
    let mut seg_vec: Vec<u8> = Vec::new();
    let mmap;
    let (seg, d_c1): (&[u8], Duration) = if par {
        let (m, d) = timed(|| {
            let f = File::open(&seg_path).unwrap();
            unsafe { memmap2::Mmap::map(&f).unwrap() }
        });
        mmap = m;
        (&mmap[..], d)
    } else {
        let (v, d) = timed(|| fs::read(&seg_path).unwrap());
        seg_vec = v;
        (&seg_vec[..], d)
    };

    // (c2) dictionary training + (c3) recompress + write blocks.
    let out_file = File::create(&out_path).unwrap();
    let mut w = BufWriter::with_capacity(8 << 20, out_file);
    use std::io::Write;
    w.write_all(&seal::FILE_MAGIC.to_le_bytes()).unwrap();
    let (dict_out, rc, d_c2, d_c3);
    if pipelined {
        let ((d_o, r_o), d) = timed(|| opt::dicts_and_recompress_pipelined(seg, &streams, &mut w, 8));
        dict_out = d_o;
        rc = r_o;
        d_c2 = Duration::ZERO;
        d_c3 = d;
    } else {
        let (d_o, d2) =
            timed(|| if par { train_dicts_par(seg, &streams) } else { train_dicts(seg, &streams) });
        dict_out = d_o;
        d_c2 = d2;
        let (r_o, d3) = timed(|| {
            if par {
                recompress_par(seg, &streams, &dict_out.dicts, &mut w, 8)
            } else {
                recompress(seg, &streams, &dict_out.dicts, &mut w, 8)
            }
        });
        rc = r_o;
        d_c3 = d3;
    }
    let blocks_end = 8 + rc.blocks.iter().map(|b| b.comp_len as u64).sum::<u64>();
    if par {
        // Kick async writeback of the 40 MiB block region now, so the final
        // fsync in write_metadata overlaps the metadata build instead of
        // paying the full flush at the end.
        w.flush().unwrap();
        unsafe {
            libc::sync_file_range(
                w.get_ref().as_raw_fd(),
                0,
                0,
                libc::SYNC_FILE_RANGE_WRITE,
            );
        }
    }

    // (d) metadata + fsync (shared code path in all modes)
    let (sections, d_d) = timed(|| {
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

    // (e) drop per-event index (baseline: on the critical path; optimized:
    // join the background drop, which finished long ago)
    let (_, d_e) = timed(|| match drop_handle {
        Some(h) => h.join().unwrap(),
        None => drop(index.take()),
    });
    let d_total = t_total.elapsed();
    let (rss, hwm) = vm();

    let sealed_bytes = fs::metadata(&out_path).unwrap().len();
    assert_eq!(sealed_bytes, sections.total);
    println!("== seal {} ==", mode);
    println!("  a consolidate      {:>8.1} ms", d_a.as_secs_f64() * 1e3);
    println!("  b filters          {:>8.1} ms", d_b.as_secs_f64() * 1e3);
    println!(
        "  c1 segment input   {:>8.1} ms  ({})",
        d_c1.as_secs_f64() * 1e3,
        if par { "mmap" } else { "read into buffer" }
    );
    if pipelined {
        print!("  c2+c3 dicts+recompress {:>4.1} ms  (pipelined; per-cat dict train:", d_c3.as_secs_f64() * 1e3);
    } else {
        print!("  c2 dict training   {:>8.1} ms  (per cat:", d_c2.as_secs_f64() * 1e3);
    }
    for t in &dict_out.train_times {
        print!(" {:.0}ms", t.as_secs_f64() * 1e3);
    }
    println!(")");
    if !pipelined {
        println!("  c3 recompress+wr   {:>8.1} ms", d_c3.as_secs_f64() * 1e3);
    }
    println!("  d  metadata+fsync  {:>8.1} ms", d_d.as_secs_f64() * 1e3);
    println!("  e  drop index      {:>8.1} ms", d_e.as_secs_f64() * 1e3);
    println!(
        "  TOTAL              {:>8.3} s   ({:.0} MB/s of active segment)",
        d_total.as_secs_f64(),
        mb(active_bytes) / d_total.as_secs_f64()
    );
    println!(
        "  sealed: {:.1} MiB = {:.2} B/event (blocks {:.1} MiB, ptrs {:.2} MiB, skips {:.2} MiB); {} blocks",
        mb(sealed_bytes),
        sealed_bytes as f64 / N_EVENTS as f64,
        mb(sections.blocks),
        mb(sections.ptrs),
        mb(sections.skips),
        rc.blocks.len()
    );
    println!("  mem: RSS {:.0} MiB, HWM {:.0} MiB", rss as f64 / 1024.0, hwm as f64 / 1024.0);
    let crc = crc_file(&out_path);
    println!("  file crc32: {:08x}", crc);

    if par {
        // Correctness gate: byte-identical to the canonical baseline seal,
        // EXCLUDING the filters section — xorf's BinaryFuse construction
        // seeds its hash randomly per process, so the (identical, sequential)
        // filter-build code emits different bytes each run in BOTH modes.
        // Everything the parallel code touches (blocks, ptrs, skips, dir,
        // block index, dicts, footer) must match exactly.
        let a = fs::read(&out_path).unwrap();
        let b = fs::read(sealed_path()).unwrap();
        // The filters section length itself varies run to run (postcard
        // varint-encodes xorf's random seeds), so compare everything up to
        // filters_off plus the footer fields that precede it.
        let fa = seglog::rd_u64(&a[a.len() - 80..], 40) as usize;
        let fb = seglog::rd_u64(&b[b.len() - 80..], 40) as usize;
        assert_eq!(fa, fb, "filters_off differs — earlier sections diverge");
        assert_eq!(a[..fa], b[..fa], "bytes differ before filters section");
        assert_eq!(a[a.len() - 80..], b[b.len() - 80..], "footers differ");
        println!(
            "  BYTE-IDENTICAL to baseline sealed file ({} bytes = blocks+ptrs+skips+dir+bidx+dicts; filters section excluded: xorf seeds randomly per process)",
            fa
        );
        fs::remove_file(&out_path).unwrap();
    } else {
        // Correctness gate: every stream replays byte-identically vs the
        // active segment (same check as seal_pipeline).
        let seg2 = fs::read(&seg_path).unwrap();
        let rec = scan(&seg2, 0, 0);
        let mut crcs: std::collections::HashMap<u64, (crc32fast::Hasher, u64)> =
            std::collections::HashMap::new();
        for b in &rec.batches {
            let e = crcs.entry(b.stream_id).or_default();
            for &(o, l) in &b.payloads {
                e.0.update(&l.to_le_bytes());
                e.0.update(&seg2[o as usize..(o + l as u64) as usize]);
                e.1 += 1;
            }
        }
        let want: std::collections::HashMap<u64, (u32, u64)> =
            crcs.into_iter().map(|(s, (h, n))| (s, (h.finalize(), n))).collect();
        let reader = SealedReader::open(&out_path);
        let mut ctx = ReadCtx::new(&reader.dicts, LRU_BLOCKS);
        let mut bad = 0usize;
        let mut total = 0u64;
        for &s in &reader.stream_ids {
            let (crc, n) = replay_stream_crc(&reader, &mut ctx, s);
            let &(wc, wn) = want.get(&s).unwrap();
            if crc != wc || n != wn {
                bad += 1;
            }
            total += n;
        }
        assert_eq!(total, N_EVENTS as u64);
        assert_eq!(bad, 0, "sealed replay diverges from active segment");
        println!(
            "  verified {} streams / {} events byte-identical across the seal (0 mismatches)",
            reader.stream_ids.len(),
            total
        );
    }
}

// ---------------------------------------------------------------------------
// Shared bench setup
// ---------------------------------------------------------------------------

struct Setup {
    reader: SealedReader,
    sample: Vec<u64>,
    hottest: u64,
}

fn setup(warm: bool) -> Setup {
    let reader = SealedReader::open(&sealed_path());
    if warm {
        // Warm the page cache for the sealed file (covers pread + mmap paths).
        let _ = fs::read(sealed_path()).unwrap();
    }
    // hottest = max count; ties -> last (matches seal_pipeline's max_by_key).
    let mut hottest = 0u64;
    let mut hc = 0u32;
    for &s in &reader.stream_ids {
        let c = reader.dir[&s].count;
        if c >= hc {
            hc = c;
            hottest = s;
        }
    }
    // 1,000 distinct random present streams excluding the hottest, seed 99
    // (same construction as seal_pipeline).
    let present = &reader.stream_ids;
    let mut rng = StdRng::seed_from_u64(SAMPLE_SEED);
    let mut set = HashSet::new();
    let mut sample = Vec::with_capacity(N_SAMPLE_STREAMS);
    while sample.len() < N_SAMPLE_STREAMS {
        let s = present[rng.random_range(0..present.len())];
        if s != hottest && set.insert(s) {
            sample.push(s);
        }
    }
    Setup { reader, sample, hottest }
}

fn expected_stats(gt: &std::collections::HashMap<u64, ReplayStats>, streams: &[u64]) -> ReplayStats {
    let mut st = ReplayStats::default();
    for s in streams {
        let g = gt[s];
        st.events += g.events;
        st.bytes += g.bytes;
        st.checksum = st.checksum.wrapping_add(g.checksum);
    }
    st
}

// ---------------------------------------------------------------------------
// bench replay (warm)
// ---------------------------------------------------------------------------

fn cmd_bench_replay() {
    let su = setup(true);
    let reader = &su.reader;
    let seg = fs::read(active_log_path()).unwrap();
    let gt = active_ground_truth(&seg);
    drop(seg);
    let all: Vec<u64> = reader.stream_ids.clone();
    let want_global = expected_stats(&gt, &all);
    let want_sample = expected_stats(&gt, &su.sample);
    let want_hot = expected_stats(&gt, &[su.hottest]);
    assert_eq!(want_global.events, N_EVENTS as u64);

    // rayon pool + mmap warm-up (untimed).
    let (w, _) = global_replay_par(reader);
    assert_eq!(w, want_global);

    const REPS: usize = 5;
    let best = |mut ds: Vec<Duration>| -> Duration {
        ds.sort_unstable();
        ds[0]
    };

    println!("== bench replay (page-cache warm, best of {REPS}) ==");

    // 1. Global replay.
    let mut ds = Vec::new();
    for _ in 0..REPS {
        let mut ctx = ReadCtx::new(&reader.dicts, 0);
        let ((st, _), d) = timed(|| global_replay(reader, &mut ctx));
        assert_eq!(st, want_global);
        ds.push(d);
    }
    let d_gseq = best(ds);
    let mut ds = Vec::new();
    for _ in 0..REPS {
        let ((st, _), d) = timed(|| global_replay_par(reader));
        assert_eq!(st, want_global);
        ds.push(d);
    }
    let d_gpar = best(ds);
    println!(
        "global   baseline seq        {:>8.2}M ev/s   ({:.1} ms)",
        evs(want_global.events, d_gseq) / 1e6,
        d_gseq.as_secs_f64() * 1e3
    );
    println!(
        "global   rayon par blocks    {:>8.2}M ev/s   ({:.1} ms)  {:.2}x",
        evs(want_global.events, d_gpar) / 1e6,
        d_gpar.as_secs_f64() * 1e3,
        d_gseq.as_secs_f64() / d_gpar.as_secs_f64()
    );

    // 2. Stream replay, 1000 random streams.
    let mut ds = Vec::new();
    for _ in 0..REPS {
        let mut ctx = ReadCtx::new(&reader.dicts, LRU_BLOCKS);
        let (st, d) = timed(|| {
            let mut st = ReplayStats::default();
            for &s in &su.sample {
                let r = replay_stream(reader, &mut ctx, s);
                st.events += r.events;
                st.bytes += r.bytes;
                st.checksum = st.checksum.wrapping_add(r.checksum);
            }
            st
        });
        assert_eq!(st, want_sample);
        ds.push(d);
    }
    let d_sseq = best(ds);
    let mut ds = Vec::new();
    for _ in 0..REPS {
        let (st, d) = timed(|| stream_replay_par_preads(reader, &su.sample, 64));
        assert_eq!(st, want_sample);
        ds.push(d);
    }
    let d_spar = best(ds);
    let mut ds = Vec::new();
    let mut ub = 0;
    for _ in 0..REPS {
        let (out, d) = timed(|| stream_replay_batch(reader, &su.sample));
        assert_eq!(out.stats, want_sample);
        ub = out.unique_blocks;
        ds.push(d);
    }
    let d_sbatch = best(ds);
    println!(
        "streams  baseline seq (LRU {LRU_BLOCKS}) {:>5.2}M ev/s   ({:.1} ms, {} events)",
        evs(want_sample.events, d_sseq) / 1e6,
        d_sseq.as_secs_f64() * 1e3,
        want_sample.events
    );
    println!(
        "streams  par preads/thread   {:>8.2}M ev/s   ({:.1} ms)  {:.2}x",
        evs(want_sample.events, d_spar) / 1e6,
        d_spar.as_secs_f64() * 1e3,
        d_sseq.as_secs_f64() / d_spar.as_secs_f64()
    );
    println!(
        "streams  batch unique-blocks {:>8.2}M ev/s   ({:.1} ms)  {:.2}x   ({} unique blocks)",
        evs(want_sample.events, d_sbatch) / 1e6,
        d_sbatch.as_secs_f64() * 1e3,
        d_sseq.as_secs_f64() / d_sbatch.as_secs_f64(),
        ub
    );

    // 3. Hottest stream.
    let mut ds = Vec::new();
    for _ in 0..REPS {
        let mut ctx = ReadCtx::new(&reader.dicts, LRU_BLOCKS);
        let (st, d) = timed(|| replay_stream(reader, &mut ctx, su.hottest));
        assert_eq!(st, want_hot);
        ds.push(d);
    }
    let d_hseq = best(ds);
    let mut ds = Vec::new();
    for _ in 0..REPS {
        let (out, d) = timed(|| stream_replay_batch(reader, &[su.hottest]));
        assert_eq!(out.stats, want_hot);
        ds.push(d);
    }
    let d_hbatch = best(ds);
    println!(
        "hottest  baseline seq        {:>8.2}M ev/s   ({:.1} ms, {} events)",
        evs(want_hot.events, d_hseq) / 1e6,
        d_hseq.as_secs_f64() * 1e3,
        want_hot.events
    );
    println!(
        "hottest  batch par blocks    {:>8.2}M ev/s   ({:.1} ms)  {:.2}x",
        evs(want_hot.events, d_hbatch) / 1e6,
        d_hbatch.as_secs_f64() * 1e3,
        d_hseq.as_secs_f64() / d_hbatch.as_secs_f64()
    );
    println!("correctness: every run checked against active-segment ground truth (events/bytes/checksum)");
}

// ---------------------------------------------------------------------------
// bench cold — where does parallel replay go NVMe-bound?
// ---------------------------------------------------------------------------

fn cmd_bench_cold() {
    let su = setup(false);
    let reader = &su.reader;
    let seg = fs::read(active_log_path()).unwrap();
    let gt = active_ground_truth(&seg);
    drop(seg);
    let all: Vec<u64> = reader.stream_ids.clone();
    let want_global = expected_stats(&gt, &all);
    let want_sample = expected_stats(&gt, &su.sample);
    let comp_bytes: u64 = reader.blocks.iter().map(|b| b.comp_len as u64).sum();

    // rayon pool warm-up on non-file work.
    use rayon::prelude::*;
    let _: u64 = (0..1_000_000u64).into_par_iter().sum();

    println!("== bench cold (posix_fadvise DONTNEED before each run) ==");
    evict_path(&sealed_path());
    let mut ctx = ReadCtx::new(&reader.dicts, 0);
    let ((st, _), d) = timed(|| global_replay(reader, &mut ctx));
    assert_eq!(st, want_global);
    println!(
        "global   baseline seq  cold  {:>8.2}M ev/s   ({:.1} ms, {:.0} MB/s compressed)",
        evs(st.events, d) / 1e6,
        d.as_secs_f64() * 1e3,
        mb(comp_bytes) / d.as_secs_f64()
    );

    evict_path(&sealed_path());
    let ((st, _), d) = timed(|| global_replay_par(reader));
    assert_eq!(st, want_global);
    println!(
        "global   rayon par     cold  {:>8.2}M ev/s   ({:.1} ms, {:.0} MB/s compressed)",
        evs(st.events, d) / 1e6,
        d.as_secs_f64() * 1e3,
        mb(comp_bytes) / d.as_secs_f64()
    );

    evict_path(&sealed_path());
    let (out, d) = timed(|| stream_replay_batch(reader, &su.sample));
    assert_eq!(out.stats, want_sample);
    println!(
        "streams  batch         cold  {:>8.2}M ev/s   ({:.1} ms, {} blocks, {:.1} MiB compressed)",
        evs(out.stats.events, d) / 1e6,
        d.as_secs_f64() * 1e3,
        out.unique_blocks,
        mb(out.comp_bytes)
    );

    evict_path(&sealed_path());
    let (st, d) = timed(|| stream_replay_par_preads(reader, &su.sample, 64));
    assert_eq!(st, want_sample);
    println!(
        "streams  par preads    cold  {:>8.2}M ev/s   ({:.1} ms)",
        evs(st.events, d) / 1e6,
        d.as_secs_f64() * 1e3
    );
}

// ---------------------------------------------------------------------------
// bench point — linear varint seek vs skip table
// ---------------------------------------------------------------------------

fn point_sample(reader: &SealedReader, hottest: u64) -> (Vec<(u64, u64)>, Vec<(u64, u64)>) {
    // Event-uniform sample: pick a global event uniformly, map to (stream,
    // version) via prefix sums over the (sorted) stream directory.
    let mut prefix: Vec<u64> = Vec::with_capacity(reader.stream_ids.len() + 1);
    prefix.push(0);
    for &s in &reader.stream_ids {
        prefix.push(prefix.last().unwrap() + reader.dir[&s].count as u64);
    }
    assert_eq!(*prefix.last().unwrap(), reader.n_events);
    let mut rng = StdRng::seed_from_u64(POINT_SEED);
    let uni: Vec<(u64, u64)> = (0..N_POINT_READS)
        .map(|_| {
            let u = rng.random_range(0..reader.n_events);
            let i = prefix.partition_point(|&p| p <= u) - 1;
            (reader.stream_ids[i], u - prefix[i])
        })
        .collect();
    let hc = reader.dir[&hottest].count as u64;
    let hot: Vec<(u64, u64)> =
        (0..N_POINT_READS).map(|_| (hottest, rng.random_range(0..hc))).collect();
    (uni, hot)
}

fn bench_points(
    label: &str,
    reader: &SealedReader,
    sample: &[(u64, u64)],
    cache: usize,
    skip: bool,
) {
    let mut ctx = ReadCtx::new(&reader.dicts, cache);
    let mut sink = 0u64;
    if cache > 0 {
        // Pre-warm the cache so the timed pass measures pure seek+slice.
        for &(s, v) in sample {
            sink = sink.wrapping_add(if skip {
                point_read_skip(reader, &mut ctx, s, v)
            } else {
                point_read(reader, &mut ctx, s, v)
            });
        }
    }
    let mut durs: Vec<Duration> = Vec::with_capacity(sample.len());
    for &(s, v) in sample {
        let t = Instant::now();
        sink = sink.wrapping_add(if skip {
            point_read_skip(reader, &mut ctx, s, v)
        } else {
            point_read(reader, &mut ctx, s, v)
        });
        durs.push(t.elapsed());
    }
    std::hint::black_box(sink);
    let mean = durs.iter().sum::<Duration>() / durs.len() as u32;
    let (p50, p99, p999) = (pct(&mut durs, 0.50), pct(&mut durs, 0.99), pct(&mut durs, 0.999));
    println!(
        "{label:<44} mean {:>7.2} us  p50 {:>7.2}  p99 {:>7.2}  p99.9 {:>7.2}",
        us(mean),
        us(p50),
        us(p99),
        us(p999)
    );
}

fn cmd_bench_point() {
    let su = setup(true);
    let reader = &su.reader;
    let (uni, hot) = point_sample(reader, su.hottest);

    // Correctness gate: skip-table reads return exactly what linear reads do.
    let mut ctx = ReadCtx::new(&reader.dicts, 8192);
    for &(s, v) in uni.iter().chain(hot.iter()) {
        let a = point_read(reader, &mut ctx, s, v);
        let b = point_read_skip(reader, &mut ctx, s, v);
        assert_eq!(a, b, "skip-table point read diverges at ({s},{v})");
    }
    println!("== bench point ({} uniform reads / {} hottest-stream reads; skip==linear verified) ==", uni.len(), hot.len());

    bench_points("uniform  no-cache  linear seek (baseline)", reader, &uni, 0, false);
    bench_points("uniform  no-cache  skip table", reader, &uni, 0, true);
    bench_points("uniform  cached    linear seek (baseline)", reader, &uni, 8192, false);
    bench_points("uniform  cached    skip table", reader, &uni, 8192, true);
    bench_points("hottest  cached    linear seek (baseline)", reader, &hot, 8192, false);
    bench_points("hottest  cached    skip table", reader, &hot, 8192, true);
}

// ---------------------------------------------------------------------------
// prof — hot loops for perf stat / perf record
// ---------------------------------------------------------------------------

fn cmd_prof(name: &str, reps: usize) {
    let su = setup(true);
    let reader = &su.reader;
    let mut sink = 0u64;
    match name {
        "global_seq" => {
            for _ in 0..reps {
                let mut ctx = ReadCtx::new(&reader.dicts, 0);
                let (st, _) = global_replay(reader, &mut ctx);
                sink = sink.wrapping_add(st.checksum);
            }
        }
        "global_par" => {
            for _ in 0..reps {
                let (st, _) = global_replay_par(reader);
                sink = sink.wrapping_add(st.checksum);
            }
        }
        "stream_seq" => {
            for _ in 0..reps {
                let mut ctx = ReadCtx::new(&reader.dicts, LRU_BLOCKS);
                for &s in &su.sample {
                    sink = sink.wrapping_add(replay_stream(reader, &mut ctx, s).checksum);
                }
            }
        }
        "stream_batch" => {
            for _ in 0..reps {
                sink = sink.wrapping_add(stream_replay_batch(reader, &su.sample).stats.checksum);
            }
        }
        "point_seq" | "point_skip" => {
            let (uni, _) = point_sample(reader, su.hottest);
            let mut ctx = ReadCtx::new(&reader.dicts, 8192);
            for &(s, v) in &uni {
                sink = sink.wrapping_add(point_read(reader, &mut ctx, s, v));
            }
            let skip = name == "point_skip";
            for _ in 0..reps {
                for &(s, v) in &uni {
                    sink = sink.wrapping_add(if skip {
                        point_read_skip(reader, &mut ctx, s, v)
                    } else {
                        point_read(reader, &mut ctx, s, v)
                    });
                }
            }
        }
        _ => panic!("unknown prof target {name}"),
    }
    println!("prof {name} x{reps}: sink {sink:x}");
}

// ---------------------------------------------------------------------------
// recovery — single-threaded vs parallel per-segment full scan
// ---------------------------------------------------------------------------

fn cmd_recovery(gib: u64) {
    use recovery::*;
    let dir = data_root().join("rec");
    let _ = fs::remove_dir_all(&dir);
    let ((gen, d_gen), _) = (timed(|| gen_dataset(&dir, 256 * MIB, gib * GIB)), ());
    println!(
        "== recovery: generated {} segments / {:.1} GiB / {} events in {:.1}s ==",
        gen.segs.len(),
        gen.bytes as f64 / GIB as f64,
        gen.events,
        d_gen.as_secs_f64()
    );

    // Sanity + identical-results gate (warm, also warms nothing we rely on:
    // cold runs evict first).
    let r_seq = recover_full(&gen.segs);
    assert_eq!((r_seq.batches, r_seq.events, r_seq.next_pos), (gen.batches, gen.events, gen.next_pos));
    assert_eq!(r_seq.last_stop, Stop::EndOfLog);
    let r_par = recover_full_par(&gen.segs);
    assert_eq!((r_par.batches, r_par.events, r_par.next_pos), (r_seq.batches, r_seq.events, r_seq.next_pos));
    assert_eq!(r_par.heads, r_seq.heads, "parallel recovery rebuilt different stream heads");
    println!(
        "identical results: {} batches, {} events, {} stream heads (seq == par, incl. per-stream head ptrs)",
        r_seq.batches,
        r_seq.events,
        r_seq.heads.len()
    );

    let gib_f = r_seq.bytes as f64 / GIB as f64;
    let report = |label: &str, d: Duration, bytes: u64| {
        println!(
            "{label:<28} {:>7.0} ms   {:>6.3} s/GiB   {:>6.0} MiB/s",
            d.as_secs_f64() * 1e3,
            d.as_secs_f64() / gib_f,
            bytes as f64 / MIB as f64 / d.as_secs_f64()
        );
    };

    evict(&gen.segs);
    let (r, d) = timed(|| recover_full(&gen.segs));
    assert_eq!((r.batches, r.events), (r_seq.batches, r_seq.events));
    report("FULL seq   cold (baseline)", d, r.bytes);

    evict(&gen.segs);
    let (r, d) = timed(|| recover_full_par(&gen.segs));
    assert_eq!((r.batches, r.events), (r_seq.batches, r_seq.events));
    assert_eq!(r.heads, r_seq.heads);
    report("FULL par   cold", d, r.bytes);

    let mut best = Duration::MAX;
    for _ in 0..2 {
        let (r, d) = timed(|| recover_full(&gen.segs));
        assert_eq!((r.batches, r.events), (r_seq.batches, r_seq.events));
        best = best.min(d);
    }
    report("FULL seq   warm (baseline)", best, r_seq.bytes);

    let mut best = Duration::MAX;
    for _ in 0..2 {
        let (r, d) = timed(|| recover_full_par(&gen.segs));
        assert_eq!((r.batches, r.events), (r_seq.batches, r_seq.events));
        assert_eq!(r.heads, r_seq.heads);
        best = best.min(d);
    }
    report("FULL par   warm", best, r_seq.bytes);
    let (rss, hwm) = vm();
    println!("mem: RSS {:.0} MiB, HWM {:.0} MiB", rss as f64 / 1024.0, hwm as f64 / 1024.0);

    for s in &gen.segs {
        fs::remove_file(&s.path).unwrap();
    }
    fs::remove_dir(&dir).ok();
    println!("cleaned {} segment files", gen.segs.len());
}

// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(|s| s.as_str()) {
        Some("gen") => cmd_gen(),
        Some("seal") => cmd_seal(args.get(1).map_or("base", |s| s.as_str())),
        Some("bench") => match args.get(1).map(|s| s.as_str()) {
            Some("replay") => cmd_bench_replay(),
            Some("point") => cmd_bench_point(),
            Some("cold") => cmd_bench_cold(),
            _ => eprintln!("bench replay|point|cold"),
        },
        Some("prof") => {
            let name = args.get(1).expect("prof <name> [reps]").clone();
            let reps = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);
            cmd_prof(&name, reps);
        }
        Some("recovery") => {
            let gib = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(4);
            cmd_recovery(gib);
        }
        Some("clean") => {
            let _ = fs::remove_dir_all(data_root());
            println!("removed bench_data");
        }
        _ => eprintln!("usage: perf_replay gen|seal base|seal par|bench replay|bench point|bench cold|prof <name> [reps]|recovery [gib]|clean"),
    }
}
