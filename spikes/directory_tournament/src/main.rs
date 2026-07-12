//! Spike H (bn-2fp) bench driver. Binary name `directory_tournament` — a
//! sibling spike (`active_microblocks`) staggers around this process name,
//! and we stagger around theirs (see `timing::contention`).
//!
//! ```text
//! directory_tournament info
//! directory_tournament gen-real [--dir D] [--streams N] [--events N] [--seg BYTES]
//! directory_tournament bench [--real D] [--sizes 1k,10k,100k,1m,10m]
//!                            [--dists dense,sparse,zipfclust] [--with-30m]
//!                            [--cands h0,h0f,h1,h2,h3,h4,h5] [--quick]
//! ```
//!
//! CSV rows land on stdout (tee to `logs/`); progress on stderr. Every
//! measured phase is quiet-guarded (loadavg < 6, no compilers, no
//! `active_microblocks`).

use std::io::Write as _;

use directory_tournament::candidates::*;
use directory_tournament::datasets::{self, Dataset, Queries};
use directory_tournament::gen_real::{GenConfig, generate};
use directory_tournament::timing::{
    self, Tsc, ensure_quiet, evict_llc, pin_to, rdtsc, rdtscp,
};
use directory_tournament::ExactDirectory;

const QUIET_MAX_WAIT_S: u64 = 6 * 3600;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("info") => info(),
        Some("gen-real") => {
            let mut cfg = GenConfig::default();
            let mut dir = default_store_dir();
            let mut i = 1;
            while i < args.len() {
                match args[i].as_str() {
                    "--dir" => {
                        dir = args[i + 1].clone().into();
                        i += 2;
                    }
                    "--streams" => {
                        cfg.streams = args[i + 1].parse().unwrap();
                        i += 2;
                    }
                    "--events" => {
                        cfg.events = args[i + 1].parse().unwrap();
                        i += 2;
                    }
                    "--seg" => {
                        cfg.segment_size = args[i + 1].parse().unwrap();
                        i += 2;
                    }
                    a => panic!("unknown gen-real arg {a}"),
                }
            }
            generate(&dir, &cfg);
            let segs = datasets::extract_real(&dir);
            eprintln!("[gen-real] extracted segment directories:");
            eprint!("{}", datasets::describe_real(&segs));
        }
        Some("bench") => bench(&args[1..]),
        _ => {
            eprintln!("usage: directory_tournament info|gen-real|bench (see src/main.rs)");
            std::process::exit(2);
        }
    }
}

fn default_store_dir() -> std::path::PathBuf {
    let home = std::env::var("HOME").unwrap();
    std::path::PathBuf::from(home)
        .join(".cache/mess-bench-scratch/directory_tournament_store")
}

fn info() {
    let tsc = timing::calibrate();
    println!(
        "tsc: {:.4} ns/tick, empty rdtsc..rdtscp overhead {} ticks (~{:.1} ns)",
        tsc.ns_per_tick,
        tsc.overhead_ticks,
        tsc.overhead_ticks as f64 * tsc.ns_per_tick
    );
    println!("contention: {:?}", timing::contention());
    println!("store dir default: {}", default_store_dir().display());
}

// ---------------------------------------------------------------------------
// Bench
// ---------------------------------------------------------------------------

struct Opts {
    quick:    bool,
    sizes:    Vec<usize>,
    dists:    Vec<String>,
    cands:    Vec<String>,
    real_dir: Option<std::path::PathBuf>,
}

fn parse_size(s: &str) -> usize {
    let s = s.to_lowercase();
    if let Some(x) = s.strip_suffix('m') {
        x.parse::<usize>().unwrap() * 1_000_000
    } else if let Some(x) = s.strip_suffix('k') {
        x.parse::<usize>().unwrap() * 1_000
    } else {
        s.parse().unwrap()
    }
}

fn bench(args: &[String]) {
    let mut opts = Opts {
        quick:    false,
        sizes:    vec![1_000, 10_000, 100_000, 1_000_000, 10_000_000],
        dists:    vec!["dense".into(), "sparse".into(), "zipfclust".into()],
        cands:    vec![
            "h0".into(),
            "h0f".into(),
            "h1".into(),
            "h2".into(),
            "h3".into(),
            "h4".into(),
            "h5".into(),
        ],
        real_dir: None,
    };
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--quick" => {
                opts.quick = true;
                i += 1;
            }
            "--with-30m" => {
                opts.sizes.push(30_000_000);
                i += 1;
            }
            "--sizes" => {
                opts.sizes = args[i + 1]
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(parse_size)
                    .collect();
                i += 2;
            }
            "--dists" => {
                opts.dists = args[i + 1]
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect();
                i += 2;
            }
            "--cands" => {
                opts.cands =
                    args[i + 1].split(',').map(str::to_string).collect();
                i += 2;
            }
            "--real" => {
                opts.real_dir = Some(args[i + 1].clone().into());
                i += 2;
            }
            a => panic!("unknown bench arg {a}"),
        }
    }

    pin_to(8); // second CCD on the 3900X, away from ambient load
    let tsc = timing::calibrate();
    eprintln!(
        "[bench] tsc {:.4} ns/tick, overhead {} ticks; quick={}",
        tsc.ns_per_tick, tsc.overhead_ticks, opts.quick
    );
    println!(
        "dataset,n,u_over_n,cand,build_p50_us,build_p99_us,open_p50_us,open_p99_us,\
         ser_bytes,res_bytes,warm_hit_p50_ns,warm_hit_p99_ns,warm_zipf_p50_ns,\
         warm_zipf_p99_ns,warm_miss_p50_ns,warm_miss_p99_ns,batch32_ns_per_op,\
         serial_ns_per_op,coldish_p50_ns,coldish_p99_ns"
    );

    let mut scratch: Vec<u8> = Vec::new();

    // Synthetic datasets, one at a time (peak memory: dataset + 1 candidate).
    for &n in &opts.sizes {
        for dist in &opts.dists {
            let ds = match dist.as_str() {
                "dense" => datasets::gen_dense(n, 0xA11CE + n as u64),
                "sparse" => datasets::gen_sparse(n, 0xB0B + n as u64),
                "zipfclust" => {
                    datasets::gen_zipf_cluster(n, 0xC0FFEE + n as u64)
                }
                d => panic!("unknown dist {d}"),
            };
            run_dataset(&ds, &opts, &tsc, &mut scratch);
        }
    }

    // Real sealed-segment directories.
    if let Some(dir) = &opts.real_dir {
        let segs = datasets::extract_real(dir);
        eprintln!("[bench] real segments:\n{}", datasets::describe_real(&segs));
        for seg in &segs {
            run_dataset(&seg.dataset, &opts, &tsc, &mut scratch);
        }
    }
}

fn run_dataset(ds: &Dataset, opts: &Opts, tsc: &Tsc, scratch: &mut Vec<u8>) {
    let per_phase = if opts.quick { 20_000 } else { 200_000 };
    let q = datasets::gen_queries(ds, per_phase, 0x5EED ^ ds.n() as u64);
    eprintln!(
        "[bench] dataset {} n={} U/n={:.2}",
        ds.name,
        ds.n(),
        ds.u_over_n
    );
    for cand in &opts.cands {
        match cand.as_str() {
            "h0" => run_candidate::<SipHashDir>(ds, &q, opts, tsc, scratch),
            "h0f" => run_candidate::<FoldHashDir>(ds, &q, opts, tsc, scratch),
            "h1" => run_candidate::<SortedDir>(ds, &q, opts, tsc, scratch),
            "h2" => {
                // Memory guard: the bitvector is U bits; only sane when the
                // universe is within ~64x the key count (§12.2's auto-select
                // rule is U/n <= 8; we build out to 64 to map the crossover).
                if ds.u_over_n <= 64.0 {
                    run_candidate::<BitRankDir>(ds, &q, opts, tsc, scratch);
                } else {
                    eprintln!(
                        "  h2_bitrank: skipped (U/n = {:.1} > 64)",
                        ds.u_over_n
                    );
                }
            }
            "h3" => run_candidate::<PefDir>(ds, &q, opts, tsc, scratch),
            "h4" => run_candidate::<PtrHashDir>(ds, &q, opts, tsc, scratch),
            "h5" => run_candidate::<KBinDir>(ds, &q, opts, tsc, scratch),
            c => panic!("unknown candidate {c}"),
        }
    }
}

fn reps_for(n: usize, quick: bool) -> usize {
    if quick {
        3
    } else if n <= 100_000 {
        9
    } else if n <= 1_000_000 {
        5
    } else if n <= 10_000_000 {
        3
    } else {
        2
    }
}

fn run_candidate<D: ExactDirectory>(
    ds: &Dataset,
    q: &Queries,
    opts: &Opts,
    tsc: &Tsc,
    scratch: &mut Vec<u8>,
) {
    ensure_quiet(QUIET_MAX_WAIT_S);
    let reps = reps_for(ds.n(), opts.quick);

    // Construction (seal-time cost): p50/p99 over reps.
    let mut build_ticks = Vec::with_capacity(reps);
    let mut built: Option<D> = None;
    for _ in 0..reps {
        let t0 = rdtsc();
        let d = D::build(&ds.pairs);
        let t1 = rdtscp();
        build_ticks.push(t1 - t0);
        built = Some(d);
    }
    let build_q = tsc.quantiles_ns_u64(&mut build_ticks);
    let built = built.unwrap();

    // Serialize once (seal-time output).
    let image = built.serialize();
    let ser_bytes = image.len();
    drop(built);

    // Open/parse cost: p50/p99 over reps; keep the last instance for lookups.
    let mut open_ticks = Vec::with_capacity(reps);
    let mut opened: Option<D> = None;
    for _ in 0..reps {
        let t0 = rdtsc();
        let d = D::open(&image).expect("open own image");
        let t1 = rdtscp();
        open_ticks.push(t1 - t0);
        opened = Some(d);
    }
    let open_q = tsc.quantiles_ns_u64(&mut open_ticks);
    let dir = opened.unwrap();
    let res_bytes = dir.resident_bytes();
    drop(image);

    // Cold-ish FIRST (before any lookup warms the structure): evict the LLC,
    // then take a short burst of per-op samples; repeat.
    let cold_rounds = if opts.quick { 16 } else { 64 };
    let mut cold_samples: Vec<u32> = Vec::with_capacity(cold_rounds * 64);
    let mut sink = 0u64;
    for r in 0..cold_rounds {
        evict_llc(scratch);
        let base = (r * 64) % q.mixed.len().saturating_sub(64).max(1);
        for &key in &q.mixed[base..base + 64] {
            let t0 = rdtsc();
            let hit = dir.lookup(key);
            let t1 = rdtscp();
            sink ^= hit.map(|e| e.ptr_off).unwrap_or(0);
            cold_samples.push((t1 - t0) as u32);
        }
    }
    let cold_q = tsc.quantiles_ns(&mut cold_samples);

    // Warm-up pass.
    for &key in &q.hit_uniform {
        sink ^= dir.lookup(key).map(|e| e.ptr_off).unwrap_or(0);
    }

    let mut phase = |keys: &[u64]| {
        let mut samples: Vec<u32> = Vec::with_capacity(keys.len());
        for &key in keys {
            let t0 = rdtsc();
            let hit = dir.lookup(key);
            let t1 = rdtscp();
            sink ^= hit.map(|e| e.ptr_off).unwrap_or(0);
            samples.push((t1 - t0) as u32);
        }
        tsc.quantiles_ns(&mut samples)
    };
    let hit_q = phase(&q.hit_uniform);
    let zipf_q = phase(&q.hit_zipf);
    let miss_q = phase(&q.miss_uniform);

    // Streaming batches of 32: timestamp per batch (measurement overhead
    // amortized to ~1 tick/op), report mean ns/op. This is the
    // throughput-style number (independent lookups, OoO overlap allowed).
    let mut batch_ticks = 0u64;
    let mut batch_ops = 0u64;
    for chunk in q.mixed.chunks_exact(32) {
        let t0 = rdtsc();
        for &key in chunk {
            sink ^= dir.lookup(key).map(|e| e.ptr_off).unwrap_or(0);
        }
        let t1 = rdtscp();
        batch_ticks += t1 - t0;
        batch_ops += 32;
    }
    let batch_ns_per_op =
        batch_ticks as f64 * tsc.ns_per_tick / batch_ops as f64;

    // Serial dependency chain: each key depends on the previous lookup's
    // result, so out-of-order overlap is impossible — TRUE serial latency
    // (the per-op rdtscp quantiles quantize to ~10 ns on Zen 2, useless
    // below that; this is the honest sub-10ns latency measure).
    let zero = std::hint::black_box(0u64);
    let t0 = rdtsc();
    for &key in &q.mixed {
        let k = key.wrapping_add(sink & zero);
        sink ^= dir.lookup(k).map(|e| e.ptr_off).unwrap_or(0);
    }
    let t1 = rdtscp();
    let serial_ns_per_op =
        (t1 - t0) as f64 * tsc.ns_per_tick / q.mixed.len() as f64;
    std::hint::black_box(sink);

    println!(
        "{},{},{:.2},{},{:.1},{:.1},{:.1},{:.1},{},{},{:.1},{:.1},{:.1},{:.1},{:.1},{:.1},{:.2},{:.2},{:.1},{:.1}",
        ds.name,
        ds.n(),
        ds.u_over_n,
        D::NAME,
        build_q.p50 / 1e3,
        build_q.p99 / 1e3,
        open_q.p50 / 1e3,
        open_q.p99 / 1e3,
        ser_bytes,
        res_bytes,
        hit_q.p50,
        hit_q.p99,
        zipf_q.p50,
        zipf_q.p99,
        miss_q.p50,
        miss_q.p99,
        batch_ns_per_op,
        serial_ns_per_op,
        cold_q.p50,
        cold_q.p99,
    );
    std::io::stdout().flush().unwrap();
    eprintln!(
        "  {:<18} build p50 {:>9.1}us open p50 {:>9.1}us ser {:>10}B res {:>10}B batch {:>6.2}ns/op serial {:>6.2}ns/op cold p50 {:>7.1}ns",
        D::NAME,
        build_q.p50 / 1e3,
        open_q.p50 / 1e3,
        ser_bytes,
        res_bytes,
        batch_ns_per_op,
        serial_ns_per_op,
        cold_q.p50
    );

    // Exactness spot-check on the real instance every run (the full property
    // suite lives in tests/): a sample of present and absent keys.
    for &(k, e) in ds.pairs.iter().step_by((ds.n() / 64).max(1)) {
        assert_eq!(dir.lookup(k), Some(&e), "{} exactness (hit)", D::NAME);
    }
    for &k in q.miss_uniform.iter().take(64) {
        assert_eq!(dir.lookup(k), None, "{} exactness (miss)", D::NAME);
    }
}
