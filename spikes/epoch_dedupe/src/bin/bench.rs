//! Bench driver for the epoch dedupe tournament (Spike G, bn-2j5).
//!
//! ```text
//! cargo run --release --bin bench -- info
//! cargo run --release --bin bench -- all            # full matrix (spawns itself for mem)
//! cargo run --release --bin bench -- lat  <g0|g1|g2|g3> <keylen> <fill_pct>
//! cargo run --release --bin bench -- mem  <g0|g1|g2|g3> <keylen> <fill_pct>
//! cargo run --release --bin bench -- steady <g0|g1|g2|g3> <keylen>
//! ```
//!
//! Measurement hygiene: bench thread pinned; every measured phase is
//! preceded by a competing-load guard (sleep-and-retry — a sibling spike may
//! be building concurrently); per-op samples close with rdtscp; quantiles
//! from full sorted sample arrays. G0 runs journal-buffered, no fsync (I5),
//! measured warm, on the same disk as the other spikes.

use std::time::Instant;

use epoch_dedupe::arena::Arena;
use epoch_dedupe::epoch::{EpochDedupe, HashActive, IcebergActive};
use epoch_dedupe::g0::FjallDedupe;
use epoch_dedupe::timing::{Tsc, calibrate, ensure_quiet, pin_to, rdtsc, rdtscp, rss_bytes};
use epoch_dedupe::{DedupeIndex, Fingerprinter, Rng, Scope, make_key};

const W: u64 = 1_000_000;
const EPOCH_SPAN: u64 = W / 8;
const KEYSEED: u64 = 0xC0FFEE;
const FPSEED: u64 = 0x5EED_5EED;
const SAMPLES: usize = 200_000;
const WARMUP: usize = 10_000;

fn fpr() -> Fingerprinter {
    Fingerprinter::production(FPSEED)
}

fn bench_dir(tag: &str) -> std::path::PathBuf {
    let d = std::path::PathBuf::from(format!("./ed_bench_data/{}_{tag}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Flat probe-key buffer: key `indices[i]` at `i*keylen`, generated ahead of
/// time so the measured op is hash+probe+verify, not keygen.
fn key_flat(indices: &[u64], keylen: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(indices.len() * keylen);
    for &i in indices {
        buf.extend_from_slice(&make_key(i, keylen, KEYSEED));
    }
    buf
}

/// Zipf(s=1.1) sampler over ranks 1..=n via a precomputed CDF; rank 1 maps
/// to the NEWEST key (retries skew recent).
struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(n: usize, s: f64) -> Self {
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0;
        for r in 1..=n {
            acc += 1.0 / (r as f64).powf(s);
            cdf.push(acc);
        }
        Zipf { cdf }
    }

    fn sample(&self, rng: &mut Rng) -> usize {
        let u = rng.f64() * self.cdf[self.cdf.len() - 1];
        self.cdf.partition_point(|&c| c < u).min(self.cdf.len() - 1) // = rank-1
    }
}

struct Measured {
    q: epoch_dedupe::timing::Quantiles,
}

fn measure_probe(
    tsc: &Tsc,
    keybuf: &[u8],
    keylen: usize,
    samples: usize,
    mut op: impl FnMut(&[u8]) -> Option<u64>,
) -> Measured {
    let count = keybuf.len() / keylen.max(1);
    assert!(count > 0);
    let key_at = |i: usize| {
        if keylen == 0 { &keybuf[..0] } else { &keybuf[(i % count) * keylen..][..keylen] }
    };
    for i in 0..WARMUP {
        std::hint::black_box(op(key_at(i)));
    }
    let mut ticks = Vec::with_capacity(samples);
    for i in 0..samples {
        let k = key_at(i);
        let t0 = rdtsc();
        let r = op(k);
        std::hint::black_box(r);
        let t1 = rdtscp();
        ticks.push((t1.wrapping_sub(t0)).min(u32::MAX as u64) as u32);
    }
    Measured { q: tsc.quantiles_ns(&mut ticks) }
}

fn print_result(cand: &str, keylen: usize, fill: u64, scenario: &str, m: &Measured) {
    let q = &m.q;
    println!(
        "RESULT cand={cand} keylen={keylen} fill={fill} scenario={scenario} \
         p50={:.0} p90={:.0} p99={:.0} p999={:.0} max={:.0} mean={:.1}",
        q.p50, q.p90, q.p99, q.p999, q.max, q.mean
    );
}

/// Populate `n` unique keys (positions 0..n) and run every latency scenario.
#[allow(clippy::too_many_arguments)]
fn run_lat<I: DedupeIndex>(
    cand: &str,
    keylen: usize,
    fill: u64,
    n: u64,
    idx: &mut I,
    tsc: &Tsc,
    active_probe: Option<impl Fn(&I, Scope, &[u8], u64, &Arena) -> Option<u64>>,
    freeze_ns: impl Fn(&I) -> Vec<u64>,
) {
    let mut arena = Arena::with_capacity((n as usize) * (21 + keylen));

    ensure_quiet(7200);
    let t0 = Instant::now();
    for i in 0..n {
        let key = make_key(i, keylen, KEYSEED);
        let ptr = arena.append(Scope::Global, &key, i);
        idx.insert(Scope::Global, &key, i, ptr);
    }
    idx.flush();
    let dt = t0.elapsed();
    println!(
        "RESULT cand={cand} keylen={keylen} fill={fill} scenario=insert \
         ns_per_op={:.0} ops_per_s={:.0} wall_s={:.2}",
        dt.as_nanos() as f64 / n as f64,
        n as f64 / dt.as_secs_f64(),
        dt.as_secs_f64()
    );
    let fz = freeze_ns(idx);
    if !fz.is_empty() {
        let sum: u64 = fz.iter().sum();
        let frozen_keys: u64 = fz.len() as u64 * EPOCH_SPAN;
        println!(
            "RESULT cand={cand} keylen={keylen} fill={fill} scenario=freeze \
             count={} mean_ms={:.2} max_ms={:.2} ns_per_key={:.1}",
            fz.len(),
            sum as f64 / fz.len() as f64 / 1e6,
            *fz.iter().max().unwrap() as f64 / 1e6,
            sum as f64 / frozen_keys as f64
        );
    }

    // Let fjall background work settle before quantile phases.
    std::thread::sleep(std::time::Duration::from_secs(2));

    let w = n;
    let mut rng = Rng::new(0xBE7C);

    // Absent keys: never-inserted indices.
    let absent: Vec<u64> = (0..SAMPLES as u64).map(|s| 2 * W + s).collect();
    let absent_buf = key_flat(&absent, keylen);

    // Hit sets.
    let active_start = ((n - 1) / EPOCH_SPAN) * EPOCH_SPAN;
    let act: Vec<u64> =
        (0..SAMPLES as u64).map(|_| active_start + rng.below(n - active_start)).collect();
    let act_buf = key_flat(&act, keylen);
    let oldest_span = EPOCH_SPAN.min(n);
    let old: Vec<u64> = (0..SAMPLES as u64).map(|_| rng.below(oldest_span)).collect();
    let old_buf = key_flat(&old, keylen);
    let zipf = Zipf::new(n as usize, 1.1);
    let zpf: Vec<u64> =
        (0..SAMPLES as u64).map(|_| n - 1 - (zipf.sample(&mut rng) as u64)).collect();
    let zpf_buf = key_flat(&zpf, keylen);

    ensure_quiet(7200);
    if let Some(ap) = &active_probe {
        let m = measure_probe(tsc, &absent_buf, keylen, SAMPLES, |k| {
            ap(idx, Scope::Global, k, w, &arena)
        });
        print_result(cand, keylen, fill, "active_miss", &m);
    }

    ensure_quiet(7200);
    let m = measure_probe(tsc, &absent_buf, keylen, SAMPLES, |k| {
        idx.check(Scope::Global, k, w, &arena)
    });
    print_result(cand, keylen, fill, "window_miss", &m);

    ensure_quiet(7200);
    let m =
        measure_probe(tsc, &act_buf, keylen, SAMPLES, |k| idx.check(Scope::Global, k, w, &arena));
    print_result(cand, keylen, fill, "hit_active_uniform", &m);

    ensure_quiet(7200);
    let m =
        measure_probe(tsc, &old_buf, keylen, SAMPLES, |k| idx.check(Scope::Global, k, w, &arena));
    print_result(cand, keylen, fill, "hit_oldest_uniform", &m);

    ensure_quiet(7200);
    let m =
        measure_probe(tsc, &zpf_buf, keylen, SAMPLES, |k| idx.check(Scope::Global, k, w, &arena));
    print_result(cand, keylen, fill, "hit_zipf", &m);

    println!(
        "RESULT cand={cand} keylen={keylen} fill={fill} scenario=deletes \
         per_key_deletes={} (structural: epoch candidates expose no per-key remove)",
        idx.deletes_issued()
    );
}

fn lat(cand: &str, keylen: usize, fill: u64) {
    let n = W * fill / 100;
    let tsc = calibrate();
    // No active/frozen distinction exists in G0: one fjall point read serves
    // both, so active_miss == window_miss and is reported by window_miss.
    type NoActive = fn(&FjallDedupe, Scope, &[u8], u64, &Arena) -> Option<u64>;
    match cand {
        "g0" => {
            let mut idx =
                FjallDedupe::open(bench_dir(&format!("lat_{keylen}_{fill}")), W, 64, true);
            run_lat(cand, keylen, fill, n, &mut idx, &tsc, None::<NoActive>, |_| Vec::new());
        }
        "g1" => {
            let mut idx = EpochDedupe::<HashActive>::new(W, fpr(), false);
            run_lat(
                cand,
                keylen,
                fill,
                n,
                &mut idx,
                &tsc,
                Some(|i: &EpochDedupe<HashActive>, s, k: &[u8], w, a: &Arena| {
                    i.check_active_only(s, k, w, a)
                }),
                |i| i.freeze_ns.clone(),
            );
        }
        "g2" => {
            let mut idx = EpochDedupe::<HashActive>::new(W, fpr(), true);
            run_lat(
                cand,
                keylen,
                fill,
                n,
                &mut idx,
                &tsc,
                Some(|i: &EpochDedupe<HashActive>, s, k: &[u8], w, a: &Arena| {
                    i.check_active_only(s, k, w, a)
                }),
                |i| i.freeze_ns.clone(),
            );
        }
        "g3" => {
            let mut idx = EpochDedupe::<IcebergActive>::new(W, fpr(), true);
            run_lat(
                cand,
                keylen,
                fill,
                n,
                &mut idx,
                &tsc,
                Some(|i: &EpochDedupe<IcebergActive>, s, k: &[u8], w, a: &Arena| {
                    i.check_active_only(s, k, w, a)
                }),
                |i| i.freeze_ns.clone(),
            );
        }
        other => panic!("unknown candidate {other}"),
    }
}

/// Fresh-process memory measurement: RSS delta across index population
/// (arena built FIRST — it stands in for the log both designs already pay
/// for), plus the candidate's own resident/serialized estimates.
fn mem(cand: &str, keylen: usize, fill: u64) {
    let n = W * fill / 100;
    let mut arena = Arena::with_capacity((n as usize) * (21 + keylen));
    let keys: Vec<Vec<u8>> = (0..n).map(|i| make_key(i, keylen, KEYSEED)).collect();
    let ptrs: Vec<u64> =
        keys.iter().enumerate().map(|(i, k)| arena.append(Scope::Global, k, i as u64)).collect();

    fn populate<I: DedupeIndex>(idx: &mut I, keys: &[Vec<u8>], ptrs: &[u64]) -> (u64, u64, u64) {
        let rss0 = rss_bytes();
        for (i, k) in keys.iter().enumerate() {
            idx.insert(Scope::Global, k, i as u64, ptrs[i]);
        }
        idx.flush();
        let rss1 = rss_bytes();
        (rss1.saturating_sub(rss0), idx.resident_bytes(), idx.serialized_bytes())
    }

    let (rss_delta, resident_est, serialized) = match cand {
        "g0" => {
            let mut idx =
                FjallDedupe::open(bench_dir(&format!("mem_{keylen}_{fill}")), W, 64, true);
            let (rss_delta, resident_est, _) = populate(&mut idx, &keys, &ptrs);
            // Give fjall's background flush a moment so the on-disk number
            // reflects steady state, then re-read the directory size.
            std::thread::sleep(std::time::Duration::from_secs(3));
            (rss_delta, resident_est, idx.serialized_bytes())
        }
        "g1" => populate(&mut EpochDedupe::<HashActive>::new(W, fpr(), false), &keys, &ptrs),
        "g2" => populate(&mut EpochDedupe::<HashActive>::new(W, fpr(), true), &keys, &ptrs),
        "g3" => populate(&mut EpochDedupe::<IcebergActive>::new(W, fpr(), true), &keys, &ptrs),
        other => panic!("unknown candidate {other}"),
    };
    println!(
        "MEM cand={cand} keylen={keylen} fill={fill} n={n} rss_delta={rss_delta} \
         resident_est={resident_est} serialized={serialized} arena_bytes={}",
        arena.len_bytes()
    );
}

/// Steady-state window turnover: 2W inserts through a W-position window —
/// the phase where G0 pays its per-key delete traffic and the epoch designs
/// drop whole epochs.
fn steady(cand: &str, keylen: usize) {
    let n = 2 * W;
    let mut arena = Arena::with_capacity((n as usize) * (21 + keylen));
    ensure_quiet(7200);

    fn run<I: DedupeIndex>(idx: &mut I, n: u64, keylen: usize, arena: &mut Arena) -> f64 {
        let t0 = Instant::now();
        for i in 0..n {
            let key = make_key(i, keylen, KEYSEED);
            let ptr = arena.append(Scope::Global, &key, i);
            idx.insert(Scope::Global, &key, i, ptr);
        }
        idx.flush();
        t0.elapsed().as_secs_f64()
    }

    match cand {
        "g0" => {
            let mut idx = FjallDedupe::open(bench_dir(&format!("steady_{keylen}")), W, 64, true);
            let s = run(&mut idx, n, keylen, &mut arena);
            println!(
                "STEADY cand=g0 keylen={keylen} n={n} ops_per_s={:.0} ns_per_op={:.0} \
                 per_key_deletes={} bytes_written={} disk_bytes={}",
                n as f64 / s,
                s * 1e9 / n as f64,
                idx.deletes_issued(),
                idx.bytes_written(),
                idx.disk_bytes()
            );
        }
        "g1" | "g2" | "g3" => {
            let with_filter = cand != "g1";
            if cand == "g3" {
                let mut idx = EpochDedupe::<IcebergActive>::new(W, fpr(), with_filter);
                let s = run(&mut idx, n, keylen, &mut arena);
                print_steady_epoch(cand, keylen, n, s, &idx.freeze_ns, idx.epochs_dropped());
            } else {
                let mut idx = EpochDedupe::<HashActive>::new(W, fpr(), with_filter);
                let s = run(&mut idx, n, keylen, &mut arena);
                print_steady_epoch(cand, keylen, n, s, &idx.freeze_ns, idx.epochs_dropped());
            }
        }
        other => panic!("unknown candidate {other}"),
    }
}

fn print_steady_epoch(cand: &str, keylen: usize, n: u64, s: f64, fz: &[u64], dropped: u64) {
    println!(
        "STEADY cand={cand} keylen={keylen} n={n} ops_per_s={:.0} ns_per_op={:.0} \
         per_key_deletes=0 freezes={} freeze_ns_per_key={:.1} epochs_dropped={dropped}",
        n as f64 / s,
        s * 1e9 / n as f64,
        fz.len(),
        fz.iter().sum::<u64>() as f64 / (fz.len() as u64 * EPOCH_SPAN).max(1) as f64
    );
}

fn info(tsc: &Tsc) {
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| {
            s.lines().find(|l| l.starts_with("model name")).map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string())
        })
        .unwrap_or_default();
    let gov = std::fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
        .unwrap_or_default();
    println!(
        "INFO cpu=\"{cpu}\" governor={} ns_per_tick={:.4} tsc_overhead_ticks={} pid={}",
        gov.trim(),
        tsc.ns_per_tick,
        tsc.overhead_ticks,
        std::process::id()
    );
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = args.first().map(String::as_str).unwrap_or("all");
    // Core 20 (second CCD): away from the cores other spikes pin (1-5) and
    // from where the scheduler tends to place the ambient processes.
    pin_to(20);

    match mode {
        "info" => info(&calibrate()),
        "lat" => lat(&args[1], args[2].parse().unwrap(), args[3].parse().unwrap()),
        "mem" => mem(&args[1], args[2].parse().unwrap(), args[3].parse().unwrap()),
        "steady" => steady(&args[1], args[2].parse().unwrap()),
        "all" => {
            let tsc = calibrate();
            info(&tsc);
            let exe = std::env::current_exe().unwrap();
            for keylen in [16usize, 256] {
                for fill in [25u64, 75, 100] {
                    for cand in ["g1", "g2", "g3", "g0"] {
                        // lat in a subprocess too: fresh heap per config, and
                        // a G0 crash can't take the whole matrix down.
                        for sub in ["lat", "mem"] {
                            let st = std::process::Command::new(&exe)
                                .args([
                                    sub,
                                    cand,
                                    &keylen.to_string(),
                                    &fill.to_string(),
                                ])
                                .status()
                                .expect("spawn self");
                            assert!(st.success(), "{sub} {cand} k={keylen} f={fill} failed");
                        }
                    }
                }
            }
            for cand in ["g1", "g2", "g3", "g0"] {
                let st = std::process::Command::new(&exe)
                    .args(["steady", cand, "16"])
                    .status()
                    .expect("spawn self");
                assert!(st.success());
            }
            let _ = std::fs::remove_dir_all("./ed_bench_data");
            println!("DONE");
        }
        other => panic!("unknown mode {other}"),
    }
}
