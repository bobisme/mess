// Spike A (bn-hzc) driver.
//
// Subcommands:
// ```text
// info                 machine + lock-freedom info
// latency [n]          read-latency scenarios (a) no writer (b) 1 writer
//                      batch-100 (c) pathological same-page, per candidate
// throughput [n]       4 readers + 1 writer (batch 100), uniform + Zipf 1.1
// apply [n]            writer apply throughput, batches of 1 / 100 / 10k
// mem <cand> <n>       (child) build+populate, print RSS delta
// mem-all              RSS matrix at 1M and 10M via child processes
// torn [secs_per_kind] page growth under readers + torn-pair invariant
// all                  everything above in report order
// ```
//
// Core pinning: reader(s) on cores 1-4, writer on core 5 (all CCD0 on this
// 3900X). Torn-pair invariant (`global == version<<32 | id`) is checked on
// EVERY read in every scenario, for every candidate.

use state_kernel_dense_heads::candidates::{
    Bench, FjallTable, LockedHashTable, Map, RawHashTable,
};
use state_kernel_dense_heads::direct::{
    DblCell, DirectTable, SeqCell, U128Cell, Update, check_pair, encode_global,
};
use state_kernel_dense_heads::keys;
use state_kernel_dense_heads::timing::{Tsc, calibrate, count_quantile, pin_to, rdtsc, rdtscp, rss_bytes};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Instant;

const READER_CORES: [usize; 4] = [1, 2, 3, 4];
const WRITER_CORE: usize = 5;
const SEED: u64 = 0x5EED_BA5E;

fn bench_dir() -> std::path::PathBuf {
    std::path::PathBuf::from("skdh_bench_data")
}

// ------------------------------------------------------------- builders --

fn populate<B: Bench>(b: &B, n: u64) {
    let mut buf: Vec<Update> = Vec::with_capacity(10_000);
    let mut id = 0u64;
    while id < n {
        buf.clear();
        let end = (id + 10_000).min(n);
        for i in id..end {
            buf.push((i, 1, encode_global(1, i)));
        }
        b.apply(&mut buf);
        id = end;
    }
}

fn build_fjall(n: u64) -> FjallTable {
    let t = FjallTable::open(bench_dir().join(format!("fjall_{}", std::process::id())));
    populate(&t, n);
    t
}

fn build_raw_hash(n: u64) -> RawHashTable {
    let mut m = Map::default();
    for id in 0..n {
        m.insert(id, (1, encode_global(1, id)));
    }
    RawHashTable(m)
}

fn build_locked_hash(n: u64) -> LockedHashTable {
    let t = LockedHashTable(std::sync::RwLock::new(Map::default()));
    populate(&t, n);
    t
}

fn build_direct<C: state_kernel_dense_heads::direct::CellKind>(n: u64) -> DirectTable<C> {
    let t = DirectTable::<C>::new(1);
    populate(&t, n);
    t
}

// ------------------------------------------------------------- writer ----

/// Continuous single writer: batches of `batch` updates drawn from `keys`,
/// version bumped per update, invariant-encoded global. Returns
/// `(cells_applied, elapsed_seconds)`.
fn writer_loop<B: Bench>(
    b: &B,
    kys: &[u64],
    n: u64,
    batch: usize,
    stop: &AtomicBool,
) -> (u64, f64) {
    pin_to(WRITER_CORE);
    let mut versions = vec![1u64; n as usize];
    let mut buf: Vec<Update> = Vec::with_capacity(batch);
    let mut ki = 0usize;
    let mut cells = 0u64;
    let t0 = Instant::now();
    while !stop.load(Relaxed) {
        buf.clear();
        for _ in 0..batch {
            let id = kys[ki];
            ki += 1;
            if ki == kys.len() {
                ki = 0;
            }
            let v = versions[id as usize] + 1;
            versions[id as usize] = v;
            buf.push((id, v, encode_global(v, id)));
        }
        b.apply(&mut buf);
        cells += batch as u64;
    }
    (cells, t0.elapsed().as_secs_f64())
}

// ------------------------------------------------------------ latency ----

struct LatencyRow {
    p50: f64,
    p99: f64,
    p999: f64,
    max: f64,
    mean: f64,
    retry_p999: u32,
    retry_max: u32,
    writer_cells_per_s: f64,
}

/// Single pinned reader; per-op rdtsc samples; optional concurrent writer.
fn read_latency<B: Bench>(
    b: &B,
    tsc: &Tsc,
    reader_keys: &[u64],
    writer_keys: Option<&[u64]>,
    n: u64,
    samples_n: usize,
) -> LatencyRow {
    let stop = AtomicBool::new(false);
    std::thread::scope(|s| {
        let wh = writer_keys.map(|wk| s.spawn(|| writer_loop(b, wk, n, 100, &stop)));
        // Let the writer reach steady state before sampling.
        if wh.is_some() {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        pin_to(READER_CORES[0]);
        let mut r = 0u32;
        // Warm-up (not recorded).
        for i in 0..100_000usize {
            let k = reader_keys[i % reader_keys.len()];
            let (v, g) = b.get(black_box(k), &mut r);
            assert!(check_pair(k, v, g), "torn pair in warmup");
        }
        let mut ticks: Vec<u32> = Vec::with_capacity(samples_n);
        let mut retries: Vec<u32> = Vec::with_capacity(samples_n);
        let mut ki = 0usize;
        for _ in 0..samples_n {
            let k = reader_keys[ki];
            ki += 1;
            if ki == reader_keys.len() {
                ki = 0;
            }
            let mut rr = 0u32;
            let t0 = rdtsc();
            let (v, g) = b.get(black_box(k), &mut rr);
            let t1 = rdtscp();
            black_box((v, g));
            assert!(
                check_pair(k, v, g),
                "TORN PAIR: cand={} id={k} v={v} g={g:#x}",
                b.name()
            );
            ticks.push((t1 - t0).min(u32::MAX as u64) as u32);
            retries.push(rr);
        }
        stop.store(true, Relaxed);
        let (wcells, wsecs) = match wh {
            Some(h) => h.join().unwrap(),
            None => (0, 1.0),
        };

        let q = tsc.quantiles_ns(&mut ticks);
        retries.sort_unstable();
        LatencyRow {
            p50: q.p50,
            p99: q.p99,
            p999: q.p999,
            max: q.max,
            mean: q.mean,
            retry_p999: count_quantile(&retries, 0.999),
            retry_max: *retries.last().unwrap(),
            writer_cells_per_s: wcells as f64 / wsecs,
        }
    })
}

fn fmt_row(name: &str, scen: &str, r: &LatencyRow) {
    println!(
        "{:<16} {:<10} p50 {:>8.1} ns  p99 {:>8.1} ns  p99.9 {:>9.1} ns  max {:>10.1} ns  mean {:>8.1} ns  retries(p99.9/max) {}/{}  writer {:>7.2} Mcells/s",
        name,
        scen,
        r.p50,
        r.p99,
        r.p999,
        r.max,
        r.mean,
        r.retry_p999,
        r.retry_max,
        r.writer_cells_per_s / 1e6
    );
}

fn latency_for<B: Bench>(b: &B, tsc: &Tsc, n: u64, samples_n: usize, with_writer: bool) {
    let rk_uniform = keys::uniform(n, 1 << 20, SEED);
    let rk_page = keys::one_page(1 << 20, SEED ^ 1);
    let wk_uniform = keys::uniform(n, 1 << 20, SEED ^ 2);
    let wk_page = keys::one_page(1 << 20, SEED ^ 3);

    let r = read_latency(b, tsc, &rk_uniform, None, n, samples_n);
    fmt_row(b.name(), "a:quiet", &r);
    if with_writer {
        let r = read_latency(b, tsc, &rk_uniform, Some(&wk_uniform), n, samples_n);
        fmt_row(b.name(), "b:writer", &r);
        let r = read_latency(b, tsc, &rk_page, Some(&wk_page), n, samples_n);
        fmt_row(b.name(), "c:samepage", &r);
    }
}

fn cmd_latency(n: u64) {
    let tsc = calibrate();
    println!(
        "== read latency, n={} (tsc {:.4} ns/tick, rdtsc overhead {} ticks = {:.1} ns) ==",
        n,
        tsc.ns_per_tick,
        tsc.overhead_ticks,
        tsc.overhead_ticks as f64 * tsc.ns_per_tick
    );
    let samples = 2_000_000usize;

    {
        let b = build_fjall(n);
        latency_for(&b, &tsc, n, 400_000, true);
    }
    {
        let b = build_raw_hash(n);
        latency_for(&b, &tsc, n, samples, false); // raw map: no-writer only
    }
    {
        let b = build_locked_hash(n);
        latency_for(&b, &tsc, n, samples, true);
    }
    {
        let b = build_direct::<SeqCell>(n);
        latency_for(&b, &tsc, n, samples, true);
    }
    {
        let b = build_direct::<DblCell>(n);
        latency_for(&b, &tsc, n, samples, true);
    }
    {
        let b = build_direct::<U128Cell>(n);
        latency_for(&b, &tsc, n, samples, true);
    }
}

// --------------------------------------------------------- throughput ----

fn throughput_for<B: Bench>(b: &B, n: u64, kys: &[u64], label: &str, secs: f64) {
    let stop = AtomicBool::new(false);
    let wk = keys::uniform(n, 1 << 20, SEED ^ 7);
    std::thread::scope(|s| {
        let readers: Vec<_> = READER_CORES
            .iter()
            .enumerate()
            .map(|(r, &core)| {
                let kys = &kys;
                let stop = &stop;
                s.spawn(move || {
                    pin_to(core);
                    let mut ki = (r * kys.len()) / READER_CORES.len();
                    let mut rr = 0u32;
                    let mut ops = 0u64;
                    while !stop.load(Relaxed) {
                        for _ in 0..1024 {
                            let k = kys[ki];
                            ki += 1;
                            if ki == kys.len() {
                                ki = 0;
                            }
                            let (v, g) = b.get(black_box(k), &mut rr);
                            assert!(check_pair(k, v, g), "torn pair ({label})");
                            black_box((v, g));
                        }
                        ops += 1024;
                    }
                    ops
                })
            })
            .collect();
        let wh = s.spawn(|| writer_loop(b, &wk, n, 100, &stop));
        let t0 = Instant::now();
        std::thread::sleep(std::time::Duration::from_secs_f64(secs));
        stop.store(true, Relaxed);
        let elapsed = t0.elapsed().as_secs_f64();
        let (wcells, wsecs) = wh.join().unwrap();
        let total_reads: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
        println!(
            "{:<16} {:<12} 4-reader aggregate {:>8.2} Mreads/s   writer {:>7.2} Mcells/s",
            b.name(),
            label,
            total_reads as f64 / elapsed / 1e6,
            wcells as f64 / wsecs / 1e6
        );
    });
}

fn cmd_throughput(n: u64) {
    println!("== read throughput: 4 readers + 1 writer (batch 100), n={n} ==");
    let uni = keys::uniform(n, 1 << 22, SEED ^ 11);
    let zip = keys::zipf(n, 1.1, 1 << 22, SEED ^ 13);
    let secs = 4.0;

    {
        let b = build_fjall(n);
        throughput_for(&b, n, &uni, "uniform", secs);
        throughput_for(&b, n, &zip, "zipf1.1", secs);
    }
    {
        let b = build_locked_hash(n);
        throughput_for(&b, n, &uni, "uniform", secs);
        throughput_for(&b, n, &zip, "zipf1.1", secs);
    }
    {
        let b = build_direct::<SeqCell>(n);
        throughput_for(&b, n, &uni, "uniform", secs);
        throughput_for(&b, n, &zip, "zipf1.1", secs);
    }
    {
        let b = build_direct::<DblCell>(n);
        throughput_for(&b, n, &uni, "uniform", secs);
        throughput_for(&b, n, &zip, "zipf1.1", secs);
    }
    {
        let b = build_direct::<U128Cell>(n);
        throughput_for(&b, n, &uni, "uniform", secs);
        throughput_for(&b, n, &zip, "zipf1.1", secs);
    }
}

// --------------------------------------------------------------- apply ----

fn apply_for<B: Bench>(b: &B, n: u64, batch: usize, total_cells: u64) {
    pin_to(WRITER_CORE);
    // Pre-generate the whole update stream OUTSIDE the timed region so the
    // measurement is pure table-apply cost, not update synthesis (the random
    // walk over the writer's private version array costs about as much as
    // the apply itself for the direct kinds).
    let kys = keys::uniform(n, 1 << 20, SEED ^ 17);
    let mut versions = vec![1u64; n as usize];
    let mut updates: Vec<Update> = Vec::with_capacity(total_cells as usize);
    let mut ki = 0usize;
    for _ in 0..total_cells {
        let id = kys[ki];
        ki += 1;
        if ki == kys.len() {
            ki = 0;
        }
        let v = versions[id as usize] + 1;
        versions[id as usize] = v;
        updates.push((id, v, encode_global(v, id)));
    }
    let t0 = Instant::now();
    for chunk in updates.chunks_mut(batch) {
        b.apply(chunk);
    }
    let dt = t0.elapsed().as_secs_f64();
    println!(
        "{:<16} batch {:>6}   {:>8.2} Mcells/s   ({} cells in {:.2}s)",
        b.name(),
        batch,
        total_cells as f64 / dt / 1e6,
        total_cells,
        dt
    );
}

fn cmd_apply(n: u64) {
    println!("== writer apply throughput (no readers), n={n} ==");
    {
        let b = build_fjall(n);
        apply_for(&b, n, 1, 200_000);
        apply_for(&b, n, 100, 2_000_000);
        apply_for(&b, n, 10_000, 2_000_000);
    }
    {
        let b = build_locked_hash(n);
        apply_for(&b, n, 1, 20_000_000);
        apply_for(&b, n, 100, 20_000_000);
        apply_for(&b, n, 10_000, 20_000_000);
    }
    {
        let b = build_direct::<SeqCell>(n);
        apply_for(&b, n, 1, 20_000_000);
        apply_for(&b, n, 100, 20_000_000);
        apply_for(&b, n, 10_000, 20_000_000);
    }
    {
        let b = build_direct::<DblCell>(n);
        apply_for(&b, n, 1, 20_000_000);
        apply_for(&b, n, 100, 20_000_000);
        apply_for(&b, n, 10_000, 20_000_000);
    }
    {
        let b = build_direct::<U128Cell>(n);
        apply_for(&b, n, 1, 20_000_000);
        apply_for(&b, n, 100, 20_000_000);
        apply_for(&b, n, 10_000, 20_000_000);
    }
}

// ----------------------------------------------------------------- mem ----

fn cmd_mem(cand: &str, n: u64) {
    let rss0 = rss_bytes();
    let analytic: Option<usize>;
    // Keep the table alive until after the final RSS read.
    let _keep: Box<dyn std::any::Any>;
    match cand {
        "a0" => {
            let b = build_fjall(n);
            analytic = None;
            _keep = Box::new(b);
        }
        "a1" => {
            let b = build_raw_hash(n);
            analytic = Some(b.0.capacity() * (size_of::<(u64, (u64, u64))>() + 1));
            _keep = Box::new(b);
        }
        "a2" => {
            let b = build_direct::<SeqCell>(n);
            analytic = Some(b.allocated_bytes());
            _keep = Box::new(b);
        }
        "a3" => {
            let b = build_direct::<DblCell>(n);
            analytic = Some(b.allocated_bytes());
            _keep = Box::new(b);
        }
        "a4" => {
            let b = build_direct::<U128Cell>(n);
            analytic = Some(b.allocated_bytes());
            _keep = Box::new(b);
        }
        _ => panic!("unknown candidate {cand}"),
    }
    let rss1 = rss_bytes();
    let delta = rss1.saturating_sub(rss0);
    println!(
        "mem {:<4} n={:<10} rss_delta {:>12} B   {:>8.2} B/stream   analytic {:>12} B ({:.2} B/stream)",
        cand,
        n,
        delta,
        delta as f64 / n as f64,
        analytic.map(|a| a.to_string()).unwrap_or_else(|| "-".into()),
        analytic.map(|a| a as f64 / n as f64).unwrap_or(f64::NAN),
    );
}

fn cmd_mem_all() {
    println!("== resident memory per stream (child process RSS deltas) ==");
    let exe = std::env::current_exe().unwrap();
    for (cand, ns) in [
        ("a0", &[1_000_000u64][..]),
        ("a1", &[1_000_000, 10_000_000][..]),
        ("a2", &[1_000_000, 10_000_000][..]),
        ("a3", &[1_000_000, 10_000_000][..]),
        ("a4", &[1_000_000, 10_000_000][..]),
    ] {
        for &n in ns {
            let out = std::process::Command::new(&exe)
                .args(["mem", cand, &n.to_string()])
                .output()
                .expect("child");
            print!("{}", String::from_utf8_lossy(&out.stdout));
            assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
        }
    }
}

// ---------------------------------------------------------------- torn ----

/// Page growth under readers, with the torn-pair invariant checked on every
/// read. Writer grows the table from 0 to `grow_to` streams (forcing repeated
/// directory doublings) while also re-updating random existing streams.
fn torn_for<C: state_kernel_dense_heads::direct::CellKind>(grow_to: u64) {
    let table = DirectTable::<C>::new(1);
    let published = AtomicU64::new(0); // ids < published are readable
    let stop = AtomicBool::new(false);
    let violations = AtomicU64::new(0);
    let reads = AtomicU64::new(0);

    std::thread::scope(|s| {
        for &core in READER_CORES.iter() {
            let table = &table;
            let published = &published;
            let stop = &stop;
            let violations = &violations;
            let reads = &reads;
            s.spawn(move || {
                pin_to(core);
                let mut rr = 0u32;
                let mut x = core as u64 * 0x9E37_79B9_7F4A_7C15;
                let mut ops = 0u64;
                while !stop.load(Relaxed) {
                    // Acquire pairs with the writer's Release publish: ids
                    // below `hi` must have visible cells.
                    let hi = published.load(std::sync::atomic::Ordering::Acquire);
                    if hi == 0 {
                        std::thread::yield_now();
                        continue;
                    }
                    // xorshift key choice
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let id = x % hi;
                    let (v, g) = table.get(id, &mut rr);
                    if !check_pair(id, v, g) || v == 0 {
                        // v==0 for id < published is also a violation:
                        // published streams must be present.
                        violations.fetch_add(1, Relaxed);
                    }
                    ops += 1;
                }
                reads.fetch_add(ops, Relaxed);
            });
        }
        // Writer: append new streams + update random old ones.
        pin_to(WRITER_CORE);
        let mut versions = vec![0u64; grow_to as usize];
        let mut buf: Vec<Update> = Vec::with_capacity(2_000);
        let mut next = 0u64;
        let mut x = 0xDEAD_BEEFu64;
        while next < grow_to {
            buf.clear();
            let end = (next + 1_000).min(grow_to);
            for id in next..end {
                versions[id as usize] = 1;
                buf.push((id, 1, encode_global(1, id)));
            }
            if next > 0 {
                for _ in 0..1_000 {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    let id = x % next;
                    let v = versions[id as usize] + 1;
                    versions[id as usize] = v;
                    buf.push((id, v, encode_global(v, id)));
                }
            }
            table.apply(&mut buf);
            // Publish AFTER apply, Release so readers observing `end` also
            // observe the directory/page/cell writes behind it.
            published.store(end, std::sync::atomic::Ordering::Release);
            next = end;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        stop.store(true, Relaxed);
    });

    println!(
        "{:<16} grow 0->{:<9} reads {:>11}   invariant violations {}   {}",
        C::NAME,
        grow_to,
        reads.load(Relaxed),
        violations.load(Relaxed),
        if violations.load(Relaxed) == 0 { "OK" } else { "FAIL" }
    );
}

fn cmd_torn() {
    println!("== page growth under 4 readers + torn-pair invariant ==");
    torn_for::<SeqCell>(4_000_000);
    torn_for::<DblCell>(4_000_000);
    torn_for::<U128Cell>(4_000_000);
}

// ---------------------------------------------------------------- info ----

fn cmd_info() {
    let model = std::fs::read_to_string("/proc/cpuinfo")
        .unwrap()
        .lines()
        .find(|l| l.starts_with("model name"))
        .map(|l| l.split(':').nth(1).unwrap().trim().to_string())
        .unwrap_or_default();
    let gov = std::fs::read_to_string(
        "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor",
    )
    .unwrap_or_default();
    println!("cpu:      {model}");
    println!("governor: {}", gov.trim());
    println!("threads:  {}", std::thread::available_parallelism().unwrap());
    println!(
        "AtomicU128 lock-free (portable_atomic): {}",
        U128Cell::is_lock_free()
    );
    let tsc = calibrate();
    println!(
        "tsc:      {:.4} ns/tick ({:.2} GHz), rdtsc overhead {} ticks = {:.1} ns",
        tsc.ns_per_tick,
        1.0 / tsc.ns_per_tick,
        tsc.overhead_ticks,
        tsc.overhead_ticks as f64 * tsc.ns_per_tick
    );
}

// ---------------------------------------------------------------- main ----

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = args.first().map(|s| s.as_str()).unwrap_or("all");
    let arg_n = |i: usize, default: u64| -> u64 {
        args.get(i).map(|s| s.parse().unwrap()).unwrap_or(default)
    };
    std::fs::create_dir_all(bench_dir()).unwrap();
    match cmd {
        "info" => cmd_info(),
        "latency" => cmd_latency(arg_n(1, 1_000_000)),
        "throughput" => cmd_throughput(arg_n(1, 1_000_000)),
        "apply" => cmd_apply(arg_n(1, 1_000_000)),
        "mem" => cmd_mem(&args[1], args[2].parse().unwrap()),
        "mem-all" => cmd_mem_all(),
        "torn" => cmd_torn(),
        "all" => {
            cmd_info();
            println!();
            cmd_latency(1_000_000);
            println!();
            cmd_latency(10_000_000);
            println!();
            cmd_throughput(1_000_000);
            println!();
            cmd_apply(1_000_000);
            println!();
            cmd_mem_all();
            println!();
            cmd_torn();
        }
        other => panic!("unknown command {other}"),
    }
    let _ = std::fs::remove_dir_all(bench_dir());
}
