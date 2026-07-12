//! Spike F bench driver. Subcommands: info | apply | latency | seek | mem |
//! reclaim | all (plus the internal mem-child). Every measured phase is
//! preceded by the quiet-guard (no compilers, no sibling
//! `directory_tournament` bench, load1 < 6.0); per-op samples open with
//! rdtsc and close with rdtscp; quantiles from full sorted samples.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use active_microblocks::timing::{
    Quantiles, Tsc, calibrate, ensure_quiet, pin_to, rdtsc, rdtscp, rss_bytes,
};
use active_microblocks::workload::{
    Rng, Schedule, Streams, Zipf, apply_groups, build_schedule,
};
use active_microblocks::{BatchEntry, F0, F1, F2, F3, Index};
use arc_swap::ArcSwap;

const WRITER_CORE: usize = 5;
const ZIPF_STREAMS: usize = 100_000;
/// Writer rate during read benches (batches/s): far above any durable
/// group-commit rate the committer can actually sustain, but bounded so
/// memory stays sane and every candidate faces the same write pressure.
const WRITER_RATE: f64 = 500_000.0;
const WARMUP: usize = 10_000;

/// AMB_SMOKE=1 shrinks every phase and skips the quiet-guard — a plumbing
/// smoke test ONLY (numbers meaningless, never reported).
fn smoke() -> bool {
    std::env::var("AMB_SMOKE").is_ok_and(|v| v == "1")
}
fn prebuild_n() -> usize {
    if smoke() { 100_000 } else { 2_000_000 }
}
fn extension_n() -> usize {
    if smoke() { 200_000 } else { 4_000_000 }
}
fn samples_n() -> usize {
    if smoke() { 20_000 } else { 200_000 }
}
fn apply_n() -> usize {
    if smoke() { 200_000 } else { 2_000_000 }
}
fn mem_n() -> usize {
    if smoke() { 100_000 } else { 1_000_000 }
}
fn reclaim_n() -> usize {
    if smoke() { 100_000 } else { 1_000_000 }
}
fn guard() {
    if !smoke() {
        ensure_quiet(3600);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(String::as_str).unwrap_or("all");
    match cmd {
        "info" => info(),
        "apply" => bench_apply(),
        "latency" => bench_latency(),
        "seek" => bench_seek(),
        "mem" => bench_mem(),
        "mem-child" => mem_child(&args[2..]),
        "reclaim" => bench_reclaim(),
        "all" => {
            info();
            bench_apply();
            bench_latency();
            bench_seek();
            bench_mem();
            bench_reclaim();
        }
        other => {
            eprintln!("unknown command {other}");
            std::process::exit(2);
        }
    }
}

fn info() {
    let tsc = calibrate();
    println!("== info ==");
    println!(
        "tsc: {:.4} ns/tick, empty rdtsc..rdtscp overhead {} ticks ({:.1} ns)",
        tsc.ns_per_tick,
        tsc.overhead_ticks,
        tsc.overhead_ticks as f64 * tsc.ns_per_tick
    );
    println!(
        "block: size {} B, {} entries -> {:.2} B/batch at full occupancy (analytic)",
        std::mem::size_of::<active_microblocks::micro::PtrMicroblock>(),
        active_microblocks::micro::ENTRIES,
        std::mem::size_of::<active_microblocks::micro::PtrMicroblock>() as f64
            / active_microblocks::micro::ENTRIES as f64
    );
    println!(
        "incumbent StreamEntry {} B + GlobalEntry {} B per batch (pre-container)",
        std::mem::size_of::<active_microblocks::StreamEntry>(),
        std::mem::size_of::<active_microblocks::GlobalEntry>()
    );
}

fn fmt_q(q: &Quantiles) -> String {
    format!(
        "p50 {:>8.1}  p90 {:>8.1}  p99 {:>9.1}  p99.9 {:>10.1}  max {:>12.0}  mean {:>8.1}",
        q.p50, q.p90, q.p99, q.p999, q.max, q.mean
    )
}

// ---------------------------------------------------------------------
// apply throughput
// ---------------------------------------------------------------------

fn bench_apply() {
    println!("== apply throughput (single writer, pinned core {WRITER_CORE}; pre-generated schedules) ==");
    pin_to(WRITER_CORE);
    let n = apply_n();
    for &events in &[1u32, 100] {
        for workload in ["hot", "zipf"] {
            let streams = if workload == "hot" {
                Streams::Hot
            } else {
                Streams::Zipf(ZIPF_STREAMS)
            };
            let sched = build_schedule(streams, n, events, 0x5EED);
            for &group in &[1usize, 64] {
                guard();
                {
                    let idx = F0::new();
                    let t0 = Instant::now();
                    apply_groups(&idx, &sched.batches, group);
                    let dt = t0.elapsed().as_secs_f64();
                    println!(
                        "apply {:<14} ev={:<3} {:<4} group={:<2}  {:>7.3} M batches/s  ({:>8.1} M events/s)",
                        idx.name(), events, workload, group,
                        n as f64 / dt / 1e6,
                        n as f64 * f64::from(events) / dt / 1e6
                    );
                    drop(idx);
                }
                guard();
                {
                    let idx = F1::new(active_microblocks::workload::SEGMENT, 8);
                    let t0 = Instant::now();
                    apply_groups(&idx, &sched.batches, group);
                    let dt = t0.elapsed().as_secs_f64();
                    let st = idx.0.stats();
                    println!(
                        "apply {:<14} ev={:<3} {:<4} group={:<2}  {:>7.3} M batches/s  ({:>8.1} M events/s)  [blocks {} escapes {} ({:.3}%)]",
                        "F-arena", events, workload, group,
                        n as f64 / dt / 1e6,
                        n as f64 * f64::from(events) / dt / 1e6,
                        st.blocks, st.escapes,
                        st.escapes as f64 * 100.0 / st.batches as f64
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------
// read latency under a concurrent writer
// ---------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum Op {
    /// Timed `resolve(sid, head)` at a fresh (untimed) head — the
    /// recent-version lookup.
    Recent,
    /// Timed `stream_head(sid)` — the head/tail resolve of the gate.
    Head,
    /// Timed `resolve(hot, head - 4096)`: 128 blocks deep at 1-event
    /// batches (differentiates the chain-walk candidates).
    Deep,
}

struct LatResult {
    q: Quantiles,
}

/// One latency scenario: writer applies `ext` in 64-batch groups at
/// WRITER_RATE while `readers` sample `op`. Returns pooled quantiles.
fn latency_scenario<I: Index>(
    idx: Arc<I>,
    ext: Arc<Vec<BatchEntry>>,
    readers: usize,
    op: Op,
    hot: bool, // single-stream workload: every reader on stream 0
    zipf_seed: u64,
    tsc: &Tsc,
) -> LatResult {
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(readers + 2));

    // Writer.
    let writer = {
        let idx = idx.clone();
        let ext = ext.clone();
        let stop = stop.clone();
        let start = start.clone();
        std::thread::spawn(move || {
            pin_to(WRITER_CORE);
            start.wait();
            let group = 64usize;
            let tick = std::time::Duration::from_secs_f64(group as f64 / WRITER_RATE);
            let t0 = Instant::now();
            let mut applied = 0usize;
            for g in ext.chunks(group) {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let wm = g.last().unwrap().end_pos();
                idx.apply(wm, g);
                applied += g.len();
                let target = tick * (applied / group) as u32;
                while t0.elapsed() < target {
                    std::hint::spin_loop();
                }
            }
            applied
        })
    };

    // Readers.
    let zipf = Arc::new(Zipf::new(ZIPF_STREAMS, 1.1, zipf_seed));
    let handles: Vec<_> = (0..readers)
        .map(|r| {
            let idx = idx.clone();
            let start = start.clone();
            let zipf = zipf.clone();
            std::thread::spawn(move || {
                // Cores 1..=4 for 4 readers; round-robin skipping the
                // writer's core for 64.
                let c = { let c = r % 23; if c >= WRITER_CORE { c + 1 } else { c } };
                pin_to(c);
                // Pre-generate the stream choices (no keygen in the loop).
                let mut rng = Rng::new(0xBEEF ^ r as u64);
                let sids: Vec<u64> = (0..samples_n() + WARMUP)
                    .map(|_| match op {
                        Op::Recent | Op::Head if !hot => zipf.sample(&mut rng),
                        _ => 0,
                    })
                    .collect();
                let mut samples = Vec::with_capacity(samples_n());
                start.wait();
                match op {
                    Op::Recent => {
                        for (i, &sid) in sids.iter().enumerate() {
                            let Some(h) = idx.head(sid) else { continue };
                            let t0 = rdtsc();
                            let r = idx.resolve(sid, h);
                            let t1 = rdtscp();
                            black_box(&r);
                            assert!(r.is_some(), "committed head must resolve");
                            if i >= WARMUP {
                                samples.push((t1 - t0) as u32);
                            }
                        }
                    }
                    Op::Head => {
                        for (i, &sid) in sids.iter().enumerate() {
                            let t0 = rdtsc();
                            let h = idx.head(sid);
                            let t1 = rdtscp();
                            black_box(&h);
                            if i >= WARMUP {
                                samples.push((t1 - t0) as u32);
                            }
                        }
                    }
                    Op::Deep => {
                        for (i, _) in sids.iter().enumerate() {
                            let Some(h) = idx.head(0) else { continue };
                            let v = h.saturating_sub(4096);
                            let t0 = rdtsc();
                            let r = idx.resolve(0, v);
                            let t1 = rdtscp();
                            black_box(&r);
                            assert!(r.is_some());
                            if i >= WARMUP {
                                samples.push((t1 - t0) as u32);
                            }
                        }
                    }
                }
                samples
            })
        })
        .collect();

    start.wait();
    let mut pooled: Vec<u32> = Vec::with_capacity(readers * samples_n());
    for h in handles {
        pooled.extend(h.join().unwrap());
    }
    stop.store(true, Ordering::Relaxed);
    let _applied = writer.join().unwrap();
    LatResult { q: tsc.quantiles_ns(&mut pooled) }
}

/// One fully isolated scenario run: build a FRESH index, prebuild it from
/// `sched`, then run the writer+readers. (An index must never receive the
/// extension twice — re-applying already-applied batches violates the
/// committer's version-continuity contract and corrupts ordering.)
fn lat_run<I: Index>(
    mk: impl Fn() -> I,
    sched: &Schedule,
    ext: &Arc<Vec<BatchEntry>>,
    readers: usize,
    op: Op,
    hot: bool,
    tsc: &Tsc,
) -> Quantiles {
    guard();
    let idx = {
        let i = mk();
        apply_groups(&i, &sched.batches, 64);
        Arc::new(i)
    };
    latency_scenario(idx, ext.clone(), readers, op, hot, 0xFACE, tsc).q
}

fn bench_latency() {
    let tsc = calibrate();
    println!("== read latency under concurrent writer (ns/op; writer {WRITER_RATE:.0} batches/s in 64-batch groups; fresh index per row) ==");

    let op_name = |op: Op| if op == Op::Head { "head" } else { "resolve" };

    // Scenario A: Zipf(1.1) over 100k streams, 1-event batches.
    {
        let sched = build_schedule(Streams::Zipf(ZIPF_STREAMS), prebuild_n(), 1, 0x5EED);
        let ext = {
            let full = build_schedule(
                Streams::Zipf(ZIPF_STREAMS),
                prebuild_n() + extension_n(),
                1,
                0x5EED,
            );
            Arc::new(full.batches[prebuild_n()..].to_vec())
        };
        macro_rules! run {
            ($mk:expr, $label:expr) => {
                for &readers in &[4usize, 64] {
                    for op in [Op::Recent, Op::Head] {
                        let q = lat_run($mk, &sched, &ext, readers, op, false, &tsc);
                        println!(
                            "lat zipf {:<16} r={:<2} {:<7} {}",
                            $label, readers, op_name(op), fmt_q(&q)
                        );
                    }
                }
            };
        }
        run!(|| F0::new(), "F0-incumbent");
        run!(|| F1::new(sched.segment, 8), "F1-micro-linear");
        run!(|| F2::new(sched.segment, 8), "F2-micro-binary");
        run!(|| F3::new(sched.segment, 8), "F3-micro-skip");
    }

    // Scenario B: adversarial — every reader on the exact stream being
    // written (same shard for F0, same tail block/page for the arena);
    // plus the deep resolve that separates the chain walks.
    {
        let sched = build_schedule(Streams::Hot, prebuild_n(), 1, 0x5EED);
        let ext = {
            let full = build_schedule(Streams::Hot, prebuild_n() + extension_n(), 1, 0x5EED);
            Arc::new(full.batches[prebuild_n()..].to_vec())
        };
        macro_rules! run_hot {
            ($mk:expr, $label:expr) => {
                for &readers in &[4usize, 64] {
                    let q = lat_run($mk, &sched, &ext, readers, Op::Recent, true, &tsc);
                    println!("lat hot  {:<16} r={:<2} resolve {}", $label, readers, fmt_q(&q));
                }
                let q = lat_run($mk, &sched, &ext, 4, Op::Head, true, &tsc);
                println!("lat hot  {:<16} r=4  head    {}", $label, fmt_q(&q));
                let q = lat_run($mk, &sched, &ext, 4, Op::Deep, true, &tsc);
                println!("lat deep {:<16} r=4  resolve {}", $label, fmt_q(&q));
            };
        }
        run_hot!(|| F0::new(), "F0-incumbent");
        run_hot!(|| F1::new(sched.segment, 8), "F1-micro-linear");
        run_hot!(|| F2::new(sched.segment, 8), "F2-micro-binary");
        run_hot!(|| F3::new(sched.segment, 8), "F3-micro-skip");
    }
}

// ---------------------------------------------------------------------
// global seek under writer
// ---------------------------------------------------------------------

fn bench_seek() {
    let tsc = calibrate();
    println!("== global seek: position -> batch, 4 readers under writer ==");
    let sched = build_schedule(Streams::Zipf(ZIPF_STREAMS), prebuild_n(), 1, 0x5EED);
    let ext = {
        let full =
            build_schedule(Streams::Zipf(ZIPF_STREAMS), prebuild_n() + extension_n(), 1, 0x5EED);
        Arc::new(full.batches[prebuild_n()..].to_vec())
    };

    fn seek_run<I: Index>(idx: Arc<I>, ext: Arc<Vec<BatchEntry>>, tsc: &Tsc, label: &str) {
        guard();
        let stop = Arc::new(AtomicBool::new(false));
        let start = Arc::new(Barrier::new(6));
        let writer = {
            let idx = idx.clone();
            let ext = ext.clone();
            let stop = stop.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                pin_to(WRITER_CORE);
                start.wait();
                let tick = std::time::Duration::from_secs_f64(64.0 / WRITER_RATE);
                let t0 = Instant::now();
                let mut n = 0usize;
                for g in ext.chunks(64) {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    idx.apply(g.last().unwrap().end_pos(), g);
                    n += 1;
                    while t0.elapsed() < tick * n as u32 {
                        std::hint::spin_loop();
                    }
                }
            })
        };
        let handles: Vec<_> = (0..4)
            .map(|r| {
                let idx = idx.clone();
                let start = start.clone();
                std::thread::spawn(move || {
                    pin_to(1 + r);
                    let mut rng = Rng::new(0xA5EE ^ r as u64);
                    let mut samples = Vec::with_capacity(samples_n());
                    start.wait();
                    for i in 0..samples_n() + WARMUP {
                        let w = idx.applied_end();
                        let pos = rng.below(w.max(1));
                        let t0 = rdtsc();
                        let s = idx.seek(pos);
                        let t1 = rdtscp();
                        black_box(&s);
                        assert!(s.is_some(), "dense log seek below watermark");
                        if i >= WARMUP {
                            samples.push((t1 - t0) as u32);
                        }
                    }
                    samples
                })
            })
            .collect();
        start.wait();
        let mut pooled = Vec::with_capacity(4 * samples_n());
        for h in handles {
            pooled.extend(h.join().unwrap());
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
        println!("seek {:<22} {}", label, fmt_q(&tsc.quantiles_ns(&mut pooled)));
    }

    {
        let f0 = F0::new();
        apply_groups(&f0, &sched.batches, 64);
        seek_run(Arc::new(f0), ext.clone(), &tsc, "F0-globalvec(range1)");
    }
    {
        let f1 = F1::new(sched.segment, 8);
        apply_groups(&f1, &sched.batches, 64);
        seek_run(Arc::new(f1), ext.clone(), &tsc, "F1-sparse-stride8");
    }
    {
        let f1 = F1::new(sched.segment, 1);
        apply_groups(&f1, &sched.batches, 64);
        seek_run(Arc::new(f1), ext.clone(), &tsc, "F1-sparse-stride1");
    }
}

// ---------------------------------------------------------------------
// memory per committed batch (fresh child process per point)
// ---------------------------------------------------------------------

fn bench_mem() {
    println!("== memory per committed batch (fresh child per point; RSS delta across apply; 1-event batches) ==");
    let exe = std::env::current_exe().unwrap();
    let mut points: Vec<(&str, &str, usize)> = Vec::new();
    for cand in ["f0", "arena-s8", "arena-s1"] {
        for workload in ["hot", "zipf"] {
            points.push((cand, workload, mem_n()));
        }
    }
    // Fuller active segment (4x): does zipf-tail block waste amortize as
    // streams accumulate more batches?
    points.push(("f0", "zipf", 4 * mem_n()));
    points.push(("arena-s8", "zipf", 4 * mem_n()));
    for (cand, workload, n) in points {
        guard();
        let out = std::process::Command::new(&exe)
            .args(["mem-child", cand, workload, &n.to_string()])
            .output()
            .expect("child");
        assert!(out.status.success(), "mem-child failed: {}", String::from_utf8_lossy(&out.stderr));
        print!("{}", String::from_utf8_lossy(&out.stdout));
    }
}

fn mem_child(args: &[String]) {
    let cand = args[0].as_str();
    let workload = args[1].as_str();
    let n: usize = args[2].parse().unwrap();
    let streams = if workload == "hot" { Streams::Hot } else { Streams::Zipf(ZIPF_STREAMS) };
    let sched = build_schedule(streams, n, 1, 0x5EED);
    let rss0 = rss_bytes();
    let t0 = Instant::now();
    let (rss1, analytic, extra, drop_ms) = match cand {
        "f0" => {
            let idx = F0::new();
            apply_groups(&idx, &sched.batches, 64);
            let rss1 = rss_bytes();
            let td = Instant::now();
            drop(idx);
            (rss1, None, String::new(), td.elapsed().as_secs_f64() * 1e3)
        }
        "arena-s8" | "arena-s1" => {
            let stride = if cand == "arena-s8" { 8 } else { 1 };
            let idx = F1::new(sched.segment, stride);
            apply_groups(&idx, &sched.batches, 64);
            let rss1 = rss_bytes();
            let st = idx.0.stats();
            let td = Instant::now();
            drop(idx);
            (
                rss1,
                Some(st.alloc_bytes),
                format!(" blocks {} escapes {} ({:.3}%)", st.blocks, st.escapes, st.escapes as f64 * 100.0 / st.batches as f64),
                td.elapsed().as_secs_f64() * 1e3,
            )
        }
        _ => panic!("unknown candidate"),
    };
    let apply_s = t0.elapsed().as_secs_f64();
    println!(
        "mem {:<9} {:<4} n={:<8}  RSS {:>7.2} B/batch  analytic {}  drop {:>8.2} ms{}  (apply+drop {:.2}s)",
        cand,
        workload,
        n,
        (rss1.saturating_sub(rss0)) as f64 / n as f64,
        analytic.map_or("   n/a  ".to_string(), |a| format!("{:>7.2} B/batch", a as f64 / n as f64)),
        drop_ms,
        extra,
        apply_s
    );
}

// ---------------------------------------------------------------------
// segment-roll reclaim: reader latency around generation retirement
// ---------------------------------------------------------------------

fn bench_reclaim() {
    let tsc = calibrate();
    println!("== segment-roll reclaim (1M-batch generation retired under 8 readers holding leases) ==");

    fn run<I: Index, F: Fn(u64) -> I>(mk: F, label: &str, tsc: &Tsc) {
        guard();
        let sched = build_schedule(Streams::Zipf(ZIPF_STREAMS), reclaim_n(), 1, 0x5EED);
        let gen_a = {
            let idx = mk(1);
            apply_groups(&idx, &sched.batches, 64);
            Arc::new(idx)
        };
        let gen_b = {
            let idx = mk(2);
            apply_groups(&idx, &sched.batches[..100_000], 64);
            Arc::new(idx)
        };
        let current: Arc<ArcSwap<I>> = Arc::new(ArcSwap::from(gen_a));
        let stop = Arc::new(AtomicBool::new(false));
        let start = Arc::new(Barrier::new(9));
        let zipf = Arc::new(Zipf::new(ZIPF_STREAMS, 1.1, 0x5EED));

        let ns_per_tick = tsc.ns_per_tick;
        let readers: Vec<_> = (0..8)
            .map(|r| {
                let current = current.clone();
                let stop = stop.clone();
                let start = start.clone();
                let zipf = zipf.clone();
                std::thread::spawn(move || {
                    let c = { let c = r % 23; if c >= WRITER_CORE { c + 1 } else { c } };
                    pin_to(c);
                    let mut rng = Rng::new(0xF00D ^ r as u64);
                    // Cyclic sid buffer + a fixed op pacing (~500k ops/s per
                    // reader) so every candidate faces the SAME op stream
                    // and no reader exhausts its samples before the roll.
                    let sids: Vec<u64> =
                        (0..1_048_576).map(|_| zipf.sample(&mut rng)).collect();
                    let pace_ticks = (2_000.0 / ns_per_tick) as u64; // 2 us
                    let mut out: Vec<(u64, u32)> = Vec::with_capacity(4_000_000);
                    start.wait();
                    let mut i = 0usize;
                    let mut next = rdtsc();
                    while !stop.load(Ordering::Relaxed) {
                        let sid = sids[i & (sids.len() - 1)];
                        i += 1;
                        let t0 = rdtsc();
                        let g = current.load();
                        g.check_canary();
                        let h = g.head(sid);
                        if let Some(h) = h {
                            black_box(g.resolve(sid, h));
                        }
                        g.check_canary();
                        let t1 = rdtscp();
                        if out.len() < out.capacity() {
                            out.push((t0, (t1 - t0) as u32));
                        }
                        next += pace_ticks;
                        while rdtsc() < next {
                            std::hint::spin_loop();
                        }
                    }
                    out
                })
            })
            .collect();

        start.wait();
        let t_start = rdtsc();
        std::thread::sleep(std::time::Duration::from_millis(800));

        // Roll: take leases on A, publish B, then drain.
        let lease = current.load_full();
        let t_swap0 = rdtsc();
        current.store(gen_b);
        let t_swap1 = rdtscp();
        std::thread::sleep(std::time::Duration::from_millis(400));
        // Every reader is on B now; the held lease is the last handle.
        let t_free0 = rdtsc();
        drop(lease);
        let t_free1 = rdtscp();
        std::thread::sleep(std::time::Duration::from_millis(800));
        stop.store(true, Ordering::Relaxed);

        let mut pre: Vec<u32> = Vec::new();
        let mut during: Vec<u32> = Vec::new();
        let mut post: Vec<u32> = Vec::new();
        let margin = (50e6 / tsc.ns_per_tick) as u64; // 50 ms in ticks
        for h in readers {
            for (ts, d) in h.join().unwrap() {
                if ts < t_swap0 - margin {
                    pre.push(d);
                } else if ts <= t_free1 + margin {
                    during.push(d);
                } else {
                    post.push(d);
                }
            }
        }
        let _ = t_start;
        println!(
            "reclaim {:<16} swap {:>9.0} ns   slab-free {:>10.2} ms   (readers held leases across both)",
            label,
            (t_swap1 - t_swap0) as f64 * tsc.ns_per_tick,
            (t_free1 - t_free0) as f64 * tsc.ns_per_tick / 1e6
        );
        for (name, mut v) in [("pre", pre), ("during", during), ("post", post)] {
            if v.is_empty() {
                continue;
            }
            let q = tsc.quantiles_ns(&mut v);
            println!("reclaim {:<16}   {:<6} ({:>8} ops) {}", label, name, v.len(), fmt_q(&q));
        }
    }

    run(|seg| { let f = F0::new(); let _ = seg; f }, "F0-incumbent", &tsc);
    run(|seg| F1::new(seg, 8), "F1-micro", &tsc);
}
