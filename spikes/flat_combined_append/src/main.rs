//! Spike B (bn-28g) benchmark harness: matched side-by-side runs of
//!
//!   bare — the raw `mess-log` committer (no validation): the lower bound
//!   log  — the current composed engine (`mess_store::LogEngine::append_batch`)
//!   b0   — FlatEngine, owner awaits the barrier inline
//!   b1   — FlatEngine, owner pipelines group N+1 validation under group N's barrier
//!
//! Same host, same session, interleaved A/B/B/A ordering (research/05 §15.1),
//! ext4 scratch (`$HOME/.cache/mess-bench` unless `MESS_BENCH_DIR` overrides —
//! NEVER tmpfs: `fdatasync` there is a no-op and durable numbers would be a
//! lie). Best-of and median reported; per-run rows appended to
//! `flat_combined_results.csv` in the CWD.
//!
//! ```text
//! cargo run --release -- matrix          # the full gate matrix
//! cargo run --release -- shapes          # 1024-stream round-robin vs hot stream
//! cargo run --release -- b1probe         # 64-writer Group-mode B0 vs B1
//! cargo run --release -- point <engine> <mode> <payload> <batch> <writers> <bpw> [shape]
//! ```

use std::collections::HashMap;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flat_combined_append::{
    FlatConfig, FlatEngine, RecordToAppend, Variant, Version,
};
use mess_log::committer::{
    AppendRequest, Committer, Durability, EventInput,
};
use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::writer::{SegmentParams, SegmentWriter};
use mess_store::backend::Backend;
use mess_store::{EngineOptions, LogEngine};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EngineKind {
    Bare,
    Log,
    B0,
    B1,
    /// Diagnostic: B0, tokio producers, owned-records entry point (one
    /// payload copy, like bare's `EventInput` handoff) — isolates the
    /// `&[RecordToAppend]` defensive-copy cost.
    B0Owned,
    /// B0Direct: the owner owns the `SegmentWriter` (design.md §6.3 — one
    /// coalesced write + one barrier on the owner itself; no committer
    /// thread). Tokio producers, borrowed records (matched to `log`).
    B0Direct,
    /// Diagnostic: B0 with producers on dedicated OS threads (the exact
    /// producer topology `bare` uses) + owned records — isolates tokio
    /// scheduling latency from the flat design itself.
    B0Threads,
    /// Diagnostic: B0Direct + OS-thread producers + owned records — the
    /// minimal-overhead flat topology (nearest to `bare`).
    B0DirectThreads,
}

impl EngineKind {
    fn name(self) -> &'static str {
        match self {
            EngineKind::Bare => "bare",
            EngineKind::Log => "log",
            EngineKind::B0 => "b0",
            EngineKind::B1 => "b1",
            EngineKind::B0Owned => "b0o",
            EngineKind::B0Direct => "b0d",
            EngineKind::B0Threads => "b0th",
            EngineKind::B0DirectThreads => "b0dth",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// One stream per writer (the envelope/durable_bench shape).
    PerWriter,
    /// 1024 distinct streams, round-robin (each writer owns 1024/writers).
    Rr1024,
    /// One hot stream, ONE writer (exact-version semantics make >1 writer
    /// on one stream a conflict storm — kept matched across engines).
    Hot1,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::PerWriter => "perwriter",
            Shape::Rr1024 => "rr1024",
            Shape::Hot1 => "hot1",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Workload {
    writers: u64,
    batch:   usize,
    bpw:     u64,
    payload: usize,
    shape:   Shape,
}

impl Workload {
    fn total_events(&self) -> u64 {
        self.writers * self.bpw * self.batch as u64
    }

    fn streams_per_writer(&self) -> u64 {
        match self.shape {
            Shape::PerWriter => 1,
            Shape::Rr1024 => (1024 / self.writers).max(1),
            Shape::Hot1 => 1,
        }
    }

    /// The stream a writer's b-th append goes to (disjoint across writers,
    /// so driver-tracked exact versions never conflict).
    fn sid_for(&self, w: u64, b: u64) -> u64 {
        match self.shape {
            Shape::PerWriter => w,
            Shape::Rr1024 => {
                let spw = self.streams_per_writer();
                w * spw + (b % spw)
            }
            Shape::Hot1 => 0,
        }
    }

    fn segment_size(&self) -> u64 {
        let est = self
            .total_events()
            .saturating_mul(self.payload as u64 + 96)
            .saturating_mul(2)
            .saturating_add(1 << 20);
        est.max(256 * 1024 * 1024).next_power_of_two()
    }
}

#[derive(Debug, Clone, Copy)]
struct RunResult {
    ev_s:          f64,
    wall_s:        f64,
    p50_us:        f64,
    p99_us:        f64,
    fsyncs:        u64,
    mean_fsync_us: f64,
}

fn pct(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Merge per-writer latency vectors, dropping each writer's first 10%
/// (warm-up), and compute p50/p99 in µs.
fn latency_stats(per_writer: Vec<Vec<u64>>) -> (f64, f64) {
    let mut all: Vec<u64> = Vec::new();
    for lats in per_writer {
        let skip = lats.len() / 10;
        all.extend_from_slice(&lats[skip..]);
    }
    all.sort_unstable();
    (pct(&all, 0.50) as f64 / 1e3, pct(&all, 0.99) as f64 / 1e3)
}

fn scratch_dir() -> mess_testkit::SweepingTempDir {
    match std::env::var_os("MESS_BENCH_DIR").map(PathBuf::from) {
        Some(root) => {
            std::fs::create_dir_all(&root).expect("create scratch base");
            mess_testkit::temp_dir_in(&root, "flat-spike")
        }
        None => {
            let home = std::env::var("HOME").expect("HOME");
            let root = PathBuf::from(home).join(".cache").join("mess-bench");
            std::fs::create_dir_all(&root).expect("create scratch base");
            mess_testkit::temp_dir_in(&root, "flat-spike")
        }
    }
}

fn payload_bytes(n: usize) -> Vec<u8> {
    (0..n as u32).map(|i| (i & 0xFF) as u8).collect()
}

// ---------------------------------------------------------------------------
// bare: raw committer, no validation
// ---------------------------------------------------------------------------

fn run_bare(wl: Workload, durability: Durability) -> RunResult {
    let scratch = scratch_dir();
    let rt = RealRuntime::new();
    let fs = rt.fs();
    let path = scratch.path().join("bare-seg-1.log");
    let mut params = SegmentParams::new(1, 0, 1, 0);
    params.segment_size = wl.segment_size();
    let writer =
        SegmentWriter::create(&fs, &path, params).expect("create segment");
    let payload = payload_bytes(wl.payload);

    let c = Committer::spawn(&rt, writer, durability);
    let start = Instant::now();
    let per_writer: Vec<Vec<u64>> = rt.block_on(async {
        let mut joins = Vec::new();
        for w in 0..wl.writers {
            let ap = c.appender();
            let payload = payload.clone();
            joins.push(rt.spawn(async move {
                let mut lats = Vec::with_capacity(wl.bpw as usize);
                let mut next_ver: HashMap<u64, u64> = HashMap::new();
                for b in 0..wl.bpw {
                    let sid = wl.sid_for(w, b);
                    let ver = next_ver.entry(sid).or_insert(0);
                    let events: Vec<EventInput> = (0..wl.batch)
                        .map(|_| EventInput::plain(1, 0, 0, payload.clone()))
                        .collect();
                    let req = AppendRequest {
                        stream_id: sid,
                        category_id: 0,
                        first_stream_version: *ver,
                        events,
                    };
                    let t = Instant::now();
                    ap.append(req).await.expect("bare append acked");
                    lats.push(t.elapsed().as_nanos() as u64);
                    *ver += wl.batch as u64;
                }
                lats
            }));
        }
        let mut out = Vec::new();
        for j in joins {
            out.push(j.await);
        }
        out
    });
    let wall_s = start.elapsed().as_secs_f64();
    let fsyncs = c.fsync_count();
    let mean_fsync_us = c.mean_fsync_nanos() as f64 / 1e3;
    rt.block_on(c.shutdown());

    let (p50_us, p99_us) = latency_stats(per_writer);
    RunResult {
        ev_s: wl.total_events() as f64 / wall_s,
        wall_s,
        p50_us,
        p99_us,
        fsyncs,
        mean_fsync_us,
    }
}

// ---------------------------------------------------------------------------
// log: the current composed engine
// ---------------------------------------------------------------------------

fn run_log(wl: Workload, durability: Durability) -> RunResult {
    let scratch = scratch_dir();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads((wl.writers as usize).clamp(2, 8))
        .enable_all()
        .build()
        .unwrap();
    let engine = LogEngine::open_with(
        scratch.path().join("store"),
        EngineOptions {
            durability,
            segment_size: wl.segment_size(),
            ..Default::default()
        },
    )
    .expect("open LogEngine");
    let payload = payload_bytes(wl.payload);

    let start = Instant::now();
    let per_writer: Vec<Vec<u64>> = rt.block_on(async {
        let mut joins = Vec::new();
        for w in 0..wl.writers {
            let engine = engine.clone();
            let payload = payload.clone();
            joins.push(tokio::spawn(async move {
                let mut lats = Vec::with_capacity(wl.bpw as usize);
                let mut expected: HashMap<u64, Version> = HashMap::new();
                for b in 0..wl.bpw {
                    let sid = wl.sid_for(w, b);
                    let stream = format!("s{sid}");
                    let exp =
                        *expected.entry(sid).or_insert(Version::NoStream);
                    let records: Vec<RecordToAppend> = (0..wl.batch)
                        .map(|_| RecordToAppend {
                            message_type: "ev.t".to_string(),
                            data:         payload.clone(),
                        })
                        .collect();
                    let t = Instant::now();
                    let out = engine
                        .append_batch(&stream, exp, &records)
                        .await
                        .expect("log append");
                    lats.push(t.elapsed().as_nanos() as u64);
                    expected.insert(sid, out.version);
                }
                lats
            }));
        }
        let mut out = Vec::new();
        for j in joins {
            out.push(j.await.expect("writer task"));
        }
        out
    });
    let wall_s = start.elapsed().as_secs_f64();
    let m = engine.metrics();
    let fsyncs = m.commit.fsync.count;
    let mean_fsync_us = m.commit.fsync.mean_nanos as f64 / 1e3;
    drop(engine);
    rt.shutdown_timeout(Duration::from_secs(10));

    let (p50_us, p99_us) = latency_stats(per_writer);
    RunResult {
        ev_s: wl.total_events() as f64 / wall_s,
        wall_s,
        p50_us,
        p99_us,
        fsyncs,
        mean_fsync_us,
    }
}

// ---------------------------------------------------------------------------
// b0 / b1: FlatEngine
// ---------------------------------------------------------------------------

fn run_flat(
    wl: Workload,
    durability: Durability,
    variant: Variant,
    owned: bool,
) -> RunResult {
    let scratch = scratch_dir();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads((wl.writers as usize).clamp(2, 8))
        .enable_all()
        .build()
        .unwrap();
    let engine = Arc::new(
        FlatEngine::open(
            scratch.path().join("store"),
            FlatConfig {
                durability,
                segment_size: wl.segment_size(),
                variant,
                ..Default::default()
            },
        )
        .expect("open FlatEngine"),
    );
    let payload = payload_bytes(wl.payload);

    let start = Instant::now();
    let per_writer: Vec<Vec<u64>> = rt.block_on(async {
        let mut joins = Vec::new();
        for w in 0..wl.writers {
            let engine = Arc::clone(&engine);
            let payload = payload.clone();
            joins.push(tokio::spawn(async move {
                let mut lats = Vec::with_capacity(wl.bpw as usize);
                let mut expected: HashMap<u64, Version> = HashMap::new();
                for b in 0..wl.bpw {
                    let sid = wl.sid_for(w, b);
                    let stream = format!("s{sid}");
                    let exp =
                        *expected.entry(sid).or_insert(Version::NoStream);
                    let records: Vec<RecordToAppend> = (0..wl.batch)
                        .map(|_| RecordToAppend {
                            message_type: "ev.t".to_string(),
                            data:         payload.clone(),
                        })
                        .collect();
                    let t = Instant::now();
                    let out = if owned {
                        engine
                            .append_batch_owned(&stream, exp, records)
                            .await
                            .expect("flat append")
                    } else {
                        engine
                            .append_batch(&stream, exp, &records)
                            .await
                            .expect("flat append")
                    };
                    lats.push(t.elapsed().as_nanos() as u64);
                    expected.insert(sid, out.version);
                }
                lats
            }));
        }
        let mut out = Vec::new();
        for j in joins {
            out.push(j.await.expect("writer task"));
        }
        out
    });
    let wall_s = start.elapsed().as_secs_f64();
    assert_eq!(
        engine.watermark(),
        wl.total_events(),
        "flat watermark must equal total committed events"
    );
    let engine = Arc::into_inner(engine).expect("sole engine handle");
    let exit = engine.close();
    assert_eq!(exit.stats.position_mismatches, 0, "shadow positions diverged");
    rt.shutdown_timeout(Duration::from_secs(10));

    let (p50_us, p99_us) = latency_stats(per_writer);
    RunResult {
        ev_s: wl.total_events() as f64 / wall_s,
        wall_s,
        p50_us,
        p99_us,
        fsyncs: exit.stats.fsyncs,
        mean_fsync_us: exit.stats.mean_fsync_nanos as f64 / 1e3,
    }
}

/// Diagnostic variant: FlatEngine driven by one OS thread per writer, each
/// thread block_on-ing its appends — the same producer topology as `bare`.
fn run_flat_threads(
    wl: Workload,
    durability: Durability,
    variant: Variant,
) -> RunResult {
    let scratch = scratch_dir();
    let engine = Arc::new(
        FlatEngine::open(
            scratch.path().join("store"),
            FlatConfig {
                durability,
                segment_size: wl.segment_size(),
                variant,
                ..Default::default()
            },
        )
        .expect("open FlatEngine"),
    );
    let payload = payload_bytes(wl.payload);

    let start = Instant::now();
    let mut joins = Vec::new();
    for w in 0..wl.writers {
        let engine = Arc::clone(&engine);
        let payload = payload.clone();
        joins.push(std::thread::spawn(move || {
            let rt = RealRuntime::new();
            let mut lats = Vec::with_capacity(wl.bpw as usize);
            let mut expected: HashMap<u64, Version> = HashMap::new();
            for b in 0..wl.bpw {
                let sid = wl.sid_for(w, b);
                let stream = format!("s{sid}");
                let exp = *expected.entry(sid).or_insert(Version::NoStream);
                let records: Vec<RecordToAppend> = (0..wl.batch)
                    .map(|_| RecordToAppend {
                        message_type: "ev.t".to_string(),
                        data:         payload.clone(),
                    })
                    .collect();
                let t = Instant::now();
                let out = rt
                    .block_on(
                        engine.append_batch_owned(&stream, exp, records),
                    )
                    .expect("flat append");
                lats.push(t.elapsed().as_nanos() as u64);
                expected.insert(sid, out.version);
            }
            lats
        }));
    }
    let per_writer: Vec<Vec<u64>> =
        joins.into_iter().map(|j| j.join().expect("writer")).collect();
    let wall_s = start.elapsed().as_secs_f64();
    assert_eq!(engine.watermark(), wl.total_events());
    let engine = Arc::into_inner(engine).expect("sole engine handle");
    let exit = engine.close();
    assert_eq!(exit.stats.position_mismatches, 0);

    let (p50_us, p99_us) = latency_stats(per_writer);
    RunResult {
        ev_s: wl.total_events() as f64 / wall_s,
        wall_s,
        p50_us,
        p99_us,
        fsyncs: exit.stats.fsyncs,
        mean_fsync_us: exit.stats.mean_fsync_nanos as f64 / 1e3,
    }
}

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

fn run_one(kind: EngineKind, wl: Workload, durability: Durability) -> RunResult {
    match kind {
        EngineKind::Bare => run_bare(wl, durability),
        EngineKind::Log => run_log(wl, durability),
        EngineKind::B0 => run_flat(wl, durability, Variant::B0, false),
        EngineKind::B1 => run_flat(wl, durability, Variant::B1, false),
        EngineKind::B0Owned => run_flat(wl, durability, Variant::B0, true),
        EngineKind::B0Direct => {
            run_flat(wl, durability, Variant::B0Direct, false)
        }
        EngineKind::B0Threads => {
            run_flat_threads(wl, durability, Variant::B0)
        }
        EngineKind::B0DirectThreads => {
            run_flat_threads(wl, durability, Variant::B0Direct)
        }
    }
}

fn mode_name(d: Durability) -> &'static str {
    match d {
        Durability::Process => "process",
        Durability::Os => "os",
        Durability::Group { .. } => "group",
    }
}

fn csv_line(
    csv: &mut std::fs::File,
    cell: &str,
    kind: EngineKind,
    wl: Workload,
    d: Durability,
    rep: usize,
    r: RunResult,
) {
    writeln!(
        csv,
        "{cell},{},{},{},{},{},{},{},{rep},{:.0},{:.4},{:.1},{:.1},{},{:.1}",
        kind.name(),
        mode_name(d),
        wl.payload,
        wl.batch,
        wl.writers,
        wl.bpw,
        wl.shape.name(),
        r.ev_s,
        r.wall_s,
        r.p50_us,
        r.p99_us,
        r.fsyncs,
        r.mean_fsync_us,
    )
    .expect("csv write");
}

/// Interleaved A/B/B/A run of `kinds` over one cell: 3 reps per engine in
/// the order ABCD DCBA ABCD. Settle pause before every durable run.
fn run_cell(
    label: &str,
    kinds: &[EngineKind],
    wl: Workload,
    d: Durability,
    csv: &mut std::fs::File,
) -> HashMap<&'static str, Vec<RunResult>> {
    let settle = match d {
        Durability::Process => Duration::from_millis(500),
        _ => Duration::from_secs(8),
    };
    let mut order: Vec<EngineKind> = Vec::new();
    order.extend_from_slice(kinds); // A B C D
    order.extend(kinds.iter().rev().copied()); // D C B A
    order.extend_from_slice(kinds); // A B C D
    let mut results: HashMap<&'static str, Vec<RunResult>> = HashMap::new();
    let mut rep_of: HashMap<&'static str, usize> = HashMap::new();
    println!(
        "== cell {label}: payload={} batch={} writers={} bpw={} shape={} \
         mode={} total_events={}",
        wl.payload,
        wl.batch,
        wl.writers,
        wl.bpw,
        wl.shape.name(),
        mode_name(d),
        wl.total_events()
    );
    for kind in order {
        std::thread::sleep(settle);
        let r = run_one(kind, wl, d);
        let rep = rep_of.entry(kind.name()).or_insert(0);
        println!(
            "  {:<4} rep{}: {:>12.0} ev/s  wall {:.3}s  p50 {:.1}us  p99 {:.1}us  \
             fsyncs {}  mean_fsync {:.1}us",
            kind.name(),
            rep,
            r.ev_s,
            r.wall_s,
            r.p50_us,
            r.p99_us,
            r.fsyncs,
            r.mean_fsync_us
        );
        csv_line(csv, label, kind, wl, d, *rep, r);
        *rep += 1;
        results.entry(kind.name()).or_default().push(r);
    }
    // Cell summary: best + median ev/s per engine.
    for kind in kinds {
        let rs = &results[kind.name()];
        let mut evs: Vec<f64> = rs.iter().map(|r| r.ev_s).collect();
        evs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let best = *evs.last().unwrap();
        let median = evs[evs.len() / 2];
        println!(
            "  {:<4} SUMMARY best {:>12.0} ev/s  median {:>12.0} ev/s",
            kind.name(),
            best,
            median
        );
    }
    results
}

fn open_csv() -> std::fs::File {
    let fresh = !std::path::Path::new("flat_combined_results.csv").exists();
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("flat_combined_results.csv")
        .expect("open csv");
    if fresh {
        writeln!(
            f,
            "cell,engine,mode,payload,batch,writers,bpw,shape,rep,ev_s,\
             wall_s,p50_us,p99_us,fsyncs,mean_fsync_us"
        )
        .unwrap();
    }
    f
}

fn matrix() {
    let mut csv = open_csv();
    let kinds = [
        EngineKind::Bare,
        EngineKind::Log,
        EngineKind::B0,
        EngineKind::B1,
        EngineKind::B0Direct,
        EngineKind::B0DirectThreads,
    ];
    for payload in [24usize, 250] {
        for batch in [10usize, 100] {
            // Process
            let bpw = if batch == 10 { 12_500 } else { 2_500 };
            let wl = Workload {
                writers: 4,
                batch,
                bpw,
                payload,
                shape: Shape::PerWriter,
            };
            let label = format!("proc-p{payload}-b{batch}");
            run_cell(&label, &kinds, wl, Durability::Process, &mut csv);
            // Group (default params)
            let bpw = 500;
            let wl = Workload {
                writers: 4,
                batch,
                bpw,
                payload,
                shape: Shape::PerWriter,
            };
            let label = format!("group-p{payload}-b{batch}");
            run_cell(&label, &kinds, wl, Durability::group_default(), &mut csv);
        }
    }
}

fn shapes() {
    let mut csv = open_csv();
    let kinds = [
        EngineKind::Bare,
        EngineKind::Log,
        EngineKind::B0,
        EngineKind::B1,
        EngineKind::B0Direct,
        EngineKind::B0DirectThreads,
    ];
    // 1024 distinct streams, round-robin.
    let wl = Workload {
        writers: 4,
        batch:   10,
        bpw:     12_500,
        payload: 250,
        shape:   Shape::Rr1024,
    };
    run_cell("proc-rr1024", &kinds, wl, Durability::Process, &mut csv);
    // Single hot stream, single writer (matched exact-version semantics).
    let wl = Workload {
        writers: 1,
        batch:   10,
        bpw:     50_000,
        payload: 250,
        shape:   Shape::Hot1,
    };
    run_cell("proc-hot1", &kinds, wl, Durability::Process, &mut csv);
}

fn b1probe() {
    let mut csv = open_csv();
    // 64 writers, Group durability: enough concurrency that intents queue
    // behind an in-flight barrier — the one place B1's overlap can matter.
    let kinds = [EngineKind::B0, EngineKind::B1, EngineKind::B0Direct];
    let wl = Workload {
        writers: 64,
        batch:   10,
        bpw:     100,
        payload: 250,
        shape:   Shape::PerWriter,
    };
    run_cell("group-64w-b10", &kinds, wl, Durability::group_default(), &mut csv);
    let wl = Workload {
        writers: 64,
        batch:   100,
        bpw:     50,
        payload: 250,
        shape:   Shape::PerWriter,
    };
    run_cell("group-64w-b100", &kinds, wl, Durability::group_default(), &mut csv);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("matrix") => matrix(),
        Some("shapes") => shapes(),
        Some("b1probe") => b1probe(),
        Some("point") => {
            let kind = match args[2].as_str() {
                "bare" => EngineKind::Bare,
                "log" => EngineKind::Log,
                "b0" => EngineKind::B0,
                "b1" => EngineKind::B1,
                "b0o" => EngineKind::B0Owned,
                "b0d" => EngineKind::B0Direct,
                "b0th" => EngineKind::B0Threads,
                "b0dth" => EngineKind::B0DirectThreads,
                other => panic!("unknown engine {other}"),
            };
            let d = match args[3].as_str() {
                "process" => Durability::Process,
                "group" => Durability::group_default(),
                other => panic!("unknown mode {other}"),
            };
            let payload: usize = args[4].parse().unwrap();
            let batch: usize = args[5].parse().unwrap();
            let writers: u64 = args[6].parse().unwrap();
            let bpw: u64 = args[7].parse().unwrap();
            let shape = match args.get(8).map(String::as_str) {
                None | Some("perwriter") => Shape::PerWriter,
                Some("rr1024") => Shape::Rr1024,
                Some("hot1") => Shape::Hot1,
                Some(other) => panic!("unknown shape {other}"),
            };
            let wl = Workload { writers, batch, bpw, payload, shape };
            let r = run_one(kind, wl, d);
            println!(
                "point {} {}: {:.0} ev/s wall {:.3}s p50 {:.1}us p99 {:.1}us \
                 fsyncs {} mean_fsync {:.1}us",
                kind.name(),
                mode_name(d),
                r.ev_s,
                r.wall_s,
                r.p50_us,
                r.p99_us,
                r.fsyncs,
                r.mean_fsync_us
            );
        }
        _ => {
            eprintln!("usage: flat_combined_append <matrix|shapes|b1probe|point ...>");
            std::process::exit(2);
        }
    }
}
