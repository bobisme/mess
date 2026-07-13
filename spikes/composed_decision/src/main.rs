//! Spike J (bn-2gu) — the composed decision: harness.
//!
//! Five engines, matched, interleaved, same session, real ext4 scratch
//! (`$HOME/.cache/mess-bench` unless `MESS_BENCH_DIR` overrides — NEVER
//! tmpfs: `fdatasync` there is a no-op and durable numbers would be a lie):
//!
//! ```text
//! bare   raw mess-log committer/appender: no registry, no validation, no
//!        index — the lower bound (identical driver to spikes/baseline_matrix)
//! log    the CURRENT composed engine on main (mess_store::LogEngine),
//!        post bn-34o / bn-2cj / bn-2ib(C) / bn-9mw(E) / bn-3of(I)
//! logsp  the same engine with EngineOptions::seal_pack = true (Spike I)
//! b0d    Spike B's FlatEngine, B0Direct (owner OWNS the writer + barrier),
//!        tokio producers, borrowed records — matched API shape to `log`
//! b0dth  B0Direct + OS-thread producers + owned records — the producer
//!        topology `bare` uses; the diagnostic upper bound of the flat design
//! ```
//!
//! ```text
//! cargo run --release -- matrix      # 32 cells x 5 engines x 3 reps
//! cargo run --release -- newname     # 100%-new-stream name-barrier cells
//! cargo run --release -- convoy      # bn-3pz: Group convoy split vs writers
//! cargo run --release -- sealmatrix  # sealpack where it bites: 8 MiB segments
//! cargo run --release -- seek        # Spike F stride-8 global-seek scan check
//! cargo run --release -- point <engine> <mode> <payload> <batch> <writers> <bpw> [newname]
//! ```
//!
//! Every measured phase is quiet-guarded ([`timing::ensure_quiet`]); every
//! run asserts a logical result check (composed watermark / flat watermark ==
//! events written; flat position_mismatches == 0). Per-run rows are appended
//! to `composed_results.csv` in the CWD.

use std::collections::HashMap;
use std::io::Write as _;
use std::os::unix::fs::FileExt as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use composed_decision::timing;
use composed_decision::{
    FlatConfig, FlatEngine, RecordToAppend, Variant, Version,
};
use mess_log::committer::{AppendRequest, Committer, Durability, EventInput};
use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::writer::{SegmentParams, SegmentWriter};
use mess_store::backend::Backend;
use mess_store::{EngineOptions, LogEngine};

// ---------------------------------------------------------------------------
// Engines
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EngineKind {
    Bare,
    Log,
    LogSealPack,
    /// FlatEngine B0Direct, tokio producers, borrowed records — the honest
    /// drop-in comparator for `log` (same async `Backend`-shaped API).
    B0Direct,
    /// FlatEngine B0Direct, OS-thread producers, owned records — bounds where
    /// the residual cost lives; NOT a drop-in engine number.
    B0DirectThreads,
}

impl EngineKind {
    fn name(self) -> &'static str {
        match self {
            EngineKind::Bare => "bare",
            EngineKind::Log => "log",
            EngineKind::LogSealPack => "logsp",
            EngineKind::B0Direct => "b0d",
            EngineKind::B0DirectThreads => "b0dth",
        }
    }
}

fn parse_engine(s: &str) -> EngineKind {
    match s {
        "bare" => EngineKind::Bare,
        "log" => EngineKind::Log,
        "logsp" => EngineKind::LogSealPack,
        "b0d" => EngineKind::B0Direct,
        "b0dth" => EngineKind::B0DirectThreads,
        other => panic!("unknown engine {other}"),
    }
}

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct Workload {
    writers: u64,
    batch: usize,
    /// Batches per writer.
    bpw: u64,
    payload: usize,
    /// Every append targets a brand-new stream (the seeder / name-barrier
    /// shape). When false, one stable stream per writer.
    new_name: bool,
    /// Segment size override (bytes). `None` = the baseline-gen2 rule: big
    /// enough that no segment ever rolls, so seal/roll work is OUT of the
    /// measured append path. `Some(n)` forces rolls (the `sealmatrix` cells).
    seg_size: Option<u64>,
}

impl Workload {
    fn total_events(&self) -> u64 {
        self.writers * self.bpw * self.batch as u64
    }

    fn total_appends(&self) -> u64 {
        self.writers * self.bpw
    }

    /// Identical to `spikes/baseline_matrix`'s rule, so every cell here is
    /// directly comparable to baseline-gen2.
    fn segment_size(&self) -> u64 {
        if let Some(n) = self.seg_size {
            return n;
        }
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
    ev_s: f64,
    per_append_us: f64,
    wall_s: f64,
    p50_us: f64,
    p99_us: f64,
    fsyncs: u64,
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
    let root = match std::env::var_os("MESS_BENCH_DIR").map(PathBuf::from) {
        Some(root) => root,
        None => {
            let home = std::env::var("HOME").expect("HOME");
            PathBuf::from(home).join(".cache").join("mess-bench")
        }
    };
    std::fs::create_dir_all(&root).expect("create scratch base");
    mess_testkit::temp_dir_in(&root, "composed-j")
}

fn payload_bytes(n: usize) -> Vec<u8> {
    (0..n as u32).map(|i| (i & 0xFF) as u8).collect()
}

// ---------------------------------------------------------------------------
// bare: raw committer, no validation, numeric stream ids
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
                let mut ver: u64 = 0;
                for b in 0..wl.bpw {
                    let sid =
                        if wl.new_name { w * wl.bpw + b + 1 } else { w + 1 };
                    let first_ver = if wl.new_name { 0 } else { ver };
                    let events: Vec<EventInput> = (0..wl.batch)
                        .map(|_| EventInput::plain(1, 0, 0, payload.clone()))
                        .collect();
                    let req = AppendRequest {
                        stream_id: sid,
                        category_id: 0,
                        first_stream_version: first_ver,
                        events,
                    };
                    let t = Instant::now();
                    ap.append(req).await.expect("bare append acked");
                    lats.push(t.elapsed().as_nanos() as u64);
                    ver += wl.batch as u64;
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
        per_append_us: wall_s * 1e6 / wl.total_appends() as f64,
        wall_s,
        p50_us,
        p99_us,
        fsyncs,
        mean_fsync_us,
    }
}

// ---------------------------------------------------------------------------
// log / logsp: the CURRENT composed engine on main
// ---------------------------------------------------------------------------

fn run_log(wl: Workload, durability: Durability, seal_pack: bool) -> RunResult {
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
            seal_pack,
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
                let mut ver = Version::NoStream;
                for b in 0..wl.bpw {
                    let stream = if wl.new_name {
                        format!("w{w}-s{b}")
                    } else {
                        format!("s{w}")
                    };
                    let exp = if wl.new_name { Version::NoStream } else { ver };
                    let records: Vec<RecordToAppend> = (0..wl.batch)
                        .map(|_| RecordToAppend {
                            message_type: "ev.t".to_string(),
                            data: payload.clone(),
                        })
                        .collect();
                    let t = Instant::now();
                    let out = engine
                        .append_batch(&stream, exp, &records)
                        .await
                        .expect("log append");
                    lats.push(t.elapsed().as_nanos() as u64);
                    ver = out.version;
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
    // Logical result check.
    assert_eq!(
        engine.total_events() as u64,
        wl.total_events(),
        "composed watermark must equal total committed events"
    );
    let m = engine.metrics();
    let fsyncs = m.commit.fsync.count;
    let mean_fsync_us = m.commit.fsync.mean_nanos as f64 / 1e3;
    drop(engine);
    rt.shutdown_timeout(Duration::from_secs(30));

    let (p50_us, p99_us) = latency_stats(per_writer);
    RunResult {
        ev_s: wl.total_events() as f64 / wall_s,
        per_append_us: wall_s * 1e6 / wl.total_appends() as f64,
        wall_s,
        p50_us,
        p99_us,
        fsyncs,
        mean_fsync_us,
    }
}

// ---------------------------------------------------------------------------
// b0d / b0dth: Spike B's FlatEngine against TODAY's mess-log
// ---------------------------------------------------------------------------

fn flat_cfg(wl: Workload, durability: Durability) -> FlatConfig {
    FlatConfig {
        durability,
        segment_size: wl.segment_size(),
        variant: Variant::B0Direct,
        ..Default::default()
    }
}

fn run_flat_tokio(wl: Workload, durability: Durability) -> RunResult {
    let scratch = scratch_dir();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads((wl.writers as usize).clamp(2, 8))
        .enable_all()
        .build()
        .unwrap();
    let engine = Arc::new(
        FlatEngine::open(
            scratch.path().join("store"),
            flat_cfg(wl, durability),
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
                let mut ver = Version::NoStream;
                for b in 0..wl.bpw {
                    let stream = if wl.new_name {
                        format!("w{w}-s{b}")
                    } else {
                        format!("s{w}")
                    };
                    let exp = if wl.new_name { Version::NoStream } else { ver };
                    let records: Vec<RecordToAppend> = (0..wl.batch)
                        .map(|_| RecordToAppend {
                            message_type: "ev.t".to_string(),
                            data: payload.clone(),
                        })
                        .collect();
                    let t = Instant::now();
                    let out = engine
                        .append_batch(&stream, exp, &records)
                        .await
                        .expect("flat append");
                    lats.push(t.elapsed().as_nanos() as u64);
                    ver = out.version;
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
    rt.shutdown_timeout(Duration::from_secs(30));

    let (p50_us, p99_us) = latency_stats(per_writer);
    RunResult {
        ev_s: wl.total_events() as f64 / wall_s,
        per_append_us: wall_s * 1e6 / wl.total_appends() as f64,
        wall_s,
        p50_us,
        p99_us,
        fsyncs: exit.stats.fsyncs,
        mean_fsync_us: exit.stats.mean_fsync_nanos as f64 / 1e3,
    }
}

fn run_flat_threads(wl: Workload, durability: Durability) -> RunResult {
    let scratch = scratch_dir();
    let engine = Arc::new(
        FlatEngine::open(
            scratch.path().join("store"),
            flat_cfg(wl, durability),
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
            let mut ver = Version::NoStream;
            for b in 0..wl.bpw {
                let stream = if wl.new_name {
                    format!("w{w}-s{b}")
                } else {
                    format!("s{w}")
                };
                let exp = if wl.new_name { Version::NoStream } else { ver };
                let records: Vec<RecordToAppend> = (0..wl.batch)
                    .map(|_| RecordToAppend {
                        message_type: "ev.t".to_string(),
                        data: payload.clone(),
                    })
                    .collect();
                let t = Instant::now();
                let out = rt
                    .block_on(engine.append_batch_owned(&stream, exp, records))
                    .expect("flat append");
                lats.push(t.elapsed().as_nanos() as u64);
                ver = out.version;
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
        per_append_us: wall_s * 1e6 / wl.total_appends() as f64,
        wall_s,
        p50_us,
        p99_us,
        fsyncs: exit.stats.fsyncs,
        mean_fsync_us: exit.stats.mean_fsync_nanos as f64 / 1e3,
    }
}

fn run_one(
    kind: EngineKind,
    wl: Workload,
    durability: Durability,
) -> RunResult {
    match kind {
        EngineKind::Bare => run_bare(wl, durability),
        EngineKind::Log => run_log(wl, durability, false),
        EngineKind::LogSealPack => run_log(wl, durability, true),
        EngineKind::B0Direct => run_flat_tokio(wl, durability),
        EngineKind::B0DirectThreads => run_flat_threads(wl, durability),
    }
}

// ---------------------------------------------------------------------------
// Orchestration
// ---------------------------------------------------------------------------

fn mode_name(d: Durability) -> &'static str {
    match d {
        Durability::Process => "process",
        Durability::Os => "os",
        Durability::Group { .. } => "group",
    }
}

fn open_csv() -> std::fs::File {
    let fresh = !std::path::Path::new("composed_results.csv").exists();
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("composed_results.csv")
        .expect("open csv");
    if fresh {
        writeln!(
            f,
            "cell,engine,mode,payload,batch,writers,bpw,kind,seg_mb,rep,ev_s,\
             per_append_us,wall_s,p50_us,p99_us,fsyncs,mean_fsync_us,load1"
        )
        .unwrap();
    }
    f
}

#[allow(clippy::too_many_arguments)]
fn csv_line(
    csv: &mut std::fs::File,
    cell: &str,
    kind: EngineKind,
    wl: Workload,
    d: Durability,
    rep: usize,
    r: RunResult,
    load1: f64,
) {
    writeln!(
        csv,
        "{cell},{},{},{},{},{},{},{},{},{rep},{:.0},{:.4},{:.4},{:.1},{:.1},\
         {},{:.1},{load1:.2}",
        kind.name(),
        mode_name(d),
        wl.payload,
        wl.batch,
        wl.writers,
        wl.bpw,
        if wl.new_name { "newname" } else { "stable" },
        wl.segment_size() / (1024 * 1024),
        r.ev_s,
        r.per_append_us,
        r.wall_s,
        r.p50_us,
        r.p99_us,
        r.fsyncs,
        r.mean_fsync_us,
    )
    .expect("csv write");
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn best(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    *v.last().unwrap()
}

/// Interleaved A/B..-..B/A ordering (research/05 §15.1), 3 reps per engine,
/// quiet-guarded, settle before every run. Returns per-engine results.
fn run_cell(
    label: &str,
    kinds: &[EngineKind],
    wl: Workload,
    d: Durability,
    csv: &mut std::fs::File,
) -> HashMap<&'static str, Vec<RunResult>> {
    let settle = match d {
        Durability::Process => Duration::from_millis(400),
        _ => Duration::from_secs(4),
    };
    let mut order: Vec<EngineKind> = Vec::new();
    order.extend_from_slice(kinds); // A B C D E
    order.extend(kinds.iter().rev().copied()); // E D C B A
    order.extend_from_slice(kinds); // A B C D E
    let mut results: HashMap<&'static str, Vec<RunResult>> = HashMap::new();
    let mut rep_of: HashMap<&'static str, usize> = HashMap::new();
    println!(
        "== cell {label}: payload={} batch={} writers={} bpw={} kind={} \
         mode={} seg={}MiB total_events={}",
        wl.payload,
        wl.batch,
        wl.writers,
        wl.bpw,
        if wl.new_name { "newname" } else { "stable" },
        mode_name(d),
        wl.segment_size() / (1024 * 1024),
        wl.total_events()
    );
    for kind in order {
        std::thread::sleep(settle);
        timing::ensure_quiet(900);
        // The load this run ACTUALLY ran under — recorded, not assumed.
        let load1 = timing::load1();
        let r = run_one(kind, wl, d);
        let rep = rep_of.entry(kind.name()).or_insert(0);
        println!(
            "  {:<6} rep{}: {:>12.0} ev/s  {:>8.3} us/app  wall {:.3}s  \
             p50 {:.1}us  p99 {:.1}us  fsyncs {}  mean_fsync {:.1}us",
            kind.name(),
            rep,
            r.ev_s,
            r.per_append_us,
            r.wall_s,
            r.p50_us,
            r.p99_us,
            r.fsyncs,
            r.mean_fsync_us
        );
        csv_line(csv, label, kind, wl, d, *rep, r, load1);
        *rep += 1;
        results.entry(kind.name()).or_default().push(r);
    }
    let med = |k: &str| -> Option<f64> {
        results.get(k).map(|rs| median(rs.iter().map(|r| r.ev_s).collect()))
    };
    for kind in kinds {
        let rs = &results[kind.name()];
        let evs: Vec<f64> = rs.iter().map(|r| r.ev_s).collect();
        let fsy: Vec<u64> = rs.iter().map(|r| r.fsyncs).collect();
        print!(
            "  {:<6} SUMMARY best {:>12.0}  median {:>12.0} ev/s",
            kind.name(),
            best(evs.clone()),
            median(evs.clone()),
        );
        if let (Some(b), Some(m)) = (med("bare"), med(kind.name())) {
            print!("  vs_bare {:>6.1}%", 100.0 * m / b);
        }
        if let (Some(l), Some(m)) = (med("log"), med(kind.name())) {
            print!("  vs_log {:>6.1}%", 100.0 * m / l);
        }
        println!("  fsyncs {fsy:?}");
    }
    results
}

/// Batches-per-writer per (batch, mode) — IDENTICAL to baseline-gen2's rule,
/// so every cell is directly comparable to `spikes/baseline_matrix`.
fn bpw_for(batch: usize, group: bool) -> u64 {
    if group {
        match batch {
            1 => 800,
            10 => 500,
            100 => 300,
            1000 => 100,
            _ => 300,
        }
    } else {
        match batch {
            1 => 40_000,
            10 => 12_500,
            100 => 2_500,
            1000 => 250,
            _ => 2_500,
        }
    }
}

const ALL: [EngineKind; 5] = [
    EngineKind::Bare,
    EngineKind::Log,
    EngineKind::LogSealPack,
    EngineKind::B0Direct,
    EngineKind::B0DirectThreads,
];

fn matrix() {
    let mut csv = open_csv();
    for payload in [24usize, 250] {
        for batch in [1usize, 10, 100, 1000] {
            for writers in [1u64, 4] {
                let wl = Workload {
                    writers,
                    batch,
                    bpw: bpw_for(batch, false),
                    payload,
                    new_name: false,
                    seg_size: None,
                };
                run_cell(
                    &format!("proc-p{payload}-b{batch}-w{writers}"),
                    &ALL,
                    wl,
                    Durability::Process,
                    &mut csv,
                );
                let wl = Workload { bpw: bpw_for(batch, true), ..wl };
                run_cell(
                    &format!("group-p{payload}-b{batch}-w{writers}"),
                    &ALL,
                    wl,
                    Durability::group_default(),
                    &mut csv,
                );
            }
        }
    }
}

/// 100%-new-stream name-barrier cells (research/05 §14 gate: "new-name
/// operation exactly one barrier"). `bare` has numeric ids and no registry at
/// all — the floor. `log`/`logsp` pay the post-bn-34o/bn-2cj Fjall name
/// persist. `b0d` keeps its shadow heads in memory with NO Fjall name table,
/// so it shows what a log-derived registry's new-name cost could be: the gap
/// between `log` and `b0d` here is exactly the price of the Fjall name table.
fn newname() {
    let mut csv = open_csv();
    let kinds = [
        EngineKind::Bare,
        EngineKind::Log,
        EngineKind::LogSealPack,
        EngineKind::B0Direct,
    ];
    for writers in [1u64, 4] {
        for (group, bpw) in [(false, 4_000u64), (true, 1_000u64)] {
            let wl = Workload {
                writers,
                batch: 1,
                bpw,
                payload: 250,
                new_name: true,
                seg_size: None,
            };
            let (label, d) = if group {
                (
                    format!("newname-group-w{writers}"),
                    Durability::group_default(),
                )
            } else {
                (format!("newname-proc-w{writers}"), Durability::Process)
            };
            run_cell(&label, &kinds, wl, d, &mut csv);
        }
    }
}

/// bn-3pz carry-forward: the Group-mode 4-writer small-batch convoy split.
/// baseline-gen2 measured the composed engine paying ~2x bare's barrier count
/// at 4 writers / low batches-per-writer (49-71% of bare) where Spike B's gen1
/// saw parity. Sweep the writer count: if the split is a group-window
/// FILL problem it must vanish as concurrency rises (Spike B's 64-writer
/// b1probe saw parity); if it is a committer regression it must persist.
/// b0d is included because Spike B had to REBUILD the early-close in its owner
/// — it is the control that shows what a correct window looks like.
fn convoy() {
    let mut csv = open_csv();
    let kinds = [EngineKind::Bare, EngineKind::Log, EngineKind::B0Direct];
    for batch in [10usize, 1000] {
        for writers in [1u64, 4, 8, 16, 32, 64] {
            // Hold total barriers roughly constant as writers grow: the group
            // window is per-COMMITTER, so bpw must shrink with writers or the
            // run time explodes.
            let bpw = (bpw_for(batch, true) * 4 / writers.max(1)).max(50);
            let wl = Workload {
                writers,
                batch,
                bpw,
                payload: 250,
                new_name: false,
                seg_size: None,
            };
            run_cell(
                &format!("convoy-b{batch}-w{writers}"),
                &kinds,
                wl,
                Durability::group_default(),
                &mut csv,
            );
        }
    }
}

/// Spike I where it actually bites the APPEND path: 8 MiB segments, so the
/// run rolls and seals segments WHILE appending. The main `matrix` uses
/// baseline-gen2's >=256 MiB segment rule, under which no segment ever rolls
/// and `seal_pack` is a no-op by construction — that is a fact worth proving,
/// not a gap to hide.
fn sealmatrix() {
    let mut csv = open_csv();
    let kinds = [EngineKind::Log, EngineKind::LogSealPack];
    for payload in [24usize, 250] {
        for (batch, bpw) in [(100usize, 5_000u64), (1000usize, 500u64)] {
            let wl = Workload {
                writers: 4,
                batch,
                bpw,
                payload,
                new_name: false,
                seg_size: Some(8 * 1024 * 1024),
            };
            run_cell(
                &format!("seal-p{payload}-b{batch}-w4"),
                &kinds,
                wl,
                Durability::Process,
                &mut csv,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Spike F carry-forward: does a stride-8 global seek really scan <= 7 batch
// headers out of the page cache, and what does that scan COST on a real v3
// segment?
// ---------------------------------------------------------------------------

/// One batch's ground truth, from the REAL scanner over a REAL segment the
/// REAL engine wrote.
#[derive(Clone, Copy)]
struct BatchRow {
    offset: u64,
    total_len: u64,
    first_global_pos: u64,
    frame_count: u32,
}

/// Read a 72-byte v3 `BatchHeader` at `off` with one `pread` — exactly what a
/// composed engine holding the segment fd would do — and return
/// `(first_global_pos, frame_count, total_len)`.
///
/// Field offsets are v3 `format.rs` §4.2: `frame_count @ 8` (u32),
/// `total_len @ 20` (u64), `first_global_pos @ 28` (u64).
fn pread_header(f: &std::fs::File, off: u64) -> (u64, u32, u64) {
    let mut hdr = [0u8; mess_log::format::HEADER_LEN];
    f.read_exact_at(&mut hdr, off).expect("pread header");
    let frame_count = u32::from_le_bytes(hdr[8..12].try_into().unwrap());
    let total_len = u64::from_le_bytes(hdr[20..28].try_into().unwrap());
    let first_global_pos = u64::from_le_bytes(hdr[28..36].try_into().unwrap());
    (first_global_pos, frame_count, total_len)
}

fn fadvise_dontneed(f: &std::fs::File) {
    use std::os::unix::io::AsRawFd;
    // POSIX_FADV_DONTNEED == 4 on Linux. Drops this file's clean page-cache
    // pages, so the "cold" row below is a real cold read, not a warm one.
    unsafe {
        let ret = libc_posix_fadvise(f.as_raw_fd(), 0, 0, 4);
        assert_eq!(ret, 0, "posix_fadvise(DONTNEED) failed");
    }
}

unsafe extern "C" {
    #[link_name = "posix_fadvise"]
    fn libc_posix_fadvise(fd: i32, offset: i64, len: i64, advice: i32) -> i32;
    #[link_name = "munmap"]
    fn libc_munmap(addr: *mut core::ffi::c_void, len: usize) -> i32;
    #[link_name = "mmap"]
    fn libc_mmap(
        addr: *mut core::ffi::c_void,
        len: usize,
        prot: i32,
        flags: i32,
        fd: i32,
        off: i64,
    ) -> *mut core::ffi::c_void;
}

/// A read-only mmap of the segment. The stride-8 forward scan then reads batch
/// headers as MEMORY LOADS instead of one `pread` per header — which is the
/// only way the "the segment is page-cached anyway" premise can actually be
/// cashed in. `pread` re-enters the kernel per header even on a cache hit.
struct Mapped {
    base: *const u8,
    len: usize,
}

impl Mapped {
    fn new(f: &std::fs::File, len: usize) -> Mapped {
        use std::os::unix::io::AsRawFd;
        // PROT_READ=1, MAP_SHARED=1
        let p = unsafe {
            libc_mmap(std::ptr::null_mut(), len, 1, 1, f.as_raw_fd(), 0)
        };
        assert!(p as isize != -1, "mmap failed");
        Mapped { base: p as *const u8, len }
    }

    /// Drop the mapping. Load-bearing for the COLD row: an active mapping pins
    /// the pages, so `fadvise(DONTNEED)` cannot evict them.
    fn unmap(self) {
        let r = unsafe {
            libc_munmap(self.base as *mut core::ffi::c_void, self.len)
        };
        assert_eq!(r, 0, "munmap failed");
    }

    /// `(first_global_pos, frame_count, total_len)` from the 72-B v3 header at
    /// `off` — same field offsets as [`pread_header`], no syscall.
    fn header(&self, off: u64) -> (u64, u32, u64) {
        let off = off as usize;
        assert!(off + mess_log::format::HEADER_LEN <= self.len);
        let h = unsafe {
            std::slice::from_raw_parts(
                self.base.add(off),
                mess_log::format::HEADER_LEN,
            )
        };
        (
            u64::from_le_bytes(h[28..36].try_into().unwrap()),
            u32::from_le_bytes(h[8..12].try_into().unwrap()),
            u64::from_le_bytes(h[20..28].try_into().unwrap()),
        )
    }
}

fn seek() {
    println!(
        "== Spike F carry-forward: stride-8 global seek on a REAL v3 segment"
    );
    // Shapes chosen so the <=7-header forward scan spans very different byte
    // distances: 7 headers of a 136 B batch live in ONE page; 7 headers of a
    // 26 KB batch span ~45 pages.
    for (batch, payload, events) in [
        (1usize, 24usize, 200_000u64),
        (10, 250, 400_000),
        (100, 250, 1_000_000),
    ] {
        timing::ensure_quiet(900);
        let scratch = scratch_dir();
        let dir = scratch.path().join("store");
        // Write a real corpus through the REAL engine (one big segment).
        let seg = 2u64 * 1024 * 1024 * 1024;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let engine = LogEngine::open_with(
            dir.clone(),
            EngineOptions {
                durability: Durability::Process,
                segment_size: seg,
                ..Default::default()
            },
        )
        .expect("open engine");
        let payload_v = payload_bytes(payload);
        let batches = events / batch as u64;
        rt.block_on(async {
            // 1024 streams round-robin: a realistic global order (batches of
            // different streams interleave, which is exactly why a global
            // seek cannot be answered from the stream index).
            let mut vers: HashMap<u64, Version> = HashMap::new();
            for b in 0..batches {
                let sid = b % 1024;
                let exp = *vers.entry(sid).or_insert(Version::NoStream);
                let records: Vec<RecordToAppend> = (0..batch)
                    .map(|_| RecordToAppend {
                        message_type: "ev.t".to_string(),
                        data: payload_v.clone(),
                    })
                    .collect();
                let out = engine
                    .append_batch(&format!("s{sid}"), exp, &records)
                    .await
                    .expect("append");
                vers.insert(sid, out.version);
            }
        });
        assert_eq!(engine.total_events() as u64, batches * batch as u64);
        drop(engine);
        rt.shutdown_timeout(Duration::from_secs(30));

        // Ground truth from the REAL scanner.
        let seg_path = std::fs::read_dir(&dir)
            .expect("readdir")
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().map(|e| e == "log").unwrap_or(false))
            .min()
            .expect("segment file");
        let fs = mess_log::runtime::real::RealFs;
        let rec = mess_log::scanner::recover_segment(&fs, &seg_path)
            .expect("recover segment");
        let rows: Vec<BatchRow> = rec
            .accepted
            .iter()
            .map(|b| BatchRow {
                offset: b.offset,
                total_len: b.total_len,
                first_global_pos: b.first_global_pos,
                frame_count: b.frame_count,
            })
            .collect();
        assert_eq!(rows.len() as u64, batches, "scanner saw every batch");
        // The forward scan is only sound because batches tile the segment
        // contiguously — assert it rather than assume it.
        for w in rows.windows(2) {
            assert_eq!(
                w[0].offset + w[0].total_len,
                w[1].offset,
                "batches must tile the segment contiguously"
            );
        }
        let seg_bytes = std::fs::metadata(&seg_path).expect("stat").len();
        let bytes_per_batch = seg_bytes as f64 / rows.len() as f64;

        // The stride-8 sparse checkpoint array (design §8.4 / Spike F): every
        // 8th batch's (first_global_pos, offset). This is the ONLY resident
        // global-side state; everything else comes off the segment.
        let ckpt: Vec<(u64, u64)> = rows
            .iter()
            .step_by(8)
            .map(|r| (r.first_global_pos, r.offset))
            .collect();

        let f = std::fs::File::open(&seg_path).expect("open segment");
        let total_events = batches * batch as u64;

        // The seek itself: binary-search the sparse array, then pread batch
        // headers forward until the batch containing `p` is found.
        let seek_one = |p: u64| -> (u64, usize) {
            let i = ckpt.partition_point(|(g, _)| *g <= p) - 1;
            let mut off = ckpt[i].1;
            let mut scanned = 0usize;
            loop {
                let (fg, fc, tl) = pread_header(&f, off);
                scanned += 1;
                if p < fg + u64::from(fc) {
                    return (off, scanned);
                }
                off += tl;
                assert!(scanned <= 8, "stride-8 must resolve within 8 headers");
            }
        };

        // Correctness first (the logical result check): every seek must land
        // on the batch the SCANNER says owns that position.
        let mut x: u64 = 0x243F_6A88_85A3_08D3;
        for _ in 0..2_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let p = x % total_events;
            let (off, _) = seek_one(p);
            let truth = rows
                .iter()
                .find(|r| {
                    p >= r.first_global_pos
                        && p < r.first_global_pos + u64::from(r.frame_count)
                })
                .expect("truth batch");
            assert_eq!(
                off, truth.offset,
                "stride-8 seek landed on wrong batch"
            );
        }

        // WARM (the assumption's own premise: the segment is page-cached).
        let n = 20_000;
        let mut lat: Vec<u64> = Vec::with_capacity(n);
        let mut scans: u64 = 0;
        for _ in 0..n {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let p = x % total_events;
            let t = Instant::now();
            let (_, s) = seek_one(p);
            lat.push(t.elapsed().as_nanos() as u64);
            scans += s as u64;
        }
        lat.sort_unstable();
        let mean_scan = scans as f64 / n as f64;

        // COLD: evict this segment's pages and seek again (the price if the
        // page-cache premise fails — e.g. a cold reader on a big store).
        fadvise_dontneed(&f);
        let ncold = 2_000;
        let mut cold: Vec<u64> = Vec::with_capacity(ncold);
        for _ in 0..ncold {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let p = x % total_events;
            let t = Instant::now();
            let _ = seek_one(p);
            cold.push(t.elapsed().as_nanos() as u64);
        }
        cold.sort_unstable();

        // COLD: evict this segment's pages and seek again (the price if the
        // page-cache premise fails — e.g. a cold reader on a big store).
        fadvise_dontneed(&f);
        let ncold = 2_000;
        let mut cold: Vec<u64> = Vec::with_capacity(ncold);
        for _ in 0..ncold {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let p = x % total_events;
            let t = Instant::now();
            let _ = seek_one(p);
            cold.push(t.elapsed().as_nanos() as u64);
        }
        cold.sort_unstable();

        // WARM, mmap'd: the same stride-8 scan with the segment mapped, so a
        // header read is a memory load rather than a syscall. This is the
        // variant that can actually cash in "the segment is page-cached".
        // Ordering is load-bearing: warm-pread -> warm-mmap -> munmap ->
        // evict -> cold-pread. An active mapping pins the pages, so a COLD row
        // taken while mapped comes back warm; and an mmap row taken right
        // AFTER an evict measures major faults, not warm loads. Both mistakes
        // were made and caught before these numbers were published.
        let map = Mapped::new(&f, seg_bytes as usize);
        let seek_mmap = |p: u64| -> u64 {
            let i = ckpt.partition_point(|(g, _)| *g <= p) - 1;
            let mut off = ckpt[i].1;
            loop {
                let (fg, fc, tl) = map.header(off);
                if p < fg + u64::from(fc) {
                    return off;
                }
                off += tl;
            }
        };
        // Same logical check as the pread path.
        for _ in 0..1_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let p = x % total_events;
            let truth = rows
                .iter()
                .find(|r| {
                    p >= r.first_global_pos
                        && p < r.first_global_pos + u64::from(r.frame_count)
                })
                .expect("truth batch");
            assert_eq!(seek_mmap(p), truth.offset, "mmap seek wrong batch");
        }
        let mut mm: Vec<u64> = Vec::with_capacity(n);
        for _ in 0..n {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let p = x % total_events;
            let t = Instant::now();
            let _ = seek_mmap(p);
            mm.push(t.elapsed().as_nanos() as u64);
        }
        mm.sort_unstable();
        map.unmap();

        // The incumbent (F0): mess-index's REAL ActiveIndex global Vec, fed
        // the same batches, answering the same seeks from memory.
        let active = mess_index::ActiveIndex::new();
        let entries: Vec<mess_index::BatchEntry> = rows
            .iter()
            .map(|r| mess_index::BatchEntry {
                stream_id: 0,
                first_stream_version: 0,
                first_global_pos: r.first_global_pos,
                frame_count: r.frame_count,
                ptr: mess_index::EventPtr { segment_id: 1, offset: r.offset },
            })
            .collect();
        active.apply_committed(total_events, &entries);
        let mut f0: Vec<u64> = Vec::with_capacity(n);
        for _ in 0..n {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let p = x % total_events;
            let t = Instant::now();
            let got = active.global_range(p, 1);
            f0.push(t.elapsed().as_nanos() as u64);
            assert!(!got.is_empty(), "F0 global_range must resolve");
        }
        f0.sort_unstable();

        println!(
            "  batch={batch} payload={payload} batches={} bytes/batch={:.0} \
             seg={:.1}MiB",
            rows.len(),
            bytes_per_batch,
            seg_bytes as f64 / 1048576.0
        );
        println!(
            "    stride-8 WARM   headers/seek {:.2} (max 8)  p50 {} ns  p90 {} \
             ns  p99 {} ns  p99.9 {} ns",
            mean_scan,
            pct(&lat, 0.50),
            pct(&lat, 0.90),
            pct(&lat, 0.99),
            pct(&lat, 0.999)
        );
        println!(
            "    stride-8 WARM mmap  (no syscall/header)      p50 {} ns  p90 {} \
             ns  p99 {} ns  p99.9 {} ns",
            pct(&mm, 0.50),
            pct(&mm, 0.90),
            pct(&mm, 0.99),
            pct(&mm, 0.999)
        );
        println!(
            "    stride-8 COLD   (fadvise DONTNEED)          p50 {} ns  p90 {} \
             ns  p99 {} ns  p99.9 {} ns",
            pct(&cold, 0.50),
            pct(&cold, 0.90),
            pct(&cold, 0.99),
            pct(&cold, 0.999)
        );
        println!(
            "    F0 ActiveIndex  global_range(p,1) in-memory  p50 {} ns  p90 \
             {} ns  p99 {} ns  p99.9 {} ns   resident {} B/batch",
            pct(&f0, 0.50),
            pct(&f0, 0.90),
            pct(&f0, 0.99),
            pct(&f0, 0.999),
            40
        );
    }
}

// ---------------------------------------------------------------------------

fn point(args: &[String]) {
    let kind = parse_engine(&args[0]);
    let d = match args[1].as_str() {
        "process" => Durability::Process,
        "group" => Durability::group_default(),
        other => panic!("unknown mode {other}"),
    };
    let payload: usize = args[2].parse().unwrap();
    let batch: usize = args[3].parse().unwrap();
    let writers: u64 = args[4].parse().unwrap();
    let bpw: u64 = args[5].parse().unwrap();
    let new_name = args.get(6).map(String::as_str) == Some("newname");
    let wl =
        Workload { writers, batch, bpw, payload, new_name, seg_size: None };
    timing::ensure_quiet(900);
    let r = run_one(kind, wl, d);
    println!(
        "point {} {}: {:.0} ev/s {:.3} us/app wall {:.3}s p50 {:.1}us \
         p99 {:.1}us fsyncs {} mean_fsync {:.1}us",
        kind.name(),
        mode_name(d),
        r.ev_s,
        r.per_append_us,
        r.wall_s,
        r.p50_us,
        r.p99_us,
        r.fsyncs,
        r.mean_fsync_us
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("matrix") => matrix(),
        Some("newname") => newname(),
        Some("convoy") => convoy(),
        Some("sealmatrix") => sealmatrix(),
        Some("seek") => seek(),
        Some("point") => point(&args[2..]),
        _ => {
            eprintln!(
                "usage: composed_decision \
                 <matrix|newname|convoy|sealmatrix|seek|point ...>"
            );
            std::process::exit(2);
        }
    }
}
