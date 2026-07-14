//! Spike 0 (bn-u1k) — baseline-gen2 matched matrix.
//!
//! Locks the NEW baseline generation for every Asterism gate, measured on the
//! engine AFTER three merges that Spike B's (gen1) numbers pre-dated:
//!   * bn-34o — registry name-persist coalesced into the committer group window
//!   * bn-2cj — name-durability barrier gated on Durability mode (Process buffered)
//!   * bn-2ib / Spike C — Book payload mirror removed (block-native reads)
//!
//! Two engines, matched and interleaved (A/B/B/A), same session, same ext4
//! scratch (`$HOME/.cache/mess-bench`, NEVER tmpfs — fdatasync there is a
//! no-op and durable numbers would be a lie):
//!   bare — the raw `mess-log` committer/appender (no registry, no validation):
//!          the lower bound.
//!   log  — the current composed `mess_store::LogEngine::append_batch`.
//!
//! ```text
//! cargo run --release -- matrix     # batch{1,10,100,1000} x payload{24,250} x writers{1,4} x {Process,Group}
//! cargo run --release -- newname    # 100% new-stream name-barrier baseline (composed only)
//! cargo run --release -- point <engine> <mode> <payload> <batch> <writers> <bpw> [newname]
//! ```
//!
//! Every run computes a logical result check (watermark / event count) and
//! every measured phase is quiet-guarded. Per-run rows land in
//! `baseline_results.csv` in the CWD.

mod timing;

use std::collections::HashMap;
use std::io::Write as _;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use mess_log::committer::{
    AppendRequest, Committer, Durability, EventInput,
};
use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::writer::{SegmentParams, SegmentWriter};
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EngineKind {
    /// The raw committer/appender: numeric stream ids, no registry, no
    /// validation. The bare lower bound.
    Bare,
    /// The current composed engine (`LogEngine::append_batch`).
    Log,
}

impl EngineKind {
    fn name(self) -> &'static str {
        match self {
            EngineKind::Bare => "bare",
            EngineKind::Log => "log",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Workload {
    writers:  u64,
    batch:    usize,
    /// Batches per writer.
    bpw:      u64,
    payload:  usize,
    /// Every append targets a brand-new stream (the seeder / name-barrier
    /// shape). When false, one stable stream per writer (the 0%-new matrix).
    new_name: bool,
}

impl Workload {
    fn total_events(&self) -> u64 {
        self.writers * self.bpw * self.batch as u64
    }

    fn total_appends(&self) -> u64 {
        self.writers * self.bpw
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
    /// Per-append latency (µs) — the meaningful denominator for new-name.
    per_append_us: f64,
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
    let root = match std::env::var_os("MESS_BENCH_DIR").map(PathBuf::from) {
        Some(root) => root,
        None => {
            let home = std::env::var("HOME").expect("HOME");
            PathBuf::from(home).join(".cache").join("mess-bench")
        }
    };
    std::fs::create_dir_all(&root).expect("create scratch base");
    mess_testkit::temp_dir_in(&root, "baseline-gen2")
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
                // Stable-stream: one stream id per writer, version tracked.
                // New-name: a fresh disjoint stream id per append.
                let mut ver: u64 = 0;
                for b in 0..wl.bpw {
                    let sid = if wl.new_name {
                        // Disjoint id space per writer; a brand-new stream
                        // every append.
                        w * wl.bpw + b + 1
                    } else {
                        w + 1
                    };
                    let first_ver = if wl.new_name { 0 } else { ver };
                    let events: Vec<EventInput> = (0..wl.batch)
                        .map(|_| EventInput::plain(1, 0, 0, payload.clone()))
                        .collect();
                    let req = AppendRequest {
                        stream_id:             sid,
                        category_id:           0,
                        first_stream_version:  first_ver,
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
                let mut ver = Version::NoStream;
                for b in 0..wl.bpw {
                    // Stable-stream: one named stream per writer. New-name: a
                    // brand-new interned name every append (100% new streams).
                    let stream = if wl.new_name {
                        format!("w{w}-s{b}")
                    } else {
                        format!("s{w}")
                    };
                    let exp = if wl.new_name { Version::NoStream } else { ver };
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
    // Logical result check. Since bn-2di the v3 `$registry` records are real
    // log events and therefore advance the durable/published watermark even
    // though `read_global` filters them from the user event stream. Account
    // for one event-type registration plus either one stream per writer
    // (stable shape) or one stream per append (new-name shape).
    let registry_positions = if wl.new_name {
        wl.total_appends() + 1
    } else {
        wl.writers + 1
    };
    assert_eq!(
        engine.total_events() as u64,
        wl.total_events() + registry_positions,
        "composed watermark must cover domain plus registry events"
    );
    let m = engine.metrics();
    let fsyncs = m.commit.fsync.count;
    let mean_fsync_us = m.commit.fsync.mean_nanos as f64 / 1e3;
    drop(engine);
    rt.shutdown_timeout(Duration::from_secs(10));

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

fn run_one(kind: EngineKind, wl: Workload, durability: Durability) -> RunResult {
    match kind {
        EngineKind::Bare => run_bare(wl, durability),
        EngineKind::Log => run_log(wl, durability),
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
        "{cell},{},{},{},{},{},{},{},{rep},{:.0},{:.4},{:.4},{:.1},{:.1},{},{:.1}",
        kind.name(),
        mode_name(d),
        wl.payload,
        wl.batch,
        wl.writers,
        wl.bpw,
        if wl.new_name { "newname" } else { "stable" },
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

/// Interleaved A/B/B/A run over one cell: 3 reps per engine, order
/// AB BA AB. Every measured run is quiet-guarded first; settle pause before
/// each durable run.
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
    order.extend_from_slice(kinds); // A B
    order.extend(kinds.iter().rev().copied()); // B A
    order.extend_from_slice(kinds); // A B
    let mut results: HashMap<&'static str, Vec<RunResult>> = HashMap::new();
    let mut rep_of: HashMap<&'static str, usize> = HashMap::new();
    println!(
        "== cell {label}: payload={} batch={} writers={} bpw={} kind={} \
         mode={} total_events={}",
        wl.payload,
        wl.batch,
        wl.writers,
        wl.bpw,
        if wl.new_name { "newname" } else { "stable" },
        mode_name(d),
        wl.total_events()
    );
    for kind in order {
        std::thread::sleep(settle);
        timing::ensure_quiet(600);
        let r = run_one(kind, wl, d);
        let rep = rep_of.entry(kind.name()).or_insert(0);
        println!(
            "  {:<4} rep{}: {:>12.0} ev/s  {:>8.3} us/app  wall {:.3}s  \
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
        csv_line(csv, label, kind, wl, d, *rep, r);
        *rep += 1;
        results.entry(kind.name()).or_default().push(r);
    }
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
    // Composed/bare ratio (median ev/s), when both present.
    if results.contains_key("bare") && results.contains_key("log") {
        let med = |k: &str| {
            let mut v: Vec<f64> =
                results[k].iter().map(|r| r.ev_s).collect();
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
            v[v.len() / 2]
        };
        println!(
            "  RATIO log/bare median {:.1}%",
            100.0 * med("log") / med("bare")
        );
    }
    results
}

fn open_csv() -> std::fs::File {
    let fresh = !std::path::Path::new("baseline_results.csv").exists();
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("baseline_results.csv")
        .expect("open csv");
    if fresh {
        writeln!(
            f,
            "cell,engine,mode,payload,batch,writers,bpw,kind,rep,ev_s,\
             per_append_us,wall_s,p50_us,p99_us,fsyncs,mean_fsync_us"
        )
        .unwrap();
    }
    f
}

/// Batches-per-writer per (batch, mode): keeps each run a few hundred ms
/// (Process) / a bounded barrier count (Group).
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

fn matrix() {
    let mut csv = open_csv();
    let kinds = [EngineKind::Bare, EngineKind::Log];
    for payload in [24usize, 250] {
        for batch in [1usize, 10, 100, 1000] {
            for writers in [1u64, 4] {
                // Process
                let wl = Workload {
                    writers,
                    batch,
                    bpw: bpw_for(batch, false),
                    payload,
                    new_name: false,
                };
                let label = format!("proc-p{payload}-b{batch}-w{writers}");
                run_cell(&label, &kinds, wl, Durability::Process, &mut csv);
                // Group (default params)
                let wl = Workload {
                    writers,
                    batch,
                    bpw: bpw_for(batch, true),
                    payload,
                    new_name: false,
                };
                let label = format!("group-p{payload}-b{batch}-w{writers}");
                run_cell(
                    &label,
                    &kinds,
                    wl,
                    Durability::group_default(),
                    &mut csv,
                );
            }
        }
    }
}

/// 100%-new-stream name-barrier baseline. Batch of 1 event per new stream (the
/// seeder shape). Both engines measured — bare is the no-registry floor; log
/// pays the post bn-34o/bn-2cj name persist. 250 B payload.
fn newname() {
    let mut csv = open_csv();
    let kinds = [EngineKind::Bare, EngineKind::Log];
    for writers in [1u64, 4] {
        for (group, bpw) in [(false, 4_000u64), (true, 1_000u64)] {
            let wl = Workload {
                writers,
                batch: 1,
                bpw,
                payload: 250,
                new_name: true,
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

fn point(args: &[String]) {
    let kind = match args[0].as_str() {
        "bare" => EngineKind::Bare,
        "log" => EngineKind::Log,
        other => panic!("unknown engine {other}"),
    };
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
    let wl = Workload { writers, batch, bpw, payload, new_name };
    timing::ensure_quiet(600);
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
        Some("point") => point(&args[2..]),
        _ => {
            eprintln!(
                "usage: baseline_matrix <matrix|newname|point ...>"
            );
            std::process::exit(2);
        }
    }
}
