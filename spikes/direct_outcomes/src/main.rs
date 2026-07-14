//! Reproducible before/after evidence for bn-21ew.
//!
//! `micro` isolates synchronous `DirectCommitter` allocation and owner-thread
//! CPU. `matrix` drives the production `LogEngine` over the four frozen batch
//! sizes in all three durability modes. Both commands append raw CSV rows; use
//! a fresh output path for each phase.

mod alloc_count;
#[path = "../../baseline_matrix/src/timing.rs"]
mod timing;

use std::hint::black_box;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloc_count::CountingAlloc;
use mess_log::committer::{
    AppendRequest, ChainInit, DirectAppendRequest, DirectCommitter, Durability,
    EventInput, Roller,
};
use mess_log::runtime::{RealRuntime, Runtime};
use mess_log::writer::{SegmentParams, SegmentWriter};
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

const PAYLOAD: usize = 24;
const WRITERS: u64 = 4;
const MICRO_APPENDS: usize = 256;

#[derive(Clone, Copy)]
struct Workload {
    batch: usize,
    bpw:   u64,
}

impl Workload {
    fn events(self) -> u64 { WRITERS * self.bpw * self.batch as u64 }

    fn segment_size(self) -> u64 {
        self.events()
            .saturating_mul((PAYLOAD + 112) as u64)
            .saturating_mul(2)
            .saturating_add(1 << 20)
            .max(256 << 20)
            .next_power_of_two()
    }
}

#[derive(Clone, Copy)]
struct Run {
    ev_s:          f64,
    wall_s:        f64,
    p50_us:        f64,
    p99_us:        f64,
    fsyncs:        u64,
    fsync_p50_us:  f64,
    fsync_p95_us:  f64,
    fsync_p99_us:  f64,
    fsync_max_us:  f64,
    mean_fsync_us: f64,
}

fn payload() -> Vec<u8> {
    (0..PAYLOAD as u32).map(|i| (i & 0xFF) as u8).collect()
}

fn scratch(prefix: &str) -> mess_testkit::SweepingTempDir {
    let root = std::env::var_os("MESS_BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var("HOME").expect("HOME"))
                .join(".cache/mess-bench")
        });
    std::fs::create_dir_all(&root).expect("create benchmark scratch");
    mess_testkit::temp_dir_in(&root, prefix)
}

fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    let rc =
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    assert_eq!(rc, 0, "clock_gettime(CLOCK_THREAD_CPUTIME_ID)");
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn mode_name(d: Durability) -> &'static str {
    match d {
        Durability::Process => "process",
        Durability::Os => "os",
        Durability::Group { .. } => "group",
    }
}

fn pct(sorted: &[u64], q: f64) -> u64 {
    let i = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

fn latency_stats(per_writer: Vec<Vec<u64>>) -> (f64, f64) {
    let mut all = Vec::new();
    for lats in per_writer {
        let skip = lats.len() / 10;
        all.extend_from_slice(&lats[skip..]);
    }
    all.sort_unstable();
    (pct(&all, 0.50) as f64 / 1e3, pct(&all, 0.99) as f64 / 1e3)
}

fn open_csv(path: &Path, header: &str) -> std::fs::File {
    let fresh = !path.exists();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open evidence CSV");
    if fresh {
        writeln!(file, "{header}").expect("write CSV header");
    }
    file
}

fn make_units(
    batch: usize,
    first_version: u64,
) -> Vec<Vec<DirectAppendRequest>> {
    let bytes = payload();
    (0..MICRO_APPENDS)
        .map(|i| {
            vec![DirectAppendRequest::Inputs(AppendRequest {
                stream_id:            i as u64 + 1,
                category_id:          0,
                first_stream_version: first_version,
                events:               (0..batch)
                    .map(|_| EventInput::plain(1, 0, 0, bytes.clone()))
                    .collect(),
            })]
        })
        .collect()
}

fn micro_batches(phase: &str, output: &Path, batches: &[usize]) {
    let mut csv = open_csv(
        output,
        "phase,batch,rep,appends,alloc_calls,alloc_bytes,\
         alloc_calls_per_append,alloc_bytes_per_append,\
         thread_cpu_ns_per_append,wall_ns_per_append,events_per_s,fsyncs",
    );
    for &batch in batches {
        for rep in 0..3 {
            timing::ensure_quiet(600);
            let dir = scratch("direct-outcomes-micro");
            let rt = RealRuntime::new();
            let fs = rt.fs();
            let path = dir.path().join("segment-1.log");
            let mut params = SegmentParams::new(1, 0, 1, 0);
            params.segment_size =
                ((MICRO_APPENDS * batch * (PAYLOAD + 112) * 3) as u64)
                    .max(256 << 20)
                    .next_power_of_two();
            let writer = SegmentWriter::create(&fs, &path, params).unwrap();
            let roll_root = Arc::new(dir.path().to_path_buf());
            let roll_for = Arc::clone(&roll_root);
            let (roll_tx, _roll_rx) = std::sync::mpsc::channel();
            let roller = Roller::new(
                move |id| roll_for.join(format!("segment-{id}.log")),
                roll_tx,
            );
            let mut direct = DirectCommitter::with_roll_chained(
                &rt,
                writer,
                Durability::Process,
                roller,
                ChainInit::off(),
            );

            // One unmeasured group establishes encoder and filesystem state.
            let warm = make_units(batch, 0);
            direct
                .commit_ordered_group(warm, |_, _, outcome| {
                    black_box(outcome);
                })
                .unwrap();
            let units = make_units(batch, batch as u64);
            let alloc0 = alloc_count::snapshot();
            let cpu0 = thread_cpu_ns();
            let wall0 = Instant::now();
            let mut completed = 0usize;
            direct
                .commit_ordered_group(units, |_, _, outcome| {
                    completed += 1;
                    black_box(outcome);
                })
                .unwrap();
            let wall_ns = wall0.elapsed().as_nanos() as u64;
            let cpu_ns = thread_cpu_ns() - cpu0;
            let alloc = alloc_count::delta(alloc0, alloc_count::snapshot());
            assert_eq!(completed, MICRO_APPENDS);
            let metrics = direct.metrics();
            let n = MICRO_APPENDS as f64;
            let events = (MICRO_APPENDS * batch) as f64;
            writeln!(
                csv,
                "{phase},{batch},{rep},{MICRO_APPENDS},{},{},{:.6},{:.3},{:.\
                 3},{:.3},{:.0},{}",
                alloc.calls,
                alloc.bytes,
                alloc.calls as f64 / n,
                alloc.bytes as f64 / n,
                cpu_ns as f64 / n,
                wall_ns as f64 / n,
                events * 1e9 / wall_ns as f64,
                metrics.fsync.count,
            )
            .unwrap();
            println!(
                "micro {phase} b{batch} r{rep}: {:.3} allocs / {:.1} B / \
                 {:.0} cpu-ns per append",
                alloc.calls as f64 / n,
                alloc.bytes as f64 / n,
                cpu_ns as f64 / n,
            );
        }
    }
}

fn micro(phase: &str, output: &Path) {
    micro_batches(phase, output, &[1, 10, 100, 1000]);
}

fn micro100(phase: &str, output: &Path) {
    micro_batches(phase, output, &[100]);
}

fn run_log(wl: Workload, durability: Durability) -> Run {
    let dir = scratch("direct-outcomes-matrix");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WRITERS as usize)
        .enable_all()
        .build()
        .unwrap();
    let engine = LogEngine::open_with(
        dir.path().join("store"),
        EngineOptions {
            durability,
            segment_size: wl.segment_size(),
            ..Default::default()
        },
    )
    .unwrap();
    let bytes = payload();
    let start = Instant::now();
    let per_writer: Vec<Vec<u64>> = rt.block_on(async {
        let mut joins = Vec::new();
        for writer in 0..WRITERS {
            let engine = engine.clone();
            let bytes = bytes.clone();
            joins.push(tokio::spawn(async move {
                let mut version = Version::NoStream;
                let mut lats = Vec::with_capacity(wl.bpw as usize);
                for _ in 0..wl.bpw {
                    let records: Vec<RecordToAppend> = (0..wl.batch)
                        .map(|_| RecordToAppend {
                            message_type: "ev.t".to_owned(),
                            data:         bytes.clone(),
                        })
                        .collect();
                    let t0 = Instant::now();
                    let appended = engine
                        .append_batch(&format!("s{writer}"), version, &records)
                        .await
                        .unwrap();
                    lats.push(t0.elapsed().as_nanos() as u64);
                    version = appended.version;
                }
                lats
            }));
        }
        let mut all = Vec::new();
        for join in joins {
            all.push(join.await.unwrap());
        }
        all
    });
    let wall_s = start.elapsed().as_secs_f64();
    assert_eq!(
        engine.total_events() as u64,
        wl.events() + WRITERS + 1,
        "domain + stable stream registrations + event type"
    );
    let metrics = engine.metrics();
    let fsyncs = metrics.commit.fsync.count;
    let fsync = metrics.commit.fsync;
    drop(engine);
    rt.shutdown_timeout(Duration::from_secs(10));
    let (p50_us, p99_us) = latency_stats(per_writer);
    Run {
        ev_s: wl.events() as f64 / wall_s,
        wall_s,
        p50_us,
        p99_us,
        fsyncs,
        fsync_p50_us: fsync.p50_nanos as f64 / 1e3,
        fsync_p95_us: fsync.p95_nanos as f64 / 1e3,
        fsync_p99_us: fsync.p99_nanos as f64 / 1e3,
        fsync_max_us: fsync.max_nanos as f64 / 1e3,
        mean_fsync_us: fsync.mean_nanos as f64 / 1e3,
    }
}

fn bpw(batch: usize, durability: Durability) -> u64 {
    match durability {
        Durability::Process => match batch {
            1 => 40_000,
            10 => 12_500,
            100 => 2_500,
            _ => 250,
        },
        Durability::Os => match batch {
            1 => 40,
            10 => 30,
            100 => 20,
            _ => 10,
        },
        Durability::Group { .. } => match batch {
            1 => 800,
            10 => 500,
            100 => 300,
            _ => 100,
        },
    }
}

fn matrix_modes(
    phase: &str,
    output: &Path,
    modes: &[Durability],
    batches: &[usize],
) {
    let mut csv = open_csv(
        output,
        "phase,mode,payload,batch,writers,bpw,rep,ev_s,wall_s,p50_us,p99_us,\
         fsyncs,mean_fsync_us,load1",
    );
    for &durability in modes {
        for &batch in batches {
            let wl = Workload { batch, bpw: bpw(batch, durability) };
            for rep in 0..3 {
                timing::ensure_quiet(600);
                let run = run_log(wl, durability);
                let load1 = std::fs::read_to_string("/proc/loadavg")
                    .unwrap_or_default()
                    .split_whitespace()
                    .next()
                    .unwrap_or("0")
                    .parse::<f64>()
                    .unwrap_or(0.0);
                writeln!(
                    csv,
                    "{phase},{},{PAYLOAD},{batch},{WRITERS},{},{rep},{:.0},{:.\
                     6},{:.3},{:.3},{},{:.3},{load1:.2}",
                    mode_name(durability),
                    wl.bpw,
                    run.ev_s,
                    run.wall_s,
                    run.p50_us,
                    run.p99_us,
                    run.fsyncs,
                    run.mean_fsync_us,
                )
                .unwrap();
                println!(
                    "matrix {phase} {} b{batch} r{rep}: {:.0} ev/s p99 {:.1} \
                     us fsyncs {}",
                    mode_name(durability),
                    run.ev_s,
                    run.p99_us,
                    run.fsyncs,
                );
            }
        }
    }
}

fn matrix(phase: &str, output: &Path) {
    matrix_modes(
        phase,
        output,
        &[Durability::Process, Durability::Os, Durability::group_default()],
        &[1, 10, 100, 1000],
    );
}

fn process_diagnostic(phase: &str, output: &Path) {
    matrix_modes(phase, output, &[Durability::Process], &[10, 100]);
}

fn os_variance(phase: &str, output: &Path) {
    let mut csv = open_csv(
        output,
        "phase,batch,writers,bpw,ev_s,wall_s,append_p50_us,append_p99_us,\
         fsyncs,fsync_p50_us,fsync_p95_us,fsync_p99_us,fsync_max_us,\
         fsync_mean_us,load1",
    );
    for batch in [1usize, 10] {
        timing::ensure_quiet(600);
        let wl = Workload { batch, bpw: bpw(batch, Durability::Os) };
        let run = run_log(wl, Durability::Os);
        let load1 = timing::load1();
        writeln!(
            csv,
            "{phase},{batch},{WRITERS},{},{:.0},{:.6},{:.3},{:.3},{},{:.3},\
             {:.3},{:.3},{:.3},{:.3},{load1:.2}",
            wl.bpw,
            run.ev_s,
            run.wall_s,
            run.p50_us,
            run.p99_us,
            run.fsyncs,
            run.fsync_p50_us,
            run.fsync_p95_us,
            run.fsync_p99_us,
            run.fsync_max_us,
            run.mean_fsync_us,
        )
        .unwrap();
        println!(
            "variance {phase} os b{batch}: append-p99 {:.1} us fsync-p99 {:.1} \
             us fsyncs {}",
            run.p99_us, run.fsync_p99_us, run.fsyncs,
        );
    }
}

/// Run one observation of each Group cell admitted by the targeted barrier
/// parity study. Keeping the cells in one invocation makes the ABBA command
/// sequence compact; the quiet guard still runs independently before each
/// observation.
fn group_parity(phase: &str, output: &Path) {
    let mut csv = open_csv(
        output,
        "phase,batch,writers,bpw,ev_s,wall_s,append_p50_us,append_p99_us,\
         barriers,mean_fsync_us,load1",
    );
    for batch in [1usize, 10] {
        timing::ensure_quiet(600);
        let durability = Durability::group_default();
        let wl = Workload { batch, bpw: bpw(batch, durability) };
        let run = run_log(wl, durability);
        let load1 = timing::load1();
        writeln!(
            csv,
            "{phase},{batch},{WRITERS},{},{:.0},{:.6},{:.3},{:.3},{},{:.3},\
             {load1:.2}",
            wl.bpw,
            run.ev_s,
            run.wall_s,
            run.p50_us,
            run.p99_us,
            run.fsyncs,
            run.mean_fsync_us,
        )
        .unwrap();
        println!(
            "group-parity {phase} b{batch}: {:.0} ev/s p99 {:.1} us barriers \
             {}",
            run.ev_s, run.p99_us, run.fsyncs,
        );
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (Some(command), Some(phase), Some(output)) =
        (args.get(1), args.get(2), args.get(3))
    else {
        eprintln!(
            "usage: direct_outcomes \
             <micro|micro100|matrix|process|os-variance|group-parity> \
             <phase> <output.csv>"
        );
        std::process::exit(2);
    };
    let output = Path::new(output);
    match command.as_str() {
        "micro" => micro(phase, output),
        "micro100" => micro100(phase, output),
        "matrix" => matrix(phase, output),
        "process" => process_diagnostic(phase, output),
        "os-variance" => os_variance(phase, output),
        "group-parity" => group_parity(phase, output),
        other => panic!("unknown command {other}"),
    }
}
