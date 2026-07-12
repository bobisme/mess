//! `seal_pack_bench` (bn-3of, Spike I): measures the consolidated SealPack
//! against the legacy `.pidx`/`.filter`/`.pcol` sidecar trio on the REAL
//! composed engine (`mess-store`), toggled by `EngineOptions::seal_pack`.
//!
//! Subcommands (each runs the quiet guard before its measured phase):
//!
//! ```text
//! seal_pack_bench seed    <pack|sidecar> <dir> <events>   # seed + seal, print sizes/seal time
//! seal_pack_bench reopen  <pack|sidecar> <dir>            # one reopen (strace this for open syscalls)
//! seal_pack_bench coldread <pack|sidecar> <dir> <n>       # time n cold message_type reads
//! ```
//!
//! Quiet guard: waits for `load1 < 6`, no sibling `capsule_v4_bench`, and no
//! compiler process, then staggers politely — so a concurrent spike bench is
//! never measured against.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use mess_log::committer::Durability;
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};

fn read_load1() -> f64 {
    std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|s| s.split_whitespace().next().map(str::to_string))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0)
}

fn pgrep(name: &str) -> bool {
    std::process::Command::new("pgrep")
        .arg("-x")
        .arg(name)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Wait for a quiet machine: load1 < 6, no sibling `capsule_v4_bench`, and no
/// compiler running, then stagger politely. Bounded: after
/// `SEAL_PACK_BENCH_GUARD_SECS` (default 90s) it proceeds anyway with a loud
/// note, so a persistently busy machine (a concurrent sibling suite) never
/// hangs the bench — timing numbers taken then carry a "machine-busy" caveat,
/// while the load-independent measurements (strace syscall counts, file sizes)
/// stand regardless.
fn quiet_guard() {
    let compilers = ["cc1", "cc1plus", "rustc", "clang", "clang++", "lld"];
    let budget = std::env::var("SEAL_PACK_BENCH_GUARD_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(90u64);
    let deadline = Instant::now() + Duration::from_secs(budget);
    loop {
        let load = read_load1();
        let sibling = pgrep("capsule_v4_bench");
        let compiling = compilers.iter().any(|c| pgrep(c));
        if load < 6.0 && !sibling && !compiling {
            break;
        }
        if Instant::now() >= deadline {
            eprintln!(
                "quiet-guard: BUDGET EXPIRED, proceeding NOT-QUIET \
                 (load1={load:.2} sibling={sibling} compiling={compiling}) \
                 — timing numbers are contended"
            );
            break;
        }
        eprintln!(
            "quiet-guard: waiting (load1={load:.2} sibling={sibling} \
             compiling={compiling})"
        );
        std::thread::sleep(Duration::from_secs(2));
    }
    std::thread::sleep(Duration::from_millis(250));
}

fn opts(pack: bool) -> EngineOptions {
    EngineOptions {
        durability: Durability::Process,
        segment_size: 32 * 1024, // small -> many rolls -> many sealed segments
        seal_pack: pack,
        ..Default::default()
    }
}

fn payload(i: u64) -> Vec<u8> {
    if i.is_multiple_of(2) {
        let mut m = vec![0x82, 0xA3];
        m.extend_from_slice(b"seq");
        m.push(0xCF);
        m.extend_from_slice(&i.to_be_bytes());
        m.push(0xA4);
        m.extend_from_slice(b"kind");
        m.push(0xA4);
        m.extend_from_slice(b"demo");
        m
    } else {
        let mut v = vec![0xFF, 0x00];
        v.extend_from_slice(&i.to_le_bytes());
        v.extend(std::iter::repeat_n((i % 251) as u8, (i % 29) as usize));
        v
    }
}

fn mtype(i: u64) -> String {
    match i % 4 {
        0 => "acct.opened",
        1 => "acct.deposited",
        2 => "acct.withdrawn",
        _ => "acct.closed",
    }
    .to_string()
}

async fn seed(engine: &LogEngine, streams: usize, total: u64, per: u64) {
    let mut heads = vec![Version::NoStream; streams];
    let mut gp = 0u64;
    let batches = total / per;
    for b in 0..batches {
        let s = (b as usize) % streams;
        let name = format!("acct-{s:04}");
        let recs: Vec<RecordToAppend> = (0..per)
            .map(|k| RecordToAppend {
                message_type: mtype(gp + k),
                data:         payload(gp + k),
            })
            .collect();
        let out =
            engine.append_batch(&name, heads[s], &recs).await.expect("append");
        heads[s] = out.version;
        gp += per;
    }
}

fn await_seals(engine: &LogEngine, want: usize) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while engine.sealed_segment_count() < want {
        assert!(Instant::now() < deadline, "sealer stalled");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn dir_bytes(dir: &Path) -> (u64, usize) {
    let mut bytes = 0u64;
    let mut files = 0usize;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            if let Ok(m) = e.metadata() {
                if m.is_file() {
                    bytes += m.len();
                    files += 1;
                }
            }
        }
    }
    (bytes, files)
}

fn cmd_seed(pack: bool, dir: &Path, events: u64) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let engine = LogEngine::open_with(dir, opts(pack)).expect("open");
    quiet_guard();
    let t0 = Instant::now();
    rt.block_on(seed(&engine, 16, events, 5));
    // Wait for a healthy number of seals so seal timing is meaningful.
    await_seals(&engine, 20);
    let wall = t0.elapsed();
    let m = engine.metrics();
    let sealed = dir.join("sealed");
    let (sbytes, sfiles) = dir_bytes(&sealed);
    println!("mode={}", if pack { "pack" } else { "sidecar" });
    println!("events={events} seal_wall_incl_append_ms={:.1}", wall.as_secs_f64() * 1e3);
    println!("seals={} seal_duration_mean_us={:.2} seal_duration_p99_us={:.2}",
        m.seals,
        m.seal_duration.mean_nanos as f64 / 1e3,
        m.seal_duration.p99_nanos as f64 / 1e3,
    );
    println!("sealed_dir_bytes={sbytes} sealed_dir_files={sfiles}");
    println!("sealed_segments={}", engine.sealed_segment_count());
    drop(engine);
}

fn cmd_reopen(pack: bool, dir: &Path) {
    quiet_guard();
    let t0 = Instant::now();
    let engine = LogEngine::open_with(dir, opts(pack)).expect("reopen");
    let wall = t0.elapsed();
    println!("mode={} reopen_ms={:.2} total_events={} sealed_segments={} recover_payload_decodes={}",
        if pack { "pack" } else { "sidecar" },
        wall.as_secs_f64() * 1e3,
        engine.total_events(),
        engine.sealed_segment_count(),
        engine.recover_payload_decodes(),
    );
    drop(engine);
}

fn cmd_coldread(pack: bool, dir: &Path, n: u64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let engine = LogEngine::open_with(dir, opts(pack)).expect("reopen");
    let total = engine.total_events() as u64;
    quiet_guard();
    // Cold point reads scattered across the (sealed) history: read one record
    // per stream at a pseudo-random early version, forcing sealed-tier reads.
    let mut samples: Vec<Duration> = Vec::with_capacity(n as usize);
    let mut acc = 0u64; // consume message_type so the read is not optimized out
    rt.block_on(async {
        for i in 0..n {
            let s = (i * 2654435761 % 16) as usize;
            let name = format!("acct-{s:04}");
            let v = (i * 40503) % (total / 16).max(1);
            let t0 = Instant::now();
            let recs = engine
                .read_stream(&name, Version::At(v.saturating_sub(1)), 1)
                .await
                .expect("read");
            samples.push(t0.elapsed());
            acc += recs.first().map(|r| r.message_type.len() as u64).unwrap_or(0);
        }
    });
    samples.sort_unstable();
    let p = |q: f64| samples[((samples.len() as f64 * q) as usize).min(samples.len() - 1)];
    println!("mode={} coldreads={} p50_us={:.2} p99_us={:.2} max_us={:.2} sink={}",
        if pack { "pack" } else { "sidecar" },
        n,
        p(0.50).as_secs_f64() * 1e6,
        p(0.99).as_secs_f64() * 1e6,
        p(0.999).as_secs_f64() * 1e6,
        acc,
    );
    drop(engine);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!(
            "usage: seal_pack_bench <seed|reopen|coldread> <pack|sidecar> <dir> [n]"
        );
        std::process::exit(2);
    }
    let sub = args[1].as_str();
    let pack = match args[2].as_str() {
        "pack" => true,
        "sidecar" => false,
        other => panic!("mode must be pack|sidecar, got {other}"),
    };
    let dir = PathBuf::from(&args[3]);
    let n: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(10_000);
    match sub {
        "seed" => cmd_seed(pack, &dir, n),
        "reopen" => cmd_reopen(pack, &dir),
        "coldread" => cmd_coldread(pack, &dir, n),
        other => panic!("unknown subcommand {other}"),
    }
}
