//! perf_append spike: profile-guided optimization of the composed Engine A
//! append path (custom segment log + pointer index) until it beats the
//! RocksDB baseline (532k ev/s buffered) at its own game.
//!
//! Workload (identical to spikes/vertical_slice): 1M events, ~250 B JSON
//! payloads, 10k streams Zipf(1.1), batches of 10, BUFFERED durability.
//!
//! Subcommands:
//!   all                                  full progression + gates (default)
//!   bench <variant> <writers> [batch] [events]   one load, minimal output
//!                                        (wrap with `perf stat` / `perf record`)
//!   gen-only [batch] [events]            workload generation only (perf-stat
//!                                        counter subtraction)
//!   ceiling                              single-thread log ceiling + CRC bakeoff
//!
//! Variants: baseline-tokio baseline logonly mutex-batched mutex-inmem
//!           actor-naive actor-group actor-inmem reserve rocks

mod alloc_count;
mod engine_actor;
mod engine_mutex;
mod engine_reserve;
mod engine_rocks;
mod seglog;
mod verify;
mod workload;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloc_count::CountingAlloc;
use engine_actor::{ActorEngine, ActorIndex, WriteMode};
use engine_mutex::{IndexMode, MutexEngine};
use engine_reserve::ReserveEngine;
use engine_rocks::RocksEngine;
use verify::{expected_from, fnv_via_ptrs, sample_streams, verify_log, verify_stream_reads, Expected};
use workload::BatchSpec;

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

const EVENTS: usize = 1_000_000;
const BATCH: usize = 10;
const SAMPLE_STREAMS: usize = 200;

// ---------------------------------------------------------------- helpers --

fn file_alloc(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    fs::metadata(path).map(|m| m.blocks() * 512).unwrap_or(0)
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0;
    if path.is_file() {
        return file_alloc(path);
    }
    if let Ok(entries) = fs::read_dir(path) {
        for e in entries.flatten() {
            let p = e.path();
            total += if p.is_dir() { dir_size(&p) } else { file_alloc(&p) };
        }
    }
    total
}

fn fmt_bytes(b: u64) -> String {
    if b >= 1 << 30 {
        format!("{:.2} GiB", b as f64 / (1u64 << 30) as f64)
    } else if b >= 1 << 20 {
        format!("{:.2} MiB", b as f64 / (1u64 << 20) as f64)
    } else {
        format!("{:.1} KiB", b as f64 / 1024.0)
    }
}

fn pct(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

fn us(ns: u64) -> f64 {
    ns as f64 / 1_000.0
}

#[derive(Clone)]
struct RunStats {
    name: String,
    writers: usize,
    events: usize,
    wall: Duration,
    p50: u64,
    p95: u64,
    p99: u64,
    allocs_per_ev: f64,
    disk: u64,
    disk_detail: String,
}

impl RunStats {
    fn evps(&self) -> f64 {
        self.events as f64 / self.wall.as_secs_f64()
    }
    fn print(&self) {
        println!(
            "  {:<14} w={:<2} {:>8.2}s  {:>9.0} ev/s  p50 {:>7.1}us p95 {:>7.1}us p99 {:>7.1}us  alloc/ev {:>5.2}  disk {:>10} {}",
            self.name,
            self.writers,
            self.wall.as_secs_f64(),
            self.evps(),
            us(self.p50),
            us(self.p95),
            us(self.p99),
            self.allocs_per_ev,
            fmt_bytes(self.disk),
            self.disk_detail,
        );
    }
}

// ----------------------------------------------------------------- drivers --

/// Drive a shared-engine (&self) append fn from `writers` OS threads.
fn drive_shared<F>(master: &Arc<Vec<BatchSpec>>, writers: usize, append: F) -> (Duration, Vec<u64>)
where
    F: Fn(&BatchSpec) + Sync,
{
    let parts = workload::partition(master, writers);
    let t0 = Instant::now();
    let lats = std::thread::scope(|s| {
        let mut joins = Vec::new();
        for part in &parts {
            let append = &append;
            let master = master.clone();
            joins.push(s.spawn(move || {
                let mut lat = Vec::with_capacity(part.len());
                for &i in part {
                    let b = &master[i as usize];
                    let t = Instant::now();
                    append(b);
                    lat.push(t.elapsed().as_nanos() as u64);
                }
                lat
            }));
        }
        let mut all = Vec::new();
        for j in joins {
            all.extend(j.join().unwrap());
        }
        all
    });
    (t0.elapsed(), lats)
}

/// Drive per-writer handles (owned, &mut self) from OS threads.
fn drive_handles<H, F>(
    master: &Arc<Vec<BatchSpec>>,
    handles: Vec<H>,
    append: F,
) -> (Duration, Vec<u64>)
where
    H: Send,
    F: Fn(&mut H, u32) + Sync,
{
    let writers = handles.len();
    let parts = workload::partition(master, writers);
    let t0 = Instant::now();
    let lats = std::thread::scope(|s| {
        let mut joins = Vec::new();
        for (part, mut h) in parts.iter().zip(handles.into_iter()) {
            let append = &append;
            joins.push(s.spawn(move || {
                let mut lat = Vec::with_capacity(part.len());
                for &i in part {
                    let t = Instant::now();
                    append(&mut h, i);
                    lat.push(t.elapsed().as_nanos() as u64);
                }
                lat
            }));
        }
        let mut all = Vec::new();
        for j in joins {
            all.extend(j.join().unwrap());
        }
        all
    });
    (t0.elapsed(), lats)
}

/// The original vertical_slice driver shape: tokio multi-thread runtime,
/// one task per writer (baseline-fidelity check).
fn drive_tokio(engine: Arc<MutexEngine>, master: &Arc<Vec<BatchSpec>>, writers: usize) -> (Duration, Vec<u64>) {
    let parts = workload::partition(master, writers);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let t0 = Instant::now();
    let all = rt.block_on(async {
        let mut hs = Vec::new();
        for part in parts {
            let e = engine.clone();
            let m = master.clone();
            hs.push(tokio::spawn(async move {
                let mut lat = Vec::with_capacity(part.len());
                for i in part {
                    let b = &m[i as usize];
                    let t = Instant::now();
                    e.append_batch(b.stream, b.expected, &b.payloads);
                    lat.push(t.elapsed().as_nanos() as u64);
                }
                lat
            }));
        }
        let mut all = Vec::new();
        for h in hs {
            all.extend(h.await.unwrap());
        }
        all
    });
    (t0.elapsed(), all)
}

// -------------------------------------------------------------- run one --

fn run_variant(
    variant: &str,
    dir: &Path,
    master: &Arc<Vec<BatchSpec>>,
    exp: &Expected,
    writers: usize,
    verify: bool,
) -> RunStats {
    let _ = fs::remove_dir_all(dir);
    let n_events = exp.n_events as usize;
    let sample = sample_streams(exp, SAMPLE_STREAMS);

    let a0;
    let (wall, mut lat);
    let (disk, disk_detail);

    match variant {
        "baseline" | "baseline-tokio" | "logonly" | "mutex-batched" | "mutex-inmem" => {
            let mode = match variant {
                "baseline" | "baseline-tokio" => IndexMode::PerEvent,
                "logonly" => IndexMode::None,
                "mutex-batched" => IndexMode::Batched,
                _ => IndexMode::InMem,
            };
            let (engine, _) = MutexEngine::open(dir, mode);
            let engine = Arc::new(engine);
            a0 = alloc_count::snapshot();
            (wall, lat) = if variant == "baseline-tokio" {
                drive_tokio(engine.clone(), master, writers)
            } else {
                drive_shared(master, writers, |b| {
                    engine.append_batch(b.stream, b.expected, &b.payloads);
                })
            };
            engine.finalize();
            assert_eq!(engine.next_global_pos() as usize, n_events);
            if verify {
                verify_log(&dir.join("log"), exp, variant);
                let files = engine.seg_files();
                verify_stream_reads(exp, &sample, variant, |s| {
                    engine.stream_ptrs(s).map(|p| fnv_via_ptrs(&files, &p))
                });
            }
            let (l, i) = (dir_size(&dir.join("log")), dir_size(&dir.join("index")));
            disk = l + i;
            disk_detail = format!("[log {} + index {}]", fmt_bytes(l), fmt_bytes(i));
            drop(engine);
        }
        "actor-naive" | "actor-group" | "actor-inmem" | "actor-pipe" => {
            let (wm, im) = match variant {
                "actor-naive" => (WriteMode::PerBatch, ActorIndex::FjallBatch),
                "actor-group" => (WriteMode::Group, ActorIndex::FjallBatch),
                _ => (WriteMode::Group, ActorIndex::InMem),
            };
            let async_seal = variant == "actor-pipe";
            let (mut engine, handles) =
                ActorEngine::new(dir, master.clone(), writers, wm, im, async_seal);
            a0 = alloc_count::snapshot();
            (wall, lat) = drive_handles(master, handles, |h, i| {
                h.append(i);
            });
            engine.finalize();
            assert_eq!(engine.next_global_pos() as usize, n_events);
            if verify {
                verify_log(&dir.join("log"), exp, variant);
                let files = engine.seg_files();
                verify_stream_reads(exp, &sample, variant, |s| {
                    engine.stream_ptrs(s).map(|p| fnv_via_ptrs(&files, &p))
                });
            }
            let (l, i) = (dir_size(&dir.join("log")), dir_size(&dir.join("index")));
            disk = l + i;
            disk_detail = format!("[log {} + index {}]", fmt_bytes(l), fmt_bytes(i));
            drop(engine);
        }
        "reserve" | "reserve-pipe" => {
            let (mut engine, handles) =
                ReserveEngine::new(dir, master.clone(), writers, variant == "reserve-pipe");
            a0 = alloc_count::snapshot();
            (wall, lat) = drive_handles(master, handles, |h, i| {
                h.append(i);
            });
            engine.finalize();
            assert_eq!(engine.next_global_pos() as usize, n_events);
            if verify {
                verify_log(&dir.join("log"), exp, variant);
                let files = engine.seg_files();
                verify_stream_reads(exp, &sample, variant, |s| {
                    engine.stream_ptrs(s).map(|p| fnv_via_ptrs(&files, &p))
                });
            }
            let (l, i) = (dir_size(&dir.join("log")), dir_size(&dir.join("index")));
            disk = l + i;
            disk_detail = format!("[log {} + index {}]", fmt_bytes(l), fmt_bytes(i));
            drop(engine);
        }
        "rocks" => {
            let engine = Arc::new(RocksEngine::open(dir));
            a0 = alloc_count::snapshot();
            (wall, lat) = drive_shared(master, writers, |b| {
                engine.append_batch(b.stream, b.expected, &b.payloads);
            });
            engine.finalize();
            if verify {
                let (count, ck) = engine.global_check();
                assert_eq!(count as usize, n_events, "rocks: event count");
                assert_eq!(ck, exp.global_ck, "rocks: global checksum");
                verify_stream_reads(exp, &sample, variant, |s| Some(engine.stream_fnv(s)));
            }
            disk = dir_size(dir);
            disk_detail = String::new();
            drop(engine);
        }
        other => panic!("unknown variant {other}"),
    }

    let a1 = alloc_count::snapshot();
    let d = alloc_count::delta(a0, a1);
    lat.sort_unstable();
    RunStats {
        name: variant.into(),
        writers,
        events: n_events,
        wall,
        p50: pct(&lat, 0.50),
        p95: pct(&lat, 0.95),
        p99: pct(&lat, 0.99),
        allocs_per_ev: d.calls as f64 / n_events as f64,
        disk,
        disk_detail,
    }
}

/// Recovery gate: reopen with the baseline recovery path (loads fjall index,
/// scans last segment, repairs) and re-verify log + index reads.
fn recovery_gate(dir: &Path, exp: &Expected, label: &str) {
    let t = Instant::now();
    let (engine, st) = MutexEngine::open(dir, IndexMode::PerEvent);
    let open_time = t.elapsed();
    assert_eq!(st.next_global_pos, exp.n_events, "{label}: recovery next_global_pos");
    assert_eq!(st.truncated_bytes, 0, "{label}: clean log should have no torn tail");
    verify_log(&dir.join("log"), exp, &format!("{label}/recovered"));
    let files = engine.seg_files();
    let sample = sample_streams(exp, SAMPLE_STREAMS);
    verify_stream_reads(exp, &sample, &format!("{label}/recovered"), |s| {
        engine.stream_ptrs(s).map(|p| fnv_via_ptrs(&files, &p))
    });
    println!(
        "    recovery gate OK: reopen {:.0} ms (scanned {} in {} batches, repaired {}), scan + index reads verified",
        open_time.as_secs_f64() * 1e3,
        fmt_bytes(st.scanned_bytes),
        st.scanned_batches,
        st.repaired_batches,
    );
}

fn gen_master(events: usize, batch: usize) -> (Arc<Vec<BatchSpec>>, Expected) {
    let t = Instant::now();
    let (batches, _counts) = workload::generate(events, batch, 42);
    let master = Arc::new(batches);
    let exp = expected_from(&master);
    eprintln!(
        "workload: {} events, batch {}, generated in {:.1}s",
        events,
        batch,
        t.elapsed().as_secs_f64()
    );
    (master, exp)
}

// ------------------------------------------------------------------ modes --

fn cmd_bench(variant: &str, writers: usize, batch: usize, events: usize) {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bench_data");
    let (master, exp) = gen_master(events, batch);
    let dir = base.join(format!("bench-{variant}"));
    let st = run_variant(variant, &dir, &master, &exp, writers, false);
    st.print();
    let _ = fs::remove_dir_all(&dir);
}

fn cmd_ceiling() {
    use seglog::{batch_len, encode_batch_into};
    println!("== ceiling: CRC bakeoff (2,748 B blocks — one encoded batch) ==");
    let block = vec![0xABu8; 2748];
    for (name, f) in [
        ("crc32fast", Box::new(|d: &[u8]| {
            let mut h = crc32fast::Hasher::new();
            h.update(d);
            h.finalize()
        }) as Box<dyn Fn(&[u8]) -> u32>),
        ("crc32c(hw)", Box::new(|d: &[u8]| crc32c::crc32c(d))),
    ] {
        let iters = 400_000; // ~1.1 GB
        let t = Instant::now();
        let mut acc = 0u32;
        for _ in 0..iters {
            acc = acc.wrapping_add(f(&block));
        }
        let el = t.elapsed();
        println!(
            "  {name}: {:.2} GB/s (acc {acc})",
            (iters as u64 * block.len() as u64) as f64 / el.as_secs_f64() / 1e9
        );
    }

    println!("== ceiling: single-thread group encode + write (no channels, no index) ==");
    let (master, _) = gen_master(1_000_000, 10);
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bench_data");
    for group in [1usize, 4, 16, 64, 256] {
        let dir = base.join("ceiling");
        let _ = fs::remove_dir_all(&dir);
        let mut log = seglog::SegmentLog::create(&dir.join("log"));
        let mut buf: Vec<u8> = Vec::with_capacity(4 << 20);
        let t = Instant::now();
        let mut n = 0u64;
        for chunk in master.chunks(group) {
            buf.clear();
            for b in chunk {
                let blen = batch_len(&b.payloads) as u64;
                let virt = log.seg_len + buf.len() as u64;
                if virt + blen > seglog::SEGMENT_SIZE && virt > 0 {
                    use std::io::Write;
                    (&*log.file).write_all(&buf).unwrap();
                    log.seg_len += buf.len() as u64;
                    buf.clear();
                    log.roll();
                }
                encode_batch_into(
                    &mut buf,
                    log.next_batch_id,
                    log.next_global_pos,
                    b.stream,
                    0, // versions don't matter for the ceiling
                    &b.payloads,
                    |_, _| {},
                );
                log.next_batch_id += 1;
                log.next_global_pos += b.payloads.len() as u64;
                n += b.payloads.len() as u64;
            }
            use std::io::Write;
            (&*log.file).write_all(&buf).unwrap();
            log.seg_len += buf.len() as u64;
        }
        let el = t.elapsed();
        let bytes = log.seg_id * seglog::SEGMENT_SIZE + log.seg_len; // approx
        println!(
            "  group={group:>3}: {:>9.0} ev/s  ({:.0} MB/s log write)",
            n as f64 / el.as_secs_f64(),
            bytes as f64 / el.as_secs_f64() / 1e6,
        );
        let _ = fs::remove_dir_all(&dir);
    }

    println!("== ceiling: memcpy bandwidth (64 MiB blocks) ==");
    let src = vec![0xCDu8; 64 << 20];
    let mut dst = vec![0u8; 64 << 20];
    let t = Instant::now();
    let reps = 32;
    for _ in 0..reps {
        dst.copy_from_slice(std::hint::black_box(&src));
        std::hint::black_box(&mut dst);
    }
    let el = t.elapsed();
    println!(
        "  memcpy: {:.1} GB/s",
        (reps as u64 * src.len() as u64) as f64 / el.as_secs_f64() / 1e9
    );
}

fn cmd_all() {
    let events: usize = std::env::var("PERF_APPEND_EVENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(EVENTS);
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bench_data");
    fs::create_dir_all(&base).unwrap();
    println!("== perf_append spike ==");
    println!(
        "workload: {events} events, batches of {BATCH}, {} streams Zipf({}), BUFFERED durability",
        workload::STREAMS,
        workload::ZIPF_S
    );
    let (master, exp) = gen_master(events, BATCH);

    let mut rows: Vec<RunStats> = Vec::new();
    let progression: &[(&str, &[usize])] = &[
        // locked baselines
        ("baseline-tokio", &[4]),                 // vertical_slice fidelity check
        ("baseline", &[1, 4, 12]),                // C0
        ("logonly", &[4]),                        // log-alone reference (~224k in round 2)
        ("rocks", &[1, 4, 12]),                   // the 532k target, reproduced
        // progression
        ("mutex-batched", &[1, 4, 12]),           // C1: F1 minimal fix
        ("mutex-inmem", &[1, 4, 12]),             // C2: F1 upper bound (D5 endgame index)
        ("actor-naive", &[1, 4, 12]),             // C3: F8a sequencing only
        ("actor-group", &[1, 4, 12]),             // C4: + group encode, 1 write()/drain
        ("actor-inmem", &[1, 4, 12]),             // C5: + in-mem index (composed endgame)
        ("actor-pipe", &[1, 4, 12]),              // C6: + seal off the critical path
        ("reserve", &[1, 4, 12]),                 // C7: F8b alternative
        ("reserve-pipe", &[1, 4, 12]),            // C8: F8b + async seal
    ];

    // The machine hosts other concurrent work; run each config REPS times and
    // keep the best wall time (measures the code, not the interference).
    // Correctness gates run on the first rep.
    const REPS: usize = 3;
    let only = std::env::var("PERF_APPEND_ONLY").ok(); // comma-separated variant filter
    for (variant, writer_counts) in progression {
        if let Some(f) = &only {
            if !f.split(',').any(|v| v == *variant) {
                continue;
            }
        }
        println!("== {variant} ==");
        for &w in *writer_counts {
            let dir = base.join(format!("{variant}-w{w}"));
            let mut best: Option<RunStats> = None;
            let mut evps = Vec::new();
            for rep in 0..REPS {
                let st = run_variant(variant, &dir, &master, &exp, w, rep == 0);
                if rep == 0 && *variant != "rocks" && *variant != "logonly" {
                    recovery_gate(&dir, &exp, variant);
                }
                evps.push(st.evps());
                if best.as_ref().is_none_or(|b| st.wall < b.wall) {
                    best = Some(st);
                }
                let _ = fs::remove_dir_all(&dir);
            }
            let best = best.unwrap();
            best.print();
            println!(
                "    reps: [{}] ev/s",
                evps.iter().map(|e| format!("{e:.0}")).collect::<Vec<_>>().join(", ")
            );
            rows.push(best);
        }
    }

    if std::env::var("PERF_APPEND_SKIP_SWEEP").is_ok() {
        println!("\n== summary ==");
        for r in &rows {
            r.print();
        }
        return;
    }

    // Batch-size sensitivity on the winner at 4 writers.
    println!("== batch-size sensitivity (actor-pipe + reserve-pipe, 4 writers) ==");
    for batch in [10usize, 100, 1000] {
        let (m, e) = if batch == BATCH {
            (master.clone(), expected_from(&master))
        } else {
            gen_master(events, batch)
        };
        for variant in ["actor-pipe", "reserve-pipe"] {
            let dir = base.join(format!("{variant}-b{batch}"));
            let mut best: Option<RunStats> = None;
            for rep in 0..3 {
                let st = run_variant(variant, &dir, &m, &e, 4, rep == 0);
                if best.as_ref().is_none_or(|b| st.wall < b.wall) {
                    best = Some(st);
                }
                let _ = fs::remove_dir_all(&dir);
            }
            let mut st = best.unwrap();
            st.name = format!("{variant}/b{batch}");
            st.print();
        }
    }

    println!("\n== summary ==");
    for r in &rows {
        r.print();
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()).unwrap_or("all") {
        "all" => cmd_all(),
        "ceiling" => cmd_ceiling(),
        "gen-only" => {
            let batch = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(BATCH);
            let events = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(EVENTS);
            let (_m, e) = gen_master(events, batch);
            eprintln!("generated; global_ck {}", e.global_ck);
        }
        "bench" => {
            let variant = args.get(2).expect("bench <variant> <writers> [batch] [events]");
            let writers = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(4);
            let batch = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(BATCH);
            let events = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(EVENTS);
            cmd_bench(variant, writers, batch, events);
        }
        other => panic!("unknown subcommand {other}"),
    }
}
