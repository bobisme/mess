//! Vertical-slice spike: does a custom append-only segment log (payloads once)
//! + fjall pointer index (Engine A, "Meridian slice") beat storing payloads
//! twice in RocksDB (Engine B, current mess_db shape)?
//!
//! Deliverable: REPORT.md. Run with `cargo run --release`.

mod engine_a;
mod engine_b;
mod seglog;
mod workload;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand_distr::{Distribution, Zipf};

use engine_a::{Durability, MeridianEngine};
use engine_b::{BDurability, RocksEngine};
use workload::BatchSpec;

const WRITERS: usize = 4;
const FULL_EVENTS: usize = 1_000_000;
const REDUCED_EVENTS: usize = 250_000; // for configs that would exceed ~10 min at 1M
const STREAM_REPLAY_STREAMS: usize = 1_000;
const GLOBAL_REPLAY_REPS: usize = 3;

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

struct LoadRow {
    name: String,
    events: usize,
    wall: Duration,
    p50: u64,
    p95: u64,
    p99: u64,
    disk: u64,
    disk_detail: String,
}

impl LoadRow {
    fn print(&self) {
        println!(
            "  {:<18} {:>9} ev  {:>8.2}s  {:>9.0} ev/s  p50 {:>9.1}us  p95 {:>9.1}us  p99 {:>9.1}us  disk {:>10} ({:.1} B/ev) {}",
            self.name,
            self.events,
            self.wall.as_secs_f64(),
            self.events as f64 / self.wall.as_secs_f64(),
            us(self.p50),
            us(self.p95),
            us(self.p99),
            fmt_bytes(self.disk),
            self.disk as f64 / self.events as f64,
            self.disk_detail,
        );
    }
}

// ----------------------------------------------------------------- drivers --

async fn drive_a(
    engine: Arc<MeridianEngine>,
    master: Arc<Vec<BatchSpec>>,
    parts: Vec<Vec<u32>>,
) -> (Duration, Vec<u64>) {
    let t0 = Instant::now();
    let mut handles = Vec::new();
    for idx in parts {
        let e = engine.clone();
        let m = master.clone();
        handles.push(tokio::spawn(async move {
            let mut lat = Vec::with_capacity(idx.len());
            for i in idx {
                let b = &m[i as usize];
                let t = Instant::now();
                e.append_batch(b.stream, b.expected, &b.payloads).await;
                lat.push(t.elapsed().as_nanos() as u64);
            }
            lat
        }));
    }
    let mut all = Vec::new();
    for h in handles {
        all.extend(h.await.unwrap());
    }
    (t0.elapsed(), all)
}

async fn drive_b(
    engine: Arc<RocksEngine>,
    master: Arc<Vec<BatchSpec>>,
    parts: Vec<Vec<u32>>,
) -> (Duration, Vec<u64>) {
    let t0 = Instant::now();
    let mut handles = Vec::new();
    for idx in parts {
        let e = engine.clone();
        let m = master.clone();
        handles.push(tokio::spawn(async move {
            let mut lat = Vec::with_capacity(idx.len());
            for i in idx {
                let b = &m[i as usize];
                let t = Instant::now();
                e.append_batch(b.stream, b.expected, &b.payloads).await;
                lat.push(t.elapsed().as_nanos() as u64);
            }
            lat
        }));
    }
    let mut all = Vec::new();
    for h in handles {
        all.extend(h.await.unwrap());
    }
    (t0.elapsed(), all)
}

fn pick_replay_streams(counts: &[u64], n: usize, seed: u64) -> Vec<u64> {
    let mut nonempty: Vec<u64> = counts
        .iter()
        .enumerate()
        .filter(|(_, &c)| c > 0)
        .map(|(s, _)| s as u64)
        .collect();
    let mut rng = StdRng::seed_from_u64(seed);
    nonempty.shuffle(&mut rng);
    nonempty.truncate(n);
    nonempty
}

// -------------------------------------------------------------- crash child --

fn crash_child(dir: &Path) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let (engine, _) = MeridianEngine::open(dir, Durability::Buffered);
        engine.set_split_writes(true); // widen the torn-batch window
        let mut rng = StdRng::seed_from_u64(0xC0FFEE);
        let zipf = Zipf::new(workload::STREAMS as f64, workload::ZIPF_S).unwrap();
        loop {
            let s = (zipf.sample(&mut rng) as u64).clamp(1, workload::STREAMS) - 1;
            let expected = engine.head(s);
            let first = expected.map_or(0, |h| h + 1);
            let payloads: Vec<Vec<u8>> = (0..workload::BATCH as u64)
                .map(|i| workload::payload(&mut rng, s, first + i))
                .collect();
            engine.append_batch(s, expected, &payloads).await;
        }
    });
}

// --------------------------------------------------------------------- main --

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(|s| s.as_str()) == Some("crash-child") {
        crash_child(Path::new(&args[2]));
        return;
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let phase = args.get(1).cloned().unwrap_or_else(|| "all".into());
    rt.block_on(run(&phase));
}

/// `phase`: all | load-a | load-b | replays | recovery (each fits well
/// under 10 minutes; "all" runs everything).
async fn run(phase: &str) {
    let want = |p: &str| phase == "all" || phase == p;
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bench_data");
    fs::create_dir_all(&base).unwrap();

    println!("== vertical_slice spike ==");
    println!(
        "workload: {} events max, batches of {}, {} streams Zipf(s={}), {} writers",
        FULL_EVENTS,
        workload::BATCH,
        workload::STREAMS,
        workload::ZIPF_S,
        WRITERS
    );

    let t = Instant::now();
    let (batches, counts) = workload::generate(FULL_EVENTS, 42);
    let total_payload: u64 = batches
        .iter()
        .flat_map(|b| b.payloads.iter())
        .map(|p| p.len() as u64)
        .sum();
    let distinct = counts.iter().filter(|&&c| c > 0).count();
    let hottest = *counts.iter().max().unwrap();
    println!(
        "generated in {:.1}s: {} batches, mean payload {:.1} B, total payload {}, distinct streams {}, hottest stream {} events",
        t.elapsed().as_secs_f64(),
        batches.len(),
        total_payload as f64 / FULL_EVENTS as f64,
        fmt_bytes(total_payload),
        distinct,
        hottest
    );
    println!();
    let master = Arc::new(batches);

    // ------------------------------------------------------------ Engine A --
    let mut rows: Vec<LoadRow> = Vec::new();
    if want("load-a") {
    println!("== load: Engine A (Meridian slice: segment log + fjall ptr index) ==");
    let a_cfgs: Vec<(&str, Durability, usize)> = vec![
        ("A-buffered", Durability::Buffered, FULL_EVENTS),
        ("A-sync-per-batch", Durability::SyncPerBatch, FULL_EVENTS),
        ("A-group-1ms", Durability::Group(Duration::from_millis(1)), FULL_EVENTS),
        ("A-group-5ms", Durability::Group(Duration::from_millis(5)), FULL_EVENTS),
        ("A-group-25ms", Durability::Group(Duration::from_millis(25)), REDUCED_EVENTS),
    ];
    for (name, dur, n_events) in a_cfgs {
        let dir = base.join(name);
        let _ = fs::remove_dir_all(&dir);
        let (engine, _) = MeridianEngine::open(&dir, dur);
        let engine = Arc::new(engine);
        let n_batches = n_events / workload::BATCH;
        let parts = workload::partition(&master, n_batches, WRITERS);
        let (wall, mut lat) = drive_a(engine.clone(), master.clone(), parts).await;
        engine.finalize();
        // sanity: every event visible, contiguous
        let g = engine.read_global(0, u64::MAX);
        assert_eq!(g.events as usize, n_events, "{name}: global replay count");
        drop(engine);
        lat.sort_unstable();
        let log_b = dir_size(&dir.join("log"));
        let idx_b = dir_size(&dir.join("index"));
        let row = LoadRow {
            name: name.into(),
            events: n_events,
            wall,
            p50: pct(&lat, 0.50),
            p95: pct(&lat, 0.95),
            p99: pct(&lat, 0.99),
            disk: log_b + idx_b,
            disk_detail: format!("[log {} + index {}]", fmt_bytes(log_b), fmt_bytes(idx_b)),
        };
        row.print();
        rows.push(row);
        if name != "A-buffered" {
            let _ = fs::remove_dir_all(&dir);
        }
    }
    println!();
    } // want("load-a")

    // ------------------------------------------------------------ Engine B --
    if want("load-b") {
    println!("== load: Engine B (RocksDB, mess_db shape: payload duplicated in 2 CFs) ==");
    let b_cfgs: Vec<(&str, BDurability, usize)> = vec![
        ("B-nosync", BDurability::NoSync, FULL_EVENTS),
        ("B-sync-per-batch", BDurability::SyncPerBatch, FULL_EVENTS),
        ("B-group-1ms", BDurability::Group(Duration::from_millis(1)), FULL_EVENTS),
        ("B-group-5ms", BDurability::Group(Duration::from_millis(5)), FULL_EVENTS),
        ("B-group-25ms", BDurability::Group(Duration::from_millis(25)), REDUCED_EVENTS),
    ];
    let mut b_compacted: Option<u64> = None;
    for (name, dur, n_events) in b_cfgs {
        let dir = base.join(name);
        let _ = fs::remove_dir_all(&dir);
        let engine = Arc::new(RocksEngine::open(&dir, dur));
        let n_batches = n_events / workload::BATCH;
        let parts = workload::partition(&master, n_batches, WRITERS);
        let (wall, mut lat) = drive_b(engine.clone(), master.clone(), parts).await;
        engine.finalize();
        let g = engine.read_global(0, u64::MAX);
        assert_eq!(g.events as usize, n_events, "{name}: global replay count");
        let disk = dir_size(&dir);
        let mut detail = String::new();
        if name == "B-nosync" {
            engine.compact();
            let after = dir_size(&dir);
            b_compacted = Some(after);
            detail = format!("[after full compaction: {}]", fmt_bytes(after));
        }
        drop(engine);
        lat.sort_unstable();
        let row = LoadRow {
            name: name.into(),
            events: n_events,
            wall,
            p50: pct(&lat, 0.50),
            p95: pct(&lat, 0.95),
            p99: pct(&lat, 0.99),
            disk,
            disk_detail: detail,
        };
        row.print();
        rows.push(row);
        if name != "B-nosync" {
            let _ = fs::remove_dir_all(&dir);
        }
    }
    let _ = b_compacted;
    println!();
    } // want("load-b")

    // ------------------------------------------------------------- replays --
    // Same data in every durability config; replay on the buffered/nosync dirs.
    if want("replays") {
    println!("== replay: Engine A (dir A-buffered, warm page cache) ==");
    let t = Instant::now();
    let (a_eng, a_open_stats) = MeridianEngine::open(&base.join("A-buffered"), Durability::Buffered);
    let a_clean_open = t.elapsed();
    println!(
        "  clean reopen: {:?} (scanned last segment: {} in {} batches, repaired {}, truncated {} B, heads loaded {})",
        a_clean_open,
        fmt_bytes(a_open_stats.scanned_bytes),
        a_open_stats.scanned_batches,
        a_open_stats.repaired_batches,
        a_open_stats.truncated_bytes,
        a_open_stats.heads_loaded,
    );
    for rep in 0..GLOBAL_REPLAY_REPS {
        let t = Instant::now();
        let st = a_eng.read_global(0, u64::MAX);
        let el = t.elapsed();
        println!(
            "  global replay #{}: {} events, {} in {:.3}s -> {:.0} MB/s, {:.0} ev/s (checksum {})",
            rep,
            st.events,
            fmt_bytes(st.bytes),
            el.as_secs_f64(),
            st.bytes as f64 / el.as_secs_f64() / 1e6,
            st.events as f64 / el.as_secs_f64(),
            st.checksum,
        );
    }
    let targets = pick_replay_streams(&counts, STREAM_REPLAY_STREAMS, 7);
    let mut a_stream_lat = Vec::with_capacity(targets.len());
    let mut a_stream_events = 0u64;
    let t_total = Instant::now();
    for &s in &targets {
        let t = Instant::now();
        let st = a_eng.read_stream(s, 0, u64::MAX);
        a_stream_lat.push(t.elapsed().as_nanos() as u64);
        assert_eq!(st.events, counts[s as usize], "A stream {s} replay count");
        a_stream_events += st.events;
    }
    let a_stream_total = t_total.elapsed();
    a_stream_lat.sort_unstable();
    println!(
        "  stream replay: {} streams, {} events in {:.3}s -> {:.0} ev/s; per-stream p50 {:.1}us p99 {:.1}us max {:.1}us",
        targets.len(),
        a_stream_events,
        a_stream_total.as_secs_f64(),
        a_stream_events as f64 / a_stream_total.as_secs_f64(),
        us(pct(&a_stream_lat, 0.50)),
        us(pct(&a_stream_lat, 0.99)),
        us(*a_stream_lat.last().unwrap()),
    );
    drop(a_eng);
    println!();

    println!("== replay: Engine B (dir B-nosync, warm page cache) ==");
    let t = Instant::now();
    let b_eng = RocksEngine::open(&base.join("B-nosync"), BDurability::NoSync);
    println!("  reopen: {:?}", t.elapsed());
    for rep in 0..GLOBAL_REPLAY_REPS {
        let t = Instant::now();
        let st = b_eng.read_global(0, u64::MAX);
        let el = t.elapsed();
        println!(
            "  global replay #{}: {} events, {} (logical kv) in {:.3}s -> {:.0} MB/s, {:.0} ev/s (checksum {})",
            rep,
            st.events,
            fmt_bytes(st.bytes),
            el.as_secs_f64(),
            st.bytes as f64 / el.as_secs_f64() / 1e6,
            st.events as f64 / el.as_secs_f64(),
            st.checksum,
        );
    }
    let mut b_stream_lat = Vec::with_capacity(targets.len());
    let mut b_stream_events = 0u64;
    let t_total = Instant::now();
    for &s in &targets {
        let t = Instant::now();
        let st = b_eng.read_stream(s, 0, u64::MAX);
        b_stream_lat.push(t.elapsed().as_nanos() as u64);
        assert_eq!(st.events, counts[s as usize], "B stream {s} replay count");
        b_stream_events += st.events;
    }
    let b_stream_total = t_total.elapsed();
    b_stream_lat.sort_unstable();
    println!(
        "  stream replay: {} streams, {} events in {:.3}s -> {:.0} ev/s; per-stream p50 {:.1}us p99 {:.1}us max {:.1}us",
        targets.len(),
        b_stream_events,
        b_stream_total.as_secs_f64(),
        b_stream_events as f64 / b_stream_total.as_secs_f64(),
        us(pct(&b_stream_lat, 0.50)),
        us(pct(&b_stream_lat, 0.99)),
        us(*b_stream_lat.last().unwrap()),
    );
    drop(b_eng);
    println!();
    } // want("replays")

    // ------------------------------------------------------------ recovery --
    if want("recovery") {
    println!("== recovery: Engine A, crash (SIGKILL mid-write) ==");
    let exe = std::env::current_exe().unwrap();
    for run in 0..3 {
        let dir = base.join(format!("A-crash-{run}"));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let mut child = Command::new(&exe)
            .arg("crash-child")
            .arg(&dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        std::thread::sleep(Duration::from_millis(2500));
        child.kill().unwrap();
        child.wait().unwrap();

        let t = Instant::now();
        let (eng, st) = MeridianEngine::open(&dir, Durability::Buffered);
        let open_time = t.elapsed();
        let g = eng.read_global(0, u64::MAX);
        assert_eq!(g.events, st.next_global_pos, "crash-{run}: recovered log contiguous");
        // spot-check the index against the log for 25 streams
        let heads = eng.heads_snapshot();
        let mut rng = StdRng::seed_from_u64(run);
        let mut sample = heads.clone();
        sample.shuffle(&mut rng);
        for &(s, head) in sample.iter().take(25) {
            let st = eng.read_stream(s, 0, u64::MAX);
            assert_eq!(st.events, head + 1, "crash-{run}: stream {s} index vs log");
        }
        println!(
            "  crash run {}: {} events survive; open {:?} (scan {} / {} batches @ {:.0} MB/s, repaired {}, truncated {} B, stop {:?}); index spot-checks pass",
            run,
            st.next_global_pos,
            open_time,
            fmt_bytes(st.scanned_bytes),
            st.scanned_batches,
            st.scanned_bytes as f64 / open_time.as_secs_f64() / 1e6,
            st.repaired_batches,
            st.truncated_bytes,
            st.stop.unwrap(),
        );
        drop(eng);
        let _ = fs::remove_dir_all(&dir);
    }
    println!();
    } // want("recovery")

    if !rows.is_empty() {
        println!("== summary (load) ==");
        for r in &rows {
            r.print();
        }
    }
    println!("\ndone ({phase}). bench_data/ left on disk: A-buffered, B-nosync (delete freely).");
}
