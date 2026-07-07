//! perf_group_commit spike: find the actual ceiling (and the knee) of
//! fully-durable group-commit appends on this machine (Samsung 970 EVO Plus).
//!
//! Phases (`cargo run --release -- <phase>`):
//!   baseline  fdatasync/O_DSYNC latency distributions, concurrent-file fsync
//!             scaling, and the vertical_slice 4-writer reproduction (F2)
//!   matrix    writers {4,16,64,256,512} x batch {1,10,100} for the primary
//!             designs (sequential; device drifts — see REPORT.md 5)
//!   curve     the authoritative settle-paced d7 curve with drift sentinels
//!   compare   secondary designs at selected points
//!   head2head interleaved medians at the top points (drift-controlled)
//!   duel      d7 vs commit-fsync vs commit-dsync, alternating, long settles
//!   crash     SIGKILL mid-load + recovery/ack verification (paranoid gate)
//!   all       baseline + matrix + curve + compare + head2head + crash
//!   clean     remove bench_data/
//!
//! Internal: `crash-child <dir> <design> <writers> <batch>`.

mod engines;
mod seglog;

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use engines::make_engine;
use seglog::recover_dir;

const MATRIX_WRITERS: [usize; 5] = [4, 16, 64, 256, 512];
const MATRIX_BATCH: [usize; 3] = [1, 10, 100];
const POINT_DURATION: Duration = Duration::from_millis(2500);
const CAP_EVENTS: u64 = 3_000_000; // bound disk traffic on fast configs
const POOL_ENTRIES: usize = 256;
const ACK_MAGIC: u32 = 0xACC5_ACC5;
const ACK_REC_LEN: usize = 36;

// ---------------------------------------------------------------- helpers --

fn pct(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

fn ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

/// ~250 B JSON-ish payload, same shape as vertical_slice/src/workload.rs.
fn payload(rng: &mut StdRng, stream: u64, seq: u64) -> Vec<u8> {
    let amount: u32 = rng.random_range(1..100_000);
    let cents: u32 = rng.random_range(0..100);
    let user: u32 = rng.random_range(0..10_000);
    let note_len: usize = rng.random_range(10..80);
    let note: String = (0..note_len)
        .map(|_| (b'a' + rng.random_range(0..26u8)) as char)
        .collect();
    format!(
        r#"{{"type":"AccountCredited","stream":"stream-{stream:05}","seq":{seq},"amount":{amount}.{cents:02},"currency":"USD","actor":"user-{user:04}","source":"perf-group-commit","note":"{note}","occurred_at":"2026-07-07T12:34:56.789Z","schema":"v1"}}"#
    )
    .into_bytes()
}

/// Pre-generated batches so payload generation stays off the timed path.
/// Returns (batches, crc32 of each batch's concatenated payloads).
fn make_pool(batch: usize, seed: u64) -> (Vec<Arc<Vec<Vec<u8>>>>, Vec<u32>) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut pool = Vec::with_capacity(POOL_ENTRIES);
    let mut crcs = Vec::with_capacity(POOL_ENTRIES);
    for i in 0..POOL_ENTRIES {
        let evs: Vec<Vec<u8>> =
            (0..batch).map(|j| payload(&mut rng, i as u64, j as u64)).collect();
        let mut h = crc32fast::Hasher::new();
        for e in &evs {
            h.update(e);
        }
        crcs.push(h.finalize());
        pool.push(Arc::new(evs));
    }
    (pool, crcs)
}

// ------------------------------------------------------------ point runner --

struct PointRow {
    design: String,
    writers: usize,
    batch: usize,
    events: u64,
    wall: f64,
    evs: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    fsyncs: u64,
    fsync_s: f64,
    ev_per_fsync: f64,
}

impl PointRow {
    fn header() {
        println!(
            "  {:<16} {:>4}w {:>4}b {:>10} {:>7} {:>10} {:>9} {:>9} {:>9} {:>8} {:>9} {:>9}",
            "design", "", "", "events", "wall_s", "ev/s", "p50_ms", "p95_ms", "p99_ms", "fsyncs", "fsync/s", "ev/fsync"
        );
    }
    fn print(&self) {
        println!(
            "  {:<16} {:>4}w {:>4}b {:>10} {:>7.2} {:>10.0} {:>9.2} {:>9.2} {:>9.2} {:>8} {:>9.0} {:>9.1}",
            self.design, self.writers, self.batch, self.events, self.wall, self.evs,
            self.p50, self.p95, self.p99, self.fsyncs, self.fsync_s, self.ev_per_fsync
        );
    }
}

/// Per-run budget. The drive is 95% full: sustained writing exhausts its SLC
/// cache and fdatasync degrades ~50x (3 ms -> 150+ ms), poisoning everything
/// measured after it. `settle` idles before the run so each point starts from
/// comparable device state; keep per-point write volume small for comparisons.
#[derive(Clone, Copy)]
struct Budget {
    dur: Duration,
    cap: u64,
    settle: Duration,
}

const DEFAULT_BUDGET: Budget =
    Budget { dur: POINT_DURATION, cap: CAP_EVENTS, settle: Duration::ZERO };

fn run_point(
    design: &str,
    writers: usize,
    batch: usize,
    pool: &[Arc<Vec<Vec<u8>>>],
    base: &Path,
    verify: bool,
    budget: Budget,
) -> PointRow {
    std::thread::sleep(budget.settle);
    let dir = base.join(format!("{design}-w{writers}-b{batch}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let eng = make_engine(design, &dir);

    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(writers + 1));

    let cap = budget.cap;
    let mut handles = Vec::with_capacity(writers);
    for wid in 0..writers {
        let eng = eng.clone();
        let pool: Vec<Arc<Vec<Vec<u8>>>> = pool.to_vec();
        let stop = stop.clone();
        let acked = acked.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let mut lat: Vec<u64> = Vec::with_capacity(1 << 14);
            let mut ver = 0u64;
            let mut i = wid;
            loop {
                let evs = &pool[i % pool.len()];
                i += 1;
                let t = Instant::now();
                eng.append(wid as u64, ver, evs);
                lat.push(t.elapsed().as_nanos() as u64);
                ver += batch as u64;
                let total = acked.fetch_add(batch as u64, Ordering::Relaxed) + batch as u64;
                if total >= cap {
                    stop.store(true, Ordering::Relaxed);
                }
                if stop.load(Ordering::Relaxed) {
                    break;
                }
            }
            lat
        }));
    }

    barrier.wait();
    let t0 = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        if t0.elapsed() >= budget.dur {
            stop.store(true, Ordering::Relaxed);
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    let mut lat: Vec<u64> = Vec::new();
    for h in handles {
        lat.extend(h.join().unwrap());
    }
    let wall = t0.elapsed();

    eng.finalize();
    let fsyncs = eng.fsyncs();
    drop(eng);
    let events = acked.load(Ordering::Relaxed);

    if verify {
        // Correctness gate on every point: everything acked must recover,
        // exactly tiled, nothing discarded after a clean finalize.
        let rec = recover_dir(&dir);
        assert_eq!(rec.total_events, events, "{design} w{writers} b{batch}: recovered != acked");
        assert_eq!(rec.discarded, 0, "{design} w{writers} b{batch}: clean log discarded batches");
    }
    let _ = fs::remove_dir_all(&dir);

    lat.sort_unstable();
    let wall_s = wall.as_secs_f64();
    PointRow {
        design: design.into(),
        writers,
        batch,
        events,
        wall: wall_s,
        evs: events as f64 / wall_s,
        p50: ms(pct(&lat, 0.50)),
        p95: ms(pct(&lat, 0.95)),
        p99: ms(pct(&lat, 0.99)),
        fsyncs,
        fsync_s: fsyncs as f64 / wall_s,
        ev_per_fsync: events as f64 / fsyncs.max(1) as f64,
    }
}

// ------------------------------------------------------------ micro bench --

fn micro_sync_lat(base: &Path) {
    let dir = base.join("micro");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let payload = [0xA5u8; 256];

    let stats = |name: &str, mut lat: Vec<u64>| {
        lat.sort_unstable();
        let mean = lat.iter().sum::<u64>() as f64 / lat.len() as f64;
        println!(
            "  {:<26} n={:<5} p50 {:>7.3} ms  p90 {:>7.3} ms  p99 {:>7.3} ms  max {:>7.3} ms  mean {:>7.3} ms  (~{:.0}/s serial)",
            name, lat.len(), ms(pct(&lat, 0.50)), ms(pct(&lat, 0.90)), ms(pct(&lat, 0.99)),
            ms(*lat.last().unwrap()), mean / 1e6, 1e9 / mean
        );
    };

    // 1: fdatasync after a small append (the D7 cost unit).
    {
        let f = OpenOptions::new().create(true).read(true).write(true)
            .open(dir.join("fdatasync.dat")).unwrap();
        for _ in 0..20 {
            (&f).write_all(&payload).unwrap();
            f.sync_data().unwrap();
        }
        let mut lat = Vec::with_capacity(1000);
        for _ in 0..1000 {
            (&f).write_all(&payload).unwrap();
            let t = Instant::now();
            f.sync_data().unwrap();
            lat.push(t.elapsed().as_nanos() as u64);
        }
        stats("fdatasync (256B dirty)", lat);
    }

    // 2: fdatasync with nothing dirty (pure flush command).
    {
        let f = OpenOptions::new().create(true).read(true).write(true)
            .open(dir.join("clean.dat")).unwrap();
        (&f).write_all(&payload).unwrap();
        f.sync_data().unwrap();
        let mut lat = Vec::with_capacity(500);
        for _ in 0..500 {
            let t = Instant::now();
            f.sync_data().unwrap();
            lat.push(t.elapsed().as_nanos() as u64);
        }
        stats("fdatasync (clean)", lat);
    }

    // 3: O_DSYNC small write.
    {
        let f = OpenOptions::new().create(true).read(true).write(true)
            .custom_flags(libc::O_DSYNC)
            .open(dir.join("dsync.dat")).unwrap();
        for _ in 0..20 {
            (&f).write_all(&payload).unwrap();
        }
        let mut lat = Vec::with_capacity(1000);
        for _ in 0..1000 {
            let t = Instant::now();
            (&f).write_all(&payload).unwrap();
            lat.push(t.elapsed().as_nanos() as u64);
        }
        stats("O_DSYNC write (256B)", lat);
    }

    // 4: O_DSYNC large write (group-sized): does one dsync write beat
    //    write+fdatasync at group granularity?
    {
        let f = OpenOptions::new().create(true).read(true).write(true)
            .custom_flags(libc::O_DSYNC)
            .open(dir.join("dsync-1m.dat")).unwrap();
        let big = vec![0xA5u8; 1 << 20];
        let mut lat = Vec::with_capacity(200);
        for _ in 0..200 {
            let t = Instant::now();
            (&f).write_all(&big).unwrap();
            lat.push(t.elapsed().as_nanos() as u64);
        }
        stats("O_DSYNC write (1 MiB)", lat);
    }
    {
        let f = OpenOptions::new().create(true).read(true).write(true)
            .open(dir.join("fsync-1m.dat")).unwrap();
        let big = vec![0xA5u8; 1 << 20];
        let mut lat = Vec::with_capacity(200);
        for _ in 0..200 {
            let t = Instant::now();
            (&f).write_all(&big).unwrap();
            f.sync_data().unwrap();
            lat.push(t.elapsed().as_nanos() as u64);
        }
        stats("write+fdatasync (1 MiB)", lat);
    }
    let _ = fs::remove_dir_all(&dir);
}

/// Do independent files give parallel flush capacity? (Informs striping.)
fn micro_sync_concurrency(base: &Path) {
    let dir = base.join("micro-conc");
    println!("  concurrent fdatasync streams (own file each, 256B append + fdatasync loop, 2s):");
    for k in [1usize, 2, 4, 8] {
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::new();
        for i in 0..k {
            let stop = stop.clone();
            let path = dir.join(format!("f{i}.dat"));
            handles.push(std::thread::spawn(move || {
                let f = OpenOptions::new().create(true).read(true).write(true).open(path).unwrap();
                let payload = [0x5Au8; 256];
                let mut n = 0u64;
                let mut tot_ns = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    (&f).write_all(&payload).unwrap();
                    let t = Instant::now();
                    f.sync_data().unwrap();
                    tot_ns += t.elapsed().as_nanos() as u64;
                    n += 1;
                }
                (n, tot_ns)
            }));
        }
        let t0 = Instant::now();
        std::thread::sleep(Duration::from_secs(2));
        stop.store(true, Ordering::Relaxed);
        let mut n = 0u64;
        let mut tot = 0u64;
        for h in handles {
            let (hn, ht) = h.join().unwrap();
            n += hn;
            tot += ht;
        }
        let wall = t0.elapsed().as_secs_f64();
        println!(
            "    {k} files: {:>6.0} fsync/s aggregate ({:>6.0}/s per file), mean latency {:.3} ms",
            n as f64 / wall,
            n as f64 / wall / k as f64,
            ms(tot / n.max(1))
        );
    }
    let _ = fs::remove_dir_all(&dir);
}

// ------------------------------------------------------------------ phases --

fn phase_baseline(base: &Path, pools: &HashMap<usize, Vec<Arc<Vec<Vec<u8>>>>>) {
    println!("== baseline: device sync primitives ==");
    micro_sync_lat(base);
    micro_sync_concurrency(base);
    println!();
    println!("== baseline: vertical_slice reproduction (4 writers, batch 10) ==");
    println!("   (round-2 spike measured: sync-per-batch/group-1ms ~3.7-4.2k ev/s p50 ~5.3ms; 5ms ~2.7k; 25ms ~950)");
    PointRow::header();
    let budget = Budget { settle: Duration::from_secs(10), ..DEFAULT_BUDGET };
    for design in ["spb", "v0-1ms", "v0-5ms", "v0-25ms", "d7"] {
        run_point(design, 4, 10, &pools[&10], base, true, budget).print();
    }
    println!();
}

fn phase_matrix(base: &Path, pools: &HashMap<usize, Vec<Arc<Vec<Vec<u8>>>>>) {
    println!("== scaling matrix: writers x events-per-append, fully durable acks ==");
    for design in ["spb", "d7", "d7-pwrite", "striped2", "striped4", "striped4-pwrite", "commit-dsync"] {
        println!("-- {design} --");
        PointRow::header();
        for &b in &MATRIX_BATCH {
            for &w in &MATRIX_WRITERS {
                run_point(design, w, b, &pools[&b], base, true, DEFAULT_BUDGET).print();
            }
        }
        println!();
    }
}

/// The authoritative writers x batch curve for the recommended design, with
/// settle pauses and drift sentinels (same spb point repeated so device-state
/// drift is visible in the output).
fn phase_curve(base: &Path, pools: &HashMap<usize, Vec<Arc<Vec<Vec<u8>>>>>) {
    println!("== curve: d7 (early-close group commit), settle-paced, with drift sentinels ==");
    let budget = Budget {
        dur: Duration::from_millis(2000),
        cap: 1_000_000,
        settle: Duration::from_secs(15),
    };
    PointRow::header();
    run_point("spb", 4, 10, &pools[&10], base, true, budget).print(); // sentinel
    for &b in &MATRIX_BATCH {
        for &w in &MATRIX_WRITERS {
            run_point("d7", w, b, &pools[&b], base, true, budget).print();
        }
        run_point("spb", 4, 10, &pools[&10], base, true, budget).print(); // sentinel
    }
    println!();
}

fn phase_compare(base: &Path, pools: &HashMap<usize, Vec<Arc<Vec<Vec<u8>>>>>) {
    println!("== compare: secondary designs at selected points ==");
    let points: [(usize, usize); 7] =
        [(4, 10), (64, 10), (256, 10), (512, 10), (256, 100), (512, 100), (512, 1)];
    let budget = Budget {
        dur: Duration::from_millis(1500),
        cap: 750_000,
        settle: Duration::from_secs(12),
    };
    for design in ["v0-1ms", "commit-fsync", "commit-piped", "striped2-pwrite"] {
        println!("-- {design} --");
        PointRow::header();
        for &(w, b) in &points {
            run_point(design, w, b, &pools[&b], base, true, budget).print();
        }
        println!();
    }
}

/// The drive is 95% full and its SLC cache state drifts across a long run,
/// so cross-design conclusions come from interleaved repetitions at fixed
/// points rather than from the (sequential) matrix.
fn phase_head2head(base: &Path, pools: &HashMap<usize, Vec<Arc<Vec<Vec<u8>>>>>) {
    println!("== head-to-head: interleaved reps + settle pauses (device drift control) ==");
    let designs =
        ["d7", "d7-pwrite", "striped2-pwrite", "striped4-pwrite", "commit-piped", "commit-dsync"];
    let points: [(usize, usize); 2] = [(512, 10), (512, 100)];
    let budget = Budget {
        dur: Duration::from_millis(1500),
        cap: 750_000,
        settle: Duration::from_secs(12),
    };
    for &(w, b) in &points {
        println!("-- point {w}w x {b}b, 3 interleaved reps per design (median shown) --");
        PointRow::header();
        let mut rows: HashMap<&str, Vec<PointRow>> = HashMap::new();
        for _rep in 0..3 {
            for design in designs {
                let row = run_point(design, w, b, &pools[&b], base, true, budget);
                rows.entry(design).or_default().push(row);
            }
        }
        for design in designs {
            let mut rs = rows.remove(design).unwrap();
            rs.sort_by(|a, b| a.evs.total_cmp(&b.evs));
            let median = &rs[rs.len() / 2];
            median.print();
            let evs: Vec<String> = rs.iter().map(|r| format!("{:.0}", r.evs)).collect();
            println!("      reps ev/s: [{}]", evs.join(", "));
        }
        println!();
    }
}

/// Focused duel with longer settles and all reps shown: is the O_DSYNC
/// committer really beating d7, and is it the coalesced write or the FUA
/// barrier that does it?
fn phase_duel(base: &Path, pools: &HashMap<usize, Vec<Arc<Vec<Vec<u8>>>>>) {
    println!("== duel: d7 vs commit-fsync vs commit-dsync, alternating, 20s settles ==");
    let designs = ["d7", "commit-fsync", "commit-dsync"];
    let budget = Budget {
        dur: Duration::from_millis(1500),
        cap: 750_000,
        settle: Duration::from_secs(20),
    };
    for &(w, b) in &[(512usize, 10usize), (512, 100)] {
        println!("-- point {w}w x {b}b, 4 alternating reps --");
        PointRow::header();
        let mut rows: HashMap<&str, Vec<PointRow>> = HashMap::new();
        for _rep in 0..4 {
            for design in designs {
                let row = run_point(design, w, b, &pools[&b], base, true, budget);
                rows.entry(design).or_default().push(row);
            }
        }
        for design in designs {
            for r in rows.remove(design).unwrap() {
                r.print();
            }
        }
        println!();
    }
}

// -------------------------------------------------------------- crash test --

fn crash_child(dir: &Path, design: &str, writers: usize, batch: usize) {
    let data_dir = dir.join("data");
    let ack_dir = dir.join("acks");
    fs::create_dir_all(&data_dir).unwrap();
    fs::create_dir_all(&ack_dir).unwrap();
    let (pool, crcs) = make_pool(batch, 0xC0FFEE);
    let eng = make_engine(design, &data_dir);
    let mut handles = Vec::new();
    for wid in 0..writers {
        let eng = eng.clone();
        let pool = pool.clone();
        let crcs = crcs.clone();
        let ack_path = ack_dir.join(format!("w{wid:04}.bin"));
        handles.push(std::thread::spawn(move || {
            let mut ack_file = OpenOptions::new().create(true).append(true).open(ack_path).unwrap();
            let mut ver = 0u64;
            let mut i = wid;
            loop {
                let pi = i % pool.len();
                let evs = &pool[pi];
                i += 1;
                let gpos = eng.append(wid as u64, ver, evs);
                // Record strictly AFTER the ack. A record's existence proves
                // the ack happened; a lost record only weakens the test in
                // the safe direction. Fixed-size records; torn tail ignored.
                let mut rec = [0u8; ACK_REC_LEN];
                rec[0..4].copy_from_slice(&ACK_MAGIC.to_le_bytes());
                rec[4..12].copy_from_slice(&gpos.to_le_bytes());
                rec[12..20].copy_from_slice(&(wid as u64).to_le_bytes());
                rec[20..28].copy_from_slice(&ver.to_le_bytes());
                rec[28..32].copy_from_slice(&(evs.len() as u32).to_le_bytes());
                rec[32..36].copy_from_slice(&crcs[pi].to_le_bytes());
                ack_file.write_all(&rec).unwrap();
                ver += batch as u64;
            }
        }));
    }
    for h in handles {
        let _ = h.join(); // unreachable: parent SIGKILLs us
    }
}

fn crash_round(base: &Path, design: &str, writers: usize, batch: usize, round: u64) {
    let dir = base.join(format!("crash-{design}-{round}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    let exe = std::env::current_exe().unwrap();
    let mut child = Command::new(&exe)
        .arg("crash-child")
        .arg(&dir)
        .arg(design)
        .arg(writers.to_string())
        .arg(batch.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut rng = StdRng::seed_from_u64(0xDEAD ^ round);
    let live_ms: u64 = rng.random_range(1200..2600);
    std::thread::sleep(Duration::from_millis(live_ms));
    child.kill().unwrap(); // SIGKILL
    child.wait().unwrap();

    // Recover the (possibly striped) log; build gpos -> batch.
    let rec = recover_dir(&dir.join("data"));
    let mut by_gpos: HashMap<u64, &seglog::RecBatch> = HashMap::new();
    for b in &rec.accepted {
        by_gpos.insert(b.first_global_pos, b);
    }

    // Verify every recorded ack against the recovered log.
    let mut acked = 0u64;
    let mut torn_records = 0u64;
    for wid in 0..writers {
        let path = dir.join("acks").join(format!("w{wid:04}.bin"));
        let data = fs::read(&path).unwrap_or_default();
        for chunk in data.chunks(ACK_REC_LEN) {
            if chunk.len() < ACK_REC_LEN
                || u32::from_le_bytes(chunk[0..4].try_into().unwrap()) != ACK_MAGIC
            {
                torn_records += 1;
                continue;
            }
            let gpos = u64::from_le_bytes(chunk[4..12].try_into().unwrap());
            let stream = u64::from_le_bytes(chunk[12..20].try_into().unwrap());
            let ver = u64::from_le_bytes(chunk[20..28].try_into().unwrap());
            let n = u32::from_le_bytes(chunk[28..32].try_into().unwrap());
            let crc = u32::from_le_bytes(chunk[32..36].try_into().unwrap());
            let b = by_gpos.get(&gpos).unwrap_or_else(|| {
                panic!("{design} round {round}: ACKED batch at gpos {gpos} (writer {stream}) NOT recovered")
            });
            assert_eq!(b.stream_id, stream, "gpos {gpos}: stream mismatch");
            assert_eq!(b.first_version, ver, "gpos {gpos}: version mismatch");
            assert_eq!(b.nevents, n, "gpos {gpos}: event count mismatch");
            assert_eq!(b.payload_crc, crc, "gpos {gpos}: payload bytes corrupted");
            acked += 1;
        }
    }
    let surfaced = rec.accepted.len() as u64 - acked; // upper bound: some acks may be unrecorded
    println!(
        "  {design} round {round}: killed after {live_ms} ms; recovered {} batches / {} events ({} stripes); \
acked-and-recorded {acked} -> ALL intact; written-not-acked surfaced <= {surfaced}; merge-discarded {}; torn ack records {torn_records}",
        rec.accepted.len(),
        rec.total_events,
        rec.stripes,
        rec.discarded,
    );
    assert!(acked > 0, "{design} round {round}: no acks recorded — child never got going");
    let _ = fs::remove_dir_all(&dir);
}

fn phase_crash(base: &Path) {
    println!("== crash: SIGKILL mid-load, recovery must contain every acked batch ==");
    println!("   (most aggressive configs; striped recovery must merge deterministically)");
    for round in 0..4 {
        crash_round(base, "striped4-pwrite", 512, 100, round);
    }
    for round in 0..3 {
        crash_round(base, "striped4", 256, 10, round);
    }
    for round in 0..3 {
        crash_round(base, "d7-pwrite", 512, 100, round);
    }
    println!();
}

// -------------------------------------------------------------------- main --

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let phase = args.get(1).cloned().unwrap_or_else(|| "all".into());

    if phase == "crash-child" {
        let dir = PathBuf::from(&args[2]);
        let design = args[3].clone();
        let writers: usize = args[4].parse().unwrap();
        let batch: usize = args[5].parse().unwrap();
        crash_child(&dir, &design, writers, batch);
        return;
    }

    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bench_data");
    if phase == "clean" {
        let _ = fs::remove_dir_all(&base);
        println!("bench_data/ removed");
        return;
    }
    fs::create_dir_all(&base).unwrap();

    let mut pools = HashMap::new();
    for b in MATRIX_BATCH {
        pools.insert(b, make_pool(b, 42 + b as u64).0);
    }

    let want = |p: &str| phase == "all" || phase == p;
    let t0 = Instant::now();
    if want("baseline") {
        phase_baseline(&base, &pools);
    }
    if want("matrix") {
        phase_matrix(&base, &pools);
    }
    if want("curve") {
        phase_curve(&base, &pools);
    }
    if want("compare") {
        phase_compare(&base, &pools);
    }
    if want("head2head") {
        phase_head2head(&base, &pools);
    }
    if phase == "duel" {
        phase_duel(&base, &pools);
    }
    if want("crash") {
        phase_crash(&base);
    }
    let _ = fs::remove_dir_all(&base);
    println!("done ({phase}) in {:.0}s. bench_data/ cleaned up.", t0.elapsed().as_secs_f64());
}
