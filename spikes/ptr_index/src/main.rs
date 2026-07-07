//! Spike: quantify D5 (notes/mess-research/12_convergence.md).
//!
//! Compares two index-maintenance strategies for the *active* (unsealed) segment
//! index, on two embedded backends (redb, fjall):
//!
//!   A. PER-EVENT:  key = stream_id(8B BE) || version(8B BE)  -> EventPtr (20B)
//!   B. RMW BLOCK:  key = stream_id(8B BE) || block_no(8B BE) -> packed block of
//!      up to 256 EventPtrs; every append reads the block, appends 20B, writes it back.
//!
//! Workload: N appends over 10k streams, Zipf(s=1.1) stream choice, identical
//! seeded sequence for every combo, committed in batches of 100.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand_distr::{Distribution, Zipf};

const STREAMS: u64 = 10_000;
const ZIPF_S: f64 = 1.1;
const BATCH: usize = 100;
const BLOCK_CAP: u64 = 256;
const PTR_LEN: usize = 20;
const KEY_LEN: u64 = 16;
const HOT_SCAN_REPS: u32 = 50;
const COLD_STREAMS: usize = 1_000;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Strategy {
    PerEvent,
    RmwBlock,
}

struct Append {
    stream: u64,
    version: u64,
    ptr: [u8; PTR_LEN],
}

#[derive(Default)]
struct Metrics {
    label: String,
    wall: Duration,
    appends: usize,
    logical_write_bytes: u64,
    logical_read_bytes: u64, // strategy B only: bytes read back for RMW
    rmw_entries_written: u64, // sum of block entry counts written (B only)
    disk_bytes: u64,
    hot_stream_events: u64,
    hot_scan: Duration, // per full scan of hot stream (avg over reps)
    cold_total: Duration, // total for COLD_STREAMS scans
    cold_events: u64,
}

fn key16(stream: u64, second: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&stream.to_be_bytes());
    k[8..].copy_from_slice(&second.to_be_bytes());
    k
}

fn gen_workload(n: usize) -> (Vec<Append>, Vec<u64>) {
    let mut rng = StdRng::seed_from_u64(42);
    let zipf = Zipf::new(STREAMS as f64, ZIPF_S).unwrap();
    let mut versions = vec![0u64; STREAMS as usize];
    let mut out = Vec::with_capacity(n);
    let mut offset = 0u64;
    for i in 0..n {
        let s = (zipf.sample(&mut rng) as u64).clamp(1, STREAMS) - 1;
        let v = versions[s as usize];
        versions[s as usize] += 1;
        let len = 100 + (i as u32 % 1900);
        let mut ptr = [0u8; PTR_LEN];
        ptr[..8].copy_from_slice(&((i as u64) / 50_000).to_be_bytes()); // segment_id
        ptr[8..16].copy_from_slice(&offset.to_be_bytes()); // offset
        ptr[16..].copy_from_slice(&len.to_be_bytes()); // len
        offset += len as u64;
        out.push(Append { stream: s, version: v, ptr });
    }
    (out, versions)
}

/// Pick the hottest stream and a seeded uniform sample of "cold" streams
/// (non-empty, outside the top 100 by event count).
fn pick_read_targets(versions: &[u64]) -> (u64, Vec<u64>) {
    let mut by_count: Vec<(u64, u64)> = versions
        .iter()
        .enumerate()
        .filter(|(_, &c)| c > 0)
        .map(|(s, &c)| (s as u64, c))
        .collect();
    by_count.sort_by(|a, b| b.1.cmp(&a.1));
    let hot = by_count[0].0;
    let mut cold_pool: Vec<u64> = by_count.iter().skip(100).map(|&(s, _)| s).collect();
    let mut rng = StdRng::seed_from_u64(7);
    cold_pool.shuffle(&mut rng);
    cold_pool.truncate(COLD_STREAMS);
    (hot, cold_pool)
}

/// Actual allocated bytes (like `du`), not apparent length — fjall preallocates
/// sparse journal files.
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

fn fresh_dir(base: &Path, name: &str) -> PathBuf {
    let dir = base.join(name);
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

// ---------------------------------------------------------------- redb ----

const REDB_TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("ptrs");

use redb::{ReadableDatabase, ReadableTable};

fn run_redb(strategy: Strategy, work: &[Append], versions: &[u64], base: &Path) -> Metrics {
    let name = format!("redb-{:?}", strategy);
    let dir = fresh_dir(base, &name);
    let db = redb::Database::create(dir.join("index.redb")).unwrap();

    let mut m = Metrics { label: name, appends: work.len(), ..Default::default() };
    let t0 = Instant::now();
    for chunk in work.chunks(BATCH) {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(REDB_TABLE).unwrap();
            for a in chunk {
                match strategy {
                    Strategy::PerEvent => {
                        let k = key16(a.stream, a.version);
                        table.insert(&k[..], &a.ptr[..]).unwrap();
                        m.logical_write_bytes += KEY_LEN + PTR_LEN as u64;
                    }
                    Strategy::RmwBlock => {
                        let k = key16(a.stream, a.version / BLOCK_CAP);
                        let mut block = table
                            .get(&k[..])
                            .unwrap()
                            .map(|g| g.value().to_vec())
                            .unwrap_or_default();
                        m.logical_read_bytes += KEY_LEN + block.len() as u64;
                        block.extend_from_slice(&a.ptr);
                        m.logical_write_bytes += KEY_LEN + block.len() as u64;
                        m.rmw_entries_written += (block.len() / PTR_LEN) as u64;
                        table.insert(&k[..], &block[..]).unwrap();
                    }
                }
            }
        }
        txn.commit().unwrap(); // redb default durability: Immediate (fsync per commit)
    }
    m.wall = t0.elapsed();

    // ---- read path ----
    let (hot, cold) = pick_read_targets(versions);
    let rt = db.begin_read().unwrap();
    let table = rt.open_table(REDB_TABLE).unwrap();

    let scan_stream = |stream: u64| -> u64 {
        let mut events = 0u64;
        match strategy {
            Strategy::PerEvent => {
                let lo = key16(stream, 0);
                let hi = key16(stream, u64::MAX);
                for r in table.range(&lo[..]..=&hi[..]).unwrap() {
                    let (_, v) = r.unwrap();
                    assert_eq!(v.value().len(), PTR_LEN);
                    events += 1;
                }
            }
            Strategy::RmwBlock => {
                let mut block_no = 0u64;
                loop {
                    let k = key16(stream, block_no);
                    match table.get(&k[..]).unwrap() {
                        Some(g) => {
                            events += (g.value().len() / PTR_LEN) as u64;
                            block_no += 1;
                        }
                        None => break,
                    }
                }
            }
        }
        events
    };

    let t = Instant::now();
    for _ in 0..HOT_SCAN_REPS {
        m.hot_stream_events = scan_stream(hot);
    }
    m.hot_scan = t.elapsed() / HOT_SCAN_REPS;
    assert_eq!(m.hot_stream_events, versions[hot as usize]);

    let t = Instant::now();
    for &s in &cold {
        m.cold_events += scan_stream(s);
    }
    m.cold_total = t.elapsed();

    drop(table);
    drop(rt);
    drop(db);
    m.disk_bytes = dir_size(&dir);
    m
}

// --------------------------------------------------------------- fjall ----

fn run_fjall(strategy: Strategy, sync: bool, work: &[Append], versions: &[u64], base: &Path) -> Metrics {
    let name = format!("fjall{}-{:?}", if sync { "-sync" } else { "" }, strategy);
    let dir = fresh_dir(base, &name);
    let db = fjall::Database::open(fjall::Config::new(&dir)).unwrap();
    let part = db
        .keyspace("ptrs", fjall::KeyspaceCreateOptions::default)
        .unwrap();

    let mut m = Metrics { label: name, appends: work.len(), ..Default::default() };
    let t0 = Instant::now();
    for chunk in work.chunks(BATCH) {
        for a in chunk {
            match strategy {
                Strategy::PerEvent => {
                    let k = key16(a.stream, a.version);
                    part.insert(&k[..], &a.ptr[..]).unwrap();
                    m.logical_write_bytes += KEY_LEN + PTR_LEN as u64;
                }
                Strategy::RmwBlock => {
                    let k = key16(a.stream, a.version / BLOCK_CAP);
                    let mut block = part.get(&k[..]).unwrap().map(|s| s.to_vec()).unwrap_or_default();
                    m.logical_read_bytes += KEY_LEN + block.len() as u64;
                    block.extend_from_slice(&a.ptr);
                    m.logical_write_bytes += KEY_LEN + block.len() as u64;
                    m.rmw_entries_written += (block.len() / PTR_LEN) as u64;
                    part.insert(&k[..], &block[..]).unwrap();
                }
            }
        }
        if sync {
            db.persist(fjall::PersistMode::SyncAll).unwrap();
        }
    }
    // make the final state durable in both modes before timing stops
    db.persist(fjall::PersistMode::SyncAll).unwrap();
    m.wall = t0.elapsed();

    // ---- read path ----
    let (hot, cold) = pick_read_targets(versions);
    let scan_stream = |stream: u64| -> u64 {
        let mut events = 0u64;
        match strategy {
            Strategy::PerEvent => {
                for guard in part.prefix(stream.to_be_bytes()) {
                    let (_, v) = guard.into_inner().unwrap();
                    assert_eq!(v.len(), PTR_LEN);
                    events += 1;
                }
            }
            Strategy::RmwBlock => {
                let mut block_no = 0u64;
                loop {
                    let k = key16(stream, block_no);
                    match part.get(&k[..]).unwrap() {
                        Some(v) => {
                            events += (v.len() / PTR_LEN) as u64;
                            block_no += 1;
                        }
                        None => break,
                    }
                }
            }
        }
        events
    };

    let t = Instant::now();
    for _ in 0..HOT_SCAN_REPS {
        m.hot_stream_events = scan_stream(hot);
    }
    m.hot_scan = t.elapsed() / HOT_SCAN_REPS;
    assert_eq!(m.hot_stream_events, versions[hot as usize]);

    let t = Instant::now();
    for &s in &cold {
        m.cold_events += scan_stream(s);
    }
    m.cold_total = t.elapsed();

    // flush the memtable to sstables so on-disk size reflects the LSM
    // representation, not just the journal
    part.rotate_memtable_and_wait().unwrap();
    drop(part);
    drop(db);
    m.disk_bytes = dir_size(&dir);
    m
}

// ---------------------------------------------------------------- main ----

fn fmt_bytes(b: u64) -> String {
    if b >= 1 << 30 {
        format!("{:.2} GiB", b as f64 / (1u64 << 30) as f64)
    } else if b >= 1 << 20 {
        format!("{:.2} MiB", b as f64 / (1u64 << 20) as f64)
    } else {
        format!("{:.1} KiB", b as f64 / 1024.0)
    }
}

fn report(m: &Metrics) {
    let secs = m.wall.as_secs_f64();
    println!("== {} ==", m.label);
    println!("  wall time            : {:.2}s", secs);
    println!("  appends/sec          : {:.0}", m.appends as f64 / secs);
    println!("  logical bytes written: {} ({} B)", fmt_bytes(m.logical_write_bytes), m.logical_write_bytes);
    if m.logical_read_bytes > 0 {
        println!("  logical bytes read   : {} (RMW read-back)", fmt_bytes(m.logical_read_bytes));
        println!(
            "  avg block entries/append (written): {:.1}  (~{} B read+written per append)",
            m.rmw_entries_written as f64 / m.appends as f64,
            (m.logical_read_bytes + m.logical_write_bytes) / m.appends as u64,
        );
    }
    println!("  on-disk size         : {} ({} B)", fmt_bytes(m.disk_bytes), m.disk_bytes);
    println!(
        "  hot stream replay    : {:?} for {} events ({:.0} ns/event)",
        m.hot_scan,
        m.hot_stream_events,
        m.hot_scan.as_nanos() as f64 / m.hot_stream_events as f64
    );
    println!(
        "  cold streams replay  : {:?} total for {} streams / {} events ({:.1} us/stream)",
        m.cold_total,
        COLD_STREAMS,
        m.cold_events,
        m.cold_total.as_micros() as f64 / COLD_STREAMS as f64
    );
    println!();
}

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(500_000);

    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bench_data");
    fs::create_dir_all(&base).unwrap();

    println!(
        "workload: {} appends, {} streams, Zipf(s={}), batch={} appends/commit, block cap={}",
        n, STREAMS, ZIPF_S, BATCH, BLOCK_CAP
    );
    let (work, versions) = gen_workload(n);
    let distinct = versions.iter().filter(|&&c| c > 0).count();
    let max = versions.iter().max().unwrap();
    println!("  distinct streams used: {}, hottest stream: {} events\n", distinct, max);

    let mut all = Vec::new();
    for strategy in [Strategy::PerEvent, Strategy::RmwBlock] {
        let m = run_redb(strategy, &work, &versions, &base);
        report(&m);
        all.push(m);
        let m = run_fjall(strategy, false, &work, &versions, &base);
        report(&m);
        all.push(m);
        let m = run_fjall(strategy, true, &work, &versions, &base);
        report(&m);
        all.push(m);
    }

    println!("== write amplification (logical bytes, B vs A) ==");
    for backend in ["redb", "fjall", "fjall-sync"] {
        let a = all.iter().find(|m| m.label == format!("{}-PerEvent", backend)).unwrap();
        let b = all.iter().find(|m| m.label == format!("{}-RmwBlock", backend)).unwrap();
        println!(
            "  {backend}: logical {:.1}x, wall-time {:.2}x, disk {:.2}x",
            b.logical_write_bytes as f64 / a.logical_write_bytes as f64,
            b.wall.as_secs_f64() / a.wall.as_secs_f64(),
            b.disk_bytes as f64 / a.disk_bytes as f64,
        );
    }
}
