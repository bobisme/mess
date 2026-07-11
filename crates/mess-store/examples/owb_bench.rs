//! Spike C (bn-2ib) measurement harness: `open_without_book`.
//!
//! One phase per PROCESS so `/proc/self/status VmHWM` is a clean peak-RSS
//! measurement of exactly that phase. Run with `--release`; each subcommand
//! prints a single machine-greppable `OWB ...` line.
//!
//! ```text
//! cargo run -p mess-store --release --example owb_bench -- \
//!     seed   <dir> [events] [streams] [per_batch] [payload_bytes] [segment_mb]
//!     open   <dir>            # reopen wall time + peak RSS + total_events
//!     hot    <dir> [iters]    # repeated full load of the fattest stream
//!     replay <dir>            # paged read_global + all-stream read_stream
//!     point  <dir> [n]        # random single-record stream reads
//! ```
//!
//! The harness intentionally touches only the PUBLIC `LogEngine`/`Backend`
//! API, so the identical binary measures the pre-change (Book) engine and the
//! post-change (block-native) engine — the before/after tables in
//! `spikes/open_without_book/REPORT.md` come from running it on both builds.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use mess_log::committer::Durability;
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EngineOptions, LogEngine, Version};

/// Counting allocator: allocation count for the append-allocations/event gate.
struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(
        &self,
        ptr: *mut u8,
        layout: Layout,
        new_size: usize,
    ) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// `VmHWM` (peak resident set) in KiB from `/proc/self/status`.
fn vm_hwm_kib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            return rest.trim().trim_end_matches(" kB").trim().parse().unwrap();
        }
    }
    0
}

/// A shreddable MessagePack map `{"seq": <i>, "kind": "demo", "pad": <str>}` —
/// engages the `.pcol` columnar path at seal — padded to ~`bytes`.
fn msgpack_payload(i: u64, bytes: usize) -> Vec<u8> {
    let mut m = vec![0x83]; // fixmap(3)
    // "seq" : uint64
    m.push(0xA3);
    m.extend_from_slice(b"seq");
    m.push(0xCF);
    m.extend_from_slice(&i.to_be_bytes());
    // "kind" : "demo"
    m.push(0xA4);
    m.extend_from_slice(b"kind");
    m.push(0xA4);
    m.extend_from_slice(b"demo");
    // "pad" : str8 of filler
    let pad = bytes.saturating_sub(m.len() + 7).min(255);
    m.push(0xA3);
    m.extend_from_slice(b"pad");
    m.push(0xD9); // str8
    m.push(pad as u8);
    m.extend(std::iter::repeat_n(b'x', pad));
    m
}

fn stream_name(s: u64) -> String { format!("acct-{s:06}") }

fn open(dir: &str) -> LogEngine {
    LogEngine::open_with(
        dir,
        EngineOptions {
            durability: Durability::Process,
            // Small segments so a multi-segment sealed corpus forms.
            segment_size: 8 * 1024 * 1024,
            ..Default::default()
        },
    )
    .expect("open engine")
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn seed(dir: &str, args: &[String]) {
    let events: u64 = args.first().map_or(2_000_000, |a| a.parse().unwrap());
    let streams: u64 = args.get(1).map_or(1_000, |a| a.parse().unwrap());
    let per_batch: u64 = args.get(2).map_or(10, |a| a.parse().unwrap());
    let payload: usize = args.get(3).map_or(64, |a| a.parse().unwrap());
    let seg_mb: u64 = args.get(4).map_or(8, |a| a.parse().unwrap());

    let engine = LogEngine::open_with(
        dir,
        EngineOptions {
            durability: Durability::Process,
            segment_size: seg_mb * 1024 * 1024,
            ..Default::default()
        },
    )
    .expect("open engine");
    let rt = rt();

    let batches = events / per_batch;
    let mut heads: Vec<Version> = vec![Version::NoStream; streams as usize];

    // Warm the interners/streams first so the measured loop is the hot path.
    let started = Instant::now();
    let a0 = ALLOCS.load(Ordering::Relaxed);
    rt.block_on(async {
        let mut gp = 0u64;
        for b in 0..batches {
            let s = (b % streams) as usize;
            let recs: Vec<RecordToAppend> = (0..per_batch)
                .map(|k| RecordToAppend {
                    message_type: "acct.evt".to_string(),
                    data:         msgpack_payload(gp + k, payload),
                })
                .collect();
            let out = engine
                .append_batch(&stream_name(s as u64), heads[s], &recs)
                .await
                .expect("append");
            heads[s] = out.version;
            gp += per_batch;
        }
    });
    let a1 = ALLOCS.load(Ordering::Relaxed);
    let secs = started.elapsed().as_secs_f64();
    // Let the background roll-sealer drain before dropping (drop also drains).
    drop(engine);
    let total = batches * per_batch;
    println!(
        "OWB seed events={total} streams={streams} per_batch={per_batch} \
         payload={payload} wall_s={secs:.3} ev_per_s={:.0} \
         allocs_per_event={:.2} vm_hwm_kib={}",
        total as f64 / secs,
        (a1 - a0) as f64 / total as f64,
        vm_hwm_kib()
    );
}

fn open_phase(dir: &str) {
    let started = Instant::now();
    let engine = open(dir);
    let secs = started.elapsed().as_secs_f64();
    println!(
        "OWB open wall_s={secs:.4} total_events={} sealed_segments={} \
         vm_hwm_kib={}",
        engine.total_events(),
        engine.sealed_segment_count(),
        vm_hwm_kib()
    );
}

fn hot(dir: &str, args: &[String]) {
    let iters: usize = args.first().map_or(200, |a| a.parse().unwrap());
    let engine = open(dir);
    let rt = rt();
    // The fattest stream is stream 0 (round-robin seeding).
    let name = stream_name(0);
    let mut total_records = 0usize;
    // Warm once.
    rt.block_on(async {
        let _ = engine.read_stream(&name, Version::NoStream, 1_000_000).await;
    });
    let started = Instant::now();
    rt.block_on(async {
        for _ in 0..iters {
            let mut after = Version::NoStream;
            loop {
                let page = engine
                    .read_stream(&name, after, 512)
                    .await
                    .expect("read_stream");
                if page.is_empty() {
                    break;
                }
                total_records += page.len();
                after = Version::At(page.last().unwrap().stream_position);
                if page.len() < 512 {
                    break;
                }
            }
        }
    });
    let secs = started.elapsed().as_secs_f64();
    println!(
        "OWB hot iters={iters} records={total_records} wall_s={secs:.4} \
         ev_per_s={:.0} us_per_load={:.1} vm_hwm_kib={}",
        total_records as f64 / secs,
        secs * 1e6 / iters as f64,
        vm_hwm_kib()
    );
}

fn replay(dir: &str) {
    let engine = open(dir);
    let rt = rt();

    // Global replay: page through everything.
    let started = Instant::now();
    let mut n = 0u64;
    let mut sum = 0u64;
    rt.block_on(async {
        let mut after: Option<u64> = None;
        loop {
            let page =
                engine.read_global(after, 4096).await.expect("read_global");
            if page.is_empty() {
                break;
            }
            n += page.len() as u64;
            sum += page.iter().map(|r| r.data.len() as u64).sum::<u64>();
            after = Some(page.last().unwrap().global_position);
        }
    });
    let g_secs = started.elapsed().as_secs_f64();

    // Stream replay: full load of every stream.
    let started = Instant::now();
    let mut sn = 0u64;
    rt.block_on(async {
        for s in 0.. {
            let name = stream_name(s);
            let mut after = Version::NoStream;
            let mut got_any = false;
            loop {
                let page = engine
                    .read_stream(&name, after, 4096)
                    .await
                    .expect("read_stream");
                if page.is_empty() {
                    break;
                }
                got_any = true;
                sn += page.len() as u64;
                after = Version::At(page.last().unwrap().stream_position);
                if page.len() < 4096 {
                    break;
                }
            }
            if !got_any {
                break;
            }
        }
    });
    let s_secs = started.elapsed().as_secs_f64();
    println!(
        "OWB replay global_events={n} global_ev_per_s={:.0} bytes={sum} \
         stream_events={sn} stream_ev_per_s={:.0} vm_hwm_kib={}",
        n as f64 / g_secs,
        sn as f64 / s_secs,
        vm_hwm_kib()
    );
}

fn point(dir: &str, args: &[String]) {
    let n: u64 = args.first().map_or(20_000, |a| a.parse().unwrap());
    let engine = open(dir);
    let rt = rt();
    let total = engine.total_events() as u64;
    let mut x = 0x243F_6A88_85A3_08D3u64; // xorshift seed
    let started = Instant::now();
    let mut got = 0u64;
    rt.block_on(async {
        for _ in 0..n {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let gp = x % total.max(1);
            let page = engine
                .read_global(Some(gp.saturating_sub(1)), 1)
                .await
                .expect("read_global");
            got += page.len() as u64;
        }
    });
    let secs = started.elapsed().as_secs_f64();
    println!(
        "OWB point n={n} got={got} wall_s={secs:.4} us_per_read={:.2} \
         vm_hwm_kib={}",
        secs * 1e6 / n as f64,
        vm_hwm_kib()
    );
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = args.first().map(String::as_str).unwrap_or("");
    let dir = args.get(1).cloned().unwrap_or_default();
    let rest = &args[2.min(args.len())..];
    match cmd {
        "seed" => seed(&dir, rest),
        "open" => open_phase(&dir),
        "hot" => hot(&dir, rest),
        "replay" => replay(&dir),
        "point" => point(&dir, rest),
        _ => {
            eprintln!("usage: owb_bench <seed|open|hot|replay|point> <dir> …");
            std::process::exit(2);
        }
    }
}
