//! Spike D bench/gate driver.
//!
//! ```text
//! bench corpus <histories> [threads]   # the >=100k digest-equivalence gate
//! bench rate <capsules> <pool>         # review-S3 effect apply-rate gate + scaling
//! bench open <capsules> <pool>         # checkpoint open <= 10% of full scan
//! bench effectbytes <histories>        # effect bytes per touched stream
//! bench incr <streams>                 # dirty-page proportionality
//! bench info                           # host line
//! ```
//!
//! Every measured phase is preceded by the competing-load quiet guard
//! (no rustc/cc/ld anywhere, no cargo/bench outside our ancestor chain,
//! load1 < 6.0 — this host carries ~4-5 ambient load; see
//! spikes/epoch_dedupe/REPORT.md §3).

use std::time::Instant;

use segment_effect::checkpoint::{self, FsDir, MemDir};
use segment_effect::hist;
use segment_effect::kernel::KernelState;
use segment_effect::model::Log;
use segment_effect::oracle::OracleState;
use segment_effect::recover::{
    self, build_all, build_all_parallel, reduce_ordered_parallel,
    reduce_ordered_tree,
};
use segment_effect::timing::ensure_quiet;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(String::as_str).unwrap_or("help");
    match cmd {
        "corpus" => corpus(
            args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100_000),
            args.get(3).and_then(|s| s.parse().ok()).unwrap_or(num_cpus()),
        ),
        "rate" => rate(
            args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10_000_000),
            args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1_000_000),
        ),
        "open" => open_gate(
            args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10_000_000),
            args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1_000_000),
        ),
        "effectbytes" => effectbytes(
            args.get(2).and_then(|s| s.parse().ok()).unwrap_or(500),
        ),
        "incr" => incr(
            args.get(2).and_then(|s| s.parse().ok()).unwrap_or(4_000_000),
        ),
        "info" => info(),
        _ => {
            eprintln!(
                "usage: bench corpus|rate|open|effectbytes|incr|info [args]"
            );
        }
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8)
}

fn info() {
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("model name"))
        .map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string())
        .unwrap_or_default();
    println!(
        "INFO cpu=\"{cpu}\" threads={} load1={:.2}",
        num_cpus(),
        segment_effect::timing::load1()
    );
}

// ---------------------------------------------------------------------------
// corpus: the >=100k randomized-history digest-equivalence gate
// ---------------------------------------------------------------------------

fn corpus(histories: u64, threads: usize) {
    println!(
        "CORPUS start histories={histories} threads={threads} \
         (distribution: 97% [100,1e3], 2.9% (1e3,1e4], 0.1% (1e4,1e5] capsules)"
    );
    let t0 = Instant::now();
    let done = std::sync::atomic::AtomicU64::new(0);
    let capsules_total = std::sync::atomic::AtomicU64::new(0);
    let results: Vec<(u64, u64, u64)> = std::thread::scope(|s| {
        let mut handles = Vec::new();
        for t in 0..threads as u64 {
            let done = &done;
            let capsules_total = &capsules_total;
            handles.push(s.spawn(move || {
                let mut ok = 0u64;
                let mut rescans = 0u64;
                let mut ckpt_fallbacks = 0u64;
                let mut seed = t;
                while seed < histories {
                    let (r, c) = recover::differential_check(seed);
                    rescans += r.0;
                    ckpt_fallbacks += r.1;
                    ok += 1;
                    capsules_total
                        .fetch_add(c, std::sync::atomic::Ordering::Relaxed);
                    let d = done
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                        + 1;
                    if d % 10_000 == 0 {
                        eprintln!("  ... {d}/{histories}");
                    }
                    seed += threads as u64;
                }
                (ok, rescans, ckpt_fallbacks)
            }));
        }
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let ok: u64 = results.iter().map(|r| r.0).sum();
    let rescans: u64 = results.iter().map(|r| r.1).sum();
    let fallbacks: u64 = results.iter().map(|r| r.2).sum();
    println!(
        "CORPUS RESULT histories={ok}/{histories} all-digests-identical \
         capsules={} v6-rescans={rescans} v7-effects-fallbacks={fallbacks} \
         wall={:.1}s",
        capsules_total.load(std::sync::atomic::Ordering::Relaxed),
        t0.elapsed().as_secs_f64()
    );
}

// ---------------------------------------------------------------------------
// rate: review-S3 — ordered effect compose+apply head-transition rate
// ---------------------------------------------------------------------------

fn rate(capsules: u64, pool: u64) {
    println!("RATE gen capsules={capsules} pool={pool}");
    let h = hist::generate_with(0xda7a, capsules, pool);
    let log = &h.log;
    let nseg = log.segments.len();
    println!("RATE segments={nseg}");

    // Build phase (single thread), reported for context.
    ensure_quiet(1800);
    let t = Instant::now();
    let effects = build_all(log).expect("build");
    let build_s = t.elapsed().as_secs_f64();
    let transitions: u64 = effects.iter().map(|e| e.heads.len() as u64).sum();
    println!(
        "RATE build-1t wall={build_s:.3}s capsules/s={:.2}M transitions={transitions}",
        capsules as f64 / build_s / 1e6
    );

    // THE GATE: ordered compose+apply, single thread, sequential apply.
    ensure_quiet(1800);
    let t = Instant::now();
    let mut st = KernelState::new(log.dedupe_span, log.epoch_span);
    for e in &effects {
        st.apply_effect(e).expect("apply");
    }
    let seq_s = t.elapsed().as_secs_f64();
    let seq_digest = st.digest();
    println!(
        "RATE seq-apply-1t wall={seq_s:.3}s head-transitions/s={:.2}M  [gate >= 5M]",
        transitions as f64 / seq_s / 1e6
    );

    // Tree-reduce + single apply, single thread.
    ensure_quiet(1800);
    let t = Instant::now();
    let one = reduce_ordered_tree(&effects).expect("reduce");
    let mut st = KernelState::new(log.dedupe_span, log.epoch_span);
    st.apply_effect(&one).expect("apply reduced");
    let tree_s = t.elapsed().as_secs_f64();
    assert_eq!(st.digest(), seq_digest, "tree reduce diverged");
    println!(
        "RATE tree-reduce+apply-1t wall={tree_s:.3}s head-transitions/s={:.2}M",
        transitions as f64 / tree_s / 1e6
    );

    // End-to-end parallel scaling: build (parallel map) + ordered parallel
    // reduce + apply, at 1/2/4/8 threads.
    for threads in [1usize, 2, 4, 8] {
        ensure_quiet(1800);
        let t = Instant::now();
        let effects = build_all_parallel(log, threads).expect("build");
        let build_p = t.elapsed().as_secs_f64();
        let t = Instant::now();
        let one = reduce_ordered_parallel(&effects, threads).expect("reduce");
        let mut st = KernelState::new(log.dedupe_span, log.epoch_span);
        st.apply_effect(&one).expect("apply");
        let reduce_p = t.elapsed().as_secs_f64();
        assert_eq!(st.digest(), seq_digest, "parallel diverged at {threads}t");
        println!(
            "RATE parallel t={threads} build={build_p:.3}s reduce+apply={reduce_p:.3}s \
             total={:.3}s capsules/s={:.2}M transitions/s(reduce)={:.2}M",
            build_p + reduce_p,
            capsules as f64 / (build_p + reduce_p) / 1e6,
            transitions as f64 / reduce_p / 1e6
        );
    }
}

// ---------------------------------------------------------------------------
// open: checkpoint open <= 10% of full-scan fold
// ---------------------------------------------------------------------------

fn open_gate(capsules: u64, pool: u64) {
    println!("OPEN gen capsules={capsules} pool={pool}");
    let h = hist::generate_with(0x0be1, capsules, pool);
    let log = &h.log;
    let nseg = log.segments.len();

    // Full sequential scan fold — BOTH implementations; the kernel fold is
    // the faster and therefore the conservative denominator. (This is a
    // pure in-memory fold: the production full scan additionally pays I/O,
    // CRC, and byte-decode, so a pass here is conservative.)
    ensure_quiet(1800);
    let t = Instant::now();
    let mut ost = OracleState::new(log.dedupe_span);
    ost.fold(&log.capsules).expect("oracle fold");
    let oracle_s = t.elapsed().as_secs_f64();
    let d_oracle = ost.digest();
    drop(ost);
    println!("OPEN full-scan oracle-fold wall={oracle_s:.3}s");

    ensure_quiet(1800);
    let t = Instant::now();
    let mut kst = KernelState::new(log.dedupe_span, log.epoch_span);
    kst.fold(&log.capsules).expect("kernel fold");
    let kernel_s = t.elapsed().as_secs_f64();
    assert_eq!(kst.digest(), d_oracle);
    drop(kst);
    println!("OPEN full-scan kernel-fold wall={kernel_s:.3}s (denominator)");

    // Checkpoint at the boundary covering ~99% of capsules; suffix = the
    // rest (the "active tail").
    let target = (capsules as usize) * 99 / 100;
    let cut = log
        .segments
        .iter()
        .position(|s| s.hi >= target)
        .map(|i| i + 1)
        .unwrap_or(nseg);
    let upto = log.segments[cut - 1].hi;
    let mut st = KernelState::new(log.dedupe_span, log.epoch_span);
    st.fold(&log.capsules[..upto]).expect("prefix fold");
    let root = scratch_dir();
    let mut dir = FsDir::new(root.clone()).expect("scratch");
    let t = Instant::now();
    let inst = checkpoint::install(&mut dir, &st, None).expect("install");
    println!(
        "OPEN install wall={:.3}s blobs={} bytes={} suffix-capsules={}",
        t.elapsed().as_secs_f64(),
        inst.blobs_written,
        inst.blob_bytes_written,
        log.capsules.len() - upto
    );
    drop(st);

    // Measured phase: §10.4 open + suffix fold, warm fs metadata.
    ensure_quiet(1800);
    let t = Instant::now();
    let (mut st, m) = checkpoint::open(&dir, log).expect("open");
    let suffix = &log.capsules[m.cursor.idx as usize..];
    st.fold(suffix).expect("suffix fold");
    let open_s = t.elapsed().as_secs_f64();
    let d_open = st.digest();
    assert_eq!(d_open, d_oracle, "checkpoint-open state != full fold");
    println!(
        "OPEN checkpoint-open+suffix wall={open_s:.3}s ratio-vs-kernel={:.2}% \
         ratio-vs-oracle={:.2}%  [gate <= 10%]",
        open_s / kernel_s * 100.0,
        open_s / oracle_s * 100.0
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// effectbytes: <= 16 B per touched stream (head component)
// ---------------------------------------------------------------------------

fn effectbytes(histories: u64) {
    let mut tot_head_bytes = 0u64;
    let mut tot_streams = 0u64;
    let mut tot_bytes = 0u64;
    let mut tot_dedupe_control = 0u64;
    let mut worst: f64 = 0.0;
    for seed in 0..histories {
        let h = hist::generate(seed ^ 0xb17e5);
        let effects = build_all(&h.log).expect("valid history");
        for e in &effects {
            if e.heads.is_empty() {
                continue;
            }
            let hb = e.head_section_bytes() as u64;
            let full = e.encode().len() as u64;
            let per = hb as f64 / e.heads.len() as f64;
            worst = worst.max(per);
            tot_head_bytes += hb;
            tot_streams += e.heads.len() as u64;
            tot_bytes += full;
            // dedupe + control payload accounting (excluded by the gate):
            // everything except the head section and the fixed frame.
            let frame = 4 + 64 + 4; // magic + anchors + crc
            tot_dedupe_control += full - hb - frame as u64;
        }
    }
    println!(
        "EFFECTBYTES histories={histories} touched-streams={tot_streams} \
         head-bytes/stream avg={:.2} worst-effect={worst:.2} [gate <= 16] \
         full-effect-bytes/stream={:.2} dedupe+control-bytes={tot_dedupe_control}",
        tot_head_bytes as f64 / tot_streams as f64,
        tot_bytes as f64 / tot_streams as f64
    );
}

// ---------------------------------------------------------------------------
// incr: incremental checkpoint bytes proportional to dirty pages
// ---------------------------------------------------------------------------

fn incr(streams: u64) {
    // Dense heads over `streams` streams -> streams/4096 pages. Full
    // checkpoint, then touch ~1% of PAGES (one stream per touched page) and
    // measure the incremental install's written bytes.
    let mut log_capsules = Vec::new();
    let mut pos = 0u64;
    for sid in 0..streams {
        log_capsules.push(segment_effect::model::Capsule::UserBatch {
            stream_id: sid,
            first_version: 0,
            event_count: 1,
            first_global_pos: pos,
        });
        pos += 1;
    }
    let n = log_capsules.len();
    let log = Log::seal(log_capsules, &[n / 2], 1_000_000);
    let mut st = KernelState::new(log.dedupe_span, log.epoch_span);
    st.fold(&log.capsules).expect("fold");
    let mut dir = MemDir::new();
    let full = checkpoint::install(&mut dir, &st, None).expect("full install");
    st.clear_dirty();
    let pages = streams.div_ceil(segment_effect::PAGE_CELLS);

    for pct in [1u64, 5, 10] {
        let dirty_target = (pages * pct / 100).max(1);
        // Touch one stream on each of `dirty_target` distinct pages, via
        // real capsule applies appended to the log model (positions after
        // the sealed end are fine for the state; we only measure bytes).
        let mut st2 = st.clone();
        for p in 0..dirty_target {
            let sid = p * segment_effect::PAGE_CELLS; // first cell of page p
            let cur = st2.head_count(sid);
            let c = segment_effect::model::Capsule::UserBatch {
                stream_id: sid,
                first_version: cur,
                event_count: 1,
                first_global_pos: st2.cursor.pos,
            };
            st2.apply(&c).expect("touch");
        }
        let mut dir2 = dir.clone();
        let inst = checkpoint::install(&mut dir2, &st2, Some(&full.manifest))
            .expect("incremental install");
        println!(
            "INCR streams={streams} pages={pages} dirty-pages={} ({pct}%) \
             full-bytes={} incr-bytes={} ratio={:.2}% [expect ~{pct}%]",
            dirty_target,
            full.blob_bytes_written,
            inst.blob_bytes_written,
            inst.blob_bytes_written as f64 / full.blob_bytes_written as f64
                * 100.0
        );
    }
}

fn scratch_dir() -> std::path::PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    let root = std::path::PathBuf::from(home)
        .join(".cache/mess-bench")
        .join(format!("segeff-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    root
}
