//! Bench (bn-2en, R1): parallel vs serial whole-log **full** recovery wall time
//! on a synthetic multi-segment corpus, on the real fs.
//!
//! Full recovery (§8.2) scans and CRC-validates every batch of every segment,
//! so it is the CPU-bound path R1 parallelizes. This builds a chain of sealed
//! segments on a temp dir, then times `recover_whole_log(Full, parallel=false)`
//! against `parallel=true` and reports wall time + throughput (GiB/s) for each.
//!
//! Tunables (env): `MESS_BENCH_SEGS` (segment count, default 8),
//! `MESS_BENCH_SEG_MIB` (approx MiB per segment, default 64). Default corpus is
//! ~512 MiB; raise for a multi-GiB run.
//!
//! Run: `cargo run -p mess-log --release --example recover_bench`.

use std::path::{Path, PathBuf};
use std::time::Instant;

use mess_log::encode::Subframe;
use mess_log::recover_all::{
    RecoverOptions, RecoveryMode, SegmentFile, recover_whole_log,
};
use mess_log::runtime::real::RealFs;
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn build(dir: &Path, n_segs: usize, seg_bytes: u64) -> (Vec<SegmentFile>, u64) {
    let fs = RealFs;
    let path = |i: usize| dir.join(format!("seg-{i:04}"));
    // ~60 KiB payload ⇒ each batch ~60 KiB, so a 64 MiB segment holds ~1000
    // batches — enough tiling/CRC work to be CPU-bound, cheap to build.
    let payload = vec![0x5Au8; 60 * 1024];
    let subs = [Subframe::plain(0x11, 0, 0, &payload)];
    let mk_spec = |v: u64| BatchSpec {
        stream_id:            1,
        category_id:          101,
        first_stream_version: v,
        crypto_chain:         None,
        subframes:            &subs,
    };

    let mut segs = Vec::new();
    let mut params = SegmentParams::new(1, 0, 10, 0);
    params.segment_size = seg_bytes;
    let mut w = Some(SegmentWriter::create(&fs, &path(0), params).unwrap());
    segs.push(SegmentFile::new(1, path(0)));
    let mut total_content = 0u64;
    let mut version = 0u64;

    for i in 0..n_segs {
        let mut cur = w.take().unwrap();
        // Fill this segment until the next batch would not fit.
        while matches!(cur.would_fit(&mk_spec(version)), Ok(true)) {
            let r = cur.append(&mk_spec(version)).unwrap();
            version += u64::from(r.frame_count);
        }
        total_content += cur.summary().content_len;
        // Seal every segment (full recovery scans them all regardless), then
        // open the next with the same forced small size. `roll_sealed` would
        // reset to the default 256 MiB, so seal + create manually.
        let summary = cur.seal().unwrap();
        if i + 1 < n_segs {
            let id = (i + 2) as u64;
            let mut np =
                SegmentParams::new(id, summary.end_pos, 10 + id, summary.epoch);
            np.segment_size = seg_bytes;
            w = Some(SegmentWriter::create(&fs, &path(i + 1), np).unwrap());
            segs.push(SegmentFile::new(id, path(i + 1)));
        }
    }
    (segs, total_content)
}

fn time_recovery(
    fs: &RealFs,
    segs: &[SegmentFile],
    parallel: bool,
) -> std::time::Duration {
    let opts = RecoverOptions { mode: RecoveryMode::Full, parallel };
    let start = Instant::now();
    let whole = recover_whole_log(fs, segs, None, opts).unwrap();
    let dur = start.elapsed();
    // Consume the result so the scan is not optimized away.
    std::hint::black_box(&whole);
    dur
}

fn main() {
    let n_segs = env_u64("MESS_BENCH_SEGS", 8) as usize;
    let seg_bytes = env_u64("MESS_BENCH_SEG_MIB", 64) * 1024 * 1024;

    let dir: PathBuf = {
        let mut p = std::env::temp_dir();
        p.push(format!("mess-recover-bench-{}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        p
    };

    eprintln!(
        "building corpus: {n_segs} segments × ~{} MiB ...",
        seg_bytes / (1024 * 1024)
    );
    let (segs, total_content) = build(&dir, n_segs, seg_bytes);
    let gib = total_content as f64 / (1024.0 * 1024.0 * 1024.0);
    eprintln!(
        "corpus: {segs_len} segments, {gib:.3} GiB of content",
        segs_len = segs.len()
    );

    let fs = RealFs;
    // Warm the page cache so both runs measure CPU scan, not first-touch I/O.
    let _ = time_recovery(&fs, &segs, false);

    let serial = time_recovery(&fs, &segs, false);
    let parallel = time_recovery(&fs, &segs, true);

    let thr = |d: std::time::Duration| gib / d.as_secs_f64();
    println!("| recovery      | wall (s) | GiB/s | s/GiB |");
    println!("|---------------|----------|-------|-------|");
    println!(
        "| serial        | {:8.3} | {:5.2} | {:5.3} |",
        serial.as_secs_f64(),
        thr(serial),
        serial.as_secs_f64() / gib
    );
    println!(
        "| parallel ({n}) | {:8.3} | {:5.2} | {:5.3} |",
        parallel.as_secs_f64(),
        thr(parallel),
        parallel.as_secs_f64() / gib,
        n = n_segs,
    );
    println!(
        "speedup: {:.2}×",
        serial.as_secs_f64() / parallel.as_secs_f64().max(1e-9)
    );

    let _ = std::fs::remove_dir_all(&dir);
}
