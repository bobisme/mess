//! Recovery time: full whole-log recovery wall time over a synthetic
//! multi-segment corpus on the real fs, reported as s/GiB (the unit
//! `docs/perf/envelope.md`'s bn-2en round used and the unit bn-4pk's
//! acceptance criteria names: "recovery <=0.5 s/GiB"). Ported from
//! `mess-log/examples/recover_bench.rs`. Full-size corpus (8 segments x
//! ~64MiB = ~512MiB, ~60KiB payloads so the scan is CPU- not I/O-bound)
//! matches that example's default.

use std::path::Path;
use std::time::Instant;

use mess_log::encode::Subframe;
use mess_log::recover_all::{
    RecoverOptions, RecoveryMode, SegmentFile, recover_whole_log,
};
use mess_log::runtime::real::RealFs;
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};

use crate::{Metric, RunSize};

fn build(dir: &Path, n_segs: usize, seg_bytes: u64) -> (Vec<SegmentFile>, u64) {
    let fs = RealFs;
    let path = |i: usize| dir.join(format!("seg-{i:04}"));
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
        while matches!(cur.would_fit(&mk_spec(version)), Ok(true)) {
            let r = cur.append(&mk_spec(version)).unwrap();
            version += u64::from(r.frame_count);
        }
        total_content += cur.summary().content_len;
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

pub fn run(size: RunSize, scratch: &Path) -> Vec<Metric> {
    let (n_segs, seg_mib): (usize, u64) = match size {
        RunSize::Full => (8, 64),
        RunSize::Smoke => (2, 1),
    };
    let seg_bytes = seg_mib * 1024 * 1024;

    let dir = scratch.join("recovery");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("mkdir");

    let (segs, total_content) = build(&dir, n_segs, seg_bytes);
    let gib = total_content as f64 / (1024.0 * 1024.0 * 1024.0);

    let fs = RealFs;
    let serial_opts =
        RecoverOptions { mode: RecoveryMode::Full, parallel: false };
    let parallel_opts =
        RecoverOptions { mode: RecoveryMode::Full, parallel: true };
    // Warm the page cache so the measured runs time CPU scan, not first-touch
    // I/O.
    let _ = recover_whole_log(&fs, &segs, None, serial_opts).unwrap();

    let start = Instant::now();
    let whole = recover_whole_log(&fs, &segs, None, serial_opts).unwrap();
    let serial_dt = start.elapsed();
    std::hint::black_box(&whole);

    // The gated metric: per-segment parallel recovery (R1, §8.2), the
    // production full-recovery path on a multi-segment cold-open corpus —
    // this is the number the "recovery <=0.5 s/GiB" floor (origin:
    // spikes/perf_replay's 0.32-0.38 s/GiB round-4 measurement) was set
    // against. Determinism note (recover_all.rs module docs): the parallel
    // path is stitched deterministically by segment_id regardless of
    // completion order — only the deterministic-simulation harness forces
    // `parallel: false`, which is a correctness-testing constraint, not a
    // production one.
    let start = Instant::now();
    let whole_p = recover_whole_log(&fs, &segs, None, parallel_opts).unwrap();
    let parallel_dt = start.elapsed();
    std::hint::black_box(&whole_p);

    let serial_s_per_gib = serial_dt.as_secs_f64() / gib.max(1e-12);
    let parallel_s_per_gib = parallel_dt.as_secs_f64() / gib.max(1e-12);

    let _ = std::fs::remove_dir_all(&dir);

    vec![
        Metric::new(
            "recovery.s_per_gib",
            parallel_s_per_gib,
            "s/GiB",
            format!(
                "full recovery (RecoveryMode::Full, per-segment parallel / \
                 R1), {n_segs} segments x ~{seg_mib}MiB (~60KiB payloads, \
                 CPU-bound scan), {gib:.3} GiB content, page cache warmed; \
                 real-fs scratch"
            ),
        ),
        Metric::new(
            "recovery.wall_s",
            parallel_dt.as_secs_f64(),
            "s",
            format!("same run: absolute wall time over {gib:.3} GiB, parallel"),
        ),
        Metric::new(
            "recovery.serial.s_per_gib",
            serial_s_per_gib,
            "s/GiB",
            format!(
                "same corpus, RecoveryMode::Full serial (parallel: false) — \
                 informational, not floor-gated; {gib:.3} GiB content"
            ),
        ),
    ]
}
