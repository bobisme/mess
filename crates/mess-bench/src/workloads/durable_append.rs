//! Durable append: `Durability::group_default()` (D7 early-close group
//! commit, `fdatasync`/group). Ported from
//! `mess-log/examples/durable_bench.rs`'s `durable` mode. Full-size params
//! (4 writers x 100-event batches x 500 batches/writer = 200,000 events,
//! best-of-5) match `mess_log.durable.ev_per_s` in `docs/perf/envelope.md`.
//!
//! MUST run on a real fs — caller ([`crate::run_all`]'s caller) is required
//! to have already run [`crate::assert_real_fs`] over the shared scratch
//! root before any workload runs.

use std::path::Path;

use mess_log::committer::Durability;

use super::common::drive_committer;
use crate::{Metric, RunSize};

pub fn run(size: RunSize, scratch: &Path) -> Vec<Metric> {
    let (writers, batch, batches_per_writer, reps) = match size {
        RunSize::Full => (4u64, 100usize, 500u64, 5usize),
        RunSize::Smoke => (2u64, 5usize, 3u64, 1usize),
    };
    let dir = scratch.join("durable");
    let r = drive_committer(
        &dir,
        "durable",
        Durability::group_default(),
        writers,
        batch,
        batches_per_writer,
        reps,
    );
    let _ = std::fs::remove_dir_all(&dir);

    let ev_per_fsync = if r.fsyncs_of_best > 0 {
        r.total_events as f64 / r.fsyncs_of_best as f64
    } else {
        0.0
    };

    vec![
        Metric::new(
            "mess_log.durable.ev_per_s",
            r.best_ev_per_s,
            "ev/s",
            format!(
                "Durability::group_default (D7 early-close, fdatasync/group); {writers} writers x \
                 {batch}-event batches x {batches_per_writer}/writer = {} events; ~250B payload; \
                 real-fs scratch (see machine.scratch_fs); best-of-{reps}; ev/fsync {ev_per_fsync:.0}",
                r.total_events
            ),
        ),
        Metric::new(
            "mess_log.durable.mean_fsync_us",
            r.mean_fsync_us_of_best,
            "us",
            "one durability barrier, best-of-N rep".to_string(),
        ),
    ]
}
