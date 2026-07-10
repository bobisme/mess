//! Buffered append: `Durability::Process` (write(2) into page cache, no
//! barrier). Ported from `mess-log/examples/durable_bench.rs`'s `buffered`
//! mode. Full-size params (4 writers x 100-event batches x 5,000
//! batches/writer = 2,000,000 events, best-of-5) match the
//! `mess_log.buffered.ev_per_s` row in `docs/perf/envelope.md` exactly.

use std::path::Path;

use mess_log::committer::Durability;

use super::common::drive_committer;
use crate::{Metric, RunSize};

pub fn run(size: RunSize, scratch: &Path) -> Vec<Metric> {
    let (writers, batch, batches_per_writer, reps) = match size {
        RunSize::Full => (4u64, 100usize, 5_000u64, 5usize),
        RunSize::Smoke => (2u64, 10usize, 5u64, 1usize),
    };
    let dir = scratch.join("buffered");
    let r = drive_committer(
        &dir,
        "buffered",
        Durability::Process,
        writers,
        batch,
        batches_per_writer,
        reps,
    );
    let _ = std::fs::remove_dir_all(&dir);

    vec![Metric::new(
        "mess_log.buffered.ev_per_s",
        r.best_ev_per_s,
        "ev/s",
        format!(
            "Durability::Process; {writers} writers x {batch}-event batches x {batches_per_writer}/writer \
             = {} events; ~250B payload; real-fs scratch (see machine.scratch_fs); best-of-{reps}",
            r.total_events
        ),
    )]
}
