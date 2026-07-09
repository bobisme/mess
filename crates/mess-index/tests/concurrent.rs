//! Concurrent-reader invariant (bn-25d): with the committer as the sole writer
//! and many readers running concurrently, a reader **never observes an
//! uncommitted entry** — every read returns an exact committed prefix, never a
//! future, torn, or gapped pointer (the D7 discipline `mess_log::reader`
//! enforces on the raw tail, here for the in-memory index).
//!
//! The workload and its group boundaries are generated from `mess_log`'s seeded
//! [`Rng`] so the property is checked across many deterministic seeds. The
//! concurrency itself is real OS threads (the active index is a concurrent
//! structure, not driven by the async executor), so these are
//! `#[cfg_attr(miri, ignore)]` per the bone's thread-test rule.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use mess_index::{ActiveIndex, BatchEntry, EventPtr, GlobalEntry};
use mess_log::runtime::Rng;

const N_STREAMS: u64 = 8;
const N_BATCHES: usize = 160;
const N_READERS: usize = 3;

/// A deterministic workload: a flat batch list in global-position order (dense
/// positions from 0), each batch continuing its stream's versions.
struct Workload {
    plan: Vec<BatchEntry>,
    /// Group boundaries: index i is applied in group `groups[i]`.
    group_ends: Vec<usize>,
    /// Position-ordered global entries the plan will produce.
    expected_global: Vec<GlobalEntry>,
    /// Valid committed head versions per stream (each batch's last_version).
    valid_heads: BTreeMap<u64, HashSet<u64>>,
    /// The final committed batch per stream head → its ptr (for resolve checks).
    head_ptr: BTreeMap<(u64, u64), EventPtr>,
}

fn generate(seed: u64) -> Workload {
    let mut rng = Rng::new(seed);
    let mut next_version: BTreeMap<u64, u64> = BTreeMap::new();
    let mut pos: u64 = 0;
    let mut off: u64 = 52; // pretend segment header length
    let mut plan = Vec::with_capacity(N_BATCHES);
    let mut valid_heads: BTreeMap<u64, HashSet<u64>> = BTreeMap::new();
    let mut head_ptr: BTreeMap<(u64, u64), EventPtr> = BTreeMap::new();

    for _ in 0..N_BATCHES {
        let stream_id = rng.below(N_STREAMS);
        let n = 1 + rng.below(4) as u32;
        let first_version = *next_version.get(&stream_id).unwrap_or(&0);
        let ptr = EventPtr { segment_id: 1, offset: off };
        let b = BatchEntry {
            stream_id,
            first_stream_version: first_version,
            frame_count: n,
            first_global_pos: pos,
            ptr,
        };
        let last_version = first_version + u64::from(n) - 1;
        valid_heads.entry(stream_id).or_default().insert(last_version);
        head_ptr.insert((stream_id, last_version), ptr);
        next_version.insert(stream_id, first_version + u64::from(n));
        pos += u64::from(n);
        off += u64::from(n) * 96 + 88; // arbitrary monotone byte accounting
        plan.push(b);
    }

    // Random group boundaries so the writer publishes at varied granularity.
    let mut group_ends = Vec::new();
    let mut i = 0usize;
    while i < plan.len() {
        let step = 1 + rng.below(6) as usize;
        i = (i + step).min(plan.len());
        group_ends.push(i);
    }

    let expected_global = plan
        .iter()
        .map(|b| GlobalEntry {
            first_global_pos: b.first_global_pos,
            frame_count: b.frame_count,
            stream_id: b.stream_id,
            ptr: b.ptr,
        })
        .collect();

    Workload { plan, group_ends, expected_global, valid_heads, head_ptr }
}

/// Assert one reader observation is a valid committed prefix (never torn,
/// future, or gapped).
fn check_reader_view(index: &ActiveIndex, wl: &Workload) {
    // 1. The global view is an EXACT prefix of the plan: same entries, same
    //    order, dense positions. This is the core "no uncommitted entry" proof.
    let g = index.global_committed();
    assert!(g.len() <= wl.expected_global.len());
    for (i, e) in g.iter().enumerate() {
        assert_eq!(*e, wl.expected_global[i], "global entry {i} diverged from plan");
    }
    // Density: each entry begins exactly where the previous ended.
    for w in g.windows(2) {
        assert_eq!(w[0].end_pos(), w[1].first_global_pos, "gap in committed global view");
    }

    // 2. Every per-stream head is a real committed head, and resolves to the
    //    planned pointer — never a version the writer has not committed.
    for stream in 0..N_STREAMS {
        if let Some(head) = index.stream_head(stream) {
            let valid = wl.valid_heads.get(&stream).expect("stream present in plan");
            assert!(valid.contains(&head), "stream {stream} head {head} not a committed head");
            let want = wl.head_ptr[&(stream, head)];
            assert_eq!(index.resolve(stream, head), Some(want), "stream {stream} resolve(head)");
        }
    }
}

fn run_seed(seed: u64) {
    let wl = generate(seed);
    let index = Arc::new(ActiveIndex::new());
    let done = Arc::new(AtomicBool::new(false));

    std::thread::scope(|scope| {
        // Readers: hammer the index until the writer signals done, then one
        // last check on the fully-applied index.
        for _ in 0..N_READERS {
            let index = Arc::clone(&index);
            let done = Arc::clone(&done);
            let wl = &wl;
            scope.spawn(move || {
                let mut last_end = 0u64;
                while !done.load(Ordering::Acquire) {
                    // applied_end is monotone from any single reader's view.
                    let e = index.applied_end();
                    assert!(e >= last_end, "applied_end regressed {last_end} -> {e}");
                    last_end = e;
                    check_reader_view(&index, wl);
                }
                check_reader_view(&index, wl);
            });
        }

        // Sole writer: apply the plan group by group, advancing the watermark
        // to each group's cumulative durable end.
        let index_w = Arc::clone(&index);
        let done_w = Arc::clone(&done);
        let wl_w = &wl;
        scope.spawn(move || {
            let mut start = 0usize;
            for &end in &wl_w.group_ends {
                let group = &wl_w.plan[start..end];
                let watermark = group.last().map(|b| b.end_pos()).unwrap_or(0);
                index_w.apply_committed(watermark, group);
                start = end;
                std::thread::yield_now();
            }
            done_w.store(true, Ordering::Release);
        });
    });

    // Final differential: the concurrently-built index equals a clean
    // single-shot build of the whole plan.
    let reference = ActiveIndex::new();
    let final_pos = wl.plan.last().map(|b| b.end_pos()).unwrap_or(0);
    reference.apply_committed(final_pos, &wl.plan);
    assert_eq!(index.snapshot(), reference.snapshot(), "seed {seed} final differential");
}

#[test]
#[cfg_attr(miri, ignore)]
fn concurrent_readers_see_only_committed_entries_over_seeds() {
    for seed in 1..=24u64 {
        run_seed(seed);
    }
}
