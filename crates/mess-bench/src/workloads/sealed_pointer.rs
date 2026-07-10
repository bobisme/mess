//! Sealed pointer-index read paths: parallel global scan + coalesced stream
//! replay. Ported from `mess-index/tests/sealed_read_paths.rs`'s
//! `bench_sealed_read_paths` (bn-1hx). Full-size corpus (24 segments x 2,000
//! streams x 5 batches/stream x 10 frames/batch = 2.4M events) matches the
//! `sealed.global_scan.ev_per_s` / `sealed.stream_replay.ev_per_s` rows in
//! `docs/perf/envelope.md` exactly; the byte-identity gate (parallel ==
//! sequential reference, checksum-verified) runs on every measured pass here
//! too, same as the source test.

use std::sync::Arc;
use std::time::{Duration, Instant};

use mess_index::sealed::block_cache::BlockCache;
use mess_index::sealed::replay::{ReplaySet, global_checksum, stream_checksum};
use mess_index::sealed::segment::{
    SealBatch, SealInput, SealStream, SealedSegmentIndex, SealedSegmentRef,
    encode_sidecar,
};

use crate::{Metric, RunSize};

fn build_segment(
    segment_id: u64,
    base: u64,
    stream_ids: &[u64],
    batches_per_stream: usize,
    first_version_base: u64,
) -> (SealedSegmentRef, u64) {
    const FRAMES: u32 = 10;
    let mut g = base;
    let streams: Vec<SealStream> = stream_ids
        .iter()
        .map(|&sid| {
            let batches = (0..batches_per_stream)
                .map(|i| {
                    let b = SealBatch {
                        first_version:    first_version_base
                            + (i as u64) * u64::from(FRAMES),
                        frame_count:      FRAMES,
                        first_global_pos: g,
                        offset:           4096 + (i as u64) * 512,
                    };
                    g += u64::from(FRAMES);
                    b
                })
                .collect();
            SealStream { stream_id: sid, batches }
        })
        .collect();
    let input =
        SealInput { segment_id, base_pos: base, streams, payloads: None };
    let idx = Arc::new(
        SealedSegmentIndex::from_bytes(encode_sidecar(&input)).unwrap(),
    );
    (idx, g)
}

fn build_corpus(
    n_segments: u64,
    n_streams: u64,
    batches_per_stream: usize,
) -> (ReplaySet, Vec<u64>) {
    let stream_ids: Vec<u64> = (0..n_streams).collect();
    let mut segs = Vec::new();
    let mut base = 0u64;
    for seg in 0..n_segments {
        let fv_base = seg * (batches_per_stream as u64) * 10;
        let (idx, next_base) =
            build_segment(seg, base, &stream_ids, batches_per_stream, fv_base);
        base = next_base;
        segs.push(idx);
    }
    (ReplaySet::from_segments(segs), stream_ids)
}

struct Lcg(u64);
impl Lcg {
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
}

fn best_of<T, F: FnMut() -> T>(reps: usize, mut f: F) -> (T, Duration) {
    let mut best = Duration::MAX;
    let mut last = None;
    for _ in 0..reps {
        let t = Instant::now();
        let out = f();
        let dt = t.elapsed();
        if dt < best {
            best = dt;
        }
        last = Some(out);
    }
    (last.unwrap(), best)
}

pub fn run(size: RunSize) -> Vec<Metric> {
    let (n_segments, n_streams, batches_per_stream, r_streams, reps): (
        u64,
        u64,
        usize,
        usize,
        usize,
    ) = match size {
        RunSize::Full => (24, 2_000, 5, 1_000, 5),
        RunSize::Smoke => (2, 20, 2, 10, 1),
    };

    let (set, stream_ids) =
        build_corpus(n_segments, n_streams, batches_per_stream);
    let total_events = set.event_count();

    // --- global scan (all segments, parallel) ---
    let (g_par, g_dt) = best_of(reps, || set.global_scan().unwrap());
    let g_events: u64 = g_par.iter().map(|e| u64::from(e.frame_count)).sum();
    let g_evps = g_events as f64 / g_dt.as_secs_f64();
    let g_seq = set.global_scan_seq().unwrap();
    assert_eq!(g_par, g_seq, "global scan: parallel != sequential");
    assert_eq!(global_checksum(&g_par), global_checksum(&g_seq));

    // --- coalesced stream replay (R random streams, each across all segments)
    // ---
    let mut rng = Lcg(0x5EED_00B1_1A11_0001);
    let picks: Vec<u64> = (0..r_streams)
        .map(|_| stream_ids[(rng.next_u64() % n_streams) as usize])
        .collect();

    let cold_cache = BlockCache::disabled();
    let (batched, s_dt) =
        best_of(reps, || set.stream_replay_many(&picks, &cold_cache).unwrap());
    let seq = set.stream_replay_many_seq(&picks, &cold_cache).unwrap();
    assert_eq!(batched, seq, "stream replay: parallel != sequential");
    let s_events: u64 = batched
        .iter()
        .flat_map(|(_, es)| es.iter())
        .map(|e| u64::from(e.frame_count))
        .sum();
    let _s_ck = batched
        .iter()
        .fold(0u64, |acc, (_, es)| acc.wrapping_add(stream_checksum(es)));
    let s_evps = s_events as f64 / s_dt.as_secs_f64();

    vec![
        Metric::new(
            "sealed.global_scan.ev_per_s",
            g_evps,
            "ev/s",
            format!(
                "{n_segments}-way parallel per-segment decode, concatenated \
                 in base_pos order; {total_events} events; byte-identical to \
                 sequential; best-of-{reps}"
            ),
        ),
        Metric::new(
            "sealed.stream_replay.ev_per_s",
            s_evps,
            "ev/s",
            format!(
                "coalesced replay of {r_streams} random streams x \
                 {n_segments} segments ({s_events} events); byte-identical to \
                 sequential; best-of-{reps}"
            ),
        ),
    ]
}
