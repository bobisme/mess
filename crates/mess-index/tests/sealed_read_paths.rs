//! bn-1hx sealed read paths: the corruptor/SIGBUS-stance test and the
//! multi-segment replay/scan/cache benchmark.
//!
//! Run the benchmark (ignored, needs release to be meaningful):
//!
//! ```text
//! cargo test -p mess-index --release --test sealed_read_paths bench -- --ignored --nocapture
//! ```

use std::sync::Arc;

use mess_index::sealed::block_cache::BlockCache;
use mess_index::sealed::replay::{ReplaySet, global_checksum, stream_checksum};
use mess_index::sealed::segment::{
    SealBatch, SealInput, SealStream, SealedSegmentIndex, SealedSegmentRef, SidecarError,
    encode_sidecar,
};

// ---------------------------------------------------------------------------
// Corpus generation (deterministic)
// ---------------------------------------------------------------------------

/// Build a sealed segment whose `base_pos` is `base`, holding `streams`
/// (present in every segment so a stream replay coalesces across all of them),
/// each stream with `batches_per_stream` 10-frame batches. Global positions are
/// laid out contiguously from `base`, so segments tile the A1 axis.
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
                        first_version: first_version_base + (i as u64) * u64::from(FRAMES),
                        frame_count: FRAMES,
                        first_global_pos: g,
                        offset: 4096 + (i as u64) * 512,
                    };
                    g += u64::from(FRAMES);
                    b
                })
                .collect();
            SealStream { stream_id: sid, batches }
        })
        .collect();
    let input = SealInput { segment_id, base_pos: base, streams, payloads: None };
    let idx = Arc::new(SealedSegmentIndex::from_bytes(encode_sidecar(&input)).unwrap());
    (idx, g)
}

/// A multi-segment corpus: `n_segments` tiling the A1 axis, each holding the
/// same `n_streams` streams with `batches_per_stream` batches. Returns the set
/// and the stream ids.
fn build_corpus(
    n_segments: u64,
    n_streams: u64,
    batches_per_stream: usize,
) -> (ReplaySet, Vec<u64>) {
    let stream_ids: Vec<u64> = (0..n_streams).collect();
    let mut segs = Vec::new();
    let mut base = 0u64;
    for seg in 0..n_segments {
        // A stream's versions grow across later segments (strictly ascending is
        // required by the coalesce): stride the version base by the per-segment
        // span so segment k starts past segment k-1's last version.
        let fv_base = seg * (batches_per_stream as u64) * 10;
        let (idx, next_base) =
            build_segment(seg, base, &stream_ids, batches_per_stream, fv_base);
        base = next_base;
        segs.push(idx);
    }
    (ReplaySet::from_segments(segs), stream_ids)
}

/// Minimal deterministic LCG (no rand dependency).
struct Lcg(u64);
impl Lcg {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0
    }
}

// ---------------------------------------------------------------------------
// SIGBUS / corruptor stance
// ---------------------------------------------------------------------------

/// A truncated sealed sidecar surfaces as a **typed** [`SidecarError`] at open —
/// never a panic, never a `SIGBUS` (the reader owns the bytes and CRC-checks
/// them before any query; it maps nothing). The corrupt segment is simply not
/// admitted to the [`ReplaySet`], and replay over the survivors is exact.
#[test]
#[cfg_attr(miri, ignore)] // real filesystem
fn truncated_sidecar_is_typed_error_and_replay_falls_back_exact() {
    let dir = tempfile::tempdir().unwrap();
    let stream_ids: Vec<u64> = (0..8).collect();

    // Three good segments + one we will truncate on disk.
    let (good0, b1) = build_segment(0, 0, &stream_ids, 3, 0);
    let (good1, b2) = build_segment(1, b1, &stream_ids, 3, 30);
    let (bad, b3) = build_segment(2, b2, &stream_ids, 3, 60);
    let (good2, _b4) = build_segment(3, b3, &stream_ids, 3, 90);

    // Persist the "bad" one, then truncate it mid-body and try to reopen.
    let bad_bytes = encode_sidecar(&SealInput {
        segment_id: 2,
        base_pos: b2,
        streams: (0..8)
            .map(|sid| SealStream {
                stream_id: sid,
                batches: (0..3)
                    .map(|i| SealBatch {
                        first_version: 60 + i as u64 * 10,
                        frame_count: 10,
                        first_global_pos: b2 + sid * 30 + i as u64 * 10,
                        offset: 4096 + i as u64 * 512,
                    })
                    .collect(),
            })
            .collect(),
        payloads: None,
    });
    let path = dir.path().join("seg-2.pidx");
    std::fs::write(&path, &bad_bytes).unwrap();
    // Truncate to half — a torn/short file.
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len((bad_bytes.len() / 2) as u64).unwrap();
    drop(f);

    // Opening the truncated file is a typed error, not a crash.
    let reopened = SealedSegmentIndex::open(&path);
    assert!(
        matches!(reopened, Err(SidecarError::Corrupt(_)) | Err(SidecarError::Io(_))),
        "truncated sidecar must be a typed SidecarError, got {reopened:?}"
    );
    // (Silence unused: `bad` stands for the in-memory twin we did NOT admit.)
    let _ = bad;

    // Admit only the segments that opened cleanly; replay is exact and the
    // verified path (the degraded-mode entry point) agrees with the plain one.
    let survivors = ReplaySet::from_segments([good0, good1, good2]);
    let cache = BlockCache::disabled();
    let streams: Vec<u64> = (0..8).collect();
    let plain = survivors.stream_replay_many(&streams, &cache).unwrap();
    let verified = survivors.stream_replay_many_verified(&streams, &cache).unwrap();
    let seq = survivors.stream_replay_many_seq(&streams, &cache).unwrap();
    assert_eq!(plain, seq);
    assert_eq!(verified, seq);
    for (sid, entries) in &plain {
        // Each survivor carries 3 batches for this stream: 3 segments * 3 = 9.
        assert_eq!(entries.len(), 9, "stream {sid}");
    }
}

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

fn best_of<T, F: FnMut() -> T>(reps: usize, mut f: F) -> (T, std::time::Duration) {
    let mut best = std::time::Duration::MAX;
    let mut last = None;
    for _ in 0..reps {
        let t = std::time::Instant::now();
        let out = f();
        let dt = t.elapsed();
        if dt < best {
            best = dt;
        }
        last = Some(out);
    }
    (last.unwrap(), best)
}

#[test]
#[ignore = "benchmark; run with --release --nocapture"]
#[cfg_attr(miri, ignore)]
fn bench_sealed_read_paths() {
    const N_SEGMENTS: u64 = 24;
    const N_STREAMS: u64 = 2_000;
    const BATCHES_PER_STREAM: usize = 5;

    let (set, stream_ids) = build_corpus(N_SEGMENTS, N_STREAMS, BATCHES_PER_STREAM);
    let total_events = set.event_count();
    eprintln!(
        "\n=== bn-1hx sealed read paths — corpus: {N_SEGMENTS} segments, \
         {N_STREAMS} streams, {total_events} events ===",
    );

    // --- global scan (all segments, parallel) ---
    let (g_par, g_dt) = best_of(5, || set.global_scan().unwrap());
    let g_events: u64 = g_par.iter().map(|e| u64::from(e.frame_count)).sum();
    let g_evps = g_events as f64 / g_dt.as_secs_f64();
    // Byte-identity gate: parallel == sequential.
    let g_seq = set.global_scan_seq().unwrap();
    assert_eq!(g_par, g_seq, "global scan: parallel != sequential");
    assert_eq!(global_checksum(&g_par), global_checksum(&g_seq));
    eprintln!(
        "global scan (parallel, {N_SEGMENTS}-way): {g_events} events in {:.3} ms => {:.1}M ev/s \
         [checksum {:#018x}, byte-identical to seq]",
        g_dt.as_secs_f64() * 1e3,
        g_evps / 1e6,
        global_checksum(&g_par),
    );

    // --- coalesced stream replay (1000 random streams, each across all segments) ---
    const R: usize = 1_000;
    let mut rng = Lcg(0x5EED_00B1_1A11_0001);
    let picks: Vec<u64> =
        (0..R).map(|_| stream_ids[(rng.next_u64() % N_STREAMS) as usize]).collect();

    // Cold: fresh (disabled) cache, decode every block; batched parallel path.
    let cold_cache = BlockCache::disabled();
    let (batched, s_dt) = best_of(5, || set.stream_replay_many(&picks, &cold_cache).unwrap());
    // Byte-identity gate: batched parallel == sequential reference.
    let seq = set.stream_replay_many_seq(&picks, &cold_cache).unwrap();
    assert_eq!(batched, seq, "stream replay: parallel != sequential");
    let s_events: u64 = batched
        .iter()
        .flat_map(|(_, es)| es.iter())
        .map(|e| u64::from(e.frame_count))
        .sum();
    let s_ck = batched
        .iter()
        .fold(0u64, |acc, (_, es)| acc.wrapping_add(stream_checksum(es)));
    let s_evps = s_events as f64 / s_dt.as_secs_f64();
    eprintln!(
        "stream replay (coalesced, {R} streams x {N_SEGMENTS} segments): {s_events} events in \
         {:.3} ms => {:.1}M ev/s [checksum {s_ck:#018x}, byte-identical to seq]",
        s_dt.as_secs_f64() * 1e3,
        s_evps / 1e6,
    );

    // --- cache hit-rate on repeat replay ---
    // Budget: enough for the touched blocks (R streams x N_SEGMENTS blocks).
    let cache = BlockCache::with_budget_bytes(64 << 20, (R as u64 * N_SEGMENTS) as usize);
    // Pass 1: cold — all misses. Pass 2: warm — all hits. Two identical passes
    // over the same working set land the hit rate at ~50% (perf_replay's "48%
    // hit on repeat replay"). Both passes go through the real replay path.
    for &sid in &picks {
        let _ = set.stream_replay(sid, &cache).unwrap();
    }
    let cold_misses = cache.misses();
    for &sid in &picks {
        let _ = set.stream_replay(sid, &cache).unwrap();
    }
    eprintln!(
        "cache hit-rate on repeat replay: {:.1}% ({} hits / {} lookups); resident {:.1} MiB, \
         {} blocks [cold pass misses: {}]",
        cache.hit_rate() * 100.0,
        cache.hits(),
        cache.hits() + cache.misses(),
        cache.weight_bytes() as f64 / (1024.0 * 1024.0),
        cache.len(),
        cold_misses,
    );

    // Acceptance floors.
    assert!(s_evps >= 2.5e6, "stream replay {s_evps:.0} ev/s below 2.5M floor");
    eprintln!(
        "PASS: stream replay {:.1}M ev/s >= 2.5M floor; global scan {:.1}M ev/s\n",
        s_evps / 1e6,
        g_evps / 1e6,
    );
}
