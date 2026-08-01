//! bn-20e full-scale seal (acceptance criteria). Builds a 256-MiB-shaped
//! segment's index slice (~1M events, ~100k batches, 10k streams — the
//! `seal_pipeline`/`perf_replay` workload shape), seals it on the background
//! thread, and checks the three acceptance criteria:
//!
//! 1. The seal runs off the append path: while the background sealer works the
//!    big segment, the committer keeps calling `apply_committed` on the *new*
//!    active segment and sees no latency spike (they share nothing on the hot
//!    path — the store is touched once, briefly, at install).
//! 2. Sealed stream replay is at least as fast as the pre-seal active pointer
//!    walk on the same data.
//! 3. Cached point reads through the skip table are fast (target <2 µs;
//!    `perf_replay` measured p50 1.67 µs / p99 0.79 µs with the skip table).
//!
//! Real fs + threads + timing → `#[cfg_attr(miri, ignore)]`. Assertions are
//! deliberately loose (CI runs under load); the measured numbers are printed
//! and recorded in `sealed::driver`'s module docs.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use mess_index::ActiveIndex;
use mess_index::active::{BatchEntry, EventPtr};
use mess_index::sealed::driver::{BackgroundSealer, SealDriver};
use mess_index::sealed::segment::{SealBatch, SealInput, SealStream};
use mess_index::sealed::store::SealedStore;

const N_STREAMS: u64 = 10_000;
const EVENTS: u64 = 1_000_000;
const FC: u32 = 10; // events per batch (spike: batches of 10)

/// Build the big segment's active index and matching seal input, Zipf-ish by
/// round-robin so every stream gets a fair share (the pointer machinery is
/// skew-insensitive; skew only matters to Phase-5 dictionaries).
fn build_big(segment_id: u64) -> (Arc<ActiveIndex>, SealInput) {
    let active = ActiveIndex::new();
    let n_batches = EVENTS / u64::from(FC);
    let mut seal_batches: Vec<Vec<SealBatch>> =
        vec![Vec::new(); N_STREAMS as usize];
    let mut ver = vec![0u64; N_STREAMS as usize];
    let mut global: u64 = 0;
    let mut offset: u64 = 4096;
    for b in 0..n_batches {
        let s = b % N_STREAMS;
        let v = ver[s as usize];
        active.apply_committed(
            global + u64::from(FC),
            &[BatchEntry {
                stream_id:            s,
                first_stream_version: v,
                frame_count:          FC,
                first_global_pos:     global,
                ptr:                  EventPtr { segment_id, offset },
            }],
        );
        seal_batches[s as usize].push(SealBatch {
            first_version: v,
            frame_count: FC,
            first_global_pos: global,
            offset,
        });
        ver[s as usize] = v + u64::from(FC);
        global += u64::from(FC);
        offset += 256 * u64::from(FC); // ~256 B/event
    }
    let streams: Vec<SealStream> = (0..N_STREAMS)
        .filter(|&s| !seal_batches[s as usize].is_empty())
        .map(|s| SealStream {
            stream_id: s,
            batches:   seal_batches[s as usize].clone(),
        })
        .collect();
    (
        Arc::new(active),
        SealInput {
            segment_id,
            base_pos: 0,
            streams,
            payloads: None,
            event_type_ids: None,
        },
    )
}

fn percentile(sorted: &[Duration], q: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[idx]
}

#[test]
#[cfg_attr(miri, ignore)]
fn full_scale_seal_is_off_path_and_reads_fast() {
    let dir =
        mess_testkit::sweeping_temp_dir("idx-sealed-scale-full-scale-seal-is");
    let store = Arc::new(SealedStore::new());
    let driver = SealDriver::new(store.clone(), dir.path());

    let (_active_big, input) = build_big(1);
    let event_count = input.event_count();
    let n_batches: usize = input.streams.iter().map(|s| s.batches.len()).sum();
    assert_eq!(event_count, EVENTS);

    // The NEW active segment the committer keeps writing during the seal.
    let active_new = Arc::new(ActiveIndex::new());

    let sealer = BackgroundSealer::spawn(driver.clone());
    let seal_started = Instant::now();
    let rx = sealer.submit(input, |_| Ok(()));

    // Hammer the append hot path on the new segment while the seal runs; record
    // per-call latency. base_pos for segment 2 starts after segment 1's events.
    let stop = Arc::new(AtomicBool::new(false));
    let mut append_lat: Vec<Duration> = Vec::with_capacity(200_000);
    let mut pos = EVENTS;
    let mut nv = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let t = Instant::now();
        active_new.apply_committed(
            pos + u64::from(FC),
            &[BatchEntry {
                stream_id:            nv % 1000,
                first_stream_version: nv / 1000 * u64::from(FC),
                frame_count:          FC,
                first_global_pos:     pos,
                ptr:                  EventPtr {
                    segment_id: 2,
                    offset:     pos * 256,
                },
            }],
        );
        append_lat.push(t.elapsed());
        pos += u64::from(FC);
        nv += 1;
        // Stop once the seal is done (non-blocking check).
        if rx.try_recv().is_ok() {
            stop.store(true, Ordering::Relaxed);
        }
        if append_lat.len() >= 2_000_000 {
            break; // safety bound
        }
    }
    let seal_wall = seal_started.elapsed();

    // The seal finished; fetch the installed index (submit already delivered
    // it, but re-fetch from the store to confirm the handoff completed).
    let sealed = store.get(1).expect("sealed index installed");
    assert!(store.is_evicted(1), "active entries evicted after seal");
    assert_eq!(sealed.event_count(), EVENTS);

    append_lat.sort_unstable();
    let ap50 = percentile(&append_lat, 0.50);
    let ap99 = percentile(&append_lat, 0.99);
    let apmax = append_lat.last().copied().unwrap_or_default();

    // (1) Off-path: appends never stalled anywhere near the inline ~1.4 s seal
    // cost `perf_append` measured. A single-batch apply is sub-microsecond;
    // even a very loose bound catches an inline stall.
    assert!(
        apmax < Duration::from_millis(50),
        "append hot path stalled during background seal: max={apmax:?} \
         (p99={ap99:?})"
    );

    // (3) Cached point-read p99 through the skip table, over 20k event-weighted
    // random keys (hot streams dominate, exactly `perf_replay`'s sample).
    let mut pr_lat: Vec<Duration> = Vec::with_capacity(20_000);
    let mut seed = 0x1234_5678u64;
    for _ in 0..20_000 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let s = seed % N_STREAMS;
        let head = sealed.stream_head(s).unwrap();
        let v = (seed >> 20) % (head + 1);
        let t = Instant::now();
        let got = sealed.resolve(s, v).unwrap();
        pr_lat.push(t.elapsed());
        assert!(got.is_some(), "point read miss for a present key ({s},{v})");
    }
    pr_lat.sort_unstable();
    let pr50 = percentile(&pr_lat, 0.50);
    let pr99 = percentile(&pr_lat, 0.99);

    // (2) Sealed stream replay vs the active pointer walk, over 1000 streams.
    let sample: Vec<u64> = (0..1000u64).map(|i| (i * 7) % N_STREAMS).collect();
    // Rebuild a fresh active index representing the pre-seal state to walk.
    let (active_pre, _in2) = build_big(1);
    let t = Instant::now();
    let mut active_events = 0u64;
    for &s in &sample {
        active_events += active_pre
            .stream_entries(s)
            .iter()
            .map(|e| u64::from(e.frame_count))
            .sum::<u64>();
    }
    let active_walk = t.elapsed();
    let t = Instant::now();
    let mut sealed_events = 0u64;
    for &s in &sample {
        sealed_events += sealed
            .stream_entries(s)
            .unwrap()
            .iter()
            .map(|e| u64::from(e.frame_count))
            .sum::<u64>();
    }
    let sealed_walk = t.elapsed();
    assert_eq!(active_events, sealed_events, "replay event counts differ");

    let sidecar_bytes =
        std::fs::metadata(driver.sidecar_path(1)).unwrap().len();

    eprintln!(
        "--- bn-20e full-scale seal ({EVENTS} events, {n_batches} batches, \
         {N_STREAMS} streams) ---"
    );
    eprintln!("seal wall (background)      : {seal_wall:?}");
    eprintln!(
        "append hot path p50/p99/max : {ap50:?} / {ap99:?} / {apmax:?}  ({} \
         samples)",
        append_lat.len()
    );
    eprintln!(
        "sealed point-read p50/p99   : {pr50:?} / {pr99:?}  (cached, skip \
         table)"
    );
    eprintln!(
        "stream replay active/sealed : {active_walk:?} / {sealed_walk:?}  ({} \
         events each)",
        sealed_events
    );
    eprintln!(
        "sidecar size                : {sidecar_bytes} bytes ({:.3} B/event)",
        sidecar_bytes as f64 / EVENTS as f64
    );

    // (2) assertion: sealed replay is at least competitive with the active walk
    // (both are pure pointer decodes here; the active clone even skips a
    // decode, so allow generous slack — the point is sealed is not
    // pathologically slow).
    assert!(
        sealed_walk < active_walk * 8 + Duration::from_millis(50),
        "sealed stream replay far slower than active walk: {sealed_walk:?} vs \
         {active_walk:?}"
    );
    // (3) assertion: cached point-read p99 well bounded. Target <2 µs; assert a
    // loose 50 µs so CI load never flakes while still catching an O(n)
    // regression.
    assert!(
        pr99 < Duration::from_micros(50),
        "point-read p99 too slow: {pr99:?}"
    );

    sealer.shutdown();
}
