//! bn-20e: the sealed-or-active **handoff** race. The D5 invariant is that a
//! committed pointer resolves at every instant across the seal swap — never a
//! gap. This exercises the swap under concurrent readers and randomized timing.
//!
//! Real threads → `#[cfg_attr(miri, ignore)]` per the crate's thread-test rule.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use mess_index::ActiveIndex;
use mess_index::active::{BatchEntry, EventPtr};
use mess_index::sealed::segment::{
    SealBatch, SealInput, SealStream, SealedSegmentIndex, encode_sidecar,
};
use mess_index::sealed::store::{SealedStore, resolve};

/// Build the active index and matching seal input for one segment of
/// `n_streams` streams, each with `batches_per` contiguous batches of `fc`
/// events.
fn build(
    segment_id: u64,
    n_streams: u64,
    batches_per: u64,
    fc: u32,
) -> (ActiveIndex, SealInput) {
    let active = ActiveIndex::new();
    let mut streams = Vec::new();
    let mut global: u64 = 0;
    let mut offset: u64 = 4096;
    // Interleave batches across streams in commit order so the active index and
    // the seal input see the same global ordering.
    let mut per_stream_ver = vec![0u64; n_streams as usize];
    let mut seal_batches: Vec<Vec<SealBatch>> =
        vec![Vec::new(); n_streams as usize];
    for _round in 0..batches_per {
        for s in 0..n_streams {
            let v = per_stream_ver[s as usize];
            let be = BatchEntry {
                stream_id:            s,
                first_stream_version: v,
                frame_count:          fc,
                first_global_pos:     global,
                ptr:                  EventPtr { segment_id, offset },
            };
            active.apply_committed(global + u64::from(fc), &[be]);
            seal_batches[s as usize].push(SealBatch {
                first_version: v,
                frame_count: fc,
                first_global_pos: global,
                offset,
            });
            per_stream_ver[s as usize] = v + u64::from(fc);
            global += u64::from(fc);
            offset += 512;
        }
    }
    let streams_vec: Vec<SealStream> = (0..n_streams)
        .map(|s| SealStream {
            stream_id: s,
            batches:   seal_batches[s as usize].clone(),
        })
        .collect();
    streams.extend(streams_vec);
    (active, SealInput { segment_id, base_pos: 0, streams, payloads: None })
}

#[test]
#[cfg_attr(miri, ignore)]
fn swap_never_exposes_a_gap() {
    let segment_id = 1u64;
    let n_streams = 64u64;
    let batches_per = 40u64;
    let fc = 4u32;
    let (active, input) = build(segment_id, n_streams, batches_per, fc);
    let active = Arc::new(active);
    let last_version = batches_per * u64::from(fc) - 1;

    // Run many swap iterations; each installs a fresh sealed index while
    // readers hammer resolves. A gap (None for a key that must exist) fails
    // the test.
    for iter in 0..50 {
        let store = Arc::new(SealedStore::new());
        let index = Arc::new(
            SealedSegmentIndex::from_bytes(encode_sidecar(&input)).unwrap(),
        );

        let stop = Arc::new(AtomicBool::new(false));
        let gaps = Arc::new(AtomicU64::new(0));
        let reads = Arc::new(AtomicU64::new(0));

        let mut readers = Vec::new();
        for r in 0..4 {
            let active = active.clone();
            let store = store.clone();
            let stop = stop.clone();
            let gaps = gaps.clone();
            let reads = reads.clone();
            readers.push(std::thread::spawn(move || {
                // Deterministic per-reader probe sequence over (stream,
                // version).
                let mut seed =
                    0x9E37_79B9u64.wrapping_mul(r + 1).wrapping_add(iter);
                while !stop.load(Ordering::Relaxed) {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    let s = seed % n_streams;
                    let v = (seed >> 8) % (last_version + 1);
                    match resolve(&active, &store, s, v) {
                        Some(_) => {}
                        None => {
                            gaps.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    reads.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        // Give readers a moment to spin up and observe the active-only state.
        while reads.load(Ordering::Relaxed) < 1_000 {
            std::hint::spin_loop();
        }

        // The swap: install THEN evict, the driver's order.
        store.install(index.clone());
        store.mark_active_evicted(segment_id);

        // Let readers observe the post-swap state.
        let target = reads.load(Ordering::Relaxed) + 5_000;
        while reads.load(Ordering::Relaxed) < target {
            std::hint::spin_loop();
        }
        stop.store(true, Ordering::Relaxed);
        for h in readers {
            h.join().unwrap();
        }
        assert_eq!(
            gaps.load(Ordering::Relaxed),
            0,
            "iter {iter}: reader saw a gap during the swap"
        );
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn sealed_resolves_agree_with_active_for_all_keys() {
    // The sealed index must resolve every committed key identically to the
    // active index it replaced.
    let segment_id = 5u64;
    let (active, input) = build(segment_id, 32, 20, 3);
    let sealed =
        SealedSegmentIndex::from_bytes(encode_sidecar(&input)).unwrap();
    let last_version = 20 * 3 - 1;
    for s in 0..32u64 {
        for v in 0..=last_version {
            let a = active.resolve(s, v);
            let b = sealed.resolve(s, v).unwrap();
            assert_eq!(a, b, "stream {s} version {v}");
        }
        assert_eq!(active.stream_head(s), sealed.stream_head(s));
    }
}
