//! bn-1i7 acceptance: seal-time `BinaryFuse16` membership filters. Builds a
//! filter over a `sealed_scale`-shaped set of stream ids (10,000 streams),
//! measures build and query cost, and checks:
//!
//! 1. Zero false negatives across every present key.
//! 2. False-positive rate over a large absent-key sample stays well under 1%
//!    (the round-3 spike measured ~0.002% at this key count).
//! 3. Build and query cost are recorded (printed here; copied into
//!    `sealed::filter`'s module docs).
//!
//! Timing-sensitive → `#[cfg_attr(miri, ignore)]` (real wall-clock
//! measurement, not correctness-relevant to miri).

use std::time::Instant;

use mess_index::sealed::filter::SegmentFilter;

const N_STREAMS: u64 = 10_000;

fn xorshift(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

#[test]
#[cfg_attr(miri, ignore)]
fn filter_build_query_cost_and_fpr_at_scale() {
    // Present keys: sparse, non-contiguous stream ids (like real stream_ids
    // — not small dense integers), the same shape `sealed_scale` uses.
    let mut seed = 0xD1B5_4A32_D192_ED03u64;
    let mut present = std::collections::BTreeSet::new();
    while present.len() < N_STREAMS as usize {
        present.insert(xorshift(&mut seed));
    }
    let ids: Vec<u64> = present.iter().copied().collect();

    let t = Instant::now();
    let filter = SegmentFilter::build(1, &ids).unwrap();
    let build_time = t.elapsed();

    let bytes = filter.to_bytes();

    // No false negatives.
    let t = Instant::now();
    for &id in &ids {
        assert!(
            filter.might_contain(id),
            "false negative for present key {id}"
        );
    }
    let hit_query_time = t.elapsed() / ids.len() as u32;

    // False positive rate over a large absent-key sample.
    let mut false_positives = 0u64;
    let mut checked = 0u64;
    let t = Instant::now();
    while checked < 200_000 {
        let candidate = xorshift(&mut seed);
        if present.contains(&candidate) {
            continue;
        }
        checked += 1;
        if filter.might_contain(candidate) {
            false_positives += 1;
        }
    }
    let miss_query_time = t.elapsed() / checked as u32;
    let fpr = false_positives as f64 / checked as f64;

    eprintln!("--- bn-1i7 filter scale ({N_STREAMS} stream ids) ---");
    eprintln!("build time                 : {build_time:?}");
    eprintln!(
        "serialized size            : {} bytes ({:.3} B/key)",
        bytes.len(),
        bytes.len() as f64 / N_STREAMS as f64
    );
    eprintln!(
        "query (present / absent)   : {hit_query_time:?} / {miss_query_time:?}"
    );
    eprintln!(
        "false positive rate        : {false_positives}/{checked} = {:.5}%",
        fpr * 100.0
    );

    assert!(fpr < 0.01, "FPR sanity band exceeded: {fpr}");

    // Round-trip through the on-disk byte format too — build cost measured
    // above is the in-memory `xorf` construction; this checks the persisted
    // form answers identically.
    let reopened = SegmentFilter::from_bytes(&bytes).unwrap();
    for &id in &ids {
        assert!(
            reopened.might_contain(id),
            "false negative after round-trip for {id}"
        );
    }
}
