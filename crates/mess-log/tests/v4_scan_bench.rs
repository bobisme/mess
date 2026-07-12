//! bn-9mw Spike E: scan-overhead measurement — v4 **no-control** capsule scan
//! vs v3 batch scan on equal corpora. Gate: control support must not tax the
//! common (no-control) scan path by more than **2%** on per-byte throughput.
//!
//! `#[ignore]`d — a timing measurement, not a correctness gate; run explicitly
//! under the quiet guard (no concurrent compilers, load1 < 6):
//!
//! ```text
//! cargo test -p mess-log --release --test v4_scan_bench -- --ignored --nocapture
//! ```
//!
//! Both corpora carry the same number of single-event batches/capsules with
//! identical payloads. v3 uses [`BatchEncoder`]/`scan_image`; v4 uses events-
//! only capsules (0 controls) / `scan_v4_image_physical`. We report ns per
//! event, ns per byte, and MB/s for each, and assert the v4 per-byte throughput
//! is within 2% of v3 — isolating scanning efficiency from v4's larger fixed
//! header/marker framing (which moves more CRC bytes per capsule by design).

use std::time::Instant;

use mess_log::encode::{BatchEncoder, BatchInput, Subframe};
use mess_log::format::{
    FORMAT_VERSION, SEGMENT_HEADER_CRC_OFF, SEGMENT_HEADER_LEN, SEGMENT_MAGIC,
};
use mess_log::scanner::scan_image;
use mess_log::v4::capsule::{CapsuleEncoder, CapsuleInput};
use mess_log::v4::recover::scan_v4_image_physical;

const N: usize = 20_000; // batches/capsules
const PAYLOAD: usize = 64;
const REPS: usize = 40;

fn v3_segment() -> Vec<u8> {
    let mut img = segment_header(FORMAT_VERSION);
    let mut enc = BatchEncoder::new();
    let payload = vec![0xABu8; PAYLOAD];
    let mut gpos = 0u64;
    for id in 0..N as u64 {
        let sfs = [Subframe::plain(1, 0, 0, &payload)];
        let input = BatchInput {
            segment_epoch:        1,
            batch_id:             id,
            first_global_pos:     gpos,
            stream_id:            9,
            category_id:          0,
            first_stream_version: gpos,
            crypto_chain:         None,
            subframes:            &sfs,
        };
        img.extend_from_slice(enc.encode(&input).unwrap());
        gpos += 1;
    }
    img
}

fn v4_segment() -> Vec<u8> {
    let mut img = segment_header(4);
    let mut enc = CapsuleEncoder::new();
    let payload = vec![0xABu8; PAYLOAD];
    let mut gpos = 0u64;
    for id in 0..N as u64 {
        let sfs = [Subframe::plain(1, 0, 0, &payload)];
        let input = CapsuleInput {
            segment_epoch:        1,
            batch_id:             id,
            first_global_pos:     gpos,
            stream_id:            9,
            category_id:          0,
            first_stream_version: gpos,
            crypto_chain:         None,
            controls:             &[],
            subframes:            &sfs,
        };
        img.extend_from_slice(enc.encode(&input).unwrap().to_vec().as_slice());
        gpos += 1;
    }
    img
}

/// Minimal valid 52-byte segment header of the given format version.
fn segment_header(version: u16) -> Vec<u8> {
    let mut h = vec![0u8; SEGMENT_HEADER_LEN];
    h[0..4].copy_from_slice(&SEGMENT_MAGIC.to_le_bytes());
    h[4..6].copy_from_slice(&version.to_le_bytes());
    // segment_id=1 at offset 8, base_pos=0, epoch=1 at offset 24.
    h[8..16].copy_from_slice(&1u64.to_le_bytes());
    h[24..32].copy_from_slice(&1u64.to_le_bytes());
    let crc = crc32c::crc32c(&h[..SEGMENT_HEADER_CRC_OFF]);
    h[SEGMENT_HEADER_CRC_OFF..SEGMENT_HEADER_CRC_OFF + 4]
        .copy_from_slice(&crc.to_le_bytes());
    h
}

fn best_ns<F: Fn() -> usize>(f: F) -> u128 {
    // Warmup.
    for _ in 0..3 {
        std::hint::black_box(f());
    }
    let mut best = u128::MAX;
    for _ in 0..REPS {
        let t = Instant::now();
        let accepted = f();
        let ns = t.elapsed().as_nanos();
        std::hint::black_box(accepted);
        best = best.min(ns);
    }
    best
}

#[test]
#[ignore = "timing measurement; run under the quiet guard"]
fn v4_no_control_scan_overhead_under_2pct() {
    let v3 = v3_segment();
    let v4 = v4_segment();

    let v3_ns = best_ns(|| {
        let r = scan_image(std::hint::black_box(&v3), None);
        r.accepted.len()
    });
    let v4_ns = best_ns(|| {
        let r = scan_v4_image_physical(std::hint::black_box(&v4));
        r.accepted.len()
    });

    // Sanity: both scanned all N.
    assert_eq!(scan_image(&v3, None).accepted.len(), N);
    assert_eq!(scan_v4_image_physical(&v4).accepted.len(), N);

    let v3_bytes = v3.len() as f64;
    let v4_bytes = v4.len() as f64;
    let v3_ns_per_byte = v3_ns as f64 / v3_bytes;
    let v4_ns_per_byte = v4_ns as f64 / v4_bytes;
    let v3_ns_per_ev = v3_ns as f64 / N as f64;
    let v4_ns_per_ev = v4_ns as f64 / N as f64;
    let v3_mbps = v3_bytes / (v3_ns as f64) * 1000.0;
    let v4_mbps = v4_bytes / (v4_ns as f64) * 1000.0;
    let per_byte_overhead =
        (v4_ns_per_byte - v3_ns_per_byte) / v3_ns_per_byte * 100.0;

    println!(
        "--- v4 scan overhead (N={N}, payload={PAYLOAD}B, best of {REPS}) ---"
    );
    println!(
        "v3: {v3_ns} ns total | {v3_ns_per_ev:.2} ns/event | \
         {v3_ns_per_byte:.4} ns/byte | {v3_mbps:.0} MB/s | {} bytes",
        v3.len()
    );
    println!(
        "v4: {v4_ns} ns total | {v4_ns_per_ev:.2} ns/event | \
         {v4_ns_per_byte:.4} ns/byte | {v4_mbps:.0} MB/s | {} bytes",
        v4.len()
    );
    println!(
        "per-byte throughput overhead: {per_byte_overhead:+.2}% (gate < 2%)"
    );

    assert!(
        per_byte_overhead < 2.0,
        "v4 no-control per-byte scan overhead {per_byte_overhead:.2}% exceeds \
         the 2% gate"
    );
}
