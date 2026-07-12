//! One-shot corpus-seed generator, NOT a fuzz target (bn-gux). Run with:
//!
//! ```text
//! cargo run --manifest-path crates/mess-log/fuzz/Cargo.toml --bin gen_corpus
//! ```
//!
//! Writes small seed files under `corpus/fuzz_scanner/` and
//! `corpus/fuzz_batch_decode/`, derived from the real [`SegmentWriter`] /
//! [`BatchEncoder`] (the same encoder the golden-file suite,
//! `crates/mess-log/tests/golden.rs`, checks byte-for-byte against the spec
//! tables) plus a few hand-corrupted mutations of the same bytes covering the
//! adversarial classes `tests/recovery_scanner.rs` exercises structurally
//! (bad magic, truncated tail, flipped CRC, wrong epoch). Re-run any time a
//! richer seed set is wanted; the committed corpus is intentionally minimal
//! (small seeds only, per the bn-gux protocol) — libFuzzer's own coverage-
//! guided mutation does the rest.

use std::fs;
use std::path::{Path, PathBuf};

use mess_log::encode::Subframe;
use mess_log::runtime::{Fault, FileHandle, Fs, OpenOpts, SimFs};
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};

fn write_seed(dir: &Path, name: &str, bytes: &[u8]) {
    fs::create_dir_all(dir).expect("create corpus dir");
    fs::write(dir.join(name), bytes).expect("write seed");
}

/// Build a durable one-batch (or few-batch) segment image (SegmentHeader +
/// batches) entirely through the real writer on the sim fs, then read the
/// whole image back — exactly `tests/recovery_scanner.rs`'s `build_segment`
/// helper, duplicated here so the fuzz crate has no dev-dependency on the
/// main crate's `tests/` (which isn't a library target).
fn build_segment_image(epoch: u64, base_pos: u64, payload_sets: &[&[&[u8]]]) -> Vec<u8> {
    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("seed.seg");
    let mut w = SegmentWriter::create(&fs, path, SegmentParams::new(1, base_pos, epoch, 0))
        .expect("create segment");
    for (i, payloads) in payload_sets.iter().enumerate() {
        let sfs: Vec<Subframe> = payloads
            .iter()
            .map(|p| Subframe::plain(0x11, 0, 0, p))
            .collect();
        w.append(&BatchSpec {
            stream_id: i as u64,
            category_id: 100 + i as u64,
            first_stream_version: 0,
            crypto_chain: None,
            subframes: &sfs,
        })
        .expect("append batch");
    }
    w.sync().expect("sync");

    let f = fs.open(path, OpenOpts::read_only()).expect("reopen");
    let len = f.len().expect("len") as usize;
    let mut buf = vec![0u8; len];
    let n = f.pread(0, &mut buf).expect("pread");
    buf.truncate(n);
    buf
}

fn main() {
    let root: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus");
    let scanner_dir = root.join("fuzz_scanner");
    let decode_dir = root.join("fuzz_batch_decode");

    // --- fuzz_scanner seeds: whole segment images -------------------------

    // 1. A single-batch, single-frame segment — the minimal clean case.
    let clean_one = build_segment_image(1, 0, &[&[&[0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]]);
    write_seed(&scanner_dir, "clean_one_batch", &clean_one);

    // 2. A multi-batch, multi-frame segment (three batches, varying frame
    //    counts) — exercises the accept loop advancing `off` repeatedly.
    let clean_many = build_segment_image(
        7,
        1000,
        &[
            &[b"hello".as_slice(), b"world!!".as_slice()],
            &[b"a".as_slice()],
            &[b"xy".as_slice(), b"z".as_slice(), b"longer-payload".as_slice()],
        ],
    );
    write_seed(&scanner_dir, "clean_three_batches", &clean_many);

    // 3. Truncated tail: a clean image with the last batch's final bytes cut
    //    off (torn write) — must stop at `Incomplete`, not panic or resync.
    let mut torn = clean_many.clone();
    torn.truncate(torn.len() - 5);
    write_seed(&scanner_dir, "torn_tail", &torn);

    // 4. Flipped CRC byte in the second batch's header region — must stop at
    //    `BadCrc`/`BadMagic`, never accept.
    let mut bad_crc = clean_many.clone();
    let flip_at = clean_many.len() / 2;
    bad_crc[flip_at] ^= 0xFF;
    write_seed(&scanner_dir, "flipped_byte_mid_segment", &bad_crc);

    // 5. Just the bare SegmentHeader, no batches (a freshly-opened, empty
    //    segment) — must stop cleanly at `EndOfSegment`.
    let empty = build_segment_image(3, 0, &[]);
    write_seed(&scanner_dir, "header_only_no_batches", &empty);

    // 6. Zeroed bytes the size of a header — the "recycled, never written"
    //    disk region A11/A9 defend against.
    write_seed(&scanner_dir, "all_zero_header_sized", &[0u8; 52]);

    // 7. Empty file.
    write_seed(&scanner_dir, "empty_file", &[]);

    // --- fuzz_batch_decode seeds: raw batch bytes at offset 0 -------------

    // Reuse the clean single-batch image minus its 52-byte SegmentHeader: the
    // remainder is exactly one valid batch starting at offset 0.
    let one_batch = &clean_one[52..];
    write_seed(&decode_dir, "clean_batch", one_batch);

    let multi_frame_batch = {
        // First batch of `clean_many` (hello/world!! two-frame batch).
        // Its `total_len` lives at header offset [20,28).
        let start = 52usize;
        let total_len = u64::from_le_bytes(clean_many[start + 20..start + 28].try_into().unwrap());
        clean_many[start..start + total_len as usize].to_vec()
    };
    write_seed(&decode_dir, "clean_multi_frame_batch", &multi_frame_batch);

    // Truncated batch (torn header / torn frames).
    write_seed(&decode_dir, "torn_batch_header", &one_batch[..40]);
    write_seed(
        &decode_dir,
        "torn_batch_mid_frame",
        &one_batch[..one_batch.len() - 8],
    );

    // Flipped payload byte -> CRC must reject.
    let mut bad_batch_crc = one_batch.to_vec();
    let mid = bad_batch_crc.len() / 2;
    bad_batch_crc[mid] ^= 0x01;
    write_seed(&decode_dir, "flipped_payload_byte", &bad_batch_crc);

    // Corrupt frame_count (huge value) while everything else stays byte-valid
    // up to that field — exercises the subframe-tiling bounds arithmetic
    // without a real CRC match (decode still must not panic/overflow).
    let mut huge_frame_count = one_batch.to_vec();
    huge_frame_count[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    write_seed(&decode_dir, "huge_frame_count", &huge_frame_count);

    // Corrupt compressed_len on the single subframe to a huge value.
    let mut huge_compressed_len = one_batch.to_vec();
    // Subframe header starts at HEADER_LEN=72; compressed_len at +16 = 88.
    huge_compressed_len[88..92].copy_from_slice(&u32::MAX.to_le_bytes());
    write_seed(&decode_dir, "huge_compressed_len", &huge_compressed_len);

    write_seed(&decode_dir, "empty_input", &[]);
    write_seed(&decode_dir, "single_byte", &[0x42]);

    // --- bn-9mw Spike E: v4 commit-capsule seeds -------------------------
    gen_v4_seeds(&root);

    eprintln!(
        "wrote seeds to {} and {}",
        scanner_dir.display(),
        decode_dir.display()
    );
}

/// Seeds for the three v4 fuzz targets, built from the real v4 encoder /
/// writer so they match the format byte-for-byte, plus a few hand-corrupted
/// mutations covering the adversarial classes.
fn gen_v4_seeds(root: &Path) {
    use mess_log::v4::capsule::{CapsuleEncoder, CapsuleInput};
    use mess_log::v4::control::ControlRecord;
    use mess_log::v4::format::{
        CAPSULE_HEADER_LEN, DEDUPE_SCOPE_STREAM,
    };
    use mess_log::v4::writer::{CapsuleSpec, CapsuleWriter, SegmentParamsV4};

    let capsule_dir = root.join("fuzz_v4_capsule");
    let control_dir = root.join("fuzz_v4_control");
    let scan_dir = root.join("fuzz_v4_scan");

    let all_controls = vec![
        ControlRecord::CategoryRegistered { category_id: 2, name: "orders".into() },
        ControlRecord::StreamRegistered {
            stream_id: 9,
            category_id: 2,
            name: "s9".into(),
        },
        ControlRecord::EventTypeRegistered {
            event_type_id: 3,
            codec_id: 1,
            current_schema_version: 1,
            schema_fingerprint: [0xAB; 32],
            name: "T".into(),
        },
        ControlRecord::DedupeKey {
            scope_kind: DEDUPE_SCOPE_STREAM,
            scope_id: 9,
            key: b"dedupe".to_vec(),
        },
        ControlRecord::ProjectionCheckpoint {
            projection_id: 1,
            position: 5,
            state_ref_kind: 1,
            state_ref: b"ref".to_vec(),
        },
    ];

    // fuzz_v4_control seeds: encoded control regions.
    for (i, c) in all_controls.iter().enumerate() {
        let mut buf = Vec::new();
        c.encode_into(&mut buf);
        write_seed(&control_dir, &format!("control_{i}"), &buf);
    }
    // All controls tiled into one region.
    let mut all_region = Vec::new();
    for c in &all_controls {
        c.encode_into(&mut all_region);
    }
    write_seed(&control_dir, "all_controls", &all_region);
    write_seed(&control_dir, "empty", &[]);

    // fuzz_v4_capsule seeds: encoded capsules of each shape.
    let mut enc = CapsuleEncoder::new();
    // control-only.
    let co = enc
        .encode(&CapsuleInput {
            segment_epoch: 1,
            batch_id: 0,
            first_global_pos: 0,
            stream_id: 0,
            category_id: 0,
            first_stream_version: 0,
            crypto_chain: None,
            controls: &all_controls,
            subframes: &[],
        })
        .unwrap()
        .to_vec();
    write_seed(&capsule_dir, "control_only", &co);
    // mixed (register + use).
    let sfs = [Subframe::plain(3, 1, 1, b"ev")];
    let mixed = enc
        .encode(&CapsuleInput {
            segment_epoch: 1,
            batch_id: 0,
            first_global_pos: 0,
            stream_id: 9,
            category_id: 2,
            first_stream_version: 0,
            crypto_chain: None,
            controls: &all_controls[..3],
            subframes: &sfs,
        })
        .unwrap()
        .to_vec();
    write_seed(&capsule_dir, "mixed", &mixed);
    // events-only.
    let eo = enc
        .encode(&CapsuleInput {
            segment_epoch: 1,
            batch_id: 0,
            first_global_pos: 0,
            stream_id: 9,
            category_id: 0,
            first_stream_version: 0,
            crypto_chain: None,
            controls: &[],
            subframes: &sfs,
        })
        .unwrap()
        .to_vec();
    write_seed(&capsule_dir, "events_only", &eo);
    // torn header + flipped crc mutations.
    write_seed(&capsule_dir, "torn_header", &co[..CAPSULE_HEADER_LEN - 1]);
    let mut bad = mixed.clone();
    let m = bad.len() / 2;
    bad[m] ^= 0xFF;
    write_seed(&capsule_dir, "flipped_mid", &bad);
    write_seed(&capsule_dir, "empty", &[]);

    // fuzz_v4_scan seeds: whole v4 segment images.
    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("v4seed.seg");
    let mut w =
        CapsuleWriter::create(&fs, path, SegmentParamsV4::new(1, 0, 5, 0))
            .unwrap();
    w.append(&CapsuleSpec {
        stream_id: 0,
        category_id: 0,
        first_stream_version: 0,
        crypto_chain: None,
        controls: &all_controls,
        subframes: &[],
    })
    .unwrap();
    w.append(&CapsuleSpec {
        stream_id: 9,
        category_id: 0,
        first_stream_version: 0,
        crypto_chain: None,
        controls: &[],
        subframes: &sfs,
    })
    .unwrap();
    w.close().unwrap();
    let f = fs.open(path, OpenOpts::read_only()).unwrap();
    let len = f.len().unwrap() as usize;
    let mut img = vec![0u8; len];
    let n = f.pread(0, &mut img).unwrap();
    img.truncate(n);
    write_seed(&scan_dir, "clean_two_capsules", &img);
    let mut torn = img.clone();
    torn.truncate(torn.len() - 4);
    write_seed(&scan_dir, "torn_tail", &torn);
    write_seed(&scan_dir, "header_only", &img[..52]);
    write_seed(&scan_dir, "all_zero", &[0u8; 52]);
    write_seed(&scan_dir, "empty", &[]);
}
