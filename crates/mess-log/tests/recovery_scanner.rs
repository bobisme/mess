//! Adversarial conformance suite for the recovery scanner (bn-39n).
//!
//! Every case drives [`recover_segment`] through the runtime [`Fs`] seam on the
//! **sim fault fs** — that is the conformance mechanism. Fixtures are built with
//! the real [`SegmentWriter`]/[`BatchEncoder`] and then either mutated (byte
//! corruption classes) or crashed through the fault media (torn-tail /
//! sector-reorder classes), so the bytes are current spec-v3, not the spikes' v1.
//!
//! Coverage:
//! - the 19 deterministic edge cases ported from `spikes/crash_log` (§2, A1–A5);
//! - the A9 stale-generation and A10 resync-bait cases from
//!   `spikes/torn_write` (§3, §5);
//! - re-recovery idempotence (§1 / formal-model property 4).

use std::path::Path;

use mess_log::encode::{BatchEncoder, BatchInput, Subframe};
use mess_log::format::*;
use mess_log::runtime::{
    CrashPlan, Fault, FileHandle, Fs, OpenOpts, SectorPlan, SimFs, TailPlan,
};
use mess_log::scanner::{recover_segment, recover_segment_anchored, EpochAnchor, ScanStop};
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};

// ---------------------------------------------------------------------------
// Fixture helpers — all bytes come from the real encoder / writer.
// ---------------------------------------------------------------------------

/// A batch description the writer will stamp epoch/batch_id/pos onto.
struct Batch {
    stream_id: u64,
    category_id: u64,
    first_stream_version: u64,
    payloads: Vec<Vec<u8>>,
}

impl Batch {
    fn new(stream_id: u64, first_stream_version: u64, payloads: &[&[u8]]) -> Self {
        Batch {
            stream_id,
            category_id: 100 + stream_id,
            first_stream_version,
            payloads: payloads.iter().map(|p| p.to_vec()).collect(),
        }
    }
}

/// Read a file's whole durable image back through the Fs.
fn read_all(fs: &SimFs, path: &Path) -> Vec<u8> {
    let f = fs.open(path, OpenOpts::read_only()).expect("reopen");
    let len = f.len().unwrap() as usize;
    let mut buf = vec![0u8; len];
    let n = f.pread(0, &mut buf).unwrap();
    buf.truncate(n);
    buf
}

/// Write `batches` to a fresh segment via the real writer and return the
/// durable image (SegmentHeader + batches), all through the Fs.
fn build_segment(epoch: u64, base_pos: u64, batches: &[Batch]) -> Vec<u8> {
    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("build.seg");
    let mut w =
        SegmentWriter::create(&fs, path, SegmentParams::new(1, base_pos, epoch, 0)).unwrap();
    for b in batches {
        let sfs: Vec<Subframe> = b
            .payloads
            .iter()
            .map(|p| Subframe::plain(0x11, 0, 0, p))
            .collect();
        w.append(&BatchSpec {
            stream_id: b.stream_id,
            category_id: b.category_id,
            first_stream_version: b.first_stream_version,
            crypto_chain: None,
            subframes: &sfs,
        })
        .unwrap();
    }
    w.sync().unwrap();
    read_all(&fs, path)
}

/// A standalone batch's raw bytes (no segment header) via the encoder, letting
/// the caller stamp an arbitrary epoch / position — used to synthesize stale
/// and mispositioned batches that the writer would never emit.
fn encode_batch(
    epoch: u64,
    batch_id: u64,
    first_global_pos: u64,
    stream_id: u64,
    first_stream_version: u64,
    payloads: &[&[u8]],
) -> Vec<u8> {
    let sfs: Vec<Subframe> = payloads.iter().map(|p| Subframe::plain(0x11, 0, 0, p)).collect();
    let mut enc = BatchEncoder::new();
    enc.encode(&BatchInput {
        segment_epoch: epoch,
        batch_id,
        first_global_pos,
        stream_id,
        category_id: 100 + stream_id,
        first_stream_version,
        crypto_chain: None,
        subframes: &sfs,
    })
    .unwrap()
    .to_vec()
}

/// Seed `image` as a recycled/durable segment file on a fresh fault fs and
/// recover it through the Fs.
fn recover_image(image: Vec<u8>) -> mess_log::scanner::Recovery {
    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("seg");
    fs.seed(path, Fault::SECTOR_512, image);
    recover_segment(&fs, path).unwrap()
}

/// The two reference batches (mirrors crash_log's `batch0`/`batch1`).
fn batch0() -> Batch {
    Batch::new(0, 0, &[b"alpha", b"bravo-longer"]) // 2 events, pos 0..2
}
fn batch1() -> Batch {
    Batch::new(0, 2, &[b"charlie"]) // 1 event, pos 2..3
}

/// Offset of `batch0` in a segment: right after the header.
const B0_OFF: usize = SEGMENT_HEADER_LEN;

/// `total_len` of `batch0`: 72 + (28+5)+(28+12) + 16.
fn b0_len() -> usize {
    let img = build_segment(1, 0, &[batch0()]);
    let rec = recover_image(img);
    rec.accepted[0].total_len as usize
}

// ===========================================================================
// The 19 deterministic edge cases (ported from spikes/crash_log, spec-v3).
// ===========================================================================

// (1) An empty (header-only) segment scans clean — the v3 analog of the
// spike's empty log.
#[test]
fn c01_empty_segment_scans_clean() {
    let img = build_segment(7, 42, &[]);
    let rec = recover_image(img);
    assert!(rec.accepted.is_empty());
    assert_eq!(rec.stop, ScanStop::EndOfSegment);
    assert_eq!(rec.safe_offset, SEGMENT_HEADER_LEN as u64);
    assert_eq!(rec.next_pos, 42);
    assert_eq!(rec.next_batch_id, 0);
}

// (2) Torn batch header: fewer than HEADER_LEN bytes of the batch remain.
#[test]
fn c02_torn_header() {
    let mut img = build_segment(1, 0, &[batch0()]);
    img.truncate(B0_OFF + HEADER_LEN - 5);
    let rec = recover_image(img);
    assert!(rec.accepted.is_empty());
    assert_eq!(rec.stop, ScanStop::TornHeader);
    assert_eq!(rec.safe_offset, B0_OFF as u64);
}

// (3) Full header, partial frames: total_len exceeds remaining bytes.
#[test]
fn c03_header_but_partial_frames() {
    let mut img = build_segment(1, 0, &[batch0()]);
    img.truncate(B0_OFF + HEADER_LEN + 3);
    let rec = recover_image(img);
    assert!(rec.accepted.is_empty());
    assert_eq!(rec.stop, ScanStop::Incomplete);
}

// (4) Full frames but no marker: still incomplete (total_len needs the marker).
#[test]
fn c04_full_frames_but_no_marker() {
    let len = b0_len();
    let mut img = build_segment(1, 0, &[batch0()]);
    img.truncate(B0_OFF + len - MARKER_LEN);
    let rec = recover_image(img);
    assert!(rec.accepted.is_empty());
    assert_eq!(rec.stop, ScanStop::Incomplete);
}

// (5) Torn marker: last bytes of the marker missing.
#[test]
fn c05_torn_marker() {
    let len = b0_len();
    let mut img = build_segment(1, 0, &[batch0()]);
    img.truncate(B0_OFF + len - 7);
    let rec = recover_image(img);
    assert!(rec.accepted.is_empty());
    assert_eq!(rec.stop, ScanStop::Incomplete);
}

// (6) Marker CRC echo corrupted: A3 rejects before the CRC even runs.
#[test]
fn c06_marker_crc_echo_mismatch() {
    let len = b0_len();
    let mut img = build_segment(1, 0, &[batch0()]);
    let last = B0_OFF + len - 1;
    img[last] ^= 0xFF;
    let rec = recover_image(img);
    assert!(rec.accepted.is_empty());
    assert_eq!(rec.stop, ScanStop::BadMarker);
}

// (7) Marker total_len echo corrupted.
#[test]
fn c07_marker_len_echo_mismatch() {
    let len = b0_len();
    let mut img = build_segment(1, 0, &[batch0()]);
    // total_len_echo sits at marker+4 == (batch end - MARKER_LEN + 4).
    let echo = B0_OFF + len - MARKER_LEN + 4;
    img[echo] ^= 0x01;
    let rec = recover_image(img);
    assert!(rec.accepted.is_empty());
    assert_eq!(rec.stop, ScanStop::BadMarker);
}

// (8) A payload byte flipped: only the A4 CRC catches this (marker is intact).
#[test]
fn c08_payload_corruption_caught_by_crc() {
    let mut img = build_segment(1, 0, &[batch0()]);
    // First payload byte: header + first subframe header + 2.
    img[B0_OFF + HEADER_LEN + SUBFRAME_HDR_LEN + 2] ^= 0x40;
    let rec = recover_image(img);
    assert!(rec.accepted.is_empty());
    assert_eq!(rec.stop, ScanStop::BadCrc);
}

// (9) Garbage after a valid marker: long tail -> BadMagic; short tail ->
// TornHeader. In both, the first batch is accepted and the scan stops at it.
#[test]
fn c09_garbage_after_valid_marker() {
    let len = b0_len();
    let base = build_segment(1, 0, &[batch0()]);

    let mut long = base.clone();
    long.extend(std::iter::repeat_n(0xDBu8, HEADER_LEN + 10));
    let r = recover_image(long);
    assert_eq!(r.accepted.len(), 1);
    assert_eq!(r.stop, ScanStop::BadMagic);
    assert_eq!(r.safe_offset, (B0_OFF + len) as u64);
    assert_eq!(r.next_pos, 2);

    let mut short = base;
    short.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    let r2 = recover_image(short);
    assert_eq!(r2.accepted.len(), 1);
    assert_eq!(r2.stop, ScanStop::TornHeader);
    assert_eq!(r2.safe_offset, (B0_OFF + len) as u64);
}

// (10) Valid batch followed by a torn second batch: keep the first, stop at
// the second's offset.
#[test]
fn c10_valid_then_torn_second_batch() {
    let len0 = b0_len();
    let mut img = build_segment(1, 0, &[batch0(), batch1()]);
    // Truncate partway into batch1, keeping its full header but not its body.
    img.truncate(B0_OFF + len0 + HEADER_LEN + 8);
    let rec = recover_image(img);
    assert_eq!(rec.accepted.len(), 1);
    assert_eq!(rec.accepted[0].batch_id, 0);
    assert_eq!(rec.safe_offset, (B0_OFF + len0) as u64);
    assert_eq!(rec.stop, ScanStop::Incomplete);
}

// (11) Two valid batches: both accepted, clean tail, resume state correct.
#[test]
fn c11_two_valid_batches() {
    let img = build_segment(1, 0, &[batch0(), batch1()]);
    let rec = recover_image(img.clone());
    assert_eq!(rec.accepted.len(), 2);
    assert_eq!(rec.next_pos, 3);
    assert_eq!(rec.next_batch_id, 2);
    assert_eq!(rec.safe_offset, img.len() as u64);
    assert_eq!(rec.stop, ScanStop::EndOfSegment);
    // Stream head: stream 0 saw versions 0,1 (batch0) then 2 (batch1) -> 2.
    assert_eq!(rec.stream_heads.get(&0), Some(&2));
}

// (12) A1: a CRC-valid but mispositioned batch after the last good one is
// stale recycled data — the position guard rejects it.
#[test]
fn c12_stale_valid_batch_wrong_position() {
    let mut img = build_segment(1, 0, &[batch0()]); // covers pos 0..2
    // A perfectly valid batch, current epoch, but first_global_pos == 40.
    let stale = encode_batch(1, 99, 40, 0, 40, &[b"stale"]);
    img.extend_from_slice(&stale);
    let rec = recover_image(img);
    assert_eq!(rec.accepted.len(), 1);
    assert_eq!(rec.stop, ScanStop::PositionGap);
}

// (13) Wrong format_version stops the scan (checked before the CRC).
#[test]
fn c13_version_mismatch() {
    let mut img = build_segment(1, 0, &[batch0()]);
    img[B0_OFF + 4] = 0xEE; // format_version low byte
    let rec = recover_image(img);
    assert!(rec.accepted.is_empty());
    assert_eq!(rec.stop, ScanStop::BadVersion);
}

// (14) A2: an insane total_len (too large or too small) stops the scan.
#[test]
fn c14_insane_total_len() {
    let big = (MAX_BATCH_LEN + 1).to_le_bytes();
    let mut img = build_segment(1, 0, &[batch0()]);
    img[B0_OFF + 20..B0_OFF + 28].copy_from_slice(&big); // total_len field
    assert_eq!(recover_image(img).stop, ScanStop::BadLength);

    let tiny = 5u64.to_le_bytes();
    let mut img2 = build_segment(1, 0, &[batch0()]);
    img2[B0_OFF + 20..B0_OFF + 28].copy_from_slice(&tiny);
    assert_eq!(recover_image(img2).stop, ScanStop::BadLength);
}

// (15) Crash mid batch-header, modelled by the torn-tail fault medium.
#[test]
fn c15_crash_mid_header() {
    let fs = SimFs::new(Fault::Tail);
    let path = Path::new("seg");
    let mut w = SegmentWriter::create(&fs, path, SegmentParams::new(1, 0, 1, 0)).unwrap();
    let p0 = b"alpha".to_vec();
    let p1 = b"bravo-longer".to_vec();
    let sfs = [Subframe::plain(0x11, 0, 0, &p0), Subframe::plain(0x11, 0, 0, &p1)];
    w.append(&BatchSpec {
        stream_id: 0,
        category_id: 100,
        first_stream_version: 0,
        crypto_chain: None,
        subframes: &sfs,
    })
    .unwrap();
    // Header fdatasync'd (synced == 52); the batch pwrite is unsynced. Keep
    // only 36 more bytes: the batch header is torn.
    fs.crash(path, CrashPlan::Tail(TailPlan { keep: B0_OFF + 36, scramble: vec![] }))
        .unwrap();
    let rec = recover_segment(&fs, path).unwrap();
    assert!(rec.accepted.is_empty());
    assert_eq!(rec.stop, ScanStop::TornHeader);
    assert_eq!(rec.safe_offset, B0_OFF as u64);
}

// (16) Crash mid-frames.
#[test]
fn c16_crash_mid_frames() {
    let fs = SimFs::new(Fault::Tail);
    let path = Path::new("seg");
    let mut w = SegmentWriter::create(&fs, path, SegmentParams::new(1, 0, 1, 0)).unwrap();
    let p0 = b"alpha".to_vec();
    let p1 = b"bravo-longer".to_vec();
    let sfs = [Subframe::plain(0x11, 0, 0, &p0), Subframe::plain(0x11, 0, 0, &p1)];
    w.append(&BatchSpec {
        stream_id: 0,
        category_id: 100,
        first_stream_version: 0,
        crypto_chain: None,
        subframes: &sfs,
    })
    .unwrap();
    fs.crash(path, CrashPlan::Tail(TailPlan { keep: B0_OFF + HEADER_LEN + 6, scramble: vec![] }))
        .unwrap();
    let rec = recover_segment(&fs, path).unwrap();
    assert!(rec.accepted.is_empty());
    assert_eq!(rec.stop, ScanStop::Incomplete);
}

// (17) Crash mid-marker.
#[test]
fn c17_crash_mid_marker() {
    let len = b0_len();
    let fs = SimFs::new(Fault::Tail);
    let path = Path::new("seg");
    let mut w = SegmentWriter::create(&fs, path, SegmentParams::new(1, 0, 1, 0)).unwrap();
    let p0 = b"alpha".to_vec();
    let p1 = b"bravo-longer".to_vec();
    let sfs = [Subframe::plain(0x11, 0, 0, &p0), Subframe::plain(0x11, 0, 0, &p1)];
    w.append(&BatchSpec {
        stream_id: 0,
        category_id: 100,
        first_stream_version: 0,
        crypto_chain: None,
        subframes: &sfs,
    })
    .unwrap();
    fs.crash(path, CrashPlan::Tail(TailPlan { keep: B0_OFF + len - 5, scramble: vec![] }))
        .unwrap();
    let rec = recover_segment(&fs, path).unwrap();
    assert!(rec.accepted.is_empty());
    assert_eq!(rec.stop, ScanStop::Incomplete);
}

// (18) Crash after the marker but before fsync (A6). If the tail survived, the
// complete-but-unacked batch MAY surface; if it was lost, nothing surfaces.
#[test]
fn c18_crash_after_marker_before_fsync() {
    let len = b0_len();
    let build = |keep: usize| {
        let fs = SimFs::new(Fault::Tail);
        let path = Path::new("seg");
        let mut w = SegmentWriter::create(&fs, path, SegmentParams::new(1, 0, 1, 0)).unwrap();
        let p0 = b"alpha".to_vec();
        let p1 = b"bravo-longer".to_vec();
        let sfs = [Subframe::plain(0x11, 0, 0, &p0), Subframe::plain(0x11, 0, 0, &p1)];
        w.append(&BatchSpec {
            stream_id: 0,
            category_id: 100,
            first_stream_version: 0,
            crypto_chain: None,
            subframes: &sfs,
        })
        .unwrap();
        // No sync after append: the batch is unacknowledged.
        fs.crash(path, CrashPlan::Tail(TailPlan { keep, scramble: vec![] })).unwrap();
        recover_segment(&fs, path).unwrap()
    };

    // Tail survived in full: A6 permits surfacing the complete unacked batch.
    let surfaced = build(B0_OFF + len);
    assert_eq!(surfaced.accepted.len(), 1);
    assert_eq!(surfaced.stop, ScanStop::EndOfSegment);

    // Tail lost back to the fsync watermark (52): nothing surfaces.
    let lost = build(B0_OFF);
    assert!(lost.accepted.is_empty());
    assert_eq!(lost.stop, ScanStop::EndOfSegment);
}

// (19) Crash after fsync of batch0 while batch1 is in flight: batch0 is acked
// and always recovers; batch1 either surfaces (A6) or is lost, never partial.
#[test]
fn c19_crash_after_fsync() {
    let len0 = b0_len();
    let fs = SimFs::new(Fault::Tail);
    let path = Path::new("seg");
    let mut w = SegmentWriter::create(&fs, path, SegmentParams::new(1, 0, 1, 0)).unwrap();
    let p0 = b"alpha".to_vec();
    let p1 = b"bravo-longer".to_vec();
    let sfs0 = [Subframe::plain(0x11, 0, 0, &p0), Subframe::plain(0x11, 0, 0, &p1)];
    w.append(&BatchSpec {
        stream_id: 0,
        category_id: 100,
        first_stream_version: 0,
        crypto_chain: None,
        subframes: &sfs0,
    })
    .unwrap();
    w.sync().unwrap(); // batch0 ACKED (synced == 52 + len0)
    let p2 = b"charlie".to_vec();
    let sfs1 = [Subframe::plain(0x11, 0, 0, &p2)];
    w.append(&BatchSpec {
        stream_id: 0,
        category_id: 100,
        first_stream_version: 2,
        crypto_chain: None,
        subframes: &sfs1,
    })
    .unwrap();
    // Crash keeping only the acked prefix: batch1 lost.
    fs.crash(path, CrashPlan::Tail(TailPlan { keep: B0_OFF + len0, scramble: vec![] }))
        .unwrap();
    let rec = recover_segment(&fs, path).unwrap();
    assert_eq!(rec.accepted.len(), 1, "acked batch0 must recover");
    assert_eq!(rec.accepted[0].batch_id, 0);
    assert_eq!(rec.next_pos, 2);
}

// ===========================================================================
// A9 — stale-generation adversarial cases (spikes/torn_write, §5).
// ===========================================================================

// The A9 headline: a recycled segment carries a fresh SegmentHeader (epoch
// E_new) but its post-header disk region still holds an intact stale batch from
// a previous generation (epoch E_old) at the *coincident* expected position.
// Zero new sectors persisted. Full byte validation (magic+echoes+CRC+A1)
// ACCEPTS it; only the epoch check rejects.
#[test]
fn a9_stale_generation_at_coincident_position_rejected() {
    let e_old = 4u64;
    let e_new = 9u64;
    let base_pos = 0u64;

    // Header for the new generation (via the real writer).
    let header = build_segment(e_new, base_pos, &[]);
    assert_eq!(header.len(), SEGMENT_HEADER_LEN);

    // Stale batch encoded under the OLD epoch, at the coincident position the
    // fresh scan expects — a genuine byte-valid fossil.
    let stale = encode_batch(e_old, 0, base_pos, 0, 0, &[b"old secret"]);

    let mut recycled = header;
    recycled.extend_from_slice(&stale);

    let rec = recover_image(recycled);
    assert!(rec.accepted.is_empty(), "the stale prior-generation batch must be rejected");
    assert_eq!(rec.stop, ScanStop::EpochMismatch);
    assert_eq!(rec.safe_offset, SEGMENT_HEADER_LEN as u64);
    assert_eq!(rec.next_pos, base_pos);
}

// Proof the fossil is genuinely byte-valid: stamped under E_new it is accepted.
// So only the A9 epoch check — not any byte defect — kept it dead above.
#[test]
fn a9_same_bytes_current_epoch_are_accepted() {
    let base_pos = 0u64;
    let header = build_segment(9, base_pos, &[]);
    let fresh = encode_batch(9, 0, base_pos, 0, 0, &[b"old secret"]);
    let mut img = header;
    img.extend_from_slice(&fresh);
    let rec = recover_image(img);
    assert_eq!(rec.accepted.len(), 1);
    assert_eq!(rec.stop, ScanStop::EndOfSegment);
}

// The §5 durable-anchor cross-check: if a trusted anchor names an epoch newer
// than the header on disk, the header did not survive and the segment holds no
// committed batches of the new generation.
#[test]
fn a9_anchor_newer_than_header_yields_no_batches() {
    // A perfectly good segment at epoch 5 with one batch.
    let img = build_segment(5, 0, &[batch0()]);
    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("seg");
    fs.seed(path, Fault::SECTOR_512, img);

    // Without the anchor, its batch recovers.
    let plain = recover_segment(&fs, path).unwrap();
    assert_eq!(plain.accepted.len(), 1);

    // With an anchor naming epoch 6 (the manifest/predecessor knows the segment
    // was rolled to a newer generation whose header did not survive here):
    let anchored =
        recover_segment_anchored(&fs, path, Some(EpochAnchor { epoch: 6 })).unwrap();
    assert!(anchored.accepted.is_empty());
    assert_eq!(anchored.stop, ScanStop::EpochMismatch);
}

// ===========================================================================
// A10 — resync-bait: the first stop is terminal, never resync past a hole.
// ===========================================================================

// batch0 accepted; batch1 is a hole (payload corrupted); batch2 is fully valid
// and position-contiguous "bait" sitting past the hole. A scanner that resynced
// to the next HEADER_MAGIC would resurrect batch2 and punch a hole in history.
#[test]
fn a10_hole_then_valid_bait_must_not_resync() {
    let img_clean = build_segment(1, 0, &[batch0(), batch1(), Batch::new(0, 3, &[b"delta"])]);
    let rec_clean = recover_image(img_clean.clone());
    assert_eq!(rec_clean.accepted.len(), 3, "sanity: the clean image accepts all three");

    let b1_off = rec_clean.accepted[1].offset as usize;
    let b2_off = rec_clean.accepted[2].offset as usize;

    // Punch a hole in batch1: flip a byte in its payload region (BadCrc).
    let mut holed = img_clean;
    holed[b1_off + HEADER_LEN + SUBFRAME_HDR_LEN] ^= 0x80;

    let rec = recover_image(holed);
    assert_eq!(rec.accepted.len(), 1, "only batch0 is committed; must stop at the hole");
    assert_eq!(rec.accepted[0].batch_id, 0);
    assert_eq!(rec.stop, ScanStop::BadCrc);
    assert_eq!(rec.safe_offset, b1_off as u64, "safe offset is the hole, not past batch2");

    // Prove batch2 (the bait) IS intrinsically valid: standing alone at its own
    // position it is accepted — only A10's stop-at-first-failure kept it dead.
    let mut isolated = build_segment(1, 3, &[]); // header seeded at base_pos 3
    let bait = encode_batch(1, 0, 3, 0, 3, &[b"delta"]);
    isolated.extend_from_slice(&bait);
    let iso = recover_image(isolated);
    assert_eq!(iso.accepted.len(), 1, "the bait validates in isolation");
    let _ = b2_off;
}

// A10 under an authentic sector reorder: batch1's sectors all persisted while
// batch0's did not. batch1 would validate in isolation, but the scan must stop
// at the (now missing/garbage) batch0 and never reach it.
#[test]
fn a10_sector_reorder_hole_must_not_resync() {
    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("seg");
    // Big batches so each spans several 512-byte sectors distinct from the other.
    let big0 = vec![0x11u8; 1200];
    let big1 = vec![0x22u8; 1200];
    let mut w = SegmentWriter::create(&fs, path, SegmentParams::new(1, 0, 1, 0)).unwrap();
    let sfs0 = [Subframe::plain(0x11, 0, 0, &big0)];
    let r0 = w
        .append(&BatchSpec {
            stream_id: 0,
            category_id: 100,
            first_stream_version: 0,
            crypto_chain: None,
            subframes: &sfs0,
        })
        .unwrap();
    let sfs1 = [Subframe::plain(0x11, 0, 0, &big1)];
    let r1 = w
        .append(&BatchSpec {
            stream_id: 0,
            category_id: 100,
            first_stream_version: 1,
            crypto_chain: None,
            subframes: &sfs1,
        })
        .unwrap();
    // Persist only the sectors fully inside batch1; drop batch0's body sectors.
    let ss = 512usize;
    let b1_start = r1.offset as usize;
    let b1_end = (r1.offset + r1.total_len) as usize;
    let b0_start = r0.offset as usize;
    // Sectors strictly inside batch1 (not shared with batch0's start).
    let first_full = b1_start.div_ceil(ss);
    let last_full = (b1_end / ss).saturating_sub(1);
    let persist: Vec<usize> = (first_full..=last_full).collect();
    // Ensure we are genuinely leaving a hole in batch0's body.
    assert!(b0_start / ss < first_full);
    fs.crash(path, CrashPlan::Sector(SectorPlan { persist, tear: None })).unwrap();

    let rec = recover_segment(&fs, path).unwrap();
    // batch0's body is a hole; the scan must accept nothing past it.
    assert!(
        rec.accepted.is_empty() || rec.accepted.iter().all(|b| b.batch_id == 0),
        "must never accept batch1 by resyncing past batch0's hole"
    );
    assert!(rec.accepted.iter().all(|b| b.batch_id != 1), "batch1 must not resurface via resync");
}

// ===========================================================================
// Re-recovery idempotence (§1 / formal-model property 4).
// ===========================================================================

#[test]
fn idempotence_scan_twice_no_truncate_identical() {
    // An image with a hole so the scan actually stops mid-segment.
    let img_clean = build_segment(1, 0, &[batch0(), batch1(), Batch::new(0, 3, &[b"delta"])]);
    let b1_off = recover_image(img_clean.clone()).accepted[1].offset as usize;
    let mut holed = img_clean;
    holed[b1_off + HEADER_LEN + SUBFRAME_HDR_LEN] ^= 0x80;

    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("seg");
    fs.seed(path, Fault::SECTOR_512, holed);

    let first = recover_segment(&fs, path).unwrap();
    let second = recover_segment(&fs, path).unwrap();
    assert_eq!(first, second, "scanning the same durable image twice must be identical");
}

#[test]
fn idempotence_truncate_to_safe_offset_then_rescan_identical() {
    let img_clean = build_segment(1, 0, &[batch0(), batch1(), Batch::new(0, 3, &[b"delta"])]);
    let b1_off = recover_image(img_clean.clone()).accepted[1].offset as usize;
    let mut holed = img_clean;
    holed[b1_off + HEADER_LEN + SUBFRAME_HDR_LEN] ^= 0x80;

    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("seg");
    fs.seed(path, Fault::SECTOR_512, holed.clone());
    let first = recover_segment(&fs, path).unwrap();
    assert_eq!(first.accepted.len(), 1);
    assert_eq!(first.stop, ScanStop::BadCrc);

    // Truncate to the safe offset and re-scan on a fresh fault fs.
    let truncated = holed[..first.safe_offset as usize].to_vec();
    let fs2 = SimFs::new(Fault::SECTOR_512);
    let path2 = Path::new("seg2");
    fs2.seed(path2, Fault::SECTOR_512, truncated);
    let after = recover_segment(&fs2, path2).unwrap();

    // Identical committed prefix and resume state; the tail is now a clean end.
    assert_eq!(after.accepted, first.accepted);
    assert_eq!(after.next_pos, first.next_pos);
    assert_eq!(after.next_batch_id, first.next_batch_id);
    assert_eq!(after.safe_offset, first.safe_offset);
    assert_eq!(after.stop, ScanStop::EndOfSegment, "truncation turns the hole into a clean tail");
    assert_eq!(after.stream_heads, first.stream_heads);
}
