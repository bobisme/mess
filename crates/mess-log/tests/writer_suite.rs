//! Runtime-agnostic behavioural suite for [`SegmentWriter`], run against
//! **both** [`RealRuntime`] and [`SimRuntime`] — the same pattern the runtime
//! seam's own `testsuite.rs` uses (each check is generic over `R: Runtime`;
//! two entry points feed it the real and the simulated runtime). This is the
//! acceptance requirement "the writer's suite runs against BOTH runtime impls".

use std::path::{Path, PathBuf};

use mess_log::encode::Subframe;
use mess_log::format::*;
use mess_log::runtime::{Fs, FileHandle, OpenOpts, RealRuntime, Runtime, SimRuntime};
use mess_log::writer::{read_segment_header_epoch, BatchSpec, SegmentParams, SegmentWriter, WriteError};

/// The §4.7 one-frame batch, re-derived through the full write path (same
/// bytes as `tests/golden.rs` fixture 1 when the segment has `epoch = 1`,
/// `base_pos = 0`).
fn f1_hex() -> Vec<u8> {
    hx("ad4e7cba03000000010000000000000000000000800000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000a3475c291100000000000000000000000c0000000c000000000000000c000000000102030405060708090a0bed17aac08000000000000000a3475c29")
}

/// The `SegmentHeader` golden (segment_id=1, base_pos=0, epoch=1, created=0,
/// prev=0), computed by the same independent CRC32C reference as the batch
/// fixtures (§3.2).
const SEG_HEADER_HEX: &str =
    "ad1e605e030000000100000000000000000000000000000001000000000000000000000000000000000000000000000082bba8bc";

fn hx(s: &str) -> Vec<u8> {
    let s: Vec<u8> = s.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    s.chunks(2)
        .map(|c| {
            let hi = (c[0] as char).to_digit(16).unwrap() as u8;
            let lo = (c[1] as char).to_digit(16).unwrap() as u8;
            (hi << 4) | lo
        })
        .collect()
}

fn read_all<R: Runtime>(rt: &R, path: &Path, off: u64, len: usize) -> Vec<u8> {
    let f = rt.fs().open(path, OpenOpts::read_only()).expect("reopen");
    let mut buf = vec![0u8; len];
    let n = f.pread(off, &mut buf).expect("pread");
    buf.truncate(n);
    buf
}

// ---------------------------------------------------------------------------
// Generic checks — each is run once per runtime.
// ---------------------------------------------------------------------------

/// Open writes the SegmentHeader golden at offset 0; appending the §4.7 batch
/// lands the golden bytes at offset 52, and the header epoch reads back.
fn writer_header_and_batch_golden<R: Runtime>(rt: &R, path: &Path) {
    let fs = rt.fs();
    let mut w = SegmentWriter::create(&fs, path, SegmentParams::new(1, 0, 1, 0)).unwrap();

    // SegmentHeader golden at [0, 52).
    let hdr = read_all(rt, path, 0, SEGMENT_HEADER_LEN);
    assert_eq!(hdr, hx(SEG_HEADER_HEX), "SegmentHeader bytes must match the spec-derived golden");
    assert_eq!(read_segment_header_epoch(&hdr), Some(1));

    // Append the §4.7 batch; the writer stamps epoch=1, batch_id=0, pos=0.
    let payload: Vec<u8> = (0u8..12).collect();
    let sfs = [Subframe::plain(0x11, 0, 0, &payload)];
    let receipt = w
        .append(&BatchSpec { stream_id: 0, category_id: 0, first_stream_version: 0, crypto_chain: None, subframes: &sfs })
        .unwrap();
    assert_eq!(receipt.offset, SEGMENT_HEADER_LEN as u64);
    assert_eq!(receipt.total_len, 128);
    assert_eq!(receipt.batch_id, 0);
    assert_eq!(receipt.first_global_pos, 0);
    w.sync().unwrap();

    let batch = read_all(rt, path, SEGMENT_HEADER_LEN as u64, 128);
    assert_eq!(batch, f1_hex(), "batch bytes on disk must match golden fixture 1");
}

/// A5: an empty batch is rejected without touching the file offset.
fn writer_rejects_empty_batch<R: Runtime>(rt: &R, path: &Path) {
    let fs = rt.fs();
    let mut w = SegmentWriter::create(&fs, path, SegmentParams::new(2, 0, 5, 0)).unwrap();
    let empty: [Subframe; 0] = [];
    let err = w
        .append(&BatchSpec { stream_id: 1, category_id: 0, first_stream_version: 0, crypto_chain: None, subframes: &empty })
        .unwrap_err();
    assert!(matches!(err, WriteError::Encode(mess_log::encode::EncodeError::EmptyBatch)));
    // Offset unchanged: nothing was written past the header.
    assert_eq!(w.summary().content_len, SEGMENT_HEADER_LEN as u64);
}

/// A1/D-FMT-5: positions are contiguous and batch_ids increment per segment;
/// the running position seeds from base_pos.
fn writer_position_accounting<R: Runtime>(rt: &R, path: &Path) {
    let fs = rt.fs();
    let base = 1000u64;
    let mut w = SegmentWriter::create(&fs, path, SegmentParams::new(3, base, 9, 0)).unwrap();

    let p2 = [0u8; 2];
    let s2 = [Subframe::plain(1, 0, 0, &p2), Subframe::plain(1, 0, 0, &p2)]; // 2 frames
    let s1 = [Subframe::plain(2, 0, 0, &p2)]; // 1 frame

    let r0 = w.append(&spec(&s2)).unwrap();
    let r1 = w.append(&spec(&s1)).unwrap();
    let r2 = w.append(&spec(&s2)).unwrap();

    assert_eq!((r0.batch_id, r0.first_global_pos, r0.frame_count), (0, base, 2));
    assert_eq!((r1.batch_id, r1.first_global_pos, r1.frame_count), (1, base + 2, 1));
    assert_eq!((r2.batch_id, r2.first_global_pos, r2.frame_count), (2, base + 3, 2));

    let sum = w.summary();
    assert_eq!(sum.batch_count, 3);
    assert_eq!(sum.event_count, 5);
    assert_eq!(sum.end_pos, base + 5);
    // Batches are contiguous: content_len == header + Σ total_len.
    assert_eq!(sum.content_len, r2.offset + r2.total_len);
}

/// A8: a batch that will not fit is rejected with SegmentFull and does not
/// advance the writer; rolling to the next segment continues the A1/A9 chain,
/// and the rolled segment carries a strictly larger epoch and the right
/// base_pos.
fn writer_rolls_on_segment_full<R: Runtime>(rt: &R, p0: &Path, p1: &Path) {
    let fs = rt.fs();
    // A tiny segment that fits exactly one 128-byte batch after the 52-byte
    // header (52 + 128 = 180) but not two.
    let mut params = SegmentParams::new(10, 0, 100, 0);
    params.segment_size = 180;
    let mut w = SegmentWriter::create(&fs, p0, params).unwrap();

    let payload = [0u8; 12];
    let sfs = [Subframe::plain(1, 0, 0, &payload)]; // 128-byte batch
    w.append(&spec(&sfs)).unwrap();

    // Second batch does not fit -> A8 SegmentFull, writer unchanged.
    let before = w.summary();
    let err = w.append(&spec(&sfs)).unwrap_err();
    match err {
        WriteError::SegmentFull { needed, remaining } => {
            assert_eq!(needed, 128);
            assert_eq!(remaining, 0);
        }
        other => panic!("expected SegmentFull, got {other:?}"),
    }
    assert_eq!(w.summary(), before, "a rejected append must not advance the writer");
    assert!(!w.would_fit(&spec(&sfs)).unwrap());

    // Roll: close p0 unsealed, open p1 continuing the chain.
    let end_pos = w.next_pos();
    let prev_epoch = w.epoch();
    let mut w2 = w.roll(p1, 11, 101, 0).unwrap();
    // The rolled segment's header carries base_pos = end_pos, epoch 101.
    let hdr = read_all(rt, p1, 0, SEGMENT_HEADER_LEN);
    assert_eq!(read_segment_header_epoch(&hdr), Some(101));
    assert!(101 > prev_epoch);

    // First batch of the new segment resumes at end_pos with batch_id 0.
    let r = w2.append(&spec(&sfs)).unwrap();
    assert_eq!(r.batch_id, 0);
    assert_eq!(r.first_global_pos, end_pos);
    assert_eq!(w2.summary().base_pos, end_pos);
}

/// The unsealed handoff: close writes no trailer, so the segment file is
/// exactly header+batches long (no SegmentFooter). Recovery treats a
/// trailer-less segment as the active/unsealed segment (02 §8.3).
fn writer_close_leaves_unsealed<R: Runtime>(rt: &R, path: &Path) {
    let fs = rt.fs();
    let mut w = SegmentWriter::create(&fs, path, SegmentParams::new(4, 0, 7, 0)).unwrap();
    let payload = [0u8; 12];
    let sfs = [Subframe::plain(1, 0, 0, &payload)];
    w.append(&spec(&sfs)).unwrap();
    let sum = w.close().unwrap();
    // File length == content_len == header + one 128-byte batch. No footer.
    let f = fs.open(path, OpenOpts::read_only()).unwrap();
    assert_eq!(f.len().unwrap(), sum.content_len);
    assert_eq!(sum.content_len, SEGMENT_HEADER_LEN as u64 + 128);
}

fn spec<'a, 'p>(subframes: &'a [Subframe<'p>]) -> BatchSpec<'a, 'p> {
    BatchSpec { stream_id: 1, category_id: 0, first_stream_version: 0, crypto_chain: None, subframes }
}

// ---------------------------------------------------------------------------
// Real runtime entry points
// ---------------------------------------------------------------------------

struct Cleanup(Vec<PathBuf>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        for p in &self.0 {
            let _ = std::fs::remove_file(p);
        }
    }
}

fn tmp(name: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let mut p = std::env::temp_dir();
    p.push(format!("mess-log-writer-{}-{}-{}", std::process::id(), n, name));
    p
}

#[test]
fn real_writer_header_and_batch_golden() {
    let p = tmp("golden");
    let _c = Cleanup(vec![p.clone()]);
    writer_header_and_batch_golden(&RealRuntime::new(), &p);
}

#[test]
fn real_writer_rejects_empty_batch() {
    let p = tmp("empty");
    let _c = Cleanup(vec![p.clone()]);
    writer_rejects_empty_batch(&RealRuntime::new(), &p);
}

#[test]
fn real_writer_position_accounting() {
    let p = tmp("pos");
    let _c = Cleanup(vec![p.clone()]);
    writer_position_accounting(&RealRuntime::new(), &p);
}

#[test]
fn real_writer_rolls_on_segment_full() {
    let p0 = tmp("roll-0");
    let p1 = tmp("roll-1");
    let _c = Cleanup(vec![p0.clone(), p1.clone()]);
    writer_rolls_on_segment_full(&RealRuntime::new(), &p0, &p1);
}

#[test]
fn real_writer_close_leaves_unsealed() {
    let p = tmp("unsealed");
    let _c = Cleanup(vec![p.clone()]);
    writer_close_leaves_unsealed(&RealRuntime::new(), &p);
}

// ---------------------------------------------------------------------------
// Sim runtime entry points (fault-fs, in-memory paths)
// ---------------------------------------------------------------------------

#[test]
fn sim_writer_header_and_batch_golden() {
    writer_header_and_batch_golden(&SimRuntime::new(1), Path::new("/seg-golden"));
}

#[test]
fn sim_writer_rejects_empty_batch() {
    writer_rejects_empty_batch(&SimRuntime::new(1), Path::new("/seg-empty"));
}

#[test]
fn sim_writer_position_accounting() {
    writer_position_accounting(&SimRuntime::new(1), Path::new("/seg-pos"));
}

#[test]
fn sim_writer_rolls_on_segment_full() {
    writer_rolls_on_segment_full(&SimRuntime::new(1), Path::new("/seg-roll-0"), Path::new("/seg-roll-1"));
}

#[test]
fn sim_writer_close_leaves_unsealed() {
    writer_close_leaves_unsealed(&SimRuntime::new(1), Path::new("/seg-unsealed"));
}
