//! Conformance suite for segment sealing and the R2 fast path (bn-sbt).
//!
//! Spec: [`docs/spec/01-log-format.md`] §3.3 (`SegmentFooter`) / §6 (seal
//! semantics) and [`docs/spec/02-recovery.md`] §8.3 (footer validation, the R2
//! last-segment-only fast path, corrupt-footer fallback) / §8.4 (fast-path /
//! full-recovery equivalence).
//!
//! The runtime-agnostic checks run against **both** [`RealRuntime`] and
//! [`SimRuntime`] (the writer-suite pattern). The recycling-discipline checks
//! that must plant *stale prior-generation* bytes behind a fresh header use the
//! sim fault fs directly (its [`SimFs::seed`] is the resurrected-region model).

use std::path::{Path, PathBuf};

use mess_log::encode::{BatchEncoder, BatchInput, Subframe};
use mess_log::format::*;
use mess_log::runtime::{
    Fault, FileHandle, Fs, OpenOpts, RealRuntime, Runtime, SimFs, SimRuntime,
};
use mess_log::scanner::{ScanStop, recover_segment};
use mess_log::sealer::{
    FastRecovery, decode_trailer, read_trailer, recover_fast,
};
use mess_log::writer::{
    BatchSpec, SegmentParams, SegmentWriter, read_segment_header_epoch,
};

// Trailer-relative field offsets (§3.3.1), used only to corrupt on-disk bytes.
const T_MAGIC_OFF: u64 = 0;
const T_EPOCH_OFF: u64 = 16; // inside footer_crc coverage, not the magic/version

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn spec<'a, 'p>(
    stream_id: u64,
    first_v: u64,
    subframes: &'a [Subframe<'p>],
) -> BatchSpec<'a, 'p> {
    BatchSpec {
        stream_id,
        category_id: 100 + stream_id,
        first_stream_version: first_v,
        crypto_chain: None,
        subframes,
    }
}

/// Append three batches (2, 1, 2 events = 5 events, 3 batches) to `w`.
fn append_three<F: Fs>(w: &mut SegmentWriter<F>) {
    let p = [0u8; 4];
    let two =
        [Subframe::plain(0x11, 0, 0, &p), Subframe::plain(0x11, 0, 0, &p)];
    let one = [Subframe::plain(0x11, 0, 0, &p)];
    w.append(&spec(1, 0, &two)).unwrap();
    w.append(&spec(1, 2, &one)).unwrap();
    w.append(&spec(2, 0, &two)).unwrap();
}

fn file_len<F: Fs>(fs: &F, path: &Path) -> u64 {
    fs.open(path, OpenOpts::read_only()).unwrap().len().unwrap()
}

/// Flip one byte at absolute `off` in `path` (durably), to corrupt on-disk
/// bytes through the Fs seam.
fn flip_byte<F: Fs>(fs: &F, path: &Path, off: u64) {
    let f = fs.open(path, OpenOpts::create_rw()).unwrap();
    let mut b = [0u8; 1];
    f.pread(off, &mut b).unwrap();
    b[0] ^= 0xFF;
    f.pwrite(off, &b).unwrap();
    f.fdatasync().unwrap();
}

// ---------------------------------------------------------------------------
// Generic checks (run once per runtime)
// ---------------------------------------------------------------------------

/// Sealing writes the fixed trailer at `content_len`; the sealed file is
/// `content_len + SEGMENT_TRAILER_LEN` bytes and the trailer round-trips via a
/// pread-from-EOF (R2). The trailer's catalog fields equal the writer summary.
fn seal_roundtrips<R: Runtime>(rt: &R, path: &Path) {
    let fs = rt.fs();
    let mut w =
        SegmentWriter::create(&fs, path, SegmentParams::new(3, 1000, 42, 7))
            .unwrap();
    append_three(&mut w);
    let summary = w.seal().unwrap();

    // File length grew by exactly the fixed trailer; nothing else was appended.
    assert_eq!(
        file_len(&fs, path),
        summary.content_len + SEGMENT_TRAILER_LEN as u64,
        "sealed file = content + 100-byte trailer at EOF"
    );

    // R2 pread-from-EOF: the trailer validates and its catalog matches the
    // writer's summary (§3.3.1 fields).
    let cat = read_trailer(&fs, path)
        .unwrap()
        .expect("sealed segment has a valid trailer");
    assert_eq!(cat.segment_id, 3);
    assert_eq!(cat.epoch, 42); // R3: the trailer carries the A9 epoch
    assert_eq!(cat.base_pos, 1000);
    assert_eq!(cat.batch_count, 3);
    assert_eq!(cat.event_count, 5);
    assert_eq!(cat.end_pos, 1005); // base_pos + event_count (§8.1 seed)
    assert_eq!(cat.ext_offset, summary.content_len);
    assert_eq!(cat.ext_len, 0); // Phase 3: empty extension is legal
    assert_eq!(cat.ext_crc, 0); // §3.3.1: ext_crc MUST be 0 when ext_len == 0
}

/// §8.3 R2 fast path: a validly sealed segment is trusted via its trailer
/// (`Sealed`, no body scan) and its catalog agrees with a full scan (§8.4
/// fast-path/full-recovery equivalence).
fn fast_path_matches_full_scan<R: Runtime>(rt: &R, path: &Path) {
    let fs = rt.fs();
    let mut w =
        SegmentWriter::create(&fs, path, SegmentParams::new(5, 0, 9, 0))
            .unwrap();
    append_three(&mut w);
    w.seal().unwrap();

    let fast = recover_fast(&fs, path).unwrap();
    let FastRecovery::Sealed { catalog, header } = &fast else {
        panic!("a valid seal must take the R2 fast path, got {fast:?}");
    };
    assert_eq!(header.epoch, 9);
    assert_eq!(catalog.batch_count, 3);
    assert_eq!(catalog.event_count, 5);

    // Full scan of the same file (the authority, D1) must agree on the prefix.
    // Scanning a sealed file walks every batch, then stops on the trailer's
    // FOOTER_MAGIC (not a BatchHeader magic) at content_len.
    let full = recover_segment(&fs, path).unwrap();
    assert_eq!(
        full.batch_count() as u64,
        catalog.batch_count,
        "§8.4 equivalence"
    );
    assert_eq!(full.next_pos, catalog.end_pos);
    assert_eq!(full.accepted.last().unwrap().first_global_pos, 3); // 3rd batch at pos 3
    assert_eq!(fast.end_pos(), full.next_pos);
    assert_eq!(fast.epoch(), Some(9));
}

/// §8.3 corrupt-footer fallback: flipping a byte inside `footer_crc` coverage
/// makes the trailer fail to validate, so `recover_fast` treats the segment as
/// **unsealed** and returns the authoritative full scan — losing no committed
/// batch.
fn corrupt_footer_falls_back_to_scan<R: Runtime>(rt: &R, path: &Path) {
    let fs = rt.fs();
    let mut w =
        SegmentWriter::create(&fs, path, SegmentParams::new(6, 0, 11, 0))
            .unwrap();
    append_three(&mut w);
    let summary = w.seal().unwrap();

    // Corrupt the trailer's epoch byte: magic + version still parse, but
    // footer_crc no longer matches ⇒ §8.3 "treat as not sealed".
    let trailer_off = summary.content_len; // trailer begins here (ext_len == 0)
    flip_byte(&fs, path, trailer_off + T_EPOCH_OFF);

    // The trailer no longer validates.
    assert!(
        read_trailer(&fs, path).unwrap().is_none(),
        "corrupt footer_crc ⇒ no trailer"
    );

    // recover_fast falls back to a full scan and still recovers every batch.
    let rec = recover_fast(&fs, path).unwrap();
    let FastRecovery::Scanned(r) = &rec else {
        panic!("a corrupt footer must fall back to a full scan, got {rec:?}");
    };
    assert_eq!(r.batch_count(), 3, "the committed prefix survives a torn seal");
    assert_eq!(r.next_pos, 5);
    assert_eq!(rec.batch_count(), 3);
}

/// A9 / base_pos continuity across a **sealed** roll (§8.1). `roll_sealed`
/// seals segment k (its trailer carries epoch e_k and end_pos E) and opens
/// segment k+1 whose header continues the chain: `prev_segment_epoch == e_k`,
/// `base_pos == E`, and a strictly larger `epoch` (A9).
fn a9_chain_across_sealed_roll<R: Runtime>(rt: &R, p0: &Path, p1: &Path) {
    let fs = rt.fs();
    let mut w0 =
        SegmentWriter::create(&fs, p0, SegmentParams::new(10, 0, 100, 0))
            .unwrap();
    append_three(&mut w0); // 5 events ⇒ end_pos 5
    let e_k = w0.epoch();

    // Seal seg 10 and roll into seg 11 with a strictly larger epoch.
    let mut w1 = w0.roll_sealed(p1, 11, 101, 0).unwrap();

    // Sealed predecessor: its trailer is the durable A1/A9 anchor for k+1.
    let cat0 = read_trailer(&fs, p0).unwrap().expect("seg 10 is sealed");
    assert_eq!(cat0.epoch, e_k);
    assert_eq!(cat0.end_pos, 5);

    // Successor header continues the chain (§3.2 / §8.1).
    let hdr1 = read_all(&fs, p1, SEGMENT_HEADER_LEN);
    assert_eq!(read_segment_header_epoch(&hdr1), Some(101));
    let (base_pos1, epoch1, prev1) = decode_header_seed(&hdr1);
    assert_eq!(base_pos1, cat0.end_pos, "A1: k+1 base_pos == k end_pos");
    assert_eq!(prev1, e_k, "A9: k+1 prev_segment_epoch == k epoch");
    assert!(epoch1 > e_k, "A9: epoch strictly increases across a roll");

    // The first batch of seg 11 resumes at end_pos with batch_id 0.
    let p = [0u8; 4];
    let one = [Subframe::plain(0x11, 0, 0, &p)];
    let r = w1.append(&spec(1, 0, &one)).unwrap();
    assert_eq!(r.batch_id, 0);
    assert_eq!(r.first_global_pos, cat0.end_pos);

    // recover_fast over the sealed predecessor stays on the fast path and
    // hands the same end_pos across the boundary.
    let fast0 = recover_fast(&fs, p0).unwrap();
    assert!(matches!(fast0, FastRecovery::Sealed { .. }));
    assert_eq!(fast0.end_pos(), 5);
}

fn read_all<F: Fs>(fs: &F, path: &Path, len: usize) -> Vec<u8> {
    let f = fs.open(path, OpenOpts::read_only()).unwrap();
    let mut buf = vec![0u8; len];
    let n = f.pread(0, &mut buf).unwrap();
    buf.truncate(n);
    buf
}

/// Pull `(base_pos, epoch, prev_segment_epoch)` out of a SegmentHeader image
/// (§3.2 offsets 16 / 24 / 40).
fn decode_header_seed(img: &[u8]) -> (u64, u64, u64) {
    let g = |o: usize| u64::from_le_bytes(img[o..o + 8].try_into().unwrap());
    (g(16), g(24), g(40))
}

// ---------------------------------------------------------------------------
// Recycling discipline (sim-only: needs SimFs::seed to plant stale bytes)
// ---------------------------------------------------------------------------

/// Encode a standalone batch with a caller-chosen epoch (the writer would never
/// emit a stale epoch — this synthesizes the resurrected prior generation).
fn encode_batch(
    epoch: u64,
    batch_id: u64,
    first_global_pos: u64,
    first_v: u64,
) -> Vec<u8> {
    let p = [0xAAu8; 4];
    let sfs = [Subframe::plain(0x11, 0, 0, &p)];
    BatchEncoder::new()
        .encode(&BatchInput {
            segment_epoch: epoch,
            batch_id,
            first_global_pos,
            stream_id: 1,
            category_id: 101,
            first_stream_version: first_v,
            crypto_chain: None,
            subframes: &sfs,
        })
        .unwrap()
        .to_vec()
}

/// Build the durable image of a fresh, header-synced segment carrying a NEW
/// epoch, by writing it through the real writer, then return its bytes.
fn fresh_header_image(segment_id: u64, base_pos: u64, epoch: u64) -> Vec<u8> {
    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("mk");
    SegmentWriter::create(
        &fs,
        path,
        SegmentParams::new(segment_id, base_pos, epoch, 0),
    )
    .unwrap();
    read_all(&fs, path, SEGMENT_HEADER_LEN)
}

/// Recycling discipline, scan level: a new generation's header (epoch 9, synced
/// before reuse) sits atop stale prior-generation batch bytes (epoch 5) that a
/// freed-but-not-zeroed region left behind. The scan, seeded by the durable
/// header's epoch, rejects every stale batch by A9 (`EpochMismatch`) and
/// accepts nothing — the resurrected generation is never mistaken for live.
#[test]
fn recycled_region_stale_batches_rejected_by_epoch() {
    let mut img = fresh_header_image(1, 0, 9);
    // Behind the fresh epoch-9 header, the disk still holds an epoch-5 batch.
    img.extend_from_slice(&encode_batch(5, 0, 0, 0));

    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("recycled");
    fs.seed(path, Fault::SECTOR_512, img);
    let rec = recover_segment(&fs, path).unwrap();

    assert!(
        rec.accepted.is_empty(),
        "no stale-generation batch may be accepted"
    );
    assert_eq!(
        rec.stop,
        ScanStop::EpochMismatch,
        "A9 rejects the recycled generation"
    );
    assert_eq!(rec.safe_offset, SEGMENT_HEADER_LEN as u64);
}

/// Recycling discipline, fast-path level: a fresh unsealed header (epoch 9)
/// with a **stale, still-self-consistent** trailer (epoch 5) resurrected at EOF
/// behind it. `recover_fast` MUST NOT trust the trailer — the §3.3.1
/// header/trailer cross-check (`epoch`/`base_pos`/`segment_id`) fails — so it
/// falls back to the full scan, which rejects the stale batch by A9. A stale
/// seal is never mistaken for the live segment's.
#[test]
fn recycled_stale_trailer_not_trusted_by_fast_path() {
    // Fresh generation: epoch 9, segment_id 1, base_pos 0.
    let mut img = fresh_header_image(1, 0, 9);
    let stale_batch = encode_batch(5, 0, 0, 0);
    img.extend_from_slice(&stale_batch);

    // A perfectly valid trailer, but from the STALE generation (epoch 5,
    // segment_id 1 — same slot, older epoch). Its own footer_crc is correct.
    let content_len = img.len() as u64;
    let stale =
        mess_log::sealer::TrailerFields::phase3(1, 5, 0, 1, 1, content_len);
    img.extend_from_slice(&mess_log::sealer::encode_trailer(&stale));

    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("stale-trailer");
    fs.seed(path, Fault::SECTOR_512, img);

    // The stale trailer is self-consistent and decodes...
    let cat =
        read_trailer(&fs, path).unwrap().expect("stale trailer is self-valid");
    assert_eq!(cat.epoch, 5);

    // ...but recover_fast refuses to trust it against the epoch-9 header.
    let rec = recover_fast(&fs, path).unwrap();
    let FastRecovery::Scanned(r) = &rec else {
        panic!(
            "a stale trailer must not be trusted; expected a full-scan \
             fallback, got {rec:?}"
        );
    };
    assert!(
        r.accepted.is_empty(),
        "the stale batch is rejected by A9 on the scan"
    );
    assert_eq!(r.stop, ScanStop::EpochMismatch);
}

/// A trailer whose fields are internally coherent AND match the header is
/// trusted; this pins the positive side of the cross-check so the negative
/// tests above are meaningful (not vacuously falling back).
#[test]
fn coherent_trailer_is_trusted() {
    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("good");
    let mut w =
        SegmentWriter::create(&fs, path, SegmentParams::new(1, 0, 9, 0))
            .unwrap();
    append_three(&mut w);
    w.seal().unwrap();
    assert!(matches!(
        recover_fast(&fs, path).unwrap(),
        FastRecovery::Sealed { .. }
    ));
}

/// An unsealed (trailer-less) segment takes the full-scan path, never the fast
/// path — the active segment is always scanned (§8.3).
#[test]
fn unsealed_segment_is_scanned() {
    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("active");
    let mut w =
        SegmentWriter::create(&fs, path, SegmentParams::new(1, 0, 9, 0))
            .unwrap();
    append_three(&mut w);
    w.close().unwrap(); // close leaves it unsealed (no trailer)

    assert!(read_trailer(&fs, path).unwrap().is_none());
    let rec = recover_fast(&fs, path).unwrap();
    let FastRecovery::Scanned(r) = &rec else {
        panic!("an unsealed segment must be scanned, got {rec:?}");
    };
    assert_eq!(r.batch_count(), 3);
}

/// `decode_trailer` locates the trailer at the tail of a whole-file image, so a
/// full read (header + batches + trailer) validates the seal.
#[test]
fn whole_file_image_trailer_decodes() {
    let fs = SimFs::new(Fault::SECTOR_512);
    let path = Path::new("whole");
    let mut w =
        SegmentWriter::create(&fs, path, SegmentParams::new(2, 500, 33, 0))
            .unwrap();
    append_three(&mut w);
    let summary = w.seal().unwrap();

    let img = read_all(
        &fs,
        path,
        (summary.content_len + SEGMENT_TRAILER_LEN as u64) as usize,
    );
    let cat = decode_trailer(&img).expect("trailer at file tail decodes");
    assert_eq!(cat.segment_id, 2);
    assert_eq!(cat.base_pos, 500);
    assert_eq!(cat.epoch, 33);
    // Magic corruption at the trailer head ⇒ undecodable (wrong magic path).
    let mut bad = img.clone();
    let m = bad.len() - SEGMENT_TRAILER_LEN + T_MAGIC_OFF as usize;
    bad[m] ^= 0x01;
    assert!(decode_trailer(&bad).is_none());
}

// ---------------------------------------------------------------------------
// Real-runtime entry points (excluded from Miri: real `open` needs the OS)
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
    p.push(format!("mess-log-sealer-{}-{}-{}", std::process::id(), n, name));
    p
}

#[test]
#[cfg_attr(miri, ignore)]
fn real_seal_roundtrips() {
    let p = tmp("seal");
    let _c = Cleanup(vec![p.clone()]);
    seal_roundtrips(&RealRuntime::new(), &p);
}

#[test]
#[cfg_attr(miri, ignore)]
fn real_fast_path_matches_full_scan() {
    let p = tmp("fast");
    let _c = Cleanup(vec![p.clone()]);
    fast_path_matches_full_scan(&RealRuntime::new(), &p);
}

#[test]
#[cfg_attr(miri, ignore)]
fn real_corrupt_footer_falls_back_to_scan() {
    let p = tmp("corrupt");
    let _c = Cleanup(vec![p.clone()]);
    corrupt_footer_falls_back_to_scan(&RealRuntime::new(), &p);
}

#[test]
#[cfg_attr(miri, ignore)]
fn real_a9_chain_across_sealed_roll() {
    let p0 = tmp("roll0");
    let p1 = tmp("roll1");
    let _c = Cleanup(vec![p0.clone(), p1.clone()]);
    a9_chain_across_sealed_roll(&RealRuntime::new(), &p0, &p1);
}

// ---------------------------------------------------------------------------
// Sim-runtime entry points (in-memory fault fs; stay in the Miri lane)
// ---------------------------------------------------------------------------

#[test]
fn sim_seal_roundtrips() {
    seal_roundtrips(&SimRuntime::new(1), Path::new("/seal"));
}

#[test]
fn sim_fast_path_matches_full_scan() {
    fast_path_matches_full_scan(&SimRuntime::new(1), Path::new("/fast"));
}

#[test]
fn sim_corrupt_footer_falls_back_to_scan() {
    corrupt_footer_falls_back_to_scan(
        &SimRuntime::new(1),
        Path::new("/corrupt"),
    );
}

#[test]
fn sim_a9_chain_across_sealed_roll() {
    a9_chain_across_sealed_roll(
        &SimRuntime::new(1),
        Path::new("/roll0"),
        Path::new("/roll1"),
    );
}
