//! recovery_scale spike — recovery-time-at-scale numbers for segment sizing.
//!
//! Batch-framed segment format reused from spikes/vertical_slice/src/seglog.rs
//! (format v2: 54-byte BatchHeader with hoisted stream_id/first_stream_version,
//! 8-byte subframe headers, 16-byte CommitMarker; A1 contiguity check kept),
//! with two additions needed by this spike:
//!
//!   * a parameterized batch checksum (CRC32 IEEE via crc32fast, CRC32C via
//!     crc32c, BLAKE3 truncated to the u32 field) — same coverage rule as
//!     seglog (whole batch with both checksum fields zeroed, A3/A4);
//!   * a 40-byte SegmentFooter appended when a segment seals, so the A7/F5
//!     "trust sealed segments, scan only the active one" fast path has
//!     something to trust. The footer is advisory: FULL recovery still scans
//!     and CRC-validates every batch and cross-checks the footer counts.
//!
//! Measurements (see REPORT.md):
//!   1. FULL recovery (scan + checksum every batch, rebuild index skeleton)
//!      cold- and warm-cache, for totals {1,4,10} GiB x segments {64,256,1024} MiB.
//!   2. LAST-SEGMENT-ONLY recovery (footers for sealed segments + full scan of
//!      one worst-case full active segment).
//!   3. SIGKILL realism: child process appends with split writes, parent
//!      SIGKILLs it mid-stream, recovery must truncate the torn tail and be
//!      idempotent.
//!   4. CRC32(IEEE) vs CRC32C vs BLAKE3 scan throughput.
//!
//! Cold-cache method: every file is fdatasync'd at generation time (pages
//! clean), then posix_fadvise(POSIX_FADV_DONTNEED) over the whole file right
//! before the cold run. This evicts the file's clean page-cache pages without
//! root. Warm runs re-read the same files immediately after.

use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Format constants (identical to seglog.rs)
// ---------------------------------------------------------------------------

const HEADER_MAGIC: u32 = 0xBA7C_4EAD;
const MARKER_MAGIC: u32 = 0xC0AA_17ED;
const FOOTER_MAGIC: u32 = 0x5EA1_F007; // new in this spike (sealed-segment footer)
const FORMAT_VERSION: u16 = 2;

const HEADER_LEN: usize = 4 + 2 + 4 + 8 + 8 + 8 + 8 + 8 + 4; // 54
const SUBFRAME_HDR_LEN: usize = 4 + 4; // 8
const MARKER_LEN: usize = 4 + 8 + 4; // 16
const MIN_BATCH_LEN: usize = HEADER_LEN + MARKER_LEN;
const MAX_BATCH_LEN: u64 = 16 << 20; // A2 sanity cap
const HEADER_CRC_OFF: usize = HEADER_LEN - 4;

/// SegmentFooter (sealed segments only):
///   magic u32 | batch_count u64 | event_count u64 | base_pos u64 |
///   next_pos u64 | crc32(ieee, first 36 bytes) u32
const FOOTER_LEN: usize = 4 + 8 + 8 + 8 + 8 + 4; // 40

const PAYLOAD_LEN: usize = 250;
const EVENTS_PER_BATCH: usize = 10;
const N_STREAMS: u64 = 10_000;

const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;

fn rd_u16(d: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(d[o..o + 2].try_into().unwrap())
}
fn rd_u32(d: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(d[o..o + 4].try_into().unwrap())
}
fn rd_u64(d: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(d[o..o + 8].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// Checksums
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Ck {
    Crc32Ieee, // crc32fast — what seglog.rs currently uses
    Crc32c,    // crc32c crate (SSE4.2 hardware)
    Blake3,    // truncated to the 4-byte field; measures compute cost only
}

impl Ck {
    fn name(self) -> &'static str {
        match self {
            Ck::Crc32Ieee => "crc32-ieee (crc32fast)",
            Ck::Crc32c => "crc32c (hw)",
            Ck::Blake3 => "blake3 (truncated)",
        }
    }
}

const ZERO4: [u8; 4] = [0u8; 4];

/// Checksum of a fully-encoded batch whose two checksum fields may be filled:
/// hashes the batch with both 4-byte fields substituted by zeros (A3/A4
/// coverage rule), without copying the batch.
fn batch_sum(ck: Ck, batch: &[u8]) -> u32 {
    let echo_off = batch.len() - 4;
    match ck {
        Ck::Crc32Ieee => {
            let mut h = crc32fast::Hasher::new();
            h.update(&batch[..HEADER_CRC_OFF]);
            h.update(&ZERO4);
            h.update(&batch[HEADER_CRC_OFF + 4..echo_off]);
            h.update(&ZERO4);
            h.finalize()
        }
        Ck::Crc32c => {
            let mut c = crc32c::crc32c(&batch[..HEADER_CRC_OFF]);
            c = crc32c::crc32c_append(c, &ZERO4);
            c = crc32c::crc32c_append(c, &batch[HEADER_CRC_OFF + 4..echo_off]);
            crc32c::crc32c_append(c, &ZERO4)
        }
        Ck::Blake3 => {
            let mut h = blake3::Hasher::new();
            h.update(&batch[..HEADER_CRC_OFF]);
            h.update(&ZERO4);
            h.update(&batch[HEADER_CRC_OFF + 4..echo_off]);
            h.update(&ZERO4);
            let d = h.finalize();
            u32::from_le_bytes(d.as_bytes()[..4].try_into().unwrap())
        }
    }
}

// ---------------------------------------------------------------------------
// Encoding (seglog::encode_batch, checksum parameterized)
// ---------------------------------------------------------------------------

fn encode_batch(
    ck: Ck,
    batch_id: u64,
    first_global_pos: u64,
    stream_id: u64,
    first_stream_version: u64,
    events: &[&[u8]],
) -> Vec<u8> {
    assert!(!events.is_empty(), "empty batches forbidden (A5)");
    let frames_len: usize = events.iter().map(|e| SUBFRAME_HDR_LEN + e.len()).sum();
    let total_len = HEADER_LEN + frames_len + MARKER_LEN;
    assert!((total_len as u64) <= MAX_BATCH_LEN);

    let mut buf = Vec::with_capacity(total_len);
    buf.extend(HEADER_MAGIC.to_le_bytes());
    buf.extend(FORMAT_VERSION.to_le_bytes());
    buf.extend((events.len() as u32).to_le_bytes());
    buf.extend(batch_id.to_le_bytes());
    buf.extend((total_len as u64).to_le_bytes());
    buf.extend(first_global_pos.to_le_bytes());
    buf.extend(stream_id.to_le_bytes());
    buf.extend(first_stream_version.to_le_bytes());
    buf.extend(0u32.to_le_bytes()); // batch_crc placeholder

    for e in events {
        buf.extend(1u32.to_le_bytes()); // event_type_id (interning is D3, not this spike)
        buf.extend((e.len() as u32).to_le_bytes());
        buf.extend_from_slice(e);
    }

    buf.extend(MARKER_MAGIC.to_le_bytes());
    buf.extend((total_len as u64).to_le_bytes());
    buf.extend(0u32.to_le_bytes()); // crc echo placeholder
    debug_assert_eq!(buf.len(), total_len);

    let crc = batch_sum(ck, &buf);
    buf[HEADER_CRC_OFF..HEADER_CRC_OFF + 4].copy_from_slice(&crc.to_le_bytes());
    let echo_off = total_len - 4;
    buf[echo_off..].copy_from_slice(&crc.to_le_bytes());
    buf
}

// ---------------------------------------------------------------------------
// Segment files / footer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SegMeta {
    id: u64,
    base_pos: u64,
    path: PathBuf,
    len: u64,
}

fn segment_file_name(id: u64, base_pos: u64) -> String {
    format!("seg-{:06}-{:015}.log", id, base_pos)
}

fn list_segments(dir: &Path) -> Vec<SegMeta> {
    let mut out = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(rest) = name.strip_prefix("seg-").and_then(|r| r.strip_suffix(".log")) {
                let mut it = rest.splitn(2, '-');
                if let (Some(id), Some(base)) = (it.next(), it.next()) {
                    if let (Ok(id), Ok(base)) = (id.parse::<u64>(), base.parse::<u64>()) {
                        let len = e.metadata().map(|m| m.len()).unwrap_or(0);
                        out.push(SegMeta { id, base_pos: base, path: e.path(), len });
                    }
                }
            }
        }
    }
    out.sort_by_key(|s| s.id);
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Footer {
    batch_count: u64,
    event_count: u64,
    base_pos: u64,
    next_pos: u64,
}

fn encode_footer(f: &Footer) -> [u8; FOOTER_LEN] {
    let mut b = [0u8; FOOTER_LEN];
    b[..4].copy_from_slice(&FOOTER_MAGIC.to_le_bytes());
    b[4..12].copy_from_slice(&f.batch_count.to_le_bytes());
    b[12..20].copy_from_slice(&f.event_count.to_le_bytes());
    b[20..28].copy_from_slice(&f.base_pos.to_le_bytes());
    b[28..36].copy_from_slice(&f.next_pos.to_le_bytes());
    let crc = {
        let mut h = crc32fast::Hasher::new();
        h.update(&b[..36]);
        h.finalize()
    };
    b[36..].copy_from_slice(&crc.to_le_bytes());
    b
}

fn decode_footer(b: &[u8]) -> Option<Footer> {
    if b.len() != FOOTER_LEN || rd_u32(b, 0) != FOOTER_MAGIC {
        return None;
    }
    let crc = {
        let mut h = crc32fast::Hasher::new();
        h.update(&b[..36]);
        h.finalize()
    };
    if crc != rd_u32(b, 36) {
        return None;
    }
    Some(Footer {
        batch_count: rd_u64(b, 4),
        event_count: rd_u64(b, 12),
        base_pos: rd_u64(b, 20),
        next_pos: rd_u64(b, 28),
    })
}

/// Fast-path read of a sealed segment's footer: one pread of the last 40 bytes.
fn read_footer(path: &Path, file_len: u64) -> Option<Footer> {
    if file_len < FOOTER_LEN as u64 {
        return None;
    }
    let mut f = File::open(path).ok()?;
    f.seek(SeekFrom::Start(file_len - FOOTER_LEN as u64)).ok()?;
    let mut b = [0u8; FOOTER_LEN];
    f.read_exact(&mut b).ok()?;
    decode_footer(&b)
}

// ---------------------------------------------------------------------------
// Recovery scanner (seglog::scan, adapted: no per-batch copy for the CRC,
// footer-terminated sealed segments, inline index-skeleton rebuild)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    EndOfLog,
    SealedFooter,
    TornHeader,
    BadHeaderMagic,
    BadVersion,
    BadLength,
    IncompleteBatch,
    BadMarker,
    BadCrc,
    BadFrames,
    PositionDiscontinuity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EventPtr {
    segment_id: u64,
    offset: u64,
    len: u32,
}

/// Index skeleton rebuilt during recovery: per-stream head (version + last
/// event ptr) plus global counters. The real system writes these to fjall;
/// this spike measures the scan side (see vertical_slice F6 for the fjall
/// re-verify cost).
#[derive(Default)]
struct IndexSkeleton {
    heads: HashMap<u64, (u64, EventPtr)>,
    batches: u64,
    events: u64,
}

struct SegScan {
    batches: u64,
    events: u64,
    next_pos: u64,
    safe_offset: u64,
    stop: Stop,
    footer: Option<Footer>,
}

fn scan_segment(data: &[u8], seg_id: u64, mut expect_pos: u64, ck: Ck, idx: &mut IndexSkeleton) -> SegScan {
    let mut off = 0usize;
    let mut batches = 0u64;
    let mut events = 0u64;
    let mut footer = None;

    let stop = loop {
        let rem = data.len() - off;
        if rem == 0 {
            break Stop::EndOfLog;
        }
        if rem >= 4 && rd_u32(data, off) == FOOTER_MAGIC {
            footer = decode_footer(&data[off..(off + FOOTER_LEN).min(data.len())]);
            break Stop::SealedFooter;
        }
        if rem < HEADER_LEN {
            break Stop::TornHeader;
        }
        if rd_u32(data, off) != HEADER_MAGIC {
            break Stop::BadHeaderMagic;
        }
        if rd_u16(data, off + 4) != FORMAT_VERSION {
            break Stop::BadVersion;
        }
        let frame_count = rd_u32(data, off + 6);
        let total_len = rd_u64(data, off + 18);
        let first_global_pos = rd_u64(data, off + 26);
        let stream_id = rd_u64(data, off + 34);
        let first_stream_version = rd_u64(data, off + 42);
        let header_crc = rd_u32(data, off + HEADER_CRC_OFF);

        if total_len < MIN_BATCH_LEN as u64 || total_len > MAX_BATCH_LEN {
            break Stop::BadLength;
        }
        let total_len = total_len as usize;
        if total_len > rem {
            break Stop::IncompleteBatch;
        }
        let batch = &data[off..off + total_len];

        // CommitMarker validation (A3).
        let m = total_len - MARKER_LEN;
        if rd_u32(batch, m) != MARKER_MAGIC
            || rd_u64(batch, m + 4) != total_len as u64
            || rd_u32(batch, m + 12) != header_crc
        {
            break Stop::BadMarker;
        }

        // Whole-batch checksum, fields zeroed (A4). No copy (unlike seglog).
        if batch_sum(ck, batch) != header_crc {
            break Stop::BadCrc;
        }

        // Subframes must exactly tile header..marker.
        let mut p = HEADER_LEN;
        let frames_end = total_len - MARKER_LEN;
        let mut ok = frame_count > 0; // A5
        let mut last_payload = (0usize, 0u32);
        for _ in 0..frame_count {
            if p + SUBFRAME_HDR_LEN > frames_end {
                ok = false;
                break;
            }
            let dlen = rd_u32(batch, p + 4) as usize;
            p += SUBFRAME_HDR_LEN;
            if p + dlen > frames_end {
                ok = false;
                break;
            }
            last_payload = (off + p, dlen as u32);
            p += dlen;
        }
        if !ok || p != frames_end {
            break Stop::BadFrames;
        }

        // A1 contiguity guard.
        if first_global_pos != expect_pos {
            break Stop::PositionDiscontinuity;
        }

        idx.heads.insert(
            stream_id,
            (
                first_stream_version + frame_count as u64 - 1,
                EventPtr { segment_id: seg_id, offset: last_payload.0 as u64, len: last_payload.1 },
            ),
        );
        expect_pos += frame_count as u64;
        batches += 1;
        events += frame_count as u64;
        off += total_len;
    };

    SegScan { batches, events, next_pos: expect_pos, safe_offset: off as u64, stop, footer }
}

// ---------------------------------------------------------------------------
// Full + fast recovery over a segment set
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct RecoveryOut {
    batches: u64,
    events: u64,
    next_pos: u64,
    bytes: u64,
    streams: usize,
    last_stop: Stop,
    last_safe_offset: u64,
}

/// FULL recovery: read + scan + checksum every batch of every segment,
/// rebuilding the index skeleton. Sealed footers are cross-checked but not
/// trusted (D1: the log is the only authority).
fn recover_full(segs: &[SegMeta], ck: Ck) -> RecoveryOut {
    let mut idx = IndexSkeleton::default();
    let mut expect = segs.first().map(|s| s.base_pos).unwrap_or(0);
    let mut bytes = 0u64;
    let mut last_stop = Stop::EndOfLog;
    let mut last_safe = 0u64;
    for (i, s) in segs.iter().enumerate() {
        assert_eq!(s.base_pos, expect, "segment {} base_pos mismatch", s.id);
        let data = fs::read(&s.path).unwrap();
        bytes += data.len() as u64;
        let r = scan_segment(&data, s.id, expect, ck, &mut idx);
        if i + 1 < segs.len() {
            // Interior segment must be sealed and footer must agree with the scan.
            assert_eq!(r.stop, Stop::SealedFooter, "interior segment {} not sealed", s.id);
            let f = r.footer.expect("sealed segment footer failed to decode");
            assert_eq!((f.batch_count, f.event_count, f.next_pos), (r.batches, r.events, r.next_pos),
                "footer/scan disagreement on segment {}", s.id);
        }
        idx.batches += r.batches;
        idx.events += r.events;
        expect = r.next_pos;
        last_stop = r.stop;
        last_safe = r.safe_offset;
    }
    RecoveryOut {
        batches: idx.batches,
        events: idx.events,
        next_pos: expect,
        bytes,
        streams: idx.heads.len(),
        last_stop,
        last_safe_offset: last_safe,
    }
}

/// LAST-SEGMENT-ONLY recovery (A7/F5 fast path): sealed segments are trusted
/// via their footers (one 40-byte pread each); only the final segment is
/// scanned and checksummed.
fn recover_fast(segs: &[SegMeta], ck: Ck) -> RecoveryOut {
    assert!(!segs.is_empty());
    let mut batches = 0u64;
    let mut events = 0u64;
    let mut expect = segs[0].base_pos;
    let (active, sealed) = segs.split_last().unwrap();
    for s in sealed {
        let f = read_footer(&s.path, s.len).expect("sealed segment must have a valid footer");
        assert_eq!(f.base_pos, expect, "footer base_pos discontinuity at segment {}", s.id);
        batches += f.batch_count;
        events += f.event_count;
        expect = f.next_pos;
    }
    assert_eq!(active.base_pos, expect);
    let mut idx = IndexSkeleton::default();
    let data = fs::read(&active.path).unwrap();
    let r = scan_segment(&data, active.id, expect, ck, &mut idx);
    RecoveryOut {
        batches: batches + r.batches,
        events: events + r.events,
        next_pos: r.next_pos,
        bytes: data.len() as u64,
        streams: idx.heads.len(),
        last_stop: r.stop,
        last_safe_offset: r.safe_offset,
    }
}

// ---------------------------------------------------------------------------
// Generation
// ---------------------------------------------------------------------------

struct GenOut {
    segs: Vec<SegMeta>,
    /// (batches, events) written per segment — generation-side ground truth
    /// for the recovery-count sanity checks.
    seg_counts: Vec<(u64, u64)>,
    batches: u64,
    events: u64,
    next_pos: u64,
    payload_bytes: u64,
}

/// Build one batch's events: 10 x 250-byte payloads, first 16 bytes stamped
/// with the event counter so batches are not byte-identical.
fn make_events(counter: &mut u64, buf: &mut [[u8; PAYLOAD_LEN]; EVENTS_PER_BATCH]) {
    for e in buf.iter_mut() {
        e[..8].copy_from_slice(&counter.to_le_bytes());
        e[8..16].copy_from_slice(&(!*counter).to_le_bytes());
        *counter += 1;
    }
}

fn gen_dataset(dir: &Path, ck: Ck, seg_size: u64, total_target: u64) -> GenOut {
    fs::create_dir_all(dir).unwrap();
    let mut seg_id = 0u64;
    let mut base = 0u64;
    let mut pos = 0u64;
    let mut batch_id = 0u64;
    let mut path = dir.join(segment_file_name(0, 0));
    let mut w = BufWriter::with_capacity(8 << 20, File::create(&path).unwrap());
    let mut seg_len = 0u64;
    let mut seg_batches = 0u64;
    let mut seg_events = 0u64;
    let mut segs = Vec::new();
    let mut seg_counts = Vec::new();
    let mut total = 0u64;
    let mut batches = 0u64;
    let mut events = 0u64;
    let mut counter = 0u64;
    let mut payloads = [[0u8; PAYLOAD_LEN]; EVENTS_PER_BATCH];
    // deterministic pseudo-random background fill
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    for e in payloads.iter_mut() {
        for c in e.chunks_mut(8) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let b = x.to_le_bytes();
            c.copy_from_slice(&b[..c.len()]);
        }
    }

    let mut batch_index = 0u64;
    while total < total_target {
        make_events(&mut counter, &mut payloads);
        let stream_id = batch_index % N_STREAMS;
        let first_version = (batch_index / N_STREAMS) * EVENTS_PER_BATCH as u64;
        let ev_refs: Vec<&[u8]> = payloads.iter().map(|e| &e[..]).collect();
        let bytes = encode_batch(ck, batch_id, pos, stream_id, first_version, &ev_refs);
        let blen = bytes.len() as u64;

        if seg_len + blen > seg_size && seg_len > 0 {
            // Seal: footer + fdatasync (A7: roll syncs the old segment).
            let mut f = w.into_inner().unwrap();
            let footer = Footer { batch_count: seg_batches, event_count: seg_events, base_pos: base, next_pos: pos };
            f.write_all(&encode_footer(&footer)).unwrap();
            f.sync_data().unwrap();
            segs.push(SegMeta { id: seg_id, base_pos: base, path: path.clone(), len: seg_len + FOOTER_LEN as u64 });
            seg_counts.push((seg_batches, seg_events));
            seg_id += 1;
            base = pos;
            batch_id = 0;
            path = dir.join(segment_file_name(seg_id, base));
            w = BufWriter::with_capacity(8 << 20, File::create(&path).unwrap());
            seg_len = 0;
            seg_batches = 0;
            seg_events = 0;
        }

        w.write_all(&bytes).unwrap();
        seg_len += blen;
        total += blen;
        seg_batches += 1;
        seg_events += EVENTS_PER_BATCH as u64;
        batches += 1;
        events += EVENTS_PER_BATCH as u64;
        pos += EVENTS_PER_BATCH as u64;
        batch_id += 1;
        batch_index += 1;
    }
    // Final segment stays active: flushed + synced, NO footer.
    let f = w.into_inner().unwrap();
    f.sync_data().unwrap();
    segs.push(SegMeta { id: seg_id, base_pos: base, path, len: seg_len });
    seg_counts.push((seg_batches, seg_events));
    GenOut { segs, seg_counts, batches, events, next_pos: pos, payload_bytes: total }
}

// ---------------------------------------------------------------------------
// Cache eviction (cold approximation)
// ---------------------------------------------------------------------------

/// Best-effort eviction of a file's page-cache pages. All generated files are
/// fdatasync'd, so their pages are clean and DONTNEED drops them. Not as
/// strong as `echo 3 > drop_caches` (needs root) but the cold/warm deltas in
/// the results confirm it works.
fn evict(paths: &[&Path]) {
    for p in paths {
        if let Ok(f) = File::open(p) {
            unsafe {
                libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
            }
        }
    }
    // Give the kernel a beat to complete the eviction.
    std::thread::sleep(Duration::from_millis(100));
}

fn time_it<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let t0 = Instant::now();
    let out = f();
    (out, t0.elapsed())
}

fn mibs(bytes: u64, d: Duration) -> f64 {
    bytes as f64 / MIB as f64 / d.as_secs_f64()
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn fmt_size(b: u64) -> String {
    if b >= GIB {
        format!("{} GiB", b / GIB)
    } else {
        format!("{} MiB", b / MIB)
    }
}

// ---------------------------------------------------------------------------
// SIGKILL child writer
// ---------------------------------------------------------------------------

/// Child mode: append batches forever with split writes (header+frames, then
/// marker, as two write() calls — widens the torn-batch window, same trick as
/// crash_log/seglog). fdatasync every SYNC_EVERY batches, then persist the
/// durable counters to `status` (write tmp + rename).
fn run_child(dir: &Path, seg_size: u64) -> ! {
    const SYNC_EVERY: u64 = 64;
    let ck = Ck::Crc32c;
    fs::create_dir_all(dir).unwrap();
    let mut seg_id = 0u64;
    let mut base = 0u64;
    let mut pos = 0u64;
    let mut batch_id = 0u64;
    let mut path = dir.join(segment_file_name(0, 0));
    let mut file = OpenOptions::new().create(true).append(true).open(&path).unwrap();
    let mut seg_len = 0u64;
    let mut seg_batches = 0u64;
    let mut seg_events = 0u64;
    let mut batches = 0u64;
    let mut events = 0u64;
    let mut counter = 0u64;
    let mut payloads = [[7u8; PAYLOAD_LEN]; EVENTS_PER_BATCH];
    let mut batch_index = 0u64;
    loop {
        make_events(&mut counter, &mut payloads);
        let stream_id = batch_index % N_STREAMS;
        let first_version = (batch_index / N_STREAMS) * EVENTS_PER_BATCH as u64;
        let ev_refs: Vec<&[u8]> = payloads.iter().map(|e| &e[..]).collect();
        let bytes = encode_batch(ck, batch_id, pos, stream_id, first_version, &ev_refs);
        let blen = bytes.len() as u64;

        if seg_len + blen > seg_size && seg_len > 0 {
            let footer = Footer { batch_count: seg_batches, event_count: seg_events, base_pos: base, next_pos: pos };
            file.write_all(&encode_footer(&footer)).unwrap();
            file.sync_data().unwrap();
            seg_id += 1;
            base = pos;
            batch_id = 0;
            path = dir.join(segment_file_name(seg_id, base));
            file = OpenOptions::new().create(true).append(true).open(&path).unwrap();
            seg_len = 0;
            seg_batches = 0;
            seg_events = 0;
        }

        // Split write: a SIGKILL between the two write()s leaves a marker-less
        // torn tail on disk (recovery must discard it). The window between
        // two back-to-back write()s is only ~microseconds, so every 24
        // batches it is widened with a 3 ms sleep — otherwise a random kill
        // essentially never lands inside it (SIGKILL cannot tear a single
        // in-flight write(): the syscall completes in the kernel).
        let cut = bytes.len() - MARKER_LEN;
        file.write_all(&bytes[..cut]).unwrap();
        if batch_index % 24 == 0 {
            std::thread::sleep(Duration::from_millis(3));
        }
        file.write_all(&bytes[cut..]).unwrap();

        seg_len += blen;
        seg_batches += 1;
        seg_events += EVENTS_PER_BATCH as u64;
        batches += 1;
        events += EVENTS_PER_BATCH as u64;
        pos += EVENTS_PER_BATCH as u64;
        batch_id += 1;
        batch_index += 1;

        if batch_index % SYNC_EVERY == 0 {
            file.sync_data().unwrap();
            let tmp = dir.join("status.tmp");
            let mut sf = File::create(&tmp).unwrap();
            writeln!(sf, "{} {} {}", batches, events, pos).unwrap();
            sf.sync_data().unwrap();
            fs::rename(&tmp, dir.join("status")).unwrap();
        }
    }
}

fn read_status(dir: &Path) -> (u64, u64, u64) {
    match fs::read_to_string(dir.join("status")) {
        Ok(s) => {
            let v: Vec<u64> = s.split_whitespace().filter_map(|t| t.parse().ok()).collect();
            if v.len() == 3 {
                (v[0], v[1], v[2])
            } else {
                (0, 0, 0)
            }
        }
        Err(_) => (0, 0, 0),
    }
}

// ---------------------------------------------------------------------------
// Benchmark driver
// ---------------------------------------------------------------------------

struct Report(Vec<String>);

impl Report {
    fn line(&mut self, s: String) {
        eprintln!("{}", s);
        self.0.push(s);
    }
}

fn prefix_for_total(segs: &[SegMeta], total: u64) -> &[SegMeta] {
    let mut acc = 0u64;
    for (i, s) in segs.iter().enumerate() {
        acc += s.len;
        if acc >= total {
            return &segs[..=i];
        }
    }
    segs
}

fn seg_paths(segs: &[SegMeta]) -> Vec<&Path> {
    segs.iter().map(|s| s.path.as_path()).collect()
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() >= 2 && args[1] == "child" {
        let dir = PathBuf::from(&args[2]);
        let seg_size: u64 = args[3].parse().unwrap();
        run_child(&dir, seg_size);
    }

    let quick = env::var("RS_QUICK").is_ok();
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bench_data");
    fs::create_dir_all(&base).unwrap();

    let (seg_sizes, totals): (Vec<u64>, Vec<u64>) = if quick {
        (vec![16 * MIB, 32 * MIB], vec![64 * MIB, 128 * MIB])
    } else {
        (vec![64 * MIB, 256 * MIB, 1024 * MIB], vec![GIB, 4 * GIB, 10 * GIB])
    };
    let main_ck = Ck::Crc32c;

    let mut rep = Report(Vec::new());
    rep.line(format!(
        "# recovery_scale results ({}, {} threads not used — single-threaded scan)",
        if quick { "QUICK mode" } else { "full run" },
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0)
    ));
    rep.line(format!("main-matrix checksum: {}", main_ck.name()));

    // ------------------------------------------------------------------
    // 1+2. Full + last-segment-only recovery matrix
    // ------------------------------------------------------------------
    rep.line("\n## matrix: total x segment size".into());
    rep.line("| total | seg | mode | cold ms | cold MiB/s | warm ms | warm MiB/s | batches | events | streams |".into());
    rep.line("|---|---|---|---|---|---|---|---|---|---|".into());

    for &seg_size in &seg_sizes {
        let dir = base.join(format!("gen_seg{}m", seg_size / MIB));
        eprintln!("== generating {} at segment size {} MiB ...", fmt_size(*totals.last().unwrap()), seg_size / MIB);
        let (gen, gd) = time_it(|| gen_dataset(&dir, main_ck, seg_size, *totals.last().unwrap()));
        eprintln!(
            "   generated {} segments, {} batches, {} events, {:.1} MiB in {:.1}s ({:.0} MiB/s)",
            gen.segs.len(), gen.batches, gen.events,
            gen.payload_bytes as f64 / MIB as f64, gd.as_secs_f64(), mibs(gen.payload_bytes, gd)
        );

        // Sanity: full recovery finds exactly what was written; re-recovery
        // is idempotent.
        let r1 = recover_full(&gen.segs, main_ck);
        assert_eq!(r1.batches, gen.batches, "recovered batches != written");
        assert_eq!(r1.events, gen.events, "recovered events != written");
        assert_eq!(r1.next_pos, gen.next_pos);
        assert_eq!(r1.last_stop, Stop::EndOfLog);
        let r2 = recover_full(&gen.segs, main_ck);
        assert_eq!((r2.batches, r2.events, r2.next_pos, r2.streams), (r1.batches, r1.events, r1.next_pos, r1.streams), "re-recovery not idempotent");
        eprintln!("   sanity ok: {} batches / {} events / {} streams, idempotent", r1.batches, r1.events, r1.streams);

        for &total in &totals {
            let segs = prefix_for_total(&gen.segs, total);
            let n = segs.len();
            let exp: (u64, u64) = gen.seg_counts[..n]
                .iter()
                .fold((0, 0), |a, c| (a.0 + c.0, a.1 + c.1));

            // FULL recovery, cold then warm x2 (best warm reported).
            evict(&seg_paths(segs));
            let (rc, dc) = time_it(|| recover_full(segs, main_ck));
            let (rw1, dw1) = time_it(|| recover_full(segs, main_ck));
            let (_rw2, dw2) = time_it(|| recover_full(segs, main_ck));
            assert_eq!((rc.batches, rc.events), exp, "full recovery count mismatch");
            assert_eq!((rw1.batches, rw1.events), exp);
            let dw = if dw1 < dw2 { dw1 } else { dw2 };
            rep.line(format!(
                "| {} | {} MiB | FULL | {:.0} | {:.0} | {:.0} | {:.0} | {} | {} | {} |",
                fmt_size(total), seg_size / MIB, ms(dc), mibs(rc.bytes, dc), ms(dw), mibs(rc.bytes, dw),
                rc.batches, rc.events, rc.streams
            ));

            // LAST-SEGMENT-ONLY. The scanned segment must be a FULL segment
            // (worst-case active segment); the dataset's real active tail is
            // a small residual, so drop it when the prefix is the whole set.
            let fsegs = if n == gen.segs.len() && n > 1 { &segs[..n - 1] } else { segs };
            let fexp: (u64, u64) = gen.seg_counts[..fsegs.len()]
                .iter()
                .fold((0, 0), |a, c| (a.0 + c.0, a.1 + c.1));
            evict(&seg_paths(fsegs));
            let (fc, fdc) = time_it(|| recover_fast(fsegs, main_ck));
            let (fw1, fdw1) = time_it(|| recover_fast(fsegs, main_ck));
            let (_fw2, fdw2) = time_it(|| recover_fast(fsegs, main_ck));
            assert_eq!((fc.batches, fc.events), fexp, "fast path count mismatch");
            assert_eq!((fw1.batches, fw1.events), fexp);
            let fdw = if fdw1 < fdw2 { fdw1 } else { fdw2 };
            rep.line(format!(
                "| {} | {} MiB | LAST-SEG | {:.0} | {:.0} | {:.0} | {:.0} | {} | {} | {} |",
                fmt_size(total), seg_size / MIB, ms(fdc), mibs(fc.bytes, fdc), ms(fdw), mibs(fc.bytes, fdw),
                fc.batches, fc.events, fc.streams
            ));
        }

        for s in &gen.segs {
            fs::remove_file(&s.path).unwrap();
        }
        fs::remove_dir(&dir).ok();
        eprintln!("   deleted {} data files", gen.segs.len());
    }

    // ------------------------------------------------------------------
    // 4. Checksum comparison: scan throughput per algorithm
    // ------------------------------------------------------------------
    let ck_total = if quick { 64 * MIB } else { GIB };
    let ck_seg = if quick { 32 * MIB } else { 256 * MIB };
    rep.line(format!("\n## checksum comparison (FULL recovery of {} MiB at {} MiB segments)", ck_total / MIB, ck_seg / MIB));
    rep.line("| checksum | cold ms | cold MiB/s | warm ms | warm MiB/s |".into());
    rep.line("|---|---|---|---|---|".into());
    for ck in [Ck::Crc32Ieee, Ck::Crc32c, Ck::Blake3] {
        let dir = base.join("gen_ck");
        let gen = gen_dataset(&dir, ck, ck_seg, ck_total);
        evict(&seg_paths(&gen.segs));
        let (rc, dc) = time_it(|| recover_full(&gen.segs, ck));
        assert_eq!(rc.batches, gen.batches);
        let mut best = Duration::MAX;
        for _ in 0..3 {
            let (rw, dw) = time_it(|| recover_full(&gen.segs, ck));
            assert_eq!(rw.batches, gen.batches);
            if dw < best {
                best = dw;
            }
        }
        rep.line(format!(
            "| {} | {:.0} | {:.0} | {:.0} | {:.0} |",
            ck.name(), ms(dc), mibs(rc.bytes, dc), ms(best), mibs(rc.bytes, best)
        ));
        for s in &gen.segs {
            fs::remove_file(&s.path).unwrap();
        }
        fs::remove_dir(&dir).ok();
    }

    // ------------------------------------------------------------------
    // 3. SIGKILL realism
    // ------------------------------------------------------------------
    rep.line("\n## SIGKILL crash-writer recovery (64 MiB segments, split writes, fdatasync every 64 batches)".into());
    rep.line("| kill after | segs | written MiB | durable batches | recovered batches | tail stop | tail bytes cut | full rec ms | fast rec ms |".into());
    rep.line("|---|---|---|---|---|---|---|---|---|".into());
    let kill_seg = if quick { 8 * MIB } else { 64 * MIB };
    let kill_delays = if quick { vec![300u64, 700] } else { vec![400u64, 900, 1500, 2200, 3000] };
    let exe = env::current_exe().unwrap();
    for (i, delay) in kill_delays.iter().enumerate() {
        let dir = base.join(format!("kill_{}", i));
        fs::remove_dir_all(&dir).ok();
        let mut child = Command::new(&exe)
            .arg("child")
            .arg(&dir)
            .arg(kill_seg.to_string())
            .spawn()
            .unwrap();
        std::thread::sleep(Duration::from_millis(*delay));
        child.kill().unwrap(); // SIGKILL on unix
        child.wait().unwrap();

        let (dur_batches, dur_events, dur_pos) = read_status(&dir);
        let segs = list_segments(&dir);
        assert!(!segs.is_empty(), "child wrote nothing before kill");
        let written: u64 = segs.iter().map(|s| s.len).sum();

        evict(&seg_paths(&segs));
        let (rf, df) = time_it(|| recover_full(&segs, Ck::Crc32c));
        let (rfast, dfast) = time_it(|| recover_fast(&segs, Ck::Crc32c));
        assert_eq!((rfast.batches, rfast.events, rfast.next_pos), (rf.batches, rf.events, rf.next_pos));

        // The durably-acknowledged prefix must survive.
        assert!(rf.batches >= dur_batches, "lost durable batches: {} < {}", rf.batches, dur_batches);
        assert!(rf.events >= dur_events && rf.next_pos >= dur_pos);

        // Torn tail: truncate at safe_offset, re-recover, must be identical
        // and end cleanly (idempotent re-recovery after truncation).
        let last = segs.last().unwrap();
        let tail_cut = last.len - rf.last_safe_offset;
        if rf.last_stop != Stop::EndOfLog {
            let f = OpenOptions::new().write(true).open(&last.path).unwrap();
            f.set_len(rf.last_safe_offset).unwrap();
            f.sync_data().unwrap();
        }
        let segs2 = list_segments(&dir);
        let (r2, _) = time_it(|| recover_full(&segs2, Ck::Crc32c));
        assert_eq!((r2.batches, r2.events, r2.next_pos), (rf.batches, rf.events, rf.next_pos), "post-truncation recovery differs");
        assert_eq!(r2.last_stop, Stop::EndOfLog);

        rep.line(format!(
            "| {} ms | {} | {:.1} | {} | {} | {:?} | {} | {:.1} | {:.1} |",
            delay, segs.len(), written as f64 / MIB as f64, dur_batches, rf.batches,
            rf.last_stop, tail_cut, ms(df), ms(dfast)
        ));

        fs::remove_dir_all(&dir).unwrap();
    }

    fs::remove_dir(&base).ok(); // leaves dir if anything is left behind

    println!("\n===== BEGIN RESULTS =====");
    for l in &rep.0 {
        println!("{}", l);
    }
    println!("===== END RESULTS =====");
}
