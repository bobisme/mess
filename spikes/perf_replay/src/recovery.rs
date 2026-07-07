//! Recovery-at-scale benchmark: multi-segment generation + FULL recovery
//! scan, single-threaded (baseline, from spikes/recovery_scale) vs parallel
//! per-segment (optimized). CRC32C checksums throughout — the round-3
//! recovery baseline (~1.1 s/GiB cold) was measured with crc32c.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::Duration;

use rayon::prelude::*;

use crate::seglog::{rd_u16, rd_u32, rd_u64};

const HEADER_MAGIC: u32 = 0xBA7C_4EAD;
const MARKER_MAGIC: u32 = 0xC0AA_17ED;
const FOOTER_MAGIC: u32 = 0x5EA1_F007;
const FORMAT_VERSION: u16 = 2;

const HEADER_LEN: usize = 54;
const SUBFRAME_HDR_LEN: usize = 8;
const MARKER_LEN: usize = 16;
const MIN_BATCH_LEN: usize = HEADER_LEN + MARKER_LEN;
const MAX_BATCH_LEN: u64 = 16 << 20;
const HEADER_CRC_OFF: usize = HEADER_LEN - 4;
const FOOTER_LEN: usize = 40;

const PAYLOAD_LEN: usize = 250;
const EVENTS_PER_BATCH: usize = 10;
const N_STREAMS: u64 = 10_000;

pub const MIB: u64 = 1 << 20;
pub const GIB: u64 = 1 << 30;

const ZERO4: [u8; 4] = [0u8; 4];

/// Whole-batch checksum with both 4-byte checksum fields zeroed, no copy.
fn batch_sum(batch: &[u8]) -> u32 {
    let echo_off = batch.len() - 4;
    let mut c = crc32c::crc32c(&batch[..HEADER_CRC_OFF]);
    c = crc32c::crc32c_append(c, &ZERO4);
    c = crc32c::crc32c_append(c, &batch[HEADER_CRC_OFF + 4..echo_off]);
    crc32c::crc32c_append(c, &ZERO4)
}

fn encode_batch(
    batch_id: u64,
    first_global_pos: u64,
    stream_id: u64,
    first_stream_version: u64,
    events: &[&[u8]],
) -> Vec<u8> {
    let frames_len: usize = events.iter().map(|e| SUBFRAME_HDR_LEN + e.len()).sum();
    let total_len = HEADER_LEN + frames_len + MARKER_LEN;
    let mut buf = Vec::with_capacity(total_len);
    buf.extend(HEADER_MAGIC.to_le_bytes());
    buf.extend(FORMAT_VERSION.to_le_bytes());
    buf.extend((events.len() as u32).to_le_bytes());
    buf.extend(batch_id.to_le_bytes());
    buf.extend((total_len as u64).to_le_bytes());
    buf.extend(first_global_pos.to_le_bytes());
    buf.extend(stream_id.to_le_bytes());
    buf.extend(first_stream_version.to_le_bytes());
    buf.extend(0u32.to_le_bytes());
    for e in events {
        buf.extend(1u32.to_le_bytes());
        buf.extend((e.len() as u32).to_le_bytes());
        buf.extend_from_slice(e);
    }
    buf.extend(MARKER_MAGIC.to_le_bytes());
    buf.extend((total_len as u64).to_le_bytes());
    buf.extend(0u32.to_le_bytes());
    let crc = batch_sum(&buf);
    buf[HEADER_CRC_OFF..HEADER_CRC_OFF + 4].copy_from_slice(&crc.to_le_bytes());
    let echo_off = total_len - 4;
    buf[echo_off..].copy_from_slice(&crc.to_le_bytes());
    buf
}

// ---------------------------------------------------------------------------
// Segments + footer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SegMeta {
    pub id: u64,
    pub base_pos: u64,
    pub path: PathBuf,
    pub len: u64,
}

fn segment_file_name(id: u64, base_pos: u64) -> String {
    format!("seg-{:06}-{:015}.log", id, base_pos)
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

// ---------------------------------------------------------------------------
// Scanner (baseline logic from recovery_scale, unchanged)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
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
pub struct EventPtr {
    pub segment_id: u64,
    pub offset: u64,
    pub len: u32,
}

pub type Heads = HashMap<u64, (u64, EventPtr)>;

struct SegScan {
    batches: u64,
    events: u64,
    next_pos: u64,
    stop: Stop,
    footer: Option<Footer>,
}

fn scan_segment(data: &[u8], seg_id: u64, mut expect_pos: u64, heads: &mut Heads) -> SegScan {
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

        let m = total_len - MARKER_LEN;
        if rd_u32(batch, m) != MARKER_MAGIC
            || rd_u64(batch, m + 4) != total_len as u64
            || rd_u32(batch, m + 12) != header_crc
        {
            break Stop::BadMarker;
        }
        if batch_sum(batch) != header_crc {
            break Stop::BadCrc;
        }

        let mut p = HEADER_LEN;
        let frames_end = total_len - MARKER_LEN;
        let mut ok = frame_count > 0;
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
        if first_global_pos != expect_pos {
            break Stop::PositionDiscontinuity;
        }

        heads.insert(
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

    SegScan { batches, events, next_pos: expect_pos, stop, footer }
}

// ---------------------------------------------------------------------------
// Full recovery: sequential baseline vs parallel per-segment
// ---------------------------------------------------------------------------

pub struct RecoveryOut {
    pub batches: u64,
    pub events: u64,
    pub next_pos: u64,
    pub bytes: u64,
    pub heads: Heads,
    pub last_stop: Stop,
}

fn check_interior(i: usize, n: usize, seg: &SegMeta, r: &SegScan) {
    if i + 1 < n {
        assert_eq!(r.stop, Stop::SealedFooter, "interior segment {} not sealed", seg.id);
        let f = r.footer.expect("sealed segment footer failed to decode");
        assert_eq!(
            (f.batch_count, f.event_count, f.next_pos),
            (r.batches, r.events, r.next_pos),
            "footer/scan disagreement on segment {}",
            seg.id
        );
    }
}

/// BASELINE: read + scan + checksum every batch of every segment, in order,
/// on one thread (recovery_scale::recover_full).
pub fn recover_full(segs: &[SegMeta]) -> RecoveryOut {
    let mut heads = Heads::default();
    let mut expect = segs.first().map(|s| s.base_pos).unwrap_or(0);
    let mut bytes = 0u64;
    let mut batches = 0u64;
    let mut events = 0u64;
    let mut last_stop = Stop::EndOfLog;
    for (i, s) in segs.iter().enumerate() {
        assert_eq!(s.base_pos, expect, "segment {} base_pos mismatch", s.id);
        let data = fs::read(&s.path).unwrap();
        bytes += data.len() as u64;
        let r = scan_segment(&data, s.id, expect, &mut heads);
        check_interior(i, segs.len(), s, &r);
        batches += r.batches;
        events += r.events;
        expect = r.next_pos;
        last_stop = r.stop;
    }
    RecoveryOut { batches, events, next_pos: expect, bytes, heads, last_stop }
}

/// OPTIMIZED: scan segments in parallel (each segment's scan is independent
/// given its base_pos, which the file name / SegMeta carries), then merge in
/// segment order. Merging heads in segment order reproduces the sequential
/// "last writer wins" semantics exactly; the A1 contiguity chain is verified
/// afterwards (each segment's next_pos must equal the next one's base_pos).
pub fn recover_full_par(segs: &[SegMeta]) -> RecoveryOut {
    // Prime kernel readahead for every segment before any worker blocks on
    // its own read: cold recovery is then bounded by aggregate NVMe
    // bandwidth instead of per-stream readahead windows.
    for s in segs {
        if let Ok(f) = File::open(&s.path) {
            unsafe {
                libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_WILLNEED);
            }
        }
    }
    let parts: Vec<(SegScan, Heads, u64)> = segs
        .par_iter()
        .map(|s| {
            // Scan straight out of an mmap: no 256 MiB copy into a user
            // buffer, no allocation on the critical path. (The baseline
            // keeps fs::read, faithful to recovery_scale.)
            let f = File::open(&s.path).unwrap();
            let data = unsafe { memmap2::Mmap::map(&f).unwrap() };
            let mut heads = Heads::default();
            let r = scan_segment(&data, s.id, s.base_pos, &mut heads);
            let len = data.len() as u64;
            (r, heads, len)
        })
        .collect();

    let mut heads = Heads::default();
    let mut expect = segs.first().map(|s| s.base_pos).unwrap_or(0);
    let mut bytes = 0u64;
    let mut batches = 0u64;
    let mut events = 0u64;
    let mut last_stop = Stop::EndOfLog;
    for (i, (r, h, len)) in parts.into_iter().enumerate() {
        assert_eq!(segs[i].base_pos, expect, "segment {} base_pos mismatch", segs[i].id);
        check_interior(i, segs.len(), &segs[i], &r);
        heads.extend(h);
        bytes += len;
        batches += r.batches;
        events += r.events;
        expect = r.next_pos;
        last_stop = r.stop;
    }
    RecoveryOut { batches, events, next_pos: expect, bytes, heads, last_stop }
}

// ---------------------------------------------------------------------------
// Generation (recovery_scale::gen_dataset, crc32c)
// ---------------------------------------------------------------------------

pub struct GenOut {
    pub segs: Vec<SegMeta>,
    pub batches: u64,
    pub events: u64,
    pub next_pos: u64,
    pub bytes: u64,
}

fn make_events(counter: &mut u64, buf: &mut [[u8; PAYLOAD_LEN]; EVENTS_PER_BATCH]) {
    for e in buf.iter_mut() {
        e[..8].copy_from_slice(&counter.to_le_bytes());
        e[8..16].copy_from_slice(&(!*counter).to_le_bytes());
        *counter += 1;
    }
}

pub fn gen_dataset(dir: &Path, seg_size: u64, total_target: u64) -> GenOut {
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
    let mut total = 0u64;
    let mut batches = 0u64;
    let mut events = 0u64;
    let mut counter = 0u64;
    let mut payloads = [[0u8; PAYLOAD_LEN]; EVENTS_PER_BATCH];
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
        let bytes = encode_batch(batch_id, pos, stream_id, first_version, &ev_refs);
        let blen = bytes.len() as u64;

        if seg_len + blen > seg_size && seg_len > 0 {
            let mut f = w.into_inner().unwrap();
            let footer =
                Footer { batch_count: seg_batches, event_count: seg_events, base_pos: base, next_pos: pos };
            f.write_all(&encode_footer(&footer)).unwrap();
            f.sync_data().unwrap();
            segs.push(SegMeta {
                id: seg_id,
                base_pos: base,
                path: path.clone(),
                len: seg_len + FOOTER_LEN as u64,
            });
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
    let f = w.into_inner().unwrap();
    f.sync_data().unwrap();
    segs.push(SegMeta { id: seg_id, base_pos: base, path, len: seg_len });
    GenOut { segs, batches, events, next_pos: pos, bytes: total }
}

// ---------------------------------------------------------------------------
// Cold-cache eviction (fdatasync'd files + POSIX_FADV_DONTNEED)
// ---------------------------------------------------------------------------

pub fn evict(segs: &[SegMeta]) {
    for s in segs {
        if let Ok(f) = File::open(&s.path) {
            unsafe {
                libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
            }
        }
    }
    std::thread::sleep(Duration::from_millis(200));
}
