//! Active segment: batch-framed append-only log.
//!
//! Format (v2, 54-byte BatchHeader + subframes + 16-byte CommitMarker) and
//! recovery scanner adapted from spikes/vertical_slice/src/seglog.rs, which in
//! turn came from spikes/crash_log. This spike only needs the writer and the
//! forward scan; segment rolling is removed (one segment, sealed in place).

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub const HEADER_MAGIC: u32 = 0xBA7C_4EAD;
pub const MARKER_MAGIC: u32 = 0xC0AA_17ED;
pub const FORMAT_VERSION: u16 = 2;

pub const HEADER_LEN: usize = 4 + 2 + 4 + 8 + 8 + 8 + 8 + 8 + 4; // 54
pub const SUBFRAME_HDR_LEN: usize = 4 + 4; // 8
pub const MARKER_LEN: usize = 4 + 8 + 4; // 16
pub const MIN_BATCH_LEN: usize = HEADER_LEN + MARKER_LEN;
pub const MAX_BATCH_LEN: u64 = 16 << 20;

const HEADER_CRC_OFF: usize = HEADER_LEN - 4; // 50

fn crc32(bytes: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(bytes);
    h.finalize()
}

pub fn rd_u16(d: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(d[o..o + 2].try_into().unwrap())
}
pub fn rd_u32(d: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(d[o..o + 4].try_into().unwrap())
}
pub fn rd_u64(d: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(d[o..o + 8].try_into().unwrap())
}

/// Shared read-benchmark accumulator (same formula as vertical_slice so the
/// pre/post-seal replay paths can be cross-checked for identical results).
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayStats {
    pub events: u64,
    pub bytes: u64,
    pub checksum: u64, // forces payload bytes to actually be touched
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

pub struct EncodedBatch {
    pub bytes: Vec<u8>,
    /// (payload offset within the batch, payload len) per event.
    pub payload_offs: Vec<(u32, u32)>,
}

pub fn encode_batch(
    batch_id: u64,
    first_global_pos: u64,
    stream_id: u64,
    first_stream_version: u64,
    events: &[Vec<u8>],
) -> EncodedBatch {
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
    buf.extend(0u32.to_le_bytes()); // batch_crc placeholder (zeroed for hashing)

    let mut payload_offs = Vec::with_capacity(events.len());
    for e in events {
        buf.extend(1u32.to_le_bytes()); // event_type_id (interning is D3, not this spike)
        buf.extend((e.len() as u32).to_le_bytes());
        payload_offs.push((buf.len() as u32, e.len() as u32));
        buf.extend_from_slice(e);
    }

    buf.extend(MARKER_MAGIC.to_le_bytes());
    buf.extend((total_len as u64).to_le_bytes());
    buf.extend(0u32.to_le_bytes()); // crc echo placeholder
    debug_assert_eq!(buf.len(), total_len);

    let crc = crc32(&buf);
    buf[HEADER_CRC_OFF..HEADER_CRC_OFF + 4].copy_from_slice(&crc.to_le_bytes());
    let echo_off = total_len - 4;
    buf[echo_off..].copy_from_slice(&crc.to_le_bytes());
    EncodedBatch { bytes: buf, payload_offs }
}

// ---------------------------------------------------------------------------
// Recovery / replay scanner (D1) — unchanged from vertical_slice
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedBatch {
    pub batch_id: u64,
    pub first_global_pos: u64,
    pub stream_id: u64,
    pub first_stream_version: u64,
    pub offset: u64,
    pub total_len: u64,
    /// (absolute payload offset within the segment, payload len) per event.
    pub payloads: Vec<(u64, u32)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    EndOfLog,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovery {
    pub batches: Vec<ScannedBatch>,
    pub safe_offset: u64,
    pub next_global_pos: u64,
    pub next_batch_id: u64,
    pub stop: StopReason,
}

pub fn scan(data: &[u8], start: usize, mut expect_pos: u64) -> Recovery {
    let mut off = start;
    let mut batches: Vec<ScannedBatch> = Vec::new();

    let stop = loop {
        let rem = data.len() - off;
        if rem == 0 {
            break StopReason::EndOfLog;
        }
        if rem < HEADER_LEN {
            break StopReason::TornHeader;
        }
        if rd_u32(data, off) != HEADER_MAGIC {
            break StopReason::BadHeaderMagic;
        }
        if rd_u16(data, off + 4) != FORMAT_VERSION {
            break StopReason::BadVersion;
        }
        let frame_count = rd_u32(data, off + 6);
        let batch_id = rd_u64(data, off + 10);
        let total_len = rd_u64(data, off + 18);
        let first_global_pos = rd_u64(data, off + 26);
        let stream_id = rd_u64(data, off + 34);
        let first_stream_version = rd_u64(data, off + 42);
        let header_crc = rd_u32(data, off + HEADER_CRC_OFF);

        if total_len < MIN_BATCH_LEN as u64 || total_len > MAX_BATCH_LEN {
            break StopReason::BadLength;
        }
        let total_len = total_len as usize;
        if total_len > rem {
            break StopReason::IncompleteBatch;
        }

        let batch = &data[off..off + total_len];

        let m = total_len - MARKER_LEN;
        if rd_u32(batch, m) != MARKER_MAGIC
            || rd_u64(batch, m + 4) != total_len as u64
            || rd_u32(batch, m + 12) != header_crc
        {
            break StopReason::BadMarker;
        }

        let mut tmp = batch.to_vec();
        tmp[HEADER_CRC_OFF..HEADER_CRC_OFF + 4].fill(0);
        let echo_off = total_len - 4;
        tmp[echo_off..].fill(0);
        if crc32(&tmp) != header_crc {
            break StopReason::BadCrc;
        }

        let mut p = HEADER_LEN;
        let frames_end = total_len - MARKER_LEN;
        let mut payloads = Vec::with_capacity(frame_count as usize);
        let mut ok = frame_count > 0;
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
            payloads.push(((off + p) as u64, dlen as u32));
            p += dlen;
        }
        if !ok || p != frames_end {
            break StopReason::BadFrames;
        }

        if first_global_pos != expect_pos {
            break StopReason::PositionDiscontinuity;
        }

        expect_pos += frame_count as u64;
        batches.push(ScannedBatch {
            batch_id,
            first_global_pos,
            stream_id,
            first_stream_version,
            offset: off as u64,
            total_len: total_len as u64,
            payloads,
        });
        off += total_len;
    };

    let next_batch_id = batches.last().map(|b| b.batch_id + 1).unwrap_or(0);
    Recovery {
        batches,
        safe_offset: off as u64,
        next_global_pos: expect_pos,
        next_batch_id,
        stop,
    }
}

// ---------------------------------------------------------------------------
// Writer (single segment; rolling removed for this spike)
// ---------------------------------------------------------------------------

pub struct SegmentWriter {
    pub path: PathBuf,
    pub file: File,
    pub seg_len: u64,
    pub next_batch_id: u64,
    pub next_global_pos: u64,
}

impl SegmentWriter {
    pub fn create(dir: &Path) -> Self {
        fs::create_dir_all(dir).unwrap();
        let path = dir.join("seg-000000.log");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        SegmentWriter { path, file, seg_len: 0, next_batch_id: 0, next_global_pos: 0 }
    }

    /// Buffered write of one framed batch. Returns (first_global_pos,
    /// per-event (absolute payload offset, len)).
    pub fn append(
        &mut self,
        stream_id: u64,
        first_version: u64,
        events: &[Vec<u8>],
    ) -> (u64, Vec<(u64, u32)>) {
        let enc = encode_batch(
            self.next_batch_id,
            self.next_global_pos,
            stream_id,
            first_version,
            events,
        );
        let batch_off = self.seg_len;
        self.file.write_all(&enc.bytes).unwrap();
        let ptrs = enc
            .payload_offs
            .iter()
            .map(|&(o, l)| (batch_off + o as u64, l))
            .collect();
        let first_pos = self.next_global_pos;
        self.seg_len += enc.bytes.len() as u64;
        self.next_batch_id += 1;
        self.next_global_pos += events.len() as u64;
        (first_pos, ptrs)
    }
}
