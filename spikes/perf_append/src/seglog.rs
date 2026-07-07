//! Segmented append-only log with D2 batch framing.
//!
//! Format and recovery scanner copied byte-for-byte compatible from
//! spikes/vertical_slice/src/seglog.rs (format v2). The scanner is UNCHANGED —
//! it is the correctness gate every optimized writer variant must satisfy.
//! Writer-side additions for this spike: `batch_len` (size known before
//! encode), `encode_batch_into` (append into a caller-owned reusable buffer,
//! no allocation on the steady-state hot path).
//!
//! On-disk layout (little-endian, no alignment padding):
//!
//! ```text
//! BatchHeader (54 bytes)
//!   magic                u32   0xBA7C4EAD
//!   format_version       u16   2
//!   frame_count          u32
//!   batch_id             u64   // per-segment, resets to 0 on roll
//!   total_len            u64   // whole batch on disk: header + subframes + marker
//!   first_global_pos     u64
//!   stream_id            u64   // batch-constant (one stream per batch)
//!   first_stream_version u64
//!   batch_crc            u32   // crc32 over whole batch with both crc fields zeroed
//!
//! EventSubframe* (8 bytes + payload)
//!   event_type_id        u32
//!   data_len             u32
//!   payload              [u8; data_len]
//!
//! CommitMarker (16 bytes)
//!   magic                u32   0xC0AA17ED
//!   total_len echo       u64
//!   batch_crc echo       u32
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const HEADER_MAGIC: u32 = 0xBA7C_4EAD;
pub const MARKER_MAGIC: u32 = 0xC0AA_17ED;
pub const FORMAT_VERSION: u16 = 2;

pub const HEADER_LEN: usize = 4 + 2 + 4 + 8 + 8 + 8 + 8 + 8 + 4; // 54
pub const SUBFRAME_HDR_LEN: usize = 4 + 4; // 8
pub const MARKER_LEN: usize = 4 + 8 + 4; // 16
pub const MIN_BATCH_LEN: usize = HEADER_LEN + MARKER_LEN;
/// Sanity bound so a corrupted total_len cannot cause huge scans (A2).
pub const MAX_BATCH_LEN: u64 = 16 << 20;
pub const SEGMENT_SIZE: u64 = 256 << 20;

const HEADER_CRC_OFF: usize = HEADER_LEN - 4; // 50

fn crc32(bytes: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(bytes);
    h.finalize()
}

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
// EventPtr (D5): what the index stores per event
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventPtr {
    pub segment_id: u64,
    pub offset: u64, // byte offset of the payload within the segment file
    pub len: u32,    // payload length
}

impl EventPtr {
    pub fn encode(&self) -> [u8; 20] {
        let mut b = [0u8; 20];
        b[..8].copy_from_slice(&self.segment_id.to_be_bytes());
        b[8..16].copy_from_slice(&self.offset.to_be_bytes());
        b[16..].copy_from_slice(&self.len.to_be_bytes());
        b
    }
    pub fn decode(b: &[u8]) -> Self {
        EventPtr {
            segment_id: u64::from_be_bytes(b[..8].try_into().unwrap()),
            offset: u64::from_be_bytes(b[8..16].try_into().unwrap()),
            len: u32::from_be_bytes(b[16..20].try_into().unwrap()),
        }
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// Exact on-disk size of a batch, computable before encoding.
pub fn batch_len(events: &[Vec<u8>]) -> usize {
    HEADER_LEN
        + events
            .iter()
            .map(|e| SUBFRAME_HDR_LEN + e.len())
            .sum::<usize>()
        + MARKER_LEN
}

/// Encode one batch by appending to `buf` (reusable across calls; the caller
/// clears it when flushed). Calls `on_payload(offset_within_buf, len)` for
/// each event payload, in order. Allocation-free when `buf` has capacity.
pub fn encode_batch_into(
    buf: &mut Vec<u8>,
    batch_id: u64,
    first_global_pos: u64,
    stream_id: u64,
    first_stream_version: u64,
    events: &[Vec<u8>],
    mut on_payload: impl FnMut(u32, u32),
) {
    assert!(!events.is_empty(), "empty batches forbidden (A5)");
    let total_len = batch_len(events);
    assert!((total_len as u64) <= MAX_BATCH_LEN);
    let start = buf.len();
    buf.reserve(total_len);

    buf.extend(HEADER_MAGIC.to_le_bytes());
    buf.extend(FORMAT_VERSION.to_le_bytes());
    buf.extend((events.len() as u32).to_le_bytes());
    buf.extend(batch_id.to_le_bytes());
    buf.extend((total_len as u64).to_le_bytes());
    buf.extend(first_global_pos.to_le_bytes());
    buf.extend(stream_id.to_le_bytes());
    buf.extend(first_stream_version.to_le_bytes());
    buf.extend(0u32.to_le_bytes()); // batch_crc placeholder (zeroed for hashing)

    for e in events {
        buf.extend(1u32.to_le_bytes()); // event_type_id (fixed; interning is D3, not this spike)
        buf.extend((e.len() as u32).to_le_bytes());
        on_payload(buf.len() as u32, e.len() as u32);
        buf.extend_from_slice(e);
    }

    buf.extend(MARKER_MAGIC.to_le_bytes());
    buf.extend((total_len as u64).to_le_bytes());
    buf.extend(0u32.to_le_bytes()); // crc echo placeholder
    debug_assert_eq!(buf.len() - start, total_len);

    let crc = crc32(&buf[start..]);
    buf[start + HEADER_CRC_OFF..start + HEADER_CRC_OFF + 4].copy_from_slice(&crc.to_le_bytes());
    let echo_off = start + total_len - 4;
    buf[echo_off..].copy_from_slice(&crc.to_le_bytes());
}

pub struct EncodedBatch {
    pub bytes: Vec<u8>,
    /// (payload offset within the batch, payload len) per event.
    pub payload_offs: Vec<(u32, u32)>,
}

/// Baseline allocating encoder (identical behavior to vertical_slice).
pub fn encode_batch(
    batch_id: u64,
    first_global_pos: u64,
    stream_id: u64,
    first_stream_version: u64,
    events: &[Vec<u8>],
) -> EncodedBatch {
    let mut bytes = Vec::with_capacity(batch_len(events));
    let mut payload_offs = Vec::with_capacity(events.len());
    encode_batch_into(
        &mut bytes,
        batch_id,
        first_global_pos,
        stream_id,
        first_stream_version,
        events,
        |o, l| payload_offs.push((o, l)),
    );
    EncodedBatch { bytes, payload_offs }
}

// ---------------------------------------------------------------------------
// Recovery scanner (D1) — UNCHANGED from vertical_slice; the correctness gate.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedBatch {
    pub batch_id: u64,
    pub first_global_pos: u64,
    pub stream_id: u64,
    pub first_stream_version: u64,
    pub offset: u64, // batch offset within the segment
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
    PositionDiscontinuity, // A1 contiguity guard
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovery {
    pub batches: Vec<ScannedBatch>,
    pub safe_offset: u64,
    pub next_global_pos: u64,
    pub next_batch_id: u64,
    pub stop: StopReason,
}

/// Scan forward from `start`, accepting a batch only if its CommitMarker
/// validates (magic + total_len echo + CRC echo + CRC verifies) AND its
/// first_global_pos is contiguous (A1). Stops at first failure.
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

        // CommitMarker validation (D2 / A3).
        let m = total_len - MARKER_LEN;
        if rd_u32(batch, m) != MARKER_MAGIC
            || rd_u64(batch, m + 4) != total_len as u64
            || rd_u32(batch, m + 12) != header_crc
        {
            break StopReason::BadMarker;
        }

        // CRC over the whole batch with both crc fields zeroed (A4).
        let mut tmp = batch.to_vec();
        tmp[HEADER_CRC_OFF..HEADER_CRC_OFF + 4].fill(0);
        let echo_off = total_len - 4;
        tmp[echo_off..].fill(0);
        if crc32(&tmp) != header_crc {
            break StopReason::BadCrc;
        }

        // Parse subframes; they must exactly tile header..marker.
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

        // A1 contiguity guard: a CRC-valid batch at the wrong global position
        // is stale data.
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
// Segment files
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SegMeta {
    pub id: u64,
    pub base_pos: u64, // first global position in this segment (A1 seed for scan)
    pub path: PathBuf,
}

pub fn segment_file_name(id: u64, base_pos: u64) -> String {
    format!("seg-{:06}-{:015}.log", id, base_pos)
}

pub fn list_segments(dir: &Path) -> Vec<SegMeta> {
    let mut out = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(rest) = name.strip_prefix("seg-").and_then(|r| r.strip_suffix(".log")) {
                let mut it = rest.splitn(2, '-');
                if let (Some(id), Some(base)) = (it.next(), it.next()) {
                    if let (Ok(id), Ok(base)) = (id.parse::<u64>(), base.parse::<u64>()) {
                        out.push(SegMeta { id, base_pos: base, path: e.path() });
                    }
                }
            }
        }
    }
    out.sort_by_key(|s| s.id);
    out
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

pub struct AppendOut {
    pub first_global_pos: u64,
    pub ptrs: Vec<EventPtr>,
    pub file: Arc<File>, // segment file covering this batch (for fdatasync)
    pub seg_id: u64,
    /// Set when this append rolled into a new segment: (new_seg_id, new_file).
    pub rolled: Option<(u64, Arc<File>)>,
}

pub struct SegmentLog {
    pub dir: PathBuf,
    pub seg_id: u64,
    pub seg_base: u64,
    pub file: Arc<File>,
    pub seg_len: u64,
    pub next_batch_id: u64,
    pub next_global_pos: u64,
}

pub fn open_segment_file(path: &Path) -> File {
    OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
        .unwrap()
}

impl SegmentLog {
    pub fn create(dir: &Path) -> Self {
        fs::create_dir_all(dir).unwrap();
        let path = dir.join(segment_file_name(0, 0));
        SegmentLog {
            dir: dir.to_path_buf(),
            seg_id: 0,
            seg_base: 0,
            file: Arc::new(open_segment_file(&path)),
            seg_len: 0,
            next_batch_id: 0,
            next_global_pos: 0,
        }
    }

    /// Roll to a new segment (A8: batches never span segments). The old
    /// segment is sealed with an fdatasync, exactly like vertical_slice.
    pub fn roll(&mut self) -> (u64, Arc<File>) {
        self.file.sync_data().unwrap();
        self.seg_id += 1;
        self.seg_base = self.next_global_pos;
        let path = self.dir.join(segment_file_name(self.seg_id, self.seg_base));
        self.file = Arc::new(open_segment_file(&path));
        self.seg_len = 0;
        self.next_batch_id = 0;
        (self.seg_id, self.file.clone())
    }

    /// Roll WITHOUT the inline fdatasync: returns the old file so a
    /// background sealer can sync it off the critical path (seal pipeline).
    pub fn roll_nosync(&mut self) -> (Arc<File>, u64, Arc<File>) {
        let old = self.file.clone();
        self.seg_id += 1;
        self.seg_base = self.next_global_pos;
        let path = self.dir.join(segment_file_name(self.seg_id, self.seg_base));
        self.file = Arc::new(open_segment_file(&path));
        self.seg_len = 0;
        self.next_batch_id = 0;
        (old, self.seg_id, self.file.clone())
    }

    /// Baseline buffered write of one framed batch (allocates per batch;
    /// identical behavior to vertical_slice::SegmentLog::append).
    pub fn append(&mut self, stream_id: u64, first_version: u64, events: &[Vec<u8>]) -> AppendOut {
        let enc = encode_batch(
            self.next_batch_id,
            self.next_global_pos,
            stream_id,
            first_version,
            events,
        );
        let blen = enc.bytes.len() as u64;

        let mut rolled = None;
        if self.seg_len + blen > SEGMENT_SIZE && self.seg_len > 0 {
            rolled = Some(self.roll());
        }

        let batch_off = self.seg_len;
        (&*self.file).write_all(&enc.bytes).unwrap();

        let ptrs = enc
            .payload_offs
            .iter()
            .map(|&(o, l)| EventPtr {
                segment_id: self.seg_id,
                offset: batch_off + o as u64,
                len: l,
            })
            .collect();

        let out = AppendOut {
            first_global_pos: self.next_global_pos,
            ptrs,
            file: self.file.clone(),
            seg_id: self.seg_id,
            rolled,
        };
        self.seg_len += blen;
        self.next_batch_id += 1;
        self.next_global_pos += events.len() as u64;
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let evs = vec![b"alpha".to_vec(), b"bravo-longer".to_vec()];
        let enc = encode_batch(0, 0, 7, 3, &evs);
        let r = scan(&enc.bytes, 0, 0);
        assert_eq!(r.stop, StopReason::EndOfLog);
        assert_eq!(r.batches.len(), 1);
        let b = &r.batches[0];
        assert_eq!(b.stream_id, 7);
        assert_eq!(b.first_stream_version, 3);
        assert_eq!(b.payloads.len(), 2);
        let (o, l) = b.payloads[1];
        assert_eq!(&enc.bytes[o as usize..o as usize + l as usize], b"bravo-longer");
        assert_eq!(r.next_global_pos, 2);
    }

    #[test]
    fn group_encode_matches_scan() {
        let mut buf = Vec::new();
        let evs1 = vec![b"one".to_vec(), b"two".to_vec()];
        let evs2 = vec![b"three".to_vec()];
        encode_batch_into(&mut buf, 0, 0, 7, 0, &evs1, |_, _| {});
        encode_batch_into(&mut buf, 1, 2, 9, 5, &evs2, |_, _| {});
        let r = scan(&buf, 0, 0);
        assert_eq!(r.stop, StopReason::EndOfLog);
        assert_eq!(r.batches.len(), 2);
        assert_eq!(r.batches[1].stream_id, 9);
        assert_eq!(r.next_global_pos, 3);
    }

    #[test]
    fn torn_marker_discards_batch() {
        let enc = encode_batch(0, 0, 7, 0, &[b"x".to_vec()]);
        let r = scan(&enc.bytes[..enc.bytes.len() - 7], 0, 0);
        assert!(r.batches.is_empty());
        assert_eq!(r.stop, StopReason::IncompleteBatch);
    }

    #[test]
    fn stale_valid_batch_rejected_by_contiguity() {
        let mut data = encode_batch(0, 0, 7, 0, &[b"a".to_vec(), b"b".to_vec()]).bytes;
        data.extend_from_slice(&encode_batch(99, 40, 8, 5, &[b"stale".to_vec()]).bytes);
        let r = scan(&data, 0, 0);
        assert_eq!(r.batches.len(), 1);
        assert_eq!(r.stop, StopReason::PositionDiscontinuity);
    }
}
