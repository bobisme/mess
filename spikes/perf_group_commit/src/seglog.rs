//! Batch-framed segment log. Format v2 is byte-identical to
//! spikes/vertical_slice/src/seglog.rs (itself derived from spikes/crash_log):
//!
//! ```text
//! BatchHeader (54 bytes)
//!   magic                u32   0xBA7C4EAD
//!   format_version       u16   2
//!   frame_count          u32
//!   batch_id             u64
//!   total_len            u64   // whole batch on disk: header + subframes + marker
//!   first_global_pos     u64
//!   stream_id            u64   // batch-constant (one stream per batch)
//!   first_stream_version u64
//!   batch_crc            u32   // crc32 over whole batch with both crc fields zeroed
//! EventSubframe* (8 bytes + payload)
//!   event_type_id        u32
//!   data_len             u32
//!   payload              [u8; data_len]
//! CommitMarker (16 bytes)
//!   magic                u32   0xC0AA17ED
//!   total_len echo       u64
//!   batch_crc echo       u32
//! ```
//!
//! New in this spike (design probe for D7 group-commit striping):
//!
//! - stripe-aware segment naming: `seg-s{stripe:02}-{id:06}.log`. Stripe 0
//!   only == the plain unstriped log.
//! - `scan()` grows a mode: `expect = Some(pos)` is the classic A1 exact
//!   contiguity scan; `expect = None` is the per-stripe scan, which validates
//!   framing/CRC only and leaves ordering to the merge.
//! - `recover_dir()` implements the striped recovery-merge rule:
//!
//!   ```text
//!   1. per stripe, in segment order: scan; stop the stripe at the FIRST
//!      invalid batch (A10 per stripe — never resync past a hole).
//!   2. sort all surviving batches by first_global_pos.
//!   3. accept the maximal exactly-tiled prefix from position 0; the first
//!      gap OR overlap stops acceptance; everything beyond is discarded.
//!   ```
//!
//!   Overlap detection at the merge replaces the A1 in-scan contiguity check
//!   with equivalent strength (a stale recycled batch either duplicates or
//!   gaps the tiling). The write side must guarantee an acked batch is never
//!   beyond a durable gap — see engines.rs (durable global watermark).

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
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
/// 1 GiB here (D2 picked 256 MiB) purely to keep segment rolls out of the
/// throughput measurement; rolls do not change group-commit conclusions.
pub const SEGMENT_SIZE: u64 = 1 << 30;

const HEADER_CRC_OFF: usize = HEADER_LEN - 4; // 50

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
// Encoding
// ---------------------------------------------------------------------------

pub fn batch_total_len(events: &[Vec<u8>]) -> usize {
    HEADER_LEN + events.iter().map(|e| SUBFRAME_HDR_LEN + e.len()).sum::<usize>() + MARKER_LEN
}

/// Append one framed batch to `buf`.
pub fn encode_batch_into(
    buf: &mut Vec<u8>,
    batch_id: u64,
    first_global_pos: u64,
    stream_id: u64,
    first_stream_version: u64,
    events: &[Vec<u8>],
) {
    assert!(!events.is_empty(), "empty batches forbidden (A5)");
    let total_len = batch_total_len(events);
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
        buf.extend(1u32.to_le_bytes()); // event_type_id (interning is D3, not this spike)
        buf.extend((e.len() as u32).to_le_bytes());
        buf.extend_from_slice(e);
    }

    buf.extend(MARKER_MAGIC.to_le_bytes());
    buf.extend((total_len as u64).to_le_bytes());
    buf.extend(0u32.to_le_bytes()); // crc echo placeholder
    debug_assert_eq!(buf.len() - start, total_len);

    let crc = crc32fast::hash(&buf[start..]);
    let crc_off = start + HEADER_CRC_OFF;
    buf[crc_off..crc_off + 4].copy_from_slice(&crc.to_le_bytes());
    let echo_off = start + total_len - 4;
    buf[echo_off..].copy_from_slice(&crc.to_le_bytes());
}

pub fn encode_batch(
    batch_id: u64,
    first_global_pos: u64,
    stream_id: u64,
    first_stream_version: u64,
    events: &[Vec<u8>],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(batch_total_len(events));
    encode_batch_into(&mut buf, batch_id, first_global_pos, stream_id, first_stream_version, events);
    buf
}

// ---------------------------------------------------------------------------
// Recovery scanner (D1)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedBatch {
    pub batch_id: u64,
    pub first_global_pos: u64,
    pub stream_id: u64,
    pub first_stream_version: u64,
    pub offset: u64,
    pub total_len: u64,
    /// (absolute payload offset within the scanned buffer, payload len) per event.
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
    PositionDiscontinuity, // A1 contiguity guard (exact mode only)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovery {
    pub batches: Vec<ScannedBatch>,
    pub safe_offset: u64,
    pub stop: StopReason,
}

/// Scan forward from `start`, accepting a batch only if its CommitMarker
/// validates (magic + total_len echo + CRC echo + CRC verifies). Stops at the
/// first failure (A10: never resynchronize past a hole).
///
/// `expect = Some(pos)`: additionally require exact global-position
/// contiguity (A1). `expect = None`: stripe mode — the caller merges stripes
/// and enforces global tiling there (physical order within a stripe file is
/// NOT global-position order under concurrent writers).
pub fn scan(data: &[u8], start: usize, expect: Option<u64>) -> Recovery {
    let mut off = start;
    let mut expect_pos = expect;
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
        if crc32fast::hash(&tmp) != header_crc {
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

        // A1 exact contiguity (unstriped mode only).
        if let Some(exp) = expect_pos {
            if first_global_pos != exp {
                break StopReason::PositionDiscontinuity;
            }
            expect_pos = Some(exp + frame_count as u64);
        }

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

    Recovery { batches, safe_offset: off as u64, stop }
}

// ---------------------------------------------------------------------------
// Segment files
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SegMeta {
    pub stripe: u32,
    pub id: u64,
    pub path: PathBuf,
}

pub fn segment_file_name(stripe: u32, id: u64) -> String {
    format!("seg-s{stripe:02}-{id:06}.log")
}

pub fn list_segments(dir: &Path) -> Vec<SegMeta> {
    let mut out = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(rest) = name.strip_prefix("seg-s").and_then(|r| r.strip_suffix(".log")) {
                let mut it = rest.splitn(2, '-');
                if let (Some(stripe), Some(id)) = (it.next(), it.next()) {
                    if let (Ok(stripe), Ok(id)) = (stripe.parse::<u32>(), id.parse::<u64>()) {
                        out.push(SegMeta { stripe, id, path: e.path() });
                    }
                }
            }
        }
    }
    out.sort_by_key(|s| (s.stripe, s.id));
    out
}

fn open_segment_file(path: &Path, dsync: bool) -> File {
    let mut o = OpenOptions::new();
    o.create(true).read(true).write(true);
    if dsync {
        o.custom_flags(libc::O_DSYNC);
    }
    // NOT O_APPEND: pwrite on an O_APPEND fd ignores the offset on Linux, and
    // the pwrite engine positions writes explicitly. Locked-mode writers share
    // the fd cursor under a lock instead.
    o.open(path).unwrap()
}

// ---------------------------------------------------------------------------
// Writer (locked mode)
// ---------------------------------------------------------------------------

pub struct AppendOut {
    pub first_global_pos: u64,
    pub seg_id: u64,
    pub file: Arc<File>,
    pub bytes: u64,
}

pub struct SegmentLog {
    pub dir: PathBuf,
    pub stripe: u32,
    pub dsync: bool,
    pub seg_id: u64,
    pub file: Arc<File>,
    pub seg_len: u64,
    pub next_batch_id: u64,
    /// Callers may overwrite before append() when positions are assigned
    /// externally (striped engines).
    pub next_global_pos: u64,
}

impl SegmentLog {
    pub fn create(dir: &Path, stripe: u32, dsync: bool) -> Self {
        fs::create_dir_all(dir).unwrap();
        let path = dir.join(segment_file_name(stripe, 0));
        SegmentLog {
            dir: dir.to_path_buf(),
            stripe,
            dsync,
            seg_id: 0,
            file: Arc::new(open_segment_file(&path, dsync)),
            seg_len: 0,
            next_batch_id: 0,
            next_global_pos: 0,
        }
    }

    /// Roll to a new segment. `sync_old = false` is only sound when the
    /// caller's fsync pipeline still holds an Arc to the old file and will
    /// cover its unsynced batches (pwrite/committer engines).
    pub fn roll(&mut self, sync_old: bool) {
        if sync_old {
            self.file.sync_data().unwrap();
        }
        self.seg_id += 1;
        let path = self.dir.join(segment_file_name(self.stripe, self.seg_id));
        self.file = Arc::new(open_segment_file(&path, self.dsync));
        self.seg_len = 0;
        self.next_batch_id = 0;
    }

    /// Buffered write of one framed batch at `self.next_global_pos`
    /// (durability handled by the caller).
    pub fn append(&mut self, stream_id: u64, first_version: u64, events: &[Vec<u8>]) -> AppendOut {
        let blen = batch_total_len(events) as u64;
        if self.seg_len + blen > SEGMENT_SIZE && self.seg_len > 0 {
            self.roll(true); // A8: batches never span segments
        }
        let buf = encode_batch(
            self.next_batch_id,
            self.next_global_pos,
            stream_id,
            first_version,
            events,
        );
        (&*self.file).write_all(&buf).unwrap();
        let out = AppendOut {
            first_global_pos: self.next_global_pos,
            seg_id: self.seg_id,
            file: self.file.clone(),
            bytes: blen,
        };
        self.seg_len += blen;
        self.next_batch_id += 1;
        self.next_global_pos += events.len() as u64;
        out
    }
}

// ---------------------------------------------------------------------------
// Striped recovery merge
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecBatch {
    pub stripe: u32,
    pub first_global_pos: u64,
    pub stream_id: u64,
    pub first_version: u64,
    pub nevents: u32,
    pub payload_crc: u32, // crc32 over the concatenated payload bytes
}

#[derive(Debug)]
pub struct DirRecovery {
    /// Accepted batches, in global-position order, exactly tiled from 0.
    pub accepted: Vec<RecBatch>,
    pub total_events: u64,
    /// Framing-valid batches discarded by the merge (beyond the first gap).
    pub discarded: usize,
    pub stripes: u32,
}

/// Recover a (possibly striped) log directory per the merge rule in the
/// module docs. Deterministic: output depends only on the file contents.
pub fn recover_dir(dir: &Path) -> DirRecovery {
    use std::collections::BTreeMap;
    let mut by_stripe: BTreeMap<u32, Vec<SegMeta>> = BTreeMap::new();
    for m in list_segments(dir) {
        by_stripe.entry(m.stripe).or_default().push(m);
    }
    let stripes = by_stripe.len() as u32;

    let mut all: Vec<RecBatch> = Vec::new();
    for (stripe, metas) in &by_stripe {
        'stripe: for m in metas {
            let data = fs::read(&m.path).unwrap();
            let rec = scan(&data, 0, None);
            for b in &rec.batches {
                let mut h = crc32fast::Hasher::new();
                for &(o, l) in &b.payloads {
                    h.update(&data[o as usize..o as usize + l as usize]);
                }
                all.push(RecBatch {
                    stripe: *stripe,
                    first_global_pos: b.first_global_pos,
                    stream_id: b.stream_id,
                    first_version: b.first_stream_version,
                    nevents: b.payloads.len() as u32,
                    payload_crc: h.finalize(),
                });
            }
            if rec.stop != StopReason::EndOfLog {
                break 'stripe; // A10 per stripe: stop at the first invalid batch
            }
        }
    }

    let scanned = all.len();
    all.sort_by_key(|b| b.first_global_pos);
    let mut accepted = Vec::with_capacity(all.len());
    let mut expect = 0u64;
    for b in all {
        if b.first_global_pos != expect {
            break; // gap (unsynced earlier batch) or overlap (stale batch)
        }
        expect += b.nevents as u64;
        accepted.push(b);
    }
    let discarded = scanned - accepted.len();
    DirRecovery { total_events: expect, discarded, accepted, stripes }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn testdir(name: &str) -> PathBuf {
        let d = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/testdata").join(name);
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn roundtrip() {
        let evs = vec![b"alpha".to_vec(), b"bravo-longer".to_vec()];
        let buf = encode_batch(0, 0, 7, 3, &evs);
        let r = scan(&buf, 0, Some(0));
        assert_eq!(r.stop, StopReason::EndOfLog);
        assert_eq!(r.batches.len(), 1);
        let b = &r.batches[0];
        assert_eq!(b.stream_id, 7);
        assert_eq!(b.first_stream_version, 3);
        assert_eq!(b.payloads.len(), 2);
        let (o, l) = b.payloads[1];
        assert_eq!(&buf[o as usize..o as usize + l as usize], b"bravo-longer");
    }

    #[test]
    fn torn_marker_discards_batch() {
        let buf = encode_batch(0, 0, 7, 0, &[b"x".to_vec()]);
        let r = scan(&buf[..buf.len() - 7], 0, Some(0));
        assert!(r.batches.is_empty());
        assert_eq!(r.stop, StopReason::IncompleteBatch);
    }

    #[test]
    fn exact_mode_rejects_discontinuity_stripe_mode_defers() {
        let mut data = encode_batch(0, 0, 7, 0, &[b"a".to_vec(), b"b".to_vec()]);
        data.extend_from_slice(&encode_batch(99, 40, 8, 5, &[b"stale".to_vec()]));
        let r = scan(&data, 0, Some(0));
        assert_eq!(r.batches.len(), 1);
        assert_eq!(r.stop, StopReason::PositionDiscontinuity);
        let r = scan(&data, 0, None);
        assert_eq!(r.batches.len(), 2); // stripe mode: merge is responsible
    }

    #[test]
    fn hole_stops_scan() {
        let mut data = vec![0u8; 200];
        data.extend_from_slice(&encode_batch(0, 0, 7, 0, &[b"beyond-the-hole".to_vec()]));
        let r = scan(&data, 0, None);
        assert!(r.batches.is_empty());
        assert_eq!(r.stop, StopReason::BadHeaderMagic);
    }

    #[test]
    fn striped_merge_reproduces_global_order() {
        let dir = testdir("merge_order");
        // Stripe 0 holds gpos 0 and 3 (physically out of order), stripe 1
        // holds gpos 1..=2. Merge must yield 0,1,3 tiling.
        let mut s0 = SegmentLog::create(&dir, 0, false);
        let mut s1 = SegmentLog::create(&dir, 1, false);
        s0.next_global_pos = 3;
        s0.append(7, 3, &[b"third".to_vec()]);
        s0.next_global_pos = 0;
        s0.append(7, 0, &[b"first".to_vec()]);
        s1.next_global_pos = 1;
        s1.append(8, 0, &[b"second-a".to_vec(), b"second-b".to_vec()]);
        s0.file.sync_data().unwrap();
        s1.file.sync_data().unwrap();

        let rec = recover_dir(&dir);
        assert_eq!(rec.stripes, 2);
        assert_eq!(rec.total_events, 4);
        assert_eq!(rec.discarded, 0);
        let gpos: Vec<u64> = rec.accepted.iter().map(|b| b.first_global_pos).collect();
        assert_eq!(gpos, vec![0, 1, 3]);
        let stripes: Vec<u32> = rec.accepted.iter().map(|b| b.stripe).collect();
        assert_eq!(stripes, vec![0, 1, 0]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn striped_merge_discards_past_gap() {
        let dir = testdir("merge_gap");
        // Stripe 0: gpos 0 (1 event). Stripe 1: gpos 2 (1 event) — gpos 1
        // missing (its stripe-0 batch was never written). The valid batch at
        // gpos 2 must be discarded even though it is durable.
        let mut s0 = SegmentLog::create(&dir, 0, false);
        let mut s1 = SegmentLog::create(&dir, 1, false);
        s0.next_global_pos = 0;
        s0.append(7, 0, &[b"zero".to_vec()]);
        s1.next_global_pos = 2;
        s1.append(9, 0, &[b"two".to_vec()]);
        let rec = recover_dir(&dir);
        assert_eq!(rec.total_events, 1);
        assert_eq!(rec.discarded, 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn merge_rejects_overlap() {
        let dir = testdir("merge_overlap");
        // Two batches claiming gpos 0 (stale recycled batch scenario): the
        // second one (by sort order equal, either) must stop the tiling after
        // the first — expect only 1 accepted and 1+ discarded.
        let mut s0 = SegmentLog::create(&dir, 0, false);
        s0.next_global_pos = 0;
        s0.append(7, 0, &[b"a".to_vec(), b"b".to_vec()]);
        s0.next_global_pos = 1; // overlaps [0,2)
        s0.append(8, 0, &[b"stale".to_vec()]);
        let rec = recover_dir(&dir);
        assert_eq!(rec.accepted.len(), 1);
        assert_eq!(rec.total_events, 2);
        assert_eq!(rec.discarded, 1);
        let _ = fs::remove_dir_all(&dir);
    }
}
