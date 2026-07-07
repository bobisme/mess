//! Spike: crash-safety of the batch-framed append-only log per
//! notes/mess-research/12_convergence.md sections D1 (log-with-commit-markers
//! is the sole commit authority) and D2 (batch framing).
//!
//! Throwaway code. Clarity over polish. The deliverable is REPORT.md.
//!
//! On-disk layout (little-endian, no alignment padding):
//!
//! ```text
//! BatchHeader (38 bytes)
//!   magic            u32   0xBA7C4EAD
//!   format_version   u16
//!   frame_count      u32
//!   batch_id         u64
//!   total_len        u64   // whole batch on disk: header + subframes + marker
//!   first_global_pos u64
//!   batch_crc        u32   // crc32 over the whole batch with both crc fields zeroed
//!
//! EventSubframe* (8 bytes + payload)
//!   event_type_id    u32
//!   data_len         u32
//!   payload          [u8; data_len]
//!
//! CommitMarker (16 bytes)
//!   magic            u32   0xC0AA17ED
//!   total_len echo   u64
//!   batch_crc echo   u32
//! ```

use crc32fast::Hasher;

pub const HEADER_MAGIC: u32 = 0xBA7C_4EAD;
pub const MARKER_MAGIC: u32 = 0xC0AA_17ED;
pub const FORMAT_VERSION: u16 = 1;

pub const HEADER_LEN: usize = 4 + 2 + 4 + 8 + 8 + 8 + 4; // 38
pub const SUBFRAME_HDR_LEN: usize = 4 + 4; // 8
pub const MARKER_LEN: usize = 4 + 8 + 4; // 16
pub const MIN_BATCH_LEN: usize = HEADER_LEN + MARKER_LEN;

/// Sanity bound so a corrupted total_len cannot cause huge scans/allocations.
/// A real implementation needs this pinned in the spec (see REPORT.md).
pub const MAX_BATCH_LEN: u64 = 1 << 20;

const HEADER_CRC_OFF: usize = HEADER_LEN - 4; // 34

fn crc32(bytes: &[u8]) -> u32 {
    let mut h = Hasher::new();
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
// Encoding
// ---------------------------------------------------------------------------

/// Encode one batch. `events` are raw payloads; the spike uses a fixed
/// event_type_id of 1 (the framing protocol, not the field set, is the point).
pub fn encode_batch(batch_id: u64, first_global_pos: u64, events: &[Vec<u8>]) -> Vec<u8> {
    assert!(!events.is_empty(), "spike disallows empty batches (spec ambiguity, see REPORT)");
    let frames_len: usize = events.iter().map(|e| SUBFRAME_HDR_LEN + e.len()).sum();
    let total_len = HEADER_LEN + frames_len + MARKER_LEN;
    assert!((total_len as u64) <= MAX_BATCH_LEN);

    let mut buf = Vec::with_capacity(total_len);
    // header
    buf.extend(HEADER_MAGIC.to_le_bytes());
    buf.extend(FORMAT_VERSION.to_le_bytes());
    buf.extend((events.len() as u32).to_le_bytes());
    buf.extend(batch_id.to_le_bytes());
    buf.extend((total_len as u64).to_le_bytes());
    buf.extend(first_global_pos.to_le_bytes());
    buf.extend(0u32.to_le_bytes()); // batch_crc placeholder (zeroed for hashing, per D2)
    // subframes
    for e in events {
        buf.extend(1u32.to_le_bytes()); // event_type_id
        buf.extend((e.len() as u32).to_le_bytes());
        buf.extend(e.iter());
    }
    // marker
    buf.extend(MARKER_MAGIC.to_le_bytes());
    buf.extend((total_len as u64).to_le_bytes());
    buf.extend(0u32.to_le_bytes()); // crc echo placeholder
    debug_assert_eq!(buf.len(), total_len);

    // CRC over the whole batch with both crc fields zeroed, then patch both.
    let crc = crc32(&buf);
    buf[HEADER_CRC_OFF..HEADER_CRC_OFF + 4].copy_from_slice(&crc.to_le_bytes());
    let echo_off = total_len - 4;
    buf[echo_off..].copy_from_slice(&crc.to_le_bytes());
    buf
}

// ---------------------------------------------------------------------------
// Fault-injecting "disk"
// ---------------------------------------------------------------------------

/// A simulated file + page cache. `buf` is everything the process wrote;
/// `synced` is the fsync watermark (bytes guaranteed durable). A configured
/// crash stops writes at an absolute byte offset (keeping the prefix), or
/// fails an fsync call. `crash()` then materializes what survives: a
/// prefix whose length is anywhere in [synced, written] (torn tail),
/// optionally with garbage scribbled into the surviving-but-unsynced region.
#[derive(Debug)]
pub struct FaultWriter {
    pub buf: Vec<u8>,
    pub synced: usize,
    pub crash_at: Option<usize>,
    /// If the write stream reaches exactly `crash_at`, crash inside the next
    /// fsync call instead of the write (models "marker written, fsync lost").
    pub crash_on_fsync: bool,
    pub crashed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Crashed;

impl FaultWriter {
    pub fn new(crash_at: Option<usize>, crash_on_fsync: bool) -> Self {
        FaultWriter { buf: Vec::new(), synced: 0, crash_at, crash_on_fsync, crashed: false }
    }

    /// Reopen a recovered file: contents are durable, no fault configured.
    pub fn from_recovered(bytes: Vec<u8>) -> Self {
        let n = bytes.len();
        FaultWriter { buf: bytes, synced: n, crash_at: None, crash_on_fsync: false, crashed: false }
    }

    pub fn write_all(&mut self, bytes: &[u8]) -> Result<(), Crashed> {
        if self.crashed {
            return Err(Crashed);
        }
        if let Some(at) = self.crash_at {
            let room = at.saturating_sub(self.buf.len());
            if bytes.len() > room {
                self.buf.extend_from_slice(&bytes[..room]); // torn write: prefix only
                self.crashed = true;
                return Err(Crashed);
            }
        }
        self.buf.extend_from_slice(bytes);
        Ok(())
    }

    pub fn fsync(&mut self) -> Result<(), Crashed> {
        if self.crashed {
            return Err(Crashed);
        }
        if self.crash_on_fsync && self.crash_at == Some(self.buf.len()) {
            self.crashed = true; // data written, durability never acknowledged
            return Err(Crashed);
        }
        self.synced = self.buf.len();
        Ok(())
    }

    /// What is on disk after the crash. Everything up to `synced` survives.
    /// If `drop_unsynced`, the un-fsynced tail survives only up to a random
    /// length; if `scramble`, surviving-but-unsynced bytes may be corrupted
    /// (torn sector / partially persisted page).
    pub fn crash(&self, rng: &mut impl rand::Rng, drop_unsynced: bool, scramble: bool) -> Vec<u8> {
        let end = if drop_unsynced {
            rng.gen_range(self.synced..=self.buf.len())
        } else {
            self.buf.len()
        };
        let mut out = self.buf[..end].to_vec();
        if scramble && end > self.synced {
            for _ in 0..rng.gen_range(1..=4usize) {
                let i = rng.gen_range(self.synced..end);
                out[i] = rng.gen();
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitInfo {
    pub batch_id: u64,
    pub first_global_pos: u64,
    pub offset: u64,
    pub len: u64,
}

pub struct Log {
    pub disk: FaultWriter,
    pub next_batch_id: u64,
    pub next_global_pos: u64,
}

impl Log {
    pub fn new(disk: FaultWriter) -> Self {
        Log { disk, next_batch_id: 0, next_global_pos: 0 }
    }

    /// Reopen after recovery: continue from the recovered state.
    pub fn reopen(disk: FaultWriter, rec: &Recovery) -> Self {
        assert_eq!(disk.buf.len() as u64, rec.safe_offset, "reopen requires file truncated to safe_offset");
        Log { disk, next_batch_id: rec.next_batch_id, next_global_pos: rec.next_global_pos }
    }

    /// Encode + write + fsync one batch. Ok(_) == durability acknowledged.
    pub fn append_batch(&mut self, events: &[Vec<u8>]) -> Result<CommitInfo, Crashed> {
        let bytes = encode_batch(self.next_batch_id, self.next_global_pos, events);
        let offset = self.disk.buf.len() as u64;
        self.disk.write_all(&bytes)?;
        self.disk.fsync()?;
        let info = CommitInfo {
            batch_id: self.next_batch_id,
            first_global_pos: self.next_global_pos,
            offset,
            len: bytes.len() as u64,
        };
        self.next_batch_id += 1;
        self.next_global_pos += events.len() as u64;
        Ok(info)
    }
}

// ---------------------------------------------------------------------------
// Recovery scanner (D1)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredBatch {
    pub batch_id: u64,
    pub first_global_pos: u64,
    pub offset: u64,
    pub total_len: u64,
    pub events: Vec<(u32, Vec<u8>)>, // (event_type_id, payload)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    EndOfLog,
    TornHeader,          // fewer bytes than a header remain
    BadHeaderMagic,
    BadVersion,
    BadLength,           // total_len out of sane bounds
    IncompleteBatch,     // total_len exceeds remaining bytes (no room for a marker)
    BadMarker,           // marker magic / total_len echo / crc echo mismatch
    BadCrc,              // marker fields echo the header but the CRC doesn't verify
    BadFrames,           // CRC valid but subframes don't tile total_len (writer bug)
    PositionDiscontinuity, // structurally valid batch whose first_global_pos != expected
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovery {
    pub batches: Vec<RecoveredBatch>,
    /// Byte offset of the first invalid/incomplete batch: the safe truncation
    /// point and the offset at which appends resume.
    pub safe_offset: u64,
    pub next_global_pos: u64,
    pub next_batch_id: u64,
    pub stop: StopReason,
}

/// Scan forward from `start`, accepting a batch only if its CommitMarker
/// validates (magic + total_len echo + CRC echo + CRC verifies) AND its
/// first_global_pos is contiguous with the previous accepted batch.
/// Stops at the first failure. All-or-nothing per batch.
pub fn scan(data: &[u8], start: usize, mut expect_pos: u64) -> Recovery {
    let mut off = start;
    let mut batches: Vec<RecoveredBatch> = Vec::new();

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
        let header_crc = rd_u32(data, off + HEADER_CRC_OFF);

        if total_len < MIN_BATCH_LEN as u64 || total_len > MAX_BATCH_LEN {
            break StopReason::BadLength;
        }
        let total_len = total_len as usize;
        if total_len > rem {
            break StopReason::IncompleteBatch;
        }

        let batch = &data[off..off + total_len];

        // CommitMarker validation: magic + length echo + crc echo (D2).
        let m = total_len - MARKER_LEN;
        if rd_u32(batch, m) != MARKER_MAGIC
            || rd_u64(batch, m + 4) != total_len as u64
            || rd_u32(batch, m + 12) != header_crc
        {
            break StopReason::BadMarker;
        }

        // CRC over the whole batch with both crc fields zeroed.
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
        let mut events = Vec::with_capacity(frame_count as usize);
        let mut ok = true;
        for _ in 0..frame_count {
            if p + SUBFRAME_HDR_LEN > frames_end {
                ok = false;
                break;
            }
            let ety = rd_u32(batch, p);
            let dlen = rd_u32(batch, p + 4) as usize;
            p += SUBFRAME_HDR_LEN;
            if p + dlen > frames_end {
                ok = false;
                break;
            }
            events.push((ety, batch[p..p + dlen].to_vec()));
            p += dlen;
        }
        if !ok || p != frames_end {
            break StopReason::BadFrames;
        }

        // Continuity guard: a CRC-valid batch at the wrong global position is
        // stale data (e.g. recycled segment contents past a truncation point).
        // Not in the D1/D2 spec text — see REPORT.md.
        if first_global_pos != expect_pos {
            break StopReason::PositionDiscontinuity;
        }

        expect_pos += frame_count as u64;
        batches.push(RecoveredBatch {
            batch_id,
            first_global_pos,
            offset: off as u64,
            total_len: total_len as u64,
            events,
        });
        off += total_len;
    };

    let next_batch_id = batches.last().map(|b| b.batch_id + 1).unwrap_or(0);
    Recovery { batches, safe_offset: off as u64, next_global_pos: expect_pos, next_batch_id, stop }
}

// ---------------------------------------------------------------------------
// Deterministic edge-case tests (4a)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn batch0() -> Vec<u8> {
        encode_batch(0, 0, &[b"alpha".to_vec(), b"bravo-longer".to_vec()])
    }
    fn batch1() -> Vec<u8> {
        encode_batch(1, 2, &[b"charlie".to_vec()])
    }

    #[test]
    fn empty_file() {
        let r = scan(&[], 0, 0);
        assert!(r.batches.is_empty());
        assert_eq!(r.safe_offset, 0);
        assert_eq!(r.stop, StopReason::EndOfLog);
    }

    #[test]
    fn torn_header() {
        let enc = batch0();
        let r = scan(&enc[..HEADER_LEN - 5], 0, 0);
        assert!(r.batches.is_empty());
        assert_eq!(r.safe_offset, 0);
        assert_eq!(r.stop, StopReason::TornHeader);
    }

    #[test]
    fn header_but_partial_frames() {
        let enc = batch0();
        let r = scan(&enc[..HEADER_LEN + 3], 0, 0);
        assert!(r.batches.is_empty());
        assert_eq!(r.stop, StopReason::IncompleteBatch);
    }

    #[test]
    fn full_frames_but_no_marker() {
        let enc = batch0();
        let r = scan(&enc[..enc.len() - MARKER_LEN], 0, 0);
        assert!(r.batches.is_empty());
        assert_eq!(r.stop, StopReason::IncompleteBatch);
    }

    #[test]
    fn torn_marker() {
        let enc = batch0();
        let r = scan(&enc[..enc.len() - 7], 0, 0);
        assert!(r.batches.is_empty());
        assert_eq!(r.stop, StopReason::IncompleteBatch);
    }

    #[test]
    fn marker_crc_echo_mismatch() {
        let mut enc = batch0();
        let n = enc.len();
        enc[n - 1] ^= 0xFF; // corrupt crc echo in the marker
        let r = scan(&enc, 0, 0);
        assert!(r.batches.is_empty());
        assert_eq!(r.stop, StopReason::BadMarker);
    }

    #[test]
    fn marker_len_echo_mismatch() {
        let mut enc = batch0();
        let n = enc.len();
        enc[n - 12] ^= 0x01; // corrupt total_len echo
        let r = scan(&enc, 0, 0);
        assert_eq!(r.stop, StopReason::BadMarker);
    }

    #[test]
    fn payload_corruption_caught_by_crc() {
        // Marker fully intact and self-consistent, but a payload byte flipped:
        // only the CRC catches this. Magic+len alone would wrongly accept.
        let mut enc = batch0();
        enc[HEADER_LEN + SUBFRAME_HDR_LEN + 2] ^= 0x40;
        let r = scan(&enc, 0, 0);
        assert!(r.batches.is_empty());
        assert_eq!(r.stop, StopReason::BadCrc);
    }

    #[test]
    fn garbage_after_valid_marker() {
        let enc = batch0();
        // long garbage tail (>= header size): bad magic
        let mut data = enc.clone();
        data.extend(std::iter::repeat(0xDBu8).take(HEADER_LEN + 10));
        let r = scan(&data, 0, 0);
        assert_eq!(r.batches.len(), 1);
        assert_eq!(r.safe_offset, enc.len() as u64);
        assert_eq!(r.stop, StopReason::BadHeaderMagic);
        assert_eq!(r.next_global_pos, 2);
        // short garbage tail (< header size): torn header
        let mut data2 = enc.clone();
        data2.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        let r2 = scan(&data2, 0, 0);
        assert_eq!(r2.batches.len(), 1);
        assert_eq!(r2.safe_offset, enc.len() as u64);
        assert_eq!(r2.stop, StopReason::TornHeader);
    }

    #[test]
    fn valid_then_torn_second_batch() {
        let enc0 = batch0();
        let enc1 = batch1();
        let mut data = enc0.clone();
        data.extend_from_slice(&enc1[..enc1.len() / 2]);
        let r = scan(&data, 0, 0);
        assert_eq!(r.batches.len(), 1);
        assert_eq!(r.batches[0].batch_id, 0);
        assert_eq!(r.safe_offset, enc0.len() as u64);
    }

    #[test]
    fn two_valid_batches() {
        let mut data = batch0();
        data.extend_from_slice(&batch1());
        let r = scan(&data, 0, 0);
        assert_eq!(r.batches.len(), 2);
        assert_eq!(r.next_global_pos, 3);
        assert_eq!(r.next_batch_id, 2);
        assert_eq!(r.safe_offset, data.len() as u64);
        assert_eq!(r.stop, StopReason::EndOfLog);
        assert_eq!(r.batches[1].events, vec![(1u32, b"charlie".to_vec())]);
    }

    #[test]
    fn stale_valid_batch_after_marker_stops_scan() {
        // A recycled-segment scenario: after batch0, the disk still holds a
        // stale but CRC-VALID batch from a previous life of this file region.
        // Marker validation alone would accept it; only the global-position
        // continuity guard rejects it. THIS IS NOT IN THE SPEC TEXT (report).
        let mut data = batch0(); // covers global pos 0..2
        data.extend_from_slice(&encode_batch(99, 40, &[b"stale".to_vec()]));
        let r = scan(&data, 0, 0);
        assert_eq!(r.batches.len(), 1);
        assert_eq!(r.stop, StopReason::PositionDiscontinuity);
    }

    #[test]
    fn version_mismatch_stops_scan() {
        let mut enc = batch0();
        enc[4] = 0xEE;
        let r = scan(&enc, 0, 0);
        assert_eq!(r.stop, StopReason::BadVersion);
    }

    #[test]
    fn insane_total_len_stops_scan() {
        let mut enc = batch0();
        enc[18..26].copy_from_slice(&(MAX_BATCH_LEN + 1).to_le_bytes());
        let r = scan(&enc, 0, 0);
        assert_eq!(r.stop, StopReason::BadLength);
        let mut enc2 = batch0();
        enc2[18..26].copy_from_slice(&5u64.to_le_bytes());
        assert_eq!(scan(&enc2, 0, 0).stop, StopReason::BadLength);
    }

    // ---- protocol-step crash points through the FaultWriter ----

    fn crash_case(crash_at: usize, crash_on_fsync: bool) -> (Log, Result<CommitInfo, Crashed>) {
        let mut log = Log::new(FaultWriter::new(Some(crash_at), crash_on_fsync));
        let res = log.append_batch(&[b"alpha".to_vec(), b"bravo-longer".to_vec()]);
        (log, res)
    }

    #[test]
    fn crash_mid_header() {
        let (log, res) = crash_case(HEADER_LEN / 2, false);
        assert_eq!(res, Err(Crashed));
        let r = scan(&log.disk.buf, 0, 0);
        assert!(r.batches.is_empty());
        assert_eq!(r.safe_offset, 0);
    }

    #[test]
    fn crash_mid_frames() {
        let (log, res) = crash_case(HEADER_LEN + 6, false);
        assert_eq!(res, Err(Crashed));
        let r = scan(&log.disk.buf, 0, 0);
        assert!(r.batches.is_empty());
    }

    #[test]
    fn crash_mid_marker() {
        let total = batch0().len();
        let (log, res) = crash_case(total - 5, false);
        assert_eq!(res, Err(Crashed));
        let r = scan(&log.disk.buf, 0, 0);
        assert!(r.batches.is_empty());
        assert_eq!(r.stop, StopReason::IncompleteBatch);
    }

    #[test]
    fn crash_after_marker_before_fsync() {
        // Batch fully written, fsync fails => NOT acknowledged. If the page
        // cache flushed anyway, recovery MAY surface it (that is allowed:
        // recovering an unacked-but-complete batch is safe). If the tail is
        // lost, recovery finds nothing. Both outcomes must be consistent.
        let total = batch0().len();
        let (log, res) = crash_case(total, true);
        assert_eq!(res, Err(Crashed));
        assert_eq!(log.disk.synced, 0);

        // tail survived the crash:
        let r = scan(&log.disk.buf, 0, 0);
        assert_eq!(r.batches.len(), 1); // complete batch, surfacing it is fine

        // tail lost (dropped back to fsync watermark):
        let r2 = scan(&log.disk.buf[..log.disk.synced], 0, 0);
        assert!(r2.batches.is_empty());
    }

    #[test]
    fn crash_after_fsync() {
        // First batch acked; crash before any further write.
        let total = batch0().len();
        let mut log = Log::new(FaultWriter::new(Some(total), false));
        let res = log.append_batch(&[b"alpha".to_vec(), b"bravo-longer".to_vec()]);
        assert!(res.is_ok());
        assert_eq!(log.disk.synced, total);
        let res2 = log.append_batch(&[b"charlie".to_vec()]);
        assert_eq!(res2, Err(Crashed));
        let r = scan(&log.disk.buf[..log.disk.synced], 0, 0);
        assert_eq!(r.batches.len(), 1);
        assert_eq!(r.next_global_pos, 2);
    }
}
