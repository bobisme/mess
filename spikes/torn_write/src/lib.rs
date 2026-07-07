//! Spike: batch acceptance under BLOCK-WRITE REORDERING (A4 in
//! notes/mess-research/12_convergence.md D2).
//!
//! The crash_log spike validated crash-at-byte-offset (prefix truncation)
//! failures. This spike models what real disks actually do to un-fsynced
//! data: individual SECTORS persist in arbitrary order. The CommitMarker's
//! sector can hit disk BEFORE the frame sectors it vouches for. The claim
//! under test: the full-batch CRC echo in the marker is what makes
//! acceptance safe under reordering; magic + length echo alone is NOT.
//!
//! Format is copied verbatim from spikes/crash_log (little-endian, no
//! alignment padding):
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
//!
//! Throwaway code. The deliverable is REPORT.md.

use std::collections::BTreeSet;

use crc32fast::Hasher;

pub const HEADER_MAGIC: u32 = 0xBA7C_4EAD;
pub const MARKER_MAGIC: u32 = 0xC0AA_17ED;
pub const FORMAT_VERSION: u16 = 1;

pub const HEADER_LEN: usize = 4 + 2 + 4 + 8 + 8 + 8 + 4; // 38
pub const SUBFRAME_HDR_LEN: usize = 4 + 4; // 8
pub const MARKER_LEN: usize = 4 + 8 + 4; // 16
pub const MIN_BATCH_LEN: usize = HEADER_LEN + MARKER_LEN;

/// Sanity bound so a corrupted total_len cannot cause huge scans/allocations (A2).
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
// Encoding (identical to crash_log)
// ---------------------------------------------------------------------------

/// Encode one batch. `events` are raw payloads; the spike uses a fixed
/// event_type_id of 1 (the framing protocol, not the field set, is the point).
pub fn encode_batch(batch_id: u64, first_global_pos: u64, events: &[Vec<u8>]) -> Vec<u8> {
    assert!(!events.is_empty(), "empty batches forbidden (A5)");
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

    // CRC over the whole batch with both crc fields zeroed, then patch both (A3).
    let crc = crc32(&buf);
    buf[HEADER_CRC_OFF..HEADER_CRC_OFF + 4].copy_from_slice(&crc.to_le_bytes());
    let echo_off = total_len - 4;
    buf[echo_off..].copy_from_slice(&crc.to_le_bytes());
    buf
}

// ---------------------------------------------------------------------------
// ALICE-style simulated block device
// ---------------------------------------------------------------------------

/// A fixed-capacity, sector-granular block device modeling a preallocated
/// segment file (which also models recycled disk space: the constructor takes
/// an arbitrary background image — zeros, garbage, or a stale previous
/// generation of the log).
///
/// Semantics:
/// - `append` writes into an in-memory shadow image and marks every touched
///   sector PENDING. Nothing is durable yet.
/// - `fsync` is a barrier: ALL pending sectors become durable atomically
///   (durable image := shadow image), pending set cleared. `Ok` from the
///   writer's append+fsync == durability acknowledged.
/// - `crash_*` materializes a post-crash image: start from the durable image,
///   then apply an ARBITRARY SUBSET of the pending sector writes — each
///   pending sector independently persisted or not. A later sector can
///   persist while an earlier one does not: this is write reordering.
///   Optionally one sector is TORN: only a prefix of its new bytes persisted,
///   the rest keeps the previous durable content.
#[derive(Debug, Clone)]
pub struct SectorDisk {
    pub sector_size: usize,
    durable: Vec<u8>,
    shadow: Vec<u8>,
    pub write_head: usize,
    pending: BTreeSet<usize>,
}

impl SectorDisk {
    pub fn new(sector_size: usize, background: Vec<u8>) -> Self {
        assert!(sector_size.is_power_of_two());
        assert_eq!(background.len() % sector_size, 0, "capacity must be sector-aligned");
        SectorDisk {
            sector_size,
            durable: background.clone(),
            shadow: background,
            write_head: 0,
            pending: BTreeSet::new(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.shadow.len()
    }

    /// Append at the write head (the log is append-only). Marks touched
    /// sectors pending. Panics if the segment is full: segment sizing is the
    /// caller's job (A8: batches never span segments).
    pub fn append(&mut self, bytes: &[u8]) {
        let start = self.write_head;
        let end = start + bytes.len();
        assert!(end <= self.shadow.len(), "segment full");
        self.shadow[start..end].copy_from_slice(bytes);
        if !bytes.is_empty() {
            for s in start / self.sector_size..=(end - 1) / self.sector_size {
                self.pending.insert(s);
            }
        }
        self.write_head = end;
    }

    /// fsync/fdatasync barrier: everything written so far becomes durable.
    pub fn fsync(&mut self) {
        self.durable.copy_from_slice(&self.shadow);
        self.pending.clear();
    }

    pub fn pending_sectors(&self) -> Vec<usize> {
        self.pending.iter().copied().collect()
    }

    /// Deterministic crash: persist exactly `applied` (must be pending), then
    /// optionally tear one sector: `(sector, keep)` persists only the first
    /// `keep` bytes of that sector's new content; the rest keeps the previous
    /// durable content (partial sector write).
    pub fn crash_apply(&self, applied: &[usize], tear: Option<(usize, usize)>) -> Vec<u8> {
        let ss = self.sector_size;
        let mut img = self.durable.clone();
        for &s in applied {
            assert!(self.pending.contains(&s), "sector {s} is not pending");
            img[s * ss..(s + 1) * ss].copy_from_slice(&self.shadow[s * ss..(s + 1) * ss]);
        }
        if let Some((s, keep)) = tear {
            assert!(self.pending.contains(&s), "torn sector {s} is not pending");
            assert!(keep <= ss);
            img[s * ss..s * ss + keep].copy_from_slice(&self.shadow[s * ss..s * ss + keep]);
            img[s * ss + keep..(s + 1) * ss]
                .copy_from_slice(&self.durable[s * ss + keep..(s + 1) * ss]);
        }
        img
    }

    /// Randomized crash: each pending sector independently persists with
    /// probability 1/2 (arbitrary subset == reordering); with probability
    /// `tear_prob`, one of the applied sectors is additionally torn at a
    /// random byte cut.
    pub fn crash_random(&self, rng: &mut impl rand::Rng, tear_prob: f64) -> Vec<u8> {
        let applied: Vec<usize> =
            self.pending.iter().copied().filter(|_| rng.gen_bool(0.5)).collect();
        let tear = if !applied.is_empty() && rng.gen_bool(tear_prob) {
            let s = applied[rng.gen_range(0..applied.len())];
            Some((s, rng.gen_range(0..self.sector_size)))
        } else {
            None
        };
        self.crash_apply(&applied, tear)
    }
}

// ---------------------------------------------------------------------------
// Writer (same protocol as crash_log: encode + write + fsync == ack)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitInfo {
    pub batch_id: u64,
    pub first_global_pos: u64,
    pub offset: u64,
    pub len: u64,
}

pub struct Log {
    pub disk: SectorDisk,
    pub next_batch_id: u64,
    pub next_global_pos: u64,
}

impl Log {
    pub fn new(disk: SectorDisk) -> Self {
        Log { disk, next_batch_id: 0, next_global_pos: 0 }
    }

    /// Encode + write + fsync one batch. Returning == durability acknowledged.
    pub fn append_batch(&mut self, events: &[Vec<u8>]) -> CommitInfo {
        let info = self.append_batch_nosync(events);
        self.disk.fsync();
        info
    }

    /// Encode + write WITHOUT fsync: in flight, never acknowledged.
    pub fn append_batch_nosync(&mut self, events: &[Vec<u8>]) -> CommitInfo {
        let bytes = encode_batch(self.next_batch_id, self.next_global_pos, events);
        let offset = self.disk.write_head as u64;
        self.disk.append(&bytes);
        let info = CommitInfo {
            batch_id: self.next_batch_id,
            first_global_pos: self.next_global_pos,
            offset,
            len: bytes.len() as u64,
        };
        self.next_batch_id += 1;
        self.next_global_pos += events.len() as u64;
        info
    }
}

// ---------------------------------------------------------------------------
// Recovery scanner, with a validation-strength switch for the differential
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Validation {
    /// Production config: marker magic + total_len echo + CRC echo, AND the
    /// full-batch CRC verifies, AND subframes tile, AND A1 contiguity.
    Full,
    /// Weakened (the A4 differential): marker magic + total_len echo ONLY.
    /// No CRC-echo comparison, no CRC verify. Tiling and contiguity kept, so
    /// the differential isolates exactly what the CRC buys.
    Weak,
}

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
    TornHeader, // fewer bytes than a header remain
    BadHeaderMagic,
    BadVersion,
    BadLength,             // total_len out of sane bounds
    IncompleteBatch,       // total_len exceeds remaining bytes (no room for a marker)
    BadMarker,             // marker magic / total_len echo / crc echo mismatch
    BadCrc,                // marker fields echo the header but the CRC doesn't verify
    BadFrames,             // subframes don't tile total_len
    PositionDiscontinuity, // structurally valid batch whose first_global_pos != expected (A1)
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

/// Scan forward from `start`. Stops at the first failure; NO resynchronization
/// (a valid-looking batch after a hole must never be accepted — see REPORT).
/// All-or-nothing per batch.
pub fn scan(data: &[u8], start: usize, mut expect_pos: u64, v: Validation) -> Recovery {
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

        // CommitMarker validation. Full: magic + length echo + crc echo (D2).
        // Weak: magic + length echo only — the A4 differential.
        let m = total_len - MARKER_LEN;
        if rd_u32(batch, m) != MARKER_MAGIC || rd_u64(batch, m + 4) != total_len as u64 {
            break StopReason::BadMarker;
        }
        if v == Validation::Full && rd_u32(batch, m + 12) != header_crc {
            break StopReason::BadMarker;
        }

        // CRC over the whole batch with both crc fields zeroed (Full only).
        if v == Validation::Full {
            let mut tmp = batch.to_vec();
            tmp[HEADER_CRC_OFF..HEADER_CRC_OFF + 4].fill(0);
            let echo_off = total_len - 4;
            tmp[echo_off..].fill(0);
            if crc32(&tmp) != header_crc {
                break StopReason::BadCrc;
            }
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

        // A1 continuity guard (kept in BOTH modes so the differential isolates
        // the CRC, not the contiguity rule).
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
// Deterministic adversarial cases (deliverable item 3c / 4)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const SS: usize = 512;

    fn garbage_bg(sectors: usize) -> Vec<u8> {
        vec![0xDB; sectors * SS]
    }

    /// One event, 1200-byte payload => total_len 1262 => sectors 0,1,2 at 512B.
    /// Sector 0: header + subframe header + payload start. Sector 1 (512..1024):
    /// PURE payload bytes. Sector 2: payload tail + CommitMarker (1246..1262).
    fn three_sector_batch() -> (Vec<u8>, Vec<u8>) {
        let payload = vec![0xA5u8; 1200];
        let enc = encode_batch(0, 0, &[payload.clone()]);
        assert_eq!(enc.len(), 1262);
        (enc, payload)
    }

    // -- the headline A4 case ------------------------------------------------

    #[test]
    fn reorder_middle_frame_sector_missing_full_rejects_weak_accepts() {
        // Header sector and marker sector persisted; the middle frame sector
        // did NOT persist (still background garbage). This is exactly the
        // marker-before-frames reordering A4 warns about.
        let (enc, payload) = three_sector_batch();
        let mut disk = SectorDisk::new(SS, garbage_bg(4));
        disk.append(&enc); // never fsynced, never acked
        let img = disk.crash_apply(&[0, 2], None);

        // FULL validation: marker itself is pristine (magic, len echo, CRC
        // echo all match the header) — only the full-batch CRC catches this.
        let full = scan(&img, 0, 0, Validation::Full);
        assert!(full.batches.is_empty());
        assert_eq!(full.stop, StopReason::BadCrc);
        assert_eq!(full.safe_offset, 0);

        // WEAK validation (magic + length echo only): ACCEPTS a corrupt batch.
        let weak = scan(&img, 0, 0, Validation::Weak);
        assert_eq!(weak.batches.len(), 1, "weak validation must accept the corrupt batch");
        let got = &weak.batches[0].events[0].1;
        assert_ne!(got, &payload, "accepted payload differs from what was written");
        // the middle 512 bytes are raw background garbage:
        assert_eq!(&got[512 - 46..1024 - 46], &vec![0xDBu8; 512][..]);
    }

    #[test]
    fn reorder_marker_sector_only() {
        // Only the marker's sector persisted, zero frame sectors and no header
        // sector: even weak validation rejects, because the header is garbage.
        // (The dangerous variant is header+marker without frames, above.)
        let (enc, _) = three_sector_batch();
        let mut disk = SectorDisk::new(SS, garbage_bg(4));
        disk.append(&enc);
        let img = disk.crash_apply(&[2], None);
        for v in [Validation::Full, Validation::Weak] {
            let r = scan(&img, 0, 0, v);
            assert!(r.batches.is_empty());
            assert_eq!(r.stop, StopReason::BadHeaderMagic);
        }
    }

    #[test]
    fn stale_previous_generation_in_missing_sector() {
        // Recycled-space variant of A1 meets A4: the un-persisted middle
        // sector holds bytes from a PREVIOUS generation of the log. Weak
        // validation resurrects stale data inside a "new" batch.
        let mut bg = encode_batch(7, 1_000_000, &[vec![0x77u8; 1400]]); // 1462 bytes
        bg.resize(4 * SS, 0xEE);
        let (enc, payload) = three_sector_batch();
        let mut disk = SectorDisk::new(SS, bg);
        disk.append(&enc);
        let img = disk.crash_apply(&[0, 2], None);

        let full = scan(&img, 0, 0, Validation::Full);
        assert!(full.batches.is_empty());
        assert_eq!(full.stop, StopReason::BadCrc);

        let weak = scan(&img, 0, 0, Validation::Weak);
        assert_eq!(weak.batches.len(), 1);
        let got = &weak.batches[0].events[0].1;
        assert_ne!(got, &payload);
        // the resurrected middle contains the stale generation's payload bytes
        assert!(got[512..900].iter().all(|&b| b == 0x77));
    }

    #[test]
    fn torn_marker_sector() {
        // Marker sector applied but TORN: persisted only up to mid-marker.
        // Marker magic survives, the length echo does not.
        let (enc, _) = three_sector_batch();
        let mut disk = SectorDisk::new(SS, garbage_bg(4));
        disk.append(&enc);
        // marker at 1246..1262; sector 2 starts at 1024; keep 226 => bytes up
        // to 1250 persist: payload tail + marker magic, but not the echoes.
        let img = disk.crash_apply(&[0, 1], Some((2, 226)));
        for v in [Validation::Full, Validation::Weak] {
            let r = scan(&img, 0, 0, v);
            assert!(r.batches.is_empty());
            assert_eq!(r.stop, StopReason::BadMarker);
        }
        // tear before the marker magic: same verdict
        let img2 = disk.crash_apply(&[0, 1], Some((2, 100)));
        for v in [Validation::Full, Validation::Weak] {
            assert!(scan(&img2, 0, 0, v).batches.is_empty());
        }
    }

    #[test]
    fn torn_header_sector() {
        let (enc, _) = three_sector_batch();
        let mut disk = SectorDisk::new(SS, garbage_bg(4));
        disk.append(&enc);
        // header sector torn 20 bytes in: magic/version/frame_count survive,
        // total_len is garbage (0xDB..). Everything else persisted.
        let img = disk.crash_apply(&[1, 2], Some((0, 20)));
        for v in [Validation::Full, Validation::Weak] {
            let r = scan(&img, 0, 0, v);
            assert!(r.batches.is_empty(), "{v:?} accepted a torn-header batch");
        }
    }

    #[test]
    fn hole_then_fully_persisted_valid_batch_must_not_resync() {
        // Batch 1's sectors ALL persisted; batch 0's did not (reordering).
        // Batch 1 is genuinely, fully valid on disk — but the scan must stop
        // at the hole and never accept it, or ordering/prefix-consistency dies.
        let (enc0, _) = three_sector_batch(); // offsets 0..1262
        let enc1 = encode_batch(1, 1, &[vec![0x33u8; 300]]); // offsets 1262..1624
        let mut disk = SectorDisk::new(SS, garbage_bg(4));
        disk.append(&enc0);
        disk.append(&enc1);
        // persist sectors 2,3 (all of batch 1 + batch 0's marker tail), not 0,1
        let img = disk.crash_apply(&[2, 3], None);

        for v in [Validation::Full, Validation::Weak] {
            let r = scan(&img, 0, 0, v);
            assert!(r.batches.is_empty(), "{v:?} accepted past a hole");
            assert_eq!(r.safe_offset, 0);
        }
        // proof that batch 1 IS fully intact and would validate in isolation —
        // i.e. only the stop-at-first-failure rule protects us; a scanner that
        // "resyncs to the next magic" would resurrect it:
        let iso = scan(&img, 1262, 1, Validation::Full);
        assert_eq!(iso.batches.len(), 1);
        assert_eq!(iso.batches[0].batch_id, 1);
    }

    #[test]
    fn recycled_segment_stale_batch_at_coincident_position_passes_full_validation() {
        // A9 (new finding): a recycled segment still holds a stale batch from
        // a previous generation at offset 0 with first_global_pos == 0 — the
        // exact position a fresh scan expects (possible when a segment is
        // reused for the same position range after an unclean rollback, or
        // when segment files are recycled without header stamping).
        // The new write's sectors ALL fail to persist (legal under
        // reordering). FULL validation — magic + echoes + CRC + contiguity —
        // ACCEPTS the stale batch. Nothing in A1..A8 catches this; only a
        // segment epoch / generation stamp in the header would.
        let stale = encode_batch(7, 0, &[b"old secret".to_vec()]);
        let mut bg = stale.clone();
        bg.resize(2 * SS, 0xEE);
        let mut disk = SectorDisk::new(SS, bg);
        disk.append(&encode_batch(0, 0, &[b"new data".to_vec()]));
        let img = disk.crash_apply(&[], None); // zero new sectors persisted

        let full = scan(&img, 0, 0, Validation::Full);
        assert_eq!(full.batches.len(), 1, "stale batch WAS accepted by full validation");
        assert_eq!(full.batches[0].batch_id, 7);
        assert_eq!(full.batches[0].events[0].1, b"old secret".to_vec());
    }

    #[test]
    fn header_straddling_sector_boundary() {
        // Probe (4d): does BatchHeader need to fit in one sector? Batches are
        // not sector-aligned, so headers straddle boundaries routinely. Here
        // batch 1's header spans sectors 0|1 (bytes 500..538); only the first
        // 12 header bytes persist (magic+version+frame_count look valid).
        let b0 = encode_batch(0, 0, &[vec![0x11u8; 438]]); // exactly 500 bytes
        assert_eq!(b0.len(), 500);
        let b1 = encode_batch(1, 1, &[vec![0x22u8; 300]]);
        let mut disk = SectorDisk::new(SS, garbage_bg(4));
        disk.append(&b0);
        disk.fsync(); // batch 0 ACKED
        disk.append(&b1); // in flight
        let img = disk.crash_apply(&[0], None); // sector 0 persisted, sector 1 not

        for v in [Validation::Full, Validation::Weak] {
            let r = scan(&img, 0, 0, v);
            assert_eq!(r.batches.len(), 1, "{v:?}: acked batch must survive");
            assert_eq!(r.batches[0].batch_id, 0);
            assert_eq!(r.safe_offset, 500, "{v:?}: scan must stop at the torn header");
        }
    }

    #[test]
    fn acked_batches_survive_any_reordering_of_later_writes() {
        // fsync is a barrier: acked sectors are durable regardless of what
        // happens to later pending sectors, including the shared boundary
        // sector being torn.
        let mut log = Log::new(SectorDisk::new(SS, garbage_bg(8)));
        let e0 = vec![vec![1u8; 700]];
        let e1 = vec![vec![2u8; 900]];
        log.append_batch(&e0); // acked
        log.append_batch(&e1); // acked
        let i2 = log.append_batch_nosync(&vec![vec![3u8; 1300]]); // in flight

        // tear the boundary sector shared by acked batch 1 and in-flight batch 2
        let boundary = i2.offset as usize / SS;
        let pend = log.disk.pending_sectors();
        assert!(pend.contains(&boundary));
        let img = log.disk.crash_apply(&[], Some((boundary, 3)));

        let r = scan(&img, 0, 0, Validation::Full);
        assert_eq!(r.batches.len(), 2);
        assert_eq!(r.batches[0].events[0].1, e0[0]);
        assert_eq!(r.batches[1].events[0].1, e1[0]);
    }

    #[test]
    fn unacked_but_fully_persisted_batch_may_surface() {
        // All sectors of an un-fsynced batch persisted (A6): surfacing it is
        // the permitted duplicate-side outcome; it must be complete and exact.
        let mut log = Log::new(SectorDisk::new(SS, garbage_bg(8)));
        let e0 = vec![vec![9u8; 100]];
        log.append_batch(&e0);
        let e1 = vec![vec![8u8; 1000]];
        log.append_batch_nosync(&e1);
        let all = log.disk.pending_sectors();
        let img = log.disk.crash_apply(&all, None);
        let r = scan(&img, 0, 0, Validation::Full);
        assert_eq!(r.batches.len(), 2);
        assert_eq!(r.batches[1].events[0].1, e1[0]);
    }

    #[test]
    fn empty_segment_scans_clean() {
        for bg in [vec![0u8; 2 * SS], vec![0xDB; 2 * SS]] {
            let disk = SectorDisk::new(SS, bg);
            let img = disk.crash_apply(&[], None);
            let r = scan(&img, 0, 0, Validation::Full);
            assert!(r.batches.is_empty());
            assert_eq!(r.safe_offset, 0);
        }
    }
}
