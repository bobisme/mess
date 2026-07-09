//! Packed pointer blocks and intra-block skip tables — **this crate owns these
//! bytes** (bn-20e). The seal pass turns the write-optimized in-memory active
//! index ([`crate::active`]) into a read-optimized, on-disk representation off
//! the append hot path (D5).
//!
//! # What a pointer block is
//!
//! The active index stores, per stream, a `Vec<StreamEntry>` — one entry per
//! *committed batch* (a batch is stream-constant with consecutive versions,
//! D-FMT-6). Within a single segment a stream's batches ascend in all three of
//! `first_version`, `first_global_pos`, and `offset` (batches are appended in
//! commit order, and commit order is increasing byte offset). A **packed
//! pointer block** is that per-stream batch list, varint-delta encoded — the
//! `seal_pipeline` spike's varint-delta layout, specialized to per-batch
//! pointers (the log payload is truth, D1; Phase-5 payload rewriting slots in
//! as a later staged pass and does not touch this encoding).
//!
//! For a stream with `n` batches the block is:
//!
//! ```text
//! varint  n
//! varint  first_version[0]          (absolute)
//! varint  frame_count[0]
//! varint  offset[0]                 (absolute, within the segment)
//! varint  global[0]                 (absolute A1 position)
//! for i in 1..n:
//!   varint  first_version[i] - first_version[i-1]   (> 0, monotone)
//!   varint  frame_count[i]
//!   varint  offset[i]      - offset[i-1]            (> 0, monotone)
//!   varint  global[i]      - global[i-1]            (> 0, monotone)
//! ```
//!
//! Because a stream's batches are contiguous in the common case, the version
//! and global deltas are exactly `frame_count[i-1]` and the offset delta is one
//! batch's on-disk length — all small, so each subsequent batch costs a handful
//! of bytes. The encoding stays fully general: any monotone-increasing batch
//! sequence round-trips.
//!
//! # The skip table (perf_replay)
//!
//! A point read of `(stream, version)` must find the batch whose
//! `[first_version, last_version]` covers `version`. A naive decode scans every
//! varint from the block start — O(batch index of `version`), which the
//! `perf_replay` spike measured as the sealed-path point-read p99 (108 µs on a
//! 150k-entry stream). The fix is an **intra-block skip table**: every
//! [`SKIP_K`] batches we record an absolute checkpoint
//! ([`SkipEntry`]) — the batch's `first_version`, its `offset`, its `global`,
//! and the byte offset into the varint stream *at* that batch. A point read
//! binary-searches the checkpoints by `first_version` (they ascend), jumps to
//! the nearest checkpoint at or before the target, and decodes at most
//! `SKIP_K` batches forward. `perf_replay` measured this collapsing p99 from
//! 108 µs to 0.79 µs for +0.16 B/event.

/// Batches per skip-table checkpoint (`perf_replay`'s `SKIP_K = 64`): a point
/// read decodes at most this many batches after a checkpoint jump.
pub const SKIP_K: usize = 64;

/// A fixed-size skip-table checkpoint. Checkpoint `j` describes batch index
/// `(j + 1) * SKIP_K` of a stream's pointer block: the absolute pointer state
/// at that batch plus the byte offset to resume varint decoding from. Stored
/// little-endian, [`SKIP_ENTRY_LEN`] bytes each; checkpoints ascend by
/// `first_version`, so a point read binary-searches them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SkipEntry {
    /// `first_version` of the checkpoint batch — the binary-search key.
    pub first_version: u64,
    /// Absolute byte offset of the checkpoint batch within the segment.
    pub offset: u64,
    /// Absolute A1 global position of the checkpoint batch's first event.
    pub global: u64,
    /// Byte offset into the pointer block's varint stream at the checkpoint
    /// batch (i.e. where that batch's `first_version` delta begins).
    pub byte_off: u32,
}

/// On-disk size of a [`SkipEntry`]: `u64 + u64 + u64 + u32`.
pub const SKIP_ENTRY_LEN: usize = 8 + 8 + 8 + 4;

/// One decoded batch pointer: the read-side view of a [`crate::StreamEntry`]
/// recovered from a packed pointer block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchPtr {
    /// Stream version of the batch's first event.
    pub first_version: u64,
    /// Number of events (subframes) in the batch.
    pub frame_count: u32,
    /// Global position (A1) of the batch's first event.
    pub first_global_pos: u64,
    /// Byte offset of the batch within its segment.
    pub offset: u64,
}

impl BatchPtr {
    /// Stream version of the batch's last event.
    #[inline]
    pub fn last_version(&self) -> u64 {
        self.first_version + u64::from(self.frame_count) - 1
    }

    /// Whether `version` falls in this batch's `[first_version, last_version]`.
    #[inline]
    pub fn contains_version(&self, version: u64) -> bool {
        version >= self.first_version && version <= self.last_version()
    }
}

// ---------------------------------------------------------------------------
// Varint (LEB128, unsigned) — the seal_pipeline spike's encoding.
// ---------------------------------------------------------------------------

/// Append `v` to `out` as an unsigned LEB128 varint.
#[inline]
pub fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        if v < 0x80 {
            out.push(v as u8);
            return;
        }
        out.push((v as u8 & 0x7f) | 0x80);
        v >>= 7;
    }
}

/// Read an unsigned LEB128 varint from `d` at `*p`, advancing `*p`. Returns
/// `None` on a truncated stream or an overlong (> 10-byte) encoding rather than
/// panicking, so a corrupt sidecar degrades to "rebuild" instead of a crash.
#[inline]
pub fn read_varint(d: &[u8], p: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    let mut shift = 0u32;
    loop {
        let b = *d.get(*p)?;
        *p += 1;
        if shift >= 64 {
            return None; // overlong: more than 10 groups.
        }
        v |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some(v);
        }
        shift += 7;
    }
}

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

/// Encode one stream's batches into a packed pointer block, appending its
/// skip-table checkpoints to `skips`. `batches` MUST be non-empty and ascend
/// strictly in `first_version`, `offset`, and `first_global_pos` (the active
/// index maintains exactly this order; debug-asserted). Returns the block bytes.
pub fn encode_ptr_block(batches: &[BatchPtr], skips: &mut Vec<SkipEntry>) -> Vec<u8> {
    debug_assert!(!batches.is_empty(), "a stream with no batches has no pointer block");
    let mut out = Vec::with_capacity(batches.len() * 4 + 8);
    write_varint(&mut out, batches.len() as u64);

    let b0 = &batches[0];
    write_varint(&mut out, b0.first_version);
    write_varint(&mut out, u64::from(b0.frame_count));
    write_varint(&mut out, b0.offset);
    write_varint(&mut out, b0.first_global_pos);

    for i in 1..batches.len() {
        let prev = &batches[i - 1];
        let cur = &batches[i];
        debug_assert!(
            cur.first_version > prev.first_version
                && cur.offset > prev.offset
                && cur.first_global_pos > prev.first_global_pos,
            "pointer block requires strictly ascending batches"
        );
        // A checkpoint lands *at* batch index `i` when `i` is a positive
        // multiple of SKIP_K — recorded before this batch's deltas are written,
        // so `byte_off` points at the start of batch `i`'s varints.
        if i % SKIP_K == 0 {
            skips.push(SkipEntry {
                first_version: cur.first_version,
                offset: cur.offset,
                global: cur.first_global_pos,
                byte_off: out.len() as u32,
            });
        }
        write_varint(&mut out, cur.first_version - prev.first_version);
        write_varint(&mut out, u64::from(cur.frame_count));
        write_varint(&mut out, cur.offset - prev.offset);
        write_varint(&mut out, cur.first_global_pos - prev.first_global_pos);
    }
    out
}

/// Serialize skip checkpoints to their fixed-size little-endian form.
pub fn encode_skips(skips: &[SkipEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(skips.len() * SKIP_ENTRY_LEN);
    for s in skips {
        out.extend_from_slice(&s.first_version.to_le_bytes());
        out.extend_from_slice(&s.offset.to_le_bytes());
        out.extend_from_slice(&s.global.to_le_bytes());
        out.extend_from_slice(&s.byte_off.to_le_bytes());
    }
    out
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

/// Malformed pointer-block or skip bytes. Every variant means "the sidecar is
/// unusable; fall back to rebuilding the active index from the log" — never a
/// panic, so a corrupt or truncated sidecar degrades gracefully (D1: the log is
/// truth, the sealed index is an advisory read-optimization).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// A varint ran past the end of the buffer or was overlong.
    #[error("truncated or overlong varint in pointer block")]
    Truncated,
    /// A delta or count was inconsistent (e.g. non-monotone, or `n == 0`).
    #[error("malformed pointer block")]
    Malformed,
    /// The skip section length is not a whole number of entries.
    #[error("malformed skip table")]
    MalformedSkips,
}

#[inline]
fn read_skip_entry(d: &[u8], i: usize) -> Option<SkipEntry> {
    let base = i * SKIP_ENTRY_LEN;
    let s = d.get(base..base + SKIP_ENTRY_LEN)?;
    Some(SkipEntry {
        first_version: u64::from_le_bytes(s[0..8].try_into().unwrap()),
        offset: u64::from_le_bytes(s[8..16].try_into().unwrap()),
        global: u64::from_le_bytes(s[16..24].try_into().unwrap()),
        byte_off: u32::from_le_bytes(s[24..28].try_into().unwrap()),
    })
}

/// Decode a whole pointer block into its batch list (stream replay). Consumes
/// the full block; a partial or non-monotone decode is a [`DecodeError`].
pub fn decode_ptr_block(block: &[u8]) -> Result<Vec<BatchPtr>, DecodeError> {
    let mut p = 0usize;
    let n = read_varint(block, &mut p).ok_or(DecodeError::Truncated)? as usize;
    if n == 0 {
        return Err(DecodeError::Malformed);
    }
    let mut out = Vec::with_capacity(n);
    let first_version = read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
    let frame_count = u32::try_from(read_varint(block, &mut p).ok_or(DecodeError::Truncated)?)
        .map_err(|_| DecodeError::Malformed)?;
    let mut offset = read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
    let mut global = read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
    let mut version = first_version;
    out.push(BatchPtr { first_version: version, frame_count, first_global_pos: global, offset });

    for _ in 1..n {
        version += read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
        let fc = u32::try_from(read_varint(block, &mut p).ok_or(DecodeError::Truncated)?)
            .map_err(|_| DecodeError::Malformed)?;
        offset += read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
        global += read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
        out.push(BatchPtr {
            first_version: version,
            frame_count: fc,
            first_global_pos: global,
            offset,
        });
    }
    Ok(out)
}

/// Point read: resolve `version` to the [`BatchPtr`] that covers it, using the
/// skip table to bound the scan to O([`SKIP_K`]) varints. `n_batches` is the
/// stream's batch count (from its directory entry). Returns `None` when
/// `version` is past the stream's last batch in this segment (a legitimate
/// miss); returns `Err` only on corrupt bytes.
pub fn point_read(
    block: &[u8],
    skips: &[u8],
    n_batches: usize,
    version: u64,
) -> Result<Option<BatchPtr>, DecodeError> {
    if n_batches == 0 {
        return Ok(None);
    }
    if !skips.len().is_multiple_of(SKIP_ENTRY_LEN) {
        return Err(DecodeError::MalformedSkips);
    }
    let n_skips = skips.len() / SKIP_ENTRY_LEN;

    // Binary-search checkpoints for the last one with first_version <= version.
    // Checkpoints ascend by first_version. `chosen` is its index, or None to
    // start from the block header.
    let mut lo = 0usize;
    let mut hi = n_skips;
    let mut chosen: Option<SkipEntry> = None;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let e = read_skip_entry(skips, mid).ok_or(DecodeError::MalformedSkips)?;
        if e.first_version <= version {
            chosen = Some(e);
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    // Establish the decode cursor: either at a checkpoint or the block header.
    // `cur` holds the batch at `batch_idx`; `p` is positioned just past `cur`'s
    // varints so the forward scan reads batch `batch_idx + 1` next.
    let (mut cur, mut p, mut batch_idx) = match chosen {
        Some(e) => {
            // Checkpoint `chosen_idx` sits at batch index (chosen_idx+1)*SKIP_K.
            // Its stored fields are the ABSOLUTE first_version/offset/global at
            // that batch; the varints at `byte_off` are that batch's deltas
            // (relative to the previous batch). We take the absolutes from the
            // checkpoint and read only `frame_count` from the stream, consuming
            // the other three deltas to align `p` for the forward scan.
            let chosen_idx = lo - 1;
            let batch_idx = (chosen_idx + 1) * SKIP_K;
            let mut p = e.byte_off as usize;
            let _fv_delta = read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
            let fc = u32::try_from(read_varint(block, &mut p).ok_or(DecodeError::Truncated)?)
                .map_err(|_| DecodeError::Malformed)?;
            let _off_delta = read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
            let _gl_delta = read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
            let cur = BatchPtr {
                first_version: e.first_version,
                frame_count: fc,
                first_global_pos: e.global,
                offset: e.offset,
            };
            (cur, p, batch_idx)
        }
        None => {
            // Start from the header (batch 0).
            let mut p = 0usize;
            let _n = read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
            let fv = read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
            let fc = u32::try_from(read_varint(block, &mut p).ok_or(DecodeError::Truncated)?)
                .map_err(|_| DecodeError::Malformed)?;
            let off = read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
            let gl = read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
            let cur =
                BatchPtr { first_version: fv, frame_count: fc, first_global_pos: gl, offset: off };
            (cur, p, 0usize)
        }
    };

    // Forward scan: at most SKIP_K batches. `cur` holds the current batch.
    loop {
        if cur.contains_version(version) {
            return Ok(Some(cur));
        }
        if version < cur.first_version {
            // The requested version precedes this batch — a hole between
            // batches (only possible if the stream is non-contiguous). Miss.
            return Ok(None);
        }
        batch_idx += 1;
        if batch_idx >= n_batches {
            return Ok(None); // past the last batch.
        }
        let fv = cur.first_version + read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
        let fc = u32::try_from(read_varint(block, &mut p).ok_or(DecodeError::Truncated)?)
            .map_err(|_| DecodeError::Malformed)?;
        let off = cur.offset + read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
        let gl =
            cur.first_global_pos + read_varint(block, &mut p).ok_or(DecodeError::Truncated)?;
        cur = BatchPtr { first_version: fv, frame_count: fc, first_global_pos: gl, offset: off };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(v: u64, fc: u32, g: u64, off: u64) -> BatchPtr {
        BatchPtr { first_version: v, frame_count: fc, first_global_pos: g, offset: off }
    }

    /// Build a contiguous stream of `n` batches, each `fc` events, starting at
    /// version 0 / global `g0` / offset `off0`, batch stride `stride` bytes.
    fn contiguous(n: usize, fc: u32, g0: u64, off0: u64, stride: u64) -> Vec<BatchPtr> {
        let mut out = Vec::with_capacity(n);
        let mut v = 0u64;
        let mut g = g0;
        let mut off = off0;
        for _ in 0..n {
            out.push(batch(v, fc, g, off));
            v += u64::from(fc);
            g += u64::from(fc);
            off += stride;
        }
        out
    }

    #[test]
    fn varint_round_trips() {
        for v in [0u64, 1, 127, 128, 300, 16_384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            write_varint(&mut buf, v);
            let mut p = 0;
            assert_eq!(read_varint(&buf, &mut p), Some(v));
            assert_eq!(p, buf.len());
        }
    }

    #[test]
    fn read_varint_rejects_truncated_and_overlong() {
        // Truncated: continuation bit set but no more bytes.
        assert_eq!(read_varint(&[0x80], &mut 0), None);
        // Overlong: 11 continuation groups.
        let overlong = [0x80u8; 11];
        assert_eq!(read_varint(&overlong, &mut 0), None);
    }

    #[test]
    fn block_round_trips_and_is_delta_compact() {
        let batches = contiguous(300, 10, 1000, 4096, 320);
        let mut skips = Vec::new();
        let block = encode_ptr_block(&batches, &mut skips);
        // 300 batches, checkpoint at 64/128/192/256 -> 4 skip entries.
        assert_eq!(skips.len(), 300 / SKIP_K);
        let decoded = decode_ptr_block(&block).unwrap();
        assert_eq!(decoded, batches);
        // Contiguous stream: ~ a few bytes/batch, far under a fixed 28-byte rep.
        assert!(block.len() < batches.len() * 8, "block not compact: {}", block.len());
    }

    #[test]
    fn point_read_every_version_matches_linear() {
        let batches = contiguous(500, 7, 0, 0, 200);
        let mut skips = Vec::new();
        let block = encode_ptr_block(&batches, &mut skips);
        let skip_bytes = encode_skips(&skips);
        let last_version = batches.last().unwrap().last_version();
        for v in 0..=last_version {
            let got = point_read(&block, &skip_bytes, batches.len(), v).unwrap();
            let expect = batches.iter().copied().find(|b| b.contains_version(v));
            assert_eq!(got, expect, "version {v}");
        }
        // Past the end -> miss.
        assert_eq!(point_read(&block, &skip_bytes, batches.len(), last_version + 1).unwrap(), None);
    }

    #[test]
    fn point_read_single_batch_no_skips() {
        let batches = vec![batch(0, 3, 100, 4096)];
        let mut skips = Vec::new();
        let block = encode_ptr_block(&batches, &mut skips);
        let skip_bytes = encode_skips(&skips);
        assert!(skips.is_empty());
        assert_eq!(point_read(&block, &skip_bytes, 1, 0).unwrap(), Some(batches[0]));
        assert_eq!(point_read(&block, &skip_bytes, 1, 2).unwrap(), Some(batches[0]));
        assert_eq!(point_read(&block, &skip_bytes, 1, 3).unwrap(), None);
    }

    #[test]
    fn point_read_non_contiguous_hole_is_a_miss() {
        // Version gap between batch 0 (v0..=2) and batch 1 (v10..=12).
        let batches = vec![batch(0, 3, 0, 0), batch(10, 3, 3, 100)];
        let mut skips = Vec::new();
        let block = encode_ptr_block(&batches, &mut skips);
        let skip_bytes = encode_skips(&skips);
        assert_eq!(point_read(&block, &skip_bytes, 2, 2).unwrap(), Some(batches[0]));
        assert_eq!(point_read(&block, &skip_bytes, 2, 5).unwrap(), None); // in the hole
        assert_eq!(point_read(&block, &skip_bytes, 2, 11).unwrap(), Some(batches[1]));
    }

    #[test]
    fn decode_rejects_truncated_block() {
        let batches = contiguous(10, 4, 0, 0, 100);
        let mut skips = Vec::new();
        let block = encode_ptr_block(&batches, &mut skips);
        assert_eq!(decode_ptr_block(&block[..block.len() - 1]), Err(DecodeError::Truncated));
    }

    #[test]
    fn point_read_uses_every_checkpoint() {
        // Exactly spanning several checkpoints; probe versions inside each
        // SKIP_K-run to force different binary-search outcomes.
        let n = SKIP_K * 5 + 3;
        let batches = contiguous(n, 1, 0, 0, 16);
        let mut skips = Vec::new();
        let block = encode_ptr_block(&batches, &mut skips);
        let skip_bytes = encode_skips(&skips);
        assert_eq!(skips.len(), (n - 1) / SKIP_K);
        for v in [0u64, 1, 63, 64, 65, 127, 128, 200, (n - 1) as u64] {
            let got = point_read(&block, &skip_bytes, n, v).unwrap();
            let expect = batches.iter().copied().find(|b| b.contains_version(v));
            assert_eq!(got, expect, "version {v}");
        }
    }
}

// ---------------------------------------------------------------------------
// Kani round-trip proofs (bn-y0b: planned ptr-delta encode/decode round-trip).
// Bounded, pure, no I/O. Run: `cargo kani -p mess-index --harness <name>`.
// ---------------------------------------------------------------------------
#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// A single varint round-trips for every `u64`.
    #[kani::proof]
    fn varint_round_trips() {
        let v: u64 = kani::any();
        let mut buf = Vec::new();
        write_varint(&mut buf, v);
        let mut p = 0usize;
        let got = read_varint(&buf, &mut p);
        assert!(got == Some(v));
        assert!(p == buf.len());
    }

    /// A small monotone pointer block encode/decode round-trips exactly. Bounds
    /// keep the state space finite: up to 3 batches, small field values (so the
    /// harness stays inside Kani's unwind budget while exercising the multi-batch
    /// delta path, header path, and count varint).
    #[kani::proof]
    #[kani::unwind(6)]
    fn ptr_block_round_trips_bounded() {
        const MAXB: usize = 3;
        let n: usize = kani::any();
        kani::assume(n >= 1 && n <= MAXB);

        // Build a strictly-ascending batch list from non-negative gaps.
        let mut batches: Vec<BatchPtr> = Vec::new();
        let mut v: u64 = kani::any();
        kani::assume(v <= 8);
        let mut off: u64 = kani::any();
        kani::assume(off <= 8);
        let mut g: u64 = kani::any();
        kani::assume(g <= 8);
        for _ in 0..n {
            let fc: u32 = kani::any();
            kani::assume(fc >= 1 && fc <= 4);
            batches.push(BatchPtr {
                first_version: v,
                frame_count: fc,
                first_global_pos: g,
                offset: off,
            });
            // Strictly ascending gaps for the next batch.
            let dv: u64 = kani::any();
            kani::assume(dv >= 1 && dv <= 4);
            let doff: u64 = kani::any();
            kani::assume(doff >= 1 && doff <= 4);
            let dg: u64 = kani::any();
            kani::assume(dg >= 1 && dg <= 4);
            v += dv;
            off += doff;
            g += dg;
        }

        let mut skips = Vec::new();
        let block = encode_ptr_block(&batches, &mut skips);
        let decoded = decode_ptr_block(&block).unwrap();
        assert!(decoded.len() == batches.len());
        let mut i = 0;
        while i < batches.len() {
            assert!(decoded[i] == batches[i]);
            i += 1;
        }
    }
}
