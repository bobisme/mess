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
    // bn-fyo: reject an `n` the buffer can't possibly back before sizing the
    // allocation on it. Every batch (including the first) costs >= 1 byte of
    // varints, so `n` can never legitimately exceed `block.len()` — this
    // bound is data-derived (no magic constant) and never rejects a block
    // `encode_ptr_block` actually produced. Without it, a corrupted or
    // adversarial sidecar with a huge `n` varint reaches `Vec::with_capacity`
    // directly, which aborts the process on allocation failure — violating
    // this module's own "never a panic" contract on untrusted bytes (found
    // while diagnosing the Kani harness blowup this bone fixes: an unbounded
    // `n` is also what made `decode_ptr_block`'s loop intractable for CBMC —
    // see the `kani_proofs` module doc).
    if n > block.len() {
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

    /// bn-fyo: a corrupted/adversarial `n` the buffer can't possibly back
    /// (here, `u64::MAX`) must degrade to `DecodeError::Malformed`, not reach
    /// `Vec::with_capacity` and abort the process. Regression test for the
    /// bound added in `decode_ptr_block` (see its doc comment).
    #[test]
    fn decode_rejects_absurd_batch_count_instead_of_allocating() {
        let mut block = Vec::new();
        write_varint(&mut block, u64::MAX);
        assert_eq!(decode_ptr_block(&block), Err(DecodeError::Malformed));
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

    // -----------------------------------------------------------------------
    // Randomized round-trip properties (bn-fyo).
    //
    // These replace the coverage the removed Kani round-trip harnesses were
    // meant to provide (see the `kani_proofs` module doc below for why
    // `decode_ptr_block` can't practically be driven through Kani on this
    // host): full-`u64`-domain header fields, LEB128 width-boundary deltas,
    // and multi-batch delta chains, all checked by exact equality against
    // the input rather than any closed-form model. `mess-index` deliberately
    // keeps `proptest` off its dependency list (matching `mess-store`'s
    // stated "keep deps minimal" convention — see `mess-store/tests/
    // snapshot_law.rs`), so this follows this crate's own established
    // pattern instead: a tiny seeded xorshift64* PRNG, the same one
    // `sealed::payload`'s and `columnar`'s property/fuzz-style tests use.
    // -----------------------------------------------------------------------

    /// Tiny deterministic xorshift RNG (no external rng dep; mirrors
    /// `sealed::payload::tests::Rng` and `columnar::tests`'s generator).
    struct Rng(u64);
    impl Rng {
        fn new(seed: u64) -> Self {
            Rng(seed ^ 0x9e37_79b9_7f4a_7c15)
        }
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next_u64() % n
        }
    }

    /// One-batch pointer block round-trips exactly across the **full**
    /// `u64`/`u32` domain of every header field (the property
    /// `ptr_block_single_batch_round_trips` could not establish via Kani:
    /// see the module doc below). 20_000 random headers, including the
    /// type-level extremes (`0`, `u64::MAX`/`u32::MAX`) explicitly.
    #[test]
    fn property_header_round_trips_full_domain() {
        let mut rng = Rng::new(0xC0FF_EE01);
        // Type-level extremes, explicit, before the randomized sweep.
        let extreme_headers: [BatchPtr; 4] = [
            BatchPtr { first_version: 0, frame_count: 1, first_global_pos: 0, offset: 0 },
            BatchPtr { first_version: 1, frame_count: 1, first_global_pos: 1, offset: 1 },
            BatchPtr {
                first_version: u32::MAX as u64,
                frame_count: u32::MAX,
                first_global_pos: u32::MAX as u64,
                offset: u32::MAX as u64,
            },
            BatchPtr {
                first_version: u64::MAX,
                frame_count: u32::MAX,
                first_global_pos: u64::MAX,
                offset: u64::MAX,
            },
        ];
        for b in extreme_headers {
            let batches = [b];
            let mut skips = Vec::new();
            let block = encode_ptr_block(&batches, &mut skips);
            let decoded = decode_ptr_block(&block).unwrap();
            assert_eq!(decoded, batches, "extreme header {b:?}");
        }
        for i in 0..20_000u64 {
            let first_version = rng.next_u64();
            let frame_count = 1 + (rng.next_u64() as u32);
            let offset = rng.next_u64();
            let first_global_pos = rng.next_u64();
            let batches = [BatchPtr { first_version, frame_count, first_global_pos, offset }];
            let mut skips = Vec::new();
            let block = encode_ptr_block(&batches, &mut skips);
            assert!(skips.is_empty());
            let decoded = decode_ptr_block(&block).unwrap();
            assert_eq!(decoded, batches, "header round trip failed for random step {i}");
        }
    }

    /// Two-batch pointer block round-trips exactly with the delta placed at
    /// (or near) every LEB128 width boundary up to 10 bytes — the property
    /// `ptr_block_delta_width_boundaries` could not establish via Kani (see
    /// the module doc below): the old Kani monolith's deltas were hard-capped
    /// at `1..=4` and so never encoded a multi-byte varint at all.
    #[test]
    fn property_delta_width_boundaries_round_trip() {
        let boundaries: [u64; 10] = std::array::from_fn(|i| 1u64 << (7 * i as u32));
        let mut rng = Rng::new(0xB0DE_A11E);
        for &boundary in &boundaries {
            for wobble in [-1i64, 0, 1] {
                let delta = boundary.saturating_add_signed(wobble).max(1);
                // Small bases (not full-range): `delta` alone can be up to
                // ~2^63 (the last boundary), so a full-range base plus delta
                // could overflow u64. Base-value range is already covered by
                // `property_header_round_trips_full_domain`; this test's job
                // is the delta width, not the base.
                let base_v = rng.below(1000);
                let base_o = rng.below(1000);
                let base_g = rng.below(1000);
                let b0 = BatchPtr {
                    first_version: base_v,
                    frame_count: 1,
                    first_global_pos: base_g,
                    offset: base_o,
                };
                let b1 = BatchPtr {
                    first_version: base_v + delta,
                    frame_count: 1,
                    first_global_pos: base_g + delta,
                    offset: base_o + delta,
                };
                let batches = [b0, b1];
                let mut skips = Vec::new();
                let block = encode_ptr_block(&batches, &mut skips);
                let decoded = decode_ptr_block(&block).unwrap();
                assert_eq!(decoded, batches, "delta {delta} (boundary {boundary}, wobble {wobble})");
            }
        }
    }

    /// Random 2..=8-batch monotone pointer blocks round-trip exactly and
    /// decode to a strictly-ascending sequence — the combined property
    /// `ptr_block_two_batches_round_trip` / `_three_batches_round_trip` /
    /// `_decode_preserves_strict_order` could not establish via Kani (see the
    /// module doc below), now checked over thousands of random chains
    /// instead of the old monolith's hard cap of 3 batches with deltas
    /// `1..=4`.
    #[test]
    fn property_random_batch_chains_round_trip_and_stay_ascending() {
        let mut rng = Rng::new(0x5EA1_ED51);
        for _ in 0..5_000u32 {
            let n = 2 + rng.below(7) as usize; // 2..=8 batches
            let mut batches = Vec::with_capacity(n);
            let mut v = rng.next_u64() >> 8;
            let mut o = rng.next_u64() >> 8;
            let mut g = rng.next_u64() >> 8;
            for _ in 0..n {
                let fc = 1 + (rng.below(1000) as u32);
                batches.push(BatchPtr { first_version: v, frame_count: fc, first_global_pos: g, offset: o });
                // Deltas span every LEB128 width up to ~3 bytes, never 0.
                v += 1 + rng.below(1 << 20);
                o += 1 + rng.below(1 << 20);
                g += 1 + rng.below(1 << 20);
            }
            let mut skips = Vec::new();
            let block = encode_ptr_block(&batches, &mut skips);
            let decoded = decode_ptr_block(&block).unwrap();
            assert_eq!(decoded, batches);
            for w in decoded.windows(2) {
                assert!(w[0].first_version < w[1].first_version);
                assert!(w[0].offset < w[1].offset);
                assert!(w[0].first_global_pos < w[1].first_global_pos);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Kani proofs (bn-y0b / bn-20e: ptr-delta encode/decode round-trip; bn-fyo:
// restructured after the round-trip harness blew up cbmc's SAT solver for
// 9+ hours at ~9GB RSS, then killed as an orphan).
// Bounded, pure, no I/O. Run: `cargo kani -p mess-index --harness <name>`.
//
// ## Runtime-class and `#[cfg(kani_slow)]` convention (bn-fyo)
//
// Every harness in this module states its measured runtime class in its doc
// comment ("sub-second", "~Ns", ...). A harness that cannot be bounded into
// a few minutes on this host belongs behind `#[cfg(kani_slow)]` — register
// `kani_slow` in this crate's `[lints.rust] unexpected_cfgs.check-cfg`
// alongside `kani` — and excluded from the default `cargo kani -p
// mess-index` sweep; the property it would have proved should get a
// randomized test in `mod tests` above instead, so the invariant stays
// checked by *something*. No harness below currently needs that gate — see
// why below.
//
// ## What blew up, and the two-layer diagnosis (bn-fyo)
//
// The original `ptr_block_round_trips_bounded` (removed by this commit)
// built its batch list with a `for _ in 0..n` loop pushing onto a
// `Vec<BatchPtr>`, where `n` itself was a *symbolic* `kani::any()` value
// merely constrained by `kani::assume(n <= 3)`, all under one
// `#[kani::unwind(6)]` applied to every loop in the harness at once —
// including `write_varint`/`read_varint`'s own internal shift loops nested
// inside the batch loop, `encode_ptr_block`'s `for i in 1..batches.len()`,
// and a final `while i < batches.len()` equality-check loop. CBMC does not
// use a `kani::assume` bound to shrink the unwinding proof obligation up
// front — for every nested loop it must still discharge a "no (N+1)th
// iteration is reachable" assertion, over a `Vec<BatchPtr>`/`Vec<u8>` heap
// model (dynamic allocation, per-push aliasing/bounds checks) rather than a
// stack array. That is layer one of the bug, and it is real: even
// `varint_round_trips` below — reported as "fine" — turned out to have no
// `#[kani::unwind]` at all. Run standalone with a hard `timeout 300`
// (confirmed independently while diagnosing this bone), it unwound
// `write_varint`'s loop past 1000 iterations with climbing RSS and had to be
// killed. Adding `#[kani::unwind(11)]` (see that harness's doc comment for
// the arithmetic) fixed it: verifies in ~2s.
//
// Layer one is not the whole story, though, and the rest of this comment is
// the part worth reading before adding another harness here. Fixing it
// (fixed-size `[BatchPtr; N]` stack arrays instead of a symbolic-length
// `Vec`, tight per-harness `#[kani::unwind]` bounds, narrow discretized
// value ranges instead of wide continuous ones) was necessary but not
// sufficient. A systematic ablation — building up from a fully-concrete
// 2-batch round trip and adding exactly one degree of freedom at a time —
// found the actual, deeper cause:
//
// | harness (2 batches, `#[kani::unwind(3)]`)                          | result |
// |----------------------------------------------------------------------|--------|
// | fully concrete (0 free variables)                                   | 1.7s |
// | 1 symbolic `u8` selector (2 values) driving one delta               | > 3 GB RSS, still climbing after ~10s (killed) |
// | same, but only calling `encode_ptr_block` (no decode, no compare)   | 2.2s |
// | same, decode added back, only checking `decoded.len() == 2`         | > 3 GB RSS, still climbing (killed) |
//
// `encode_ptr_block` alone tolerates symbolic input fine. The instant
// `decode_ptr_block` is added — even with a single 2-valued selector, even
// dropping the final equality check entirely — cbmc explodes. The cause is
// `decode_ptr_block`'s own `let n = read_varint(..)`; `for _ in 1..n`: `n`
// is read back out of a `Vec<u8>` that (however narrowly) depends on
// symbolic input, so CBMC cannot fold it down to the constant it always
// concretely is in these harnesses (`batches.len()`, written by `encode_ptr_
// block` as a literal) — it must treat decode's loop bound as genuinely
// unresolved and re-derive constraints on it by symbolically inverting the
// entire preceding `write_varint` call chain. That reasoning, not variable
// count or value-range width, is what makes decode-involving round-trip
// harnesses intractable here: every redesign attempted for this bone
// (discretized selectors, narrow LEB128-boundary windows, cut-down variable
// counts) still called `decode_ptr_block` on a partially-symbolic buffer and
// still blew up the same way. A harness that never calls `decode_ptr_block`
// on such a buffer (`varint_round_trips`, and encode-only checks) stays
// fast regardless of how wide its symbolic domain is.
//
// This diagnosis directly turned up a second, independent finding, fixed by
// this commit: `decode_ptr_block` had no upper bound on `n` before `Vec::
// with_capacity(n)` — a corrupted or adversarial sidecar with a huge `n`
// varint would reach that allocation directly, which aborts the process on
// failure rather than returning `DecodeError` as this module's own doc
// promises ("never a panic"). Fixed with a data-derived bound (`n <=
// block.len()`, since every batch costs >= 1 byte) — see `decode_ptr_block`
// and `decode_rejects_absurd_batch_count_instead_of_allocating` in `mod
// tests` above.
//
// ## Where the round-trip coverage lives instead
//
// Given the above, no round-trip harness that exercises `decode_ptr_block`
// on symbolic input is included here — every design tried is not merely
// slow but reliably catastrophic (multi-GB RSS growth within ~10s even for
// a single 2-valued selector), which is a stronger and different condition
// than "would finish given more unwind budget or more time," and is exactly
// the failure mode this bone exists to eliminate. The properties those
// harnesses would have proved are instead covered in `mod tests` above by
// randomized tests using this crate's existing seeded-xorshift-RNG
// convention (`sealed::payload`'s and `columnar`'s tests use the same
// pattern; `mess-index` and `mess-store` both deliberately keep `proptest`
// off the dependency list):
//
// - [`tests::property_header_round_trips_full_domain`] — one-batch header
//   fields across the full `u64`/`u32` domain (the "block header decode"
//   claim), 20k random cases plus the type-level extremes explicitly.
// - [`tests::property_delta_width_boundaries_round_trip`] — two-batch delta
//   at every LEB128 width boundary from 1 to 10 bytes (the "varint width
//   edges" claim) — coverage the old Kani monolith's `1..=4`-capped deltas
//   never reached at all.
// - [`tests::property_random_batch_chains_round_trip_and_stay_ascending`] —
//   2..=8-batch monotone chains, 5000 random cases, checking both exact
//   round-trip equality and the strict-ascending invariant `point_read`'s
//   forward scan and `BatchPtr::contains_version` depend on (the
//   "monotonicity check" claim) — strictly more batches and a far wider
//   delta range than the old monolith's `MAXB = 3` / `1..=4`.
//
// The skip-table / `point_read` machinery (a checkpoint requires >= SKIP_K
// == 64 batches) was never Kani-covered even by the original monolith
// (`MAXB = 3` never got near a checkpoint), so nothing is being demoted
// there either; it stays covered by `point_read_every_version_matches_
// linear` and `point_read_uses_every_checkpoint` in `mod tests` above,
// which check every version of a concretely-built multi-checkpoint stream
// against a linear-scan oracle.
// ---------------------------------------------------------------------------
#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// A single varint round-trips for every `u64`. Runtime class:
    /// sub-second (~2s measured). Unwind bound 11: LEB128 of a `u64` needs
    /// at most `ceil(64/7) = 10` groups, so 10 loop iterations always
    /// suffice; 11 gives CBMC one iteration of margin to prove no 11th is
    /// reachable, rather than leaving the bound unspecified (see the module
    /// doc — that omission is exactly what made this harness run away).
    #[kani::proof]
    #[kani::unwind(11)]
    fn varint_round_trips() {
        let v: u64 = kani::any();
        let mut buf = Vec::new();
        write_varint(&mut buf, v);
        let mut p = 0usize;
        let got = read_varint(&buf, &mut p);
        assert!(got == Some(v));
        assert!(p == buf.len());
    }
}

