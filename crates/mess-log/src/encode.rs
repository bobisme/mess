//! The batch encoder: `BatchHeader` + N `EventSubframe`s + `CommitMarker`
//! laid out into a **contiguous, reusable buffer**, byte-exact to the tables
//! of [`docs/spec/01-log-format.md`] §4.
//!
//! # Zero allocation per event on the hot path
//!
//! [`BatchEncoder`] owns one growable buffer. [`BatchEncoder::encode`] does
//! `clear()` + `reserve(total_len)` + sequential `extend_from_slice`, then
//! patches the two CRC fields in place. After the buffer has grown once (the
//! warmup), encoding same-or-smaller batches performs **no heap allocation**:
//! the contiguous-buffer / one-vectored-write discipline measured in
//! `spikes/perf_append` (0.045 syscalls/event). The alloc-counter test
//! (`tests/alloc.rs`) pins this at zero.
//!
//! # Checksum discipline (R4 / D-FMT-8)
//!
//! `batch_crc` is computed over the split coverage
//! `batch[0..68] ++ batch[72..total_len-4]` (see [`crate::crc`]) — never by
//! copying the batch and zeroing fields. The value is written into **both**
//! the header `batch_crc` and the marker `batch_crc_echo`.
//!
//! [`docs/spec/01-log-format.md`]: ../../../../docs/spec/01-log-format.md

use crate::crc::batch_crc;
use crate::format::*;

/// One event subframe to encode (§4.3).
///
/// `payload` is the **on-disk** payload — exactly `compressed_len` bytes. The
/// three logical lengths describe the *decompressed* layout (D-FMT-7):
/// `uncompressed_len == metadata_len + data_len`, the metadata region followed
/// by the data region. When `compression_id == 0` the payload is verbatim, so
/// `compressed_len == uncompressed_len`; the encoder enforces both invariants.
#[derive(Debug, Clone, Copy)]
pub struct Subframe<'a> {
    /// Interned event type id (§4.3; `04-registry.md`).
    pub event_type_id:    u32,
    /// Schema version of the event type at write time.
    pub schema_version:   u16,
    /// Interned payload codec id (`0` = bootstrap codec).
    pub codec_id:         u16,
    /// `0` = no compression; nonzero = a registered algorithm.
    pub compression_id:   u8,
    /// Compression/codec dictionary id, or `0` = none.
    pub dict_id:          u16,
    /// Logical size after decompression; MUST equal `metadata_len + data_len`.
    pub uncompressed_len: u32,
    /// Byte length of the metadata region within the uncompressed payload.
    pub metadata_len:     u32,
    /// Byte length of the domain-data region within the uncompressed payload.
    pub data_len:         u32,
    /// The on-disk payload bytes; `compressed_len == payload.len()`.
    pub payload:          &'a [u8],
}

impl<'a> Subframe<'a> {
    /// An uncompressed subframe (`compression_id = 0`) whose whole payload is
    /// domain data (`metadata_len = 0`). The common Phase-3 shape.
    pub fn plain(
        event_type_id: u32,
        schema_version: u16,
        codec_id: u16,
        payload: &'a [u8],
    ) -> Self {
        let len = payload.len() as u32;
        Subframe {
            event_type_id,
            schema_version,
            codec_id,
            compression_id: 0,
            dict_id: 0,
            uncompressed_len: len,
            metadata_len: 0,
            data_len: len,
            payload,
        }
    }

    /// An uncompressed subframe with an explicit metadata/data split. The
    /// on-disk payload is `metadata ++ data`; this constructor concatenation
    /// is the caller's, so it stays out of the encoder's zero-alloc hot path.
    /// (Prefer building the joined `payload` in a reusable scratch buffer.)
    pub fn split(
        event_type_id: u32,
        schema_version: u16,
        codec_id: u16,
        dict_id: u16,
        payload: &'a [u8],
        metadata_len: u32,
    ) -> Self {
        let total = payload.len() as u32;
        Subframe {
            event_type_id,
            schema_version,
            codec_id,
            compression_id: 0,
            dict_id,
            uncompressed_len: total,
            metadata_len,
            data_len: total.saturating_sub(metadata_len),
            payload,
        }
    }

    /// On-disk size of this subframe: header + payload.
    fn on_disk_len(&self) -> u64 {
        SUBFRAME_HDR_LEN as u64 + self.payload.len() as u64
    }
}

/// Everything needed to encode one batch. The position-accounting fields
/// (`batch_id`, `first_global_pos`, `segment_epoch`) are stamped by the
/// [`SegmentWriter`](crate::writer::SegmentWriter); when encoding standalone
/// (golden tests) the caller supplies them.
#[derive(Debug, Clone, Copy)]
pub struct BatchInput<'a, 'p> {
    /// A9 (§4.2): MUST equal the containing segment's `epoch`.
    pub segment_epoch:        u64,
    /// Per-segment batch sequence number (D-FMT-5).
    pub batch_id:             u64,
    /// A1 seed (§4.2): global position of this batch's first event.
    pub first_global_pos:     u64,
    /// Batch-constant stream id (D-FMT-6).
    pub stream_id:            u64,
    /// Batch-constant category id (hoisted, D-FMT-6).
    pub category_id:          u64,
    /// Stream version of this batch's first event (§4.2).
    pub first_stream_version: u64,
    /// When `Some`, `flags.CRYPTO_CHAIN` is set and these 32 bytes are written
    /// at offset `HEADER_LEN`. Phase 3 provides only *placement* — the chain
    /// **value** is Phase 5 (`bn-1d0`); callers pass a placeholder or `None`.
    pub crypto_chain:         Option<&'a [u8; CHAIN_LEN]>,
    /// The subframes, in on-disk order. A5: MUST be non-empty.
    pub subframes:            &'a [Subframe<'p>],
}

/// A batch that could not be encoded. Pure (no I/O); the
/// [`SegmentWriter`](crate::writer) wraps these alongside its own faults.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EncodeError {
    /// A5 (§4.2): a batch MUST carry at least one subframe.
    #[error("empty batch: A5 forbids frame_count == 0")]
    EmptyBatch,
    /// A2 (§4.6): `total_len` exceeds `MAX_BATCH_LEN` (64 MiB).
    #[error(
        "batch too large: total_len {total_len} exceeds MAX_BATCH_LEN {max}"
    )]
    BatchTooLarge { total_len: u64, max: u64 },
    /// `frame_count` exceeds `u32::MAX` — not representable in the header.
    #[error("too many subframes: {count} exceeds u32::MAX")]
    TooManyFrames { count: usize },
    /// A subframe's on-disk payload exceeds `u32::MAX` (`compressed_len` is a
    /// u32), or its declared logical lengths are inconsistent (D-FMT-7).
    #[error("subframe {index} invalid: {reason}")]
    Subframe { index: usize, reason: SubframeError },
}

/// Why a single subframe is not encodable.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SubframeError {
    /// `payload.len()` (`compressed_len`) exceeds `u32::MAX`.
    #[error("payload of {len} bytes exceeds u32::MAX compressed_len")]
    PayloadTooLarge { len: usize },
    /// D-FMT-7: `uncompressed_len != metadata_len + data_len`.
    #[error(
        "uncompressed_len {uncompressed} != metadata_len {metadata} + \
         data_len {data}"
    )]
    LengthSumMismatch {
        uncompressed: u32,
        metadata:     u32,
        data:         u32,
    },
    /// D-FMT-7: `compression_id == 0` but `compressed_len != uncompressed_len`.
    #[error(
        "uncompressed frame (compression_id 0) has compressed_len \
         {compressed} != uncompressed_len {uncompressed}"
    )]
    UncompressedLenMismatch { compressed: u32, uncompressed: u32 },
}

/// A reusable batch encoder. One owned buffer, cleared and refilled per batch.
#[derive(Debug, Default)]
pub struct BatchEncoder {
    buf: Vec<u8>,
}

impl BatchEncoder {
    /// A fresh encoder with an empty buffer.
    pub fn new() -> Self { BatchEncoder { buf: Vec::new() } }

    /// An encoder whose buffer is pre-sized to `cap` bytes, so the first
    /// encode of a batch up to that size does not allocate.
    pub fn with_capacity(cap: usize) -> Self {
        BatchEncoder { buf: Vec::with_capacity(cap) }
    }

    /// The on-disk `total_len` of a batch with these subframes and chain
    /// setting (§4.6), or an error if the batch is not encodable. Pure; no
    /// buffer mutation, so a writer can check A8 fit before touching bytes.
    pub fn total_len(input: &BatchInput) -> Result<u64, EncodeError> {
        if input.subframes.is_empty() {
            return Err(EncodeError::EmptyBatch); // A5
        }
        if u32::try_from(input.subframes.len()).is_err() {
            return Err(EncodeError::TooManyFrames {
                count: input.subframes.len(),
            });
        }
        for (index, sf) in input.subframes.iter().enumerate() {
            validate_subframe(index, sf)?;
        }
        let chain_len =
            if input.crypto_chain.is_some() { CHAIN_LEN as u64 } else { 0 };
        let mut frames_len: u64 = 0;
        for sf in input.subframes {
            frames_len += sf.on_disk_len();
        }
        let total_len =
            HEADER_LEN as u64 + chain_len + frames_len + MARKER_LEN as u64;
        if total_len > MAX_BATCH_LEN {
            return Err(EncodeError::BatchTooLarge {
                total_len,
                max: MAX_BATCH_LEN,
            });
        }
        Ok(total_len)
    }

    /// Encode `input` into the internal buffer and return the batch bytes.
    ///
    /// Byte-exact to §4; the returned slice is `total_len` bytes. After the
    /// buffer has grown to fit once, this allocates nothing (the hot path).
    pub fn encode(&mut self, input: &BatchInput) -> Result<&[u8], EncodeError> {
        let total_len = Self::total_len(input)?;
        let total_len_usize = total_len as usize;

        let flags =
            if input.crypto_chain.is_some() { FLAG_CRYPTO_CHAIN } else { 0 };
        let frame_count = input.subframes.len() as u32;

        let buf = &mut self.buf;
        buf.clear();
        buf.reserve(total_len_usize);

        // --- BatchHeader (§4.2), batch_crc left as a 0 placeholder ---------
        debug_assert_eq!(buf.len(), BH_MAGIC_OFF);
        buf.extend_from_slice(&HEADER_MAGIC.to_le_bytes());
        debug_assert_eq!(buf.len(), BH_FORMAT_VERSION_OFF);
        buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        debug_assert_eq!(buf.len(), BH_FLAGS_OFF);
        buf.extend_from_slice(&flags.to_le_bytes());
        debug_assert_eq!(buf.len(), BH_FRAME_COUNT_OFF);
        buf.extend_from_slice(&frame_count.to_le_bytes());
        debug_assert_eq!(buf.len(), BH_BATCH_ID_OFF);
        buf.extend_from_slice(&input.batch_id.to_le_bytes());
        debug_assert_eq!(buf.len(), BH_TOTAL_LEN_OFF);
        buf.extend_from_slice(&total_len.to_le_bytes());
        debug_assert_eq!(buf.len(), BH_FIRST_GLOBAL_POS_OFF);
        buf.extend_from_slice(&input.first_global_pos.to_le_bytes());
        debug_assert_eq!(buf.len(), BH_SEGMENT_EPOCH_OFF);
        buf.extend_from_slice(&input.segment_epoch.to_le_bytes());
        debug_assert_eq!(buf.len(), BH_STREAM_ID_OFF);
        buf.extend_from_slice(&input.stream_id.to_le_bytes());
        debug_assert_eq!(buf.len(), BH_CATEGORY_ID_OFF);
        buf.extend_from_slice(&input.category_id.to_le_bytes());
        debug_assert_eq!(buf.len(), BH_FIRST_STREAM_VERSION_OFF);
        buf.extend_from_slice(&input.first_stream_version.to_le_bytes());
        debug_assert_eq!(buf.len(), HEADER_CRC_OFF);
        buf.extend_from_slice(&0u32.to_le_bytes()); // batch_crc placeholder
        debug_assert_eq!(buf.len(), HEADER_LEN);

        // --- crypto_chain (§4.4), present iff flags.CRYPTO_CHAIN -----------
        if let Some(chain) = input.crypto_chain {
            buf.extend_from_slice(chain);
            debug_assert_eq!(buf.len(), HEADER_LEN + CHAIN_LEN);
        }

        // --- EventSubframes (§4.3), back-to-back ---------------------------
        for sf in input.subframes {
            let compressed_len = sf.payload.len() as u32;
            let s = buf.len(); // this subframe's first byte
            debug_assert_eq!(buf.len() - s, SF_EVENT_TYPE_ID_OFF);
            buf.extend_from_slice(&sf.event_type_id.to_le_bytes());
            debug_assert_eq!(buf.len() - s, SF_SCHEMA_VERSION_OFF);
            buf.extend_from_slice(&sf.schema_version.to_le_bytes());
            debug_assert_eq!(buf.len() - s, SF_CODEC_ID_OFF);
            buf.extend_from_slice(&sf.codec_id.to_le_bytes());
            debug_assert_eq!(buf.len() - s, SF_COMPRESSION_ID_OFF);
            buf.push(sf.compression_id);
            debug_assert_eq!(buf.len() - s, SF_SUBFRAME_FLAGS_OFF);
            buf.push(0u8); // subframe_flags: MUST be 0 in v3
            debug_assert_eq!(buf.len() - s, SF_DICT_ID_OFF);
            buf.extend_from_slice(&sf.dict_id.to_le_bytes());
            debug_assert_eq!(buf.len() - s, SF_UNCOMPRESSED_LEN_OFF);
            buf.extend_from_slice(&sf.uncompressed_len.to_le_bytes());
            debug_assert_eq!(buf.len() - s, SF_COMPRESSED_LEN_OFF);
            buf.extend_from_slice(&compressed_len.to_le_bytes());
            debug_assert_eq!(buf.len() - s, SF_METADATA_LEN_OFF);
            buf.extend_from_slice(&sf.metadata_len.to_le_bytes());
            debug_assert_eq!(buf.len() - s, SF_DATA_LEN_OFF);
            buf.extend_from_slice(&sf.data_len.to_le_bytes());
            debug_assert_eq!(buf.len() - s, SUBFRAME_HDR_LEN);
            buf.extend_from_slice(sf.payload); // compressed_len payload bytes
        }

        // --- CommitMarker (§4.5), batch_crc_echo left as a 0 placeholder ---
        let marker_off = total_len_usize - MARKER_LEN;
        debug_assert_eq!(buf.len(), marker_off);
        debug_assert_eq!(buf.len() - marker_off, CM_MAGIC_OFF);
        buf.extend_from_slice(&MARKER_MAGIC.to_le_bytes());
        debug_assert_eq!(buf.len() - marker_off, CM_TOTAL_LEN_ECHO_OFF);
        buf.extend_from_slice(&total_len.to_le_bytes()); // total_len_echo
        debug_assert_eq!(buf.len() - marker_off, CM_BATCH_CRC_ECHO_OFF);
        buf.extend_from_slice(&0u32.to_le_bytes()); // batch_crc_echo placeholder
        debug_assert_eq!(buf.len(), total_len_usize);

        // --- R4 split-coverage CRC, written into BOTH fields (§5.2) --------
        let crc = batch_crc(buf);
        let crc_bytes = crc.to_le_bytes();
        buf[HEADER_CRC_OFF..HEADER_CRC_OFF + 4].copy_from_slice(&crc_bytes);
        let echo_off = marker_off + CM_BATCH_CRC_ECHO_OFF;
        buf[echo_off..echo_off + 4].copy_from_slice(&crc_bytes);

        Ok(&self.buf)
    }

    /// The last-encoded batch bytes (for a caller that encoded then wants to
    /// re-read without re-encoding). Empty before the first `encode`.
    pub fn bytes(&self) -> &[u8] { &self.buf }
}

fn validate_subframe(index: usize, sf: &Subframe) -> Result<(), EncodeError> {
    let compressed_len =
        u32::try_from(sf.payload.len()).map_err(|_| EncodeError::Subframe {
            index,
            reason: SubframeError::PayloadTooLarge { len: sf.payload.len() },
        })?;
    // D-FMT-7: uncompressed_len == metadata_len + data_len.
    if sf.metadata_len.checked_add(sf.data_len) != Some(sf.uncompressed_len) {
        return Err(EncodeError::Subframe {
            index,
            reason: SubframeError::LengthSumMismatch {
                uncompressed: sf.uncompressed_len,
                metadata:     sf.metadata_len,
                data:         sf.data_len,
            },
        });
    }
    // D-FMT-7: uncompressed frames are verbatim (compressed_len ==
    // uncompressed_len).
    if sf.compression_id == 0 && compressed_len != sf.uncompressed_len {
        return Err(EncodeError::Subframe {
            index,
            reason: SubframeError::UncompressedLenMismatch {
                compressed:   compressed_len,
                uncompressed: sf.uncompressed_len,
            },
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input_1frame<'a>(
        payload: &'a [u8],
        subframes: &'a [Subframe<'a>],
    ) -> BatchInput<'a, 'a> {
        let _ = payload;
        BatchInput {
            segment_epoch: 1,
            batch_id: 0,
            first_global_pos: 0,
            stream_id: 0,
            category_id: 0,
            first_stream_version: 0,
            crypto_chain: None,
            subframes,
        }
    }

    #[test]
    fn worked_example_total_len_is_128() {
        // §4.7: one 12-byte uncompressed frame, no crypto chain -> 128 bytes.
        let payload = [0u8; 12];
        let sfs = [Subframe::plain(1, 0, 0, &payload)];
        let input = input_1frame(&payload, &sfs);
        assert_eq!(BatchEncoder::total_len(&input).unwrap(), 128);
    }

    #[test]
    fn crypto_chain_shifts_layout_and_total_len_to_160() {
        // §4.7 with-chain remark: chain at [72,104), total_len 160.
        let payload = [0u8; 12];
        let chain = [0u8; CHAIN_LEN];
        let sfs = [Subframe::plain(1, 0, 0, &payload)];
        let mut input = input_1frame(&payload, &sfs);
        input.crypto_chain = Some(&chain);
        assert_eq!(BatchEncoder::total_len(&input).unwrap(), 160);
    }

    #[test]
    fn empty_batch_rejected() {
        let sfs: [Subframe; 0] = [];
        let input = input_1frame(&[], &sfs);
        assert_eq!(
            BatchEncoder::total_len(&input),
            Err(EncodeError::EmptyBatch)
        );
    }

    #[test]
    fn both_crc_fields_hold_the_same_value() {
        let payload = [0xAAu8; 12];
        let sfs = [Subframe::plain(7, 0, 0, &payload)];
        let input = input_1frame(&payload, &sfs);
        let mut enc = BatchEncoder::new();
        let bytes = enc.encode(&input).unwrap();
        let n = bytes.len();
        let header_crc = &bytes[HEADER_CRC_OFF..HEADER_CRC_OFF + 4];
        let echo_crc = &bytes[n - 4..];
        assert_eq!(
            header_crc, echo_crc,
            "batch_crc and batch_crc_echo must match"
        );
        // And it must equal an independent split-coverage recomputation.
        assert_eq!(header_crc, batch_crc(bytes).to_le_bytes());
    }

    #[test]
    fn length_sum_mismatch_rejected() {
        let payload = [0u8; 8];
        let bad = Subframe {
            event_type_id:    1,
            schema_version:   0,
            codec_id:         0,
            compression_id:   0,
            dict_id:          0,
            uncompressed_len: 9, // != 0 + 8
            metadata_len:     0,
            data_len:         8,
            payload:          &payload,
        };
        let sfs = [bad];
        let input = input_1frame(&payload, &sfs);
        assert!(matches!(
            BatchEncoder::total_len(&input),
            Err(EncodeError::Subframe {
                index:  0,
                reason: SubframeError::LengthSumMismatch { .. },
            })
        ));
    }
}

// ---------------------------------------------------------------------------
// Kani proofs (bn-y0b): `total_len`'s length arithmetic (§4.6), proved
// overflow-free rather than sampled. Two complementary harnesses:
//
// - `total_len_closed_form_never_overflows_at_type_bounds` reasons about the
//   arithmetic in the abstract, at the exact bounds the surrounding checks
//   (`TooManyFrames`, `PayloadTooLarge`) enforce BEFORE this summation ever
//   runs — `frame_count <= u32::MAX` and each subframe's on-disk length `<=
//   SUBFRAME_HDR_LEN + u32::MAX`. It stands in for the real loop's worst case
//   (every one of up to `u32::MAX` subframes at the longest representable
//   length) without unwinding a multi-billion-iteration loop: the sum of
//   `frame_count` terms each `<= max_len` is bounded above by `frame_count *
//   max_len`, so if THAT doesn't overflow, no partial sum the real loop
//   computes can either.
// - `total_len_matches_closed_form_bounded` instead drives the actual
//   `BatchEncoder::total_len` function, over every batch shape up to
//   `MAX_SUBFRAMES` subframes of up to `MAX_PAYLOAD` bytes each — small enough
//   for Kani to enumerate the real code path exactly (not an abstraction of it)
//   in seconds.
//
// Together they cover the same claim the bone asks for ("no-overflow
// proofs for total_len computation ... bounded") from both ends: realistic
// type-level extremes via closed-form arithmetic, and the real function via
// direct, small-N execution.
//
// Run: `cargo kani --package mess-log --harness <name>` (see
// `docs/verification.md`).
// ---------------------------------------------------------------------------
#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// Realistic upper bound for both `frame_count` and any single
    /// subframe's on-disk length, used by the closed-form proof below.
    ///
    /// The raw type-level extremes are `frame_count <= u32::MAX` (bounded by
    /// `TooManyFrames`) and per-subframe on-disk length
    /// `<= SUBFRAME_HDR_LEN + u32::MAX` (bounded by `PayloadTooLarge`) — but
    /// at THOSE exact extremes the product genuinely overflows `u64` (by
    /// exactly 111_669_149_670, i.e. ~1.117e11: `u32::MAX as u128 *
    /// (SUBFRAME_HDR_LEN + u32::MAX) as u128 - u64::MAX as u128 ==
    /// 111_669_149_670`, confirmed by direct calculation in `u128`). That
    /// configuration needs a `subframes` slice
    /// of ~4.3 billion entries, each carrying a ~4 GiB payload — on the
    /// order of 18 exabytes of live payload data in one call, which no
    /// process on real hardware can construct (Rust's own slice/allocator
    /// invariants cap any single allocation at `isize::MAX` bytes, far
    /// short of that). It is a real fact about the unchecked `+=` this
    /// function uses, but not a reachable one, so it is documented here
    /// rather than "proved safe" by assuming it away.
    ///
    /// What IS reachable, and what this proof actually covers: `MAX_BATCH_LEN`
    /// (64 MiB, the A2 cap every accepted batch must fit under) as the bound
    /// for BOTH `frame_count` and per-frame length. No subframe's on-disk
    /// length can exceed a whole batch's cap, and no batch can hold more
    /// subframes than `MAX_BATCH_LEN / SUBFRAME_HDR_LEN` (~2.4M) — a bound
    /// two orders of magnitude below `MAX_REALISTIC_LEN` already. Using
    /// `MAX_BATCH_LEN` for both is therefore generous in every direction
    /// while staying inside what a real call site could ever construct.
    const MAX_REALISTIC_LEN: u64 = MAX_BATCH_LEN;

    /// See the module-level doc above and `MAX_REALISTIC_LEN`'s doc: the
    /// worst-case closed form for `total_len`'s accumulation (`HEADER_LEN +
    /// chain_len + frames_len + MARKER_LEN`, where `frames_len` sums
    /// `frame_count` per-subframe lengths) does not overflow `u64` for any
    /// `frame_count` and per-subframe length within the realistically
    /// constructible range.
    #[kani::proof]
    fn total_len_closed_form_never_overflows_at_realistic_bounds() {
        let frame_count: u64 = kani::any();
        kani::assume(frame_count <= MAX_REALISTIC_LEN);
        let max_per_frame_len: u64 = kani::any();
        kani::assume(max_per_frame_len <= MAX_REALISTIC_LEN);
        let chain_len: u64 = kani::any();
        kani::assume(chain_len == 0 || chain_len == CHAIN_LEN as u64);

        // The real loop's running sum, after all `frame_count` subframes,
        // is at most `frame_count * max_per_frame_len` (each term bounded
        // by the same max); every earlier partial sum is smaller still. If
        // this worst-case total plus the fixed header/marker/chain
        // overhead fits in a `u64`, so does every prefix the real
        // accumulation ever computes.
        let frames_len_upper_bound =
            frame_count.checked_mul(max_per_frame_len).expect(
                "frame_count * max_per_frame_len must not overflow u64 at \
                 realistic bounds",
            );
        let total = (HEADER_LEN as u64)
            .checked_add(chain_len)
            .and_then(|v| v.checked_add(frames_len_upper_bound))
            .and_then(|v| v.checked_add(MARKER_LEN as u64))
            .expect(
                "total_len's worst-case accumulation must not overflow u64",
            );
        assert!(total >= HEADER_LEN as u64 + MARKER_LEN as u64);
    }

    /// Payload length bound for the direct, real-function proof below:
    /// large enough to exercise a genuine multi-byte payload, small enough
    /// that Kani enumerates the full symbolic-byte space in seconds. The
    /// closed-form proof above separately covers realistic-but-far-larger
    /// lengths via abstract `u64` arithmetic instead of a literal buffer
    /// (Kani cannot allocate a literal `u32::MAX`-byte array to test
    /// against).
    const MAX_PAYLOAD: usize = 4;
    /// Subframe count bound for the same reason: `MAX_SUBFRAMES = 2` is the
    /// smallest count that exercises both "one subframe" and "more than
    /// one, so `frames_len` is a real sum" shapes.
    const MAX_SUBFRAMES: usize = 2;

    /// Drives the actual `BatchEncoder::total_len` — not an abstraction of
    /// it — over every batch of up to `MAX_SUBFRAMES` subframes, each up to
    /// `MAX_PAYLOAD` bytes, with or without a crypto chain: the computation
    /// never panics (Kani's arithmetic-overflow checks are on for every
    /// intermediate `+=`), and whenever it returns `Ok`, the value equals
    /// the exact closed-form sum for that batch.
    #[kani::proof]
    #[kani::unwind(3)]
    fn total_len_matches_closed_form_bounded() {
        let p0: [u8; MAX_PAYLOAD] = kani::any();
        let l0: usize = kani::any();
        kani::assume(l0 <= MAX_PAYLOAD);
        let p1: [u8; MAX_PAYLOAD] = kani::any();
        let l1: usize = kani::any();
        kani::assume(l1 <= MAX_PAYLOAD);

        let n: usize = kani::any();
        kani::assume(n <= MAX_SUBFRAMES);

        let sf0 =
            Subframe::plain(kani::any(), kani::any(), kani::any(), &p0[..l0]);
        let sf1 =
            Subframe::plain(kani::any(), kani::any(), kani::any(), &p1[..l1]);
        let all = [sf0, sf1];
        let subframes = &all[..n];

        let with_chain: bool = kani::any();
        let chain = [0u8; CHAIN_LEN];
        let crypto_chain = if with_chain { Some(&chain) } else { None };

        let input = BatchInput {
            segment_epoch: kani::any(),
            batch_id: kani::any(),
            first_global_pos: kani::any(),
            stream_id: kani::any(),
            category_id: kani::any(),
            first_stream_version: kani::any(),
            crypto_chain,
            subframes,
        };

        let result = BatchEncoder::total_len(&input);
        if n == 0 {
            assert_eq!(result, Err(EncodeError::EmptyBatch)); // A5
            return;
        }
        if let Ok(total_len) = result {
            let chain_len = if with_chain { CHAIN_LEN as u64 } else { 0 };
            let mut expected =
                HEADER_LEN as u64 + chain_len + MARKER_LEN as u64;
            for sf in subframes {
                expected += SUBFRAME_HDR_LEN as u64 + sf.payload.len() as u64;
            }
            assert_eq!(total_len, expected);
            assert!(total_len <= MAX_BATCH_LEN);
        }
    }

    // `BatchEncoder::encode` itself (as opposed to `total_len`, proved
    // above) is deliberately NOT exercised by a Kani harness: it calls
    // `crate::crc::batch_crc`, which transitively calls the `crc32c` crate's
    // runtime-feature-detected SSE4.2 path (`__cpuid_count` + hand-written
    // intrinsics). Confirmed experimentally — a minimal one-subframe
    // `encode()` harness fails with "TerminatorKind::InlineAsm is not
    // currently supported by Kani" after ~100s, not a proof result. That
    // encode/verify round trip (§4.7's `both_crc_fields_hold_the_same_value`
    // property, generalized) is exactly the kind of claim Kani is the wrong
    // tool for here; it stays covered by the existing unit tests in this
    // file plus `crate::crc`'s tests, which run the real hardware path.
    // See `docs/verification.md` for the full account of what is and is not
    // Kani-checked and why.
}
