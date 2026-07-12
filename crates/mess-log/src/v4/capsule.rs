//! The v4 commit-capsule encoder + physical decoder (§3, §4, §17, §18).
//!
//! A capsule is laid out contiguously, byte-exact to the resolved sketch:
//!
//! ```text
//! CapsuleHeader              96 bytes
//! [CryptoChainEntry]         32 bytes when flags.CRYPTO_CHAIN
//! ControlRecord * N          control_len bytes (TLV, §7)
//! EventSubframe * M          event_region_len bytes (v3 28-byte subframe, §15)
//! CommitMarker               32 bytes
//! ```
//!
//! # CRC discipline (§18, R4)
//!
//! [`capsule_crc`] is computed over the **split coverage** — the whole capsule
//! with exactly the two 4-byte checksum fields excluded (`capsule_crc` in the
//! header at `[80,84)` and `capsule_crc_echo` in the marker at
//! `[total_len-8, total_len-4)`), fed to the hasher as three ranges with no
//! copy-and-zero, exactly as v3's [`crate::crc::batch_crc`] does. The value is
//! written into BOTH the header field and the marker echo.
//!
//! # This module owns only bytes + structure
//!
//! [`decode_capsule`] proves a capsule is a marker-terminated, CRC-valid,
//! exactly-tiling capsule and returns its header fields plus the decoded
//! control/event regions. It does NOT enforce the protocol rules (batch_id
//! contiguity, first_global_pos contiguity, control-only invariants, registry
//! semantics) — those live in [`super::recover`], the same division of
//! responsibility v3 has between `scanner::decode_batch` and the acceptance
//! kernel.

use super::control::{ControlDecodeError, ControlRecord};
use super::format::*;
use crate::crc::crc32c_two;
use crate::encode::Subframe;
use crate::format::{SF_COMPRESSED_LEN_OFF, SUBFRAME_HDR_LEN};

/// Compute a capsule's `capsule_crc` over the §18 split coverage.
///
/// Three ranges, no copy-and-zero: `[0,80)` (header before `capsule_crc`),
/// `[84, total_len-8)` (everything after `capsule_crc` up to `capsule_crc_echo`
/// — the chain, controls, events, and the marker prefix), and
/// `[total_len-4, total_len)` (the `marker_reserved` word after the echo).
///
/// # Panics
/// Debug-asserts `capsule.len() >= MIN_CAPSULE_LEN` (the encoder never emits a
/// shorter one; both checksum fields must fit).
#[inline]
pub fn capsule_crc(capsule: &[u8]) -> u32 {
    let total_len = capsule.len();
    debug_assert!(
        total_len >= MIN_CAPSULE_LEN as usize,
        "capsule shorter than MIN_CAPSULE_LEN cannot carry both checksum \
         fields",
    );
    // A ++ B, then continue over C.
    let a = &capsule[..CAPSULE_HEADER_CRC_OFF]; // [0, 80)
    let b = &capsule[CAPSULE_HEADER_CRC_OFF + 4..total_len - 8]; // [84, tl-8)
    let c = &capsule[total_len - 4..]; // [tl-4, tl)
    let partial = crc32c_two(a, b);
    crc32c::crc32c_append(partial, c)
}

/// Everything needed to encode one capsule. The position-accounting fields are
/// stamped by the [`super::writer::CapsuleWriter`]; standalone (golden) callers
/// supply them.
#[derive(Debug, Clone)]
pub struct CapsuleInput<'a, 'p> {
    pub segment_epoch:        u64,
    pub batch_id:             u64,
    pub first_global_pos:     u64,
    /// Domain stream; MUST be 0 for a control-only capsule (§6).
    pub stream_id:            u64,
    /// Domain category; MUST be 0 for a control-only capsule (§6).
    pub category_id:          u64,
    /// Prior event count for the domain stream; MUST be 0 for control-only.
    pub first_stream_version: u64,
    /// When `Some`, sets `flags.CRYPTO_CHAIN` and writes these 32 bytes after
    /// the header (§16). Phase-3-style placement only; the chain value is a
    /// fold-cert concern.
    pub crypto_chain:         Option<&'a [u8; CAPSULE_CHAIN_LEN]>,
    /// The control records, in ordinal (apply) order (§7).
    pub controls:             &'a [ControlRecord],
    /// The event subframes, in on-disk order (§15).
    pub subframes:            &'a [Subframe<'p>],
}

/// Why a capsule could not be encoded (pure; no I/O).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CapsuleEncodeError {
    /// §6: `control_count + event_count == 0`.
    #[error("empty capsule: control_count + event_count must be >= 1")]
    Empty,
    /// §6: a control-only capsule (`event_count == 0`) must have
    /// `stream_id == 0 && category_id == 0 && first_stream_version == 0`.
    #[error(
        "control-only capsule must have zero \
         stream_id/category_id/first_stream_version"
    )]
    ControlOnlyNonzeroStream,
    /// §19: `total_len` exceeds `MAX_CAPSULE_LEN`.
    #[error("capsule too large: total_len {total_len} exceeds {max}")]
    TooLarge { total_len: u64, max: u64 },
    /// §19: `control_count` exceeds `MAX_CONTROL_COUNT`.
    #[error("too many controls: {count} exceeds {max}")]
    TooManyControls { count: usize, max: u32 },
    /// §19: the control region exceeds `MAX_CONTROL_LEN`.
    #[error("control region too large: {control_len} exceeds {max}")]
    ControlRegionTooLarge { control_len: u64, max: u32 },
    /// `event_count`/`control_count` exceeds `u32::MAX`.
    #[error("count exceeds u32::MAX")]
    CountOverflow,
    /// A subframe's declared logical lengths are inconsistent (D-FMT-7), or its
    /// payload exceeds `u32::MAX`.
    #[error("subframe {index} invalid: {reason}")]
    Subframe { index: usize, reason: crate::encode::SubframeError },
}

/// A reusable capsule encoder. One owned buffer, cleared and refilled per
/// capsule (the v3 [`crate::encode::BatchEncoder`] discipline).
#[derive(Debug, Default)]
pub struct CapsuleEncoder {
    buf: Vec<u8>,
}

impl CapsuleEncoder {
    #[must_use]
    pub fn new() -> Self { CapsuleEncoder { buf: Vec::new() } }

    /// The derived `logical_flags` for `input` (CONTROL_ONLY when no events,
    /// HAS_DEDUPE when exactly one dedupe control, REGISTRY_INTRODUCES_IDS when
    /// any registration control). The encoder always writes these so they can
    /// never disagree with content (§5: redundant flags cross-checked on read).
    fn logical_flags(input: &CapsuleInput) -> u32 {
        let mut f = 0u32;
        if input.subframes.is_empty() {
            f |= LFLAG_CONTROL_ONLY;
        }
        let dedupe_count = input
            .controls
            .iter()
            .filter(|c| matches!(c, ControlRecord::DedupeKey { .. }))
            .count();
        if dedupe_count == 1 {
            f |= LFLAG_HAS_DEDUPE;
        }
        let introduces = input.controls.iter().any(|c| {
            matches!(
                c,
                ControlRecord::StreamRegistered { .. }
                    | ControlRecord::EventTypeRegistered { .. }
                    | ControlRecord::CategoryRegistered { .. }
            )
        });
        if introduces {
            f |= LFLAG_REGISTRY_INTRODUCES_IDS;
        }
        f
    }

    /// Layout sizes for `input`, validating §6/§19, or the encode error. Pure.
    fn layout(input: &CapsuleInput) -> Result<Layout, CapsuleEncodeError> {
        let event_count = input.subframes.len();
        let control_count = input.controls.len();
        if event_count + control_count == 0 {
            return Err(CapsuleEncodeError::Empty);
        }
        if event_count == 0
            && (input.stream_id != 0
                || input.category_id != 0
                || input.first_stream_version != 0)
        {
            return Err(CapsuleEncodeError::ControlOnlyNonzeroStream);
        }
        if u32::try_from(event_count).is_err()
            || u32::try_from(control_count).is_err()
        {
            return Err(CapsuleEncodeError::CountOverflow);
        }
        if control_count > MAX_CONTROL_COUNT as usize {
            return Err(CapsuleEncodeError::TooManyControls {
                count: control_count,
                max:   MAX_CONTROL_COUNT,
            });
        }
        // Control region length.
        let mut control_len: u64 = 0;
        for c in input.controls {
            control_len += c.on_disk_len() as u64;
        }
        if control_len > MAX_CONTROL_LEN as u64 {
            return Err(CapsuleEncodeError::ControlRegionTooLarge {
                control_len,
                max: MAX_CONTROL_LEN,
            });
        }
        // Event region length.
        let mut event_region_len: u64 = 0;
        for (index, sf) in input.subframes.iter().enumerate() {
            validate_subframe(index, sf)?;
            event_region_len +=
                SUBFRAME_HDR_LEN as u64 + sf.payload.len() as u64;
        }
        let chain_len = if input.crypto_chain.is_some() {
            CAPSULE_CHAIN_LEN as u64
        } else {
            0
        };
        let total_len = CAPSULE_HEADER_LEN as u64
            + chain_len
            + control_len
            + event_region_len
            + CAPSULE_MARKER_LEN as u64;
        if total_len > MAX_CAPSULE_LEN {
            return Err(CapsuleEncodeError::TooLarge {
                total_len,
                max: MAX_CAPSULE_LEN,
            });
        }
        Ok(Layout {
            event_count: event_count as u32,
            control_count: control_count as u32,
            control_len: control_len as u32,
            event_region_len: event_region_len as u32,
            total_len,
        })
    }

    /// The on-disk `total_len` for `input`, or the encode error. Pure — a
    /// writer checks the segment-fit before touching bytes.
    pub fn total_len(input: &CapsuleInput) -> Result<u64, CapsuleEncodeError> {
        Ok(Self::layout(input)?.total_len)
    }

    /// Encode `input` into the internal buffer, returning the capsule bytes
    /// (`total_len` bytes). Byte-exact to §4/§7/§15/§17; after the buffer grows
    /// once this allocates nothing.
    pub fn encode(
        &mut self,
        input: &CapsuleInput,
    ) -> Result<&[u8], CapsuleEncodeError> {
        let layout = Self::layout(input)?;
        let total_len = layout.total_len;
        let total_len_usize = total_len as usize;

        let flags =
            if input.crypto_chain.is_some() { FLAG_CRYPTO_CHAIN } else { 0 };
        let logical_flags = Self::logical_flags(input);

        let buf = &mut self.buf;
        buf.clear();
        buf.reserve(total_len_usize);

        // --- CapsuleHeader (§4), CRC left as 0 placeholder ------------------
        put_u32(buf, CAPSULE_MAGIC);
        put_u16(buf, FORMAT_VERSION_V4);
        put_u16(buf, flags);
        put_u32(buf, layout.event_count);
        put_u32(buf, layout.control_count);
        put_u64(buf, input.batch_id);
        put_u64(buf, total_len);
        put_u64(buf, input.first_global_pos);
        put_u64(buf, input.segment_epoch);
        put_u64(buf, input.stream_id);
        put_u64(buf, input.category_id);
        put_u64(buf, input.first_stream_version);
        put_u32(buf, layout.control_len);
        put_u32(buf, layout.event_region_len);
        debug_assert_eq!(buf.len(), CAPSULE_HEADER_CRC_OFF);
        put_u32(buf, 0); // capsule_crc placeholder
        put_u32(buf, 0); // reserved_hdr (§23.2, MUST be 0)
        put_u32(buf, logical_flags);
        put_u32(buf, 0); // reserved (MUST be 0)
        debug_assert_eq!(buf.len(), CAPSULE_HEADER_LEN);

        // --- crypto chain (§16), if present --------------------------------
        if let Some(chain) = input.crypto_chain {
            buf.extend_from_slice(chain);
        }

        // --- control region (§7), in ordinal order -------------------------
        let control_start = buf.len();
        for c in input.controls {
            c.encode_into(buf);
        }
        debug_assert_eq!(
            buf.len() - control_start,
            layout.control_len as usize
        );

        // --- event region (§15), v3 28-byte subframes ----------------------
        for sf in input.subframes {
            let compressed_len = sf.payload.len() as u32;
            put_u32(buf, sf.event_type_id);
            put_u16(buf, sf.schema_version);
            put_u16(buf, sf.codec_id);
            buf.push(sf.compression_id);
            buf.push(0u8); // subframe_flags: MUST be 0
            put_u16(buf, sf.dict_id);
            put_u32(buf, sf.uncompressed_len);
            put_u32(buf, compressed_len);
            put_u32(buf, sf.metadata_len);
            put_u32(buf, sf.data_len);
            buf.extend_from_slice(sf.payload);
        }

        // --- CommitMarker (§17), crc echo left as 0 placeholder ------------
        let marker_off = total_len_usize - CAPSULE_MARKER_LEN;
        debug_assert_eq!(buf.len(), marker_off);
        put_u32(buf, CAPSULE_MARKER_MAGIC);
        put_u32(buf, 0); // marker_flags reserved
        put_u64(buf, input.batch_id); // batch_id_echo
        put_u64(buf, total_len); // total_len_echo
        put_u32(buf, 0); // capsule_crc_echo placeholder
        put_u32(buf, 0); // marker_reserved
        debug_assert_eq!(buf.len(), total_len_usize);

        // --- split-coverage CRC into BOTH fields (§18) ---------------------
        let crc = capsule_crc(buf);
        let crc_bytes = crc.to_le_bytes();
        buf[CAPSULE_HEADER_CRC_OFF..CAPSULE_HEADER_CRC_OFF + 4]
            .copy_from_slice(&crc_bytes);
        let echo_off = marker_off + CM_CAPSULE_CRC_ECHO_OFF;
        buf[echo_off..echo_off + 4].copy_from_slice(&crc_bytes);

        Ok(&self.buf)
    }

    /// The last-encoded capsule bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] { &self.buf }
}

struct Layout {
    event_count:      u32,
    control_count:    u32,
    control_len:      u32,
    event_region_len: u32,
    total_len:        u64,
}

fn validate_subframe(
    index: usize,
    sf: &Subframe,
) -> Result<(), CapsuleEncodeError> {
    let compressed_len = u32::try_from(sf.payload.len()).map_err(|_| {
        CapsuleEncodeError::Subframe {
            index,
            reason: crate::encode::SubframeError::PayloadTooLarge {
                len: sf.payload.len(),
            },
        }
    })?;
    if sf.metadata_len.checked_add(sf.data_len) != Some(sf.uncompressed_len) {
        return Err(CapsuleEncodeError::Subframe {
            index,
            reason: crate::encode::SubframeError::LengthSumMismatch {
                uncompressed: sf.uncompressed_len,
                metadata:     sf.metadata_len,
                data:         sf.data_len,
            },
        });
    }
    if sf.compression_id == 0 && compressed_len != sf.uncompressed_len {
        return Err(CapsuleEncodeError::Subframe {
            index,
            reason: crate::encode::SubframeError::UncompressedLenMismatch {
                compressed:   compressed_len,
                uncompressed: sf.uncompressed_len,
            },
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Physical decode
// ---------------------------------------------------------------------------

/// The physically-validated header fields of a capsule (§4). All protocol
/// checks (contiguity, control-only invariants, registry semantics) are the
/// caller's ([`super::recover`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapsuleHeader {
    pub flags:                u16,
    pub logical_flags:        u32,
    pub event_count:          u32,
    pub control_count:        u32,
    pub batch_id:             u64,
    pub total_len:            u64,
    pub first_global_pos:     u64,
    pub segment_epoch:        u64,
    pub stream_id:            u64,
    pub category_id:          u64,
    pub first_stream_version: u64,
    pub control_len:          u32,
    pub event_region_len:     u32,
    pub has_crypto_chain:     bool,
}

/// A fully physically-decoded capsule: header + decoded controls + decoded
/// event subframe descriptors (payloads borrow from the image).
#[derive(Debug, Clone)]
pub struct DecodedCapsule<'a> {
    pub header:   CapsuleHeader,
    pub controls: Vec<ControlRecord>,
    pub events:   Vec<DecodedEvent<'a>>,
}

/// One decoded event subframe (§15). Payload borrows the image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedEvent<'a> {
    pub event_type_id:  u32,
    pub schema_version: u16,
    pub codec_id:       u16,
    pub payload:        &'a [u8],
}

/// Why a capsule failed physical decode (§4, §17, §18). A superset detail; the
/// recovery scanner maps each to its typed stop.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CapsuleDecodeError {
    /// Fewer than `CAPSULE_HEADER_LEN` bytes remain (torn header, A11).
    #[error("torn header: {got} bytes remain, need {} ", CAPSULE_HEADER_LEN)]
    TornHeader { got: usize },
    /// `magic != CAPSULE_MAGIC`.
    #[error("bad capsule magic")]
    BadMagic,
    /// `format_version != 4`.
    #[error("bad capsule version {0}")]
    BadVersion(u16),
    /// A physical `flags` bit outside `FLAGS_KNOWN_MASK` is set (§5).
    #[error("unknown flags {0:#06x}")]
    UnknownFlags(u16),
    /// A `logical_flags` bit outside its known mask is set (§5).
    #[error("unknown logical_flags {0:#010x}")]
    UnknownLogicalFlags(u32),
    /// A reserved header word (`reserved_hdr`, `reserved`) is nonzero (§4).
    #[error("reserved header word nonzero")]
    ReservedNonzero,
    /// §19: `total_len` outside `[MIN_CAPSULE_LEN, MAX_CAPSULE_LEN]`.
    #[error("bad total_len {0}")]
    BadLength(u64),
    /// `control_count`/`control_len` exceed their §19 caps.
    #[error("control caps exceeded")]
    BadControlCaps,
    /// `total_len` exceeds the bytes remaining.
    #[error("incomplete: total_len {total_len} > {remaining} remaining")]
    Incomplete { total_len: u64, remaining: u64 },
    /// §17: marker magic / batch_id echo / total_len echo / crc echo mismatch.
    #[error("bad marker")]
    BadMarker,
    /// §18: the full-capsule CRC over the split coverage does not verify.
    #[error("bad crc")]
    BadCrc,
    /// The declared region lengths do not sum to `total_len` (with the fixed
    /// header/marker/chain overhead) — the regions do not tile exactly.
    #[error("region lengths do not tile total_len")]
    BadRegionTiling,
    /// A control record's bytes are malformed or its `(kind,version)` unknown.
    #[error("control decode: {0}")]
    Control(#[from] ControlDecodeError),
    /// The controls do not tile `control_len` exactly.
    #[error("controls do not tile control_len")]
    ControlTilingMismatch,
    /// The event subframes do not tile `event_region_len` exactly.
    #[error("events do not tile event_region_len")]
    EventTilingMismatch,
    /// §6 redundant `CONTROL_ONLY` flag disagrees with `event_count`.
    #[error("CONTROL_ONLY flag disagrees with event_count")]
    ControlOnlyFlagMismatch,
}

/// Physically validate the capsule at `off` in `img` **without materializing**
/// its controls or events — the allocation-free fast path the recovery scanner
/// runs for every accept decision (§4 steps: magic, version, flags, caps,
/// marker echoes, the **mandatory** split-coverage CRC, and exact control +
/// event *framing* tiling). The CRC is ALWAYS computed — no path skips it.
///
/// Returns just the [`CapsuleHeader`] (a `Copy` struct): no `Vec` is allocated,
/// so a no-control capsule scans as cheaply as a v3 batch. Control payloads and
/// event descriptors are materialized separately, on demand, by
/// [`decode_controls`] / [`decode_events`] / [`decode_capsule`] (which the
/// registry seam and materializing readers call). The control-*framing* walk
/// here proves the TLV records tile `control_len` exactly; an unknown control
/// `(kind, version)` is caught when the controls are actually decoded (the
/// registry seam always decodes a capsule's controls before accepting it).
///
/// Precondition: `off < img.len()` (the scan loop upholds it).
pub fn validate_capsule(
    img: &[u8],
    off: usize,
) -> Result<CapsuleHeader, CapsuleDecodeError> {
    let rem = img.len() - off;
    if rem < CAPSULE_HEADER_LEN {
        return Err(CapsuleDecodeError::TornHeader { got: rem });
    }
    let h = &img[off..];
    if rd_u32(h, CH_MAGIC_OFF) != CAPSULE_MAGIC {
        return Err(CapsuleDecodeError::BadMagic);
    }
    let version = rd_u16(h, CH_FORMAT_VERSION_OFF);
    if version != FORMAT_VERSION_V4 {
        return Err(CapsuleDecodeError::BadVersion(version));
    }
    let flags = rd_u16(h, CH_FLAGS_OFF);
    if flags & !FLAGS_KNOWN_MASK != 0 {
        return Err(CapsuleDecodeError::UnknownFlags(flags));
    }
    let logical_flags = rd_u32(h, CH_LOGICAL_FLAGS_OFF);
    if logical_flags & !LOGICAL_FLAGS_KNOWN_MASK != 0 {
        return Err(CapsuleDecodeError::UnknownLogicalFlags(logical_flags));
    }
    // §4 reserved words MUST be zero.
    if rd_u32(h, CH_RESERVED_HDR_OFF) != 0 || rd_u32(h, CH_RESERVED_OFF) != 0 {
        return Err(CapsuleDecodeError::ReservedNonzero);
    }
    let has_crypto_chain = flags & FLAG_CRYPTO_CHAIN != 0;

    let event_count = rd_u32(h, CH_EVENT_COUNT_OFF);
    let control_count = rd_u32(h, CH_CONTROL_COUNT_OFF);
    let batch_id = rd_u64(h, CH_BATCH_ID_OFF);
    let total_len = rd_u64(h, CH_TOTAL_LEN_OFF);
    let first_global_pos = rd_u64(h, CH_FIRST_GLOBAL_POS_OFF);
    let segment_epoch = rd_u64(h, CH_SEGMENT_EPOCH_OFF);
    let stream_id = rd_u64(h, CH_STREAM_ID_OFF);
    let category_id = rd_u64(h, CH_CATEGORY_ID_OFF);
    let first_stream_version = rd_u64(h, CH_FIRST_STREAM_VERSION_OFF);
    let control_len = rd_u32(h, CH_CONTROL_LEN_OFF);
    let event_region_len = rd_u32(h, CH_EVENT_REGION_LEN_OFF);
    let header_crc = rd_u32(h, CAPSULE_HEADER_CRC_OFF);

    // A2: length cap FIRST, so a corrupt total_len cannot drive a wild read.
    if !(MIN_CAPSULE_LEN..=MAX_CAPSULE_LEN).contains(&total_len) {
        return Err(CapsuleDecodeError::BadLength(total_len));
    }
    if total_len > rem as u64 {
        return Err(CapsuleDecodeError::Incomplete {
            total_len,
            remaining: rem as u64,
        });
    }
    // Control caps before any control slicing.
    if control_count > MAX_CONTROL_COUNT || control_len > MAX_CONTROL_LEN {
        return Err(CapsuleDecodeError::BadControlCaps);
    }
    let total_len_usize = total_len as usize;
    let capsule = &img[off..off + total_len_usize];

    // §6: the redundant CONTROL_ONLY flag must agree with event_count. (A hint,
    // never a substitute — but a disagreement is corruption.)
    let control_only_flag = logical_flags & LFLAG_CONTROL_ONLY != 0;
    if control_only_flag != (event_count == 0) {
        return Err(CapsuleDecodeError::ControlOnlyFlagMismatch);
    }

    // §17: marker magic + all echoes.
    let m = total_len_usize - CAPSULE_MARKER_LEN;
    if rd_u32(capsule, m + CM_MAGIC_OFF) != CAPSULE_MARKER_MAGIC
        || rd_u64(capsule, m + CM_BATCH_ID_ECHO_OFF) != batch_id
        || rd_u64(capsule, m + CM_TOTAL_LEN_ECHO_OFF) != total_len
        || rd_u32(capsule, m + CM_CAPSULE_CRC_ECHO_OFF) != header_crc
    {
        return Err(CapsuleDecodeError::BadMarker);
    }

    // §18: the mandatory full-capsule CRC over the split coverage. The one
    // check that catches a marker that persisted before its regions did.
    if capsule_crc(capsule) != header_crc {
        return Err(CapsuleDecodeError::BadCrc);
    }

    // Region tiling: header(+chain) + control_len + event_region_len + marker
    // must equal total_len exactly.
    let chain_len = if has_crypto_chain { CAPSULE_CHAIN_LEN } else { 0 };
    let regions_span = (CAPSULE_HEADER_LEN + chain_len)
        .checked_add(control_len as usize)
        .and_then(|v| v.checked_add(event_region_len as usize))
        .and_then(|v| v.checked_add(CAPSULE_MARKER_LEN));
    if regions_span != Some(total_len_usize) {
        return Err(CapsuleDecodeError::BadRegionTiling);
    }

    // --- control-region FRAMING tiling (no allocation) ---------------------
    let control_start = CAPSULE_HEADER_LEN + chain_len;
    let control_end = control_start + control_len as usize;
    let control_region = &capsule[control_start..control_end];
    let mut cpos = 0usize;
    for _ in 0..control_count {
        if cpos + CONTROL_TLV_HDR_LEN > control_region.len() {
            return Err(CapsuleDecodeError::ControlTilingMismatch);
        }
        let plen = rd_u32(control_region, cpos + CT_PAYLOAD_LEN_OFF) as usize;
        let next = cpos
            .checked_add(CONTROL_TLV_HDR_LEN)
            .and_then(|v| v.checked_add(plen))
            .ok_or(CapsuleDecodeError::ControlTilingMismatch)?;
        if next > control_region.len() {
            return Err(CapsuleDecodeError::ControlTilingMismatch);
        }
        cpos = next;
    }
    if cpos != control_region.len() {
        return Err(CapsuleDecodeError::ControlTilingMismatch);
    }

    // --- event-region FRAMING tiling (no allocation) -----------------------
    let event_start = control_end;
    let event_end = event_start + event_region_len as usize;
    let event_region = &capsule[event_start..event_end];
    let mut epos = 0usize;
    for _ in 0..event_count {
        if epos + SUBFRAME_HDR_LEN > event_region.len() {
            return Err(CapsuleDecodeError::EventTilingMismatch);
        }
        let compressed_len =
            rd_u32(event_region, epos + SF_COMPRESSED_LEN_OFF) as usize;
        let next = epos
            .checked_add(SUBFRAME_HDR_LEN)
            .and_then(|v| v.checked_add(compressed_len))
            .ok_or(CapsuleDecodeError::EventTilingMismatch)?;
        if next > event_region.len() {
            return Err(CapsuleDecodeError::EventTilingMismatch);
        }
        epos = next;
    }
    if epos != event_region.len() {
        return Err(CapsuleDecodeError::EventTilingMismatch);
    }

    Ok(CapsuleHeader {
        flags,
        logical_flags,
        event_count,
        control_count,
        batch_id,
        total_len,
        first_global_pos,
        segment_epoch,
        stream_id,
        category_id,
        first_stream_version,
        control_len,
        event_region_len,
        has_crypto_chain,
    })
}

/// The byte range of the control region within a `capsule` slice
/// (`&img[off..off+total_len]`), given its validated header.
fn control_region_range(header: &CapsuleHeader) -> std::ops::Range<usize> {
    let chain = if header.has_crypto_chain { CAPSULE_CHAIN_LEN } else { 0 };
    let start = CAPSULE_HEADER_LEN + chain;
    start..start + header.control_len as usize
}

/// The byte range of the event region within a `capsule` slice.
fn event_region_range(header: &CapsuleHeader) -> std::ops::Range<usize> {
    let c = control_region_range(header);
    c.end..c.end + header.event_region_len as usize
}

/// Materialize a capsule's control records from a validated `capsule` slice.
/// Catches an unknown control `(kind, version)` (the framing was already proven
/// to tile by [`validate_capsule`]). Allocates — call only when the controls
/// are actually needed (the registry seam, materializing readers).
pub fn decode_controls(
    capsule: &[u8],
    header: &CapsuleHeader,
) -> Result<Vec<ControlRecord>, CapsuleDecodeError> {
    let region = &capsule[control_region_range(header)];
    let mut controls = Vec::with_capacity(header.control_count as usize);
    let mut cpos = 0usize;
    for _ in 0..header.control_count {
        let (rec, consumed) = ControlRecord::decode_at(region, cpos)?;
        controls.push(rec);
        cpos += consumed;
    }
    Ok(controls)
}

/// Materialize a capsule's event subframe descriptors from a validated
/// `capsule` slice. Infallible on a slice [`validate_capsule`] accepted.
pub fn decode_events<'a>(
    capsule: &'a [u8],
    header: &CapsuleHeader,
) -> Vec<DecodedEvent<'a>> {
    let region = &capsule[event_region_range(header)];
    let mut events = Vec::with_capacity(header.event_count as usize);
    let mut epos = 0usize;
    for _ in 0..header.event_count {
        let compressed_len =
            rd_u32(region, epos + SF_COMPRESSED_LEN_OFF) as usize;
        let payload_start = epos + SUBFRAME_HDR_LEN;
        let payload_end = payload_start + compressed_len;
        events.push(DecodedEvent {
            event_type_id:  rd_u32(region, epos),
            schema_version: rd_u16(region, epos + 4),
            codec_id:       rd_u16(region, epos + 6),
            payload:        &region[payload_start..payload_end],
        });
        epos = payload_end;
    }
    events
}

/// Visit each event subframe's `event_type_id` in order, **without allocating**
/// — the registry seam's event-resolution walk (`validate_capsule` proved the
/// framing tiles, so every read is in bounds). Returns `false` (short-circuit)
/// as soon as `f` returns `false`.
pub fn for_each_event_type_id(
    capsule: &[u8],
    header: &CapsuleHeader,
    mut f: impl FnMut(u32) -> bool,
) -> bool {
    let region = &capsule[event_region_range(header)];
    let mut epos = 0usize;
    for _ in 0..header.event_count {
        if !f(rd_u32(region, epos)) {
            return false;
        }
        let compressed_len =
            rd_u32(region, epos + SF_COMPRESSED_LEN_OFF) as usize;
        epos += SUBFRAME_HDR_LEN + compressed_len;
    }
    true
}

/// Decode + physically validate the capsule at `off`, **materializing** its
/// controls and events (allocates). Composes [`validate_capsule`] with
/// [`decode_controls`]/[`decode_events`]; use it when the full decoded content
/// is wanted (materializing readers, the D4 engine shim, fuzz round-trips). The
/// recovery scanner uses the cheaper [`validate_capsule`] on its hot path.
///
/// Precondition: `off < img.len()`.
pub fn decode_capsule(
    img: &[u8],
    off: usize,
) -> Result<DecodedCapsule<'_>, CapsuleDecodeError> {
    let header = validate_capsule(img, off)?;
    let capsule = &img[off..off + header.total_len as usize];
    let controls = decode_controls(capsule, &header)?;
    let events = decode_events(capsule, &header);
    Ok(DecodedCapsule { header, controls, events })
}

#[inline]
fn put_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}
#[inline]
fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
#[inline]
fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
#[inline]
fn rd_u16(d: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(d[o..o + 2].try_into().unwrap())
}
#[inline]
fn rd_u32(d: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(d[o..o + 4].try_into().unwrap())
}
#[inline]
fn rd_u64(d: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(d[o..o + 8].try_into().unwrap())
}
