//! Seal-time per-segment **registry delta** (bn-26pp): the `$registry` records
//! a sealed segment contains, copied into a sibling `.reg` file at seal so
//! engine open can fold them **sequentially** instead of chasing one random
//! `pread` per registration.
//!
//! # Why this file exists
//!
//! `bn-2di` made the log's `$registry` stream the sole authority for the
//! `id ↔ name` bijection, and recovery rebuilds it by folding every `$registry`
//! record. For a *sidecar-trusted* sealed segment (Spike C: its bytes are never
//! read at open) the engine locates those records through the pointer sidecar's
//! per-stream index and point-reads them — one `pread` per registration batch.
//!
//! That is cheap when a store has a few thousand streams and brutal when it has
//! a few hundred thousand: bn-2u01 measured a cold open of an 8 GiB /
//! 250k-stream store at 10.5 s, of which **89.9%** was 250,000 cold random
//! `pread`s at ~37 µs each. The reads are tiny, scattered across the whole log,
//! and there are `O(#names)` of them.
//!
//! The bytes those `pread`s return are already in the sealer's hands: the seal
//! re-reads the rolled segment to build the pointer and payload sidecars, so
//! the `$registry` payloads pass through `SealInput::payloads` on the way.
//! Writing them out once, in one contiguous file per segment, turns the
//! open-time cost from `O(#names)` random reads into `O(#segments)` sequential
//! ones.
//!
//! # This is an accelerator, never an authority (D1 / I5)
//!
//! The `.reg` is derived data in the exact mould of `.pcol` / `.pidx` /
//! `.filter`: **absent, truncated, corrupt, stale, or from a future version, it
//! is silently dropped and open falls back to the `pread` path** — same
//! records, same fold, same state, just slower. It is never an error and it can
//! never produce a different registry, because:
//!
//! 1. the file is `crc32c`-covered end to end, so accidental damage is caught
//!    before a single byte is handed to the fold;
//! 2. [`SealedSegmentIndex::accepts_registry_delta`] refuses a delta whose
//!    `segment_id` / `base_pos` / `event_count` disagree with the pointer
//!    sidecar it would ride on; and
//! 3. it further refuses one whose `(first_global_pos, frame_count)` list is
//!    not **exactly** the list the pointer sidecar's own directory gives for
//!    that stream. The pointer sidecar is CRC-validated and (at open)
//!    cross-checked against the segment footer, so the batch layout the fold
//!    consumes is pinned by the same artifact that pins every other pointer in
//!    the segment. A delta can therefore only ever supply the *payload bytes*
//!    of batches the sidecar already agrees exist, at the positions it already
//!    agrees they occupy.
//!
//! What is deliberately **not** verified at open is that those payload bytes
//! equal the log's — that would mean reading the log, which is the cost being
//! removed. The bytes are copied from the durable segment image at seal time
//! (the same image the `.pcol`'s verify-on-seal byte-compares against), so the
//! same trust boundary that already applies to every other sealed sidecar
//! applies here.
//!
//! [`SealedSegmentIndex::accepts_registry_delta`]:
//!     crate::sealed::segment::SealedSegmentIndex::accepts_registry_delta
//!
//! # Read once, never retained
//!
//! Unlike the `.filter` (consulted on every read) and the `.pcol` (the sealed
//! payload source), a delta has exactly one consumer: recovery's `$registry`
//! fold. It is therefore opened during recovery, drained into the fold, and
//! dropped — it is *not* attached to the [`SealedSegmentIndex`], and the sealer
//! does not keep the copy it just wrote either. That matters precisely where
//! this format matters: at 250k streams the deltas are 12 MiB, at 10M they
//! would be ~0.5 GiB, and holding them resident for the life of the process
//! would trade a cold-open win for a permanent RSS loss. It also means a
//! segment recovery never reaches (because the scan path covers it) costs no
//! `.reg` read at all.
//!
//! [`SealedSegmentIndex`]: crate::sealed::segment::SealedSegmentIndex
//!
//! # One delta per segment, in whichever container sealed it (bn-3h64)
//!
//! A segment is sealed either as a legacy `.pidx` trio or as a consolidated
//! [`SealPack`](crate::sealed::pack), and its delta rides in whichever one that
//! was: a sibling `.reg` file for the former, the pack's `REGISTRY_DELTA`
//! section for the latter — **never both**. The bytes are identical either way
//! ([`encode_registry_delta`] is the only producer and
//! [`RegistryDelta::from_bytes`] the only parser), so recovery folds the same
//! records from the same image regardless of which container it found; only the
//! `open`-vs-`pread`-a-section step differs. That is deliberate: forking a
//! "pack-native" layout would have bought nothing (the section's body needs its
//! own `crc32c` regardless, because the pack directory's checksums cannot
//! travel with a body that is also a standalone file format) and would have
//! doubled the corruption-test surface for one accelerator.
//!
//! Everything below about trust, fallback, and non-retention applies verbatim
//! to the section; see
//! [`SealedSegmentIndex::read_registry_delta`](crate::sealed::segment::SealedSegmentIndex::read_registry_delta)
//! for how the layout cross-check adapts when the sidecar that vouches for the
//! delta and the container that carries it are the same file.
//!
//! # Why a sibling file rather than a `.pidx` section
//!
//! Same argument [`crate::sealed::filter`] makes one level down: the `.pidx`
//! header/footer commit fixed-width offsets validated by an exact `content_crc`
//! and an exact `footer_start - dir_off == dir_len` invariant, so splicing a
//! variable-length region in means a `format_version` bump for every reader of
//! every existing store. A sibling file is independently optional,
//! independently rebuildable, written with the same crash-atomic
//! temp → fsync → rename → dir-fsync discipline, and costs a pre-existing store
//! nothing: it simply has no `.reg` and keeps the old path.
//!
//! None of that applies to the consolidated [`SealPack`](crate::sealed::pack),
//! which is versioned as a whole and whose section directory is variable-length
//! by construction — so there the delta simply *is* a section
//! (`KIND_REGISTRY_DELTA`), added without disturbing a single existing reader
//! (unknown kinds are skipped; a pack without the section reads exactly as
//! before). See "One delta per segment" above.
//!
//! # Only segments that carry registrations get a file
//!
//! [`encode_registry_delta`] returns `None` when the segment holds no
//! `$registry` batch at all, which is the overwhelmingly common case once a
//! store's names are established (registrations are one per name **ever**, not
//! one per event). Those segments cost the old path nothing either — the engine
//! checks the sidecar's stream list first and reads nothing — so no file is
//! written and nothing changes for them.
//!
//! # File layout
//!
//! ```text
//! Header (48 bytes):
//!   0   u32  magic = REGDELTA_MAGIC
//!   4   u16  format_version = 1
//!   6   u16  flags = 0
//!   8   u64  segment_id        (cross-checked against the .pidx it pairs with)
//!   16  u64  base_pos          (idem)
//!   24  u64  event_count       (idem)
//!   32  u64  stream_id         (the stream these batches belong to — REGISTRY_STREAM_ID)
//!   40  u32  n_batches
//!   44  u32  n_records         (total payloads across all batches)
//!
//! BATCH region, `n_batches` records, ascending `first_global_pos`:
//!   u64  first_global_pos      (the fold's replay-order key)
//!   u32  frame_count           (payloads that follow)
//!   u32  reserved = 0
//!   frame_count × { u32 len; len bytes }
//!
//! Footer (8 bytes):
//!   0   u32  content_crc (crc32c over everything before the footer)
//!   4   u32  magic = REGDELTA_MAGIC
//! ```
//!
//! Parsing is strictly bounded: every length is checked against the bytes that
//! remain before the footer, and the batch region must end *exactly* at the
//! footer. A file that does not is corrupt, which means "dropped", which means
//! "the `pread` path runs".

use crate::sealed::segment::SealInput;

/// Registry-delta file magic (`"REG"` + version byte, in the shape of
/// [`PCOL_MAGIC`](crate::sealed::payload::PCOL_MAGIC)).
pub const REGDELTA_MAGIC: u32 = 0x5245_4701;
/// Current registry-delta `format_version`. A reader that does not recognise
/// the version drops the file (and falls back), it does not guess.
pub const REGDELTA_FORMAT_VERSION: u16 = 1;
/// Registry-delta header length (bytes).
pub const REGDELTA_HEADER_LEN: usize = 48;
/// Registry-delta footer length (bytes).
pub const REGDELTA_FOOTER_LEN: usize = 8;
/// Per-batch fixed prefix in the BATCH region (bytes).
const BATCH_PREFIX_LEN: usize = 16;

/// REG1's reserved `$registry` `stream_id`.
///
/// The same value as `mess_store::registry::REGISTRY_STREAM_ID` — `mess-index`
/// sits *below* `mess-store` and cannot name it, so the constant is restated
/// here and `mess-store` asserts the two agree at compile time. The value is
/// also written into every `.reg` header rather than assumed by the reader, so
/// the cross-check in
/// [`accepts_registry_delta`](crate::sealed::segment::SealedSegmentIndex::accepts_registry_delta)
/// validates against whatever the file actually claims.
pub const REGISTRY_STREAM_ID: u64 = 0;

/// Errors parsing a persisted registry delta. Every variant is *soft*: the
/// caller drops the delta and the engine folds `$registry` from the log exactly
/// as it did before this format existed.
#[derive(Debug, thiserror::Error)]
pub enum RegDeltaError {
    /// I/O error reading the `.reg` file.
    #[error("registry delta I/O: {0}")]
    Io(#[from] std::io::Error),
    /// The bytes are too short, mis-magicked, wrong-version, size-mismatched,
    /// or CRC-mismatched.
    #[error("registry delta corrupt: {0}")]
    Corrupt(&'static str),
}

fn rd_u16(d: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(d[at..at + 2].try_into().unwrap())
}
fn rd_u32(d: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(d[at..at + 4].try_into().unwrap())
}
fn rd_u64(d: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(d[at..at + 8].try_into().unwrap())
}

/// One batch's index entry: where its payload spans start and how many.
#[derive(Debug, Clone, Copy)]
struct BatchIndex {
    first_global_pos: u64,
    first_span:       u32,
    frame_count:      u32,
}

/// A borrowed view of one `$registry` batch inside a [`RegistryDelta`] — the
/// same `(first_global_pos, payloads)` shape the fold consumes.
#[derive(Debug, Clone, Copy)]
pub struct DeltaBatch<'a> {
    first_global_pos: u64,
    spans:            &'a [(u32, u32)],
    bytes:            &'a [u8],
}

impl<'a> DeltaBatch<'a> {
    /// The batch's global position (A1) — the fold's replay-order key.
    #[must_use]
    pub fn first_global_pos(&self) -> u64 { self.first_global_pos }

    /// Number of `$registry` records in this batch.
    #[must_use]
    pub fn frame_count(&self) -> usize { self.spans.len() }

    /// The batch's record payloads, in subframe order — byte-identical to what
    /// a `pread` of the batch and a frame walk would yield.
    pub fn payloads(&self) -> impl Iterator<Item = &'a [u8]> + 'a {
        let bytes = self.bytes;
        self.spans.iter().map(move |&(off, len)| {
            &bytes[off as usize..off as usize + len as usize]
        })
    }
}

/// One sealed segment's `$registry` records, parsed from its `.reg` sibling.
///
/// Owns the file image; every accessor is a slice, no further I/O. Cheap to
/// hold: proportional to the *registrations* in one segment, not to the
/// segment.
#[derive(Debug)]
pub struct RegistryDelta {
    segment_id:  u64,
    base_pos:    u64,
    event_count: u64,
    stream_id:   u64,
    bytes:       Vec<u8>,
    batches:     Vec<BatchIndex>,
    /// `(offset, len)` into `bytes`, flat across batches in batch order.
    spans:       Vec<(u32, u32)>,
}

impl RegistryDelta {
    /// The segment this delta was built for.
    #[must_use]
    pub fn segment_id(&self) -> u64 { self.segment_id }

    /// The segment's A1 base position at seal time.
    #[must_use]
    pub fn base_pos(&self) -> u64 { self.base_pos }

    /// The segment's event count at seal time.
    #[must_use]
    pub fn event_count(&self) -> u64 { self.event_count }

    /// The stream these batches belong to (REG1's reserved
    /// [`REGISTRY_STREAM_ID`] for every delta this crate writes).
    #[must_use]
    pub fn stream_id(&self) -> u64 { self.stream_id }

    /// Number of batches carried.
    #[must_use]
    pub fn batch_count(&self) -> usize { self.batches.len() }

    /// Whether this delta carries no batches at all. [`encode_registry_delta`]
    /// never produces such a file (it returns `None` instead), so this is
    /// realistically always `false`; provided for the `len`/`is_empty`
    /// convention.
    #[must_use]
    pub fn is_empty(&self) -> bool { self.batches.is_empty() }

    /// Total `$registry` records across all batches.
    #[must_use]
    pub fn record_count(&self) -> usize { self.spans.len() }

    /// The batches in ascending `first_global_pos` order.
    pub fn batches(&self) -> impl Iterator<Item = DeltaBatch<'_>> + '_ {
        self.batches.iter().map(move |b| DeltaBatch {
            first_global_pos: b.first_global_pos,
            spans:            &self.spans[b.first_span as usize
                ..b.first_span as usize + b.frame_count as usize],
            bytes:            &self.bytes,
        })
    }

    /// `(first_global_pos, frame_count)` per batch, ascending — the list the
    /// pointer sidecar cross-check compares against.
    pub(crate) fn layout(&self) -> impl Iterator<Item = (u64, u32)> + '_ {
        self.batches.iter().map(|b| (b.first_global_pos, b.frame_count))
    }

    /// Parse a `.reg` byte image, validating magic, version, sizes, and CRC.
    /// The bytes are moved in and retained.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, RegDeltaError> {
        if bytes.len() < REGDELTA_HEADER_LEN + REGDELTA_FOOTER_LEN {
            return Err(RegDeltaError::Corrupt("shorter than header + footer"));
        }
        // `spans` stores 32-bit offsets into the image; refuse anything that
        // could not be addressed by them rather than truncating silently.
        if bytes.len() > u32::MAX as usize {
            return Err(RegDeltaError::Corrupt("image larger than 4 GiB"));
        }
        if rd_u32(&bytes, 0) != REGDELTA_MAGIC {
            return Err(RegDeltaError::Corrupt("bad header magic"));
        }
        if rd_u16(&bytes, 4) != REGDELTA_FORMAT_VERSION {
            return Err(RegDeltaError::Corrupt("unknown format_version"));
        }
        // No flag bits are defined yet; an unknown one means a writer newer
        // than this reader, so drop rather than misinterpret.
        if rd_u16(&bytes, 6) != 0 {
            return Err(RegDeltaError::Corrupt("unknown flags"));
        }
        let segment_id = rd_u64(&bytes, 8);
        let base_pos = rd_u64(&bytes, 16);
        let event_count = rd_u64(&bytes, 24);
        let stream_id = rd_u64(&bytes, 32);
        let n_batches = rd_u32(&bytes, 40) as usize;
        let n_records = rd_u32(&bytes, 44) as usize;

        let footer_start = bytes.len() - REGDELTA_FOOTER_LEN;
        if rd_u32(&bytes, footer_start + 4) != REGDELTA_MAGIC {
            return Err(RegDeltaError::Corrupt("bad footer magic"));
        }
        let stored_crc = rd_u32(&bytes, footer_start);
        if crc32c::crc32c(&bytes[..footer_start]) != stored_crc {
            return Err(RegDeltaError::Corrupt("content CRC mismatch"));
        }

        // Cheapest possible structural rejection before allocating: each batch
        // needs at least its fixed prefix, each record at least its length
        // word.
        let min_body = n_batches
            .checked_mul(BATCH_PREFIX_LEN)
            .and_then(|b| {
                n_records.checked_mul(4).and_then(|r| b.checked_add(r))
            })
            .ok_or(RegDeltaError::Corrupt("batch region size overflow"))?;
        if footer_start - REGDELTA_HEADER_LEN < min_body {
            return Err(RegDeltaError::Corrupt("batch region too short"));
        }

        let mut batches = Vec::with_capacity(n_batches);
        let mut spans = Vec::with_capacity(n_records);
        let mut at = REGDELTA_HEADER_LEN;
        let mut last_pos: Option<u64> = None;
        for _ in 0..n_batches {
            if footer_start - at < BATCH_PREFIX_LEN {
                return Err(RegDeltaError::Corrupt("truncated batch header"));
            }
            let first_global_pos = rd_u64(&bytes, at);
            let frame_count = rd_u32(&bytes, at + 8);
            if rd_u32(&bytes, at + 12) != 0 {
                return Err(RegDeltaError::Corrupt("batch reserved field set"));
            }
            if frame_count == 0 {
                return Err(RegDeltaError::Corrupt("empty batch"));
            }
            // Ascending, strictly: the committer assigns `first_global_pos`
            // densely and never reuses it, so duplicates mean a bad file.
            if last_pos.is_some_and(|p| first_global_pos <= p) {
                return Err(RegDeltaError::Corrupt(
                    "batches not strictly ascending by first_global_pos",
                ));
            }
            last_pos = Some(first_global_pos);
            at += BATCH_PREFIX_LEN;

            let first_span = spans.len();
            if first_span + frame_count as usize > n_records {
                return Err(RegDeltaError::Corrupt(
                    "batch frame counts exceed header n_records",
                ));
            }
            for _ in 0..frame_count {
                if footer_start - at < 4 {
                    return Err(RegDeltaError::Corrupt(
                        "truncated record length",
                    ));
                }
                let len = rd_u32(&bytes, at) as usize;
                at += 4;
                if footer_start - at < len {
                    return Err(RegDeltaError::Corrupt("truncated record"));
                }
                spans.push((at as u32, len as u32));
                at += len;
            }
            batches.push(BatchIndex {
                first_global_pos,
                first_span: first_span as u32,
                frame_count,
            });
        }
        if at != footer_start {
            return Err(RegDeltaError::Corrupt(
                "batch region does not end at the footer",
            ));
        }
        if spans.len() != n_records {
            return Err(RegDeltaError::Corrupt(
                "record count disagrees with header",
            ));
        }

        Ok(RegistryDelta {
            segment_id,
            base_pos,
            event_count,
            stream_id,
            bytes,
            batches,
            spans,
        })
    }

    /// Read and parse a `.reg` file from `path`.
    pub fn open(path: &std::path::Path) -> Result<Self, RegDeltaError> {
        Self::from_bytes(std::fs::read(path)?)
    }
}

/// The registry-delta (`.reg`) path for `segment_id` under `dir`:
/// `<dir>/seg-<id>.reg`. The single source of truth for the naming, shared by
/// [`SealDriver`](crate::sealed::driver::SealDriver) (write at seal) and engine
/// recovery (read at open) so the two can never drift.
#[must_use]
pub fn reg_path(dir: &std::path::Path, segment_id: u64) -> std::path::PathBuf {
    dir.join(format!("seg-{segment_id:020}.reg"))
}

/// The registry-delta (`.reg`) path for a given pointer-sidecar path: same
/// directory and stem, `.reg` extension in place of `.pidx`. Equivalent to
/// [`reg_path`] for a sidecar this crate wrote; provided for callers that
/// already hold the `.pidx` path.
#[must_use]
pub fn regdelta_path_for(sidecar_path: &std::path::Path) -> std::path::PathBuf {
    sidecar_path.with_extension("reg")
}

/// Serialize the `stream_id` slice of a [`SealInput`] into the `.reg` byte
/// image (see the module docs for the layout). Pure: no I/O — the driver writes
/// and fsyncs the bytes with the same crash-atomic discipline as every other
/// sidecar.
///
/// Returns `None` — meaning "write no file", not "something went wrong" —
/// when:
///
/// - the input carries no payloads (a pointer-only seal has nothing to copy);
/// - the segment holds no batch for `stream_id` (the common case: no
///   registration happened in this segment); or
/// - a batch's payload span does not lie inside the payload vector, i.e. the
///   input violates [`SealInput::payloads`]' stored-order contract. Emitting
///   nothing keeps the `pread` path, which reads the log and is always right.
#[must_use]
pub fn encode_registry_delta(
    input: &SealInput,
    stream_id: u64,
) -> Option<Vec<u8>> {
    let payloads = input.payloads.as_ref()?;
    let stream = input.streams.iter().find(|s| s.stream_id == stream_id)?;
    if stream.batches.is_empty() {
        return None;
    }

    // Ascending by global position — the fold's replay order, and the order
    // the pointer-sidecar cross-check compares against. `SealStream::batches`
    // is version-ascending, which for a single stream is the same order; sort
    // anyway so the file's own invariant does not depend on that.
    let mut batches: Vec<_> = stream.batches.iter().collect();
    batches.sort_unstable_by_key(|b| b.first_global_pos);

    let mut n_records: usize = 0;
    let mut body_len: usize = 0;
    for b in &batches {
        let start = b.first_global_pos.checked_sub(input.base_pos)? as usize;
        let end = start.checked_add(b.frame_count as usize)?;
        let slice = payloads.get(start..end)?;
        n_records = n_records.checked_add(slice.len())?;
        body_len = body_len.checked_add(BATCH_PREFIX_LEN)?;
        for p in slice {
            // A single record wider than u32 cannot be described by the format
            // (nor produced by the engine: `$registry` records are tens of
            // bytes). Refuse to write rather than truncate.
            if p.len() > u32::MAX as usize {
                return None;
            }
            body_len = body_len.checked_add(4)?.checked_add(p.len())?;
        }
    }
    if n_records > u32::MAX as usize || batches.len() > u32::MAX as usize {
        return None;
    }
    let total = REGDELTA_HEADER_LEN
        .checked_add(body_len)?
        .checked_add(REGDELTA_FOOTER_LEN)?;
    if total > u32::MAX as usize {
        return None;
    }

    let mut buf: Vec<u8> = Vec::with_capacity(total);
    buf.extend_from_slice(&REGDELTA_MAGIC.to_le_bytes());
    buf.extend_from_slice(&REGDELTA_FORMAT_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes()); // flags
    buf.extend_from_slice(&input.segment_id.to_le_bytes());
    buf.extend_from_slice(&input.base_pos.to_le_bytes());
    buf.extend_from_slice(&input.event_count().to_le_bytes());
    buf.extend_from_slice(&stream_id.to_le_bytes());
    buf.extend_from_slice(&(batches.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(n_records as u32).to_le_bytes());
    debug_assert_eq!(buf.len(), REGDELTA_HEADER_LEN);

    for b in &batches {
        let start = (b.first_global_pos - input.base_pos) as usize;
        let slice = &payloads[start..start + b.frame_count as usize];
        buf.extend_from_slice(&b.first_global_pos.to_le_bytes());
        buf.extend_from_slice(&b.frame_count.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // reserved
        for p in slice {
            buf.extend_from_slice(&(p.len() as u32).to_le_bytes());
            buf.extend_from_slice(p);
        }
    }

    let content_crc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&content_crc.to_le_bytes());
    buf.extend_from_slice(&REGDELTA_MAGIC.to_le_bytes());
    debug_assert_eq!(buf.len(), total);
    Some(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sealed::segment::{SealBatch, SealStream};

    /// A two-stream input: stream 0 (the registry) with two batches, plus a
    /// domain stream, over a dense payload vector.
    fn sample() -> SealInput {
        let payloads: Vec<Vec<u8>> = (0..6u8)
            .map(|i| vec![i; usize::from(i) + 1]) // distinct bytes and lengths
            .collect();
        SealInput {
            segment_id:     7,
            base_pos:       100,
            streams:        vec![
                SealStream {
                    stream_id: REGISTRY_STREAM_ID,
                    batches:   vec![
                        SealBatch {
                            first_version:    0,
                            frame_count:      2,
                            first_global_pos: 100,
                            offset:           64,
                        },
                        SealBatch {
                            first_version:    2,
                            frame_count:      1,
                            first_global_pos: 104,
                            offset:           400,
                        },
                    ],
                },
                SealStream {
                    stream_id: 9,
                    batches:   vec![SealBatch {
                        first_version:    0,
                        frame_count:      3,
                        first_global_pos: 102,
                        offset:           200,
                    }],
                },
            ],
            payloads:       Some(payloads),
            event_type_ids: None,
        }
    }

    #[test]
    fn round_trip_carries_the_registry_batches_only() {
        let input = sample();
        let bytes =
            encode_registry_delta(&input, REGISTRY_STREAM_ID).expect("encode");
        let d = RegistryDelta::from_bytes(bytes).expect("parse");

        assert_eq!(d.segment_id(), 7);
        assert_eq!(d.base_pos(), 100);
        assert_eq!(d.event_count(), 6);
        assert_eq!(d.stream_id(), REGISTRY_STREAM_ID);
        assert_eq!(d.batch_count(), 2);
        assert_eq!(d.record_count(), 3);

        let got: Vec<(u64, Vec<Vec<u8>>)> = d
            .batches()
            .map(|b| {
                (
                    b.first_global_pos(),
                    b.payloads().map(<[u8]>::to_vec).collect(),
                )
            })
            .collect();
        // payload index = global_pos - base_pos: batch@100 owns [0,1],
        // batch@104 owns [4]. The domain stream's [2,3,4]... wait, 104 is the
        // registry's own second batch, so index 4.
        assert_eq!(
            got,
            vec![
                (100, vec![vec![0u8; 1], vec![1u8; 2]]),
                (104, vec![vec![4u8; 5]]),
            ]
        );
    }

    #[test]
    fn absent_stream_or_payloads_encodes_nothing() {
        let mut input = sample();
        assert!(encode_registry_delta(&input, 4242).is_none());
        input.payloads = None;
        assert!(encode_registry_delta(&input, REGISTRY_STREAM_ID).is_none());
    }

    /// A payload span outside the payload vector means the caller broke the
    /// stored-order contract; emit nothing rather than a wrong file.
    #[test]
    fn out_of_range_span_encodes_nothing() {
        let mut input = sample();
        input.streams[0].batches[1].first_global_pos = 1_000;
        assert!(encode_registry_delta(&input, REGISTRY_STREAM_ID).is_none());
        let mut input = sample();
        input.streams[0].batches[0].first_global_pos = 0; // below base_pos
        assert!(encode_registry_delta(&input, REGISTRY_STREAM_ID).is_none());
    }

    fn encoded() -> Vec<u8> {
        encode_registry_delta(&sample(), REGISTRY_STREAM_ID).expect("encode")
    }

    #[test]
    fn every_single_byte_flip_is_rejected() {
        let good = encoded();
        for i in 0..good.len() {
            let mut bytes = good.clone();
            bytes[i] ^= 0xFF;
            assert!(
                RegistryDelta::from_bytes(bytes).is_err(),
                "byte {i} flip accepted"
            );
        }
    }

    #[test]
    fn truncation_at_any_length_is_rejected() {
        let good = encoded();
        for n in 0..good.len() {
            assert!(
                RegistryDelta::from_bytes(good[..n].to_vec()).is_err(),
                "truncation to {n} accepted"
            );
        }
        // ...and trailing garbage.
        let mut extended = good.clone();
        extended.push(0);
        assert!(RegistryDelta::from_bytes(extended).is_err());
    }

    #[test]
    fn wrong_version_or_unknown_flags_are_rejected() {
        for (at, val) in [(4usize, 2u16), (6, 1)] {
            let mut bytes = encoded();
            bytes[at..at + 2].copy_from_slice(&val.to_le_bytes());
            // Re-CRC so the version/flag check, not the CRC, is what rejects.
            let footer_start = bytes.len() - REGDELTA_FOOTER_LEN;
            let crc = crc32c::crc32c(&bytes[..footer_start]);
            bytes[footer_start..footer_start + 4]
                .copy_from_slice(&crc.to_le_bytes());
            assert!(matches!(
                RegistryDelta::from_bytes(bytes),
                Err(RegDeltaError::Corrupt(_))
            ));
        }
    }

    /// A CRC-consistent file whose batches are out of order (or duplicated) is
    /// still refused — the fold's replay-order key must be a strict sequence.
    #[test]
    fn non_ascending_batches_are_rejected() {
        let mut bytes = encoded();
        // Rewrite the second batch's first_global_pos to equal the first's.
        // Batch 1 prefix starts at HEADER + 16 + (4+1) + (4+2) = 48+16+11 = 75.
        let second = REGDELTA_HEADER_LEN + BATCH_PREFIX_LEN + (4 + 1) + (4 + 2);
        bytes[second..second + 8].copy_from_slice(&100u64.to_le_bytes());
        let footer_start = bytes.len() - REGDELTA_FOOTER_LEN;
        let crc = crc32c::crc32c(&bytes[..footer_start]);
        bytes[footer_start..footer_start + 4]
            .copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            RegistryDelta::from_bytes(bytes),
            Err(RegDeltaError::Corrupt(_))
        ));
    }

    #[test]
    fn path_pairs_with_the_pointer_sidecar() {
        let dir = std::path::Path::new("/s/sealed");
        let p = dir.join("seg-00000000000000000012.pidx");
        let want = dir.join("seg-00000000000000000012.reg");
        assert_eq!(regdelta_path_for(&p), want);
        assert_eq!(reg_path(dir, 12), want);
    }
}
