//! The sealed per-segment pointer index: the on-disk **sidecar** that replaces
//! a sealed segment's slice of the in-memory active index (D5). One sidecar per
//! sealed segment, built once by the background sealer
//! ([`crate::sealed::driver`]) and thereafter read-only.
//!
//! # Why a sidecar, not a footer extension section
//!
//! Spec 01 §3.3.2 lets the segment footer carry typed extension sections and
//! requires readers to **advisory-skip** any section kind they do not
//! understand (D-FMT-3): a section is never a commit authority — batch
//! acceptance is decided solely by the per-batch checksums (§5), and losing a
//! section costs at most a rebuild. The sealed pointer index is the archetype
//! of that contract: it is a *rebuildable read-optimization* — the log segment
//! is truth (D1), and [`crate::rebuild`] can reconstruct the same pointers by
//! scanning the segment. So it is legitimately advisory.
//!
//! We place it in a **sidecar file** rather than a footer section for three
//! reasons, all consistent with the advisory-skip rule:
//!
//! 1. **Crate ownership.** mess-log owns the segment bytes and the footer
//!    section *framing* (§3.3.2); this crate owns the sealed-index *bytes* (the
//!    bone's split). mess-log's `SegmentWriter::seal` finalizes an empty (or
//!    fold-anchor-only) extension in one seal `fdatasync`; making it accept a
//!    large, index-crate-owned section would invert that ownership. A sidecar
//!    keeps each crate's bytes on its own side of the seam without
//!    restructuring mess-log.
//! 2. **R2 fast path.** The footer trailer is read by a fixed `pread` from EOF
//!    (R2, §02). A multi-hundred-KiB pointer index in the extension region
//!    would bloat the seal write and sit in front of the trailer for no
//!    fast-path benefit; out of band it never touches R2.
//! 3. **Same forward-compat guarantee, stronger.** An old reader ignores an
//!    unknown section by skipping its `payload_len`; it ignores a sidecar by
//!    simply not opening it. Both fall back to scanning/rebuilding. The sidecar
//!    is the out-of-band analogue of the repair sidecar (D-FMT-3), which spec
//!    01 already blesses.
//!
//! # File layout
//!
//! ```text
//! Header (40 bytes):
//!   0   u32  magic = SIDECAR_MAGIC
//!   4   u16  format_version = 1
//!   6   u16  flags = 0
//!   8   u64  segment_id
//!   16  u64  base_pos            (segment's A1 base position)
//!   24  u64  event_count         (Σ frame_count)
//!   32  u32  n_streams
//!   36  u32  reserved = 0
//!
//! PTR region:  per-stream packed pointer blocks, back to back
//! SKIP region: per-stream skip tables, back to back
//! DIR region:  n_streams DirEntry records, ascending by stream_id
//!
//! DirEntry (56 bytes):
//!   0   u64  stream_id
//!   8   u64  first_version       (of the stream's first batch in this segment)
//!   16  u64  last_version        (of the stream's last batch — the head)
//!   24  u64  ptr_off             (absolute file offset of the ptr block)
//!   32  u32  ptr_len
//!   36  u32  n_batches
//!   40  u64  skip_off            (absolute file offset of the skip table)
//!   48  u32  skip_len            (n_skips = skip_len / SKIP_ENTRY_LEN)
//!   52  u32  reserved = 0
//!
//! Footer (fixed 40 bytes, at EOF):
//!   0   u64  dir_off
//!   8   u64  ptr_region_off
//!   16  u64  skip_region_off
//!   24  u32  content_crc         (crc32c over [0, footer_start))
//!   28  u32  reserved = 0
//!   32  u32  n_streams           (redundant, cross-checks the header)
//!   36  u32  magic = SIDECAR_MAGIC
//! ```
//!
//! The whole file is small (~1.2 B/event: `seal_pipeline` measured packed
//! pointer blocks at 1.015 B/event + `perf_replay`'s skip table at 0.16
//! B/event), so the reader loads it fully into memory and answers every query
//! without further I/O — this is the "cached" regime `perf_replay` benched at
//! p50 1.67 µs / p99 0.79 µs per point read.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::Arc;

use crate::active::{EventPtr, GlobalEntry, StreamEntry};
use crate::sealed::filter::SegmentFilter;
use crate::sealed::payload::{DictResolver, PayloadError, SealedPayloadIndex};
use crate::sealed::ptr_block::{
    self, BatchPtr, DecodeError, SkipEntry, encode_ptr_block, encode_skips,
};

/// Sidecar magic (`"SXI1"` little-endian-ish): identifies a sealed pointer
/// index and its byte order.
pub const SIDECAR_MAGIC: u32 = 0x5359_4901;
/// Header length (bytes).
pub const HEADER_LEN: usize = 40;
/// Directory-entry length (bytes). One 48-byte fixed record per stream. The
/// `n_skips` a reader needs is derived from `skip_off` spans, so the record
/// stores an explicit `skip_len` instead (see [`DirEntry`]).
pub const DIR_ENTRY_LEN: usize = 56;
/// Footer length (bytes), at EOF.
pub const FOOTER_LEN: usize = 40;
/// Current sidecar `format_version`.
pub const FORMAT_VERSION: u16 = 1;

// ---------------------------------------------------------------------------
// Seal input
// ---------------------------------------------------------------------------

/// One batch to seal — the read-relevant fields of a [`StreamEntry`] plus the
/// segment-relative pointer offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealBatch {
    /// Stream version of the batch's first event.
    pub first_version:    u64,
    /// Number of events in the batch.
    pub frame_count:      u32,
    /// Global position (A1) of the batch's first event.
    pub first_global_pos: u64,
    /// Byte offset of the batch within the segment.
    pub offset:           u64,
}

impl SealBatch {
    #[inline]
    pub(crate) fn as_batch_ptr(&self) -> BatchPtr {
        BatchPtr {
            first_version:    self.first_version,
            frame_count:      self.frame_count,
            first_global_pos: self.first_global_pos,
            offset:           self.offset,
        }
    }
}

/// One stream's batches in a segment, ascending by version. Non-empty.
#[derive(Debug, Clone)]
pub struct SealStream {
    /// The stream id.
    pub stream_id: u64,
    /// The stream's batches in this segment, version-ascending.
    pub batches:   Vec<SealBatch>,
}

/// The seal INPUT: everything the sealer needs to turn one segment's slice of
/// the active index into a sidecar. Derive it from [`SealInput::from_snapshot`]
/// or hand-build it in tests.
#[derive(Debug, Clone)]
pub struct SealInput {
    /// The segment being sealed (all pointers resolve into it).
    pub segment_id:     u64,
    /// The segment's A1 base position.
    pub base_pos:       u64,
    /// Per-stream batch lists, ascending by `stream_id`.
    pub streams:        Vec<SealStream>,
    /// The segment's event **payloads** in stored / global-position order:
    /// index `i` is the payload of the event at global position
    /// `base_pos + i`. When `Some`,
    /// [`crate::sealed::driver::SealDriver::seal`] also emits the D6
    /// payload-block sidecar (`.pcol`) — columnar by default, row fallback
    /// where the codec cannot shred, verify-on-seal — and attaches
    /// the resulting [`crate::sealed::payload::SealedPayloadIndex`] to the
    /// installed segment so the sealed read path
    /// ([`ReplaySet`](crate::sealed::replay::ReplaySet)) can reassemble
    /// payloads without touching the raw log. `None` seals the pointer
    /// sidecar only (the caller has no payload bytes in hand — e.g. a
    /// pointer-only rebuild).
    pub payloads:       Option<Vec<Vec<u8>>>,
    /// The segment's per-event `event_type_id` in stored / global-position
    /// order (bn-3of): index `i` is the type id of the event at global
    /// position `base_pos + i`. Consumed only by the **consolidated SealPack**
    /// path ([`crate::sealed::driver::SealDriver::with_pack`]) to build the
    /// `EVENT_TYPE_IDS` section, so cold `message_type` reads never decode the
    /// raw batch (bn-3fn carry-forward #1). `None` (the legacy default) omits
    /// the section; the three-sidecar path ignores this field entirely.
    pub event_type_ids: Option<Vec<u32>>,
}

impl SealInput {
    /// Total events across all streams.
    pub fn event_count(&self) -> u64 {
        self.streams
            .iter()
            .flat_map(|s| s.batches.iter())
            .map(|b| u64::from(b.frame_count))
            .sum()
    }

    /// Attach the segment's payloads in stored / global-position order (builder
    /// form), so [`crate::sealed::driver::SealDriver::seal`] emits the `.pcol`
    /// payload sidecar alongside the pointer sidecar. See
    /// [`SealInput::payloads`] for the ordering contract.
    #[must_use]
    pub fn with_payloads(mut self, payloads: Vec<Vec<u8>>) -> Self {
        self.payloads = Some(payloads);
        self
    }

    /// Attach the segment's per-event `event_type_id` column in stored /
    /// global-position order (builder form), so the consolidated SealPack path
    /// emits the `EVENT_TYPE_IDS` section (bn-3of). See
    /// [`SealInput::event_type_ids`] for the ordering contract.
    #[must_use]
    pub fn with_event_type_ids(mut self, type_ids: Vec<u32>) -> Self {
        self.event_type_ids = Some(type_ids);
        self
    }

    /// Build the seal input for `segment_id` from a committed
    /// [`crate::IndexSnapshot`], keeping only the entries whose pointer lands
    /// in that segment. Streams end up ascending by id (the snapshot's
    /// `BTreeMap` order), each stream's batches version-ascending (the
    /// snapshot preserves insert order). `base_pos` is the segment's A1
    /// base (from the segment header / trailer).
    pub fn from_snapshot(
        snapshot: &crate::IndexSnapshot,
        segment_id: u64,
        base_pos: u64,
    ) -> Self {
        let mut streams = Vec::new();
        for (&stream_id, entries) in &snapshot.streams {
            let batches: Vec<SealBatch> = entries
                .iter()
                .filter(|e| e.ptr.segment_id == segment_id)
                .map(|e: &StreamEntry| SealBatch {
                    first_version:    e.first_version,
                    frame_count:      e.frame_count,
                    first_global_pos: e.first_global_pos,
                    offset:           e.ptr.offset,
                })
                .collect();
            if !batches.is_empty() {
                streams.push(SealStream { stream_id, batches });
            }
        }
        SealInput {
            segment_id,
            base_pos,
            streams,
            payloads: None,
            event_type_ids: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------

/// Errors from opening or validating a sealed sidecar.
#[derive(Debug, thiserror::Error)]
pub enum SidecarError {
    /// I/O error reading the sidecar file.
    #[error("sidecar I/O: {0}")]
    Io(#[from] io::Error),
    /// The bytes are too short, mis-magicked, wrong-version, or CRC-mismatched.
    #[error("sidecar corrupt: {0}")]
    Corrupt(&'static str),
    /// A pointer block or skip table failed to decode.
    #[error("sidecar decode: {0}")]
    Decode(#[from] DecodeError),
}

/// The seal-time membership filter's path for a given sidecar path (bn-1i7):
/// same directory and stem, `.filter` extension in place of `.pidx`. The
/// single source of truth for the pairing, used by both
/// [`crate::sealed::driver::SealDriver`] (write) and
/// [`SealedSegmentIndex::open`] (read) so the two files can never drift.
pub fn filter_path_for(sidecar_path: &Path) -> std::path::PathBuf {
    sidecar_path.with_extension("filter")
}

/// The D6 payload-block sidecar (`.pcol`) path for a given pointer-sidecar path
/// (bn-zge): same directory and stem, `.pcol` extension in place of `.pidx`.
/// The single source of truth for the pairing, used by both
/// [`crate::sealed::driver::SealDriver`] (write) and
/// [`SealedSegmentIndex::open`] (re-attach) so the two files can never drift.
pub fn payload_path_for(sidecar_path: &Path) -> std::path::PathBuf {
    sidecar_path.with_extension("pcol")
}

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
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

/// Serialize a [`SealInput`] into the sidecar byte image (header + regions +
/// footer, CRC-covered). Pure: no I/O — the driver writes and fsyncs the bytes.
pub fn encode_sidecar(input: &SealInput) -> Vec<u8> {
    // Encode all pointer blocks and skip tables first, tracking per-stream
    // spans; then lay out header, PTR region, SKIP region, DIR region, footer.
    struct Enc {
        stream_id:     u64,
        first_version: u64,
        last_version:  u64,
        n_batches:     u32,
        block:         Vec<u8>,
        skips:         Vec<u8>,
    }
    let mut encs: Vec<Enc> = Vec::with_capacity(input.streams.len());
    for s in &input.streams {
        debug_assert!(!s.batches.is_empty());
        let ptrs: Vec<BatchPtr> =
            s.batches.iter().map(SealBatch::as_batch_ptr).collect();
        let mut skips: Vec<SkipEntry> = Vec::new();
        let block = encode_ptr_block(&ptrs, &mut skips);
        let last = ptrs.last().unwrap();
        encs.push(Enc {
            stream_id: s.stream_id,
            first_version: ptrs[0].first_version,
            last_version: last.last_version(),
            n_batches: ptrs.len() as u32,
            block,
            skips: encode_skips(&skips),
        });
    }

    let ptr_region_off = HEADER_LEN as u64;
    let ptr_region_len: u64 = encs.iter().map(|e| e.block.len() as u64).sum();
    let skip_region_off = ptr_region_off + ptr_region_len;
    let skip_region_len: u64 = encs.iter().map(|e| e.skips.len() as u64).sum();
    let dir_off = skip_region_off + skip_region_len;

    let mut buf: Vec<u8> = Vec::with_capacity(
        HEADER_LEN
            + ptr_region_len as usize
            + skip_region_len as usize
            + encs.len() * DIR_ENTRY_LEN
            + FOOTER_LEN,
    );

    // Header.
    put_u32(&mut buf, SIDECAR_MAGIC);
    buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes()); // flags
    put_u64(&mut buf, input.segment_id);
    put_u64(&mut buf, input.base_pos);
    put_u64(&mut buf, input.event_count());
    put_u32(&mut buf, encs.len() as u32);
    put_u32(&mut buf, 0); // reserved
    debug_assert_eq!(buf.len(), HEADER_LEN);

    // PTR region, recording each stream's absolute offset.
    let mut ptr_offs = Vec::with_capacity(encs.len());
    for e in &encs {
        ptr_offs.push(buf.len() as u64);
        buf.extend_from_slice(&e.block);
    }
    // SKIP region.
    let mut skip_offs = Vec::with_capacity(encs.len());
    for e in &encs {
        skip_offs.push(buf.len() as u64);
        buf.extend_from_slice(&e.skips);
    }
    // DIR region.
    debug_assert_eq!(buf.len() as u64, dir_off);
    for (i, e) in encs.iter().enumerate() {
        put_u64(&mut buf, e.stream_id);
        put_u64(&mut buf, e.first_version);
        put_u64(&mut buf, e.last_version);
        put_u64(&mut buf, ptr_offs[i]);
        put_u32(&mut buf, e.block.len() as u32);
        put_u32(&mut buf, e.n_batches);
        put_u64(&mut buf, skip_offs[i]);
        put_u32(&mut buf, e.skips.len() as u32);
        put_u32(&mut buf, 0); // reserved, keeps the record 56 bytes.
    }

    // Footer. content_crc covers everything written so far.
    let content_crc = crc32c::crc32c(&buf);
    put_u64(&mut buf, dir_off);
    put_u64(&mut buf, ptr_region_off);
    put_u64(&mut buf, skip_region_off);
    put_u32(&mut buf, content_crc);
    put_u32(&mut buf, 0); // reserved
    put_u32(&mut buf, encs.len() as u32);
    put_u32(&mut buf, SIDECAR_MAGIC);
    buf
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// A stream's directory entry, parsed from the DIR region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DirEntry {
    first_version: u64,
    last_version:  u64,
    ptr_off:       u64,
    ptr_len:       u32,
    n_batches:     u32,
    skip_off:      u64,
    skip_len:      u32,
}

/// The read-only sealed pointer index for one segment. Owns the sidecar bytes
/// in memory; every query is a slice + decode with no further I/O.
#[derive(Debug)]
pub struct SealedSegmentIndex {
    segment_id:  u64,
    base_pos:    u64,
    event_count: u64,
    bytes:       Vec<u8>,
    dir:         HashMap<u64, DirEntry>,
    /// Stream ids ascending — for global replay and deterministic iteration.
    stream_ids:  Vec<u64>,
    /// The seal-time `BinaryFuse16` stream-id membership filter (bn-1i7), if
    /// one is attached — see [`Self::might_contain_stream`]. `None` when no
    /// filter was built (e.g. an empty segment) or none was found/valid on
    /// disk; callers must treat that exactly like a filter that always
    /// answers "maybe" (I5 — never a wrong answer, only lost skip-ahead).
    filter:      Option<SegmentFilter>,
    /// The D6 payload-block sidecar (`.pcol`) for this segment (bn-zge), if
    /// one was emitted at seal and attached here (or re-attached by
    /// [`Self::open`] from the sibling file). `None` for a pointer-only
    /// seal. When present, the sealed read path reassembles payloads from
    /// it ([`Self::reassemble_payload`]) instead of the raw log.
    payload:     Option<SealedPayloadIndex>,
    /// Per-event `event_type_id` in stored (global-position) order, indexed by
    /// segment-local position (bn-3of / bn-3fn carry-forward #1). Present only
    /// when this index was built from a [`SealPack`](crate::sealed::pack) that
    /// carried an `EVENT_TYPE_IDS` section. When present, the cold read path
    /// resolves an event's `message_type` **without decoding the raw batch**;
    /// `None` (legacy sidecars, or a dropped/corrupt section) falls back to
    /// the raw-batch decode exactly as before.
    event_types: Option<Vec<u32>>,
}

impl SealedSegmentIndex {
    /// The segment this index covers.
    pub fn segment_id(&self) -> u64 { self.segment_id }

    /// The segment's A1 base position.
    pub fn base_pos(&self) -> u64 { self.base_pos }

    /// Total events indexed.
    pub fn event_count(&self) -> u64 { self.event_count }

    /// Number of streams present in this segment.
    pub fn stream_count(&self) -> usize { self.stream_ids.len() }

    /// The stream ids present in this segment, ascending (bn-2ug's retention
    /// rule walks these to build a segment's per-stream frame spans).
    pub fn stream_ids(&self) -> &[u64] { &self.stream_ids }

    /// `(first_version, last_version)` — the inclusive committed version range
    /// of `stream_id`'s frames within this segment, or `None` if the stream is
    /// absent. This is the "segment frame-range" the bn-2ug retention rule
    /// (`docs/spec/05-fold-certificates.md` §8.2) checks a live snapshot's
    /// certification frames against.
    pub fn stream_range(&self, stream_id: u64) -> Option<(u64, u64)> {
        self.dir.get(&stream_id).map(|e| (e.first_version, e.last_version))
    }

    /// Parse a sidecar byte image, validating magic, version, and CRC. The
    /// bytes are moved in and retained.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, SidecarError> {
        if bytes.len() < HEADER_LEN + FOOTER_LEN {
            return Err(SidecarError::Corrupt("shorter than header + footer"));
        }
        if rd_u32(&bytes, 0) != SIDECAR_MAGIC {
            return Err(SidecarError::Corrupt("bad header magic"));
        }
        if rd_u16(&bytes, 4) != FORMAT_VERSION {
            return Err(SidecarError::Corrupt("unknown format_version"));
        }
        let segment_id = rd_u64(&bytes, 8);
        let base_pos = rd_u64(&bytes, 16);
        let event_count = rd_u64(&bytes, 24);
        let n_streams = rd_u32(&bytes, 32) as usize;

        let footer_start = bytes.len() - FOOTER_LEN;
        let foot = &bytes[footer_start..];
        if rd_u32(foot, 36) != SIDECAR_MAGIC {
            return Err(SidecarError::Corrupt("bad footer magic"));
        }
        if rd_u32(foot, 32) as usize != n_streams {
            return Err(SidecarError::Corrupt(
                "footer/header n_streams disagree",
            ));
        }
        let dir_off = rd_u64(foot, 0) as usize;
        let stored_crc = rd_u32(foot, 24);
        let got_crc = crc32c::crc32c(&bytes[..footer_start]);
        if got_crc != stored_crc {
            return Err(SidecarError::Corrupt("content CRC mismatch"));
        }

        // Parse the directory.
        let dir_len = n_streams
            .checked_mul(DIR_ENTRY_LEN)
            .ok_or(SidecarError::Corrupt("dir length overflow"))?;
        if dir_off > footer_start || footer_start - dir_off != dir_len {
            return Err(SidecarError::Corrupt("dir region size mismatch"));
        }
        let mut dir = HashMap::with_capacity(n_streams);
        let mut stream_ids = Vec::with_capacity(n_streams);
        for i in 0..n_streams {
            let b = dir_off + i * DIR_ENTRY_LEN;
            let stream_id = rd_u64(&bytes, b);
            let entry = DirEntry {
                first_version: rd_u64(&bytes, b + 8),
                last_version:  rd_u64(&bytes, b + 16),
                ptr_off:       rd_u64(&bytes, b + 24),
                ptr_len:       rd_u32(&bytes, b + 32),
                n_batches:     rd_u32(&bytes, b + 36),
                skip_off:      rd_u64(&bytes, b + 40),
                skip_len:      rd_u32(&bytes, b + 48),
            };
            // Bounds-check the spans so later slicing cannot panic.
            let ptr_end = entry.ptr_off as usize + entry.ptr_len as usize;
            let skip_end = entry.skip_off as usize + entry.skip_len as usize;
            if ptr_end > footer_start || skip_end > footer_start {
                return Err(SidecarError::Corrupt("dir span out of range"));
            }
            dir.insert(stream_id, entry);
            stream_ids.push(stream_id);
        }

        Ok(SealedSegmentIndex {
            segment_id,
            base_pos,
            event_count,
            bytes,
            dir,
            stream_ids,
            filter: None,
            payload: None,
            event_types: None,
        })
    }

    /// Parse a **SealPack** byte image (bn-3of, [`crate::sealed::pack`]) into
    /// the same read surface a legacy `.pidx`+`.filter`+`.pcol` trio yields.
    /// Validates the header/directory blake3 hash and every section's
    /// directory-committed `crc32c` + `content_hash_prefix` (see
    /// [`crate::sealed::pack::parse_pack`]); a corrupt mandatory section or any
    /// structural fault returns [`SidecarError::Corrupt`] so the caller
    /// raw-scans. The optional filter / payload / event-type sections are
    /// attached only when they verify — a corrupt one is silently dropped and
    /// that accelerator degrades locally (the pointer resolution stays exact).
    pub fn from_pack(bytes: Vec<u8>) -> Result<Self, SidecarError> {
        use crate::sealed::pack;
        let parsed = pack::parse_pack(&bytes)
            .map_err(|pack::PackError::Corrupt(m)| SidecarError::Corrupt(m))?;
        let n_streams = parsed.n_streams as usize;

        // Mandatory sections are guaranteed present + CRC-valid by parse_pack.
        let dir_sec = parsed
            .section(pack::KIND_STREAM_DIRECTORY)
            .ok_or(SidecarError::Corrupt("missing stream directory"))?;
        let pb = parsed
            .section(pack::KIND_POINTER_BLOCKS)
            .ok_or(SidecarError::Corrupt("missing pointer blocks"))?;
        let ps = parsed
            .section(pack::KIND_POINTER_SKIPS)
            .ok_or(SidecarError::Corrupt("missing pointer skips"))?;

        let dir_body = &bytes[dir_sec.offset..dir_sec.offset + dir_sec.length];
        let raws = pack::decode_stream_directory(
            dir_sec.codec_id,
            dir_body,
            n_streams,
        )
        .map_err(|pack::PackError::Corrupt(m)| SidecarError::Corrupt(m))?;

        let mut dir = HashMap::with_capacity(n_streams);
        let mut stream_ids = Vec::with_capacity(n_streams);
        for r in raws {
            // Rebase the section-relative offsets onto absolute pack offsets so
            // `ptr_slice`/`skip_slice` index straight into `bytes`, and
            // bounds-check each span inside its section.
            let ptr_end = (r.ptr_off as usize)
                .checked_add(r.ptr_len as usize)
                .ok_or(SidecarError::Corrupt("dir ptr span overflow"))?;
            let skip_end = (r.skip_off as usize)
                .checked_add(r.skip_len as usize)
                .ok_or(SidecarError::Corrupt("dir skip span overflow"))?;
            if ptr_end > pb.length || skip_end > ps.length {
                return Err(SidecarError::Corrupt("dir span out of section"));
            }
            let entry = DirEntry {
                first_version: r.first_version,
                last_version:  r.last_version,
                ptr_off:       pb.offset as u64 + r.ptr_off,
                ptr_len:       r.ptr_len,
                n_batches:     r.n_batches,
                skip_off:      ps.offset as u64 + r.skip_off,
                skip_len:      r.skip_len,
            };
            dir.insert(r.stream_id, entry);
            stream_ids.push(r.stream_id);
        }
        // Both directory codecs emit ascending stream ids; keep the contract
        // the sidecar path guarantees for global replay / retention.
        stream_ids.sort_unstable();

        // Optional accelerators: attach only when they verify + cross-check the
        // segment id, else drop (local degradation).
        let filter = parsed.section(pack::KIND_STREAM_FILTER).and_then(|s| {
            SegmentFilter::from_bytes(&bytes[s.offset..s.offset + s.length])
                .ok()
                .filter(|f| f.segment_id() == parsed.segment_id)
        });
        let payload =
            parsed.section(pack::KIND_PAYLOAD_COLUMNS).and_then(|s| {
                SealedPayloadIndex::from_bytes(
                    bytes[s.offset..s.offset + s.length].to_vec(),
                )
                .ok()
                .filter(|p| p.segment_id() == parsed.segment_id)
            });
        let event_types =
            parsed.section(pack::KIND_EVENT_TYPE_IDS).and_then(|s| {
                pack::decode_event_types(&bytes[s.offset..s.offset + s.length])
                    .ok()
                    .filter(|v| v.len() as u64 == parsed.event_count)
            });

        Ok(SealedSegmentIndex {
            segment_id: parsed.segment_id,
            base_pos: parsed.base_pos,
            event_count: parsed.event_count,
            bytes,
            dir,
            stream_ids,
            filter,
            payload,
            event_types,
        })
    }

    /// Read and parse a SealPack from `path` (bn-3of). The whole artifact —
    /// pointers, filter, payload columns, event-type ids — lives in the one
    /// file, so there are no sibling opens (unlike [`Self::open`]).
    pub fn open_pack(path: &Path) -> Result<Self, SidecarError> {
        let bytes = std::fs::read(path)?;
        Self::from_pack(bytes)
    }

    /// The `event_type_id` of the event at **segment-local** stored index
    /// `local_idx` (global position `base_pos + local_idx`), from the pack's
    /// `EVENT_TYPE_IDS` section — the cold read path's message-type source that
    /// never touches the raw batch (bn-3of). `None` when no verified event-type
    /// section is attached (legacy sidecars, or the section was dropped as
    /// corrupt) or `local_idx` is out of range; the caller then decodes the raw
    /// batch exactly as before.
    #[inline]
    pub fn event_type_id(&self, local_idx: u64) -> Option<u32> {
        self.event_types
            .as_ref()
            .and_then(|v| v.get(local_idx as usize).copied())
    }

    /// Whether a verified `EVENT_TYPE_IDS` section is attached (bn-3of).
    #[inline]
    pub fn has_event_types(&self) -> bool { self.event_types.is_some() }

    /// Read and parse a sidecar from `path`, opportunistically attaching the
    /// sibling `.filter` file ([`filter_path_for`]) if one exists, parses,
    /// and cross-checks by `segment_id` (bn-1i7). A missing, corrupt, or
    /// mismatched filter is silently dropped — the sidecar open still
    /// succeeds and [`Self::might_contain_stream`] degrades to always `true`
    /// (I5: the filter is advisory and independently rebuildable).
    pub fn open(path: &Path) -> Result<Self, SidecarError> {
        let bytes = std::fs::read(path)?;
        let mut index = Self::from_bytes(bytes)?;
        if let Ok(filter) = SegmentFilter::open(&filter_path_for(path))
            && filter.segment_id() == index.segment_id
        {
            index.filter = Some(filter);
        }
        // bn-zge: opportunistically re-attach the sibling `.pcol` payload
        // sidecar. Like the filter, this is best-effort at open — a missing,
        // corrupt, or wrong-segment `.pcol` is silently dropped and the sealed
        // read path simply reports no columnar payload for this segment (the
        // raw log remains the payload authority, D1).
        //
        // bn-bka2: the attach is LAZY. `SealedPayloadIndex::open` reads the
        // sidecar's header, footer, and block index — bytes proportional to the
        // block count, not to the payload — and keeps the file handle; block
        // bytes are read on the first columnar read that touches them. Reading
        // whole `.pcol`s here was 98.5% of warm engine-open time and ~all of
        // post-open RSS at 8 GiB (bn-2u01). The identity cross-check below is
        // unchanged, and so is the drop-on-anything-wrong contract; what shifts
        // is that damage inside the DATA region surfaces at the first read of
        // the affected block (as a typed error the read path degrades on)
        // rather than at open. A sidecar sealed before bn-bka2 carries no
        // per-block checksums and so still attaches eagerly and whole-image
        // CRC-verified — nothing an existing store checks today is lost. See
        // the `payload` module docs.
        if let Ok(Ok(payload)) =
            SealedPayloadIndex::open(&payload_path_for(path))
            && payload.segment_id() == index.segment_id
        {
            index.payload = Some(payload);
        }
        Ok(index)
    }

    /// Attach a seal-time membership filter built for this segment (bn-1i7).
    /// Normally called by [`crate::sealed::driver::SealDriver::seal`] right
    /// after a successful [`crate::sealed::filter::SegmentFilter::build`], or
    /// by [`Self::open`] when a valid sibling `.filter` file is found.
    pub fn attach_filter(&mut self, filter: SegmentFilter) {
        self.filter = Some(filter);
    }

    /// Attach the D6 payload-block sidecar for this segment (bn-zge). Called by
    /// [`crate::sealed::driver::SealDriver::seal`] right after it durably
    /// writes the `.pcol`, and by [`Self::open`] when a valid sibling
    /// `.pcol` is found. The attached index is what the sealed read path
    /// reassembles payloads from ([`Self::reassemble_payload`] /
    /// [`ReplaySet`](crate::sealed::replay::ReplaySet)).
    pub fn attach_payload(&mut self, payload: SealedPayloadIndex) {
        self.payload = Some(payload);
    }

    /// Whether a D6 payload sidecar (`.pcol`) is attached to this segment —
    /// i.e. the sealed read path can reassemble this segment's payloads
    /// columnar-side rather than from the raw log.
    #[inline]
    pub fn has_payload(&self) -> bool { self.payload.is_some() }

    /// The attached payload-block index, if any.
    #[inline]
    pub fn payload_index(&self) -> Option<&SealedPayloadIndex> {
        self.payload.as_ref()
    }

    /// Reassemble the payload of the event at **segment-local** stored index
    /// `local_idx` (global position `base_pos + local_idx`) from the attached
    /// `.pcol`, byte-exact, dispatching through `resolver` for a row-fallback
    /// dictionary block. `Ok(None)` when no payload sidecar is attached (the
    /// caller falls back to the raw log); `Err` on an out-of-range index or a
    /// block decode/dict failure.
    pub fn reassemble_payload(
        &self,
        local_idx: u64,
        resolver: &impl DictResolver,
    ) -> Result<Option<Vec<u8>>, PayloadError> {
        match &self.payload {
            Some(p) => p.reassemble_event(local_idx, resolver).map(Some),
            None => Ok(None),
        }
    }

    /// Advisory pre-check consulting this segment's `BinaryFuse16` stream-id
    /// filter (bn-1i7), if one is attached. `false` means `stream_id` is
    /// **definitely absent** from this segment — safe to skip
    /// [`Self::resolve`]/[`Self::stream_head`]/[`Self::stream_entries`]
    /// entirely without touching the directory. `true` means "maybe": either
    /// the filter says so, or no filter is attached (missing/corrupt
    /// degrades to always-`true`, i.e. unfiltered — I5, never a wrong
    /// answer). Callers that skip on `false` MUST NOT skip on `true` — the
    /// directory remains the source of truth.
    #[inline]
    pub fn might_contain_stream(&self, stream_id: u64) -> bool {
        match &self.filter {
            Some(f) => f.might_contain(stream_id),
            None => true,
        }
    }

    #[inline]
    fn ptr_slice(&self, e: &DirEntry) -> &[u8] {
        &self.bytes[e.ptr_off as usize..e.ptr_off as usize + e.ptr_len as usize]
    }

    #[inline]
    fn skip_slice(&self, e: &DirEntry) -> &[u8] {
        &self.bytes
            [e.skip_off as usize..e.skip_off as usize + e.skip_len as usize]
    }

    /// The committed head version of `stream_id` in this segment (its last
    /// event's version), or `None` if the stream is absent. A sealed segment is
    /// fully durable, so there is no watermark clamp: every indexed pointer is
    /// visible.
    pub fn stream_head(&self, stream_id: u64) -> Option<u64> {
        self.dir.get(&stream_id).map(|e| e.last_version)
    }

    /// Resolve `(stream_id, version)` to the batch's [`EventPtr`], or `None`
    /// when the stream/version is not in this segment. `Err` only on corrupt
    /// bytes (a defensive path; a validated sidecar never errors here).
    pub fn resolve(
        &self,
        stream_id: u64,
        version: u64,
    ) -> Result<Option<EventPtr>, DecodeError> {
        let Some(e) = self.dir.get(&stream_id) else {
            return Ok(None);
        };
        if version < e.first_version || version > e.last_version {
            return Ok(None);
        }
        let bp = ptr_block::point_read(
            self.ptr_slice(e),
            self.skip_slice(e),
            e.n_batches as usize,
            version,
        )?;
        Ok(bp.map(|b| EventPtr {
            segment_id: self.segment_id,
            offset:     b.offset,
        }))
    }

    /// All of `stream_id`'s batch entries in this segment, version order — the
    /// sealed-path stream replay. Returns an empty vec if the stream is absent.
    pub fn stream_entries(
        &self,
        stream_id: u64,
    ) -> Result<Vec<StreamEntry>, DecodeError> {
        let Some(e) = self.dir.get(&stream_id) else {
            return Ok(Vec::new());
        };
        let batches = ptr_block::decode_ptr_block(self.ptr_slice(e))?;
        Ok(batches
            .into_iter()
            .map(|b| StreamEntry {
                first_version:    b.first_version,
                frame_count:      b.frame_count,
                first_global_pos: b.first_global_pos,
                ptr:              EventPtr {
                    segment_id: self.segment_id,
                    offset:     b.offset,
                },
            })
            .collect())
    }

    /// The segment's batches in global-position order (A1) — the sealed-path
    /// global replay. Reconstructed by merging every stream's version-ordered
    /// batches and sorting by `first_global_pos` (position-order replay off the
    /// sealed index; the retained log segment remains the primary global-order
    /// product, D1).
    pub fn global_entries(&self) -> Result<Vec<GlobalEntry>, DecodeError> {
        let mut out: Vec<GlobalEntry> = Vec::new();
        for &sid in &self.stream_ids {
            let e = &self.dir[&sid];
            for b in ptr_block::decode_ptr_block(self.ptr_slice(e))? {
                out.push(GlobalEntry {
                    first_global_pos: b.first_global_pos,
                    frame_count:      b.frame_count,
                    stream_id:        sid,
                    ptr:              EventPtr {
                        segment_id: self.segment_id,
                        offset:     b.offset,
                    },
                });
            }
        }
        out.sort_by_key(|g| g.first_global_pos);
        Ok(out)
    }
}

/// A cheaply-clonable handle to a sealed segment index.
pub type SealedSegmentRef = Arc<SealedSegmentIndex>;

#[cfg(test)]
mod tests {
    use super::*;

    fn seal_stream(id: u64, batches: &[(u64, u32, u64, u64)]) -> SealStream {
        SealStream {
            stream_id: id,
            batches:   batches
                .iter()
                .map(|&(v, fc, g, off)| SealBatch {
                    first_version:    v,
                    frame_count:      fc,
                    first_global_pos: g,
                    offset:           off,
                })
                .collect(),
        }
    }

    fn sample_input() -> SealInput {
        SealInput {
            segment_id:     7,
            base_pos:       1000,
            streams:        vec![
                seal_stream(10, &[(0, 3, 1000, 4096), (3, 2, 1003, 8192)]),
                seal_stream(20, &[(0, 1, 1005, 12288)]),
                seal_stream(30, &[(0, 5, 1006, 16384), (5, 5, 1011, 20480)]),
            ],
            payloads:       None,
            event_type_ids: None,
        }
    }

    #[test]
    fn round_trip_resolve_and_head() {
        let input = sample_input();
        let bytes = encode_sidecar(&input);
        let idx = SealedSegmentIndex::from_bytes(bytes).unwrap();

        assert_eq!(idx.segment_id(), 7);
        assert_eq!(idx.base_pos(), 1000);
        assert_eq!(idx.event_count(), 3 + 2 + 1 + 5 + 5);
        assert_eq!(idx.stream_count(), 3);

        assert_eq!(idx.stream_head(10), Some(4));
        assert_eq!(idx.stream_head(20), Some(0));
        assert_eq!(idx.stream_head(30), Some(9));
        assert_eq!(idx.stream_head(999), None);

        assert_eq!(idx.resolve(10, 0).unwrap().unwrap().offset, 4096);
        assert_eq!(idx.resolve(10, 2).unwrap().unwrap().offset, 4096);
        assert_eq!(idx.resolve(10, 3).unwrap().unwrap().offset, 8192);
        assert_eq!(idx.resolve(10, 4).unwrap().unwrap().offset, 8192);
        assert_eq!(idx.resolve(10, 5).unwrap(), None);
        assert_eq!(idx.resolve(30, 7).unwrap().unwrap().offset, 20480);
        assert_eq!(idx.resolve(999, 0).unwrap(), None);
    }

    #[test]
    fn stream_and_global_replay() {
        let input = sample_input();
        let idx =
            SealedSegmentIndex::from_bytes(encode_sidecar(&input)).unwrap();

        let s10 = idx.stream_entries(10).unwrap();
        assert_eq!(s10.len(), 2);
        assert_eq!(s10[0].ptr.offset, 4096);
        assert_eq!(s10[1].first_version, 3);
        assert!(idx.stream_entries(999).unwrap().is_empty());

        let g = idx.global_entries().unwrap();
        assert_eq!(g.len(), 5);
        for w in g.windows(2) {
            assert!(w[0].first_global_pos < w[1].first_global_pos);
        }
        assert_eq!(g[0].first_global_pos, 1000);
    }

    #[test]
    fn corrupt_crc_is_rejected() {
        let mut bytes = encode_sidecar(&sample_input());
        // Flip a byte in the PTR region.
        bytes[HEADER_LEN] ^= 0xFF;
        assert!(matches!(
            SealedSegmentIndex::from_bytes(bytes),
            Err(SidecarError::Corrupt(_))
        ));
    }

    #[test]
    fn truncated_is_rejected() {
        let bytes = encode_sidecar(&sample_input());
        let short = bytes[..HEADER_LEN + FOOTER_LEN - 1].to_vec();
        assert!(matches!(
            SealedSegmentIndex::from_bytes(short),
            Err(SidecarError::Corrupt(_))
        ));
    }

    #[test]
    fn from_snapshot_filters_by_segment() {
        use crate::ActiveIndex;
        use crate::active::BatchEntry;
        let idx = ActiveIndex::new();
        // stream 10: one batch in seg 1, one in seg 2. stream 20: only seg 2.
        idx.apply_committed(
            3,
            &[BatchEntry {
                stream_id:            10,
                first_stream_version: 0,
                frame_count:          3,
                first_global_pos:     0,
                ptr:                  EventPtr {
                    segment_id: 1,
                    offset:     100,
                },
            }],
        );
        idx.apply_committed(
            7,
            &[
                BatchEntry {
                    stream_id:            10,
                    first_stream_version: 3,
                    frame_count:          2,
                    first_global_pos:     3,
                    ptr:                  EventPtr {
                        segment_id: 2,
                        offset:     200,
                    },
                },
                BatchEntry {
                    stream_id:            20,
                    first_stream_version: 0,
                    frame_count:          2,
                    first_global_pos:     5,
                    ptr:                  EventPtr {
                        segment_id: 2,
                        offset:     300,
                    },
                },
            ],
        );
        let snap = idx.snapshot();

        let seg1 = SealInput::from_snapshot(&snap, 1, 0);
        assert_eq!(seg1.streams.len(), 1);
        assert_eq!(seg1.streams[0].stream_id, 10);
        assert_eq!(seg1.streams[0].batches.len(), 1);
        assert_eq!(seg1.event_count(), 3);

        let seg2 = SealInput::from_snapshot(&snap, 2, 3);
        assert_eq!(seg2.streams.len(), 2);
        assert_eq!(seg2.event_count(), 4);
        // Sealing seg 2 and resolving matches the active index.
        let sidx =
            SealedSegmentIndex::from_bytes(encode_sidecar(&seg2)).unwrap();
        assert_eq!(sidx.resolve(10, 4).unwrap().unwrap().offset, 200);
        assert_eq!(sidx.resolve(20, 1).unwrap().unwrap().offset, 300);
    }

    /// bn-1i7: no filter attached (the plain `from_bytes` path) degrades to
    /// always-`true` — `might_contain_stream` must never cause a skip when
    /// there is nothing to consult.
    #[test]
    fn no_filter_attached_is_always_maybe() {
        let idx =
            SealedSegmentIndex::from_bytes(encode_sidecar(&sample_input()))
                .unwrap();
        assert!(idx.might_contain_stream(10));
        assert!(
            idx.might_contain_stream(999),
            "absent stream is still \"maybe\" without a filter"
        );
    }

    /// bn-3of: a SealPack built from the same seal input answers every
    /// pointer / head / global-replay query byte-identically to the legacy
    /// `.pidx` sidecar, and additionally serves the new `event_type_id` column
    /// and (when present) the attached membership filter — the index-level half
    /// of the Spike I semantic-equivalence gate.
    #[test]
    fn pack_matches_sidecar_pointer_results_and_adds_type_ids() {
        use crate::sealed::pack::{self, PackInput};

        let input = sample_input(); // segment 7, base 1000, 3 streams
        // Per-event type ids in stored (global-position) order. event_count=16.
        let type_ids: Vec<u32> =
            (0..input.event_count() as u32).map(|i| (i % 3) + 1).collect();
        let filter = crate::sealed::filter::SegmentFilter::build(
            input.segment_id,
            &input.streams.iter().map(|s| s.stream_id).collect::<Vec<_>>(),
        );

        let sidecar =
            SealedSegmentIndex::from_bytes(encode_sidecar(&input)).unwrap();
        let pack_bytes = pack::encode_pack(&PackInput {
            segment_id:     input.segment_id,
            base_pos:       input.base_pos,
            streams:        &input.streams,
            event_type_ids: &type_ids,
            filter:         filter.as_ref(),
            payload_bytes:  None,
        });
        let packed = SealedSegmentIndex::from_pack(pack_bytes).unwrap();

        // Identical scalar surface.
        assert_eq!(packed.segment_id(), sidecar.segment_id());
        assert_eq!(packed.base_pos(), sidecar.base_pos());
        assert_eq!(packed.event_count(), sidecar.event_count());
        assert_eq!(packed.stream_ids(), sidecar.stream_ids());

        // Identical resolve for every (stream, version) including misses.
        for &sid in sidecar.stream_ids() {
            for v in 0..12u64 {
                assert_eq!(
                    packed.resolve(sid, v).unwrap(),
                    sidecar.resolve(sid, v).unwrap(),
                    "resolve mismatch stream {sid} version {v}"
                );
            }
            assert_eq!(packed.stream_head(sid), sidecar.stream_head(sid));
            assert_eq!(
                packed.stream_entries(sid).unwrap(),
                sidecar.stream_entries(sid).unwrap()
            );
        }
        assert_eq!(
            packed.global_entries().unwrap(),
            sidecar.global_entries().unwrap()
        );

        // New: event-type ids come from the pack, matching the input column.
        assert!(packed.has_event_types());
        for (i, &t) in type_ids.iter().enumerate() {
            assert_eq!(packed.event_type_id(i as u64), Some(t));
        }
        assert_eq!(packed.event_type_id(type_ids.len() as u64), None);

        // The filter attached through the pack (no false negatives).
        for &sid in packed.stream_ids() {
            assert!(packed.might_contain_stream(sid));
        }
    }

    /// bn-3of review F1: a REAL single flipped byte inside an OPTIONAL section
    /// (the filter) — no trailer repair, exactly what bitrot produces — must
    /// leave the pack openable with only that section dropped: the filter
    /// degrades to always-"maybe" while pointer resolution stays byte-identical
    /// to an uncorrupted index. The trailer hash covers header + directory
    /// only, so it does not (and must not) trip here; the section's own
    /// directory-committed CRC is what catches the flip.
    #[test]
    fn pack_optional_section_corruption_degrades_locally() {
        use crate::sealed::pack::{self, KIND_STREAM_FILTER, PackInput};

        let input = sample_input();
        let ids: Vec<u64> = input.streams.iter().map(|s| s.stream_id).collect();
        let filter =
            crate::sealed::filter::SegmentFilter::build(input.segment_id, &ids);
        let mut bytes = pack::encode_pack(&PackInput {
            segment_id:     input.segment_id,
            base_pos:       input.base_pos,
            streams:        &input.streams,
            event_type_ids: &[],
            filter:         filter.as_ref(),
            payload_bytes:  None,
        });

        // Flip ONE byte in the middle of the filter section body. Nothing
        // else is touched — a genuine bitrot injection.
        let parsed = pack::parse_pack(&bytes).unwrap();
        let fsec = parsed.section(KIND_STREAM_FILTER).unwrap();
        bytes[fsec.offset + fsec.length / 2] ^= 0xFF;

        // (a) the pack still opens;
        let packed = SealedSegmentIndex::from_pack(bytes).unwrap();
        // (b) only the filter was dropped -> always "maybe";
        assert!(packed.might_contain_stream(999));
        // (c) reads remain byte-identical to an uncorrupted index.
        let clean =
            SealedSegmentIndex::from_bytes(encode_sidecar(&input)).unwrap();
        for &sid in clean.stream_ids() {
            for v in 0..12u64 {
                assert_eq!(
                    packed.resolve(sid, v).unwrap(),
                    clean.resolve(sid, v).unwrap(),
                    "resolve mismatch after filter corruption: {sid}/{v}"
                );
            }
            assert_eq!(packed.stream_head(sid), clean.stream_head(sid));
        }
        assert_eq!(
            packed.global_entries().unwrap(),
            clean.global_entries().unwrap()
        );
    }

    /// bn-1i7 acceptance: with a real filter attached, every present stream
    /// answers "maybe" (zero false negatives) and the vast majority of a
    /// large absent-key sample answers "no" — i.e. filtering actually skips
    /// segments in stream-replay planning, at the FPR the round-3 spike
    /// measured (~0.002%), well under the 1% sanity band.
    #[test]
    fn filter_skips_most_absent_streams_with_no_false_negatives() {
        let n_streams = 4_000u64;
        let streams: Vec<SealStream> = (0..n_streams)
            .map(|i| seal_stream(i * 2, &[(0, 3, i, 4096 + i)])) // even ids only
            .collect();
        let input = SealInput {
            segment_id: 1,
            base_pos: 0,
            streams,
            payloads: None,
            event_type_ids: None,
        };
        let stream_ids: Vec<u64> =
            input.streams.iter().map(|s| s.stream_id).collect();
        let filter =
            crate::sealed::filter::SegmentFilter::build(1, &stream_ids)
                .unwrap();

        let mut idx =
            SealedSegmentIndex::from_bytes(encode_sidecar(&input)).unwrap();
        idx.attach_filter(filter);

        for &id in &stream_ids {
            assert!(
                idx.might_contain_stream(id),
                "false negative for present stream {id}"
            );
        }

        // Odd ids were never inserted -- definitely absent.
        let absent: Vec<u64> = (0..n_streams).map(|i| i * 2 + 1).collect();
        let skipped =
            absent.iter().filter(|&&id| !idx.might_contain_stream(id)).count();
        let skip_rate = skipped as f64 / absent.len() as f64;
        assert!(
            skip_rate > 0.99,
            "filter should skip the overwhelming majority of absent streams: \
             {skip_rate}"
        );
    }
}
