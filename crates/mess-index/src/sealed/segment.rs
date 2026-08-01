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
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use crate::active::{EventPtr, GlobalEntry, StreamEntry};
use crate::sealed::filter::SegmentFilter;
use crate::sealed::pack::{EventTypeColumn, PackIdentity};
use crate::sealed::payload::{DictResolver, PayloadError, SealedPayloadIndex};
use crate::sealed::ptr_block::{
    self, BatchPtr, DecodeError, SkipEntry, encode_ptr_block, encode_skips,
};
use crate::sealed::regdelta::RegistryDelta;

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

/// The `STREAM_DIRECTORY` codec a sealed artifact on disk used (bn-we9x) —
/// [`crate::sealed::pack::DIRCODEC_SORTED`] or
/// [`crate::sealed::pack::DIRCODEC_BITRANK`], nameable with
/// [`crate::sealed::pack::dircodec_name`].
///
/// Reads the header, the section directory, and the trailer — **no section
/// body**, no directory decode, no map rebuild — so an operator tool can
/// report the chooser's decision for every segment without paying an open.
/// The pack's header/directory hash is still verified, so the answer is never
/// read out of unvalidated bytes.
///
/// Accepts either sealed shape: a `.seal` pack, or a legacy `.pidx` sidecar —
/// which reports `DIRCODEC_SORTED`, because its fixed DIR region is exactly
/// the sorted codec's 56-byte-record layout (it predates the chooser, so
/// "sorted" is a description of its bytes, not a decision it made).
pub fn dir_codec_of(path: &Path) -> Result<u16, SidecarError> {
    use crate::sealed::pack;
    let corrupt = |pack::PackError::Corrupt(m)| SidecarError::Corrupt(m);

    let file = File::open(path)?;
    let len = file.metadata()?.len();
    let mut magic = [0u8; 4];
    file.read_exact_at(&mut magic, 0)?;
    match u32::from_le_bytes(magic) {
        SIDECAR_MAGIC => Ok(pack::DIRCODEC_SORTED),
        pack::PACK_MAGIC => {
            if len < (pack::HEADER_LEN + pack::TRAILER_LEN) as u64 {
                return Err(SidecarError::Corrupt(
                    "shorter than header + trailer",
                ));
            }
            let mut head = [0u8; pack::HEADER_LEN];
            file.read_exact_at(&mut head, 0)?;
            let h = pack::parse_pack_header(&head).map_err(corrupt)?;
            let mut dirbuf = vec![0u8; h.dir_len()];
            file.read_exact_at(&mut dirbuf, pack::HEADER_LEN as u64)?;
            let mut trailer = [0u8; pack::TRAILER_LEN];
            file.read_exact_at(&mut trailer, len - pack::TRAILER_LEN as u64)?;
            let parsed =
                pack::parse_pack_directory(&head, &dirbuf, &trailer, len)
                    .map_err(corrupt)?;
            parsed
                .section(pack::KIND_STREAM_DIRECTORY)
                .map(|s| s.codec_id)
                .ok_or(SidecarError::Corrupt("missing stream directory"))
        }
        _ => Err(SidecarError::Corrupt("not a sealed pack or sidecar")),
    }
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

/// The in-memory stream directory: interned stream id → [`DirEntry`], hashed
/// with `foldhash` instead of std's SipHash-1-3 (bn-dcr, Spike H's `h0f` arm).
///
/// # Why a non-SipHash hasher is safe *here specifically*
///
/// The keys are **internal interned stream ids** — sequential `u64`s the
/// `$registry` stream hands out, never a caller-supplied name, byte string, or
/// any other externally-chosen value. An attacker who can pick stream *names*
/// cannot pick the *ids*: interning assigns them in registration order, so the
/// key an adversary influences is "the next integer", not a preimage they get
/// to choose. That removes the HashDoS threat model SipHash exists to defend
/// against, which is the whole reason the swap is admissible.
///
/// Two consequences, both load-bearing:
///
/// * **Do not reuse this alias, or `foldhash`, for any string/byte-keyed map in
///   this crate.** Payload dictionaries, skeleton/path tries, name lookups —
///   anything whose key an untrusted producer can shape — must keep the default
///   SipHash `RandomState`. `foldhash::fast` is explicitly not
///   HashDoS-resistant.
/// * The hasher is still **per-process randomly seeded** (`fast::RandomState`,
///   not `FixedState`), so bucket layout is not reproducible across runs. That
///   is fine because nothing observable depends on it: this map is only ever
///   probed by key ([`SealedSegmentIndex::stream_head`],
///   [`SealedSegmentIndex::resolve`], [`SealedSegmentIndex::stream_entries`]),
///   never iterated. Every ordered product — [`SealedSegmentIndex::stream_ids`]
///   and [`SealedSegmentIndex::global_entries`] — walks the ascending
///   `stream_ids` vector, and serialization ([`seal_sidecar`] /
///   [`crate::sealed::pack`]) is built from the ascending [`SealStream`] input,
///   not from this map. Keep it that way: an iteration over `dir` would make
///   output order hasher-dependent.
///
/// Measured (bn-dcr, 39 engine-written segment directories + synthetic
/// sparse/clustered shapes, ABBA-ordered with a second SipHash map as a null
/// control): +47.7% median batch-lookup throughput, +38.5% p99, +27.4% serial
/// latency; 39/39 real segments clear the +20% admission bar.
type DirMap = HashMap<u64, DirEntry, foldhash::fast::RandomState>;

/// The read-only sealed pointer index for one segment. Owns the sidecar bytes
/// in memory; every query is a slice + decode with no further I/O.
#[derive(Debug)]
pub struct SealedSegmentIndex {
    segment_id:  u64,
    base_pos:    u64,
    event_count: u64,
    bytes:       Vec<u8>,
    dir:         DirMap,
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
    ///
    /// bn-dbz: for a lazily opened pack the column is **file-backed** — the
    /// resident part is the type dictionary plus one `u32` per 4096 events —
    /// where it used to be a `Vec<u32>` per event (~1.5 GB at 385M events).
    event_types: Option<EventTypeColumn>,
    /// Which `STREAM_DIRECTORY` codec the bytes this index was built from used
    /// (bn-we9x) — the §12.6 chooser's decision, made observable. See
    /// [`Self::dir_codec`].
    dir_codec:   u16,
    /// The verified [`PackIdentity`] of the SealPack this index was built from
    /// (bn-11g), and that pack's `format_version`.
    ///
    /// `Some` for every pack path ([`Self::from_pack`], [`Self::open_pack`],
    /// [`Self::open_pack_eager`]) — the identity is the trailer hash those
    /// paths already recomputed and verified, so holding one is proof the
    /// bytes behind this index hash to it. `None` for a legacy
    /// `.pidx`+`.filter`+`.pcol` trio ([`Self::from_bytes`]/[`Self::open`]),
    /// which has no such identity to offer and therefore can never satisfy a
    /// footer that names a pack (spec 01 §3.3.3 reader rule 3).
    pack:        Option<(PackIdentity, u16)>,
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

    /// Which `STREAM_DIRECTORY` codec this segment's directory was serialized
    /// with (bn-we9x) — [`crate::sealed::pack::DIRCODEC_SORTED`] or
    /// [`crate::sealed::pack::DIRCODEC_BITRANK`], nameable with
    /// [`crate::sealed::pack::dircodec_name`].
    ///
    /// Purely informational: the codec is a **serialization** choice, so both
    /// values rehydrate into the identical in-memory directory and no read
    /// path branches on this. It exists so the §12.6 chooser's decision is
    /// explainable operationally — `mess inspect` reports it per segment, and
    /// [`dir_codec_of`] answers the same question straight off disk without an
    /// open.
    pub fn dir_codec(&self) -> u16 { self.dir_codec }

    /// The verified [`PackIdentity`] of the SealPack behind this index
    /// (bn-11g), or `None` for a legacy sidecar trio.
    ///
    /// This is the value a segment footer's `SealPackIdentity` section must
    /// equal before the index may be installed (spec 01 §3.3.3). It is derived
    /// — the open recomputed the blake3 over the header + directory it read —
    /// so comparing it to the footer compares two independently produced
    /// values, which is exactly what a self-attesting pack cannot offer.
    pub fn pack_identity(&self) -> Option<PackIdentity> {
        self.pack.map(|(id, _)| id)
    }

    /// The `format_version` of the SealPack behind this index, or `None` for a
    /// legacy sidecar trio (bn-11g). Cross-checked against the footer entry's
    /// `pack_format_version`.
    pub fn pack_format_version(&self) -> Option<u16> {
        self.pack.map(|(_, v)| v)
    }

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
        let mut dir = DirMap::with_capacity_and_hasher(
            n_streams,
            foldhash::fast::RandomState::default(),
        );
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
            // A legacy sidecar's DIR region IS the sorted codec's layout
            // (56-byte records, ascending) — see [`dir_codec_of`].
            dir_codec: crate::sealed::pack::DIRCODEC_SORTED,
            pack: None,
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

        let mut dir = DirMap::with_capacity_and_hasher(
            n_streams,
            foldhash::fast::RandomState::default(),
        );
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
                EventTypeColumn::from_body(
                    bytes[s.offset..s.offset + s.length].to_vec(),
                )
                .ok()
                .filter(|c| c.event_count() == parsed.event_count)
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
            dir_codec: dir_sec.codec_id,
            pack: Some((parsed.identity, parsed.format_version)),
        })
    }

    /// Read a SealPack from `path` **lazily** (bn-3of open path, bn-dbz
    /// residency fix). The whole artifact — pointers, filter, payload columns,
    /// event-type ids — lives in the one file, so there are no sibling opens
    /// (unlike [`Self::open`]); what this does *not* do is read the one file
    /// whole.
    ///
    /// Eagerly read and verified, because a pointer resolution needs every byte
    /// of them and they are sized by the segment's **stream count**:
    ///
    /// - the 64-byte header, the 40-byte trailer, and the section directory
    ///   (the trailer's blake3 binds the two, so the directory's per-section
    ///   checksums are trustworthy from here on);
    /// - the three mandatory sections — `STREAM_DIRECTORY`, `POINTER_BLOCKS`,
    ///   `POINTER_SKIPS` — each checked against its directory-committed
    ///   `crc32c` + `content_hash_prefix`. A failure is
    ///   [`SidecarError::Corrupt`] and the caller raw-scans, as before;
    /// - the `STREAM_FILTER` (a few bytes per stream), dropped on any fault.
    ///
    /// Left on disk behind the retained handle, because they are sized by the
    /// segment's **content**:
    ///
    /// - `PAYLOAD_COLUMNS`, attached through [`SealedPayloadIndex::attach_at`]
    ///   — block table now, block bytes at first touch, each checksummed then
    ///   (bn-bka2);
    /// - `EVENT_TYPE_IDS`, attached through [`EventTypeColumn::attach`] —
    ///   dictionary and CRC table now, index blocks at first touch, each
    ///   checksummed then (bn-dbz).
    ///
    /// `STATS` is informational and never read, so it is neither read nor
    /// verified here (it was, when the whole image was in hand).
    ///
    /// Reading the pack whole cost a resident copy of every sealed byte plus a
    /// second copy of the payload columns plus 4 B/event of decoded type ids;
    /// Spike J measured that as +18%/+36% reopen RSS at 2M/10M events. See the
    /// [`pack`](crate::sealed::pack) module docs for where each integrity check
    /// moved to.
    ///
    /// The retained handle is **one file descriptor per sealed segment**, and
    /// only when the pack actually carries a lazily attachable section — the
    /// same fd cost the `.pcol` path has had since bn-bka2, not a new one (the
    /// pack's two file-backed sections share the single handle).
    pub fn open_pack(path: &Path) -> Result<Self, SidecarError> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        Self::attach_pack(Arc::new(file), len)
    }

    /// The lazy pack attach proper, split out so tests can hand in a handle.
    fn attach_pack(file: Arc<File>, len: u64) -> Result<Self, SidecarError> {
        use crate::sealed::pack;
        let corrupt = |pack::PackError::Corrupt(m)| SidecarError::Corrupt(m);

        if len < (pack::HEADER_LEN + pack::TRAILER_LEN) as u64 {
            return Err(SidecarError::Corrupt("shorter than header + trailer"));
        }
        let mut head = [0u8; pack::HEADER_LEN];
        file.read_exact_at(&mut head, 0)?;
        let h = pack::parse_pack_header(&head).map_err(corrupt)?;
        let mut dirbuf = vec![0u8; h.dir_len()];
        file.read_exact_at(&mut dirbuf, pack::HEADER_LEN as u64)?;
        let mut trailer = [0u8; pack::TRAILER_LEN];
        file.read_exact_at(&mut trailer, len - pack::TRAILER_LEN as u64)?;
        let parsed = pack::parse_pack_directory(&head, &dirbuf, &trailer, len)
            .map_err(corrupt)?;
        drop(dirbuf);
        let n_streams = parsed.n_streams as usize;

        // The three mandatory sections, read and fully verified: without any
        // one of them the pack cannot resolve a pointer at all.
        let read_mandatory = |kind| -> Result<(_, Vec<u8>), SidecarError> {
            let s = parsed.section(kind).ok_or(SidecarError::Corrupt(
                "missing/corrupt mandatory section",
            ))?;
            let mut body = vec![0u8; s.length];
            file.read_exact_at(&mut body, s.offset as u64)?;
            if !s.verify(&body) {
                return Err(SidecarError::Corrupt(
                    "missing/corrupt mandatory section",
                ));
            }
            Ok((s, body))
        };
        let (dir_sec, dir_body) = read_mandatory(pack::KIND_STREAM_DIRECTORY)?;
        let (_, ptr_body) = read_mandatory(pack::KIND_POINTER_BLOCKS)?;
        let (_, skip_body) = read_mandatory(pack::KIND_POINTER_SKIPS)?;

        let raws = pack::decode_stream_directory(
            dir_sec.codec_id,
            &dir_body,
            n_streams,
        )
        .map_err(corrupt)?;
        drop(dir_body);

        // The retained bytes are the two pointer sections back to back — the
        // only pack regions `resolve`/`stream_entries`/`global_entries` slice.
        // Directory offsets rebase onto that concatenation instead of onto
        // absolute pack offsets, so every read path below is unchanged.
        let ptr_len = ptr_body.len();
        let skip_len = skip_body.len();
        let mut bytes = ptr_body;
        bytes.reserve_exact(skip_len);
        bytes.extend_from_slice(&skip_body);
        drop(skip_body);

        let mut dir = DirMap::with_capacity_and_hasher(
            n_streams,
            foldhash::fast::RandomState::default(),
        );
        let mut stream_ids = Vec::with_capacity(n_streams);
        for r in raws {
            let ptr_end = (r.ptr_off as usize)
                .checked_add(r.ptr_len as usize)
                .ok_or(SidecarError::Corrupt("dir ptr span overflow"))?;
            let skip_end = (r.skip_off as usize)
                .checked_add(r.skip_len as usize)
                .ok_or(SidecarError::Corrupt("dir skip span overflow"))?;
            if ptr_end > ptr_len || skip_end > skip_len {
                return Err(SidecarError::Corrupt("dir span out of section"));
            }
            dir.insert(
                r.stream_id,
                DirEntry {
                    first_version: r.first_version,
                    last_version:  r.last_version,
                    ptr_off:       r.ptr_off,
                    ptr_len:       r.ptr_len,
                    n_batches:     r.n_batches,
                    skip_off:      ptr_len as u64 + r.skip_off,
                    skip_len:      r.skip_len,
                },
            );
            stream_ids.push(r.stream_id);
        }
        stream_ids.sort_unstable();

        // Optional accelerators. Each is attached only when it verifies and
        // cross-checks the segment id; anything else drops it and degrades
        // locally, exactly as the eager path does.
        let filter = parsed.section(pack::KIND_STREAM_FILTER).and_then(|s| {
            let mut body = vec![0u8; s.length];
            file.read_exact_at(&mut body, s.offset as u64).ok()?;
            if !s.verify(&body) {
                return None;
            }
            SegmentFilter::from_bytes(&body)
                .ok()
                .filter(|f| f.segment_id() == parsed.segment_id)
        });
        let payload =
            parsed.section(pack::KIND_PAYLOAD_COLUMNS).and_then(|s| {
                SealedPayloadIndex::attach_at(
                    Arc::clone(&file),
                    s.offset as u64,
                    s.length as u64,
                )
                .ok()
                .filter(|p| p.segment_id() == parsed.segment_id)
            });
        let event_types =
            parsed.section(pack::KIND_EVENT_TYPE_IDS).and_then(|s| {
                EventTypeColumn::attach(&file, &s)
                    .ok()
                    .filter(|c| c.event_count() == parsed.event_count)
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
            dir_codec: dir_sec.codec_id,
            pack: Some((parsed.identity, parsed.format_version)),
        })
    }

    /// Read and parse a SealPack from `path` **eagerly**: the whole image is
    /// pulled into memory and every section — including the ones
    /// [`Self::open_pack`] leaves on disk — verified against its
    /// directory-committed checksums before anything else happens. An offline
    /// verifier or a differential test wants the strongest check the format
    /// offers and is about to touch every byte anyway; engine open uses
    /// [`Self::open_pack`].
    pub fn open_pack_eager(path: &Path) -> Result<Self, SidecarError> {
        Self::from_pack(std::fs::read(path)?)
    }

    /// The `event_type_id` of the event at **segment-local** stored index
    /// `local_idx` (global position `base_pos + local_idx`), from the pack's
    /// `EVENT_TYPE_IDS` section — the cold read path's message-type source that
    /// never touches the raw batch (bn-3of). `None` when no verified event-type
    /// section is attached (legacy sidecars, or the section was dropped as
    /// corrupt) or `local_idx` is out of range; the caller then decodes the raw
    /// batch exactly as before.
    ///
    /// bn-dbz: on a lazily opened pack this may `pread` (and checksum) the
    /// covering index block, so a caller reading a contiguous run should use
    /// [`Self::event_type_ids_range`] and pay that once for the whole run.
    /// `None` now additionally covers a failed read or a block whose checksum
    /// does not match — the same degradation an absent section produces.
    #[inline]
    pub fn event_type_id(&self, local_idx: u64) -> Option<u32> {
        self.event_types.as_ref().and_then(|c| c.get(local_idx))
    }

    /// The `event_type_id`s of the **segment-local** stored events in
    /// `[lo, hi)`, in order — the batch-shaped form of [`Self::event_type_id`]
    /// (bn-dbz). `None` under exactly the same conditions, and for the same
    /// caller response (decode the raw batch).
    ///
    /// This is the shape the read path actually wants: one batch's frames are a
    /// contiguous run, and a file-backed column serves the whole run from the
    /// one index block it almost always lies inside.
    #[inline]
    pub fn event_type_ids_range(&self, lo: u64, hi: u64) -> Option<Vec<u32>> {
        self.event_types.as_ref().and_then(|c| c.range(lo, hi))
    }

    /// Whether a verified `EVENT_TYPE_IDS` section is attached (bn-3of).
    #[inline]
    pub fn has_event_types(&self) -> bool { self.event_types.is_some() }

    /// Whether every attached accelerator holds its bytes in memory rather than
    /// reading them on demand — the bn-dbz laziness observable, mirroring
    /// [`SealedPayloadIndex::is_resident`]. `true` for
    /// [`Self::from_bytes`]/[`Self::from_pack`]/[`Self::open_pack_eager`] and
    /// for a legacy sidecar; `false` for a pack opened by [`Self::open_pack`]
    /// that carries a lazily attachable payload or event-type section.
    pub fn sections_resident(&self) -> bool {
        self.payload.as_ref().is_none_or(SealedPayloadIndex::is_resident)
            && self.event_types.as_ref().is_none_or(|c| c.is_resident())
    }

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

    /// Whether `delta` is a registry delta this sidecar vouches for (bn-26pp).
    ///
    /// The delta only ever supplies payload *bytes*; the batch layout it
    /// describes must already be one this pointer sidecar agrees with. Four
    /// things are checked, and any failure means the caller must ignore the
    /// delta and read the `$registry` batches from the log instead (which is
    /// always right — D1):
    ///
    /// 1. `segment_id` — it is this segment's delta, not a neighbour's;
    /// 2. `base_pos` and `event_count` — it was built from the same seal of the
    ///    same span, not an earlier partial (`seal_active`) one;
    /// 3. the delta's stream is present in this sidecar at all; and
    /// 4. its `(first_global_pos, frame_count)` list is **exactly** the list
    ///    this sidecar's directory yields for that stream, in the same order —
    ///    so a delta can neither invent a batch, drop one, nor move one.
    ///
    /// This is a `&self` predicate and the delta is **not** retained: it is a
    /// recovery input, read once and dropped, so a store with hundreds of
    /// thousands of names does not carry its registrations in RSS forever
    /// (unlike the `.filter`, which every read consults).
    #[must_use]
    pub fn accepts_registry_delta(&self, delta: &RegistryDelta) -> bool {
        if delta.segment_id() != self.segment_id
            || delta.base_pos() != self.base_pos
            || delta.event_count() != self.event_count
        {
            return false;
        }
        let Ok(entries) = self.stream_entries(delta.stream_id()) else {
            return false;
        };
        if entries.is_empty() || entries.len() != delta.batch_count() {
            return false;
        }
        // `stream_entries` is version-ordered; for a single stream that is
        // global-position order too, but sort defensively rather than rely on
        // it — the delta is written strictly position-ascending.
        let mut want: Vec<(u64, u32)> = entries
            .iter()
            .map(|e| (e.first_global_pos, e.frame_count))
            .collect();
        want.sort_unstable();
        delta.layout().eq(want)
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

    /// bn-dcr: the directory map's hasher (`DirMap` = foldhash, replacing std's
    /// SipHash) must be invisible from outside. `fast::RandomState` reseeds per
    /// instance, so every `from_bytes`/`from_pack` call builds a differently
    /// laid-out map from identical bytes — if any observable product depended
    /// on bucket order, this test would flap. Covers both directory codecs
    /// (a dense stream-id set selects `DIRCODEC_BITRANK`, a sparse one
    /// `DIRCODEC_SORTED`) and both readers, and pins the AC: present/absent
    /// lookups exact, ascending iteration deterministic, serialization
    /// hasher-independent.
    #[test]
    fn directory_products_are_hasher_independent() {
        use crate::sealed::pack::{self, PackInput};

        // Four key shapes over the §12.6 chooser's two arms. bn-we9x added
        // the middle pair: at n = 64 the byte break-even is U = 64*(n-3) =
        // 3904, so stride 61 (U = 3844) is the last bitrank win and stride 62
        // (U = 3907) the first sorted one — i.e. two directories that differ
        // by one id of span yet serialize through different codecs, which is
        // exactly where a hasher-order leak would show up as a flapping
        // re-encode.
        for stride in [1u64, 61, 62, 1013] {
            let streams: Vec<SealStream> = (0..64u64)
                .map(|i| {
                    let id = 5 + i * stride;
                    seal_stream(
                        id,
                        &[
                            (0, 2, 1000 + i * 3, 4096 * (i + 1)),
                            (2, 1, 1002 + i * 3, 8192 * (i + 1)),
                        ],
                    )
                })
                .collect();
            let input = SealInput {
                segment_id: 11,
                base_pos: 1000,
                streams,
                payloads: None,
                event_type_ids: None,
            };
            let sidecar_bytes = encode_sidecar(&input);
            let pack_bytes = pack::encode_pack(&PackInput {
                segment_id:     input.segment_id,
                base_pos:       input.base_pos,
                streams:        &input.streams,
                event_type_ids: &[],
                filter:         None,
                payload_bytes:  None,
            });
            // The four strides really do straddle the chooser (U = 63*stride
            // + 1 against a break-even of 3904) — otherwise the comment above
            // would be an unchecked claim.
            assert_eq!(
                SealedSegmentIndex::from_pack(pack_bytes.clone())
                    .unwrap()
                    .dir_codec()
                    == pack::DIRCODEC_BITRANK,
                stride <= 61,
                "chooser arm for stride {stride}"
            );

            // Serialization is built from the ascending input, never from the
            // map — re-encoding must be byte-identical.
            assert_eq!(encode_sidecar(&input), sidecar_bytes);
            assert_eq!(
                pack::encode_pack(&PackInput {
                    segment_id:     input.segment_id,
                    base_pos:       input.base_pos,
                    streams:        &input.streams,
                    event_type_ids: &[],
                    filter:         None,
                    payload_bytes:  None,
                }),
                pack_bytes
            );

            let present: Vec<u64> =
                input.streams.iter().map(|s| s.stream_id).collect();
            // Absent probes: below, above, and between the present ids.
            let absent: Vec<u64> = std::iter::once(0)
                .chain(std::iter::once(u64::MAX))
                .chain(present.iter().map(|&id| id + 1))
                .chain(present.iter().filter_map(|&id| id.checked_sub(1)))
                .filter(|k| !present.contains(k))
                .collect();

            let mut expected_ids: Option<Vec<u64>> = None;
            let mut expected_global: Option<Vec<GlobalEntry>> = None;
            // 8 independent opens per reader = 16 independent hasher seeds.
            for _ in 0..8 {
                for idx in [
                    SealedSegmentIndex::from_bytes(sidecar_bytes.clone())
                        .unwrap(),
                    SealedSegmentIndex::from_pack(pack_bytes.clone()).unwrap(),
                ] {
                    let ids = idx.stream_ids().to_vec();
                    assert!(
                        ids.windows(2).all(|w| w[0] < w[1]),
                        "stream_ids not strictly ascending (stride {stride})"
                    );
                    assert_eq!(ids, present);
                    match &expected_ids {
                        None => expected_ids = Some(ids),
                        Some(e) => assert_eq!(e, &idx.stream_ids().to_vec()),
                    }

                    let ge = idx.global_entries().unwrap();
                    match &expected_global {
                        None => expected_global = Some(ge),
                        Some(e) => assert_eq!(
                            e, &ge,
                            "global_entries order depends on the hasher \
                             (stride {stride})"
                        ),
                    }

                    for (i, &id) in present.iter().enumerate() {
                        assert_eq!(idx.stream_head(id), Some(2));
                        assert_eq!(
                            idx.resolve(id, 0).unwrap().map(|p| p.offset),
                            Some(4096 * (i as u64 + 1))
                        );
                        assert_eq!(
                            idx.resolve(id, 2).unwrap().map(|p| p.offset),
                            Some(8192 * (i as u64 + 1))
                        );
                        assert_eq!(idx.resolve(id, 3).unwrap(), None);
                        assert_eq!(idx.stream_entries(id).unwrap().len(), 2);
                        assert_eq!(idx.stream_range(id), Some((0, 2)));
                    }
                    for &k in &absent {
                        assert_eq!(idx.stream_head(k), None, "absent {k}");
                        assert_eq!(idx.resolve(k, 0).unwrap(), None);
                        assert!(idx.stream_entries(k).unwrap().is_empty());
                        assert_eq!(idx.stream_range(k), None);
                    }
                }
            }
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

    // -----------------------------------------------------------------------
    // bn-dbz: the lazy pack open
    // -----------------------------------------------------------------------

    /// A pack big enough to have several payload blocks and several event-type
    /// CRC blocks, with a filter — i.e. every section a real seal emits.
    fn big_pack(segment_id: u64) -> (Vec<u8>, Vec<Vec<u8>>, Vec<u32>) {
        use crate::sealed::pack::{self, PackInput};
        use crate::sealed::payload::{PayloadSealOpts, encode_payload_sidecar};

        const N: usize = 9000; // > 2 event-type CRC blocks
        const STREAMS: u64 = 12;
        let per = N as u64 / STREAMS;
        let streams: Vec<SealStream> = (0..STREAMS)
            .map(|s| SealStream {
                stream_id: 100 + s,
                batches:   (0..per / 10)
                    .map(|b| SealBatch {
                        first_version:    b * 10,
                        frame_count:      10,
                        first_global_pos: s * per + b * 10,
                        offset:           4096 + (s * per + b * 10) * 8,
                    })
                    .collect(),
            })
            .collect();
        let payloads: Vec<Vec<u8>> = (0..N)
            .map(|i| {
                format!("{{\"n\":{i},\"kind\":\"ev\",\"pad\":\"{:0>32}\"}}", i)
                    .into_bytes()
            })
            .collect();
        let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let pcol = encode_payload_sidecar(
            segment_id,
            &refs,
            &PayloadSealOpts::default(),
        )
        .unwrap();
        let type_ids: Vec<u32> = (0..N as u32).map(|i| (i % 6) + 1).collect();
        let ids: Vec<u64> = streams.iter().map(|s| s.stream_id).collect();
        let filter =
            crate::sealed::filter::SegmentFilter::build(segment_id, &ids);
        let bytes = pack::encode_pack(&PackInput {
            segment_id,
            base_pos: 0,
            streams: &streams,
            event_type_ids: &type_ids,
            filter: filter.as_ref(),
            payload_bytes: Some(&pcol),
        });
        (bytes, payloads, type_ids)
    }

    fn write_pack(dir: &Path, seg: u64, bytes: &[u8]) -> std::path::PathBuf {
        let p = crate::sealed::pack::seal_pack_path(dir, seg);
        std::fs::write(&p, bytes).unwrap();
        p
    }

    /// bn-we9x: the §12.6 chooser's decision is observable — the same answer
    /// from every reader (eager pack, lazy pack, legacy sidecar) and from
    /// [`dir_codec_of`], which reads it straight off disk without an open.
    ///
    /// Swept across the byte break-even (`U == 64*(n-3)`) so the assertion is
    /// not just "some pack says bitrank" but "the reported codec tracks the
    /// chooser exactly, on both sides of the threshold and at the tie".
    #[test]
    #[cfg_attr(miri, ignore)]
    fn dir_codec_is_reported_by_every_reader_and_off_disk() {
        use crate::sealed::pack::{
            self, DIRCODEC_BITRANK, DIRCODEC_SORTED, PackInput, dircodec_name,
        };
        let dir = mess_testkit::sweeping_temp_dir("seg-dir-codec");
        let n = 64usize;
        let break_even = 64 * (n as u64 - 3);
        for (seg, span, want) in [
            (1u64, n as u64, DIRCODEC_BITRANK), // fully dense (U/n = 1)
            (2, break_even, DIRCODEC_BITRANK),  // last bitrank win
            (3, break_even + 1, DIRCODEC_SORTED), // first sorted win
            (4, 64 * n as u64 * 4, DIRCODEC_SORTED), // sparse
        ] {
            // n ids spanning exactly `span`, endpoints pinned.
            let step = (span - 1) / (n as u64 - 1);
            let mut ids: Vec<u64> =
                (0..n as u64).map(|i| 100 + i * step).collect();
            *ids.last_mut().unwrap() = 100 + span - 1;
            let streams: Vec<SealStream> = ids
                .iter()
                .enumerate()
                .map(|(i, &id)| {
                    seal_stream(id, &[(0, 2, 1000 + i as u64 * 2, 4096)])
                })
                .collect();
            let bytes = pack::encode_pack(&PackInput {
                segment_id:     seg,
                base_pos:       1000,
                streams:        &streams,
                event_type_ids: &[],
                filter:         None,
                payload_bytes:  None,
            });
            let path = write_pack(dir.path(), seg, &bytes);
            let want_name = dircodec_name(want);

            assert_eq!(
                SealedSegmentIndex::from_pack(bytes.clone())
                    .unwrap()
                    .dir_codec(),
                want,
                "from_pack at span={span} (want {want_name})"
            );
            assert_eq!(
                SealedSegmentIndex::open_pack(&path).unwrap().dir_codec(),
                want,
                "open_pack at span={span} (want {want_name})"
            );
            assert_eq!(
                SealedSegmentIndex::open_pack_eager(&path).unwrap().dir_codec(),
                want,
                "open_pack_eager at span={span} (want {want_name})"
            );
            assert_eq!(
                dir_codec_of(&path).unwrap(),
                want,
                "dir_codec_of at span={span} (want {want_name})"
            );

            // A legacy sidecar has no chooser: its DIR region is always the
            // sorted layout, and both the reader and the off-disk probe say so.
            let side_path = dir.path().join(format!("seg-{seg}.pidx"));
            let side = encode_sidecar(&SealInput {
                segment_id:     seg,
                base_pos:       1000,
                streams:        streams.clone(),
                payloads:       None,
                event_type_ids: None,
            });
            std::fs::write(&side_path, &side).unwrap();
            assert_eq!(
                SealedSegmentIndex::from_bytes(side).unwrap().dir_codec(),
                DIRCODEC_SORTED
            );
            assert_eq!(dir_codec_of(&side_path).unwrap(), DIRCODEC_SORTED);
        }

        // Neither shape: a refusal, not a guess.
        let junk = dir.path().join("junk.pidx");
        std::fs::write(&junk, b"not a sealed artifact at all").unwrap();
        assert!(dir_codec_of(&junk).is_err());
        assert!(dir_codec_of(&dir.path().join("absent.seal")).is_err());
    }

    /// A lazily opened pack answers every query — pointers, heads, replay,
    /// filter, event types, payloads — byte-identically to the same pack read
    /// whole, and reports itself as non-resident while doing so.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn lazy_pack_open_matches_eager_on_every_query() {
        use crate::sealed::payload::NoDicts;
        let dir = mess_testkit::sweeping_temp_dir("seg-pack-lazy-parity");
        let seg = 41u64;
        let (bytes, payloads, type_ids) = big_pack(seg);
        let path = write_pack(dir.path(), seg, &bytes);

        let lazy = SealedSegmentIndex::open_pack(&path).unwrap();
        let eager = SealedSegmentIndex::open_pack_eager(&path).unwrap();
        assert!(!lazy.sections_resident(), "open_pack must attach lazily");
        assert!(eager.sections_resident(), "open_pack_eager must be resident");

        assert_eq!(lazy.segment_id(), eager.segment_id());
        assert_eq!(lazy.base_pos(), eager.base_pos());
        assert_eq!(lazy.event_count(), eager.event_count());
        assert_eq!(lazy.stream_ids(), eager.stream_ids());
        assert_eq!(lazy.stream_count(), eager.stream_count());
        for &sid in eager.stream_ids() {
            for v in [0u64, 1, 9, 10, 55, 749, 750, 10_000] {
                assert_eq!(
                    lazy.resolve(sid, v).unwrap(),
                    eager.resolve(sid, v).unwrap(),
                    "resolve {sid}/{v}"
                );
            }
            assert_eq!(lazy.stream_head(sid), eager.stream_head(sid));
            assert_eq!(lazy.stream_range(sid), eager.stream_range(sid));
            assert_eq!(
                lazy.stream_entries(sid).unwrap(),
                eager.stream_entries(sid).unwrap()
            );
            assert!(lazy.might_contain_stream(sid));
        }
        assert_eq!(
            lazy.global_entries().unwrap(),
            eager.global_entries().unwrap()
        );
        assert_eq!(lazy.resolve(9_999_999, 0).unwrap(), None);

        assert!(lazy.has_event_types());
        for (i, &t) in type_ids.iter().enumerate() {
            assert_eq!(lazy.event_type_id(i as u64), Some(t), "type id {i}");
        }
        assert_eq!(
            lazy.event_type_ids_range(0, type_ids.len() as u64).unwrap(),
            type_ids
        );
        assert_eq!(lazy.event_type_id(type_ids.len() as u64), None);

        assert!(lazy.has_payload());
        for i in [0usize, 1, 127, 128, 5000, payloads.len() - 1] {
            assert_eq!(
                lazy.reassemble_payload(i as u64, &NoDicts).unwrap(),
                Some(payloads[i].clone()),
                "payload {i}"
            );
        }
    }

    /// The direct laziness proof (bn-bka2's pattern, applied to the pack).
    /// `open_pack` never reads the `PAYLOAD_COLUMNS` or `EVENT_TYPE_IDS`
    /// bodies, so a pack whose optional bodies are torn still opens and still
    /// resolves every pointer exactly; the damage surfaces as a `None`/`Err`
    /// at the read that touches it, which every caller already treats like an
    /// absent accelerator (raw log stays authority, D1/I5). The whole-image
    /// read (`open_pack_eager`) catches the same damage at open, and drops the
    /// same two sections — the difference is purely *when*.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn lazy_pack_open_skips_optional_bodies_and_tears_surface_at_read() {
        use crate::sealed::pack;
        use crate::sealed::payload::NoDicts;
        let dir = mess_testkit::sweeping_temp_dir("seg-pack-lazy-torn");
        let seg = 42u64;
        let (mut bytes, _payloads, type_ids) = big_pack(seg);
        let parsed = pack::parse_pack(&bytes).unwrap();
        let pl = parsed.section(pack::KIND_PAYLOAD_COLUMNS).unwrap();
        let et = parsed.section(pack::KIND_EVENT_TYPE_IDS).unwrap();
        // Tear a byte deep inside each optional body — no directory or trailer
        // repair, exactly what bitrot produces.
        bytes[pl.offset + pl.length / 2] ^= 0xFF;
        bytes[et.offset + et.length / 2] ^= 0xFF;
        let path = write_pack(dir.path(), seg, &bytes);

        // (a) The pack still OPENS: neither body was read.
        let lazy = SealedSegmentIndex::open_pack(&path).unwrap();
        assert!(lazy.has_payload(), "the block table is intact and attached");
        assert!(lazy.has_event_types(), "the prologue is intact and attached");

        // (b) Pointer resolution is untouched and byte-identical to a clean
        //     pack — the mandatory sections were verified at open.
        let clean_path = write_pack(dir.path(), seg + 1, &big_pack(seg + 1).0);
        let clean = SealedSegmentIndex::open_pack(&clean_path).unwrap();
        for &sid in clean.stream_ids() {
            for v in [0u64, 10, 55, 740] {
                assert_eq!(
                    lazy.resolve(sid, v).unwrap().map(|p| p.offset),
                    clean.resolve(sid, v).unwrap().map(|p| p.offset),
                    "resolve {sid}/{v} after optional-section damage"
                );
            }
        }

        // (c) The damage surfaces at the read that touches it, and only there.
        let n = type_ids.len() as u64;
        let torn_et_reads = (0..n)
            .step_by(97)
            .filter(|&i| lazy.event_type_id(i).is_none())
            .count();
        assert!(torn_et_reads > 0, "the torn event-type block must refuse");
        assert!(
            (0..n).step_by(97).any(|i| lazy.event_type_id(i).is_some()),
            "untouched event-type blocks must keep serving"
        );
        let torn_payload_reads = (0..n)
            .step_by(97)
            .filter(|&i| lazy.reassemble_payload(i, &NoDicts).is_err())
            .count();
        assert!(torn_payload_reads > 0, "the torn payload block must refuse");
        assert!(
            (0..n)
                .step_by(97)
                .any(|i| lazy.reassemble_payload(i, &NoDicts).is_ok()),
            "untouched payload blocks must keep serving"
        );

        // (d) Control: reading the same file whole drops both sections at open
        //     — same verdict, discovered earlier.
        let eager = SealedSegmentIndex::open_pack_eager(&path).unwrap();
        assert!(!eager.has_payload());
        assert!(!eager.has_event_types());
    }

    /// A corrupt MANDATORY section still rejects the whole pack on the lazy
    /// path: those three sections ARE read and fully verified at open, because
    /// nothing resolves without them (the reader raw-scans instead).
    #[test]
    #[cfg_attr(miri, ignore)]
    fn lazy_pack_open_rejects_a_corrupt_mandatory_section() {
        use crate::sealed::pack;
        let dir = mess_testkit::sweeping_temp_dir("seg-pack-lazy-mandatory");
        let seg = 43u64;
        for kind in [
            pack::KIND_STREAM_DIRECTORY,
            pack::KIND_POINTER_BLOCKS,
            pack::KIND_POINTER_SKIPS,
        ] {
            let (mut bytes, ..) = big_pack(seg);
            let s = pack::parse_pack(&bytes).unwrap().section(kind).unwrap();
            bytes[s.offset + s.length / 2] ^= 0xFF;
            let path = write_pack(dir.path(), seg, &bytes);
            assert!(
                matches!(
                    SealedSegmentIndex::open_pack(&path),
                    Err(SidecarError::Corrupt(_))
                ),
                "a torn mandatory section (kind {kind}) must reject the pack"
            );
        }
    }

    /// Header/directory damage is fatal on the lazy path too — the trailer's
    /// blake3 is what makes the directory (and therefore every section
    /// checksum a lazy read relies on) trustworthy in the first place.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn lazy_pack_open_rejects_header_or_directory_damage() {
        use crate::sealed::pack;
        let dir = mess_testkit::sweeping_temp_dir("seg-pack-lazy-hdr");
        let seg = 44u64;
        for at in [8usize, pack::HEADER_LEN, pack::HEADER_LEN + 12] {
            let (mut bytes, ..) = big_pack(seg);
            bytes[at] ^= 0xFF;
            let path = write_pack(dir.path(), seg, &bytes);
            assert!(
                matches!(
                    SealedSegmentIndex::open_pack(&path),
                    Err(SidecarError::Corrupt(_))
                ),
                "a flip at {at} must reject the pack"
            );
        }
        // A truncated tail loses the trailer entirely.
        let (bytes, ..) = big_pack(seg);
        let path = write_pack(dir.path(), seg, &bytes[..bytes.len() - 8]);
        assert!(SealedSegmentIndex::open_pack(&path).is_err());
        let path = write_pack(dir.path(), seg, &bytes[..16]);
        assert!(SealedSegmentIndex::open_pack(&path).is_err());
    }

    // -----------------------------------------------------------------------
    // Bench: the sealed PACK read surface, lazy vs eager, on a 1M-event pack.
    // The AC's latency question — does moving the payload columns and the
    // event-type column off the heap cost the reads that use them? Run with:
    //   TMPDIR=$HOME/.cache/mess-test-tmp cargo test -p mess-index --release \
    //     sealed::segment::tests::pack_read_bench -- --ignored --nocapture
    // -----------------------------------------------------------------------
    #[test]
    #[ignore = "perf bench; run explicitly with --release --ignored --nocapture"]
    fn pack_read_bench() {
        use std::time::Instant;

        use crate::sealed::pack::{self, PackInput};
        use crate::sealed::payload::{
            NoDicts, PayloadSealOpts, encode_payload_sidecar,
        };

        const N: usize = 1_000_000;
        const STREAMS: u64 = 1_000;
        const PER: u64 = N as u64 / STREAMS;
        let seg = 7u64;
        let streams: Vec<SealStream> = (0..STREAMS)
            .map(|s| SealStream {
                stream_id: s,
                batches:   (0..PER / 10)
                    .map(|b| SealBatch {
                        first_version:    b * 10,
                        frame_count:      10,
                        first_global_pos: s * PER + b * 10,
                        offset:           4096 + (s * PER + b * 10) * 8,
                    })
                    .collect(),
            })
            .collect();
        let payloads: Vec<Vec<u8>> = (0..N)
            .map(|i| {
                format!(
                    "{{\"seq\":{i},\"account\":\"acct-{:07}\",\"amount\":{},\"\
                     memo\":\"{:0>64}\"}}",
                    i % 1000,
                    i * 7 % 100_000,
                    i
                )
                .into_bytes()
            })
            .collect();
        let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let pcol =
            encode_payload_sidecar(seg, &refs, &PayloadSealOpts::default())
                .unwrap();
        let type_ids: Vec<u32> = (0..N as u32).map(|i| (i % 6) + 1).collect();
        let ids: Vec<u64> = streams.iter().map(|s| s.stream_id).collect();
        let filter = crate::sealed::filter::SegmentFilter::build(seg, &ids);
        let bytes = pack::encode_pack(&PackInput {
            segment_id:     seg,
            base_pos:       0,
            streams:        &streams,
            event_type_ids: &type_ids,
            filter:         filter.as_ref(),
            payload_bytes:  Some(&pcol),
        });
        let dir = mess_testkit::sweeping_temp_dir("seg-pack-read-bench");
        let path = write_pack(dir.path(), seg, &bytes);
        println!(
            "pack: {:.1} MiB, {N} events, {STREAMS} streams",
            bytes.len() as f64 / 1048576.0
        );

        let mut xs: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            xs ^= xs << 13;
            xs ^= xs >> 7;
            xs ^= xs << 17;
            xs
        };
        let probes: Vec<u64> = (0..20_000).map(|_| next() % N as u64).collect();

        for (label, idx) in [
            ("eager", SealedSegmentIndex::open_pack_eager(&path).unwrap()),
            ("lazy ", SealedSegmentIndex::open_pack(&path).unwrap()),
        ] {
            let t = Instant::now();
            let _ = SealedSegmentIndex::open_pack(&path);
            let _ = t;
            let mut times: Vec<u64> = Vec::with_capacity(probes.len());
            let mut sink = 0u64;
            for &p in &probes {
                let t = Instant::now();
                sink += u64::from(idx.event_type_id(p).unwrap());
                times.push(t.elapsed().as_nanos() as u64);
            }
            report(&format!("{label} event_type_id  "), &mut times);
            times.clear();
            for &p in &probes {
                let lo = p.min(N as u64 - 10);
                let t = Instant::now();
                sink +=
                    idx.event_type_ids_range(lo, lo + 10).unwrap()[0] as u64;
                times.push(t.elapsed().as_nanos() as u64);
            }
            report(&format!("{label} type range x10"), &mut times);
            times.clear();
            for &p in &probes {
                let sid = p % STREAMS;
                let v = p % PER;
                let t = Instant::now();
                sink += idx.resolve(sid, v).unwrap().map_or(0, |e| e.offset);
                times.push(t.elapsed().as_nanos() as u64);
            }
            report(&format!("{label} resolve       "), &mut times);
            times.clear();
            for &p in &probes[..5_000] {
                let t = Instant::now();
                sink += idx
                    .reassemble_payload(p, &NoDicts)
                    .unwrap()
                    .map_or(0, |v| v.len() as u64);
                times.push(t.elapsed().as_nanos() as u64);
            }
            report(&format!("{label} payload point "), &mut times);
            assert!(sink > 0);
        }
    }

    #[cfg(test)]
    fn report(label: &str, times: &mut Vec<u64>) {
        times.sort_unstable();
        let pct = |p: f64| {
            times[((times.len() as f64 * p) as usize).min(times.len() - 1)]
        };
        let mean = times.iter().sum::<u64>() as f64 / times.len() as f64;
        println!(
            "{label}  n={:<6} mean={:>8.0}ns p50={:>8}ns p99={:>8}ns \
             p999={:>9}ns",
            times.len(),
            mean,
            pct(0.50),
            pct(0.99),
            pct(0.999)
        );
    }

    /// One lazily opened pack behind an `Arc` serves concurrent readers with no
    /// lock on the read path: positioned reads do not touch a shared file
    /// offset, and both file-backed sections share the one handle.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn lazy_pack_serves_concurrent_readers() {
        use crate::sealed::payload::NoDicts;
        let dir = mess_testkit::sweeping_temp_dir("seg-pack-lazy-concurrent");
        let seg = 45u64;
        let (bytes, payloads, type_ids) = big_pack(seg);
        let path = write_pack(dir.path(), seg, &bytes);
        let idx = Arc::new(SealedSegmentIndex::open_pack(&path).unwrap());

        std::thread::scope(|s| {
            for t in 0..4u64 {
                let idx = Arc::clone(&idx);
                let payloads = &payloads;
                let type_ids = &type_ids;
                s.spawn(move || {
                    let n = type_ids.len() as u64;
                    for round in 0..3u64 {
                        for k in (0..n).step_by(53) {
                            let i = (k + t * 37 + round) % n;
                            assert_eq!(
                                idx.event_type_id(i),
                                Some(type_ids[i as usize])
                            );
                            assert_eq!(
                                idx.reassemble_payload(i, &NoDicts).unwrap(),
                                Some(payloads[i as usize].clone())
                            );
                        }
                    }
                });
            }
        });
    }
}
