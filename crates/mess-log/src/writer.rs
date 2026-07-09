//! The segment writer: THE canonical write path (D1). Writes a
//! `SegmentHeader` on open, appends batches contiguously over the runtime
//! [`Fs`] seam, and hands off an unsealed segment on close/roll.
//!
//! # Runs on real fs AND the sim fault fs
//!
//! [`SegmentWriter`] is generic over [`Fs`](crate::runtime::Fs), never over
//! `std::fs`. It therefore runs unchanged on [`RealRuntime`] and on the
//! fault-injecting [`SimRuntime`] in-memory fs — the whole reason the runtime
//! seam (`bn-z98`) exists. The behavioural suite in `tests/writer_suite.rs`
//! drives both.
//!
//! # What this writer does and does not do
//!
//! - **Open** (§6): encode the `SegmentHeader` (§3.2), `pwrite` it at offset 0,
//!   and `fdatasync` — the header (with its fresh, larger `epoch`) is durable
//!   before any batch, which is what gives A9 its teeth against recycled files.
//! - **Append** (§4, §4.6): stamp `segment_epoch` (A9/R3), the per-segment
//!   `batch_id` (D-FMT-5), and the running `first_global_pos` (A1) into each
//!   `BatchHeader`; reject empty batches (A5); enforce that a batch lies wholly
//!   within `SEGMENT_SIZE` (A8) and never spans a segment; append the batch
//!   bytes at the running offset with **no alignment padding** (A11) so batch
//!   boundaries coincide with byte boundaries.
//! - **Durability is the committer's job** (`bn-11m`, `03-durability.md`): a
//!   plain [`append`](SegmentWriter::append) does **not** `fdatasync`; the
//!   caller batches syncs (group commit). [`sync`](SegmentWriter::sync) exposes
//!   the barrier, and [`close`](SegmentWriter::close) syncs once so the handoff
//!   is durable.
//! - **Sealing is Phase 4** (`bn-25j`). This writer does **not** write a
//!   `SegmentFooter`. On [`close`](SegmentWriter::close)/[`roll`] the segment is
//!   left **trailer-less**, i.e. *unsealed*. Per `02-recovery.md` §8.3, a
//!   segment with no valid trailer "is treated as **not sealed**: it MUST be
//!   fully scanned exactly as the active segment is." Leaving no trailer is the
//!   spec-sanctioned partial state; writing a bogus/partial trailer would be
//!   strictly worse (a reader would treat any `footer_crc` mismatch as unsealed
//!   anyway). So the writer deliberately writes nothing at close beyond the
//!   final data sync, and hands the sealer a [`SegmentSummary`] carrying every
//!   fixed-trailer field it will need (`ext_offset == content_len`, counts,
//!   `epoch`, `base_pos`, `end_pos`).

use std::io;
use std::path::Path;

use crate::encode::{BatchEncoder, BatchInput, EncodeError, Subframe};
use crate::format::*;
use crate::runtime::{Fs, FileHandle, OpenOpts};

/// Parameters to open (create) a fresh segment (§3.2).
#[derive(Debug, Clone, Copy)]
pub struct SegmentParams {
    /// Monotonic, writer-assigned, never reused (§3.2).
    pub segment_id: u64,
    /// Global position of the segment's first event (A1 seed, §3.2). For the
    /// first segment this is `0`; on a roll it is the previous segment's
    /// `end_pos`.
    pub base_pos: u64,
    /// Segment generation (A9, §3.2). MUST be strictly larger than any
    /// previously durable segment's epoch.
    pub epoch: u64,
    /// Epoch of the immediately preceding segment, or `0` for the first (§3.2).
    pub prev_segment_epoch: u64,
    /// Wall-clock creation time; advisory (§3.2). Callers with a real clock
    /// pass a UNIX-nanos reading; tests may pass `0`.
    pub created_unix_nanos: u64,
    /// Logical segment size (§3.1). Defaults to [`SEGMENT_SIZE`] (256 MiB) via
    /// [`SegmentParams::new`]; tests set it small to force rolls.
    pub segment_size: u64,
}

impl SegmentParams {
    /// Parameters for a 256 MiB segment (the normative size, §3.1).
    pub fn new(segment_id: u64, base_pos: u64, epoch: u64, prev_segment_epoch: u64) -> Self {
        SegmentParams {
            segment_id,
            base_pos,
            epoch,
            prev_segment_epoch,
            created_unix_nanos: 0,
            segment_size: SEGMENT_SIZE,
        }
    }
}

/// One batch to append. The writer stamps `segment_epoch`, `batch_id`, and
/// `first_global_pos`; the caller supplies the stream-level fields.
#[derive(Debug, Clone, Copy)]
pub struct BatchSpec<'a, 'p> {
    /// Batch-constant stream id (D-FMT-6).
    pub stream_id: u64,
    /// Batch-constant category id (D-FMT-6).
    pub category_id: u64,
    /// Stream version of this batch's first event (§4.2).
    pub first_stream_version: u64,
    /// `Some` sets `flags.CRYPTO_CHAIN` and writes these 32 bytes (§4.4). Phase
    /// 3 provides placement only; the chain **value** is Phase 5.
    pub crypto_chain: Option<&'a [u8; CHAIN_LEN]>,
    /// The subframes (A5: non-empty).
    pub subframes: &'a [Subframe<'p>],
}

/// What one successful [`append`](SegmentWriter::append) committed to the
/// buffer/file. All fields are stamped by the writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Receipt {
    /// Per-segment sequence number stamped in the header (D-FMT-5).
    pub batch_id: u64,
    /// A1 global position of this batch's first event.
    pub first_global_pos: u64,
    /// Number of events (subframes) in the batch.
    pub frame_count: u32,
    /// On-disk length of the batch (§4.6).
    pub total_len: u64,
    /// Byte offset within the segment where the batch was written.
    pub offset: u64,
}

/// The state an unsealed segment hands off at close/roll — everything the
/// Phase 4 sealer needs for the fixed trailer (§3.3.1) and everything the next
/// segment needs to continue the A1/A9 chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentSummary {
    /// This segment's id (§3.2).
    pub segment_id: u64,
    /// This segment's epoch (A9/R3). The next segment's epoch MUST exceed it.
    pub epoch: u64,
    /// This segment's `base_pos` (A1 seed).
    pub base_pos: u64,
    /// `base_pos + event_count`: the `base_pos` the next segment MUST use, and
    /// the trailer's `end_pos` (§3.3.1).
    pub end_pos: u64,
    /// Number of accepted batches (trailer `batch_count`).
    pub batch_count: u64,
    /// Total events (trailer `event_count`).
    pub event_count: u64,
    /// Byte length of all content written (header + batches). The first byte
    /// after the last `CommitMarker` — the sealer's `ext_offset` (§3.3.1).
    pub content_len: u64,
}

/// A fault on the write path.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    /// The batch could not be encoded (A5/A2/D-FMT-7); see [`EncodeError`].
    #[error("encode: {0}")]
    Encode(#[from] EncodeError),
    /// A8 (§3.1): the batch would extend beyond `segment_size`. The caller
    /// MUST seal/roll and retry on a fresh segment. `remaining` is the space
    /// left before `segment_size`; `needed` is the batch's `total_len`.
    #[error("segment full: batch needs {needed} bytes, {remaining} remain before segment_size")]
    SegmentFull { needed: u64, remaining: u64 },
    /// A short `pwrite` (fewer bytes accepted than offered).
    #[error("short write at offset {offset}: wrote {wrote} of {expected}")]
    ShortWrite { offset: u64, expected: usize, wrote: usize },
    /// Underlying filesystem I/O error.
    #[error("io: {0}")]
    Io(#[from] io::Error),
}

/// The append-only writer for one segment file.
#[derive(Debug)]
pub struct SegmentWriter<F: Fs> {
    fs: F,
    file: F::File,
    segment_id: u64,
    epoch: u64,
    base_pos: u64,
    segment_size: u64,
    /// Next append offset (== bytes of content written so far).
    write_off: u64,
    /// Next per-segment batch id (starts at 0, D-FMT-5).
    next_batch_id: u64,
    /// Running global position (A1): `base_pos + events written so far`.
    next_pos: u64,
    batch_count: u64,
    event_count: u64,
    encoder: BatchEncoder,
}

impl<F: Fs> SegmentWriter<F> {
    /// Open (create) `path` as a fresh segment: encode + `pwrite` the
    /// `SegmentHeader` at offset 0 and `fdatasync` it durable before any batch
    /// (§6). Fails if the file exists non-empty is NOT checked here — the
    /// caller (allocator) guarantees a fresh name; `OpenOpts::create_rw` does
    /// not truncate, so a pre-existing longer file would keep its tail, but a
    /// recycled name must carry a larger `epoch` (A9) which recovery enforces.
    pub fn create(fs: &F, path: &Path, params: SegmentParams) -> Result<Self, WriteError> {
        let file = fs.open(path, OpenOpts::create_rw())?;
        let header = encode_segment_header(&params);
        write_all_at(&file, 0, &header)?;
        file.fdatasync()?;
        Ok(SegmentWriter {
            fs: fs.clone(),
            file,
            segment_id: params.segment_id,
            epoch: params.epoch,
            base_pos: params.base_pos,
            segment_size: params.segment_size,
            write_off: SEGMENT_HEADER_LEN as u64,
            next_batch_id: 0,
            next_pos: params.base_pos,
            batch_count: 0,
            event_count: 0,
            encoder: BatchEncoder::new(),
        })
    }

    /// Build the [`BatchInput`] this writer would encode for `spec`, stamping
    /// the writer-owned fields. Used by [`append`] and [`would_fit`].
    fn input_for<'a, 'p>(&self, spec: &BatchSpec<'a, 'p>) -> BatchInput<'a, 'p> {
        BatchInput {
            segment_epoch: self.epoch,
            batch_id: self.next_batch_id,
            first_global_pos: self.next_pos,
            stream_id: spec.stream_id,
            category_id: spec.category_id,
            first_stream_version: spec.first_stream_version,
            crypto_chain: spec.crypto_chain,
            subframes: spec.subframes,
        }
    }

    /// Bytes remaining before `segment_size`.
    pub fn remaining(&self) -> u64 {
        self.segment_size.saturating_sub(self.write_off)
    }

    /// Whether `spec` would fit in this segment (A8) without rolling. Returns
    /// the encode error for a fundamentally invalid batch (A5/A2/D-FMT-7).
    pub fn would_fit(&self, spec: &BatchSpec) -> Result<bool, EncodeError> {
        let total_len = BatchEncoder::total_len(&self.input_for(spec))?;
        Ok(total_len <= self.remaining())
    }

    /// Append one batch (§4). Stamps A9/A1/`batch_id`, enforces A5/A2/A8,
    /// encodes byte-exact into the reusable buffer, and `pwrite`s it at the
    /// running offset. Does **not** sync (the committer batches durability).
    pub fn append(&mut self, spec: &BatchSpec) -> Result<Receipt, WriteError> {
        let input = self.input_for(spec);
        let total_len = BatchEncoder::total_len(&input)?; // A5/A2/D-FMT-7
        let remaining = self.remaining();
        if total_len > remaining {
            return Err(WriteError::SegmentFull { needed: total_len, remaining }); // A8
        }
        let frame_count = input.subframes.len() as u32;
        let first_global_pos = input.first_global_pos;
        let batch_id = input.batch_id;
        let offset = self.write_off;

        // Encode into the reusable buffer, then one positioned write. The
        // `bytes` borrow of `self.encoder` and the `self.file` borrow are
        // disjoint fields, and `bytes` is dead after `pwrite` returns, so the
        // accounting updates below are unborrowed.
        let bytes = self.encoder.encode(&input)?;
        debug_assert_eq!(bytes.len() as u64, total_len);
        write_all_at(&self.file, offset, bytes)?;

        self.write_off += total_len;
        self.next_batch_id += 1;
        self.next_pos += u64::from(frame_count);
        self.batch_count += 1;
        self.event_count += u64::from(frame_count);

        Ok(Receipt { batch_id, first_global_pos, frame_count, total_len, offset })
    }

    /// The durability barrier (§6, D7): `fdatasync` all appended bytes. This is
    /// the committer's group-commit call; exposed here so the writer is usable
    /// stand-alone and testable.
    pub fn sync(&self) -> io::Result<()> {
        self.file.fdatasync()
    }

    /// A snapshot of the handoff state without consuming the writer.
    pub fn summary(&self) -> SegmentSummary {
        SegmentSummary {
            segment_id: self.segment_id,
            epoch: self.epoch,
            base_pos: self.base_pos,
            end_pos: self.next_pos,
            batch_count: self.batch_count,
            event_count: self.event_count,
            content_len: self.write_off,
        }
    }

    /// This segment's epoch (A9). The next segment MUST use a strictly larger
    /// one.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The next global position (A1) — the `base_pos` a rolled segment MUST
    /// adopt.
    pub fn next_pos(&self) -> u64 {
        self.next_pos
    }

    /// Sync and close, leaving the segment **unsealed** (trailer-less, see the
    /// module docs). Returns the [`SegmentSummary`] the sealer / next segment
    /// need. The `F::File` handle is dropped.
    pub fn close(self) -> io::Result<SegmentSummary> {
        self.file.fdatasync()?;
        Ok(self.summary())
    }

    /// Roll to the next segment (§3.1): close this one (unsealed) and
    /// [`create`](SegmentWriter::create) the next at `next_path`, continuing
    /// the A1 chain (`base_pos = end_pos`) and the A9 chain
    /// (`prev_segment_epoch = this epoch`). `next_epoch` MUST be strictly larger
    /// than this segment's epoch (A9).
    ///
    /// # Panics (debug)
    ///
    /// Debug-asserts `next_epoch > self.epoch`.
    pub fn roll(
        self,
        next_path: &Path,
        next_segment_id: u64,
        next_epoch: u64,
        created_unix_nanos: u64,
    ) -> Result<SegmentWriter<F>, WriteError> {
        debug_assert!(next_epoch > self.epoch, "A9: a rolled segment needs a strictly larger epoch");
        let fs = self.fs.clone();
        let prev_epoch = self.epoch;
        let summary = self.close()?;
        let params = SegmentParams {
            segment_id: next_segment_id,
            base_pos: summary.end_pos,
            epoch: next_epoch,
            prev_segment_epoch: prev_epoch,
            created_unix_nanos,
            segment_size: SEGMENT_SIZE,
        };
        SegmentWriter::create(&fs, next_path, params)
    }
}

/// Encode the fixed 52-byte `SegmentHeader` (§3.2), including its trailing
/// `header_crc` over `[0, 48)`.
fn encode_segment_header(params: &SegmentParams) -> [u8; SEGMENT_HEADER_LEN] {
    let mut h = [0u8; SEGMENT_HEADER_LEN];
    put_u32(&mut h, SH_MAGIC_OFF, SEGMENT_MAGIC);
    put_u16(&mut h, SH_FORMAT_VERSION_OFF, FORMAT_VERSION);
    put_u16(&mut h, SH_FLAGS_OFF, 0); // reserved, MUST be 0
    put_u64(&mut h, SH_SEGMENT_ID_OFF, params.segment_id);
    put_u64(&mut h, SH_BASE_POS_OFF, params.base_pos);
    put_u64(&mut h, SH_EPOCH_OFF, params.epoch);
    put_u64(&mut h, SH_CREATED_UNIX_NANOS_OFF, params.created_unix_nanos);
    put_u64(&mut h, SH_PREV_SEGMENT_EPOCH_OFF, params.prev_segment_epoch);
    let crc = crc32c::crc32c(&h[..SEGMENT_HEADER_CRC_OFF]);
    put_u32(&mut h, SEGMENT_HEADER_CRC_OFF, crc);
    h
}

#[inline]
fn put_u16(buf: &mut [u8], off: usize, v: u16) {
    buf[off..off + 2].copy_from_slice(&v.to_le_bytes());
}
#[inline]
fn put_u32(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
}
#[inline]
fn put_u64(buf: &mut [u8], off: usize, v: u64) {
    buf[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

/// `pwrite` the whole buffer at `off`, looping on short writes. Returns a
/// typed [`WriteError::ShortWrite`] only if the file handle makes no progress.
fn write_all_at<H: FileHandle>(file: &H, off: u64, mut buf: &[u8]) -> Result<(), WriteError> {
    let mut cur = off;
    while !buf.is_empty() {
        let n = file.pwrite(cur, buf)?;
        if n == 0 {
            return Err(WriteError::ShortWrite { offset: cur, expected: buf.len(), wrote: 0 });
        }
        cur += n as u64;
        buf = &buf[n..];
    }
    Ok(())
}

/// Decode the epoch field out of an in-memory `SegmentHeader` image, for tests
/// / tools that need to read back what [`create`](SegmentWriter::create) wrote.
/// Returns `None` if the image is too short, has the wrong magic/version, or
/// fails its `header_crc` (§3.2).
pub fn read_segment_header_epoch(image: &[u8]) -> Option<u64> {
    if image.len() < SEGMENT_HEADER_LEN {
        return None;
    }
    let magic = u32::from_le_bytes(image[SH_MAGIC_OFF..SH_MAGIC_OFF + 4].try_into().ok()?);
    let ver = u16::from_le_bytes(image[SH_FORMAT_VERSION_OFF..SH_FORMAT_VERSION_OFF + 2].try_into().ok()?);
    if magic != SEGMENT_MAGIC || ver != FORMAT_VERSION {
        return None;
    }
    let want = u32::from_le_bytes(
        image[SEGMENT_HEADER_CRC_OFF..SEGMENT_HEADER_CRC_OFF + 4].try_into().ok()?,
    );
    if crc32c::crc32c(&image[..SEGMENT_HEADER_CRC_OFF]) != want {
        return None;
    }
    Some(u64::from_le_bytes(image[SH_EPOCH_OFF..SH_EPOCH_OFF + 8].try_into().ok()?))
}
