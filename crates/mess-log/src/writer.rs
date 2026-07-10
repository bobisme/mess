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
//! - **Sealing** (`bn-sbt`, §3.3/§6): [`seal`](SegmentWriter::seal) appends the
//!   `SegmentFooter` (a Phase-3 *empty* extension region + the fixed 100-byte
//!   trailer at `content_len`) and `fdatasync`s it durable in the single seal
//!   barrier, so recovery's R2 fast path can trust the segment via its trailer
//!   without scanning its body (§8.3). The footer bytes are encoded by
//!   [`crate::sealer`]. [`roll_sealed`](SegmentWriter::roll_sealed) seals then
//!   opens the next segment, continuing the A1/A9 chain.
//! - **Unsealed handoff.** [`close`](SegmentWriter::close)/[`roll`] leave the
//!   segment **trailer-less**, i.e. *unsealed*. Per `02-recovery.md` §8.3, a
//!   segment with no valid trailer "is treated as **not sealed**: it MUST be
//!   fully scanned exactly as the active segment is." Leaving no trailer is the
//!   spec-sanctioned partial state; a torn/partial trailer is no worse (a reader
//!   treats any `footer_crc` mismatch as unsealed anyway). Either way the handed
//!   [`SegmentSummary`] carries every fixed-trailer field
//!   (`ext_offset == content_len`, counts, `epoch`, `base_pos`, `end_pos`).

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

/// Parameters to [`resume`](SegmentWriter::resume) an existing segment after
/// recovery (bn-20b). Every field comes straight from a
/// [`scanner::Recovery`](crate::scanner::Recovery) of the segment: the header
/// (`segment_id`/`base_pos`/`epoch`), `safe_offset` (→ `write_off`), `next_pos`,
/// `next_batch_id`, and the accepted-prefix counts.
#[derive(Debug, Clone, Copy)]
pub struct ResumeParams {
    /// The existing segment's id (from its `SegmentHeader`).
    pub segment_id: u64,
    /// The existing segment's `base_pos` (unchanged — the header is not
    /// rewritten).
    pub base_pos: u64,
    /// The existing segment's `epoch` (unchanged — not recycled, not bumped).
    pub epoch: u64,
    /// Logical segment size, re-reserved on resume.
    pub segment_size: u64,
    /// Recovery `safe_offset`: the first byte past the committed prefix, where
    /// new appends resume.
    pub write_off: u64,
    /// Recovery `next_batch_id`: the per-segment id the next append stamps.
    pub next_batch_id: u64,
    /// Recovery `next_pos`: the running A1 global position to resume at.
    pub next_pos: u64,
    /// Accepted batches so far (the trailer/summary `batch_count`).
    pub batch_count: u64,
    /// Accepted events so far (the trailer/summary `event_count`).
    pub event_count: u64,
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
    /// Disk-full at segment **preallocation** (`bn-36y`,
    /// `docs/spec/03-durability.md` §2.6): [`SegmentWriter::create`] could not
    /// reserve a full segment via [`FileHandle::allocate`], so the segment
    /// roll that this append triggered failed. This is the ONE predictable,
    /// recoverable point at which disk-full is designed to strike: no batch
    /// bytes were written, no partial segment file is left behind, and every
    /// already-committed batch stays durable and readable. The triggering
    /// append earns this typed error; the store is full, not corrupt.
    #[error("store full: could not preallocate {requested} bytes for a new segment")]
    StoreFull {
        /// The segment size the failed [`FileHandle::allocate`] requested.
        requested: u64,
    },
    /// The store is **poisoned** (`bn-36y`, the D8 shape of §2.6): a durability
    /// barrier (`fdatasync`) on this segment previously failed with `ENOSPC`,
    /// so which bytes actually reached the device is unknowable. The writer
    /// fails every subsequent [`append`](SegmentWriter::append) /
    /// [`sync`](SegmentWriter::sync) fast with this typed error rather than
    /// risk writing atop an indeterminate durable state; recovery on reopen
    /// re-establishes the committed prefix. (Full D8 policy — degraded reads,
    /// restart orchestration — is a later bone; this is the minimal
    /// poison-on-barrier-ENOSPC.)
    #[error("store poisoned: a prior fdatasync failed with ENOSPC; reopen and recover")]
    StorePoisoned,
    /// Underlying filesystem I/O error.
    #[error("io: {0}")]
    Io(#[from] io::Error),
    /// bn-221: [`SegmentWriter::resume`]'s `write_off` named a position that
    /// cannot be a real recovered batch boundary of `path` — either before
    /// the fixed segment header (batches never start there) or past the
    /// file's own on-disk length (the scan that produced `write_off` can
    /// never advance past bytes the file does not contain). Resuming there
    /// would place subsequent appends at the wrong offset — inside the
    /// header, or leaving an unaccounted gap — so it is rejected rather than
    /// silently corrupting the segment.
    #[error(
        "invalid resume: write_off {write_off} is not a valid batch boundary \
         (segment header ends at {header_len}, file len is {file_len})"
    )]
    InvalidResume { write_off: u64, header_len: u64, file_len: u64 },
}

/// Whether `e` is an `ENOSPC` (disk-full). The sim fs injects an error carrying
/// the same `raw_os_error()` so classification is identical to a real one.
fn is_enospc(e: &io::Error) -> bool {
    e.raw_os_error() == Some(libc::ENOSPC)
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
    /// Set once a barrier failed with `ENOSPC` (`bn-36y`): the segment's
    /// durable state is unknowable, so every subsequent append/sync fails with
    /// [`WriteError::StorePoisoned`] until the store is reopened and recovered.
    poisoned: bool,
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
        // bn-36y: preallocate the FULL segment before writing anything, so
        // disk-full strikes HERE (a clean, recoverable point) rather than
        // mid-commit. `allocate` reserves blocks without extending the logical
        // length (FALLOC_FL_KEEP_SIZE), so `len()`/recovery are unaffected. On
        // ENOSPC the file is still empty (no header) — remove the husk so a
        // failed roll leaves the store exactly as it was, then surface the
        // typed StoreFull. Other allocate errors propagate as Io.
        if let Err(e) = file.allocate(params.segment_size) {
            drop(file);
            let _ = fs.remove(path); // best-effort; the husk carries no committed bytes
            return Err(if is_enospc(&e) {
                WriteError::StoreFull { requested: params.segment_size }
            } else {
                WriteError::Io(e)
            });
        }
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
            poisoned: false,
        })
    }

    /// **Resume** appending into an EXISTING segment after recovery (bn-20b —
    /// the engine's reopen path).
    ///
    /// Unlike [`create`](SegmentWriter::create), this writes **no** fresh
    /// `SegmentHeader` (the segment keeps its original `base_pos`/`epoch`) and
    /// does not truncate. It opens the file read+write, re-reserves its blocks
    /// (idempotent — the segment was preallocated at create; keeps the bn-36y
    /// "ENOSPC only at a recoverable point" invariant across reopen), and seeds
    /// the write cursor from the recovery result: `write_off` at the recovery
    /// `safe_offset` (the first byte past the committed prefix), `next_pos` /
    /// `next_batch_id` / counts where recovery left off. New appends land at
    /// `write_off`, overwriting any torn/uncommitted tail (A10: never
    /// committed) and extending the committed prefix **in place**, so a later
    /// recovery re-derives the identical, now-longer prefix.
    ///
    /// No epoch bump: the segment is *continued*, not recycled — every batch
    /// (old and newly appended) carries the same `epoch`, so A9 has nothing to
    /// distinguish and recovery accepts the whole contiguous chain. (The
    /// spec's "fresh larger epoch on open" discipline, §6, guards *recycled*
    /// files; resuming the same live segment is the distinct, non-recycling
    /// case.)
    pub fn resume(fs: &F, path: &Path, params: ResumeParams) -> Result<Self, WriteError> {
        let file = fs.open(path, OpenOpts::create_rw())?;

        // bn-221/bn-28u: `write_off` MUST be a real recovered batch boundary of
        // this exact file — it came from `Recovery::safe_offset`, which by
        // construction can never name a position before the fixed segment
        // header (batches start at `SEGMENT_HEADER_LEN`) or past the bytes
        // the scan actually read (`safe_offset <= file len` at scan time; the
        // file is never truncated between recovery and resume). A `write_off`
        // violating either bound cannot have come from a real recovery of
        // `path` — resuming there would silently place new appends inside the
        // header or past a gap of unaccounted bytes. Checked unconditionally
        // with a real typed error (not `debug_assert!`) because `write_off`
        // crosses a public API boundary from a caller-supplied `ResumeParams`,
        // not a value this function derived itself, and misuse-resistance MUST
        // hold in every build profile, `--release` included (bn-221's intent;
        // `debug_assert!` disappears under `--release`, which would have
        // silently accepted a bogus `write_off` instead of erroring).
        let file_len = file.len()?;
        if !is_valid_resume_boundary(params.write_off, file_len) {
            return Err(WriteError::InvalidResume {
                write_off: params.write_off,
                header_len: SEGMENT_HEADER_LEN as u64,
                file_len,
            });
        }

        // Re-reserve the segment's blocks so a post-reopen append cannot hit
        // ENOSPC mid-commit (bn-36y). Idempotent on an already-allocated file.
        if let Err(e) = file.allocate(params.segment_size) {
            return Err(if is_enospc(&e) {
                WriteError::StoreFull { requested: params.segment_size }
            } else {
                WriteError::Io(e)
            });
        }
        Ok(SegmentWriter {
            fs: fs.clone(),
            file,
            segment_id: params.segment_id,
            epoch: params.epoch,
            base_pos: params.base_pos,
            segment_size: params.segment_size,
            write_off: params.write_off,
            next_batch_id: params.next_batch_id,
            next_pos: params.next_pos,
            batch_count: params.batch_count,
            event_count: params.event_count,
            encoder: BatchEncoder::new(),
            poisoned: false,
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
        if self.poisoned {
            return Err(WriteError::StorePoisoned); // bn-36y: barrier ENOSPC poisoned the store
        }
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
    ///
    /// `bn-36y`: if the barrier fails with `ENOSPC` the segment's durable state
    /// is unknowable, so the writer is **poisoned** — the error propagates here
    /// (the committer already treats a failed barrier as non-durable), and
    /// every subsequent [`append`](SegmentWriter::append)/`sync` fails fast
    /// with [`WriteError::StorePoisoned`]. Takes `&mut self` to record the
    /// poison flag.
    pub fn sync(&mut self) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other("store poisoned"));
        }
        match self.file.fdatasync() {
            Ok(()) => Ok(()),
            Err(e) => {
                if is_enospc(&e) {
                    self.poisoned = true;
                }
                Err(e)
            }
        }
    }

    /// Whether a barrier `ENOSPC` has poisoned this writer (`bn-36y`). Once
    /// true, every append/sync fails with [`WriteError::StorePoisoned`] until
    /// the store is reopened and recovered.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
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

    /// This segment's id (§3.2). The next segment MUST use a strictly larger,
    /// never-reused id — the committer's live auto-roll (`bn-1vu`) stamps
    /// `segment_id() + 1`.
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// This segment's logical size (§3.1) — preserved across a live roll so a
    /// small test segment keeps rolling (the default [`roll`] hardwires
    /// [`SEGMENT_SIZE`]).
    pub fn segment_size(&self) -> u64 {
        self.segment_size
    }

    /// The next global position (A1) — the `base_pos` a rolled segment MUST
    /// adopt.
    pub fn next_pos(&self) -> u64 {
        self.next_pos
    }

    /// `fdatasync` all appended bytes and return the handoff [`SegmentSummary`]
    /// **without consuming** the writer (the segment stays open and, per the
    /// module docs, **unsealed** — trailer-less). This is the committer's
    /// live-auto-roll (`bn-1vu`) counterpart to [`close`](SegmentWriter::close),
    /// which consumes: the committer makes the just-full segment durable, then
    /// switches its live writer to the next segment ([`open_next`]) in place.
    ///
    /// Leaving the rolled segment unsealed is the crash-safe state: recovery
    /// fully scans a trailer-less segment (§8.3), so a crash between the roll
    /// and the background footer-finalize loses nothing — the background sealer
    /// writes the trailer only after the sidecar is durable.
    ///
    /// `bn-36y`: a poisoned writer refuses with [`WriteError::StorePoisoned`].
    pub fn sync_and_summary(&mut self) -> Result<SegmentSummary, WriteError> {
        if self.poisoned {
            return Err(WriteError::StorePoisoned);
        }
        self.file.fdatasync()?;
        Ok(self.summary())
    }

    /// Create the **next** segment continuing this one's A1 chain
    /// (`base_pos = self.next_pos`), A9 chain (`prev_segment_epoch = self.epoch`,
    /// `next_epoch` strictly larger), and logical `segment_size` — **without
    /// consuming or closing** this writer. The caller MUST first make this
    /// segment durable ([`sync_and_summary`](SegmentWriter::sync_and_summary))
    /// and then replace its live writer with the returned one. Unlike
    /// [`roll`](SegmentWriter::roll)/[`roll_sealed`](SegmentWriter::roll_sealed)
    /// (which consume `self` and hardwire [`SEGMENT_SIZE`]), this preserves
    /// `self.segment_size` and leaves `self` intact so a failed preallocation
    /// (`bn-36y` `StoreFull`) leaves the live writer usable — nothing is lost.
    ///
    /// # Panics (debug)
    ///
    /// Debug-asserts `next_epoch > self.epoch`.
    pub fn open_next(
        &self,
        next_path: &Path,
        next_segment_id: u64,
        next_epoch: u64,
        created_unix_nanos: u64,
    ) -> Result<SegmentWriter<F>, WriteError> {
        debug_assert!(
            next_epoch > self.epoch,
            "A9: a rolled segment needs a strictly larger epoch"
        );
        let params = SegmentParams {
            segment_id: next_segment_id,
            base_pos: self.next_pos,
            epoch: next_epoch,
            prev_segment_epoch: self.epoch,
            created_unix_nanos,
            segment_size: self.segment_size,
        };
        SegmentWriter::create(&self.fs, next_path, params)
    }

    /// Sync and close, leaving the segment **unsealed** (trailer-less, see the
    /// module docs). Returns the [`SegmentSummary`] the sealer / next segment
    /// need. The `F::File` handle is dropped.
    pub fn close(self) -> io::Result<SegmentSummary> {
        self.file.fdatasync()?;
        Ok(self.summary())
    }

    /// **Seal** this segment (bn-sbt, §3.3, §6): append the `SegmentFooter` and
    /// make it durable in one seal `fdatasync`, so recovery's R2 fast path can
    /// trust the segment via its trailer without scanning its body (§8.3). The
    /// footer is the (Phase-3 empty) extension region followed by the fixed
    /// 100-byte trailer at `content_len`; the sealed file is therefore
    /// `content_len + SEGMENT_TRAILER_LEN` bytes long, and the trailer occupies
    /// the final [`SEGMENT_TRAILER_LEN`] bytes (R2 pread-from-EOF). Returns the
    /// [`SegmentSummary`]; the `F::File` handle is dropped.
    ///
    /// Ordering (§6): every batch is already durable (the committer synced each
    /// group) — the seal `fdatasync` here makes the *footer* durable. A crash
    /// before this returns leaves the segment trailer-less, i.e. unsealed, which
    /// recovery fully scans exactly as the active segment (§8.3); no committed
    /// batch is lost either way.
    ///
    /// `bn-36y`: a poisoned writer (a prior barrier `ENOSPC`) refuses to seal
    /// with [`WriteError::StorePoisoned`] rather than write a footer atop an
    /// indeterminate durable state.
    pub fn seal(self) -> Result<SegmentSummary, WriteError> {
        if self.poisoned {
            return Err(WriteError::StorePoisoned);
        }
        let summary = self.summary();
        // Phase 3: empty extension region ⇒ the trailer begins at content_len
        // (== ext_offset), and the whole footer is just the fixed trailer.
        let fields = crate::sealer::TrailerFields::phase3(
            summary.segment_id,
            summary.epoch,
            summary.base_pos,
            summary.batch_count,
            summary.event_count,
            summary.content_len,
        );
        let trailer = crate::sealer::encode_trailer(&fields);
        write_all_at(&self.file, summary.content_len, &trailer)?;
        self.file.fdatasync()?; // the single seal fsync (§6)
        Ok(summary)
    }

    /// Roll to the next segment, **sealing** the current one first (§3.1, §3.3):
    /// [`seal`](SegmentWriter::seal) this segment (write + fsync its footer),
    /// then [`create`](SegmentWriter::create) the next at `next_path` continuing
    /// the A1 chain (`base_pos = end_pos`) and the A9 chain
    /// (`prev_segment_epoch = this epoch`). `next_epoch` MUST be strictly larger
    /// than this segment's epoch (A9). This is the sealing counterpart of
    /// [`roll`](SegmentWriter::roll), which leaves the old segment unsealed.
    ///
    /// # Panics (debug)
    ///
    /// Debug-asserts `next_epoch > self.epoch`.
    pub fn roll_sealed(
        self,
        next_path: &Path,
        next_segment_id: u64,
        next_epoch: u64,
        created_unix_nanos: u64,
    ) -> Result<SegmentWriter<F>, WriteError> {
        debug_assert!(next_epoch > self.epoch, "A9: a rolled segment needs a strictly larger epoch");
        let fs = self.fs.clone();
        let prev_epoch = self.epoch;
        let summary = self.seal()?;
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

    /// Roll to the next segment (§3.1): close this one (unsealed) and
    /// [`create`](SegmentWriter::create) the next at `next_path`, continuing
    /// the A1 chain (`base_pos = end_pos`) and the A9 chain
    /// (`prev_segment_epoch = this epoch`). `next_epoch` MUST be strictly larger
    /// than this segment's epoch (A9).
    ///
    /// `bn-36y`: the new segment is preallocated in full by
    /// [`create`](SegmentWriter::create). If that preallocation hits `ENOSPC`
    /// the roll fails with [`WriteError::StoreFull`] and no partial segment is
    /// left behind — this old segment has already been closed durably (its
    /// data stays readable via recovery), and the never-headered husk of the
    /// new segment is removed. The triggering append thus earns a typed
    /// disk-full error at this single, recoverable point.
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

/// bn-221: whether `write_off` is a position [`SegmentWriter::resume`] could
/// legitimately resume at for a file of `file_len` bytes — i.e. one a real
/// `Recovery::safe_offset` could actually have produced: not before the fixed
/// segment header (batches never start there, §3.2) and not past the file's
/// own on-disk length (a scan can never advance past bytes the file does not
/// contain). Pulled out of [`SegmentWriter::resume`] so the predicate itself
/// is unit-testable independent of `debug_assert!`'s build-profile gating.
fn is_valid_resume_boundary(write_off: u64, file_len: u64) -> bool {
    write_off >= SEGMENT_HEADER_LEN as u64 && write_off <= file_len
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

#[cfg(test)]
mod resume_tests {
    //! bn-221: `resume`'s `write_off` must be a real recovered batch boundary
    //! of the file being resumed. Before this bone a bogus `write_off` (a
    //! caller bug, not a real `Recovery::safe_offset`) was accepted silently
    //! and would land subsequent appends at the wrong offset.
    use super::*;
    use crate::encode::Subframe;
    use crate::runtime::{Runtime, SimRuntime};

    /// The pure boundary predicate, independent of `debug_assert!`'s
    /// build-profile gating (a normal `cargo test` binary has
    /// `debug_assertions` on, which makes the `resume` `Err` path below
    /// unreachable through the public API — this covers that logic directly).
    #[test]
    fn valid_resume_boundary_rejects_before_header_and_past_file_len() {
        let header = SEGMENT_HEADER_LEN as u64;
        assert!(!is_valid_resume_boundary(0, 1_000));
        assert!(!is_valid_resume_boundary(header - 1, 1_000));
        assert!(!is_valid_resume_boundary(1_001, 1_000));
        assert!(is_valid_resume_boundary(header, header)); // empty-but-headered segment
        assert!(is_valid_resume_boundary(500, 1_000)); // mid-file, still <= file_len
        assert!(is_valid_resume_boundary(1_000, 1_000)); // exactly EOF
    }

    /// Write a tiny one-batch segment and return its handoff summary — the
    /// exact shape a real `Recovery` would resume from.
    fn seed_segment<F: Fs>(fs: &F, path: &Path) -> SegmentSummary {
        let mut writer = SegmentWriter::create(fs, path, SegmentParams::new(1, 0, 1, 0)).unwrap();
        let subs = vec![Subframe::plain(1, 0, 0, b"hello")];
        writer
            .append(&BatchSpec {
                stream_id: 1,
                category_id: 0,
                first_stream_version: 0,
                crypto_chain: None,
                subframes: &subs,
            })
            .unwrap();
        writer.close().unwrap()
    }

    /// The `ResumeParams` a real recovery of `seed_segment`'s output would
    /// hand back: `write_off == content_len` (the file has no torn tail).
    fn real_resume_params(summary: &SegmentSummary) -> ResumeParams {
        ResumeParams {
            segment_id: summary.segment_id,
            base_pos: summary.base_pos,
            epoch: summary.epoch,
            segment_size: SEGMENT_SIZE,
            write_off: summary.content_len,
            next_batch_id: summary.batch_count,
            next_pos: summary.end_pos,
            batch_count: summary.batch_count,
            event_count: summary.event_count,
        }
    }

    #[test]
    fn resume_accepts_the_real_recovered_boundary() {
        let rt = SimRuntime::new(3);
        let fs = rt.fs();
        let path = Path::new("/seg-resume-ok");
        let summary = seed_segment(&fs, path);
        let params = real_resume_params(&summary);
        SegmentWriter::resume(&fs, path, params).expect("the real safe_offset must resume cleanly");
    }

    #[test]
    fn resume_rejects_write_off_past_file_len() {
        let rt = SimRuntime::new(5);
        let fs = rt.fs();
        let path = Path::new("/seg-resume-past-eof");
        let summary = seed_segment(&fs, path);
        let mut params = real_resume_params(&summary);
        let bogus_write_off = summary.content_len + 1_000_000; // far past the file's own bytes
        params.write_off = bogus_write_off;
        let err = match SegmentWriter::resume(&fs, path, params) {
            Ok(_) => panic!("write_off past file len must be rejected in every build profile"),
            Err(e) => e,
        };
        match err {
            WriteError::InvalidResume { write_off, header_len, file_len } => {
                assert_eq!(write_off, bogus_write_off);
                assert_eq!(header_len, SEGMENT_HEADER_LEN as u64);
                assert_eq!(file_len, summary.content_len);
            }
            other => panic!("expected WriteError::InvalidResume, got {other:?}"),
        }
    }

    #[test]
    fn resume_rejects_write_off_before_header() {
        let rt = SimRuntime::new(9);
        let fs = rt.fs();
        let path = Path::new("/seg-resume-in-header");
        let summary = seed_segment(&fs, path);
        let mut params = real_resume_params(&summary);
        params.write_off = 4; // inside the fixed SegmentHeader, before any batch
        let err = match SegmentWriter::resume(&fs, path, params) {
            Ok(_) => panic!("write_off before the header must be rejected in every build profile"),
            Err(e) => e,
        };
        match err {
            WriteError::InvalidResume { write_off, header_len, file_len } => {
                assert_eq!(write_off, 4);
                assert_eq!(header_len, SEGMENT_HEADER_LEN as u64);
                assert_eq!(file_len, summary.content_len);
            }
            other => panic!("expected WriteError::InvalidResume, got {other:?}"),
        }
    }
}
