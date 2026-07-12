//! A v4 **capsule** segment writer (§22): the opt-in write path for v4
//! segments.
//!
//! This is deliberately **not** the default engine write path — no production
//! store writes v4 until the migration boundary (§22, review C1). It exists so
//! the exhaustive model, torn matrix, golden fixtures, and D4 retry tests can
//! produce real v4 segments through the same [`Fs`] seam the v3 writer uses.
//! It stamps `segment_epoch`/`batch_id`/`first_global_pos` exactly as §6
//! requires and appends capsules contiguously with no alignment padding.

use std::io;
use std::path::Path;

use super::capsule::{CapsuleEncodeError, CapsuleEncoder, CapsuleInput};
use super::control::ControlRecord;
use super::format::{CAPSULE_CHAIN_LEN, FORMAT_VERSION_V4};
use crate::encode::Subframe;
use crate::format::{
    SEGMENT_HEADER_CRC_OFF, SEGMENT_HEADER_LEN, SEGMENT_MAGIC, SH_BASE_POS_OFF,
    SH_CREATED_UNIX_NANOS_OFF, SH_EPOCH_OFF, SH_FLAGS_OFF,
    SH_FORMAT_VERSION_OFF, SH_MAGIC_OFF, SH_PREV_SEGMENT_EPOCH_OFF,
    SH_SEGMENT_ID_OFF,
};
use crate::runtime::{FileHandle, Fs, OpenOpts};

/// Parameters to create a fresh v4 segment (the v3 [`SegmentParams`] family
/// with `format_version = 4`).
#[derive(Debug, Clone, Copy)]
pub struct SegmentParamsV4 {
    pub segment_id:         u64,
    pub base_pos:           u64,
    pub epoch:              u64,
    pub prev_segment_epoch: u64,
    pub created_unix_nanos: u64,
    pub segment_size:       u64,
}

impl SegmentParamsV4 {
    #[must_use]
    pub fn new(
        segment_id: u64,
        base_pos: u64,
        epoch: u64,
        prev_segment_epoch: u64,
    ) -> Self {
        SegmentParamsV4 {
            segment_id,
            base_pos,
            epoch,
            prev_segment_epoch,
            created_unix_nanos: 0,
            segment_size: crate::format::SEGMENT_SIZE,
        }
    }
}

/// One capsule to append: the caller supplies the stream-level + control-level
/// content; the writer stamps `segment_epoch`/`batch_id`/`first_global_pos`.
#[derive(Debug, Clone)]
pub struct CapsuleSpec<'a, 'p> {
    /// Domain stream (0 for a control-only capsule — §6).
    pub stream_id:            u64,
    pub category_id:          u64,
    pub first_stream_version: u64,
    pub crypto_chain:         Option<&'a [u8; CAPSULE_CHAIN_LEN]>,
    pub controls:             &'a [ControlRecord],
    pub subframes:            &'a [Subframe<'p>],
}

/// What one successful append committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapsuleReceipt {
    pub batch_id:         u64,
    pub first_global_pos: u64,
    pub event_count:      u32,
    pub control_count:    u32,
    pub total_len:        u64,
    pub offset:           u64,
}

/// A fault on the v4 write path.
#[derive(Debug, thiserror::Error)]
pub enum WriteErrorV4 {
    #[error("encode: {0}")]
    Encode(#[from] CapsuleEncodeError),
    #[error("segment full: capsule needs {needed} bytes, {remaining} remain")]
    SegmentFull { needed: u64, remaining: u64 },
    #[error("short write at offset {offset}: wrote 0 of {expected}")]
    ShortWrite { offset: u64, expected: usize },
    #[error("io: {0}")]
    Io(#[from] io::Error),
}

/// The append-only v4 capsule writer for one segment.
#[derive(Debug)]
pub struct CapsuleWriter<F: Fs> {
    file:          F::File,
    epoch:         u64,
    segment_size:  u64,
    write_off:     u64,
    next_batch_id: u64,
    next_pos:      u64,
    encoder:       CapsuleEncoder,
}

impl<F: Fs> CapsuleWriter<F> {
    /// Create `path` as a fresh v4 segment: encode + `pwrite` the v4
    /// `SegmentHeader` (format_version=4) at offset 0 and `fdatasync` it.
    pub fn create(
        fs: &F,
        path: &Path,
        params: SegmentParamsV4,
    ) -> Result<Self, WriteErrorV4> {
        let file = fs.open(path, OpenOpts::create_rw())?;
        let header = encode_segment_header_v4(&params);
        write_all_at(&file, 0, &header)?;
        file.fdatasync()?;
        Ok(CapsuleWriter {
            file,
            epoch: params.epoch,
            segment_size: params.segment_size,
            write_off: SEGMENT_HEADER_LEN as u64,
            next_batch_id: 0,
            next_pos: params.base_pos,
            encoder: CapsuleEncoder::new(),
        })
    }

    fn input_for<'a, 'p>(
        &self,
        spec: &CapsuleSpec<'a, 'p>,
    ) -> CapsuleInput<'a, 'p> {
        CapsuleInput {
            segment_epoch:        self.epoch,
            batch_id:             self.next_batch_id,
            first_global_pos:     self.next_pos,
            stream_id:            spec.stream_id,
            category_id:          spec.category_id,
            first_stream_version: spec.first_stream_version,
            crypto_chain:         spec.crypto_chain,
            controls:             spec.controls,
            subframes:            spec.subframes,
        }
    }

    #[must_use]
    pub fn remaining(&self) -> u64 {
        self.segment_size.saturating_sub(self.write_off)
    }

    #[must_use]
    pub fn next_pos(&self) -> u64 { self.next_pos }

    #[must_use]
    pub fn next_batch_id(&self) -> u64 { self.next_batch_id }

    /// Append one capsule. Stamps position fields, encodes byte-exact, and
    /// `pwrite`s at the running offset. Does not sync (the caller batches
    /// durability, mirroring the committer's group commit).
    pub fn append(
        &mut self,
        spec: &CapsuleSpec,
    ) -> Result<CapsuleReceipt, WriteErrorV4> {
        let input = self.input_for(spec);
        let total_len = CapsuleEncoder::total_len(&input)?;
        let remaining = self.remaining();
        if total_len > remaining {
            return Err(WriteErrorV4::SegmentFull {
                needed: total_len,
                remaining,
            });
        }
        let event_count = input.subframes.len() as u32;
        let control_count = input.controls.len() as u32;
        let first_global_pos = input.first_global_pos;
        let batch_id = input.batch_id;
        let offset = self.write_off;

        let bytes = self.encoder.encode(&input)?;
        debug_assert_eq!(bytes.len() as u64, total_len);
        write_all_at(&self.file, offset, bytes)?;

        self.write_off += total_len;
        self.next_batch_id += 1;
        self.next_pos += u64::from(event_count);

        Ok(CapsuleReceipt {
            batch_id,
            first_global_pos,
            event_count,
            control_count,
            total_len,
            offset,
        })
    }

    /// The durability barrier: `fdatasync` all appended bytes.
    pub fn sync(&mut self) -> io::Result<()> { self.file.fdatasync() }

    /// Sync and close (unsealed).
    pub fn close(self) -> io::Result<()> { self.file.fdatasync() }
}

/// Encode the fixed 52-byte v4 `SegmentHeader` (v3 family, `format_version=4`).
fn encode_segment_header_v4(
    params: &SegmentParamsV4,
) -> [u8; SEGMENT_HEADER_LEN] {
    let mut h = [0u8; SEGMENT_HEADER_LEN];
    put_u32(&mut h, SH_MAGIC_OFF, SEGMENT_MAGIC);
    put_u16(&mut h, SH_FORMAT_VERSION_OFF, FORMAT_VERSION_V4);
    put_u16(&mut h, SH_FLAGS_OFF, 0);
    put_u64(&mut h, SH_SEGMENT_ID_OFF, params.segment_id);
    put_u64(&mut h, SH_BASE_POS_OFF, params.base_pos);
    put_u64(&mut h, SH_EPOCH_OFF, params.epoch);
    put_u64(&mut h, SH_CREATED_UNIX_NANOS_OFF, params.created_unix_nanos);
    put_u64(&mut h, SH_PREV_SEGMENT_EPOCH_OFF, params.prev_segment_epoch);
    let crc = crc32c::crc32c(&h[..SEGMENT_HEADER_CRC_OFF]);
    put_u32(&mut h, SEGMENT_HEADER_CRC_OFF, crc);
    h
}

fn write_all_at<H: FileHandle>(
    file: &H,
    off: u64,
    mut buf: &[u8],
) -> Result<(), WriteErrorV4> {
    let mut cur = off;
    while !buf.is_empty() {
        let n = file.pwrite(cur, buf)?;
        if n == 0 {
            return Err(WriteErrorV4::ShortWrite {
                offset:   cur,
                expected: buf.len(),
            });
        }
        cur += n as u64;
        buf = &buf[n..];
    }
    Ok(())
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
