//! Seal-time per-segment membership filters (bn-1i7): a `BinaryFuse16` filter
//! over a sealed segment's distinct `stream_id`s, built once at seal time and
//! consulted **before** touching the segment's directory or pointer blocks.
//!
//! # Correctness rule: filters only ever skip work
//!
//! `might_contain(key)` returning `false` means the key is **definitely
//! absent** from the segment (zero false negatives — safe to skip the
//! segment's directory/pointer blocks entirely). Returning `true` means
//! "maybe" — a hash collision may have produced a false positive, so the
//! caller must still consult the exact directory. A filter can only ever
//! narrow a caller's search space, never wrongly exclude a present key.
//!
//! Round-3 spike pick (`notes/mess-research/15_spike_results_round3.md`):
//! `BinaryFuse16` — 0.002% FPR, 3.61 B/key, ~26 KiB/segment (7,411 streams),
//! ~4 ns/query, zero false negatives — beats `BinaryFuse8` (0.372% FPR) at a
//! modest size cost. The [`xorf`] crate implements the filter; this module
//! owns *persisting* it (a small self-describing file) and the safe
//! reconstruction path for querying it.
//!
//! # Scope: stream ids only, not category
//!
//! The bone also invites a per-category filter "if cheaply available from
//! entries" — it is not, today. [`SealInput`](crate::sealed::segment::SealInput)
//! and the active index it is built from ([`crate::active`]) carry no
//! `category` field at all; category derivation belongs to the Phase-5
//! payload-column work (per-category dictionaries, `sealed::mod` module
//! docs), which this bone does not touch. Adding a category filter here would
//! mean plumbing category through `SealBatch`/`StreamEntry` first — out of
//! scope for a filter module. Scoped to stream-id membership only.
//!
//! # Why a parallel file, not a sidecar region
//!
//! The `.pidx` sidecar ([`crate::sealed::segment`]) has its own header/footer
//! with fixed-width offsets validated by an exact `content_crc`; splicing a
//! variable-length filter region into it would mean growing the header
//! (`format_version` bump) or relaxing the footer's `dir_off` invariant
//! (`footer_start - dir_off == dir_len`, checked exactly). Both are avoidable
//! churn for a structure that is independently optional and independently
//! rebuildable (I5): a `.filter` file next to `seg-<id>.pidx` follows the
//! same crash-atomic `write_durable` discipline
//! ([`crate::sealed::driver::SealDriver`]) as its own file, and losing or
//! corrupting it degrades to `might_contain` always answering `true`
//! (unfiltered — see [`SegmentFilter::open`]) without touching pointer-block
//! parsing at all. This is the same "sidecar vs. footer section" argument
//! `segment` makes about the pointer index itself, one level down.
//!
//! # File layout
//!
//! ```text
//! Header (32 bytes):
//!   0   u32  magic = FILTER_MAGIC
//!   4   u16  format_version = 1
//!   6   u16  flags = 0
//!   8   u64  segment_id       (cross-checked against the sidecar it pairs with)
//!   16  u32  n_fingerprints
//!   20  u32  reserved = 0
//!   24  u64  reserved = 0
//!
//! Descriptor (20 bytes): xorf's `BinaryFuse16::DESCRIPTOR_LEN` bytes, as
//!   produced by `DmaSerializable::dma_copy_descriptor_to` (already
//!   little-endian by that trait's own contract — see `xorf::bfuse16`).
//!
//! Fingerprints: n_fingerprints * u16, little-endian.
//!
//! Footer (8 bytes):
//!   0   u32  content_crc (crc32c over everything before the footer)
//!   4   u32  magic = FILTER_MAGIC
//! ```
//!
//! # Reconstruction without unsafe zero-copy
//!
//! `xorf::BinaryFuse16`'s fingerprints are a private-descriptor struct we
//! cannot rebuild through its public constructors (only `try_from`/
//! `try_from_iterator` build one, and both re-run the filter *construction*
//! algorithm — not what we want when loading a filter we already built).
//! The crate's sanctioned persistence path is [`xorf::DmaSerializable`] /
//! [`xorf::FilterRef`]: `BinaryFuse16Ref::from_dma` builds a *borrowed* filter
//! straight from a descriptor + fingerprint byte slice, but panics if the
//! fingerprint slice is not 2-byte aligned. A slice into an arbitrary
//! `Vec<u8>` read from disk is not guaranteed that alignment. We sidestep
//! this by decoding fingerprints into an owned `Vec<u16>` — whose allocation
//! the standard library guarantees is `align_of::<u16>() == 2`-aligned — and
//! reinterpreting *that* as bytes via the one small `unsafe` in this module
//! ([`SegmentFilter::might_contain`]), which can never see a non-empty
//! prefix/suffix given that guarantee. This mirrors what `xorf` does
//! internally in `dma_fingerprints` (same cast, opposite direction).
//!
//! # Measured (this machine, `tests/filter_scale.rs`, release)
//!
//! 10,000 stream ids (`sealed_scale`'s segment shape), 200,000 sampled absent
//! keys:
//!
//! | metric | value |
//! |---|---|
//! | build (`SegmentFilter::build`) | **396 µs** for 10k keys |
//! | serialized size | **2.57 B/key** (25,660 bytes / 10,000 keys) |
//! | query, present key (`might_contain`, hit) | **~7 ns** |
//! | query, absent key (`might_contain`, miss) | **~66 ns** (extra branch on the `contains` miss path) |
//! | false positive rate | **0.0005–0.0015%** across runs — matches the round-3 spike's ~0.002% band, comfortably under the `< 1%` sanity assertion |
//! | false negatives | **zero**, every run (property-tested, `no_false_negatives_and_fpr_sanity`) |
//!
//! `might_contain` rebuilds a `BinaryFuse16Ref` per call (a descriptor copy +
//! a slice, no allocation); at ~7–66 ns/query it is not worth caching.

use std::path::Path;

use xorf::{BinaryFuse16, BinaryFuse16Ref, DmaSerializable, Filter, FilterRef};

/// Filter-file magic (`"SXF1"`-ish, distinct from the sidecar's `SIDECAR_MAGIC`).
pub const FILTER_MAGIC: u32 = 0x5359_4602;
/// Current filter-file `format_version`.
pub const FILTER_FORMAT_VERSION: u16 = 1;
/// Filter-file header length (bytes).
pub const FILTER_HEADER_LEN: usize = 32;
/// Descriptor length (bytes) — `xorf::BinaryFuse16::DESCRIPTOR_LEN`.
pub const FILTER_DESCRIPTOR_LEN: usize = BinaryFuse16::DESCRIPTOR_LEN;
/// Filter-file footer length (bytes).
pub const FILTER_FOOTER_LEN: usize = 8;

/// Errors reading a persisted filter file. Every variant is a *soft* failure
/// from the caller's point of view: [`SegmentFilter::open`] turns all of them
/// into "no filter attached" (I5 — corrupt/missing degrades to unfiltered
/// reads, never a wrong answer).
#[derive(Debug, thiserror::Error)]
pub enum FilterError {
    /// I/O error reading the filter file.
    #[error("filter I/O: {0}")]
    Io(#[from] std::io::Error),
    /// The bytes are too short, mis-magicked, wrong-version, size-mismatched,
    /// or CRC-mismatched.
    #[error("filter corrupt: {0}")]
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

/// A durable, self-describing `BinaryFuse16` membership filter over one
/// sealed segment's `stream_id`s.
#[derive(Debug, Clone)]
pub struct SegmentFilter {
    segment_id: u64,
    descriptor: [u8; FILTER_DESCRIPTOR_LEN],
    /// Owned so the allocation is guaranteed `u16`-aligned — see the module
    /// docs' "Reconstruction without unsafe zero-copy" section.
    fingerprints: Vec<u16>,
}

impl SegmentFilter {
    /// Build a filter over `stream_ids` (need not be sorted; MUST be
    /// distinct — true by construction from a sidecar directory, one entry
    /// per stream). Returns `None` if construction fails: `xorf` documents
    /// this as "usually only [happening] if there are duplicate keys", or
    /// when there is nothing to filter (`stream_ids` empty). Either way the
    /// segment seals successfully without a filter — [`SealedSegmentIndex`]
    /// then degrades to always-`true` (unfiltered), same as a
    /// missing/corrupt filter file (I5).
    ///
    /// [`SealedSegmentIndex`]: crate::sealed::segment::SealedSegmentIndex
    pub fn build(segment_id: u64, stream_ids: &[u64]) -> Option<Self> {
        if stream_ids.is_empty() {
            return None;
        }
        let filter = BinaryFuse16::try_from(stream_ids).ok()?;
        let mut descriptor = [0u8; FILTER_DESCRIPTOR_LEN];
        filter.dma_copy_descriptor_to(&mut descriptor);
        let fingerprints: Vec<u16> = filter.fingerprints.to_vec();
        Some(SegmentFilter { segment_id, descriptor, fingerprints })
    }

    /// The segment this filter was built for.
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// Number of fingerprint slots (not the number of keys — `BinaryFuse16`
    /// over-allocates; see `xorf`'s docs for the ~1.13x factor).
    pub fn len(&self) -> usize {
        self.fingerprints.len()
    }

    /// Whether this filter has no fingerprint slots at all. [`Self::build`]
    /// never returns such a filter (it rejects empty key sets), so this is
    /// realistically always `false`; provided to satisfy the `len`/`is_empty`
    /// convention (`clippy::len_without_is_empty`).
    pub fn is_empty(&self) -> bool {
        self.fingerprints.is_empty()
    }

    /// `false` — the key is **definitely absent**: safe to skip the
    /// segment's directory/pointer blocks entirely. `true` — "maybe":
    /// consult the exact directory.
    #[inline]
    pub fn might_contain(&self, key: u64) -> bool {
        // SAFETY: `fingerprints` is a `Vec<u16>`; the standard allocator
        // guarantees its allocation is aligned to `align_of::<u16>() == 2`.
        // Reinterpreting a `u16` slice as `u8` can therefore never leave a
        // non-empty prefix/suffix (u8's alignment of 1 divides u16's
        // alignment of 2, and there is no inter-element padding for a
        // primitive integer array) — `align_to::<u8>` is sound and total
        // here. `BinaryFuse16Ref::from_dma` needs exactly this borrowed byte
        // view; it is rebuilt per call rather than cached because it is a
        // few-field, allocation-free struct (a descriptor copy + a slice) —
        // see `tests/filter_scale.rs` for the measured per-query cost.
        let (prefix, bytes, suffix) = unsafe { self.fingerprints.align_to::<u8>() };
        debug_assert!(prefix.is_empty() && suffix.is_empty());
        let filter_ref = BinaryFuse16Ref::from_dma(&self.descriptor, bytes);
        filter_ref.contains(&key)
    }

    /// Serialize to the on-disk filter-file byte image (see module docs for
    /// the layout). Pure: no I/O — [`crate::sealed::driver`] writes and
    /// fsyncs the bytes with the same crash-atomic discipline as the sidecar.
    pub fn to_bytes(&self) -> Vec<u8> {
        let n_fingerprints = self.fingerprints.len();
        let mut buf = Vec::with_capacity(
            FILTER_HEADER_LEN + FILTER_DESCRIPTOR_LEN + n_fingerprints * 2 + FILTER_FOOTER_LEN,
        );
        buf.extend_from_slice(&FILTER_MAGIC.to_le_bytes());
        buf.extend_from_slice(&FILTER_FORMAT_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes()); // flags
        buf.extend_from_slice(&self.segment_id.to_le_bytes());
        buf.extend_from_slice(&(n_fingerprints as u32).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // reserved
        buf.extend_from_slice(&0u64.to_le_bytes()); // reserved
        debug_assert_eq!(buf.len(), FILTER_HEADER_LEN);

        buf.extend_from_slice(&self.descriptor);
        for fp in &self.fingerprints {
            buf.extend_from_slice(&fp.to_le_bytes());
        }

        let content_crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&content_crc.to_le_bytes());
        buf.extend_from_slice(&FILTER_MAGIC.to_le_bytes());
        buf
    }

    /// Parse a filter-file byte image, validating magic, version, size, and
    /// CRC.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FilterError> {
        let min_len = FILTER_HEADER_LEN + FILTER_DESCRIPTOR_LEN + FILTER_FOOTER_LEN;
        if bytes.len() < min_len {
            return Err(FilterError::Corrupt("shorter than header + descriptor + footer"));
        }
        if rd_u32(bytes, 0) != FILTER_MAGIC {
            return Err(FilterError::Corrupt("bad header magic"));
        }
        if rd_u16(bytes, 4) != FILTER_FORMAT_VERSION {
            return Err(FilterError::Corrupt("unknown format_version"));
        }
        let segment_id = rd_u64(bytes, 8);
        let n_fingerprints = rd_u32(bytes, 16) as usize;

        let expected_len = FILTER_HEADER_LEN
            + FILTER_DESCRIPTOR_LEN
            + n_fingerprints
                .checked_mul(2)
                .ok_or(FilterError::Corrupt("fingerprint length overflow"))?
            + FILTER_FOOTER_LEN;
        if bytes.len() != expected_len {
            return Err(FilterError::Corrupt("length does not match header n_fingerprints"));
        }

        let footer_start = bytes.len() - FILTER_FOOTER_LEN;
        if rd_u32(bytes, footer_start + 4) != FILTER_MAGIC {
            return Err(FilterError::Corrupt("bad footer magic"));
        }
        let stored_crc = rd_u32(bytes, footer_start);
        let got_crc = crc32c::crc32c(&bytes[..footer_start]);
        if got_crc != stored_crc {
            return Err(FilterError::Corrupt("content CRC mismatch"));
        }

        let mut descriptor = [0u8; FILTER_DESCRIPTOR_LEN];
        descriptor.copy_from_slice(&bytes[FILTER_HEADER_LEN..FILTER_HEADER_LEN + FILTER_DESCRIPTOR_LEN]);

        let fp_start = FILTER_HEADER_LEN + FILTER_DESCRIPTOR_LEN;
        let fingerprints: Vec<u16> = bytes[fp_start..footer_start]
            .chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]))
            .collect();

        Ok(SegmentFilter { segment_id, descriptor, fingerprints })
    }

    /// Read and parse a filter file from `path`.
    pub fn open(path: &Path) -> Result<Self, FilterError> {
        let bytes = std::fs::read(path)?;
        Self::from_bytes(&bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_contains_present_keys() {
        let ids: Vec<u64> = (0..500u64).map(|i| i * 3 + 7).collect();
        let filter = SegmentFilter::build(42, &ids).unwrap();
        assert_eq!(filter.segment_id(), 42);

        let bytes = filter.to_bytes();
        let reopened = SegmentFilter::from_bytes(&bytes).unwrap();
        assert_eq!(reopened.segment_id(), 42);

        for &id in &ids {
            assert!(reopened.might_contain(id), "false negative for present key {id}");
        }
    }

    #[test]
    fn empty_keys_build_none() {
        assert!(SegmentFilter::build(1, &[]).is_none());
    }

    #[test]
    fn corrupt_crc_is_rejected() {
        let ids: Vec<u64> = (0..100u64).collect();
        let filter = SegmentFilter::build(1, &ids).unwrap();
        let mut bytes = filter.to_bytes();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xff;
        assert!(matches!(SegmentFilter::from_bytes(&bytes), Err(FilterError::Corrupt(_))));
    }

    #[test]
    fn truncated_is_rejected() {
        let ids: Vec<u64> = (0..100u64).collect();
        let filter = SegmentFilter::build(1, &ids).unwrap();
        let bytes = filter.to_bytes();
        let short = bytes[..bytes.len() - 1].to_vec();
        assert!(matches!(SegmentFilter::from_bytes(&short), Err(FilterError::Corrupt(_))));
    }

    #[test]
    fn bad_magic_is_rejected() {
        let ids: Vec<u64> = (0..10u64).collect();
        let filter = SegmentFilter::build(1, &ids).unwrap();
        let mut bytes = filter.to_bytes();
        bytes[0] ^= 0xff;
        assert!(matches!(SegmentFilter::from_bytes(&bytes), Err(FilterError::Corrupt(_))));
    }

    /// Correctness property: NO false negatives across a seeded corpus of
    /// present keys, and the false-positive rate over absent keys stays well
    /// under 1% (spike band: ~0.002%).
    #[test]
    fn no_false_negatives_and_fpr_sanity() {
        // Seeded xorshift64 so the corpus is deterministic without pulling in
        // a `rand` dev-dependency.
        fn xorshift(seed: &mut u64) -> u64 {
            *seed ^= *seed << 13;
            *seed ^= *seed >> 7;
            *seed ^= *seed << 17;
            *seed
        }

        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut present = std::collections::BTreeSet::new();
        while present.len() < 20_000 {
            present.insert(xorshift(&mut seed));
        }
        let ids: Vec<u64> = present.iter().copied().collect();
        let filter = SegmentFilter::build(7, &ids).unwrap();

        for &id in &ids {
            assert!(filter.might_contain(id), "false negative for present key {id}");
        }

        let mut false_positives = 0u64;
        let mut checked = 0u64;
        while checked < 100_000 {
            let candidate = xorshift(&mut seed);
            if present.contains(&candidate) {
                continue; // only sample genuinely absent keys
            }
            checked += 1;
            if filter.might_contain(candidate) {
                false_positives += 1;
            }
        }
        let fpr = false_positives as f64 / checked as f64;
        assert!(fpr < 0.01, "false positive rate too high: {fpr} ({false_positives}/{checked})");
        eprintln!("bn-1i7 FPR sanity: {false_positives}/{checked} = {:.5}%", fpr * 100.0);
    }
}
