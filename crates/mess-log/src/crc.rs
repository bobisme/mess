//! CRC32C (Castagnoli) with the R4 / D-FMT-8 **split coverage**.
//!
//! # Why CRC32C and not `crc32fast`
//!
//! The spec (§5.1) mandates **CRC32C** — the Castagnoli polynomial
//! (`0x1EDC6F41`, reflected `0x82F63B78`), the hardware-accelerated
//! (`crc32` SSE4.2 / ARM CRC) variant. The popular `crc32fast` crate computes
//! the **CRC-32/ISO-HDLC** polynomial (`0x04C11DB7`) used by zip/gzip — a
//! *different* polynomial that produces different bytes and does not
//! interoperate with a conforming log. We therefore depend on the
//! [`crc32c`](https://docs.rs/crc32c) crate, which computes the correct
//! Castagnoli CRC and uses the SSE4.2/ARM CRC instructions when available.
//! (Decision recorded in the bone summary.)
//!
//! # The split coverage (R4, D-FMT-8, §5.2)
//!
//! > **R4.** CRC/hash coverage is split **around** the checksum fields —
//! > recovery never copies a batch to zero fields before verifying.
//!
//! A batch has two 4-byte checksum-bearing fields that cannot cover
//! themselves: `batch_crc` in the header at `[68, 72)` and `batch_crc_echo`
//! in the marker at `[total_len - 4, total_len)`. The CRC is computed over the
//! batch bytes with **exactly those two 4-byte fields excluded** — two
//! contiguous ranges:
//!
//! ```text
//! batch_crc = CRC32C( batch[0 .. 68]  ++  batch[72 .. total_len - 4] )
//! ```
//!
//! This is computed by feeding the hasher the two slices in order
//! ([`crc32c::crc32c_append`] continues a running CRC), **never** by
//! materializing a zeroed copy of the batch. This differs bit-for-bit from the
//! spikes' copy-and-zero approach (D-FMT-8): spike test vectors are therefore
//! not reusable as golden bytes.

use crate::format::HEADER_CRC_OFF;

/// The CRC32C of the concatenation `data_a ++ data_b`, computed as a single
/// running CRC with no intermediate allocation. This is the primitive R4 is
/// about: two ranges, no copy-and-zero.
#[inline]
pub fn crc32c_two(data_a: &[u8], data_b: &[u8]) -> u32 {
    let c = crc32c::crc32c(data_a);
    crc32c::crc32c_append(c, data_b)
}

/// Compute a batch's `batch_crc` over the R4 split coverage (§5.2).
///
/// `batch` is the fully laid-out batch of length `total_len`. The bytes at
/// `[68, 72)` (the header `batch_crc`) and `[total_len - 4, total_len)` (the
/// marker `batch_crc_echo`) are skipped; everything else — both magics,
/// `total_len_echo`, the crypto chain if present, and every subframe and
/// payload byte — is covered.
///
/// # Panics
///
/// Debug-asserts that `batch.len() >= MIN_BATCH_LEN`; a shorter slice is a
/// caller bug (the encoder never produces one).
#[inline]
pub fn batch_crc(batch: &[u8]) -> u32 {
    let total_len = batch.len();
    debug_assert!(
        total_len >= crate::format::MIN_BATCH_LEN as usize,
        "batch shorter than MIN_BATCH_LEN cannot carry both checksum fields",
    );
    // [0, 68) then [72, total_len - 4): split AROUND the two crc fields.
    crc32c_two(
        &batch[..HEADER_CRC_OFF],
        &batch[HEADER_CRC_OFF + 4..total_len - 4],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Castagnoli check value: CRC32C of the nine ASCII bytes "123456789"
    /// is `0xE3069283` — the canonical CRC-32/ISCSI check constant. This pins
    /// that we are on the Castagnoli polynomial, not CRC-32/ISO-HDLC (whose
    /// check value for the same input is `0xCBF43926`).
    #[test]
    fn castagnoli_check_value() {
        assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283);
        assert_ne!(
            crc32c::crc32c(b"123456789"),
            0xCBF4_3926,
            "that is CRC-32/ISO, wrong polynomial"
        );
    }

    /// `crc32c_two` is exactly the CRC of the concatenation.
    #[test]
    fn two_slice_equals_concatenation() {
        let a = b"the log is the only";
        let b = b" commit authority";
        let mut cat = Vec::new();
        cat.extend_from_slice(a);
        cat.extend_from_slice(b);
        assert_eq!(crc32c_two(a, b), crc32c::crc32c(&cat));
    }

    /// The split coverage is NOT the copy-and-zero result the spikes used
    /// (D-FMT-8): zeroing the two 4-byte fields and hashing the whole 88-byte
    /// span includes eight zero bytes the split excludes, so the values differ.
    #[test]
    fn split_differs_from_copy_and_zero() {
        let mut batch = vec![0xABu8; 128];
        // Put non-zero sentinels in the two checksum fields so the difference
        // is real regardless of surrounding bytes.
        batch[HEADER_CRC_OFF..HEADER_CRC_OFF + 4]
            .copy_from_slice(&[1, 2, 3, 4]);
        let n = batch.len();
        batch[n - 4..].copy_from_slice(&[5, 6, 7, 8]);

        let split = batch_crc(&batch);

        // The spike approach: zero the fields, hash the whole contiguous span.
        let mut zeroed = batch.clone();
        zeroed[HEADER_CRC_OFF..HEADER_CRC_OFF + 4].fill(0);
        zeroed[n - 4..].fill(0);
        let copy_and_zero = crc32c::crc32c(&zeroed);

        assert_ne!(
            split, copy_and_zero,
            "R4 split must diverge from copy-and-zero"
        );
    }
}
