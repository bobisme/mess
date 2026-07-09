//! bn-gux: coverage-guided fuzzing of the recovery scanner's byte-slice scan
//! entry point, [`mess_log::scanner::scan_image`].
//!
//! `data` is treated as a whole segment image (`SegmentHeader` followed by
//! zero or more batches — exactly the bytes [`scan_image`] is documented to
//! consume). This is A12's negative space: the scanner must never panic,
//! never over-allocate relative to the input it was given (A2's cap bounds
//! any single accepted batch; the accept loop can never read past `data`),
//! and must never accept a batch that doesn't fully re-verify against the
//! input bytes — only ever stop with a typed [`mess_log::scanner::ScanStop`].
//!
//! No `unwrap`/`panic` in this harness is reachable from fuzzer input: every
//! assertion below is a genuine invariant `scan_image` itself must uphold,
//! chosen so a violation is a real bug, not a harness bug.
#![no_main]

use libfuzzer_sys::fuzz_target;
use mess_log::format::{MAX_BATCH_LEN, MIN_BATCH_LEN, SEGMENT_HEADER_LEN};
use mess_log::scanner::scan_image;

fuzz_target!(|data: &[u8]| {
    let recovery = scan_image(data, None);

    // The scan can never claim to have consumed more of the input than
    // exists: `safe_offset` is the byte offset of the first invalid/
    // incomplete batch (or end-of-segment), always <= data.len().
    assert!(
        recovery.safe_offset as usize <= data.len(),
        "safe_offset {} exceeds input length {}",
        recovery.safe_offset,
        data.len()
    );

    // Every accepted batch must independently re-verify: it lies fully
    // within the input, its `total_len` respects the A2 cap (which bounds
    // the allocation/read any single candidate can trigger), and accepted
    // batches are laid out back-to-back with no gap or overlap (A1's
    // contiguity is a *position* invariant enforced by the acceptance
    // kernel; here we check the *byte* invariant that follows from it: each
    // batch's on-disk span is exactly `total_len` bytes starting where the
    // previous one ended). A batch is only ever attempted once the
    // `SegmentHeader` itself validated (`recovery.header.is_some()`); when it
    // didn't, `accepted` is always empty and `safe_offset == 0`.
    if recovery.header.is_some() {
        let mut expect_off = SEGMENT_HEADER_LEN as u64;
        for b in &recovery.accepted {
            assert!(b.total_len >= MIN_BATCH_LEN, "accepted batch below MIN_BATCH_LEN (A2)");
            assert!(b.total_len <= MAX_BATCH_LEN, "accepted batch above MAX_BATCH_LEN (A2)");
            assert_eq!(b.offset, expect_off, "accepted batches must be contiguous, no resync (A10)");
            assert!(
                b.offset + b.total_len <= data.len() as u64,
                "accepted batch span exceeds input length"
            );
            expect_off = b.offset + b.total_len;
        }
        assert_eq!(
            expect_off, recovery.safe_offset,
            "safe_offset must sit exactly after the last accepted batch"
        );
    } else {
        assert!(recovery.accepted.is_empty());
        assert_eq!(recovery.safe_offset, 0);
    }
});
