//! bn-9mw Spike E: coverage-guided fuzzing of the v4 recovery scanner's
//! whole-image entry point
//! ([`mess_log::v4::recover::scan_v4_image_physical`]).
//!
//! `data` is a whole v4 segment image (`SegmentHeader` + zero or more
//! capsules). The scan must never panic, never claim to consume more than the
//! input, and must only ever accept a contiguous prefix of capsules with
//! mandatory `batch_id` contiguity (`0,1,2,…`) and byte spans within the input
//! — the v4 analogue of `fuzz_scanner`'s A10/A2 invariants.
#![no_main]

use libfuzzer_sys::fuzz_target;
use mess_log::v4::format::{MAX_CAPSULE_LEN, MIN_CAPSULE_LEN};
use mess_log::v4::recover::scan_v4_image_physical;

const SEGMENT_HEADER_LEN: u64 = 52;

fuzz_target!(|data: &[u8]| {
    let rec = scan_v4_image_physical(data);

    assert!(
        rec.safe_offset as usize <= data.len(),
        "safe_offset exceeds input length"
    );

    if rec.header.is_some() {
        let mut expect_off = SEGMENT_HEADER_LEN;
        let mut expect_batch = 0u64;
        let mut expect_gp = rec
            .accepted
            .first()
            .map(|c| c.first_global_pos)
            .unwrap_or(0);
        for c in &rec.accepted {
            assert!(c.total_len >= MIN_CAPSULE_LEN, "below MIN_CAPSULE_LEN");
            assert!(c.total_len <= MAX_CAPSULE_LEN, "above MAX_CAPSULE_LEN");
            assert_eq!(c.offset, expect_off, "capsules must be contiguous");
            assert_eq!(c.batch_id, expect_batch, "batch_id must be contiguous");
            assert_eq!(c.first_global_pos, expect_gp, "gp must be contiguous");
            assert!(
                c.offset + c.total_len <= data.len() as u64,
                "capsule span exceeds input"
            );
            // control-only capsules advance no global position.
            assert_eq!(c.control_only, c.event_count == 0);
            expect_off += c.total_len;
            expect_batch += 1;
            expect_gp += u64::from(c.event_count);
        }
        assert_eq!(
            expect_off, rec.safe_offset,
            "safe_offset must sit exactly after the last accepted capsule"
        );
        assert_eq!(rec.next_batch_id, expect_batch);
    } else {
        assert!(rec.accepted.is_empty());
        assert_eq!(rec.safe_offset, 0);
    }
});
