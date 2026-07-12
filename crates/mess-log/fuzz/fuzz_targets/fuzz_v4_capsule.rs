//! bn-9mw Spike E: coverage-guided fuzzing of the v4 capsule physical decoder
//! ([`mess_log::v4::capsule::decode_capsule`]) — **parse-only** plus a
//! **parse-then-reencode** round trip of the decoded control region.
//!
//! `data` is treated as one candidate capsule at offset 0. `decode_capsule`
//! must be total: never panic, never over-allocate (the A2 `total_len` cap and
//! the §19 control caps bound every allocation/read before it happens), and an
//! `Ok` result must be a capsule that fully re-verified against these exact
//! bytes (marker echoes, the mandatory split-coverage CRC, exact control+event
//! tiling). For a successfully decoded capsule, re-encoding its controls into a
//! fresh control-only capsule must itself decode and yield the same controls —
//! the parse-then-reencode leg for the control region.
#![no_main]

use libfuzzer_sys::fuzz_target;
use mess_log::v4::capsule::{
    CapsuleEncoder, CapsuleInput, decode_capsule,
};
use mess_log::v4::format::{
    CAPSULE_HEADER_LEN, CAPSULE_MARKER_LEN, MAX_CAPSULE_LEN, MIN_CAPSULE_LEN,
};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let Ok(decoded) = decode_capsule(data, 0) else {
        return; // typed error, never a panic — the core invariant.
    };
    let h = &decoded.header;

    // A2 bounds hold for anything accepted.
    assert!(h.total_len >= MIN_CAPSULE_LEN, "below MIN_CAPSULE_LEN");
    assert!(h.total_len <= MAX_CAPSULE_LEN, "above MAX_CAPSULE_LEN");
    assert!(
        h.total_len as usize <= data.len(),
        "accepted capsule span exceeds input"
    );
    // §6: control-only iff no events; counts agree with decoded regions.
    assert_eq!(decoded.controls.len(), h.control_count as usize);
    assert_eq!(decoded.events.len(), h.event_count as usize);
    assert!(h.control_count as u64 + u64::from(h.event_count) >= 1);

    // Parse-then-reencode the control region: encode the decoded controls into
    // a fresh control-only capsule and decode it back — the controls must be
    // byte-reversible.
    if !decoded.controls.is_empty() {
        let input = CapsuleInput {
            segment_epoch:        1,
            batch_id:             0,
            first_global_pos:     0,
            stream_id:            0,
            category_id:          0,
            first_stream_version: 0,
            crypto_chain:         None,
            controls:             &decoded.controls,
            subframes:            &[],
        };
        let mut enc = CapsuleEncoder::new();
        if let Ok(bytes) = enc.encode(&input) {
            let bytes = bytes.to_vec();
            assert!(bytes.len() >= CAPSULE_HEADER_LEN + CAPSULE_MARKER_LEN);
            let round = decode_capsule(&bytes, 0)
                .expect("a freshly re-encoded capsule must decode");
            assert_eq!(
                round.controls, decoded.controls,
                "control region must round-trip through re-encode"
            );
        }
    }
});
