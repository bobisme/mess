//! bn-9mw Spike E: coverage-guided fuzzing of the v4 control-record TLV codec
//! ([`mess_log::v4::control::ControlRecord::decode_at`]) — **parse-only** plus
//! **parse-then-reencode**.
//!
//! `data` is treated as a control region: walk it decoding one record at a time.
//! Every decode must be total (never panic, never over-allocate: all length-
//! prefixed fields are §19-capped before slicing). For every record that
//! decodes, re-encode it and re-decode the result — the codec must round-trip
//! exactly (encode(decode(x)) == x at the value level), and the re-encoded
//! bytes must be self-consistent (the frame's `payload_len` equals what the
//! kind consumes).
#![no_main]

use libfuzzer_sys::fuzz_target;
use mess_log::v4::control::ControlRecord;

fuzz_target!(|data: &[u8]| {
    let mut off = 0usize;
    // Bound the walk so a pathological input can't spin forever; the real
    // control_count cap is MAX_CONTROL_COUNT (4096).
    let mut guard = 0u32;
    while off < data.len() && guard < 8192 {
        guard += 1;
        match ControlRecord::decode_at(data, off) {
            Ok((record, consumed)) => {
                assert!(consumed > 0, "a decoded record must consume > 0 bytes");
                assert_eq!(
                    consumed,
                    record.on_disk_len(),
                    "consumed must equal on_disk_len"
                );

                // Parse-then-reencode: re-encode and re-decode; the value must
                // round-trip exactly and tile its own frame.
                let mut buf = Vec::new();
                record.encode_into(&mut buf);
                assert_eq!(
                    buf.len(),
                    record.on_disk_len(),
                    "re-encoded record length must equal on_disk_len"
                );
                let (round, round_consumed) = ControlRecord::decode_at(&buf, 0)
                    .expect("a freshly re-encoded record must decode");
                assert_eq!(round, record, "control codec must round-trip");
                assert_eq!(round_consumed, buf.len(), "round-trip must tile");

                off += consumed;
            }
            Err(_) => break, // typed error, never a panic — the invariant.
        }
    }
});
