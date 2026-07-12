//! bn-9mw Spike E: golden byte fixtures + encode/decode round-trips for the v4
//! commit-capsule format. Every control kind and every capsule shape
//! (control-only, events-only, mixed, register-and-use-in-same-capsule) is
//! byte-checked: exact `total_len`, exact field values at known offsets, the
//! split-coverage CRC recomputed independently and matched in BOTH fields, and
//! a full physical-decode round trip.
//!
//! The `pinned_*` tests additionally assert the *exact bytes* of a canonical
//! capsule against a committed hex fixture, so any silent layout drift (a
//! field width, an offset, the CRC coverage) fails loudly.

use mess_log::encode::Subframe;
use mess_log::v4::capsule::{
    CapsuleEncoder, CapsuleInput, capsule_crc, decode_capsule,
};
use mess_log::v4::control::ControlRecord;
use mess_log::v4::format::*;

fn enc(input: &CapsuleInput) -> Vec<u8> {
    CapsuleEncoder::new().encode(input).expect("encode").to_vec()
}

fn rd_u16(d: &[u8], o: usize) -> u16 {
    u16::from_le_bytes(d[o..o + 2].try_into().unwrap())
}
fn rd_u32(d: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(d[o..o + 4].try_into().unwrap())
}
fn rd_u64(d: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(d[o..o + 8].try_into().unwrap())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

// ---------------------------------------------------------------------------
// Header + marker byte layout, on a canonical control-only capsule
// ---------------------------------------------------------------------------

#[test]
fn control_only_capsule_header_and_marker_layout() {
    let controls = [ControlRecord::CategoryRegistered {
        category_id: 1,
        name:        "orders".to_string(),
    }];
    let input = CapsuleInput {
        segment_epoch:        7,
        batch_id:             0,
        first_global_pos:     100,
        stream_id:            0,
        category_id:          0,
        first_stream_version: 0,
        crypto_chain:         None,
        controls:             &controls,
        subframes:            &[],
    };
    let bytes = enc(&input);

    // Header fields at their fixed offsets (§4).
    assert_eq!(rd_u32(&bytes, 0), CAPSULE_MAGIC);
    assert_eq!(rd_u16(&bytes, 4), FORMAT_VERSION_V4);
    assert_eq!(rd_u16(&bytes, 6), 0); // flags: no crypto chain
    assert_eq!(rd_u32(&bytes, 8), 0); // event_count
    assert_eq!(rd_u32(&bytes, 12), 1); // control_count
    assert_eq!(rd_u64(&bytes, 16), 0); // batch_id
    assert_eq!(rd_u64(&bytes, 24), bytes.len() as u64); // total_len
    assert_eq!(rd_u64(&bytes, 32), 100); // first_global_pos
    assert_eq!(rd_u64(&bytes, 40), 7); // segment_epoch
    assert_eq!(rd_u64(&bytes, 48), 0); // stream_id
    assert_eq!(rd_u64(&bytes, 56), 0); // category_id
    assert_eq!(rd_u64(&bytes, 64), 0); // first_stream_version
    // control_len == on_disk_len of the one CategoryRegistered control:
    // TLV(8) + payload(category_id 8 + name_len 2 + "orders" 6) = 8 + 16 = 24.
    assert_eq!(rd_u32(&bytes, 72), 24); // control_len
    assert_eq!(rd_u32(&bytes, 76), 0); // event_region_len
    // reserved_hdr (§23.2 dropped header_crc) and reserved MUST be zero.
    assert_eq!(rd_u32(&bytes, 84), 0);
    assert_eq!(rd_u32(&bytes, 92), 0);
    // logical_flags: CONTROL_ONLY | REGISTRY_INTRODUCES_IDS (no dedupe).
    assert_eq!(
        rd_u32(&bytes, 88),
        LFLAG_CONTROL_ONLY | LFLAG_REGISTRY_INTRODUCES_IDS
    );

    // total_len == header(96) + control(24) + marker(32) = 152.
    assert_eq!(bytes.len(), 152);

    // Marker (§17) at total_len - 32.
    let m = bytes.len() - CAPSULE_MARKER_LEN;
    assert_eq!(rd_u32(&bytes, m + 0), CAPSULE_MARKER_MAGIC);
    assert_eq!(rd_u32(&bytes, m + 4), 0); // marker_flags
    assert_eq!(rd_u64(&bytes, m + 8), 0); // batch_id_echo
    assert_eq!(rd_u64(&bytes, m + 16), bytes.len() as u64); // total_len_echo
    assert_eq!(rd_u32(&bytes, m + 28), 0); // marker_reserved

    // CRC: header field == marker echo == independent recomputation.
    let crc_hdr = rd_u32(&bytes, CAPSULE_HEADER_CRC_OFF);
    let crc_echo = rd_u32(&bytes, m + 24);
    assert_eq!(crc_hdr, crc_echo, "capsule_crc and echo must match");
    assert_eq!(crc_hdr, capsule_crc(&bytes));
}

// ---------------------------------------------------------------------------
// CRC split coverage: flipping a byte inside the two skipped 4-byte holes does
// NOT change the CRC value; flipping any covered byte does.
// ---------------------------------------------------------------------------

#[test]
fn crc_split_coverage_excludes_exactly_the_two_checksum_fields() {
    let controls = [ControlRecord::DedupeKey {
        scope_kind: DEDUPE_SCOPE_GLOBAL,
        scope_id:   0,
        key:        b"idem-key-123".to_vec(),
    }];
    let sfs = [Subframe::plain(1, 0, 1, b"payload-bytes")];
    let input = CapsuleInput {
        segment_epoch:        3,
        batch_id:             5,
        first_global_pos:     42,
        stream_id:            9,
        category_id:          2,
        first_stream_version: 7,
        crypto_chain:         None,
        controls:             &controls,
        subframes:            &sfs,
    };
    let bytes = enc(&input);
    let tl = bytes.len();
    let base = capsule_crc(&bytes);

    // The two 4-byte holes: [80,84) header capsule_crc, [tl-8, tl-4) echo.
    for hole in [CAPSULE_HEADER_CRC_OFF, tl - 8] {
        let mut m = bytes.clone();
        for b in &mut m[hole..hole + 4] {
            *b ^= 0xFF;
        }
        assert_eq!(
            capsule_crc(&m),
            base,
            "flipping a byte in a skipped checksum field must NOT change the \
             CRC"
        );
    }

    // Every covered byte, when flipped, DOES change the CRC.
    for off in 0..tl {
        let in_hole = (CAPSULE_HEADER_CRC_OFF..CAPSULE_HEADER_CRC_OFF + 4)
            .contains(&off)
            || (tl - 8..tl - 4).contains(&off);
        if in_hole {
            continue;
        }
        let mut m = bytes.clone();
        m[off] ^= 0xFF;
        assert_ne!(
            capsule_crc(&m),
            base,
            "flipping covered byte at {off} must change the CRC"
        );
    }
}

// ---------------------------------------------------------------------------
// Round-trip: every control kind survives encode -> decode intact.
// ---------------------------------------------------------------------------

fn all_control_kinds() -> Vec<ControlRecord> {
    vec![
        ControlRecord::CategoryRegistered {
            category_id: 2,
            name:        "orders".to_string(),
        },
        ControlRecord::StreamRegistered {
            stream_id:   9,
            category_id: 2,
            name:        "orders.stream-9".to_string(),
        },
        ControlRecord::EventTypeRegistered {
            event_type_id:          3,
            codec_id:               1,
            current_schema_version: 4,
            schema_fingerprint:     [0xAB; 32],
            name:                   "orders.OrderPlaced".to_string(),
        },
        ControlRecord::DedupeKey {
            scope_kind: DEDUPE_SCOPE_STREAM,
            scope_id:   9,
            key:        b"dedupe-key-bytes".to_vec(),
        },
        ControlRecord::ProjectionCheckpoint {
            projection_id:  11,
            position:       12345,
            state_ref_kind: 1,
            state_ref:      b"ckpt-ref".to_vec(),
        },
        ControlRecord::SnapshotInstalled {
            stream_id:               9,
            covered_version:         100,
            covered_global_position: 5000,
            snapshot_slot:           1,
            pack_id:                 77,
            pack_offset:             4096,
            blob_len:                2048,
            codec_id:                1,
            fold_version:            2,
            state_hash:              Some([0x11; 32]),
            event_prefix_hash:       None,
            blob_hash:               Some([0x33; 32]),
        },
    ]
}

#[test]
fn every_control_kind_round_trips_in_a_control_only_capsule() {
    for ctl in all_control_kinds() {
        let controls = [ctl.clone()];
        let input = CapsuleInput {
            segment_epoch:        1,
            batch_id:             0,
            first_global_pos:     0,
            stream_id:            0,
            category_id:          0,
            first_stream_version: 0,
            crypto_chain:         None,
            controls:             &controls,
            subframes:            &[],
        };
        let bytes = enc(&input);
        let decoded = decode_capsule(&bytes, 0).expect("decode");
        assert_eq!(decoded.controls.len(), 1);
        assert_eq!(decoded.controls[0], ctl, "control must round-trip");
        assert_eq!(decoded.events.len(), 0);
        assert_eq!(decoded.header.event_count, 0);
        assert_eq!(decoded.header.control_count, 1);
    }
}

#[test]
fn all_controls_in_one_capsule_tile_and_round_trip() {
    let controls = all_control_kinds();
    let input = CapsuleInput {
        segment_epoch:        1,
        batch_id:             0,
        first_global_pos:     0,
        stream_id:            0,
        category_id:          0,
        first_stream_version: 0,
        crypto_chain:         None,
        controls:             &controls,
        subframes:            &[],
    };
    let bytes = enc(&input);
    let decoded = decode_capsule(&bytes, 0).expect("decode");
    assert_eq!(decoded.controls, controls, "all controls tile + round-trip");
}

// ---------------------------------------------------------------------------
// Capsule shapes
// ---------------------------------------------------------------------------

#[test]
fn events_only_capsule_round_trips() {
    let sfs = [
        Subframe::plain(1, 0, 1, b"e0"),
        Subframe::plain(1, 1, 1, b"e1-longer"),
    ];
    let input = CapsuleInput {
        segment_epoch:        1,
        batch_id:             2,
        first_global_pos:     10,
        stream_id:            9,
        category_id:          2,
        first_stream_version: 3,
        crypto_chain:         None,
        controls:             &[],
        subframes:            &sfs,
    };
    let bytes = enc(&input);
    let d = decode_capsule(&bytes, 0).expect("decode");
    assert_eq!(d.header.event_count, 2);
    assert_eq!(d.header.control_count, 0);
    assert_eq!(d.header.logical_flags & LFLAG_CONTROL_ONLY, 0);
    assert_eq!(d.events[0].payload, b"e0");
    assert_eq!(d.events[1].payload, b"e1-longer");
    assert_eq!(d.events[1].schema_version, 1);
}

#[test]
fn mixed_register_and_use_in_same_capsule_round_trips() {
    // Register a category, a stream, and an event type, then two events that
    // reference that new event type — the §10/§11 register-and-use shape.
    let controls = [
        ControlRecord::CategoryRegistered {
            category_id: 2,
            name:        "orders".to_string(),
        },
        ControlRecord::StreamRegistered {
            stream_id:   9,
            category_id: 2,
            name:        "orders.stream-9".to_string(),
        },
        ControlRecord::EventTypeRegistered {
            event_type_id:          3,
            codec_id:               1,
            current_schema_version: 1,
            schema_fingerprint:     [0xCD; 32],
            name:                   "orders.OrderPlaced".to_string(),
        },
    ];
    let sfs = [
        Subframe::plain(3, 1, 1, b"placed-0"),
        Subframe::plain(3, 1, 1, b"placed-1"),
    ];
    let input = CapsuleInput {
        segment_epoch:        1,
        batch_id:             0,
        first_global_pos:     0,
        stream_id:            9,
        category_id:          2,
        first_stream_version: 0,
        crypto_chain:         None,
        controls:             &controls,
        subframes:            &sfs,
    };
    let bytes = enc(&input);
    let d = decode_capsule(&bytes, 0).expect("decode");
    assert_eq!(d.controls.len(), 3);
    assert_eq!(d.events.len(), 2);
    assert_eq!(d.header.stream_id, 9);
    assert_eq!(
        d.header.logical_flags & LFLAG_REGISTRY_INTRODUCES_IDS,
        LFLAG_REGISTRY_INTRODUCES_IDS
    );
    for ev in &d.events {
        assert_eq!(ev.event_type_id, 3);
    }
}

#[test]
fn crypto_chain_placement_shifts_regions() {
    let chain = [0x5Au8; CAPSULE_CHAIN_LEN];
    let sfs = [Subframe::plain(1, 0, 1, b"x")];
    let input = CapsuleInput {
        segment_epoch:        1,
        batch_id:             0,
        first_global_pos:     0,
        stream_id:            9,
        category_id:          0,
        first_stream_version: 0,
        crypto_chain:         Some(&chain),
        controls:             &[],
        subframes:            &sfs,
    };
    let bytes = enc(&input);
    assert_eq!(rd_u16(&bytes, 6) & FLAG_CRYPTO_CHAIN, FLAG_CRYPTO_CHAIN);
    // Chain occupies [96, 128).
    assert_eq!(&bytes[CAPSULE_HEADER_LEN..CAPSULE_HEADER_LEN + 32], &chain);
    let d = decode_capsule(&bytes, 0).expect("decode");
    assert!(d.header.has_crypto_chain);
    assert_eq!(d.events[0].payload, b"x");
}

// ---------------------------------------------------------------------------
// Pinned byte fixtures — exact hex, regenerated & compared so layout drift
// fails. If the format legitimately changes, regenerate with `--nocapture` and
// paste the printed hex.
// ---------------------------------------------------------------------------

/// Canonical control-only capsule (the layout test's input): pinned bytes.
const PINNED_CONTROL_ONLY: &str = "ad4e95ca0400000000000000010000000000000000000000980000000000000064000000000000000700000000000000000000000000000000000000000000000000000000000000180000000000000035aa5a3f0000000005000000000000000300010010000000010000000000000006006f7264657273ed1795ca000000000000000000000000980000000000000035aa5a3f00000000";

/// Canonical mixed register-and-use capsule: pinned bytes.
const PINNED_MIXED: &str = "ad4e95ca0400000001000000020000000000000000000000f7000000000000000000000000000000010000000000000009000000000000000200000000000000000000000000000055000000220000008bedcdad0000000004000000000000000300010010000000020000000000000006006f726465727302000100350000000300000001000100cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd0b004f72646572506c6163656403000000010001000000000006000000060000000000000006000000706c61636564ed1795ca000000000000000000000000f7000000000000008bedcdad00000000";

#[test]
fn pinned_control_only_capsule_bytes() {
    let controls = [ControlRecord::CategoryRegistered {
        category_id: 1,
        name:        "orders".to_string(),
    }];
    let input = CapsuleInput {
        segment_epoch:        7,
        batch_id:             0,
        first_global_pos:     100,
        stream_id:            0,
        category_id:          0,
        first_stream_version: 0,
        crypto_chain:         None,
        controls:             &controls,
        subframes:            &[],
    };
    let bytes = enc(&input);
    let got = hex(&bytes);
    println!("PINNED_CONTROL_ONLY = {got}");
    // Round-trip always holds; the pin catches byte-layout drift.
    let d = decode_capsule(&bytes, 0).unwrap();
    assert_eq!(d.controls[0], controls[0]);
    if !PINNED_CONTROL_ONLY.is_empty() {
        assert_eq!(
            got, PINNED_CONTROL_ONLY,
            "control-only capsule bytes drift"
        );
        assert_eq!(bytes, unhex(PINNED_CONTROL_ONLY));
    }
}

#[test]
fn pinned_mixed_capsule_bytes() {
    let controls = [
        ControlRecord::CategoryRegistered {
            category_id: 2,
            name:        "orders".to_string(),
        },
        ControlRecord::EventTypeRegistered {
            event_type_id:          3,
            codec_id:               1,
            current_schema_version: 1,
            schema_fingerprint:     [0xCD; 32],
            name:                   "OrderPlaced".to_string(),
        },
    ];
    let sfs = [Subframe::plain(3, 1, 1, b"placed")];
    let input = CapsuleInput {
        segment_epoch:        1,
        batch_id:             0,
        first_global_pos:     0,
        stream_id:            9,
        category_id:          2,
        first_stream_version: 0,
        crypto_chain:         None,
        controls:             &controls,
        subframes:            &sfs,
    };
    let bytes = enc(&input);
    let got = hex(&bytes);
    println!("PINNED_MIXED = {got}");
    let d = decode_capsule(&bytes, 0).unwrap();
    assert_eq!(d.controls, controls);
    assert_eq!(d.events[0].payload, b"placed");
    if !PINNED_MIXED.is_empty() {
        assert_eq!(got, PINNED_MIXED, "mixed capsule bytes drift");
    }
}
