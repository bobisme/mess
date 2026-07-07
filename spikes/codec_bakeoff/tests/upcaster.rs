//! Upcaster pipeline tests, including golden fixtures: V1/V2 bytes committed
//! as constants must keep decoding to the correct V3 values forever.

use codec_bakeoff::upcaster::*;

// ---------------------------------------------------------------- fixtures
//
// Golden bytes: msgpack-named (codec_id 1) encodings captured once and
// committed. If struct definitions, serde attributes, or the codec drift in a
// way that breaks old stored events, these tests fail. This is exactly the
// harness shape a derive would generate from `#[event(fixture = "...")]`.
//
// Regenerate (only when intentionally re-capturing): the `print_fixtures`
// test below, `cargo test --release -- --ignored --nocapture`.

// TripCompletedV1 { trip_id: 42, driver: "maria", distance_miles: 12.5, completed_at: 1_750_000_000 }
const V1_FIXTURE: &[u8] = &[
    0x84, 0xa7, 0x74, 0x72, 0x69, 0x70, 0x5f, 0x69, 0x64, 0x2a, 0xa6, 0x64, 0x72, 0x69, 0x76,
    0x65, 0x72, 0xa5, 0x6d, 0x61, 0x72, 0x69, 0x61, 0xae, 0x64, 0x69, 0x73, 0x74, 0x61, 0x6e,
    0x63, 0x65, 0x5f, 0x6d, 0x69, 0x6c, 0x65, 0x73, 0xcb, 0x40, 0x29, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0xac, 0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x5f, 0x61, 0x74,
    0xce, 0x68, 0x4e, 0xe1, 0x80,
];

// TripCompletedV2 { trip_id: 43, driver_name: "yusuf", distance_miles: 3.2, completed_at: 1_750_000_100 }
const V2_FIXTURE: &[u8] = &[
    0x84, 0xa7, 0x74, 0x72, 0x69, 0x70, 0x5f, 0x69, 0x64, 0x2b, 0xab, 0x64, 0x72, 0x69, 0x76,
    0x65, 0x72, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0xa5, 0x79, 0x75, 0x73, 0x75, 0x66, 0xae, 0x64,
    0x69, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x5f, 0x6d, 0x69, 0x6c, 0x65, 0x73, 0xcb, 0x40,
    0x09, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a, 0xac, 0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74,
    0x65, 0x64, 0x5f, 0x61, 0x74, 0xce, 0x68, 0x4e, 0xe1, 0xe4,
];

fn stored(version: u16, payload: &[u8]) -> StoredEvent {
    StoredEvent {
        event_name: "trip.completed".to_string(),
        schema_version: version,
        codec_id: CODEC_ID_MSGPACK_NAMED,
        payload: payload.to_vec(),
    }
}

#[test]
fn v1_fixture_upcasts_to_correct_v3() {
    let v3 = decode_trip_completed(&stored(1, V1_FIXTURE)).unwrap();
    assert_eq!(
        v3,
        TripCompletedV3 {
            trip_id: 42,
            driver_name: "maria".to_string(),        // renamed by V1->V2 upcast
            distance_m: 12.5 * METERS_PER_MILE,      // 20116.8 — unit change
            completed_at_ms: 1_750_000_000_000,      // s -> ms
            rating: None,
        }
    );
}

#[test]
fn v2_fixture_upcasts_to_correct_v3() {
    let v3 = decode_trip_completed(&stored(2, V2_FIXTURE)).unwrap();
    assert_eq!(v3.trip_id, 43);
    assert_eq!(v3.driver_name, "yusuf");
    assert!((v3.distance_m - 3.2 * METERS_PER_MILE).abs() < 1e-9);
    assert_eq!(v3.completed_at_ms, 1_750_000_100_000);
    assert_eq!(v3.rating, None);
}

#[test]
fn v3_decodes_directly_without_upcasting() {
    let v3 = TripCompletedV3 {
        trip_id: 44,
        driver_name: "kenji".to_string(),
        distance_m: 800.0,
        completed_at_ms: 1_750_000_200_000,
        rating: Some(5),
    };
    let ev = store("trip.completed", 3, &v3);
    assert_eq!(decode_trip_completed(&ev).unwrap(), v3);
}

#[test]
fn round_trip_through_every_version() {
    // Write-path sanity: encode each version's struct, decode through the
    // pipeline, verify semantic equivalence.
    let v1 = TripCompletedV1 {
        trip_id: 7,
        driver: "ana".to_string(),
        distance_miles: 1.0,
        completed_at: 1_700_000_000,
    };
    let out = decode_trip_completed(&store("trip.completed", 1, &v1)).unwrap();
    assert_eq!(out.driver_name, "ana");
    assert!((out.distance_m - METERS_PER_MILE).abs() < 1e-9);
    assert_eq!(out.completed_at_ms, 1_700_000_000_000);
}

#[test]
fn unknown_version_fails_loudly() {
    let err = decode_trip_completed(&stored(99, V1_FIXTURE)).unwrap_err();
    assert_eq!(
        err,
        DecodeError::UnknownVersion { event_name: "trip.completed".to_string(), version: 99 }
    );
}

#[test]
fn unknown_codec_fails_loudly() {
    let mut ev = stored(1, V1_FIXTURE);
    ev.codec_id = 200;
    assert_eq!(decode_trip_completed(&ev).unwrap_err(), DecodeError::UnknownCodec(200));
}

#[test]
fn wrong_event_name_fails_loudly() {
    let mut ev = stored(1, V1_FIXTURE);
    ev.event_name = "trip.cancelled".to_string();
    assert!(matches!(
        decode_trip_completed(&ev).unwrap_err(),
        DecodeError::WrongEventName { .. }
    ));
}

#[test]
fn corrupt_payload_fails_loudly() {
    let ev = stored(1, &V1_FIXTURE[..10]);
    assert!(matches!(decode_trip_completed(&ev).unwrap_err(), DecodeError::Codec(_)));
}

/// Determinism guard: encoding the fixture values today must still produce
/// the committed bytes (msgpack-named is deterministic for these shapes).
#[test]
fn encoder_still_produces_fixture_bytes() {
    let v1 = TripCompletedV1 {
        trip_id: 42,
        driver: "maria".to_string(),
        distance_miles: 12.5,
        completed_at: 1_750_000_000,
    };
    assert_eq!(store("trip.completed", 1, &v1).payload, V1_FIXTURE);
    let v2 = TripCompletedV2 {
        trip_id: 43,
        driver_name: "yusuf".to_string(),
        distance_miles: 3.2,
        completed_at: 1_750_000_100,
    };
    assert_eq!(store("trip.completed", 2, &v2).payload, V2_FIXTURE);
}

/// One-time fixture capture helper.
#[test]
#[ignore]
fn print_fixtures() {
    let v1 = TripCompletedV1 {
        trip_id: 42,
        driver: "maria".to_string(),
        distance_miles: 12.5,
        completed_at: 1_750_000_000,
    };
    let v2 = TripCompletedV2 {
        trip_id: 43,
        driver_name: "yusuf".to_string(),
        distance_miles: 3.2,
        completed_at: 1_750_000_100,
    };
    for (name, bytes) in [
        ("V1", store("trip.completed", 1, &v1).payload),
        ("V2", store("trip.completed", 2, &v2).payload),
    ] {
        let hex: Vec<String> = bytes.iter().map(|b| format!("0x{b:02x}")).collect();
        println!("{name}: &[{}]", hex.join(", "));
    }
}
