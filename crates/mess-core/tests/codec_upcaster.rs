//! Upcaster pipeline tests, including golden fixtures: V1/V2 bytes
//! committed as constants must keep decoding to the correct V3 values
//! forever.
//!
//! Ported from `spikes/codec_bakeoff/tests/upcaster.rs`. The fixture bytes
//! below are copied verbatim from that spike (not regenerated) per the
//! bake-off's rule: "every released schema version keeps a committed byte
//! fixture that must decode to latest."

use mess_core::codec::{StoredEvent, Upcast, UpcastError};
use mess_core::event_versions;
use serde::{Deserialize, Serialize};

// ------------------------------------------------------- the example event
//
// A 3-version event with real semantic migrations, matching
// `spikes/codec_bakeoff/src/upcaster.rs` exactly (same field names, same
// upcast logic) so the committed fixture bytes below remain valid.

/// V1: driver referenced by free-text name, distance in miles, timestamp
/// in whole seconds.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct TripCompletedV1 {
    trip_id: u64,
    driver: String,
    distance_miles: f64,
    completed_at: i64, // unix seconds
}

/// V2: semantic migration #1 — field rename `driver` -> `driver_name`
/// (the rename happens in the upcaster, NOT via `#[serde(rename)]`; old
/// bytes still say "driver" and only V1's struct ever reads them).
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct TripCompletedV2 {
    trip_id: u64,
    driver_name: String,
    distance_miles: f64,
    completed_at: i64, // unix seconds
}

/// V3: semantic migration #2 — unit changes (miles -> meters, seconds ->
/// milliseconds) plus an additive optional field.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct TripCompletedV3 {
    trip_id: u64,
    driver_name: String,
    distance_m: f64,
    completed_at_ms: i64,
    rating: Option<u8>,
}

const METERS_PER_MILE: f64 = 1609.344;

impl Upcast<TripCompletedV1> for TripCompletedV2 {
    fn upcast(v1: TripCompletedV1) -> Self {
        TripCompletedV2 {
            trip_id: v1.trip_id,
            driver_name: v1.driver, // the rename, expressed in code
            distance_miles: v1.distance_miles,
            completed_at: v1.completed_at,
        }
    }
}

impl Upcast<TripCompletedV2> for TripCompletedV3 {
    fn upcast(v2: TripCompletedV2) -> Self {
        TripCompletedV3 {
            trip_id: v2.trip_id,
            driver_name: v2.driver_name,
            distance_m: v2.distance_miles * METERS_PER_MILE, // unit change
            // Regression (bn-meo fuzzing): a plain `*` here panicked
            // ("attempt to multiply with overflow") on an old-version
            // event whose `completed_at` (an attacker/corruption-supplied
            // i64, decoded straight off an untrusted historical payload --
            // this Upcast impl has no say over what value showed up) was
            // large enough that `* 1000` overflows i64. An `Upcast` impl
            // is ordinary Rust code once the payload has decoded to a
            // typed struct, so this isn't the codec layer's job to
            // validate -- but it must still never panic on adversarial
            // *values* within a well-typed field, the same "loud typed
            // failure or a well-defined answer, never a crash" bar as the
            // rest of this pipeline. Saturating is the well-defined answer
            // here: for every value this multiplication does NOT overflow
            // for (all real timestamps), it's bit-for-bit identical to `*`.
            completed_at_ms: v2.completed_at.saturating_mul(1000),
            rating: None,
        }
    }
}

event_versions! {
    name: "trip.completed",
    latest: TripCompletedV3,
    versions: [ 1 => TripCompletedV1, 2 => TripCompletedV2, 3 => TripCompletedV3 ],
    decode_fn: decode_trip_completed
}

// ---------------------------------------------------------------- fixtures
//
// Golden bytes: msgpack-named (codec_id 1) encodings, copied verbatim from
// `spikes/codec_bakeoff/tests/upcaster.rs`. If struct definitions, serde
// attributes, or the codec drift in a way that breaks old stored events,
// these tests fail.

// TripCompletedV1 { trip_id: 42, driver: "maria", distance_miles: 12.5, completed_at: 1_750_000_000 }
const V1_FIXTURE: &[u8] = &[
    0x84, 0xa7, 0x74, 0x72, 0x69, 0x70, 0x5f, 0x69, 0x64, 0x2a, 0xa6, 0x64,
    0x72, 0x69, 0x76, 0x65, 0x72, 0xa5, 0x6d, 0x61, 0x72, 0x69, 0x61, 0xae,
    0x64, 0x69, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x5f, 0x6d, 0x69, 0x6c,
    0x65, 0x73, 0xcb, 0x40, 0x29, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xac,
    0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x5f, 0x61, 0x74,
    0xce, 0x68, 0x4e, 0xe1, 0x80,
];

// TripCompletedV2 { trip_id: 43, driver_name: "yusuf", distance_miles: 3.2, completed_at: 1_750_000_100 }
const V2_FIXTURE: &[u8] = &[
    0x84, 0xa7, 0x74, 0x72, 0x69, 0x70, 0x5f, 0x69, 0x64, 0x2b, 0xab, 0x64,
    0x72, 0x69, 0x76, 0x65, 0x72, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0xa5, 0x79,
    0x75, 0x73, 0x75, 0x66, 0xae, 0x64, 0x69, 0x73, 0x74, 0x61, 0x6e, 0x63,
    0x65, 0x5f, 0x6d, 0x69, 0x6c, 0x65, 0x73, 0xcb, 0x40, 0x09, 0x99, 0x99,
    0x99, 0x99, 0x99, 0x9a, 0xac, 0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x74,
    0x65, 0x64, 0x5f, 0x61, 0x74, 0xce, 0x68, 0x4e, 0xe1, 0xe4,
];

fn stored(version: u16, payload: &[u8]) -> StoredEvent {
    StoredEvent {
        event_name: "trip.completed".to_string(),
        schema_version: version,
        codec_id: mess_core::codec::CODEC_ID_MSGPACK_NAMED,
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
            driver_name: "maria".to_string(), // renamed by V1->V2 upcast
            distance_m: 12.5 * METERS_PER_MILE, // 20116.8 — unit change
            completed_at_ms: 1_750_000_000_000, // s -> ms
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
    let ev = StoredEvent::encode("trip.completed", 3, &v3).unwrap();
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
    let ev = StoredEvent::encode("trip.completed", 1, &v1).unwrap();
    let out = decode_trip_completed(&ev).unwrap();
    assert_eq!(out.driver_name, "ana");
    assert!((out.distance_m - METERS_PER_MILE).abs() < 1e-9);
    assert_eq!(out.completed_at_ms, 1_700_000_000_000);
}

/// Regression (bn-meo fuzzing, `fuzz_upcast_stored_event` crash
/// `crash-a77ecf4a124ca7b8d5f7c8022e8d4ffdaf075bb7`): a V2 event whose
/// `completed_at` is large enough that the V2->V3 `Upcast`'s `* 1000`
/// unit conversion overflows i64 used to panic ("attempt to multiply with
/// overflow") instead of returning a well-defined answer. The value is
/// decoded straight off an untrusted stored payload, so an old event with
/// a corrupted or adversarial `completed_at` must not crash the process.
#[test]
fn old_version_event_with_overflowing_field_does_not_panic() {
    let v2 = TripCompletedV2 {
        trip_id: 1,
        driver_name: "x".to_string(),
        distance_miles: 1.0,
        completed_at: i64::MAX / 2, // * 1000 overflows i64
    };
    let ev = StoredEvent::encode("trip.completed", 2, &v2).unwrap();
    let out = decode_trip_completed(&ev).unwrap();
    assert_eq!(out.completed_at_ms, i64::MAX); // saturated, not panicked
}

#[test]
fn unknown_version_fails_loudly() {
    let err = decode_trip_completed(&stored(99, V1_FIXTURE)).unwrap_err();
    match err {
        UpcastError::UnknownSchemaVersion {
            event_name,
            version,
            known_versions,
        } => {
            assert_eq!(event_name, "trip.completed");
            assert_eq!(version, 99);
            assert_eq!(known_versions, &[1, 2, 3]);
        }
        other => panic!("expected UnknownSchemaVersion, got {other:?}"),
    }
}

#[test]
fn unknown_codec_fails_loudly() {
    let mut ev = stored(1, V1_FIXTURE);
    ev.codec_id = 200;
    let err = decode_trip_completed(&ev).unwrap_err();
    match err {
        UpcastError::Codec(mess_core::codec::CodecError::UnknownCodecId {
            codec_id,
        }) => {
            assert_eq!(codec_id, 200);
        }
        other => panic!("expected Codec(UnknownCodecId), got {other:?}"),
    }
}

#[test]
fn bootstrap_codec_id_fails_loudly() {
    let mut ev = stored(1, V1_FIXTURE);
    ev.codec_id = mess_core::codec::CODEC_ID_BOOTSTRAP;
    let err = decode_trip_completed(&ev).unwrap_err();
    assert!(matches!(
        err,
        UpcastError::Codec(
            mess_core::codec::CodecError::BootstrapCodecUnsupported
        )
    ));
}

#[test]
fn wrong_event_name_fails_loudly() {
    let mut ev = stored(1, V1_FIXTURE);
    ev.event_name = "trip.cancelled".to_string();
    let err = decode_trip_completed(&ev).unwrap_err();
    assert!(matches!(err, UpcastError::WrongEventName { .. }));
}

#[test]
fn corrupt_payload_fails_loudly() {
    let ev = stored(1, &V1_FIXTURE[..10]);
    let err = decode_trip_completed(&ev).unwrap_err();
    assert!(matches!(
        err,
        UpcastError::Codec(mess_core::codec::CodecError::Decode(_))
    ));
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
    assert_eq!(
        StoredEvent::encode("trip.completed", 1, &v1).unwrap().payload,
        V1_FIXTURE
    );
    let v2 = TripCompletedV2 {
        trip_id: 43,
        driver_name: "yusuf".to_string(),
        distance_miles: 3.2,
        completed_at: 1_750_000_100,
    };
    assert_eq!(
        StoredEvent::encode("trip.completed", 2, &v2).unwrap().payload,
        V2_FIXTURE
    );
}

/// Same event, encoded twice, must produce identical bytes.
#[test]
fn encoder_determinism_guard() {
    let v1 = TripCompletedV1 {
        trip_id: 42,
        driver: "maria".to_string(),
        distance_miles: 12.5,
        completed_at: 1_750_000_000,
    };
    let a = StoredEvent::encode("trip.completed", 1, &v1).unwrap();
    let b = StoredEvent::encode("trip.completed", 1, &v1).unwrap();
    assert_eq!(a.payload, b.payload);
}
