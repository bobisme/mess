//! bn-meo: coverage-guided fuzzing of the upcaster/msgpack decode path —
//! `event_versions!`-generated `decode_*` dispatchers over hostile
//! `StoredEvent`s (unknown `event_name`, unknown `schema_version`, unknown
//! `codec_id`, and truncated/hostile/deeply-nested msgpack `payload`
//! bytes).
//!
//! There's no real multi-version event type in `mess-core`'s library
//! surface to fuzz directly (`event_versions!` is a macro callers use to
//! declare their own event chains — see `tests/codec_upcaster.rs`), so this
//! target declares one itself: the exact `TripCompletedV1/V2/V3` chain from
//! `mess-core/tests/codec_upcaster.rs` (same field names, same upcast
//! logic, same golden fixture shapes) so the shredder/upcaster's real
//! dispatch, decode, and multi-hop `Upcast` composition all run, not a
//! stub.
//!
//! `data` is parsed into a `StoredEvent` by [`parse_input`] — see its doc
//! for the format — and handed to `decode_trip_completed`. The only
//! invariant checked is the one the whole codec/upcaster layer exists to
//! guarantee: never panic, never crash (a stack overflow from unbounded
//! msgpack nesting is this target's reason for existing — see
//! `mess_core::codec::msgpack::check_msgpack_depth`, added after this
//! target's first local run found exactly that), always return either
//! `Ok` or a typed [`mess_core::codec::UpcastError`].
#![no_main]

use libfuzzer_sys::fuzz_target;
use mess_core::codec::{StoredEvent, Upcast, UpcastError};
use mess_core::event_versions;
use serde::{Deserialize, Serialize};

// ------------------------------------------------------- the example event
//
// Verbatim port of `mess-core/tests/codec_upcaster.rs`'s 3-version chain:
// same field names, same upcast logic, so this target exercises the real
// multi-hop `Upcast` composition (not just `StoredEvent::decode`'s single
// codec layer) and the golden fixture bytes committed there remain valid
// seeds here too (see `gen_corpus.rs`).

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct TripCompletedV1 {
    trip_id: u64,
    driver: String,
    distance_miles: f64,
    completed_at: i64,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct TripCompletedV2 {
    trip_id: u64,
    driver_name: String,
    distance_miles: f64,
    completed_at: i64,
}

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
            driver_name: v1.driver,
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
            distance_m: v2.distance_miles * METERS_PER_MILE,
            // `saturating_mul`, not `*`: `completed_at` is decoded straight
            // off an untrusted/fuzzed payload, and this target found (and
            // `tests/codec_upcaster.rs` now regression-tests) that a plain
            // `*` panics on overflow for a large-enough old-version value.
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

/// Carve a [`StoredEvent`] out of `data`. Every length is clamped to what's
/// actually left, so this never panics on short input — worst case it
/// returns `None` and the fuzz iteration is a no-op.
///
/// Format (unrelated to any on-disk format — purely how this target slices
/// up its own input bytes):
/// ```text
/// byte 0:        name_len (clamped to 24 and to the remaining input)
/// name_len bytes: event_name (lossily decoded as UTF-8, so any bytes work
///                 -- deliberately includes the all-hostile-bytes case)
/// u16 LE:        schema_version
/// u16 LE:        codec_id
/// remainder:     payload (raw, hostile/truncated/deeply-nested msgpack)
/// ```
/// Feeding `schema_version`/`codec_id` straight from fuzzer bytes covers
/// both "old-version shapes" (when it lands on 1 or 2, exercising the real
/// `Upcast` chain against a hostile payload) and "unknown version/codec"
/// (any other value, exercising `UpcastError::UnknownSchemaVersion` /
/// `CodecError::UnknownCodecId`/`BootstrapCodecUnsupported`) from the same
/// input space, letting libFuzzer's coverage-guided mutation find the
/// boundary between them rather than needing a mode byte to pick one.
fn parse_input(data: &[u8]) -> Option<StoredEvent> {
    if data.is_empty() {
        return None;
    }
    let name_len = (data[0] as usize).min(24).min(data.len().saturating_sub(1));
    let mut p = 1usize;
    let event_name = String::from_utf8_lossy(data.get(p..p + name_len)?).into_owned();
    p += name_len;
    let schema_version = u16::from_le_bytes([*data.get(p)?, *data.get(p + 1)?]);
    p += 2;
    let codec_id = u16::from_le_bytes([*data.get(p)?, *data.get(p + 1)?]);
    p += 2;
    let payload = data[p..].to_vec();
    Some(StoredEvent { event_name, schema_version, codec_id, payload })
}

fuzz_target!(|data: &[u8]| {
    let Some(ev) = parse_input(data) else { return };
    // Never panic; always Ok or a typed UpcastError.
    match decode_trip_completed(&ev) {
        Ok(_) | Err(UpcastError::WrongEventName { .. }) => {}
        Err(UpcastError::UnknownSchemaVersion { known_versions, .. }) => {
            assert_eq!(known_versions, &[1, 2, 3]);
        }
        Err(UpcastError::Codec(_)) => {}
    }
});
