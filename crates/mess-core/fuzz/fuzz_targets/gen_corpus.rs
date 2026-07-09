//! One-shot corpus-seed generator, NOT a fuzz target (bn-meo). Run with:
//!
//! ```text
//! cargo run --manifest-path crates/mess-core/fuzz/Cargo.toml --bin gen_corpus
//! ```
//!
//! Writes small seed files under `corpus/fuzz_upcast_stored_event/`, each
//! laid out in `fuzz_upcast_stored_event::parse_input`'s own sub-format
//! (name_len byte, name bytes, schema_version u16 LE, codec_id u16 LE,
//! payload) so every seed is a valid `StoredEvent` from run 1 — real
//! encodes across every declared schema version (mirroring
//! `tests/codec_upcaster.rs`'s golden fixtures) plus a handful of the
//! hostile shapes the acceptance bar calls out explicitly: unknown
//! version/codec, truncated payload, and a deeply-nested payload (the
//! shape that, pre-fix, stack-overflowed `serde`'s unknown-field skip --
//! see `mess_core::codec::msgpack::check_msgpack_depth`).

use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;

const NAME: &str = "trip.completed";
const CODEC_ID_MSGPACK_NAMED: u16 = 1;
const CODEC_ID_BOOTSTRAP: u16 = 0;

#[derive(Serialize)]
struct TripCompletedV1 {
    trip_id: u64,
    driver: String,
    distance_miles: f64,
    completed_at: i64,
}

#[derive(Serialize)]
struct TripCompletedV2 {
    trip_id: u64,
    driver_name: String,
    distance_miles: f64,
    completed_at: i64,
}

#[derive(Serialize)]
struct TripCompletedV3 {
    trip_id: u64,
    driver_name: String,
    distance_m: f64,
    completed_at_ms: i64,
    rating: Option<u8>,
}

fn write_seed(dir: &Path, name: &str, bytes: &[u8]) {
    fs::create_dir_all(dir).expect("create corpus dir");
    fs::write(dir.join(name), bytes).expect("write seed");
}

/// Build one `parse_input`-shaped seed.
fn stored_seed(event_name: &str, schema_version: u16, codec_id: u16, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let name_bytes = event_name.as_bytes();
    let name_len = name_bytes.len().min(24) as u8;
    out.push(name_len);
    out.extend_from_slice(&name_bytes[..name_len as usize]);
    out.extend_from_slice(&schema_version.to_le_bytes());
    out.extend_from_slice(&codec_id.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Deeply nested msgpack array -- see `check_msgpack_depth`'s doc
/// (`crates/mess-core/src/codec/msgpack.rs`) for why this shape matters.
fn deep_nested(depth: usize) -> Vec<u8> {
    let mut v = vec![0x91u8; depth]; // depth x fixarray, len 1
    v.push(0x00); // fixint 0
    v
}

fn main() {
    let base: PathBuf =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("corpus/fuzz_upcast_stored_event");

    // ---- real encodes across every declared version ----
    let v1 = TripCompletedV1 {
        trip_id: 42,
        driver: "maria".to_string(),
        distance_miles: 12.5,
        completed_at: 1_750_000_000,
    };
    write_seed(
        &base,
        "v1_valid",
        &stored_seed(NAME, 1, CODEC_ID_MSGPACK_NAMED, &rmp_serde::to_vec_named(&v1).unwrap()),
    );

    let v2 = TripCompletedV2 {
        trip_id: 43,
        driver_name: "yusuf".to_string(),
        distance_miles: 3.2,
        completed_at: 1_750_000_100,
    };
    write_seed(
        &base,
        "v2_valid",
        &stored_seed(NAME, 2, CODEC_ID_MSGPACK_NAMED, &rmp_serde::to_vec_named(&v2).unwrap()),
    );

    let v3 = TripCompletedV3 {
        trip_id: 44,
        driver_name: "kenji".to_string(),
        distance_m: 800.0,
        completed_at_ms: 1_750_000_200_000,
        rating: Some(5),
    };
    write_seed(
        &base,
        "v3_valid",
        &stored_seed(NAME, 3, CODEC_ID_MSGPACK_NAMED, &rmp_serde::to_vec_named(&v3).unwrap()),
    );

    // ---- hostile: unknown version/codec ----
    let v1_bytes = rmp_serde::to_vec_named(&v1).unwrap();
    write_seed(&base, "unknown_schema_version", &stored_seed(NAME, 99, CODEC_ID_MSGPACK_NAMED, &v1_bytes));
    write_seed(&base, "unknown_codec_id", &stored_seed(NAME, 1, 200, &v1_bytes));
    write_seed(&base, "bootstrap_codec_id", &stored_seed(NAME, 1, CODEC_ID_BOOTSTRAP, &v1_bytes));
    write_seed(&base, "wrong_event_name", &stored_seed("trip.cancelled", 1, CODEC_ID_MSGPACK_NAMED, &v1_bytes));

    // ---- hostile: truncated / corrupt payload ----
    write_seed(&base, "truncated_payload", &stored_seed(NAME, 1, CODEC_ID_MSGPACK_NAMED, &v1_bytes[..10]));
    write_seed(&base, "empty_payload", &stored_seed(NAME, 1, CODEC_ID_MSGPACK_NAMED, &[]));
    write_seed(&base, "garbage_payload", &stored_seed(NAME, 1, CODEC_ID_MSGPACK_NAMED, &[0xff, 0x00, 0xca, 0x99]));

    // ---- hostile: deeply nested (the stack-overflow class) ----
    write_seed(&base, "deep_nested_top_level", &stored_seed(NAME, 1, CODEC_ID_MSGPACK_NAMED, &deep_nested(2000)));
    // Known fields + one unknown/extra field mapped to a deeply nested
    // value -- the specific shape that stack-overflowed pre-fix (serde's
    // default #[serde(deny_unknown_fields)]-less struct decode must skip
    // unknown fields via IgnoredAny, which recurses into the value).
    let mut extra_field_payload = Vec::new();
    extra_field_payload.push(0x85u8); // fixmap, 5 entries: 4 known + 1 unknown
    extra_field_payload.extend_from_slice(&[0xa7]);
    extra_field_payload.extend_from_slice(b"trip_id");
    extra_field_payload.push(0x01);
    extra_field_payload.extend_from_slice(&[0xa6]);
    extra_field_payload.extend_from_slice(b"driver");
    extra_field_payload.extend_from_slice(&[0xa1, b'x']);
    extra_field_payload.extend_from_slice(&[0xae]);
    extra_field_payload.extend_from_slice(b"distance_miles");
    extra_field_payload.push(0x01);
    extra_field_payload.extend_from_slice(&[0xac]);
    extra_field_payload.extend_from_slice(b"completed_at");
    extra_field_payload.push(0x01);
    extra_field_payload.extend_from_slice(&[0xa5]);
    extra_field_payload.extend_from_slice(b"extra");
    extra_field_payload.extend_from_slice(&deep_nested(2000));
    write_seed(
        &base,
        "deep_nested_unknown_field",
        &stored_seed(NAME, 1, CODEC_ID_MSGPACK_NAMED, &extra_field_payload),
    );

    // ---- degenerate ----
    write_seed(&base, "empty_input", &[]);
    write_seed(&base, "single_byte", &[0x00]);

    eprintln!("wrote corpus seeds to {}", base.display());
}
