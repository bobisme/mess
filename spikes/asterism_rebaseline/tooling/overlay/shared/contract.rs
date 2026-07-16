//! Fail-closed compile-time identity and canonical contract output.

use crate::schema::{json_bool, json_string, json_u64};

pub const CONTRACT_SCHEMA: &str = "bn-2l3n-binary-contract-v3";

macro_rules! identity {
    ($name:literal) => {
        match option_env!($name) {
            Some(value) => value,
            None => "",
        }
    };
}

pub const PROTOCOL: &str = identity!("ASTERISM_BUILD_PROTOCOL");
pub const PROTOCOL_SHA256: &str = identity!("ASTERISM_BUILD_PROTOCOL_SHA256");
pub const TOOLING_COMMIT: &str = identity!("ASTERISM_BUILD_TOOLING_COMMIT");
pub const TOOLING_TREE: &str = identity!("ASTERISM_BUILD_TOOLING_TREE");
pub const VARIANT: &str = identity!("ASTERISM_BUILD_VARIANT");
pub const PRODUCT_COMMIT: &str = identity!("ASTERISM_BUILD_PRODUCT_COMMIT");
pub const PRODUCT_TREE: &str = identity!("ASTERISM_BUILD_PRODUCT_TREE");
pub const ADAPTER_SHA256: &str = identity!("ASTERISM_BUILD_ADAPTER_SHA256");
pub const SHARED_MANIFEST_SHA256: &str =
    identity!("ASTERISM_BUILD_SHARED_MANIFEST_SHA256");
pub const CARGO_LOCK_SHA256: &str =
    identity!("ASTERISM_BUILD_CARGO_LOCK_SHA256");
pub const SOURCE_APPROVAL_SHA256: &str =
    identity!("ASTERISM_BUILD_SOURCE_APPROVAL_SHA256");
pub const BUILD_NONCE: &str = identity!("ASTERISM_BUILD_NONCE");
pub const BINARY_KIND: &str = identity!("ASTERISM_BUILD_BINARY_KIND");
pub const TIMED_SURFACE: &str = identity!("ASTERISM_BUILD_TIMED_SURFACE");

fn lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn profile_role_lifetime() -> String {
    if VARIANT != "C" {
        return json_string("not_applicable");
    }
    crate::schema::canonical_object(&[
        ("blocking_thread_keep_alive_ns", json_u64(3_600_000_000_000)),
        ("maximum_profile_child_timeout_ns", json_u64(120_000_000_000)),
        ("ready_to_measured_other_thread_birth_sites", json_u64(0)),
        ("ready_to_measured_spawn_blocking_sites", json_u64(1)),
        ("schema", json_string("bn-2l3n-c-role-lifetime-v3")),
    ])
}

pub fn validate(expected_kind: &str, expected_surface: &str) {
    assert_eq!(PROTOCOL, "bn-2l3n-asterism-rebaseline-v3");
    assert!(matches!(VARIANT, "A" | "B" | "C" | "D"));
    assert_eq!(BINARY_KIND, expected_kind);
    assert_eq!(TIMED_SURFACE, expected_surface);
    for (label, value) in [
        ("tooling commit", TOOLING_COMMIT),
        ("tooling tree", TOOLING_TREE),
        ("product commit", PRODUCT_COMMIT),
        ("product tree", PRODUCT_TREE),
    ] {
        assert!(lower_hex(value, 40), "invalid embedded {label}");
    }
    for (label, value) in [
        ("protocol SHA-256", PROTOCOL_SHA256),
        ("adapter SHA-256", ADAPTER_SHA256),
        ("shared manifest SHA-256", SHARED_MANIFEST_SHA256),
        ("Cargo.lock SHA-256", CARGO_LOCK_SHA256),
        ("source approval SHA-256", SOURCE_APPROVAL_SHA256),
        ("build nonce", BUILD_NONCE),
    ] {
        assert!(lower_hex(value, 64), "invalid embedded {label}");
    }
}

/// Emit one canonical JSON object and one newline. Keys are byte-sorted.
pub fn emit(expected_kind: &str, expected_surface: &str) {
    validate(expected_kind, expected_surface);
    let fields = [
        ("adapter_sha256", json_string(ADAPTER_SHA256)),
        ("binary_kind", json_string(BINARY_KIND)),
        ("build_nonce", json_string(BUILD_NONCE)),
        ("cargo_lock_sha256", json_string(CARGO_LOCK_SHA256)),
        ("contract_mode", json_bool(true)),
        ("correctness_oracle_mode", json_bool(BINARY_KIND == "public")),
        ("product_commit", json_string(PRODUCT_COMMIT)),
        ("product_tree", json_string(PRODUCT_TREE)),
        ("profile_role_lifetime", profile_role_lifetime()),
        ("protocol", json_string(PROTOCOL)),
        ("protocol_sha256", json_string(PROTOCOL_SHA256)),
        ("rows_written", json_u64(0)),
        ("schema", json_string(CONTRACT_SCHEMA)),
        ("shared_manifest_sha256", json_string(SHARED_MANIFEST_SHA256)),
        ("source_approval_sha256", json_string(SOURCE_APPROVAL_SHA256)),
        ("timed_surface", json_string(TIMED_SURFACE)),
        ("tooling_commit", json_string(TOOLING_COMMIT)),
        ("tooling_tree", json_string(TOOLING_TREE)),
        ("variant", json_string(VARIANT)),
    ];
    println!("{}", crate::schema::canonical_object(&fields));
}
