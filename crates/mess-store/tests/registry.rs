//! Acceptance tests for `bn-2py` / `docs/spec/04-registry.md`, driven
//! against the in-memory [`MockBackend`] through the [`Backend`] seam.
//!
//! - **Restore-from-bytes-alone (I5 proof).** A fresh [`Registry::bootstrap`]
//!   over a backend that already holds `$registry` records — built either by
//!   a live writer or by hand-appending raw `codec_id 0` bytes — resolves
//!   every name identically to the registry that wrote them, with zero
//!   carried-over state.
//! - **Bootstrap acyclicity (§7).** Decoding `$registry`'s own records never
//!   consults a `RegistryState` — proven by bootstrapping directly from
//!   hand-encoded bytes with no writer-side `Registry` ever having existed.
//! - **Rename is alias, not mutation (§5).** The old name keeps resolving
//!   after a `NameAliased`.
//! - **REG-rule enforcement.** Registered-before-referenced (REG12),
//!   no-reserved-reuse (REG2), double-registration (REG14), name rebinding
//!   (REG16), reserved-alias-target (REG17), and the D-REG-E
//!   zero-extension rule are all typed-error rejections.

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::registry::{
    REGISTRY_EVENT_TYPE_NAME, REGISTRY_STREAM, RESERVED_CATEGORY_ID,
    RESERVED_CATEGORY_NAME, RESERVED_EVENT_TYPE_ID, RESERVED_EVENT_TYPE_NAME,
    RESERVED_STREAM_ID, RESERVED_STREAM_NAME, Registry, RegistryError,
    RegistryRecord, TARGET_KIND_CATEGORY, TARGET_KIND_EVENT_TYPE,
    TARGET_KIND_STREAM,
};
use mess_store::{MockBackend, Version};

// ---------------------------------------------------------------------------
// Reserved IDs (REG1) resolve with no log records at all.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bootstrap_from_empty_log_resolves_reserved_names() {
    let backend = MockBackend::new();
    let registry = Registry::bootstrap(backend)
        .await
        .expect("empty $registry bootstraps trivially");

    assert_eq!(
        registry.state().stream_name(RESERVED_STREAM_ID),
        Some(RESERVED_STREAM_NAME)
    );
    assert_eq!(
        registry.state().stream_id(RESERVED_STREAM_NAME),
        Some(RESERVED_STREAM_ID)
    );
    assert_eq!(
        registry.state().category_name(RESERVED_CATEGORY_ID),
        Some(RESERVED_CATEGORY_NAME)
    );
    assert_eq!(
        registry.state().event_type_name(RESERVED_EVENT_TYPE_ID),
        Some(RESERVED_EVENT_TYPE_NAME)
    );
    // Nothing else has ever been registered.
    assert_eq!(registry.state().stream_high_water_mark(), 0);
    assert_eq!(registry.state().category_high_water_mark(), 0);
    assert_eq!(registry.state().event_type_high_water_mark(), 0);
    assert_eq!(registry.state().dict_high_water_mark(), 0);
    assert_eq!(registry.state().stream_id("nope"), None);
}

// ---------------------------------------------------------------------------
// Restore-from-bytes-alone: a fresh Registry over a backend that already
// holds committed $registry records resolves everything a live writer did,
// with no shared state between the two Registry instances.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn restore_from_bytes_alone_resolves_everything() {
    let backend = MockBackend::new();
    let mut writer =
        Registry::bootstrap(backend.clone()).await.expect("bootstrap empty");

    let orders_cat =
        writer.register_category("orders").await.expect("register category");
    let orders_stream = writer
        .register_stream("orders-42", orders_cat)
        .await
        .expect("register stream");
    let order_placed = writer
        .register_event_type("orders.OrderPlaced", 1, [0xAB; 32])
        .await
        .expect("register event type");
    let dict_id = writer
        .register_dict(TARGET_KIND_CATEGORY, orders_cat, 1, vec![9, 9, 9])
        .await
        .expect("register dict");
    writer
        .alias_stream(orders_stream, "orders-42-renamed")
        .await
        .expect("alias stream");

    // A brand-new Registry, over the *same* backend, that never shared
    // process state with `writer` — this is the "copied log dir, open it
    // cold" scenario at the mess-store level: nothing but replay produced
    // this state.
    let reader = Registry::bootstrap(backend.clone())
        .await
        .expect("bootstrap from populated backend");

    assert_eq!(reader.state().category_name(orders_cat), Some("orders"));
    // Both the original and the aliased name resolve (REG16).
    assert_eq!(reader.state().stream_id("orders-42"), Some(orders_stream));
    assert_eq!(
        reader.state().stream_id("orders-42-renamed"),
        Some(orders_stream)
    );
    // Current preferred name follows the most recent record (REG15).
    assert_eq!(
        reader.state().stream_name(orders_stream),
        Some("orders-42-renamed")
    );
    assert_eq!(reader.state().stream_category(orders_stream), Some(orders_cat));
    assert_eq!(
        reader.state().event_type_name(order_placed),
        Some("orders.OrderPlaced")
    );
    assert_eq!(
        reader.state().event_type_meta(order_placed).map(|m| m.codec_id),
        Some(1)
    );
    let dict = reader.state().dict(dict_id).expect("dict resolves");
    assert_eq!(dict.dict_bytes, vec![9, 9, 9]);
    assert_eq!(dict.scope_id, orders_cat);

    // High-water marks match too — the next writer to bootstrap here would
    // allocate exactly where `writer` left off.
    assert_eq!(
        reader.state().stream_high_water_mark(),
        writer.state().stream_high_water_mark()
    );
    assert_eq!(
        reader.state().category_high_water_mark(),
        writer.state().category_high_water_mark()
    );
    assert_eq!(
        reader.state().event_type_high_water_mark(),
        writer.state().event_type_high_water_mark()
    );
    assert_eq!(
        reader.state().dict_high_water_mark(),
        writer.state().dict_high_water_mark()
    );
}

// ---------------------------------------------------------------------------
// Bootstrap acyclicity (§7): decode the very first $registry record ever
// written by hand-appending raw codec_id-0 bytes directly through the
// Backend seam — no `Registry`/`RegistryState` involved in producing them —
// then bootstrap over that backend. Step 2 (materializing $registry) proves
// it depends on nothing but step 1's bytes and the compiled-in §3 tables.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bootstrap_from_hand_encoded_bytes_needs_no_prior_registry() {
    let backend = MockBackend::new();

    // Hand-build the exact §3.9 worked example bytes and append them
    // directly — simulating a log that was written by some other process
    // entirely, or hand-crafted for a corruption/compat test.
    let category_record = RegistryRecord::CategoryRegistered {
        category_id: 1,
        name: "orders".to_string(),
    };
    let payload = category_record.encode();
    assert_eq!(
        payload,
        vec![
            0x03, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00,
            0x6F, 0x72, 0x64, 0x65, 0x72, 0x73,
        ]
    );

    backend
        .append_batch(
            REGISTRY_STREAM,
            Version::NoStream,
            &[RecordToAppend {
                message_type: REGISTRY_EVENT_TYPE_NAME.to_string(),
                data: payload,
            }],
        )
        .await
        .expect("hand-append raw registry bytes");

    // Bootstrap sees only the bytes on disk (§7.1 step 1's output) and the
    // frozen §3 tables (step 2) — no Registry ever constructed these bytes
    // in-process.
    let registry = Registry::bootstrap(backend)
        .await
        .expect("bootstrap from hand-encoded bytes");
    assert_eq!(registry.state().category_name(1), Some("orders"));
    assert_eq!(registry.state().category_id("orders"), Some(1));
    assert_eq!(registry.state().category_high_water_mark(), 1);
}

// ---------------------------------------------------------------------------
// Rename is alias, never mutation (§5): the old name keeps resolving.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rename_produces_alias_event_old_name_still_resolves() {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let cat = registry.register_category("payments").await.unwrap();
    let stream = registry.register_stream("payments-1", cat).await.unwrap();

    registry.alias_stream(stream, "payments-1-v2").await.unwrap();

    // Old name still resolves to the same id (REG16).
    assert_eq!(registry.state().stream_id("payments-1"), Some(stream));
    // New name is now the current preferred name (REG15).
    assert_eq!(registry.state().stream_name(stream), Some("payments-1-v2"));
    assert_eq!(registry.state().stream_id("payments-1-v2"), Some(stream));

    // A second alias: both prior names keep resolving.
    registry.alias_stream(stream, "payments-1-v3").await.unwrap();
    assert_eq!(registry.state().stream_id("payments-1"), Some(stream));
    assert_eq!(registry.state().stream_id("payments-1-v2"), Some(stream));
    assert_eq!(registry.state().stream_name(stream), Some("payments-1-v3"));
}

#[tokio::test]
async fn category_and_event_type_alias_also_preserve_old_names() {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let cat = registry.register_category("billing").await.unwrap();
    registry.alias_category(cat, "billing-v2").await.unwrap();
    assert_eq!(registry.state().category_id("billing"), Some(cat));
    assert_eq!(registry.state().category_name(cat), Some("billing-v2"));

    let et = registry
        .register_event_type("billing.Charged", 1, [0u8; 32])
        .await
        .unwrap();
    registry.alias_event_type(et, "billing.ChargedV2").await.unwrap();
    assert_eq!(registry.state().event_type_id("billing.Charged"), Some(et));
    assert_eq!(registry.state().event_type_name(et), Some("billing.ChargedV2"));
}

// ---------------------------------------------------------------------------
// REG12: registered-before-referenced.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stream_referencing_unregistered_category_is_rejected() {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let err = registry
        .register_stream("orphan", 999)
        .await
        .expect_err("category 999 was never registered");
    assert_eq!(
        err,
        RegistryError::UnregisteredReference { namespace: "category", id: 999 }
    );
}

#[tokio::test]
async fn stream_may_reference_reserved_system_category() {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let stream = registry
        .register_stream("sys-stream", RESERVED_CATEGORY_ID)
        .await
        .expect("category 0 ($system) is always visible");
    assert_eq!(registry.state().stream_category(stream), Some(0));
}

#[tokio::test]
async fn dict_referencing_unregistered_scope_is_rejected() {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let err = registry
        .register_dict(TARGET_KIND_CATEGORY, 42, 1, vec![1, 2, 3])
        .await
        .expect_err("category 42 was never registered");
    assert_eq!(
        err,
        RegistryError::UnregisteredReference { namespace: "category", id: 42 }
    );
}

#[tokio::test]
async fn alias_of_unregistered_stream_is_rejected() {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let err = registry
        .alias_stream(12345, "ghost")
        .await
        .expect_err("stream 12345 was never registered");
    assert_eq!(
        err,
        RegistryError::UnregisteredReference { namespace: "stream", id: 12345 }
    );
}

// ---------------------------------------------------------------------------
// REG17: the four reserved IDs may never be targeted by NameAliased.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn aliasing_reserved_stream_id_is_rejected() {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let err = registry
        .alias_stream(RESERVED_STREAM_ID, "nope")
        .await
        .expect_err("stream 0 is reserved");
    assert_eq!(err, RegistryError::ReservedIdTargeted { namespace: "stream" });
}

#[tokio::test]
async fn aliasing_reserved_category_id_is_rejected() {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let err = registry
        .alias_category(RESERVED_CATEGORY_ID, "nope")
        .await
        .expect_err("category 0 is reserved");
    assert_eq!(
        err,
        RegistryError::ReservedIdTargeted { namespace: "category" }
    );
}

#[tokio::test]
async fn aliasing_reserved_event_type_id_is_rejected() {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let err = registry
        .alias_event_type(RESERVED_EVENT_TYPE_ID, "nope")
        .await
        .expect_err("event_type 0 is reserved");
    assert_eq!(
        err,
        RegistryError::ReservedIdTargeted { namespace: "event_type" }
    );
}

// ---------------------------------------------------------------------------
// D-REG-E: event_type alias target_id with nonzero high 32 bits is rejected.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn event_type_alias_with_nonzero_high_bits_is_rejected() {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let et = registry
        .register_event_type("t", 1, [0u8; 32])
        .await
        .expect("register event type");

    // Craft a NameAliased record directly with a corrupted target_id whose
    // high 32 bits are nonzero, bypassing the (correct) typed API to prove
    // the *decoder/state* rejects it, not just the convenience wrapper.
    let corrupted_target = (u64::from(et)) | (1u64 << 32);
    let record = RegistryRecord::NameAliased {
        target_kind: TARGET_KIND_EVENT_TYPE,
        target_id: corrupted_target,
        new_name: "renamed".to_string(),
    };
    let mut state = registry.state().clone();
    let err = state
        .apply::<std::convert::Infallible>(record)
        .expect_err("nonzero high bits must be rejected");
    assert_eq!(
        err,
        RegistryError::NonZeroHighBits { target_id: corrupted_target }
    );
}

// ---------------------------------------------------------------------------
// REG2/REG14: no reserved-id reuse, no double registration.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn registering_reserved_stream_id_is_rejected_at_decode_state_level() {
    let mut state = mess_store::registry::RegistryState::new();
    let err = state
        .apply::<std::convert::Infallible>(RegistryRecord::StreamRegistered {
            stream_id: RESERVED_STREAM_ID,
            category_id: 0,
            name: "nope".to_string(),
        })
        .expect_err("stream_id 0 must never be registered (REG2)");
    assert_eq!(err, RegistryError::ReservedIdRegistered { record_kind: 0x01 });
}

#[tokio::test]
async fn double_registration_of_same_category_id_is_corruption() {
    let mut state = mess_store::registry::RegistryState::new();
    state
        .apply::<std::convert::Infallible>(RegistryRecord::CategoryRegistered {
            category_id: 1,
            name: "orders".to_string(),
        })
        .unwrap();
    let err = state
        .apply::<std::convert::Infallible>(RegistryRecord::CategoryRegistered {
            category_id: 1,
            name: "orders-again".to_string(),
        })
        .expect_err("category 1 was already registered (REG14)");
    assert_eq!(
        err,
        RegistryError::AlreadyRegistered { namespace: "category", id: 1 }
    );
}

// ---------------------------------------------------------------------------
// REG16: a name, once bound in a namespace, never rebinds to a different id.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rebinding_a_name_to_a_different_id_is_rejected() {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let a = registry.register_category("alpha").await.unwrap();
    let _b = registry.register_category("beta").await.unwrap();

    // Try to alias `beta`'s category to the name "alpha" — already bound to
    // a different id.
    let beta = registry.state().category_id("beta").unwrap();
    let err = registry
        .alias_category(beta, "alpha")
        .await
        .expect_err("\"alpha\" is already bound to a different category id");
    assert_eq!(
        err,
        RegistryError::NameAlreadyBound {
            namespace: "category",
            name: "alpha".to_string()
        }
    );
    // `a` is untouched.
    assert_eq!(registry.state().category_id("alpha"), Some(a));
}

// ---------------------------------------------------------------------------
// REG9/REG20: codec_id 0 may never be declared for an event type or dict.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn event_type_with_reserved_codec_id_is_rejected() {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let err = registry
        .register_event_type("bad", 0, [0u8; 32])
        .await
        .expect_err("codec_id 0 is reserved to $registry itself (REG9)");
    assert_eq!(err, RegistryError::ReservedCodecId { record_kind: 0x02 });
}

#[tokio::test]
async fn dict_with_reserved_codec_id_is_rejected() {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let cat = registry.register_category("c").await.unwrap();
    let err = registry
        .register_dict(TARGET_KIND_CATEGORY, cat, 0, vec![1])
        .await
        .expect_err("codec_id 0 is reserved (REG20)");
    assert_eq!(err, RegistryError::ReservedCodecId { record_kind: 0x05 });
}

// ---------------------------------------------------------------------------
// Writer-assigned allocation (§4.1): sequential, per-namespace, starting
// at 1, derived purely from replay (I5) — no separate persistence.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn allocation_is_sequential_per_namespace_from_one() {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let c1 = registry.register_category("a").await.unwrap();
    let c2 = registry.register_category("b").await.unwrap();
    let c3 = registry.register_category("c").await.unwrap();
    assert_eq!((c1, c2, c3), (1, 2, 3));

    let s1 = registry.register_stream("s1", c1).await.unwrap();
    let s2 = registry.register_stream("s2", c1).await.unwrap();
    assert_eq!((s1, s2), (1, 2));

    // Independent namespaces: streams and categories don't share counters
    // (3 categories registered, but streams still started fresh at 1).
    assert_eq!(registry.state().stream_high_water_mark(), 2);
    assert_eq!(registry.state().category_high_water_mark(), 3);
}

// ---------------------------------------------------------------------------
// Dict lifecycle (§6): registered-before-referenced, and — v1 has no
// DictRetired event at all (REG19) — there is no removal API to test against;
// a dict, once registered, resolves forever.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn registered_dict_resolves_forever_no_deletion_path_exists() {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let et =
        registry.register_event_type("t.Event", 1, [1u8; 32]).await.unwrap();
    let dict_id = registry
        .register_dict(TARGET_KIND_EVENT_TYPE, u64::from(et), 1, vec![7; 16])
        .await
        .unwrap();
    assert!(registry.state().dict(dict_id).is_some());
    // (There is deliberately no `retire_dict`/`delete_dict` method on
    // `Registry` — REG19 says that is not a thing that can happen in v1.)
}

#[tokio::test]
async fn dict_scope_stream_is_rejected() {
    // §3.8: scope_kind 1 (stream) is deliberately not a legal dictionary
    // scope; only category (2) and event_type (3).
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");
    let cat = registry.register_category("c").await.unwrap();
    let stream = registry.register_stream("s", cat).await.unwrap();
    let err = registry
        .register_dict(TARGET_KIND_STREAM, stream, 1, vec![1])
        .await
        .expect_err("stream is not a legal dictionary scope");
    assert_eq!(err, RegistryError::InvalidScopeKind(TARGET_KIND_STREAM));
}

// ---------------------------------------------------------------------------
// REG13: a rejected record must never reach the backend, and a rejected
// call must never desync `Registry`'s in-memory head from the backend's.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rejected_registration_never_touches_the_backend_and_never_desyncs_head()
 {
    let mut registry =
        Registry::bootstrap(MockBackend::new()).await.expect("bootstrap");

    let head_before = registry
        .backend()
        .head(REGISTRY_STREAM)
        .await
        .expect("head read");

    // A REG12-violating call: category 999 was never registered.
    let err = registry
        .register_stream("orphan", 999)
        .await
        .expect_err("category 999 was never registered");
    assert_eq!(
        err,
        RegistryError::UnregisteredReference { namespace: "category", id: 999 }
    );

    // REG13: the invalid record must not have been durably appended —
    // $registry is append-only and never compacted (REG4/I1), so any write
    // here would be permanent even though the caller got an `Err`.
    let head_after_reject = registry
        .backend()
        .head(REGISTRY_STREAM)
        .await
        .expect("head read");
    assert_eq!(
        head_before, head_after_reject,
        "a rejected record must never be appended to $registry"
    );

    // A subsequent legitimate call on the same `Registry` must succeed —
    // it must not panic via the D9-conflict branch, which would fire if
    // the earlier failure had advanced the backend without advancing
    // `Registry`'s in-memory head to match.
    let category_id = registry
        .register_category("valid")
        .await
        .expect("ordinary write after an unrelated validation failure must \
                 succeed, not panic");
    assert_eq!(registry.state().category_name(category_id), Some("valid"));
}
