//! bn-150 — name persistence must be co-durable with the covering append.
//!
//! # Background
//!
//! `EngineError::Meta` on reopen ("no interned name for stream_id …") was
//! reachable before this fix: a newly-interned stream/type name's
//! `stream_names`/`type_names` meta-table row was written through fjall's
//! default `PersistMode::Buffer` — a real `write(2)` to the OS page cache,
//! but never `fsync`ed — with no ordering barrier against the covering
//! append's own durable write. Under a `Durability::Os`/`Group` engine (a
//! real `fdatasync`/`fsync` barrier on every ack), a genuine power-loss
//! event between the two could keep the append durable while losing its
//! name, because — unlike every other meta table (`stream_heads`,
//! `snapshot_heads`, dedupe) — `stream_names`/`type_names` are NOT a
//! derived cache the log can rebuild (I5): this engine's log frames carry
//! only the numeric `stream_id`/`event_type_id`, never the name string (see
//! `engine.rs`'s module doc; the spec's `$registry`, `04-registry.md`,
//! would carry names in the log, but that is not implemented here).
//!
//! # Why these tests assert call counts, not "kill -9 and reopen"
//!
//! A real power-loss event cannot be simulated portably from inside
//! `cargo test`: fjall's default insert/commit path already performs a real
//! `write(2)` to the OS page cache on every call (see
//! `fjall::keyspace::Keyspace::insert` / `Database::batch`, both default to
//! `PersistMode::Buffer`, which the crate's own docs describe as "flushes
//! data to OS buffers"). That means an abrupt process exit (`kill -9`, or
//! `std::process::exit` skipping every destructor) does **not** reproduce
//! the bug — the write already reached the kernel and survives any
//! process-level death; only an actual OS crash / hardware power cut,
//! which loses the page cache itself, would. (`mess-log`'s own
//! `tests/sigkill_harness.rs` documents the identical limitation for its
//! `Os`/`Group` durability modes — a real SIGKILL rarely loses a page-cache
//! write either.)
//!
//! So instead of a doomed crash simulation, these tests pin the actual
//! mechanism the fix relies on: [`LogEngine::persist_new_names`] calls
//! [`mess_index::meta::MetaStore::persist`] (a real `fsync`/`SyncAll`, the
//! same class of durable barrier the covering log append uses) exactly when
//! — and only when — a name was actually newly interned, and it does so
//! before the covering append is submitted. The call-count instrumentation
//! (`meta_persist_call_count`, bn-150, `#[doc(hidden)]` test/diagnostic
//! surface matching the existing `total_events`/`sealed_segment_count`
//! pattern in `engine.rs`) makes that assertable directly, deterministically,
//! and without flakiness — proof by construction that the durable flush
//! cannot be skipped, rather than an attempt to empirically observe a class
//! of data loss this environment cannot produce.

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{LogEngine, Version};

fn rec(message_type: &str, data: &[u8]) -> RecordToAppend {
    RecordToAppend {
        message_type: message_type.to_string(),
        data:         data.to_vec(),
    }
}

/// The bone's core regression: an append that interns a brand-new stream
/// name must durably flush the meta store (a real `fsync`) before it
/// returns — not zero times, not deferred to some later, unbarriered point.
#[tokio::test]
async fn new_stream_name_forces_a_durable_meta_flush() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = LogEngine::open(dir.path().join("store")).expect("open");

    assert_eq!(engine.meta_persist_call_count(), 0, "nothing flushed yet");

    engine
        .append_batch(
            "new-stream",
            Version::NoStream,
            &[rec("evt.a", b"payload")],
        )
        .await
        .expect("append with a new stream name");

    assert_eq!(
        engine.meta_persist_call_count(),
        1,
        "a newly-interned stream name must force exactly one durable meta \
         flush"
    );
}

/// A new event-TYPE name (on an already-known stream) must equally force a
/// durable flush — the type-name call site is a second, independent path
/// through `persist_new_names` in `append_batch`, folded with the
/// stream-name call site into one flush per append (bn-150).
#[tokio::test]
async fn new_type_name_forces_a_durable_meta_flush() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = LogEngine::open(dir.path().join("store")).expect("open");

    engine
        .append_batch("s", Version::NoStream, &[rec("evt.a", b"1")])
        .await
        .expect("append b0 (new stream AND new type, folded into ONE flush)");
    assert_eq!(
        engine.meta_persist_call_count(),
        1,
        "a new stream + a new type in the SAME append must fold into one \
         flush, not two"
    );

    engine
        .append_batch("s", Version::At(0), &[rec("evt.b", b"2")])
        .await
        .expect("append b1 (same stream, NEW type: +1)");
    assert_eq!(
        engine.meta_persist_call_count(),
        2,
        "a newly-interned event type must also force a durable meta flush"
    );
}

/// The hot path: once a stream and its event type(s) are already interned,
/// repeated appends must add ZERO durable meta flushes — the append-latency
/// cost this bone requires stay at zero on the common path. Exercised with a
/// meaningful volume (not just one append) so an accidental "flush every
/// Nth call" regression would also be caught.
#[tokio::test]
async fn hot_path_appends_add_zero_meta_flushes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = LogEngine::open(dir.path().join("store")).expect("open");

    // Prime the interner: one stream, one type. This is the only flush this
    // test expects, ever.
    engine
        .append_batch("hot", Version::NoStream, &[rec("hot.type", b"0")])
        .await
        .expect("priming append");
    assert_eq!(
        engine.meta_persist_call_count(),
        1,
        "priming append: exactly one flush"
    );

    let mut expected = Version::At(0);
    for i in 0..500u64 {
        let out = engine
            .append_batch("hot", expected, &[rec("hot.type", &i.to_le_bytes())])
            .await
            .expect("hot-path append");
        expected = out.version;
    }

    assert_eq!(
        engine.meta_persist_call_count(),
        1,
        "500 no-new-name appends must add ZERO durable meta flushes beyond \
         the priming one"
    );
    assert_eq!(engine.head("hot").await.unwrap(), Version::At(500));
}

/// Concurrent appends across MANY distinct brand-new streams (exercising the
/// per-stream append gate / publish sequencer alongside the fix) must still
/// flush exactly once per newly-interned stream name, no more, no less, and
/// every one of them must reopen correctly-named afterwards — the
/// concurrency counterpart to the sequential tests above.
#[tokio::test]
async fn concurrent_new_stream_names_each_flush_exactly_once_and_survive_reopen()
 {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_path = dir.path().join("store");
    let engine = LogEngine::open(&store_path).expect("open");

    const N: usize = 20;
    let mut handles = Vec::new();
    for i in 0..N {
        let engine = engine.clone();
        handles.push(tokio::spawn(async move {
            engine
                .append_batch(
                    &format!("concurrent-{i}"),
                    Version::NoStream,
                    &[rec("evt", &(i as u64).to_le_bytes())],
                )
                .await
                .expect("concurrent new-stream append")
        }));
    }
    for h in handles {
        h.await.expect("join");
    }

    assert_eq!(
        engine.meta_persist_call_count(),
        N as u64,
        "exactly one durable flush per newly-interned stream name, even under \
         concurrency"
    );

    drop(engine);
    let engine = LogEngine::open(&store_path).expect("reopen");
    for i in 0..N {
        assert_eq!(
            engine.head(&format!("concurrent-{i}")).await.unwrap(),
            Version::At(0),
            "stream {i}'s durable event must survive reopen"
        );
    }
}

/// A genuine fresh reopen (the facade-level effect of the fix, not just the
/// call-count mechanism) after a new-name append must never surface
/// `EngineError::Meta` and must resolve the exact correct name through every
/// read path — pins the end-to-end behaviour the call-count tests above
/// exist to explain the mechanism of.
#[tokio::test]
async fn reopen_after_new_name_append_resolves_correct_name_everywhere() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store_path = dir.path().join("store");

    {
        let engine = LogEngine::open(&store_path).expect("open fresh");
        engine
            .append_batch(
                "acct-42",
                Version::NoStream,
                &[rec("account.opened", b"carol")],
            )
            .await
            .expect("append with new stream + new type");
    }

    let engine = LogEngine::open(&store_path)
        .expect("reopen must not error EngineError::Meta");
    assert_eq!(engine.head("acct-42").await.unwrap(), Version::At(0));
    let s = engine.read_stream("acct-42", Version::NoStream, 10).await.unwrap();
    assert_eq!(s.len(), 1);
    assert_eq!(s[0].stream_id, "acct-42");
    assert_eq!(s[0].message_type, "account.opened");
    assert_eq!(s[0].data, b"carol");
    let g = engine.read_global(None, 10).await.unwrap();
    assert_eq!(g.len(), 1);
    assert_eq!(g[0].stream_id, "acct-42");
}
