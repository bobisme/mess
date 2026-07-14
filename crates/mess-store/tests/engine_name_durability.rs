//! bn-2di — a committed event may never out-live the name of the stream or
//! type it references, and that guarantee costs **nothing**.
//!
//! # The invariant, and where it lives now
//!
//! v3 log frames carry the numeric `stream_id` / `event_type_id` and never the
//! name string, so *something* durable has to hold the `id → name` bijection:
//! recovery hard-fails without it ("no interned name for stream_id …").
//!
//! Before bn-2di that something was **fjall** — `stream_names`/`type_names`,
//! the one meta keyspace that was NOT a derived cache the log could rebuild
//! (I5). Upholding the invariant therefore meant ordering two *separate*
//! storage systems, and that cost a real barrier:
//!
//! * bn-150 added a `SyncAll` per new name (~3.4 ms/new-stream — spike bn-1jg
//!   measured it at 98.8% of new-stream latency);
//! * bn-2cj gated it by durability mode;
//! * bn-34o coalesced the barriered modes' fsyncs into the committer's group
//!   window;
//! * and Spike J *still* found a second serialized `SyncAll` worth ~953 µs per
//!   new stream that the `commit.fsync` counter could not even see.
//!
//! Since bn-2di that something is **the log itself**. A new name is a
//! `$registry` record (spec `04-registry.md`), written by the same committer,
//! into the same segment, in the same commit group, at a *lower offset* than
//! the batch that first references the id it mints (`Appender::submit_ordered`,
//! pushed under the interner lock so no concurrent appender can overtake it).
//! Recovery accepts a **contiguous prefix** of the log, so no crash can keep
//! the reference and lose the registration: the ordering is structural, and
//! structure needs no fsync.
//!
//! The fjall name tables are **gone** — not shadowed, not optional: the
//! keyspaces do not exist, `MetaStore` has no method to write one, and
//! `mess-log`'s committer no longer has a `PreBarrier` hook to hang a name
//! flush on. So the assertions here **invert**: where the old suite demanded a
//! flush per new name, this one demands zero, in every durability mode, and
//! pins the guarantee where it actually lives — in the bytes of the log.

use std::time::{Duration, Instant};

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{Durability, EngineOptions, LogEngine, Version};

fn rec(message_type: &str, data: &[u8]) -> RecordToAppend {
    RecordToAppend {
        message_type: message_type.to_string(),
        data:         data.to_vec(),
    }
}

fn open_with(dir: &std::path::Path, durability: Durability) -> LogEngine {
    LogEngine::open_with(
        dir,
        EngineOptions { durability, ..EngineOptions::default() },
    )
    .expect("open")
}

const MODES: [Durability; 3] = [
    Durability::Process,
    Durability::Os,
    Durability::Group { max_delay: Duration::from_micros(200), max_bytes: 0 },
];

// ---------------------------------------------------------------------------
// The headline: a store keeps NO names in fjall, at all, ever.
// ---------------------------------------------------------------------------

/// The acceptance criterion for retiring the keyspace, in its strongest form:
/// the fjall name tables are never created, so a store must open, resolve every
/// name, and serve every read with **no name data in fjall in existence**.
///
/// This is not "deleted after a migration" — there is nothing to delete. The
/// `MetaStore` has no name keyspace and no method to write one; the only place
/// a name has ever been written is the log.
#[tokio::test]
async fn a_store_resolves_every_name_with_no_fjall_name_tables_in_existence() {
    let dir = mess_testkit::sweeping_temp_dir("name-dur-no-fjall-names");
    let store_path = dir.path().join("store");

    {
        let engine = open_with(&store_path, Durability::Process);
        for i in 0..8u64 {
            engine
                .append_batch(
                    &format!("acct-{i}"),
                    Version::NoStream,
                    &[rec(&format!("evt.type-{i}"), &i.to_le_bytes())],
                )
                .await
                .expect("append");
        }
    }

    // Reopen: every name comes back, from the log and only the log.
    let engine = open_with(&store_path, Durability::Process);
    for i in 0..8u64 {
        let s = engine
            .read_stream(&format!("acct-{i}"), Version::NoStream, 10)
            .await
            .expect("read");
        assert_eq!(s.len(), 1, "stream acct-{i} must be readable");
        assert_eq!(s[0].stream_id, format!("acct-{i}"));
        assert_eq!(
            s[0].message_type,
            format!("evt.type-{i}"),
            "the event TYPE name must fold back out of the log too"
        );
        assert_eq!(s[0].data, i.to_le_bytes());
    }

    // And the registry it folded them from is a real, readable log stream.
    let reg = engine
        .read_stream("$registry", Version::NoStream, 100)
        .await
        .expect("$registry is readable");
    assert_eq!(reg.len(), 16, "8 stream names + 8 type names");
    assert!(reg.iter().all(|r| r.message_type == "RegistryEventV1"));
}

/// A new name adds **no barrier to a second storage system** — and, in the
/// production `Group` mode, no barrier at all.
///
/// There is no meta-flush counter to assert on any more
/// (`meta_persist_call_count` died with the barrier it counted), so this
/// asserts the thing that actually matters and that a counter could never
/// prove: what the *log's own* fsync count does when an append mints two
/// brand-new names versus none.
///
/// The two modes answer differently, and the difference is inherent, not a
/// defect:
///
/// * **`Group`** (the production mode, and the one the perf gate is stated in):
///   the registration and the append it precedes are pushed back-to-back under
///   one gate span, so the committer's gather drains BOTH into the SAME commit
///   group. One `fdatasync` covers both. A new name costs **zero** extra
///   barriers.
/// * **`Os`**: `Durability::Os` is *defined* as one `fdatasync` per batch (spec
///   03 §1.2 — a group IS a single append), so two batches are two barriers, by
///   construction, in any mechanism. That is not a regression: the pre-bn-2di
///   engine paid ~the same two barriers for a new stream under `Os` (one log
///   `fdatasync` + at least one meta `SyncAll`). What changed is that both
///   barriers are now the LOG's own — there is no second storage system in the
///   path, no shared-`MetaStore` serialization point that `N` concurrent new
///   streams funnel through (spike bn-1jg's finding), and no phantom `SyncAll`
///   invisible to `commit.fsync` (Spike J's).
#[tokio::test]
async fn a_new_name_adds_no_barrier_beyond_the_log_s_own() {
    for durability in [
        Durability::Os,
        Durability::Group {
            max_delay: Duration::from_micros(200),
            max_bytes: 0,
        },
    ] {
        let dir = mess_testkit::sweeping_temp_dir("name-dur-no-extra-fsync");
        let engine = open_with(&dir.path().join("store"), durability);

        engine
            .append_batch("prime", Version::NoStream, &[rec("prime.t", b"0")])
            .await
            .expect("prime");

        // A run of appends to an EXISTING stream + type: no registration.
        let before = engine.metrics().commit.fsync.count;
        let mut expected = Version::At(0);
        for i in 0..20u64 {
            let out = engine
                .append_batch(
                    "prime",
                    expected,
                    &[rec("prime.t", &i.to_le_bytes())],
                )
                .await
                .expect("hot append");
            expected = out.version;
        }
        let hot = engine.metrics().commit.fsync.count - before;

        // A run of appends each minting a brand-new stream AND a brand-new
        // type.
        let before = engine.metrics().commit.fsync.count;
        for i in 0..20u64 {
            engine
                .append_batch(
                    &format!("fresh-{i}"),
                    Version::NoStream,
                    &[rec(&format!("fresh.t-{i}"), &i.to_le_bytes())],
                )
                .await
                .expect("new-name append");
        }
        let new_name = engine.metrics().commit.fsync.count - before;

        match durability {
            Durability::Group { .. } => assert!(
                new_name <= hot,
                "Group: 20 new-name appends took {new_name} barriers vs {hot} \
                 for 20 hot appends — the registration must ride the append's \
                 OWN commit group, adding no barrier"
            ),
            Durability::Os => assert!(
                new_name <= 2 * hot,
                "Os: 20 new-name appends took {new_name} barriers vs {hot} \
                 for 20 hot appends. Os is one fdatasync per BATCH, and a \
                 new-name append writes two (registration, then use), so 2x \
                 is the floor — but never more, and never a barrier to a \
                 second store."
            ),
            Durability::Process => unreachable!("not exercised here"),
        }
    }
}

/// Corroborating timing proof (spike bn-1jg's yardstick shape): under
/// `Process` — which issues no log barrier at all — a run of all-new-stream
/// appends must have a per-append MEDIAN far below the `fsync` floor a barrier
/// would impose. This is the guard against a per-new-stream fsync creeping back
/// in by ANY route, including one no counter watches (which is exactly how
/// Spike J's phantom ~953 µs `SyncAll` hid).
#[tokio::test]
async fn process_new_stream_appends_do_not_block_on_any_fsync() {
    let dir = mess_testkit::sweeping_temp_dir("name-dur-process-timing");
    let engine = open_with(&dir.path().join("store"), Durability::Process);

    const N: usize = 200;
    let mut samples: Vec<Duration> = Vec::with_capacity(N);
    for i in 0..N {
        let t = Instant::now();
        engine
            .append_batch(
                &format!("stream-{i}"),
                Version::NoStream,
                &[rec("evt", &(i as u64).to_le_bytes())],
            )
            .await
            .expect("new-stream append");
        samples.push(t.elapsed());
    }

    samples.sort_unstable();
    let p50 = samples[N / 2];
    assert!(
        p50 < Duration::from_millis(1),
        "Process new-stream append p50 = {p50:?} — an fsync (~ms) appears to \
         have crept back into the new-name path"
    );
}

// ---------------------------------------------------------------------------
// Durability: every name survives, in every mode, across reopen.
// ---------------------------------------------------------------------------

/// A burst of 32 concurrent brand-new streams — the shape that used to
/// serialize on the shared `MetaStore`'s fsync — must reopen with every name
/// resolved, in every durability mode. Concurrency is the interesting part: two
/// appenders on different streams race, and each must get its registration into
/// the log ahead of any batch that references the id it minted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_concurrent_burst_of_new_names_all_survive_reopen() {
    for durability in MODES {
        let dir = mess_testkit::sweeping_temp_dir("name-dur-burst");
        let store_path = dir.path().join("store");
        let engine = open_with(&store_path, durability);

        const K: usize = 32;
        let gate = std::sync::Arc::new(tokio::sync::Barrier::new(K));
        let mut handles = Vec::new();
        for i in 0..K {
            let engine = engine.clone();
            let gate = gate.clone();
            handles.push(tokio::spawn(async move {
                gate.wait().await;
                engine
                    .append_batch(
                        &format!("burst-{i}"),
                        Version::NoStream,
                        &[rec(&format!("evt-{i}"), &(i as u64).to_le_bytes())],
                    )
                    .await
                    .expect("burst new-stream append")
            }));
        }
        for h in handles {
            h.await.expect("join");
        }

        drop(engine);
        let engine = open_with(&store_path, durability);
        for i in 0..K {
            assert_eq!(
                engine.head(&format!("burst-{i}")).await.unwrap(),
                Version::At(0),
                "{durability:?}: burst stream {i} must survive reopen with \
                 its name resolved"
            );
            let s = engine
                .read_stream(&format!("burst-{i}"), Version::NoStream, 10)
                .await
                .unwrap();
            assert_eq!(s[0].message_type, format!("evt-{i}"));
        }
    }
}

/// The registration must be IN THE LOG, at a **lower global position** than the
/// event that references it. That ordering — not a barrier — is the entire
/// durability argument, because recovery accepts a contiguous prefix: a crash
/// that keeps the event necessarily kept the registration too.
#[tokio::test]
async fn a_registration_always_precedes_the_event_that_uses_it() {
    for durability in MODES {
        let dir = mess_testkit::sweeping_temp_dir("name-dur-ordering");
        let engine = open_with(&dir.path().join("store"), durability);

        engine
            .append_batch(
                "acct-42",
                Version::NoStream,
                &[rec("account.opened", b"carol")],
            )
            .await
            .expect("append with new stream + new type");

        let reg = engine
            .read_stream("$registry", Version::NoStream, 10)
            .await
            .expect("$registry is readable");
        assert_eq!(
            reg.len(),
            2,
            "one StreamRegistered, one EventTypeRegistered"
        );
        let event =
            engine.read_stream("acct-42", Version::NoStream, 10).await.unwrap();
        assert!(
            reg[1].global_position < event[0].global_position,
            "{durability:?}: both registrations must precede the event that \
             references them ({} vs {})",
            reg[1].global_position,
            event[0].global_position
        );

        // The registry records consume global positions but are NEVER delivered
        // to an application.
        let g = engine.read_global(None, 10).await.unwrap();
        assert_eq!(g.len(), 1, "system records must not leak into read_global");
        assert_eq!(g[0].stream_id, "acct-42");
    }
}

/// Once a name is interned, repeated appends add ZERO `$registry` records — the
/// registry is append-only-and-tiny by construction (REG4: one record per name
/// ever created, not one per event), and the hot append path must not notice it
/// exists.
#[tokio::test]
async fn hot_path_appends_add_no_registry_records() {
    for durability in MODES {
        let dir = mess_testkit::sweeping_temp_dir("name-dur-hot-path");
        let engine = open_with(&dir.path().join("store"), durability);

        engine
            .append_batch("hot", Version::NoStream, &[rec("hot.type", b"0")])
            .await
            .expect("priming append");
        let base = engine
            .read_stream("$registry", Version::NoStream, 100)
            .await
            .expect("read $registry")
            .len();
        assert_eq!(base, 2, "the priming append minted one stream + one type");

        let mut expected = Version::At(0);
        for i in 0..500u64 {
            let out = engine
                .append_batch(
                    "hot",
                    expected,
                    &[rec("hot.type", &i.to_le_bytes())],
                )
                .await
                .expect("hot-path append");
            expected = out.version;
        }

        assert_eq!(
            engine
                .read_stream("$registry", Version::NoStream, 100)
                .await
                .unwrap()
                .len(),
            base,
            "{durability:?}: 500 no-new-name appends must add ZERO $registry \
             records"
        );
        assert_eq!(engine.head("hot").await.unwrap(), Version::At(500));
    }
}

/// `$registry` takes REGISTRY RECORDS and nothing else (review F2).
///
/// The stream is not write-*locked* — spec 04's `Registry<B>` writer drives it
/// through this very seam, and `tests/registry.rs` proves that end to end
/// against the real engine. What it is, is **type-checked**: every record must
/// be a `RegistryEventV1` whose payload decodes as a `codec_id 0` registry
/// record AND folds cleanly into the live `RegistryState` (REG13). Anything
/// else — a domain frame, corrupt bytes, a REG-rule violation — is refused
/// before it can reach the log, because those are the bytes recovery decodes as
/// registry records: one accepted, and the store never opens again.
#[tokio::test]
async fn registry_stream_takes_only_valid_registry_records() {
    use mess_store::registry::{
        REGISTRY_EVENT_TYPE_NAME, RESERVED_STREAM_ID, RegistryRecord,
    };

    let dir = mess_testkit::sweeping_temp_dir("name-dur-registry-write");
    let engine = open_with(&dir.path().join("store"), Durability::Process);

    // 1. A domain frame: refused on its message type.
    let err = engine
        .append_batch("$registry", Version::NoStream, &[rec("evil", b"x")])
        .await
        .expect_err("a domain frame in $registry must be refused");
    assert!(
        format!("{err}").contains("RegistryEventV1"),
        "expected a record-type refusal, got: {err}"
    );

    // 2. The right message type, garbage bytes: refused at the decode.
    let err = engine
        .append_batch(
            "$registry",
            Version::NoStream,
            &[rec(REGISTRY_EVENT_TYPE_NAME, b"\xffnot a registry record")],
        )
        .await
        .expect_err("undecodable registry bytes must be refused");
    assert!(
        format!("{err}").contains("$registry"),
        "expected a decode refusal, got: {err}"
    );

    // 3. A decodable record that violates a REG-rule (REG2: stream_id 0 is
    //    reserved and may never be registered): refused at the fold.
    let reserved = RegistryRecord::StreamRegistered {
        stream_id:   RESERVED_STREAM_ID,
        category_id: 0,
        name:        "nope".to_string(),
    };
    let err = engine
        .append_batch(
            "$registry",
            Version::NoStream,
            &[rec(REGISTRY_EVENT_TYPE_NAME, &reserved.encode())],
        )
        .await
        .expect_err("a REG-rule violation must be refused");
    assert!(
        format!("{err}").contains("$registry"),
        "expected a REG-rule refusal, got: {err}"
    );

    // REG13: not one of them touched the log.
    assert_eq!(engine.head("$registry").await.unwrap(), Version::NoStream);
    drop(engine);
    let engine = open_with(&dir.path().join("store"), Durability::Process);
    assert_eq!(engine.head("$registry").await.unwrap(), Version::NoStream);

    // ...and a VALID record is accepted through the same seam.
    let cat = RegistryRecord::CategoryRegistered {
        category_id: 1,
        name:        "orders".to_string(),
    };
    engine
        .append_batch(
            "$registry",
            Version::NoStream,
            &[rec(REGISTRY_EVENT_TYPE_NAME, &cat.encode())],
        )
        .await
        .expect("a valid registry record is appendable");
    assert_eq!(engine.head("$registry").await.unwrap(), Version::At(0));

    drop(engine);
    let engine = open_with(&dir.path().join("store"), Durability::Process);
    assert_eq!(engine.head("$registry").await.unwrap(), Version::At(0));
    assert_eq!(
        engine.fold_registry().await.unwrap().category_name(1),
        Some("orders")
    );
}

// ---------------------------------------------------------------------------
// How close is fjall to deletable? (the lead's question, answered by test)
// ---------------------------------------------------------------------------

/// **The engine reads NOTHING from fjall on open.**
///
/// Delete the entire `meta/` directory — every keyspace, not just the names —
/// and the store must still open, resolve every name, serve every read, and
/// keep appending. Because it does, fjall is now a pure write-behind derived
/// cache for the engine: `stream_heads` is the only thing the append path still
/// writes there, heads are re-derived from the log scan on every open, and
/// nothing on the open path consults fjall at all (`recover` does not even take
/// a `&MetaStore` any more).
///
/// This is the test that says how close we are to deleting fjall outright: for
/// the ENGINE, the answer is "it already is". What still needs it is the
/// separate, opt-in `FjallSnapshotBackend` (the app's snapshot sidecar, its own
/// database under `.snapshots/`), which is a different component with a
/// different lifecycle.
#[tokio::test]
async fn the_engine_opens_and_serves_with_the_whole_meta_directory_deleted() {
    let dir = mess_testkit::sweeping_temp_dir("name-dur-no-meta-dir");
    let store_path = dir.path().join("store");

    {
        let engine = open_with(&store_path, Durability::Process);
        for i in 0..6u64 {
            engine
                .append_batch(
                    &format!("acct-{i}"),
                    Version::NoStream,
                    &[rec(&format!("evt-{i}"), &i.to_le_bytes())],
                )
                .await
                .expect("append");
        }
        // A second event on one stream, so heads are non-trivial.
        engine
            .append_batch("acct-0", Version::At(0), &[rec("evt-0", b"second")])
            .await
            .expect("append");
    }

    // Nuke the ENTIRE fjall metadata store — every keyspace it has. The flat
    // owner no longer writes the derived stream-head cache, so an append-only
    // fixture may legitimately never create this directory at all.
    let meta_dir = store_path.join("meta");
    if meta_dir.exists() {
        std::fs::remove_dir_all(&meta_dir).expect("delete the meta directory");
    }

    let engine = open_with(&store_path, Durability::Process);
    for i in 0..6u64 {
        let s = engine
            .read_stream(&format!("acct-{i}"), Version::NoStream, 10)
            .await
            .expect("read");
        let want = if i == 0 { 2 } else { 1 };
        assert_eq!(s.len(), want, "acct-{i} must be fully readable");
        assert_eq!(s[0].message_type, format!("evt-{i}"));
    }
    assert_eq!(
        engine.head("acct-0").await.unwrap(),
        Version::At(1),
        "heads are re-derived from the log, not from fjall"
    );

    // And it keeps working: a new append (minting a new name) still commits.
    engine
        .append_batch("acct-new", Version::NoStream, &[rec("evt.new", b"x")])
        .await
        .expect("append after the meta store was deleted");
    assert_eq!(engine.head("acct-new").await.unwrap(), Version::At(0));
}
