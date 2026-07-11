//! bn-150 / bn-2cj — name persistence must be co-durable with the covering
//! append, at the strength the engine's [`Durability`] mode asks for.
//!
//! # Background
//!
//! `EngineError::Meta` on reopen ("no interned name for stream_id …") is
//! reachable whenever a newly-interned stream/type name is lost while a
//! covering event survives: unlike every other meta table (`stream_heads`,
//! `snapshot_heads`, dedupe), `stream_names`/`type_names` are NOT a derived
//! cache the log can rebuild (I5) — this engine's log frames carry only the
//! numeric `stream_id`/`event_type_id`, never the name string (see
//! `engine.rs`'s module doc; the spec's `$registry`, `04-registry.md`, would
//! carry names in the log, but that is not implemented here). So recovery
//! resolves names purely from these tables, and the invariant it needs is:
//! **no committed event may out-live its stream/type name.**
//!
//! # The mechanism, and why it is mode-gated (bn-2cj)
//!
//! The name flush is issued strictly BEFORE the covering committer append is
//! submitted; *which* flush depends on the engine's [`Durability`] mode:
//!
//! * `Os`/`Group` (the log append is acked only after its own real `fdatasync`
//!   barrier): the name rides a matching `SyncAll` (`fsync`) barrier —
//!   [`MetaStore::persist`], counted by [`LogEngine::meta_persist_call_count`].
//!   The name is on stable storage before the event that references it can be.
//!   bn-150, unchanged.
//! * `Process` (the log append is acked the instant its own `write(2)` reaches
//!   the OS page cache; §1.1 promises process-crash survival only, with an
//!   unbounded, OS-governed power-loss window): a per-name `fsync` would be
//!   strictly stronger than the operator asked for — and it is exactly that
//!   `fsync` that dominated new-stream latency (spike bn-1jg, ~3.4 ms/new
//!   stream, 98.8% of it). So the name is pushed to the SAME OS page cache the
//!   event bytes go to, WITHOUT a barrier — [`MetaStore::persist_buffered`],
//!   counted by [`LogEngine::meta_buffered_persist_call_count`] — ordered
//!   strictly before the covering event's own `write(2)`.
//!
//! # Why these tests assert call counts, not "kill -9 and reopen"
//!
//! A real power-loss event cannot be simulated portably from inside
//! `cargo test`: fjall's default insert/commit path already performs a real
//! `write(2)` to the OS page cache on every call, so an abrupt process exit
//! (`kill -9`, or `std::process::exit` skipping every destructor) does not
//! reproduce the loss — the write already reached the kernel and survives any
//! process-level death; only an actual OS crash / hardware power cut, which
//! loses the page cache itself, would. (`mess-log`'s own
//! `tests/sigkill_harness.rs` documents the identical limitation for its
//! `Os`/`Group` durability modes.) So instead these tests pin the actual
//! mechanism by construction: the SyncAll-barrier count (barriered modes) and
//! the buffered-flush count (`Process`), each fired exactly when — and only
//! when — a name was newly interned, before the covering append is submitted.
//! The reopen tests then confirm the end-to-end effect (no `EngineError::Meta`,
//! correct names everywhere) that a process crash after such an append
//! preserves in every mode.

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

// ---------------------------------------------------------------------------
// Barriered modes (`Os`/`Group`): the SyncAll barrier still holds (bn-150).
// ---------------------------------------------------------------------------

/// Under `Os`, an append that interns a brand-new stream name must issue
/// exactly one real `fsync` barrier (`SyncAll`) before it returns — the
/// bn-150 guarantee, which bn-2cj must NOT weaken for a barriered mode.
#[tokio::test]
async fn new_stream_name_under_os_forces_a_durable_meta_flush() {
    let dir =
        mess_testkit::sweeping_temp_dir("name-durability-os-new-stream-forces");
    let engine = open_with(&dir.path().join("store"), Durability::Os);

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
        "a newly-interned stream name under Os must force exactly one durable \
         (SyncAll) meta flush"
    );
    // The barrier IS the durability here; no barrier-free flush was used.
    assert_eq!(engine.meta_buffered_persist_call_count(), 0);
}

/// Under `Os`, a new event-TYPE name (on an already-known stream) must equally
/// force a durable barrier — the type-name call site is a second, independent
/// path through `persist_new_names`, folded with the stream-name call site
/// into one barrier per append (bn-150).
#[tokio::test]
async fn new_type_name_under_os_forces_a_durable_meta_flush() {
    let dir =
        mess_testkit::sweeping_temp_dir("name-durability-os-new-type-forces");
    let engine = open_with(&dir.path().join("store"), Durability::Os);

    engine
        .append_batch("s", Version::NoStream, &[rec("evt.a", b"1")])
        .await
        .expect("append b0 (new stream AND new type, folded into ONE flush)");
    assert_eq!(
        engine.meta_persist_call_count(),
        1,
        "a new stream + a new type in the SAME append must fold into one \
         barrier, not two"
    );

    engine
        .append_batch("s", Version::At(0), &[rec("evt.b", b"2")])
        .await
        .expect("append b1 (same stream, NEW type: +1)");
    assert_eq!(
        engine.meta_persist_call_count(),
        2,
        "a newly-interned event type must also force a durable barrier"
    );
}

// ---------------------------------------------------------------------------
// `Process`: a new name is flushed to the page cache WITHOUT an fsync barrier.
// This is the bn-2cj win — the per-new-stream fsync that dominated latency is
// gone, replaced by a barrier-free page-cache write ordered before the event.
// ---------------------------------------------------------------------------

/// The bone's core assertion: under `Durability::Process` an append that
/// interns a brand-new stream name does NOT issue an `fsync` barrier — it
/// flushes the name to the OS page cache (`Buffer`) instead. Zero `SyncAll`
/// calls, exactly one buffered flush. This is the counting-wrapper proof that
/// the new-stream append cannot block on a meta `fsync`.
#[tokio::test]
async fn new_stream_name_under_process_flushes_without_a_barrier() {
    let dir = mess_testkit::sweeping_temp_dir(
        "name-durability-process-new-stream-no-barrier",
    );
    let engine = open_with(&dir.path().join("store"), Durability::Process);

    assert_eq!(engine.meta_persist_call_count(), 0);
    assert_eq!(engine.meta_buffered_persist_call_count(), 0);

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
        0,
        "under Process a new stream name must NOT force a SyncAll fsync \
         barrier — the log itself issues none"
    );
    assert_eq!(
        engine.meta_buffered_persist_call_count(),
        1,
        "under Process a new stream name must be pushed to the OS page cache \
         via exactly one barrier-free (Buffer) flush"
    );
}

/// Corroborating timing proof (bn-2cj, spike bn-1jg's yardstick shape): under
/// `Process`, a run of all-new-stream appends must have a per-append MEDIAN
/// well below the `fsync` floor the barrier would impose. On this class of
/// host the spike measured ~3.4 ms/new-stream WITH the barrier and ~0.03 ms
/// WITHOUT it (existing-stream floor); asserting the median stays under 1 ms
/// separates the two by a wide margin while a p50 (not max) is robust to the
/// occasional writeback/scheduler spike. Deliberately lenient: the
/// deterministic proof is the call-count test above; this guards against a
/// silent reintroduction of a per-new-stream barrier.
#[tokio::test]
async fn process_new_stream_appends_do_not_block_on_a_meta_fsync() {
    let dir = mess_testkit::sweeping_temp_dir(
        "name-durability-process-new-stream-timing",
    );
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

    // Every one of the N appends interned a new stream name → N buffered
    // flushes, zero fsync barriers.
    assert_eq!(engine.meta_buffered_persist_call_count(), N as u64);
    assert_eq!(engine.meta_persist_call_count(), 0);

    samples.sort_unstable();
    let p50 = samples[N / 2];
    assert!(
        p50 < Duration::from_millis(1),
        "Process new-stream append p50 = {p50:?} — a per-new-stream fsync \
         (~ms) appears to have crept back in; the barrier-free page-cache \
         flush should keep this well under 1ms"
    );
}

// ---------------------------------------------------------------------------
// The hot path (either mode): once names are interned, ZERO added flushes.
// ---------------------------------------------------------------------------

/// Once a stream and its event type(s) are interned, repeated appends must add
/// ZERO name flushes of EITHER kind — the append-latency cost this bone
/// requires stay at zero on the common path. Run under both modes.
#[tokio::test]
async fn hot_path_appends_add_zero_meta_flushes() {
    for durability in [Durability::Process, Durability::Os] {
        let dir =
            mess_testkit::sweeping_temp_dir("name-durability-hot-path-appends");
        let engine = open_with(&dir.path().join("store"), durability);

        // Prime the interner: one stream, one type. The only flush this loop
        // expects, ever (a barrier under Os, a buffered flush under Process).
        engine
            .append_batch("hot", Version::NoStream, &[rec("hot.type", b"0")])
            .await
            .expect("priming append");

        let base_barrier = engine.meta_persist_call_count();
        let base_buffered = engine.meta_buffered_persist_call_count();

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
            engine.meta_persist_call_count(),
            base_barrier,
            "{durability:?}: 500 no-new-name appends must add ZERO barriers"
        );
        assert_eq!(
            engine.meta_buffered_persist_call_count(),
            base_buffered,
            "{durability:?}: 500 no-new-name appends must add ZERO buffered \
             flushes"
        );
        assert_eq!(engine.head("hot").await.unwrap(), Version::At(500));
    }
}

// ---------------------------------------------------------------------------
// Concurrency + reopen: every new name survives a clean reopen, both modes.
// ---------------------------------------------------------------------------

/// Concurrent appends across MANY distinct brand-new streams must flush
/// exactly once per newly-interned name (a SyncAll barrier under `Os`, a
/// buffered flush under `Process`), and every one must reopen correctly-named.
/// A clean `drop` (destructors run, page cache intact) models the process-exit
/// class both modes preserve.
#[tokio::test]
async fn concurrent_new_stream_names_each_flush_once_and_survive_reopen() {
    for durability in [Durability::Process, Durability::Os] {
        let dir = mess_testkit::sweeping_temp_dir(
            "name-durability-concurrent-new-streams",
        );
        let store_path = dir.path().join("store");
        let engine = open_with(&store_path, durability);

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

        match durability {
            Durability::Process => {
                assert_eq!(
                    engine.meta_buffered_persist_call_count(),
                    N as u64,
                    "Process: one buffered flush per newly-interned name"
                );
                assert_eq!(engine.meta_persist_call_count(), 0);
            }
            _ => {
                assert_eq!(
                    engine.meta_persist_call_count(),
                    N as u64,
                    "Os: one SyncAll barrier per newly-interned name"
                );
            }
        }

        drop(engine);
        let engine = open_with(&store_path, durability);
        for i in 0..N {
            assert_eq!(
                engine.head(&format!("concurrent-{i}")).await.unwrap(),
                Version::At(0),
                "{durability:?}: stream {i}'s durable event must survive \
                 reopen"
            );
        }
    }
}

/// A genuine fresh reopen after a new-name append must never surface
/// `EngineError::Meta` and must resolve the exact correct name through every
/// read path — the end-to-end effect the call-count tests explain the
/// mechanism of. Under `Process` this is the crux of the bn-2cj contract: the
/// buffered name `write(2)` (ordered before the event's own `write(2)`) is
/// enough for a clean reopen after the process exits, even though no `fsync`
/// was issued. Run under both modes.
#[tokio::test]
async fn reopen_after_new_name_append_resolves_correct_name_everywhere() {
    for durability in [Durability::Process, Durability::Os] {
        let dir = mess_testkit::sweeping_temp_dir(
            "name-durability-reopen-after-new-name",
        );
        let store_path = dir.path().join("store");

        {
            let engine = open_with(&store_path, durability);
            engine
                .append_batch(
                    "acct-42",
                    Version::NoStream,
                    &[rec("account.opened", b"carol")],
                )
                .await
                .expect("append with new stream + new type");
        }

        let engine = LogEngine::open_with(
            &store_path,
            EngineOptions { durability, ..EngineOptions::default() },
        )
        .expect("reopen must not error EngineError::Meta");
        assert_eq!(engine.head("acct-42").await.unwrap(), Version::At(0));
        let s =
            engine.read_stream("acct-42", Version::NoStream, 10).await.unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].stream_id, "acct-42");
        assert_eq!(s[0].message_type, "account.opened");
        assert_eq!(s[0].data, b"carol");
        let g = engine.read_global(None, 10).await.unwrap();
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].stream_id, "acct-42");
    }
}
