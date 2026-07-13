//! bn-2di (review F1/F3) — the two permanent-corruption paths, as regression
//! tests.
//!
//! Both were reachable from the **public API**, with no crash, no disk-full and
//! no fault injection, because the engine minted stream/event-type ids into its
//! in-memory registry *before* their `$registry` record was durable and never
//! rolled them back when the append failed:
//!
//! * **F1** — append a >64 MiB batch to a NEW stream. The batch is rejected
//!   (`EncodeError::BatchTooLarge`, an ordinary documented user error), but the
//!   ids were already minted and published to every other thread. The next
//!   append to that stream saw them as already-registered, emitted no
//!   `$registry` record, and committed events referencing ids that exist
//!   nowhere in the log. Reopen: *"no interned name for stream_id 1"* — the
//!   store was UNOPENABLE FOREVER, with acked events in it.
//!
//! * **F3** — the same trigger with an existing stream and a new *event type*
//!   was worse: the open-time integrity check covered `stream_id`s only, so the
//!   store opened CLEANLY and then failed every `read_stream` on that stream
//!   ("no interned name for event_type_id 2") — including for its perfectly
//!   valid events, with no warning at open.
//!
//! The fix is structural (nothing is minted until its `$registry` record is
//! irrevocably in the committer's channel), so the F1/F3 triggers can no longer
//! produce a dangling id at all. The checks are still tested directly, against
//! a hand-built log that *does* contain one.

use mess_log::runtime::{Fs, OpenOpts, RealRuntime, Runtime};
use mess_log::writer::{BatchSpec, SegmentParams, SegmentWriter};
use mess_store::backend::{Backend, RecordToAppend};
use mess_store::registry::{
    self, RegistryRecord, event_type_registered, stream_registered,
};
use mess_store::{LogEngine, Version};

fn rec(message_type: &str, data: &[u8]) -> RecordToAppend {
    RecordToAppend {
        message_type: message_type.to_string(),
        data:         data.to_vec(),
    }
}

/// A batch whose encoded length exceeds `MAX_BATCH_LEN` (64 MiB) — the
/// reviewer's trigger, and an ordinary user error the API documents.
fn oversized_batch() -> Vec<RecordToAppend> {
    vec![rec("BigEvent", &vec![0u8; 65 * 1024 * 1024])]
}

// ---------------------------------------------------------------------------
// F1: a rejected append must mint NOTHING.
// ---------------------------------------------------------------------------

/// The reviewer's exact reproduction: reject a >64 MiB batch on a brand-new
/// stream, then use that same stream and event type normally. The second append
/// must register the names it needs (it is the first one that ever really used
/// them) and the store must reopen and read back.
#[tokio::test]
async fn an_oversized_batch_to_a_new_stream_mints_no_ids() {
    let dir = mess_testkit::sweeping_temp_dir("registry-integrity-f1");
    let store = dir.path().join("store");
    let engine = LogEngine::open(&store).expect("open");

    // 1. The rejected append. Nothing may be minted, because nothing reached
    //    the log.
    let err = engine
        .append_batch("acct-1", Version::NoStream, &oversized_batch())
        .await
        .expect_err("a >64 MiB batch must be rejected");
    assert!(
        format!("{err}").to_lowercase().contains("too large")
            || format!("{err}").to_lowercase().contains("encode"),
        "expected an encode/too-large rejection, got: {err}"
    );
    assert_eq!(
        engine.stream_id_of("acct-1"),
        None,
        "F1: a rejected append must not mint a stream id"
    );
    assert_eq!(
        engine.event_type_id_of("BigEvent"),
        None,
        "F1: a rejected append must not mint an event-type id"
    );

    // 2. The SAME stream and event type, for real this time. Before the fix
    //    this committed events referencing unregistered ids.
    let out = engine
        .append_batch(
            "acct-1",
            Version::NoStream,
            &[rec("BigEvent", b"small"), rec("Other", b"x")],
        )
        .await
        .expect("an ordinary append after a rejected one must succeed");
    assert_eq!(out.version, Version::At(1));

    // 3. Reopen. Before the fix: "no interned name for stream_id 1", forever.
    drop(engine);
    let engine = LogEngine::open(&store).expect(
        "the store must reopen: every id it references was registered in the \
         log",
    );
    let got = engine
        .read_stream("acct-1", Version::NoStream, 10)
        .await
        .expect("read back");
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].message_type, "BigEvent");
    assert_eq!(got[0].data, b"small");
    assert_eq!(got[1].message_type, "Other");
}

/// F3's variant: an EXISTING stream and a brand-new event type. The oversized
/// batch must not mint the type id, and the next append with that type must
/// register it.
#[tokio::test]
async fn an_oversized_batch_with_a_new_type_mints_no_type_id() {
    let dir = mess_testkit::sweeping_temp_dir("registry-integrity-f3");
    let store = dir.path().join("store");
    let engine = LogEngine::open(&store).expect("open");

    let out = engine
        .append_batch("acct-1", Version::NoStream, &[rec("Opened", b"a")])
        .await
        .expect("first append");
    assert_eq!(out.version, Version::At(0));

    engine
        .append_batch("acct-1", Version::At(0), &oversized_batch())
        .await
        .expect_err("a >64 MiB batch must be rejected");
    assert_eq!(
        engine.event_type_id_of("BigEvent"),
        None,
        "F3: a rejected append must not mint an event-type id"
    );

    // The stream must still be appendable AND readable — before the fix, the
    // dangling type id opened cleanly and then poisoned every read of this
    // stream, valid events included.
    engine
        .append_batch("acct-1", Version::At(0), &[rec("BigEvent", b"b")])
        .await
        .expect("append with the same (now really used) type");

    drop(engine);
    let engine = LogEngine::open(&store).expect("reopen");
    let got = engine
        .read_stream("acct-1", Version::NoStream, 10)
        .await
        .expect("F3: reads must not be poisoned by a dangling event_type_id");
    assert_eq!(got.len(), 2);
    assert_eq!(got[1].message_type, "BigEvent");
}

// ---------------------------------------------------------------------------
// The open-time integrity check itself (REG21), against hand-built logs that
// DO carry a dangling id. The engine can no longer produce one, so this is the
// only way to test the check — and the check is the load-bearing backstop: it
// is what turns "the ids have no meaning" into a refusal to open instead of a
// silent, half-readable store.
// ---------------------------------------------------------------------------

/// Write a single-segment log by hand: one `$registry` batch carrying
/// `registrations`, then one domain batch on `stream_id` whose subframes carry
/// `type_ids`. Produces exactly the byte layout the engine writes, so
/// `LogEngine::open` recovers it normally.
fn handwrite_log(
    store: &std::path::Path,
    registrations: &[RegistryRecord],
    stream_id: u64,
    type_ids: &[u32],
) {
    std::fs::create_dir_all(store).expect("mkdir store");
    let rt = RealRuntime::new();
    let fs = rt.fs();
    let path = store.join("seg-00000001.log");
    let mut params = SegmentParams::new(1, 0, 1, 0);
    params.segment_size = 1024 * 1024;
    let mut w = SegmentWriter::create(&fs, &path, params).expect("create seg");

    let payloads: Vec<Vec<u8>> =
        registrations.iter().map(|r| r.encode()).collect();
    let subs: Vec<mess_log::encode::Subframe<'_>> = payloads
        .iter()
        .map(|p| {
            mess_log::encode::Subframe::plain(
                registry::REGISTRY_EVENT_TYPE_ID,
                registry::REGISTRY_SCHEMA_VERSION,
                registry::REGISTRY_CODEC_ID,
                p,
            )
        })
        .collect();
    w.append(&BatchSpec {
        stream_id:            registry::REGISTRY_STREAM_ID,
        category_id:          registry::ENGINE_CATEGORY_ID,
        first_stream_version: 0,
        crypto_chain:         None,
        subframes:            &subs,
    })
    .expect("write $registry batch");

    let subs: Vec<mess_log::encode::Subframe<'_>> = type_ids
        .iter()
        .map(|&t| mess_log::encode::Subframe::plain(t, 0, 0, b"payload"))
        .collect();
    w.append(&BatchSpec {
        stream_id,
        category_id: 0,
        first_stream_version: 0,
        crypto_chain: None,
        subframes: &subs,
    })
    .expect("write domain batch");
    w.close().expect("close seg");
    assert!(fs.open(&path, OpenOpts::read_only()).is_ok());
}

/// A log whose domain batch references an `event_type_id` no `$registry` record
/// ever registered must REFUSE to open (review F3). Before the fix it opened
/// cleanly and then failed every read of that stream.
#[test]
fn a_dangling_event_type_id_refuses_to_open() {
    let dir = mess_testkit::sweeping_temp_dir("registry-integrity-dangling-t");
    let store = dir.path().join("store");
    handwrite_log(
        &store,
        &[stream_registered(1, "acct-1"), event_type_registered(1, "Opened")],
        1,
        // Type 1 is registered; type 2 is not — one dangling id among valid
        // events, which is exactly what made the old failure so quiet.
        &[1, 2],
    );

    let err = LogEngine::open(&store)
        .err()
        .expect("a dangling event_type_id must refuse to open");
    let msg = format!("{err}");
    assert!(
        msg.contains("event_type_id 2"),
        "expected a loud dangling-type refusal, got: {msg}"
    );
}

/// The `stream_id` half of the same check, still enforced (it was, before).
#[test]
fn a_dangling_stream_id_refuses_to_open() {
    let dir = mess_testkit::sweeping_temp_dir("registry-integrity-dangling-s");
    let store = dir.path().join("store");
    handwrite_log(
        &store,
        &[event_type_registered(1, "Opened")],
        7, // never registered
        &[1],
    );

    let err = LogEngine::open(&store)
        .err()
        .expect("a dangling stream_id must refuse to open");
    assert!(
        format!("{err}").contains("stream_id 7"),
        "expected a loud dangling-stream refusal, got: {err}"
    );
}

/// The control: the same hand-built shape with every id registered opens, and
/// reads back. (Without this, the two refusals above could be passing for the
/// wrong reason.)
#[test]
fn a_fully_registered_handwritten_log_opens_and_reads() {
    let dir = mess_testkit::sweeping_temp_dir("registry-integrity-control");
    let store = dir.path().join("store");
    handwrite_log(
        &store,
        &[
            stream_registered(1, "acct-1"),
            event_type_registered(1, "Opened"),
            event_type_registered(2, "Closed"),
        ],
        1,
        &[1, 2],
    );

    let engine = LogEngine::open(&store).expect("a well-formed log opens");
    let rt = tokio::runtime::Runtime::new().unwrap();
    let got = rt
        .block_on(engine.read_stream("acct-1", Version::NoStream, 10))
        .expect("read");
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].message_type, "Opened");
    assert_eq!(got[1].message_type, "Closed");
}
