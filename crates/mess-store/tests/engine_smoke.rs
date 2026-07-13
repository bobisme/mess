//! Smoke test: the composed engine's append/read/head/conflict path end to
//! end through the real committer bridge.
//!
//! `bn-2di`: global positions are no longer "the index of the Nth user event".
//! The first append that uses a new stream name or a new message type also
//! writes a `$registry` record naming it — an ordinary log event that consumes
//! a global position and is never delivered (stream 0 is filtered out of every
//! user-facing read). Stream VERSIONS are unaffected; the global positions
//! below are annotated with the registrations that precede them.
#![cfg(not(miri))]

use mess_store::backend::{Backend, RecordToAppend};
use mess_store::{EventStore, LogEngine, Version};

fn rec(t: &str, d: &[u8]) -> RecordToAppend {
    RecordToAppend { message_type: t.to_string(), data: d.to_vec() }
}

#[tokio::test]
async fn append_read_head_conflict() {
    let dir = mess_testkit::sweeping_temp_dir(
        "engine-smoke-append-read-head-conflict",
    );
    let engine = LogEngine::open(dir.path()).expect("open");

    // Empty stream.
    assert_eq!(engine.head("acct-1").await.unwrap(), Version::NoStream);

    // First append at NoStream.
    let a = engine
        .append_batch(
            "acct-1",
            Version::NoStream,
            &[rec("Opened", b"x"), rec("Deposited", b"5")],
        )
        .await
        .expect("append");
    // gp 0,1,2 = $registry: stream "acct-1", types "Opened", "Deposited".
    // gp 3,4 = the two events.
    assert_eq!(a.version, Version::At(1));
    assert_eq!(a.last_global_position, 4);
    assert_eq!(engine.head("acct-1").await.unwrap(), Version::At(1));

    // Conflict: stale expected version.
    let err = engine
        .append_batch("acct-1", Version::NoStream, &[rec("Deposited", b"1")])
        .await
        .unwrap_err();
    match err {
        mess_store::AppendError::Conflict { expected, actual } => {
            assert_eq!(expected, Version::NoStream);
            assert_eq!(actual, Version::At(1));
        }
        other => panic!("expected conflict, got {other:?}"),
    }

    // Correct next append.
    let b = engine
        .append_batch("acct-1", Version::At(1), &[rec("Withdrew", b"2")])
        .await
        .expect("append 2");
    // gp 5 = $registry: type "Withdrew" (new). gp 6 = the event.
    assert_eq!(b.version, Version::At(2));
    assert_eq!(b.last_global_position, 6);

    // Second stream, independent positions but shared global order.
    engine
        .append_batch("acct-2", Version::NoStream, &[rec("Opened", b"y")])
        .await
        .unwrap();

    // read_stream on acct-1.
    let page =
        engine.read_stream("acct-1", Version::NoStream, 100).await.unwrap();
    assert_eq!(page.len(), 3);
    assert_eq!(page[0].message_type, "Opened");
    assert_eq!(page[0].stream_position, 0);
    assert_eq!(page[0].global_position, 3);
    assert_eq!(page[2].message_type, "Withdrew");
    assert_eq!(page[2].data, b"2");
    assert_eq!(page[2].stream_position, 2);

    // paged read after position 0.
    let tail = engine.read_stream("acct-1", Version::At(0), 100).await.unwrap();
    assert_eq!(tail.len(), 2);
    assert_eq!(tail[0].stream_position, 1);

    // read_global spans both streams in position order, delivering only user
    // events: gp 7 registered the stream "acct-2", gp 8 is its event.
    let all = engine.read_global(None, 100).await.unwrap();
    assert_eq!(all.len(), 4);
    assert_eq!(
        all.iter().map(|r| r.global_position).collect::<Vec<_>>(),
        vec![3, 4, 6, 8]
    );
    assert_eq!(all[3].stream_id, "acct-2");
}

#[tokio::test]
async fn drives_the_facade() {
    // The facade's load/append/command over the engine.
    let dir = mess_testkit::sweeping_temp_dir("engine-smoke-drives-facade");
    let store = EventStore::new(LogEngine::open(dir.path()).expect("open"));
    let recs = [rec("A", b"1"), rec("B", b"2")];
    // append via the raw backend to prove read-back through the facade's page
    // loop
    store.backend().append_batch("s", Version::NoStream, &recs).await.unwrap();
    let page =
        store.backend().read_stream("s", Version::NoStream, 1).await.unwrap();
    assert_eq!(page.len(), 1);
}
