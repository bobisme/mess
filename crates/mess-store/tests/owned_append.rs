//! End-to-end ownership-transfer regressions for `bn-2yye`.

use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use mess_core::{Aggregate, CodecError, Decide, Event};
use mess_log::committer::Durability;
use mess_store::backend::{
    AppendError, Appended, Backend, OwnedAppendBatch, RecordToAppend,
    StoredRecord,
};
use mess_store::registry::{
    REGISTRY_EVENT_TYPE_NAME, RESERVED_STREAM_ID, Registry, RegistryRecord,
};
use mess_store::{
    EngineOptions, EventStore, FjallSnapshotBackend, LogEngine, MockBackend,
    Snapshottable, StateCodecError, Version,
};

#[derive(Debug, Clone)]
struct NamedEvent {
    name: &'static str,
    data: Vec<u8>,
}

impl Event for NamedEvent {
    fn name(&self) -> &'static str { self.name }

    fn encode(&self) -> Result<Vec<u8>, CodecError> { Ok(self.data.clone()) }

    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError> {
        let name = match name {
            "type.a" => "type.a",
            "type.b" => "type.b",
            "type.a.v2" => "type.a.v2",
            other => return Err(CodecError::UnknownEventName(other.to_owned())),
        };
        Ok(Self { name, data: data.to_vec() })
    }
}

fn event(name: &'static str, byte: u8) -> NamedEvent {
    NamedEvent { name, data: vec![byte; 32] }
}

fn owned_record(message_type: &str, data: Vec<u8>) -> OwnedAppendBatch {
    OwnedAppendBatch::from_records(vec![RecordToAppend {
        message_type: message_type.to_owned(),
        data,
    }])
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CounterEvent(u64);

impl Event for CounterEvent {
    fn name(&self) -> &'static str { "counter.incremented" }

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.to_le_bytes().to_vec())
    }

    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError> {
        if name != "counter.incremented" {
            return Err(CodecError::UnknownEventName(name.to_owned()));
        }
        let bytes: [u8; 8] =
            data.try_into().map_err(|_| CodecError::Decode {
                event_name: name.to_owned(),
                source:     "counter increment must be eight bytes".into(),
            })?;
        Ok(Self(u64::from_le_bytes(bytes)))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Counter(u64);

impl Aggregate for Counter {
    type Event = CounterEvent;

    fn apply(&mut self, event: &Self::Event) { self.0 += event.0; }
}

#[derive(Debug, Clone, Copy)]
struct Increment(u64);

impl Decide<Increment> for Counter {
    type Rejection = Infallible;

    fn decide(
        &self,
        command: Increment,
    ) -> Result<Vec<Self::Event>, Self::Rejection> {
        Ok(vec![CounterEvent(command.0)])
    }
}

impl Snapshottable for Counter {
    const FOLD_VERSION: u32 = 1;

    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
        Ok(self.0.to_le_bytes().to_vec())
    }

    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
        let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
            StateCodecError("counter state must be eight bytes".into())
        })?;
        Ok(Self(u64::from_le_bytes(bytes)))
    }
}

#[tokio::test]
async fn public_facade_moves_payloads_without_a_defensive_copy() {
    let dir = mess_testkit::sweeping_temp_dir("owned-append-public");
    let engine = LogEngine::open(dir.path().join("store")).expect("open");
    let store = EventStore::new(engine.clone());
    let events = [event("type.a", 1), event("type.a", 2), event("type.b", 3)];

    let commit = store
        .append("stream", Version::NoStream, &events)
        .await
        .expect("append");
    assert_eq!(commit.version, Version::At(2));
    let input = engine.append_input_metrics();
    assert_eq!(input.owned_batches, 1);
    assert_eq!(input.owned_records, 3);
    assert_eq!(input.owned_payload_bytes, 96);
    assert_eq!(input.borrowed_batches, 0);
    assert_eq!(input.copied_records, 0);
    assert_eq!(input.copied_bytes, 0);

    let read = engine
        .read_stream("stream", Version::NoStream, 10)
        .await
        .expect("read");
    assert_eq!(read.len(), 3);
    assert_eq!(read[0].message_type, "type.a");
    assert_eq!(read[1].data, vec![2; 32]);
    assert_eq!(read[2].message_type, "type.b");
}

#[tokio::test]
async fn production_snapshot_wrapper_forwards_owned_submission() {
    let dir = mess_testkit::sweeping_temp_dir("owned-append-fjall-wrapper");
    let engine = LogEngine::open(dir.path().join("store")).expect("open");
    let backend = FjallSnapshotBackend::open(
        engine.clone(),
        dir.path().join("snapshots"),
    )
    .expect("open snapshot wrapper");
    let store = EventStore::new(backend);
    let before = engine.metrics().commit;

    store
        .append("stream", Version::NoStream, &[event("type.a", 7)])
        .await
        .expect("append through production wrapper");
    let input = engine.append_input_metrics();
    assert_eq!(input.owned_batches, 1);
    assert_eq!(input.owned_records, 1);
    assert_eq!(input.owned_payload_bytes, 32);
    assert_eq!(input.borrowed_batches, 0);
    assert_eq!(input.borrowed_records, 0);
    assert_eq!(input.copied_records, 0);
    assert_eq!(input.copied_bytes, 0);
    let after = engine.metrics().commit;
    assert_eq!(after.fsync.count - before.fsync.count, 0);
}

#[tokio::test]
async fn barriered_public_paths_select_borrowed_compatibility_behavior() {
    for (label, durability, fresh_groups) in
        [("group", Durability::group_default(), 1), ("os", Durability::Os, 2)]
    {
        let dir = mess_testkit::sweeping_temp_dir(&format!(
            "owned-append-barriered-selection-{label}"
        ));
        let engine = LogEngine::open_with(
            dir.path().join("store"),
            EngineOptions { durability, ..EngineOptions::default() },
        )
        .expect("open");
        let backend = FjallSnapshotBackend::open(
            engine.clone(),
            dir.path().join("snapshots"),
        )
        .expect("open snapshot wrapper");
        let store = EventStore::new(backend);
        let before_fresh = engine.metrics().commit;

        store
            .append(
                "stream",
                Version::NoStream,
                &[event("type.a", 7), event("type.a", 8)],
            )
            .await
            .expect("append through barriered production composition");

        let input = engine.append_input_metrics();
        assert_eq!(input.owned_batches, 0, "{label}: owned fast path disabled");
        assert_eq!(
            input.owned_records, 0,
            "{label}: no owned records retained"
        );
        assert_eq!(
            input.owned_payload_bytes, 0,
            "{label}: no owned bytes retained"
        );
        assert_eq!(
            input.borrowed_batches, 1,
            "{label}: one compatibility batch"
        );
        assert_eq!(
            input.borrowed_records, 2,
            "{label}: two compatibility records"
        );
        assert_eq!(input.copied_records, 2, "{label}: small batch is cloned");
        assert_eq!(
            input.copied_bytes,
            2 * ("type.a".len() + 32) as u64,
            "{label}: exact compatibility-boundary bytes",
        );
        let after_fresh = engine.metrics().commit;
        assert_eq!(
            after_fresh.groups - before_fresh.groups,
            fresh_groups,
            "{label}: exact fresh registry-plus-domain groups"
        );
        assert_eq!(
            after_fresh.fsync.count - before_fresh.fsync.count,
            fresh_groups,
            "{label}: exact fresh registry-plus-domain barriers",
        );

        store
            .append("stream", Version::At(1), &[event("type.a", 9)])
            .await
            .expect("append after event type is registered");

        let after_hot = engine.metrics().commit;
        assert_eq!(
            after_hot.groups - after_fresh.groups,
            1,
            "{label}: one hot domain group"
        );
        assert_eq!(
            after_hot.fsync.count - after_fresh.fsync.count,
            1,
            "{label}: one hot domain barrier",
        );
        let input = engine.append_input_metrics();
        assert_eq!(input.owned_batches, 0, "{label}: fast path disabled");
        assert_eq!(input.borrowed_batches, 2, "{label}: two public appends");
        assert_eq!(input.borrowed_records, 3, "{label}: three domain records");
        assert_eq!(input.copied_records, 3, "{label}: all records cloned");
        assert_eq!(input.copied_bytes, 3 * ("type.a".len() + 32) as u64);
    }
}

#[tokio::test]
async fn command_paths_reach_owned_log_engine_submission() {
    let dir = mess_testkit::sweeping_temp_dir("owned-append-command-paths");
    let engine = LogEngine::open(dir.path().join("store")).expect("open");
    let backend = FjallSnapshotBackend::open(
        engine.clone(),
        dir.path().join("snapshots"),
    )
    .expect("open snapshot wrapper");
    let store = EventStore::new(backend);

    store
        .command::<Counter, _>("ordinary-command", Increment(2))
        .await
        .expect("ordinary command");
    store
        .command_cached::<Counter, _>("cached-command", Increment(3))
        .await
        .expect("cached command");

    let input = engine.append_input_metrics();
    assert_eq!(input.owned_batches, 2);
    assert_eq!(input.owned_records, 2);
    assert_eq!(input.owned_payload_bytes, 16);
    assert_eq!(input.borrowed_batches, 0);
    assert_eq!(input.borrowed_records, 0);
    assert_eq!(input.copied_records, 0);
    assert_eq!(input.copied_bytes, 0);
}

#[tokio::test]
async fn large_owned_batch_survives_fjall_wrapper_reopen() {
    let dir = mess_testkit::sweeping_temp_dir("owned-append-large-reopen");
    let store_path = dir.path().join("store");
    let snapshot_path = dir.path().join("snapshots");
    let engine = LogEngine::open(&store_path).expect("open");
    let backend = FjallSnapshotBackend::open(engine.clone(), &snapshot_path)
        .expect("open snapshot wrapper");
    let store = EventStore::new(backend.clone());
    let events = [
        NamedEvent { name: "type.a", data: vec![0xA1; 20 * 1024] },
        NamedEvent { name: "type.b", data: vec![0xB2; 20 * 1024] },
    ];

    store
        .append("large-stream", Version::NoStream, &events)
        .await
        .expect("large append through snapshot wrapper");
    let input = engine.append_input_metrics();
    assert_eq!(input.owned_batches, 1);
    assert_eq!(input.owned_records, 2);
    assert_eq!(input.owned_payload_bytes, 40 * 1024);
    assert_eq!(input.borrowed_batches, 0);
    assert_eq!(input.copied_bytes, 0);

    drop(store);
    drop(backend);
    drop(engine);

    let reopened = LogEngine::open(&store_path).expect("reopen engine");
    let backend = FjallSnapshotBackend::open(reopened, &snapshot_path)
        .expect("reopen snapshot wrapper");
    let records = backend
        .read_stream("large-stream", Version::NoStream, 10)
        .await
        .expect("read reopened large batch");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].message_type, "type.a");
    assert_eq!(records[0].data, vec![0xA1; 20 * 1024]);
    assert_eq!(records[1].message_type, "type.b");
    assert_eq!(records[1].data, vec![0xB2; 20 * 1024]);
}

/// A backend written before the owned method existed: implementing only the
/// original borrowed surface must remain sufficient and must receive the same
/// records/error semantics through the trait's default adapter.
#[derive(Clone)]
struct BorrowOnly {
    inner: MockBackend,
    calls: Arc<AtomicU64>,
}

impl Backend for BorrowOnly {
    type Error = Infallible;

    async fn head(&self, stream_id: &str) -> Result<Version, Self::Error> {
        self.inner.head(stream_id).await
    }

    async fn read_stream(
        &self,
        stream_id: &str,
        after: Version,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        self.inner.read_stream(stream_id, after, limit).await
    }

    async fn read_global(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        self.inner.read_global(after, limit).await
    }

    async fn append_batch(
        &self,
        stream_id: &str,
        expected: Version,
        records: &[RecordToAppend],
    ) -> Result<Appended, AppendError<Self::Error>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.append_batch(stream_id, expected, records).await
    }
}

#[tokio::test]
async fn default_owned_adapter_keeps_borrowed_backends_source_compatible() {
    let calls = Arc::new(AtomicU64::new(0));
    let backend =
        BorrowOnly { inner: MockBackend::new(), calls: Arc::clone(&calls) };
    let store = EventStore::new(backend);
    store
        .append("stream", Version::NoStream, &[event("type.a", 9)])
        .await
        .expect("owned facade through borrowed-only backend");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AdapterFailure;

impl fmt::Display for AdapterFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("adapter failure")
    }
}

impl std::error::Error for AdapterFailure {}

#[derive(Clone)]
struct RecordingBorrowOnly {
    seen: Arc<Mutex<Vec<RecordToAppend>>>,
    fail: bool,
}

impl Backend for RecordingBorrowOnly {
    type Error = AdapterFailure;

    async fn head(&self, _stream_id: &str) -> Result<Version, Self::Error> {
        Ok(Version::NoStream)
    }

    async fn read_stream(
        &self,
        _stream_id: &str,
        _after: Version,
        _limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        Ok(Vec::new())
    }

    async fn read_global(
        &self,
        _after: Option<u64>,
        _limit: usize,
    ) -> Result<Vec<StoredRecord>, Self::Error> {
        Ok(Vec::new())
    }

    async fn append_batch(
        &self,
        _stream_id: &str,
        _expected: Version,
        records: &[RecordToAppend],
    ) -> Result<Appended, AppendError<Self::Error>> {
        *self.seen.lock().expect("seen lock") = records.to_vec();
        if self.fail {
            Err(AppendError::Backend(AdapterFailure))
        } else {
            Ok(Appended {
                version:              Version::At(
                    records.len().saturating_sub(1) as u64,
                ),
                last_global_position: records.len().saturating_sub(1) as u64,
            })
        }
    }
}

#[test]
fn consuming_owned_conversion_preserves_mixed_record_order_and_data() {
    let records = vec![
        RecordToAppend {
            message_type: "type.a".into(),
            data:         vec![1, 2],
        },
        RecordToAppend { message_type: "type.b".into(), data: vec![3] },
        RecordToAppend {
            message_type: "type.a".into(),
            data:         vec![4, 5],
        },
        RecordToAppend {
            message_type: "type.c".into(),
            data:         Vec::new(),
        },
        RecordToAppend { message_type: "type.b".into(), data: vec![6] },
    ];
    let restored =
        OwnedAppendBatch::from_records(records.clone()).into_records();
    assert_eq!(restored, records);
}

#[tokio::test]
async fn default_owned_adapter_preserves_mixed_order_and_backend_errors() {
    let records = vec![
        RecordToAppend { message_type: "type.a".into(), data: vec![1] },
        RecordToAppend { message_type: "type.b".into(), data: vec![2] },
        RecordToAppend { message_type: "type.a".into(), data: vec![3] },
        RecordToAppend { message_type: "type.c".into(), data: vec![4] },
        RecordToAppend { message_type: "type.b".into(), data: vec![5] },
    ];
    let seen = Arc::new(Mutex::new(Vec::new()));
    let backend = RecordingBorrowOnly { seen: Arc::clone(&seen), fail: false };
    let appended = backend
        .append_batch_owned(
            "stream",
            Version::NoStream,
            OwnedAppendBatch::from_records(records.clone()),
        )
        .await
        .expect("default adapter append");
    assert_eq!(appended.version, Version::At(4));
    assert_eq!(*seen.lock().expect("seen lock"), records);

    let failing = RecordingBorrowOnly { seen: Arc::clone(&seen), fail: true };
    let error = failing
        .append_batch_owned(
            "stream",
            Version::NoStream,
            OwnedAppendBatch::from_records(records.clone()),
        )
        .await
        .expect_err("backend failure must propagate");
    assert_eq!(error, AppendError::Backend(AdapterFailure));
    assert_eq!(*seen.lock().expect("seen lock"), records);
}

/// The direct owned API must preserve the ordinary `$registry` seam in every
/// durability mode. Type/decode checks happen at the API boundary, while the
/// authoritative expected-version check precedes REG-state folding inside
/// the owner; rejected inputs never become durable registry records.
#[tokio::test]
async fn direct_owned_registry_preserves_validation_and_reopen_parity() {
    for (label, durability) in [
        ("process", Durability::Process),
        ("group", Durability::group_default()),
        ("os", Durability::Os),
    ] {
        let dir = mess_testkit::sweeping_temp_dir(&format!(
            "owned-append-registry-parity-{label}"
        ));
        let path = dir.path().join("store");
        let open = || {
            LogEngine::open_with(
                &path,
                EngineOptions { durability, ..EngineOptions::default() },
            )
            .expect("open")
        };
        let engine = open();

        let wrong_type = engine
            .append_batch_owned(
                "$registry",
                Version::NoStream,
                owned_record("evil", vec![0xEE]),
            )
            .await
            .expect_err("domain event type must be rejected");
        assert!(matches!(&wrong_type, AppendError::Backend(_)));
        assert!(
            format!("{wrong_type}").contains(REGISTRY_EVENT_TYPE_NAME),
            "{label}: wrong-type refusal must name the required type",
        );

        let corrupt = engine
            .append_batch_owned(
                "$registry",
                Version::NoStream,
                owned_record(
                    REGISTRY_EVENT_TYPE_NAME,
                    b"\xffnot a registry record".to_vec(),
                ),
            )
            .await
            .expect_err("corrupt registry payload must be rejected");
        assert!(matches!(&corrupt, AppendError::Backend(_)));
        assert!(
            format!("{corrupt}").contains("$registry"),
            "{label}: decode refusal must identify the registry stream",
        );

        let invalid_fold = RegistryRecord::StreamRegistered {
            stream_id:   RESERVED_STREAM_ID,
            category_id: 0,
            name:        "forbidden-reserved-stream".to_owned(),
        };
        let invalid = engine
            .append_batch_owned(
                "$registry",
                Version::NoStream,
                owned_record(REGISTRY_EVENT_TYPE_NAME, invalid_fold.encode()),
            )
            .await
            .expect_err("REG-state violation must be rejected");
        assert!(matches!(&invalid, AppendError::Backend(_)));
        assert!(
            format!("{invalid}").contains("$registry"),
            "{label}: fold refusal must identify the registry stream",
        );

        assert_eq!(
            engine.head("$registry").await.unwrap(),
            Version::NoStream,
            "{label}: rejected owned records must not touch the live log",
        );
        drop(engine);
        let engine = open();
        assert_eq!(
            engine.head("$registry").await.unwrap(),
            Version::NoStream,
            "{label}: rejected owned records must not appear after reopen",
        );

        let valid = RegistryRecord::CategoryRegistered {
            category_id: 1,
            name:        format!("orders-{label}"),
        };
        let appended = engine
            .append_batch_owned(
                "$registry",
                Version::NoStream,
                owned_record(REGISTRY_EVENT_TYPE_NAME, valid.encode()),
            )
            .await
            .expect("valid owned registry record");
        assert_eq!(appended.version, Version::At(0));
        drop(engine);

        let engine = open();
        assert_eq!(engine.head("$registry").await.unwrap(), Version::At(0));
        let expected_name = format!("orders-{label}");
        let folded = engine.fold_registry().await.unwrap();
        assert_eq!(
            folded.category_name(1),
            Some(expected_name.as_str()),
            "{label}: valid owned record must survive reopen",
        );

        // Message-type and wire decoding happen before owner admission, so
        // their errors precede a stale expected version.
        let stale_wrong_type = engine
            .append_batch_owned(
                "$registry",
                Version::NoStream,
                owned_record("evil", vec![0xEE]),
            )
            .await
            .expect_err("wrong type must precede stale expected");
        assert!(matches!(&stale_wrong_type, AppendError::Backend(_)));
        assert!(
            format!("{stale_wrong_type}").contains(REGISTRY_EVENT_TYPE_NAME)
        );
        let stale_corrupt = engine
            .append_batch_owned(
                "$registry",
                Version::NoStream,
                owned_record(REGISTRY_EVENT_TYPE_NAME, vec![0xFF]),
            )
            .await
            .expect_err("decode error must precede stale expected");
        assert!(matches!(&stale_corrupt, AppendError::Backend(_)));

        // Decodable records reach the authoritative owner. There the stale
        // expected version precedes both REG folding and a valid append.
        let conflict = AppendError::Conflict {
            expected: Version::NoStream,
            actual:   Version::At(0),
        };
        let stale_invalid = engine
            .append_batch_owned(
                "$registry",
                Version::NoStream,
                owned_record(REGISTRY_EVENT_TYPE_NAME, invalid_fold.encode()),
            )
            .await
            .expect_err("stale expected must precede REG fold validation");
        assert_eq!(stale_invalid, conflict);
        let stale_valid = engine
            .append_batch_owned(
                "$registry",
                Version::NoStream,
                owned_record(
                    REGISTRY_EVENT_TYPE_NAME,
                    RegistryRecord::CategoryRegistered {
                        category_id: 2,
                        name:        "never-written".to_owned(),
                    }
                    .encode(),
                ),
            )
            .await
            .expect_err("valid stale record must conflict");
        assert_eq!(stale_valid, conflict);
        assert_eq!(engine.head("$registry").await.unwrap(), Version::At(0));
        assert_eq!(
            engine.fold_registry().await.unwrap().category_name(2),
            None,
        );
    }
}

#[tokio::test]
async fn empty_owned_batch_still_validates_expected_version() {
    let dir = mess_testkit::sweeping_temp_dir("owned-append-empty-version");
    let engine = LogEngine::open(dir.path().join("store")).expect("open");
    engine
        .append_batch_owned(
            "stream",
            Version::NoStream,
            OwnedAppendBatch::from_records(vec![RecordToAppend {
                message_type: "type.a".into(),
                data:         vec![1],
            }]),
        )
        .await
        .expect("prime");

    let conflict = engine
        .append_batch_owned(
            "stream",
            Version::NoStream,
            OwnedAppendBatch::new(),
        )
        .await
        .expect_err("empty append must not bypass expected-version check");
    assert_eq!(
        conflict,
        AppendError::Conflict {
            expected: Version::NoStream,
            actual:   Version::At(0),
        }
    );

    let appended = engine
        .append_batch_owned("stream", Version::At(0), OwnedAppendBatch::new())
        .await
        .expect("valid empty append");
    assert_eq!(appended.version, Version::At(0));
    assert_eq!(engine.head("stream").await.unwrap(), Version::At(0));
    let input = engine.append_input_metrics();
    assert_eq!(input.owned_batches, 3, "submitted, not committed, batches");
    assert_eq!(input.owned_records, 1);
    assert_eq!(input.owned_payload_bytes, 1);
}

#[tokio::test]
async fn version_conflict_precedes_owned_oversize_validation() {
    let dir = mess_testkit::sweeping_temp_dir("owned-append-oversize-conflict");
    let engine = LogEngine::open(dir.path().join("store")).expect("open");
    engine
        .append_batch_owned(
            "stream",
            Version::NoStream,
            OwnedAppendBatch::from_records(vec![RecordToAppend {
                message_type: "type.a".into(),
                data:         vec![1],
            }]),
        )
        .await
        .expect("prime");

    // MAX_BATCH_LEN is 64 MiB; this payload alone exceeds the encoded limit.
    // The authoritative owner must still report the stale expected version
    // before the framing error, matching the borrowed path's ordering.
    let oversized = OwnedAppendBatch::from_records(vec![RecordToAppend {
        message_type: "type.a".into(),
        data:         vec![0; 64 * 1024 * 1024],
    }]);
    let error = engine
        .append_batch_owned("stream", Version::NoStream, oversized)
        .await
        .expect_err("stale oversized append must conflict first");
    assert_eq!(
        error,
        AppendError::Conflict {
            expected: Version::NoStream,
            actual:   Version::At(0),
        }
    );
}

#[tokio::test]
async fn barriered_fallback_preserves_conflict_before_oversize_ordering() {
    for (label, durability) in
        [("group", Durability::group_default()), ("os", Durability::Os)]
    {
        let dir = mess_testkit::sweeping_temp_dir(&format!(
            "owned-append-barriered-oversize-conflict-{label}"
        ));
        let engine = LogEngine::open_with(
            dir.path().join("store"),
            EngineOptions { durability, ..EngineOptions::default() },
        )
        .expect("open");
        engine
            .append_batch_owned(
                "stream",
                Version::NoStream,
                OwnedAppendBatch::from_records(vec![RecordToAppend {
                    message_type: "type.a".into(),
                    data:         vec![1],
                }]),
            )
            .await
            .expect("prime");

        let oversized = OwnedAppendBatch::from_records(vec![RecordToAppend {
            message_type: "type.a".into(),
            data:         vec![0; 64 * 1024 * 1024],
        }]);
        let error = engine
            .append_batch_owned("stream", Version::NoStream, oversized)
            .await
            .expect_err("stale oversized append must conflict first");
        assert_eq!(
            error,
            AppendError::Conflict {
                expected: Version::NoStream,
                actual:   Version::At(0),
            },
            "{label}: borrowed-compatible validation order",
        );
        let input = engine.append_input_metrics();
        assert_eq!(input.owned_batches, 0, "{label}: fast path disabled");
        assert_eq!(input.borrowed_batches, 2, "{label}: both calls fallback");
    }
}

#[tokio::test]
async fn aliases_are_revalidated_by_registry_state_in_every_mode() {
    for (label, durability) in [
        ("process", Durability::Process),
        ("group", Durability::group_default()),
        ("os", Durability::Os),
    ] {
        let dir = mess_testkit::sweeping_temp_dir(&format!(
            "owned-append-alias-{label}"
        ));
        let engine = LogEngine::open_with(
            dir.path().join("store"),
            EngineOptions { durability, ..EngineOptions::default() },
        )
        .expect("open");
        let store = EventStore::new(engine.clone());
        store
            .append("stream", Version::NoStream, &[event("type.a", 1)])
            .await
            .expect("register old name");
        let type_id = engine.event_type_id_of("type.a").expect("type id");

        let mut registry =
            Registry::bootstrap(engine.clone()).await.expect("registry");
        registry.alias_event_type(type_id, "type.a.v2").await.expect("alias");
        store
            .append(
                "stream",
                Version::At(0),
                &[event("type.a.v2", 2), event("type.a", 3)],
            )
            .await
            .expect("both permanent names resolve");

        assert_eq!(engine.event_type_id_of("type.a"), Some(type_id));
        assert_eq!(engine.event_type_id_of("type.a.v2"), Some(type_id));
        assert_eq!(
            engine.fold_registry().await.unwrap().event_type_high_water_mark(),
            type_id,
            "{label}: batch-local type slots must not mint registry ids",
        );
    }
}

fn poll_once_then_drop<F: Future>(future: F) -> bool {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut future = pin!(future);
    matches!(future.as_mut().poll(&mut cx), Poll::Pending)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_after_owned_transfer_still_publishes() {
    let dir = mess_testkit::sweeping_temp_dir("owned-append-cancel");
    let engine = LogEngine::open(dir.path().join("store")).expect("open");
    let store = EventStore::new(engine.clone());
    store
        .append("stream", Version::NoStream, &[event("type.a", 0)])
        .await
        .expect("prime");

    let events = [event("type.a", 1), event("type.a", 2)];
    assert!(
        poll_once_then_drop(store.append("stream", Version::At(0), &events)),
        "first poll must transfer ownership then await owner completion",
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let read = engine
            .read_stream("stream", Version::NoStream, 10)
            .await
            .expect("read");
        if read.len() == 3 {
            assert_eq!(read[1].data, vec![1; 32]);
            assert_eq!(read[2].data, vec![2; 32]);
            break;
        }
        assert!(Instant::now() < deadline, "owned append never published");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let metrics = engine.metrics();
    assert_eq!(metrics.owner_intent_slots_in_use, 0);
    assert_eq!(metrics.owner_intent_bytes_in_use, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn barriered_fallback_survives_cancellation_after_submission() {
    for (label, durability) in
        [("group", Durability::group_default()), ("os", Durability::Os)]
    {
        let dir = mess_testkit::sweeping_temp_dir(&format!(
            "owned-append-barriered-cancel-{label}"
        ));
        let engine = LogEngine::open_with(
            dir.path().join("store"),
            EngineOptions { durability, ..EngineOptions::default() },
        )
        .expect("open");
        let store = EventStore::new(engine.clone());
        store
            .append("stream", Version::NoStream, &[event("type.a", 0)])
            .await
            .expect("prime");

        let events = [event("type.a", 1), event("type.a", 2)];
        let submitted = poll_once_then_drop(store.append(
            "stream",
            Version::At(0),
            &events,
        ));
        assert!(
            submitted,
            "{label}: first poll must enqueue before cancellation",
        );

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let read = engine
                .read_stream("stream", Version::NoStream, 10)
                .await
                .expect("read");
            if read.len() == 3 {
                assert_eq!(read[1].data, vec![1; 32]);
                assert_eq!(read[2].data, vec![2; 32]);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{label}: cancelled fallback append never published",
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let input = engine.append_input_metrics();
        assert_eq!(input.owned_batches, 0, "{label}: fast path disabled");
        assert_eq!(input.borrowed_batches, 2, "{label}: both calls fallback");
        assert_eq!(
            input.borrowed_records, 3,
            "{label}: exact fallback records"
        );
        let metrics = engine.metrics();
        assert_eq!(
            metrics.owner_intent_slots_in_use, 0,
            "{label}: cancelled append retained an owner intent slot"
        );
        assert_eq!(
            metrics.owner_intent_bytes_in_use, 0,
            "{label}: cancelled append retained owner byte permits"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_new_name_owned_unit_publishes_reopens_and_conflicts() {
    let dir = mess_testkit::sweeping_temp_dir("owned-append-new-name-cancel");
    let path = dir.path().join("store");
    let engine = LogEngine::open(&path).expect("open");
    let store = EventStore::new(engine.clone());
    let events = [event("type.b", 0xB2)];

    assert!(
        poll_once_then_drop(store.append(
            "fresh-owned-stream",
            Version::NoStream,
            &events,
        )),
        "first poll must transfer the fresh-name unit to the owner",
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let records = engine
            .read_stream("fresh-owned-stream", Version::NoStream, 10)
            .await
            .expect("live read");
        if records.len() == 1 {
            assert_eq!(records[0].message_type, "type.b");
            assert_eq!(records[0].data, vec![0xB2; 32]);
            break;
        }
        assert!(Instant::now() < deadline, "fresh-name unit never published");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert!(engine.event_type_id_of("type.b").is_some());

    drop(store);
    drop(engine);
    let reopened = LogEngine::open(&path).expect("reopen");
    let records = reopened
        .read_stream("fresh-owned-stream", Version::NoStream, 10)
        .await
        .expect("reopened read");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].message_type, "type.b");
    assert_eq!(records[0].data, vec![0xB2; 32]);

    let retry = EventStore::new(reopened)
        .append("fresh-owned-stream", Version::NoStream, &events)
        .await
        .expect_err("stale retry must conflict after cancelled caller commit");
    assert!(matches!(
        retry,
        AppendError::Conflict {
            expected: Version::NoStream,
            actual:   Version::At(0),
        }
    ));
}
