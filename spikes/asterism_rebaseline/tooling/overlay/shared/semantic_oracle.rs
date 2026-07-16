//! Generation-neutral public-facade correctness oracle for variants A/C/D.
//!
//! Keep product-generation accounting outside this module. The registry
//! consumes different physical positions in the three generations, while the
//! application-visible append, replay, subscription, conflict, and codec-error
//! semantics below must remain byte-identical.

use mess_core::{Aggregate, CodecError, Event};
use mess_store::{
    AppendError, EventStore, FjallSnapshotBackend, LogEngine, Version,
};

use crate::digest::LogicalDigest;

const EVENT_NAME: &str = "asterism.rebaseline.event";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OracleEvent {
    payload: Vec<u8>,
}

impl OracleEvent {
    pub fn new(payload: impl Into<Vec<u8>>) -> Self {
        Self { payload: payload.into() }
    }
}

impl Event for OracleEvent {
    fn name(&self) -> &'static str { EVENT_NAME }

    fn encode(&self) -> Result<Vec<u8>, CodecError> { Ok(self.payload.clone()) }

    fn decode(name: &str, bytes: &[u8]) -> Result<Self, CodecError> {
        if name != EVENT_NAME {
            return Err(CodecError::UnknownEventName(name.to_owned()));
        }
        Ok(Self::new(bytes))
    }
}

#[derive(Clone, Debug)]
struct RejectedEvent;

impl Event for RejectedEvent {
    fn name(&self) -> &'static str { "asterism.rebaseline.rejected" }

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Err(CodecError::Encode("common oracle rejection".to_owned()))
    }

    fn decode(_name: &str, _bytes: &[u8]) -> Result<Self, CodecError> {
        unreachable!("the rejected oracle event is never durable")
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OracleAggregate {
    payloads: Vec<Vec<u8>>,
}

impl OracleAggregate {
    pub fn event_count(&self) -> usize { self.payloads.len() }

    pub fn logical_digest(&self) -> LogicalDigest {
        let mut digest = LogicalDigest::default();
        for payload in &self.payloads {
            digest.update_bytes(payload);
        }
        digest
    }
}

impl Aggregate for OracleAggregate {
    type Event = OracleEvent;

    fn apply(&mut self, event: &Self::Event) {
        self.payloads.push(event.payload.clone());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SemanticOracleObservations {
    pub domain_events:  u64,
    pub fresh_streams:  u64,
    pub public_appends: u64,
}

/// Exercise the exact public semantic core shared by A, C, and D.
///
/// The caller owns construction and lifecycle so every generation still uses
/// its exact `LogEngine -> FjallSnapshotBackend -> EventStore` composition.
pub async fn run_generation_neutral_semantic_oracle(
    store: &EventStore<FjallSnapshotBackend<LogEngine>>,
) -> SemanticOracleObservations {
    let payloads = [
        b"common-oracle/alpha/0".to_vec(),
        b"common-oracle/alpha/1".to_vec(),
        b"common-oracle/beta/0".to_vec(),
        b"common-oracle/alpha/2".to_vec(),
    ];
    let first = store
        .append(
            "oracle-alpha",
            Version::NoStream,
            &[
                OracleEvent::new(payloads[0].clone()),
                OracleEvent::new(payloads[1].clone()),
            ],
        )
        .await
        .expect("oracle alpha initial append");
    assert_eq!((first.events_appended, first.version), (2, Version::At(1)));
    let second = store
        .append(
            "oracle-beta",
            Version::NoStream,
            &[OracleEvent::new(payloads[2].clone())],
        )
        .await
        .expect("oracle beta append");
    assert_eq!((second.events_appended, second.version), (1, Version::At(0)));
    let third = store
        .append(
            "oracle-alpha",
            Version::At(1),
            &[OracleEvent::new(payloads[3].clone())],
        )
        .await
        .expect("oracle alpha continuation append");
    assert_eq!((third.events_appended, third.version), (1, Version::At(2)));
    let conflict = store
        .append(
            "oracle-alpha",
            Version::At(0),
            &[OracleEvent::new(b"must-not-land".to_vec())],
        )
        .await;
    assert!(matches!(
        conflict,
        Err(AppendError::Conflict {
            expected: Version::At(0),
            actual:   Version::At(2),
        })
    ));
    let common_error =
        store.append("oracle-error", Version::NoStream, &[RejectedEvent]).await;
    assert!(matches!(common_error, Err(AppendError::Backend(_))));

    let cursors = [
        first.last_global_position.expect("first oracle cursor"),
        second.last_global_position.expect("second oracle cursor"),
        third.last_global_position.expect("third oracle cursor"),
    ];
    assert!(cursors.windows(2).all(|pair| pair[0] < pair[1]));

    let alpha = store
        .load::<OracleAggregate>("oracle-alpha")
        .await
        .expect("load oracle alpha");
    let beta = store
        .load::<OracleAggregate>("oracle-beta")
        .await
        .expect("load oracle beta");
    let rejected = store
        .load::<OracleAggregate>("oracle-error")
        .await
        .expect("load rejected oracle stream");
    let mut expected_alpha = LogicalDigest::default();
    for payload in [&payloads[0], &payloads[1], &payloads[3]] {
        expected_alpha.update_bytes(payload);
    }
    let mut expected_beta = LogicalDigest::default();
    expected_beta.update_bytes(&payloads[2]);
    assert_eq!((alpha.events_replayed, alpha.version), (3, Version::At(2)));
    assert_eq!((beta.events_replayed, beta.version), (1, Version::At(0)));
    assert_eq!(alpha.state.event_count(), 3);
    assert_eq!(alpha.state.logical_digest(), expected_alpha);
    assert_eq!(beta.state.event_count(), 1);
    assert_eq!(beta.state.logical_digest(), expected_beta);
    assert_eq!(
        (rejected.events_replayed, rejected.version),
        (0, Version::NoStream)
    );

    let mut subscription = store.subscribe(None);
    let records = subscription
        .next_batch()
        .await
        .expect("read oracle subscription history");
    assert_eq!(records.len(), payloads.len());
    let expected = [
        ("oracle-alpha", 0, &payloads[0]),
        ("oracle-alpha", 1, &payloads[1]),
        ("oracle-beta", 0, &payloads[2]),
        ("oracle-alpha", 2, &payloads[3]),
    ];
    for (record, (stream, position, payload)) in records.iter().zip(expected) {
        assert_eq!(record.stream_id, stream);
        assert_eq!(record.stream_position, position);
        assert_eq!(record.message_type, EVENT_NAME);
        assert_eq!(&record.data, payload);
    }
    assert!(
        records
            .windows(2)
            .all(|pair| pair[0].global_position < pair[1].global_position)
    );
    let mut observed = LogicalDigest::default();
    let mut expected_digest = LogicalDigest::default();
    for record in &records {
        observed.update_bytes(&record.data);
    }
    for payload in &payloads {
        expected_digest.update_bytes(payload);
    }
    assert_eq!(observed, expected_digest);

    SemanticOracleObservations {
        domain_events:  4,
        fresh_streams:  2,
        public_appends: 3,
    }
}
