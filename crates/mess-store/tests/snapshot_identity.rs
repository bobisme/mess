//! `bn-2gns`: the public identity and coverage contract, exercised through the
//! public seams.
//!
//! Three laws live here that no other suite can state:
//!
//! 1. **One identity names one aggregate.** Two aggregates claiming the same
//!    complete [`SnapshotCompatibility`] would share a head key and corrupt
//!    each other, so it fails loudly at the first touch of the snapshot path —
//!    the one snapshot-path condition that is not degraded into a miss.
//! 2. **`Empty` and `Through(0)` are different values**, sequentially and under
//!    concurrent publication. ADR 0002 §1 makes both directions mandatory.
//! 3. **A refused save is reported, not swallowed.** Equal coverage with
//!    different content is a conflict; the valid current record stays.

use std::sync::Arc;

use mess_core::{Aggregate, CodecError, Event};
use mess_store::snapshot::{
    SnapshotCompatibility, SnapshotCoverage, SnapshotSaveOutcome,
    SnapshotStore, SnapshotTrust, Snapshottable, StableSnapshotId,
    StateCodecError, StoredSnapshot, register_snapshot_identity,
};
use mess_store::{
    EventStore, MockBackend, SnapshotRef, StoreError, Version,
    interim_stream_id,
};

// ---------------------------------------------------------------------------
// Two aggregates, one identity — the collision this API is designed to catch
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct Bumped(i64);

impl Event for Bumped {
    fn name(&self) -> &'static str { "bumped" }

    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        Ok(self.0.to_le_bytes().to_vec())
    }

    fn decode(name: &str, data: &[u8]) -> Result<Self, CodecError> {
        let b: [u8; 8] = data.try_into().map_err(|_| CodecError::Decode {
            event_name: name.to_string(),
            source:     "want 8 bytes".to_string(),
        })?;
        Ok(Bumped(i64::from_le_bytes(b)))
    }
}

macro_rules! counter_aggregate {
    ($name:ident, $id:literal, $fold:literal) => {
        #[derive(Debug, Default, Clone, PartialEq, Eq)]
        struct $name(i64);

        impl Aggregate for $name {
            type Event = Bumped;

            fn apply(&mut self, e: &Bumped) {
                self.0 = self.0.wrapping_add(e.0);
            }
        }

        impl Snapshottable for $name {
            const AGGREGATE_SCHEMA_ID: StableSnapshotId =
                StableSnapshotId::new($id);
            const FOLD_VERSION: u32 = $fold;

            fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> {
                Ok(self.0.to_le_bytes().to_vec())
            }

            fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> {
                let b: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| StateCodecError("want 8 bytes".to_string()))?;
                Ok($name(i64::from_le_bytes(b)))
            }
        }
    };
}

// The honest pair: one aggregate, two folds. Same schema id on purpose — that
// is what a fold bump *is* — and they must coexist.
counter_aggregate!(Ledger, "test.ledger", 1);
counter_aggregate!(LedgerV2, "test.ledger", 2);

// The mistake: a different aggregate that copy-pasted `Ledger`'s identity
// wholesale, down to the fold version.
counter_aggregate!(Impostor, "test.ledger", 1);

// An unrelated aggregate, for the negative control.
counter_aggregate!(Unrelated, "test.unrelated", 1);

#[test]
fn one_complete_identity_names_exactly_one_aggregate() {
    // Repeat claims by the same type are free.
    register_snapshot_identity::<Ledger>().expect("first claim");
    register_snapshot_identity::<Ledger>().expect("idempotent");

    // A different fold of the same aggregate is a different identity, so it
    // coexists: this is what a deploy looks like from inside one process.
    register_snapshot_identity::<LedgerV2>()
        .expect("a bumped fold is a different key, not a collision");

    // An unrelated aggregate with its own id is fine.
    register_snapshot_identity::<Unrelated>().expect("unrelated");

    // A second aggregate claiming the SAME complete identity would share
    // `Ledger`'s head key. That is the corruption this check exists to prevent,
    // and it fails loudly rather than degrading to a miss.
    let err = register_snapshot_identity::<Impostor>()
        .expect_err("a shared identity must fail loudly");
    assert_eq!(err.compatibility, Ledger::snapshot_compatibility());
    let msg = err.to_string();
    assert!(msg.contains("Ledger"), "the first claimant is named: {msg}");
    assert!(msg.contains("Impostor"), "so is the second: {msg}");
}

#[tokio::test]
async fn a_shared_identity_surfaces_at_the_first_snapshot_touch() {
    let store = EventStore::new(MockBackend::new());
    let stream = "ledger-1";
    store
        .append(stream, Version::NoStream, &[Bumped(3)])
        .await
        .expect("append");

    // Whichever aggregate gets there first owns the identity...
    store.save_snapshot::<Ledger>(stream).await.expect("first claimant");

    // ...and the second is refused, at its first save AND its first
    // accelerated load, with a message naming both types.
    let err = store
        .save_snapshot::<Impostor>(stream)
        .await
        .expect_err("the impostor must not be allowed to write");
    assert!(matches!(err, StoreError::SnapshotIdentity(_)), "got {err:?}");

    let err =
        store.load_cached::<Impostor>(stream).await.expect_err("nor to read");
    assert!(matches!(err, StoreError::SnapshotIdentity(_)), "got {err:?}");

    // The rightful owner is unaffected.
    assert_eq!(
        store.load_cached::<Ledger>(stream).await.expect("owner").state,
        Ledger(3)
    );
}

// ---------------------------------------------------------------------------
// Empty vs Through(0), sequentially and concurrently
// ---------------------------------------------------------------------------

fn stored(
    stream: &str,
    coverage: SnapshotCoverage,
    state: i64,
) -> StoredSnapshot {
    StoredSnapshot {
        snapshot_ref: SnapshotRef {
            compatibility: Unrelated::snapshot_compatibility(),
            coverage,
            trust: SnapshotTrust::UnverifiedCache,
            stream_id: interim_stream_id(stream),
        },
        state_blob:   state.to_le_bytes().to_vec(),
    }
}

async fn head_coverage(
    backend: &MockBackend,
    stream: &str,
) -> Option<SnapshotCoverage> {
    backend
        .load_snapshot(stream, Unrelated::snapshot_compatibility())
        .await
        .expect("infallible")
        .hit()
        .map(|s| s.snapshot_ref.coverage)
}

#[tokio::test]
async fn empty_then_through_zero_advances_and_the_reverse_is_refused() {
    let backend = MockBackend::new();

    // Empty -> Through(0): an increase.
    let s = "seq-up";
    assert_eq!(
        backend
            .save_snapshot(s, stored(s, SnapshotCoverage::Empty, 0))
            .await
            .unwrap(),
        SnapshotSaveOutcome::Published
    );
    assert_eq!(
        backend
            .save_snapshot(s, stored(s, SnapshotCoverage::Through(0), 1))
            .await
            .unwrap(),
        SnapshotSaveOutcome::Published
    );
    assert_eq!(
        head_coverage(&backend, s).await,
        Some(SnapshotCoverage::Through(0))
    );

    // Through(0) -> Empty: a regression, refused, and reported.
    let s = "seq-down";
    backend
        .save_snapshot(s, stored(s, SnapshotCoverage::Through(0), 1))
        .await
        .unwrap();
    assert_eq!(
        backend
            .save_snapshot(s, stored(s, SnapshotCoverage::Empty, 0))
            .await
            .unwrap(),
        SnapshotSaveOutcome::CoverageRegressed {
            current: SnapshotCoverage::Through(0),
        },
        "Empty is strictly weaker than Through(0), never equal to it"
    );
    assert_eq!(
        head_coverage(&backend, s).await,
        Some(SnapshotCoverage::Through(0)),
        "the stronger head survives"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_empty_and_through_zero_always_settle_on_through_zero() {
    // Whichever order the two publications interleave, the lattice decides:
    // `Through(0)` is the answer, every time, for every stream.
    for round in 0..64u64 {
        let backend = MockBackend::new();
        let stream = Arc::new(format!("race-{round}"));

        let a = {
            let backend = backend.clone();
            let stream = Arc::clone(&stream);
            tokio::spawn(async move {
                backend
                    .save_snapshot(
                        &stream,
                        stored(&stream, SnapshotCoverage::Empty, 0),
                    )
                    .await
                    .expect("save")
            })
        };
        let b = {
            let backend = backend.clone();
            let stream = Arc::clone(&stream);
            tokio::spawn(async move {
                backend
                    .save_snapshot(
                        &stream,
                        stored(&stream, SnapshotCoverage::Through(0), 1),
                    )
                    .await
                    .expect("save")
            })
        };
        let (ra, rb) = (a.await.expect("a"), b.await.expect("b"));

        assert_eq!(
            head_coverage(&backend, &stream).await,
            Some(SnapshotCoverage::Through(0)),
            "round {round}: outcomes were {ra:?} / {rb:?}"
        );
        // The weaker save either published first or was refused; it can never
        // report having replaced the stronger one.
        assert!(
            ra == SnapshotSaveOutcome::Published
                || matches!(
                    ra,
                    SnapshotSaveOutcome::CoverageRegressed {
                        current: SnapshotCoverage::Through(0),
                    }
                ),
            "round {round}: unexpected {ra:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// The equal-coverage rule, through the public seam
// ---------------------------------------------------------------------------

#[tokio::test]
async fn equal_coverage_is_idempotent_for_identical_content_and_a_conflict_otherwise()
 {
    let backend = MockBackend::new();
    let s = "equal";
    backend
        .save_snapshot(s, stored(s, SnapshotCoverage::Through(7), 42))
        .await
        .unwrap();

    assert_eq!(
        backend
            .save_snapshot(s, stored(s, SnapshotCoverage::Through(7), 42))
            .await
            .unwrap(),
        SnapshotSaveOutcome::Idempotent,
        "a retry of the same record is a no-op"
    );
    assert_eq!(
        backend
            .save_snapshot(s, stored(s, SnapshotCoverage::Through(7), 99))
            .await
            .unwrap(),
        SnapshotSaveOutcome::Conflict {
            coverage: SnapshotCoverage::Through(7),
        },
        "two different answers for one prefix cannot both be right"
    );

    let head = backend
        .load_snapshot(s, Unrelated::snapshot_compatibility())
        .await
        .unwrap()
        .hit()
        .expect("head");
    assert_eq!(
        head.state_blob,
        42i64.to_le_bytes().to_vec(),
        "the valid current record remains current"
    );
}

#[test]
fn compatibility_is_the_whole_tuple_not_any_part_of_it() {
    let base = Unrelated::snapshot_compatibility();
    let variants = [
        SnapshotCompatibility {
            aggregate_schema_id: StableSnapshotId::new("test.other"),
            ..base
        },
        SnapshotCompatibility { fold_version: base.fold_version + 1, ..base },
        SnapshotCompatibility {
            codec_id: StableSnapshotId::new("test.other-codec"),
            ..base
        },
        SnapshotCompatibility { codec_version: base.codec_version + 1, ..base },
    ];
    for v in variants {
        assert_ne!(v, base, "every field participates in identity");
    }
    // The codec defaults to the aggregate's own name and version 1, so the
    // smallest impl still declares a complete, explicit identity.
    assert_eq!(base.codec_id, base.aggregate_schema_id);
    assert_eq!(base.codec_version, 1);
}
