//! The **discardable-acceleration law**, asserted end to end.
//!
//! The log is the sole authority; the projection checkpoint is acceleration a
//! reader is allowed to throw away. Three consequences, one test each:
//!
//! 1. A checkpoint resume must byte-match a clean from-0 rebuild (`chatter
//!    rebuild`).
//! 2. A **deleted** checkpoint is a silent full rebuild — not a warning, not an
//!    error. A store that has never been read has no checkpoint.
//! 3. A **corrupt** checkpoint is **reported and then rebuilt from**. Reads
//!    still work. It is never an error that blocks a read.

use std::time::Duration;

use chatter::Id;
use chatter::ops::WriteOps;
use chatter::projections::{CheckpointStatus, Projections};
use chatter::rebuild::rebuild_compare;
use chatter::seed::{self, SeedConfig};
use chatter::store_backend::{
    Store, checkpoint_path, create_store, sealed_census,
};
use mess_testkit::{SweepingTempDir, sweeping_temp_dir};

/// A seeded, multi-segment store plus the temp dir guarding it.
async fn seeded(
    tag: &str,
    seed: u64,
) -> (SweepingTempDir, std::path::PathBuf, Store) {
    let t = sweeping_temp_dir(tag);
    let dir = t.path().join("store");
    let cfg = SeedConfig::tiny(seed);
    let store = create_store(&dir, cfg.store_config()).expect("create store");
    seed::generate(&store, &cfg).await;
    (t, dir, store)
}

/// A cadence that never fires, so a test only checkpoints when it says so.
const NEVER: (u64, Duration) = (u64::MAX, Duration::from_secs(24 * 60 * 60));

/// The headline proof: a checkpoint resume byte-matches a clean from-0
/// rebuild, and the proof is **non-vacuous** — the resume really did start from
/// a checkpoint behind the head and replay only the suffix.
#[tokio::test]
async fn rebuild_byte_compare_passes_with_a_real_resume() {
    let (_t, dir, store) = seeded("chatter-rebuild", 4).await;
    let ckpt = checkpoint_path(&dir);

    // Checkpoint the read model at the current head...
    {
        let head = store.watermark().await.unwrap();
        let proj = Projections::with_checkpoint_cadence(
            &store, &ckpt, NEVER.0, NEVER.1,
        )
        .await;
        proj.wait_for(head.saturating_sub(1)).await;
        proj.checkpoint_now().await.expect("write checkpoint");
    }
    // ...then commit MORE events, so a resume must replay a non-empty suffix.
    // That is what makes the byte-compare bite rather than compare two
    // identical from-0 rebuilds.
    {
        let cfg =
            SeedConfig { channels: 2, messages: 200, ..SeedConfig::tiny(99) };
        seed::generate(&store, &cfg).await;
    }

    let report = rebuild_compare(&store, &ckpt).await;
    assert!(
        report.matched,
        "a checkpoint-resumed read model MUST byte-match a from-0 rebuild:\n{}",
        report.render()
    );
    assert!(
        report.resumed_from > 0,
        "expected a real resume (a non-vacuous proof):\n{}",
        report.render()
    );
    assert!(
        matches!(report.checkpoint, CheckpointStatus::Loaded { .. }),
        "expected a loaded checkpoint, got {:?}",
        report.checkpoint
    );
    assert_eq!(
        report.from0_position, report.resumed_position,
        "both folds must reach the same head"
    );
    assert!(report.fingerprint_len > 0);

    // The store this ran against really is multi-segment, so the from-0
    // rebuild genuinely replayed sealed history.
    let census = sealed_census(&store, &dir);
    assert!(
        census.sealed_segments > 1,
        "the proof should run over a multi-segment store, got {}",
        census.sealed_segments
    );
}

/// **Checkpoint absent → silent rebuild.** Deleting the sidecar is a supported
/// operation with no effect other than a colder next start.
#[tokio::test]
async fn a_deleted_checkpoint_rebuilds_silently() {
    let (_t, dir, store) = seeded("chatter-ckpt-deleted", 5).await;
    let ckpt = checkpoint_path(&dir);

    let expected = {
        let proj = Projections::with_checkpoint_cadence(
            &store, &ckpt, NEVER.0, NEVER.1,
        )
        .await;
        proj.checkpoint_now().await.expect("write checkpoint");
        assert!(ckpt.exists(), "the checkpoint sidecar should exist now");
        (proj.state_fingerprint().await, proj.cardinalities().await)
    };

    // Throw it away. This is allowed, at any time, with the writer stopped.
    std::fs::remove_file(&ckpt).expect("delete the checkpoint");

    let proj =
        Projections::with_checkpoint_cadence(&store, &ckpt, NEVER.0, NEVER.1)
            .await;
    assert_eq!(
        proj.checkpoint_status(),
        &CheckpointStatus::Absent,
        "an absent checkpoint is SILENT — not a rejection, not a warning"
    );
    assert_eq!(proj.resumed_from(), 0, "it rebuilt from the log");
    assert_eq!(
        proj.state_fingerprint().await,
        expected.0,
        "the rebuilt read model must be byte-identical to the checkpointed one"
    );
    assert_eq!(proj.cardinalities().await, expected.1);
    // And reads work, which is the whole point.
    assert!(!proj.channels().await.is_empty());
}

/// **Checkpoint corrupt → reported, then rebuilt.** Every flavour of unusable
/// sidecar behaves the same way: a report, a rebuild, and working reads.
#[tokio::test]
async fn a_corrupt_checkpoint_is_reported_and_reads_still_work() {
    let (_t, dir, store) = seeded("chatter-ckpt-corrupt", 6).await;
    let ckpt = checkpoint_path(&dir);

    let expected = {
        let proj = Projections::with_checkpoint_cadence(
            &store, &ckpt, NEVER.0, NEVER.1,
        )
        .await;
        proj.checkpoint_now().await.expect("write checkpoint");
        proj.state_fingerprint().await
    };
    let good = std::fs::read(&ckpt).expect("read the checkpoint");
    assert!(good.len() > 64, "fixture sanity: a checkpoint with content");

    // (a) TRUNCATED — the classic torn write.
    std::fs::write(&ckpt, &good[..good.len() / 2]).unwrap();
    assert_rebuilds_after_rejection(&store, &ckpt, &expected, "truncated")
        .await;

    // (b) GARBAGE — a foreign file that is not msgpack at all.
    std::fs::write(&ckpt, b"this is not a checkpoint, it is a haiku").unwrap();
    assert_rebuilds_after_rejection(&store, &ckpt, &expected, "garbage").await;

    // (c) HEAD-CORRUPTED — decodable framing, wrong magic.
    let mut flipped = good.clone();
    for byte in flipped.iter_mut().take(8) {
        *byte ^= 0xFF;
    }
    std::fs::write(&ckpt, &flipped).unwrap();
    assert_rebuilds_after_rejection(&store, &ckpt, &expected, "head-corrupt")
        .await;

    // (d) STALE FOLD VERSION — structurally valid, but written by logic this
    // build no longer has. Discarded, never partially trusted.
    {
        let proj = Projections::with_checkpoint_cadence(
            &store, &ckpt, NEVER.0, NEVER.1,
        )
        .await;
        proj.checkpoint_now_as_version(chatter::PROJECTION_VERSION + 1)
            .await
            .expect("write a stale-version checkpoint");
    }
    assert_rebuilds_after_rejection(&store, &ckpt, &expected, "stale-version")
        .await;

    // Finally: the good checkpoint still resumes, so the rejection paths above
    // were about the damage, not about the loader refusing everything.
    std::fs::write(&ckpt, &good).unwrap();
    let proj =
        Projections::with_checkpoint_cadence(&store, &ckpt, NEVER.0, NEVER.1)
            .await;
    assert!(
        matches!(proj.checkpoint_status(), CheckpointStatus::Loaded { .. }),
        "the undamaged checkpoint must still load, got {:?}",
        proj.checkpoint_status()
    );
    assert!(proj.resumed_from() > 0);
}

/// Build the read model over a damaged checkpoint and assert the contract:
/// **reported** (not silent), **rebuilt** (not partially trusted), **reads
/// work** (not an error).
async fn assert_rebuilds_after_rejection(
    store: &Store,
    ckpt: &std::path::Path,
    expected_fingerprint: &[u8],
    flavour: &str,
) {
    let proj =
        Projections::with_checkpoint_cadence(store, ckpt, NEVER.0, NEVER.1)
            .await;
    match proj.checkpoint_status() {
        CheckpointStatus::Rejected { reason } => {
            assert!(
                !reason.is_empty(),
                "{flavour}: a rejection must carry a reason an operator can \
                 read"
            );
        }
        other => panic!(
            "{flavour}: a damaged checkpoint must be REPORTED as rejected, \
             got {other:?}"
        ),
    }
    assert_eq!(
        proj.resumed_from(),
        0,
        "{flavour}: a rejected checkpoint must rebuild from position 0"
    );
    assert_eq!(
        proj.state_fingerprint().await,
        expected_fingerprint,
        "{flavour}: the rebuilt state must byte-match the healthy one"
    );
    assert!(
        !proj.channels().await.is_empty(),
        "{flavour}: reads must still work"
    );
}

/// A checkpoint that is *ahead* of the store's watermark describes positions
/// the log does not have. It is rejected and reported like any other unusable
/// sidecar rather than trusted into an inconsistent read model.
#[tokio::test]
async fn a_checkpoint_ahead_of_the_log_is_rejected() {
    let (_t, dir, store) = seeded("chatter-ckpt-ahead", 8).await;
    let ckpt = checkpoint_path(&dir);
    {
        let proj = Projections::with_checkpoint_cadence(
            &store, &ckpt, NEVER.0, NEVER.1,
        )
        .await;
        proj.checkpoint_now().await.expect("write checkpoint");
    }
    // A *second*, empty store: the same checkpoint is now far ahead of its log.
    let t2 = sweeping_temp_dir("chatter-ckpt-ahead-empty");
    let dir2 = t2.path().join("store");
    let empty = create_store(&dir2, SeedConfig::tiny(8).store_config())
        .expect("create");
    let ckpt2 = checkpoint_path(&dir2);
    std::fs::copy(&ckpt, &ckpt2).expect("plant the checkpoint");

    let proj =
        Projections::with_checkpoint_cadence(&empty, &ckpt2, NEVER.0, NEVER.1)
            .await;
    assert!(
        matches!(proj.checkpoint_status(), CheckpointStatus::Rejected { .. }),
        "a checkpoint ahead of the log must be rejected, got {:?}",
        proj.checkpoint_status()
    );
    assert_eq!(proj.resumed_from(), 0);
    assert_eq!(proj.cardinalities().await.messages, 0, "the empty log wins");
    let _ = dir;
}

/// Read-your-writes through the live pump: a write committed after the
/// projection was built is folded, and `wait_for` resolves on it.
#[tokio::test]
async fn the_live_pump_folds_writes_after_construction() {
    let (_t, dir, store) = seeded("chatter-live-pump", 12).await;
    let proj = Projections::with_checkpoint_cadence(
        &store,
        checkpoint_path(&dir),
        NEVER.0,
        NEVER.1,
    )
    .await;
    let before = proj.cardinalities().await;

    let channel = proj
        .channels()
        .await
        .first()
        .map(|c| c.id)
        .expect("the seeded store has channels");
    let author = Id::from_parts(1, [42; 10]);
    let pos = store
        .post_message(channel, author, "a message after the fold".into())
        .await
        .expect("post");
    proj.wait_for(pos).await;

    let after = proj.cardinalities().await;
    assert_eq!(after.messages, before.messages + 1);
    let timeline = proj.timeline(1).await;
    assert_eq!(timeline[0].preview, "a message after the fold");
    assert_eq!(timeline[0].seq, pos);
}
