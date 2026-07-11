//! The `--rebuild` checkpoint-correctness proof.
//!
//! [`rebuild_check`] is the party trick that proves the checkpointed read model
//! is *correct*, not merely fast: it builds the projection **two ways** over
//! the same on-disk log —
//!
//! 1. a full **rebuild from position 0** ([`Projections::new`]), and
//! 2. a **checkpoint resume** ([`Projections::with_checkpoint`]) that loads the
//!    persisted checkpoint and replays only the log *suffix* committed since —
//!
//! catches both up to the same head, then **byte-compares** their folded state
//! (via [`Projections::state_fingerprint`], a canonical iteration-order-
//! independent serialization). If the resume path ever diverged from a clean
//! rebuild — a stale checkpoint trusted too far, a suffix-replay off-by-one —
//! the fingerprints differ and the check fails. This is the read-path analogue
//! of `tests/snapshots.rs`'s cache-on == cache-off differential.
//!
//! Both the `social-web --rebuild` binary path and
//! [`tests::rebuild_matches_after_checkpoint`] call this one function, so the
//! CLI and the test exercise identical logic.
//!
//! # When the resume is non-trivial
//!
//! The proof only *bites* when a valid checkpoint exists at a position behind
//! the current head: then the resume genuinely replays a suffix and the
//! comparison tests that suffix-replay. With **no** checkpoint present (a
//! freshly seeded, never-served store), [`Projections::with_checkpoint`] falls
//! back to a from-0 rebuild too, so the check compares two from-0 rebuilds — a
//! valid but vacuous PASS. [`RebuildReport::resumed_from`] is `0` in that case
//! and non-zero when a real resume happened; the binary prints which.

use std::path::Path;
use std::time::{Duration, Instant};

use crate::Projections;
use crate::projections::Cardinalities;
use crate::store_backend::{OpenError, Store, checkpoint_path, open_store};

/// The outcome of a [`rebuild_check`]: whether the two folds matched, plus the
/// counts and positions worth printing.
#[derive(Debug, Clone)]
pub struct RebuildReport {
    /// The headline: did the checkpoint-resumed state byte-match a clean
    /// from-0 rebuild?
    pub matched:          bool,
    /// Read watermark the from-0 rebuild caught up to (event count folded).
    pub from0_position:   u64,
    /// Read watermark the checkpoint-resumed projection caught up to. Equals
    /// [`from0_position`](Self::from0_position) on a match.
    pub resumed_position: u64,
    /// Global position the resume *started* folding from: `0` if no valid
    /// checkpoint was present (a vacuous match against another from-0
    /// rebuild), non-zero when a real checkpoint was resumed and only its
    /// suffix replayed.
    pub resumed_from:     u64,
    /// Folded cardinalities (identical on a match; reported from the rebuild).
    pub cardinalities:    Cardinalities,
    /// Length in bytes of the canonical state fingerprint that was compared.
    pub fingerprint_len:  usize,
    /// Wall-clock to build the read model by a **full replay from position 0**
    /// (the cold-start cost — synchronous catch-up over the whole log).
    pub from0_build:      Duration,
    /// Wall-clock to build the read model by **resuming from the checkpoint**
    /// (loading the sidecar + replaying only the suffix). Much smaller than
    /// [`from0_build`](Self::from0_build) when a checkpoint is present and
    /// near the head; equal to it when there is no checkpoint.
    pub resume_build:     Duration,
}

impl RebuildReport {
    /// A human-readable PASS/FAIL line plus the counts, for the CLI.
    #[must_use]
    pub fn render(&self) -> String {
        let verdict = if self.matched { "PASS" } else { "FAIL" };
        let resume = if self.resumed_from == 0 {
            "no checkpoint present (compared two from-0 rebuilds)".to_string()
        } else {
            format!("resumed from checkpoint at position {}", self.resumed_from)
        };
        format!(
            "rebuild-compare: {verdict}\n  {resume}\n  from-0 rebuild folded \
             to position {} in {:.3}s\n  checkpoint resume folded to position \
             {} in {:.3}s\n  counts: {} users, {} posts, {} follow edges, {} \
             like edges\n  compared {} bytes of canonical state fingerprint",
            self.from0_position,
            self.from0_build.as_secs_f64(),
            self.resumed_position,
            self.resume_build.as_secs_f64(),
            self.cardinalities.users,
            self.cardinalities.posts,
            self.cardinalities.follow_edges,
            self.cardinalities.like_edges,
            self.fingerprint_len,
        )
    }
}

/// Open the store at `dir` and run the rebuild-compare proof against its
/// sidecar checkpoint. The CLI (`social-web --rebuild`) entry point; opens the
/// store fresh, so it must not be called while another handle holds the store
/// lock (use [`rebuild_compare`] with an already-open [`Store`] for that — e.g.
/// in-process tests).
pub async fn rebuild_check(dir: &Path) -> Result<RebuildReport, OpenError> {
    let store = open_store(dir)?;
    Ok(rebuild_compare(&store, &checkpoint_path(dir)).await)
}

/// Build the read model from position 0 and from the `checkpoint`, catch both
/// up to the same head, and byte-compare their folded state. See the module
/// docs.
///
/// Takes an already-open [`Store`] so it can run against a live store without
/// re-acquiring the on-disk lock. Never writes to the store or the checkpoint:
/// the resume projection is built with an effectively-infinite checkpoint
/// cadence, so running the proof leaves the on-disk checkpoint untouched.
pub async fn rebuild_compare(
    store: &Store,
    checkpoint: &Path,
) -> RebuildReport {
    // Both projections tail the same store directly — `FjallSnapshotBackend`
    // forwards `SubscribeBackend`, so no separate read handle is needed.
    // (1) Clean rebuild from position 0 — the cold-start cost.
    let t0 = Instant::now();
    let from0 = Projections::new(store).await;
    let from0_build = t0.elapsed();
    // (2) Checkpoint resume — with a cadence so large it never rewrites the
    // sidecar while we inspect it (the proof is read-only).
    let t1 = Instant::now();
    let resumed = Projections::with_checkpoint_cadence(
        store,
        checkpoint,
        u64::MAX,
        Duration::from_secs(24 * 60 * 60),
    )
    .await;
    let resume_build = t1.elapsed();

    // Construction synchronously caught both up to the store head, so their
    // fingerprints are comparable immediately.
    let fp0 = from0.state_fingerprint().await;
    let fpr = resumed.state_fingerprint().await;

    RebuildReport {
        matched: fp0 == fpr,
        from0_position: from0.applied_position(),
        resumed_position: resumed.applied_position(),
        resumed_from: resumed.resumed_from(),
        cardinalities: from0.cardinalities().await,
        fingerprint_len: fp0.len(),
        from0_build,
        resume_build,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mess_testkit::sweeping_temp_dir;

    use super::*;
    use crate::ReadModels;
    use crate::seed::{self, SeedConfig};
    use crate::store_backend::open_store;

    /// The headline checkpoint-correctness proof, in-process: seed a store,
    /// persist a checkpoint at a position *behind* the final head (by
    /// checkpointing then writing more events), then run [`rebuild_compare`]
    /// and assert it resumed from a real checkpoint AND matched a clean
    /// from-0 rebuild byte-for-byte.
    ///
    /// One [`Store`] stays open for the whole test (the on-disk log lock is
    /// process-exclusive), so the proof runs via [`rebuild_compare`] on that
    /// open handle rather than [`rebuild_check`]'s fresh open.
    #[tokio::test]
    async fn rebuild_matches_after_checkpoint() {
        let dir = sweeping_temp_dir("social-rebuild");
        let store_dir = dir.path().join("store");
        std::fs::create_dir_all(&store_dir).unwrap();

        let store = open_store(&store_dir).expect("open store");
        let ckpt = checkpoint_path(&store_dir);

        // Phase 1: seed a first batch and checkpoint the read model behind it.
        {
            let cfg = SeedConfig {
                seed: 4,
                users: 12,
                posts: 30,
                deletes: 3,
                unfollows: 3,
                ..SeedConfig::demo()
            };
            seed::generate(&store, &cfg).await;

            let head = store.watermark().await.unwrap();
            let proj = Projections::with_checkpoint_cadence(
                &store,
                &ckpt,
                u64::MAX,
                Duration::from_secs(3600),
            )
            .await;
            proj.wait_for(head.saturating_sub(1)).await;
            proj.checkpoint_now().await.expect("write checkpoint");
        }

        // Phase 2: append MORE events after the checkpoint (through the SAME
        // open store), so a resume must replay a non-empty suffix — this is
        // what makes the proof bite. Distinct seed/streams so nothing
        // collides.
        {
            let cfg = SeedConfig {
                seed: 99,
                users: 6,
                posts: 15,
                deletes: 1,
                unfollows: 0,
                ..SeedConfig::demo()
            };
            seed::generate(&store, &cfg).await;
        }

        // The proof, against the still-open store.
        let report = rebuild_compare(&store, &ckpt).await;
        assert!(
            report.matched,
            "checkpoint-resumed state must byte-match a from-0 rebuild:\n{}",
            report.render()
        );
        assert!(
            report.resumed_from > 0,
            "expected a real checkpoint resume (a non-vacuous proof), got \
             resumed_from=0:\n{}",
            report.render()
        );
        assert_eq!(
            report.from0_position, report.resumed_position,
            "both folds must reach the same head"
        );
        assert!(report.cardinalities.users >= 18, "both seed batches folded");
    }
}
