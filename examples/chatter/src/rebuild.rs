//! The `chatter rebuild` checkpoint-correctness proof.
//!
//! [`rebuild_compare`] builds the read model **two ways** over the same on-disk
//! log —
//!
//! 1. a full **rebuild from position 0** ([`Projections::new`]), and
//! 2. a **checkpoint resume** ([`Projections::with_checkpoint`]) that loads the
//!    persisted checkpoint and replays only the log *suffix* committed since —
//!
//! catches both up to the same head, then **byte-compares** their folded state
//! via [`Projections::state_fingerprint`], a canonical, iteration-order-
//! independent serialization. If the resume path ever diverged from a clean
//! rebuild — a stale checkpoint trusted too far, a suffix-replay off-by-one, a
//! bounded window that evicted differently on the two paths — the fingerprints
//! differ and the check fails.
//!
//! This is the proof that the checkpoint is an **acceleration**, not a second
//! source of truth: the log alone reproduces it exactly.
//!
//! # When the proof bites
//!
//! It only bites when a valid checkpoint exists at a position *behind* the
//! current head: then the resume genuinely replays a suffix. With no checkpoint
//! present (a freshly seeded, never-read store),
//! [`Projections::with_checkpoint`] falls back to a from-0 rebuild too, so the
//! check compares two from-0 rebuilds — a valid but vacuous PASS.
//! [`RebuildReport::resumed_from`] is `0` in that case and non-zero when a real
//! resume happened; the CLI prints which.

use std::path::Path;
use std::time::{Duration, Instant};

use crate::Projections;
use crate::projections::{Cardinalities, CheckpointStatus};
use crate::store_backend::{OpenError, Store, checkpoint_path, open_store};

/// The outcome of a [`rebuild_compare`].
#[derive(Debug, Clone)]
pub struct RebuildReport {
    /// The headline: did the checkpoint-resumed state byte-match a clean
    /// from-0 rebuild?
    pub matched:          bool,
    /// Read watermark the from-0 rebuild caught up to.
    pub from0_position:   u64,
    /// Read watermark the checkpoint-resumed projection caught up to.
    pub resumed_position: u64,
    /// Global position the resume *started* folding from: `0` if no valid
    /// checkpoint was present (a vacuous match), non-zero for a real resume.
    pub resumed_from:     u64,
    /// What happened to the checkpoint — absent, loaded, or
    /// rejected-and-reported.
    pub checkpoint:       CheckpointStatus,
    /// Folded cardinalities (identical on a match; reported from the rebuild).
    pub cardinalities:    Cardinalities,
    /// Length in bytes of the canonical fingerprint that was compared.
    pub fingerprint_len:  usize,
    /// Wall-clock to build the read model by a **full replay from position 0**
    /// — the cold-start cost over the whole log.
    pub from0_build:      Duration,
    /// Wall-clock to build the read model by **resuming from the checkpoint**.
    pub resume_build:     Duration,
}

impl RebuildReport {
    /// A human-readable PASS/FAIL block, for the CLI.
    #[must_use]
    pub fn render(&self) -> String {
        let verdict = if self.matched { "PASS" } else { "FAIL" };
        let resume = match &self.checkpoint {
            CheckpointStatus::Loaded { applied } => {
                format!("resumed from checkpoint at position {applied}")
            }
            CheckpointStatus::Absent => "no checkpoint present (compared two \
                                         from-0 rebuilds)"
                .to_string(),
            CheckpointStatus::Rejected { reason } => format!(
                "checkpoint REJECTED and reported ({reason}); rebuilt from \
                 the log"
            ),
            CheckpointStatus::Disabled => "checkpointing disabled".to_string(),
        };
        format!(
            "rebuild-compare: {verdict}\n  {resume}\n  from-0 rebuild folded \
             to position {} in {:.3}s\n  checkpoint resume folded to position \
             {} in {:.3}s\n  counts: {} users, {} channels, {} messages, {} \
             reactions\n  compared {} bytes of canonical state fingerprint",
            self.from0_position,
            self.from0_build.as_secs_f64(),
            self.resumed_position,
            self.resume_build.as_secs_f64(),
            self.cardinalities.users,
            self.cardinalities.channels,
            self.cardinalities.messages,
            self.cardinalities.reactions,
            self.fingerprint_len,
        )
    }
}

/// Open the store at `dir` and run the proof against its sidecar checkpoint.
/// Must not be called while another handle holds the store lock — use
/// [`rebuild_compare`] with an already-open [`Store`] for that.
pub async fn rebuild_check(dir: &Path) -> Result<RebuildReport, OpenError> {
    let store = open_store(dir)?;
    Ok(rebuild_compare(&store, &checkpoint_path(dir)).await)
}

/// Build the read model from position 0 and from `checkpoint`, catch both up to
/// the same head, and byte-compare their folded state.
///
/// Never writes to the store or the checkpoint: the resume projection is built
/// with an effectively-infinite cadence, so running the proof leaves the
/// on-disk checkpoint untouched.
pub async fn rebuild_compare(
    store: &Store,
    checkpoint: &Path,
) -> RebuildReport {
    // (1) Clean rebuild from position 0 — the cold-start cost.
    let t0 = Instant::now();
    let from0 = Projections::new(store).await;
    let from0_build = t0.elapsed();
    // (2) Checkpoint resume, with a cadence so large it never rewrites the
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
        checkpoint: resumed.checkpoint_status().clone(),
        cardinalities: from0.cardinalities().await,
        fingerprint_len: fp0.len(),
        from0_build,
        resume_build,
    }
}
