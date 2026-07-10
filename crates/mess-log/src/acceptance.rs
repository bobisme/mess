//! The batch-acceptance kernel: the pure commit-authority decision (D1)
//! over byte-validated batch candidates.
//!
//! Spec: `notes/mess-research/12_convergence.md` D1/D2, rules A1–A12.
//!
//! # Division of responsibility
//!
//! The byte layer (the recovery scanner, bn-39n) proves, per candidate
//! batch, in on-disk order: header magic/version, the A2 `total_len`
//! sanity cap, marker magic + `total_len`/CRC echoes (A3), the full-batch
//! CRC (A4 — mandatory, no CRC-off path may exist, A12), and exact
//! subframe tiling. Each candidate reduces to a [`CandidateStatus`].
//!
//! This kernel then makes every accept/stop decision:
//!
//! - **A5**: empty batches (`frame_count == 0`) are rejected.
//! - **A9**: a batch whose segment epoch differs from the scanned segment's
//!   current epoch is rejected — this is what stops a recycled segment's stale
//!   prior-generation batch at a coincident position from resurrecting deleted
//!   data.
//! - **A1**: `first_global_pos` must equal the expected next position.
//! - **A10**: the first rejection is TERMINAL. The kernel latches the stop;
//!   feeding it further candidates — however valid — can never yield another
//!   acceptance. Resynchronization is impossible by construction, not by
//!   scanner discipline.
//!
//! # Contract with the scanner
//!
//! The production recovery scanner MUST route every accept/stop decision
//! through [`AcceptState::step`] (or the [`accepted_prefix`] fold built
//! on it). The stateright model (`tests/stateright.rs`)
//! exhaustively checks THIS function against a reordering-disk
//! abstraction; that guarantee transfers to production only if the
//! scanner calls it rather than re-implementing the rules.

/// The protocol-relevant fields of one byte-validated batch, as decoded
/// from its `BatchHeader` (D2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Candidate {
    /// Segment epoch / generation stamped in the header (A9, R3).
    pub epoch:            u64,
    pub batch_id:         u64,
    pub first_global_pos: u64,
    pub frame_count:      u32,
}

/// Byte-layer verdict for one candidate slot, in on-disk order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CandidateStatus {
    /// Header magic/version, A2 length cap, marker echoes (A3), the
    /// full-batch CRC (A4/A12), and subframe tiling all verified.
    ByteValid(Candidate),
    /// Anything else: torn/absent header, bad magic/version/length,
    /// bad marker, CRC mismatch, frames that do not tile. The scanner
    /// keeps its own detailed reason; the kernel only needs to know the
    /// bytes did not validate.
    ByteInvalid,
}

/// Why the scan stopped. The byte-level detail for `ByteFault` lives in
/// the scanner; the kernel-level reasons are the protocol rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StopReason {
    /// The candidate iterator was exhausted without a fault.
    EndOfScan,
    /// The byte layer rejected the candidate (see scanner for detail).
    ByteFault,
    /// `frame_count == 0` (A5).
    EmptyBatch,
    /// Header epoch differs from the segment's current epoch (A9).
    EpochMismatch,
    /// `first_global_pos` is not the expected next position (A1).
    PositionDiscontinuity,
}

/// Result of one [`AcceptState::step`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// The batch is committed history; the caller may surface it.
    Accept(Candidate),
    /// The scan is over — this batch and EVERYTHING after it is dead
    /// space (A10). The caller must treat the current offset as the
    /// safe truncation point and never look further.
    Stopped(StopReason),
}

/// Streaming acceptance state for one segment scan.
///
/// A10 is enforced structurally: once any step returns
/// [`Step::Stopped`], every subsequent step returns the same stop and
/// can never accept again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AcceptState {
    expected_epoch: u64,
    expected_pos:   u64,
    stopped:        Option<StopReason>,
}

impl AcceptState {
    /// Start a scan of a segment stamped with `segment_epoch` whose
    /// first batch must begin at `segment_base_pos` (A7: scans always
    /// start at the segment start; sealed footers seed the base).
    pub fn new(segment_epoch: u64, segment_base_pos: u64) -> Self {
        AcceptState {
            expected_epoch: segment_epoch,
            expected_pos:   segment_base_pos,
            stopped:        None,
        }
    }

    /// Decide the next candidate. All rule checks live here — see the
    /// module docs for which rule each branch is.
    pub fn step(&mut self, status: CandidateStatus) -> Step {
        if let Some(reason) = self.stopped {
            // A10: the stop is terminal; no resynchronization.
            return Step::Stopped(reason);
        }
        let cand = match status {
            CandidateStatus::ByteInvalid => {
                return self.stop(StopReason::ByteFault)
            }
            CandidateStatus::ByteValid(c) => c,
        };
        if cand.frame_count == 0 {
            return self.stop(StopReason::EmptyBatch); // A5
        }
        if cand.epoch != self.expected_epoch {
            return self.stop(StopReason::EpochMismatch); // A9
        }
        if cand.first_global_pos != self.expected_pos {
            return self.stop(StopReason::PositionDiscontinuity); // A1
        }
        self.expected_pos += u64::from(cand.frame_count);
        Step::Accept(cand)
    }

    /// The global position that follows the last accepted batch.
    pub fn next_pos(&self) -> u64 { self.expected_pos }

    /// The latched stop, if the scan has ended.
    pub fn stopped(&self) -> Option<StopReason> { self.stopped }

    fn stop(&mut self, reason: StopReason) -> Step {
        self.stopped = Some(reason);
        Step::Stopped(reason)
    }
}

/// Outcome of scanning a whole segment's candidates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScanOutcome {
    /// The accepted prefix, in on-disk order. `accepted.len()` is the
    /// index of the first dead slot — the byte layer maps it back to
    /// the safe truncation offset.
    pub accepted: Vec<Candidate>,
    /// Global position following the last accepted batch.
    pub next_pos: u64,
    /// Why the scan ended.
    pub stop:     StopReason,
}

/// Fold [`AcceptState::step`] over a segment's candidates: the
/// commit-authority decision (D1) as one pure function.
pub fn accepted_prefix(
    segment_epoch: u64,
    segment_base_pos: u64,
    statuses: impl IntoIterator<Item = CandidateStatus>,
) -> ScanOutcome {
    let mut state = AcceptState::new(segment_epoch, segment_base_pos);
    let mut accepted = Vec::new();
    for status in statuses {
        match state.step(status) {
            Step::Accept(c) => accepted.push(c),
            Step::Stopped(_) => break,
        }
    }
    ScanOutcome {
        accepted,
        next_pos: state.next_pos(),
        stop: state.stopped().unwrap_or(StopReason::EndOfScan),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(epoch: u64, id: u64, pos: u64, frames: u32) -> Candidate {
        Candidate {
            epoch,
            batch_id: id,
            first_global_pos: pos,
            frame_count: frames,
        }
    }

    fn valid(epoch: u64, id: u64, pos: u64, frames: u32) -> CandidateStatus {
        CandidateStatus::ByteValid(cand(epoch, id, pos, frames))
    }

    #[test]
    fn accepts_contiguous_current_epoch_prefix() {
        let out = accepted_prefix(
            1,
            10,
            [valid(1, 0, 10, 2), valid(1, 1, 12, 1), valid(1, 2, 13, 3)],
        );
        assert_eq!(out.accepted.len(), 3);
        assert_eq!(out.next_pos, 16);
        assert_eq!(out.stop, StopReason::EndOfScan);
    }

    #[test]
    fn byte_fault_stops_scan() {
        let out = accepted_prefix(
            0,
            0,
            [valid(0, 0, 0, 1), CandidateStatus::ByteInvalid],
        );
        assert_eq!(out.accepted.len(), 1);
        assert_eq!(out.stop, StopReason::ByteFault);
    }

    #[test]
    fn a10_stop_is_terminal_even_for_valid_bait() {
        // The resync-bait shape from spikes/torn_write: a hole followed
        // by a fully valid, position-contiguous-looking batch. The
        // kernel must never accept it.
        let out = accepted_prefix(
            0,
            0,
            [
                valid(0, 0, 0, 1),
                CandidateStatus::ByteInvalid, // batch 1: reordering hole
                valid(0, 2, 3, 1),            // batch 2: fully persisted bait
            ],
        );
        assert_eq!(out.accepted.len(), 1);
        assert_eq!(out.stop, StopReason::ByteFault);

        // Even bait at the exact expected position must stay dead.
        let mut st = AcceptState::new(0, 0);
        assert_eq!(st.step(valid(0, 0, 0, 1)), Step::Accept(cand(0, 0, 0, 1)));
        assert_eq!(
            st.step(CandidateStatus::ByteInvalid),
            Step::Stopped(StopReason::ByteFault)
        );
        assert_eq!(
            st.step(valid(0, 1, 1, 1)),
            Step::Stopped(StopReason::ByteFault),
            "a latched stop must repeat, never accept"
        );
    }

    #[test]
    fn a9_stale_epoch_rejected_even_at_coincident_position() {
        // spikes/torn_write's A9 headline: a recycled segment holding an
        // intact stale-generation batch at the exact expected position,
        // with zero new sectors persisted. Only the epoch check rejects.
        let out = accepted_prefix(1, 0, [valid(0, 7, 0, 1)]);
        assert!(out.accepted.is_empty());
        assert_eq!(out.stop, StopReason::EpochMismatch);
    }

    #[test]
    fn a1_position_discontinuity_rejected() {
        // Stale batch in recycled space past the last good batch, at a
        // non-matching position (crash_log's original A1 case).
        let out = accepted_prefix(0, 0, [valid(0, 0, 0, 2), valid(0, 9, 7, 1)]);
        assert_eq!(out.accepted.len(), 1);
        assert_eq!(out.stop, StopReason::PositionDiscontinuity);
    }

    #[test]
    fn a5_empty_batch_rejected() {
        let out = accepted_prefix(0, 0, [valid(0, 0, 0, 0)]);
        assert!(out.accepted.is_empty());
        assert_eq!(out.stop, StopReason::EmptyBatch);
    }

    #[test]
    fn empty_segment_scans_clean() {
        let out = accepted_prefix(3, 42, []);
        assert!(out.accepted.is_empty());
        assert_eq!(out.next_pos, 42);
        assert_eq!(out.stop, StopReason::EndOfScan);
    }
}

// ---------------------------------------------------------------------------
// Kani proofs (bn-y0b): the acceptance predicate proved as an invariant over
// the FULL input domain (up to the documented bounds), not sampled examples.
// `cargo test -p mess-log` above already covers the example shapes pulled
// from `spikes/torn_write`; these harnesses instead ask "does any input
// exist that both passes `AcceptState::step` and violates A1/A5/A9/A10?"
// and let Kani's model checker search the whole space for a counterexample.
//
// Run: `cargo kani --package mess-log --harness <name>` (see
// `docs/verification.md` for the full list and expected run times).
// ---------------------------------------------------------------------------
#[cfg(kani)]
mod kani_proofs {
    use super::*;

    fn any_candidate() -> Candidate {
        Candidate {
            epoch:            kani::any(),
            batch_id:         kani::any(),
            first_global_pos: kani::any(),
            frame_count:      kani::any(),
        }
    }

    fn any_status() -> CandidateStatus {
        if kani::any() {
            CandidateStatus::ByteValid(any_candidate())
        } else {
            CandidateStatus::ByteInvalid
        }
    }

    fn any_stop_reason() -> StopReason {
        match kani::any::<u8>() % 5 {
            0 => StopReason::EndOfScan,
            1 => StopReason::ByteFault,
            2 => StopReason::EmptyBatch,
            3 => StopReason::EpochMismatch,
            _ => StopReason::PositionDiscontinuity,
        }
    }

    /// A1/A5/A9, as ONE invariant over a single [`AcceptState::step`]: for
    /// every pre-step state and every candidate, IF the step accepts, THEN
    /// the accepted candidate has nonzero `frame_count` (A5), an `epoch`
    /// equal to the state's `expected_epoch` (A9), and a `first_global_pos`
    /// equal to the state's `expected_pos` (A1). Equivalently: no input can
    /// both pass acceptance and violate contiguity or the empty-batch rule.
    /// `expected_epoch`/every `Candidate` field ranges over its full
    /// `u64`/`u32` domain; `expected_pos` is bounded to `POSITION_BOUND`
    /// (defined below, alongside the dedicated no-overflow proof) so that
    /// this contiguity property — which has nothing to do with overflow —
    /// isn't entangled with the separate, deliberately-bounded overflow
    /// claim `step_position_advance_no_overflow_bounded` makes.
    #[kani::proof]
    fn step_accept_implies_contiguous_and_nonempty() {
        let expected_pos: u64 = kani::any();
        kani::assume(expected_pos <= POSITION_BOUND);
        let mut state = AcceptState {
            expected_epoch: kani::any(),
            expected_pos,
            stopped: None,
        };
        let pre_epoch = state.expected_epoch;
        let pre_pos = state.expected_pos;

        let result = state.step(any_status());

        if let Step::Accept(c) = result {
            assert_ne!(c.frame_count, 0, "A5: accepted an empty batch");
            assert_eq!(
                c.epoch, pre_epoch,
                "A9: accepted a mismatched-epoch batch"
            );
            assert_eq!(
                c.first_global_pos, pre_pos,
                "A1: accepted a discontinuous batch"
            );
        }
    }

    /// The converse direction of the same invariant: a byte-valid candidate
    /// that violates A1, A5, or A9 is NEVER accepted (`step` stops instead).
    /// Together with `step_accept_implies_contiguous_and_nonempty`, this
    /// pins `step` as implementing exactly the A1/A5/A9 predicate rather
    /// than something merely consistent with it on one side.
    #[kani::proof]
    fn step_rejects_every_a1_a5_a9_violation() {
        let expected_epoch: u64 = kani::any();
        let expected_pos: u64 = kani::any();
        // Bounded per the module doc on `POSITION_BOUND`: a non-violating
        // candidate takes the accept path, which advances `expected_pos`;
        // that arithmetic's overflow-freedom is `POSITION_BOUND`'s job, not
        // this rejection-focused proof's.
        kani::assume(expected_pos <= POSITION_BOUND);
        let mut state =
            AcceptState { expected_epoch, expected_pos, stopped: None };

        let cand = any_candidate();
        let violates = cand.frame_count == 0
            || cand.epoch != expected_epoch
            || cand.first_global_pos != expected_pos;

        let result = state.step(CandidateStatus::ByteValid(cand));
        if violates {
            assert!(matches!(result, Step::Stopped(_)));
        }
    }

    /// A10: a latched stop is TERMINAL. Once `stopped` is `Some(reason)`,
    /// every subsequent `step` — for ANY candidate, including a byte-valid,
    /// position-and-epoch-contiguous "bait" batch (the `spikes/torn_write`
    /// resync-bait shape, generalized here to every possible bait rather
    /// than the one example `cargo test` pins) — returns the SAME stop and
    /// never advances `expected_pos`. Resynchronization is impossible by
    /// construction, proved rather than spot-checked.
    #[kani::proof]
    fn a10_stop_is_terminal_for_any_bait() {
        let reason = any_stop_reason();
        let expected_epoch: u64 = kani::any();
        let expected_pos: u64 = kani::any();
        let mut state =
            AcceptState { expected_epoch, expected_pos, stopped: Some(reason) };

        let bait = any_status();
        let result = state.step(bait);

        assert_eq!(result, Step::Stopped(reason));
        assert_eq!(
            state.expected_pos, expected_pos,
            "a latched stop must never advance position"
        );
        assert_eq!(state.stopped, Some(reason));
    }

    /// Position-advance no-overflow (A1's arithmetic): `expected_pos +=
    /// frame_count` on an accepted step never overflows `u64`. Bound:
    /// `expected_pos <= POSITION_BOUND` (2^40 ~= 1.1e12 events — at a
    /// sustained 1M events/sec that is ~35 years of continuous writes, far
    /// past any real deployment's lifetime); `frame_count` ranges over its
    /// full type maximum (`u32::MAX`), the largest a single batch header can
    /// declare. Documented bound, not the full `u64` range, so the proof
    /// finishes in seconds rather than needing an inductive argument for
    /// positions no real log will ever reach.
    const POSITION_BOUND: u64 = 1 << 40;

    #[kani::proof]
    fn step_position_advance_no_overflow_bounded() {
        let expected_epoch: u64 = kani::any();
        let expected_pos: u64 = kani::any();
        kani::assume(expected_pos <= POSITION_BOUND);
        let mut state =
            AcceptState { expected_epoch, expected_pos, stopped: None };

        let frame_count: u32 = kani::any();
        let cand = Candidate {
            epoch: expected_epoch,
            batch_id: kani::any(),
            first_global_pos: expected_pos,
            frame_count,
        };

        let result = state.step(CandidateStatus::ByteValid(cand));
        if let Step::Accept(_) = result {
            // No panic above (Kani's overflow checks are on) is itself part
            // of the proof; this pins the resulting value too.
            assert_eq!(
                state.expected_pos,
                expected_pos + u64::from(frame_count)
            );
        }
    }

    /// The whole-scan shape of the invariant, folded by hand over three
    /// candidates (the loop-free equivalent of [`accepted_prefix`], which
    /// allocates a `Vec` — avoided here so the harness stays allocation-free
    /// and fast). For every three-candidate sequence: every accepted
    /// candidate has nonzero `frame_count` (A5); the first accepted's
    /// `epoch`/`first_global_pos` match the segment's; every later
    /// accepted's `first_global_pos` continues exactly where the previous
    /// one left off (A1); and nothing is ever accepted after a stop (A10).
    /// Three steps is the smallest bound that exercises "accept, stop,
    /// bait" in one run; going further multiplies run time without adding
    /// a new shape the single-step proofs above don't already cover.
    #[kani::proof]
    fn three_step_fold_is_contiguous_and_terminal() {
        let segment_epoch: u64 = kani::any();
        let segment_base_pos: u64 = kani::any();
        kani::assume(segment_base_pos <= POSITION_BOUND);

        let mut state = AcceptState::new(segment_epoch, segment_base_pos);
        let mut expected_next = segment_base_pos;
        let mut seen_stop = false;

        for _ in 0..3 {
            match state.step(any_status()) {
                Step::Accept(c) => {
                    assert!(!seen_stop, "A10: accepted after a stop");
                    assert_ne!(c.frame_count, 0, "A5");
                    assert_eq!(c.epoch, segment_epoch, "A9");
                    assert_eq!(c.first_global_pos, expected_next, "A1");
                    expected_next += u64::from(c.frame_count);
                }
                Step::Stopped(_) => {
                    seen_stop = true;
                }
            }
        }
    }
}
