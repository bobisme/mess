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
//! - **A9**: a batch whose segment epoch differs from the scanned
//!   segment's current epoch is rejected — this is what stops a recycled
//!   segment's stale prior-generation batch at a coincident position from
//!   resurrecting deleted data.
//! - **A1**: `first_global_pos` must equal the expected next position.
//! - **A10**: the first rejection is TERMINAL. The kernel latches the
//!   stop; feeding it further candidates — however valid — can never
//!   yield another acceptance. Resynchronization is impossible by
//!   construction, not by scanner discipline.
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
    pub epoch: u64,
    pub batch_id: u64,
    pub first_global_pos: u64,
    pub frame_count: u32,
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
        let out =
            accepted_prefix(0, 0, [valid(0, 0, 0, 2), valid(0, 9, 7, 1)]);
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
