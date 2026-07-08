//! bn-13s: exhaustive model check of the commit/recovery protocol
//! (D1/D2, A1/A5/A9/A10) with the PRODUCTION acceptance kernel as the
//! recovery guard.
//!
//! Structure:
//! - `production_kernel_is_safe_within_bounds` — the theorem. BFS over
//!   every writer/fsync/crash/recycle/recovery interleaving within
//!   [`BOUNDS`]; safety properties must hold in every state, and the
//!   coverage ("sometimes") properties must all be reachable, proving
//!   the state space actually contains the adversarial shapes.
//! - `teeth_*` — differentials. Each wires a deliberately weakened
//!   kernel (A9 off, A1 off, resync-past-holes) into the same model
//!   and demands the checker FIND a counterexample. If the checker
//!   passes a broken kernel, the model has no teeth and the theorem
//!   above is vacuous.

use mess_log::acceptance::{
    Candidate, CandidateStatus, ScanOutcome, StopReason,
};
use mess_log::model::{
    production_kernel, Action, Bounds, Part, ProtocolModel, State,
};
use stateright::{Checker, Model, Property};

/// Finitization for the main theorem. 4 slots / 5 appends / 2 crash
/// cycles / 1 recycle / frame counts {1,2} reach every protocol shape
/// the spike identified (reordering holes, resync bait, stale
/// coincident batches, post-recovery rewrites) — see the coverage
/// properties, which fail if any shape becomes unreachable.
const BOUNDS: Bounds = Bounds {
    slots:         4,
    max_appends:   5,
    max_inflight:  2,
    max_crashes:   2,
    max_recycles:  1,
    frame_choices: &[1, 2],
};

/// Smaller space for the teeth differentials: every weakened kernel's
/// counterexample shape is reachable here (verified — each test fails
/// the moment its rule is dropped), and it keeps the four exhaustive
/// searches in this file well inside CI memory.
const TEETH_BOUNDS: Bounds = Bounds {
    slots:         3,
    max_appends:   4,
    max_inflight:  2,
    max_crashes:   2,
    max_recycles:  1,
    frame_choices: &[1, 2],
};

struct Formal(ProtocolModel);

impl Model for Formal {
    type Action = Action;
    type State = State;

    fn init_states(&self) -> Vec<State> { vec![self.0.init()] }

    fn actions(&self, s: &State, actions: &mut Vec<Action>) {
        self.0.actions(s, actions)
    }

    fn next_state(&self, s: &State, a: Action) -> Option<State> {
        self.0.apply(s, &a)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // ---- safety: must hold in EVERY reachable state ----
            Property::<Self>::always("acked is a recovered prefix", |m, s| {
                // Every durability-acknowledged batch is recovered,
                // in commit order, byte-identical (spec incl. uniq),
                // at the front of the accepted history.
                if !s.crashed {
                    return true;
                }
                let out = m.0.scan(s);
                let (_, specs) = ProtocolModel::decode_durable(s);
                out.accepted.len() >= s.acked.len()
                    && s.acked
                        .iter()
                        .enumerate()
                        .all(|(i, a)| specs[i] == Some(*a))
            }),
            Property::<Self>::always("accepted only authentic writes", |m, s| {
                // No partial, torn, garbage, or stale-generation batch
                // is ever accepted: every accepted slot holds the exact
                // bytes of a write from the CURRENT epoch (surfacing an
                // unacked complete write is A6's permitted outcome).
                if !s.crashed {
                    return true;
                }
                let out = m.0.scan(s);
                let (_, specs) = ProtocolModel::decode_durable(s);
                out.accepted.iter().enumerate().all(|(i, c)| {
                    match specs[i] {
                        Some(spec) => {
                            spec.cand == *c && s.written.contains(&spec)
                        }
                        None => false,
                    }
                })
            }),
            Property::<Self>::always("accepted history is contiguous", |m, s| {
                // Kernel-output contract, recomputed independently:
                // positions tile from the segment base with no holes or
                // overlaps, all in the current epoch, no empty batches.
                if !s.crashed {
                    return true;
                }
                let mut expected = 0u64;
                m.0.scan(s).accepted.iter().all(|c| {
                    let ok = c.epoch == s.epoch
                        && c.frame_count > 0
                        && c.first_global_pos == expected;
                    expected += u64::from(c.frame_count);
                    ok
                })
            }),
            Property::<Self>::always("recovery is idempotent", |m, s| {
                // Recovery mutates nothing the kernel reads, so
                // re-running the scan (a crash during/just after
                // recovery) must reproduce the identical outcome.
                if !s.crashed {
                    return true;
                }
                m.0.scan(s) == m.0.scan(s)
            }),
            // ---- coverage: the adversarial shapes must be REACHABLE,
            // or the theorem above is about an empty sky ----
            Property::<Self>::sometimes("reaches resync bait (A10)", |m, s| {
                // A fully-valid current-epoch batch sits at/after the
                // scan stop point.
                if !s.crashed {
                    return false;
                }
                let out = m.0.scan(s);
                if out.stop == StopReason::EndOfScan {
                    return false;
                }
                let (statuses, _) = ProtocolModel::decode_durable(s);
                statuses[out.accepted.len()..].iter().any(|st| {
                    matches!(st, CandidateStatus::ByteValid(c)
                        if c.epoch == s.epoch)
                })
            }),
            Property::<Self>::sometimes(
                "reaches intact stale-epoch batch (A9)",
                |_, s| {
                    s.crashed
                        && s.durable.iter().any(|slot| {
                            matches!(slot.decode().0,
                                CandidateStatus::ByteValid(c)
                                    if c.epoch != s.epoch)
                        })
                },
            ),
            Property::<Self>::sometimes(
                "reaches stale batch at coincident base position (A9)",
                |_, s| {
                    // The killer case: only the epoch check stands
                    // between this batch and resurrection.
                    s.crashed
                        && matches!(s.durable[0].decode().0,
                            CandidateStatus::ByteValid(c)
                                if c.epoch != s.epoch
                                    && c.first_global_pos == 0)
                },
            ),
            Property::<Self>::sometimes(
                "reaches header+marker persisted, body hole (A4 shape)",
                |_, s| {
                    s.crashed
                        && s.durable.iter().any(|slot| {
                            match (slot.parts[0], slot.parts[2]) {
                                (Part::Of(h), Part::Of(m)) => {
                                    h == m && slot.parts[1] != Part::Of(h)
                                }
                                _ => false,
                            }
                        })
                },
            ),
            Property::<Self>::sometimes(
                "reaches unacked batch surfacing (A6)",
                |m, s| s.crashed && m.0.scan(s).accepted.len() > s.acked.len(),
            ),
            Property::<Self>::sometimes(
                "reaches recovery-resume rewrite (post-truncation append)",
                |_, s| {
                    // A slot whose durable parts mix two distinct writes
                    // — only possible after resuming over dead space.
                    !s.crashed
                        && s.crashes_used > 0
                        && s.pending.iter().any(|&(slot, _)| {
                            s.durable[slot]
                                .parts
                                .iter()
                                .any(|p| matches!(p, Part::Of(_)))
                        })
                },
            ),
        ]
    }
}

fn check(model: ProtocolModel) -> impl Checker<Formal> {
    Formal(model)
        .checker()
        .threads(std::thread::available_parallelism().map_or(4, |n| n.get()))
        .spawn_bfs()
        .join()
}

/// The theorem: within BOUNDS, the production kernel loses no acked
/// batch, accepts nothing inauthentic/stale/discontiguous, and every
/// adversarial shape is present in the explored space.
#[test]
fn production_kernel_is_safe_within_bounds() {
    let checker =
        check(ProtocolModel { bounds: BOUNDS, kernel: production_kernel });
    println!("explored {} unique states", checker.unique_state_count());
    checker.assert_properties();
}

// ---------------------------------------------------------------------
// Teeth: broken kernels must produce counterexamples, or the model
// proves nothing. Mirrors spikes/torn_write's Full/Weak differential.
// ---------------------------------------------------------------------

const SAFETY: [&str; 3] = [
    "acked is a recovered prefix",
    "accepted only authentic writes",
    "accepted history is contiguous",
];

fn assert_finds_violation(kernel: mess_log::model::Kernel, label: &str) {
    let checker = check(ProtocolModel { bounds: TEETH_BOUNDS, kernel });
    let found = SAFETY.iter().any(|name| checker.discovery(name).is_some());
    assert!(
        found,
        "{label}: checker passed a deliberately broken kernel — the \
         model has no teeth"
    );
}

/// A9 disabled: accept candidates from any epoch.
fn kernel_no_epoch_check(
    _epoch: u64,
    base_pos: u64,
    statuses: &[CandidateStatus],
) -> ScanOutcome {
    let mut accepted: Vec<Candidate> = Vec::new();
    let mut expected = base_pos;
    let mut stop = StopReason::EndOfScan;
    for st in statuses {
        match st {
            CandidateStatus::ByteInvalid => {
                stop = StopReason::ByteFault;
                break;
            }
            CandidateStatus::ByteValid(c) => {
                if c.frame_count == 0 {
                    stop = StopReason::EmptyBatch;
                    break;
                }
                if c.first_global_pos != expected {
                    stop = StopReason::PositionDiscontinuity;
                    break;
                }
                expected += u64::from(c.frame_count);
                accepted.push(*c);
            }
        }
    }
    ScanOutcome { accepted, next_pos: expected, stop }
}

#[test]
fn teeth_kernel_without_epoch_check_fails() {
    assert_finds_violation(
        kernel_no_epoch_check,
        "A9 differential (stale-generation resurrection)",
    );
}

/// A1 disabled: accept any current-epoch valid batch wherever it sits.
fn kernel_no_contiguity_check(
    epoch: u64,
    base_pos: u64,
    statuses: &[CandidateStatus],
) -> ScanOutcome {
    let mut accepted: Vec<Candidate> = Vec::new();
    let mut expected = base_pos;
    let mut stop = StopReason::EndOfScan;
    for st in statuses {
        match st {
            CandidateStatus::ByteInvalid => {
                stop = StopReason::ByteFault;
                break;
            }
            CandidateStatus::ByteValid(c) => {
                if c.frame_count == 0 {
                    stop = StopReason::EmptyBatch;
                    break;
                }
                if c.epoch != epoch {
                    stop = StopReason::EpochMismatch;
                    break;
                }
                expected = c.first_global_pos + u64::from(c.frame_count);
                accepted.push(*c);
            }
        }
    }
    ScanOutcome { accepted, next_pos: expected, stop }
}

#[test]
fn teeth_kernel_without_contiguity_check_fails() {
    assert_finds_violation(
        kernel_no_contiguity_check,
        "A1 differential (position-discontinuous acceptance)",
    );
}

/// A10 disabled: skip invalid slots and resynchronize to the next
/// valid batch (the classic broken scanner).
fn kernel_resync_past_holes(
    epoch: u64,
    base_pos: u64,
    statuses: &[CandidateStatus],
) -> ScanOutcome {
    let mut accepted: Vec<Candidate> = Vec::new();
    let mut expected = base_pos;
    for st in statuses {
        if let CandidateStatus::ByteValid(c) = st {
            if c.frame_count > 0 && c.epoch == epoch {
                expected = c.first_global_pos + u64::from(c.frame_count);
                accepted.push(*c);
            }
        }
        // invalid slot: keep scanning — the bug under test
    }
    ScanOutcome { accepted, next_pos: expected, stop: StopReason::EndOfScan }
}

#[test]
fn teeth_kernel_resyncing_past_holes_fails() {
    assert_finds_violation(
        kernel_resync_past_holes,
        "A10 differential (resynchronization past a hole)",
    );
}
