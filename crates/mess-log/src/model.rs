//! Abstract single-segment commit/recovery protocol model (bn-13s).
//!
//! A plain, deterministic state machine over the D1/D2 protocol:
//! writer appends, group-commit fsync barriers, sector-reordering
//! crashes, segment recycling (A9's precondition), and recovery whose
//! accept/stop decisions are made by a pluggable [`Kernel`] — in the
//! checker, the PRODUCTION [`acceptance`] kernel.
//!
//! `tests/stateright.rs` wraps this in stateright and
//! exhaustively explores every interleaving within bounds. The types
//! here are deliberately stateright-free so the DST harness (bn-3kn)
//! can drive the same model as its in-memory reference.
//!
//! # Abstractions (and why they are sound)
//!
//! - **Perfect CRC (A4).** A slot byte-validates iff its header, body, and
//!   marker regions all hold bytes of the SAME complete write. That the real
//!   CRC actually delivers this — against torn sectors, holes, stale bytes,
//!   straddled headers (A2/A3/A11/A12) — is exactly what `spikes/torn_write`
//!   proved empirically over 24k randomized crash images. The model layers
//!   protocol logic above that result.
//! - **Slot granularity.** Every batch occupies one slot of three
//!   independently-persisted parts (header/body/marker) — the minimal geometry
//!   in which marker-before-frames reordering (A4's killer) exists. Byte
//!   geometry (alignment, straddling) is the harness's domain, not the model's.
//! - **fsync is a true barrier**, matching the harness fault model (no
//!   volatile-cache lies).
//! - **Content is identity.** A write's bytes are represented by its [`Spec`];
//!   `uniq` distinguishes writes whose header fields collide (e.g. a
//!   post-recovery rewrite of a dead slot), so the oracle can tell WHICH
//!   write's bytes were accepted even when the kernel-visible candidate fields
//!   are identical.

use crate::acceptance::{Candidate, CandidateStatus, ScanOutcome};

/// The decision function under test. Production wires in
/// [`crate::acceptance::accepted_prefix`]; the differential ("teeth")
/// tests wire in deliberately weakened kernels and demand the checker
/// find counterexamples.
pub type Kernel = fn(u64, u64, &[CandidateStatus]) -> ScanOutcome;

/// The production kernel, adapted to the [`Kernel`] signature.
pub fn production_kernel(
    epoch: u64,
    base_pos: u64,
    statuses: &[CandidateStatus],
) -> ScanOutcome {
    crate::acceptance::accepted_prefix(
        epoch,
        base_pos,
        statuses.iter().copied(),
    )
}

/// Identity of one write: the kernel-visible candidate fields plus a
/// nonce distinguishing distinct writes with colliding fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Spec {
    pub cand: Candidate,
    pub uniq: u32,
}

/// Durable content of one third of a slot (header/body/marker region).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Part {
    /// Background: never written, or a torn write (mixed generations of
    /// bytes match no complete write — the perfect-CRC abstraction).
    Garbage,
    /// This region holds `Spec`'s bytes for that region.
    Of(Spec),
}

pub const PARTS_PER_SLOT: usize = 3;

/// One batch slot: three independently-persisted parts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Slot {
    pub parts: [Part; PARTS_PER_SLOT], // [header, body, marker]
}

impl Slot {
    pub const GARBAGE: Slot = Slot { parts: [Part::Garbage; PARTS_PER_SLOT] };

    pub fn of(spec: Spec) -> Slot { Slot { parts: [Part::Of(spec); 3] } }

    pub fn header(&self) -> Part { self.parts[0] }

    /// The byte layer's verdict on this slot, plus the underlying write
    /// when the slot is intact (for the oracle — the kernel never sees
    /// `uniq`).
    ///
    /// Perfect-CRC abstraction: byte-valid iff all three regions hold
    /// the same write's bytes. A slot whose header decodes but whose
    /// body/marker mismatch is a CRC/marker fault (the A4 shape).
    pub fn decode(&self) -> (CandidateStatus, Option<Spec>) {
        match self.header() {
            Part::Garbage => (CandidateStatus::ByteInvalid, None),
            Part::Of(spec) => {
                if self.parts[1] == Part::Of(spec)
                    && self.parts[2] == Part::Of(spec)
                {
                    (CandidateStatus::ByteValid(spec.cand), Some(spec))
                } else {
                    (CandidateStatus::ByteInvalid, None)
                }
            }
        }
    }
}

/// What happens to one pending part at a crash: real disks persist
/// un-fsynced sectors in arbitrary order, or tear them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PartFate {
    /// The write did not reach the platter; prior durable bytes remain.
    Keep,
    /// The write persisted in full.
    Persist,
    /// The write persisted partially: a mix of new and prior bytes,
    /// which matches no complete write (perfect-CRC abstraction).
    Torn,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    /// Encode + write one batch of `frames` events at the write head.
    /// Not durable until fsync.
    Append { frames: u32 },
    /// Group-commit barrier: all pending parts become durable; every
    /// in-flight batch is durability-acknowledged.
    Fsync,
    /// Crash: one fate per pending part, in `State::pending` order.
    Crash { fates: Vec<PartFate> },
    /// Run recovery: decode the durable image, ask the kernel for the
    /// accepted prefix, resume the writer at the safe slot.
    Recover,
    /// Retention freed this segment; reuse the FILE for a new
    /// generation WITHOUT zeroing (real filesystems don't) and — the
    /// A9 adversarial choice — for the SAME base position range.
    /// The epoch stamp is the only thing distinguishing generations.
    Recycle,
}

/// Model bounds: the finitization knobs. Exhaustiveness is within these
/// bounds only; see the spec note in docs/spec/ for the argument that
/// they cover the protocol's interesting shapes.
#[derive(Debug, Clone, Copy)]
pub struct Bounds {
    /// Batch slots per segment.
    pub slots:         usize,
    /// Total appends across the whole trace (all epochs).
    pub max_appends:   u32,
    /// Max un-fsynced batches (bounds crash branching: 3 parts each).
    pub max_inflight:  usize,
    /// Crash/recover cycles.
    pub max_crashes:   u32,
    /// Segment recycles (epoch bumps).
    pub max_recycles:  u32,
    /// Per-append frame-count choices (variety gives A1 bite).
    pub frame_choices: &'static [u32],
}

/// The model: bounds + the decision kernel under test.
#[derive(Debug, Clone, Copy)]
pub struct ProtocolModel {
    pub bounds: Bounds,
    pub kernel: Kernel,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct State {
    pub epoch:   u64,
    /// What a post-crash read of the file returns.
    pub durable: Vec<Slot>,
    /// The file as the writer believes it (durable + pending writes).
    pub shadow:  Vec<Slot>,
    /// Un-fsynced (slot, part) writes, in write order.
    pub pending: Vec<(usize, usize)>,
    pub crashed: bool,

    pub write_head:    usize,
    pub next_batch_id: u64,
    pub next_pos:      u64,
    pub next_uniq:     u32,

    /// Oracle: durability-acknowledged batches, current epoch, in
    /// commit order. THE correctness reference — an ack is a promise.
    pub acked:    Vec<Spec>,
    /// Written but not yet acked (fsync pending).
    pub inflight: Vec<Spec>,
    /// Every write of the current epoch, including ones dead/overwritten
    /// after a recovery truncation (surfacing an unacked complete batch
    /// is the A6-permitted duplicate-side outcome).
    pub written:  Vec<Spec>,

    pub appends_used:  u32,
    pub crashes_used:  u32,
    pub recycles_used: u32,
}

impl ProtocolModel {
    pub fn init(&self) -> State {
        State {
            epoch:         0,
            durable:       vec![Slot::GARBAGE; self.bounds.slots],
            shadow:        vec![Slot::GARBAGE; self.bounds.slots],
            pending:       Vec::new(),
            crashed:       false,
            write_head:    0,
            next_batch_id: 0,
            next_pos:      0,
            next_uniq:     0,
            acked:         Vec::new(),
            inflight:      Vec::new(),
            written:       Vec::new(),
            appends_used:  0,
            crashes_used:  0,
            recycles_used: 0,
        }
    }

    /// Byte-layer verdicts for the durable image, in on-disk order,
    /// plus each slot's underlying write for the oracle.
    pub fn decode_durable(
        s: &State,
    ) -> (Vec<CandidateStatus>, Vec<Option<Spec>>) {
        s.durable.iter().map(Slot::decode).unzip()
    }

    /// Run the kernel over the durable image, exactly as recovery does.
    pub fn scan(&self, s: &State) -> ScanOutcome {
        let (statuses, _) = Self::decode_durable(s);
        // A7: scan from the segment start; the recycled segment reuses
        // base position 0 on purpose (the A9 adversarial choice).
        (self.kernel)(s.epoch, 0, &statuses)
    }

    pub fn actions(&self, s: &State, out: &mut Vec<Action>) {
        let b = &self.bounds;
        if s.crashed {
            out.push(Action::Recover);
            return;
        }
        if s.appends_used < b.max_appends
            && s.write_head < b.slots
            && s.inflight.len() < b.max_inflight
        {
            for &frames in b.frame_choices {
                out.push(Action::Append { frames });
            }
        }
        if !s.pending.is_empty() {
            out.push(Action::Fsync);
        }
        if s.crashes_used < b.max_crashes {
            self.push_crash_actions(s, out);
        }
        if s.recycles_used < b.max_recycles && s.pending.is_empty() {
            out.push(Action::Recycle);
        }
    }

    /// Every combination of per-part fates — the exhaustive analogue of
    /// the harness's randomized "each pending sector independently
    /// persisted or not, one optionally torn".
    fn push_crash_actions(&self, s: &State, out: &mut Vec<Action>) {
        // `Torn` leaves Garbage; when the prior durable part is already
        // Garbage that is indistinguishable from `Keep`, so skip the
        // redundant branch.
        let fates_for = |&(slot, part): &(usize, usize)| {
            if s.durable[slot].parts[part] == Part::Garbage {
                &[PartFate::Keep, PartFate::Persist][..]
            } else {
                &[PartFate::Keep, PartFate::Persist, PartFate::Torn][..]
            }
        };
        let mut combos: Vec<Vec<PartFate>> = vec![Vec::new()];
        for p in &s.pending {
            let mut next =
                Vec::with_capacity(combos.len() * fates_for(p).len());
            for combo in &combos {
                for &fate in fates_for(p) {
                    let mut c = combo.clone();
                    c.push(fate);
                    next.push(c);
                }
            }
            combos = next;
        }
        out.extend(combos.into_iter().map(|fates| Action::Crash { fates }));
    }

    pub fn apply(&self, s: &State, action: &Action) -> Option<State> {
        let b = &self.bounds;
        match action {
            Action::Append { frames } => {
                if s.crashed
                    || s.appends_used >= b.max_appends
                    || s.write_head >= b.slots
                    || s.inflight.len() >= b.max_inflight
                {
                    return None;
                }
                let mut n = s.clone();
                let spec = Spec {
                    cand: Candidate {
                        epoch:            n.epoch,
                        batch_id:         n.next_batch_id,
                        first_global_pos: n.next_pos,
                        frame_count:      *frames,
                    },
                    uniq: n.next_uniq,
                };
                n.shadow[n.write_head] = Slot::of(spec);
                for part in 0..PARTS_PER_SLOT {
                    n.pending.push((n.write_head, part));
                }
                n.write_head += 1;
                n.next_batch_id += 1;
                n.next_pos += u64::from(*frames);
                n.next_uniq += 1;
                n.appends_used += 1;
                n.inflight.push(spec);
                n.written.push(spec);
                Some(n)
            }
            Action::Fsync => {
                if s.crashed || s.pending.is_empty() {
                    return None;
                }
                let mut n = s.clone();
                n.durable = n.shadow.clone();
                n.pending.clear();
                let acked = std::mem::take(&mut n.inflight);
                n.acked.extend(acked);
                Some(n)
            }
            Action::Crash { fates } => {
                if s.crashed
                    || s.crashes_used >= b.max_crashes
                    || fates.len() != s.pending.len()
                {
                    return None;
                }
                let mut n = s.clone();
                for (&(slot, part), &fate) in s.pending.iter().zip(fates) {
                    n.durable[slot].parts[part] = match fate {
                        PartFate::Keep => n.durable[slot].parts[part],
                        PartFate::Persist => s.shadow[slot].parts[part],
                        PartFate::Torn => Part::Garbage,
                    };
                }
                n.shadow = n.durable.clone();
                n.pending.clear();
                n.inflight.clear();
                n.crashed = true;
                n.crashes_used += 1;
                Some(n)
            }
            Action::Recover => {
                if !s.crashed {
                    return None;
                }
                let outcome = self.scan(s);
                debug_assert_eq!(
                    outcome,
                    self.scan(s),
                    "recovery must be idempotent (pure over the image)"
                );
                let mut n = s.clone();
                n.crashed = false;
                // Everything the kernel accepted is now committed
                // history (including A6-surfaced unacked batches); the
                // writer resumes at the first dead slot, overwriting
                // dead space lazily (A10: it is dead, not history).
                let (_, specs) = Self::decode_durable(s);
                n.acked = outcome
                    .accepted
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        // With the production kernel, accepted batch i
                        // is always slot i's intact write. A broken
                        // kernel (the teeth tests) can accept slots
                        // with no authentic bytes; record the promise
                        // with a sentinel uniq that matches no real
                        // write, so the acked-prefix property flags it
                        // at the next crash instead of panicking here.
                        specs
                            .get(i)
                            .copied()
                            .flatten()
                            .filter(|spec| spec.cand == *c)
                            .unwrap_or(Spec { cand: *c, uniq: u32::MAX })
                    })
                    .collect();
                n.write_head = outcome.accepted.len();
                n.next_pos = outcome.next_pos;
                n.next_batch_id = outcome
                    .accepted
                    .last()
                    .map(|c| c.batch_id + 1)
                    .unwrap_or(0);
                Some(n)
            }
            Action::Recycle => {
                if s.crashed
                    || s.recycles_used >= b.max_recycles
                    || !s.pending.is_empty()
                {
                    return None;
                }
                let mut n = s.clone();
                n.epoch += 1;
                // The file is NOT zeroed: durable/shadow keep the stale
                // generation's bytes. Same base position range (A9).
                n.write_head = 0;
                n.next_batch_id = 0;
                n.next_pos = 0;
                n.acked.clear();
                n.inflight.clear();
                n.written.clear();
                n.recycles_used += 1;
                Some(n)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acceptance::StopReason;

    const BOUNDS: Bounds = Bounds {
        slots:         3,
        max_appends:   4,
        max_inflight:  2,
        max_crashes:   2,
        max_recycles:  1,
        frame_choices: &[1, 2],
    };

    fn model() -> ProtocolModel {
        ProtocolModel { bounds: BOUNDS, kernel: production_kernel }
    }

    fn apply(m: &ProtocolModel, s: &State, a: Action) -> State {
        m.apply(s, &a).expect("action must be enabled")
    }

    /// Deterministic replay of torn_write's A9 headline case: recycled
    /// segment, intact stale batch at the coincident position, zero new
    /// sectors persisted. The production kernel must reject the stale
    /// batch on epoch alone.
    #[test]
    fn a9_recycled_segment_stale_coincident_batch_rejected() {
        let m = model();
        let s = m.init();
        let s = apply(&m, &s, Action::Append { frames: 1 });
        let s = apply(&m, &s, Action::Fsync); // stale gen durable
        let s = apply(&m, &s, Action::Recycle);
        let s = apply(&m, &s, Action::Append { frames: 1 });
        // zero of the new write's parts persist — legal under reordering
        let s = apply(&m, &s, Action::Crash { fates: vec![PartFate::Keep; 3] });

        // the stale batch IS byte-intact at slot 0, position 0
        let (statuses, _) = ProtocolModel::decode_durable(&s);
        assert!(matches!(statuses[0], CandidateStatus::ByteValid(c)
            if c.epoch == 0 && c.first_global_pos == 0));

        let out = m.scan(&s);
        assert!(out.accepted.is_empty(), "stale batch must not resurrect");
        assert_eq!(out.stop, StopReason::EpochMismatch);
    }

    /// The A10 "zombie" shape, found by walking the model: recovery
    /// stops before a fully-persisted unacked batch (resync bait), the
    /// writer resumes and overwrites the hole with the SAME frame
    /// count, and a second crash persists nothing new. The dead bait
    /// batch then byte-validates at exactly the expected position and
    /// epoch, and IS accepted. That is spec-legal — it was never acked,
    /// its content is authentic current-epoch data, and surfacing an
    /// unacked complete batch is A6's permitted duplicate-side outcome
    /// — but it means "dead space" after a stop is only dead until the
    /// positions line up again. Recorded in docs/spec/.
    #[test]
    fn a10_dead_bait_can_legally_resurface_after_rewrite() {
        let m = model();
        let s = m.init();
        // b0 (frames=1) and b1 (frames=1) written, nothing fsynced
        let s = apply(&m, &s, Action::Append { frames: 1 });
        let s = apply(&m, &s, Action::Append { frames: 1 });
        let bait = *s.written.last().unwrap();
        // crash: b1 persists fully, b0 not at all — the bait shape
        let mut fates = vec![PartFate::Keep; 3];
        fates.extend([PartFate::Persist; 3]);
        let s = apply(&m, &s, Action::Crash { fates });
        let s = apply(&m, &s, Action::Recover);
        assert_eq!(s.acked.len(), 0, "scan must stop at the b0 hole");
        assert_eq!(s.write_head, 0);

        // writer rewrites slot 0 with the same frame count and acks it
        let s = apply(&m, &s, Action::Append { frames: 1 });
        let s = apply(&m, &s, Action::Fsync);
        // second crash persists nothing (no pending parts anyway)
        let s = apply(&m, &s, Action::Crash { fates: vec![] });

        let out = m.scan(&s);
        assert_eq!(out.accepted.len(), 2, "rewrite + resurfaced bait");
        let (_, specs) = ProtocolModel::decode_durable(&s);
        assert_eq!(specs[1], Some(bait), "slot 1 is the dead bait batch");
        assert!(s.written.contains(&bait), "authentic current-epoch write");
    }
}
