# Formal model: commit/recovery protocol (bn-13s)

Status: **clean bill within bounds** for the production acceptance
kernel, with one spec observation (Z1 below). Checked 2026-07-08.

## What exists

- `mess_log/src/acceptance.rs` — the **production batch-acceptance
  kernel**: a pure fold making every accept/stop decision of recovery
  (D1 commit authority) over byte-validated candidates. Rules owned by
  the kernel: A5 (no empty batches), A9 (segment epoch), A1 (position
  contiguity), A10 (the first stop is terminal — resynchronization is
  impossible by construction, the state latches). Byte-level rules
  (A2/A3/A4/A11/A12) belong to the scanner's decoder and were proven
  empirically by `spikes/torn_write` (24k randomized sector-reordering
  crashes).
- `mess_log/src/model.rs` — an abstract single-segment protocol model:
  writer appends, fsync barriers, sector-reordering crashes (each
  un-fsynced header/body/marker part independently kept, persisted, or
  torn), segment recycling without zeroing (A9's precondition), and
  recovery whose guard is a pluggable kernel. Stateright-free on
  purpose: it is intended as the shared in-memory reference for the DST
  harness (bn-3kn).
- `mess_log/tests/stateright.rs` — the exhaustive check, run as a
  normal cargo test.

## The key move

The model's recovery transition and all four safety properties call the
**production** `acceptance::accepted_prefix` — not a transcription of
it. The checker exhaustively verifies the function the real scanner
will execute. This only transfers to production if the scanner (bn-39n)
routes every accept/stop decision through `AcceptState::step` /
`accepted_prefix`; that contract is stated in the kernel's module docs.

## Abstractions (and their justification)

- **Perfect CRC (A4):** a slot byte-validates iff header/body/marker
  regions hold the same complete write's bytes. That the real CRC
  delivers this against holes, tears, stale bytes, and straddled
  headers is exactly what `spikes/torn_write` established; the model
  layers protocol logic above that result rather than re-proving it.
- **Slot granularity:** one batch = one slot of three independently
  persisted parts — the minimal geometry in which marker-before-frames
  reordering exists. Byte geometry (alignment, straddling, A11) is the
  harness's domain.
- **fsync is a true barrier** — same fault model as the harness.

## The theorem

Within bounds (4 slots, ≤5 appends, ≤2 in-flight batches, ≤2 crash
cycles, ≤1 recycle, frame counts {1,2}; **1,831,742 unique states**,
exhaustive BFS, ~2 s), in every reachable crashed state:

1. **acked is a recovered prefix** — every durability-acknowledged
   batch is accepted, in commit order, byte-identical, at the front of
   history.
2. **accepted only authentic writes** — nothing partial, torn, stale,
   or fabricated is ever accepted; extras beyond acked are only
   complete current-epoch writes (A6's permitted duplicate-side
   outcome).
3. **accepted history is contiguous** — positions tile from the segment
   base, current epoch only, no empty batches.
4. **recovery is idempotent** — the scan is pure over the durable
   image; crashing during/after recovery reproduces it.

Coverage ("sometimes") properties prove the space contains the
adversarial shapes: resync bait past a stop (A10), intact
stale-generation batches including at the coincident base position
(A9's killer), header+marker-persisted-body-hole (A4's shape), unacked
batches surfacing, and post-recovery rewrites over dead space. If a
bounds change makes any shape unreachable, the test fails.

**Teeth:** three differential tests wire deliberately broken kernels
(A9 dropped, A1 dropped, resync-past-holes) into the same model and
require the checker to produce a counterexample. All three are caught.
This is the model-level analogue of torn_write's Full/Weak
differential, and it guards against the theorem passing vacuously.

## Findings

- **Clean bill:** no counterexample against the production kernel
  within bounds. No spec change to A1–A12 required.
- **Z1 (observation, spec wording): dead space can legally resurface.**
  Found by walking the model (deterministic replay:
  `model::tests::a10_dead_bait_can_legally_resurface_after_rewrite`).
  Sequence: recovery stops at a hole with a fully-persisted unacked
  batch B beyond it (resync bait, A10 keeps it dead); the writer
  resumes at the safe offset and rewrites the hole slot with the same
  frame count; a second crash persists none of the rewrite. B now
  byte-validates at exactly the expected position and epoch and **is
  accepted**. This is spec-legal — B was never acked, its bytes are
  authentic current-epoch writes, and surfacing an unacked complete
  batch is A6's permitted outcome — but "everything at/after the stop
  offset is dead space" (A10) is true only *until the positions line up
  again*. Consequences:
  - bn-21c (spec doc): state explicitly that dead space is dead by
    *acceptance rules*, not by erasure, and that discarded unacked
    batches may resurface across a later crash exactly like other A6
    duplicates — dedupe (D5's window) is the mechanism that must absorb
    them.
  - bn-39n (scanner) / bn-11m (committer): if resurfacing is ever
    unacceptable, the fix is physically stamping/truncating at the safe
    offset before the first post-recovery ack — a policy choice, not a
    correctness requirement under the current invariants.
- **A5 note:** the model's writer never produces empty batches, so the
  kernel's A5 branch is exercised only by unit tests, not the checker.

## Non-goals / deferred

- **TLA+/Apalache independence pass** (toolchain pinned in `mise.toml`,
  `mise run fetch-formal`): deferred per the bone — warranted if the
  model had found something subtle (it did not) or before publishing
  crash-safety claims externally.
- Multi-segment scans (A7/A8 make segments independent; per-segment
  base positions seed A1), byte-level geometry (harness), Lean proofs
  (demoted in the bone).

## Running it

```sh
cargo test -p mess_log                  # kernel units + model + checker
cargo test -p mess_log --test stateright -- --nocapture  # state counts
```

Bounds live in `mess_log/tests/stateright.rs` (`BOUNDS`). Cost scaling
for the curious: appends=6 explores 11.4M states (~11 s, ~9.5 GiB RSS)
with no new findings; the committed bounds keep CI at ~2 s / ~1.8 GiB.
