# Verification: Kani model-checking proofs

Status: **informational** (process documentation, not a format/protocol
spec — those live in `docs/spec/`).

This document tracks where [Kani](https://github.com/model-checking/kani)
(bounded model checking over CBMC) is used in `mess`, what each proof
establishes, how to run it, and what is deliberately left unproved and why.
Kani proves a property for *every* input up to the stated bounds — the right
tool for small, pure, branch-heavy kernels where example-based tests give
false comfort about the input space they didn't happen to sample. It is the
wrong tool for anything with I/O, async, real time, or (as discovered below)
hardware-intrinsic-dispatching dependencies.

Current coverage is `crates/mess-log` (bn-y0b, Phase 3) and
`crates/mess-index`'s `sealed::ptr_block` (bn-20e's packed pointer block
encode/decode, restructured by bn-fyo after the original monolithic harness
blew up cbmc's SAT solver — see that module's `kani_proofs` doc comment for
the harness-by-harness coverage story and `docs/verification.md`'s "Running
the proofs" section below for the per-crate invocation).

## bn-y0b acceptance-criteria status (do not close as fully done)

bn-y0b's acceptance criteria list three checkboxes. As of this commit:

- **Acceptance predicate proof** (no input both passes validation and
  violates the contiguity/empty-batch rules) — **met**. See
  `step_accept_implies_contiguous_and_nonempty` /
  `step_rejects_every_a1_a5_a9_violation` below.
- **No-overflow proofs: position arithmetic, watermark advance** —
  **met**. Position arithmetic is proved
  (`step_position_advance_no_overflow_bounded`). Watermark-advance is now
  proved too (`watermark::kani_proofs`, below): monotonicity
  (`advance_never_decreases_value`, folded across a sequence in
  `advance_three_step_fold_is_monotone`), the exact `max(initial, to)`
  semantics that stands in for a no-overflow claim
  (`advance_sets_value_to_max_no_overflow` — `advance` does no arithmetic,
  see that harness's doc comment for why), and the await-past predicate
  `WaitFor::poll` evaluates (`advance_establishes_await_past_threshold`).
  bn-11m landed the watermark module on trunk since this bone's first
  commit, unblocking this half of the criterion.
- **Round-trip proofs: ptr delta encode/decode, cursor encode/decode** —
  **not met (0%), explicitly out of scope for this follow-up**. The
  `mess-index` kernels these would verify don't exist yet; that crate is
  Phase 4 work tracked as bn-25d, which is still open. Writing speculative
  kernels here to have something to prove against remains out of scope.

This is a bone-sequencing gap (bn-y0b was scoped/dispatched before its two
prerequisite kernels — bn-25d's mess-index encoders, bn-11m's
committer/watermark code — existed), not an omission in this commit. Two
of the three criteria are now met; the ptr/cursor round-trip proofs stay
split to a bone gated on bn-25d landing (per the prior commit's option 1)
— **this bone still cannot be marked fully done until bn-25d lands and
that follow-on bone completes.**

## Running the proofs

```sh
# Everything in mess-log — 11 harnesses (~2-10s wall clock with a warm
# target/kani build cache; a cold-cache run — e.g. first CI invocation,
# before CBMC/goto compilation artifacts exist — is noticeably slower):
cargo kani --package mess-log

# One harness at a time (useful while iterating — each is independently fast):
cargo kani --package mess-log --harness step_accept_implies_contiguous_and_nonempty
cargo kani --package mess-log --harness advance_never_decreases_value

# mess-index — one harness (varint_round_trips, ~2s). See
# sealed::ptr_block::kani_proofs's module doc for why the round-trip
# proofs a naive reading of bn-y0b's criteria might expect are covered by
# randomized tests in that module's `mod tests` instead, not by Kani.
cargo kani --package mess-index
```

`cargo kani` requires `cargo-kani` on `PATH` (installed via `cargo install
kani-verifier && cargo kani setup`; already installed on the standard dev
box — see the global `CUDA`/Python notes elsewhere for this machine's other
toolchain quirks, unrelated to Kani). Harnesses are ordinary `#[cfg(kani)]`
modules appended at the end of the file they verify; they do not exist for
`cargo build`/`cargo test`, and `cargo test -p mess-log` / `cargo clippy -p
mess-log --all-targets -- -D warnings` are unaffected (`kani` is a
non-Cargo-feature `cfg` registered in each crate's `[lints.rust]
unexpected_cfgs.check-cfg` so it doesn't trip `-D warnings` under normal
rustc).

## What is proved today

### `acceptance.rs` — the commit-authority kernel (A1/A5/A9/A10)

Five harnesses in `acceptance::kani_proofs`, all sub-second:

- `step_accept_implies_contiguous_and_nonempty` / `step_rejects_every_a1_a5_a9_violation` —
  the two directions of one invariant: `AcceptState::step` accepts a
  candidate if and only if it has nonzero `frame_count` (A5), an `epoch`
  matching the scan's `expected_epoch` (A9), and a `first_global_pos`
  matching `expected_pos` (A1). Exhaustive over the full `u64`/`u32` domain
  of a single step (bar `expected_pos`, see below) — not sampled from the
  example shapes `cargo test`'s unit tests pull from `spikes/torn_write`.
- `a10_stop_is_terminal_for_any_bait` — once `stopped` is latched, every
  later `step`, for *any* candidate (including a byte-valid,
  position-and-epoch-contiguous "resync bait" batch), returns the same stop
  and never advances `expected_pos`. Generalizes the
  `a10_stop_is_terminal_even_for_valid_bait` unit test from one example bait
  to every possible bait.
- `step_position_advance_no_overflow_bounded` — `expected_pos += frame_count`
  on an accepted step does not overflow `u64`, for `expected_pos <=
  POSITION_BOUND` (`2^40`, ~1.1e12 events — at a sustained 1M events/sec
  that's ~35 years of continuous writes) and `frame_count` at its true type
  maximum (`u32::MAX`). The two contiguity harnesses above reuse the same
  `POSITION_BOUND` on `expected_pos` so their (unrelated) claim isn't
  entangled with this overflow claim — Kani found the entanglement
  immediately when the bound was missing (see "bounds that mattered" below).
- `three_step_fold_is_contiguous_and_terminal` — the whole-scan shape,
  folded by hand over three candidates (`AcceptState::step` called
  directly rather than through `accepted_prefix`, which allocates a `Vec`
  Kani doesn't need to reason about here): every accepted candidate is
  nonzero-length, epoch-matched, and position-contiguous with the one
  before it, and nothing is ever accepted after a stop. Three steps is the
  smallest bound that exercises "accept, then stop, then bait" in one run.

### `encode.rs` — `total_len` arithmetic (§4.6)

Two harnesses in `encode::kani_proofs`:

- `total_len_closed_form_never_overflows_at_realistic_bounds` — reasons
  about the accumulation abstractly (`frame_count * max_per_frame_len +
  HEADER_LEN + chain_len + MARKER_LEN`) for `frame_count` and per-subframe
  on-disk length both bounded by `MAX_BATCH_LEN` (64 MiB, the spec's own A2
  cap — no subframe can exceed a whole batch's cap, and no batch can hold
  more subframes than `MAX_BATCH_LEN / SUBFRAME_HDR_LEN` ≈ 2.4M, two orders
  of magnitude below this bound already). See "a real edge case" below for
  why the bound is `MAX_BATCH_LEN` and not the raw type-level maximum.
- `total_len_matches_closed_form_bounded` — drives the actual
  `BatchEncoder::total_len` (not an abstraction of it) over every batch
  shape up to 2 subframes of up to 4 bytes each, with or without a crypto
  chain: never panics, and whenever it returns `Ok`, the value equals the
  exact closed-form sum. Small bound chosen so Kani enumerates the real
  code path exactly rather than approximating it.

### `watermark.rs` — the position-ordered durable watermark (D7)

Four harnesses in `watermark::kani_proofs`, all sub-second, all driving the
real `Watermark::new` / `advance` / `get` (not a reimplementation of the
comparison inside `advance`'s body) over the full `u64` domain of both the
starting value and the target — no bound needed, since each call is O(1)
comparison-and-assign work:

- `advance_never_decreases_value` — for any `initial` and any `to`, the
  value after one `advance(to)` call is never less than `initial`. Proves
  the monotonicity the module doc claims ("a reader that has observed
  `value >= p` never later observes it regress while the store is live").
- `advance_sets_value_to_max_no_overflow` — the exact semantics, not just
  the inequality above: `advance(to)` sets the value to
  `max(initial, to)`. This stands in for a no-overflow proof:
  `advance` does no arithmetic at all (unlike `AcceptState::step`'s
  `expected_pos += frame_count`, it never adds to the current value — it
  only compares, then conditionally assigns the caller's already-computed
  `to`), so there is no `checked_add`/bound to prove; this harness pins
  that fact against the real code so a future refactor that turns
  `advance` into an accumulating delta (which *would* need an overflow
  proof) trips this proof first.
- `advance_establishes_await_past_threshold` — the await-past semantics
  predicate: `WaitFor::poll` resolves a waiter with `threshold` the
  instant `st.value >= threshold` (watermark.rs, `poll`'s body). This
  proves `advance`'s postcondition against that *exact* `>=` predicate
  (not a restated inequality): after `advance(to)` returns, `get() >= to`
  always holds, so a `wait_for(to)` polled immediately afterward is
  guaranteed `Ready` — whether this call moved the value or was a no-op
  because an earlier call already covered `to`.
- `advance_three_step_fold_is_monotone` — the whole-scan-shaped
  counterpart (mirrors `acceptance.rs`'s `three_step_fold_...` convention
  of folding a few steps by hand rather than looping over a `Vec` Kani
  doesn't need to reason about): three arbitrary targets applied in
  sequence, including targets smaller than the running value (exercising
  the no-op path repeatedly) — the value never decreases at any
  observation point, and after each step the value has cleared every
  target offered so far, not just the most recent one.

`WaitFor::poll` itself is not harnessed directly: it drives a `Waker`, and
`kani::any::<Waker>()` has no meaningful arbitrary instantiation (a
`Waker` wraps an unsafe hand-rolled vtable) — the same "wrong tool" call
made below for `crc32c`'s inline-asm path. The predicate `poll` evaluates
is proved instead, against `advance`'s postcondition, as described above.

## A real edge case Kani found (and why it's not fixed)

The first version of the closed-form `total_len` proof used the raw
type-level bounds enforced by the checks that run *before* the summation
(`TooManyFrames`: `frame_count <= u32::MAX`; `PayloadTooLarge`: per-subframe
on-disk length `<= SUBFRAME_HDR_LEN + u32::MAX`). Kani found a genuine
counterexample: at those exact extremes, `frame_count * max_per_frame_len`
overflows `u64` by exactly `111_669_149_670` (~1.117e11)
(`u32::MAX as u128 * (SUBFRAME_HDR_LEN + u32::MAX) as u128 - u64::MAX as u128
== 111_669_149_670`, confirmed by direct calculation in `u128`).
`BatchEncoder::total_len` accumulates `frames_len`
with a plain `+=`, not `checked_add`, so this is a real latent overflow (a
panic under Kani's/debug's overflow checks; a silent wraparound in release,
which could in principle let an oversized batch slip past the `total_len >
MAX_BATCH_LEN` cap that runs immediately after).

It is not fixed here because it is not reachable: that configuration needs a
`subframes` slice of ~4.3 billion entries each carrying a ~4 GiB payload —
on the order of 18 exabytes of live payload data passed into one `encode()`
call. Rust's own allocator/slice invariants (any single allocation is capped
at `isize::MAX` bytes) already make this uninhabitable on real hardware, well
before `u64` arithmetic enters into it. The closed-form proof was narrowed to
`MAX_BATCH_LEN`-scale bounds (documented on `MAX_REALISTIC_LEN` in
`encode.rs`) — the largest inputs any real call site could construct — and
now passes. The unreachable extreme is recorded here rather than silently
dropped; if `total_len`'s accumulation is ever changed to `checked_add`
(closing the gap for real, at zero realistic cost), this note and the
narrowed bound can be revisited.

## What is deliberately not Kani-checked, and why

- **`BatchEncoder::encode` (and anything that calls `crate::crc::batch_crc`)** —
  confirmed experimentally, not just by inspection: a minimal one-subframe
  `encode()` harness fails after ~100s with `TerminatorKind::InlineAsm is not
  currently supported by Kani`, from the `crc32c` crate's
  runtime-feature-detected SSE4.2 path (`__cpuid_count` plus hand-written
  intrinsics; see the `crc32c` dependency's `hw_x86_64.rs`). Kani compiles to
  CBMC's goto-program IR, not real machine code, and does not model raw CPU
  intrinsics. The encode/verify round trip this would have proved (`encode()`
  writes `batch_crc` matching an independent `batch_crc()` recomputation) is
  the kind of claim Kani is the wrong tool for here; it stays covered by
  `crc.rs`'s and `encode.rs`'s existing unit tests, which exercise the real
  hardware path the way it actually runs in production.
- **`SegmentWriter::remaining` (`segment_size.saturating_sub(write_off)`,
  writer.rs)** — the "segment-remaining arithmetic" the parent bone names.
  `saturating_sub` cannot overflow or panic by construction (it saturates at
  zero instead), so there is no overflow property to prove; noted here
  rather than proved. `writer.rs` is also outside this bone's scope (owned
  by a parallel worker on the same crate).
- **ptr-delta / cursor / skip-table round-trips** — named in the parent bone
  as a proof target, but the kernels they'd verify (`mess-index`'s encoding)
  do not exist yet; that crate is Phase 4 (bn-25d), still open. Recorded
  here as planned, blocked on bn-25d landing the kernel — writing
  speculative kernels to have something to prove against is out of scope
  for this bone and its follow-up.
- **A full segment-header encode/decode round trip**
  (`writer::encode_segment_header` / `writer::read_segment_header_epoch`) —
  a genuine pure round-trip pair exists today, but it lives in `writer.rs`,
  which this bone does not own. Left as a good target for whichever bone
  next extends Kani coverage there.

## Adding a new harness

- Append `#[cfg(kani)] mod kani_proofs { ... }` at the end of the file being
  verified (matches the append-only convention every kernel-owning bone in
  this crate follows).
- Keep functions Kani-friendly: no I/O, no unbounded loops over
  runtime-length data (use small fixed-size arrays with a symbolic,
  `kani::assume`-bounded length instead, as `encode.rs`'s harnesses do).
- Document *why* a bound is what it is, not just its value — see
  `MAX_REALISTIC_LEN`'s and `POSITION_BOUND`'s doc comments for the pattern.
  A bound justified by a spec constant (`MAX_BATCH_LEN`) or a physical
  argument (RAM, elapsed time) is far more useful to a future reader than
  a round number picked to make the proof pass.
- Run the new harness alone first (`--harness <name>`) while iterating; add
  it to the "what is proved today" list above once it's green.
