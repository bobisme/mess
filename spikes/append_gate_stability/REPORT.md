# Distinct-stream append-gate stabilization (bn-2rk)

## Diagnosis

The former
`engine_append_gate::distinct_streams_overlap_under_durable_commit_path`
gate compared two wall-clock intervals and required the concurrent interval to
be at least 2x faster. Pre-existing runs on the same source reversed between
0.59x, 1.32x, 1.81x, and 1.94x as host load changed. That ratio mixed device
and scheduler latency into a property the engine exposes through exact
committer counters.

The first replacement put 16 tasks behind a Tokio start barrier and asserted
that their hot appends formed no more than eight commit groups. It removed the
elapsed-time gate and was empirically stable, but review found that the barrier
was still before `append_batch`: it did not guarantee those tasks reached the
owner's in-flight/channel/gather boundary before the owner closed a group.
Consequently, neither `groups <= K/2` nor even `groups < batches` was a
deterministic invariant of that test setup.

## Deterministic gate

The final regression lives in the engine's private unit-test module. A
`cfg(test)`-only one-shot cohort rendezvous spans the exact owner admission
boundary:

1. each producer sends its intent into the bounded owner channel;
2. while the producer's in-flight guard is still held, the test gate records
   that admission;
3. after receiving the first intent, the owner parks before `gather`;
4. when all 16 sends are admitted, the owner gathers the one received intent
   plus the 15 channel-resident intents as one eligible cohort.

The owner channel has 1,024 slots, so this 16-intent cohort cannot fill it
while the owner is parked. A generation-scoped RAII guard disarms the gate and
notifies the owner on panic or timeout; the gate is one-shot on success. Its
types, fields, wiring, and calls are all conditional on `cfg(test)`, leaving
no release API, state, or hot-path branch.

The exact structural assertions are:

- 16 fully awaited hot appends produce `16/16/16`
  batches/groups/fsyncs;
- the admitted 16-intent distinct-stream cohort produces `16/1/1`; and
- every stream reaches version 1, proving the measured appends landed.

Those assertions prove the engine can integrate an owner-visible cohort into
one direct commit and covering barrier. They do not promise a production
group-count ratio for arbitrary task scheduling. Production coalescing quality
is empirical; the same counters measure it exactly.

## Pre-review empirical repetitions

Base: `24814831`. The original command was run in an independent process for
each normal repetition:

```text
cargo test -p mess-store --test engine_append_gate \
  distinct_streams_overlap_under_durable_commit_path -- --exact --nocapture
```

| environment | run | serial batches/groups/fsyncs | concurrent batches/groups/fsyncs |
|---|---:|---:|---:|
| normal | 1 | 16/16/16 | 16/1/1 |
| normal | 2 | 16/16/16 | 16/1/1 |
| normal | 3 | 16/16/16 | 16/1/1 |
| normal | 4 | 16/16/16 | 16/1/1 |
| normal | 5 | 16/16/16 | 16/1/1 |
| normal | 6 | 16/16/16 | 16/1/1 |
| normal | 7 | 16/16/16 | 16/1/1 |
| normal | 8 | 16/16/16 | 16/1/1 |
| normal | 9 | 16/16/16 | 16/1/1 |
| normal | 10 | 16/16/16 | 16/1/1 |
| CPU-0 contention | 1 | 16/16/16 | 16/1/1 |
| CPU-0 contention | 2 | 16/16/16 | 16/1/1 |
| CPU-0 contention | 3 | 16/16/16 | 16/1/1 |
| CPU-0 contention | 4 | 16/16/16 | 16/1/1 |
| CPU-0 contention | 5 | 16/16/16 | 16/1/1 |

These 15 passes are retained as empirical quality evidence for that host and
source revision. They are not the structural proof above.

## Reproducible CPU-0 contention command

Run from the repository/workspace root. The EXIT trap kills and reaps every
load process on success, test failure, interrupt, or termination. It then uses
`kill -0` on each recorded PID and fails if any remains.

```bash
set -euo pipefail

pids=()

cleanup() {
  status=$?
  trap - EXIT
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      echo "cleanup failed: PID $pid remains" >&2
      status=1
    fi
  done
  if (( status == 0 )); then
    echo "cleanup verified: no contention PID remains"
  fi
  exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for _ in 1 2 3 4; do
  taskset -c 0 sh -c 'while :; do :; done' &
  pids+=("$!")
done

for run in 1 2 3 4 5; do
  echo "contention run $run"
  taskset -c 0 cargo test -p mess-store --lib \
    engine::append_gate_tests::distinct_streams_overlap_under_durable_commit_path \
    -- --exact --nocapture
done
```

## Post-review validation

The deterministic cohort regression passed three independent debug runs and
three independent release runs. Every run reported exactly:

```text
serial batches/groups/fsyncs=16/16/16, cohort=16/1/1
```

The explicit incomplete-generation regression passed in debug and release: it
waited until one of two required intents was admitted, dropped the RAII guard,
then observed both the admitted append and owner-thread shutdown complete. The
32-task same-stream Exact race passed with its new pre-submit rendezvous.

- `cargo check --workspace --all-targets`: pass.
- `cargo check -p mess-store --release`: pass. This compiles the non-test
  engine with every cohort type, field, wiring expression, and call excluded
  by `cfg(test)`, proving the release append path has no hook API/state/branch.
- `just fmt-check`: pass.
- `git diff --check`: pass.
