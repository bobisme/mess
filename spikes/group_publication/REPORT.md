# bn-3vj4 grouped-publication measurement

## Decision status

The frozen candidate is **not admitted**. Attempt 8 completed the full locked
160-row matrix with clean provenance but passed only 3/8 cells. No threshold
was changed and none of the seven earlier partial/zero-row attempts is used as
decision evidence. The implementation remains isolated while the measured
hot-path and tail regressions are diagnosed.

Correctness is independently green through the production
reducer/publication path: one committed direct group warms
caches while unreachable, applies every positive registry/head/index effect in
canonical order under one Book lock, advances one reader frontier, and only
then completes waiters. Deterministic debug and release tests cover an
all-success cohort, registry-only partial success, zero landed outcomes,
independent-unit filtering, cancellation at the publication pause, a
roll-spanning group with reopen, Exact/Os behavior, and poison outcomes.

## Frozen comparison

The performance decision compares two release binaries built before any row:

- A: current production main `c7ba3b9fbc65` plus the measurement-only harness
  at control checkpoint `f712b03d123855f72e37b010cc621fc58709f1f5`.
- B: grouped-publication candidate
  `35fb7585b27a66323d9d46939aaaeff27dc792ce` plus the byte-identical
  measurement-only harness.

The frozen binary SHA-256 values are
`0fe4bf83a71fa875e400704597c93b4e0bf3856fc6af45aff74114b568b7d705`
for A and
`afbb7ab5a64bd8c6f730ee97178ada1b650e44ba1b4fa46aeb590fccf45a61fa`
for B. Both harness sources are
`e063a95a1df126e9e81ec7a336ce177f1d8dab0af66aaaaa086f546273367a21`.

Both execute `EventStore -> FjallSnapshotBackend -> LogEngine`, use a fresh
ext4 store per observation, four writers, 250-byte events, and the matrix
`{Process, Group} x {1, 10, 100, 1000}`. Work per writer is fixed at
40,000/12,500/2,500/250 for Process and 800/500/300/100 for Group. Stream and
type registration are warm before measurement; Group also gets four matched
concurrent warm rounds before counter and allocator snapshots. The source file
for the timed harness must have the same SHA-256 in A and B.

For each cell run five counterbalanced cycles. Odd cycles are `A,B,B,A`; even
cycles are `B,A,A,B`, yielding ten observations per variant/cell. Before each
observation require no foreign compiler/build/benchmark process and load1
below 6. Retain every completed row, including any fixed 50 ms durability
alarm and its complete throughput/p99 penalty. A process/guard failure
fail-stops the study; do not replace partial rows.

Record source and binary SHA-256, tracked dirty state, harness and Cargo.lock
SHA-256, toolchain, kernel, CPU/governor, scratch filesystem/free space,
timestamp/order, pre/post load, throughput, append p50/p99, allocation
calls/bytes, exact input and commit deltas, groups/fsyncs, and degradation
status.

## Locked gates

The evaluator takes the median of each cycle's two B observations divided by
the median of its two A observations, then the median of five cycle ratios.
Every cell must pass; no threshold may move after results:

- throughput B/A at least 97%;
- append p99 B/A at most 110%;
- allocation calls/event and bytes/event B/A each at most 105%;
- Process fsync count exactly zero in every row;
- Group aggregate B fsync count at most 100.25% of A and median cycle-level
  B-minus-A fsync delta no greater than zero;
- exact event/batch totals and borrowed-boundary traffic in every row;
- all harness assertions pass, with every degraded row retained.

The deterministic structural gate remains exact: 16 serial appends produce 16
commit groups/fsyncs/publication transitions, while one owner-visible cohort
produces 16 batches in one group, one fsync, and one publication transition.
A passing matrix authorizes final code review; it does not replace correctness.

## Attempt audit

Attempts 1-7 are retained as invalid operational evidence only. Attempts 1 and
7 produced zero rows because load did not fall below 6 within 120 seconds.
Attempts 2-6 fail-stopped when a foreign build appeared, after respectively
27, 19, 9, 6, and 22 completed rows. Their rows were never combined, replaced,
or used for an admission decision.

Attempt 8 (`attempt8-20260715`) completed all 160 rows. It used the frozen
source/binary/harness hashes above, a performance governor on all 24 logical
CPUs, fresh stores on ext4, and no foreign build or benchmark process. The raw
artifact hashes are:

- `paired.csv`:
  `d2baf6d3f38092c9bd3d4b0ea0c08b15b7af0747db4c5ea8d655d37d68783b2e`
- `run.log`:
  `5b48961b882e188d1af94bda8f576e9415e093adad40318d297c048accd34a66`
- `provenance.txt`:
  `eee290a53598425cd5eb2a72b25c673501450ce508eecffb865f2fb786da4f47`
- `evaluation.txt`:
  `d3bad1f30aac526f86406364f155b0179a40abd8c4ea37dfe57897fc5071455e`

The literal evaluator result was:

| mode | batch | throughput B/A | p99 B/A | fsync B-A | verdict |
| --- | ---: | ---: | ---: | ---: | --- |
| Process | 1 | 0.934063 | 1.064695 | 0.0 | FAIL |
| Process | 10 | 1.036869 | 0.975271 | 0.0 | PASS |
| Process | 100 | 0.963535 | 1.215165 | 0.0 | FAIL |
| Process | 1000 | 1.022001 | 1.044287 | 0.0 | PASS |
| Group | 1 | 0.996035 | 0.898165 | -0.5 | PASS |
| Group | 10 | 0.973245 | 1.169831 | 0.0 | FAIL |
| Group | 100 | 0.964019 | 1.179679 | -0.5 | FAIL |
| Group | 1000 | 0.998722 | 1.116571 | 0.0 | FAIL |

Allocation calls and bytes passed every cell. The median Group barrier-delta
gate passed every cell, but the aggregate Group/b1000 gate did not: B issued
1027 barriers versus A's 1022, or 100.489%, above the locked 100.25% ceiling.
Grouped publication improved Group/b1 p99 by 10.2% and reduced its median cycle
barrier delta by 0.5. The admission failure is nevertheless real: Process/b1
throughput is 6.6% below control, Process/b100 p99 is 21.5% worse, and Group
b10/b100/b1000 exceed at least one locked throughput, p99, or barrier limit.

Five candidate and three control rows crossed the fixed 50 ms fdatasync alarm.
They remain in every timing median. Group append p99 generally tracks fsync p99,
but the Process regressions have zero fsyncs and therefore require a production
publication-path explanation rather than a storage-device excuse.

Post-measurement adversarial review also found that this checkpoint does not
yet have one reader linearization frontier. `publish_group` releases
`ActiveIndex::applied_end` and only then advances `read_watermark`;
`read_stream` is clamped by the former while global/subscription surfaces use
the latter. A stream read can therefore observe the new group in the interval
before a global/watermark read can. The existing test gate pauses before both
stores and does not exercise that interval. This is an independent correctness
rejection; it must be closed with one authoritative public frontier and an
exact between-store concurrency test before another performance candidate is
eligible.

## Focused Process/b1 diagnosis

A three-cycle counterbalanced `perf stat` diagnostic reran Process/b1 against
the same frozen binaries after rejection. Candidate throughput was 0.961630x
and p50 was 1.048023x control, reproducing the wall-time direction. For the
same fixed event count, however, candidate user cycles were 0.987339x and user
instructions were 0.991721x control. The added publication metric RMWs and
scratch instructions therefore do not dominate the regression.

The evidence instead points to lost pipeline utilization. A warm cohort has at
most four plans (one per writer; repeated streams force a flush). The control
warms, publishes, and wakes plan 0 before retiring plans 1-3, allowing its
producer to prepare/refill concurrently. The candidate performs every plan's
cache materialization and scratch staging before the one cut and wakes nobody
until all post-I/O work is done. The next implementation must retain one
visibility cut while moving correctness-transparent cache work out of that
pre-completion interval. Full ratios and the falsified hypothesis are retained
in `attempt8-20260715/process-b1-perf-summary.md`.
