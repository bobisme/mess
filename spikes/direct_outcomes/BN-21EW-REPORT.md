# bn-21ew direct outcomes evidence

## Locked method and gates

`GATES.md` and the pre-change rows were committed before production code was
edited. `BN-21EW-PROVENANCE.md` records the host and ext4 scratch. Raw rows are
never silently discarded: every contaminated or superseded run has
`REJECTED` in its filename.

The original pre rows were measured at load1 6.58--6.97. A later candidate
matrix at load1 5.46--6.17 was slower in several cells despite the lower load;
it also contained explicit 139--207 ms device stalls. Because time-separated
host state could not distinguish code from storage variance, that matrix is
retained as `BN-21EW-REJECTED-MATRIX-time-separated.csv`. The final production
gate uses same-host AB/BA alternation against a frozen `7aea2488` baseline
binary, with the quiet guard before every repetition.

The original bn-21ew spike accidentally used shorter Process/Group runs than
the frozen BN-2SU cells. The final paired harness corrects both binaries to
BN-exact `bpw`: Process 40,000/12,500/2,500/250 and Group
800/500/300/100 for batches 1/10/100/1000. Os remains 40/30/20/10. The
shorter rows remain only as superseded diagnostics.

## Direct micro

Medians are per appended batch. Allocation goals passed, but b100 CPU/wall did
not; this run exposed 256 relaxed atomic spill-counter increments in the
measured 256-append group.

| events/batch | alloc calls pre/post | allocated B pre/post | CPU ns pre/post | CPU ratio | wall ns pre/post | wall ratio | verdict |
|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 4.020/0 | 504/0 | 1,451/1,066 | 73.4% | 1,486/1,085 | 73.0% | pass |
| 10 | 4.020/0 | 1,224/0 | 1,645/1,706 | 103.7% | 1,633/1,702 | 104.2% | pass |
| 100 | 4.020/1 | 8,424/5,120 | 7,444/9,428 | 126.7% | 7,492/9,455 | 126.2% | reject |
| 1000 | 4.020/1 | 80,424/40,960 | 69,471/64,434 | 92.7% | 70,127/65,487 | 93.4% | pass |

The accepted design aggregates the exact spill count once per direct group
and caches last-reported direct/owner scratch capacities, storing their
observability atomics only when capacity changes or an oversize group trims.
The exact-final `BN-21EW-FINAL-MICRO.csv` passes every allocation and time
gate:

| events/batch | alloc calls pre/final | allocated B pre/final | CPU ns pre/final | CPU ratio | wall ns pre/final | wall ratio |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4.020/0 | 504/0 | 1,451/1,082 | 74.6% | 1,486/1,078 | 72.5% |
| 10 | 4.020/0 | 1,224/0 | 1,645/1,658 | 100.8% | 1,633/1,663 | 101.8% |
| 100 | 4.020/1 | 8,424/5,120 | 7,444/7,599 | 102.1% | 7,492/7,745 | 103.4% |
| 1000 | 4.020/1 | 80,424/40,960 | 69,471/54,860 | 79.0% | 70,127/55,298 | 78.9% |

`BN-21EW-POST-FIX-MICRO100.csv` is the preceding targeted proof that the
telemetry correction fixed b100. The first header-only attempt overlapped a
required full-suite build, produced no samples, and is retained as
`BN-21EW-REJECTED-MICRO100-overlap.csv`.

## Time-separated production matrix: rejected

These medians compare `BN-21EW-REJECTED-MATRIX-time-separated.csv` with the
locked pre rows and the matching log-engine rows in
`../baseline_matrix/BN-2SU-FINAL.csv`. `fsync` is paired pre/post count.

| mode | batch | pre ev/s | post ev/s | paired ratio | BN-2SU ev/s | BN ratio | pre p99 us | post p99 us | p99 ratio | fsync | verdict |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Process | 1 | 151,942 | 160,313 | 105.5% | 164,291 | 97.6% | 50.2 | 45.4 | 90.3% | 0/0 | pass |
| Process | 10 | 1,094,339 | 934,132 | 85.4% | 1,151,131 | 81.1% | 61.5 | 81.6 | 132.6% | 0/0 | reject |
| Process | 100 | 2,780,353 | 2,172,151 | 78.1% | 2,406,276 | 90.3% | 246.2 | 282.7 | 114.8% | 0/0 | reject |
| Process | 1000 | 8,811,627 | 8,247,357 | 93.6% | 10,559,041 | 78.1% | 260.1 | 220.5 | 84.8% | 0/0 | reject |
| Os | 1 | 376 | 334 | 88.8% | -- | -- | 13,320.8 | 16,316.5 | 122.5% | 164/164 | reject |
| Os | 10 | 3,710 | 2,200 | 59.3% | -- | -- | 12,636.8 | 171,319.8 | 1,355.7% | 124/124 | reject: device stall |
| Os | 100 | 33,628 | 30,419 | 90.5% | -- | -- | 16,112.2 | 14,794.5 | 91.8% | 84/84 | pass |
| Os | 1000 | 327,572 | 282,211 | 86.2% | -- | -- | 17,953.5 | 22,640.4 | 126.1% | 44/44 | reject |
| Group | 1 | 1,368 | 1,236 | 90.4% | 1,410 | 87.7% | 5,814.1 | 5,447.7 | 93.7% | 203/203 | reject BN |
| Group | 10 | 13,600 | 10,936 | 80.4% | 13,743 | 79.6% | 4,890.5 | 7,620.3 | 155.8% | 153/153 | reject: device stall |
| Group | 100 | 122,874 | 108,987 | 88.7% | 141,064 | 77.3% | 4,822.5 | 5,547.0 | 115.0% | 103/103 | reject |
| Group | 1000 | 1,071,704 | 687,774 | 64.2% | 1,069,060 | 64.3% | 6,032.3 | 7,960.4 | 132.0% | 53/53 | reject |

## Paired diagnostic and layout evidence

`BN-21EW-DIAG-ABBA.csv` alternates candidate A / baseline B / baseline B /
candidate A on identical Process b10/b100 work. Combined medians contradicted
the time-separated regression:

| batch | candidate/base throughput | candidate/base p99 | verdict |
|---:|---:|---:|---|
| 10 | 100.1% | 104.3% | within gates |
| 100 | 104.5% | 96.7% | within gates |

Temporary group instrumentation (`BN-21EW-DIAG-GROUPS.csv`) showed equivalent
owner shapes: b10 candidate/baseline averaged about 2.25/2.22 plans per commit,
b100 1.79/1.81, max four. `BN-21EW-DIAG-PERF-STAT.txt` contains the four ABBA
counter sets. Cycle profiles had zero lost samples; candidate direct completion
was 0.33% plus 0.05% in `commit_group_inner`, versus baseline 0.53% in
`DirectCommitter::commit_ordered_group` plus 0.08% in `commit_group`. The large
binary `perf.data` inputs were intentionally deleted after this compact report.

GDB/DWARF layout checks and compile-time assertions report:

| type | frozen baseline bytes | candidate bytes |
|---|---:|---:|
| `CommitReq` | 136 | 136 |
| `DomainPlan` | 136 | 136 |
| `DirectBatchOutcome` | 120 | 120 |
| `CommitEffect` | n/a | 48 |
| `DirectIndex` | n/a | 8 |
| `PlanOutcomes` | n/a | 240 |

At the named 256-slot cap, direct request/effect/index storage is 49,152 bytes
and owner outcome storage is 61,440 bytes (110,592 bytes aggregate), below the
1 MiB cap. Large subframe descriptors spill transiently and are dropped.

## Final paired gate

`BN-21EW-FINAL-ABBA.csv` contains 144 BN-exact rows in candidate A1 / baseline
B1 / baseline B2 / candidate A2 order. The table uses the median of all six
candidate and all six baseline repetitions. Barrier totals are the sum of six
repetitions; phase medians are in the raw file.

| mode | batch | baseline ev/s | candidate ev/s | paired ratio | BN-2SU ev/s | BN ratio | baseline p99 us | candidate p99 us | p99 ratio | barrier totals base/cand | literal verdict |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Process | 1 | 161,916 | 160,492 | 99.1% | 164,291 | 97.7% | 43.7 | 47.5 | 108.9% | 0/0 | pass |
| Process | 10 | 1,057,415 | 1,088,384 | 102.9% | 1,151,131 | 94.5% | 65.4 | 61.5 | 94.1% | 0/0 | fail BN by 0.5 pp |
| Process | 100 | 2,536,362 | 2,429,029 | 95.8% | 2,406,276 | 100.9% | 272.1 | 260.0 | 95.5% | 0/0 | pass |
| Process | 1000 | 8,976,912 | 9,106,170 | 101.4% | 10,559,041 | 86.2% | 313.8 | 287.6 | 91.7% | 0/0 | fail BN |
| Os | 1 | 392 | 368 | 94.0% | -- | -- | 12,080.6 | 13,420.9 | 111.1% | 984/984 | fail p99 |
| Os | 10 | 3,683 | 3,654 | 99.2% | -- | -- | 13,125.0 | 14,700.1 | 112.0% | 744/744 | fail p99 |
| Os | 100 | 36,178 | 35,139 | 97.1% | -- | -- | 13,689.3 | 13,234.8 | 96.7% | 504/504 | pass |
| Os | 1000 | 326,306 | 318,855 | 97.7% | -- | -- | 18,776.3 | 19,122.1 | 101.8% | 264/264 | pass |
| Group | 1 | 1,406 | 1,408 | 100.1% | 1,410 | 99.8% | 5,712.7 | 5,769.9 | 101.0% | 4,821/4,823 | fail literal barrier total |
| Group | 10 | 13,540 | 13,886 | 102.6% | 13,743 | 101.0% | 5,780.6 | 5,735.6 | 99.2% | 3,022/3,023 | fail literal barrier total |
| Group | 100 | 127,942 | 135,112 | 105.6% | 141,064 | 95.8% | 5,542.6 | 5,447.1 | 98.3% | 1,823/1,821 | pass |
| Group | 1000 | 1,052,332 | 1,046,206 | 99.4% | 1,069,060 | 97.9% | 7,670.1 | 6,270.7 | 81.8% | 624/623 | pass |

Candidate/baseline throughput passes every paired cell. The two Process BN
misses are also present in the frozen baseline on this host (baseline/BN is
91.9% at b10 and 85.0% at b1000), so they demonstrate historical-reference
drift rather than a candidate regression. The Os p99 misses have byte-identical
sync counts and are storage-latency dominated. Group b1/b10 differ by only two
and one barriers across 4,800/3,000-plus barriers, while their two phase
medians have the same 803/804 and 504 distributions. These facts explain the
variance but do not rewrite the predeclared literal gates: the result is a
non-pass pending the predeclared targeted variance study below, and no
risk-high approval has been granted.

## Predeclared targeted Group barrier-parity study: PASS

This study is restricted to the two open barrier cells, Group b1 and b10. It
uses the same frozen baseline/candidate production sources, final harness,
payload, four writers, Group durability, and BN-exact work per writer (b1 800,
b10 500). Run five complete candidate A1 / baseline B1 / baseline B2 /
candidate A2 cycles. Each phase emits exactly one observation per cell, with
the quiet guard run independently before every observation. The resulting raw
file therefore contains 40 rows: ten candidate and ten baseline observations
per cell. No Process, Os, b100, or b1000 cells are admitted.

The decision rule is fixed before measurement and applies independently to
each batch size. For cycle `N`, its paired barrier delta is
`(candidate-a1 + candidate-a2) - (baseline-b1 + baseline-b2)`. PASS requires
both (1) the candidate's total barriers across all ten observations to be no
greater than the baseline total and (2) the median of the five cycle deltas to
be non-positive. Every observation must also complete the exact event count.
Any missing row or failure of either barrier condition declines bn-21ew; the
rule will not be widened after measurement. This aggregate-plus-paired rule
tests the frozen “barriers may not increase” contract without accepting a
positive result as scheduler noise.

Measurement status at predeclaration commit `7035ae9a`: not run. The subsequent
`BN-21EW-GROUP-PARITY-ABBA.csv` contains exactly the declared 40 rows, with no
retry or extra cell. `run_log` asserts the exact event total before returning;
all observations completed that assertion. The recorded shapes are uniformly
four writers and the declared b1/b10 work.

| batch | baseline/candidate barriers | cycle deltas in run order | median delta | verdict |
|---:|---:|---|---:|---|
| 1 | 8,039/8,036 | -1, -1, +1, -1, -1 | -1 | pass |
| 10 | 5,036/5,036 | +1, 0, -2, 0, +1 | 0 | pass |

Both independently applied conditions pass in both cells: candidate aggregate
barriers do not exceed baseline and the median paired cycle delta is
non-positive. The original final-matrix +2/+1 result remains visible rather
than being relabeled. It is admissible scheduling variance because the measured
count is the number of groups admitted from concurrent writers, the candidate
still executes one barrier per admitted Group commit, and the predeclared
same-host paired study shows neither an aggregate nor median-paired increase.
This closes the literal Group barrier gate without changing its threshold.

## Targeted Os variance study: PASS

Scope is fixed to the only paired latency misses, Os b1/b10. Run five AB/BA
cycles (20 observations per cell) with the same frozen baseline/candidate
binaries, load guard before every observation, and exact sync-count equality.
Each row records append p50/p99 and commit-fsync p50/p95/p99/max/mean.

The decision rule was fixed before measurement: paired append p99 must be at most 110%,
or any excess must track the paired fsync-p99 device drift within two percentage
points with the candidate's non-fsync residual no worse. Otherwise bn-21ew is
declined. The final report continues to show both literal candidate/BN and
drift-normalized candidate/baseline throughput; the latter is required because
the same-host frozen baseline itself reaches only 91.9%/85.0% of historical
BN-2SU at Process b10/b1000.

`BN-21EW-OS-VARIANCE-ABBA.csv` contains 40 rows: five complete ABBA cycles,
ten paired candidate/baseline observations per cell. Every row has the exact
contractual sync count (b1 164, b10 124). The study passes by the first clause:

| batch | median paired append-p99 ratio | overall append-p99 base/cand us | overall ratio | fsync-p99 base/cand us | fsync ratio | verdict |
|---:|---:|---:|---:|---:|---:|---|
| 1 | 102.28% | 13,522.8/13,906.5 | 102.84% | 4,587.5/5,242.9 | 114.29% | pass <=110% |
| 10 | 92.24% | 13,612.3/12,921.6 | 94.93% | 4,587.5/4,325.4 | 94.29% | pass <=110% |

One candidate b1 observation recorded an explicit 147.1 ms device stall
(append p99 192.7 ms, fsync p99 36.7 ms). It is retained in the raw file and
does not move the paired median. With the predeclared variance rule satisfied,
the Os latency result is accepted; the risk-high review remains the final gate.

## Validation

- `cargo check --workspace --all-targets` passed.
- The nine focused direct-committer fault/layout/preflight tests passed, as did
  the real-filesystem warmed allocation test (zero direct Process allocations)
  and the focused engine smoke, prepared, name-durability, roll, and metrics
  suites.
- `MESS_SNAPSHOT_LAW_ITERS=25 just test --test-threads=1` covered the entire
  workspace serially. It reached 948 passing tests before the unrelated
  `social::seed_profile_smoke` timing assertion reversed two nearby medians
  (62.7 versus 64.2 microseconds) and fail-fast left eight tail tests unrun.
  The timing test passed immediately in isolation; the four snapshot and four
  store-roundtrip tail tests then passed explicitly. Thus every non-ignored
  workspace test was exercised successfully.
- Earlier four-way suite attempts reproduced the already-tracked
  `bn-2nd` fold-chain timing flake under parallel load; its isolated rerun and
  the serial full-suite instance both passed.
- Final `just fmt-check` and `git diff --check` passed. No `perf.data`, build
  output, generated lockfile, or bones-ledger workspace change is included.
