# bn-2su flat append owner — matched matrix

## Final optimization rerun (2026-07-13)

The FIX-FIRST performance block is resolved. After consulting the Asterism
research and append spikes, `bn-2su` now moves deterministic large-batch frame
preparation onto producer tasks while preserving one authoritative owner for
type resolution, expected-version validation, global/stream positions, segment
epoch and batch id, fold-chain state, final CRC, ordered write, durability,
publication, and completion. The prepared final buffer is then adopted by the
bounded capsule cache, so the optimization does not trade append throughput for
a cold first read.

The cutoff is 16 KiB encoded: ordinary small appends retain the owner's reusable
encoder; large appends avoid the borrowed-record deep clone, serial framing,
and post-commit cache copy. The owner also resolves each distinct type once,
uses the frozen spike's last-type fast path, and no longer computes histogram
percentiles after every commit group. Ordered-unit preflight remains
load-bearing: an invalid domain batch can never strand the registry batch that
precedes it.

Method is unchanged from the locked matrix: release build, fresh stores,
AB/BA/AB ordering, quiet guard, and median of three for all 32 cells. Raw rows
are committed in `BN-2SU-FINAL.csv`. Percentages below compare the final
production `LogEngine` median to the exact baseline-gen2 Fjall-era median in
`REPORT.md`.

### Final Process median events/s

| payload | batch | 1 writer | 4 writers |
|---:|---:|---:|---:|
| 24 | 1 | 57,560 (+89.8%) | 164,291 (+144.2%) |
| 24 | 10 | 486,474 (+63.3%) | 1,151,131 (+72.3%) |
| 24 | 100 | 1,934,901 (+18.7%) | 2,406,276 (+20.6%) |
| 24 | 1000 | 4,640,233 (+50.3%) | 10,559,041 (+177.7%) |
| 250 | 1 | 53,741 (+64.3%) | 164,194 (+151.2%) |
| 250 | 10 | 393,602 (+50.5%) | 963,903 (+66.0%) |
| 250 | 100 | 1,948,666 (+74.3%) | 3,714,918 (+121.1%) |
| 250 | 1000 | 3,213,862 (+50.4%) | 6,230,462 (+122.2%) |

Every Process cell beats baseline-gen2. The former regressions reverse
decisively:

- 24 B x 1000 x 4: 3.803M -> 10.559M, **2.78x Fjall-era** and 2.62x the
  matched bare actor. Median p99 falls from 1,528 us to about 249 us.
- 250 B x 1000 x 4: 2.804M -> 6.230M, **2.22x Fjall-era** and 1.90x matched
  bare. Median p99 falls from 2,370 us to about 811 us.
- 250 B x 100 x 4: 1.680M -> 3.715M, **2.21x Fjall-era**.

The fact that the prepared composed path can beat the matched bare actor is not
a weakened comparison: the bare actor intentionally serializes frame encoding;
the production owner now overlaps pure preparation across producer tasks, the
same large-batch crossover measured in `spikes/perf_append`. Canonical ordering
and all durability work remain serial under the owner.

### Final Group median events/s

| payload | batch | 1 writer | 4 writers |
|---:|---:|---:|---:|
| 24 | 1 | 351 (-3.8%) | 1,410 (-0.4%) |
| 24 | 10 | 3,737 (-4.6%) | 13,743 (+69.8%) |
| 24 | 100 | 35,694 (-8.9%) | 141,064 (+15.5%) |
| 24 | 1000 | 336,056 (-3.1%) | 1,069,060 (+74.5%) |
| 250 | 1 | 363 (-4.7%) | 1,459 (+13.4%) |
| 250 | 10 | 3,658 (-6.3%) | 14,312 (+115.1%) |
| 250 | 100 | 35,797 (-3.6%) | 134,259 (+1.6%) |
| 250 | 1000 | 296,163 (+34.9%) | 944,687 (+143.0%) |

No Group cell regresses by 10%; the durable non-regression gate passes. The
previously blocked 250 B x 100 x 4 cell rises from 120,476 (91.2% of
baseline-gen2) to 134,259 (101.6%). D7 barrier parity remains fixed: typical
four-writer log/bare counts are about 803/801, 504/501, 303/301, and 103/101
for batches 1/10/100/1000. Group throughput ratios against bare move with the
device's per-repetition fsync latency; barrier counts are the stable semantic
check.

### Correctness and remaining headroom

The prepared path is byte-compared against the canonical encoder and covered
through chain-on writes, live roll, write-through cache reads, and reopen. An
initial run let the two oversized-registry tests expose a temporarily removed
ordered-unit preflight. Preflight was restored (O(1) for prepared batches), and
the final documented 25-iteration snapshot-law smoke run passes all 944
non-timing workspace tests. The known debug-mode `fold_chain_overhead`
microbenchmark passes separately in release mode (171 ns/event chain delta).

There is still real Asterism headroom, but it is no longer needed to accept
Step 2: an ack-free direct result path, group-wide index/head publication,
ordered `pwritev`, dense heads, and active microblocks are the next measured
levers. B1 barrier pipelining and log striping remain rejected by their spikes.

## Pre-optimization integration matrix (superseded)

Measured 2026-07-13 after commits `dfad51a7` and `b583fd01`, using the
baseline-matrix harness in this directory. The run used release builds, fresh
stores, interleaved A/B B/A A/B ordering, and median-of-three results for all
32 matched cells (batch 1/10/100/1000, payload 24/250 B, 1/4 writers,
Process/Group). The harness quiet guard admitted runs only below load1 6.0;
load averages immediately after completion were 1.89/2.92/5.08. The current
harness does not persist per-repetition load in its CSV, so this report does
not claim a more precise load value.

The raw CSV for this run was written outside the worktree at
`/tmp/mess-bn-2su-bench/baseline_results.csv`. Values below are the production
`LogEngine` median event rates; parenthesized percentages compare to the
baseline-gen2 production medians in `REPORT.md`.

## Process median events/s

| payload | batch | 1 writer | 4 writers |
|---:|---:|---:|---:|
| 24 | 1 | 54,567 (+79.9%) | 159,532 (+137.1%) |
| 24 | 10 | 482,064 (+61.8%) | 1,174,011 (+75.7%) |
| 24 | 100 | 1,804,395 (+10.7%) | 2,485,890 (+24.6%) |
| 24 | 1000 | 1,803,135 (-41.6%) | 2,443,840 (-35.7%) |
| 250 | 1 | 56,931 (+74.0%) | 165,837 (+153.7%) |
| 250 | 10 | 438,354 (+67.7%) | 983,216 (+69.4%) |
| 250 | 100 | 1,503,675 (+34.5%) | 2,097,276 (+24.8%) |
| 250 | 1000 | 1,562,128 (-26.9%) | 1,845,762 (-34.2%) |

The flat owner materially reduces fixed per-append orchestration cost through
batch 100. It does not reproduce Spike B's all-cell prototype win at batch
1000; at that width the current production encode/index/publish work dominates
and baseline-gen2 was faster. This is evidence, not a hidden exception: the
known async/API ceiling remains and no 85%-of-bare claim is made.

## Group median events/s

| payload | batch | 1 writer | 4 writers |
|---:|---:|---:|---:|
| 24 | 1 | 357 (-2.2%) | 1,462 (+3.3%) |
| 24 | 10 | 3,662 (-6.5%) | 14,388 (+77.7%) |
| 24 | 100 | 36,712 (-6.3%) | 129,602 (+6.1%) |
| 24 | 1000 | 306,897 (-11.5%) | 690,854 (+12.8%) |
| 250 | 1 | 365 (-4.2%) | 1,442 (+12.0%) |
| 250 | 10 | 3,577 (-8.4%) | 14,591 (+119.2%) |
| 250 | 100 | 35,342 (-4.8%) | 120,476 (-8.8%) |
| 250 | 1000 | 247,049 (+12.5%) | 584,621 (+50.4%) |

The decisive correctness/performance result is barrier coalescing. Stable
four-writer log/bare fsync counts were approximately 803/801 at batch 1,
504/501 at batch 10, 304/301 at batch 100, and 104/101 at batch 1000 (minor
rep-to-rep variation). Baseline-gen2 split the affected convoys to roughly
2x barriers in several cells, including 1000/526 at 24 B batch 10 and 200/100
at 250 B batch 1000. The owner's D7 target-width/inflight/grace close therefore
fixes the `bn-3pz` failure mode.

One bare repetition at 250 B, batch 1, four writers issued 1,202 fsyncs instead
of about 801; ABBA median-of-three prevented that outlier from determining the
cell result. No corresponding systematic split appears in the log results.
