# Publication v2 focused gate: rejected

The diagnostics-free publication-v2 candidate is **not admitted as a
standalone storage-engine change**. The one authorized focused study completed
all 60 predeclared observations, but only Process/b100 passed every locked
gate. No threshold changed, no completed row was discarded, and no replacement
run or full 160-row matrix is authorized.

## Frozen comparison

- Control source: `f712b03d123855f72e37b010cc621fc58709f1f5`.
- Control binary SHA-256:
  `0fe4bf83a71fa875e400704597c93b4e0bf3856fc6af45aff74114b568b7d705`.
- Candidate source: `f5a30820c7f51a39a84dcd002cd4b089e01661d3`.
- Candidate binary SHA-256:
  `1855e100e5bebe7f4b9778ef2d529775368ec1dd8272371c28df52a66cf3c770`.
- Both harnesses:
  `e063a95a1df126e9e81ec7a336ce177f1d8dab0af66aaaaa086f546273367a21`.
- Both ignored lockfiles:
  `abc989ca20918a57678e6ad7058d7ecd2c84fcb399ae3cd976acf0e2238befbe`.

The candidate contains the one-authoritative-frontier correctness work and
post-frontier cache warm/completion ordering, but none of the temporary
OwnerDiagnostics clocks, counters, sidecar, or API. The ignored lockfiles were
made byte-identical before the candidate's `--locked` build; this removed
otherwise-confounding `syn` and runtime `xxhash-rust` version drift while
preserving the frozen control binary.

Every row executes the production
`EventStore -> FjallSnapshotBackend -> LogEngine` path with a fresh ext4 store,
four writers, and 250-byte events. Five cycles use A,B,B,A on odd cycles and
B,A,A,B on even cycles. The observed pre-run load1 range was 2.92..3.59, all 24
logical CPUs used the performance governor, and no row crossed the durability
degradation alarm.

## Literal result

| cell | throughput B/A | p99 B/A | alloc calls B/A | alloc bytes B/A | median barrier B-A | aggregate barriers B/A | verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| Process/b1 | 0.962144 | 1.031216 | 0.999164 | 0.997984 | 0.0 | n/a | FAIL |
| Process/b100 | 1.015569 | 0.950989 | 0.999820 | 1.000007 | 0.0 | n/a | PASS |
| Group/b1000 | 1.008579 | 1.070458 | 1.000000 | 1.000000 | +0.5 | 1.006944 | FAIL |

Process/b1 failed only the 0.97 throughput floor. Its five cycle ratios were
0.951010, 0.984765, 0.994097, 0.937765, and 0.962144; all five remained below
parity. Moving correctness-transparent work after the frontier materially
improved the prior candidate's 0.934063 result, but not enough to admit it.
Process/b100 passed every gate and reversed the prior p99 failure.

Group/b1000 passed the throughput, p99, and allocation ratios but failed both
barrier requirements. The candidate issued 1,015 barriers versus 1,008 for the
control, or 100.6944%, above the locked 100.25% ceiling; its median cycle
barrier delta was +0.5 rather than at most zero. Group timings were visibly
device-nonstationary (cycle throughput ratios 0.881600..1.346608 and fsync p99
9.7..18.4 ms), so the passing latency medians are not evidence of a standalone
win. The exact barrier excess remains a protocol failure independent of that
timing noise.

## Decision

Do not merge this workspace into the production engine and do not run the
full matrix. Retain the canonical-frontier proof and post-frontier ordering as
design evidence, then integrate the contract only as part of the already
planned dense-head and active-microblock composition, where its removed locks,
index writes, and wake topology can pay for the fixed cut instead of adding a
standalone stage to the current ActiveIndex/Book path.

Raw rows are in `paired.csv`; stdout is in `run.log`; machine, source, binary,
harness, dependency, and command provenance is in `provenance.txt`;
`evaluation.txt` is the runner's stdout and `evaluation-combined.txt` retains
the deterministic evaluator's stdout plus rejection diagnostics. Hashes are
in `SHA256SUMS`.
