# bn-21ew benchmark provenance

Pre-change run: 2026-07-14 13:13–13:15 EDT (`-04:00`).

- Commit: `7aea2488818c18ff9fac086f0ad6a707f38ed447`
- Kernel: `Linux 8o8-arch 7.0.12-arch1-1 x86_64`, PREEMPT_DYNAMIC
- CPU: AMD Ryzen 9 3900X, 12 cores / 24 threads, boost enabled
- Rust: `rustc 1.97.0 (2d8144b78 2026-07-07)`, LLVM 22.1.6
- Cargo: `cargo 1.97.0 (c980f4866 2026-06-30)`
- Scratch: `$HOME/.cache/mess-bench` on `/dev/nvme0n1p3`, ext4,
  `rw,relatime`, 83% used at start
- Load average: `4.42 3.92 3.88` immediately before; `5.72 4.35 4.01`
  immediately after
- Build: release, thin LTO, debug symbols retained
- Shape: 24-byte payloads, four production writers, stable stream names,
  batches 1/10/100/1000, three repetitions for Process/Os/Group

Commands:

```text
MESS_BENCH_DIR=$HOME/.cache/mess-bench cargo run --release \
  --manifest-path spikes/direct_outcomes/Cargo.toml -- \
  micro pre spikes/direct_outcomes/BN-21EW-PRE-MICRO.csv
MESS_BENCH_DIR=$HOME/.cache/mess-bench cargo run --release \
  --manifest-path spikes/direct_outcomes/Cargo.toml -- \
  matrix pre spikes/direct_outcomes/BN-21EW-PRE-MATRIX.csv
```

One Group/batch-100 repetition reported a 119.5 ms device stall. It is kept
in the raw evidence rather than silently discarded; the median throughput and
median p99 remain represented by the two uncontaminated repetitions.

## Candidate and paired runs

Candidate measurements ran on 2026-07-14 from 13:27 through 14:22 EDT. The
accepted final runs were recorded from 14:02 through 14:22 EDT. The candidate
was the bn-21ew workspace implementation on top of the locked measurement
commit `fa4cca8e`; the comparison binary used the same final harness and the
frozen production source at `7aea2488818c18ff9fac086f0ad6a707f38ed447`.

The host, toolchain, filesystem, payload, and writer count were unchanged from
the pre-change run. `BN-21EW-FINAL-ABBA.csv` records load1 3.63--4.35 for all
144 observations. `BN-21EW-OS-VARIANCE-ABBA.csv` records load1 5.08--5.87 for
all 40 observations. The quiet guard ran before every production observation.

The final matrix used BN-2SU-exact work per writer: Process
40,000/12,500/2,500/250, Group 800/500/300/100, and Os 40/30/20/10 for
batches 1/10/100/1000. Candidate and frozen-baseline binaries were invoked in
A1/B1/B2/A2 order; every invocation appended its raw rows to the named CSV.
The command shapes were:

```text
MESS_BENCH_DIR=$HOME/.cache/mess-bench <candidate> matrix candidate-a1 \
  spikes/direct_outcomes/BN-21EW-FINAL-ABBA.csv
MESS_BENCH_DIR=$HOME/.cache/mess-bench <baseline> matrix baseline-b1 \
  spikes/direct_outcomes/BN-21EW-FINAL-ABBA.csv
MESS_BENCH_DIR=$HOME/.cache/mess-bench <baseline> matrix baseline-b2 \
  spikes/direct_outcomes/BN-21EW-FINAL-ABBA.csv
MESS_BENCH_DIR=$HOME/.cache/mess-bench <candidate> matrix candidate-a2 \
  spikes/direct_outcomes/BN-21EW-FINAL-ABBA.csv

MESS_BENCH_DIR=$HOME/.cache/mess-bench <candidate> os-variance cN-candidate-a1 \
  spikes/direct_outcomes/BN-21EW-OS-VARIANCE-ABBA.csv
MESS_BENCH_DIR=$HOME/.cache/mess-bench <baseline> os-variance cN-baseline-b1 \
  spikes/direct_outcomes/BN-21EW-OS-VARIANCE-ABBA.csv
MESS_BENCH_DIR=$HOME/.cache/mess-bench <baseline> os-variance cN-baseline-b2 \
  spikes/direct_outcomes/BN-21EW-OS-VARIANCE-ABBA.csv
MESS_BENCH_DIR=$HOME/.cache/mess-bench <candidate> os-variance cN-candidate-a2 \
  spikes/direct_outcomes/BN-21EW-OS-VARIANCE-ABBA.csv

MESS_BENCH_DIR=$HOME/.cache/mess-bench cargo run --release \
  --manifest-path spikes/direct_outcomes/Cargo.toml -- \
  micro final spikes/direct_outcomes/BN-21EW-FINAL-MICRO.csv
```

The Os variance sequence was repeated for `N=1..5`. One candidate batch-1
observation captured a 147.1 ms device stall; it remains in the raw file. All
superseded or contaminated measurements are retained under `REJECTED` names.

## Group barrier-parity follow-up

The decision rule and restricted Group b1/b10 harness were committed as
`7035ae9a` before measurement. The study ran from 2026-07-14 14:53 through
14:55 EDT, after the two authorized focused release builds. No `cargo`,
`rustc`, or `direct_outcomes` process remained at start; load1 was 4.77 and
below the declared 6.0 ceiling. The raw rows record load1 3.62--4.64, and
load1 was 3.01 at completion. There were no retries or additional cells.

- Candidate production commit: `7035ae9a` (implementation `68434ad5`)
- Candidate committer SHA-256: `94921adfbc3c9489d11c577201e88db404e79b8abf4d96fe1b24b12702762cda`
- Candidate binary SHA-256: `2668a3be17b43befc52c9c626b9967e2e13572eed407e72add6d0c0efaf4bb2b`
- Frozen baseline production commit: `7aea2488818c18ff9fac086f0ad6a707f38ed447`
- Baseline committer SHA-256: `54fad9024ad538f29f40c690e57a6835987b14e089988828e23347c6f2831f58`
- Baseline binary SHA-256: `916d20aa6320f1ac87e58821f8530e8f144429c9450764ebc7624704174b8072`
- Raw CSV SHA-256: `27f3bc42e6c7fb7e6c4773c44affb3a221fe8a8b8381687b6f2490c407765291`

For each `N=1..5`, the exact command order was:

```text
MESS_BENCH_DIR=$HOME/.cache/mess-bench <candidate> group-parity \
  cN-candidate-a1 spikes/direct_outcomes/BN-21EW-GROUP-PARITY-ABBA.csv
MESS_BENCH_DIR=$HOME/.cache/mess-bench <baseline> group-parity \
  cN-baseline-b1 spikes/direct_outcomes/BN-21EW-GROUP-PARITY-ABBA.csv
MESS_BENCH_DIR=$HOME/.cache/mess-bench <baseline> group-parity \
  cN-baseline-b2 spikes/direct_outcomes/BN-21EW-GROUP-PARITY-ABBA.csv
MESS_BENCH_DIR=$HOME/.cache/mess-bench <candidate> group-parity \
  cN-candidate-a2 spikes/direct_outcomes/BN-21EW-GROUP-PARITY-ABBA.csv
```

Each phase emitted one Group b1 and one Group b10 observation and ran the
quiet guard separately before both. Thus five cycles produced exactly 40 rows.
The predeclared aggregate-plus-median decision rule passed independently for
both cells; the exact totals and cycle deltas are in `BN-21EW-REPORT.md`.
