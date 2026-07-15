# bn-1zv6: ordered `pwritev` decision experiment

Status: **ATTEMPT 1 INCONCLUSIVE; ATTEMPT 2 PROSPECTIVELY AMENDED — no
performance decision exists yet**

The original pre-row clarification was frozen before any timed row. Attempt 1
then completed correctness, all 136 prepared-pipeline rows, all 952
exploratory rows, and 11 of 440 boundary rows. Boundary row 11 passed its
pre-row quiet guard but failed the post-row load guard. The original runner
retained that row and fail-stopped before selection. Attempt 1 is therefore
classified `INCONCLUSIVE_INFRASTRUCTURE`, not `DECLINED`: none of its timing
rows may enter selection, confirmation, or the final verdict. Its raw files
and hashes are preserved under `evidence/attempt1-inconclusive-infrastructure/`.

Before any Attempt-2 row, the protocol was amended prospectively to make the
documented noisy-row behavior executable. The amendment uses no Attempt-1
timing result: Attempt 2 starts in a fresh output directory at row zero, keeps
the same matrix, row order, candidates, thresholds, and admission gates, and
never retries, replaces, or imports a row from Attempt 1. Post-row noisy
observations remain in their raw CSV and execution proceeds only after the
next row's pre-row quiet-load guard succeeds. Tool, correctness, provenance,
contract, schema, and row-integrity failures remain fatal and fail-stop.

This is a decision spike, not a production implementation. It compares ways
to write the already-prepared, already-ordered physical batches in one real
flat-owner group. Production code remains unchanged until this experiment
either satisfies every admission gate below or records `DECLINED`.

## 1. Production seam and invariants

Source traced at `248148316b68fc2318e9ec8d8345320196a5134a`:

- `mess-store::FlatOwner` gathers ordered units and calls
  `DirectCommitter::commit_ordered_group`.
- A unit is one domain batch or an ordered `$registry` + domain pair.
- `PreparedBatch` is one contiguous v3 batch. The producer lays out frame
  bodies; `SegmentWriter::append_prepared` stamps type IDs, epoch, batch ID,
  global/stream positions, optional chain entry, and the covering CRC before
  issuing one positioned write.
- The current physical group is therefore `k` adjacent, independently valid
  prepared batch buffers followed by: no barrier for `Process`, one barrier
  per physical batch for `Os`, or one covering barrier for `Group`.
- A roll partitions a group. The old file is synced, the next segment is
  allocated, its header is written and synced, and only then can batches be
  written to the new file. No gathered call may span files or skip these
  lifecycle barriers.

The experiment may replace only the `k` adjacent positioned writes inside one
same-file `Process`/`Group` partition. It must not change validation, ordering,
prepared bytes, receipt assignment, CRC coverage, fold-chain advancement,
publication, completion, segment allocation, or durability policy. `Os`
always falls back to one write + one barrier per physical batch; coalescing it
would change the contract even if the result were stronger.

## 2. Hypotheses and variants

Ranked hypotheses:

1. `pwritev` removes material syscall/owner CPU at small and medium batch
   widths without copying payload bytes.
2. A bounded iovec count avoids pathological kernel import cost and makes
   groups larger than `IOV_MAX` predictable.
3. Copying into one contiguous scratch buffer may beat iovec import for many
   tiny batches, but should lose once copied bytes dominate.
4. The current `k_pwrite` path should remain best for a group of one, `Os`,
   roll partitions, very large single batches, and regions where gathering
   saves too little to repay policy complexity.

Matched variants:

| name | physical operation | copied bytes | eligibility |
|---|---|---:|---|
| `k_pwrite` | current `write_at` loop per batch | 0 | all cells; reference |
| `pwritev_full` | vectored retry loop, up to runtime `IOV_MAX` | 0 | same-file `Process`/`Group` |
| `pwritev_64` | consecutive chunks of at most 64 iovecs | 0 | same-file `Process`/`Group` |
| `pwritev_256` | consecutive chunks of at most 256 iovecs | 0 | same-file `Process`/`Group` |
| `copy_contiguous` | copy partition into reusable scratch, one `write_at` loop | exact partition bytes | same-file `Process`/`Group`, bounded by 8 MiB |
| `adaptive` | best predeclared bounded rule derived from exploratory rows | reported exactly | must retain every fallback above |

Every variant uses the same `write_all` semantics: retry `EINTR`, advance by a
reported short-write byte count (including through the middle of an iovec),
and reject zero progress. The runtime iovec limit is
`sysconf(_SC_IOV_MAX)` (1024 on the reference host); calls never exceed it.
Total requested bytes are checked against `isize::MAX` and the selected byte
chunk cap before constructing the syscall arguments.

The adaptive rule is not allowed to tune per repetition. Exploratory data may
choose only among these fixed candidate boundaries:

- minimum physical batches: 2, 4, 8, or 16;
- iovec cap: 64, 256, or runtime `IOV_MAX`;
- byte cap: 1 MiB, 4 MiB, or 8 MiB;
- contiguous-copy ceiling: disabled, 64 KiB, 256 KiB, or 1 MiB.

These four axes form exactly 144 configurations. A configuration is applied
deterministically to each same-file partition: use `k_pwrite` below its
minimum physical-batch count; otherwise use `copy_contiguous` when copying is
enabled and partition bytes do not exceed the copy ceiling; otherwise use
`pwritev` with its selected iovec and byte caps. The natural-shape sweep
measures `k_pwrite`, the three 8-MiB pwritev iovec caps, and eligible copy.
The 1/4-MiB byte caps are exercised in the explicit boundary tier rather than
repeated over the full Cartesian product. The evaluator replays exact
iovec/byte partitioning for all 144 rules, but boundary results are never
extrapolated to a different natural shape: a configuration whose distinct
natural physical call shape lacks a directly paired row is rejected.
Duplicate physical outcomes (same buffer cuts,
copy decision, and syscall arguments) are measured once and referenced by all
equivalent rules.

The chosen rule is frozen in the report before the confirmation matrix. If no
single rule passes, the verdict is `DECLINED`; isolated cherry-picked cells do
not justify integration.

## 3. Canonical corpus and exact parity oracle

The harness builds each corpus through the real production layout:

1. `PreparedBatch::encode` creates the producer-prepared buffer and
   `set_event_type_ids` resolves its placeholders.
2. A real `SegmentWriter::append_prepared` stamps writer-owned fields and
   writes a golden segment. Receipts capture exact batch offsets and lengths.
3. The golden batch ranges are read back without transformation. These exact
   buffers are the iovecs/copy inputs for every candidate.
4. Each candidate starts from the same canonical segment header and writes the
   same corpus at the same offsets. The standard recovery scanner validates
   the result.

For every successful row, all variants must have identical:

- complete file bytes and file length;
- receipt tuples `(batch_id, first_global_pos, frame_count, total_len, offset)`;
- batch/header and marker CRC values, plus scanner acceptance/safe offset;
- dense global positions and per-stream versions;
- chain-off and chain-on exit heads;
- logical group boundaries and barrier counts.

The timed I/O phase excludes golden-corpus construction so all variants pay
the same already-completed preparation/stamping/CRC work. Separate
prepared-write-pipeline accounting rows measure preparation + stamping + CRC
and report the maximum possible pipeline value of eliminating the positioned
syscalls; the decision must not present that result as a full owner or engine
speedup.

Those rows run the production `PreparedBatch` encoding and `SegmentWriter`
stamping/CRC path against a counting `Fs` whose writes retain no bytes and
perform no syscall. For a matching group, prepared-write-pipeline CPU is
`preparation_cpu + measured_io_cpu`. “Material prepared-write-pipeline” is
frozen at the same 10% threshold as the isolated gate: the selected rule must
improve pipeline CPU or throughput by at least 10%, while still passing the
original isolated >=10% CPU-or-throughput gate. FlatOwner grouping,
publication, and completion are not included; a full owner/product gain is a
mandatory gate in the separate integration bone.

## 4. Matrix

The matrix is tiered to remain reproducible rather than multiplying every
axis into hours of device drift.

### 4.1 Kernel-shape sweep

- owner intents per same-file group: `1, 2, 4, 16, 64, 256, 1024, 1025`;
- events per physical batch: `1, 10, 100, 1000`;
- payload bytes per event: `24, 250, 4096, 65536`, omitting combinations
  whose one batch would exceed v3 `MAX_BATCH_LEN`;
- unit shape: domain-only (`1` batch/intent) and new-name
  (`$registry + domain`, `2` adjacent batches/intent);
- chain: off in the performance sweep; on in parity/fault confirmation;
- durability: `Process`, `Os`, `Group` with exact production barrier policy;
- group byte partitions: natural size plus explicit `64 KiB, 256 KiB, 1 MiB,
  4 MiB, 8 MiB` byte boundaries and effective `64, 256, runtime IOV_MAX`
  iovec boundaries for every bounded policy transition. Configured iovec caps
  are always clamped to the captured runtime limit before transition planning.
- policy activation widths: below/equal/above `2, 4, 8, 16` physical batches,
  using the canonical 24-byte, one-event domain batch.

Stage 1 is a kernel-policy selection study and therefore runs in `Process`
only. `Os` is the mandatory semantic `k_pwrite` fallback, while `Group` adds
the same unchanged covering `fdatasync` after the selected positioned-write
policy; both are exercised at measured owner widths in the full Stage-2
confirmation, where they may accept or decline the frozen rule but cannot
retune it.

The natural-shape tier is exact rather than a redundant Cartesian product.
Owner-intent widths 2 and 4 retain every valid payload × events × unit-shape
combination. Widths 16, 64, 256, 1024, and 1025 retain both unit shapes for
the representative buffer geometries `(24,1)`, `(24,10)`, `(250,10)`, and
`(4096,10)`. The explicit byte/IOV boundary tier supplies every remaining
policy transition. This is sufficient because the isolated kernel observes
only ordered buffer lengths/counts and caps, not logical payload/event labels;
any adaptive configuration whose physical call shape is absent is rejected.

Exploratory screening stops at the first of 32 MiB or 250 ms after warm-up;
targeted boundary rows stop at the first of 64 MiB or 500 ms; confirmation
stops at the first of 128 MiB or 750 ms. Every phase is capped at 2 GiB and
finishes its current physical group/trace before stopping. It records physical bytes,
logical events, groups, batches, write syscalls, iovec count distribution,
short-write retries, copied bytes, barriers/lifecycle barriers, thread CPU,
wall time, events/s, bytes/s, and per-group p50/p95/p99.

`owner_intents` is the sweep axis. Raw rows derive and retain
`physical_batches = owner_intents` for domain-only and
`physical_batches = 2 * owner_intents` for new-name, so the impossible odd
“physical width” interpretation is not used.

The runner is two-stage. Stage 1 runs every exploratory physical policy plus
targeted boundaries, then evaluates all 144 adaptive configurations using
only those rows. A deterministic rank first removes any rule that fails
exactness, the >=2x eligible syscall reduction, the 5% per-cell
CPU/throughput/p99 limits, or the 10% prepared-write-pipeline gate; it then maximizes the
weighted geometric-mean CPU improvement, breaks a tie by throughput, then by
fewer copied bytes, fewer syscalls, and finally lexicographic configuration.
It freezes exactly one fixed policy or adaptive configuration in a
machine-readable artifact, including all rejected candidates and reasons,
before Stage 2 starts. If no rule clears Stage 1, the verdict is `DECLINED`
and confirmation does not run. Stage 2 runs only `k_pwrite` versus that frozen
winner; other candidates are never selected from confirmation results.

Every Stage-1 candidate/shape comparison is physically paired as one complete
ABBA cycle (`k_pwrite`, candidate, candidate, `k_pwrite`), with the inverse
BAAB order on alternating shapes. All four rows are retained. Thus the strict
5% exploratory regression screen never compares time-separated singleton
observations. A physical comparison is usable only when all four of its rows
are clean and accepted. A rule that needs a missing or noisy natural or
boundary comparison is rejected rather than evaluated from a partial cycle.
Likewise, every prepared-pipeline shape needed by a rule must have one clean,
accepted row. Rejected/noisy rows remain in raw evidence but contribute no
metric and are never replaced.

Tiering removes semantic no-ops before row generation: Stage 1 omits `Os` and
`Group`, physical width 1 needs no selection row because all 144 adaptive
rules fall back below their minimum width, pwritev iovec caps
that cut the same buffers are one physical policy, and copy is omitted above
the fixed 8-MiB retained-scratch ceiling. The manifest records every omitted
logical rule and the measured physical-policy row that represents each
deduplicated rule.

Targeted boundary rows are below/equal/above `64 KiB`, `256 KiB`, `1 MiB`,
`4 MiB`, and `8 MiB`, plus below/equal/above effective 64, 256, and runtime
`IOV_MAX` physical batches where those captured caps are distinct.
They also cover below/equal/above each configured minimum activation width.
Byte-boundary groups use respectively 64, 256, 256, 1024,
and 1024 real domain batches. For a target
of `cap-1`, `cap`, or `cap+1`, the payload budget after each batch's fixed
116 bytes is divided quotient/remainder across those batches, producing the
exact requested group length with every batch legal. Iovec rows use the
reference 24-byte, one-event domain shape and the requested physical width.

### 4.2 Production-envelope confirmation

The frozen BN-2SU important region is confirmation, not tuning:

- payload `24, 250` B;
- batch `1, 10, 100, 1000` events;
- writers `1, 4`;
- `Process` and `Group` for all 32 cells;
- `Os` at batch `1, 10` for both payload/writer values, proving the mandatory
  fallback and exact per-batch barrier count;
- actual group widths are replayed from owner-shaped writer feeds and also
  emitted in raw output, so a throughput change cannot be credited to a
  different gather width.

The Process replay source is the committed production evidence
`spikes/direct_outcomes/BN-21EW-DIAG-GROUPS.csv`, hashed into the manifest.
For each Process source row `(owner_groups=G, owner_plans=P, max_group=M)`, the runner
reconstructs the frozen conservative histogram by assigning
`q = (P-G) / (M-1)` groups width `M`, one group width `1 + remainder` when the
remainder is nonzero, and all remaining groups width 1. It validates that the
histogram has exactly `G` groups, `P` plans, and maximum `M`. The six source
rows are replayed once each by the six paired observations. Writers=1 replays width 1.
Writers=4 maps batch 1/10 to the six batch-10 rows and batch 100/1000 to the
six batch-100 rows; the histogram is payload-independent.

Group uses separate durable evidence: all Group rows from
`BN-21EW-FINAL-ABBA.csv`, plus the targeted b1/b10 repeats in
`BN-21EW-GROUP-PARITY-ABBA.csv`, both hashed into the manifest. For every
four-writer row, `P = 4 * bpw` and `G = barriers` exactly. Source inspection
confirms the recorded counter is incremented only at the owner Group barrier;
segment/header syncs are outside that histogram, so the roughly four extra
barriers are real scheduling/startup owner groups and are not subtracted.
The steady-domain replay excludes first-name registry batches. The runner
constructs the least-skew exact histogram: `G - (P mod G)` groups of width
`floor(P/G)` and `P mod G` groups of width `ceil(P/G)`. This preserves the
observed plans and group barriers without inventing a Process histogram.
Group b1/b10 use both source files; b100/b1000 use the final ABBA file.
Writers=1 remains width 1.

The Os fallback uses the steady-domain work counts from the `Os` rows in
`BN-21EW-FINAL-ABBA.csv` as a width-1 physical-batch trace. It is explicitly a
fallback batch trace, not a measured Os owner histogram.

Each Stage-2 timed observation replays its entire assigned frozen compressed
width histogram inside the release harness and emits one aggregate row. The
six Process ABBA pairs use the six source-row histograms exactly once. Group
and Os source rows are deterministically partitioned round-robin into six
nonempty histograms, one per pair, so all source evidence is exhausted without
repeating the aggregate or breaching the 2-GiB cap.
The harness
records the real latency of every owner group in the trace, so cell p99 is
measured from the aggregate distribution and is never synthesized from
per-width percentiles. ABBA/BAAB therefore compares identical full traces;
the evaluator does not frequency-average per-width ratios.

Confirmation order is paired `A1/B1/B2/A2`, alternating which variant runs
first by repetition. It uses release + thin LTO, fresh preallocated files,
`$MESS_BENCH_DIR` or `$HOME/.cache/mess-bench` on the same ext4 device and a
load-1 quiet guard below 6.0. Ratios are formed only from adjacent baseline /
candidate rows when both rows are clean and accepted; no feed or repetition
may substitute for another. All six adjacent pairs in every confirmation
cell are required for `ADOPT`. Three to five clean pairs may be reported as
diagnostic medians but force `INCONCLUSIVE_NOISE`; fewer than three also force
`INCONCLUSIVE_NOISE` without a decision metric. Rejected/noisy rows remain in
raw CSV with their guard values and are never retried or replaced.

Attempt 2 has one absolute two-hour elapsed deadline measured from the start
of the runner, including preflight and quiet-load cooldown. If it expires,
execution stops and the attempt is `INCONCLUSIVE_TIMEOUT`; it is not restarted.
The runner records the incomplete row/manifests and a machine-readable outcome.
There is no third attempt under this protocol.

The confirmation weights are exact: weight 2 for `Process`/`Group`, four
writers, batch 10 or 100, and payload 24 or 250; weight 1 for every other
`Process`/`Group` confirmation cell. `Os` is excluded from the gain ranking;
it remains mandatory correctness/fallback evidence and must stay within 5%.

### 4.3 Roll and oversize boundaries

Untimed correctness plus targeted timed rows cover:

- a group that ends exactly at segment capacity;
- a batch that forces a roll before itself;
- a group partitioned into nonempty old/new segment runs;
- one batch larger than empty-segment capacity (typed `SegmentFull`, no roll);
- 64, 256, and runtime `IOV_MAX`, each minus/equal/plus one buffer;
- 64 KiB, 1 MiB, 4 MiB, and 8 MiB
  minus/equal/plus one-byte chunk boundaries;
- a 64 MiB legal batch and a rejected over-`MAX_BATCH_LEN` input.

## 5. Fault contract

A scripted vectored sink drives deterministic call results and records every
offset/iovec passed. Tests cover:

- partial progress within the first iovec, exactly on a boundary, across
  several iovecs, and one byte before the final boundary;
- repeated partial progress to completion;
- `EINTR` before progress and after earlier successful short calls;
- zero progress;
- `ENOSPC` and `EIO` before progress and after a partial prefix;
- runtime `IOV_MAX` lower than each configured cap;
- checked offset/length overflow;
- roll partitioning and a fault in the old/new partition.

Successful retry must be byte/receipt/chain/barrier identical. A terminal data
error is never retried except `EINTR`. The harness reports the exact fully
written batch prefix, partial batch (if any), and unattempted suffix. It then
compares current and candidate recovery-visible prefixes. Any candidate fault
behavior that would acknowledge a batch the current path cannot, skip a
required barrier, write past an invalid partial batch, or advance a chain/head
without a complete batch is an automatic `DECLINED`, regardless of speed.

`ENOSPC` is expected to occur at preallocation in production; injected
mid-write `ENOSPC` remains covered so the gathered path does not weaken D8.
`fdatasync` `EIO`/`ENOSPC` behavior is unchanged: no retry, whole barrier group
indeterminate, sticky poison, frozen watermark.

Before any performance row, the same release binary emits a machine-readable
correctness artifact covering every success, positioned-I/O fault, roll,
oversize, chain, recovery, iovec, copy, and executable barrier case above. The runner hashes
that artifact alongside its source, harness, evaluator, and binary hashes and
fails closed if a named case is missing or not `pass`. A test-only binary or
an artifact from a different executable hash is not admissible.

The same binary must first emit an exact contract artifact declaring accepted
command arities, phase thresholds, CSV schemas, and the correctness schema.
The runner hardcodes and checks that contract before correctness or timing, and
its static self-test validates the length of every constructed command.

The standalone spike does not expose the production public cancellation API
and therefore does not claim cancellation coverage through local booleans or
a model. Cancellation-before-admission and admitted-I/O-terminal behavior are
hardcoded prerequisites for the separate production integration bone, which
must execute them through the real committer surface before adoption ships.

## 6. Predeclared admission gates

The important region is the BN-2SU confirmation matrix, with extra weight on
`Process`/`Group`, 4 writers, batch 10/100, payload 24/250 B: these cells form
real multi-batch owner groups without making copies trivially dominant.

`ADOPT` requires all of:

1. exact success parity and every fault/roll/oversize test passes;
2. at least **10% geometric-mean owner-thread CPU reduction** or **10%
   throughput increase** over `k_pwrite` in the weighted important region;
3. write syscalls fall by at least **2x** in eligible important-region rows;
4. no confirmation cell loses more than **5%** throughput or owner CPU;
5. no confirmation p99 exceeds **105%** of paired baseline;
6. `Os` byte/receipt/barrier results are identical and its performance stays
   within 5% because it uses the current fallback;
7. copied bytes are reported and the admitted rule has a fixed retained
   scratch cap no larger than 8 MiB;
8. the prepared-write-pipeline accounting reaches the separately frozen
   **10% pipeline CPU or throughput improvement**, not only a faster isolated
   syscall loop. Full-owner/product value remains an integration-bone gate.
9. every confirmation cell has all six predeclared adjacent pairs clean and
   accepted. Any confirmation noise makes the experiment
   `INCONCLUSIVE_NOISE`, even when the available diagnostic pairs would pass.

If the important region wins but any other eligible sweep region regresses by
more than 5%, the verdict is `DECLINED`. Attempt 2 does not permit a rule
revision, confirmation rerun, replacement row, or additional attempt.

No production fast path is edited by this bone. An `ADOPT` verdict creates a
separate integration bone whose review must re-run production `LogEngine`
Process/Os/Group, roll, chain, recovery, cancellation, barrier-parity, and
BN-2SU gates. This spike is evidence for that work, never a substitute for it.

## 7. Provenance to capture with raw rows

Every CSV row carries: git commit and dirty flag, harness SHA-256, UTC time,
hostname/kernel, CPU model/topology/governor, rustc/LLVM, build profile,
filesystem/mount/device/model/scheduler, free space, page size, runtime
`IOV_MAX`, scratch root, load averages before/after, corpus seed/shape, variant
policy, repetition/order, and acceptance/rejection reason.

Reference-host facts observed while predeclaring (not benchmark results):

```text
kernel: Linux 7.0.12-arch1-1 x86_64
cpu: AMD Ryzen 9 3900X, 12C/24T, 64 MiB L3
rustc: 1.97.0 (LLVM 22.1.6)
scratch filesystem: /dev/nvme0n1p3, ext4
device: Samsung SSD 970 EVO Plus 2TB; scheduler none
page size: 4096
IOV_MAX: 1024
```

Raw CSV and compact text provenance are committed. Large `perf.data`, segment
images, and scratch directories are not.
