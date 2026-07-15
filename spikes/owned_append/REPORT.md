# Public owned-append measurements

## bn-22it final decision — ADOPT

The narrowed Process-only owned-append candidate is admitted for production
integration. A fresh, complete 80-row comparison passed every per-cell,
logical, provenance, source, build, and material-benefit gate. The runner then
released the global measurement lease and a separate verifier validated the
complete terminal chain. The final outcome is `ADOPT`; no historical or
partial row contributed to it.

The exact measured source graph was:

- reviewed tooling baseline
  `fa6bc0cc2d533a9ef5e9fb54755007e2287ad28d`;
- neutral control `0890cec2d734b047a71c3236db16faaeb38654df`;
- candidate `e5c3da658abd619c240851d384642fe741d93047`; and
- canonical source approval SHA-256
  `d86203d0b1397d0ee0c5f5d44ee9c5fe09a6e50e57361ee8408f4017ca12fece`.

The five-cycle median ratios were:

| Process batch | throughput B/A | p99 B/A | allocation calls B/A | allocated bytes B/A |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1.0232 | 0.9651 | 0.7590 | 0.8121 |
| 10 | 1.2570 | 0.8131 | 0.4422 | 0.6635 |
| 100 | 1.2336 | 0.6440 | 0.5389 | 0.8999 |
| 1000 | 1.4543 | 0.5727 | 0.5050 | 0.9132 |

Across all four batch sizes, the geometric mean was **1.2325x throughput**,
**0.5498x allocation calls**, and **0.8158x allocated bytes**. In plain
language, the public Process append path was about 23% faster overall, made
about 45% fewer allocation calls, and allocated about 18% fewer bytes. Every
individual cell also stayed within the frozen throughput, p99, allocation,
counter, and correctness limits.

The admitted scope remains intentionally narrow. `EventStore` constructs and
forwards owned input, and `LogEngine` consumes it directly under
`Durability::Process`. `Durability::Group` and `Durability::Os` use the
borrowed-compatible path and are admitted on semantic and structural parity,
not on a timing claim.

The retained evidence snapshot is under `adopt-20260715/`. Its decisive
bindings are:

- prepared pair SHA-256
  `fd5603a9a6c1a70c55a61ecbf296405464dd812ffd5c3117de7b641834256fff`;
- final CSV SHA-256
  `5715cf6883c8561560770f70181b50dcdab9cb66b239ef9a4552e8c1377f0d3b`;
- evaluator result SHA-256
  `6714401d9db3b6c329ff299f772f757d17e9352ce8ca18516feb83db8edd9086`;
- terminal SHA-256
  `94e1660c9e76c01508f7f43a63df6c2f48e3acdfea18a9e2a7b52984678041b1`;
  and
- terminal-verification SHA-256
  `67e1ad0dc0c2e235727c25fed76de9634a4377c490b695415f50e846e76b5fe3`.

The immediately preceding retry completed 80 rows but was classified
`INVALID_EVIDENCE` because the evaluator mistakenly reapplied a string-only
external-path validator to an already validated `Path`. Its rows were never
used for admission. The evaluator boundary was repaired, self-tested,
reviewed, and frozen in the tooling baseline above before the source graph was
reapproved, both binaries were rebuilt, and this comparison started from row
zero.

Any commit after the measured candidate must contain documentation and retained
evidence only until the candidate is merged. Product integration is authorized
only because the exact measured candidate remains its ancestor and the public
correctness matrix is rerun after merge.

## Historical bn-2yye decision status

At the end of bn-2yye, the owned path was not admitted as integrated. The complete
mode-independent Attempt 5 failed two locked Group cells, and its narrower
Process-only successor did not produce a decision: Attempt 6 fail-stopped on
its frozen exact-name process guard after 69 of 80 rows. Attempt 6 is
`INCONCLUSIVE_INFRASTRUCTURE`; it authorizes neither admission nor integration,
and its partial rows may not be completed, retried, substituted, or used as
acceptance evidence. The older rows in `owned_append_results.csv` are likewise
retained only for auditability: their control predates direct owner outcomes,
and the candidate rows are an incomplete Process-only diagnostic. No
historical or partial row may be carried into another comparison.

The repository disposition is evidence-only. All production, manifest,
example, and test changes from the unadmitted candidate were restored to the
current `main` tree; no dormant owned-append branch remains in `Backend`,
`EventStore`, `FjallSnapshotBackend`, or `LogEngine`. This report, its frozen
tools, and raw artifacts preserve what was tried without integrating it.

Correctness is measured separately through the real product composition. The
focused suite drives public append, command, and cached-command calls through
`EventStore -> FjallSnapshotBackend -> LogEngine`; verifies zero defensive
copy counters, borrowed-adapter compatibility, registry/alias authority,
empty/conflict/oversize ordering, cancellation after a fresh-name ownership
transfer, and live plus reopen reads; and runs prepared chain/roll/cache and
exact owner-cohort proofs through both borrowed and owned submissions.

## Predeclared Process-only successor

Attempt 5 permanently rejects candidate `e25c98610db53ae8fb3375760be741576cea932e`
as a mode-independent optimization. Its Group/b1 and Group/b10 failures may
not be waived, reinterpreted into a pass, or replaced by another run of the
same executable. The useful result is narrower: all four Process cells passed,
with exact ownership-transfer counters and large allocation wins, while the
failed Group timing and barrier-delta gates were coupled to device/order and
cohort-quantization noise.

The successor is therefore a genuinely new, Process-only fast-path candidate:

- `EventStore` still constructs an `OwnedAppendBatch`, and
  `FjallSnapshotBackend` still forwards the owned submission unchanged.
- `LogEngine` consumes that ownership directly only under
  `Durability::Process`.
- `Durability::Group` and `Durability::Os` materialize the owned batch into the
  existing borrowed compatibility path before validation/enqueue. Their scope
  is semantic and structural parity, not an owned-path performance claim.

No Attempt-5 timing row is carried forward. After the implementation and all
correctness checks are frozen, build one control and one candidate release
binary before any row. Run only
`Process x {1, 10, 100, 1000}` with five counterbalanced cycles per cell: odd
cycles `A,B,B,A`, even cycles `B,A,A,B`. This is 20 rows per cell and 80 rows
total. Retain every completed row; the existing process/load guards,
provenance, fixed work, payload size, writer count, fresh ext4 store, and
cycle-median calculation remain unchanged.

`run_paired.sh` and `evaluate.py` are the executable form of this successor
contract. The runner physically emits ABBA/BAAB order, refuses an existing
output directory (which must be outside both source workspaces), and pins both
source HEADs, clean worktrees, binaries, identical harnesses, Cargo.lock,
runner, and evaluator before every row. The evaluator requires the same
physical 80-row order rather than merely grouping row labels after the fact.
After row 80 the runner freezes the final CSV SHA-256 and row/column
cardinality into the mandatory provenance artifact. The evaluator accepts both
artifacts together and revalidates every frozen source, binary, harness,
lockfile, runner, evaluator, and CSV hash against the current files. Thus rows
or provenance copied from Attempt 5 cannot satisfy the successor protocol.

### Neutral measurement-only control

Control checkpoint `97f73c0d4e478bab1a13f22c8dbd8bbd502393c1` is exactly
parent `e26584e8` plus measurement instrumentation; it deliberately retains
the old borrowed behavior. Its tracked delta is limited to:

- the same seven-field `AppendInputMetrics` snapshot API and relaxed atomic
  counters as the candidate, incrementing borrowed batch/record traffic for
  every submission and defensive-copy record/byte traffic only on the existing
  small-batch clone branch;
- a re-export of that metrics type and the byte-identical benchmark harness
  (`78f4a65e897b59eb2e46f76ff6ade738e67af317c1d0cdd170d848951ca18eff`),
  which now reads counters rather than fabricating control values; and
- the candidate's `smallvec` manifest entry solely to keep dependency
  resolution and the ignored workspace Cargo.lock byte-identical
  (`4ba9ac66a698ac043852f33cb2c9a05659fdc548d54d871dc9f91556572436d0`).

It adds no owned backend method, facade forwarding, owner intent, validation,
or queue behavior. The measured control still executes
`DomainInput::Records(records.to_vec())`; owned counters remain exactly zero.

Every Process cell must independently satisfy all of these frozen gates:

- throughput B/A at least 0.97;
- append p99 B/A at most 1.10;
- allocation calls/event B/A at most 1.05;
- allocated bytes/event B/A at most 1.05;
- zero fsyncs in every row;
- candidate `owned_batches == measured appends`,
  `owned_records == measured events`, and
  `owned_payload_bytes == measured events * 250`;
- candidate borrowed-batch, borrowed-record, copied-record, and copied-byte
  deltas all exactly zero;
- exact event/batch totals and every logical harness assertion pass.

In addition, the candidate must demonstrate a material end-to-end benefit
across the complete Process workload rather than merely avoid regressions. The
geometric means of the four per-cell ratios must satisfy all three gates:

- throughput B/A at least 1.10;
- allocation calls/event B/A at most 0.90; and
- allocated bytes/event B/A at most 0.90.

As a non-admission calibration only, the rejected Attempt-5 Process ratios
have geometric means `1.247591`, `0.550053`, and `0.816369` respectively.
Thus the material gates distinguish the previously observed effect while
leaving substantial headroom for an independently instrumented control; no
Attempt-5 row or executable is carried into the new decision.

Group and Os are deliberately outside the timing decision. Their mandatory
gates are correctness, registry/error-ordering/cancellation equivalence, and
exact structural durability parity only. A fresh public append that introduces
an event type emits registry plus domain batches: Group covers both with
exactly one group/fsync, while Os issues exactly two groups/fsyncs. Once names
are primed, a serial hot append is exactly one group/fsync in both modes, and
one admitted 16-batch Group cohort remains exactly one group/fsync. No Group
or Os throughput, latency, allocation, or barrier-count timing row can admit
or reject this narrowed candidate.

## Rejected mode-independent comparison (Attempts 1-5)

The rejected mode-independent study compared two release binaries built before
any row:

- A: current production main `c7ba3b9fbc65`, plus a measurement-only copy of
  the one-cell harness. Its public `EventStore` path remains borrowed.
- B: the `bn-2yye` candidate checkpoint containing the owned path and the same
  timed workload.

Both variants instantiate the production composition
`EventStore -> FjallSnapshotBackend -> LogEngine`, use a fresh ext4 store per
observation, four writers, 250-byte event payloads, and the exact matrix
`{Process, Group} x {1, 10, 100, 1000}`. The work per writer is fixed at
40,000/12,500/2,500/250 for Process and 800/500/300/100 for Group.

One harness invocation runs one cell. Task allocation, batch clones, stream
names, and latency-vector capacity are prepared before allocator sampling;
the tasks rendezvous before the sample and start together. Committer and input
counters are snapshotted immediately before release, so batches, groups,
barriers, and boundary traffic are deltas over the measured region rather than
warm-up totals. From Attempt 5 onward, Group cells also run four matched
four-writer concurrent warm-up rounds before those snapshots, preventing the
serial registry warm-up's adaptive gather target of one from making a single
start/end group decide the steady-state barrier gate.

For each cell, run five counterbalanced cycles. Odd cycles use `A,B,B,A` and
even cycles `B,A,A,B`, producing ten observations per variant/cell. Before
each observation require no foreign `cargo`, `rustc`, linker, or benchmark
process and load1 below 6. Retain every completed row. If either harness
reports `fsync_degraded=true` (the engine's fixed 50 ms barrier alarm), retain
the row and its full throughput/p99 penalty in the five-cycle medians. An
invocation/process guard failure still fail-stops the entire study: do not
retry, replace, or infer from the partial timing rows.

Record source commit, dirty state, harness and binary SHA-256, Cargo.lock
SHA-256, toolchain, kernel, CPU, governor, scratch filesystem/device/free
space, timestamp, exact order, pre/post load, throughput, append p50/p99,
allocation calls/bytes, every append-input counter, and exact
batches/groups/fsync deltas.

## Rejected comparison's locked gates

The historical evaluator used the median of each cycle's two B observations
divided by the median of that cycle's two A observations, then the median of
the five cycle ratios. Gates applied independently to every cell; none were
waived or widened after seeing results.

- Process throughput: B at least 97% of A.
- Group throughput: B at least 90% of A.
- append p99: B at most 110% of A.
- batch 1 and 10 allocation calls/event and bytes/event: B at most 95% of A.
- batch 100 and 1000 allocation calls/event and bytes/event: B at most 105%
  of A.
- Process fsync delta: exactly zero in every row.
- Group: B's aggregate fsync count at most 100.25% of A's for each cell, and
  the median cycle-level B-minus-A fsync delta no greater than zero. The small
  aggregate equivalence band is bounded by the observed scheduler granularity;
  the cfg(test) cohort proof remains exact at 16 batches -> 1 group/fsync for
  both paths.
- Every B row: `owned_batches == measured appends`,
  `owned_records == measured events`, `owned_payload_bytes == events * 250`,
  and all borrowed/copy counters exactly zero.
- Every row: exact event/batch totals and all logical assertions pass. Degraded
  rows are counted by variant/cell and remain in the timing medians.

Any failed cell declines the candidate as currently integrated. A passing
matrix authorizes code review; it does not replace the correctness suite.

## Attempt 1 — invalidated, no timing inference

The first contemporary invocation retained 105 completed rows: all 80 Process
rows, all 20 Group batch-1 rows, and the first five Group batch-10 rows. It
then fail-stopped exactly as predeclared when candidate cycle 2 slot 1 for
Group batch 10 reported `fsync_degraded=true`: one `fdatasync` took 54.2 ms,
crossing the engine's fixed 50 ms operational alarm. Its row recorded append
p99 33.8199 ms and the cumulative fsync histogram p99 33.030144 ms.

The raw CSV, log, and provenance are retained under
`final-20260714/`. No completed row, including the complete Process half, is
used for acceptance or carried into another attempt. The runner's governor
probe also emitted an ambiguous-redirect warning before row 1; it did not
affect execution, but the empty provenance field is preserved and the probe
is corrected before any independent attempt.

Artifact SHA-256: `paired.csv`
`74488d4bfb7aff6daabce42be995d53b79f357ff9c4729eb9c1c5700c8192e42`,
`run.log`
`809aa4b6a20b6bf81ac6f00facf7437e95d2c1ed80102140b8993a4cdfac08b0`,
and `provenance.txt`
`f828413e377f85efbc046dddb2e7c3dc01eaf16401dd089966d485013570ba58`.

## Attempt 2 — invalidated, no timing inference

The second independent invocation started from zero rows and retained 22
completed Process rows: all 20 batch-1 rows and the first two batch-10 rows.
Candidate batch-10 cycle 1 slot 2 completed with post-run load1 6.21. The
outer runner then fail-stopped before slot 3 because its precondition checked
load only once, even though the harness's already-frozen quiet guard was
designed to wait up to 120 seconds. No third row started.

All Attempt-2 rows are excluded. Before another independent attempt, the
outer guard is corrected to wait up to 120 seconds for the same load1-below-6
precondition. This changes neither a timing row nor an acceptance threshold:
the next binary is still never invoked above the threshold, and failure to
reach it within the bound still fail-stops without a row.

Artifacts under `attempt2-20260714/`: `paired.csv`
`169085a3c5e62bb709c194c804fa721436e050f8445119c85b1cf1e9404a40d4`,
`run.log`
`01ad885cf9e4756f302a8431096061ce405688d2513a3ccc57e8e5e0939c6cfc`,
and `provenance.txt`
`2e511b6a384670706f74c953518a69dcef876d32cc5eb7bad525f7b8987a03f7`.

## Attempt 3 — invalidated, no timing inference

The third invocation used the bounded load wait and completed all 80 Process
rows. The fifth Group row, candidate batch-1 cycle 2 slot 1, then crossed the
same operational threshold: a 53.9 ms `fdatasync`, with row append p99
32.1811 ms and cumulative fsync p99 31.981568 ms. It fail-stopped with 85 rows
retained and no timing inference.

Artifacts under `attempt3-20260714/`: `paired.csv`
`a96543cd548dd8f859616c45772ead56a68b416a15390ced095042a932b901c6`,
`run.log`
`ef243cbd2820af2f66e5c0fe4a4e5a08274e36e2a8c9a29f4fcd39d8b4f4dcec`,
and `provenance.txt`
`6bdb5b51a9b37c53a9e4715d70393b64a7c56fcdca1f2c372d67a79e934c0b3d`.

## Protocol revision before Attempt 4

Attempts 1 and 3 show that a single 50 ms barrier is a recurring property of
this shared, 85%-full ext4 device, not a rare admission tool failure. Making
one sticky alarm invalidate 160 rows prevents either variant from being
measured and conflicts with the historical policy that retained the control's
66.2 ms barrier outlier. Attempt 4 therefore starts from zero rows and retains
every alarmed row; its throughput and p99 penalty counts normally in the
frozen five-cycle paired medians. No row is retried, replaced, or removed.

The Group aggregate-count gate is also expressed as a 0.25% equivalence band
plus a non-positive median paired-cycle delta. Partial Attempt-1 batch-1 rows
showed natural +/-2 group variation around roughly 800 barriers per row even
though the deterministic test proves exact semantic parity. This is a noise
model correction, not a production-semantic waiver: exceeding either the
band or paired delta still fails the candidate.

## Attempt 4 — complete, candidate not admitted

Attempt 4 completed all 160 rows without a durability alarm. Seven of eight
cells passed. The literal evaluator result was:

| mode | batch | throughput B/A | p99 B/A | allocations B/A | bytes B/A | median fsync B-A | verdict |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Process | 1 | 1.038882 | 0.998008 | 0.758847 | 0.812071 | 0.0 | pass |
| Process | 10 | 1.248456 | 0.811243 | 0.442847 | 0.664498 | 0.0 | pass |
| Process | 100 | 1.249996 | 0.774807 | 0.538744 | 0.899880 | 0.0 | pass |
| Process | 1000 | 1.541280 | 0.755206 | 0.504986 | 0.913238 | 0.0 | pass |
| Group | 1 | 0.993808 | 1.079132 | 0.762460 | 0.810323 | -0.5 | pass |
| Group | 10 | 1.003440 | 0.996220 | 0.444052 | 0.665757 | +0.5 | **fail** |
| Group | 100 | 1.026615 | 0.936225 | 0.538583 | 0.899005 | -0.5 | pass |
| Group | 1000 | 1.090351 | 1.015216 | 0.504843 | 0.913171 | -3.0 | pass |

Group batch 10 used 5,009 candidate barriers versus 5,004 control barriers,
inside the 0.25% aggregate band but failing the separately locked
non-positive median paired delta. The candidate is therefore not admitted from
Attempt 4.

The harness diagnosis is source-derived and independent of which variant won:
the only pre-sample writes were serial registry/name warm-ups, leaving
`FlatOwner::target == 1` immediately before a four-writer Group workload. A
one-group start/end quantization then dominates a gate whose steady state is
roughly 500 groups. Before Attempt 5, both harnesses add four concurrent warm
rounds outside every measured counter/timer; all acceptance gates remain
unchanged and no Attempt-4 row is carried forward.

Artifacts under `attempt4-20260714/`: `paired.csv`
`d4d56229884e4909800a3318759151be4e6d2942704b0b2172e1e93ac98d07a2`,
`run.log`
`0c31ec595952a2d1b5d19834e494378409c84bab1c752d0e5296d7315b01a05b`,
`provenance.txt`
`7e276461c4a8eb2c486e78379b2ef9c7aee78d27e68c453b4d729dc4263dd893`,
and `evaluation.txt`
`b4e49f339975096f2f496d3d5b0aca97d2e3a5f9f64562958319fde5a6b5b55d`.

## Attempt 5 — complete, candidate not admitted

Attempt 5 started from zero rows with the matched concurrent Group warm-up
described above and completed all 160 rows. Six of eight cells passed. The
literal evaluator result was:

| mode | batch | throughput B/A | p99 B/A | allocations B/A | bytes B/A | median fsync B-A | verdict |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Process | 1 | 1.015587 | 1.006524 | 0.758974 | 0.812522 | 0.0 | pass |
| Process | 10 | 1.199389 | 0.836018 | 0.443383 | 0.665199 | 0.0 | pass |
| Process | 100 | 1.268585 | 0.725932 | 0.538655 | 0.899862 | 0.0 | pass |
| Process | 1000 | 1.567806 | 0.782075 | 0.505011 | 0.913238 | 0.0 | pass |
| Group | 1 | 0.992296 | 0.986541 | 0.762366 | 0.810231 | +0.5 | **fail** |
| Group | 10 | 0.980268 | 1.118757 | 0.443905 | 0.665710 | +0.5 | **fail** |
| Group | 100 | 1.002703 | 0.982384 | 0.538566 | 0.899041 | -1.0 | pass |
| Group | 1000 | 1.107284 | 0.819424 | 0.504843 | 0.913176 | -1.5 | pass |

Group batch 1 failed only the locked non-positive paired barrier-delta gate.
Group batch 10 failed that gate and the p99 gate. Its candidate cycle 3 slot 2
row retained a 58.8 ms `fdatasync` alarm, 58.4953 ms append p99, and its full
throughput penalty exactly as predeclared; it was not retried or removed. The
candidate used zero boundary copies in every row and passed every exact
logical/counter assertion, but those wins do not waive either Group failure.
The owned path is therefore not admitted as currently integrated.

The candidate and control source checkpoints were respectively
`e25c98610db53ae8fb3375760be741576cea932e` and
`f712b03d123855f72e37b010cc621fc58709f1f5`; their release-binary SHA-256
values were `c2eb42200cbbbe0b410799e410ac2678fce7179e4f3f83b395b3a3dd5c989f85`
and `0fe4bf83a71fa875e400704597c93b4e0bf3856fc6af45aff74114b568b7d705`.

Artifacts under `attempt5-20260714/`: `paired.csv`
`e126100f63a9d4268cc0f63ac73b550be8f62d1f0547766fa8dbf0bb6d387175`,
`run.log`
`2b5a5ff1ae5147394ec8c96f7f270b7e45c7127e53866eeabb11cb2c867bd4ee`,
`provenance.txt`
`3cd4e040ba2a717e1827b341203d6a332605f0f6a002de91001dca99a41b9fff`,
and `evaluation.txt`
`396d6b6ed549fe67cdc01914d5d05d4bd24794519c5e99089cd6ddad57731964`.

## Attempt 6 — infrastructure-inconclusive, no admission

Attempt 6 froze the Process-only successor at candidate source
`75694cb483844cbd18f1e1b0ee4164adf278ef58` and neutral borrowed control
`97f73c0d4e478bab1a13f22c8dbd8bbd502393c1`. Their release binaries were
respectively
`e11d8951b7735b8b3f8b3dac16f9902e5768da877b3ba1b92f1245a5214bdf08`
and
`b11e3c7fea9edc40b76d732c29ae26de922e5a4dcab14c6f947ee25f8734e29f`.
Before row zero, the candidate passed all 20 focused correctness/structural
tests and the control passed all six applicable structural tests. Both
worktrees were clean, both release builds succeeded, the harness and lockfile
hashes matched, and the runner's static physical-order check covered exactly
80 rows.

The runner retained 69 rows: all 20 rows for each of batches 1, 10, and 100,
then nine rows for batch 1000 through control cycle 3 slot 1. Before batch 1000
cycle 3 slot 2, the pre-row guard reported an exact-name match for `cargo`,
`rustc`, `cc`, `ld`, `collect2`, or `owned_append_bench` and fail-stopped with
exit code 3. No such process remained in the immediate post-mortem probe, so
the matching executable is unidentified. The guard recorded neither the
matching PID/`comm`, process start time, parent, nor command line before the
match vanished. The bn-1zv6/pwritev independent reviewer did run `cargo fmt`,
tests, and Clippy from 19:17:50 through 19:18:04 UTC, but its exact activity log
records the last Cargo completion at 19:18:04.889, about 21.1 seconds before
the owned runner's approximately 19:18:26 fail-stop; those checks therefore did
not overlap the guard match. The lead's later full `just test` began only after
the owned timing slot was released. The evidence still cannot distinguish some
other genuinely foreign workload from a transient or spurious exact-name
match. A future experiment guard should snapshot process identities before
fail-stop; this observability gap does not authorize a retry of Attempt 6.
Every completed row remains retained; none was retried, replaced, or removed.

Because row 70 never started, the runner correctly did not append the final
CSV SHA/cardinality fields to `provenance.txt`, did not invoke the evaluator,
and did not produce `evaluation.txt`. The 69 rows include useful diagnostic
signals—exact candidate ownership/copy counters, zero fsyncs, and complete
batch-1/10/100 timings—but the frozen evaluator requires all 80 physical rows
plus the final CSV binding. Those partial signals are explicitly
non-admissible: no per-cell or material gate verdict is inferred from them.
Attempt 6 is therefore `INCONCLUSIVE_INFRASTRUCTURE`, with no admission, no
integration, and no retry.

Raw artifacts under `attempt6-20260715/`: `paired.csv`
`8a7bb00d356812382b1fa49b4670b13d19d443d912b12719bc1999b4e9e69069`
(69 data rows, 32 columns), `run.log`
`90c1452d1096ad6cec51dbaf2e1332b26c2393b3c9c612bb1937da2909234284`,
and `provenance.txt`
`53a4003c462e747862f2cdb95b633e8ff083404b4a1a497db28f4d4cb2f063f7`.

## Historical rows

The original three-row control medians and preliminary candidate rows remain
in `owned_append_results.csv` and `before.log`/`after.log` for auditability.
They explain the original opportunity (small borrowed submissions cloned
message names plus payloads) but are explicitly excluded from the current
decision because direct outcomes changed owner allocation/reuse behavior after
that baseline and the preliminary candidate never completed Group or the full
Process matrix.
