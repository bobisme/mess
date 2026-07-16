# bn-2l3n Asterism production rebaseline protocol

## Status and decision boundary

The canonical protocol identity is `bn-2l3n-asterism-rebaseline-v3`.

This is the frozen pre-build contract for `bn-2l3n`. No benchmark binary,
correctness build, timing row, or profile row may be produced until this
document and the measurement tooling that implements it have received an
independent risk-high review. Tooling may implement this contract; it may not
change a source checkpoint, workload, order, statistic, gate, or outcome after
the first build starts. Any such change creates a new protocol version and
invalidates every prepared artifact and row from this version.

Version 3 supersedes the independently approved version-2 document at
SHA-256 `1d56a91c26b6d06c045850d13474a563811c1a07e87146818849b3753c3409f6`
before any rebaseline correctness build, benchmark build, timing row, or
profile row was produced. Its only measurement-contract changes are the
`A`/`B` source binding below, the source-path completion boundary that binding
contains, the exact names of already-nongating fairness diagnostics, and the
historical `C` structural metadata classification. All workloads, orders,
statistics, thresholds, correctness gates, and terminal outcomes are
unchanged.

The newly bound current source releases an accepted intent's byte permit
before sending its terminal one-shot notification. Append completion is the
ownership boundary: once success, conflict, empty-append, backend failure, or
a later sentinel after an abandoned receiver is observable, the completed
intent's slot and byte permits are already reusable. Permit release is
therefore inside `EventStore::append` completion and inside the measured
current public source path; it is not post-completion cleanup that the runner
may exclude. After the final append completion is observable, the exported
quiescent owner intent-slot and byte-permit occupancies are deterministically
zero rather than merely expected to drain eventually. This clarifies the
already-required exact-release gate; it adds no workload or threshold.

Every version-2 rebaseline tooling constant and protocol-versioned tooling or
evidence schema is invalid, including any version-2 source plan, source
approval, resolution-only `r5` lock candidate, binary contract, prepared
artifact, or runner/evaluator checkpoint. All
`bn-2l3n-*-v2` and `asterism-rebaseline-*-v2` schemas owned by this rebaseline
must become their corresponding version-3 identities, be regenerated from
this document, and receive fresh independent risk-high review before any
build. No rebaseline benchmark, correctness, profile, or timing evidence
exists from version 1 or version 2; contaminated compiler output from an
aborted version-2 tooling review is non-evidence and cannot be adopted.

The question is deliberately end to end: does the current public storage
composition preserve the completed flat-owner win, improve materially on the
Fjall-era production engine, and quantify the remaining gap to the freshly
built bare log? A component-only table, synthetic Fjall point lookup, direct
`LogEngine` call, spike-only owner, or previously printed number cannot answer
that question.

The smallest complete evidence set is:

1. one 32-cell stable-stream matrix over four exact source variants;
2. one focused all-new-name matrix;
3. one focused 64-writer fairness matrix;
4. allocation and CPU accounting in the primary rows plus syscall/CPU profiles
   for four sentinel cells; and
5. one focused public reopen/RSS comparison; and
6. public-composition correctness, cancellation, roll, recovery, and fault
   gates.

The only terminal outcomes are `ADMIT`, `NARROW`, `REVERT`, and
`INCONCLUSIVE`, as defined below. This study does not adopt a new optimization:
it accepts, narrows, or rejects the integrated Phase-4 baseline and writes the
budget against which later Asterism work is judged.

## Exact variants and source binding

Variant letters and order are fixed:

| id | variant | product commit | product tree | timed surface |
| --- | --- | --- | --- | --- |
| `A` | `current-public` | `d644dc583dfe6a3d2cd07e71ce0212a323875ab4` | `205d853905bdb648ee997900c6aef24a323aa380` | `EventStore<FjallSnapshotBackend<LogEngine>>`, snapshot policy off, using the public batch append surface |
| `B` | `current-bare` | `d644dc583dfe6a3d2cd07e71ce0212a323875ab4` | `205d853905bdb648ee997900c6aef24a323aa380` | raw `mess-log` `Committer`/`Appender`, numeric IDs, no registry or `mess-store` validation |
| `C` | `fjall-public` | `f0ab89e92e44253f8fe48cf19d7a93e39263585b` | `7ca2228fcd1c65ef942da35144fcf507d8a72e12` | the same public composition, with the baseline-gen2 Fjall-era `LogEngine` |
| `D` | `flat-public` | `69b95604b9e7c924314cf7d82b86a2edb7dccde6` | `f738cde2b5414d1a2926ac86573163d505754ba9` | the same public composition at the integrated optimized BN-2SU checkpoint |

`C` is the production Fjall-era comparator. Synthetic `MetaStore` head/dedupe
lookups and the RocksDB engine in `vertical_slice` are orientation evidence
only and are prohibited as substitutes. `D` is the integrated checkpoint whose
tree contains the optimized source measured at
`095460b7a597327662da9971dbdd4a209e49a3c1` (tree
`40f1072cf176bd8d153e00415c03d675481f28c9`) plus only the two-line BN-2SU
report correction; `git diff 095460b7..69b95604` must remain limited to that
report. The earlier `b583fd01` source is the report's explicitly superseded
pre-optimization checkpoint and is forbidden as `D`. `A` includes direct
outcomes, owner-ring fairness/publication fixes, the admitted Process-only
owned-input path, read-only reservation-occupancy metrics, and the
release-before-terminal-notification completion ordering required by the
frozen fairness gate. The occupancy seam adds no work to append admission,
ownership, publication, or durability; it is sampled only after the measured
window at quiescence. The permit release itself remains in the append
completion path and measured source boundary. `B` must be compiled anew from
the named current source;
historical bare rows may be shown in the report but may not enter a ratio or
outcome.

The public variants must construct a fresh `LogEngine`, wrap it in a fresh
`FjallSnapshotBackend`, and drive it through `EventStore`. Snapshot policy is
off so the wrapper is a real pass-through composition without timed snapshot
writes. Direct `LogEngine::append_batch`, spike `FlatEngine`, and benchmark-
private publication entry points may be used only by non-admission diagnostics
clearly separated from the evidence files.

All four variants use the same byte-identical workload generator, payload
generator, clocks, allocator counter, latency collector, logical digest, CSV
schema, and runner protocol. Source-version adapters may contain only the
minimum API spelling and generation-specific expected-position normalization.
Their complete diffs and hashes require independent review. In particular,
performance/comparator adapters may not bypass `EventStore`, pre-intern names
on one public variant only, change durability, or patch product code. The sole
product-source overlay is the separately built, `cfg(test)`-only current fault
hook defined in the correctness section; it is never linked into these four
performance binaries.

Only `A`/`B` track a root `Cargo.lock`; they use its exact SHA-256
`9c24189940d9b43d7798c6680c8aeab6ddc270ef9b450390334d9327405cbea0`.
`C` and `D` use separately frozen comparator root locks: their historical
spike-lock inputs have SHA-256
`ab405326315f1be1782feaf97b6e1c031f24480f32dc5532bfccdeb425ed3c97`
and `c419b2f347aa384f695611003ff764d63da77fd3fb87615ced2e6aa099e3aea7`
respectively, but are not assumed to resolve the new root overlay unchanged.
Before any build, the tooling stage must retain the expected failure or success
of the current-lock offline check, generate each required comparator root lock
offline with the frozen toolchain, record the exact resolver command and
complete current/historical/final dependency diff, and obtain independent
approval of both canonical lock SHA-256 values. No common-lock requirement may
force a historical manifest change. Timing cannot begin with an untracked,
mutable, or Cargo-generated-at-build lockfile. Every actual build is
`--locked --offline` and rechecks its approved lock hash before and after
compilation.

The benchmark implementation and adapters do not yet exist in this protocol
commit. Before any build, a separately reviewed tooling checkpoint must bind
their exact commit/tree, every file hash, adapter allowlist and binary patch,
runner/evaluator hashes, and this document's SHA-256. That checkpoint may only
realize the frozen contract above. This is an implementation task, not an open
measurement decision. The default architecture is one overlaid `mess-store`
example shared byte-for-byte by public `A`/`C`/`D` and one overlaid `mess-log`
example for bare `B`, without manifest or production-source changes. Shared
workload, payload, clock, allocator, latency, digest, and schema modules must be
byte-identical. Reviewed compile-time adapters may expose `A` input counters
and `C` Fjall metadata barriers, but may not alter the timed call graph.

### Harness derivation, not evidence reuse

The tooling implementation reuses reviewed mechanisms, never old timing rows:

- `baseline_matrix` supplies the 32-cell shapes, work counts, payload generator,
  latency warm-up rule, and segment-size rule; its direct-`LogEngine` rows are
  historical orientation only, never an absolute gate for the fresh public
  topology;
- `owned_append` supplies the public composition, exact input/copy counters,
  read-only source materialization, sequential attested builds, single-use
  prepared artifacts, global lease, exact `/proc` guard, child reaping,
  provenance replay, evaluator, and post-release terminal verification;
- `direct_outcomes` supplies allocation accounting, serialized-thread CPU
  accounting, Process/Group barrier metrics, and the aggregate-plus-paired
  barrier comparison;
- `flat_combined_append`, `perf_append`, and `composed_decision` supply the
  producer-topology interpretation and the rule that bare is a lower bound,
  not a headline universal gate;
- `group_publication`/`publication_v2` supply the counter warm-up and per-writer
  completion/fairness diagnostics, but their rejected candidate is not a
  comparator; and
- `pwritev_group` supplies the requirement to smoke every exact subprocess
  transition and the prohibition on inferring a gathered-write win from
  partial rows.

The old baseline runner's fixed load polling, unsealed live-worktree build,
time-separated rows, synthetic Fjall point reads, best-of-N decision, and any
rejected/inconclusive spike rows are explicitly not reused.

## Primary 32-cell matrix

Every primary cell is the Cartesian product:

```text
durability:  Process, Group::default()
payload:     24 B, 250 B of deterministic non-compressibility-neutral bytes
batch:       1, 10, 100, 1000 domain events per public append
writers:     1, 4
stream:      one stable stream per writer; one stable event type
readers:     none during the timed interval
```

This is 32 workload cells. Each variant runs once in each of four balanced
blocks, for 16 physical observations per cell and 512 primary rows total. The
four fixed Williams sequences are:

```text
block 1: A B D C
block 2: B C A D
block 3: C D B A
block 4: D A C B
```

Every variant occupies every ordinal once and every ordered adjacent pair
occurs once. Cells execute in a seeded, recorded order; blocks 2 and 4 traverse
the cell list in reverse to counter device-age drift. No row may be reordered
afterward merely to make a pair look like ABBA. Four observations per variant
exceed the required median-of-three while remaining the smallest fully
balanced four-variant design.

Work per writer is inherited exactly from the locked baseline:

| batch | Process batches/writer | Group batches/writer |
| ---: | ---: | ---: |
| 1 | 40,000 | 800 |
| 10 | 12,500 | 500 |
| 100 | 2,500 | 300 |
| 1000 | 250 | 100 |

Each observation creates and opens a fresh store on real ext4. Segment size is
`next_power_of_two(max(256 MiB, 2 * events * (payload + 96) + 1 MiB))`, matching
the locked no-roll append matrix. Store creation, runtime creation, and open
are outside the timed interval. Initial stable stream/type registration is
inside the measured work. Latency drops the first 10% of samples independently
for each writer; throughput includes all work.

The measurement phases are exact and common across variants:

1. Before any wall, allocator, or CPU sample, create the store/runtime, open the
   engine, allocate the workload descriptors, deterministic reusable payload
   source bytes, task handles, result slots, and latency-vector capacity. Do
   not materialize an append's record/input vector or intern its first-use
   names here. Spawn all writers and hold them at a ready barrier.
2. After every writer proves ready, snapshot allocator, engine/path/barrier,
   process CPU, serialized-role CPU, and context-switch counters. Take wall
   `t0` immediately before releasing the common start barrier; no work or
   cooldown lies between `t0` and release.
3. For public `A`/`C`/`D`, each writer starts its append-latency clock immediately
   before materializing that append's public typed input and stops it after
   `EventStore::append` completes. Thus record-vector construction, payload
   ownership/copying, public validation/encoding, registry work, queueing, I/O,
   durability, publication, release of the accepted intent's byte permit, and
   terminal notification are inside both wall and allocation accounting. The
   completion cannot become observable before that permit release.
   `B` materializes the equivalent raw `EventInput` inside the wall and
   allocator interval, but starts its per-append latency clock immediately
   before the raw `Appender` call. That declared latency-boundary difference is
   part of why `B` is only a lower bound; its construction cost still affects
   throughput and allocation ratios.
4. The coordinator awaits every writer, validates its exact completion count,
   and takes wall `t1` immediately after the final join. It then snapshots the
   same counters. At this boundary `A`'s completed intents must already expose
   exact zero slot and byte occupancy; a post-join drain wait is forbidden.
   Counter subtraction and all quantile/statistical work occur after `t1`; no
   sample collection is performed by a background consumer.

Payload bytes are generated once before the interval, but every append's owned
or borrowed record/vector objects are created inside it. A worker's elapsed
time is the common `t0` release through that writer's last append completion;
row wall time is `t1 - t0` and therefore also includes task wake and join
overhead. The exact phase markers and counter snapshots are emitted in every
row so an adapter cannot silently move work across a boundary.

The primary path uses no test-only sleeps, forced group widths, internal owner
calls, seal work, readers, snapshots, or pre-existing data.

## Focused public topologies

### All-new-name path

Run `payload=250`, `batch=1`, writers `{1,4}`, and both primary durability
modes. Every append names a distinct stream and uses the same event-type name.
Process uses 4,000 batches/writer; Group uses 1,000. Use the same four Williams
blocks and fresh-store rules, producing 64 rows. All variants run, but `B` is a
registry-free lower bound and cannot satisfy a registry semantic gate.

The public variants must report domain events, registry/control events,
visible global events, log-position high water, log barriers, and all
sync-family syscalls. Generation-specific numeric global positions are not
compared across `C` and v3-registry variants: current `$registry` events
legitimately consume hidden positions. Domain stream versions, visible event
order/payload, monotone opaque cursor behavior, and the declared per-generation
position accounting must be exact.

The 64 timing rows are untraced. Afterward, run one separate non-timing
structural trace for each public variant, mode, and writer count (12 rows) on a
fresh store with eight new-name appends per writer. Child-emitted durable
begin/end markers delimit the append interval so open/close syscalls cannot be
misclassified as new-name work. These traces are the authority for the removed
Fjall metadata barrier; their wall/latency values are discarded.
For historical `C`, the source-approved metadata markers include `log/meta/`
while log data remains classified by the nonoverlapping `log/seg-` marker.
This records the historical layout faithfully; it changes no structural or
performance gate.

### Queue fairness and owner saturation

Run all four variants with 64 writers, 250-byte payloads, one stable stream per
writer, and cells `{Process,Group} x {batch 1,batch 100}`. Process work is
5,000/500 batches per writer for batch 1/100. Group work is 200/200. Before the
measured counter snapshot, execute four matched concurrent warm rounds solely
to establish each implementation's production grouping target; warm rows do
not enter latency or throughput. Stable names are established by those warm
rounds for this fairness topology only. Use the four Williams blocks,
producing 64 rows.

Record per-writer completed appends, elapsed time, p50/p99/max, the Jain index
of per-writer rates, minimum/median writer rate, maximum/median writer p99,
aggregate batches/events per committed group, and barrier count. The exact
additional field names are `queue_depth`, `queue_bytes`,
`group_width_distribution`, `adaptive_group_width_target`, and
`oldest_queued_age_ns`. They are not exported neutrally by current or
historical production engines; every variant reports all five as the literal
string `not_available`, never zero, and they are not gates. Separately, `A`
reports the production engine's exact quiescent
`waiter_reservations_after` and
`byte_reservations_after` occupancy as integers in every fairness row; both
must be zero. `B`, `C`, and `D` report those two fields as `not_available`
because they do not expose the same admission-boundary state. No wrapper-side
estimate may impersonate it. Every writer must complete exact work; no
aggregate total can hide a starved writer. Writer elapsed is the common start
release through that writer's final append completion, and writer rate is
completed domain events divided by that elapsed time. Jain fairness is
`(sum(rate))^2 / (64 * sum(rate^2))`. Per-writer p50 and p99 use nearest-rank
indexes `ceil(q * n) - 1` on sorted samples; “median writer p99” is the median
of the 64 resulting p99 values and “maximum writer p99” is their maximum. No
pooled latency distribution may substitute for these fairness fields.

### CPU and syscall sentinel profiles

Profile all variants at 250 bytes, four writers, and
`{Process,Group} x {batch 1,batch 1000}`. Use the four Williams blocks. Run a
64-row CPU pass and a separate 64-row syscall pass; neither is an extra timing
repetition, and the two profilers may not be stacked on one child.

The CPU profile records measured-interval process user/system CPU and the
append-critical roles: current/flat owner; Fjall-era committer,
producer-runtime, and post-start `spawn_blocking` publication pool as three
separate labels; and bare committer. Thread identities
are bound by TID, `/proc` start ticks, and declared role before counters are
read. If a generation has no owner, the report says so and does not relabel a
different thread “owner.” At minimum retain thread/process CPU nanoseconds,
context switches, and, when available, cycles, instructions, and task-clock.
Process user/system time comes from `getrusage(RUSAGE_SELF)`. Serialized-role
CPU comes from the first nanosecond field in
`/proc/<pid>/task/<tid>/schedstat`; voluntary/nonvoluntary context-switch
deltas come from that TID's `status`. `A`/`D` bind the `mess-flat-owner` TID;
`C`/`B` bind the reviewed committer identity. `C` identifies the committer as
the sole new unnamed open-phase TID after named sealer/Fjall workers are
excluded; initial runtime TIDs are producers and later TID births are the
publication pool, with the blocking-thread keepalive frozen so they survive
sampling. A preflight CPU-bound helper must prove that `schedstat` is monotone
and record its smallest observed nonzero
increment. Every decision-driving serialized-role delta must be at least 20
times that measured increment; otherwise the CPU gate is invalid and the
attempt is `INCONCLUSIVE`. Hardware-counter availability is probed once before
row zero and may be `not_available` only with the retained permission result;
it never weakens mandatory `getrusage` or `schedstat` evidence.
The hardware profile uses `perf stat` inherited into child threads with its
control descriptor disabled at process start, enabled at the common start
release, and disabled at `t1`. Exact `perf` path/version/hash, events, argv, and
control acknowledgements are source-approved; profiler wall/latency never
enters the primary performance rows.

The syscall pass runs under one frozen tracing tool and records exact counts
for `write`, `pwrite64`, `writev`, `pwritev`, `pwritev2`, `fsync`,
`fdatasync`, `futex`, and file create/rename/unlink operations. Trace overhead
means its wall time and latency cannot enter a performance gate. Primary-row
engine barrier counters remain the timing authority. Missing tracing support
before row zero is `INCONCLUSIVE`, not permission to omit syscall evidence.

### Reopen and peak RSS

For each public variant, seed the locked post-Book corpus through the public
composition: 2,000,000 domain events, 1,000 stable streams, batch 10, 64-byte
payloads, 8 MiB segments, Process durability. Verify the corpus, close it,
record its byte manifest/digest, and preserve it as a read-only archive source.
Each measured open receives a newly materialized, byte-identical private copy;
the runner rechecks the manifest, calls `syncfs`, and completes the declared
quiet/settle guard before spawning exactly one opener against that copy. The
declared cache state is warm page cache from materialization and verification,
identical by construction rather than falsely labeled cold.

Run three quiet-guarded opens in separate processes and fixed Latin orders:
`A C D`, `C D A`, and `D A C`, so every variant occupies every ordinal once.
There is no append or read warm-up after the pre-open manifest check. The
untraced row reports wall time, `VmHWM`, `/proc/<pid>/io` bytes and syscall
deltas sampled while the child is `SIGSTOP`-parked immediately before and
after the single open, exported payload frames decoded during recovery,
registry/head digest, and total visible/log events.
Segment/directory/file-open counts come from one separate non-timing
structural trace per variant against another fresh verified copy; traced wall
time is discarded and cannot fill a reopen row. This focused profile is
excluded from the append row count. `B` has no equivalent public
recovery/index surface and is marked `not_applicable`, not simulated.

## Metrics and row invariants

Every primary row reports raw integers before normalized values:

- domain events, visible events, log/control events, appends, accepted batches,
  conflicts, writers, payload bytes, and logical digest;
- wall nanoseconds, events/second, and pooled post-warmup append p50/p99/max;
- allocation calls and allocated bytes for the complete timed public path,
  both normalized per domain event and per append;
- Process-owned versus borrowed batches/records/payload bytes, defensive-copy
  records/bytes, and the selected path label;
- mandatory process user/system CPU and the declared role CPU fields;
- group count and aggregate batches/events per group, barrier count, and fsync
  p50/p95/p99/max/degraded status;
- write-like syscall and host-write byte counters where the untraced product
  exposes them; and
- per-writer/fairness fields for the focused topology.

`A` must select the owned path in Process and the borrowed-compatible path in
Group. `C` and `D` must report their actual borrowed path; `B` reports
`raw-numeric`. For `A` Process, owned batches equal appends, owned records equal
domain events, owned payload bytes equal `events * payload`, and every
borrowed/copy delta is zero. For `A` Group, borrowed batches/records equal the
public submissions and owned-consumption counters are zero. A generation that
cannot export a counter must obtain it from a reviewed neutral measurement
adapter or report `not_available`; required gates cannot pass on
`not_available`.

For every row:

```text
appends = writers * batches_per_writer
domain_events = appends * batch
payload_bytes = domain_events * payload
Process barriers = 0
latencies are finite, nonnegative, and p50 <= p99 <= max
all writer totals sum exactly to the row totals
fresh store identity is unique and absent before creation
```

Group must use exactly one covering barrier per admitted commit group; no
public wrapper or Fjall side write may be silently omitted from syscall
accounting. A fixed durability alarm marks a row degraded but does not delete
or replace it. The row and its complete tail penalty remain in every statistic.

## Statistics and comparisons

Raw rows are authoritative. For each cell and metric, report every observation,
the median of four, best of four, minimum/maximum, median absolute deviation,
and the exact block ratios. The decision ratio for `A/reference` is the median
of the four within-block ratios; for an even sample, median means the arithmetic
mean of the two central sorted values. Throughput uses `A/reference`; latency,
allocation, CPU, syscall, and barrier ratios use `A/reference` with lower being
better. No ratio may divide a rounded CSV field.

The report must show three distinct comparisons:

1. `A/D`: change since the exact flat-owner checkpoint;
2. `A/C`: improvement over the production Fjall-era engine; and
3. `A/B`: current public API/topology cost relative to freshly measured bare.

The `A/B` ratio is budget evidence, not a universal 85%-of-bare gate. Report it
for every batch size and do not collapse it into one headline. Also compare
fresh public `A` and `D` descriptively to the matching direct-`LogEngine`
`BN-2SU-FINAL.csv` cells, in a separately labeled historical-orientation table.
Those cross-topology values cannot enter a threshold, outcome, drift proof, or
causal attribution. If both fresh variants differ from that CSV, describe the
difference only as “cross-topology and/or host/device difference.” The paired
fresh public `A/D` ratio is the sole causal preservation control.

No row is retried, replaced, winsorized, or silently dropped. No historical row
is pooled with fresh data. An incomplete or invalid matrix is not a negative
performance result.

## Locked performance gates

Correctness and evidence validity are prerequisites. The following thresholds
are fixed before build.

Group performance outcomes use one exhaustive device-variance rule. First,
correctness/durability failures follow the correctness outcomes below. Second,
any current Group barrier, allocation, syscall-shape, fairness/boundedness, or
resource gate failure is `NARROW`; device latency cannot excuse it. Third, if
all such structural/resource gates pass but a Group throughput or append-p99
gate fails, compare the same four paired blocks' engine-exported fsync mean and
p99. If the median within-block `A/reference` ratios for both are at most
`1.02`, the performance miss is `NARROW`. If either exceeds `1.02`, the
decision cannot separate current code from device variance and is
`INCONCLUSIVE`. A literal Group pass remains a pass. This mapping is applied to
primary and new-name Group performance gates. Fairness warm rounds cannot be
subtracted from the engine's cumulative fsync histogram, so fairness Group
throughput or append-p99 misses map deterministically to `NARROW`; cumulative
warm-plus-measured fsync mean/p99 remain labeled descriptive evidence and
cannot invoke the device-variance exception. No targeted rerun or best-row
substitution is allowed.

### Preserve the flat-owner baseline (`A/D`)

Every primary Process cell must satisfy:

- throughput at least `0.97` of `D`;
- p99 no more than `1.10` of `D`;
- allocation calls/event and allocated bytes/event each no more than `1.05`;
- serialized owner CPU/event no more than `1.05` where both variants expose
  the exact role; and
- zero barriers and no durability degradation in every row.

Every primary Group cell must satisfy:

- throughput at least `0.90` of `D`;
- p99 no more than `1.10` of `D`;
- allocation calls/event and bytes/event each no more than `1.05`;
- `A` aggregate barriers no more than `100.25%` of `D`; and
- the median block-level `A-D` barrier delta no greater than zero.

Group must also retain its declared one-covering-barrier structure. There is no
post-hoc targeted variance run in this protocol.

### Beat the Fjall-era production path (`A/C`)

Every primary Process cell must achieve throughput at least `1.10` of `C` and
p99 no more than `1.10` of `C`. Across the 16 Process cells, the geometric
means must be at least `1.20` throughput, no more than `0.90` allocation calls,
and no more than `0.95` allocated bytes. These material gates are deliberately
strong: BN-2SU already beat baseline-gen2 in every Process cell.

Every primary Group cell must retain at least `0.90` of `C`, p99 no more than
`1.10` of `C`, and no greater aggregate or median block-level barrier count.
Durable throughput is barrier/device dominated, so equality is success; no
invented durable speedup is required.

For the new-name topology, `A` must retain at least `0.97` of `D` in Process
and `0.90` in Group, and at least `1.05` of `C` in Process and `0.95` in Group.
More importantly, Process issues zero sync-family calls and Group has one
covering log barrier with no serialized Fjall metadata barrier. The syscall
trace must make the removed second durability spine observable.

### Fairness, boundedness, and write shape

In every current fairness row:

- Jain writer-rate fairness is at least `0.99`;
- the slowest writer rate is at least `0.50` of the median;
- maximum writer p99 is no more than `2.0` times median writer p99;
- every writer completes exact work with no waiter/byte reservation leak; and
- focused boundedness tests prove the current 1,024-intent owner-ring bound,
  configured Group byte/time bounds, and exact zero retained reservations
  after completion or cancellation.

Current fairness throughput must retain `0.95` of `D` in Process and `0.90` in
Group; Group barrier gates are identical to the primary matrix. The syscall
sentinels require zero sync-family calls in Process and no more write-like or
sync-family calls per append than `1.05` times `D`. A `pwritev` win is not
assumed: the prior pwritev experiment was inconclusive and only fresh profile
evidence may change that roadmap item.

Current median reopen wall and peak RSS must each be no more than `1.10` of
both `C` and `D`; recovery decodes zero historical payload frames, and all
three opens reproduce the exact corpus digest. A resource miss is `NARROW`
unless accompanied by a correctness mismatch, which is `REVERT`.

## Correctness, recovery, and fault gates

All timed rows compute a deterministic logical digest. Before timing, the
current source must pass the complete focused product matrix through
`EventStore<FjallSnapshotBackend<LogEngine>>`; after timing, the exact
integrated source is rerun through the same public matrix. Historical adapters
must pass the common workload/digest oracle before their binaries are admitted.

The mandatory gates are:

- ordinary append, commands, cached commands, head, stream/global read, and
  subscription all traverse the public composition and agree with a model;
- same-stream concurrent `Exact(v)` races have exactly one winner, with no
  gaps or duplicate domain positions within that generation's documented
  position model;
- a `$registry` batch and first-use domain batch are one ordered failure unit;
  partial landing publishes every landed predecessor before surfacing error;
- empty, conflict, oversize, reserved-registry, and alias/name error ordering
  is unchanged;
- cancellation before queue admission reserves nothing; cancellation after
  ownership transfer cannot prevent an accepted/committed append, including a
  fresh-name append, from reaching publication and releasing exact bytes;
  success, conflict, empty append, and backend-error paths release the exact
  byte permit before terminal notification, while an abandoned receiver
  releases by the same owner path and a later completed sentinel proves the
  owner has retired both reservations;
- borrowed and owned paths preserve mixed-record order, type authority, and
  byte-equivalent domain results;
- at least two live rolls preserve chain state, first read/cache behavior,
  pointer coverage, published watermarks, and reopen results;
- clean reopen, repeated reopen, active-tail recovery, and sealed-directory
  recovery reproduce heads, user-visible event types/payload/order, opaque
  cursor order, and the semantic logical digest;
- real-process kill points cover pre-write, partial write, post-write/pre-
  barrier, post-barrier/pre-publication, and post-publication/pre-completion;
- injected short write, write error, `fdatasync` error, torn/truncated tail,
  invalid marker/CRC, corrupt registry record, and refuted/corrupt sidecar have
  the documented fail-stop or rebuild fallback; and
- uncertain persistence poisons future writes one way, while no acknowledged
  Group append disappears after reopen.

Cross-generation comparison normalizes out the documented v3 `$registry`
position gaps. It compares user event sequence, stream versions, payload/type,
success/conflict/error outcomes, known stream-name heads, and opaque cursor
ordering. `C` has no public internal registry digest/ID-lookup surface, so no
byte-identical internal-registry claim is made. The oracle separately checks
each generation's exported high-water and registry/barrier accounting;
renumbering current user positions to resemble Fjall-era positions is
forbidden.

Syscall injection and corpus mutation cover short/write/sync errors and
on-disk corruption. Exact cancellation-before-admission and kill phases that
are not publicly observable use a separately reviewed `cfg(test)`-only current
instrumentation overlay. Those hooks may pause only the named phase, run only
in prebuilt correctness/fault children, and are prohibited from every timing,
profile, and comparator binary. Source approval must prove the hooks compile
out and that the release product/performance binary is byte-identical to the
unhooked build. A hook observation can validate current semantics but cannot
add a performance result.

Any current correctness, cancellation, durability, poison, roll, or recovery
failure is a `REVERT` result once the harness itself is independently shown
sound. A failure confined to a historical adapter is `INCONCLUSIVE`, because a
broken comparator cannot authorize a current verdict.

## Fresh artifacts, lease, quiet guard, and provenance

Reuse the hardened owned-append evidence machinery rather than the old
baseline runner's fixed load check:

- source approval binds this protocol, exact commits/trees, archive and
  materialized manifests, reviewed adapter allowlists/patches, tooling, and
  per-source lockfiles;
- each of the four binaries is built sequentially from a read-only
  `git archive` materialization in a fresh sandbox and a unique target, with
  the host root read-only and no network;
- each binary embeds protocol/tool/source/tree/adapter/lock hashes plus a unique
  nonce and exposes a non-timed contract mode;
- builds and contract smokes finish before any timing; all four binaries and
  their distinct SHA-256 values are then immutable;
- every exact subprocess transition, including evaluator and tracer argv,
  executable mode, contract readback, output cardinality, exit status, and
  child/group reaping, is smoke-tested through the real runner path before row
  zero; and
- prepared artifacts are single-use and result output is a previously absent
  directory outside every source/materialized tree.

The runner holds the host-wide nonblocking exclusive lease
`$HOME/.cache/mess-bench/global-measurement.lock` from pre-row validation
through evaluator exit. Record lock path/device/inode, PID/start ticks, UID,
host, boot ID, nonce, acquisition/release, and `/proc/locks` proof. A second
exclusive lock must fail while evidence is live. Builds, tests, formatters,
reviews, and other benchmarks may not overlap the lease. The prebuilt,
source-approved correctness/fault executables are declared evidence children,
not Cargo test/build activity, and run under that same lease before and after
timing as required; their commands and hashes are guarded like timed children.

Before and after every child, atomically retain one `/proc` snapshot for the
source-approved exact `comm` allowlist (including Linux's declared truncation):
compiler/linker tools, all four benchmark binaries, correctness/fault/reopen
children, runner, evaluator, terminal verifier, tracer/profiler wrappers, and
every helper executable. Classify runner, current child, declared helper,
unexplained foreign process, or vanished/unresolved identity using PID plus
start ticks. Any unexplained or unresolved match fail-stops the whole attempt.
Do not use `pgrep -f`, which can match its own shell. Explicitly wait/reap the
child and prove its process group absent before continuing.

Scratch and result materializations live under
`$HOME/.cache/mess-bench/asterism-rebaseline`; the runner requires at least
`137438953472` free bytes (128 GiB) and `1000000` free inodes before every row.
Falling below either floor fail-stops the attempt as `INCONCLUSIVE` before
spawn. Load1 at or above `6.0` causes a pre-row wait of at most 120 seconds,
polled once per second. After each Process child the runner observes a 400 ms
settle, and after each Group child a 4 s settle, retaining load and free-space
samples throughout; these are fixed scheduling phases, never discarded trial
results. The final quiet/resource/process guard immediately precedes spawn, so
no unobserved cooldown lies between the final guard and the child. A post-row
load spike or fixed fsync alarm is retained in the row and never triggers
replacement. If quiet is not reached within 120 seconds, the attempt is
`INCONCLUSIVE` with all partial rows retained as non-decision evidence.

Provenance records toolchain and flags, kernel/boot ID, CPU model/topology,
governor/turbo/affinity, page size, memory, filesystem/mount/device/scheduler,
scratch/free bytes before and after, timestamps/order/load, source/archive/
adapter/lock/binary hashes, exact commands and environment, seed, row/column
counts, CSV prefix hashes around each child, guard/child manifests, logical
digests, and final artifact SHA-256 values. The evaluator independently replays
all source, build, lease, guard, child, row-order, metric-identity, and hash
checks. Invalid evidence can never be translated into a performance decline.

## Evidence files and report requirements

The completed workspace must retain, at minimum:

```text
spikes/asterism_rebaseline/
  BN-2L3N-PROTOCOL.md
  REPORT.md
  primary.csv
  new_names.csv
  fairness.csv
  reopen.csv
  cpu_profiles.csv
  syscall_profiles.csv
  config.json
  provenance.json
  source-approval.json
  prepared-artifacts.json
  guard-manifest.jsonl
  child-manifest.jsonl
  result.json
  terminal.json
  SHA256SUMS
```

The report lists every per-cell gate, both medians and best-of-four, raw
barrier totals, all degraded rows, correctness/fault commands and outcomes,
admitted versus declined Phase-4 mechanisms, and the residual `A/B` budget by
batch size. It must explain performance in plain English against both the
Fjall-era production engine and bare without calling the public/bare quotient
pure engine overhead. Later roadmap budgets are updated only from this fresh
matrix.

## Terminal outcomes

- `ADMIT`: evidence is complete and terminal-verified; every correctness,
  Process, Group, allocation, CPU, syscall, fairness, and barrier gate passes.
  The current public engine becomes the new rebaseline and later Asterism work
  uses its per-cell budgets.
- `NARROW`: evidence and correctness are complete, no `REVERT` condition is
  present, but one or more declared performance/resource gates fail. The
  result names exact modes/cells/mechanisms that remain acceptable and freezes
  the rest from further integration until a new predeclared candidate wins.
- `REVERT`: the current public composition has a reproducible correctness,
  durability, cancellation, roll/recovery, poison, or paired unapproved
  Process regression against fresh public `D`. Historical direct-`LogEngine`
  rows cannot cause or prevent reversion. The report identifies the first
  causal integrated checkpoint; reversion is a separately reviewed product
  action, never an automatic benchmark-script mutation.
- `INCONCLUSIVE`: the matrix is incomplete or invalid, a source/build/adapter,
  lease/quiet/guard, tracer, child/evaluator, provenance, or historical-oracle
  failure prevents a valid decision, or the evidence cannot distinguish host
  drift from code under the frozen rules. Partial rows remain audit evidence
  and cannot be resumed, pooled, or manually evaluated.

There are no retries, replacement observations, threshold edits, manual
evaluator invocations, historical-row substitutions, or “mostly pass”
translations after row zero. Only a fresh protocol version may authorize a new
attempt.
