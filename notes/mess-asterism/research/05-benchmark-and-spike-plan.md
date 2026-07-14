# Research 05: benchmark and spike plan

## 1. Principle

The design contains mechanisms with very different confidence levels. The spike program must answer architecture questions in dependency order and must not let a microbenchmark victory hide an end-to-end loss.

Every result is labeled:

```text
correctness gate -> must pass, no trade allowed
performance gate -> predeclared threshold
resource gate    -> memory, bytes, syscalls, build time
operational gate -> recovery, corruption fallback, inspectability
```

A mechanism that misses its gate is removed or reduced to an optional experiment.

## 1.1 Program status

The plan has been executed. Reports and raw artifacts under `spikes/` are the
result authority; this document preserves the predeclared method and records
the carried decisions:

| spike | carried result |
|---|---|
| A dense heads | adopt packed `AtomicU128`, page-seqlock fallback |
| B flat owner | adopt owner-direct; reject second committer and B1 pipelining |
| C no payload Book | integrated, including raw-segment seal source |
| D effects/checkpoints | proceed; explicit bottom, allocator first/last, parallel build + sequential apply |
| E v4 | format admissible/off by default; no-alloc validate/materialize split mandatory |
| F microblocks | adopt stream F3; reject binary search and stride-8 globals; use stride-1 |
| G dedupe | adopt G2 mechanism only if product idempotency is admitted; baseline synthetic |
| H directory | adopt bitvector+rank dense arm with deterministic fallback |
| I SealPack | admit default-off pending named default-on follow-ups |
| J composed | narrow/adopt proven parts; use per-cell gates, never one headline ratio |

Decision-relevant committed evidence includes the baseline
[`REPORT.md`](../../../spikes/baseline_matrix/REPORT.md) and
[`baseline_results.csv`](../../../spikes/baseline_matrix/baseline_results.csv),
the post-integration [`BN-2SU-REPORT.md`](../../../spikes/baseline_matrix/BN-2SU-REPORT.md)
and [`BN-2SU-FINAL.csv`](../../../spikes/baseline_matrix/BN-2SU-FINAL.csv),
Spike B's [`REPORT.md`](../../../spikes/flat_combined_append/REPORT.md) and
[`flat_combined_results.csv`](../../../spikes/flat_combined_append/flat_combined_results.csv),
and Spike J's [`REPORT.md`](../../../spikes/composed_decision/REPORT.md) and
[`composed_results.csv`](../../../spikes/composed_decision/composed_results.csv).
The reports identify which rows were accepted or rejected and preserve the
commands, machine/load context, and interpretation; the CSVs remain the raw
matrix authority.

## 2. Locked baselines

Before changing code, reproduce and record on the same host/device. The locked
baseline generation is now `spikes/baseline_matrix/` plus the post-integration
`BN-2SU` matrix named in research 13. Accepted and rejected raw rows remain
committed; a summary table without its raw matrix is not evidence.

1. bare `mess-log` Process append at batches 10 and 100;
2. bare `mess-log` Group/Os append at 4×100 and high concurrency;
3. current composed `LogEngine` append at the same event sizes/batches;
4. Fjall `stream_head`, dedupe hit/miss, and `apply_group` rates as synthetic
   component orientation only;
5. current production ActiveIndex read/update/read-path rates;
6. current sealed sidecar point, stream, and open/RSS costs;
7. current block-native open/RSS plus the historical Book baseline over 1M and
   10M events, then an early measured 100M-event run (a synthetic corpus is
   acceptable); until completed, label any 100M extrapolation derived;
8. current snapshot save/load and file-count behavior.

Do not compare a 5,000-event Asterism batch to a 10-event baseline. The valid
historical 4-writer x 10-event pair was about 84%; the once-claimed 63% ratio
was unmatched and is invalid. Every append gate is stratified by batch size,
payload, concurrency, stream distribution, durability, cache/device state, and
producer topology.

## 3. Shared corpus matrix

### 3.1 Stream popularity

```text
uniform over N streams
Zipf s=0.8
Zipf s=1.1
Zipf s=1.4
single hot stream
hot set of 64 streams + long tail
```

### 3.2 Stream counts

```text
1k, 10k, 1M, 10M streams
synthetic 30M static keys for large-directory tests
```

### 3.3 Batch and payload

```text
batch events: 1, 4, 10, 100, 1000
payload: 0, 24, 250, 4KiB, 64KiB
new-name rate: 0%, 0.01%, 1%, 100%
dedupe rate: 0%, 1%, 50% retries
```

### 3.4 Readers

```text
0 readers
1 head-heavy reader
N CPU-saturating head readers
stream replay readers
subscription reader
mixed 90/9/1 append/head/replay
```

### 3.5 Cache states

```text
warm CPU/cache
warm OS page cache
cold file cache where possible
post-idle settled SSD
sustained-write SSD
low-free-space SSD test partition
```

## 4. Instrumentation

Collect:

```text
throughput
p50/p95/p99/p99.9/max latency
CPU cycles, instructions, branches, branch misses
L1/LLC misses where perf access exists
allocations and allocated bytes
syscalls by type
bytes written at host
fsync/fdatasync count and latency
RSS and peak RSS
file count and directory operations
seal/checkpoint backlog
recovery bytes scanned and payload bytes decoded
```

Every performance run computes a logical result hash or byte-identity check. No unchecked fast benchmark result enters the ledger.

## 5. Spike A — dense state kernel

### Question

Can workload-shaped direct tables materially beat Fjall/HashMap while remaining safe under concurrent readers?

### Implementations

```text
A0 Fjall point table
A1 hashbrown HashMap<u64, Head>
A2 chunked [AtomicU64; 2] cells + page seqlock
A3 double-copy cell + selector
A4 portable/target AtomicU128 when lock-free
```

### Workload

- 1M and 10M dense stream IDs;
- writer updates 1, 100, or 10k cells per published group;
- readers uniform and Zipf;
- pathological updates concentrated in one page;
- page growth while readers run.

### Correctness

- Loom model for publication and page growth;
- randomized readers compare each returned pair against a versioned reference history;
- no torn `(version, global_position)` pairs;
- retry/latch slow path cannot deadlock.

### Gates

```text
head p50 <= 50 ns
head p99 <= 150 ns under one writer + readers
update apply >= 20M cells/s
resident <= 20 B/allocated stream
no unbounded reader retry; p99.9 bounded
```

If no direct design clears the latency gate by at least 5× over Fjall and 2× over `HashMap`, keep `HashMap` for simplicity and proceed with the larger architectural changes.

## 6. Spike B — flat-combined append owner on v3

**Result: NARROW, then ADOPT/INTEGRATE owner-direct.** The owner must own the
writer/barrier. Fronting the existing committer cost 35%. B1 next-group
pipeline did not win durable workloads and lost 5–18% at 64 writers, so it is
rejected. The original 85%-of-bare line failed below batch 1000 because it
priced the async API/topology; the replacement gates below are cell-matched.

### Question

Does collapsing validation, position assignment, and publication into one owner remove enough composed overhead?

### Historical scope used by the spike

Do **not** change the on-disk format. Keep v3 log bytes and temporary Fjall name persistence. Replace:

- per-stream `AppendGate`;
- one `spawn_blocking` task per append;
- `PublishSequencer`;
- post-ack head write to Fjall;

with:

- bounded intent ring;
- one owner that owns the writer/DirectCommitter;
- direct/HashMap shadow head state;
- completion slots.

The spike initially kept dedupe and Book, then profiled. Subsequent work made
production dedupe's dormancy explicit and Spike C removed payload history from
Book; this sentence records the experiment sequence, not current architecture.

### Correctness

- same-stream `Exact(v)` races: exactly one success;
- cross-stream ordering: dense global positions;
- dropped futures: no position gaps and committed append still publishes;
- strict-FIFO space admission: large intents do not starve, oversize intents
  fail, and dropping a pre-admission waiter reserves/releases no bytes;
- dropping an admitted completion receiver still reaches one terminal owner
  state and releases exactly its byte reservation;
- conflicts/empty appends preserve API behavior;
- 12k crash, 24k torn, and real SIGKILL harnesses unchanged;
- differential oracle against the then-current pre-flat-owner engine.

### Gates

These were Spike B's historical admission gates against the matched
pre-flat-owner engine; the adopted flat owner passed them. They are not a
perpetual `+20%` demand against current production.

```text
Process throughput >= 1.20x matched pre-flat-owner engine, per cell
Group throughput >= 95% matched pre-flat-owner engine at >=4 writers, per cell
p99 append latency <= matched pre-flat-owner engine
barrier count == matched bare log
zero per-append blocking-pool tasks
zero post-commit condvar sequencing
queue memory bounded
```

The old 85%-of-bare result remains an aspirational API gate for an owned-record,
interned-type producer-combining experiment, not a reason to discard the
adopted owner.

## 7. Spike C — remove the Book

**Result: PASS and integrated.** “Book” now means the compact O(streams +
types) registry/head structure; payload and per-stream position history are
gone.

### Question

Can the API be served from log/SealPack blocks with bounded caches and no all-history payload mirror?

### Steps

1. implement active capsule read by `EventPtr`;
2. return block-backed internal record views;
3. adapt existing owned `StoredRecord` API on top;
4. route sealed reads to `.pcol` plus pointer sidecar;
5. stop populating `Book.payloads` and `stream_events`;
6. remove full payload rehydration from open.
7. re-source the sealer from the rolled raw segment and gate readiness on the
   canonical published watermark, never Book length.

### Workloads

- aggregate load with tails 0/1/10/100/10k;
- global replay 1M/10M;
- repeated hot-stream loads;
- random point reads;
- mixed readers and appends;
- cold reopen.

### Correctness

- byte-equivalent `StoredRecord` sequences;
- upcaster and fold-certificate suites;
- subscription handoff suite;
- truncate/corrupt payload pack falls back or returns typed error;
- bounded cache eviction cannot change results.
- reassembled event payloads and pointer/version/global-position results are
  identical; compressed `.pcol` bytes are only an auxiliary same-build check;
- corrupt/missing pack validation selects the raw-log fallback.

### Gates

```text
open performs zero old payload decodes when sealed/checkpointed
peak RSS <= 50% of Book baseline on 10M-event corpus
repeated hot load no slower than current by >10%
stream/global replay meets existing gates
append allocations/event decrease
```

## 8. Spike D — SegmentEffect and checkpoint

**Result: PROCEED.** 100k randomized histories and all fallback gates passed.
The corpus exposed and fixed a missing explicit frontier bottom. Allocators
carry first/last boundaries. Build effects in parallel (6.8x at eight threads)
and apply them sequentially in segment order (24.3M head transitions/s); full
tree composition was much slower and is not the production default.

### Question

Can compact effects and content-addressed pages reproduce full recovery and change startup asymptotics?

### Corpus

Generate histories with:

- 100–10M capsules;
- 1k–1M streams;
- registry additions/aliases;
- snapshots/checkpoints;
- dedupe entries and expiry;
- v3 synthetic controls where v4 not yet available.

### Variants

```text
full sequential scan oracle
per-segment effect sequential apply
parallel map + ordered tree reduce
full checkpoint + suffix
incremental dirty-page checkpoint + suffix
corrupt/missing effect fallback
corrupt/missing page fallback
```

### Correctness

At least 100k randomized histories compare canonical state digests. Inject:

- transition gaps/overlaps;
- registry conflicts;
- stale checkpoint anchors;
- wrong page hash;
- missing page;
- reordered effects;
- duplicate effect application;
- interrupted checkpoint GC.

### Gates

```text
all state digests identical on valid histories
all invalid histories rejected or safely fall back
checkpoint open <= 10% of full scan time
no event payload decode for metadata recovery
effect bytes <= 16 B/touched stream + dedupe/control payloads
incremental checkpoint writes proportional to dirty pages
ordered effect apply >= 5M head transitions/s and keeps the §18.3 budget reachable
```

## 9. Spike E — v4 control prelude

**Result: FORMAT ADMISSIBLE, product write OFF.** Zero violations in 96,654
model states and 24k sector cases; D4 retry tests and fuzz gates passed. Naive
materialize-everything decode cost +24.5%; the mandatory allocation-free
`validate_capsule` plus on-demand materialization reduced overhead to +0.47%.
ADR 0002 still withholds snapshot/dedupe/projection product admission.

### Question

Can registry/control and user events share one atomic capsule without weakening recovery or cursors?

### Implement

- v4 header and marker sketch;
- mandatory batch-ID continuity;
- control TLV decoder with frozen codec;
- registration + first event;
- dedupe key + events;
- control-only snapshot/checkpoint capsule;
- v3/v4 segment boundary reader.
- allocation-free structural validation separated from on-demand control/event
  materialization.

### Exhaustive model

Small-state exhaustive enumeration over:

```text
1–3 capsules
0–2 events per capsule
0–2 control records
sector persistence subsets
stale prior-generation bytes
crash before/after every write/barrier
```

### Random fault matrix

Extend existing 24k matrix with control regions and zero-event capsules at 512B/4KiB sectors.

### Safety properties

```text
no control/event split
no accepted duplicate/stale batch ID
no global-position gap or advance from control-only capsule
registry assignment exists before same-capsule event decode
recovery stops at first invalid capsule
accepted prefix identical across repeated recovery
```

### Gate

Zero violations. Performance is secondary; control parsing must add <2% to ordinary no-control scan.

## 10. Spike F — active pointer microblocks

**Result: ADOPT F3 stream side.** Use linear 32-entry blocks and an every-eight-
block skip chain; reject binary search. Spike J later rejected stride-8 global
checkpoints because real header `pread`s were 10–17x slower than the incumbent;
use the measured stride-1 fallback (16 B/batch).

### Question

Do direct tails and append-only blocks beat the current sharded map-of-vectors under actual contention?

### Compare

```text
current ActiveIndex
microblock arena, linear scan
microblock arena, binary search
microblock + periodic skip
```

### Workloads

- one-event and 100-event batches;
- single hot stream and Zipf;
- 64 readers on same/different shards/pages;
- segment roll/reclaim while readers hold leases.

### Gates

```text
apply throughput >= current * 1.25
head/tail resolve p99 <= current * 0.75
memory <= current * 0.7
no lock blocking in reader fast path
Loom/epoch reclamation green
```

If gains are small, retain current ActiveIndex after removing Book/Fjall; it is already solid.

## 11. Spike G — exact epoch dedupe

**Result: ADOPT G2 AS CANDIDATE MECHANISM ONLY.** Production dedupe is dormant,
so the Fjall primary+order comparison is synthetic. ADR 0002 routes the product
decision to `bn-2ctq`; no current append API carries a key.

### Question

Can exact dedupe eliminate mutable KV write/deletion work without regressing latency?

### Compare

```text
synthetic best-effort Fjall primary+order tables
hashbrown active + sorted frozen epochs
specialized Swiss active + BinaryFuse frozen
Iceberg-style active + BinaryFuse/APBF
optional k-PHF/Ribbon frozen candidate router
```

### Correctness corpus

- arbitrary key lengths 0–4KiB;
- stream-scoped and global keys;
- forced 128-bit fingerprint collisions;
- retries before/at/after exact boundary;
- unacked-but-recovered capsule retry;
- checkpoint loss and rebuild;
- adversarial same-prefix keys.

### Gates

```text
exact equality with VecDeque+BTreeMap reference
active miss p50 <= 100 ns
whole-window miss target <= 80 ns or >=5x Fjall
hit <= 300 ns excluding cold canonical-key I/O
zero per-key delete/tombstone writes
frozen+active resident/disk <= Fjall footprint * 0.5
canonical keys retained through the exact inclusive window and rebuild
batch-level retry returns the original result; no-key/expired follows expected version
```

The 80 ns whole-window target is intentionally extreme; failure does not kill epoching if write amplification and composed throughput clearly win. Report the trade.

## 12. Spike H — static directory tournament

**Result: ADOPT H2 for dense/real segments.** Bitvector+rank won the real
distribution with a deterministic sorted/HashMap fallback. Decline the tested
PEF, PtrHash, and k-bin variants; use exact verification and record the chosen
codec in the section header.

### Candidates

```text
current HashMap
sorted key/entry arrays
bitvector+rank
partitioned Elias–Fano
PtrHash + exact key array
cache-line k-PHF packed bins
Ribbon retrieval + exact candidate verification
```

### Datasets

Use every real sealed segment generated by benchmark workloads, preserving actual `min/max`, density, and Zipf stream distribution. Add synthetic 30M-key sets for large-static behavior.

### Metrics

```text
hit/miss/streaming latency
cold and warm
serialized bytes
resident bytes
open/parse time
build p50/p99
failure rate/retry count
seal wall time
branch and LLC misses
```

### Admission rule

A new representation enters production only when it wins at least one important size/density region by ≥20% and never lacks a deterministic fallback. Otherwise document the negative result and keep `HashMap` or sorted arrays.

## 13. Spike I — consolidated SealPack

**Result: ADMIT default-off.** Semantic replay, local optional-section
degradation, file/open count, seal time, and cold-read gates passed. Default-on
still requires the named RSS and footer-to-pack-hash follow-ups.

### Compare

Current separate `.pidx/.filter/.pcol` against one pack.

### Inject

- missing section;
- corrupt section;
- wrong segment ID/hash;
- interrupted temp write;
- rename without directory sync in simulator;
- footer durable without pack (fault-injection only);
- old reader skipping new section kind.

### Gates

```text
byte-identical replay
no more install states than current
open syscalls/files reduced materially
seal <= current * 1.10
corrupt optional section degrades locally
```

## 14. Spike J — full composed engine without Fjall

**Result: NARROW/ADOPT PROVEN PARTS.** J was a composed projection and rematch,
not an integrated all-Asterism engine. It confirmed owner-direct, rejected
stride-8 globals, retained the Book-removal win, and replaced the headline
ratio with batch-stratified gates. Snapshot discovery—not dormant dedupe or
projection rows—is the only live Fjall deletion blocker.

### Matrix

Run all shared corpora under:

```text
current composed engine
Asterism, v3 compatibility mode
Asterism, v4 capsules
bare log lower bound
```

### Product gates

These are the historical Spike J/admission gates against its then-current
pre-flat-owner control:

```text
Process >= 1.20x pre-flat-owner engine per cell
Group >= 95% pre-flat-owner engine at >=4 writers per cell
p99 <= pre-flat-owner engine; barrier count == bare
new-name barriers reported by mode: Process 0; current v3 Os 2 per-batch barriers; Group 1 covering
head/snapshot targets
Book removed and bounded RSS
checkpoint open target
dedupe exact and faster/lower-WA
existing replay/storage gates
all crash/formal suites green
```

Forward Phase 4 comparisons use the locked `BN-2SU-FINAL` production cells:
no Process regression without explicit product approval, Group non-regression
and barrier parity, a predeclared p99/tolerance budget, and a committed raw
matrix for every decision.

No isolated result can waive a failed composed gate. No headline average can
waive a failed batch-size cell, and every gate must link its committed raw
matrix and machine/load metadata.

## 15. Measurement discipline

### 15.1 Run ordering

Interleave variants to reduce device-state bias:

```text
A B B A
B A A B
```

Settle between sustained durable runs. Record free space and recent barrier latency.

### 15.2 Statistics

Report median-of-runs and best-of-runs separately. Best-of-N estimates uncontended capability; median shows expected operation. Tail latency uses all operations after warm-up, not per-run averages.

### 15.3 Compiler and CPU

Pin toolchain/flags, record CPU governor/turbo, pin threads where useful, and report portable scalar plus architecture-specific SIMD results separately.

### 15.4 Reproducibility

Each spike emits:

```text
config JSON
machine/device info
seed
logical checksum
raw sample CSV/JSON
summary Markdown
exact command
commit hash
```

Promoted results enter `mess-bench` floors with tolerance and a nightly lane.

## 16. Stop/go sequence

```text
A dense tables: continue even if only modest; useful primitive
B flat owner:       ADOPT owner-direct; old 85%-bare gate narrowed to per-cell current-engine gates
C no Book:          KILL payload-mirror removal only if reads collapse
D effects/checkpoint: KILL fast-recovery design if equivalence/space fails
E v4 capsule:       KILL format if proof/harness is not clean
F microblocks:      ADOPT F3 stream blocks; reject binary search and stride-8 globals
G dedupe:           optional product; synthetic mechanism evidence cannot create a contract
H static indexes:   optional; simplest winner is acceptable
I SealPack:         ADMIT default-off; block default-on until RSS/footer binding follow-ups pass
J composed:         NARROW/ADOPT proven parts; no integrated-stack claim
```

The purpose of the program is to make it easy to discard beautiful ideas that do not survive the complete workload.
