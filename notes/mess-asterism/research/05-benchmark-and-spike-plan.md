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

## 2. Locked baselines

Before changing code, reproduce and record on the same host/device:

1. bare `mess-log` Process append at batches 10 and 100;
2. bare `mess-log` Group/Os append at 4×100 and high concurrency;
3. current composed `LogEngine` append at the same event sizes/batches;
4. Fjall `stream_head`, dedupe hit/miss, and `apply_group` rates;
5. current ActiveIndex read/update rates;
6. current sealed sidecar point, stream, and open/RSS costs;
7. current open-with-Book-rehydration over 1M, 10M, and—if storage permits—100M events;
8. current snapshot save/load and file-count behavior.

Do not compare a 5,000-event Asterism batch to a 10-event Fjall batch. The repository’s historical rows remain context, but each spike gets a fresh side-by-side baseline.

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

### Question

Does collapsing validation, position assignment, and publication into one owner remove enough composed overhead?

### Scope

Do **not** change the on-disk format. Keep v3 log bytes and temporary Fjall name persistence. Replace:

- per-stream `AppendGate`;
- one `spawn_blocking` task per append;
- `PublishSequencer`;
- post-ack head write to Fjall;

with:

- bounded intent ring;
- one owner using the existing committer/writer;
- direct/HashMap shadow head state;
- completion slots.

Keep dedupe and Book initially, then profile.

### Correctness

- same-stream `Exact(v)` races: exactly one success;
- cross-stream ordering: dense global positions;
- dropped futures: no position gaps and committed append still publishes;
- conflicts/empty appends preserve API behavior;
- 12k crash, 24k torn, and real SIGKILL harnesses unchanged;
- differential oracle against current engine.

### Gates

```text
Process composed throughput >= 85% of bare log on equal workload
p99 append latency no worse than current by >10%
zero per-append blocking-pool tasks
zero post-commit condvar sequencing
queue memory bounded
```

This is the architecture’s first kill point.

## 7. Spike C — remove the Book

### Question

Can the API be served from log/SealPack blocks with bounded caches and no all-history payload mirror?

### Steps

1. implement active capsule read by `EventPtr`;
2. return block-backed internal record views;
3. adapt existing owned `StoredRecord` API on top;
4. route sealed reads to `.pcol` plus pointer sidecar;
5. stop populating `Book.payloads` and `stream_events`;
6. remove full payload rehydration from open.

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

### Gates

```text
open performs zero old payload decodes when sealed/checkpointed
peak RSS <= 50% of Book baseline on 10M-event corpus
repeated hot load no slower than current by >10%
stream/global replay meets existing gates
append allocations/event decrease
```

## 8. Spike D — SegmentEffect and checkpoint

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
```

## 9. Spike E — v4 control prelude

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

### Question

Can exact dedupe eliminate mutable KV write/deletion work without regressing latency?

### Compare

```text
current Fjall primary+order tables
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
```

The 80 ns whole-window target is intentionally extreme; failure does not kill epoching if write amplification and composed throughput clearly win. Report the trade.

## 12. Spike H — static directory tournament

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

### Matrix

Run all shared corpora under:

```text
current composed engine
Asterism, v3 compatibility mode
Asterism, v4 capsules
bare log lower bound
```

### Product gates

```text
append ratio >= 85% bare log
new-name operation one barrier
head/snapshot targets
Book removed and bounded RSS
checkpoint open target
dedupe exact and faster/lower-WA
existing replay/storage gates
all crash/formal suites green
```

No isolated result can waive a failed composed gate.

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
B flat owner:       KILL architecture if <85% bare log
C no Book:          KILL payload-mirror removal only if reads collapse
D effects/checkpoint: KILL fast-recovery design if equivalence/space fails
E v4 capsule:       KILL format if proof/harness is not clean
G dedupe:           keep Fjall only for dedupe if custom exact set loses
H static indexes:   optional; simplest winner is acceptable
J composed:         final decision
```

The purpose of the program is to make it easy to discard beautiful ideas that do not survive the complete workload.
