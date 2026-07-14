# Research 04: durability, file layout, and SSD-aware placement

## 1. Objective

The custom engine must be more durable because it has fewer authoritative states, not because it invents a more elaborate sync protocol.

The target rule is:

> Accepted log bytes are the only durable fact needed to reconstruct event
> visibility, stream heads, and registry interpretation. Optional dedupe or
> projection state joins that authority only after its own product decision and
> atomic encoding. Every checkpoint/sidecar is either explicitly discardable
> or a named blob whose publication ordering is explicit.

This note extends the existing A1–A12 crash model rather than replacing it.

**Status.** Production remains v3. The flat owner/direct committer and
block-native read/seal path are current. SealPack is admitted default-off;
SegmentEffects/checkpoints are proven but not integrated; v4 is format
admissible but not a product default. ADR 0002 is normative for snapshot packs
and keeps discovery discardable with no `SnapshotInstalled` log record.

## 2. Authority classes

| artifact | authority | failure consequence |
|---|---|---|
| event/control capsule in `.log` | sole commit authority | invalid capsule terminates accepted prefix |
| snapshot pack + discovery root | discardable acceleration; event log remains authority | older candidate or full replay |
| SealPack | advisory/rebuildable | scan raw segment and rebuild |
| SegmentEffect | advisory/rebuildable | scan that segment |
| kernel checkpoint pages/manifest | advisory/rebuildable | use older checkpoint or fold log/effects |
| filter/static function | advisory | exact fallback |
| parity pack | repair aid | no change to normal visibility |
| `current.a/b` checkpoint pointer | advisory hint | enumerate/validate manifests |
| optional dedupe/projection control | no current product authority | canonical only after a later product decision and atomic encoding |

No manifest or sidecar advances event visibility.

## 3. Commit-capsule durability

This section specifies the gated v4 alternative. Current v3 registry/domain
batches are adjacent members of one ordered unit, not one capsule.

### 3.1 Physical write

A commit group contains one or more individually framed capsules. The writer
may encode them into one contiguous buffer or `writev` vector. Each capsule has
its own CRC and marker so recovery can accept a prefix of the group only if
those capsules reached disk completely. Acknowledgment follows the configured
mode: `Process` has no barrier, `Os` syncs each capsule/batch, and `Group` waits
for one covering barrier for the gathered group.

### 3.2 Control/event atomicity

Control records and domain events share one capsule CRC and marker. The possible outcomes are:

```text
capsule absent/torn/invalid -> neither control nor events exist
capsule accepted            -> both control and events exist
```

There is no state in which a stream registration is durable but the same-capsule first event is not accepted, or vice versa. If the entire capsule is written but unacknowledged before crash, recovery may surface it under the existing “unacked but committed” rule. A retry first resolves the recovered name to its existing ID. If product idempotency is admitted, a matching live key returns the original commit; otherwise expected-version checking conflicts and appends nothing. Registry idempotence alone never silently duplicates the event.

### 3.3 Zero-event control capsules

v4 may accept `event_count == 0` only when `control_count > 0`. Since global position does not advance, recovery must make per-segment `batch_id` continuity mandatory:

```text
batch_id == previous_batch_id + 1
first_global_pos == expected_global_pos
```

The batch ID and segment epoch are covered by the capsule CRC. Recovery never resynchronizes past a failed capsule. This prevents stale or reordered control capsules from being accepted at the same unchanged global position.

### 3.4 Full CRC remains mandatory

The existing torn-write work established that marker/header structure is insufficient when payload sectors can be reordered or omitted. v4’s control region is inside the same full-capsule CRC. There is no control-only structural fast path and no “trust the marker because there are no events” exception.

## 4. Group commit

The current flat owner is the direct committer. In `Group` mode its shape is:

```text
gather intents
validate and assign centrally
current: k positioned writes; target: gathered/coalesced write where possible
one durability barrier
advance crash-stable watermark in position order
publish effects
ack
```

Window closure:

- close early when all known in-flight producers are waiting;
- close at `max_bytes` near the measured device throughput knee;
- close at `max_delay` as a latency cap;
- close before the next capsule would cross a segment boundary.

`Process` runs the ordered write/apply/publish sequence without a barrier. `Os`
is deliberately sync-per-batch; a current v3 `$registry`+domain ordered unit
therefore has two syncs, whereas one future v4 mixed capsule would have one.
The state-kernel owner must remain the committer rather than an additional
layer around it. B1 cross-barrier pipelining is rejected by measured evidence.

## 5. Failure-state table

| failure point | durable state | recovery action |
|---|---|---|
| before capsule write | no capsule | nothing |
| partial header/control/event/marker | invalid capsule | stop at capsule start; truncate tail if configured |
| complete capsule before barrier | may survive or disappear | accepted if fully valid; retry resolves registration, then admitted dedupe or expected-version conflict |
| barrier success before state publish | capsule durable | recovery folds it; in-process owner must publish before ack |
| after publish before caller receives ack | capsule durable and visible | retry sees admitted dedupe or expected-version conflict |
| during SealPack temp write | raw segment durable; temp partial | delete/ignore temp |
| SealPack durable before rename | orphan temp/final candidate | verify and reuse or delete |
| SealPack rename before segment footer | orphan valid pack | verify and attach during resumed seal or delete |
| footer durable | segment + named pack complete | fast open permitted |
| snapshot record partial | no committed record | truncate/ignore tail |
| record/pages exist without root | unreachable orphan | reclaim under writer lock later |
| snapshot root published | every named record validates; Durable mode also proves complete closure | load candidate or fall back/replay |
| checkpoint page partial | no valid page hash | checkpoint invalid/older used |
| manifest partial | invalid manifest | older checkpoint |
| current pointer stale/torn | advisory only | enumerate manifests |
| `fdatasync` returns EIO | persistence uncertain | poison writes; restart/recovery defines truth |

## 6. SealPack layout and atomic install

### 6.1 One file, typed sections

Use one immutable file per sealed segment:

```text
Header
SectionDirectory[n]
Sections...
Trailer
```

Each directory entry:

```rust
struct SectionRef {
    kind: u16,
    version: u16,
    flags: u32,
    offset: u64,
    length: u64,
    uncompressed_length: u64,
    crc32c: u32,
    codec_id: u16,
    reserved: u16,
    content_hash_prefix: u64,
}
```

The trailer hashes the header and section directory. Directory entries bind
each section's CRC and content-hash prefix, so mandatory-section corruption
rejects the pack while an optional-section failure can degrade locally. The
segment-footer-to-pack-hash binding remains a required follow-up before the
default-off SealPack can become default-on.

### 6.2 Ordering

```text
write pack.tmp
verify first/last pointers and semantic payload/type equality against raw input
fdatasync(pack.tmp)
rename(pack.tmp, pack)
fsync(seals directory)
append/write segment footer naming pack hash
fsync(segment)
```

The segment footer is the durable installation point, but not event commit authority. A footer without a matching pack causes the engine to ignore the accelerator and rebuild.

### 6.3 Why directory fsync matters

A file `fsync` does not universally guarantee the directory entry created by rename survives a power failure. The install protocol syncs the containing directory before the segment footer can durably advertise the name/hash.

### 6.4 Seal failure

Sealing is best-effort for availability. A failed or timed-out seal leaves a durable unsealed segment. Recovery scans it, rebuilds active state, and queues reseal. The append path can roll to a new segment without waiting for full pack construction, subject to a bounded backlog policy.

## 7. Kernel checkpoint durability

### 7.1 Content-addressed pages

Page files are immutable and named by a cryptographic hash of canonical bytes. A page write uses temp + `fdatasync` + rename; duplicate hashes reuse existing files after validation.

### 7.2 Manifests

A manifest is immutable and cursor-named. It references only durable page hashes and a durable log-prefix anchor. It is written only after all missing pages are durable.

### 7.3 Alternating current pointers

`current.a` and `current.b` each contain:

```text
generation
manifest filename/hash
CRC
```

Write the older slot via temp+rename+directory sync. On open, validate both and choose the highest valid generation. If both fail, enumerate manifests. Since the pointer is advisory, stale selection costs replay but cannot return wrong state.

### 7.4 Page garbage collection

GC computes reachability from retained valid manifests, writes a durable GC plan, and unlinks unreachable pages. A crash during GC cannot invalidate a retained manifest because the implementation never deletes a page still referenced by the durable reachability set. Keep at least two checkpoints across GC cycles.

## 8. Snapshot-pack durability

ADR 0002 selects a separate snapshot sidecar, not a log install control. One OS
writer lock owns a UUID namespace. An active `.open` pack contains independently
commit-framed records; rolling appends and syncs an index/footer, renames to an
immutable `.pack`, and syncs the directory. Discovery is a bounded immutable
copy-on-write tree keyed by stream plus stable snapshot compatibility identity,
under immutable generation roots. Offline readers are read-only and use exact
range `pread`; missing/corrupt artifacts are a miss/replay.

Two publication modes are explicit:

```text
Buffered -> atomic process visibility; no power-loss promise
Durable  -> every pack range/page/directory entry reachable from the complete
            root is proven durable before the root is acknowledged
```

`Durable` is a complete-closure promise, not “sync the new record.” A
checksummed durability-proof ledger may avoid repeat I/O only for exact
previously proven immutable identities/frontiers. GC first publishes a new
complete-closure Durable root, retains at least two validated roots, syncs root
pruning, then deletes unreachable content and syncs deletion directories.
Snapshot roots never authorize event-log retention. ADR 0002 remains normative
for identity, `Empty < Through(0)`, trust modes, administrative scans,
same-coverage conflicts/repair, and the required evidence matrix.

## 9. Out-of-place storage and SSD behavior

The March 2026 PVLDB paper *How to Write to SSDs* argues that out-of-place DBMS writes and lifetime-aware grouping can substantially reduce both software and flash write amplification; its redesigned LeanStore reports 1.65–2.24× throughput and 6.2–9.8× fewer flash writes per YCSB-A operation in the evaluated settings. Asterism should borrow the principles, not the numbers.

### 9.1 Asterism write classes

```text
class 0: active event segment             sequential, hot, soon sealed
class 1: immutable retained event segment long-lived
class 2: SealPack                         same lifetime as segment
class 3: active snapshot pack             medium-lived
class 4: sealed snapshot pack             retention-dependent
class 5: checkpoint page                  content-addressed, shared generations
class 6: dedupe epoch temp/frozen          short-lived
class 7: temp build files                 very short-lived
```

Do not mix short-lived dedupe/checkpoint temp data into the same large files as indefinitely retained events.

### 9.2 FDP

On NVMe Flexible Data Placement devices, map lifetime classes to placement handles/reclaim groups. Measure host and device write amplification, tail latency, and reclaim behavior. The default file layout should expose the grouping even when FDP is absent.

### 9.3 ZNS

A 256 MiB segment may not equal the device zone size. A ZNS backend chooses a logical segment size that is an integer number of zones or packs multiple logical segments into a zone set. Writes remain sequential; segment/pack retirement resets whole zones only when every live object in the zone set is dead.

ZNS support must not force the normal filesystem format to trust device append pointers for logical commit. The capsule marker/CRC remains authoritative.

### 9.4 Near-full-device behavior

The existing Mess performance work observed severe `fdatasync` degradation on a nearly full consumer SSD. Asterism must expose:

```text
barrier p50/p95/p99/max
bytes per barrier
seal/checkpoint write bandwidth
filesystem free space
optional device wear/SMART metrics
sustained-vs-idle recovery behavior
```

Operational alarms should trigger on barrier-latency multipliers and low free-space thresholds, not merely throughput.

## 10. Read I/O policy

### 10.1 Explicit reads first

Use `pread`/`readv` for active segments and large SealPack payload sections. It provides explicit errors, bounded buffers, and backpressure.

### 10.2 Small immutable metadata

Small directory/filter/effect sections may be read fully into immutable byte arrays. This avoids `mmap` lifetime/SIGBUS hazards and still removes object-by-object parsing.

### 10.3 Optional mmap

Allow mmap only when:

- files are immutable after rename;
- the process owns the directory and forbids external truncation;
- a typed fallback exists;
- the benchmark shows a real gain.

### 10.4 io_uring

An optional backend can batch independent sealed reads and register stable files/buffers. The state machine submits logical reads through a trait; completion ordering never determines event order. Do not couple commit correctness to linked-operation folklore.

## 11. Checksums and hashes

Use the cheapest primitive that matches the threat:

| use | primitive |
|---|---|
| capsule accidental corruption/reordering | CRC32C, mandatory |
| section/page accidental corruption | CRC32C |
| content-addressed checkpoint pages | BLAKE3-256 or equivalent cryptographic hash |
| SealPack identity/footer binding | cryptographic hash |
| optional stream fold/tamper evidence | existing BLAKE3 fold chain |
| in-memory hash table attack resistance | per-store keyed hash/fingerprint |

Do not replace mandatory recovery CRC with BLAKE3; current measurements already show crypto slows the recovery scanner materially. Hashes and CRCs solve different problems.

## 12. Formal crash extensions

Extend the existing sector model with operations:

```text
WriteCapsule(bytes)
BarrierSegment
WriteSealTemp(section sectors)
BarrierSeal
RenameSeal
BarrierDirectory
WriteFooter
BarrierSegmentFooter
WriteCheckpointPage
BarrierPage
RenamePage
WriteManifest
BarrierManifest
WriteCurrentSlot
BarrierKernelDir
WriteSnapshotRecord
SealSnapshotPack
PromoteSnapshotClosure
WriteSnapshotProofLedger
PublishSnapshotRoot
PruneSnapshotRoots
DeleteSnapshotContent
```

Model arbitrary persistence subsets between barriers, stale prior-generation sectors, rename durability, and crash at every operation. Safety properties:

```text
accepted capsule prefix is contiguous by batch ID and global position
no control/event split
no acked capsule lost under promised durability
checkpoint never causes state beyond accepted log
footer never causes corrupt pack to be trusted
snapshot Buffered roots remain discardable; acknowledged Durable roots have a
complete proven closure and GC never deletes retained-root content
recovery is idempotent
```

## 13. Primary references

- [Mess log format and A1–A12](https://github.com/bobisme/mess/blob/43e4aca0192f01bb47670627f41182bca182759e/docs/spec/01-log-format.md)
- [Mess durability specification](https://github.com/bobisme/mess/blob/43e4aca0192f01bb47670627f41182bca182759e/docs/spec/03-durability.md)
- [How to Write to SSDs](https://arxiv.org/abs/2603.09927)
- [SplinterDB](https://www.usenix.org/conference/atc20/presentation/conway)
- [F2: Designing a Key-Value Store for Large Skewed Workloads](https://arxiv.org/abs/2305.01516)
