# Research 10: formal verification and adversarial testing plan

## 1. Goal

Asterism changes the concurrency owner and broadens the log transaction from “events” to “events plus engine control.” Those changes must be justified by executable models, not prose alone.

Verification is layered:

```text
TLA+/state-machine model -> protocol safety and crash states
Stateright/proptest       -> executable exhaustive/random histories
Loom                      -> Rust memory-order publication and reclamation
Kani/fuzz                 -> byte decoders and arithmetic
fault filesystem          -> sector persistence/reordering
real SIGKILL              -> OS/filesystem integration
full differential oracle  -> optimized state == canonical fold
```

No single layer substitutes for the others.

## 2. Protocol state machine

### 2.1 State variables

A minimal append/control model contains:

```text
queue: sequence<Intent>
owner_state: Idle | Gathering | Writing | Syncing | Applying | Publishing
spec_heads: map Stream -> Version
committed_capsules: sequence<Capsule>
durable_prefix_len: Nat
published_prefix_len: Nat
completions: map IntentId -> Pending | Success | Conflict | Duplicate | Error
segment_epoch: Nat
next_batch_id: Nat
next_global_pos: Nat
registry: Registry
active_dedupe: ExactSet
poisoned: Bool
```

The disk model separates issued writes from persisted sectors and barriers.

### 2.2 Actions

```text
Enqueue(intent)
BeginGroup
ValidateAccept(intent)
ValidateReject(intent)
EncodeCapsules
IssueWrite(sectors)
PersistSubset
BarrierSuccess
BarrierFailure
ApplyEffects
PublishGroup
CompleteWaiters
CancelReceiver(intent_id)
Crash
Recover
RollSegment
SealSegment
WriteCheckpointPage
InstallCheckpoint
GarbageCollectPages
```

### 2.3 Safety properties

```text
S1 NoAckedLoss:
   every Success completion under Os/closed Group is in recovered durable prefix

S2 NoPartialVisibility:
   every published user event belongs to an accepted complete capsule

S3 ControlEventAtomicity:
   same-capsule controls and events are both applied or neither applied

S4 DenseGlobalPositions:
   visible user events occupy exactly [0, log_end)

S5 StreamContinuity:
   each stream's accepted transitions form one path with no gap/overlap

S6 RegistryBeforeUse:
   every accepted event reference resolves after applying prior/same-capsule controls

S7 PublishAfterDurability:
   under Os/closed Group, published_prefix <= durable_prefix

S8 CompletionAfterPublish:
   Success implies the corresponding effects are reader-visible

S9 CancellationNoGap:
   dropping a receiver cannot prevent an accepted capsule from publishing

S10 PoisonMonotonic:
   once a barrier uncertainty poisons writes, no later append succeeds before recovery

S11 CheckpointBounded:
   checkpoint watermark <= recovered canonical log end

S12 CheckpointEquivalence:
   applying suffix to a valid checkpoint equals full canonical fold

S13 ExactDedupe:
   Duplicate iff an exact equal key exists within semantic window

S14 NoUseAfterFree:
   active microblocks/pages reachable by any reader lease are not reclaimed
```

### 2.4 Liveness properties

Under fair scheduling and successful I/O:

```text
queued intent eventually completes
published watermark eventually reaches durable watermark
seal backlog eventually drains below bound
checkpoint request eventually installs or reports failure
bounded reader retry eventually returns or takes slow path
```

Do not overstate liveness under a permanently slow disk or caller that never releases a reader lease; those require timeouts/operational policy.

## 3. TLA+ model outline

Modules:

```text
AsterismCapsule     framing-independent commit/control semantics
AsterismGroupCommit queue, validation, write/sync/apply/publish
AsterismCrashDisk   sectors, stale epochs, barriers, rename/dir sync
AsterismCheckpoint content-addressed pages and manifest selection
AsterismDedupe      exact window + epoch representation
AsterismMigration   v3/Fjall import and v4 cutover phases
```

Use small finite domains:

```text
2 streams
2 names/types
3 intents
2 events/capsule
2 control records/capsule
2 segment epochs
2 checkpoint pages
window span 2–4 positions
```

Small domains are enough to find ordering/state bugs.

Model both:

- centralized owner reference;
- implementation refinements such as receiver cancellation and early-close grouping.

The refinement mapping projects implementation state to canonical accepted capsule sequence and folded kernel state.

## 4. Stateright or hand-rolled executable model

A Rust state-machine model has two advantages:

1. it can share serialized corpus fixtures and error enums with tests without sharing optimized state code;
2. counterexamples can become regression tests directly.

State transitions choose nondeterministically:

- producer enqueue order;
- same-stream/cross-stream intents;
- conflict and dedupe states;
- cancellation timing;
- write sector subset and reorder;
- barrier success/failure;
- crash point;
- checkpoint/SealPack operation interruption.

The canonical oracle is a simple immutable vector of accepted logical capsules plus `BTreeMap`/`VecDeque` fold.

## 5. Loom plan

Loom models only small concurrent components, not file I/O.

### 5.1 Head page publication

Threads:

```text
one writer updates two cells under page sequence
2–3 readers read arbitrary cells
optional grower publishes a new page directory
```

Assertions:

- no mixed head pair accepted;
- no reader observes uninitialized page/cell;
- sequence retry terminates via modeled slow path;
- release/acquire ordering exposes all prior cell writes.

### 5.2 Microblock publication

Model:

- writer fills entries and publishes count/tail;
- readers traverse tail/previous;
- generation retires after reader lease.

Assertions:

- reader never reads uninitialized entry;
- published count is monotone;
- tail publication makes initialized header/entries visible;
- slab is not reclaimed while reachable.

### 5.3 Completion slot/ring

Model bounded MPSC producer pushes, owner drain, cancellation/drop, and completion. Assertions:

- each accepted intent completes at most once;
- dropped receiver does not leak ring capacity;
- owner never reads partially initialized intent;
- wake cannot be lost.

### 5.4 Checkpoint snapshot generation

Model writer dirty-bit/page updates and checkpoint worker snapshot. Assert manifest pages correspond to one declared published kernel generation or are rejected by digest/anchor.

## 6. Kani and decoder proofs

Targets:

- capsule header length arithmetic;
- control TLV tiling;
- event subframe tiling;
- marker offset/echo arithmetic;
- CRC split ranges;
- partitioned Elias–Fano/bitvector rank bounds;
- Stream VByte scalar decoder output bounds;
- SealPack section directory bounds/non-overlap;
- checkpoint page table bounds;
- snapshot blob record bounds.

Properties:

```text
no integer overflow/underflow
no out-of-bounds slice
accepted tiling consumes exactly declared region
encode(decode(bytes)) canonical where applicable
decode(encode(value)) == value
invalid length never allocates above cap
unknown version/flag returns typed error
```

Unsafe SIMD decoders are tested against scalar decoders on arbitrary inputs and gated by validated lengths.

## 7. Fuzzing

Persistent corpora:

```text
v3/v4 capsule bytes
golden control records
mixed-version segments
SealPacks with every section kind
checkpoint manifests/pages
snapshot packs
registry import chunks/manifests
dedupe epochs with collisions
```

Fuzz targets:

- parse only;
- parse then re-encode;
- recovery scan sequence;
- effect build and canonical digest;
- fast recovery vs full recovery;
- mutate checksums and recompute outer checksums to exercise semantic validation;
- truncate at every byte for small fixtures;
- swap/repeat control records;
- decompression bombs constrained by authenticated output caps.

Use structure-aware mutators for lengths/counts/flags so fuzzing reaches deep semantic states.

## 8. Fault filesystem extension

The existing simulator should gain named operations for:

```text
file write ranges
file data barrier
rename
link/unlink
directory barrier
allocation/reuse with stale bytes
truncate
partial/torn sectors
read error/short read
EIO on write/barrier
```

### 8.1 Capsule matrix

For each generated group:

- arbitrary persisted sector subset before barrier;
- header/control/event/marker sectors independently selected;
- stale previous-epoch bytes;
- same global position across control-only capsules;
- batch-ID repeats/skips;
- valid “resync bait” beyond first invalid capsule.

### 8.2 SealPack matrix

Crash after every file operation. Verify:

- footer never causes an invalid pack to be trusted;
- orphan valid pack is safe;
- missing optional section degrades;
- raw segment remains readable;
- repeated recovery/seal is idempotent.

### 8.3 Checkpoint matrix

Crash during page write, rename, manifest write, current-slot update, and page GC. Verify selection of highest valid anchored manifest and no deletion of live pages.

### 8.4 Snapshot matrix

Crash during blob record, pack barrier, install capsule, and head publish. Verify head/blob ordering and fallback.

## 9. Differential state testing

For each randomized operation sequence:

```text
reference = full accepted-capsule fold in boring structures
live      = incremental state-kernel result
fastopen  = checkpoint + effects + tail
nochk     = effects + tail
forensic  = full physical scan + semantic fold
```

Compare:

```text
heads
registry rows and allocator next IDs
snapshot heads
projection frontiers
dedupe exact answers for all generated keys/boundaries
active/sealed stream reads
global event sequence
state digest
```

Run after every simulated crash/reopen, not only at sequence end.

## 10. Dedupe adversarial suite

Inject a test fingerprint function with configurable bit width, including zero bits, so all keys collide. Generate:

- identical key in different scopes;
- same scope/key before, exactly at, and after boundary;
- same fingerprint/different bytes;
- overwritten/retried capsule;
- recovered-but-unacknowledged capsule;
- frozen epoch straddling boundary;
- checkpoint missing active epoch;
- imported v3 key without canonical capsule key.

The exact answer must match a simple reference set/deque.

## 11. Static-directory verification

Every representation implements:

```rust
trait ExactDirectory {
    fn lookup(&self, key: u64) -> Option<Entry>;
    fn iter(&self) -> impl ExactSizeIterator<Item=(u64, Entry)>;
}
```

Build from one canonical sorted vector. Tests assert:

```text
all present keys return exact entries
all generated absent keys return None
iteration exactly equals source
serialize/deserialize preserves equality
corrupt bytes return error, not wrong entry
construction failure falls back deterministically
```

For MPHF/Ribbon, explicitly test arbitrary nonmember outputs are rejected by exact key verification.

## 12. Migration verification

Model migration phases and crashes:

```text
inventory
shadow kernel
registry import chunks
import manifest
v4 boundary
live dedupe import
snapshot/checkpoint import
Fjall retirement
```

Safety:

- no v3 referenced ID loses meaning;
- partial import is never canonical;
- old binary cannot write mixed directory;
- dedupe answer remains exact across boundary;
- snapshots/checkpoints survive authority transfer;
- re-running a phase is idempotent;
- retiring Fjall before prerequisites is refused.

A release fixture should contain a real v3 store generated by the prior version and be upgraded by the new binary in CI.

## 13. Real-process tests

Run `SIGKILL` and, where infrastructure permits, VM/power-cut tests under:

```text
Process
Os
Group low/high concurrency
new-name registration + first event
control-only checkpoints
snapshot install
segment roll/seal
checkpoint install/GC
migration cutover
```

After each kill:

- full verify;
- fast open and full-scan digest comparison;
- append more events and verify contiguous positions;
- run exact dedupe retries;
- repeat recovery to prove idempotence.

## 14. Performance correctness gates

Fast paths always retain validation:

- capsule CRC on recovery;
- section/page checksums at open/read boundary according to cache policy;
- exact key verification after filters/static functions;
- logical result checksum in benchmarks;
- verify-on-seal byte comparison for payload packs;
- effect digest comparison in build/test modes.

A benchmark-only `unchecked` feature may exist for diagnosis but cannot compile into release artifacts by default and cannot supply published acceptance numbers.

## 15. Release evidence bundle

A storage-format release archives:

```text
TLA+/model version and checked bounds
counterexample-free state count
Loom test results
Kani proof results
fuzz corpus/hash and run duration
crash/torn/SIGKILL totals
mixed-version upgrade fixture results
benchmark ledger and raw samples
golden wire fixtures
format/spec commit hash
known limitations
```

The package lets a future maintainer know exactly what was proved, tested, and merely hypothesized.

## 16. Minimum bar before v4 production write

```text
TLA+/executable model satisfies S1–S14 in checked domains
Loom publication/reclamation suites green
all decoder arithmetic targets proven/fuzzed
>=24k v4 sector-reordering cases green
real SIGKILL across registration/control/checkpoint paths green
100k randomized differential histories green
mixed v3/v4 upgrade fixture green
no unchecked recovery path
```

The core promise of Mess is that high performance does not come from making crash semantics vague. Asterism should raise that bar: the custom state kernel is acceptable only when every optimized state has a simple canonical fold oracle and every persistent accelerator can be thrown away.
