# Research 08: Asterism risk register

Scale:

- **Severity:** 1 cosmetic, 5 data loss/corruption.
- **Likelihood:** 1 unlikely, 5 expected without mitigation.
- **Detectability:** 1 immediately obvious, 5 hard to detect before harm.
- **RPN:** severity × likelihood × detectability; prioritize high values.

## 1. Top risks

| ID | risk | S | L | D | RPN | primary mitigation | kill/rollback trigger |
|---|---|---:|---:|---:|---:|---|---|
| R1 | v4 zero-event/control capsule weakens crash acceptance | 5 | 3 | 4 | 60 | mandatory batch-ID continuity, full CRC, epoch, exhaustive model, no resync | any control/event split or stale control acceptance |
| R2 | fresh-store Fjall replacement diverges from the canonical log fold | 5 | 2 | 3 | 30 | one effect path, full-scan oracle, differential tests, checkpoints discardable | persistent digest mismatch or bypassed oracle |
| R3 | **RETIRED:** authoritative name mappings lost during legacy-store migration | — | — | — | — | no legacy stores; names now live in canonical `$registry` records | re-open only if persisted pre-registry stores ever exist |
| R4 | exact dedupe becomes probabilistic by accident | 5 | 3 | 4 | 60 | retain all same-fingerprint candidates; compare full key; forced-collision tests | any synthetic collision false negative |
| R5 | snapshot install references non-durable blob | 5 | 2 | 5 | 50 | blob barrier before install capsule; crash model | any recovered head with unavailable promised blob |
| R6 | checkpoint accepted for the wrong prefix of a fresh store's canonical log | 5 | 2 | 4 | 40 | segment epoch/cursor/root anchor; fallback; corruption suite | checkpoint+suffix digest differs from full scan |
| R7 | sequence-counter implementation has Rust UB/torn state | 5 | 2 | 5 | 50 | atomic fields only, Loom, bounded latch fallback | sanitizer/Loom issue or unexplained head pair |
| R8 | single owner becomes CPU bottleneck | 3 | 4 | 2 | 24 | profile, preallocation, vectorized group validation, bulk mode | composed <85% bare log |
| R9 | SealPack install ordering trusts missing/corrupt pack | 4 | 2 | 4 | 32 | pack durable+dir sync before footer, hash binding, raw fallback | any wrong read rather than fallback/error |
| R10 | checkpoint page GC deletes live pages | 5 | 2 | 5 | 50 | durable reachability set, retain generations, interrupted-GC model | valid retained manifest references missing page |
| R11 | static function returns wrong nonmember result | 5 | 3 | 4 | 60 | exact key comparison; optional structure only | any result path omits verification |
| R12 | direct arrays explode on sparse/malicious IDs | 3 | 3 | 2 | 18 | writer-only dense allocator; validate/remap external IDs; sparse page directory | external ID directly controls page index |
| R13 | Book removal regresses hot aggregate loads | 3 | 3 | 2 | 18 | bounded capsule/block caches; block-native views | >10% sustained hot-load regression without RSS win justification |
| R14 | checkpoint/open still loads all names/heads and misses scale goal | 3 | 4 | 2 | 24 | resident/tiered profiles, compressed registry base, lazy pages | 100M-event open remains event-count proportional |
| R15 | per-segment effects grow with events rather than touched keys | 3 | 3 | 2 | 18 | net final updates only; dedupe separately epoched | effect size exceeds declared per-key gate |
| R16 | seal/checkpoint backlog grows without bound | 4 | 3 | 3 | 36 | bounded queues, backpressure/roll policy, operator alarms | unbounded disk/RSS or append outage |
| R17 | async caller cancellation leaks completion/queue state | 3 | 3 | 3 | 27 | owner owns lifecycle; dropped receiver only; state-machine tests | committed capsule not published or queue slot leaked |
| R18 | control TLV parser becomes attack surface | 5 | 3 | 3 | 45 | fixed caps, checked arithmetic, fuzz/Kani, frozen codec | panic/OOM/out-of-bounds on arbitrary bytes |
| R19 | unknown or mismatched segment version is silently skipped or opened writable | 5 | 2 | 4 | 40 | fail-closed decoder dispatch; v3-only open refusal; downgrade tests | any binary writes after encountering an unsupported version |
| R20 | performance claims depend on warm cache/device state | 2 | 5 | 3 | 30 | cold/warm separate, interleaved runs, raw samples, device telemetry | result not reproducible within tolerance |

## 2. Correctness risks in detail

### R1 — control-only capsule ambiguity

**Failure mode:** multiple control-only capsules share the same `first_global_pos`, so a scanner that relies only on global contiguity can accept stale/reordered control state.

**Mitigation:** v4 elevates per-segment `batch_id` from informational to mandatory contiguous sequence. The segment epoch prevents old-generation bytes from matching. The full capsule CRC covers all fields and controls. Recovery stops at first failure.

**Verification:** exhaustive small-state model plus the full sector-reordering matrix. Include zero, garbage, and stale backgrounds.

### R2 — optimized state diverges from canonical fold

**Failure mode:** a fast path updates direct heads/dedupe differently from full recovery; a clean process appears correct until restart or a rare sequence.

**Mitigation:** one `Effect` type and one application routine are used by live
commit and suffix recovery. The oracle has a separately implemented boring
model. State digests are compared continuously in tests and against full-scan
recovery; there is no legacy Fjall shadow-deployment phase to operate.

**Operational response:** poison writes on a live digest mismatch. Do not “repair” by choosing the in-memory answer.

### R3 — registry migration loss (**retired**)

**Failure mode:** v3 log bytes contain only numeric IDs; deleting Fjall name rows makes records uninterpretable.

**Retirement basis:** Mess has no existing stores to migrate, and names now live
as canonical `$registry` records ordered before their first references. There
are no legacy Fjall name rows to import or lose. The general lesson remains:
never delete a persistence domain until every referenced ID resolves from an
earlier canonical record. Re-open this risk if persisted pre-registry stores ever
exist; do not silently revive the old M0–M9 plan.

### R4 — dedupe collision

**Failure mode:** two keys share a compact fingerprint; an implementation stores only one, causing the other to be forgotten and a duplicate accepted.

**Mitigation:** fingerprint maps to a run/list of candidates; full keys live in
canonical records; every candidate is compared. Test hash function injection
deliberately maps all keys to the same fingerprint.

### R5 — snapshot head outruns blob

**Failure mode:** install capsule survives but blob does not, making the latest head unusable.

**Mitigation:** durable blob first. The install capsule includes blob hash and pack/offset. A snapshot durability mode cannot exceed blob durability. On checksum failure, fall back to previous snapshot/full replay and mark suspect.

### R6 — false checkpoint anchor

**Failure mode:** a state page set from prefix `P1` is applied to suffix of different prefix `P2`, yielding plausible but wrong heads.

**Mitigation:** manifest carries exact commit cursor, segment epoch, end global position, registry version, and cryptographic prefix/fold anchor. Validation cross-checks the canonical segment footer/chain. No heuristic “watermark only” match.

**Narrowed scope:** there is no imported Fjall state or mixed-v3/v4 migration
boundary to bind. The remaining risk is entirely within one fresh store: a
checkpoint from one canonical prefix must never be paired with another suffix.

### R7 — publication races

**Failure mode:** reader sees new version with old global position, follows a freed page, or loops indefinitely on a hot sequence counter.

**Mitigation:** all raced scalars atomic; page pointers lifetime-managed by `Arc`/epoch; release/acquire specified; Loom model; bounded retries then latch; no pointer-containing state protected solely by seqlock.

### R9/R10 — accelerator install/GC

**Failure mode:** a footer or manifest makes a partially installed artifact look complete, or GC removes still-referenced content.

**Mitigation:** temp write, file barrier, rename, directory barrier, then durable reference; immutable content hashes; at least two retained checkpoint generations; GC from a durable reachability snapshot.

### R19 — unknown-format open

**Failure mode:** a v3-only binary encounters a v4 or otherwise unsupported
segment and skips, truncates, or opens the directory writable. This can happen
with fresh stores through binary downgrade, copied directories, or operator
error; it does not require a fleet migration.

**Mitigation:** decoder dispatch is fail-closed before any writable open. A
v3-only binary must loudly refuse a store containing v4 segments, and unknown
versions are never treated as an empty tail. Exercise downgrade and
unknown-version fixtures even while v4 remains off by default.

## 3. Performance risks

### R8 — one owner saturates

The owner may spend too much time on:

- hashing long dedupe keys;
- resolving strings;
- encoding MessagePack/control records;
- copying large payloads;
- applying huge groups;
- waking many waiters.

Mitigations:

```text
producer-side pure payload encoding where deterministic
pre-hash name/dedupe bytes with owner revalidation
writev for large payloads
preallocated group/effect arenas
bulk completion wakeups
separate background seal/checkpoint
range-reservation bulk API for trusted construction
```

Do not shard the canonical log prematurely. If the owner cannot reach 85% of bare log, profile first. A sharded validation front end with one final ordered committer is a later option, but it increases state complexity.

### R13 — no Book means more I/O

The Book is fast because everything is resident. Removing it trades unbounded memory/startup for possible active-log reads.

Mitigations:

- cache whole active capsules, not individual events;
- keep hot aggregate folded-state cache at a higher layer;
- coalesce stream pointers by segment/offset;
- return block views to avoid allocation;
- tune cache by bytes and reuse;
- snapshots cap replay.

The acceptance decision uses total system cost: hot-load latency, append CPU, RSS, and reopen—not one metric.

### R14 — live namespace dominates memory/open

A checkpoint cannot compress away the need to answer heads for live streams. At 100M live streams, even 16 bytes/head is 1.6 GB.

Mitigations/profiles:

```text
ResidentKernel: all head pages resident, fastest
TieredKernel: immutable cold checkpoint pages + hot mutable overlay
Server profile: memory budget admission and prefetch
Embedded profile: lower stream-count guardrails
```

Document that retained events and live streams are different scale axes.

### R15 — effects too large

A segment touching one million one-event streams necessarily has at least one final head per stream; no algebra removes that information. The design wins when events per touched stream >1 and when packed keys/values beat generic KV metadata.

Use information-theoretic accounting in reports. Do not claim constant-size segment summaries.

### R16 — background backlog

A producer can fill segments faster than one sealer/checkpointer. Policies:

```text
max unsealed segments
max uncheckpointed dirty bytes
parallel seal workers bounded by I/O/CPU
append throttling before disk exhaustion
skip expensive optional static structures under backlog
always produce minimal pointer/effect pack first
upgrade pack sections later only if format permits safe replacement
```

A minimal seal should be fast and sufficient for recovery; “alien” structures are shed first under pressure.

## 4. Operability risks

### Format archaeology

Every on-disk structure needs:

- magic/version/length;
- byte-order declaration;
- checked decoder;
- golden byte fixtures;
- human dump command;
- unknown-version behavior;
- upgrade policy;
- fuzz corpus.

A design that only its author can inspect is not durable.

### Repair and rebuild time

Accelerators are rebuildable, but a multi-terabyte full rebuild can be operationally unacceptable. Keep:

- SegmentEffects with independent verification;
- checkpoints at configured cadence;
- repair/rebuild progress and resume markers;
- per-segment parallelism;
- online read availability where safe;
- clear estimate before starting.

### Filesystem semantics

Atomic rename and directory sync details vary. The simulator models the intended POSIX contract; production support documents tested filesystems. Network filesystems and exotic mount options may be unsupported for write mode.

### Observability

Required metrics:

```text
owner queue age/bytes
validate/encode/write/sync/apply durations
barrier latency multiplier
unsealed segment backlog
checkpoint dirty pages/age
SealPack/effect fallback counts
dedupe epoch/filter candidate rates
seqlock retries/slow-path count
cache hit/miss/eviction bytes
state digest mismatch count (must stay zero)
```

## 5. Security/adversarial risks

- Keyed fingerprints prevent deliberate hash-flood choice but keys are still exactly compared.
- Control/event length fields have hard caps and checked arithmetic before allocation.
- Static-function builders run with memory/time budgets; hostile distributions fall back.
- Registry names have byte-length and UTF-8/canonicalization rules; aliases cannot create ambiguous resolution silently.
- Externally supplied IDs are validated before allocating dense pages.
- Corrupt sidecars cannot influence commit acceptance.
- Decompression has output caps equal to authenticated uncompressed lengths.
- CLI forensic modes avoid trusting filenames and advisory manifests.

## 6. Maintainability guardrails

1. One canonical `Effect` definition.
2. One decoder per format version with golden fixtures.
3. Every optional accelerator implements a simple exact fallback trait.
4. No unsafe code in format decoding; unsafe SIMD isolated behind tested scalar equivalence.
5. No performance optimization may disable CRC/hash validation in production paths.
6. Every benchmark variant computes result identity.
7. Every new background worker has bounded queues and shutdown/drain semantics.
8. “Temporary” authorities are documented with a removal condition and a
   canonical rebuild proof.
9. Fresh-paper mechanisms stay feature-gated until multiple architectures reproduce wins.
10. The regression suite is a release requirement, not a research artifact.

## 7. Final risk posture

The legacy-store migration risk is retired: there are no users or existing
stores, and names already live in the canonical log. The remaining Fjall roles
still require an authority audit; no keyspace is deleted until it is
log-derived or proven safely discardable. The highest remaining risks are
therefore the v4 shift from event-only batches to mixed control/event capsules
(if v4 is adopted), fail-closed handling of unsupported formats, exact
replacement-state semantics, and accepting an accelerator or checkpoint for the
wrong canonical prefix.

The safest high-value subset is therefore:

```text
single owner on v3
resident direct heads
remove Book
SegmentEffects/checkpoints derived from the canonical log
```

Even if v4 is rejected, that subset can materially improve startup, memory, and
composed throughput. Fjall can be removed without a legacy migration only after
the authority audit and proven replacements settle every remaining role; no
import or compatibility rollout is implied.
