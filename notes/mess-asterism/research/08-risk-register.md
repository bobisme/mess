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
| R4 | **CONDITIONAL:** admitted exact dedupe becomes probabilistic by accident | 5 | 3 | 4 | 60 | retain all same-fingerprint candidates; compare full canonical key; forced-collision tests | any synthetic collision false absorb/miss |
| R5 | discardable snapshot discovery publishes missing, misidentified, non-durable-closure, or semantically overclaimed pack bytes | 4 | 2 | 4 | 32 | exclusive writer; UUID/PackId; commit frames; complete-closure durable proof; explicit trust mode; corruption always falls back | any snapshot changes the result vs full replay or makes canonical reads unavailable |
| R6 | checkpoint accepted for the wrong prefix of a fresh store's canonical log | 5 | 2 | 4 | 40 | segment epoch/cursor/root anchor; fallback; corruption suite | checkpoint+suffix digest differs from full scan |
| R7 | sequence-counter implementation has Rust UB/torn state | 5 | 2 | 5 | 50 | atomic fields only, Loom, bounded latch fallback | sanitizer/Loom issue or unexplained head pair |
| R8 | single owner becomes CPU bottleneck | 3 | 4 | 2 | 24 | profile, preallocation, vectorized group validation, bulk mode | current phase misses any prelocked `BN-2SU-FINAL` per-cell throughput/p99/barrier budget, uses an undeclared tolerance, or omits the raw matrix |
| R9 | SealPack install ordering trusts missing/corrupt pack | 4 | 2 | 4 | 32 | pack durable+dir sync before footer, hash binding, raw fallback | any wrong read rather than fallback/error |
| R10 | checkpoint/snapshot page GC deletes live pages | 5 | 2 | 5 | 50 | complete-closure Durable GC root, immutable IDs, ordered prune/delete, interrupted-GC model | acknowledged durable retained root references missing page |
| R11 | static function returns wrong nonmember result | 5 | 3 | 4 | 60 | exact key comparison; optional structure only | any result path omits verification |
| R12 | direct arrays explode on sparse/malicious IDs | 3 | 3 | 2 | 18 | writer-only dense allocator; validate/remap external IDs; sparse page directory | external ID directly controls page index |
| R13 | Book removal regresses hot aggregate loads | 3 | 3 | 2 | 18 | bounded capsule/block caches; block-native views | >10% sustained hot-load regression without RSS win justification |
| R14 | checkpoint/open still loads all names/heads and misses scale goal | 3 | 4 | 2 | 24 | resident/tiered profiles, compressed registry base, lazy pages | 100M-event open remains event-count proportional |
| R15 | per-segment effects grow with events rather than touched keys | 3 | 3 | 2 | 18 | net final updates only; dedupe separately epoched | effect size exceeds declared per-key gate |
| R16 | seal/checkpoint backlog grows without bound | 4 | 3 | 3 | 36 | bounded queues, backpressure/roll policy, operator alarms | unbounded disk/RSS or append outage |
| R17 | async caller cancellation leaks completion/queue state | 3 | 3 | 3 | 27 | owner owns admitted lifecycle; FIFO space-waiter drop reserves nothing; state-machine tests | committed capsule not published or queue bytes leaked |
| R18 | control TLV parser becomes attack surface | 5 | 3 | 3 | 45 | fixed caps, checked arithmetic, fuzz/Kani, frozen codec | panic/OOM/out-of-bounds on arbitrary bytes |
| R19 | unknown or mismatched segment version is silently skipped or opened writable | 5 | 2 | 4 | 40 | fail-closed decoder dispatch; v3-only open refusal; downgrade tests | any binary writes after encountering an unsupported version |
| R20 | performance claims depend on warm cache/device state | 2 | 5 | 3 | 30 | cold/warm separate, interleaved runs, raw samples, device telemetry | result not reproducible within tolerance |
| R21 | admitted dedupe retention deletes still-live canonical keys/results | 5 | 3 | 5 | 75 | exact `end_exclusive <= window_start` law or canonical boundary carrying live keys | dangling verification pointer, false absorb/miss, or checkpoint-loss rebuild mismatch |
| R22 | byte-ring admission starves a large waiter or cancellation leaks capacity | 3 | 3 | 3 | 27 | strict FIFO space waiters, oversize rejection, explicit drop states, Loom | indefinite waiter or reserved-byte mismatch |

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

**Decision status:** this risk is conditional. ADR 0002 leaves exact batch
idempotency optional and routes ADMIT/DECLINE to `bn-2ctq`; dormant Fjall
dedupe does not block deletion. If admitted, `bn-2ctq` must also freeze atomic
key+event encoding, retention, retry-result, authorization, allocation caps,
and composed A/B evidence before this component ships.

### R5 — snapshot publication outruns or overclaims bytes

**Failure mode:** discovery becomes visible while referenced pack bytes are
partial/missing, belong to a reused filename or another stream/schema, or are
only physically checksummed but presented as proof of a correct fold. A second
writer or flat all-head rewrite can also lose an otherwise valid concurrent
head.

**Mitigation:** ADR 0002 admits one OS-locked writer owner shared by all clones,
non-reused `(store UUID, PackId)` identities, independently commit-framed build
records, sealed-pack roll ordering, and immutable content-addressed discovery
pages under an independently resolvable generation root. `Buffered` and
`Durable` publication are explicit. `Durable` means complete-root survival: it
promotes every reachable pack frontier/page plus creation/rename directory
entry not proven by a prior acknowledged Durable root or synced proof ledger.
This includes unrelated state inherited from Buffered roots, existing
content-addressed pages whose writes were skipped, and a new active `.open`
pack directory entry. Readability or hash equality alone is not durability.
Any failed promotion, ledger/root barrier, validation, unknown sidecar version,
or corrupt page prevents acknowledgement or selects an older root/full replay
without blocking canonical reads. Equal coverage first validates the current
record, allowing corrupt-current repair while rejecting a different valid
state.

Discovery is keyed by stream plus stable author-supplied aggregate/schema,
fold, and codec identities; compiler names and unstable hashes are forbidden.
Coverage orders `Empty < Through(0) < Through(1) ...`. Copy-on-write updates
are bounded by changed paths rather than `O(all heads)`, and pack GC validates
every retained root graph before pruning roots and only then deletes unreachable
sealed content. UUID and pack ID non-reuse make stale-FD outcomes old-correct or
miss/replay, never ABA.

The physical record hash is not a fold proof. `UnverifiedCache` omits both
semantic hashes and assumes an honest producer. `CertifiedSnapshotRef` requires
and validates both state and canonical prefix hashes, but still cannot prove a
malicious writer executed the fold; that threat requires replay-and-compare or
a future proof system. Exactly one semantic hash is invalid.

Snapshot discovery remains acceleration, not authority. It emits no v3/v4
`SnapshotInstalled` record and cannot authorize deletion of canonical events.

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

**Failure mode:** a footer/root makes a partially installed artifact look
complete, a Durable root reuses Buffered-only bytes that disappear after power
loss, or GC mistakes a readable but non-durable graph for a safe deletion
anchor. A corrupt administrative traversal can also omit a live reference from
a destructive retention decision.

**Mitigation:** temp write, file barrier, rename, directory barrier, then durable
reference; immutable content hashes and UUID-scoped IDs; at least two retained
root generations. Reuse without I/O requires an exact prior acknowledged
Durable-root/ledger proof; otherwise existing pages, pack frontiers, and their
creation/rename directories are promoted before root acknowledgement.
Destructive GC is a durability boundary independent of save mode: under the
writer lock it first publishes a new Durable root with complete durable
closure, then validates additional retained graphs, prunes obsolete roots and
syncs the root directory, and only then deletes unreachable sealed content and
syncs every deletion directory. A promotion/root failure prevents pruning; a
failed prune barrier aborts before content deletion. `Buffered` may defer
reclamation but cannot weaken this ordering. The active build pack is never
collected.

Administrative enumeration pins one root with a shared deletion lease and uses
root-bound capped cursors with bounded memory. Doctor/inspect may label a scan
partial; retention must complete and revalidate the same root with no corrupt
page or cursor error before mutation. A one-million-head full traversal has
wall-time/RSS evidence.

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

Do not shard the canonical log prematurely. The historical Spike B
`Process >=1.20x` pre-flat-owner admission gate has already passed; the flat
owner is now the production baseline. Gate Phase 4 and later changes against
the prelocked `BN-2SU-FINAL` cells: Process and Group throughput, p99, and
barrier parity are compared per cell; no Process regression is accepted
without explicit product approval; and every tolerance is declared before the
run. Raw matrices are mandatory. The old 85%-of-bare headline was narrowed
after profiling showed it priced async API wake topology. A sharded validation
front end with one final ordered committer is a later option, but it increases
state complexity.

### R21 — dedupe retention boundary

If exact idempotency is admitted, canonical full keys and original results must
remain readable for the entire inclusive position window. With `W > 0`,
`window_start = w.saturating_sub(W)`. A segment `[base, end_exclusive)` is
deletable for dedupe only when `end_exclusive <= window_start`; an inclusive
end uses strict `<`. Otherwise a canonical retention boundary must carry every
still-live full key/result. Ambiguous `end_position` comparisons are forbidden.

### R22 — byte-ring fairness and cancellation

Space admission is strict FIFO. This avoids starvation but deliberately
accepts head-of-line blocking behind a large intent; an intent larger than the
total bound fails immediately. Dropping before admission unlinks a waiter that
reserved nothing. Dropping after admission leaves terminal processing to the
owner, which releases the exact byte reservation even if completion delivery
fails. Loom covers both cancellation phases and queue shutdown.

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
snapshot save group count/bytes/deadline and barrier count
snapshot root update bytes/pages and retained generations
snapshot durable-closure promotion bytes/files/pack frontiers
snapshot data/directory/proof-ledger/root barrier counts
snapshot administrative scan entries/pages/wall time/peak RSS/partial count
snapshot unverified/certified/invalid record counts
snapshot same-coverage repair/conflict counts
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

1. One canonical `Effect` definition, with registry application delegated to
   strict `RegistryState` after stable control-identity deduplication.
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
stores, and names already live in the canonical log. Research 13 and ADR 0002
completed the Fjall authority classification: snapshot discovery is the only
live Fjall-backed state role and is safely discardable, while direct CLI
`metaread`/doctor/inspect/retention and `rebuild-index --meta` paths remain
operational consumers to migrate or remove. `bn-3l8n` is the end-to-end
application/offline-tool adoption gate before deletion. The highest remaining
risks are therefore the v4 shift from event-only batches to mixed control/event
capsules (if v4 is adopted), fail-closed handling of unsupported formats,
exact replacement-state semantics, and accepting an accelerator or checkpoint
for the wrong canonical prefix.

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
