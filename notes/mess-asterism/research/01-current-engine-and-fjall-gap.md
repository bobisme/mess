# Research 01: current Mess engine, Fjall's role, and the remaining gap

**Current source checked:** `73625098` (2026-07-14)

**Historical baseline:** the original pack inspected `43e4aca0`; accepted
Spike B/C work has since changed the architecture.

**Purpose:** describe production now. Research 13 is the exhaustive authority
and deletion audit; this note is the performance-oriented summary.

## 1. Executive finding

The event engine is already Fjall-free:

```text
canonical events/registry -> v3 mess-log segments
live append owner          -> FlatOwner owning DirectCommitter
active pointers            -> in-memory ActiveIndex
sealed pointers/payloads   -> .pidx/.pcol/.filter or default-off .seal
reader payload cache       -> bounded decoded-capsule/block caches
resident Book              -> RegistryState + interned arcs + heads/allocator
production Fjall           -> opt-in FjallSnapshotBackend discovery only
```

`LogEngine::open`, append, head, stream/global read, seal, and recovery do not
construct or call `MetaStore`. The old per-append Fjall head batch, authoritative
name rows, `AppendGate`, per-append `spawn_blocking`, `PublishSequencer`, and
all-history payload/position Book were removed. Statements about those paths in
the original review are historical evidence, not current facts.

The remaining opportunity is narrower and clearer:

1. replace the discardable Fjall snapshot wrapper end to end, then delete the
   dormant `MetaStore`/Fjall production surface;
2. integrate already-proven dense heads, stream microblocks, SegmentEffects and
   checkpoints where their production gates still win;
3. keep exact idempotency, projection controls, and v4 conditional on their
   independent product/format decisions.

## 2. Current authority and Fjall inventory

| state | current producer/consumer | authority/fallback |
|---|---|---|
| events and registry | `LogEngine`; `$registry` is stream 0 | accepted v3 log prefix is canonical |
| stream heads | compact `Book` + `ActiveIndex`, rebuilt from batch headers/sealed directories | rebuildable accelerator |
| snapshot heads | `FjallSnapshotBackend` and offline tooling | discardable discovery; loss/corruption must replay |
| projection checkpoints | `MetaStore` tests only; social app uses its own file | dormant component; app sidecar is discardable |
| dedupe/order rows | `MetaStore` tests/bench only | dormant; no append key or canonical production bytes |
| shared high-waters | internal `MetaStore` bookkeeping | neither event authority nor a usable cross-table frontier |

The seven `MetaStore` keyspaces and every API/deletion consequence are enumerated
in research 13 §4. Snapshot discovery is the only production Fjall blocker.
ADR 0002 admits immutable self-describing snapshot packs plus discardable
copy-on-write discovery metadata. It explicitly emits no `SnapshotInstalled`
record and leaves projection/idempotency decisions optional.

## 3. Current append critical path

The production v3 path is logically:

```text
producer prepares an intent and enters the byte/count-bounded ring
FlatOwner dequeues in deterministic group order
  -> resolve/stage RegistryState assignments
  -> validate expected versions and allocate registry IDs/positions
  -> form an ordered unit: optional $registry batch before domain batch
  -> DirectCommitter writes each batch (current: k positioned writes)
  -> durability: no barrier (Process), one sync per batch (Os), or one
     covering barrier for the gathered group (Group)
  -> apply strict registry fold and ActiveIndex/head publication in order
  -> advance published/read watermark
  -> complete waiters
```

The owner is the committer/writer. Spike B measured a 35% loss when an owner
fronted the old committer thread, so that topology is rejected. B1 speculative
cross-barrier pipelining is also rejected: it did not improve durable throughput,
lost 5–18% at 64 writers, and introduced a nondeterministic barrier cut.

The current physical writer uses positioned writes: `Process` has no barrier,
`Os` is sync-per-batch (so a registry+domain unit uses two), and `Group` uses
one covering barrier for the gathered group. One
gathered/coalesced write where size permits, with `writev` or bounded chunks for
oversized groups, remains an Asterism performance target rather than a current
implementation claim.

## 4. State frontiers

Use four distinct terms:

```text
speculative       validation accepted; may still be discarded on I/O failure
written/accepted  write returned; Process completion can become eligible
crash-stable      covered by a successful Os/Group barrier
published         reader-visible effects
```

For `Os` and a closed `Group`, `published <= crash-stable`; success completion
implies both, although a short post-barrier interval can have strict inequality.
For `Process`, the exact crash-stable prefix is unknowable until recovery and a
published prefix may exceed the eventual recovered prefix. Calling the Process
watermark “durable” overstates what the process knows.

## 5. The Book after Spike C

The current `Book` is not an all-history event mirror. It contains:

- the canonical in-memory `RegistryState` fold;
- per-ID interned stream/event-type `Arc<str>` projections;
- stream heads;
- `registry_next_version` and a runtime-only registry poison bit.

It is O(streams + event types). Payloads and per-stream event-position vectors
are gone. Reads resolve through `ActiveIndex`/sealed directories and fetch log
or `.pcol`/SealPack bytes through bounded caches. Recovery scans metadata for
the unsealed tail and can use verified sealed directory summaries; it does not
rehydrate old payload objects.

The measured 10M-event result was 268 MiB peak RSS versus 1,872 MiB for the old
Book baseline, with reopen improving from 5.91 s to 3.76 s in the composed
decision run. “Remove Book” in future work means replacing its remaining
heads/registry publication role, not repeating the completed payload deletion.

## 6. Active and sealed indexes

Production block-native stream/global reads now use `ActiveIndex`; the review's
older statement that reads bypass it became obsolete after Spike C. Spike F
proved stream-side fixed microblocks with linear search and an eight-block skip
chain, but that mechanism is not integrated yet. Spike J rejected the proposed
stride-8 global array: real `pread` header scans were 10–17x slower than the
incumbent. The accepted global fallback is stride-1, 16 bytes per batch.

The sealed directory still has a mutable `HashMap` reconstruction path. Spike H
admitted bitvector+rank for dense/real segment distributions and a deterministic
sorted/HashMap fallback. SealPack is admitted behind a default-off option; it
reduces three sealed-tier opens to one and adds event-type IDs, but its named
RSS/footer-binding follow-ups must land before default-on.

The seal pipeline no longer consumes Book payloads. It gates on the published
watermark, re-reads the rolled raw segment, and verifies semantic equality:
payload bytes plus pointer/version/global-position results. Compressed sidecar
byte identity is only an auxiliary same-build regression check.

## 7. Measurements and acceptance discipline

The old 4-writer x 10-event pair (2.038M bare, 1.710M composed, about 84%) was a
valid orientation point; the old 63% quotient was not matched. The later locked
32-cell matrices supersede both as decision evidence. Ratios vary materially by
batch size, payload, concurrency, and barrier count, so every gate is
batch-stratified and cell-matched. Accepted and rejected raw rows, machine/load
metadata, and barrier counts are committed beside each report.

Head lookup has not been demonstrated as a bottleneck in measured composed
workloads. Dense heads earn their place by enabling bounded resident state,
coherent ownership, and fast checkpoint recovery; nanoseconds alone are not the
thesis. Likewise, the Fjall dedupe comparison is a synthetic component baseline,
not current production behavior.

## 8. Current work order

```text
snapshot packs/discovery per ADR 0002
  -> social/public API/CLI/retention/backup adoption
  -> remove dormant MetaStore and production Fjall dependency

independent integrations:
  dense heads
  stream microblocks + stride-1 globals
  SegmentEffects/checkpoints (parallel build, ordered sequential apply)
  bitrank directory / SealPack follow-ups

conditional decisions:
  exact batch idempotency (bn-2ctq)
  projection controls (bn-11mk)
  v4 product admission (bn-1ojm)
```

There are no existing stores or users to migrate. Research 06 is a superseded
historical trap analysis, not an implementation roadmap.

## 9. Source and evidence map

- current authority/call graph: `research/13-authority-and-fjall-deletion-map.md`
- capability authority: `docs/adr/0002-asterism-capability-authority.md`
- owner/current engine: `crates/mess-store/src/engine.rs`
- direct writer: `crates/mess-log/src/committer.rs`
- snapshot wrapper: `crates/mess-store/src/fjall_snapshot.rs`
- dormant metadata component: `crates/mess-index/src/meta/mod.rs`
- locked/raw evidence: `spikes/baseline_matrix/`, `spikes/flat_combined_append/`,
  `spikes/open_without_book/`, and `spikes/composed_decision/`
