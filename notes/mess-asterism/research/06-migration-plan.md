# Research 06: migration from Fjall/v3 to Asterism/v4

> [!IMPORTANT]
> **Historical design record — superseded 2026-07-13.** Mess has no users and
> no existing stores, so there is no installed v3/Fjall state to migrate. The
> M0–M9 rollout, compatibility shadows, rollback matrix, mixed-version cutover,
> and `mess migrate` commands below are **not on the implementation roadmap and
> will not be built**. Development proceeds from fresh stores instead: names are
> already canonical `$registry` records in the log. Fjall retirement requires
> auditing every remaining keyspace and making each role log-derived or
> explicitly discardable, but it does not require migrating legacy stores. See
> [design §20](../design.md#20-migration) for the current decision.
>
> This document remains intentionally intact as counterfactual architecture and
> trap analysis. Its authority inventory, ordering rules, downgrade fences, and
> data-loss cases are useful if Mess ever acquires persisted stores before a
> future format transition. In the remainder, “must”, phase exits, and operator
> commands describe that hypothetical legacy-store migration; they are not
> commitments to ship migration machinery.

## 0. Why this superseded plan is retained

The plan identified a durable lesson even though its rollout is unnecessary:
before deleting a persistence domain, classify every value as canonical,
derivable, or operationally optional. At the time of writing, Fjall's name
tables were the sole non-derivable authority. That finding led directly to the
log-derived `$registry`; it did **not** justify carrying the rest of the
migration program forward once the project confirmed that no stores exist.

The remaining reusable traps are independent of this abandoned rollout:

- a referenced numeric ID must always have an earlier canonical registration;
- dedupe migration, if ever needed, requires full keys rather than fingerprints;
- snapshot heads must not outrun durable blobs;
- checkpoints must bind to the exact log prefix they summarize;
- unknown on-disk versions must be refused, never skipped or truncated.

---

## Historical migration design (not scheduled)

## 1. Migration constraints

The current store has three distinct truth classes:

1. v3 event capsules are canonical for events;
2. Fjall stream/type name tables are currently canonical for ID interpretation;
3. other Fjall metadata is intended to be rebuildable but may include operational state such as snapshots/checkpoints that must not be silently lost.

A migration that simply deletes `meta/` would preserve event bytes but can make numeric IDs anonymous and can discard valid snapshot/checkpoint state. Therefore the migration must first make every persistent semantic state representable in the canonical/control log.

## 2. Compatibility goals

- No in-place rewrite of existing event segments is required.
- A store may contain v3 and v4 segments; format version is per segment.
- Upgrade is online or bounded-stop, not a full historical rewrite.
- Before the v4 point of no return, rollback to the old engine is supported.
- After a v4 capsule is durable, old binaries fail read-write open loudly.
- A read-only export tool remains possible across versions.
- Every cutover phase has a differential shadow and a rollback procedure.

## 3. Phase M0 — inventory and state digest

Add `mess doctor migration-inventory` that records:

```text
log durable end
segment list/epochs/versions
Fjall keyspaces and high-waters
stream name count and digest
stream head count and digest
type name count and digest
snapshot head count and digest
checkpoint count and digest
dedupe configuration/window and digest
active/unsealed segments
sealed sidecar inventory
```

The digest uses canonical key order and stable encoding. It is stored outside the data directory or printed for operator capture; it is not authority.

Block migration when:

- name IDs are not dense/unique as expected;
- Fjall name rows are missing for IDs used by v3 log bytes;
- a snapshot head names a missing/corrupt blob and no documented fallback exists;
- the log itself fails full verification;
- a background seal/checkpoint job is in an indeterminate state.

## 4. Phase M1 — introduce state-kernel shadows on v3

Implement direct head/snapshot tables and active dedupe behind a feature, but continue serving and writing Fjall.

On open:

1. load Fjall state;
2. construct shadow kernel state from Fjall plus log recovery;
3. compare digests.

On append:

```text
current canonical log append
current Book/ActiveIndex/Fjall publish
shadow kernel effect apply
assert current head == shadow head
sample/compare dedupe and registry lookups
```

The shadow must observe exactly the same published watermark. A mismatch emits enough capsule/key detail to reproduce and is fatal in tests.

Exit gate: multi-day stress/soak and all randomized differential suites show zero mismatches.

## 5. Phase M2 — block-native reads and Book retirement

This phase changes no durable format.

### M2.1 Dual read

For a sampled fraction:

```text
result_book = old Book path
result_block = active log / sealed payload path
assert bytes and positions equal
return old result
```

Then return the block path while still comparing sampled old results.

### M2.2 Stop extending the Book

Once active/sealed reads cover all APIs, stop adding new payload objects and stream-position vectors to the Book. Keep only registry/head compatibility state. Reopen no longer decodes historical payloads.

### M2.3 Remove Book

Delete all-history fields after a release of shadow operation. Keep a diagnostic full-scan command for comparison.

Rollback: re-enable the old engine and rebuild Book from v3 log; no format changed.

## 6. Phase M3 — single-owner append/publish

Route append intents through the state-kernel owner while continuing to write v3 capsules and Fjall metadata.

Cutover sequence:

1. owner validates against shadow direct heads;
2. old Fjall head read is sampled for comparison;
3. existing committer writes v3 bytes;
4. owner applies direct state and then writes the compatibility Fjall group;
5. readers move to direct state;
6. Fjall head writes become shadow-only;
7. stop Fjall head writes after soak.

The per-stream gates and publish sequencer are removed only after cancellation/race tests pass under the owner.

Rollback: owner can be disabled; v3+Fjall remains current.

## 7. Phase M4 — canonical registry import

### 7.1 Import object

Create one or more `RegistryImportedV1` control records containing canonical sorted rows:

```text
stream IDs and names/categories
event-type IDs, names, codec/schema metadata
category IDs and names
codec/dictionary objects
aliases
next allocator values
source Fjall digest
```

For large registries, chunk records and finish with a manifest record that commits the ordered chunk hashes. The import is accepted only as a complete manifest-described set.

### 7.2 Where to write before v4

Options:

- write explicit `$registry` v3 event batches and allow them to consume user global positions;
- write a dedicated migration control file with v4 framing, then switch to v4 segments;
- start a v4 segment and put the import in control-only capsules.

Recommended: roll to the first v4 segment and write the import as v4 control-only capsules before accepting v4 user events. This avoids a temporary second authority.

### 7.3 Verification

After import is durable:

1. replay the canonical registry into a fresh state;
2. compare its digest and every row against Fjall;
3. scan all v3 segment headers/subframes and verify every referenced ID resolves;
4. record `registry_canonicalized_at` in the v4 kernel state.

Fjall name writes continue in shadow for one release but are no longer authoritative.

## 8. Phase M5 — v4 segment boundary

### 8.1 Roll, do not rewrite

Seal the current v3 active segment. Open a new v4 segment with a new epoch and base global position. The directory now contains mixed versions:

```text
v3 sealed segments: user events, names from canonical import
v4 segments: user events + control preludes
```

The recovery dispatcher selects the decoder by segment header version.

### M5.2 Synthetic effects for v3

Build SegmentEffects for v3 segments using:

- event headers/subframes for head transitions;
- canonical imported registry for name interpretation;
- old snapshot/checkpoint/dedupe data imported as control state at the v4 boundary.

V3 effects are advisory and can be rebuilt.

### M5.3 Downgrade fence

Write a small human-readable `FORMAT` advisory file and rely on actual segment headers for truth. Old binaries detect unknown v4 segments and refuse read-write open. They must not truncate or skip them.

## 9. Phase M6 — dedupe cutover

Dedupe is particularly sensitive because a retry may target a pre-upgrade event.

> **Current qualification:** production dedupe was dormant and there are no
> stores to migrate. A hypothetical future M0 must run `mess doctor
> dedupe-status`; only a nonempty externally populated `MetaStore` would enter
> the import path below. An empty inventory means idempotency, if admitted, is
> a new feature rather than a migration.

### M6.1 Seed live window

At greatest committed inclusive position `w`, require `W > 0`, compute
`window_start = w.saturating_sub(W)`, and export every Fjall dedupe row with
`position >= window_start` into canonical dedupe epoch records or a
`DedupeWindowImported` control object. Include full keys, original results, and
canonical capsule pointers where available.

If current v3 event bytes do not contain full dedupe keys, the import record becomes their canonical source until they expire. Do not store only fingerprints.

### M6.2 Dual query

For the entire configured window duration/span after cutover:

```text
answer_new = epoch dedupe
answer_old = Fjall dedupe
assert equal
return answer_new
```

Once all imported entries expire and no mismatch occurred, the old dedupe keyspaces can be dropped.

Rollback before expiry: continue using Fjall. Rollback after Fjall deletion requires rebuilding from canonical import/control records, which the migration must test before deletion.

## 10. Phase M7 — snapshots and checkpoints

### 10.1 Snapshot migration

> **Current qualification:** ADR 0002 rejects this authority transfer for the
> actual product. Snapshot discovery remains discardable and moves to immutable
> packs/copy-on-write discovery without a `SnapshotInstalled` v3/v4 record.
> The steps below are counterfactual only.

For every live Fjall snapshot head:

1. read and verify the existing blob;
2. append it to a snapshot pack or register the existing positional file as an external blob reference;
3. write a `SnapshotInstalled` control capsule;
4. compare the new direct head slot with Fjall;
5. retain old files until the new snapshot path has survived reopen/crash tests.

Bulk-copying into packs is preferable but can be background work. External references permit a low-risk first cut.

### M7.2 Projection checkpoints

Write current checkpoints as control-only capsules with a migration source marker and frontier encoding. Projections resume from the canonical controls. Keep Fjall shadow updates until each projection has advanced beyond its imported checkpoint.

## 11. Phase M8 — kernel checkpoint bootstrap

After canonical registry/dedupe/snapshot/checkpoint state exists:

1. build the first full Merkle-page checkpoint at the current durable cursor;
2. close the engine;
3. open using checkpoint+suffix only;
4. compare state digest to a full mixed-v3/v4 scan;
5. corrupt/remove checkpoint pieces and verify fallback;
6. repeat under interrupted checkpoint writes and GC.

Only then make checkpoint open the default.

## 12. Phase M9 — Fjall retirement

Delete keyspaces in order:

```text
stream_heads
snapshot_heads
checkpoints
dedupe + dedupe_order
high-water rows
stream_names
type_names
```

Before physical deletion, ship one release where they are read-only shadows and `mess doctor` can rebuild/compare them. Then move the old `meta/` directory to `meta.retired.<timestamp>` rather than immediately unlinking it. Provide an explicit cleanup command after the rollback window.

## 13. Upgrade state machine

Persist an advisory migration record in the canonical control log:

```text
M0 InventoryComplete
M1 ShadowKernelVerified
M2 BookRetired
M3 OwnerCutover
M4 RegistryCanonical
M5 V4Started
M6 DedupeCanonical
M7 SnapshotCheckpointCanonical
M8 KernelCheckpointVerified
M9 FjallRetired
```

Each transition is monotone and idempotent. Recovery derives the phase from canonical controls and validates filesystem artifacts. The operator-facing marker is not a second truth.

## 14. Rollback matrix

| latest phase | rollback path |
|---|---|
| M0–M3 | disable feature; v3+Fjall unchanged |
| M4 before v4 user events | ignore import and continue v3+Fjall, or retain it for retry |
| M5+ | old binary cannot write; use new binary in compatibility mode that continues v4 and can shadow Fjall |
| M6 before old dedupe deleted | switch query back to Fjall |
| M6 after deletion | rebuild Fjall dedupe from canonical import/control records if rollback tool is supported |
| M7 | old snapshot files retained; switch head source back if v4 log remains readable by compatibility engine |
| M9 | restore `meta.retired` only if no later canonical controls would be lost; otherwise rebuild a compatibility view |

Rollback never rewrites or truncates canonical v4 segments.

## 15. Operator commands

```text
mess migrate plan PATH
mess migrate inventory PATH
mess migrate shadow-enable PATH
mess migrate status PATH
mess migrate verify PATH --full
mess migrate canonicalize-registry PATH
mess migrate cutover-v4 PATH
mess migrate import-dedupe PATH
mess migrate import-snapshots PATH
mess migrate build-checkpoint PATH
mess migrate retire-fjall PATH
mess migrate rollback-view PATH --to=fjall-compat
```

Every command is restartable and prints its exact canonical cursor.

## 16. Data-loss traps to test explicitly

- a v3 ID used in the log but missing from Fjall names;
- a name table row durable but first event absent;
- first event durable but old name row missing due to historical bug;
- two names mapped to one ID or one name mapped to two IDs;
- dedupe key represented only by Fjall and not v3 bytes;
- snapshot head with missing blob;
- checkpoint ahead of projection side effects;
- v4 registry import interrupted between chunks and manifest;
- old binary opening mixed-version directory;
- operator deleting retired Fjall before dedupe/snapshot import verification;
- kernel checkpoint anchored before/after the wrong mixed-version segment boundary.

## 17. Exit criteria

Fjall is removable only when:

```text
full canonical scan reconstructs every semantic table
checkpoint+suffix == full scan digest
all v3 referenced IDs resolve from canonical import
exact dedupe survives old-Fjall deletion and retry boundary tests
snapshots/checkpoints survive old-Fjall deletion
mixed v3/v4 recovery and export are tested
rollback/compatibility policy is documented and executable
```

In the historical scenario this migration would have been part of the storage
engine, not an afterthought. Its most important diagnosis has since been acted
on: the temporary Fjall name authority was removed by making `$registry` part of
the canonical log. With no legacy stores, none of the rollout machinery above
is required.
