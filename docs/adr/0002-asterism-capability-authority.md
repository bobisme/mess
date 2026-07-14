# ADR 0002: Bound Asterism snapshot authority and optional controls

- Status: Accepted
- Date: 2026-07-14
- Bone: bn-k8qd (Freeze snapshot choice and conditional control constraints)
- Deciders: mess-control-adr
- Evidence: `notes/mess-asterism/research/13-authority-and-fjall-deletion-map.md`

## Context

The original Asterism design grouped snapshot installs, projection checkpoints,
and exact dedupe with registry assignments as canonical log controls. The
source-traced authority audit disproved that grouping:

- `$registry` records are canonical log authority;
- `FjallSnapshotBackend` is the only live production Fjall consumer, but its
  head is discardable discovery state: losing it causes a correct full replay;
- `MetaStore` projection checkpoints have no production caller;
- `MetaStore` dedupe has no production caller, and the public append API carries
  no idempotency key.

There are no users or existing stores to migrate. Fjall deletion therefore
does not justify promoting an accelerator or dormant API into event authority.
Snapshot discovery is decided below. Projection checkpoints and exact batch
idempotency remain separate product decisions; this ADR freezes only the
authority and encoding envelope that an admitted branch must obey.

The evidence has different strengths and must not be conflated:

- The production flat-owner matrix is a locked, interleaved 32-cell baseline.
- The snapshot law and social application exercise the real public path. The
  documented 100,307-event social run measured a 32.364 ms full replay and a
  5.052 ms snapshot-assisted load (6.4x), establishing user value for snapshot
  acceleration, not a replacement-performance gate.
- The current Fjall point-read run (646.3 ns/head and 914.7 ns/dedupe) ran at
  load1 10.40 and is synthetic orientation only. The older snapshot-miss
  measurements (0.3–1.9 us) are likewise orientation, not admission gates.
- Spike G proves that exact epoch dedupe is mechanically strong, including
  forced-collision exactness, but its Fjall comparison is synthetic because no
  production append path performs dedupe.

## Decision

### Capability classification

| capability | current state | decision | authority after this ADR |
|---|---|---|---|
| Registry | canonical and live | **KEEP** | `$registry` records in the accepted v3 log prefix; `RegistryState` remains the one strict fold |
| Snapshot discovery | live, discardable acceleration | **ADMIT replacement** | immutable self-describing snapshot packs plus discardable discovery metadata; the event log remains the only event authority |
| Projection checkpoints | dormant in `MetaStore`; app-owned form is live | **OPTIONAL — decide in `bn-11mk`** | application-owned discardable sidecar unless and until that bone admits a canonical product capability |
| Exact batch idempotency | dormant; no public intent | **OPTIONAL — decide in `bn-2ctq`** | no current engine contract; Spike G is candidate-mechanism evidence only |

These classifications are independent. In particular, projection checkpoints
and dedupe do not block snapshot replacement or Fjall deletion, regardless of
their later product decisions.

### 1. Snapshot discovery uses packs plus discardable metadata

The replacement preserves the existing safety contract and strengthens its
storage protocol without adding a log transition.

#### Stable public identity and coverage

Snapshot compatibility is an explicit, stable public value:

```rust
pub struct SnapshotCompatibility {
    pub aggregate_schema_id: StableSnapshotId,
    pub fold_version: u32,
    pub codec_id: StableSnapshotId,
    pub codec_version: u32,
}

pub enum SnapshotCoverage {
    Empty,
    Through(u64),
}
```

`StableSnapshotId` is an author-supplied, length-capped byte/string identifier
with a canonical encoding. It must never be derived from `type_name`, `TypeId`,
a process-randomized hash, or another compiler/build-dependent value. Stream
name plus `SnapshotCompatibility` is the complete lookup key. `bn-ozi5` makes
the pre-1.0 source-breaking `Snapshottable`/`SnapshotStore` change: snapshot
implementations supply aggregate/schema and codec IDs, and load accepts the
complete identity. An implementation may additionally expose ordered fallback
candidates, but the list is strictly capped and every candidate carries its
identity; an unqualified unbounded "latest for stream" scan is forbidden.
`bn-3l8n` migrates every application, example, test, CLI, and public document to
the stable identity.

Coverage is encoded as a tag plus, only for `Through`, a `u64`. Its total order
is exactly:

```text
Empty < Through(0) < Through(1) < ... < Through(u64::MAX)
```

Code must compare `SnapshotCoverage`, never a raw `covered_version`. Empty and
event version zero therefore cannot collide. Sequential and concurrent
`Empty -> Through(0)` and `Through(0) -> Empty` publication tests are mandatory.

#### Record shape and trust boundary

An immutable snapshot record is self-describing and contains at least:

```text
format version
stream name bytes                    # canonical identity at the generic Backend seam
optional registry stream id          # acceleration/cross-check only
SnapshotCompatibility
SnapshotCoverage
state bytes length + bounded integrity hash
semantic trust mode
optional state_hash algorithm + value
optional event_prefix_hash algorithm + value
record checksum/hash
```

There are exactly two semantic trust modes:

- `UnverifiedCache`: both semantic hashes are absent. The record hash still
  protects framing and physical bytes, while compatibility, coverage, bounds,
  and codec validation protect routing. This mode assumes an honest snapshot
  producer and catches accidental corruption; a buggy or compromised writer
  can still persist plausible but wrong state.
- `CertifiedSnapshotRef`: both semantic hashes are present with known
  algorithms. The loader verifies `state_hash` over the encoded state and the
  canonical event-prefix certificate for `(stream, coverage)`; `Empty` uses the
  specified genesis prefix hash. Exactly one semantic hash, or an unknown hash
  algorithm, is invalid and becomes a miss.

The record hash is never evidence that the state is the result of the fold.
Even `CertifiedSnapshotRef` detects byte corruption, identity mixups, and a
wrong event prefix, but a malicious writer can hash a deliberately wrong
state. Treating writers as untrusted requires full replay-and-compare or a
future independently verified fold proof. Doctor/inspect reports unverified,
certified, and invalid records separately.

#### Single writer, immutable identity, and offline readers

One exclusive operating-system sidecar lock/token covers the snapshot root.
Opening a second writer process fails. All in-process `SnapshotStore` clones
share the same writer owner and serialize append, head comparison, and root
publication, so an older generation cannot overwrite a concurrent update.
Offline/read-only opens never create, truncate, repair, rename, delete, or take
the writer lock; they may only validate existing final artifacts.

Under the writer lock, a store identity file creates and durably records a
random store UUID exactly once. Pack identity is `(store_uuid, monotonically
increasing pack_sequence)` and root identity is `(store_uuid, monotonically
increasing generation)`; neither sequence or filename is reused. A reader that
finds a missing/corrupt identity treats the sidecar as unavailable. A writer
may create a new UUID namespace and ignore old artifacts, but must never adopt
their IDs. This prevents ABA after deletion, restore, or stale descriptors.
Temporary/staging names have a reserved suffix and are never open candidates.

Root descriptors are immutable, checksum- and commit-marker-framed final files
whose names include UUID and generation. Open scans final descriptors for the
current UUID in descending generation order and selects the highest valid,
independently resolvable root; corruption falls back to an older retained root
or replay. A read-only opener does not repair the directory. Only the locked
writer may scan and truncate a torn tail of the active build pack before the
next append.

#### Build-pack protocol

The active `pack-<uuid>-<sequence>.open` file has no final footer. Every
snapshot is an independently commit-framed record: a bounded header containing
identity and total length, body bytes, checksum/record hash, and final commit
marker. A discovery leaf names the exact `(PackId, record_offset,
record_total_length, record_hash)`. Readers use exact-range `pread`, then
validate every named field and frame, so an offline read may safely overlap a
writer appending a later record.

Rolling a build pack is a state transition: append the bounded index and final
footer with checksum and commit marker, sync through the footer, rename/seal to
the final `.pack` name, and sync the directory. A sealed pack is never opened
for append. Resolution is by `PackId` and accepts the appropriate `.open` or
`.pack` path during the rename transition without confusing identities. Writer
recovery truncates an uncommitted active record, completes a footer-durable but
unfinished seal, and detects a rename whose directory entry was not durable.
Tests interrupt every roll step and cover read-versus-append, stale file
descriptors during GC, and pack-name transitions. A stale descriptor may yield
old correct bytes or a miss/replay; UUID, `PackId`, and record hash prevent it
from yielding a different snapshot.

#### Bounded copy-on-write discovery

Discovery is an immutable, checksummed, content-addressed copy-on-write radix
tree (an equivalently bounded sharded tree is permitted if it meets the same
contract), keyed by stream name and `SnapshotCompatibility`. A save group
rewrites only changed leaves and their paths, `O(k log N)` for `k` updated heads,
instead of rewriting an `O(N)` flat manifest. A small independently resolvable
root descriptor contains store UUID, generation, root-page hash, format,
publication mode, durable-proof generation/closure hash when applicable,
checksum, and commit marker. It does not require a predecessor journal. Pages
and descriptors are immutable; missing or corrupt pages reject that root and
fall back or replay. A `Buffered` descriptor can never be mistaken for an
acknowledged complete-closure `Durable` descriptor after restart.

At least two complete root generations remain usable. Ordinary open and point
lookup may be lazy and must not linearly enumerate all heads. Administrative
enumeration is explicit instead of being smuggled into those paths:

```text
pin_snapshot_root() -> PinnedSnapshotRoot
scan_snapshots(pin, cursor, limit) -> { entries, next_cursor, diagnostics }
```

The pin names exactly one validated store UUID/root generation and holds a
non-mutating shared deletion lease that GC honors. The opaque checksummed cursor
binds that root identity plus the last key/tree position; a cursor from another
root is rejected. `limit` is nonzero and capped. Each page performs a
lexicographic seek/continuation in `O(log N + limit)` work and
`O(tree height + limit)` memory, never materializing all heads. Doctor/inspect
may return validated entries plus an explicit partial/corrupt diagnostic.
Destructive retention must run against one pinned root, finish every page with
no diagnostic, and revalidate the same root before mutation; a missing page,
invalid cursor, changed/lost pin, or partial traversal fails closed. Read-only
tools do not take the writer lock or mutate files merely to pin a root.

The required scale matrix uses 1, 1,000, 100,000, and 1,000,000 heads; state
sizes 64 B, 4 KiB, and 1 MiB; and low, medium, and saturation save rates
(nominally 1, 100, and 10,000 saves/s where the host can sustain them). It
records root-update bytes, save/load/open p50 and p99, files, and peak RSS. A
complete paginated administrative scan of the 1,000,000-head root separately
records wall time, pages/entries, peak RSS, and corrupt/partial behavior.
Unsupported cells are explicit capacity failures, not silently omitted
measurements.

#### Durability and publication

Snapshot publication exposes two explicit modes:

- `Buffered` is the default matching today's cache semantics. It preserves
  write-before-root ordering and atomic process visibility, but does not promise
  survival of power loss. Lost cache state simply replays events.
- `Durable` promises complete-root survival: before acknowledgement, the final
  root descriptor and every pack byte range/page reachable from it, including
  every file creation/rename directory entry, are durable. It is not merely a
  promise about the newly saved head.

The writer maintains a checksummed durability-proof ledger containing exact
immutable page/file identities, per-pack synced frontiers, and proven directory
entries. Ledger entries are usable only after their own durability barrier and
are discardable: missing/corrupt proof causes re-promotion, never trust. An
immutable file or subtree may be reused without I/O only when its exact
identity is reachable from a previously acknowledged `Durable` root or covered
by that durable ledger. A mutable `.open` pack may skip I/O only through its
proven synced frontier; any newly referenced higher record end forces another
pack sync, and an unproven creation entry forces its directory sync. Mere
existence, content-address equality, readability, or reachability from a
`Buffered` root is not proof.

The single owner may group clone save intents by declared byte/count/deadline
limits, append their records, and publish one root generation. A group
containing any `Durable` intent follows the durable ordering for the whole
group; every completion waits for its mode's publication frontier. Group size,
deadline, bytes, and barrier counts are observable.

Within one serialized group, publication is:

1. append and re-read/validate every complete record frame;
2. build and validate the changed immutable discovery pages;
3. for `Durable`, compute the complete reachable closure against the last
   acknowledged durable root/ledger and promote every unproven member: sync
   each referenced pack through its maximum required record end, sync every
   unproven page/file even when content addressing skipped its write, and sync
   every unproven creation/rename directory entry. This explicitly includes a
   newly created active `.open` pack and all `Buffered`-only pages or records
   for unrelated keys inherited by the new root;
4. durably publish the updated proof ledger only after those barriers; any
   promotion or ledger failure prevents durable-root acknowledgement;
5. write and sync the root under a reserved temporary suffix, rename to its
   UUID/generation final name, and sync the root directory. Only then mark it
   as an acknowledged `Durable` root;
6. publish the root in memory and complete saves. `Buffered` still writes and
   atomically renames its descriptor, but skips closure promotion/proof-ledger
   publication and makes no promise that the step 3--5 barriers survived;
   validation and atomic process visibility remain mandatory.

A crash before root publication leaves only unreachable records/pages. A final
valid root always names complete bytes. Benchmarks compare `Buffered` against
ordinary current Fjall behavior. `Durable` is compared only against a control
that syncs the Fjall snapshot blob and its directories and explicitly persists
Fjall; if exact equivalence cannot be constructed, the run says so and uses a
prelocked absolute budget. Grouped durability is reported separately with its
actual group policy and barrier count. All modes record closure-promotion
bytes, files, pages, pack frontiers, directory barriers, ledger barriers, and
the count of no-I/O reuses justified by a prior durable proof.

#### Coverage conflicts and same-head repair

Publication is monotone by `SnapshotCoverage` within one `(stream,
compatibility)` key. A higher coverage replaces a valid current head and a
lower coverage becomes an orphan. For equal coverage, the writer first reads
and fully validates the currently named record:

1. missing, unreadable, or invalid current bytes may be superseded by a new
   valid record at the same coverage, with a repair diagnostic;
2. a valid current record with the same record/state identity is idempotent;
3. a valid current record with different state identity is a conflict and
   remains current.

This order permits a corrupt-current re-save without weakening split-brain
detection. Different compatibility identities always have separate heads, so
a late old-fold save cannot hide or delete a new identity. Required tests cover
corrupt-current repair plus concurrent lower/higher/equal coverage saves.

#### Retention and garbage collection

GC is writer-only and serialized with publication. Destructive reclamation is
its own durability boundary regardless of the snapshot save mode: `Buffered`
may defer physical reclamation, but it may not non-durably prune a root and
then delete content that root names. Readability of the graph is insufficient.
GC follows this order:

1. finish or quiesce the current publication group;
2. use the `Durable` publication protocol to promote the complete closure and
   acknowledge a new durable root containing all heads to survive reclamation;
   any reused `Buffered` page/pack or unproven directory entry is synced first;
3. validate the complete graph from every additional root generation selected
   for retention (at least two roots total); abort GC if any retained graph is
   unreadable. The newly durable root, not this readability check, is the
   post-crash survival anchor;
4. remove only older root descriptors, then unconditionally sync the root
   directory and abort before content deletion if that barrier fails;
5. recompute reachability from the remaining roots plus the active `.open`
   pack, delete only unreachable sealed packs/pages, and unconditionally sync
   every affected deletion directory before reporting GC complete.

The active build pack is never collected. Pack compaction writes a new pack,
publishes a complete-closure durable root that names it, and prunes old roots
before the old pack can become eligible. A closure-promotion/root-sync failure
prevents pruning; a prune-directory sync failure prevents dependent deletion;
a later deletion-sync failure reports incomplete GC and is repaired by the next
reachability pass. A crash before root pruning leaves extra roots; a crash
after pruning but before deletion leaves orphans. The durable GC root remains
fully resolvable even if a surviving `Buffered` root validates only as a miss.
An offline reader holding an old root or file descriptor may finish from
immutable old bytes or encounter a miss and replay, but ID non-reuse prevents
ABA. Reserved temporary files are excluded from both open and reachability.

#### Load, corruption, and retention

Load validates the root path, immutable pack framing, exact record identity,
stream, compatibility, coverage, trust-mode fields, and codec before decoding.
Missing/corrupt bytes, unknown formats, a snapshot ahead of the stream, a
compatibility mismatch, decode failure, or semantic-hash mismatch selects an
older explicitly retained candidate or full replay. It does not make the event
store unavailable.

Snapshots do not authorize event-log retention. A destructive retention path
must fail closed unless a separately admitted retention-boundary contract proves
the retained canonical history sufficient. It must also complete the pinned
administrative scan above without partial/corrupt diagnostics. Doctor/inspect
may report a partial degraded snapshot inventory, but opening and ordinary
reads remain possible from the log.

#### Encoding consequence

No `SnapshotInstalled` record is emitted in v3 or v4. Snapshot save/load does
not consume a global event position or a commit cursor. `SnapshotInstalledV1`
remains a tested v4 format capability, not an admitted product transition.

### 2. Projection checkpoints remain optional (`bn-11mk`)

This ADR does not pre-empt the user-value decision in `bn-11mk`. Until that bone
admits a product capability, the supported pattern remains the social example's
application-owned sidecar: projector/fold version, complete folded state, and
an exclusive opaque global frontier; missing, stale, corrupt, or ahead-of-log
state rebuilds from position zero. The dormant arbitrary-byte
`MetaStore::{set_checkpoint, checkpoint, checkpoint_lag}` API is not an
implicit contract and is not a Fjall blocker.

If `bn-11mk` chooses **DECLINE**, it removes or explicitly demotes that dormant
API and documents the application-owned pattern. If it chooses **ADMIT**, the
smallest acceptable canonical form must satisfy all of these constraints:

- identify a projector by bounded canonical bytes plus a versioned fold/schema
  identity; arbitrary unowned byte keys are insufficient;
- define an exclusive opaque frontier, optional state digest with an explicit
  presence bit, and whether equal, advancing, or rewind updates are legal;
- define who may advance or rewind a projector. Mess has no principal model
  today, so authorization cannot be hand-waved into a storage row;
- give each logical update a stable identity. Exact retries with the same
  identity/value are idempotent; the same identity with different bytes is a
  conflict; stale updates follow the declared monotonic/rewind policy;
- recover the exact latest admitted value from canonical bytes alone. Resident
  slots, SegmentEffects, and kernel checkpoints are derived accelerators, with
  explicit absent-versus-present-at-zero state and raw-log fallback;
- specify retention of the canonical update or a boundary object carrying the
  latest still-needed value before deleting its log prefix;
- use bounded, allocation-free structural validation on the recovery scan and
  materialize projector bytes only after caps/checksums pass;
- expose lag, accepted/rejected/rewind updates, fallback mode, and recovery
  source to operators;
- demonstrate an adopted public API and production consumer, then A/B
  checkpoint bytes/write p50/p99, resume wall/RSS/events replayed, from-zero
  wall, append overhead, and global-position effects.

A canonical v3 projection update is a reserved system event/batch. It has a
stable control identity, consumes a real global position, and creates a gap in
application delivery; retries and cursors follow §4 below. A v4 control-only
form instead advances the commit cursor without consuming a domain position.
Choosing either encoding is part of the `bn-11mk` ADMIT branch.

### 3. Exact batch idempotency remains optional (`bn-2ctq`)

This ADR does not pre-empt the user-value decision in `bn-2ctq`. Today
`Backend::append_batch` carries no key, `EngineOptions::dedupe_capacity` is
unused, and the Fjall dedupe tables have no production caller. They do not
block Fjall deletion. Spike G remains strong evidence for a possible exact
index, not evidence that the product capability should ship.

If `bn-2ctq` chooses **DECLINE**, cleanup removes the dead option/tables/APIs
and closes `bn-3dp1`/`bn-1sh8`. If it chooses **ADMIT**, its canonical contract
must satisfy all of these constraints:

- define batch-level scope (global or stream namespace), bounded key bytes,
  namespace authorization, and the exact original result returned on a live
  match;
- use the order `resolve registration -> exact dedupe -> expected version`.
  A recovered-unacknowledged matching key returns the original commit; no key
  or an expired key follows normal expected-version behavior and never silently
  duplicates an event;
- commit the key, scope, expiry basis, and result identity atomically with the
  covered event batch. A preceding standalone `$dedupe` v3 batch is forbidden:
  a crash could expose a key whose event/result never committed and cause a
  false absorb;
- retain canonical full keys for the entire exact window. Positions are
  zero-based and inclusive; `w` is the greatest committed global position.
  Configuration requires `W > 0` (`W == 0` is rejected). A key committed at
  `p` is live exactly when `p >= window_start`, where
  `window_start = w.saturating_sub(W)`. Thus the key at
  `p == window_start` remains live, while `p < window_start` is expired. When
  `w < W`, `window_start` is zero and the whole committed prefix remains live.
  A segment's retention bound is named `end_exclusive` and covers
  `[base, end_exclusive)`; it is deletable for dedupe only when
  `end_exclusive <= window_start`, unless a canonical retention boundary
  carries every still-live full key/result. If an implementation instead has
  an inclusive end, its test is strictly `end_inclusive < window_start`;
  ambiguous `end_position` comparisons are forbidden;
- treat fingerprints/filters/epochs only as candidate accelerators. Every
  positive verifies all equal-fingerprint canonical keys, and missing/corrupt
  index/checkpoint state rebuilds from canonical bytes;
- bound adversarial key length, candidate runs, memory, disk, freeze work, and
  namespace growth; expose hits, absorbs, expiries, candidates/full-key reads,
  collisions, rebuild, and retention pressure;
- run public append/owner/recovery A/B under Process, Os, and Group modes with
  0%, 1%, and 50% retry mixes, adversarial collisions, crash/reopen, and the
  32-cell ordinary-append matrix. The 914.7 ns Fjall orientation number is not
  a product gate. Boundary laws include `w < W`, rejected `W == 0`, no-event
  genesis, `p == window_start`, `p == window_start - 1`, `w == p + W`,
  `w == p + W + 1`, and checked/saturating behavior at `u64` limits.

V3 has no existing field that atomically covers a dedupe key and its domain
events. Therefore an ADMIT branch must either specify and review a v3 format
extension embedded in the same accepted batch, including its visible-position
semantics, or explicitly depend on v4. It may not approximate atomicity with
two ordered v3 batches. If it pulls v4 forward, §4's amendment/roadmap rule
applies.

### 4. V3 positions, opaque cursors, and v4

This ADR itself adds no canonical v3 control records. `$registry` remains the
sole control-like v3 encoding: its events consume real global positions and
are filtered from application reads, so visible positions contain gaps.
Consumers must treat positions as opaque ordered cursors and advance with
`GlobalPage::frontier`; arithmetic such as "next event is position + 1" is not
a contract.

Any future canonical v3 system record has the same consequence: it consumes a
position, can create a visible gap, and must be included in recovery while
remaining skippable by application delivery. The ADR proposing it must state
that cost explicitly.

V4 is not pulled forward by this decision. If later admitted, a control-only
capsule advances `CommitCursor`/`batch_id` but not the domain global position;
unknown canonical versions remain fail-closed and an older binary must refuse a
writable open. Pulling v4 into any capability above requires an amendment to
this ADR and must rewire or close redundant post-Fjall v4 work, including the
`bn-1ojm` decision gate. Format support alone is not product admission.

## Integration and evidence gates

The snapshot replacement is admitted only as an end-to-end product path.
`bn-ozi5` owns pack/discovery mechanics; `bn-3l8n` owns adoption. Completion
requires all of the following:

1. **Locked, reproducible baseline.** Before implementation, record machine,
   compiler, filesystem, CPU governor, dataset, file count, exact Fjall commit,
   cache state, warmup policy, durability mode, and a host-load rejection
   threshold. Run at least ten independent, counterbalanced interleaved A/B
   pairs; randomize A/B versus B/A order per pair and use the pair/run, never an
   individual timed operation, as the bootstrap unit. Each cell declares fresh
   versus reused stores and cold, metadata-warm, or resident cache state;
   warmups are counted and discarded explicitly. A p99 cell has at least
   10,000 timed operations or is reported as insufficient for p99. Reject and
   retry a run when predeclared load/competing-process checks fail; record the
   decision and load rather than removing rows post hoc. Commit every accepted
   and rejected raw run. Before candidate code is measured, `bn-ozi5` commits
   numeric budgets for save/load hit, forced fallback, reopen p50/p99, peak
   RSS, discovery bytes/write amplification, files per saved version,
   Buffered-to-Durable closure-promotion bytes/files/barriers, and one-million-
   head administrative full-scan wall time/RSS. The default no-regression rule
   is a paired median ratio no worse than 1.10 for cost metrics, with the upper
   bound of a paired 95% run-level bootstrap interval no worse than 1.15.
   Different budgets must be numeric and justified in that pre-code lock;
   changing one after seeing candidate results requires an ADR amendment.
2. **Public-path A/B.** On identical histories, compare `save_snapshot`, head
   hit, first warm miss, forced miss/corruption, reopen, and `load_cached` p50
   and p99; record bytes written, files per saved version, write amplification,
   peak RSS, and events replayed. Run the 1/1K/100K/1M-head, 64 B/4 KiB/1 MiB
   state-size, and low/medium/saturation save-rate axes from §1. Include a
   Buffered-A/Durable-B different-key sequence and report all closure-promotion
   work, plus a complete paginated scan of the pinned one-million-head root.
   The social `hot_post_bench` and `seed_profile` are required callers, not
   optional component demos.
3. **Correctness laws.** Run the unchanged snapshot+tail equality law and the
   reopen/fold-invalidation/subscription cases. Inject torn pack/footer,
   interrupted directory barriers, missing/corrupt roots/pages, wrong pointers,
   unknown versions, duplicate generations, concurrent lower/higher/same-
   coverage saves, `Empty`/`Through(0)` races, corrupt-current same-coverage
   repair, same-version compatibility upgrades, late old-fold saves racing
   new-fold publication, writer-lock exclusion, cloned-save serialization,
   read-versus-append, pack roll at every crash point, stale descriptors during
   GC, root/page/pack retention at every interruption point, trust-mode hash
   presence combinations, and state decode failures. Durable crash cases must
   include a page written by Buffered save A and reused by Durable save B for a
   different key, an existing content-addressed page whose write is skipped, a
   new `.open` pack whose directory entry is lost, GC re-root reuse, and failure
   of every data/directory/ledger/root sync. No such failure may acknowledge a
   Durable root or permit dependent GC deletion. Administrative tests cover
   cursor/root mismatch, bounded pagination, a million-head traversal, pin/GC
   races, corrupt middle pages, partial diagnostic output, and fail-closed
   destructive retention. Every case yields the same state as full replay.
4. **Component and zero-off gates.** A warm in-process discovery-head lookup
   retains design §18.2's early kill gate: p50 <= 50 ns and p99 <= 150 ns on
   the reference host or a recorded calibrated successor. This is a resident
   lookup gate, not an end-to-end snapshot-load number, and the synthetic
   646.3 ns Fjall point read is orientation only. With snapshot policy off, the
   candidate reruns all 32 `BN-2SU-FINAL` cells as a same-host, same-session
   before/after control using the same ten-pair methodology: every Process
   median must be >=95% of its paired cell, every Group median >=90%, p99 may
   regress by at most 10%, and barrier counts/syscalls must not increase. A
   wrapper or offline-reader change may not hide ordinary append/publish cost
   in a headline average.
5. **Durability normalization.** Every result names `Buffered`, `Durable`, or a
   grouped policy. `Buffered` compares with ordinary Fjall behavior; `Durable`
   compares with the explicitly synced/persisted Fjall control defined in §1,
   or with a prelocked absolute target when equivalence is impossible. Grouped
   runs declare count/bytes/deadline limits and report barriers per completion.
   Durable rows additionally report closure-promotion bytes/files/pages, pack
   frontiers, creation/rename directory barriers, proof-ledger barriers, and
   proven no-I/O reuse. Results from unequal durability contracts are never
   divided into a headline ratio.
6. **Integration surface.** Social construction, public exports/errors,
   metaread, doctor, inspect, retention, backup/restore policy, golden fixtures,
   concurrent offline reads, and observability all consume the replacement.
   Component-only success is insufficient.
7. **Keep/reject rule.** No wrong-state or availability regression is allowed.
   Performance is compared per named metric, not by one headline ratio. A
   candidate outside a predeclared per-metric budget is rejected or requires an
   explicit ADR amendment; post-hoc appeals to a different headline metric do
   not move the gate.

The optional projection and idempotency branches use the evidence contracts in
§2 and §3. Their component results cannot satisfy product admission.

## Adversarial failure analysis

| attack/failure | required outcome |
|---|---|
| torn active record | only locked writer truncates the uncommitted tail; readers reject it |
| interrupted pack roll/footer/rename | writer completes or safely resumes sealing; readers use committed records only |
| pack write/barrier or root rename failure | no root publication; save fails; prior root or replay remains usable |
| durable pack, lost/torn root | orphan pack/pages; prior root or replay |
| root names missing/wrong/corrupt bytes | reject the root/candidate; never decode as state; replay |
| duplicate or reordered root generation | highest independently resolvable valid UUID/generation; otherwise replay |
| corrupt store identity or reused filename | sidecar miss or new UUID namespace; never adopt old IDs or accept ABA |
| concurrent lower-coverage save finishes last | serialized owner cannot regress discovery head |
| `Empty` races `Through(0)` | `Through(0)` wins under the explicit coverage total order |
| same coverage, invalid current record | validated replacement repairs the head and emits a diagnostic |
| same coverage, different valid state within one compatibility identity | diagnose conflict; retain that identity's current candidate |
| same event version, newer fold/schema/codec identity | publish a separately addressable compatible head |
| late old-fold save after/racing upgrade | may update only the old compatibility slot; cannot hide or delete the new one |
| second writer or concurrent cloned saves | second process fails lock acquisition; clones serialize through one root publication owner |
| offline reader overlaps append/GC | exact-range validation returns immutable old bytes or a miss; reader never repairs or observes ABA |
| Durable B reuses Buffered A content for another key | promote and sync A's reachable page/record and directory closure before acknowledging B |
| content-addressed page already exists without durable proof | validate and promote file plus directory entry; skipped write is not a durability proof |
| new active `.open` pack directory entry is lost | Durable root is not acknowledged until the pack data and creation directory are synced |
| any closure/ledger/root sync fails | do not acknowledge the Durable root; prior durable root or replay remains valid |
| administrative scan crosses root generations | reject the cursor; never splice results from different roots |
| administrative traversal encounters corruption | doctor/inspect may return explicit partial results; destructive retention performs no mutation |
| GC prune-directory barrier fails | abort before deleting any content newly made unreachable by that prune |
| root/page/pack GC interrupted | a complete-closure Durable root precedes pruning and pruning precedes deletion; that durable root survives while crashes may leave only extra roots/orphans |
| only one semantic hash present | reject the record; never reinterpret it as unverified |
| honest writer emits unverified state | accepted only as a discardable cache; record hash makes no fold-proof claim |
| compromised writer hashes wrong state | outside certified-hash threat boundary; replay-and-compare/proof is required |
| unknown snapshot sidecar version | safe miss because the sidecar is discardable |
| unknown canonical log/capsule version | fail closed; never treat it as an empty tail or writable store |
| projection checkpoint stale/ahead/corrupt | while optional, application rebuild from position zero; an admitted canonical form must recover exact state from raw controls |
| retry/duplicate append | normal expected-version semantics until `bn-2ctq` admits a stronger contract |
| every accelerator deleted | registry/events recover from canonical log; snapshots/projections replay; an admitted dedupe answer must rebuild from canonical keys |

## Roadmap consequences

- `bn-ozi5` owns the pre-1.0 stable `SnapshotCompatibility` and
  `SnapshotCoverage` API break, exclusive writer, commit-framed build/seal
  protocol, bounded copy-on-write discovery, complete-closure
  `Buffered`/`Durable` promotion and proof ledger, pinned paginated
  administrative scanning, repair, and GC mechanics. It depends only on this
  ADR; dense projection slots and `SegmentEffect` are not snapshot
  prerequisites.
- `bn-3l8n` migrates all applications, examples, tests, public docs, and
  offline tools to stable compatibility identities, explicit complete-closure
  durability, and pinned paginated enumeration. It proves the high-cardinality
  point and full-scan matrix and remains the mandatory adoption gate before
  `bn-fj34` deletes Fjall.
- `bn-11mk` owns the projection ADMIT/DECLINE decision in Phase 5 and depends
  only on this ADR. It is not a Fjall-deletion blocker. It blocks conditional
  `bn-1lbf`: ADMIT activates the dense-slot path and creates/orders later
  owner/effect integration; DECLINE closes the slot bone.
- `bn-2ctq` owns the exact batch-idempotency ADMIT/DECLINE decision.
  `bn-3dp1` and `bn-1sh8` remain conditional on ADMIT and are not Fjall
  blockers.
- `bn-1lbf` is projection-only, depends on `bn-11mk`, and must not start without
  ADMIT. It is closed without implementation on DECLINE. Snapshot discovery no
  longer consumes this component.
- Exact idempotency retains the conditional chain `bn-2ctq -> bn-3dp1 ->
  bn-1sh8`; the decision bone closes its downstream work on DECLINE.
- `bn-fj34` removes the dead capacity option, dormant tables/APIs, and public
  Fjall surface after snapshot adoption. It does not wait for projection,
  dedupe, or kernel-checkpoint work.
- `bn-1ojm` remains the only v4 product-admission gate. This ADR neither
  pre-approves nor requires it.

## Consequences

- Fjall deletion has one live replacement path instead of three invented
  authority migrations.
- Snapshot corruption becomes an availability-safe miss across runtime and
  offline tooling, while successful publication gains explicit durability
  ordering.
- The state-kernel product and checkpoint algebra include only state admitted
  by the time each implementation bone starts, reducing speculative resident
  memory, codec surface, and recovery combinations.
- Mess does not yet promise engine-level projection checkpointing or append
  idempotency. `bn-11mk` and `bn-2ctq` own those product decisions.
- V4 remains independently reviewable; its proven control codec is retained
  without making unused controls part of the product.

## Revisit when

- Snapshot packs need to authorize retention rather than merely accelerate
  replay; that requires a separate canonical retention-boundary decision.
- The v4 admission gate demonstrates product value sufficient to pay its
  format, downgrade, and operational costs.
