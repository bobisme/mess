# Research 13: current authority and Fjall deletion map

**Audited:** production source at `69b95604` (post `bn-2su`, flat append owner)
**Date:** 2026-07-14
**Purpose:** make `bn-187h`'s storage-authority inventory executable by later
bones. This document describes the code that exists, not the older engine
described by research 01 or the target architecture in `design.md`.

The audit used the current call graph as authority and reconciled it with the
accepted corrections in research 11/12. In particular, a statement that a
value is "rebuildable" below always names the bytes and procedure that rebuild
it. If no such bytes exist, the table says so.

## 1. Decision summary

The flat-owner engine is already Fjall-free. `LogEngine::open`, append, head,
stream/global read, seal, and recovery never construct or call `MetaStore`.
Its only source-level tie to `mess-index::meta` is the unused
`EngineOptions::dedupe_capacity` default. The core engine's durable authority is
the accepted log prefix; its resident `Book` is O(streams + event types) and is
rebuilt from log headers, sealed directories, and `$registry` payloads.

Fjall has one production consumer: the opt-in, publicly exported
`FjallSnapshotBackend<B>`. It uses a separate database under the snapshot
sidecar root for snapshot discovery. The social example constructs
`FjallSnapshotBackend<LogEngine>` by default, so Fjall is not in the event
engine but is still in a real application composition.

Three corrections change the roadmap:

1. **Snapshot heads are disposable acceleration state, not event authority.**
   Losing the Fjall head makes `load_cached` perform a correct full replay. The
   exact head cannot be reconstructed from the log today because
   `fold_version`, the empty-prefix flag, and `snapshot_ptr` exist only in the
   Fjall value. A replacement may preserve this discardable contract with an
   immutable/current-pointer sidecar. Making `SnapshotInstalled` canonical in
   the log is a separate product/format decision, not a prerequisite for
   deleting Fjall.
2. **`MetaStore` projection checkpoints are dormant.** `set_checkpoint`,
   `checkpoint`, and `checkpoint_lag` have no non-test caller. The actual
   social projection persists a full, versioned, discardable checkpoint in
   `.social-projections.ckpt`; it does not use Fjall. There is no "last
   checkpoint authority" that must be moved out of `MetaStore`.
3. **`MetaStore` dedupe is dormant and has no canonical input bytes.** The
   public append API carries no idempotency key. Shipping epoch dedupe remains
   a conditional new feature, not a blocker for deleting the unused Fjall
   tables.

Therefore the true Fjall deletion critical path is:

```text
choose snapshot semantics
  -> replace FjallSnapshotBackend end to end
  -> move doctor/inspect/retention/examples/goldens to that replacement
  -> remove the dormant MetaStore API and stale <store>/meta surfaces
  -> remove fjall from the dependency graph
```

SegmentEffect, kernel checkpoints, and optional exact dedupe remain valuable
Asterism work, but they are not technical blockers for Fjall deletion.

## 2. Authority vocabulary and frontiers

The audit uses five classifications:

- **Canonical log authority:** accepted log bytes determine the answer. A
  corrupt canonical prefix is a loud store failure, not a cache miss.
- **Rebuildable accelerator:** named canonical bytes reproduce the exact
  logical value. Loss may increase work but not change the answer.
- **Discardable acceleration state:** its exact previous value is not
  reconstructible, but loss safely selects a slower computation from canonical
  data. Snapshots are in this class today.
- **Dormant public component:** compiled/exported and testable, but no
  production construction path uses it. External callers could still depend
  on the public API, so deletion is an explicit breaking-surface decision.
- **Runtime-only state:** neither durable nor meant to be rebuilt exactly
  across a process. It is reinitialized or recomputed.

The current engine has two relevant frontiers:

- **writer-accepted/durable watermark:** the `DirectCommitter` watermark after
  an `Acked` outcome. Under `Process`, this means the covering `pwrite` returned;
  under `Os`/`Group`, the covering barrier completed.
- **published/read watermark:** advanced only after `ActiveIndex` and the
  per-stream Book head have been installed. Every event position below it is
  serviceable by the read path.

Registry state is folded after a positive ack and immediately before its
`$registry` batch is installed into `ActiveIndex`. The one flat owner serializes
those operations, but readers can observe the registry projection before the
read watermark advances. Later state-kernel work must either preserve this
ordering deliberately or publish the entire owner group as one coherent state
transition; it must not assume every current Book field changes at one atomic
frontier.

## 3. Complete `Book` inventory

`Book` is in `crates/mess-store/src/engine.rs`. It contains no payload history
and no per-event record mirror.

| field | classification and canonical bytes | rebuild/open path | live publication and corruption behavior | deletion/replacement |
|---|---|---|---|---|
| `registry: RegistryState` | Canonical log authority. Reserved id 0 comes from spec text; all other assignments are `RegistryEventV1` payloads on `$registry` in accepted v3 batches. | Recovery locates stream-0 batches through sealed pointer directories or the scanned active tail, reads only those payloads, and folds them through `registry::Fold`/`RegistryState`. | The owner stages records against a trial state, writes them before first use in the same ordered unit, then applies them only after ack. An invalid fold poisons the live owner (`registry_lost`); a corrupt/dangling durable registry makes reopen fail loudly. | Keep one canonical `RegistryState` fold. Dense pages/checkpoints may project it but may not become a second authority. |
| `stream_arcs` | Rebuildable in-memory projection of `registry`, one `Arc<str>` per id. No independent bytes. | `Book::rebuild_arcs` walks dense registry ids on open; dense registrations extend the vector in place. | Updated under the Book mutex with `registry`; a missing referenced id is rejected on open/read. | May be retained as a read cache or replaced by a dense registry page. Never checkpoint independently from `registry`. |
| `type_arcs` | Same classification as `stream_arcs`, for event-type ids. | Same, from `RegistryState`. | Same; recovery validates the maximum referenced event-type id against the dense registry before opening. | Same. |
| `heads: HashMap<u64,u64>` | Rebuildable accelerator. Canonical values are `(stream_id, first_stream_version, frame_count, first_global_pos)` in accepted batch headers; fully sealed segments also carry a verified directory summary used as an accelerator. | Unsealed/head segments are metadata-scanned; fully sealed non-head segments contribute `SealedSegmentIndex::stream_head` after footer/coverage checks. Per-stream maxima become `heads`. | `publish_batch` installs the active pointer first, then updates `heads`, then advances the read watermark. Canonical log corruption is loud; missing/refuted sealed sidecars fall back to scanning log bytes. | `bn-34v8`/`bn-2wtl` may replace the HashMap with dense head pages, but must shadow against this exact header/directory fold and preserve the pointer-before-head-before-watermark order. |
| `registry_next_version` | Rebuildable allocator projection. Canonical input is the number of accepted `$registry` events; stream-0 versions are dense. | Seeded from `registry::Fold::record_count`. | The owner advances it after an ack and before publishing the registry batch. It intentionally can lead `heads[0]`; `head("$registry")` reads this allocator, not `heads[0]`. | Any group-state publication must keep the distinction or deliberately replace it with an owner-local allocator whose published snapshot is coherent. Do not derive it from the lagging head. |
| `registry_lost` | Runtime-only sticky poison. It records an impossible post-ack in-process fold failure. | Always false on a fresh open; recovery validates canonical records anew. | Once set, the owner refuses further writes and requires reopen. | Preserve an equivalent fail-stop path; it does not belong in a checkpoint or SegmentEffect. |

Adjacent state is not part of `Book` but is load-bearing: `ActiveIndex` and
sealed pointer directories map positions to durable `(segment_id, offset)`
pointers; the bounded capsule/block caches are transparent; the durable and
read watermarks delimit accepted versus serviceable prefixes. Dense heads or
SegmentEffects are not integrated until reads, recovery, sealing, and these
frontiers all use them together.

## 4. Complete `MetaStore` storage inventory

`MetaStore::open` creates seven named Fjall keyspaces. The removed
`stream_names`/`type_names` keyspaces are not in this list.

| keyspace | exact Fjall encoding | actual producer/consumer | canonical source, rebuild, and fallback | classification and deletion action |
|---|---|---|---|---|
| `stream_heads` | key `stream_id` u64 BE; value `version` u64 LE + `global_position` u64 LE | No engine producer/consumer. `mess rebuild-index --meta` can populate it; meta tests/bench read it. | Exact values are derivable from accepted batch headers. CLI can already compute them. The engine ignores this table even when the whole `<store>/meta` directory is deleted. | Dormant public accelerator. Remove the CLI `--meta` branch and table/API after an explicit public-API decision; no runtime replacement is needed beyond the existing Book/dense-head path. |
| `snapshot_heads` | key interim `stream_id` u64 BE; value `covered_version` u64 LE + pseudo `global_position` u64 LE + opaque 14-byte ref (`tag`, `fold_version`, flags, `snapshot_ptr`) | Production `FjallSnapshotBackend::save_snapshot` writes it and `load_snapshot`, CLI metaread, doctor, inspect, and retention read it. A legacy `<store>/meta` copy is compatibility/test-only; the real app copy is under the snapshot root (social: `<store>/.snapshots/meta`). | **No exact canonical source exists.** Blob paths retain FNV id and covered version, but not stream name, fold version, empty-prefix flag, or pointer. Losing the row means full event replay, which is correct. Unknown ref or bad blob falls back; a Fjall open error or a structurally short head currently surfaces as a backend error instead of falling back. | Production discardable acceleration state and the sole real Fjall blocker. Replace the backend and offline readers together. The replacement must preserve full-replay fallback and preferably make all metadata corruption a miss, not an availability failure. |
| `snapshot_stream_names` | key interim/FNV id u64 BE; value UTF-8 stream name | `FjallSnapshotBackend::save_snapshot` writes before the blob/head; CLI joins it to heads for doctor/retention. | No independent event-engine authority for generic `B`. For `LogEngine`, names can be enumerated from `$registry` and hashed, but the generic snapshot wrapper does not require such a capability. Missing rows only hide snapshots from offline diagnostics; loads by caller-supplied name still work. | Production diagnostic side map. The replacement manifest/head must be self-describing so doctor/retention do not need a separate reverse map. |
| `checkpoints` | key arbitrary projection id bytes; value exclusive position u64 LE | `set_checkpoint`, `checkpoint`, and `checkpoint_lag` are used only by `crates/mess-index/tests/meta.rs`. | No canonical bytes and no production rebuild path. The social application's real checkpoint is a separate full-state file with magic, format/fold version, applied frontier, and from-zero fallback. | Dormant public component, not a Fjall authority. Delete or move behind an explicitly retained compatibility API. A canonical log checkpoint is an optional new feature. |
| `dedupe` | key stream id u64 BE + arbitrary key bytes; value original global position u64 LE + FIFO seq u64 LE | Only direct MetaStore tests/bench populate/query it. `LogEngine` never supplies a key. | No current log encoding or append API carries the key, so it is not reconstructible from production history. | Dormant public component and synthetic baseline. Delete with MetaStore. If product idempotency is admitted later, its canonical bytes and semantics come from `bn-2ctq`/`bn-1sh8`, not from this table. |
| `dedupe_order` | key FIFO seq u64 BE; value the primary `dedupe` key | Internal to `apply_group` eviction and `open_with_capacity` bound recovery. | Derived only from the dormant dedupe writes, not from production log bytes. Missing/corrupt rows can skew eviction; no production path is exposed. | Delete with dormant dedupe. Do not migrate its insertion-count window into the position-span epoch design by accident. |
| `hw` | keys `stream_heads`, `snapshot_heads`, `dedupe`; each value u64 LE | `apply_group` advances **all three** entries for every group. Snapshot open reads `snapshot_heads`; tests call `high_water`/`lag`. | Internal progress bookkeeping, not log authority. In a snapshot-only database, a snapshot save advances empty stream-head and dedupe high-waters to the pseudo save counter. Conversely, CLI rebuilding stream heads advances snapshot/dedupe high-waters despite writing neither. No production repair consumes those misleading values. | Delete with MetaStore. A replacement snapshot current pointer needs its own generation; state-kernel frontiers must be typed rather than sharing this cross-table counter. |

The module-level claim that every table is rebuilt by replaying the log is
therefore false for snapshot heads, projection checkpoints, and dedupe in the
current product. Their safety comes from dormancy or discardability, not an
implemented exact replay.

### 4.1 API and struct-field deletion checklist

Every public or internal `mess-index::meta` surface is accounted for here:

- Keep only until callers move: `MetaStore::{open, apply_group,
  put_snapshot_stream_name, snapshot_stream_names, snapshot_head, high_water,
  persist}` and the snapshot types/codecs used by `FjallSnapshotBackend` and
  CLI metaread.
- Dormant/removable: `open_with_capacity`, `set_checkpoint`, `checkpoint`,
  `checkpoint_lag`, `stream_head`, `dedupe_lookup`, `lag`, `persist_buffered`,
  `persist_call_count`, `buffered_persist_call_count`, `dump`, and
  `dump_checkpoints`; `MetaTable`, `CommitGroup`, `Head`, `SnapshotHead`,
  `StreamId`, `DEFAULT_DEDUPE_CAPACITY`, `Dump`, `MetaError`, and public codec
  functions disappear once no replacement imports them.
- Internal fields `db`, all seven keyspace handles, `dedupe_bounds`,
  `dedupe_capacity`, and both persist counters disappear with the module.
- `EngineOptions::dedupe_capacity` is dead configuration: construction and
  every struct update ignore it. Remove it independent of whether the new
  idempotency feature is admitted.
- `EngineError::Meta` no longer means Fjall; it reports registry fold/name
  integrity failures. Rename/split it when cleanup reaches this code so error
  text does not claim the event engine has a metadata database.

## 5. Production snapshot flow and required replacement semantics

The current save frontier is independent of the event log:

1. Hash the caller's stream name with interim FNV.
2. Insert `snapshot_stream_names[id] = name` into Fjall.
3. Write `blobs/<id-hex>/<covered-version>.blob.tmp`, then rename it to
   `.blob`. The file has `MSB1`, payload length, FNV checksum, and state bytes.
   Neither file nor containing directory is fsynced here.
4. Allocate a process-shared pseudo position (`next_pos.fetch_add`).
5. Atomically commit the snapshot head plus all three generic high-waters to
   Fjall at journal-buffered durability.
6. Return success. `FjallSnapshotBackend::persist` can later `SyncAll` the
   Fjall database, but it does not fsync the blob, so it is not a durability
   barrier for the complete snapshot installation.

Consequences a replacement must test rather than infer:

- Name-only and blob-only orphans are harmless and GC-able.
- A head must not be published before its referenced blob is readable and
  validated.
- Same-version saves target the same blob path. Concurrent completion, not
  stream-version comparison, determines the current head today. Any new
  monotonicity or compare-and-swap rule is a product behavior change.
- Missing head, unknown 14-byte ref, missing blob, checksum mismatch, a head
  beyond the event stream, state decode failure, or fold-version mismatch all
  ultimately select full replay (fold mismatch also performs best-effort
  replacement). Fjall open/read/decode errors are the exception: some surface
  as store errors. The replacement should close that availability gap.
- Snapshot state is not trusted event authority. `load_cached` must remain
  byte-identical to full replay, and retention/doctor must not make a corrupt
  accelerator necessary to open or read the event store.

The minimum no-Fjall design that preserves behavior is an immutable snapshot
record/pack plus a small atomic current-head manifest containing the stream
name, covered version, fold version, empty-prefix flag, pointer, and integrity
binding. A log-carried `SnapshotInstalled` record is stronger and may be worth
shipping, but it adds global-position, retry, format, and retention semantics.
The Phase-3 ADR must choose rather than smuggle that change into a dependency
cleanup.

## 6. Projection and dedupe paths

### 6.1 Projection checkpoints

Core `mess-store` exposes subscriptions and watermarks but no projection
checkpoint persistence API. `MetaStore::set_checkpoint` is unused outside its
unit/integration test.

The working product example is `examples/social/src/projections.rs`:

- checkpoint bytes contain magic, envelope version, projection/fold version,
  applied global frontier, and the complete folded state;
- write is temp-file plus rename (atomic visibility, but no file/dir fsync);
- missing, undecodable, stale-version, or ahead-of-log checkpoints rebuild
  from position 0;
- valid checkpoints resume a subscription at `applied`, then fold the suffix;
- tests compare the resulting state fingerprint with a from-zero rebuild.

That is discardable app state, not a canonical event transition. A generic
canonical checkpoint control would additionally need projector identity,
authorization, digest, monotonic/rewind rules, and an API adopted by an actual
consumer. It should not be justified as a `MetaStore` migration.

### 6.2 Dedupe/idempotency

The current `Backend::append_batch` and `EventStore` APIs carry no dedupe key.
`EngineOptions::dedupe_capacity` is unused. All MetaStore dedupe measurements
are component measurements of a possible feature.

If `bn-2ctq` admits batch idempotency, integration proof must include the
public intent, owner admission order, canonical key bytes, original-result
return, recovery, retention, exact collision verification, metrics, and all
durability modes. Porting the epoch data structure without those end-to-end
paths is not an implementation of product dedupe.

## 7. CLI, backup, examples, tests, and dependency surface

### 7.1 Runtime and Cargo

- `crates/mess-index/Cargo.toml` is the only production manifest with a direct
  `fjall = 3.1.6` dependency.
- `cargo tree -i fjall --workspace` reaches `mess-index`, then `mess-store`,
  `mess-cli`, `mess-bench`, `mess-soak`, and both examples.
- `mess-index::meta` is public. `mess-store::{FjallSnapshotBackend,
  SnapshotBackendError}` are public exports and widely used by social/tests.
  Deletion is a source-breaking API change even though versions are pre-1.0.
- Standalone spike crates may retain Fjall as a historical comparison; the
  production/workspace deletion gate should explicitly state whether it scans
  spikes. `bn-fj34` currently promises production/workspace removal, not
  erasing historical spike lockfiles.

### 7.2 CLI/offline behavior

| surface | current Fjall coupling | integrated replacement requirement |
|---|---|---|
| `metaread` | Opens legacy `<store>/meta` and app `<store>/.snapshots/meta`, joins `snapshot_heads` to the name side map, and string-matches `FjallError: Locked`. | Read the replacement's immutable/self-describing heads without creating files. Define safe concurrent-reader behavior; remove Fjall-specific lock parsing. |
| `doctor` | Fold-version drift depends on metaread; a live Fjall writer produces the `meta-store-locked` advisory. | Preserve non-vacuous fold drift and corruption reporting. Replace/remove the lock advisory based on the new read protocol. |
| `inspect` | Names/heads already come from log recovery; only snapshot availability depends on metaread. | Keep report schema stable while sourcing snapshots from the replacement. |
| `retention explain` | Snapshot blockers come from metaread; on failure it advises and proceeds with an empty set. | Consume replacement snapshot heads/anchors and preserve explicit unavailable/corrupt behavior. A destructive retention executor must fail closed even if this read-only explainer remains advisory. |
| `rebuild-index --meta` | Creates legacy `<store>/meta` and writes only stream heads, while generic `apply_group` also advances empty snapshot/dedupe high-waters. | Remove `--meta` or redefine it for actual state-kernel checkpoints. Pointer-sidecar rebuild remains independent. |
| `backup` | Recursively copies legacy `<store>/meta` because stale comments still call removed name tables authoritative. It does **not** collect the conventional `.snapshots` sidecar. | Remove legacy meta copying and explicitly include/exclude the new snapshot packs/manifests according to their discardable status. Add backup/restore tests for the chosen policy. |
| layout docs | `store.rs`, spec 07, testing/perf docs, and comments still describe engine `meta/` as live. | Make the event-log layout and optional snapshot/checkpoint layout distinct and current. |

### 7.3 Tests and fixtures that must be ported or deliberately retired

- `crates/mess-index/tests/meta.rs` and `bench_lookup.rs`: delete dormant
  checkpoint/dedupe coverage if those products are declined; move reusable
  codec/property cases to their real replacements. Do not retain Fjall just to
  keep a synthetic baseline runnable in production crates.
- `crates/mess-store/tests/fjall_snapshot.rs`, `snapshot_law.rs`, the shared
  snapshot backend in `tests/common`, differential tests, and snapshot-enabled
  subscription tests: run unchanged laws against the replacement backend.
- `examples/social` store construction, seed profile, hot-post bench,
  projections, web/seed binaries, and README: adopt the replacement type at
  the real application composition point.
- CLI tests `doctor_app_snapshots`, `doctor_lock_state`,
  `inspect_report_shape`, `retention_explain`, `golden`, and backup/restore:
  port to the new offline reader and chosen concurrency/failure behavior.
- `engine_name_durability` remains valuable: it proves deletion of the entire
  legacy engine meta directory cannot affect open, names, heads, reads, or
  future appends.
- Both committed golden tarballs contain a stale root `store/meta` plus a
  Fjall-backed `store/snapshots/meta`. The current golden check even requires
  the root directory although `LogEngine` does not. Regenerate fixtures or
  make an explicit legacy-compatibility fixture; do not silently keep the
  stale assertion.

## 8. Measurement contract for replacements

Every implementation bone must lock its baseline before changing code, record
the machine/compiler/filesystem/load, run interleaved repetitions on real fs,
and retain raw rows. Component speed is insufficient: the final evidence must
exercise save/load or append/recovery through the public `EventStore` and the
CLI that consumes the bytes.

### 8.1 Current reproducible baselines

| surface | baseline/evidence | required comparison |
|---|---|---|
| append and state publication | `spikes/baseline_matrix/BN-2SU-FINAL.csv` and `BN-2SU-REPORT.md`: 32 matched Process/Group cells, payload 24/250 B, batch 1/10/100/1000, 1/4 writers, AB/BA/AB medians. Current Process beats Fjall-era in every cell; no Group cell regresses 10%. | Re-run the same matrix after each resident-state/control integration. Preserve per-cell throughput, p99, barrier counts, exact positions, reopen, and read correctness. A snapshot wrapper change must also show zero cost when snapshot policy is off. |
| production heads/recovery | `owb_bench` at 2M events reported 1.670 s median reopen and 105,176 KiB peak RSS in the locked baseline; `engine_reopen`, `engine_reopen_cycles`, and `engine_name_durability` are correctness oracles. | Dense heads/effects must compare header/directory fold results, open wall, bytes/syscalls, payload decodes, and peak RSS; keep exact post-reopen append/read behavior. Do not compare only against Fjall point reads, because production `head()` reads Book. |
| Fjall component point reads | On this commit, `cargo test -p mess-index --release --test bench_lookup -- --nocapture` over 10k keys/200k hits printed 646.3 ns/head and 914.7 ns/dedupe. Host: Ryzen 9 3900X, Linux 7.0.12, rustc 1.97, ext4. Load1 was 10.40 immediately after the compile, so this is an orientation run, **not a locked gate**. The older module note reports ~492/~550 ns under its prior run. | Use only as a synthetic component reference. New head/dedupe structures need quiet, repeated production-code benches plus composed integration. |
| snapshot correctness | `snapshot_law` proves snapshot+tail equals full replay; `fjall_snapshot` covers reopen, corruption, fold invalidation, and subscription delegation. The full 3,000-iteration real-fs run is documented at 3,714.59 s; use `MESS_SNAPSHOT_LAW_ITERS` for smoke. | Run the same law/fault cases against the replacement. Record save and load p50/p99, bytes/file count per saved version, write amplification, reopen, RSS, and full-replay fallback on every corrupted component. |
| snapshot user value | `examples/social/tests/hot_post_bench.rs` measures full replay versus `load_cached`, tail length, snapshot bytes/event, and first warm miss; `seed_profile.rs` separates base append from snapshot wrapper operations. | Same workload, same aggregate/history, replacement vs Fjall backend in interleaved runs. Report both head-hit and forced-miss/corruption paths. |
| app projection checkpoints | Social tests cover kill/restart, stale fold version, truncation/garbage, mid-stream checkpoint, suffix-only resume, and equality to a from-zero state fingerprint. | Add a production-shaped event-count matrix recording checkpoint bytes/write latency, resume wall/RSS/events replayed, and from-zero wall. If canonical controls are proposed, also measure append/global-position overhead. |
| CLI/backup/goldens | Existing CLI suites named in 7.3. | Run them against a fresh no-Fjall store, a snapshot-bearing store, concurrent writer/read, each corrupt metadata component, backup/restore, and legacy fixture policy. |
| dependency/filesystem exit | `cargo tree -i fjall --workspace` currently shows Fjall through `mess-index`; fresh snapshot apps create Fjall directories. | Final gate: no production manifest/lock dependency, no public Fjall symbols, no `meta` directory from a fresh engine/snapshot run, and no Fjall-specific CLI messages. |

### 8.2 Minimum keep/reject rules

- Keep a performance change only when its named metric wins or stays within
  the predeclared per-cell tolerance and all correctness/recovery oracles pass.
- A locally faster table that is not used by production reads/recovery has not
  improved the engine.
- A snapshot replacement is incomplete until the social app, doctor, inspect,
  retention, backup policy, goldens, corruption fallback, and public facade all
  use it.
- A checkpoint or SegmentEffect cannot earn a scan waiver until missing,
  corrupt, stale, reordered, and wrong-anchor cases all select the documented
  raw-log fallback.
- Raw matrices and machine/load metadata must be committed; a single headline
  ratio is not sufficient.

## 9. Exact roadmap amendments

These changes follow directly from the call graph and should be applied before
Phase 7 starts.

### `bn-k8qd` — correct the decision premise

Replace "Fjall cannot be removed until snapshot installs, projection
checkpoints, and any admitted dedupe keys are canonical in the log" with the
classification in this audit:

- snapshot heads are discardable production acceleration state;
- MetaStore projection checkpoints and dedupe are dormant;
- `$registry` is already canonical in the log.

Require the ADR to decide independently for snapshots, projection
checkpoints, and optional idempotency whether to preserve discardable sidecar
semantics or add canonical v3/v4 controls. Include a minimum no-Fjall snapshot
manifest/pack alternative. Do not make a log-control decision an unstated
dependency-cleanup requirement.

### `bn-ozi5` — fix authority wording and compare both replacement shapes

Change the context from "Fjall snapshot-head advancement is authoritative" to
"Fjall is the only discovery copy of a discardable snapshot accelerator; event
truth remains the log and head loss falls back to replay." Make the goal
conditional on `bn-k8qd`'s choice: either an immutable/current-pointer snapshot
pack that preserves current semantics, or a canonical install transition.

Add acceptance for:

- every MetaStore/Fjall corruption class selecting full replay rather than an
  availability error;
- concurrent and same-version saves with an explicit monotonicity policy;
- blob/manifest file and directory durability ordering;
- doctor/inspect/retention and backup/restore behavior;
- an interleaved Fjall-vs-replacement `hot_post_bench`/`seed_profile` matrix
  with file count, bytes, save/load tails, reopen, and RSS.

### `bn-11mk` — make it a conditional product feature, not migration

Rename/re-scope it to "Decide and, if admitted, integrate canonical projection
checkpoint controls." Its context must state that MetaStore has no production
checkpoint caller and that the real social checkpoint is a discardable full
state sidecar. Acceptance must name an adopted production API/consumer and
measure its benefit/cost. If the product decision is DECLINE, close the
implementation path and retain discardable app checkpoints.

Remove `bn-11mk` as a hard blocker of Fjall deletion.

### `bn-fj34` — use only true deletion blockers

Its true prerequisites are the authority audit and the chosen snapshot
replacement plus its application/CLI integration. Remove hard dependencies on
`bn-11mk`, `bn-1sh8`, and `bn-3ctl`: the former two are dormant/new product
features and kernel recovery already does not use Fjall.

Add acceptance to remove `EngineOptions::dedupe_capacity`, rename the
misleading `EngineError::Meta`, remove `rebuild-index --meta` and stale backup
`meta/` collection, port the CLI lock/error schema, regenerate or classify both
goldens, and distinguish production manifests from historical spike-only
Fjall dependencies.

### Add one integration bone between snapshot implementation and deletion

Create a medium, risk-high child under Phase 7 named approximately **"Adopt the
snapshot replacement across applications and offline tooling"**. It should be
blocked by `bn-ozi5` and block `bn-fj34`. Scope: social construction and
benches, public exports/errors, metaread, doctor, inspect, retention, backup
policy/restore, goldens, concurrent offline reads, corruption fallback, and
the full snapshot law. This separation keeps `bn-ozi5` focused on storage
mechanics while making integration a first-class gate rather than cleanup
inside the final deletion commit.

### `bn-1sh8` and dedupe graph

Keep `bn-2ctq -> bn-3dp1 -> bn-1sh8` conditional on ADMIT, but do not make it a
prerequisite of removing dormant Fjall dedupe. If admitted, require the new
path to be Fjall-free by construction. If declined, deletion removes the dead
option and `EngineOptions::dedupe_capacity`.

## 10. Evidence commands

The key call-graph and baseline checks are reproducible with:

```text
rg -n '\bMetaStore\b|FjallSnapshotBackend|dedupe_capacity' crates examples
rg -n '\.(set_checkpoint|checkpoint|dedupe_lookup|apply_group)\(' crates examples
cargo tree -i fjall --workspace
cargo test -p mess-index --release --test bench_lookup -- --nocapture
cargo test -p mess-store --test engine_name_durability
cargo test -p mess-store --test fjall_snapshot
MESS_SNAPSHOT_LAW_ITERS=25 cargo test -p mess-store --release \
  --test snapshot_law snapshot_plus_tail_equals_full_replay
cargo test -p mess-cli --test doctor_app_snapshots
cargo test -p mess-cli --test retention_explain
```

The final deletion is proved by the inverse searches plus full tests, not by a
successful compile of a new component in isolation.
