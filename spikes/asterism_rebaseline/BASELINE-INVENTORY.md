# Asterism production rebaseline inventory

**Scope:** source and evidence archaeology for `bn-2l3n`; no build or timing
was performed. Checkpoint tables give full Git object IDs; prose sometimes uses
an unambiguous prefix. Other hashes are SHA-256 unless labelled as Git tree IDs.

## Bottom line

The repository contains good historical anchors, but it does **not** contain a
single admissible matrix that directly compares today's complete public append
path with both the Fjall-era engine and a bare engine. The next rebaseline must
therefore measure fresh binaries from exact checkpoints. Historical ratios must
not be multiplied into a synthetic “current versus Fjall” headline.

The usable source checkpoints are:

| role | exact source checkpoint | Git tree | evidence carrier / integration |
| --- | --- | --- | --- |
| Fjall-era composed engine | `f0ab89e92e44253f8fe48cf19d7a93e39263585b` | `7ca2228fcd1c65ef942da35144fcf507d8a72e12` | Baseline harness and results were committed later by `9732d4d442e654c3912fb1bbd1a095ce53de008a`. |
| Historical bare engine | No separate product commit: the `bare` driver uses the `mess-log` code from the source checkout being measured. | For baseline-gen2, the same `f0ab89e9...` tree above. For the flat-owner rerun, the measured workspace tree below. | The bare driver is part of `spikes/baseline_matrix`, not an integrated storage engine. |
| Final measured `bn-2su` flat owner | `095460b7a597327662da9971dbdd4a209e49a3c1` | `40f1072cf176bd8d153e00415c03d675481f28c9` | Evidence-only report correction `60663621822d0e2a936ec2662638c10d47b3a271` has tree `f738cde2b5414d1a2926ac86573163d505754ba9`, byte-identical to integrated `69b95604b9e7c924314cf7d82b86a2edb7dccde6`. The measured-to-integrated diff is one two-line report correction; product source and raw CSV are identical. |
| Direct-outcome production integration | Final measured implementation lineage `68434ad5e56982a309208a724bf83d2b44720386` / predeclared follow-up `7035ae9a15408c96fddecd5f8fb2001dfee2a431`; frozen control `7aea2488818c18ff9fac086f0ad6a707f38ed447`. | `7035ae9a...` tree `c0dad8742e0d9fc7f7637c6e5304bb71a9081be8` | Integrated as `7da5fb15ba7d20bfe4b78f4c4f11f30ea04f5392`. This is an admissible intermediate current-path integration, not today's final checkpoint. |
| Exact measured owned-append candidate | `e5c3da658abd619c240851d384642fe741d93047` | `8504ef9bedac8534ac7f1032d5ff187fb77f057d` | Integrated by `1f4d30ef32f61e16c9b437d4e86273e5ee7cfc51`; lockfile and harness follow-ups end at current `774e6454a1209042bb6ecf61eb10c56430b5e81b`. |
| Current production | `774e6454a1209042bb6ecf61eb10c56430b5e81b` | `b136676ef7aa946a6b90ca15b13acdffb727a91a` | Current workspace base when this inventory was written. Core owned-path manifests, engine/API source, and product tests are byte-identical to measured candidate `e5c3da65...`; only the live benchmark example changed after measurement, with the exact measured source retained beside the evidence. |

“Source checkpoint” and “evidence carrier” are deliberately separate. In
particular, `9732d4d4...` is not the Fjall-era product source: it is the child
commit that added measurements taken from a workspace based on `f0ab89e9...`.
Neither `f0ab89e9...`, `095460b7...`, nor integrated `69b95604...` tracks a
root `Cargo.lock`; their spike-local benchmark locks live in the evidence
commits. Current `774e6454...` does track root `Cargo.lock` as Git blob
`20f237d90ebd22d7963a38cf19ca3f8f9cb5c4ab` (90,124 bytes; SHA-256 recorded
below). Source commit identity alone is therefore not enough to reproduce a
historical binary: comparator preparation must materialize and attest the
intended benchmark lock explicitly.

## What each checkpoint actually represents

### Fjall-era composed engine: `f0ab89e9...`

This is the best exact old-engine checkpoint. It is post-Book-removal, so it
already has block-native reads and the large resident-memory win from Spike C,
but its append path still contains the old `AppendGate`, per-append
`spawn_blocking`, `PublishSequencer`, `MetaStore::apply_group`, and
Fjall-authoritative stream/type names plus `persist_new_names`. It is therefore
an honest “old Fjall-based append engine” without also resurrecting the obsolete
all-history payload mirror.

The baseline-gen2 evidence was measured from this source and committed in
`9732d4d4...`. Process results are usable historical orientation and regression
anchors. The report itself marks the four-writer small-batch Group convoy split
as **provisional**, because barrier counts were about twice bare in several
cells and disagreed with the earlier Spike-B session. Those rows are evidence
of the old engine's observed behavior, not a durable performance floor.

### Bare engine: a lower bound, not a product checkpoint

`spikes/baseline_matrix` constructs the raw `mess-log` committer/appender with
numeric stream IDs. It has no public-name registry, expected-version validation,
active/read index, snapshot wrapper, or facade. The logical payload, batch,
writer count, durability mode, work count, fresh-store rule, and measurement
session match the `log` cell, but the producer/API topology does not:

- `log` uses Tokio tasks and `LogEngine::append_batch`;
- `bare` uses direct appenders and numeric IDs;
- the bare path intentionally serializes frame encoding, while final `bn-2su`
  overlaps deterministic large-batch preparation on producer tasks.

Consequently, bare is a useful within-cell mechanical lower bound, not a claim
that production ought to have identical absolute cost. A fresh bare binary must
be built from the exact codebase named by the new contract; an old bare number
must not be paired with a new composed number from another source/session.

### Final `bn-2su`: measured `095460b7...`, integrated `69b95604...`

The final matrix includes the owner-direct writer, D7 early-close, producer-side
large-batch preparation, prepared-buffer cache adoption, and registry ordered
units. All 32 Process/Group cells use the same baseline-matrix shapes. Every
Process cell beat the Fjall-era median and no Group cell regressed by 10%; the
four-writer barrier counts regained parity with bare.

This is the strongest historical production checkpoint because Git archaeology
can prove that the measured workspace and integrated product differ only by a
two-line correction to the evidence report; the product source and raw matrix
are identical. (`072cc9ef...` is an alternate content transplant of the
optimization without `handoff.md`, not the canonical measured checkpoint.) It
still lacks sealed binary hashes, per-row load, and a final run log, so it should
be remeasured rather than treated as a cryptographically bound control
executable.

**Comparator recommendation:** use integrated main commit
`69b95604b9e7c924314cf7d82b86a2edb7dccde6` and tree
`f738cde2b5414d1a2926ac86573163d505754ba9` for the fresh public-path
rebaseline. It contains the exact final optimized product plus the corrected
evidence and is on main. Retain `095460b7a597327662da9971dbdd4a209e49a3c1`
and tree `40f1072cf176bd8d153e00415c03d675481f28c9` as original measured-source
provenance, not as the preferred build checkout.

### Current production: direct outcomes plus Process-owned public append

The current path includes two post-`bn-2su` integrations that matter to a
rebaseline:

1. `7da5fb15...` replaces direct acknowledgements with direct outcomes and
   reusable bounded scratch. Its final paired matrix and predeclared Group/Os
   variance studies are admissible for their stated cells.
2. `1f4d30ef...` integrates the terminal-verified Process-only owned append
   seam. Current `774e6454...` preserves the measured engine/API source and
   measured lockfile. `Group` and `Os` deliberately remain on the borrowed-
   compatible path.

The exact measured candidate's core files are byte-identical at current main,
including `Cargo.toml`, `Cargo.lock`, `crates/mess-store/Cargo.toml`,
`backend.rs`, `engine.rs`, `fjall_snapshot.rs`, `lib.rs`, and `store.rs`. For
example, the measured/current `engine.rs` hash is
`d088e6136608908eaaa3a25b19a55f562699da9d3eedaee7850c7e52ef4bab99` and
the measured/current `Cargo.lock` hash is
`9c24189940d9b43d7798c6680c8aeab6ddc270ef9b450390334d9327405cbea0`.

The measured harness used unconditional sealed build identities and has hash
`e7b33bcf3022a51c3f9e9f74fa3cf2de47bf8217905fb4be05920c677e7ae10e`.
Current main's live example has hash
`d9141dbea8c132e6b14861acbf3575894bbff6fa12e3d74ad49019c6ebef712c`
after a test-compilation fix; the exact measured harness is retained as
`spikes/owned_append/adopt-20260715/owned_append_bench.rs`.

## Workload and topology compatibility

| evidence | exact timed shape | topology and ordering | what it can match |
| --- | --- | --- | --- |
| baseline-gen2 | `{Process, Group} x {24 B, 250 B} x {batch 1,10,100,1000} x {1,4 writers}`; Process batches/writer `40000/12500/2500/250`, Group `800/500/300/100` | Fresh ext4 store each run; stable one-stream-per-writer names; `bare` versus direct `LogEngine::append_batch`; three reps per engine in `AB BA AB`; 10% per-writer warm-up removed from latency samples. | Exact cell semantics reused by `BN-2SU-FINAL.csv`. It does not exercise today's public owned seam. |
| baseline new-name submatrix | 250 B, batch 1, 100% new streams, Process/Group, 1/4 writers | Same harness/session; 24 raw rows are appended to `baseline_results.csv`. | Fjall-era new-name cost only. There is no `bn-2su`/current matched new-name rerun. |
| `BN-2SU-FINAL` | Same 32 cells and work counts as baseline-gen2 | Fresh ext4; direct `LogEngine::append_batch`; `bare` and `log`, three reps in `AB BA AB`. | Historical Fjall-era, bare, and post-flat-owner cells can be aligned by key. Cross-file Fjall comparisons are cross-session; within-file `bare`/`log` pairs are same-session. |
| direct outcomes | 24 B, four writers, batches 1/10/100/1000, Process/Os/Group; BN-exact work counts | Same-host candidate/control `A1 B1 B2 A2`; 144 final rows, plus separately predeclared Group b1/b10 and Os b1/b10 studies. | Isolates the integrated direct-outcome delta. It omits 250 B, one writer, Fjall-era, and bare. |
| owned append ADOPT | Process only, 250 B, four writers, batches 1/10/100/1000; work counts `40000/12500/2500/250` | Public `EventStore -> FjallSnapshotBackend -> LogEngine`; fresh ext4 per observation; five cycles, odd `ABBA`, even `BAAB`, 20 rows/cell and 80 total; exact process/lease/source/binary guards. | Exact current public Process-owned delta against a neutral borrowed control. It includes neither bare nor Fjall-era, and makes no Group/Os timing claim. |

The topology distinction is a blocking contract issue: simply rebuilding and
running `spikes/baseline_matrix` on current main calls borrowed
`LogEngine::append_batch` and bypasses the public owned-transfer win. A complete
rebaseline needs either two explicit strata or one deliberately common public
driver:

- a kernel stratum matching historical `LogEngine::append_batch` to bare; and
- a public-product stratum matching `EventStore -> snapshot wrapper -> engine`
  across old control and current production.

Do not silently call the borrowed kernel stratum “current production.”

## Immutable artifact inventory

### Baseline-gen2 / Fjall-era (`9732d4d4...` evidence commit)

| artifact at the evidence commit | SHA-256 |
| --- | --- |
| `spikes/baseline_matrix/src/main.rs` | `47514c420c4237b5fcab54ca9a7594094d91548979e89dd9d137d52f9911b395` |
| `spikes/baseline_matrix/src/timing.rs` | `c95988729efcd2cf5f3972e039293625f41e39684248472f1847658c3e7b7a7d` |
| `spikes/baseline_matrix/Cargo.toml` | `293b4783b776d14f43ca8301bcc2c8a9d766fe7c2e678078bc1d8414aeca47fd` |
| `spikes/baseline_matrix/Cargo.lock` | `ab405326315f1be1782feaf97b6e1c031f24480f32dc5532bfccdeb425ed3c97` |
| `baseline_results.csv` (216 data rows: 192 matrix + 24 new-name) | `91d3e2c8efb8a9f82549ceeca4e83dac8d7eb0b59065dcd801aa9cfd8c1e21e2` |
| `matrix_run.log` | `9b9bf61a48db0b59b4f42ae13e44cd01ea56c017c49faee5c535abc8a554c089` |
| `newname_run.log` | `2d7e5c6e81bdf3c7dff07eb0a1523381751dea7d8eca8a505a03b23ff7c5880b` |

No binary hash or per-row load field was recorded.

### Final `bn-2su` (`095460b7...` measured workspace)

| artifact | SHA-256 |
| --- | --- |
| `spikes/baseline_matrix/src/main.rs` | `eab43beb8f76f062a33f3a955755528e730bd4b149609dd113502192523f1ab4` |
| `spikes/baseline_matrix/src/timing.rs` | `c95988729efcd2cf5f3972e039293625f41e39684248472f1847658c3e7b7a7d` |
| `spikes/baseline_matrix/Cargo.toml` | `293b4783b776d14f43ca8301bcc2c8a9d766fe7c2e678078bc1d8414aeca47fd` |
| `spikes/baseline_matrix/Cargo.lock` | `c419b2f347aa384f695611003ff764d63da77fd3fb87615ced2e6aa099e3aea7` |
| `BN-2SU-FINAL.csv` (192 data rows) | `b2801a056a711de7a8643c15af2eb7d12a04b6315e0dec40a7beacc53c5bde40` |
| measured `BN-2SU-REPORT.md` at `095460b7...` | `52da22f8a273596c863e7bef00792fb969bbad454608375c92744a0076c6cbac` |
| two-line-corrected report at `60663621...` / integrated main | `5fd77aba49a656ea015d6d74a2dd6aab571dc1890a6553cfbb12660fc77fd0e2` |

There is no retained final run log, sealed binary hash, source attestation, or
per-row load column. The report only records completion loads and the exact
source is recovered from bone history plus Git, not frozen inside the CSV.

### Direct outcomes (`bn-21ew`)

| artifact | SHA-256 |
| --- | --- |
| final paired 144-row matrix | `b3576d6a3170dbdcd38fa9a97368047c355f7c8c4b2121cead959f925d4f328e` |
| predeclared Group parity study | `27f3bc42e6c7fb7e6c4773c44affb3a221fe8a8b8381687b6f2490c407765291` |
| targeted Os variance study | `a1ee5ef8691d4e492b937956b4b2140654952c078c509de68f5c189c4bffdb99` |
| final direct micro | `063f3628574abe5cd87c69196bdd155bf64ff419cf46e90b3ddf7df12047cf60` |
| harness `src/main.rs` | `1e2ef77183d082e9ef8620e35ec38d68853f6d13db15c322a6449368e1dce74e` |

The Group follow-up additionally binds candidate binary
`2668a3be17b43befc52c9c626b9967e2e13572eed407e72add6d0c0efaf4bb2b`
and frozen-control binary
`916d20aa6320f1ac87e58821f8530e8f144429c9450764ebc7624704174b8072`.

### Current owned-append admission (`bn-22it`)

| binding | SHA-256 / exact ID |
| --- | --- |
| reviewed tooling baseline | `fa6bc0cc2d533a9ef5e9fb54755007e2287ad28d`, tree `b2f2034f8ed348665a751a8125d3b5d7a55c616c` |
| neutral control | `0890cec2d734b047a71c3236db16faaeb38654df`, tree `04f40eb25a5e5d3dcce9e8d89f3f0d06cef59d4b` |
| measured candidate | `e5c3da658abd619c240851d384642fe741d93047`, tree `8504ef9bedac8534ac7f1032d5ff187fb77f057d` |
| source approval | `d86203d0b1397d0ee0c5f5d44ee9c5fe09a6e50e57361ee8408f4017ca12fece` |
| common exact harness | `e7b33bcf3022a51c3f9e9f74fa3cf2de47bf8217905fb4be05920c677e7ae10e` |
| common `Cargo.lock` | `9c24189940d9b43d7798c6680c8aeab6ddc270ef9b450390334d9327405cbea0` |
| control / candidate binaries | `452efa136b20a13bd3db963e43024f539d7f28b0a59cf496a654e6f0193c1199` / `25d57ddedd545bbcfdd98dd43dfe7b4bbee22400955c024fdba8a922cf50174b` |
| prepared pair | `fd5603a9a6c1a70c55a61ecbf296405464dd812ffd5c3117de7b641834256fff` |
| final 80-row CSV | `5715cf6883c8561560770f70181b50dcdab9cb66b239ef9a4552e8c1377f0d3b` |
| evaluator result (`ADOPT`) | `6714401d9db3b6c329ff299f772f757d17e9352ce8ca18516feb83db8edd9086` |
| terminal / independent terminal verification | `94e1660c9e76c01508f7f43a63df6c2f48e3acdfea18a9e2a7b52984678041b1` / `67e1ad0dc0c2e235727c25fed76de9634a4377c490b695415f50e846e76b5fe3` |

This is the only checkpoint in the inventory with a complete source, binary,
harness, raw-result, evaluator, lease, process-guard, and terminal chain.

## Evidence classification

### Admissible for its stated scope

- `baseline_results.csv`: accepted historical Process orientation and exact
  old-engine cells; Group four-writer small-batch rows remain explicitly
  provisional, not a locked floor.
- `BN-2SU-FINAL.csv`: accepted final flat-owner production matrix. Its
  same-session bare/log comparisons are stronger than cross-session ratios to
  baseline-gen2.
- `spikes/direct_outcomes/BN-21EW-FINAL-*` plus the predeclared Group and Os
  follow-ups: accepted for the direct-outcome integration; rejected/superseded
  files remain visibly named `REJECTED`.
- `spikes/owned_append/adopt-20260715`: terminal-verified `ADOPT`, but only for
  the public Process/250 B/four-writer cells and the stated allocation gates.

### Valid architecture evidence, not an integrated-production baseline

- `spikes/flat_combined_append`: a real measured prototype. `FlatEngine` lacks
  durable names and a production read index; `b0dth` also changes producer
  topology. It selected the design but is not current product performance.
- `spikes/composed_decision`: the real then-current engine, bare, and isolated
  flat kernel were measured, but the report explicitly says there was no fully
  integrated Asterism engine. Its composed totals are projections.
- Component spikes for dense heads, microblocks, effects, dedupe, directory,
  and SealPack remain component evidence unless their product path is named in
  Git history. A merged `spikes/**` directory is not proof of integration.
- `spikes/publication_v2`: candidate-only diagnostics; the report explicitly
  requires a paired gate before making a decision.

### Rejected or unusable for admission

- `spikes/group_publication/attempt8-20260715`: complete and well-bound, but
  only 3/8 cells passed and review found a reader-frontier correctness hole.
  The candidate is not integrated. Attempts 1–7 are partial/zero-row
  operational evidence only.
- Earlier owned-append Attempts 1–6, the first `bn-22it` 40-row run, and the
  complete retry-2 run: rejected, partial, `INCONCLUSIVE_INFRASTRUCTURE`, or
  `INVALID_EVIDENCE` exactly as their reports state. None may be pooled with
  the final 80 rows.
- `spikes/pwritev_group`: both attempts are inconclusive; Attempt 2 completed
  stage-1 rows but never produced selection/confirmation/evaluation. No timing
  inference or production integration is authorized.
- Every file explicitly named `REJECTED-*` under `spikes/direct_outcomes` is
  diagnostic only.

## Concrete gaps that require fresh measurement

1. **No direct current/Fjall/bare matrix exists.** Build fresh, source-bound
   binaries for `f0ab89e9...`, the selected bare source, final measured
   `bn-2su` integration `69b95604...` (with `095460b7...` retained as measured
   provenance), and current `774e6454...`; do not splice sessions.
2. **Public and kernel topologies are currently mixed.** Preserve the historical
   direct-`LogEngine` stratum, but add a public `EventStore -> snapshot wrapper
   -> LogEngine` stratum so current Process-owned transfer is actually exercised.
3. **The complete current shape is missing.** Freshly cover Process and Group,
   24/250 B, batch 1/10/100/1000, 1/4 writers with exact historical work counts.
   `Os` may be a focused structural/latency stratum, but cannot be inferred from
   Group. Add the historical 100%-new-name shape if old/new registry cost is a
   decision question.
4. **Old evidence lacks modern bindings.** Capture source commit/tree, clean
   status, binary, harness, lockfile, runner/evaluator, CPU/governor, toolchain,
   filesystem/device, physical order, pre/post load, child/process guards, final
   CSV hash/cardinality, and terminal outcome for every variant. In particular,
   the old product commits do not carry root locks; bind the exact spike lock or
   a predeclared common resolved lock rather than silently resolving afresh.
5. **Required metrics are not co-located.** The new matrix must emit throughput,
   p50/p99, allocations and bytes, boundary copy/owned-path counters, batches,
   groups, barriers, degradation, owner CPU, path selection, and syscall/write
   counts in the same measured interval. Queue fairness needs a predeclared
   definition and workload; candidate-only `publication_v2` phases are not a
   control.
6. **Historical Group behavior changed structurally.** Verify D7 barrier parity
   per cell rather than treating throughput as the durability oracle. Retain
   every device alarm and use a predeclared paired variance rule if needed.
7. **Reopen/RSS is stale relative to current.** The best locked old/post-Book
   anchor is roughly 1.67 s and 105 MiB at 2M events. Current owned append does
   not establish reopen/RSS neutrality; rerun the exact corpus if `bn-2l3n`
   makes an end-to-end engine claim.
8. **Correctness remains a separate gate.** Performance evidence must not
   replace cancellation, ordered registry-unit, same-stream race, roll, chain,
   recovery, EIO/ENOSPC, and reopen tests on the exact product source.

The historical evidence is valuable because it identifies the checkpoints and
workloads to preserve. It is not a substitute for a fresh, single-protocol
comparison of those exact checkpoints.
