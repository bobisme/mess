# Asterism: a log-derived state kernel for Mess

This package records the Asterism storage-engine program: its original target,
the spike results that narrowed it, the portions already merged, and the
remaining decisions. It does not propose replacing Fjall with a general LSM.

The central claim is:

> The fastest and most durable metadata store for an append-only event log is often no metadata store at all. Put every authoritative state transition in the committed log capsule, keep the live state in workload-shaped memory, summarize sealed segments algebraically, and treat every checkpoint or sidecar as a self-verifying cache.

## Documents

- [`../../docs/adr/0002-asterism-capability-authority.md`](../../docs/adr/0002-asterism-capability-authority.md) — accepted bounded decision: discardable snapshot packs are selected; projection checkpoints and exact batch idempotency remain optional decisions with frozen authority/encoding constraints; v4 remains a separate gate.
- [`design.md`](design.md) — the complete proposed architecture, on-disk model, algorithms, invariants, performance targets, and go/no-go criteria.
- [`research/01-current-engine-and-fjall-gap.md`](research/01-current-engine-and-fjall-gap.md) — current code-path analysis and where the remaining cost actually lives.
- [`research/02-algebraic-state-kernel.md`](research/02-algebraic-state-kernel.md) — state-transition algebra, segment effects, parallel recovery, and proof obligations.
- [`research/03-succinct-indexes-and-dedupe.md`](research/03-succinct-indexes-and-dedupe.md) — dense tables, active microblocks, Elias–Fano, rank/select, PtrHash, k-perfect hashing, Ribbon retrieval, and exact epoch dedupe.
- [`research/04-durability-media-and-file-layout.md`](research/04-durability-media-and-file-layout.md) — capsule durability, checkpoint atomicity, seal packs, SSD placement, ZNS/FDP paths, and failure handling.
- [`research/05-benchmark-and-spike-plan.md`](research/05-benchmark-and-spike-plan.md) — falsifiable spikes, workloads, baselines, acceptance gates, and measurement discipline.
- [`research/06-migration-plan.md`](research/06-migration-plan.md) — **superseded historical** v3/Fjall migration and rollback analysis; retained for its authority inventory and data-loss traps, not as an implementation roadmap.
- [`research/07-literature-map.md`](research/07-literature-map.md) — annotated primary-source map, including July 2026 work and explicit maturity warnings.
- [`research/08-risk-register.md`](research/08-risk-register.md) — FMEA-style risk register and kill criteria.
- [`research/09-wire-format-v4-sketch.md`](research/09-wire-format-v4-sketch.md) — byte-level sketch for commit capsules with engine-control preludes.
- [`research/10-formal-verification-plan.md`](research/10-formal-verification-plan.md) — state-machine, concurrency, codec, and crash-model verification plan.
- [`research/11-review.md`](research/11-review.md) — adversarial reconciliation of the original pack with the engine and measured spikes.
- [`research/13-authority-and-fjall-deletion-map.md`](research/13-authority-and-fjall-deletion-map.md) — post-flat-owner source audit of every remaining in-memory/Fjall state item, its real authority and recovery path, integration surface, measurement contract, and safe deletion order.

`research/11-review.md` and `research/12-response.md` are review supplements,
not normative authority. Where they disagree with merged
code, accepted spike reports, research 13, or ADR 0002, those later sources
win. The response's accepted corrections have been incorporated into the
normative documents; the supplements remain outside the integrity manifest.

## Current versus target

As of 2026-07-14 the production v3 engine already has the flat owner,
log-derived strict `RegistryState`, block-native reads, an O(streams + event
types) `Book`, and no `MetaStore` call in engine append/read/recovery. The
current committer issues positioned writes; `Process` has no barrier, `Os`
syncs each batch, and `Group` uses one covering barrier for the gathered group.
Snapshot discovery through `FjallSnapshotBackend` is the only live
Fjall-backed state role and deletion blocker; its head is discardable and loss
falls back to full replay. It is not the only consumer of the library: CLI
`metaread` (and therefore doctor/inspect/retention) plus
`rebuild-index --meta` open `MetaStore` directly. `bn-3l8n` must migrate or
remove those operational paths together with application adoption before
Fjall deletion.

The remaining Asterism mechanisms are not one indivisible roadmap. Dense heads,
microblocks, SegmentEffects/checkpoints, bitrank directories, SealPack, exact
batch idempotency, projection controls, snapshot packs, and v4 each retain the
admission status recorded in the design and spike ledger. In particular,
ADR 0002 admits snapshot packs with discardable discovery, leaves projection
checkpoints and idempotency optional, and emits no `SnapshotInstalled` log
record.

## Status vocabulary

The documents deliberately distinguish four classes of statement:

- **Measured** — already measured in the Mess repository or cited primary work.
- **Derived** — follows from code inspection, a proof, or arithmetic over measured values.
- **Target** — an acceptance threshold for an implementation.
- **Hypothesis** — a mechanism that must earn admission through a spike.

No projected throughput number is represented as measured.

## Current decision

Continue Asterism as reversible, separately gated replacements. The first
milestone is complete: production now replaces the old append/publish chain
and unbounded payload mirror with:

1. a flat owner that is the direct committer/writer;
2. canonical `$registry` batches and a strict shared `RegistryState` fold;
3. block-native reads and bounded caches instead of an all-history payload
   mirror.

The old single 85%-of-bare gate was rejected as an unstratified API/topology
gate: accepted evidence reports every batch-size cell and preserves the raw
matrix. Current work follows ADR 0002 and research 13: replace snapshot
discovery end to end, then remove the dormant MetaStore/Fjall surface. Optional
idempotency, projection controls, kernel checkpoints, and v4 proceed only
through their own product and format gates.

## Integrity manifest coverage

[`MANIFEST.sha256`](MANIFEST.sha256) covers the normative design snapshot:
this README, `design.md`, research 01–10 and 13, and ADR 0002. It deliberately
excludes review supplements 11 and 12, spike reports/raw matrices, and source
code: those are independently versioned evidence and may receive review-side
edits without invalidating the normative snapshot. Paths in the manifest are
repository-root-relative and are verified with `sha256sum -c` from the
repository root.
