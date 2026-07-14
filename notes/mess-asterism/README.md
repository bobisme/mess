# Asterism: a log-derived state kernel for Mess

This package proposes a storage-engine redesign for Mess that removes Fjall from the steady-state write and read paths rather than attempting to build a more general LSM tree.

The central claim is:

> The fastest and most durable metadata store for an append-only event log is often no metadata store at all. Put every authoritative state transition in the committed log capsule, keep the live state in workload-shaped memory, summarize sealed segments algebraically, and treat every checkpoint or sidecar as a self-verifying cache.

## Documents

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

## Status vocabulary

The documents deliberately distinguish four classes of statement:

- **Measured** — already measured in the Mess repository or cited primary work.
- **Derived** — follows from code inspection, a proof, or arithmetic over measured values.
- **Target** — an acceptance threshold for an implementation.
- **Hypothesis** — a mechanism that must earn admission through a spike.

No projected throughput number is represented as measured.

## Proposed decision

Build Asterism as a series of reversible replacements behind the current `Backend` seam. The first milestone is not a format rewrite. It is a composed-engine spike that replaces:

1. per-append Fjall metadata batches,
2. the global `Book` payload mirror,
3. per-append gate/publish sequencing,

with a single-owner state kernel fed directly by the existing durable committer. If this cannot reduce composed overhead to within 15% of the bare log on equal workloads, stop. If it does, the v4 commit-capsule format and persistent kernel checkpoints become justified.
