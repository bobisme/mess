# Mess research pack

Generated: 2026-07-07

## Intent

This pack assumes the product goal is not “yet another queue” or “a distributed database first.”
The target is:

> A Rust-native event-sourcing store with stellar developer experience, boring operation, and a storage engine physically shaped around immutable event streams, snapshots, and cursor-driven replay.

The thesis after looking through storage engines, flash papers, learned indexes, probabilistic filters, CRDT theory, and incremental computation:

```text
canonical event bytes should live once, in append-only immutable segments;
all other structures should be compact, rebuildable acceleration indexes;
snapshots should be explicit fold checkpoints with integrity certificates;
projections should be modeled as typed folds over logs, not ad-hoc consumers.
```

## Files

1. [01_architecture_thesis.md](./01_architecture_thesis.md) — proposed architecture and invariants.
2. [02_storage_engine_landscape.md](./02_storage_engine_landscape.md) — RocksDB, redb, Fjall, WiscKey, FASTER, CompassDB, Cascade Log.
3. [03_flash_io_and_durability.md](./03_flash_io_and_durability.md) — SSD, ZNS, F2FS, io_uring, fsync, group commit, recovery.
4. [04_indexes_cursors_and_filters.md](./04_indexes_cursors_and_filters.md) — pointer indexes, opaque cursors, learned indexes, perfect hashing, segment filters.
5. [05_snapshots_projections_replay.md](./05_snapshots_projections_replay.md) — instant snapshot lookup, replay tail, projection checkpoints, cache strategy.
6. [06_algebraic_event_model.md](./06_algebraic_event_model.md) — free monoids, folds, semilattices, CRDTs, CALM, invariant confluence.
7. [07_probabilistic_and_learned_structures.md](./07_probabilistic_and_learned_structures.md) — Bloom-family filters, Ribbon/Binary Fuse, learned/radix/PGM indexes.
8. [08_novel_mechanisms.md](./08_novel_mechanisms.md) — new candidate mechanisms: Meridian segments, fold certificates, causal frontiers, static seal-time indexes.
9. [09_implementation_plan.md](./09_implementation_plan.md) — concrete Rust roadmap, benchmark harness, failure tests, API shape.
10. [10_bibliography.md](./10_bibliography.md) — source map.
11. [11_review.md](./11_review.md) — independent review: citation audit, correctness holes, omissions.
12. [12_convergence.md](./12_convergence.md) — **authoritative**: agreed design after review + response; supersedes 01–09 where they conflict.
13. [13_spike_results.md](./13_spike_results.md) — measured spike results validating the convergence doc's bets.

## High-level recommendation

Build the first serious version as:

```text
mess-core
├── append-only segmented event log         // canonical bytes
├── redb/Fjall/RocksDB index backend        // metadata + pointer indexes
├── stream/category/global cursor indexes   // append-optimized, rebuildable
├── snapshot store + snapshot_head table    // O(1) load-start
├── projection checkpoint table             // consumer positions
└── typed aggregate/projection API           // the product surface
```

The first production-worthy storage engine does **not** need global distribution. It needs exact single-node semantics, brutal crash-recovery testing, great APIs, and a file format you will not regret when replication arrives.

## One-sentence product wedge

> “Eventide-shaped event sourcing for Rust, with a purpose-built immutable log engine and typed aggregate/projection ergonomics.”

