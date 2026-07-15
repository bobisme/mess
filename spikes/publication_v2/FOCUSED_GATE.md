# Publication v2 focused admission gate

This is the predeclared screening gate for the diagnostics-free candidate. It
must pass before the full 160-row matrix is eligible to run.

- Cells: Process/b1, Process/b100, and Group/b1000.
- Five counterbalanced cycles per cell: odd cycles A,B,B,A; even cycles
  B,A,A,B. This yields 60 retained observations.
- A is frozen control source `f712b03d123855f72e37b010cc621fc58709f1f5`
  and binary
  `0fe4bf83a71fa875e400704597c93b4e0bf3856fc6af45aff74114b568b7d705`.
  B is the clean candidate commit immediately preceding measurement, built
  after diagnosis-only clocks, counters, and sidecar code have been removed;
  its source and binary hashes are frozen in the pre-row provenance.
- The two release binaries are built before the first row. Their source,
  binary, Cargo.lock, and byte-identical harness hashes are recorded. Both
  harnesses must match locked SHA-256
  `e063a95a1df126e9e81ec7a336ce177f1d8dab0af66aaaaa086f546273367a21`.
- Each row uses the production
  `EventStore -> FjallSnapshotBackend -> LogEngine` path and a fresh store.
- A foreign build/benchmark process or load1 at/above 6 fail-stops the study.
  Completed rows, including durability-degraded rows, are never replaced.

For each cell, take the median of each cycle's two B observations divided by
the median of its two A observations, then the median of the five cycle
ratios. Every cell must satisfy all applicable locked gates:

- throughput B/A at least 0.97;
- append p99 B/A at most 1.10;
- allocation calls/event and bytes/event B/A each at most 1.05;
- Process fsync count exactly zero in every row;
- Group aggregate B barriers at most 100.25% of A and median cycle-level
  B-minus-A barrier delta no greater than zero;
- exact event, batch, borrowed-boundary, and copy-accounting totals.

No threshold may be changed after results. A rejection does not authorize a
replacement run. A pass authorizes the full matrix; it does not itself admit
the candidate.
