# mess-bench (bn-4pk)

The envelope regression suite: one runner over the reference workload set
`docs/perf/envelope.md` recorded by hand — buffered append, durable append,
sealed pointer-index reads (global scan + stream replay), the sealed
columnar payload codec, engine end-to-end buffered append + sealed replay,
fold-chain overhead, `load_verified` throughput, and recovery time. Emits a
JSON ledger per run and compares it against a committed `floors.json` with a
per-metric tolerance (default -10%).

See the crate-level doc comment in `src/lib.rs` for the design rationale
(why in-process ports of the existing bench entry points rather than
subprocess orchestration; the settle-pacing trade-off vs
`spikes/perf_group_commit`'s full interleaved-rep methodology; the tmpfs
refusal).

## Modes

```bash
# Full gated run: envelope-matching sizes, real-fs scratch REQUIRED,
# compared against floors.json by default. ~1-2 minutes on the reference
# host (7 workloads + 7 settle sleeps).
CLANG_PATH=/usr/bin/clang cargo build -p mess-bench --release
MESS_BENCH_DIR=$HOME/.cache/mess-bench \
  ./target/release/mess-bench run --mode full \
  --out ledger.json --floors crates/mess-bench/floors.json

# Smoke run: reduced sizes, floors never enforced. Same code paths as full,
# just tiny N — this is what `cargo test -p mess-bench` also drives
# in-process (tests/smoke.rs), so it needs no separate wiring to "not rot".
cargo run -p mess-bench -- run --mode smoke

# Re-compare an existing ledger without re-running anything.
cargo run -p mess-bench -- compare --ledger ledger.json \
  --floors crates/mess-bench/floors.json
```

Exit code: `0` if every floor-gated metric is within tolerance, `1` if any
metric regressed, `2` on a setup error (bad args, tmpfs scratch dir, I/O
failure).

## CI / nightly wiring

- **Per-PR / every `cargo test` run**: `cargo test -p mess-bench` runs the
  smoke suite (`tests/smoke.rs`) — every workload at tiny N, asserting each
  emits its expected metric key with a finite, non-negative value. This is a
  harness-rot check (did an API rename / panic / JSON shape change break a
  workload?), not a performance check; it takes well under a second and has
  no floors, no tmpfs requirement beyond `$HOME/.cache` being real-fs (true
  on every host this project builds on).
- **Nightly**: `.github/workflows/envelope-regression.yml` runs
  `mess-bench run --mode full` in `--release` on a real-fs runner path and
  fails the job on a non-zero exit, mirroring
  `.github/workflows/phase3-exit-gate.yml`'s pattern (scheduled, not
  `pull_request` — this is a ~1-2 minute composite of several fs-heavy
  benches, not per-PR-critical-path material). The ledger is uploaded as a
  build artifact so a regression can be diffed against the previous nightly
  run by hand.

## Updating floors.json

`floors.json` is a committed ratchet, not a live document — bump a floor
only when a deliberate design change moves the baseline (and say so in the
commit message + a comment in the `source` field), never to silence a real
regression. Each entry: `metric` (must match a ledger row's `metric` key
exactly), `direction` (`"min"` for a throughput floor, `"max"` for a
size/latency ceiling), `floor` (the acceptance-gate value, not necessarily
the last-measured value — see each entry's `source`), `tolerance` (a
positive fraction; `0.10` is the suite default), `source` (where the number
came from — an `envelope.md` gate row, the bn-4pk bone's own acceptance
criteria, or a seeded regression baseline for a metric with no named gate
yet).

## Machine profiles (bn-w32)

Two floors files exist:

- `floors.json` — the true envelope ratchet, seeded from doc-12 gates and the
  reference workstation (Ryzen 9 3900X). Run locally.
- `floors-ci.json` — used by `.github/workflows/envelope-regression.yml`.
  Identical except the CPU-bound BLAKE3 metrics (`phase5.foldchain.*`,
  `phase5.verify.load_verified.*`), whose floors are seeded from observed
  GitHub `ubuntu-latest` runner numbers (~30% slower at hashing than the
  workstation, while I/O metrics run faster there). A regression that clears
  the CI floor but would trip the workstation floor is caught by the local run.

When re-seeding either file, state the source run/host in each entry's
`source` field.
