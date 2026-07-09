# Performance & correctness envelope ledger

The recorded operating envelope for `mess-log` — the numbers Phase 3's exit
gate (`bn-2m1`) proved, and the seed of the Phase 6 regression suite. Every row
is one metric: `metric | value | conditions | date | source command`. Keep it
machine-greppable (one metric per line, stable metric keys) so a regression
lane can diff a fresh run against the recorded value.

Recording machine (reference hardware for every row below unless noted):

- CPU: AMD Ryzen 9 3900X (12C/24T)
- Kernel: Linux 7.0.12-arch1-1
- Store fs: ext4 on `/dev/nvme0n1p3` (NVMe, ~77% full at record time)
- Durable-bench scratch: `$HOME/.cache/mess-bench` (real ext4 — NOT tmpfs;
  `fdatasync` on tmpfs is a no-op and would fabricate durable throughput)
- Toolchain: stable rustc, `--release`

Method notes:

- Throughput rows are **best-of-N wall time** (device state and scheduler
  jitter only ever slow a run down, so the fastest rep is the truest read of
  the code path) — the `spikes/perf_group_commit` methodology.
- Harness rows are the full `#[ignore]`d profiles, run green in one local
  sitting (2026-07-09) that stands in for the first `phase3-exit-gate.yml` CI
  run; the recorded wall times prove the composite lane is nightly-feasible
  (all three back-to-back ≈ 60 s).

## Correctness harnesses (full profiles)

| metric | value | conditions | date | source command |
|---|---|---|---|---|
| crash_harness.cases | 12000 | SimRuntime + fault fs; deterministic seeds `0..12000`; 5691 injected crashes; 46906 acked batches recovered; **0 acked losses**; 0 partial batches visible | 2026-07-09 | `cargo test -p mess-log --release --test crash_harness randomized_crash_recovery_loop_full -- --ignored` |
| crash_harness.wall_s | 0.16 | test-body seconds (0.24 s wall incl. process) | 2026-07-09 | as above |
| crash_harness.green | true | pass/fail gate | 2026-07-09 | as above |
| torn_matrix.cases | 24000 | 6 configs x 4000 iters; MASTER_SEED `0x7042D15C5EED`, GOLDEN `0x9E3779B97F4A7C15`; 30016 acked verified, 3031 unacked surfaced (A6); CRC sole rejector in 1876; differential A4 demonstrated (398 weak-accepted in 392 cases, ~1.63%) | 2026-07-09 | `cargo test -p mess-log --release --test torn_matrix torn_matrix_full -- --ignored` |
| torn_matrix.wall_s | 0.78 | test-body seconds (0.87 s wall) | 2026-07-09 | as above |
| torn_matrix.green | true | pass/fail gate | 2026-07-09 | as above |
| sigkill_harness.rounds | 45 | real process spawn + real SIGKILL; 3 modes x 15 rounds; seeds `seed_for(mode, round^salt)`, salts process=100/os=101/group=102; kill window 200-2500 ms; every round recovered >= acked (contiguous prefix), 0 acked losses | 2026-07-09 | `cargo test -p mess-log --release --test sigkill_harness -- --ignored --test-threads=1` |
| sigkill_harness.wall_s | 58.71 | test-body seconds (58.80 s wall); real ext4 under `$HOME` | 2026-07-09 | as above |
| sigkill_harness.green | true | pass/fail gate | 2026-07-09 | as above |

## Durable append throughput (mess-log committer, group commit)

| metric | value | conditions | date | source command |
|---|---|---|---|---|
| mess_log.durable.ev_per_s | 148484 | Durability::Group (D7 early-close, fdatasync/group); 4 writers x 100-event batches; ~250 B payload; ext4 scratch; best-of-5; 200k events; ev/fsync ~398; mean fdatasync 2.56 ms | 2026-07-09 | `cargo run -p mess-log --release --example durable_bench -- durable 4 100 500 5` |
| mess_log.durable.gate | 100000 | acceptance floor (>=100k ev/s @ 4x100) — PASS (1.48x) | 2026-07-09 | as above |
| mess_log.durable.mean_fsync_ms | 2.56 | one durability barrier, settled NVMe ext4 | 2026-07-09 | as above |

## Buffered append throughput (mess-log committer, Durability::Process)

| metric | value | conditions | date | source command |
|---|---|---|---|---|
| mess_log.buffered.ev_per_s | 4710027 | Durability::Process (write(2) into page cache, no barrier); 4 writers x 100-event batches; ~250 B payload; ext4; best-of-5; 2M events; 0 fsyncs | 2026-07-09 | `cargo run -p mess-log --release --example durable_bench -- buffered 4 100 5000 5` |
| mess_log.buffered.gate | 1000000 | acceptance floor (>=1M ev/s) — PASS (4.71x) | 2026-07-09 | as above |
| mess_log.buffered.b10.ev_per_s | 2038287 | same, 4 writers x **10**-event batches (matches interim-backend batch for the side-by-side below); best-of-5; 1M events | 2026-07-09 | `cargo run -p mess-log --release --example durable_bench -- buffered 4 10 25000 5` |

## Interim backend (mess_db-class RocksDB actor) — side-by-side

Buffered append, the interim (Phase 2) backend's decisive workload. Measured
fresh here from `spikes/perf_append`'s `rocks` variant (the same RocksDB 0.21
pin as `mess_db`), 4 writers x 10-event batches, 1M events, ~257 B payloads.

| metric | value | conditions | date | source command |
|---|---|---|---|---|
| interim.rocksdb.buffered.ev_per_s | 453011 | RocksDB 0.21 actor; 4 writers x 10-event batches; buffered; best-of-3; ext4; fresh measurement (device ~77% full) | 2026-07-09 | `CLANG_PATH=/usr/bin/clang ./target/release/perf_append bench rocks 4 10` (in `spikes/perf_append`) |
| interim.rocksdb.buffered.ev_per_s.report | 557000 | same variant, `spikes/perf_append/REPORT.md` original day (fresher device state) | 2026-07-08 | cited from REPORT |
| mess_log.vs_interim.buffered.speedup | 4.5 | mess-log buffered 4x10 (2.04M) / RocksDB 4x10 (453k), same batch, same machine | 2026-07-09 | derived from the two rows above |
| composed_engine_a.buffered.ev_per_s | 1710000 | full composed Engine A (log + pointer index, index off the append path); 4 writers x 10; buffered; cited | 2026-07-08 | `spikes/perf_append/REPORT.md` |

## Gate summary

| metric | value |
|---|---|
| phase3.exit_gate.harnesses_green | 3 of 3 (crash 12k, torn 24k, sigkill 45 rounds) |
| phase3.exit_gate.durable_gate | PASS (148k >= 100k ev/s @ 4x100) |
| phase3.exit_gate.buffered_gate | PASS (4.71M >= 1M ev/s) |
| phase3.exit_gate.vs_interim | PASS (mess-log buffered 4.5x RocksDB at equal batch; durable 148k vs interim's no-durable-equivalent) |
| phase3.exit_gate.one_ci_run | `.github/workflows/phase3-exit-gate.yml` runs all three full profiles in one job; this local run (2026-07-09, ~60 s combined) stands in for the first CI run |
