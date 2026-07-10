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
| mess_log.buffered.metrics_overhead_pct | ~1 | bn-e2y runtime-metrics counters (3 relaxed atomic fetch_adds per batch): same-machine A/B, best-of-5 with counters 4417725 ev/s vs without 4475227 ev/s (~1.3%, inside the ~4% run-to-run noise band) — trivial, per bone requirement | 2026-07-10 | `cargo run -p mess-log --release --example durable_bench -- buffered 4 100 5000 5` |

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

## Sealed read paths (mess-index, bn-1hx)

Read paths across many sealed segments: coalesced per-stream replay, a parallel
global scan (both `std::thread::scope`, no rayon), and a bounded-bytes LRU of
decoded pointer blocks. Corpus: 24 segments tiling the A1 axis, 2,000 streams in
every segment, 5 batches/stream/segment, 10 frames/batch = **2.4M events**, ~48k
pointer blocks. **Blocks are uncompressed until Phase 5**, so these are pointer-
index decode rates (varint decode + assembly, no zstd) — the Phase-5 decompress
stage slots into the same staged path (`locate → materialize → decode →
assemble`) and will pull them toward `perf_replay`'s 95M / 24.6M ev/s. Best-of-5
wall; the byte-identity gate (parallel result == single-threaded reference,
order-independent checksum) ran on every measured pass.

| metric | value | conditions | date | source command |
|---|---|---|---|---|
| sealed.global_scan.ev_per_s | 1197000000 | 24-way parallel per-segment decode, concatenated in base_pos order; 2.4M events; byte-identical to sequential | 2026-07-09 | `cargo test -p mess-index --release --test sealed_read_paths bench -- --ignored --nocapture` |
| sealed.stream_replay.ev_per_s | 1376000000 | coalesced replay of 1,000 random streams x 24 segments (1.2M events), batched across the working set; byte-identical to sequential | 2026-07-09 | as above |
| sealed.stream_replay.floor | 2500000 | acceptance floor (>=2.5M ev/s stream replay) — PASS (~550x) | 2026-07-09 | as above |
| sealed.block_cache.hit_rate | 0.602 | repeat replay of the same 1,000-stream set (two passes); 19,104 unique blocks, 4.8 MiB resident; bounded by decoded byte weight | 2026-07-09 | as above |
| sealed.read.byte_identity | true | parallel == single-threaded reference on every run (stream + global), splitmix64 order-independent checksum | 2026-07-09 | as above |
| sealed.read.sigbus_stance | typed-error | truncated sidecar => typed SidecarError at open (no mmap, no SIGBUS surface); survivors replay exact | 2026-07-09 | `cargo test -p mess-index --test sealed_read_paths truncated -- --nocapture` |

## Phase 4 exit gate — composed engine swap (bn-20b)

The moment EventStore's default backend becomes the composed `mess-log` +
`mess-index` engine (`mess_store::engine::LogEngine`). These rows are measured
through the **full engine** (the `Backend`/`EventStore` seam), not the isolated
log or index paths above — so they include the append gate's committer bridge
and the sealed read's payload materialisation, the true end-to-end cost the
facade pays. Recorded on the bn-20b workspace host (Linux 7.0.12-arch1-1, ext4
scratch under `$HOME/.cache/mess-bn20b-bench`, `--release`, single measured
run). Reproduce: `CLANG_PATH=/usr/bin/clang cargo bench -p mess-store --bench
engine_envelope`.

| metric | value | conditions | date | source command |
|---|---|---|---|---|
| engine.buffered.ev_per_s | 2960000 | full composed engine append path (`LogEngine::append_batch` through the real `mess-log` committer, `Durability::Process`); 400 x 5,000-event batches = 2M events; ~24 B payload; ext4 scratch; single run | 2026-07-09 | `cargo bench -p mess-store --bench engine_envelope` |
| engine.buffered.gate | 1000000 | acceptance floor (>=1M ev/s buffered through the composed engine) — PASS (2.96x) | 2026-07-09 | as above |
| engine.sealed_replay.ev_per_s | 3460000 | `EventStore` load of a 2M-event sealed corpus: `read_stream` routes through the real `mess-index` `ReplaySet` cold path, then materialises `StoredRecord`s (message_type + payload) from the record book; single stream; single run | 2026-07-09 | as above |
| engine.sealed_replay.gate | 2500000 | acceptance floor (>=2.5M ev/s sealed replay through EventStore load) — PASS (1.38x) | 2026-07-09 | as above |
| engine.recovery_fast.s | 0.00014 | **`mess-log` recovery in ISOLATION** (not the engine open path): `recover_whole_log` fast path + advisory manifest over a realistic sealed corpus — 32 sealed segments, 3.2M events (200 batches/seg x 500 frames); manifest-seeded, trailer cross-check. Measures only stitching the committed prefix from footers; it does NOT decode payloads or rebuild the record book — see `engine.open_rehydrate.*` for the engine's true reopen cost | 2026-07-09 | as above |
| engine.recovery_fast.gate | 0.5 | acceptance ceiling (<=0.5s recovery fast path) — PASS (~3,500x margin) | 2026-07-09 | as above |
| engine.open_rehydrate.ev_per_s | 3476813 | **engine open-WITH-rehydration** (bn-20b), the true `LogEngine::open` reopen wall time over a populated single active segment: scans the durable log, decodes every frame's payload via `mess-log`'s `AcceptedBatch::frames`, and rebuilds the record book + active index + interner (from the meta name tables). 1,000,000 events (200 streams x 100 batches x 50 frames, ~8 B payloads); 0.2876 s; ext4 scratch; single run | 2026-07-09 | `cargo test -p mess-store --release --test engine_reopen measure_open_with_rehydration_wall_time -- --ignored --nocapture` |
| engine.open_rehydrate.s | 0.2876 | same run: 1M-event reopen wall time (book fully rehydrated, `total_events == 1_000_000` asserted). Unlike the isolation row above this pays the payload-materialisation + book-build cost the `Backend` reads depend on | 2026-07-09 | as above |
| engine.zero_api_changes | true | all Phase 1/2 mess-store suites (bank_account, cache, snapshot_law, differential_model, registry, fjall_snapshot) green on `LogEngine` with no `Backend`/facade API change; differential oracle green vs the engine across 192 randomized sequences with 198 **GENUINE** crash-reopens (drop engine, release `StoreLock`, fresh `LogEngine::open`, book rehydrated from the log — no shared `Arc`); plus the `engine_reopen` regression probe | 2026-07-09 | `cargo test -p mess-store` |

## Phase 5 exit gate — verification + compression envelope (bn-1l6)

The moment the fold chain (D4), fold certificates (`load_verified`), and the D6
columnar payload sidecar are all on trunk. These rows independently verify the
phase's two promises — **sealed tier ≤ 35 B/event with replay ≥ 2.5M ev/s**, and
**`load_verified` round-trips through the crash-recovery path** — plus the
append-side and verify-side costs. Same reference host (Ryzen 9 3900X, Linux
7.0.12-arch1-1, `--release`). The columnar rows are in-memory encode/reassemble
(no fs); the engine row uses ext4 scratch under `$HOME/.cache`.

| metric | value | conditions | date | source command |
|---|---|---|---|---|
| phase5.sealed.bytes_per_event | 8.86 | columnar `.pcol` payload sidecar, D6 default (zstd-9, 128-event blocks, no dicts); 1M-event block-clustered corpus (~90% shreddable msgpack, ~10% binary-block row fallback → 722/7813 row blocks); raw 34.7 B/event ⇒ 3.92× | 2026-07-09 | `cargo test -p mess-index --release --lib payload_replay_bench -- --ignored --nocapture` |
| phase5.sealed.bytes_per_event.gate | 35 | acceptance ceiling (≤35 B/event, columnar) — PASS (8.86, 3.95× margin) | 2026-07-09 | as above |
| phase5.sealed.columnar_replay.ev_per_s | 9370000 | `reassemble_all` over the `.pcol` sidecar: columnar-decode all 1M events in stored order; single core, in-memory (106.7 ms) | 2026-07-09 | as above |
| phase5.sealed.columnar_replay.gate | 2500000 | acceptance floor (≥2.5M ev/s sealed replay, columnar on) — PASS (3.75×) | 2026-07-09 | as above |
| phase5.sealed.point_read.us | 6.236 | one `reassemble_event` per block (7813 reads): cold decode of the owning block, mid-block index | 2026-07-09 | as above |
| phase5.seal_verify.ev_per_s | 1330000 | columnar seal encode + **permanent verify-on-seal** (every block reassembled and byte-compared) over the 1M-event corpus (749.8 ms) | 2026-07-09 | as above |
| phase5.foldchain.append_overhead_ns | 169.3 | `chain_step` delta: the second BLAKE3 (over 72 fixed bytes) the crypto chain adds on top of a frame-hashing store; 1M events, 250 B payloads, single core | 2026-07-09 | `cargo run -p mess-log --release --example fold_chain_bench` |
| phase5.foldchain.full_chain.ev_per_s | 1949088 | full chain (2× BLAKE3/event: `frame_hash` + `chain_step`); 513.1 ns/ev; same run (frame_hash-only baseline 343.8 ns/ev) | 2026-07-09 | as above |
| phase5.verify.load_verified.ev_per_s | 3590000 | `load_verified` over a 1M-event tail (snapshot at v=0 ⇒ full prefix cert + tail replay + head anchor); 278.4 ns/ev; best-of-3; single core, in-memory | 2026-07-09 | `cargo test -p mess-log --release --test crash_verify verify_throughput_bench -- --ignored --nocapture` |
| phase5.engine.sealed_replay.ev_per_s | 3600000 | end-to-end `EventStore` load of a 2M-event sealed corpus through the composed engine (`ReplaySet` cold path + record materialization); single run; ext4 scratch | 2026-07-09 | `cargo bench -p mess-store --bench engine_envelope` |
| phase5.crash_verify.green | true | `load_verified` round-trips through the **production scanner**: 6 cases — honest batch-boundary + mid-batch loads over the recovered prefix; torn-tail truncation caught as `HeadMismatch` by the pre-crash durable anchor; CRC-repaired tail payload tamper (scanner accepts) caught by the chain as `ChainBreakPrev`/`HeadMismatch`; prefix frame-v tamper → `PrefixHashMismatch{FromFrameV}`; empty-tail Path-C retention anchor loads then rejects on tamper | 2026-07-09 | `cargo test -p mess-log --test crash_verify` |
| mess_store.snapshot_law.full_run_s | 3714.59 | `snapshot_plus_tail_equals_full_replay` full 3000-iteration real fjall+`LogEngine` fsync-bound property run, `--release`; TMPDIR on `/home` ext4 (NOT tmpfs); excluded from routine test/CI runs for exceeding tool/CI timeouts (`cargo test -p mess-store -- --skip snapshot_plus_tail`), soaked separately; use `MESS_SNAPSHOT_LAW_ITERS` to scale it down for quick local runs (bn-1bs) | 2026-07-09 | `cargo test -p mess-store --release --test snapshot_law snapshot_plus_tail_equals_full_replay` |
| mess_store.snapshot_law.caveat | host_contention | ledger throughput numbers measured under host contention (e.g. this soak running concurrently with other workspace builds/tests) read ~5-13% optimistic vs fresh runs — treat contended-run throughput rows as an upper bound, not a guarantee (bn-1l6 review observation) | 2026-07-09 | n/a |

## Gate summary

| metric | value |
|---|---|
| phase5.exit_gate.bytes_per_event_gate | PASS (8.86 ≤ 35 B/event, columnar `.pcol` sidecar) |
| phase5.exit_gate.columnar_replay_gate | PASS (9.37M ≥ 2.5M ev/s, `reassemble_all` over the sidecar) |
| phase5.exit_gate.engine_sealed_replay | PASS (3.60M ≥ 2.5M ev/s, end-to-end EventStore load) |
| phase5.exit_gate.load_verified_post_crash | PASS (verified load round-trips through the scanner; truncation/tamper caught with the correct typed outcomes — `crash_verify`, 6 cases) |
| phase4.exit_gate.buffered_gate | PASS (2.96M >= 1M ev/s, composed engine append path) |
| phase4.exit_gate.sealed_replay_gate | PASS (3.46M >= 2.5M ev/s, EventStore load of sealed corpus) |
| phase4.exit_gate.recovery_gate | PASS (0.14 ms <= 0.5 s, recover_all + manifest, 3.2M ev / 32 segs) |
| phase4.exit_gate.zero_api_changes | PASS (all Phase 1/2 suites + examples green on LogEngine; interim MockBackend demoted to the `mock` feature) |
| phase3.exit_gate.harnesses_green | 3 of 3 (crash 12k, torn 24k, sigkill 45 rounds) |
| phase3.exit_gate.durable_gate | PASS (148k >= 100k ev/s @ 4x100) |
| phase3.exit_gate.buffered_gate | PASS (4.71M >= 1M ev/s) |
| phase3.exit_gate.vs_interim | PASS (mess-log buffered 4.5x RocksDB at equal batch; durable 148k vs interim's no-durable-equivalent) |
| phase3.exit_gate.one_ci_run | `.github/workflows/phase3-exit-gate.yml` runs all three full profiles in one job; this local run (2026-07-09, ~60 s combined) stands in for the first CI run |

## Regression harness (bn-4pk)

The rows above are hand-recorded, one-time captures — useful history, not a
ratchet. `crates/mess-bench` (bn-4pk) turns this same reference workload set
into one runnable harness: a JSON ledger per run, compared against a
committed `crates/mess-bench/floors.json` (seeded from the gate rows above,
default -10% tolerance). Smoke variant wired into `cargo test -p
mess-bench`; full gated mode runs nightly via
`.github/workflows/envelope-regression.yml`. See
`crates/mess-bench/README.md` for the mode breakdown and design notes.
