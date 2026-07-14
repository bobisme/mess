# Fold-chain performance-gate stabilization (bn-2nd)

## Diagnosis

The former normal-suite test timed `frame_hash`, then timed the full chain, and
subtracted the two wall-clock durations. That delta includes scheduler delay in
either phase. Under four busy loops pinned to the same CPU as the test, all
three baseline runs failed the 2,000 ns/event ceiling:

| run | frame hash (ns/event) | full chain (ns/event) | delta (ns/event) |
|---:|---:|---:|---:|
| 1 | 4,000.0 | 7,353.5 | 3,353.5 |
| 2 | 4,009.4 | 7,302.2 | 3,292.8 |
| 3 | 3,992.3 | 7,408.9 | 3,416.7 |

The exact same source on the same host, without same-core synthetic contention,
reported 663.6–674.6 ns/event in five debug-mode runs. Five release example
runs reported 169.0–172.0 ns/event. The failure therefore measured host load,
not a fold-chain regression.

Machine: AMD Ryzen 9 3900X (24 logical CPUs), Linux 7.0.12-arch1-1,
rustc 1.97.0. The ambient load average at capture was 13.17 / 16.29 / 11.75.

## Result

The normal suite now verifies the exact fixed-width construction
deterministically: one domain byte, two 32-byte hashes, and one little-endian
u64 (72 bytes after the tag). It also proves `advance` composes `frame_hash`
and `chain_step` for empty, realistic 250-byte, and 4 KiB payloads.

Wall-clock admission moved to an ignored, release-only quiet-host test. It uses
the median delta of five paired 1M-event samples and enforces both a 510
ns/event ceiling (3× the reference) and that the fixed-width chain step remains
cheaper than hashing a realistic 250-byte event.

Post-change results:

- quiet-host gate: frame hash 342.4 ns/event, full chain 514.0 ns/event,
  median delta **171.6 ns/event** — pass;
- normal deterministic test with the same four same-core busy loops: **3/3
  pass**, 0.00–0.01 seconds; the performance test remained ignored.

## Reproduction

Normal-suite coverage:

```sh
cargo test -p mess-log --test fold_chain_overhead -- --nocapture
```

Quiet-host performance admission:

```sh
cargo test -p mess-log --release --test fold_chain_overhead \
  chain_append_overhead_is_in_envelope -- --ignored --nocapture
```

The synthetic contention diagnosis pinned four busy loops and the test process
to CPU 0 with `taskset -c 0`; it is intentionally not part of routine CI.
