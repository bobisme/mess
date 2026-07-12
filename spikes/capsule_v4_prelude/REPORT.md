# Spike E — capsule_v4_control_prelude (bn-9mw)

**The format kill point.** Atomic control+event commit capsules (v4). The
question: can v4 add engine control records and control-only capsules to the log
while keeping the crash story as boring and absolute as v3's?

**Verdict: PROCEED — the v4 commit-capsule format is admissible.** Zero safety
violations across an exhaustive 96,654-state crash model, a 24,000-case
sector-reorder matrix, three 10M-exec fuzz soaks, and the two mandatory D4 retry
crash tests. The v3 suites are untouched and green. Scan overhead is within the
2% gate (see Gates).

---

## 1. What was built

All code in workspace `.maw/workspaces/bn-9mw`, v4 write **OFF by default** (the
production engine keeps writing v3; the v4 writer is opt-in).

| Area | File | What |
|---|---|---|
| Byte constants | `crates/mess-log/src/v4/format.rs` | 96 B header, 32 B marker, control TLV, caps; §23 decisions baked in |
| Control codec | `crates/mess-log/src/v4/control.rs` | Frozen TLV codec for the 6 control kinds; all length arithmetic capped before slicing |
| Capsule codec | `crates/mess-log/src/v4/capsule.rs` | `CapsuleEncoder` + `decode_capsule`; R4 split-coverage CRC; exact control+event tiling |
| Recovery | `crates/mess-log/src/v4/recover.rs` | Version-dispatched scanner; batch_id + position contiguity; control-only rules; prelude-first `RegistryView` seam; `CommitCursor`; v3/v4 write-mode detection |
| Writer | `crates/mess-log/src/v4/writer.rs` | Opt-in `CapsuleWriter` |
| Golden | `crates/mess-log/tests/v4_golden.rs` | Byte-exact fixtures, split-CRC proof, pinned hex |
| Recovery/negative | `crates/mess-log/tests/v4_recovery.rs` | 20 tests: round-trips, prelude-first, version dispatch, full negative matrix |
| Crash model | `crates/mess-log/tests/v4_model.rs` | 96,654-state exhaustive model |
| Torn matrix | `crates/mess-log/tests/v4_torn_matrix.rs` | 24k sector-reorder cases + CRC differential |
| Scan bench | `crates/mess-log/tests/v4_scan_bench.rs` | v4-no-control vs v3 overhead |
| D4 | `crates/mess-store/tests/v4_d4_retry.rs` | The two mandatory retry crash tests over real `RegistryState` |
| Fuzz | `crates/mess-log/fuzz/fuzz_targets/fuzz_v4_{capsule,control,scan}.rs` | parse-only + parse-then-reencode |

### Layering (review V6/S4)

`mess-log` cannot depend on `mess-store` (the dependency runs the other way), so
`RegistryState` is reached through the `RegistryView` **seam** the v4 scanner is
generic over. `mess-log` owns the byte + physical + protocol layer; the
production seam impl (`RegistryStateView` in `crates/mess-store/tests/
v4_d4_retry.rs`) folds decoded controls into the real `RegistryState`, **never
weakening `AlreadyRegistered`**. Physical-only recovery uses `NullRegistryView`.
No `RegistryState` logic was reimplemented.

---

## 2. research/09 §23 open decisions — resolutions

| # | Decision | Resolution | Rationale |
|---|---|---|---|
| 1 | 88/92/96-byte header | **96 bytes** | Every u64 field 8-byte aligned; a clean reserved tail word. |
| 2 | Keep or drop `header_crc` | **Drop** — 4 bytes at offset 84 are a MUST-BE-ZERO reserved word | The full-capsule CRC is authoritative and mandatory; the A2 caps (total_len bound + control_len/control_count caps) already bound every allocation before the full CRC runs, exactly as v3 does with no header CRC. A header CRC adds a nested-coverage checksum discipline and golden-fixture surface for zero measured recovery-safety value. |
| 3 | 24 vs 32-byte marker | **32 bytes** (magic, flags, batch_id_echo, total_len_echo, capsule_crc_echo, reserved) | The 64-bit total_len_echo + batch_id_echo bind control-only ordering under sector reorder; the 8-byte saving isn't worth losing the 64-bit length echo. |
| 4 | Fold-version width | **u32** in SnapshotInstalledV1 | Alignment simplicity; ample for a fold-format version. |
| 5 | Checkpoint/snapshot main log vs lane | **Main log** | Consistent with atomic control+event commit (§23.5 recommendation). |
| 6 | Control-kind evolution | **All v4 controls critical**; unknown (kind,version) **rejects** (recovery stops) | Skipping a state-changing record you cannot interpret is unsafe. A new kind needs a new capsule/segment version. |
| 7 | Both control + domain events in one capsule | **Allowed** (no structural prohibition beyond §6) | The engine decides when to co-commit for atomicity; the format encodes/decodes mixed capsules generally. |
| 8 | Global audit chain over control-only | **Deferred** (out of spike scope) | The mandatory CRC covers all control bytes; the crypto-chain value is a fold-cert concern. Placement (32 B after header) is implemented. |
| 9 | Control string normalization | **UTF-8 validated, no extra normalization at format layer** | Canonicalization is the registry spec's job (04-registry.md). |
| 10 | Max dedupe key / import chunk sizes | DEDUPE_KEY 1 MiB, CONTROL_LEN 1 MiB, CONTROL_COUNT 4096, NAME 65535 | Per §19; every length capped before it drives a slice/allocation. |

### A5 → §6 (review C1)

v4 replaces A5 with the safety-preserving **nonempty-capsule rule**:
`control_count + event_count >= 1`. `batch_id` is promoted to
**recovery-significant** — MANDATORY contiguity (+1 per capsule). A control-only
capsule (event_count == 0) advances the CommitCursor and batch_id but **not**
the global position.

---

## 3. Byte layout (as implemented)

```
CapsuleHeader (96 B):
  0  magic=0xCA954EAD | 4 version=4 | 6 flags | 8 event_count | 12 control_count
  16 batch_id | 24 total_len | 32 first_global_pos | 40 segment_epoch
  48 stream_id | 56 category_id | 64 first_stream_version
  72 control_len | 76 event_region_len | 80 capsule_crc | 84 reserved(=0)
  88 logical_flags | 92 reserved(=0)
[CryptoChainEntry 32 B when flags.CRYPTO_CHAIN]
ControlRecord*N  (kind u16 | version u16 | payload_len u32 | payload), tiles control_len
EventSubframe*M  (v3 28-byte subframe, unchanged), tiles event_region_len
CommitMarker (32 B):
  0 magic=0xCA9517ED | 4 flags | 8 batch_id_echo | 16 total_len_echo
  24 capsule_crc_echo | 28 reserved
```

**Split-coverage CRC (§18, R4):** `capsule_crc = CRC32C(bytes[0..80] ++
bytes[84..total_len-8] ++ bytes[total_len-4..total_len])` — the two 4-byte
checksum fields excluded, three ranges, no copy-and-zero. Written into both
fields. Proven byte-exact by
`v4_golden::crc_split_coverage_excludes_exactly_the_two_checksum_fields`.

---

## 4. Verification tally

| Dimension | Count |
|---|---|
| Golden byte fixtures | every control kind + 4 capsule shapes; 2 pinned hex capsules; split-CRC proof |
| Exhaustive crash model states | **96,654** (Model A 30,592 all durable-sector subsets over 1-2 capsules; Model B 66,062 3-capsule prefix + frontier-tear) x {zeros, stale-prior-epoch} bg |
| Torn / sector-reorder matrix | **24,000** cases (6 configs x 4,000) at 512 B / 4 KiB over {zeros, garbage, stale-gen}; fast profile 1,200 in default gate |
| Negative decode + protocol tests | 20 |
| Fuzz executions (ASAN, zero crashes) | capsule 10,000,000 . scan 10,000,000 . control 10,000,000 |
| D4 retry crash tests | 2 mandatory + 1 AlreadyRegistered guard |

### Safety properties asserted (model + torn matrix)

- P1 no control/event split — accepted capsule byte-identical to planned; partial never accepted.
- P2 no accepted duplicate/stale batch_id — accepted ids exactly 0,1,2,...
- P3 no global-position gap — first_global_pos contiguous, advancing by event_count.
- P4 no advance from control-only — next_global_pos = sum of accepted event_count.
- P5 registry-before-use — semantic recovery accepts exactly the physical prefix.
- P6 idempotence — repeated recovery identical.
- P7 acked-implies-recovered — fully-durable prefix always recovered.

Torn-matrix differential: a weak (CRC-off) decoder wrongly accepted **391
capsules in 388 of 24,000 cases (~1.6%)**, matching v3's ~1.63% — the
split-coverage CRC is the load-bearing check.

---

## 5. Gates

| Gate | Result |
|---|---|
| Zero safety violations (model + matrix + D4) | **PASS** (96,654 states + 24,000 cases, 0 violations) |
| v3 suites untouched-green | **PASS** (only additive change to a v3 file: `#[derive(Clone)]` on `SubframeError`) |
| Golden fixtures + fuzz targets committed | **PASS** |
| D4 retry tests green | **PASS** |
| Fuzz soak (>=10 min or 10M execs) | **PASS** (each target 10,000,000 execs under ASAN, 0 crashes) |
| SIGKILL | **DEFERRED** (see section 6) |
| Scan overhead < 2% | **PASS** — per-byte throughput overhead **+0.47%** (v3 0.3959 ns/byte, v4 0.3978 ns/byte; N=20,000, 64 B payloads, best of 40) |

---

### Scan overhead — a real finding

The **first** measurement was **+24.5%** per byte: the naive `decode_capsule`
heap-allocates a controls `Vec` and an events `Vec` per capsule, while v3's
scanner is allocation-free. The fix (kept in the code) splits decode into an
allocation-free `validate_capsule` fast path (all physical checks + framing
tiling, returns the `Copy` header) that the scanner runs for every accept
decision, and separate `decode_controls` / `for_each_event_type_id` helpers the
registry seam calls only when a capsule actually carries controls/events. A
no-control capsule now allocates nothing on the scan path. Re-measured:
**+0.47%** per byte — within the 2% gate. This is the spike's headline
engineering finding: v4 stays as cheap as v3 on the common path ONLY with the
no-alloc validate/ materialize split; a materialize-everything scanner does not.

## 6. SIGKILL

The existing `sigkill_harness` is wired to the v3 SegmentWriter + committer +
recover_segment path. A v4 SIGKILL scenario needs a v4 committer + durability
wiring that does not exist (v4 write is off by default; there is no v4
committer this spike). The exhaustive crash model already covers the durability
adversary at the byte layer far more thoroughly than a single SIGKILL child
(every durable-sector subset, both backgrounds, tearing), and the torn matrix
exercises the real CapsuleWriter + fdatasync + sector-reorder medium. A v4
SIGKILL scenario is a follow-up once a v4 committer exists.

---

## 7. Exact commands

```bash
just fmt
cargo nextest run --workspace
cargo test -p mess-log --test v4_golden
cargo test -p mess-log --test v4_recovery
cargo test -p mess-log --test v4_model
cargo test -p mess-store --test v4_d4_retry
cargo test -p mess-log --release --test v4_torn_matrix -- --ignored --nocapture
cargo +nightly fuzz run --fuzz-dir crates/mess-log/fuzz fuzz_v4_capsule -- -runs=10000000
cargo +nightly fuzz run --fuzz-dir crates/mess-log/fuzz fuzz_v4_control -- -runs=10000000
cargo +nightly fuzz run --fuzz-dir crates/mess-log/fuzz fuzz_v4_scan    -- -runs=10000000
# scan overhead (quiet guard: no concurrent compilers, load1 < 6):
cargo test -p mess-log --release --test v4_scan_bench -- --ignored --nocapture
```

---

## 8. Amendments to the research/09 sketch (feeds the design pack)

1. `header_crc` removed (§23.2, §4): field at offset 84 becomes MUST-BE-ZERO reserved; recovery rejects nonzero. Full CRC + A2 caps make it redundant.
2. `control_count + event_count >= 1` is a **protocol** rule, not physical: decode_capsule decodes a 0/0 capsule (physically well-formed); the scanner enforces nonempty (ScanStopV4::EmptyCapsule).
3. Redundant CONTROL_ONLY flag checked physically: decode_capsule rejects a capsule whose CONTROL_ONLY flag disagrees with event_count; the zero-stream-id control-only invariant is a scanner protocol check.
4. SnapshotInstalledV1 fixed prefix is 62 bytes (hash_presence byte + 3 reserved), then present 32-byte hashes in order (review V4). Fold version u32 (§23.4).
5. Marker echoes batch_id in addition to total_len/crc (32-byte form); recovery compares all three echoes.
6. CommitCursor = (segment_id, segment_epoch, batch_id, byte_offset, global_position); advances on every accepted capsule; the domain global position advances only by event_count. Both surfaced by RecoveryV4.
7. **No-alloc validate/materialize split is mandatory for the <2% gate**: the
   recovery scanner MUST use `validate_capsule` (allocation-free) on its hot
   path and materialize controls/events only on demand. A single
   materialize-everything decode per capsule costs ~24% over v3.
