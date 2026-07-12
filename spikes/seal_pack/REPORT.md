# Spike I — Consolidated SealPack (bn-3of)

**Verdict: ADMIT (behind `EngineOptions::seal_pack`, default OFF).**

One immutable typed-section `.seal` file per sealed segment replaces the
`.pidx` / `.filter` / `.pcol` sidecar trio. Byte-identical replay is proven
against the sidecar path; reopen opens **one** sealed file per segment instead
of three; seal wall time is **1.04x** (within the 1.10x budget); a corrupt
optional section degrades locally; and the new `EVENT_TYPE_IDS` section makes a
cold `message_type` read skip the raw-batch frame decode (cold p99 *improves*).
Production engine code — route to adversarial review before merge (riskiest
surface in section 7).

---

## 1. Format  (`crates/mess-index/src/sealed/pack.rs`)

```text
Header (64 B)  ->  SectionDirectory (n x SectionRef, 48 B)  ->  Sections  ->  Trailer (40 B)
```

- **Header**: magic `MSP1`, format_version, segment_id, base_pos, event_count,
  n_streams, n_sections, directory_off, sections_off.
- **SectionRef** (research/04 6.1, 48 B): `kind u16 | version u16 | flags u32 |
  offset u64 | length u64 | uncompressed_length u64 | crc32c u32 | codec_id u16 |
  reserved u16 | content_hash_prefix u64`. Offsets absolute.
- **Trailer**: `pack_hash [u8;32] = blake3(bytes[0 .. sections_off])` + magic —
  **header + section directory ONLY** (review F1). The directory commits every
  section's `crc32c` + `content_hash_prefix` (blake3 first 8 B), so the trailer
  hash binds the full pack identity *transitively* while a bit flip inside an
  OPTIONAL section fails only that section's own checksums at open — genuine
  local degradation (filter gone -> exact-but-slower, `EVENT_TYPE_IDS` gone ->
  raw-decode fallback). A flip in the header/directory fails the trailer hash;
  a flip in a MANDATORY section still rejects the whole pack (unresolvable
  without it). An earlier draft hashed all bytes, which made the
  local-degradation branch unreachable under real bitrot — caught in review.

### Section table

| kind | name | mandatory | codec | source |
|---|---|---|---|---|
| 1 | `STREAM_DIRECTORY` | yes | `0`=sorted / `1`=bitrank | new (chooser 12.6) |
| 2 | `POINTER_BLOCKS` | yes | 0 | reuses `ptr_block::encode_ptr_block` |
| 3 | `POINTER_SKIPS` | yes | 0 | reuses `ptr_block::encode_skips` |
| 4 | `GLOBAL_OFFSET_INDEX` | reserved | — | derivable from pointer blocks |
| 5 | `STREAM_FILTER` | optional | 0 | reuses `SegmentFilter::to_bytes` |
| 6 | `EVENT_TYPE_IDS` | optional | 0 | **new** (section 3) |
| 7 | `PAYLOAD_COLUMNS` | optional | 0 | reuses `payload::encode_payload_sidecar` |
| 8 | `ROW_FALLBACK_BLOCKS` | reserved | — | folded inside `PAYLOAD_COLUMNS` |
| 9 | `SEGMENT_EFFECT` | reserved | — | Spike D format (9.7) — kind reserved, not emitted |
| 10 | `REGISTRY_DELTA` | reserved | — | 9.5 |
| 11 | `STATS` | optional | 0 | informational |

Unknown section kinds are skipped by readers (D-FMT-3 forward compat).
Mandatory-section corruption -> whole pack rejected -> raw-scan fallback.
Optional-section corruption -> that accelerator dropped, rest of the pack serves.

**STREAM_DIRECTORY codec chooser (Spike H carry-forward #2):** at seal, `U/n`
is tested (`U = max-min+1`, `n` = distinct streams). Dense (`n>=8 and U<=8n`) ->
**bitrank** (bitvector+rank; the bit position IS the stream id, no key copy —
design 12.2 / Spike H, ranks rebuilt on open); else -> **sorted** 56-B records.
The chosen codec is stored in the SectionRef so a static representation never
affects correctness.

## 2. Install protocol (design 11.3) — install-state count

`SealDriver::seal_consolidated`: build pack -> parse back + verify -> 
`write_durable` (temp `.seal.tmp` -> **fdatasync** -> rename `.seal` -> **dir
fsync**) -> finalize footer -> install -> evict.

**The verification chain, precisely (review F3a — this deviates from a verbatim
reading of research/04 s11.3 step 2, deliberately):**

- **pointers**: `verify_pack` resolves every input batch's first + last version
  through the parsed pack and asserts the real byte offset — a direct
  end-to-end check of the pointer path;
- **payloads**: NOT re-reassembled at pack time. `encode_payload_sidecar`
  already ran the permanent verify-on-seal (every block reassembled and
  byte-compared against the raw frames before the bytes exist), and those exact
  bytes are then bound by the directory-committed section `crc32c` +
  `content_hash_prefix`, which the trailer hash covers transitively.
  Re-decompressing every block a second time measured ~8x seal cost for zero
  added coverage;
- **event-type ids**: decoded back from the parsed section and compared to the
  gathered per-event vector (cheap — 1 B/event — and catches an encoder bug at
  seal time, review F3b).

| path | durable-publish renames (crash-landable install states) |
|---|---|
| sidecar trio | **3** — `.pidx`, `.filter`, `.pcol` each temp->fsync->rename->dir-fsync |
| **SealPack** | **1** — one `.seal` temp->fdatasync->rename->dir-fsync |

3 -> 1 (<= current — **PASS**). The footer finalize is unchanged and remains the
single durable installation point (Spike C semantics, section 4).

## 3. `EVENT_TYPE_IDS` design (carry-forward #1)

Chosen: **dictionary of distinct type ids + per-event indices, index width
adaptive** (`1 B` for <=256 distinct, `2 B` <=65536, else `4 B`).

Rationale: real stores carry O(10) distinct event types over ~1M events/segment,
so a raw `u32`-per-event column spends 4 B/event repeating a handful of values.
The dictionary collapses that to **1 B/event** in the common case (4x shrink)
while keeping lookup **O(1)** (one indexed load + one dictionary load); the width
widens for pathological universes so the encoding stays exact for any input.

Wiring: `SealedSegmentIndex::event_type_id(local_idx)`; the engine's
`decode_capsule` takes a fully-from-pack fast path when the covering pack has
**both** the type-id section and `.pcol` columns for a batch, materializing the
capsule **without decoding the raw batch frames** — the carry-forward #1 win (no
sealed sidecar stored `event_type_id`, so cold `message_type` reads used to
decode the raw batch just for type ids).

## 4. Trust semantics (Spike C preserved)

`load_sealed` dual-reads: every `.seal` is parsed first (trailer hash +
mandatory-section CRC); its segment ids win over any legacy `.pidx`. Each
admitted index is gated exactly as before (Spike C review F2): **footer-verified**
(`segment_id`/`base_pos`/`end_pos` cross-check) -> installed trust-free;
**footerless** -> returned as a **pending candidate** that `recover`'s scan
installs only after proving the durable committed prefix reaches the coverage
end. Generation bumps per install (F1/F6). A torn `*.seal.tmp` husk is ignored;
a pack failing the trailer hash is skipped and the segment raw-scanned.

**Accepted deviation (this spike's review F2):** the segment footer today
cross-checks the pack by *coverage* (`segment_id`/`base_pos`/`end_pos`), not by
the pack's content hash — signed off for the default-OFF period; follow-up bone
**bn-11g** adds footer identity binding (footer names the pack hash) before the
flag ever defaults ON.

## 5. Injection matrix (research/05 13)

| injection | behavior | test |
|---|---|---|
| missing/corrupt mandatory section (CRC) | whole pack rejected -> raw scan | `hash_scope_header_directory_fatal_optional_sections_local` (b) |
| corrupt optional section — REAL bit flip, no trailer repair | that section alone dropped; reads byte-identical (local degradation) | `pack_optional_section_corruption_degrades_locally`, `hash_scope...` (c) |
| corrupt header/directory (any byte) | trailer hash rejects -> raw scan | `hash_scope...` (a), `pack_injection_matrix` (1) |
| truncated pack | rejected -> raw scan | `pack_injection_matrix` (2) |
| missing pack | raw scan | `pack_injection_matrix` (3) |
| wrong segment id/hash | filter/payload cross-check drops; footer cross-check refuses install | `from_pack`, `load_sealed` |
| unknown section kind | skipped | `pack::tests` (reserved kinds) |
| interrupted temp write (`.seal.tmp`) | ignored (extension mismatch) -> raw scan | `load_sealed` dual-read |
| footer-durable-without-pack | segment raw-scanned; no pack installed | Spike C `recover` (unchanged) |
| legacy sidecar + pack coexistence | `.seal` preferred, `.pidx` dual-read | `legacy_sidecars_and_packs_coexist` |
| byte-identical differential | pack vs sidecar identical `StoredRecord` seq | `differential_pack_vs_sidecar_is_byte_identical` |

## 6. Performance gates (Measured)

Corpus: 16 streams, 24,000 events, 32 KiB segments (**51 sealed segments**),
same session, interleaved; standalone `seal_pack_bench`. Timings taken with a
concurrent sibling workspace suite loading the machine (bounded quiet-guard
expired) — so the pack-vs-sidecar **relative** comparison is sound but absolute
us carry contention; the syscall/size numbers are load-independent.

| gate | target | measured | verdict |
|---|---|---|---|
| byte-identical replay (semantic) | hard | identical `StoredRecord` seq (differential + 188-test suite) | **PASS** |
| install states | <= current | **1** rename vs **3** | **PASS** |
| sealed-tier file opens on reopen | reduced materially | **153 -> 51** (3->1/segment, -66.7%) | **PASS** |
| seal wall time | <= 1.10x current | **1.04x** (pack 459.9 us vs sidecar 443.7 us mean/seal) | **PASS** |
| corrupt optional section degrades locally | required | REAL bit flip in filter section, no trailer repair -> section dropped, reads byte-identical (F1 hash-scope fix) | **PASS** |
| cold read latency | <= current; type-ids improve | pack p50 **20.1 us** / p99 **39.7 us** vs sidecar 20.9 / 45.2 | **PASS (improves)** |

### 6.1 Reopen open-syscall count

`strace -f -e trace=openat`, filtered to sealed-tier file opens on the 51-segment
store:

| store | sealed-tier file opens | files on disk |
|---|---|---|
| sidecar | **153** (`.pidx`+`.filter`+`.pcol` x 51) | 153 |
| SealPack | **51** (`.seal` x 51) | 51 |

3x / **66.7% fewer** sealed-tier opens. (Whole-process `openat` — 215,773 vs
214,748 — is dominated by the fjall meta-store LSM opening SSTables and is
orthogonal to the sealed tier; the filtered count above is the SealPack effect.)
The F1 hash-scope change alters only which byte range feeds blake3 — no file
layout or open-pattern change — so these counts are unaffected by the review
fixes.

### 6.2 Seal wall time (`SealMetrics::seal_duration`, per-seal)

| store | mean | p99 |
|---|---|---|
| sidecar | 443.70 us | 573.44 us |
| SealPack | 459.85 us | 737.28 us |

**1.036x mean — PASS.** (An earlier draft re-reassembled every payload in
`verify_pack` and measured ~8x; the fix relies on `encode_payload_sidecar`'s
existing verify-on-seal + the directory-committed section CRCs binding the exact
bytes — see section 2's verification-chain note and the driver `verify_pack`
doc. The F3b event-type decode-back self-check was added after this measurement;
it touches ~1 B/event and is noise at this scale.)

### 6.3 Cold read latency (20,000 sealed point reads)

| store | p50 | p99 | max |
|---|---|---|---|
| sidecar | 20.93 us | 45.23 us | 95.60 us |
| SealPack | 20.11 us | 39.69 us | 107.96 us |

Pack p50 -4%, p99 -12% — the `EVENT_TYPE_IDS` fast path skips the raw-frame
decode. No worse; measurably better at the tail.

### 6.4 Size

Sealed dir bytes: sidecar 254,455 vs SealPack 294,550 (**1.16x**). The pack is
larger *by design*: it adds the `EVENT_TYPE_IDS` column (~1 B/event ~= 24 KB over
51 segments) plus per-section/trailer framing — the deliberate space-for-cold-
reads trade that eliminates the raw type-id decode. Pointer/filter/payload bytes
are byte-for-byte the reused sidecar codecs.

## 7. Riskiest part of the diff (for the reviewer)

`decode_capsule`'s fully-from-pack fast path (`crates/mess-store/src/engine.rs`):
it materializes a `DecodedBatch`'s type ids from `EVENT_TYPE_IDS` and payload
from `.pcol` **without decoding the raw batch frames**. Correctness rests on
(a) the batch already being CRC-validated by `accepted_batch_at` upstream, and
(b) the pack's verify-on-seal + directory-committed section CRCs guaranteeing
the section + columns equal the raw frames. Both hold, but this is the one place
the raw-frame identity check is bypassed on a hot read path — the reviewer
confirmed the upstream CRC ordering and the coverage-boundary fall-through (an
on-demand `seal_active` of a still-growing head takes the raw path).

Review outcome: FIX-FIRST verdict addressed — F1 (trailer hash scope narrowed to
header+directory so optional-section corruption genuinely degrades locally; test
rewritten as a real bitrot injection), F2 (coverage-only footer trust accepted
for the default-OFF period; bn-11g binds footer->pack-hash before default-ON),
F3 (verification chain documented accurately in section 2; event-type decode-back
self-check added to `verify_pack`).

## 8. Exact commands

```bash
# full suite (flag OFF is the default; flag ON is the engine_seal_pack target)
CLANG_PATH=/usr/bin/clang cargo test -p mess-index -p mess-store --release

# targeted sealed subset (fast iteration)
CLANG_PATH=/usr/bin/clang cargo nextest run -p mess-index -p mess-store \
  --test engine_seal_pack --test engine_block_reads --test engine_reopen \
  --test engine_reopen_cycles --test engine_roll --test parity_seal \
  --test sealed_read_paths --test sealed_handoff --test sealed_scale \
  --test rebuild --lib

# bench (standalone spike crate)
cd spikes/seal_pack && CLANG_PATH=/usr/bin/clang cargo build --release
D=$(mktemp -d)
./target/release/seal_pack_bench seed sidecar $D/side 24000
./target/release/seal_pack_bench seed pack    $D/pack 24000
strace -f -e trace=openat ./target/release/seal_pack_bench reopen sidecar $D/side
strace -f -e trace=openat ./target/release/seal_pack_bench reopen pack    $D/pack
./target/release/seal_pack_bench coldread sidecar $D/side 20000
./target/release/seal_pack_bench coldread pack    $D/pack 20000
```
