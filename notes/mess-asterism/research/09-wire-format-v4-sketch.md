# Research 09: v4 commit-capsule wire-format sketch

> **Product-admission note (2026-07-14):** ADR 0002 does not itself admit
> `DedupeKeyV1`, `SnapshotInstalledV1`, or `ProjectionCheckpointV1` as product
> capabilities. Their proven codecs remain format research. Snapshot discovery
> uses a discardable sidecar; `bn-11mk` and `bn-2ctq` own the optional
> projection and exact batch-idempotency decisions. V4 itself remains subject
> to the separate `bn-1ojm` gate.

**Status:** non-normative spike specification.  
**Purpose:** make the control-prelude idea concrete enough for a byte-compatible prototype and crash model.  
**Rule:** this document does not supersede v3 `docs/spec/01-log-format.md` until the exhaustive and randomized gates pass.

## 1. Design requirements

V4 must:

1. preserve all v3 A1–A12 guarantees;
2. keep one-stream domain event batches;
3. atomically commit engine control records and domain events;
4. permit control-only capsules without consuming domain global positions;
5. make control-only ordering unambiguous under stale/reordered sectors;
6. let a capsule introduce registry IDs and use them in its event region;
7. retain cheap sequential scan and exact length tiling;
8. allow old segments and new segments in one store;
9. reject unknown flags/versions rather than guessing;
10. keep the full-capsule CRC mandatory and split around checksum fields.

## 2. Segment versioning

A v4 segment uses the existing segment-header family with:

```text
format_version = 4
```

Other segment-header fields retain their v3 meaning:

```text
segment_id
base_pos
epoch
created time
prev_segment_epoch
header CRC
```

V3 and v4 capsules never coexist in one segment. Recovery dispatches by segment version.

## 3. Capsule layout

```text
CapsuleHeader                   96 bytes
[CryptoChainEntry]              32 bytes when flag set
ControlRecord × control_count   variable, total control_len
EventSubframe × event_count     variable, total event_region_len
CommitMarker                    24 bytes
```

The exact 96-byte size is a spike choice, not yet frozen.

## 4. CapsuleHeader

All integers are little-endian. No implicit alignment padding exists.

| offset | size | type | field | rule |
|---:|---:|---|---|---|
| 0 | 4 | u32 | `magic` | new v4 capsule magic, e.g. `0xCA95_4EAD` |
| 4 | 2 | u16 | `format_version` | `4` |
| 6 | 2 | u16 | `flags` | known bits only; unknown bit rejects capsule |
| 8 | 4 | u32 | `event_count` | may be zero only when `control_count > 0` |
| 12 | 4 | u32 | `control_count` | number of TLV control records |
| 16 | 8 | u64 | `batch_id` | per-segment contiguous ID; **recovery-significant in v4** |
| 24 | 8 | u64 | `total_len` | header through marker, all bytes |
| 32 | 8 | u64 | `first_global_pos` | expected domain-event position; unchanged by control-only capsule |
| 40 | 8 | u64 | `segment_epoch` | must equal containing segment epoch |
| 48 | 8 | u64 | `stream_id` | domain stream; zero for control-only capsule |
| 56 | 8 | u64 | `category_id` | domain category; zero for control-only capsule |
| 64 | 8 | u64 | `first_stream_version` | prior event count for domain stream; zero for control-only capsule |
| 72 | 4 | u32 | `control_len` | exact bytes occupied by all control TLVs |
| 76 | 4 | u32 | `event_region_len` | exact bytes occupied by subframes + payloads |
| 80 | 4 | u32 | `capsule_crc` | CRC32C under split coverage; excluded from itself |
| 84 | 4 | u32 | `header_crc` | optional early-reject CRC over header excluding both CRC fields; see decision below |
| 88 | 4 | u32 | `logical_flags` | engine semantics; all unknown bits reject |
| 92 | 4 | u32 | `reserved` | must be zero |

### Decision to spike: keep or remove `header_crc`

The full capsule CRC is authoritative and mandatory. A header CRC can reject corrupt lengths before allocating/reading a large claimed capsule, but v3 already uses sanity caps and full verification. The spike should compare:

```text
A. header CRC + full CRC
B. no header CRC; checked fields + total_len cap + full CRC
```

If header CRC adds complexity without measurable recovery safety/performance value, reuse those four bytes as reserved.

## 5. Flags

`flags` physical-layout bits:

| bit | name | meaning |
|---:|---|---|
| 0 | `CRYPTO_CHAIN` | 32-byte chain entry follows header |
| 1 | `CONTROL_COMPRESSED` | control region uses a frozen compression framing; **probably reject for v4** |
| 2–15 | reserved | zero |

Recommendation: keep control records uncompressed. They are small, bootstrap-critical, and must be parseable without registry dictionaries.

`logical_flags`:

| bit | name | meaning |
|---:|---|---|
| 0 | `CONTROL_ONLY` | `event_count == 0`; redundant cross-check |
| 1 | `HAS_DEDUPE` | exactly one dedupe control exists; redundant fast hint |
| 2 | `REGISTRY_INTRODUCES_IDS` | control records introduce IDs used by this capsule |
| 3–31 | reserved | zero |

Redundant flags are accepted only when they agree with parsed content. They are hints, never substitutes for tiling/count checks.

## 6. Header invariants

For every capsule:

```text
control_count + event_count >= 1
control_len <= MAX_CONTROL_LEN
control_len + event_region_len + fixed/optional bytes == total_len
batch_id == expected_batch_id
first_global_pos == expected_global_pos
segment_epoch == segment.epoch
total_len within [MIN, MAX]
capsule does not cross segment limit
```

If `event_count == 0`:

```text
stream_id == 0
category_id == 0
first_stream_version == 0
CONTROL_ONLY flag set
```

If `event_count > 0`:

```text
stream_id != 0, except a deliberately reserved system-event policy
first_stream_version matches reconstructed stream prior count
CONTROL_ONLY flag clear
```

After acceptance:

```text
expected_batch_id += 1
expected_global_pos += event_count
```

## 7. ControlRecord framing

Each control record is:

| offset | size | type | field |
|---:|---:|---|---|
| 0 | 2 | u16 | `kind` |
| 2 | 2 | u16 | `version` |
| 4 | 4 | u32 | `payload_len` |
| 8 | `payload_len` | bytes | canonical payload |

No padding. `8 + payload_len` bytes per record. The `control_count` records must tile exactly `control_len` bytes. All arithmetic is checked before slicing/allocation.

A decoder behavior for unknown `(kind, version)` must be explicit:

- unknown **critical** control: reject/stop; the state cannot be interpreted;
- future advisory controls would need a criticality bit in `kind` or flags, but skipping state-changing records is dangerous.

Recommendation: all v4 controls are critical. A newer control kind requires a newer segment/capsule version or a declared extension mechanism with proven skip safety.

## 8. Frozen control primitive encodings

Use fixed-width little-endian integers and explicit length-prefixed bytes:

```text
Bytes16: u16 length + bytes       # names, bounded small identifiers
Bytes32: u32 length + bytes       # dedupe keys, dictionary/blob metadata
Hash256: [u8; 32]
```

Every length has a format cap lower than its integer maximum. UTF-8 validation and name canonicalization rules match the registry specification.

## 9. Initial control kinds

Suggested IDs:

| kind | name | purpose |
|---:|---|---|
| `0x0001` | `StreamRegisteredV1` | assign dense stream ID and canonical name/category |
| `0x0002` | `EventTypeRegisteredV1` | assign type ID, name, codec/schema metadata |
| `0x0003` | `CategoryRegisteredV1` | assign category ID/name |
| `0x0004` | `NameAliasedV1` | immutable alias assignment |
| `0x0005` | `DictRegisteredV1` | register immutable dictionary object/reference |
| `0x0010` | `DedupeKeyV1` | exact key covering this capsule |
| `0x0020` | `SnapshotInstalledV1` | advance snapshot head to durable blob |
| `0x0021` | `SnapshotInvalidatedV1` | mark a snapshot unusable without deleting history |
| `0x0030` | `ProjectionCheckpointV1` | advance a position/frontier |
| `0x0040` | `RegistryImportChunkV1` | migration chunk |
| `0x0041` | `RegistryImportManifestV1` | commit complete migration import |
| `0x0050` | `DedupeWindowImportedV1` | migration seed for live keys |
| `0x0060` | `MigrationPhaseV1` | monotone migration state marker |

The spike can implement only registry, dedupe, and checkpoint controls first.

## 10. `StreamRegisteredV1`

Payload:

| offset | field | encoding |
|---:|---|---|
| 0 | `stream_id` | u64 |
| 8 | `category_id` | u64 |
| 16 | `name_len` | u16 |
| 18 | `name` | UTF-8 bytes |

Rules:

```text
stream_id == next_stream_id in speculative/recovery registry
name not already mapped to a different ID
stream_id not already mapped to different bytes
category_id resolves either before capsule or via earlier control in same capsule
```

Controls are applied in ordinal order, so a capsule may register a category, then a stream, then use the stream in the event region.

## 11. `EventTypeRegisteredV1`

Payload:

```text
event_type_id: u32
codec_id: u16
current_schema_version: u16
schema_fingerprint: Hash256
name: Bytes16
```

Rules mirror stream registration. The event subframe may reference the new type after control application.

## 12. `DedupeKeyV1`

Payload:

```text
scope_kind: u8             # stream or global
reserved: [u8; 3]
scope_id: u64              # stream ID or 0/global namespace
key_len: u32
key: [u8; key_len]
```

Rules:

- at most one dedupe key per user capsule in v4;
- a control-only capsule does not use `DedupeKeyV1` unless a future operation defines semantics;
- the key covers the full user capsule, not individual subframes;
- exact key bytes are canonical for dedupe rebuild;
- append validation checks the configured window before writing the capsule.

A 128-bit fingerprint is computed in memory; it is not stored as authority, though storing it as a redundant acceleration hint is possible if the reader verifies it.

## 13. `SnapshotInstalledV1`

Payload:

```text
stream_id: u64
covered_version: u64
covered_global_position: u64
snapshot_slot: u64
pack_id: u64
pack_offset: u64
blob_len: u32
codec_id: u16
fold_version: u16 or u32 (settle width)
state_hash: Hash256
event_prefix_hash: Hash256
blob_hash: Hash256
```

Rules:

- blob record exists and is durable under the chosen policy before capsule submission;
- covered version is committed;
- fold/prefix metadata validate before publish;
- snapshot head advances monotonically by covered version;
- older/lower install controls remain in history but do not move the head backward.

## 14. `ProjectionCheckpointV1`

Single-node payload:

```text
projection_id: u64
position: u64
state_ref_kind: u16
reserved: u16
state_ref_len: u32
state_ref: bytes
```

Future frontier form should use a new control version rather than overloading the bytes ambiguously:

```text
projection_id
pair_count
(shard_id, position)* sorted by shard
```

Checkpoint controls may be Process/Group durable depending on API; their visibility follows the capsule’s durability mode.

## 15. EventSubframe

V4 may initially reuse the v3 28-byte subframe unchanged:

```text
event_type_id u32
schema_version u16
codec_id u16
compression_id u8
flags u8
dict_id u16
uncompressed_len u32
compressed_len u32
metadata_len u32
data_len u32
payload bytes
```

This reduces migration surface. Any later optimization—such as per-capsule type/codec dictionaries or shorter length encodings—should be a separate measured v5 decision.

## 16. Crypto chain placement

Retain the existing per-capsule 32-byte chain entry immediately after the header when enabled. Chain semantics incorporate control bytes as part of the capsule/frame hash so a registry/dedupe/control tamper is detected in verified streams.

Questions for the fold-certificate spec:

- Does a control-only capsule affect a stream fold chain? Usually no, because it has no domain stream.
- Does the global audit chain cover all capsules, including controls? A separate optional global capsule chain may be useful.
- A registration control in the same user capsule must be included in the capsule hash even if the stream fold step hashes only event frames.

The simplest separation:

```text
mandatory CRC: all bytes, recovery
optional capsule audit hash: all control+event bytes
a per-stream fold chain: event frames only, seeded by stream ID
```

## 17. CommitMarker

Suggested 24-byte marker:

| offset | size | field |
|---:|---:|---|
| 0 | 4 | marker magic |
| 4 | 4 | reserved/flags |
| 8 | 8 | `batch_id_echo` |
| 16 | 4 | `total_len_echo` (u32 if MAX capsule <4GiB) or rearrange for u64 |
| 20 | 4 | `capsule_crc_echo` |

A safer fully 64-bit-length layout is 32 bytes:

```text
magic u32
flags u32
batch_id_echo u64
total_len_echo u64
capsule_crc_echo u32
marker_crc/reserved u32
```

Recommendation: use the 32-byte marker unless the four-byte saving is measured important. Explicit echoes simplify scan diagnostics and bind zero-event ordering.

## 18. CRC coverage

Use the v3 split-coverage technique so verification does not copy/zero the capsule:

```text
CRC over header before capsule_crc field
skip capsule_crc field
CRC over header after capsule_crc
CRC over optional chain
CRC over control region
CRC over event region
CRC over marker before crc_echo
skip crc_echo
CRC over remaining marker bytes
```

If `header_crc` exists, exclude it from the full CRC or define nested coverage precisely. Golden fixtures must encode the exact byte ranges.

## 19. Maximums

Suggested spike caps:

```text
MAX_CAPSULE_LEN      64 MiB (retain v3)
MAX_CONTROL_LEN       1 MiB
MAX_CONTROL_COUNT     4096
MAX_EVENT_COUNT       implementation/API cap within total length
MAX_NAME_LEN          65,535 bytes, lower operational recommendation
MAX_DEDUPE_KEY_LEN     1 MiB, configurable lower application cap
```

Recovery validates caps before allocation. A control-only capsule still has a nontrivial minimum length and cannot be all-zero data.

## 20. Recovery pseudo-code

```rust
fn scan_v4_segment(bytes: &[u8], seed: ScanSeed) -> ScanResult {
    let mut off = SEGMENT_HEADER_LEN;
    let mut expected_batch = 0u64;
    let mut expected_global = seed.base_pos;
    let mut registry = seed.registry_view;
    let mut semantic_heads = seed.head_boundary_view;

    loop {
        let h = parse_bounded_header(bytes, off)?;
        require(h.version == 4);
        require(h.segment_epoch == seed.epoch);
        require(h.batch_id == expected_batch);
        require(h.first_global_pos == expected_global);
        require(valid_counts_and_lengths(h));
        let capsule = slice_exact(bytes, off, h.total_len)?;
        require(marker_echoes_match(capsule, h));
        require(full_crc_matches(capsule, h));
        let controls = parse_control_tiling(capsule, h)?;
        let events = parse_event_tiling(capsule, h)?;

        let speculative_registry = registry.apply_in_order(controls.registry_records())?;
        validate_controls(controls, &speculative_registry, semantic_heads)?;
        validate_event_ids(events, &speculative_registry)?;
        validate_stream_transition(h, semantic_heads)?;

        accept(capsule);
        registry = speculative_registry;
        semantic_heads.apply(h.stream_id, h.first_stream_version, h.event_count)?;
        expected_batch += 1;
        expected_global += h.event_count as u64;
        off += h.total_len as usize;
    }
}
```

Physical scan and semantic validation may be staged for speed, but an accepted effect/checkpoint cannot ignore semantic continuity.

## 21. Global and commit cursors

Domain APIs keep:

```text
GlobalCursor(global_position)
```

Internal replication/recovery/checkpoint uses:

```rust
struct CommitCursor {
    segment_id: u64,
    segment_epoch: u64,
    batch_id: u64,
    byte_offset: u64,
    global_position: u64,
}
```

A control-only capsule advances `CommitCursor` but not `GlobalCursor`. User subscriptions skip controls. Administrative/control replication can subscribe by commit cursor.

## 22. Compatibility

- v3 reader knows only v3 segments and must refuse a directory containing v4 in write mode.
- v4 reader handles both versions.
- v3 segments obtain registry meaning from the canonical import made at the v4 boundary.
- no v3 file is edited to claim v4.
- a segment footer records matching format version and effect/SealPack references.

## 23. Open decisions before normativity

1. 88/92/96-byte header final size.
2. Whether header CRC earns its bytes.
3. 24 vs 32-byte marker.
4. Fold-version width.
5. Whether checkpoint/snapshot controls belong in the main capsule log or a separate control lane; this design recommends main log.
6. Exact control-kind evolution policy.
7. Whether one capsule may carry both snapshot/checkpoint controls and domain events; recommendation: yes only when semantics require atomicity, otherwise separate control capsule.
8. Whether global audit chain covers control-only capsules.
9. Control payload canonical string normalization.
10. Maximum dedupe key and registry import chunk sizes.

## 24. Admission criteria

The v4 format becomes normative only after:

```text
byte-exact encoder/decoder fixtures
Kani/fuzz length arithmetic
small-state exhaustive crash model
>=24k sector-reordering cases with zero split states
mixed v3/v4 recovery tests
unknown flag/version rejection tests
registry-first same-capsule decode tests
control-only cursor tests
full-scan vs SegmentEffect digest equivalence
measured scan overhead <2% for ordinary no-control capsules
```

The format’s novelty is useful only if the crash story remains as boring and absolute as v3.
