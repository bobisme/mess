# 01 — Log format: batches, subframes, segments

Status: **normative**. This document specifies the on-disk byte format of the
mess event log: segment files, the batch framing that is the sole commit
authority (D1), the subframe layout, and the checksum coverage that makes
recovery safe under crash and block-write reordering.

An implementation that produces bytes matching the tables here is
byte-for-byte interoperable with any other conforming implementation. The
recovery algorithm that consumes these bytes is specified in
[02-recovery.md](02-recovery.md); durability modes and fsync obligations in
[03-durability.md](03-durability.md); the registry that assigns the interned
IDs referenced here (`stream_id`, `category_id`, `event_type_id`, `codec_id`,
`dict_id`) in [04-registry.md](04-registry.md); the optional per-batch
cryptographic fold-chain value in [05-fold-certificates.md](05-fold-certificates.md).

Key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**,
**MAY**, and **OPTIONAL** are to be interpreted as in RFC 2119 / RFC 8174.

All multi-byte integers are **little-endian**. There is **no implicit
alignment padding**: every field begins at the byte offset given, and every
structure is exactly as long as the sum of its fields. Sizes are in bytes.

---

## 1. Design foundations (normative context)

### 1.1 The log is the only commit authority (D1)

> **D1.** The event log, with commit markers, is the sole commit authority.
> A frame is *committed* if and only if it lies inside a durable,
> marker-terminated batch in the log. Indexes are rebuildable caches; the
> manifest is advisory metadata. Neither is truth.

Everything in this document exists to make "is this batch committed?"
answerable from the bytes alone, under adversarial crash conditions.

### 1.2 The append unit is a batch, not a frame (D2)

The smallest thing that is ever committed or recovered is a **batch**: a
`BatchHeader`, one or more `EventSubframe`s, and a terminating
`CommitMarker`. A batch is atomic — all of its events commit together or none
do. There is no per-frame commit boundary.

### 1.3 Fault model and conformance bar

The format and the recovery rules that reference it were validated against an
ALICE-style sector-reordering fault model (`spikes/torn_write`, **24,000
randomized crash cases** at 512 B and 4 KiB sector sizes over zeroed, garbage,
and stale-prior-generation backgrounds, plus 11 deterministic adversarial
cases; and an exhaustive state-model check over 1.83M states,
`docs/spec/formal-model-commit-recovery.md`). The model:

- Decomposes each write into **sectors** that persist independently and in
  **arbitrary order**. The `CommitMarker`'s sector MAY reach disk *before* the
  frame sectors it vouches for.
- Treats `fsync`/`fdatasync` as a true barrier (no volatile-cache or FUA
  lies): after a successful sync, all previously written bytes are durable.
- On crash, applies an arbitrary subset of un-synced pending sectors to the
  previously durable image, optionally tearing one sector at an arbitrary byte
  cut. Freed/recycled disk regions are **not** zeroed — they may hold a stale
  prior generation of the log.

**This is the conformance bar.** A conforming writer + recovery pair MUST lose
no acknowledged batch and MUST accept no partial, torn, stale, reordered-hole,
or otherwise corrupt batch under any outcome this model can produce. The
measured guarantee (0 acked lost, 0 corrupt accepted across 24,000 cases) is
achievable only with the full checksum discipline below; §5 records exactly
which check is load-bearing and why none MAY be dropped.

---

## 2. Choice of normative dialect

Two format dialects exist in the spike code, and they disagree. This section
records which one is normative and why.

> ### Decision D-FMT-1 — normative dialect: the v2 lineage, extended
>
> **Chosen:** the `spikes/vertical_slice` / `spikes/recovery_scale` **v2
> dialect** (batch-constant fields hoisted into the `BatchHeader`, 256 MiB
> segment files, sealed-segment footers), extended with (a) the **mandatory
> A9 segment epoch** — which no spike header actually carried — and (b) the
> **full D2 subframe field set** (`schema_version`, `codec_id`,
> `compression_id`, `dict_id`, and the four length fields), which the spikes
> stubbed to a bare `event_type_id`/`data_len`.
>
> **Rejected — the v1 dialect** (`spikes/crash_log`, `spikes/torn_write`;
> 38-byte header, no hoisted stream fields). It validated the framing and all
> of A1–A12 empirically, but D2 explicitly calls for "stream_id / category
> refs (batch-constant fields hoisted here)", which v1 lacks. Adopting v1
> would force per-subframe repetition of batch-constant data and provide no
> stream/category context to a recovery/replay scan.
>
> **Consequence.** The normative `BatchHeader` (§4.2) is neither spike's
> header verbatim: it is the v2 header **plus** a `segment_epoch` field (A9,
> upgraded from "defense in depth" to REQUIRED by `spikes/torn_write`) and a
> `flags` field. The normative `format_version` is therefore **3**, distinct
> from the spikes' v1 (`1`) and v2 (`2`); the on-disk magic numbers are
> unchanged so tooling can still recognize the family. The subframe (§4.3) is
> the full D2 field set, not the spike stub.

---

## 3. Segment files

The log is a sequence of **segment files**. Each segment is an independent,
self-describing append-only file.

```text
segment file
  ├─ SegmentHeader        (fixed, offset 0)              §3.2
  ├─ Batch                (BatchHeader..CommitMarker)    §4
  ├─ Batch
  ├─ …
  └─ SegmentFooter        (present iff sealed)           §3.3
       ├─ ExtensionRegion (variable, MAY be empty)       §3.3.2
       └─ Trailer         (fixed, occupies file tail)    §3.3.1
```

A **sealed** segment ends in a `SegmentFooter` made of a variable-length
**extension region** followed by a **fixed trailer** at end-of-file (§3.3).
The trailer's fixed size and its position at EOF preserve the
`pread`-exactly-N-bytes-from-EOF fast path (R2, §02); the extension region
carries the durable per-stream fold anchors (§3.3.2).

### 3.1 Segment rules

- **Segment size is 256 MiB** (`SEGMENT_SIZE = 256 * 1024 * 1024`). This value
  is fixed by measurement (`spikes/recovery_scale`, 10 GiB per segment-size
  configuration): full recovery time depends only on total log size
  (~1.1 s/GiB cold), so a smaller segment buys nothing; the last-segment-only
  fast path depends only on the active segment size (256 MiB → 0.32 s, flat as
  the log grows). A batch MUST NOT be placed such that it would extend beyond
  `SEGMENT_SIZE`.

- **A8 (batches never span segments).** A batch MUST lie entirely within one
  segment file — from the first byte of its `BatchHeader` to the last byte of
  its `CommitMarker`. When the next batch would not fit before `SEGMENT_SIZE`,
  the writer MUST seal the current segment (§3.3) and open a new one. Segment
  boundaries therefore always coincide with batch boundaries.

- **A11 (no sector alignment).** Neither the `BatchHeader` nor any other
  structure is required to begin on a sector boundary, and an implementation
  MUST NOT introduce alignment padding or trust a structure merely because it
  happens to be sector-atomic. Safety under a torn or straddled header comes
  from the A2 length cap (§4.2) and the A4 batch checksum (§5), never from
  layout. This rule exists to preempt alignment "optimizations" that would
  weaken acceptance.

### 3.2 SegmentHeader

Written once, durably, at offset 0 before any batch is appended to the
segment. Carries the two seeds a scan of this segment needs without consulting
any other file: the base global position (A1 seed, §02) and the segment
**epoch** (A9).

| Offset | Size | Type | Name | Description |
|---|---|---|---|---|
| 0  | 4 | u32 | `magic` | `0x5E60_1EAD`. Identifies a SegmentHeader. |
| 4  | 2 | u16 | `format_version` | `3`. A scan MUST stop on any other value. |
| 6  | 2 | u16 | `flags` | Reserved; MUST be written `0`, MUST be ignored on read in v3. |
| 8  | 8 | u64 | `segment_id` | Monotonic, assigned by the single writer; never reused. |
| 16 | 8 | u64 | `base_pos` | Global position of the first event in the segment's first batch. The A1 contiguity seed (§02). |
| 24 | 8 | u64 | `epoch` | Segment generation (A9). Strictly monotonic across the whole log; a fresh or recycled segment file MUST be stamped with a new, larger epoch than any previously durable segment. |
| 32 | 8 | u64 | `created_unix_nanos` | Wall-clock creation time. Advisory. |
| 40 | 8 | u64 | `prev_segment_epoch` | Epoch of the immediately preceding segment (the anchor for A9 cross-checks in §02), or `0` for the first segment. Advisory but SHOULD be populated. |
| 48 | 4 | u32 | `header_crc` | CRC32C over bytes `[0, 48)` of this header. |

`SEGMENT_HEADER_LEN = 52`. The first `BatchHeader` in a segment begins at
offset `52`.

> **Decision D-FMT-2 — `base_pos` lives in the header, not the filename.**
> The spikes encoded `base_pos` in the segment filename (`seg-000001-…`) and
> flagged it as a shortcut. This spec puts it in the durable header (and
> footer, §3.3): filenames are not a durable, checksummed medium, and A1
> recovery MUST NOT trust an unchecksummed seed. Filenames MAY still encode
> `segment_id`/`base_pos` for human convenience but are never authoritative.

### 3.3 SegmentFooter (trailer + extension region)

Written when a segment is **sealed** (rolled off the active head, §3.1). Its
presence marks a segment as complete; a segment with no valid trailer is
either the active segment or was interrupted mid-seal and MUST be fully
scanned. The footer has two parts, laid out in file order:

```text
… last Batch's CommitMarker
ExtensionRegion   (variable length ext_len ≥ 0)   §3.3.2
Trailer           (fixed SEGMENT_TRAILER_LEN)     §3.3.1   ← occupies the file tail
```

**What is advisory and what is durable.** For *batch acceptance* the whole
footer is advisory (D1): full recovery still scans and CRC-validates every
batch and cross-checks the trailer's counts against the scan (§02). A footer
is never a commit authority. But the **extension region is part of the durable
seal**, not an advisory cache: it carries the per-stream fold-chain anchors
(`StreamHeadTable`, `SnapshotAnchor` list) that the fold certificate of
[05-fold-certificates.md §5/§8](05-fold-certificates.md) depends on and that
are **not** rebuilt from the rebuildable index. It MUST be written and made
durable as part of the seal (§6), inside the same seal `fsync` as the trailer.
The extension is advisory-skippable only at the *section* granularity (unknown
section kinds, §3.3.2) — never as a whole.

#### 3.3.1 Trailer (fixed, at end-of-file)

The trailer is fixed-length and occupies the final `SEGMENT_TRAILER_LEN` bytes
of the sealed file, so a fast-path reader `pread`s exactly these bytes from EOF
without first knowing the segment's length (R2, §02). It carries the
segment-catalog summary (counts, epoch, positions) needed by the fast path
*without* touching the extension region, plus the `ext_offset`/`ext_len`/
`ext_crc` locator for the extension.

| Offset | Size | Type | Name | Description |
|---|---|---|---|---|
| 0  | 4 | u32 | `magic` | `0x5EA1_F007`. Identifies a SegmentFooter trailer. |
| 4  | 2 | u16 | `format_version` | `3`. |
| 6  | 2 | u16 | `flags` | Reserved; MUST be `0` in v3. |
| 8  | 8 | u64 | `segment_id` | MUST equal the SegmentHeader's `segment_id`. |
| 16 | 8 | u64 | `epoch` | **R3: the trailer carries the A9 epoch.** MUST equal the SegmentHeader's `epoch`. |
| 24 | 8 | u64 | `base_pos` | MUST equal the SegmentHeader's `base_pos`. |
| 32 | 8 | u64 | `batch_count` | Number of accepted batches in the segment. |
| 40 | 8 | u64 | `event_count` | Total events (sum of `frame_count`) in the segment. |
| 48 | 8 | u64 | `end_pos` | `base_pos + event_count`. The `base_pos` the next segment MUST use — the A1 seed handed across the segment boundary. |
| 56 | 8 | u64 | `sealed_len` | Byte offset at which this trailer begins (`= ext_offset + ext_len`). Equivalently, total footer-preceding-plus-extension content length. |
| 64 | 8 | u64 | `ext_offset` | Byte offset where the extension region begins — the first byte after the last batch's `CommitMarker`. Equals the total length of batch content `[0, ext_offset)`. |
| 72 | 8 | u64 | `ext_len` | Byte length of the extension region (§3.3.2). **`0` is legal** (e.g. a chain-disabled segment with no fold anchors); then `ext_offset == sealed_len`. |
| 80 | 4 | u32 | `ext_crc` | CRC32C over the extension region bytes `[ext_offset, ext_offset + ext_len)`. **MUST be `0` when `ext_len == 0`.** Covers the whole extension; validated by §02 §8.3. |
| 84 | 2 | u16 | `repair_sidecar_kind` | **OPTIONAL forward-compat.** `0` = none. Nonzero identifies an at-rest repair sidecar (see D-FMT-3). |
| 86 | 2 | u16 | `reserved` | MUST be `0` in v3. |
| 88 | 8 | u64 | `repair_sidecar_ref` | Opaque reference to the sidecar (e.g. a blob id or byte offset); `0` if `repair_sidecar_kind == 0`. |
| 96 | 4 | u32 | `footer_crc` | CRC32C over bytes `[0, 96)` of this trailer. |

`SEGMENT_TRAILER_LEN = 100`. The trailer occupies the final 100 bytes of a
sealed segment file; a fast-path reader `pread`s exactly these 100 bytes from
EOF (§02, R2). The `footer_crc` covers only the trailer; the extension region
is covered separately by `ext_crc` so the fast path can validate and use the
trailer without reading the extension.

#### 3.3.2 Extension region (typed sections)

The extension region is a sequence of **typed sections**, laid out back-to-back
starting at `ext_offset` and exactly filling `ext_len` bytes (it ends where the
trailer begins). Each section is a fixed 16-byte section header followed by
`payload_len` payload bytes:

**Section header (16 bytes):**

| Offset | Size | Type | Name | Description |
|---|---|---|---|---|
| 0  | 2 | u16 | `kind` | Section kind (see below). `0` is reserved and MUST NOT be written. |
| 2  | 2 | u16 | `section_flags` | Reserved; MUST be `0` in v3. |
| 4  | 4 | u32 | `entry_count` | Number of fixed-size entries in the payload (section-kind-specific). |
| 8  | 8 | u64 | `payload_len` | Byte length of the payload that immediately follows this header. The next section (if any) begins at `payload_len` bytes past the end of this header. |

**Forward-compat / advisory-skip (same rule as the repair sidecar, D-FMT-3).**
A reader walks sections by hopping `16 + payload_len` bytes each. A section
whose `kind` the reader does not understand MUST be **skipped** by its
`payload_len` — it changes nothing about batch acceptance (decided solely by
the per-batch checksums, §5) and nothing about the trailer's segment-catalog
fields. A reader MUST NOT stop or fail on an unknown `kind`. Because sections
are self-delimiting and the whole region is covered by `ext_crc`, new section
kinds are added without a format break.

**Known section kinds (v3):**

| `kind` | Name | Entry size | Payload |
|---|---|---|---|
| `1` | `StreamHeadTable` | 48 B | `entry_count` `StreamHeadEntry` records |
| `2` | `SnapshotAnchorList` | 48 B | `entry_count` `SnapshotAnchor` records |

At most one section of each known kind SHOULD appear in a segment's extension.
For both known kinds `payload_len == entry_count * 48`.

**`StreamHeadEntry` (48 bytes)** — one per stream with ≥1 committed event in
this segment; the durable Tier-1 head anchor consumed by
[05-fold-certificates.md §5](05-fold-certificates.md) (G6):

| Offset | Size | Type | Name | Description |
|---|---|---|---|---|
| 0  | 8  | u64      | `stream_id`    | Interned stream id (`04-registry.md`). |
| 8  | 8  | u64      | `last_version` | Stream version of the stream's **last** event in this segment. |
| 16 | 32 | [u8; 32] | `head_hash`    | `h[last_version]` — the fold-chain value after that event. |

**`SnapshotAnchor` (48 bytes)** — one per retention certificate recorded at
seal, the Path-C input of
[05-fold-certificates.md §7.1/§8.2](05-fold-certificates.md):

| Offset | Size | Type | Name | Description |
|---|---|---|---|---|
| 0  | 8  | u64      | `stream_id`  | Interned stream id (`04-registry.md`). |
| 8  | 8  | u64      | `version`    | The snapshot's `stream_version` `v` (0-based last-index). |
| 16 | 32 | [u8; 32] | `chain_hash` | `h[v]` — the fold-chain value certifying the prefix `0..=v`. |

This document owns the byte layout of these sections; their **semantics** (how
`head_hash`/`chain_hash` are computed and verified) are owned by
[05-fold-certificates.md](05-fold-certificates.md); their **validation** on
recovery (verifying `ext_crc`, behaviour on a corrupt extension) is owned by
[02-recovery.md §8.3](02-recovery.md).

> ### Decision D-FMT-3 — the OPTIONAL repair-sidecar reference
>
> `repair_sidecar_kind` / `repair_sidecar_ref` reserve space for a future
> at-rest repair mechanism (a Reed–Solomon parity sidecar, tracked as
> bn-2za) **without a format break**. In v3 a conforming writer MUST write
> `repair_sidecar_kind = 0`. A conforming reader MUST treat any *nonzero*
> `repair_sidecar_kind` it does not understand as **advisory and skip it** —
> it changes nothing about batch acceptance, which is decided solely by the
> per-batch checksums (§5). Because the fields already exist and are covered
> by `footer_crc`, adding parity later is a value change, never a layout
> change. The sidecar itself (parity file layout, repair procedure) is out of
> scope for this document. This reservation is **unaffected** by the extension
> region: the sidecar is referenced from the fixed trailer, not carried as an
> extension section.

> ### Decision D-FMT-9 — extension region vs. fixed trailer
>
> The seal metadata is split into a **fixed trailer at EOF** and a
> **variable-length extension region** that precedes it, rather than one
> fixed footer, because two of the seal's jobs have opposite shapes. The
> segment-catalog summary (counts, epoch, positions) is small, fixed, and on
> the hot fast path — it MUST stay `pread`-exact-from-EOF (R2), so it lives in
> the fixed trailer. The fold anchors (`StreamHeadTable`, `SnapshotAnchor`
> list, [05](05-fold-certificates.md)) are per-stream and unbounded in size,
> so they cannot live in a fixed record; they live in the extension region,
> located by the trailer's `ext_offset`/`ext_len` and integrity-checked by
> `ext_crc`. The extension is written **before** the trailer and both are made
> durable in the single seal `fsync` (§6), so the anchors are a durable part
> of the seal — not an advisory cache. Advisory-skip applies only per unknown
> *section kind* (§3.3.2), giving the same forward-compat property as the
> repair sidecar without weakening the durability of known sections. Rejected:
> (a) putting the anchors in a separate sidecar file — a second file to keep
> in sync and fsync-order against the segment, for data that is logically part
> of the seal; (b) a fixed-size head table — bounds the stream count per
> segment arbitrarily and wastes space on chain-disabled segments where
> `ext_len == 0` is the right answer.

---

## 4. Batch format

A batch is laid out as:

```text
BatchHeader                    §4.2   (72 bytes)
[ crypto_chain: 32 bytes ]     §4.4   (present iff flags.CRYPTO_CHAIN)
EventSubframe × frame_count    §4.3   (28 bytes + payload, each)
CommitMarker                   §4.5   (16 bytes)
```

### 4.1 Constants

| Name | Value | Meaning |
|---|---|---|
| `HEADER_MAGIC` | `0xBA7C_4EAD` | BatchHeader magic |
| `MARKER_MAGIC` | `0xC0AA_17ED` | CommitMarker magic |
| `FORMAT_VERSION` | `3` | On-disk format version |
| `HEADER_LEN` | `72` | Fixed BatchHeader length |
| `CHAIN_LEN` | `32` | Crypto chain value length (when present) |
| `SUBFRAME_HDR_LEN` | `28` | Fixed EventSubframe header length |
| `MARKER_LEN` | `16` | CommitMarker length |
| `MIN_BATCH_LEN` | `HEADER_LEN + MARKER_LEN` = `88` | Smallest possible (never actually valid: A5 forbids 0 frames, so a real batch is ≥ `88 + 28 + 1`) |
| `MAX_BATCH_LEN` | `64 * 1024 * 1024` (64 MiB) | A2 sanity cap (see D-FMT-4) |
| `SEGMENT_SIZE` | `256 * 1024 * 1024` | §3.1 |

> ### Decision D-FMT-4 — `MAX_BATCH_LEN = 64 MiB`
>
> D2/A2 require a `total_len` sanity cap but leave the value open; the spikes
> used 1 MiB (`crash_log`, `torn_write`) and 16 MiB (`vertical_slice`). The
> cap's only job is to bound the read/allocation that a corrupted `total_len`
> in a half-persisted header can trigger (A11: the header is not trustworthy
> just because its magic survived). It MUST be (a) far larger than any
> realistic batch and (b) safely smaller than `SEGMENT_SIZE` so A8 holds with
> margin. 64 MiB satisfies both (¼ of a segment). A batch whose `total_len`
> lies outside `[MIN_BATCH_LEN, MAX_BATCH_LEN]` MUST be rejected (A2), which
> in a forward scan means the scan stops there (§02).

### 4.2 BatchHeader

| Offset | Size | Type | Name | Description |
|---|---|---|---|---|
| 0  | 4 | u32 | `magic` | `HEADER_MAGIC` (`0xBA7C_4EAD`). |
| 4  | 2 | u16 | `format_version` | `FORMAT_VERSION` (`3`). |
| 6  | 2 | u16 | `flags` | Bitfield; see §4.2.1. |
| 8  | 4 | u32 | `frame_count` | Number of `EventSubframe`s. **A5: MUST be ≥ 1.** |
| 12 | 8 | u64 | `batch_id` | Per-segment batch sequence number, starting at `0` in each segment and incrementing by 1 (see D-FMT-5). |
| 20 | 8 | u64 | `total_len` | **A2:** the whole on-disk batch length in bytes, from this header's first byte through the `CommitMarker`'s last byte, inclusive of the crypto chain when present. |
| 28 | 8 | u64 | `first_global_pos` | Global position of this batch's first event. **A1 seed** (§02): MUST equal the running expected position. |
| 36 | 8 | u64 | `segment_epoch` | **A9:** MUST equal the containing segment's `epoch` (§3.2). This is the field that rejects a recycled segment's stale prior-generation batch. |
| 44 | 8 | u64 | `stream_id` | Batch-constant: every event in the batch belongs to this stream (see D-FMT-6). Interned ID from the registry ([04-registry.md](04-registry.md)); `0` = `$registry`. |
| 52 | 8 | u64 | `category_id` | Batch-constant category of `stream_id`. Hoisted for scan-time category filtering; `0` = `$system`. |
| 60 | 8 | u64 | `first_stream_version` | Stream version of this batch's first event (the stream's version immediately before this batch, i.e. the count of prior events in the stream). |
| 68 | 4 | u32 | `batch_crc` | **CRC32C** of the batch under the split coverage of §5. Excluded from its own coverage. |

`HEADER_LEN = 72`. `HEADER_CRC_OFF = 68`.

#### 4.2.1 `flags` bitfield

| Bit | Name | Meaning |
|---|---|---|
| 0 | `CRYPTO_CHAIN` | A 32-byte `crypto_chain` value (§4.4) is present immediately after this header. |
| 1–15 | reserved | MUST be `0` in v3; readers MUST reject a batch with an unknown flag bit set (it indicates a newer format the reader cannot validate). |

> ### Decision D-FMT-5 — `batch_id` is per-segment
>
> The v1 dialect used a log-global `batch_id`; v2 reset it to `0` per segment.
> This spec adopts **per-segment** ids (D-FMT-1 lineage). `batch_id` is
> informational only — recovery's identity and ordering come from
> `first_global_pos` (A1) and `segment_epoch` (A9), never from `batch_id`.
> Per-segment numbering lets a writer resume a segment without a global
> counter and keeps the field meaningful after a segment roll.

> ### Decision D-FMT-6 — one stream per batch; `category_id` hoisted
>
> D2 calls for "stream_id / category refs (batch-constant fields hoisted
> here)", and the v2 spike enforces one stream per batch. This spec keeps that
> constraint: all events in a batch share `stream_id` and therefore
> `category_id`. `category_id` is technically derivable from `stream_id` via
> the registry, but it is hoisted so a recovery or replay scan can filter by
> category without a registry join. A writer MUST set both consistently; a
> reader MAY trust `category_id` as written (it is inside the A4 CRC) or
> re-derive it. Multi-stream batching is out of scope for v3.

### 4.3 EventSubframe

Subframes are laid out back-to-back, exactly filling the region between the
header (or the crypto chain, if present) and the `CommitMarker`. Each subframe
is `SUBFRAME_HDR_LEN` bytes of header followed by exactly `compressed_len`
bytes of on-disk payload.

| Offset | Size | Type | Name | Description |
|---|---|---|---|---|
| 0  | 4 | u32 | `event_type_id` | Interned event type ([04-registry.md](04-registry.md)). With `schema_version` this is the upcaster dispatch key. |
| 4  | 2 | u16 | `schema_version` | Schema version of the event type at write time. Beside `event_type_id` by design (upcaster dispatch). |
| 6  | 2 | u16 | `codec_id` | Interned payload codec ([04-registry.md](04-registry.md)). `0` = bootstrap codec (frozen forever); `1` = MessagePack named-field mode for domain payloads. |
| 8  | 1 | u8  | `compression_id` | `0` = none; nonzero = a registered compression algorithm applied to the on-disk payload. |
| 9  | 1 | u8  | `subframe_flags` | Reserved; MUST be `0` in v3. |
| 10 | 2 | u16 | `dict_id` | Compression/codec dictionary id, or `0` = none. A `dict_id` referenced by any live frame MUST NOT be deleted from the registry (D3). |
| 12 | 4 | u32 | `uncompressed_len` | Byte length of the payload after decompression. MUST equal `metadata_len + data_len`. |
| 16 | 4 | u32 | `compressed_len` | Byte length of the on-disk payload that follows this header. Equals `uncompressed_len` when `compression_id == 0`. |
| 20 | 4 | u32 | `metadata_len` | Byte length of the metadata region within the uncompressed payload. |
| 24 | 4 | u32 | `data_len` | Byte length of the domain-data region within the uncompressed payload. |

`SUBFRAME_HDR_LEN = 28`, followed by `compressed_len` payload bytes.

**Payload structure.** The on-disk payload is `compressed_len` bytes. After
decompression (or verbatim when `compression_id == 0`) it is exactly
`uncompressed_len` bytes, being the metadata region (`metadata_len` bytes)
immediately followed by the data region (`data_len` bytes). The split lets a
reader locate domain data without decoding the codec, and lets metadata and
data share one compression frame and dictionary.

> ### Decision D-FMT-7 — four length fields, and the metadata/data split
>
> D2 lists four subframe lengths (`uncompressed_len`, `compressed_len`,
> `metadata_len`, `data_len`) without defining their relationship; the spikes
> carried only a single `data_len`. This spec fixes the relationship:
> `compressed_len` is the **on-disk** size (what the tiling arithmetic and the
> A4 CRC see); `uncompressed_len = metadata_len + data_len` is the **logical**
> size after decompression and is redundant-by-construction so a reader can
> validate a decompression before trusting it. Keeping all four is cheap (16
> bytes) and avoids a format break the first time metadata needs its own
> length.

### 4.4 crypto_chain (OPTIONAL)

Present if and only if `flags.CRYPTO_CHAIN` is set. Exactly `CHAIN_LEN = 32`
bytes, located immediately after the `BatchHeader` (offset `72`) and before
the first `EventSubframe`. It is a per-batch materialization of the fold-chain
value `h[i-1]` (G10: materialize per batch, recompute within a batch).

This document owns only its **placement and length**: 32 bytes at offset
`HEADER_LEN`, inside the A4 CRC coverage (§5). Its **semantics** — the genesis
formula (which MUST include `stream_id`), the chain recurrence, and
verification — are specified in [05-fold-certificates.md](05-fold-certificates.md).
A batch without `flags.CRYPTO_CHAIN` has no chain bytes; the first subframe
begins at offset `HEADER_LEN`.

The recovery scanner (§02) does **not** verify the crypto chain; recovery is
governed solely by the CRC32C `batch_crc` (A12: no CRC-off path, and no
mandatory-crypto path either — mandatory crypto would slow recovery 2.1–2.3×,
so the chain is opt-in per stream/category).

### 4.5 CommitMarker

The terminator. Its presence-and-validity is what makes a batch committed
(D1). "No marker ⇒ no batch": a torn or absent marker means the batch is
discarded, by design.

| Offset | Size | Type | Name | Description |
|---|---|---|---|---|
| 0  | 4 | u32 | `magic` | `MARKER_MAGIC` (`0xC0AA_17ED`). |
| 4  | 8 | u64 | `total_len_echo` | **A3:** MUST equal the header's `total_len`. Makes scan termination unambiguous. |
| 12 | 4 | u32 | `batch_crc_echo` | **A3:** MUST equal the header's `batch_crc`. Excluded from CRC coverage (§5). |

`MARKER_LEN = 16`. The marker occupies the final 16 bytes of the batch; its
first byte is at offset `total_len - 16`, and `batch_crc_echo` at
`total_len - 4`.

### 4.6 total_len arithmetic (A2, exact)

```text
frames_len = Σ over subframes ( SUBFRAME_HDR_LEN + compressed_len )
total_len  = HEADER_LEN
           + (CHAIN_LEN if flags.CRYPTO_CHAIN else 0)
           + frames_len
           + MARKER_LEN
```

`total_len` MUST satisfy `MIN_BATCH_LEN ≤ total_len ≤ MAX_BATCH_LEN` (A2) and,
combined with the batch's segment offset, MUST NOT exceed `SEGMENT_SIZE` (A8).

### 4.7 Worked example — a 1-frame, no-crypto batch (informative)

A minimal batch: one `EventSubframe` carrying a 12-byte uncompressed payload
(`compression_id = 0`, so `compressed_len = uncompressed_len = 12`;
`metadata_len = 0`, `data_len = 12`), and `flags.CRYPTO_CHAIN = 0` (no
`crypto_chain`). The arithmetic (§4.6):

```text
frames_len = SUBFRAME_HDR_LEN + compressed_len = 28 + 12          = 40
total_len  = HEADER_LEN + 0 (no chain) + frames_len + MARKER_LEN
           = 72 + 0 + 40 + 16                                     = 128
```

Byte map of the 128-byte batch (all offsets from the batch's first byte):

```text
[  0,  72)  BatchHeader                 (HEADER_LEN = 72)
              [ 68,  72)  batch_crc          ← excluded from CRC coverage
[ 72, 100)  EventSubframe header        (SUBFRAME_HDR_LEN = 28)
[100, 112)  subframe payload            (compressed_len = 12)
[112, 128)  CommitMarker                (MARKER_LEN = 16)
              [112, 116)  magic  = MARKER_MAGIC     (at total_len - 16)
              [116, 124)  total_len_echo = 128
              [124, 128)  batch_crc_echo           ← excluded (at total_len - 4)
```

CRC coverage (R4, §5.2) is the two ranges around the checksum fields:

```text
batch_crc = CRC32C( batch[0 .. 68]  ++  batch[72 .. 124] )
          = CRC32C( batch[0 .. 68]  ++  batch[72 .. total_len - 4] )
```

Everything except the two 4-byte checksum fields — including both magic
numbers, `total_len_echo`, the subframe header, and all 12 payload bytes — is
covered. With `flags.CRYPTO_CHAIN` set, a 32-byte `crypto_chain` (§4.4) would
sit at `[72, 104)`, the subframe would shift to `[104, 132)`, and `total_len`
would be `160`.

---

## 5. Checksum coverage (A3, A4, R4)

### 5.1 Algorithm

The batch checksum is **CRC32C** (Castagnoli, the hardware-accelerated
polynomial): measured ~1,350 MiB/s at recovery vs ~590 MiB/s for a truncated
BLAKE3, and mandatory crypto would slow recovery 2.1–2.3×. CRC32C is therefore
the always-on integrity check; the cryptographic chain (§4.4,
[05-fold-certificates.md](05-fold-certificates.md)) is a separate, opt-in
layer and is **not** a substitute for `batch_crc`.

### 5.2 Coverage is split *around* the two checksum fields (R4)

> **R4.** CRC/hash coverage is split **around** the checksum fields —
> recovery never copies a batch to zero fields before verifying.

The batch has two checksum-bearing fields that cannot cover themselves:
`batch_crc` in the header (`[HEADER_CRC_OFF, HEADER_CRC_OFF + 4)` =
`[68, 72)`) and `batch_crc_echo` in the marker (`[total_len - 4, total_len)`).
The CRC32C is computed over the batch bytes **with these two 4-byte fields
excluded** — that is, over exactly two contiguous ranges:

```text
batch_crc = CRC32C( batch[0 .. 68]  ++  batch[72 .. total_len - 4] )
```

The `total_len_echo` field, both magic numbers, the crypto chain (if present),
and every subframe and payload byte **are** covered (A3: the marker bytes are
inside CRC coverage). Only the two 4-byte checksum fields are excluded.

> ### Decision D-FMT-8 — exclude the checksum bytes; do **not** zero-and-copy
>
> The spikes computed the CRC by copying the whole batch, zeroing the two
> checksum fields, then hashing the copy. **R4 forbids this in production**
> (and the zero-fill result differs bit-for-bit from the split-coverage
> result). The normative definition **excludes** the checksum-field bytes from
> the hash input entirely — it is computed by feeding the hasher two slices
> (`[0,68)` then `[72, total_len-4)`), never by materializing a zeroed copy.
> This is streaming-friendly (no `total_len`-sized allocation on the recovery
> hot path) and unambiguous for a clean-room implementer: a conforming
> encoder and a conforming recovery scanner MUST both compute `batch_crc` over
> those exact two ranges. This is a deliberate divergence from the throwaway
> spike encoders, which are not authoritative.

Both `batch_crc` (header) and `batch_crc_echo` (marker) hold this same
value. A writer computes it once after the whole batch is laid out and writes
it into both fields. The `SegmentHeader.header_crc` and
`SegmentFooter.footer_crc` are simpler: their checksum field is the trailing
field, so their coverage is the single range `[0, crc_off)` with nothing after
it to exclude.

### 5.3 Why the full-batch CRC is load-bearing (A4, A12)

> **A4.** The full-batch CRC is load-bearing against block-write reordering
> (the marker persisting before the frames it vouches for); magic + length
> echo alone is unsafe.
>
> **A12.** No CRC-off recovery fast path may ever exist.

Empirically (`spikes/torn_write`, 24,000 cases): with the batch CRC disabled
(marker magic + `total_len` echo + subframe tiling + A1 contiguity all still
checked), **348 corrupt batches were wrongly accepted across 341 cases
(~1.4%)** — batches whose middle payload sector had not persisted and held
zeros, garbage, or resurrected stale-generation bytes, under a fully
self-consistent marker. In **1,875 cases (7.8%)** the whole-batch CRC was the
**only** rejecting check (header and marker sectors persisted, a frame sector
did not — structurally invisible to any length/tiling check). Therefore a
conforming recovery implementation **MUST** verify `batch_crc` for every
candidate batch and **MUST NOT** offer any mode ("fast path", "CRC off for
performance", "trust sealed footer instead of scanning") that accepts a batch
without it. The active recovery consequences of A4/A12 are stated in §02.

---

## 6. Encoding procedure (informative, but byte-exact)

To produce a batch, given `segment_epoch`, `first_global_pos`, `batch_id`,
`stream_id`, `category_id`, `first_stream_version`, and the subframes:

1. Compute `total_len` (§4.6).
2. Verify A5 (`frame_count ≥ 1`), A2 (`total_len` in range), A8 (fits in the
   segment). Otherwise the batch is not writable as-is.
3. Lay out `BatchHeader` with `batch_crc = 0` placeholder, then the optional
   32-byte crypto chain, then each `EventSubframe` (header + payload), then the
   `CommitMarker` with `total_len_echo = total_len` and `batch_crc_echo = 0`
   placeholder.
4. Compute `batch_crc = CRC32C(bytes[0..68] ++ bytes[72..total_len-4])` (§5.2).
5. Write `batch_crc` into the header field `[68,72)` **and** into the marker's
   `batch_crc_echo` field `[total_len-4, total_len)`.
6. Append the batch to the active segment and make it durable per
   [03-durability.md](03-durability.md). A batch is committed only once its
   `CommitMarker` is durable.

Sealing a segment appends the `SegmentFooter` (§3.3) — first the extension
region (`StreamHeadTable`, `SnapshotAnchor` list; §3.3.2), then the fixed
trailer (§3.3.1) whose `ext_offset`/`ext_len`/`ext_crc` locate and cover it —
and makes the whole footer durable in one seal `fsync`. The extension is part
of the durable seal, not written after acknowledgement. Opening a segment
first writes and syncs the `SegmentHeader` (§3.2) with a fresh, larger `epoch`
before any batch is appended (this ordering is what gives A9 its teeth against
recycled files — see §02).

---

## 7. Rule index for this document

Rules stated normatively **here** (each A/R rule appears exactly once across
this doc and [02-recovery.md](02-recovery.md)):

| Rule | Where | Gist |
|---|---|---|
| A2  | §4.1, §4.2, §4.6 | `total_len` exact definition + sanity cap |
| A3  | §4.5, §5.2 | Marker echoes + marker bytes inside CRC coverage |
| A4  | §5.3 | Full-batch CRC is load-bearing under reordering |
| A5  | §4.2 | `frame_count ≥ 1`; empty batches forbidden |
| A8  | §3.1 | Batches never span segments |
| A9  | §3.2, §4.2 | Segment epoch stamped in every BatchHeader (mandatory) |
| A11 | §3.1 | No sector-alignment requirement |
| A12 | §5.3 | No CRC-off recovery fast path may ever exist |
| R3  | §3.3.1 | Segment footer trailer carries the A9 epoch |
| R4  | §5.2 | CRC coverage split around the checksum fields |

Rules A1, A6, A7, A10, R1, and R2 are stated normatively in
[02-recovery.md](02-recovery.md).
