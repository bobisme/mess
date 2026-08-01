# 04 — Registry: bootstrap and dictionaries

Status: normative. Implements decision **D3** of
[`notes/mess-research/12_convergence.md`](../../notes/mess-research/12_convergence.md)
("Registry is event-sourced into the log"), refined by the codec/dictionary findings in
[`13_spike_results.md`](../../notes/mess-research/13_spike_results.md) and
[`15_spike_results_round3.md`](../../notes/mess-research/15_spike_results_round3.md)
(codec_bakeoff spike). Where this document makes a choice those sources leave open, the choice
is marked with a **Decision** block giving rationale and rejected alternatives.

This document is self-contained: an implementer should not need to read the research notes to
build the registry correctly. It defines:

- the four reserved IDs and why they exist out-of-band of the registry itself (§1)
- the frozen, byte-exact wire format of `codec_id 0`, the bootstrap codec (§2–§3)
- writer-assigned ID allocation rules (§4)
- name/alias resolution semantics (§5)
- compression-dictionary lifecycle (§6)
- the recovery/bootstrap ordering that makes decode acyclic, with a proof (§7)
- the closed evolution story for the registry itself (§8)

## 0. Non-goals — see the sibling spec instead

This document does **not** define:

- the general `BatchHeader` / `EventSubframe` envelope, magic numbers, CRC coverage, or the
  batch-acceptance rules (A1–A12) — see **01-log-format.md**.
- the segment scan / recovery algorithm's byte-level mechanics — see **02-recovery.md** — or
  segment footers and the A9 epoch — normative in **01-log-format.md**. This document only
  specifies *when, relative to that scan,* the registry must be materialized (§7), and what
  the registry contributes to recovery.
- fsync/durability modes and group commit — see **03-durability.md**.
- `codec_id 1` (MessagePack named-field, domain payloads) or the schema-version/upcaster
  pipeline — settled by the codec_bakeoff spike, specified in **01-log-format.md**. This
  document only says where the registry does and does not participate in that mechanism (§3.5,
  §4).
- fold certificates / chain hashes — see **05-fold-certificates.md**.
- subscription catch-up/live handoff — see **06-subscriptions.md**. This document only states
  the ordering constraint any reader (recovery or subscriber) must satisfy (§7.3).

## 1. Reserved IDs

Interned IDs (`stream_id: u64`, `category_id: u64`, `event_type_id: u32`) are assigned by the
single writer (D9) at append time and recorded durably in a system stream, `$registry`, so that
invariant **I5** (every index rebuildable by scanning the log) extends to the ID namespace
itself instead of being the one exception to it (this is the gap identified in
[`11_review.md` §2.4](../../notes/mess-research/11_review.md)).

> **Decision D-REG-G — `stream_id` is `u64`, not `u128` (lead ruling, cross-doc width sweep).**
> `stream_id` is a writer-assigned, monotonically increasing, per-namespace sequential ID (§4) —
> not a content hash or client-supplied UUID — so its cardinality is bounded by "one allocation
> per stream, ever," and `2^64` is unreachable at that rate. The `BatchHeader` envelope
> (01-log-format.md) and the fold-chain genesis formula `le64(stream_id)` (05-fold-certificates.md)
> already fix `stream_id` at `u64`; a `u128` here would either disagree with those already-frozen
> formats or require every writer to silently truncate on every path outside this document.
> `u64` throughout — envelope, registry records, and the genesis formula — is the only choice
> consistent with the rest of the format.
> Rejected: `u128` for a large "just in case" namespace — rejected because it is inconsistent
> with the already-frozen envelope and genesis formula, and buys headroom this document does not
> need and cannot spend unless those other formats also widen, which they do not.

> **Decision D-REG-H — `event_type_id` is `u32`, not `u64` (lead ruling, cross-doc width sweep).**
> `event_type_id` is the only interned ID that appears on *every* frame, inside the 28-byte
> `EventSubframe` (01-log-format.md) — unlike `stream_id`/`category_id`, which are registered
> once per stream or category ever created (§4) and never repeat per-frame. Event-type
> cardinality is small (application-defined event names), so `u32` (4B+ distinct values) is
> headroom no real system will exhaust, while `u64` would cost 4 extra bytes on every single
> frame in the log for no benefit. `u32` throughout.
> Rejected: `u64` for uniformity with `stream_id`/`category_id` — rejected because those two
> don't multiply by frame count the way a per-frame field does; uniformity there is not free the
> way it is for a field that appears once per stream/category ever created.

**REG1 (MUST).** The following four IDs are reserved and have fixed meaning:

| namespace | id | name | meaning |
|---|---|---|---|
| `stream_id` | `0` | `$registry` | the system stream this document describes |
| `event_type_id` | `0` | `RegistryEventV1` | the single event type carried by `$registry` |
| `category_id` | `0` | `$system` | the category `$registry` belongs to |
| `codec_id` | `0` | *(unnamed — "the bootstrap codec")* | §2–§3 of this document |

**REG2 (MUST).** The four reserved IDs are defined **by this specification's text**, not by any
event in the log. No implementation may expect, require, or accept a `StreamRegistered { id: 0,
.. }`, `EventTypeRegistered { id: 0, .. }`, or `CategoryRegistered { id: 0, .. }` record ever to
appear in `$registry` (§7 shows why: these four constants are exactly what make bootstrap
non-circular — there is nothing upstream of them to record them). A decoder MUST reject such a
record if one is ever found (§7.2, corruption).

**REG3 (MUST).** IDs are never reassigned, in any of the four namespaces (`stream_id`,
`category_id`, `event_type_id`, `dict_id` — the last introduced in §6). Once a `*Registered`
event for an ID is durably committed, that ID means that thing forever. Renaming is done by
`NameAliased` (§5), never by reusing or mutating the ID.

**REG4 (MUST).** Registry compaction: none in v1. Per D3, `$registry` holds exactly one event
per stream, category, event type, and dictionary ever created — it is small and append-only by
construction, not by a retention policy. If compaction is ever needed, the mechanism is a
registry snapshot with a fold certificate (machinery that will exist once 05-fold-certificates.md
lands) — out of scope here.

## 2. The bootstrap problem, and its resolution

Every other payload in the log needs the registry to make sense of it: `codec_id` tells you how
to deserialize bytes, `event_type_id`/`schema_version` select the upcaster, `dict_id` selects
decompression dictionary bytes. All of that data — the mapping from those small integers to
meaning — lives *inside* `$registry`, which is itself a stream in the same log, encoded with
some `codec_id`. If decoding `$registry`'s own events required consulting the registry, decode
would have no base case.

**Resolution:** `codec_id 0`'s wire format is fixed by this document's text and compiled into
every reader — never looked up, never data-driven, never dependent on anything durable. A reader
that has never seen a single byte of the log already knows, from reading this specification, how
to decode `codec_id 0`. This is the "hardcoded decoder" referred to throughout: reading §3's
tables *is* implementing the decoder; there is no configuration step.

**REG5 (MUST).** Every frame whose `stream_id == 0` MUST have, at the envelope level (fields
defined in 01-log-format.md):

| field | required value | why |
|---|---|---|
| `event_type_id` | `0` | REG1 |
| `codec_id` | `0` | REG1 — no other codec is ever legal for `$registry` |
| `schema_version` | `0` | see Decision D-REG-A below |
| `compression_id` | "none" | REG6 |
| `dict_id` | `0` ("none") | REG6 |

A writer MUST reject a request to append a non-conforming frame to stream 0; a recovery scanner
MUST treat a conforming-envelope-but-wrong-value frame as ordinary batch/frame corruption, per
whatever 02-recovery.md specifies for envelope validation — this document does not add a second
corruption-handling path.

**REG6 (MUST). `$registry` frames are never compressed and never dictionary-referencing.**
Decompression requires the dictionary table, which is *itself* part of the registry's
materialized state (§6) — so if `$registry`'s own frames could reference a dictionary, decoding
`$registry` would depend on having already decoded `$registry`. Forbidding compression on stream
0 breaks that cycle before it can form. (`codec_id 0` payloads are already tiny fixed-shape
records; there is nothing worth compressing.)

> **Decision D-REG-A — `schema_version` on `$registry` frames is `0`, not `1`.**
> The `(event_type_id, schema_version)` pair is the upcaster dispatch key for `codec_id ≥ 1`
> payloads (codec_bakeoff spike). `codec_id 0` payloads dispatch on the in-payload `record_kind`
> tag instead (§3.3) and never go through the upcaster pipeline — §8 explains why the whole
> point of freezing `codec_id 0` is to avoid needing one. Using `schema_version 1` would suggest
> "this is schema-versioned like everything else, expect an upcaster registered under
> `(0, 2)` some day"; that is false and would be a standing trap for whoever writes the upcaster
> dispatcher. `0` is the "not applicable, do not route this through the upcaster" sentinel.
> Rejected: reusing `1` for symmetry with "first version" — rejected because it invites exactly
> that confusion for zero benefit (nothing reads `$registry`'s schema_version for dispatch; only
> `record_kind`, defined next, does).

## 3. `RegistryEventV1` wire format (`codec_id 0`) — frozen forever

**REG7 (MUST, frozen forever).** Every byte layout in this section is permanent. It MUST NOT
change — not field order, not field width, not the meaning of an existing `record_kind` value —
for as long as `codec_id 0` exists. §8 specifies the one and only way the registry format is
allowed to evolve, and it is not by editing this section.

### 3.1 Primitive encodings

All multi-byte integers are little-endian (project-wide default; D2 states no exception for the
registry).

| primitive | encoding |
|---|---|
| `u8`, `u16`, `u32`, `u64`, `u128` | fixed-width, little-endian, unsigned |
| `str` | `u16` byte-length `N` (LE), followed by exactly `N` raw UTF-8 bytes. No NUL terminator, no padding. Max length 65,535 bytes (the width of the length prefix). A decoder MUST validate the bytes are well-formed UTF-8 and MUST reject the record (loud failure) if not. |
| `blob` | `u32` byte-length `N` (LE), followed by exactly `N` raw bytes, opaque to the registry. |

> **Decision D-REG-B — string length prefix is `u16`, not `u8` or a varint.**
> `u8` (255-byte cap) risks truncating namespaced names (e.g. `"orders.OrderPlaced"`-style
> dotted type names) and is a false economy — registry events are rare (one per name ever
> created; REG4), so the extra byte is free. A LEB128 varint (as postcard would use) was
> rejected because it makes the fixed-offset byte tables below depend on the *value* of an
> earlier field, which directly fights the "byte-exact, table-of-offsets" requirement this
> document is under, for no space win at registry volumes. Fixed `u16` keeps every table in
> this document a flat offset/size/type/name table with no variable-offset arithmetic except
> "the string field is last."

### 3.2 Payload shape

A `codec_id 0` payload (the bytes carried by a single `EventSubframe` in a `stream_id == 0`
batch) is:

```text
record_kind: u8         // offset 0, always present — see 3.3
record-kind-specific fields, per §3.4–§3.8
```

Every table in §3.4–§3.8 gives offsets **relative to the start of the payload** (i.e. `offset 0`
is the `record_kind` byte).

### 3.3 `record_kind` — the closed tag set

**REG8 (MUST, frozen forever).** `record_kind` is a `u8` at payload offset 0. Exactly five
values are legal, forever:

| `record_kind` | name | defined in |
|---|---|---|
| `0x00` | *(invalid — reserved)* | never legal; guards against a zeroed/torn payload decoding as something |
| `0x01` | `StreamRegistered` | §3.4 |
| `0x02` | `EventTypeRegistered` | §3.5 |
| `0x03` | `CategoryRegistered` | §3.6 |
| `0x04` | `NameAliased` | §3.7 |
| `0x05` | `DictRegistered` | §3.8 |
| `0x06`–`0xFF` | *(invalid — reserved)* | never legal (§8: new registry concerns get a new `event_type_id`, not a new tag here) |

A decoder MUST reject (loud failure) any payload whose `record_kind` byte is `0x00` or
`≥ 0x06`.

### 3.4 `StreamRegistered` (`record_kind = 0x01`)

| offset | size | type | name | description |
|---|---|---|---|---|
| 0 | 1 | `u8` | `record_kind` | `0x01` |
| 1 | 8 | `u64` | `stream_id` | the newly allocated stream ID (MUST NOT be `0`; see §4) |
| 9 | 8 | `u64` | `category_id` | the stream's category (MUST be `0` or an already-registered `category_id`; see §4.2) |
| 17 | 2 | `u16` | `name_len` | byte length of `name` |
| 19 | `name_len` | `str` bytes | `name` | the stream's name at registration time |

Total size: `19 + name_len` bytes.

> **Decision D-REG-C — `StreamRegistered` carries no `codec` or `schema` field, despite D3's
> pseudocode listing them.**
> D3's sketch (`12_convergence.md` §D3) writes `StreamRegistered { id, name, category, codec,
> schema, ... }` with a trailing `...` signaling it is illustrative, not literal. A stream is a
> sequence of events, potentially of many event types; codec and schema are properties of an
> *event type* (§3.5), not of the stream that happens to carry it. Putting `codec`/`schema` on
> `StreamRegistered` would either be redundant (single-event-type streams) or actively
> misleading (multi-event-type streams, where it could only name one type's codec/schema and a
> reader would have to guess which). One property, one place: kept on `EventTypeRegistered`
> only, avoiding a second source of truth that could drift from the frame's own
> `codec_id`/`schema_version` (D2 fields, present on every frame regardless).
> Rejected alternative: keep both fields anyway for literal fidelity to D3's sketch — rejected
> because D3 is explicitly a sketch ("Bootstrap rules" prose, not a byte spec) and shipping a
> field with no well-defined meaning for multi-type streams is worse than omitting it.

### 3.5 `EventTypeRegistered` (`record_kind = 0x02`)

| offset | size | type | name | description |
|---|---|---|---|---|
| 0 | 1 | `u8` | `record_kind` | `0x02` |
| 1 | 4 | `u32` | `event_type_id` | the newly allocated event-type ID (MUST NOT be `0`; see §4) |
| 5 | 2 | `u16` | `codec_id` | the declared payload codec for this event type (MUST be `≥ 1`; see note below) |
| 7 | 32 | `[u8; 32]` | `schema_fingerprint` | opaque digest identifying `schema_version 1`'s shape |
| 39 | 2 | `u16` | `name_len` | byte length of `name` |
| 41 | `name_len` | `str` bytes | `name` | the event type's name at registration time (e.g. `"orders.OrderPlaced"`) |

Total size: `41 + name_len` bytes.

**REG9 (MUST).** `codec_id` in this record MUST be `≥ 1`. `codec_id 0` is reserved to
`$registry`'s own internal use (REG1/REG5) and MUST NOT be declared as a domain event type's
codec — declaring it would claim a domain event type uses the frozen bootstrap format, which is
never true by construction (REG5 pins `codec_id 0` to `stream_id 0` only).

> **Decision D-REG-D — one `EventTypeRegistered` per `event_type_id`, ever; no per-version
> entries.**
> D3 states the registry holds "one event per stream/type/category ever created" (§D3, verbatim)
> — this settles the question directly, it is not a judgment call: `EventTypeRegistered` fires
> once, at first use of the name, exactly like `StreamRegistered`/`CategoryRegistered`. Later
> schema versions of the *same* `event_type_id` are handled entirely in-band, per frame — the
> codec_bakeoff spike established `(event_type_id, schema_version)` as the complete,
> self-contained upcaster dispatch key (`15_spike_results_round3.md` §2, "Frame-format
> consequences"), so a reader never needs the registry to resolve a schema version, only the
> frame's own header. `schema_fingerprint` here therefore records only `schema_version 1`'s
> shape, as a permanent audit anchor ("this name was first minted meaning this"); later
> versions' fixtures and fingerprints live with the application code (golden fixture tests, per
> the codec_bakeoff upcaster prototype), not in `$registry`.
> Rejected: emitting a new registry record per schema-version bump — rejected because it
> contradicts D3's explicit "one event per type ever created" characterization, would make the
> registry grow with every additive schema change instead of staying append-only-and-tiny, and
> duplicates state the frame header (`schema_version`) and the upcaster derive (fixtures) already
> own.

### 3.6 `CategoryRegistered` (`record_kind = 0x03`)

| offset | size | type | name | description |
|---|---|---|---|---|
| 0 | 1 | `u8` | `record_kind` | `0x03` |
| 1 | 8 | `u64` | `category_id` | the newly allocated category ID (MUST NOT be `0`; see §4) |
| 9 | 2 | `u16` | `name_len` | byte length of `name` |
| 11 | `name_len` | `str` bytes | `name` | the category's name at registration time |

Total size: `11 + name_len` bytes.

### 3.7 `NameAliased` (`record_kind = 0x04`)

Rename is alias, never mutation: the target's ID and every prior name remain valid forever (§5).

| offset | size | type | name | description |
|---|---|---|---|---|
| 0 | 1 | `u8` | `record_kind` | `0x04` |
| 1 | 1 | `u8` | `target_kind` | `1` = stream, `2` = category, `3` = event_type (`0` and `≥4` invalid) |
| 2 | 8 | `u64` | `target_id` | the ID being renamed, in the namespace `target_kind` selects (§ note below) |
| 10 | 2 | `u16` | `new_name_len` | byte length of `new_name` |
| 12 | `new_name_len` | `str` bytes | `new_name` | the new preferred name |

Total size: `12 + new_name_len` bytes.

**REG10 (MUST).** `target_id` MUST already be a registered, non-reserved ID in the namespace
`target_kind` selects, as of this record's position in `$registry` replay order (§4.2 defines
"as of this position" precisely, via **replay position**). Aliasing an unregistered or reserved
(`0`) ID MUST be rejected.

> **Decision D-REG-E — `target_id` is a fixed `u64`, uniform across all three renameable
> namespaces (lead ruling: `stream_id`/`category_id` are `u64`, `event_type_id` is `u32` — `u64`
> is now the widest of the three, so it is the uniform width, not a separately widened type).**
> A tagged-union encoding (field width depends on `target_kind`) would save at most 4 bytes on
> an event kind that fires only on explicit user rename requests — negligible at registry
> volumes (REG4) — at the cost of making this the one table in the document whose layout isn't
> flat (offset of `new_name_len` would depend on a preceding field's *value*, not just its
> presence). Uniform `u64` keeps every offset in every table in this document a compile-time
> constant, and matches `stream_id`/`category_id` exactly — no widening needed for those two.
> `event_type_id` values are carried zero-extended into the low 32 bits of the 64-bit field, high
> 32 bits zero; a decoder MUST reject an `event_type_id` alias whose high 32 bits are nonzero
> (defends against a corrupted/foreign ID being silently truncated into the wrong namespace).

### 3.8 `DictRegistered` (`record_kind = 0x05`)

Compression dictionaries are registry objects (D3; codec_bakeoff, §5 "Frame format
requirements"). `dict_id` is a **separate namespace** from `stream_id`/`category_id`/
`event_type_id` — its reserved `0` means "no dictionary" (D2's `dict_id: u16 // 0 = none`), not
"a bootstrap dictionary": there is no such thing, because `$registry` frames never use
compression at all (REG6).

| offset | size | type | name | description |
|---|---|---|---|---|
| 0 | 1 | `u8` | `record_kind` | `0x05` |
| 1 | 2 | `u16` | `dict_id` | the newly allocated dictionary ID (MUST NOT be `0`; see §4) |
| 3 | 1 | `u8` | `scope_kind` | `2` = category-scoped, `3` = event-type-scoped (`0`, `1`, `≥4` invalid — reuses `target_kind`'s tag values from §3.7; `1` = stream is deliberately not a legal dictionary scope, matching D3's "category/event_type scope" wording) |
| 4 | 8 | `u64` | `scope_id` | the `category_id` or `event_type_id` this dictionary applies to, per `scope_kind` (MUST already be registered) |
| 12 | 2 | `u16` | `codec_id` | which codec's encoded bytes this dictionary was trained against (MUST be `≥ 1`, same rule as REG9 — a dictionary is trained on one codec's byte shapes and is meaningless for another's) |
| 14 | 4 | `u32` | `dict_bytes_len` | byte length of `dict_bytes` |
| 18 | `dict_bytes_len` | `blob` bytes | `dict_bytes` | the trained dictionary, opaque to the registry |

Total size: `18 + dict_bytes_len` bytes.

> **Decision D-REG-F — dictionary bytes are stored inline in `DictRegistered`, not behind a
> pointer.**
> D3's sketch says `dict_bytes_ref`, suggesting an indirection to a blob stored elsewhere. No
> document defines a blob-pointer format anywhere in this project yet (`SnapshotRef.snapshot_ptr:
> BlobPtr` in D4 is equally undefined — flagged as open in `15_spike_results_round3.md`'s G6/G10
> discussion, which is 05-fold-certificates.md's problem to solve, not this document's).
> Introducing an undefined indirection into a format frozen forever (REG7) would freeze the
> *absence* of a definition along with it. Inlining sidesteps that: `$registry` stays exactly as
> rebuildable as I5 already requires (no second storage system to keep in sync), dictionaries
> are small (16 KiB per D6/codec_bakeoff) and rare (one `DictRegistered` per category-or-type
> ever trained — dictionaries are demoted to a row-format/hot-tail fallback under D6's round-4
> columnar-shredding default, so this is not a hot path), and `u32` gives headroom well past the
> 16 KiB working number without inventing a pointer format this document has no authority over.
> Rejected: leave a `BlobPtr` placeholder now and inline later if undefined — rejected because
> "frozen forever" (REG7) means there is no *later*: whatever ships first is permanent, so the
> version with no undefined dependencies ships.

### 3.9 Worked example

`CategoryRegistered { category_id: 1, name: "orders" }`, as the raw payload bytes (hex):

```text
offset  bytes                          field
0       03                             record_kind = 0x03
1       01 00 00 00 00 00 00 00        category_id = 1 (u64 LE)
9       06 00                          name_len = 6 (u16 LE)
11      6F 72 64 65 72 73              name = "orders" (UTF-8)
```

17 bytes total (`11 + 6`), matching §3.6.

## 4. ID allocation

**REG11 (MUST). IDs are writer-assigned, monotonically increasing per namespace, starting at
1.** There are four independent namespaces: `stream_id`, `category_id`, `event_type_id`,
`dict_id`. `0` is reserved in each (REG1, REG6). The single writer (D9) is the only entity that
ever allocates an ID; there is no client-supplied-ID path for these four namespaces.

### 4.1 Allocation algorithm

The writer holds, in memory, a **high-water mark** per namespace: the largest ID durably
committed in that namespace so far (`0` — meaning "none allocated yet" — before the first
registration). To register a new name in namespace `N`:

```text
next_id = high_water_mark[N] + 1
append (and durably order — §4.2) a *Registered record carrying next_id
on successful commit: high_water_mark[N] = next_id
```

The high-water marks are seeded at process start by replaying `$registry` (§7.2) — they are
ordinary registry-derived state, not separately persisted, per I5.

**Note (not a violation of REG3):** if the writer allocates `next_id`, attempts to append its
`*Registered` record, and crashes before that batch is durably committed, `next_id` was never
actually assigned — recovery's replay (§7.2) never advanced the high-water mark past it, so the
post-recovery writer allocates that same number to whatever it registers next. REG3
("never reassigned") binds only to IDs that reached a durably committed `*Registered` record;
an in-flight allocation that never committed was never assigned in the first place.

### 4.2 Dependency ordering: registered before referenced

**REG12 (MUST). An ID (in any of the four namespaces) MUST be visible in `$registry` replay
before any frame that references it.** "Visible... before" is defined by **replay position**,
the pair `(batch commit order, subframe index within the batch)` — the same order recovery
applies records in (§7.2):

- across batches: the batch containing the `*Registered` record MUST have an earlier
  `first_global_pos` than the batch containing the first reference (batches are single-stream —
  §"batch-constant" in D2 — so a `$registry` registration and a domain-stream use can never be
  the same batch; they are always at least two batches, and the registration's must sort first).
- within a batch: `$registry` batches may carry multiple `RegistryEventV1` subframes (e.g.
  registering a new category and a new stream in that category together). Ordering is by
  subframe index: a `CategoryRegistered` at subframe 0 makes that `category_id` visible to a
  `StreamRegistered` at subframe 1 of the *same* batch; the reverse order is not legal.

This single rule covers every dependency this document defines:

| dependent record | depends on | namespace |
|---|---|---|
| `StreamRegistered.category_id` (if `≠ 0`) | a `CategoryRegistered` | `category_id` |
| `NameAliased.target_id` | a `*Registered` for that `target_kind` | matching REG10 |
| `DictRegistered.scope_id` | a `CategoryRegistered` or `EventTypeRegistered`, per `scope_kind` | `category_id`/`event_type_id` |
| any domain frame's `stream_id` | a `StreamRegistered` | `stream_id` |
| any domain frame's `event_type_id` | an `EventTypeRegistered` | `event_type_id` |
| any frame's `dict_id` (if `≠ 0`) | a `DictRegistered` | `dict_id` |

**REG13 (MUST). A writer MUST enforce REG12 at append time** — it holds the full materialized
registry (§4.1) and can trivially check "is this ID already registered" before constructing the
domain batch. A reader (recovery or subscriber) is not required to re-derive this ordering to
decode correctly (decode never needs it — §7.1); REG12/REG13 exist so that *interpretation*
(resolving a `dict_id` to bytes, a `stream_id` to a name) never has a missing entry, not because
decode would otherwise fail.

**REG14 (MUST). Second registration of an already-registered ID is corruption, not an update.**
If `$registry` replay (§7.2) encounters a second `*Registered` record for an ID already present
in the materialized table (any of the four namespaces), the implementation MUST treat this as a
registry-corruption condition and fail loudly — never silently overwrite the earlier entry.
(REG3 already forbids a *correct* writer from doing this; REG14 is the reader-side guard against
a corrupted or adversarial log doing it anyway.)

### 4.3 Canonical positions and application visibility (v3)

`$registry` records are real v3 event frames, not out-of-band metadata. Each
record consumes the canonical global position assigned by
[01-log-format.md §4.2.2](01-log-format.md#422-canonical-positions-include-system-events),
and the registry batch must occupy earlier positions than the first domain
batch that references its IDs (REG12). A new stream or event type can therefore
insert one or more registry positions anywhere in the canonical sequence—not
only at genesis.

Application-facing global reads and subscriptions MUST filter `stream_id == 0`:
registry records are needed by recovery, raw inspection, and registry replay,
but they are not domain events. Filtering changes visibility, not allocation:
the watermark and subsequent domain events retain their canonical positions.
Thus returned domain records are ordered by `global_position`, but their
positions can have gaps where `$registry` records landed.

A consumer MUST treat `global_position` as an opaque monotone ordering/resume
cursor. It MUST NOT assume that it is a zero-based index into the visible event
sequence, use position differences as visible-event counts, or require the next
visible record to have the immediately following integer. Filtered paging needs
an explicit scan frontier so a registry-only range can advance the cursor even
when no domain record is returned; [06-subscriptions.md](06-subscriptions.md)
defines that contract.

The v4 control-prelude design would have avoided spending domain positions on
control-only capsules. It was **declined** at its product-admission gate —
[ADR 0003](../adr/0003-v4-admission.md) — so the behavior specified here is not
provisional: registry records consuming canonical positions, and the resulting
gaps in application-visible positions, are the normative and settled v3
contract. ADR 0003 §9 records the narrow conditions under which v4 could be
reopened; until one of them is met, no alternative encoding is pending.

## 5. Name resolution and aliasing

Renaming is `NameAliased`, never mutation of the original `*Registered` record (D3, verbatim:
"rename = alias event, never mutation") — the log is immutable (I1) and `$registry` is no
exception.

**REG15 (MUST). `id -> current_name` is single-valued and follows the most recent record.** The
current preferred name for an ID is the `name`/`new_name` of the most recent (by replay
position, §4.2) `*Registered` or `NameAliased` record targeting it.

**REG16 (MUST). `name -> id` binds forever, across all aliases, within one namespace.** Once a
name string has been used — as the original `name` in a `*Registered` record or as `new_name` in
a `NameAliased` record — in namespace `N`, it MUST NOT be bound to a different ID in namespace
`N` thereafter. (REG4's "no compaction" means old names are never freed for reuse.) A writer
MUST reject an alias or registration that would violate this. This guarantees historical
references by old name (e.g. a subscription filter written against a pre-rename name) keep
resolving to the same ID forever, even though the *current* preferred name has moved on.
Different namespaces do not share a name space with each other: a stream and a category may
legitimately have the same name string.

**REG17 (MUST). The four reserved IDs (REG1) MUST NOT be targeted by `NameAliased`.** They are
not data — REG2 already forbids `*Registered` records for them, and permanence of their names is
part of what makes them a stable, out-of-band base case (§2). A `NameAliased` naming
`target_id 0` in any namespace MUST be rejected.

## 6. Dictionary lifecycle

**REG18 (MUST). A `dict_id` MUST be registered (`DictRegistered`) before any frame references
it.** This is REG12's dictionary row, restated because it is this document's second acceptance
criterion: no frame anywhere in the log may carry a nonzero `dict_id` unless a `DictRegistered`
record for that ID has an earlier replay position (§4.2).

**REG19 (MUST). A live `dict_id` is never deleted.** There is no `DictRetired` (or equivalent)
event type defined in this document, and REG4 rules out compaction in v1 — so in the system this
document specifies, dictionary deletion is not merely disallowed, it is **not a thing that can
happen**: there is no event that expresses it. The "never deleted while referenced" acceptance
criterion holds vacuously today (nothing can delete a dictionary, referenced or not).

> Forward note (non-normative, out of scope): if a future document introduces dictionary
> retirement, §8's evolution rule applies to it like anything else — it would be a *new*
> registry-adjacent event type under its own `event_type_id` (never a new `record_kind` on
> `RegistryEventV1` — REG8), and it would have to establish its own "not referenced by any live
> frame" precondition, symmetric to REG18's "registered before referenced." That is a problem for
> whichever document defines retention/compaction (REG4); this document does not sketch its
> mechanism further than that.

**REG20 (MUST). `scope_id` and `codec_id` on `DictRegistered` MUST already be registered /
legal as of the record's replay position** — `scope_id` per REG12's table, `codec_id` per the
same non-zero rule as REG9 (codec IDs are a small, spec-defined enumeration — see the note below
— not registry-allocated, so "already registered" for `codec_id` means "a codec ID the
implementation recognizes," not "seen in `$registry` before").

> **Note — codecs are not registry objects.** Unlike streams/categories/event types/dictionaries,
> `codec_id` values are a small, closed enumeration defined by 01-log-format.md (`0` = bootstrap,
> `1` = MessagePack named-field, ...), not user-extensible data. There is deliberately no
> `CodecRegistered` record kind in §3.3's tag table: adding a codec is a format change to
> 01-log-format.md's decoder dispatch, not a runtime registration act, and mixing the two would
> let a corrupt or adversarial log claim an arbitrary "registered" codec ID that no decoder
> actually implements.

## 7. Recovery / bootstrap ordering

This section is the proof for the first acceptance criterion: **the bootstrap decode path has no
circular dependency.**

### 7.1 Three strictly layered steps

```text
step 1              step 2                    step 3
{segment   -->  {accepted   -->  {materialized  -->  {everything else:
 bytes}          batches/         $registry           name resolution,
                  frames}         table}               dict-based decompress,
                                                        other index rebuild, ...}
```

- **Step 1 — batch/frame acceptance (02-recovery.md's job).** Segments are scanned and batches
  accepted per whatever A1–A12-style rules 02-recovery.md defines. This step decodes only
  *envelope* fields — `stream_id`, `event_type_id`, `codec_id`, `compression_id`, `dict_id`,
  lengths, CRCs. Every one of these is a raw fixed-width integer sitting at a fixed offset in the
  frame/batch header (01-log-format.md); none of them is a name, and resolving none of them
  requires a lookup anywhere. **Step 1 depends on nothing but the bytes on disk.**

- **Step 2 — materialize `$registry` (this document's job).** For every accepted frame with
  `stream_id == 0`, in replay order (§4.2), decode its payload using §3's tables — a fixed,
  compiled-in decoder, not a data-driven one (§2) — and apply it to the in-memory registry table:
  id↔name maps (§5), the `dict_id -> dict_bytes` table (§6), and the four high-water marks (§4.1).
  **Step 2's only inputs are step 1's output (the accepted-frame stream) and this document's
  fixed tables.** It does not consult the registry table it is building, and it cannot: REG5/REG6
  guarantee `stream_id == 0` frames never carry a `dict_id` or non-zero `codec_id`, so nothing in
  step 2 ever needs step 2's own output to make progress.

- **Step 3 — everything that needs names, codecs-as-declared, or dictionaries.** Resolving a
  `stream_id` to a display name, decompressing a frame whose `dict_id ≠ 0`, validating an
  `event_type_id`'s frame-declared `codec_id` against its registry-declared one, and rebuilding
  any other index that wants names — all of this **depends on step 2's completed table** and
  MUST NOT run until step 2 has finished replaying every accepted `$registry` frame.

### 7.2 Why this is acyclic

Draw an edge from step *X* to step *Y* if *Y*'s work requires *X*'s output. The graph has exactly
two edges: `step 1 -> step 2` and `step 2 -> step 3`. There is no edge back to step 1 from
anywhere (nothing decoded in steps 2 or 3 is needed to accept a batch — REG5 makes sure of it: a
`$registry` frame's envelope is exactly as self-contained as any other stream's), and no edge
back to step 2 from step 3 (step 3 only reads step 2's finished table; §7.1 already argued step 2
never reads its own in-progress table). A graph with two edges and no cycle is, trivially,
acyclic — but the substance of the claim is REG5/REG6/REG2: they are precisely what block the two
edges (`3 -> 2` and `2 -> 2`, respectively) that would otherwise exist if `$registry` frames
could reference dictionaries or use a data-driven codec.

**REG21 (MUST). Implementations MUST perform step 2 to completion — full `$registry` replay —
before beginning any step-3 activity**, including but not limited to: dictionary-based
decompression of any frame, name resolution for any API surface, and rebuilding any index whose
construction consults names or dictionaries. Index rebuilds that only need raw `EventPtr`-style
data (01-log-format.md) do not depend on step 2 and may run concurrently with it.

### 7.3 Non-recovery readers

REG21 is stated for recovery, but the same dependency exists for any reader that begins
consuming the log from a position other than genesis without having replayed `$registry` up to
(at least) that position first — a subscription resuming from a saved cursor, or a snapshot
rebuild. Such a reader MUST have materialized `$registry` at least through the highest replay
position referenced by anything it is about to interpret (a `dict_id`, a name) before
interpreting it; the exact catch-up mechanics (how a subscriber efficiently gets there without a
full rescan) are 06-subscriptions.md's concern, not this document's — this document only states
the invariant 06-subscriptions.md's protocol must uphold.

## 8. Evolution: how the registry format is allowed to change

**REG22 (MUST). `codec_id 0` (§2, §3) never changes.** Not a new `record_kind`, not a
reordered field, not a widened integer — nothing. §3's tables, once merged, are permanent. This
is what "frozen forever" means operationally: there is no version field to bump inside `codec_id
0`, because bumping it would recreate exactly the bootstrap problem §2 solves (a reader would
need to know *which* version of the bootstrap format it is looking at before it could read the
byte that would tell it).

**REG23 (MUST). Registry evolution happens by registering new event types, never by touching
`codec_id 0`.** Once bootstrap is complete (§7 has run once, ever, for a given log), the registry
is no longer in a chicken-and-egg position: `EventTypeRegistered` (§3.5) can mint a brand-new
`event_type_id` for a brand-new registry-adjacent concern (say, a future dictionary-retirement
event, per §6's forward note), and that new event type can use `codec_id 1` and the full
schema-version/upcaster machinery like any other domain event type — because by construction it
is only ever decoded *after* step 2 (§7.1) has already made the registry, including this new
type's own registration, available. This is the resolution the task brief points at directly:
"evolution is new event types only." §3.3's tag table is closed (REG8) precisely so that this is
the *only* extension mechanism — there is no ambiguity about whether a given piece of registry
state lives in a frozen `record_kind` or a versioned event type.

## Cross-references

- **01-log-format.md** — `BatchHeader`/`EventSubframe` envelope, magic numbers, CRC coverage,
  `codec_id 1` (MessagePack named), `schema_version`/upcaster dispatch, `EventPtr`.
- **02-recovery.md** — segment scan, batch acceptance (A1–A12-style rules), the point in the
  recovery sequence where this document's §7 materialization step is invoked.
- **03-durability.md** — fsync/group-commit semantics; §4.2's "durably ordered" registration
  requirement is agnostic to which durability mode is in effect.
- **05-fold-certificates.md** — `BlobPtr`, if/when one is defined; not used by this document
  (D-REG-F).
- **06-subscriptions.md** — catch-up/live handoff; must uphold §7.3's ordering invariant.
