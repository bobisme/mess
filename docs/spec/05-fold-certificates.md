# 05 — Fold Certificates

Status: normative. Resolves decision record **D4** (`notes/mess-research/12_convergence.md`)
and the two blocking design questions **G6** (durable head anchor) and **G10** (chain storage
layout). This document is self-contained; an implementer needs nothing from the research notes.

> **This document BLOCKS Phase 5 (Verification/compression).** The revised roadmap lists
> "fold hashes + fold_version enforcement (D4)" with an explicit prerequisite: *resolve G6
> (durable head anchor) and G10 (chain storage layout); retention preserves certification
> frames.* Those three items are decided here. Phase 5 fold implementation MUST conform to
> this spec.

Key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, **MAY** are used per RFC 2119.

Cross-references (siblings, written in parallel): `01-log-format.md` (batch/subframe/footer byte
layout, CRC coverage), `02-recovery.md` (A1–A12 acceptance, segment scan, epoch), `03-durability.md`
(group commit, ack watermark), `04-registry.md` (interned `stream_id` assignment, D3),
`06-subscriptions.md` (ack watermark reuse). Where this document specifies a field that physically
lives in a structure owned by a sibling (BatchHeader, EventSubframe, segment footer), the sibling
owns the surrounding byte layout and this document owns the field's semantics and hash coverage.

---

## 1. Scope and threat model

### 1.1 What a fold certificate proves

A **fold certificate** lets `load_verified::<A>(stream_id)` prove that a stored snapshot
summarizes **the exact committed event prefix** of a stream — not a stale prefix, not a prefix
from a different stream, not a corrupted blob, and not a truncated or reordered log.

The certificate proves **prefix identity and integrity**. It does **NOT** prove that the fold
code (`apply`) is semantically correct — that is the job of `fold_version` plus the generated
drift test (§9). This scope limit is normative and `load_verified` documentation **MUST** state
it: *the certificate proves the snapshot summarizes the exact committed prefix; it does not prove
the fold was correct.*

### 1.2 Threat model

The verifier trusts:

1. The genesis rule and the three hash constructions in §3 (they are code, not data).
2. The **durable head anchor** (§5) — a `(head_version, head_hash)` witness that lives
   **outside the rebuildable index and outside the tail an attacker can truncate**.
3. The batch commit protocol of `02-recovery.md` (A1–A12): a batch is *committed* iff it has a
   valid synced CommitMarker, passes the full-batch CRC (A4/A12), is contiguous (A1), and carries
   the expected segment epoch (A9). After recovery, the accepted log prefix **is** truth (D1).

The verifier does **NOT** trust:

- The `stream_id` field written inside a `SnapshotRef` (attacker-writable — see §8, gap 5).
- The stored `frame_hash` of any frame (attacker-writable — always recomputed, §8 gap 3).
- A `prev_stream_hash` value read from a frame header in isolation (attacker-writable — sound
  only when discharged by tail replay ending at the head anchor, §7, gap 7).
- Any value derivable purely by scanning a possibly-truncated log (that is the whole point of
  the head anchor living elsewhere, §5).

Adversary capability assumed: arbitrary modification of snapshot blobs, `SnapshotRef` bytes, and
frame bytes at rest, including truncating or rewriting whole suffixes of the log; and silent
media corruption of already-fsynced regions. The adversary cannot forge the durable head anchor's
storage location (segment footer + epoch chain, §5) without being caught by that structure's own
checksum and the A9 epoch chain.

The `fold_cert` spike detected all 13 modeled attacks under this model, including the
counterfactual that proves the `stream_id` binding is load-bearing (§8, gap 5).

---

## 2. The certificate: `SnapshotRef`

A snapshot is a serialized aggregate state blob plus a `SnapshotRef` that certifies it. The
`SnapshotRef` is stored in the index / snapshot store (`04-registry.md` describes where); it is
untrusted data and every field is checked.

### 2.1 `SnapshotRef` fields (normative)

| offset | size | type      | name                | description |
|--------|------|-----------|---------------------|-------------|
| 0      | 8    | u64 LE    | `stream_id`         | Interned stream id (D3). Cheap first-line check only; the genesis binding (§3.1) is the real protection. |
| 8      | 8    | u64 LE    | `stream_version`    | 0-based index of the **last** event summarized. See §4 for the exact count-vs-index rule. Ignored when `covers_empty_prefix` is set. |
| 16     | 4    | u32 LE    | `fold_version`      | Explicit, human-bumped semantic version of the fold (§9). |
| 20     | 4    | u32 LE    | `flags`             | Bit 0 = `covers_empty_prefix` (§4.2). Bits 1–31 reserved, MUST be 0. |
| 24     | 32   | [u8; 32]  | `event_prefix_hash` | The chain value `h[stream_version]` (or the genesis `h[-1]` when `covers_empty_prefix`). Proves prefix identity. |
| 56     | 32   | [u8; 32]  | `state_hash`        | `BLAKE3(state_blob)`. Proves blob integrity. |
| 88     | —    | BlobPtr   | `snapshot_ptr`      | Pointer to the state blob (layout owned by `04-registry.md`). Not covered by any hash here. |

`event_prefix_hash` replaces the ambiguously-named `fold_hash` of the research notes; it is
exactly the fold-chain value `h[v]` of §3.

> **Decision — canonical field is a version index, not a count.** D4's struct uses
> `stream_version: u64`. The `fold_cert` REPORT (gap 2) warns that "count vs. last-index" is
> unpinned and every off-by-one silently invalidates certificates. We keep `stream_version` as a
> **0-based last-index** (so it agrees with the version used everywhere else in the system — the
> index keys, `02-recovery`, subscriptions) and disambiguate the empty case with an explicit flag
> (§4.2) rather than by overloading the count. Rejected: making the canonical field a *count*
> (`event_count`) — cleaner in isolation but forces a count↔version translation at every call
> site and re-introduces off-by-ones at the boundary with the version-keyed index.

---

## 3. Hash constructions (byte-exact)

All hashing is **BLAKE3** (256-bit / 32-byte output). All integers are **little-endian**.
`le64(x)` denotes the 8-byte little-endian encoding of a `u64`.

Every hash input begins with a **1-byte domain-separation tag** so the three hash families can
never collide regardless of field lengths. This closes gap 4 (the spike noted its inputs were
"unambiguous by length only by accident").

| tag byte | family        |
|----------|---------------|
| `0x00`   | genesis       |
| `0x01`   | frame hash    |
| `0x02`   | chain step    |

### 3.1 Genesis — `h[-1]` (unifies docs 08 and 12; gaps 1 & 5)

```
h[-1] = BLAKE3( 0x00 || "mess-stream-v1" || le64(stream_id) )
```

- `"mess-stream-v1"` is the 14 ASCII bytes `6D 65 73 73 2D 73 74 72 65 61 6D 2D 76 31`
  (no NUL terminator, no length prefix — its length is fixed by this spec).
- `stream_id` is the **interned u64 stream id** assigned by the registry (D3, `04-registry.md`),
  **not** the stream name. Names can be re-aliased (`NameAliased`); binding the mutable name
  would invalidate the chain on rename. The interned id is immutable and never reassigned.

> **Decision — genesis formula.** Doc 08 §2 says `H("mess", stream_id)`; the D2-era framing and
> the spike say `BLAKE3("mess-stream" || stream_id)`. Neither pinned the separator, a length
> prefix, or the `stream_id` encoding, so any two implementations diverge (gap 1). We pick the
> form above: domain tag `0x00`, the fixed literal `"mess-stream-v1"` (versioned so a future
> genesis change is a visible constant bump), then `le64(stream_id)`. The `-v1` suffix and the
> domain tag together give unambiguous cross-language, cross-protocol domain separation.
> Rejected: doc 08's `H("mess", stream_id)` (ambiguous concatenation, no length discipline);
> the bare spike form (no version, no domain tag, and it hashed the id as a UTF-8 string, which
> is wrong once ids are interned integers).

> **Decision — `stream_id` in genesis is MANDATORY.** The spike demonstrated a cross-stream
> confusion attack: with an unbound genesis, a snapshot of stream A relabeled as stream B passes
> every check when the payload prefixes coincide (and delivers silently wrong state when they do
> not — e.g. templated/system streams with common prefixes). The `SnapshotRef.stream_id` field
> does **not** protect against this because the attacker writes it. The genesis binding is the
> only load-bearing protection. Therefore: **every stream with the crypto chain enabled MUST
> derive `h[-1]` from its own interned `stream_id`.** A verifier MUST NOT accept a certificate
> whose `event_prefix_hash` was computed from a different genesis. (This is enforced structurally:
> `h[v]` transitively depends on `h[-1]`, so a wrong `stream_id` makes every `event_prefix_hash`
> mismatch.)

### 3.2 Frame hash — `frame_hash[i]` (gap 3, gap 4)

```
frame_hash[i] = BLAKE3( 0x01 || le64(i) || payload[i] )
```

- `i` is the frame's `stream_version` (0-based).
- `payload[i]` is the **event payload bytes as committed** (the subframe's `payload` field —
  post-codec/post-compression bytes exactly as they appear on disk; see `01-log-format.md`).
  Verification hashes what is stored, so the certificate binds the on-disk representation.
- Fixed-width fields precede the variable-length payload so the input is unambiguously parseable.

> **Decision — verification ALWAYS recomputes `frame_hash` from the payload; it is never trusted
> from storage (gap 3).** If a verifier trusted a *stored* `frame_hash[v]`, a tamperer who
> rewrites payload `v` and its stored hash defeats Path A entirely — nothing else re-reads frame
> `v`. Consequently **`frame_hash` is NOT materialized on disk at all** (see G10, §6): storing a
> value that must always be recomputed is pure redundancy. This also removes 32 bytes/frame from
> the naive layout.

### 3.3 Chain step — `h[i]` (gap 4)

```
h[i] = BLAKE3( 0x02 || h[i-1] || frame_hash[i] || le64(i) )
```

The three inputs after the tag are fixed-width (32 || 32 || 8), so the construction is
unambiguous. `h[i-1]` is the fold-chain value; for `i = 0` it is the genesis `h[-1]`.

This is the value D2 calls `prev_stream_hash` when stored on frame `i`'s successor:
`frame[i].prev_stream_hash == h[i-1]`. Because `prev_stream_hash` **is** the fold-chain value
(not merely the previous frame's hash), `h[v]` is computable from frame `v` alone (Path A, §7).

> Note: these constructions differ from the `fold_cert` spike (which omitted domain tags and put
> `payload` before `version`). The spike is a prototype; its pinned golden constants
> (`GOLDEN_EXPECTED_STATE_HASH` etc.) MUST be regenerated against this spec during Phase 5.

---

## 4. Versioning and the empty prefix (gap 2)

### 4.1 `stream_version` is a last-index, not a count

- `stream_version = v` means the snapshot summarizes events `0..=v` — that is `v + 1` events.
- `event_prefix_hash = h[v]`.
- `v` **MUST** be strictly less than the stream's committed event count; a `SnapshotRef` with
  `v >= count` is rejected as `SnapshotBeyondHead` (§7).
- Frame `0` carries the genesis as its `prev_stream_hash`, so a snapshot at `stream_version = 0`
  (after the first event) is a normal, certifiable case — no special handling.

### 4.2 The empty-prefix snapshot (`covers_empty_prefix`)

There is exactly one state that names **zero** events: `fold_init` (the aggregate's initial
state, having applied nothing). It has no event index. It is represented by
`flags` bit 0 = `covers_empty_prefix = 1`:

- `event_prefix_hash` **MUST** equal the genesis `h[-1]`.
- `stream_version` **MUST** be `0` and is ignored by the verifier.
- The certified state blob **MUST** deserialize to `fold_init`.
- Verification: check `event_prefix_hash == genesis(stream_id)`, check the blob, then replay the
  **entire** log `0..=head` as the tail (there is no prefix to skip). This is equivalent to a
  full verified replay but still exercises the head anchor.

> **Decision.** An empty-prefix snapshot is rarely materialized (it saves nothing), but a
> defined encoding is required so `stream_version = 0` is never ambiguously overloaded to mean
> both "after event 0" and "before any event." The explicit flag removes the ambiguity; a
> verifier that sees `stream_version = 0` with the flag clear knows unambiguously it means "after
> event 0." Rejected: reserving `stream_version = u64::MAX` as a sentinel (works but is a magic
> value that leaks into arithmetic); using a signed `-1` (the field is u64 elsewhere).

---

## 5. G6 — the trusted head anchor

> **G6 RESOLVED.** The head anchor lives in the **sealed segment footer** extension region
> (`01-log-format.md` §3.3), tiered with mess's active/sealed lifecycle (D5). It never lives
> *only* in the rebuildable index, and it is never derived by scanning the truncatable tail.
>
> **Normative ownership.** G6 — the fold-chain trusted head anchor `(head_version, head_hash)` —
> is defined **only here**. The footer's epoch/generation and position anchoring that make this
> location tamper-evident are *separate* machinery this doc depends on but does not own: they are
> `02-recovery.md`'s **A9/R3** (segment epoch chain) and its footer validation. A9/R3 are **not**
> G6; they are the substrate G6 rests on. Do not conflate the two.

### 5.1 The problem restated

Truncation detection (`HeadMismatch`) requires knowing the true `(head_version, h[head])` from a
place the attacker/corruption cannot reach by editing or truncating the log. If the head record
is rebuilt from the log during recovery, a truncated log **self-certifies**: the rebuilt anchor
matches the shortened log and the missing tail is invisible. Frame-level hashes cannot fix this —
it is an anchoring problem, not a chaining problem (gap 6).

### 5.2 The anchor and where it lives

The **head anchor** for a stream `S` is the pair `A(S) = (head_version, head_hash)` where
`head_hash = h[head_version]` over the committed prefix. It is resolved at load time from one of
two tiers, matching where `S`'s latest committed events physically are:

**Tier 1 — sealed (durable, attacker-resistant).**
At seal, the background sealer writes a `StreamHeadTable` into the **footer extension region**
(byte layout owned by `01-log-format.md` §3.3; the extension region is a durable, typed section
of the seal, not advisory): one entry per stream that has at least one committed event in that
segment.

Each `StreamHeadTable` entry MUST carry this semantic content (byte offsets owned by
`01-log-format.md` §3.3.2):

- `stream_id` (u64) — the interned stream id.
- `last_version` (u64) — the version of the stream's **last** event in this segment.
- `head_hash` (32 bytes) — `h[last_version]`, the fold-chain value after that event.

Properties that make this a valid anchor:

- **Outside the rebuildable index.** The footer is written once at seal and never
  read-modify-written. The in-memory index (D5) is *rebuildable* from the log; the footer is not
  part of it.
- **Checksum-protected.** The sealed footer's fixed trailer ends with a single `footer_crc`
  field whose coverage is the single contiguous range `[0, crc_off)` — the trailing-checksum case
  (`01-log-format.md` §5.2), **not** the R4 *split* coverage (that split brackets an interior
  checksum field and applies to the batch CRC, not the footer). The extension region carrying the
  `StreamHeadTable` is covered separately by its own `ext_crc` (`01-log-format.md` §3.3.1). A
  partial rewrite of either the trailer or the extension is detected by `02-recovery.md`'s footer
  validation.
- **Epoch-chained.** The footer carries the A9 segment epoch/generation (R3). A whole-segment
  removal or a stale recycled segment is caught by the A9 epoch chain during recovery
  (`02-recovery.md`), before any fold certificate is consulted.
- **Not derivable from a truncated tail.** `head_hash` was computed by the appender from the
  full in-memory chain at seal time and frozen. An attacker truncating sealed events would have
  to rewrite this footer's `StreamHeadTable`, every later footer that mentions `S` (each later
  `head_hash` transitively depends on all earlier events of `S`), **and** the A9 epoch chain —
  which the footer checksum and recovery kernel reject.

The **authoritative sealed head** for `S` is the `StreamHeadTable` entry from the **latest sealed
segment that lists `S`** (later footers supersede earlier ones for the same stream, because
`head_hash` already incorporates the earlier events through the chain).

**Tier 2 — active/unsealed (commit-protocol-anchored).**
A rolled-but-unsealed segment is normal under async sealing (D5). Its frames have no footer yet.
For a stream whose latest committed events are in an unsealed segment, `A(S)` is the appender's
**in-memory head**, which after a crash is re-derived from the accepted log prefix of the unsealed
segments (exactly the rebuild the D5 active index already performs). For the active tail there is
**nothing to detect**: truncation of an un-fsynced active tail is *legitimate loss* (D1: a torn
or missing marker means the batch was never committed). The accepted prefix after recovery **is**
truth, and everything up to the last seal point is still anchored by Tier 1.

> **Decision — two tiers, sealed footer is the authority.** Candidates were the segment footer
> and the manifest (gap 6). The **manifest is advisory, not truth** (D1; R2 says it merely
> *caches* footers). Anchoring truncation detection to an advisory structure would make the
> anchor forgeable by editing the cache. Therefore the **footer** is the durable authority; the
> manifest MAY cache `StreamHeadTable` entries for fast lookup (R2) but a verifier MUST fall back
> to reading footers if the cache is absent or stale, and MUST treat the footer as authoritative
> on any disagreement. The active tail deliberately relies on the commit protocol (A1–A12) rather
> than a second anchor, because inventing a durable per-append head record on the hot path would
> reintroduce the read-modify-write cost D5 exists to avoid, and would not detect anything the
> commit protocol does not already handle.

### 5.3 Durability and rebuild story

- **Write:** the sealer emits `StreamHeadTable` as part of the footer at seal (single background
  write, no hot-path cost). The head is bound by the footer checksum and A9 epoch.
- **Read (fast path):** the manifest cache (R2) yields `A(S)` in O(1) for sealed streams.
- **Read (fallback):** scan sealed footers newest-first for the latest entry naming `S`
  (O(#sealed segments) preads; R2's cache exists precisely to avoid this).
- **Rebuild after crash / lost manifest:** footers are re-read from sealed segments (they are
  never rebuilt from frame bytes — they *are* the anchor); the active head is re-derived by the
  normal D5 active-index rebuild over the accepted prefix of unsealed segments. **No recovery
  cost beyond what D5/`02-recovery.md` already pay** (CRC32C ~1,350 MiB/s; the opt-in crypto
  chain is not on the mandatory recovery path — R-notes confirm mandatory crypto would slow
  recovery 2.1–2.3×, which is why the chain stays opt-in per stream/category).

---

## 6. G10 — where `h[i-1]` lives in the batch format

> **G10 RESOLVED.** Materialize the chain **once per batch** in the `crypto_chain` field — a
> 32-byte value immediately after the 72-byte `BatchHeader` (offset 72), present iff the batch's
> `flags` bit 0 (`CRYPTO_CHAIN`) is set, inside the batch CRC coverage (byte layout owned by
> `01-log-format.md` §4.4); store **nothing per frame**; recompute intra-batch. Storage overhead
> (derived from measured batch shapes): **≤1.3% at 10-event batches, ~0.13% at 100-event
> batches**, versus **~25% flat** for the naive per-frame layout.

### 6.1 The problem restated

The naive layout stores `prev_stream_hash` (32 B) and `frame_hash` (32 B) on every subframe:
64 B on a ~250 B event ≈ **25% storage overhead**, flat, independent of batch size. This
dominates the real cost of the mechanism — the CPU is nearly free (§10). gap 10 asks where
`h[i-1]` physically lives.

### 6.2 The layout

A batch is single-stream: D2 hoists `stream_id` into the BatchHeader as a batch-constant field.
For a chain-enabled stream, a batch's frames are therefore a **contiguous run of that stream's
frames in ascending version order**. This is a normative constraint:

> **Batch/chain constraint.** For a stream (or category) with the crypto chain enabled, every
> committed batch contributing to that stream **MUST** contain only that stream's frames, in
> contiguous ascending `stream_version` order with no gaps. (Group commit — `03-durability.md` —
> coalesces *multiple streams'* batches into one fsync'd commit group; it does not interleave one
> stream's frames across batches.)

Storage:

- **Per batch:** the `crypto_chain` field carries `h[base_version - 1]` — the fold value
  *entering* the batch, i.e. the `prev_stream_hash` of the batch's first frame (`base_version` =
  the first frame's `stream_version`). For the batch containing frame 0, `crypto_chain = h[-1]`
  (genesis). It is **not** a `BatchHeader` field: it is a **32-byte value immediately after the
  72-byte `BatchHeader` (offset 72)**, present iff the batch's `flags` bit 0 (`CRYPTO_CHAIN`) is
  set — the chain is opt-in per stream/category (D2). Its placement and length are owned by
  `01-log-format.md` §4.4; this document owns its semantics. It **MUST** be inside the batch CRC
  coverage (A4; covered like the rest of the batch) so it cannot be tampered independently.
- **Per frame:** **nothing.** No `prev_stream_hash`, no `frame_hash` stored.

### 6.3 Intra-batch recompute

Every `h[i]` and every `prev_stream_hash` is reconstructed at read time from `crypto_chain`
by walking the batch's frames in order:

```
h = batch.crypto_chain                     # = h[base_version - 1]
for i in base_version ..= last_version:
    prev_stream_hash[i] = h                 # h[i-1]
    fh = BLAKE3(0x01 || le64(i) || payload[i])
    h  = BLAKE3(0x02 || h || fh || le64(i)) # h[i]
# on exit, h == h[last_version] == the batch's exit head
```

The batch's **exit head** `h[last_version]` is not stored per batch — it is recomputed here and,
for the durable anchor, recorded per stream in the sealed footer (§5). This keeps per-batch
overhead at a single 32-byte field.

### 6.4 Consequence for the verification paths

- **Path B** (read frame `v+1`'s `prev_stream_hash`) at a *batch boundary* is O(1): if frame
  `v+1` is the first frame of its batch, its `prev_stream_hash` is exactly `crypto_chain`.
  If frame `v+1` is mid-batch, its `prev_stream_hash` is recomputed from its batch's
  `crypto_chain` forward to `v+1` — bounded by intra-batch position (≤ batch size), and the
  batch is already resident because the loader reads it.
- **Path A** (recompute `h[v]` from frame `v`) requires frame `v`'s payload (to recompute
  `frame_hash`) and `v`'s `prev_stream_hash`. Both come from frame `v`'s batch: read the batch,
  recompute from `crypto_chain` up to and including `v`. Bounded by intra-batch position.
  Reading the batch is required anyway (Path A needs the payload), so the extra cost is the
  intra-batch hash walk — ≤ batch-size BLAKE3 steps over fixed 72-byte inputs (~cheap).

Neither path needs per-frame stored hashes. gap 10's worry ("Path A works only for frames at
batch boundaries") is resolved: Path A works for **any** retained frame, at the cost of an
intra-batch recompute that is bounded by batch size and free of extra I/O.

### 6.5 Overhead bound (derived from measured batch shapes)

Chain storage overhead is the arithmetic bound `32 / (N × payload_bytes)` where `N` = frames per
batch — not a directly measured storage figure, but a bound computed over the **measured**
composed-path batch shapes (`perf_group_commit`: 10- and 100-event batches; 250 B payloads):

| batch size `N` | payload | per-frame naive (64 B/frame) | **this layout (32 B/batch)** |
|----------------|---------|------------------------------|------------------------------|
| 1 (degenerate) | 250 B   | 25.6%                        | 12.8% (BatchHeader already dominates) |
| 10             | 250 B   | 25.6%                        | **1.28%** |
| 100            | 250 B   | 25.6%                        | **0.13%** |

The naive per-frame scheme is ~25% **regardless** of batch size; this layout is ≤1.3% at
realistic batch sizes and asymptotically zero. The `N = 1` case is only reached by a
single-writer sync-per-batch degenerate load, where the BatchHeader + CommitMarker fixed cost
(magic, `batch_id`, `frame_count`, `total_len`, `first_global_pos`, CRC, marker echo — on the
order of 60–100 B) already dominates, so the incremental 32 B is a minority of an
already-expensive batch.

> **Decision — per-batch entry hash + intra-batch recompute, no per-frame storage; drop the
> stored `frame_hash` entirely.** This is D2's hint ("materialize per batch, recompute within a
> batch") and gap 10's likely answer, quantified. Rejected: (a) per-frame `prev_stream_hash` +
> `frame_hash` — 25% overhead, the reason G10 was blocking; (b) per-batch *both* entry and exit
> hash (64 B/batch) — doubles the per-batch cost for a value (`exit = h[last]`) that is
> recomputable and already durably recorded in the footer (§5); (c) storing `frame_hash`
> per-frame as a "cross-check" — it is never trusted (§3.2) so it is pure redundancy.

---

## 7. Verification (`load_verified`)

`load_verified::<A>(stream_id)` runs the following steps in order. Each step names exactly which
frames it reads.

1. **Stream identity.** `ref.stream_id == requested stream_id`. No frame reads. (Redundant with
   the genesis binding in step 4, but yields a clearer error.)
2. **Fold version.** If `ref.fold_version != A::FOLD_VERSION`: the snapshot is **invalidated and
   the state rebuilt by full verified replay** (reads all frames). This is *not* surfaced as an
   error — it is the snapshot-invalidation-on-deploy story (§9). Return the rebuilt state.
3. **Snapshot bound.** `ref.stream_version < committed event count`, else `SnapshotBeyondHead`.
4. **Blob integrity.** `BLAKE3(state_blob) == ref.state_hash`, then decode the blob (else
   `StateHashMismatch` / `StateDecode`). No frame reads.
5. **Prefix certificate.** Discharge `ref.event_prefix_hash == h[v]` by at least one of Path A,
   Path B, Path C (§7.1). Reads frame `v` and/or `v+1` (§6.4), or the durable retention anchor
   (§8, gap 9). Neither path available → `NoCertificationPath`.
6. **Tail replay** `v+1 ..= head`, walked **batch by batch** (the tail's frames are grouped into
   batches; §6.2). Seed the running chain value `h` at the certified `h[v]` from step 5. Reads
   every tail frame. Walk the tail batches in ascending order (see §7.0 for why this is the only
   implementable shape under the G10 layout). Let `B0` be the **first** tail batch — the batch
   containing frame `v+1`. §6.4 permits the snapshot version `v` to fall *inside* a batch, so
   `B0` may begin *before* `v+1` (`base_B0 ≤ v`); the first tail batch therefore gets
   first-partial-batch handling, and the boundary rule below is stated with that in mind.

   For each tail batch `B` in ascending order:
   - **Batch-boundary continuity (the one independent check).** `B`'s stored `crypto_chain`
     (= `h[base_B − 1]`, CRC-covered per §6.2, independent of the running walk) MUST equal the
     current running `h`. Mismatch → `ChainBreakPrev{at_version: base_B}`.
     **First-partial-batch exception.** When `B == B0` **and** `base_B0 < v+1` (i.e. `v` is
     mid-batch, so frame `v+1` is not `B0`'s first frame), this check is **skipped**. `B0`'s
     stored `crypto_chain` is `h[base_B0 − 1]` — the value *entering* `B0`, **not** `h[v]` (the
     seed is mid-chain within `B0`, since the snapshot already summarized `B0`'s frames
     `base_B0 ..= v`). There is no stored value equal to the certified seed `h[v]`; that seed's
     independent operand is the prefix certificate discharged in step 5 (Path A/B/C) itself, not a
     stored `crypto_chain`. Comparing `B0.crypto_chain` against the seeded running `h` would
     spuriously raise `ChainBreakPrev{at_version: base_B0}` on honest data, so it is not performed.
     (When `base_B0 == v+1` the tail begins at a batch boundary and this check runs normally for
     `B0`: `B0.crypto_chain == h[v]`.)
   - **Intra-batch recompute + apply.** For each frame `i` in `start ..= last_B` — where
     `start = v+1` for the mid-batch first batch `B0` (its earlier frames `base_B0 ..= v` were
     already summarized by the snapshot, so re-applying them would **double-apply** state, and the
     seeded `h` already equals `h[v]`) and `start = base_B` for every other batch — in order:
     `stream_version == i` (reorder → `VersionOutOfOrder`); recompute
     `fh = BLAKE3(0x01 || le64(i) || payload[i])`; advance `h = chain_step(h, fh, i)`; then
     `state.apply(payload[i])`. There is **no** per-frame `prev_stream_hash == h` check: under
     §6.2 nothing is stored per frame, and the per-frame `prev_stream_hash` of §6.3 is *derived*
     from this same running walk, so such a check would be tautological.

   **Deferred comparison for a mid-batch `B0`.** When `B0`'s boundary check is skipped, the region
   `v+1 ..= last_B0` receives no independent comparison at `B0`'s own entry. That comparison is
   **deferred** to the *next* batch boundary: after applying `v+1 ..= last_B0` the running `h`
   equals `h[last_B0]`, and the first subsequent batch `B1` carries the stored, CRC-covered
   `crypto_chain = h[base_B1 − 1] = h[last_B0]`, whose continuity check (above) independently
   certifies everything walked since the seed. If `B0` is also the **final** tail batch, the
   deferral lands on the head anchor (step 7) instead. Detection granularity for tampering within
   `v+1 ..= last_B0` follows from this — see §7.0.
7. **Head anchor.** Final `h == A(stream).head_hash` (§5), else `HeadMismatch`. Catches
   truncation and any consistent whole-suffix rewrite.

### 7.0 Tamper-localization granularity is a batch, not a frame (normative)

The G10 layout (§6.2) deliberately stores **no per-frame hash** — no `frame_hash`, no
`prev_stream_hash`. The only tail operands independent of the running-h walk are (a) each batch's
stored `crypto_chain` at that batch's entry boundary, and (b) the durable head anchor (§5) at the
very end. Consequently:

- The tail replay has exactly **one** independent comparison per batch (the boundary continuity
  check in step 6) plus the head-anchor check in step 7 — **with one exception.** When the
  snapshot version `v` falls mid-batch (§6.4), the first tail batch `B0`'s boundary check is
  skipped (its stored `crypto_chain = h[base_B0 − 1]` is not the certified seed `h[v]`; §7 step 6),
  so `B0` carries *no* independent comparison of its own. `B0`'s independent comparison is
  **deferred** to the *next* batch boundary — the first subsequent batch's `crypto_chain`, which
  after the walk equals the running `h` iff `B0`'s replayed suffix is honest — or to the head anchor
  (step 7) when `B0` is the final tail batch. There is no second independent operand *inside* a
  batch, so there is **no frame-precise payload-tamper check** and none can be synthesized without
  reintroducing per-frame storage.
- A payload tamper inside a tail frame is therefore localized to the **batch** that contains it.
  It surfaces at the *next* batch's `crypto_chain` continuity check as
  `ChainBreakPrev{at_version = that next batch's base version}`; or, when the tamper is in the
  **final** tail batch (which has no succeeding `crypto_chain`), at the head anchor as
  `HeadMismatch`. Either way the certificate's core guarantee — the snapshot summarizes the exact
  committed prefix — holds; only the diagnostic *precision* is batch-granular.
- **Mid-batch first tail batch (§6.4, §7 step 6).** When `v` is mid-batch, `B0` contributes only
  its suffix `v+1 ..= last_B0` to the replay; its earlier frames `base_B0 ..= v` were summarized by
  the snapshot and are certified by step 5's prefix path (Path A recomputes `h[v]` from `B0`'s
  `crypto_chain` forward through `v`, so a tamper in `base_B0 ..= v` fails there as
  `PrefixHashMismatch`), not re-walked here. Because `B0`'s boundary check is skipped, a payload
  tamper anywhere in the replayed suffix `v+1 ..= last_B0` is localized no finer than the *next*
  batch boundary (`ChainBreakPrev` at that batch's base version), or the head anchor
  (`HeadMismatch`) when `B0` is the final tail batch — the same batch-granular surface as any other
  tail batch, with the region of ambiguity being `B0`'s tail suffix rather than a whole batch.
- The earlier frame-precise error `ChainBreakFrameHash{i}` promised by the `fold_cert` spike
  depended on a **stored** per-frame `frame_hash` as the independent operand. §6.2 deletes that
  storage, so that error is **retired** (§11). The spike's negative tests that asserted
  `ChainBreakFrameHash{at_version: 35}` / `ChainBreakPrev{at_version: 36}` at frame granularity
  MUST be regenerated for the batch-granular surface when the spike is reworked against this spec.

If a future requirement demands frame-precise localization, it MUST reintroduce a per-frame
durable anchor (reversing the §6.2 decision) — it cannot be recovered from the current layout.

### 7.1 The certification paths (both specified)

A snapshot at version `v` claims `event_prefix_hash = h[v]`. Three ways to discharge that claim:

**Path A — recompute from frame `v` (the frame itself).**
```
h[v] = chain_step( frame[v].prev_stream_hash, frame_hash(payload[v], v), v )
     = BLAKE3(0x02 || frame[v].prev_stream_hash || BLAKE3(0x01 || le64(v) || payload[v]) || le64(v))
compare to ref.event_prefix_hash.
```
- Reads: frame `v`'s **payload and `prev_stream_hash`** (the latter recomputed intra-batch from
  `crypto_chain`, §6.4).
- `frame_hash` is recomputed from the payload, never trusted from storage (§3.2, gap 3).
- **When it applies:** whenever frame `v`'s batch (payload) is retained. It is the **only** path
  when the tail is empty (`v` is the last committed event, so frame `v+1` does not exist).

**Path B — read frame `v+1`'s chain value.**
```
frame[v+1].prev_stream_hash == ref.event_prefix_hash    (byte compare, zero hashing)
```
- Reads: frame `v+1`'s `prev_stream_hash` only (its batch's `crypto_chain`, or an
  intra-batch recompute, §6.4). No payload hashing.
- **When it applies:** only when the tail is non-empty (frame `v+1` exists).
- **Trust model (gap 7):** frame `v+1`'s stored `prev_stream_hash` is attacker-writable in the
  same threat model as everything else. **Path B is a cheap pre-check, NOT a proof by itself.**
  A forged `prev_stream_hash` on frame `v+1` fools Path B locally but breaks during tail
  replay (step 6) at the next batch's `crypto_chain` continuity check (batch-granular, §7.0)
  and ultimately at the head anchor (step 7). It is sound **only in
  combination with** the full tail replay ending at the durable head anchor. A future
  optimization that skips tail replay because "Path B passed" would be **unsound and MUST NOT be
  implemented.** This is normative.

**Path C — durable retention anchor (when both frames are compacted).**
```
ref.event_prefix_hash == durable_h[v]     (from the segment footer's SnapshotAnchor, §8 gap 9)
```
- Reads: the footer/manifest `SnapshotAnchor` for `(stream_id, v)`. No frame reads.
- **When it applies:** when neither frame `v` (payload) nor frame `v+1` (header) is retained, but
  `h[v]` was durably recorded at snapshot/seal time (§8, gap 9). This is the retention-certificate
  case (doc 08 §8).

**Which path runs.** The verifier runs **every path whose inputs are available** and requires at
least one to succeed:
- If frame `v` is retained → Path A (authoritative; hashes the payload).
- If frame `v+1` is retained → Path B (one `memcmp`; belt-and-braces).
- If a durable `SnapshotAnchor` for `v` exists → Path C.
- All succeed independently; **any** mismatch is a hard `PrefixHashMismatch{path}`.
- None available → `NoCertificationPath{v}`.

The tail replay (step 6) and head anchor (step 7) run **regardless of which prefix path was
used** — they are the proof; the prefix path only certifies the starting point.

### 7.2 Cost (measured, `fold_cert` spike)

| tail (events) | `load_verified` |
|---------------|-----------------|
| 0             | 0.7 µs (two BLAKE3: blob + Path A) |
| 10            | 5.6 µs |
| 100           | 50 µs |
| 10,000        | 5.0 ms |

Linear at ~0.5 µs/tail-event (hash + fold) — verification adds essentially nothing beyond the
replay the loader must do anyway. Full-chain verification of a 1M-event stream: 504 ms
(1.98M ev/s, ~495 MB/s of payload, single core, in-memory; a real store is I/O-bound long before
hashing binds).

---

## 8. `fold_cert` REPORT gap disposition

Every one of the ten spec gaps found by the spike is dispositioned here.

| # | Gap | Disposition |
|---|-----|-------------|
| 1 | Genesis formula disagrees (08 vs 12), byte layout unpinned | **§3.1** — unified: `BLAKE3(0x00 \|\| "mess-stream-v1" \|\| le64(stream_id))`, byte-exact. |
| 2 | No empty-prefix representation; count-vs-index ambiguity | **§4** — `stream_version` = 0-based last-index; empty prefix = `covers_empty_prefix` flag (bit 0) with `event_prefix_hash = genesis`. |
| 3 | Path A must recompute `frame_hash`, not trust stored | **§3.2, §7.1 Path A** — always recomputed; and therefore `frame_hash` is not stored at all (§6.2). |
| 4 | Integer/field encodings + domain separation unspecified | **§3** — 1-byte domain tags `0x00/0x01/0x02`, `le64` little-endian, fixed-width-before-variable ordering. |
| 5 | `stream_id` in genesis load-bearing → MUST be required; opt-out streams | **§3.1** — mandatory `stream_id` binding. Opt-out streams: **§8.1** below. |
| 6 | Trusted head anchor unspecified; truncation depends on it | **§5 (G6)** — sealed segment footer `StreamHeadTable`, checksum + A9-epoch protected, outside the rebuildable index; active tail via commit protocol. |
| 7 | Path B trust model needs stating | **§7.1 Path B** — cheap pre-check only; proof is tail replay + head anchor; skipping replay is unsound (normative MUST NOT). |
| 8 | `fold_version` scope + fixture lifecycle undefined | **§9** below. |
| 9 | Retention can remove both frame `v` and `v+1` | **§8.2 + §7.1 Path C** — footer `SnapshotAnchor` records `h[v]`; retention MUST preserve a path. |
| 10 | Batch-level vs frame-level hashing interaction unspecified | **§6 (G10)** — per-batch `crypto_chain`, intra-batch recompute, no per-frame storage. |

### 8.1 Streams that opted out of the crypto chain (gap 5, tail)

The crypto chain is opt-in per stream/category (D2). A stream that opted out has **no
`crypto_chain`, no genesis binding, and cannot produce a fold certificate.** For such a
stream, `load_verified` degrades to:

- **blob integrity only** (`state_hash`), plus
- **full unverified replay** to obtain the tail (there is no `event_prefix_hash` to certify, no
  head anchor, no truncation detection).

This is stated so the degradation is explicit and honest: **D4 fold certificates require the
chain to be enabled for that stream/category.** A caller that needs the prefix/truncation
guarantees MUST enable the chain when the stream is registered (`04-registry.md`).

### 8.2 Retention preserves certification frames (gap 9)

Snapshots exist so old frames can be compacted, but an empty-tail snapshot whose frame `v` has
been archived has **no certification path** unless something is preserved. Normative rules:

1. **Compaction MUST NOT delete a frame that is the sole certification frame of a live snapshot**
   (i.e. frame `v` when frame `v+1` is also gone, or vice-versa), **unless** it has first
   recorded a durable `SnapshotAnchor` (rule 2).
2. **`SnapshotAnchor` (retention certificate).** At seal — or when compaction is about to remove
   the last certification frame for a live snapshot at version `v` — the sealer records in the
   **footer extension region** (`01-log-format.md` §3.3, alongside `StreamHeadTable`, §5) an entry
   carrying `stream_id`, the version `v`, and `h[v]`. This is Path C's input and is the doc-08-§8
   retention certificate: it proves `event_prefix_hash = h[v]` without reading any frame. Its
   durability/anti-tamper story is identical to the head anchor's (footer checksum + A9 epoch, §5).
3. Note that **Path A needs frame `v`'s payload** (to recompute `frame_hash`), not just its
   header; payload-only archival breaks Path A. If payloads are archived, retention MUST preserve
   frame `v+1`'s header (Path B) **or** a `SnapshotAnchor` (Path C).
4. `01-log-format.md` §3.3.2 owns the footer extension-region byte layout for `SnapshotAnchor`;
   `02-recovery.md` owns its validation.

---

## 9. `fold_version` and the drift test (gap 8)

`fold_version` is the D4 mechanism that answers "is this snapshot's fold semantically the same
fold my code implements now?". It is an **explicit, human-bumped `u32`**, declared on the
aggregate (`#[aggregate(fold_version = N)]`), **not** a code hash (code hashing is brittle: dep
bumps, inlining, and macro-output churn would spuriously invalidate every snapshot).

Normative rules:

- **Scope: per aggregate type**, not per stream. All snapshots produced by aggregate type `A`
  carry `A::FOLD_VERSION`. A `SnapshotRef` whose `fold_version` differs from the current
  `A::FOLD_VERSION` is **invalidated and rebuilt by full verified replay** (§7 step 2) — never
  surfaced as an error. This is the snapshot-invalidation-on-deploy story.
- **`apply` semantic change MUST bump `fold_version`.** This includes: changing how an existing
  event type folds, **and adding handling for a previously-unhandled event type** (old streams
  containing that event type now fold differently). Folds MAY skip unknown event types, but doing
  so means adding a *new handled* type is a semantic change that MUST bump the version.
- **Generated drift test (the guard against forgetting to bump).** The `#[aggregate]` derive
  generates a golden test: pinned fixture events + expected folded state (including a hash of the
  serialized state, so representation changes also trip it), committed to the repo. If `apply`
  semantics change without a `fold_version` bump, the test fails with "bump `fold_version` or fix
  your fold." The test also asserts `FOLD_VERSION == <pinned>` so a legitimate bump forces
  regenerating the fixture rather than silently reusing stale constants.
- **Fixture coverage SHOULD include at least one event of every handled event type.** The drift
  test only covers event types present in the fixture; a handled type absent from the fixture has
  no drift coverage. This is a known limitation; the derive tooling SHOULD warn.
- **Fixture lifecycle.** A legitimate bump regenerates the fixture + expected constants.
  **Old-version fixtures SHOULD be retained** to validate migration/replays of streams still on
  the old fold. (Fixture-regeneration tooling is a Phase 5 deliverable; this document owns the
  certificate and the rules, not the macro implementation.)

**Scope reminder (normative):** `fold_version` + the drift test guard *semantic* correctness of
the fold; the fold certificate (§1–§8) guards *prefix identity and integrity*. Neither proves the
other. `load_verified` documentation MUST state both scope limits.

---

## 10. Cost summary (measured)

Append-path chain maintenance (`fold_cert` bench, 1M events, 250 B payloads, release, single
core):

| append variant | ev/s | wall |
|----------------|------|------|
| raw append (no hashing) | 5,711,000 | 175 ms |
| frame_hash only (1× BLAKE3/event) | 2,011,000 | 497 ms |
| full chain (2× BLAKE3/event) | 1,509,000 | 663 ms |

- Chain maintenance on top of a store that already hashes frames: **+33% CPU, ~165 ns/event**
  (the second BLAKE3 is over 72 fixed bytes, far cheaper than the payload hash).
- Against the measured composed append path (~175k ev/s, `vertical_slice`), 165 ns/event is
  **~3% of the append budget** — noise.
- Storage (§6.5): **≤1.3% at 10-event batches**, versus ~25% for the naive per-frame layout.

The mechanism is sound and nearly free at runtime; the two costs that were blocking — storage
layout (G10) and the durable anchor (G6) — are decided above.

---

## 11. Errors (normative surface)

`load_verified` returns exactly one of:

| error | meaning |
|-------|---------|
| `StreamIdMismatch{expected, got}` | `ref.stream_id` names a different stream. |
| `SnapshotBeyondHead{snapshot_version, head}` | `ref.stream_version >= committed count`. |
| `StateHashMismatch` | blob does not hash to `state_hash`. |
| `StateDecode` | blob fails to deserialize. |
| `PrefixHashMismatch{path}` | a certification path (A/B/C) rejected `event_prefix_hash`. |
| `NoCertificationPath{version}` | no frame and no `SnapshotAnchor` can certify the prefix. |
| `VersionOutOfOrder{expected, got}` | tail frame at position `i` claims a different version (reorder). |
| `ChainBreakPrev{at_version}` | at a tail **batch boundary**, the batch whose base version is `at_version` carries a stored `crypto_chain` that does not equal the running chain value carried out of the previous batch (§7 step 6). Batch-granular: it localizes any divergence to the region ending at this boundary — either a payload tamper in the preceding tail batch(es) or a spliced/reordered batch (§7.0). **First-partial-batch case:** when the snapshot version `v` is mid-batch (§6.4), the first tail batch `B0`'s boundary check is skipped (§7 step 6) — `B0`'s stored `crypto_chain` is `h[base_B0 − 1]`, and there is no stored value equal to the mid-chain seed `h[v]` to compare against — so `ChainBreakPrev` is **never** raised with `at_version == base_B0` in that case. A divergence within `B0`'s replayed suffix `v+1 ..= last_B0` instead surfaces at the *next* batch's boundary (this error, with `at_version` = that next batch's base version, whose region-of-ambiguity extends back through `B0`'s suffix), or at `HeadMismatch` when `B0` is the final tail batch. |
| `HeadMismatch{computed, expected}` | final chain value != durable head anchor (truncation / suffix rewrite; also the surfacing point for a payload tamper in the **final** tail batch — §7.0). |

`ChainBreakFrameHash{at_version}` is **retired.** It was the `fold_cert` spike's frame-precise
payload-tamper error and depended on a stored per-frame `frame_hash`; the G10 layout (§6.2) stores
no per-frame hash, so payload-tamper localization is now batch-granular via `ChainBreakPrev` (or
`HeadMismatch` for the last batch), per §7.0. Reintroducing it would require reversing the §6.2
no-per-frame-storage decision.

A `fold_version` mismatch is **not** in this table — it triggers silent rebuild-by-replay (§7
step 2), not an error.
