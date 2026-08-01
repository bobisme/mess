# 02 — Recovery: scanning the log back to a committed prefix

Status: **normative**. This document specifies how a mess node reconstructs
committed state from the log after a restart — clean or crashed. It consumes
the byte format defined in [01-log-format.md](01-log-format.md) and produces
the *accepted prefix*: the longest run of batches that are committed under D1.

Recovery is the reader half of the commit protocol; the writer/durability half
is [03-durability.md](03-durability.md). The **manifest / segment catalog**
that seeds the fast path is owned by **this document** (§8.3, R2): it is an
advisory, implementation-defined cache of the sealed-segment trailers,
rebuildable from — and verifiable against — those trailers, and **never
authoritative** ([04-registry.md](04-registry.md) assigns the interned IDs but
does not define the manifest). Index rebuild is out of scope here (indexes are
rebuildable caches, D1). Cross-references to rule IDs (A1–A12, R1–R4) are to
[01-log-format.md](01-log-format.md) and the decision record; the fold-chain
head anchor (G6) is specified in
[05-fold-certificates.md](05-fold-certificates.md).

Key words are per RFC 2119 / RFC 8174.

The fault model and 24,000-case conformance bar are defined in
[01-log-format.md §1.3](01-log-format.md); everything below MUST hold under
it. This algorithm is the exact one whose accept/stop kernel is checked
exhaustively (1.83M states) in
[formal-model-commit-recovery.md](formal-model-commit-recovery.md); a
conforming scanner MUST route every accept/stop decision through that kernel
rather than re-implementing the rules.

---

## 1. What recovery computes

Recovery is a pure function of the durable bytes. Given the segment files it
returns, per segment and for the log as a whole:

- the **accepted batches**, in commit order;
- the **safe offset** in the active segment — the byte offset of the first
  invalid/incomplete batch, which is simultaneously the truncation point and
  the offset at which new appends resume;
- the resume state for the writer: `next_global_pos`, `next_batch_id`, active
  `segment_id`, and current `epoch`.

Recovery is **idempotent**: re-running it over the same durable image yields
the same result (proved as property 4 of the formal model). Because it is
pure over durable bytes, a crash *during* recovery changes nothing.

> **D1 (restated for orientation, not as a new rule).** A frame is committed
> iff it lies in a durable, marker-terminated batch that recovery accepts.
> The manifest and all indexes are advisory/rebuildable; recovery may
> disagree with them and recovery wins.

---

## 2. The per-segment scan

A segment scan starts just past the `SegmentHeader` (offset
`SEGMENT_HEADER_LEN = 52`) and walks batches forward. It carries two seeds
from the `SegmentHeader`: `expect_pos` (initialised to `base_pos`) and
`expect_epoch` (the segment's `epoch`).

### 2.1 Batch acceptance predicate

For each candidate batch at the current offset, in this order, the scan
performs the **byte-level** checks (owned by the decoder) and then the
**protocol** checks (owned by the acceptance kernel). Any failure stops the
scan (§3, A10); a batch is accepted all-or-nothing.

**Byte-level checks** (a failure is a byte fault — the batch, and everything
after it in this segment, is not committed):

1. **Remaining bytes ≥ `HEADER_LEN`.** Otherwise the header is torn → stop.
2. **`magic == HEADER_MAGIC` and `format_version == FORMAT_VERSION`.**
   Otherwise stop. (Garbage, a torn header, or a foreign/newer format.)
3. **A2 length cap:** `MIN_BATCH_LEN ≤ total_len ≤ MAX_BATCH_LEN`, and
   `total_len ≤ remaining bytes`. Otherwise stop. This bounds the read that a
   corrupted `total_len` in a partially-persisted header could otherwise
   trigger (A11: never trust the header just because its magic survived).
4. **A3 marker echoes:** at offset `total_len - MARKER_LEN`, `magic ==
   MARKER_MAGIC`, `total_len_echo == total_len`, and `batch_crc_echo ==`
   header `batch_crc`. Otherwise stop.
5. **A4 full-batch CRC:** recompute `CRC32C(bytes[0..68] ++
   bytes[72..total_len-4])` (the split coverage, R4, [01 §5.2](01-log-format.md))
   and require it to equal `batch_crc`. Otherwise stop. This scan performs this
   check for every candidate with no exception, as [01 §5.3](01-log-format.md)
   requires (A4 mandatory; A12 forbids any CRC-off path). It is the only check
   that catches a marker that persisted before its frames did — the defining
   reordering hazard.
6. **Subframe tiling:** the `frame_count` subframes MUST exactly tile the
   region `[HEADER_LEN (+CHAIN_LEN if flags.CRYPTO_CHAIN), total_len -
   MARKER_LEN)` — each subframe consuming `SUBFRAME_HDR_LEN + compressed_len`
   bytes, ending precisely at the marker. Any mismatch → stop. (Tiling is a
   structural check only; A12/[01 §5.3](01-log-format.md) records that tiling
   is *luck, not protection* — the CRC is what actually guards payload bytes.)

**Protocol checks** (the acceptance kernel; a failure here also stops the scan):

7. **A5 — no empty batches.** `frame_count ≥ 1`. A byte-valid batch with
   `frame_count == 0` is rejected → stop.
8. **A9 — segment epoch.** `segment_epoch == expect_epoch`. A byte-valid batch
   stamped with a different epoch is a stale prior generation left in a
   recycled segment; it MUST be rejected → stop. See §5.
9. **A1 — position contiguity.** `first_global_pos == expect_pos`. A byte-valid
   batch at the wrong global position is stale/recycled data and MUST be
   rejected → stop. See §4.

Only if all nine pass is the batch **accepted**. The scan then advances:
`expect_pos += frame_count`, `off += total_len`, record the batch.

### 2.2 Stop reasons

The scan stops on exactly one of: end of segment content; torn header (too few
bytes); bad magic/version; A2 bad length; incomplete batch (`total_len` beyond
remaining bytes); A3 bad marker; A4 bad CRC; bad frames (tiling); A5 empty
batch; A9 epoch mismatch; A1 position discontinuity. The **byte offset** at
which it stopped is the segment's `safe_offset`.

### 2.3 Pseudocode

```text
scan_segment(seg_bytes, header):
    off         = SEGMENT_HEADER_LEN            # 52; batches start after the header
    expect_pos  = header.base_pos               # A1 seed (from SegmentHeader / footer)
    expect_epoch= header.epoch                  # A9 seed
    accepted    = []

    loop:
        rem = len(seg_bytes) - off
        if rem == 0:               stop(EndOfSegment); break
        if rem < HEADER_LEN:       stop(TornHeader);   break        # A11: partial header, no trust

        h = read_batch_header(seg_bytes, off)
        if h.magic != HEADER_MAGIC:            stop(BadMagic);   break
        if h.format_version != FORMAT_VERSION: stop(BadVersion); break

        # --- A2: length cap + fits in remaining bytes ---
        if not (MIN_BATCH_LEN <= h.total_len <= MAX_BATCH_LEN): stop(BadLength);      break
        if h.total_len > rem:                                   stop(Incomplete);     break

        batch = seg_bytes[off : off + h.total_len]

        # --- A3: marker magic + length echo + crc echo ---
        m = h.total_len - MARKER_LEN
        if batch[m:].magic != MARKER_MAGIC
           or batch.total_len_echo != h.total_len
           or batch.batch_crc_echo != h.batch_crc:              stop(BadMarker);      break

        # --- A4 / A12: full-batch CRC over the R4 split coverage. MANDATORY. ---
        crc = CRC32C( batch[0:68] ++ batch[72 : h.total_len - 4] )
        if crc != h.batch_crc:                                  stop(BadCrc);         break

        # --- structural: subframes tile exactly (A12: necessary, not sufficient) ---
        if not subframes_tile(batch, h):                        stop(BadFrames);      break

        # --- protocol kernel: A5, A9, A1 (in this order) ---
        if h.frame_count == 0:                                  stop(EmptyBatch);     break   # A5
        if h.segment_epoch != expect_epoch:                     stop(EpochMismatch);  break   # A9
        if h.first_global_pos != expect_pos:                    stop(PositionGap);    break   # A1

        accepted.append(h)
        expect_pos += h.frame_count
        off        += h.total_len

    return { accepted, safe_offset: off, stop_reason, next_pos: expect_pos }
```

---

## 3. A10 — the first stop is terminal

> **A10.** Recovery MUST NEVER resynchronize past a hole. The scan stops at the
> first invalid batch, full stop, even if fully-valid batches exist beyond it.

Under sector reordering, batch *i+1* can be entirely durable while batch *i* is
not; batch *i+1* is then internally valid and would pass every byte and
protocol check **in isolation** (`spikes/torn_write` found **1,248** such
"resync bait" batches sitting past scan stop points across 24,000 cases). A
scanner that skips ahead to the next `HEADER_MAGIC` after a failure would
accept them and punch a hole in committed history, destroying prefix
consistency and ordering.

Therefore:

- The scan stop is **terminal**. Everything at and after `safe_offset` in the
  active segment is **dead space** — to be truncated or overwritten, never
  accepted. A conforming scanner MUST NOT contain any resync-to-next-magic or
  skip-a-bad-batch path.
- The A1 contiguity check (§4) is **not** a substitute for A10:
  stop-at-first-failure is the actual invariant. (A1 rejects a *mispositioned*
  valid batch; A10 forbids continuing at all after *any* stop, including
  positional ones.) In a correct implementation the two compose — the kernel
  latches its stop so that feeding it further candidates can never re-accept —
  but each is stated because dropping either was shown to admit a
  counterexample in the formal model's differential tests.

### 3.1 Z1 — dead space is dead by *rule*, not by erasure

Dead space beyond `safe_offset` is not committed, but it is not physically
gone either. A subtle, spec-**legal** consequence (formal model finding Z1): a
discarded unacknowledged batch beyond a hole can *resurface* across a later
crash. Sequence: recovery stops at a hole with a fully-persisted but unacked
batch `B` beyond it (A10 keeps `B` dead); the writer resumes at `safe_offset`
and rewrites the hole slot with the same frame count; a second crash persists
none of the rewrite. `B` now byte-validates at exactly the expected position
and epoch and **is accepted**.

This is legal — `B` was never acknowledged, its bytes are authentic
current-epoch writes, and surfacing an unacked-but-complete batch is A6's
permitted outcome (§6). But it means "dead space is dead" holds only until the
positions line up again. **Implementations MUST treat discarded unacked
batches as possible A6 duplicates**, absorbed by the dedupe window, not as
permanently unreachable. If resurfacing is ever unacceptable for a deployment,
the remedy is a *policy* choice — physically stamp or truncate at `safe_offset`
before the first post-recovery acknowledgement ([03-durability.md](03-durability.md),
committer) — not a change to these acceptance rules.

---

## 4. A1 — position contiguity

> **A1.** Marker validity alone is NOT sufficient acceptance. Recovery MUST
> also check contiguity: `first_global_pos == expect_pos`. A byte-valid batch
> at the wrong position is stale data (e.g. a recycled segment region holding a
> previous life of the file) and MUST be rejected.

`expect_pos` is seeded from the segment's `base_pos` and advanced by
`frame_count` per accepted batch. The seed is authoritative because it comes
from the checksummed `SegmentHeader`/`SegmentFooter` (never a filename — [01
D-FMT-2](01-log-format.md)). A1 was demonstrated necessary by `spikes/crash_log`:
a CRC-valid stale batch sitting after the last good batch, in recycled space,
passes every marker/CRC check and would resurrect deleted data; only the
position guard rejects it.

A1 alone is **not** enough against every recycled-space attack — see A9 (§5),
which the position guard cannot catch because the stale batch can sit at a
*coincident* position.

---

## 5. Epoch anchoring during recovery (A9, R3)

A9 (the mandatory segment epoch) and R3 (the trailer carries that epoch) are
stated normatively in [01-log-format.md §3.2, §3.3, §4.2](01-log-format.md);
this section is **reference-only** for the rules themselves and specifies how
recovery *applies* them. For orientation, A9 requires that recovery reject a
byte-valid batch whose `segment_epoch` differs from the scanned segment's
current `epoch` — see [01 §4.2](01-log-format.md) for the write-side stamping
obligation and the read-side rejection rule.

The attack A1 cannot stop (`spikes/torn_write`,
`recycled_segment_stale_batch_at_coincident_position_...`): a recycled segment
file still holds a stale prior-generation batch at offset 0 whose
`first_global_pos` **coincides** with what a fresh scan expects (e.g. a segment
reused for the same position range after an unclean rollback). The new write's
sectors all fail to persist — a legal crash outcome (zero-of-N pending sectors)
— so nothing new is on disk. Full validation *without* the epoch check (magic +
echoes + CRC + A1 contiguity) **accepts the stale batch** and resurrects
deleted data. The epoch is the only field that distinguishes "this batch was
written in the segment's current life" from "this batch is a fossil".

For A9 to have teeth during recovery, the writer and the durable epoch anchor
MUST cooperate:

- **Monotonic epochs.** Each fresh or recycled segment file MUST be stamped
  with a new `epoch` strictly greater than any previously durable segment's,
  and the `SegmentHeader` carrying it MUST be made durable **before** the first
  batch of that generation is written ([01 §3.2, §6](01-log-format.md)).
- **The trusted epoch anchor lives outside the rebuildable index.** The
  expected current `epoch` (and the head/segment it belongs to) MUST be
  recoverable from a durable anchor that is **not** rebuilt from the log — the
  sealed-segment trailers' `epoch` chain (R3) and/or the manifest (§8.3). An
  epoch anchor "rebuilt from the log" anchors truncation detection to nothing.
  Recovery MUST cross-check the active segment's `SegmentHeader.epoch` against
  this anchor; if the anchor names a newer epoch than the segment header on
  disk, the segment header itself did not survive and the segment is treated as
  containing no committed batches of the new generation. `prev_segment_epoch`
  ([01 §3.2](01-log-format.md)) provides the per-segment back-link for this
  check.

  The fold-chain trusted head anchor (**G6**) — the `(head_version, head_hash)`
  witness that rides in the sealed footer's `StreamHeadTable` alongside this
  epoch chain — is a separate, higher-layer construct specified normatively in
  [05-fold-certificates.md §5](05-fold-certificates.md); recovery here is
  concerned only with the A9 epoch anchor, on which G6 in turn depends.

Within a single segment scan, A9 reduces to the local check in §2.1 step 8:
`segment_epoch == expect_epoch`. The cross-segment/anchor obligation above is
what makes `expect_epoch` itself trustworthy.

---

## 6. A6 — unacknowledged-but-complete batches MAY surface

> **A6.** Recovery MAY legitimately surface a batch that was fully written but
> whose durability was never acknowledged (the write completed and all its
> sectors happened to persist, but `fsync` was lost or never returned).

This is the *permitted duplicate side* of the crash contract. Such a batch is
byte-valid, correctly positioned, and current-epoch, so it passes every check
in §2.1 and is accepted. That is safe: accepting a complete, authentic,
correctly-ordered batch never corrupts history; at worst it is a batch the
writer's caller did not yet believe was committed.

Consequences a conforming system MUST honour:

- **No acked batch is ever lost, and no *partial* batch is ever surfaced.**
  A6 permits surfacing only *complete* unacked batches (verified: 0 acked lost,
  0 partial/corrupt accepted across 24,000 cases). The asymmetry — an acked
  batch always recovers; an unacked complete batch *may* recover — is by
  design.
- **Duplicates are the caller's to absorb.** Because an unacked batch may or
  may not surface (and, per Z1 §3.1, a discarded one may resurface later),
  higher layers MUST be idempotent under replay of a not-yet-confirmed tail —
  the dedupe window is the mechanism. Recovery does not deduplicate;
  it reports the committed prefix as the bytes define it.

---

## 7. A7 — scan from the segment start; checkpoints are advisory

> **A7.** Segment boundaries align with batch boundaries (A8, [01
> §3.1](01-log-format.md)); recovery scans from the segment start. Checkpoint
> offsets are advisory-only and MUST NOT act as a second commit authority.

A segment scan MUST begin at the segment's own start (offset
`SEGMENT_HEADER_LEN`) and derive acceptance purely from the batch bytes.
Because A8 guarantees a batch never straddles a segment boundary, each segment
is independently scannable from its own `base_pos` seed. Any checkpoint,
cursor, or "last known good offset" a system keeps is a performance hint only:
recovery MAY start the *active-segment* scan from a checkpoint to save work,
but MUST be prepared to fall back to the segment start, and MUST NOT accept a
batch merely because a checkpoint pointed past it. The log's markers are the
sole authority (D1).

---

## 8. Whole-log recovery, and the fast path (R1, R2)

### 8.1 Ordering the segments

Segments are ordered by `segment_id`. Contiguity is threaded across the
boundary by `base_pos`: segment *k+1*'s `base_pos` MUST equal segment *k*'s
`end_pos` (its trailer's `base_pos + event_count`). This cross-segment
`base_pos`/`epoch` chain is the A1/A9 seed for each segment; it is also the
epoch-anchor material on which the fold-chain head anchor (G6,
[05-fold-certificates.md §5](05-fold-certificates.md)) depends (§5).

### 8.2 Full recovery (the authority)

Full recovery scans and CRC-validates **every** batch of **every** segment
(§2), rebuilding whatever index skeleton the reader needs. It is the
authoritative path (D1): footers and manifest are cross-checked but never
trusted in place of the bytes. Cost is ~1.1 s/GiB cold, dependent only on total
log size (`spikes/recovery_scale`, 10 GiB per segment-size configuration).

> **R1.** The recovery scan is CPU-bound (~1.3 GiB/s/core). Per-segment scans
> MAY run in **parallel**: because A8 makes segments independent and each
> segment's `SegmentHeader`/`SegmentFooter` supplies its own A1 `base_pos` seed
> and A9 `epoch`, a worker can scan segment *k* without having scanned segment
> *k-1*. The per-segment results are then stitched in `segment_id` order, and
> the cross-segment `base_pos`/`epoch` chain (§8.1) MUST be verified during the
> stitch (a segment that validates internally but whose `base_pos` does not
> continue its predecessor's `end_pos` breaks the log and MUST fail recovery).

### 8.3 Last-segment-only fast path

Only the **active** segment can contain the uncommitted tail; every sealed
segment is immutable once its footer is durable. So a warm restart MAY:

1. Trust each sealed segment via its footer **trailer** (one
   `SEGMENT_TRAILER_LEN` = 100-byte `pread` of the file tail,
   [01 §3.3.1](01-log-format.md)), reading `batch_count`, `event_count`,
   `epoch`, `base_pos`, `end_pos` without scanning its body. The trailer is
   self-checked by its own `footer_crc`; the fast path does **not** need to
   read the extension region (§3.3.2) to obtain these catalog fields;
2. Fully scan (§2) only the active (unsealed, trailer-less) segment, seeded by
   the last sealed segment's `end_pos`/`epoch`.

Measured: 256 MiB active segment → **0.32 s**, flat as the log grows.

**Footer validation.** When a reader validates a sealed segment's footer it
MUST check `footer_crc` over the trailer's `[0, 96)`. When `ext_len > 0` and
the reader consults the extension region (§3.3.2) — e.g. to resolve a
fold-chain head anchor (G6) or a `SnapshotAnchor`
([05-fold-certificates.md](05-fold-certificates.md)) — it MUST first read the
`ext_len` bytes at `ext_offset` and verify `CRC32C(extension) == ext_crc`.
When `ext_len == 0`, `ext_crc` MUST be `0` and there is no extension to read.
An `ext_crc` mismatch is handled per the Decision below; it does **not** by
itself invalidate the segment's committed batches.

> **R2.** The advisory **manifest / segment catalog is owned by this document
> (§1 intro):** an implementation-defined cache of the sealed-segment trailers
> (and, optionally, of extension-borne anchors) so the fast path is not
> `O(#sealed segments)` `pread`s. Recovery MAY read the manifest to obtain
> sealed-segment `base_pos`/`end_pos`/`epoch`/`counts` in one shot. **The
> manifest is advisory (D1), rebuildable from and verifiable against the
> footers, and never authoritative:** if it is missing, stale, or disagrees
> with a trailer, recovery MUST fall back to reading trailers, and full
> recovery MUST re-derive the counts by scanning and cross-check them against
> both manifest and trailer. A trailer or manifest entry MUST NOT be treated as
> a commit authority — it only *seeds and skips*; the bytes decide.

> ### Decision — recovery behaviour on a corrupt footer extension
>
> The trailer and the extension region are checksummed **separately**
> (`footer_crc` covers the trailer; `ext_crc` covers the extension,
> [01 §3.3](01-log-format.md)), so their failure modes are independent.
>
> - **Trailer valid, extension corrupt (`ext_crc` mismatch, or a section that
>   does not parse).** The trailer is still trusted: its `batch_count`,
>   `event_count`, `epoch`, `base_pos`, `end_pos` are covered by `footer_crc`
>   and serve the segment catalog and fast path unchanged — the segment's
>   committed batches are unaffected (each is independently CRC-validated by
>   §2; the extension plays no part in batch acceptance, A12). What is lost is
>   the durable fold anchors (`StreamHeadTable`, `SnapshotAnchor`). Recovery
>   MUST treat the extension as **absent** — it MUST NOT trust a partially
>   readable extension — and rebuild the affected anchors by **fully scanning
>   the segment** (§2) and recomputing each chain-enabled stream's
>   `h[last_version]` from the batches' `crypto_chain` and payloads
>   ([05 §6.3](05-fold-certificates.md)); a `SnapshotAnchor` that cannot be
>   re-derived (its certification frames were already compacted) makes only
>   that snapshot's Path C unavailable ([05 §7.1](05-fold-certificates.md)),
>   not the segment invalid. An implementation SHOULD re-seal a corrected
>   extension once rebuilt. This is the same forward-compat/advisory-skip
>   posture as the repair sidecar, applied to the whole region on a hard CRC
>   failure.
>
>   **Exception — a named SealPack ([01 §3.3.3](01-log-format.md)).** "Treat
>   the extension as absent" restores an anchor by *rebuilding* it, which is
>   always safe. It is NOT safe for the `SealPackIdentity` section, because
>   "absent" there reads as "this footer named no pack", which grants any
>   same-coverage pack the legacy coverage-only trust — so one flipped bit
>   anywhere in the extension would *downgrade* the segment's trust rather than
>   degrade it. The trailer's `SEAL_PACK_IDENTITY` flag is covered by
>   `footer_crc`, not by `ext_crc`, exactly so this cannot happen: when the flag
>   is set and the extension does not verify, the reader MUST install **no**
>   pack for that segment and serve it from the raw bytes (01 §3.3.3 reader rule
>   2). The anchors carried in the same corrupt extension are still handled by
>   the paragraph above; the two rules are independent because they degrade in
>   opposite directions.
> - **Trailer itself invalid (`footer_crc` mismatch, wrong `magic`/version, or
>   a short tail).** The segment is treated as **not sealed**: it MUST be
>   fully scanned (§2) exactly as the active segment is, and its counts and
>   anchors re-derived from the batch bytes. A missing or torn trailer means
>   the seal did not complete; the log is authoritative regardless.
>
> In both cases recovery loses no committed batch and trusts no unverified
> bytes: the extension is a durable *convenience* for the anchor lookups, and
> its corruption degrades to a scan, never to acceptance of bad data.

### 8.4 Fast-path/full-recovery equivalence

The fast path and full recovery MUST agree on the committed prefix. A
conforming implementation SHOULD periodically (and MUST, when a footer/manifest
disagreement is detected) run full recovery to confirm the fast path has not
been seeded from a corrupted footer. The `spikes/recovery_scale` harness
asserts this equivalence on every configuration; it is a conformance
requirement, not an optimization detail.

---

## 9. Rule index for this document

Each rule appears exactly once normatively, here or in
[01-log-format.md](01-log-format.md).

| Rule | Where (this doc) | Gist |
|---|---|---|
| A1  | §4 | Position contiguity: `first_global_pos == expect_pos` |
| A6  | §6 | Unacked-but-complete batches may surface (permitted duplicate side) |
| A7  | §7 | Scan from segment start; checkpoints advisory-only |
| A10 | §3 | First stop is terminal; never resync past a hole (+ Z1 dead-space) |
| R1  | §8.2 | Parallel per-segment scans permitted |
| R2  | §8.3 | Manifest / segment catalog is this doc's advisory cache of trailers |

Rules A2, A3, A4, A5, A8, A9, A11, A12, R3, and R4 are stated normatively in
[01-log-format.md](01-log-format.md) and are *referenced* — not restated — in
the scan predicate (§2.1) and the epoch anchoring of §5 where they bite. The
fold-chain trusted head anchor (**G6**) is stated normatively in
[05-fold-certificates.md §5](05-fold-certificates.md); §5 and §8 here only
reference it.
