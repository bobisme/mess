# fold_cert spike — fold certificates (D2 prev_stream_hash + D4 SnapshotRef)

Prototype of the fold-certificate mechanism from `notes/mess-research/08_novel_mechanisms.md` §2
and `12_convergence.md` D2/D4. In-memory stream of frames, snapshot store, `load_verified`,
negative-test battery, fold-drift golden test, and 1M-event measurements.

Run: `cargo test` (21 tests) · `cargo test --features drift --test golden` (drift demo, fails
by design) · `cargo run --release --bin bench`.

## Construction (as implemented)

```text
frame_hash[i]   = BLAKE3(payload[i] || le64(i))
h[-1]           = BLAKE3("mess-stream" || stream_id)           genesis
h[i]            = BLAKE3(h[i-1] || frame_hash[i] || le64(i))   fold chain
frame[i].prev_stream_hash = h[i-1]                             (D2 storage rule)
```

`stream_version` is the 0-based frame index. A snapshot at version `v` summarizes frames
`0..=v`; `SnapshotRef { stream_id, stream_version = v, event_prefix_hash = h[v], state_hash =
BLAKE3(blob), fold_version }`. The stream keeps a trusted head anchor `head_hash = h[n-1]`
(genesis when empty).

## The verification algorithm (`load_verified`)

Steps, in order, with exactly which frames each reads:

1. **stream identity**: `ref.stream_id == stream.stream_id`. No frame reads. (Redundant with
   step 4 because the genesis binds stream_id, but gives a better error.)
2. **fold_version**: `ref.fold_version != Aggregate::FOLD_VERSION` → snapshot is **invalidated
   and the state rebuilt by full verified replay** (reads all frames). Not surfaced as an
   error; this is the snapshot-invalidation-on-deploy story.
3. **blob integrity**: `BLAKE3(blob) == ref.state_hash`, then decode. No frame reads.
4. **prefix certificate** — the two paths implied by the D2 storage scheme:
   - **Path A (from frame v alone)** — reads frame `v` (header **and payload**):
     `h[v] = BLAKE3(frame[v].prev_stream_hash || BLAKE3(payload[v] || le64(v)) || le64(v))`,
     compare to `ref.event_prefix_hash`. `frame_hash` is **recomputed from the payload** —
     trusting the stored `frame_hash` would make the certificate vacuous against tampering of
     frame `v` itself (spec gap 3). Available whenever frame `v` is retained; the **only** path
     when the tail is empty (frame `v+1` doesn't exist).
   - **Path B (from frame v+1)** — reads frame `v+1`'s **header only**:
     `frame[v+1].prev_stream_hash == ref.event_prefix_hash` (byte compare, zero hashing).
     Available only when the tail is non-empty. It trusts frame `v+1`'s stored header; that
     trust is discharged transitively by the tail replay + head anchor (test
     `forged_frame_header_fooling_path_b_caught_by_tail_replay`: a forged `prev_stream_hash` on
     frame `v+1` fools Path B locally but breaks at frame `v+2` during replay).
   - The implementation runs **both when both frames exist** (Path B is one memcmp — free
     belt-and-braces) and requires at least one; neither available → `NoCertificationPath`.
5. **tail replay** `v+1..=head` — reads every tail frame. Per frame `i`: check
   `stream_version == i` (reorder), `prev_stream_hash == running h` (chain link), recompute
   `frame_hash` and compare to stored (localizes payload tamper to frame `i` instead of `i+1`),
   then `h = chain_next(h, fh, i)` and `state.apply(payload)`.
6. **head anchor**: final `h == stream.head_hash`. Catches truncation and consistent
   whole-suffix rewrites.

Scope stays honest (D4): the certificate proves the snapshot summarizes the exact committed
prefix. It does **not** prove the fold code was correct — that is what fold_version + the
golden test are for.

## Measured overhead (1M events, 250 B payloads, release, single core)

Append path (`bench.rs`; numbers stable across runs, second run shown):

| append variant                    | ev/s        | wall time |
|-----------------------------------|-------------|-----------|
| raw append (no hashing)           | 5,711,000   | 175 ms    |
| frame_hash only (1× BLAKE3/event) | 2,011,000   | 497 ms    |
| full chain (2× BLAKE3/event)      | 1,509,000   | 663 ms    |

- Chain maintenance on top of a store that already hashes frames: **+33% CPU, ~165 ns/event**
  (the second BLAKE3 is over 72 fixed bytes, much cheaper than the 258-byte payload hash).
- Against a store with no hashing at all: 3.8× (but that baseline has no integrity story).
- Context: the vertical_slice spike measured the composed append path at ~175k ev/s. At that
  rate, 165 ns/event of chain cost is **~3% of the append budget** — noise.

`load_verified` cost by tail length (median of 5):

| tail (events) | load_verified |
|---------------|---------------|
| 0             | 0.7 µs        |
| 10            | 5.6 µs        |
| 100           | 50 µs         |
| 10,000        | 5.0 ms        |

Tail-0 load is two BLAKE3 calls (blob + Path A) — sub-microsecond. Cost is linear in tail
length at ~0.5 µs/event (hash + fold), i.e. verification adds nothing beyond the replay the
loader had to do anyway.

Full-chain verification of the 1M-event stream: **504 ms — 1.98M ev/s, ~495 MB/s** of payload
(single core, in-memory; a real store will be I/O-bound long before hashing binds).

## Negative tests (all DETECTED — `tests/negative.rs`)

| scenario | detected as |
|---|---|
| corrupted snapshot blob (bit flip) | `StateHashMismatch` |
| snapshot claiming wrong version | `PrefixHashMismatch(FromFrameV)`; Path B also rejects independently |
| tampered tail event | `ChainBreakFrameHash{at_version: 35}` — localized to the exact frame |
| tampered tail event, stored frame_hash fixed up | `ChainBreakPrev{at_version: 36}` — next link |
| tampered **last** event, frame_hash fixed up | `HeadMismatch` — **only** the head anchor catches it |
| tampered event **before** snapshot point | invisible to snapshot load (by design — prefix not re-read); detected by full replay at the tamper point (`ChainBreakFrameHash{at_version: 5}`) |
| fold_version mismatch | snapshot invalidated + rebuilt by full verified replay; correct state |
| truncated tail (head record intact) | `HeadMismatch` |
| truncation below snapshot version | `SnapshotBeyondHead` |
| reordered tail (adjacent swap) | `VersionOutOfOrder{expected: 30, got: 31}` |
| cross-stream snapshot (identical payloads, relabeled ref) | `PrefixHashMismatch(FromFrameV)` — genesis binds stream_id |
| **counterfactual**: genesis without stream_id | **attack succeeds** (test proves the binding is load-bearing, not decorative) |
| frame v+1 header forged to match bogus snapshot | Path B fooled locally; caught at `ChainBreakPrev{at_version: v+2}` during tail replay (and by Path A when frame v exists) |

## Fold-drift golden test (D4)

`tests/golden.rs` pins fixture events + expected folded state (balance, tx_count, and BLAKE3 of
the serialized state — the last also trips on representation changes). `cargo test --features
drift` simulates a developer changing `apply()` (withdrawals now charge a fee) without bumping
`FOLD_VERSION`; the test fails with:

```text
FOLD DRIFT DETECTED for Account (fold_version = 1):
fixture fold produced balance=105 ... expected balance=107 ...
apply() semantics changed but fold_version did not.
=> bump `FOLD_VERSION` (invalidating existing snapshots) or fix your fold.
```

The test also asserts `FOLD_VERSION == pinned version` so a legitimate bump forces regenerating
the fixture constants rather than silently reusing stale ones. Works exactly as D4 hopes; the
derive-generated version needs a fixture-regeneration affordance (see gap 8).

## Spec gaps found

1. **Genesis definition disagrees between docs and is under-specified.** 08 says
   `H("mess", stream_id)`, D2-era framing says `BLAKE3("mess-stream" || stream_id)`. Neither
   pins the byte layout (separator, length prefix, stream_id encoding). Any two
   implementations will silently diverge. Pick one, write down exact bytes.
2. **No representation for an empty-prefix snapshot ("version 0" ambiguity).** With 0-based
   versions, `stream_version = 0` means *after the first event* (works fine — frame 0 carries
   the genesis as its `prev_stream_hash`, test `snapshot_at_version_0_works_via_frame_0`). But
   there is no encodable "snapshot of nothing / h[-1]" — and more importantly the spec never
   says whether `stream_version` is a count or a last-index. Off-by-one bugs here corrupt
   nothing but invalidate every certificate. Must be pinned.
3. **Path A must recompute frame_hash from the payload — spec doesn't say.** If verification
   trusts the *stored* `frame_hash[v]`, a tamperer who rewrites payload v and its stored hash
   defeats Path A entirely (nothing else re-reads frame v). The stored hash is an optimization
   /cross-check only. Same question applies at every tail step; this spike recomputes always.
4. **Integer/field encodings in hash inputs unspecified.** `le64(i)` vs varint vs decimal,
   concatenation order, domain separation between frame_hash and chain_next inputs (here both
   are `32B || 32B || 8B` vs `payload || 8B`, unambiguous by length only by accident). Needs a
   one-page byte-exact spec, or cross-language verifiers will disagree.
5. **Cross-stream confusion: stream_id in genesis is load-bearing and must be REQUIRED.**
   Demonstrated: with an unbound genesis, a snapshot from stream A relabeled as stream B passes
   every check when payload prefixes coincide (and would deliver silently wrong state when they
   don't — e.g. templated/system streams with common prefixes). The `ref.stream_id` field
   alone does not protect against this since the attacker writes it. D2's "crypto chain is
   opt-in per stream" makes this worse: what certifies snapshots of streams that opted out?
6. **The trusted head anchor is unspecified — and truncation detection depends entirely on
   it.** `HeadMismatch` detection requires knowing `(head_version, h[head])` from somewhere the
   attacker/corruption can't touch, i.e. NOT derived by scanning the (possibly truncated) log.
   In mess terms: does the stream index store the fold-chain head? Is it covered by batch CRC +
   A1/A9 recovery? If the head record is rebuilt from the log during recovery, a truncated log
   self-certifies. This is the single biggest hole; frame-level hashes cannot fix it.
7. **Path B's trust model needs stating.** Frame v+1's stored `prev_stream_hash` is
   attacker-writable in the same threat model as everything else; Path B is only sound
   *combined with* full tail replay ending at the head anchor. A future optimization that
   skips tail verification ("we checked the cert via Path B") would be unsound. The spec
   should say: Path B is a cheap pre-check, the tail replay is the proof.
8. **fold_version scope + golden-fixture lifecycle undefined.** Per aggregate type? What about
   folds that skip unknown event types (this spike's `apply` ignores unknown tags — adding a
   *new handled* event type changes fold semantics for old streams containing it, with no
   drift-test coverage if fixtures lack that event type). Also: what regenerates fixtures on a
   legitimate bump, and are old-version fixtures kept (they should be — to validate migration
   replays)? Fixture coverage is only as good as the fixture events chosen.
9. **Retention: verification needs frame v or v+1 readable — compaction can remove both.**
   Snapshots exist so old frames can be cold/compacted, but an empty-tail snapshot with frame v
   archived has **no certification path** (`NoCertificationPath`). Options: retain frame
   headers (prev_stream_hash is enough for Path B-style checks) even when payloads are
   archived; or persist h[v] in the trusted stream index at snapshot time. Also note Path A
   needs frame v's *payload* (to recompute frame_hash), not just its header — payload-only
   archival breaks Path A too (demonstrated in `path_b_alone_suffices_when_frame_v_compacted`).
10. **Batch-level vs frame-level hashing interaction unspecified.** D2 batches carry one
    `batch_crc_or_hash`; the fold chain is per-frame. Are `frame_hash`/`prev_stream_hash`
    stored per subframe (32+32 bytes on every ~250 B event = ~25% storage overhead!) or
    recomputed at read time from the chain state? If `prev_stream_hash` is materialized only at
    batch heads, Path A works only for frames at batch boundaries. The storage story for h[i-1]
    needs a decision — it dominates the real cost of this mechanism (the CPU is nearly free).

## Verdict

**Worth it — the mechanism is sound and nearly free at runtime, but the spec is not
implementable as written.** CPU cost is a rounding error: +165 ns/event on append (~3% of the
measured composed-path budget; +33% only relative to bare hashing in-memory), sub-microsecond
snapshot certification, ~2M ev/s (495 MB/s) full verification. Every modeled attack short of
head-record compromise is detected, the D2 trick (prev_stream_hash = h[i-1]) genuinely makes
h[v] computable from frame v alone, and the D4 drift test catches semantic drift exactly as
intended.

The real costs are elsewhere and the spec is silent on both: (a) **storage** — materializing
two 32-byte hashes per ~250 B frame is ~25% overhead, so where h[i-1] actually lives in the
batch format (gap 10) decides the price; (b) **the trusted head anchor** (gap 6) — without a
durable, recovery-protected `(head_version, h)` record, truncation is undetectable and the
whole certificate chain anchors to nothing. Gaps 1–4 are cheap to close (write the byte-exact
spec); 5–7 are threat-model statements that must be written before anyone optimizes; 8–10 are
design decisions with real cost trade-offs. Proceed, but spec first.
