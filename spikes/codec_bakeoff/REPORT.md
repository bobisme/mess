# Codec bake-off — payload codec + upcaster pipeline for Phase 1

Settles the open Phase 1 decision from `notes/mess-research/12_convergence.md`
("codec position — postcard alone rejected for payloads: not evolution-tolerant"):
**which payload codec is `codec_id 1`, and what does the upcaster pipeline look like.**

- Machine: AMD Ryzen 9 3900X, rustc 1.96.1, `--release` (opt-level 3, thin LTO), single thread.
- Corpus: 3 realistic event shapes (`OrderPlaced` — 11 fields with nested `Vec<LineItem>` +
  `Address`; `UserRegistered` — 9 fields with nested enum/struct; `ShipmentEvent` — 4-variant
  data-carrying enum). 10,000 test instances each, seeded realistic variance
  (name/city/SKU pools, random ids, timestamps, optional fields), plus 5,000 held-out
  training instances per type for dictionary training.
- Compression: zstd level 3; 16 KiB dictionary trained **per codec per event type** on the
  held-out samples only; block mode = 128 concatenated events per zstd frame.
- Candidates: `serde_json`, `ciborium` (CBOR), `rmp-serde` in **both** modes
  (`to_vec_named` = maps with field names; `to_vec` = positional arrays), `postcard`,
  `bincode` 1.x serde mode.

Everything below is reproducible: `cargo run --release` (tables), `cargo test --release`
(matrix assertions + upcaster fixtures). Raw run output: `run_output.md`.

---

## 1. Size

### Table 1 — mean encoded size (bytes/event, uncompressed)

| codec | OrderPlaced | UserRegistered | ShipmentEvent | overall |
|---|---|---|---|---|
| json | 640.1 | 281.7 | 147.2 | 356.3 |
| cbor | 508.8 | 224.0 | 110.7 | 281.2 |
| msgpack-named | 508.7 | 222.3 | 110.6 | 280.5 |
| msgpack-compact | 188.8 | 94.0 | 58.6 | 113.8 |
| postcard | 168.7 | 74.0 | 40.0 | 94.2 |
| bincode | 298.0 | 113.8 | 65.3 | 159.0 |

Uncompressed, the field-name tax is brutal: msgpack-named is 3.0× postcard. If this were
the final word, compact binary would win. It is not the final word.

### Table 2 — compressed size (bytes/event, overall mean)

| codec | raw | zstd-3 solo | zstd-3 +16KiB dict | 128-ev block | 128-ev block +dict | dict vs best-raw* |
|---|---|---|---|---|---|---|
| json | 356.3 | 246.4 | 75.1 | 57.7 | 53.1 | -20% |
| cbor | 281.2 | 225.1 | 72.8 | 56.3 | 51.4 | -23% |
| msgpack-named | 280.5 | 233.0 | 72.6 | 56.1 | 51.1 | -23% |
| msgpack-compact | 113.8 | 119.6 | 64.9 | 48.2 | 44.0 | -31% |
| postcard | 94.2 | 100.6 | 60.3 | 45.2 | 40.6 | -36% |
| bincode | 159.0 | 130.5 | 68.7 | 51.8 | 47.8 | -27% |

\* per-event dict-compressed size vs. the smallest **uncompressed** codec (postcard, 94.2 B).

**The key question — does verbose-but-evolvable + dictionary land close enough to compact
binary that evolvability is free? Yes, and then some:**

- msgpack-named + per-event dict = **72.6 B/event — 23% SMALLER than raw postcard** (94.2 B).
  The evolvable codec with a dictionary beats the most compact binary codec without one.
- Against postcard *with* the same dictionary (60.3 B), msgpack-named pays **+12.3 B/event
  (+20%)**. At 128-event blocks the gap is 51.1 vs 40.6 B (+26%, ~10 B/event).
  That ~10–12 bytes/event is the entire storage price of schema evolvability at rest.
- Field names are exactly what dictionaries eat: msgpack-named compresses 4.6× on
  OrderPlaced (508.7 → 111.7 B) while postcard only manages 1.85× (168.7 → 91.1 B) —
  postcard already removed the redundancy, so there is little left to compress. On the
  smallest events postcard + per-event dict is actually a *regression* (40.0 → 43.6 B,
  0.92×) because zstd frame overhead (~13 B: magic + dict-id + header) exceeds the win.
- Fixed points for the D2 frame format: per-event dict compression carries ~13 B/event of
  zstd framing (shaveable to ~4 B with the magicless format + explicit dict id in our own
  header); block compression amortizes it to nothing. Sealed blocks remain the real
  destination (cf. compression spike / D10), and there msgpack-named lands at 51.1 B/event.

### Per-type dictionary detail (full table in `run_output.md`)

| codec | type | raw | +dict | ratio |
|---|---|---|---|---|
| json | OrderPlaced | 640.1 | 110.2 | 5.81× |
| msgpack-named | OrderPlaced | 508.7 | 111.7 | 4.55× |
| msgpack-named | UserRegistered | 222.3 | 58.1 | 3.82× |
| msgpack-named | ShipmentEvent | 110.6 | 48.0 | 2.30× |
| postcard | OrderPlaced | 168.7 | 91.1 | 1.85× |
| postcard | UserRegistered | 74.0 | 46.3 | 1.60× |
| postcard | ShipmentEvent | 40.0 | 43.6 | 0.92× |

---

## 2. Speed

### Table 3 — throughput (30k events, best of 5 reps, single thread)

| codec | encode Mev/s | encode MB/s | decode Mev/s | decode MB/s | encode+dict-zstd Mev/s | dict-zstd+decode Mev/s |
|---|---|---|---|---|---|---|
| json | 1.69 | 604 | 0.91 | 323 | 0.47 | 0.63 |
| cbor | 1.91 | 538 | 0.62 | 175 | 0.53 | 0.49 |
| msgpack-named | 1.82 | 511 | 1.39 | 389 | 0.53 | 0.88 |
| msgpack-compact | 2.79 | 317 | 2.71 | 309 | 0.67 | 1.48 |
| postcard | 3.52 | 332 | 4.05 | 382 | 0.69 | 1.99 |
| bincode | 14.57 | 2317 | 4.95 | 787 | 0.81 | 1.78 |

- Among the evolution-safe (self-describing) codecs, **msgpack-named wins both directions**:
  decode 1.39 Mev/s = 1.5× json, 2.2× cbor; encode within 7% of the best named codec.
  ciborium's decoder is the laggard of the whole field (0.62 Mev/s / 175 MB/s).
- postcard decodes 2.9× faster than msgpack-named; bincode encodes 8× faster. That is the
  real cost of field names on the hot path — but at 1.39M decodes/s and 1.82M encodes/s
  single-threaded, the codec is nowhere near the bottleneck of a log that fsyncs batches.
- With per-event dictionary zstd in the loop the gap compresses: 0.88 vs 1.99 Mev/s
  (2.3×), because zstd dominates both.

Speed alone would pick bincode; bincode is disqualified below.

---

## 3. Evolution tolerance — the decisive test

Mechanical test: encode with struct V1, decode with struct V2, compare against semantic
expectation. `OK` = correct; `ERROR` = fails loudly (acceptable); `SILENT-WRONG` = decodes
successfully into garbage (**disqualifying** for an event store, where payloads outlive
code by years).

| schema change | json | cbor | msgpack-named | msgpack-compact | postcard | bincode |
|---|---|---|---|---|---|---|
| (a) added Option field | OK | OK | OK | ERROR | ERROR | ERROR |
| (b) added field w/ serde default | OK | OK | OK | OK | ERROR | ERROR |
| (c) removed field | OK | OK | OK | ERROR | ERROR | ERROR |
| (d) renamed field (no serde rename) | ERROR | ERROR | ERROR | OK† | OK† | OK† |
| (e1) reordered fields (same-typed adjacent swap) | OK | OK | OK | **SILENT-WRONG** | **SILENT-WRONG** | **SILENT-WRONG** |
| (e2) reordered fields (mixed types moved) | OK | OK | OK | ERROR | ERROR | ERROR |
| (f) enum: variant added at end | OK | OK | OK | OK | OK | OK |
| (g) enum: variants reordered | OK | OK | OK | OK | **SILENT-WRONG** | **SILENT-WRONG** |
| (h) int widened u32 → u64 | OK | OK | OK | OK | OK | ERROR |

† positional codecs "survive" a rename only because they never look at names — the same
property that produces the SILENT-WRONG rows. Not a feature.

Concrete proof of the disqualifying cells (from the run; locked in by
`tests/evolution_matrix.rs`):

- **(e1)** V1 `{ id, name, width: 1920, height: 1080, active }` decoded with V2 declaring
  `height` before `width`: postcard, bincode and msgpack-compact all return
  `height: 1920, width: 1080` — fields silently swapped, no error, every stream position
  still type-checks.
- **(g)** `EnumV1::Created(9)` decoded with variants reordered: postcard and bincode return
  `Deleted(9)` — a *different business event*, silently. (msgpack-compact escapes this one:
  rmp-serde encodes variant **names** even in compact mode — an empirical surprise.)

Findings worth keeping:

1. **postcard/bincode are confirmed rejected for payloads.** A refactor that reorders two
   same-typed struct fields, or reorders enum variants, silently corrupts every event
   already on disk. No test on current code catches it; only cross-version golden fixtures
   would — and by then the damage is designed in.
2. The three named codecs (json, cbor, msgpack-named) have **zero SILENT-WRONG cells**.
   Every tolerated change (add/remove/reorder fields, add/reorder variants, widen ints) is
   correct, and the one intolerable change (rename) fails **loudly** — which is precisely
   the trigger for "bump schema_version, write an upcaster."
3. Nuances: missing `Option` fields decode to `None` without any serde attribute (serde
   special-cases it); rmp-serde compact tolerates *appended* defaulted fields (arrays carry
   length) but not removed/reordered ones; postcard's varints make u32→u64 widening safe
   (h), but that is luck, not a contract.

Rules this buys us with msgpack-named: additive changes (new `Option`/defaulted field, new
enum variant) need **no** version bump; renames/unit-changes/type-restructuring need a
version bump + upcaster and cannot slip through silently.

---

## 4. Upcaster prototype (`src/upcaster.rs`, tests in `tests/upcaster.rs`)

Working pipeline: stored `(event_name, schema_version, codec_id, payload)` → decode as that
version's struct → upcast chain → latest type.

```text
StoredEvent { event_name, schema_version: u16, codec_id: u8, payload }
        │  codec_for_id(codec_id)?          -- unknown codec   => loud DecodeError
        │  match schema_version             -- unknown version => loud DecodeError
        ▼
   1 => TripCompletedV1 ──Upcast──> V2 ──Upcast──> V3
   2 =>                  TripCompletedV2 ──Upcast──> V3
   3 =>                                   TripCompletedV3 (identity)
```

Demonstrated with a 3-version event carrying **real semantic migrations**:

- V1→V2: field rename `driver` → `driver_name` — expressed in the upcaster, *not* with
  `#[serde(rename)]`; old bytes still say `"driver"` and only V1's struct ever reads them.
- V2→V3: unit changes `distance_miles` → `distance_m` (×1609.344) and `completed_at`
  seconds → `completed_at_ms`, plus an additive `rating: Option<u8>` (needs no upcaster
  logic — additive tolerance covers it).

What the user writes (one hop per version bump):

```rust
impl Upcast<TripCompletedV1> for TripCompletedV2 { fn upcast(v1: V1) -> Self { ... } }
impl Upcast<TripCompletedV2> for TripCompletedV3 { fn upcast(v2: V2) -> Self { ... } }
```

What the derive generates (prototyped as `event_versions!` in this spike):

```rust
event_versions! {
    name: "trip.completed",
    latest: TripCompletedV3,
    versions: [ 1 => TripCompletedV1, 2 => TripCompletedV2, 3 => TripCompletedV3 ],
    decode_fn: decode_trip_completed
}
```

which expands to transitive `UpcastsTo<Latest>` impls (pairwise composition of the one-hop
impls, identity for the latest) plus the `decode_to_latest` dispatcher. A real
`#[derive(Event)]` with `#[event(name = "trip.completed", version = N)]` generates exactly
this shape, and additionally the **compat test harness**:

- **Golden fixtures**: V1/V2 bytes committed as constants (`V1_FIXTURE`, `V2_FIXTURE` in
  `tests/upcaster.rs`) must decode through the chain to exact V3 values
  (`distance_m == 12.5 * 1609.344`, `completed_at_ms == 1_750_000_000_000`, rename applied)
  forever. This is the test that catches struct/serde/codec drift that would break stored
  data — the exact failure mode positional codecs make silent.
- Loud-failure tests: unknown schema_version, unknown codec_id, wrong event_name,
  truncated payload all return typed `DecodeError`s.
- A determinism guard (`encoder_still_produces_fixture_bytes`) pins the encoder side too.
- Fixture capture is one `--ignored` test (`print_fixtures`); a derive can emit a `#[test]`
  per committed fixture from attribute-supplied fixture paths.

Design notes for the real thing:

- Upcasting is **read-side and lazy**; the log is immutable, old versions are never
  rewritten. Snapshots interact via fold-version (D4), not via payload rewriting.
- The chain is linear by construction (version N has exactly one successor); the derive can
  enforce contiguity `1..=LATEST` at compile time.

---

## 5. Recommendation

**`codec_id 1` = MessagePack, named-field mode (`rmp_serde::to_vec_named`).**

Why msgpack-named and not the others:

- vs **postcard/bincode/msgpack-compact**: disqualified — SILENT-WRONG on same-typed field
  reorder; postcard/bincode also on enum-variant reorder ((e1), (g), proven above). An
  event store cannot ship a payload codec where a refactor corrupts history without an error.
- vs **json**: msgpack-named is 21% smaller raw (280.5 vs 356.3 B), 1.5× faster to decode,
  same evolution row, and transcodes losslessly to JSON for debugging/export (a `dump
  --json` flag, or JSON as a niche `codec_id 2` for human-in-the-loop streams).
- vs **cbor (ciborium)**: identical size and evolution behavior, but ciborium decodes at
  0.62 Mev/s vs 1.39 — 2.2× slower, the slowest decoder in the field.

**Does dictionary compression change the answer? Yes — it is what makes this answer cheap.**
Uncompressed, choosing msgpack-named over postcard costs 3.0× storage; that was the entire
case for compact binary. With 16 KiB per-type zstd-3 dictionaries, msgpack-named lands at
72.6 B/event — 23% *below* raw postcard — and within ~10–12 B/event of dictionary-compressed
postcard (51.1 vs 40.6 B at 128-event blocks). Evolvability is not free in bytes, but after
dictionaries it costs ~20–26% at rest instead of 200%, and it eliminates an entire class of
silent data corruption. Dictionary compression (already load-bearing per the vertical_slice
spike) should be treated as part of the codec decision, not an optimization afterthought.
Postcard remains fine for what it already does well: internal, frozen, versioned-by-us
frame/header structures — never domain payloads.

**Frame format requirements (refines D2):**

1. `schema_version: u16` is **per event**, placed next to `event_type_id` in the per-event
   header inside the batch frame — not frame-level (a batch mixes event types) and not
   inside the payload (the upcaster must dispatch *before* choosing the deserialize type).
   `(event_type_id, schema_version)` is the upcaster dispatch key.
2. `codec_id: u8` stays frame-level as in D2 (registry declares it per stream/event-type;
   codec 0 remains the frozen bootstrap codec, payload codecs start at 1 = msgpack-named).
3. Compression needs a `dict_id` (u16, 0 = none) alongside `compression_id` wherever
   compressed payloads appear: dictionaries are trained per codec × category at seal time
   and the exact dictionary bytes are required for decode forever → dictionaries must be
   stored durably (registry event pointing at a blob; dict ids never reassigned).
4. Use zstd's magicless frame (or raw content) inside our framing to reclaim the ~13 B
   per-event zstd header — measurable here: it is why per-event dict compression *loses*
   for postcard on small events (40.0 → 43.6 B).

**Rules to adopt with codec 1** (enforceable by derive + golden fixtures):

- additive change (new `Option`/`#[serde(default)]` field, new enum variant) → no version bump;
- rename, unit/type change, field split/merge → `schema_version += 1` + one `Upcast` impl;
- `#[serde(rename)]`/`#[serde(alias)]` on stored events is forbidden (use upcasters) —
  renames must fail loudly on old code, and they do;
- every released schema version keeps a committed byte fixture that must decode to latest.
