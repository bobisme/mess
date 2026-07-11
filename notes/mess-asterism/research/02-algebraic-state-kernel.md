# Research 02: algebraic state kernel and composable recovery

## 1. Purpose

This note makes the “metadata is a fold of the log” claim precise enough to implement, property-test, and model-check. The goal is not decorative category theory. The goal is to derive three concrete capabilities:

1. a deterministic in-memory state kernel with no persistent KV truth;
2. compact per-segment summaries that can replace event-by-event metadata recovery;
3. parallel recovery whose result is provably identical to sequential log replay.

## 2. Accepted capsules and state

Let `C*` be the finite sequences of accepted commit capsules. Concatenation with the empty sequence makes `C*` a free monoid.

Define logical kernel state:

```text
K = H × S × P × R × D × A
```

where:

- `H : StreamId ⇀ Head` — partial map of stream heads;
- `S : StreamId ⇀ SnapshotRef` — latest usable snapshot;
- `P : ProjectionId ⇀ Frontier` — projection progress;
- `R` — registry state: ID/name bijections and immutable codec/dictionary objects;
- `D` — exact dedupe state for the defined recent window;
- `A` — auxiliary monotone counters/allocators such as next IDs.

Every capsule `c` has a deterministic interpreter:

```text
δ(c) : K ⇀ K
```

The arrow is partial. It is undefined when a capsule violates semantic invariants despite passing its physical CRC—for example a stream version gap, a conflicting registry assignment, an invalid snapshot prefix, or a duplicate registration ID with different bytes.

For a valid capsule sequence `cs = [c1, ..., cn]`:

```text
fold(K0, cs) = δ(cn)(...δ(c2)(δ(c1)(K0))...)
```

The core recovery requirement is:

```text
live incremental state == fold(K0, accepted durable capsules)
```

## 3. Stream updates as path composition

A one-stream capsule with `event_count = n` and `first_stream_version = v` represents a transition:

```text
T(s, v, n):  v -> v+n
```

Here `v` is the number of events already in the stream; after the capsule the last zero-based version is `v+n-1`.

Composition is defined when endpoints meet:

```text
T(s, v, n) ; T(s, v+n, m) = T(s, v, n+m)
```

and undefined when streams differ or `v+n != v2` for transitions that are claimed to be consecutive for the same stream.

This is category-like path composition. Its engineering value is that a segment summary for a stream needs only:

```text
stream_id
first_prior_count
final_event_count
first_global_position
last_global_position
last_capsule_pointer
```

The summary can validate continuity against the incoming state and advance the head without replaying intermediate events.

### Property

For any valid consecutive transitions `a`, `b`, `c`:

```text
(a ; b) ; c = a ; (b ; c)
```

because both represent the same path from the first start to the final end. This gives associative ordered reduction of stream summaries.

## 4. Right-biased map composition

For metadata whose state is “the latest assignment in log order,” represent a segment’s net effect as a partial map. Define:

```text
(A ▷ B)(k) =
    B(k), if k ∈ dom(B)
    A(k), otherwise
```

### Theorem: associativity

For any `A`, `B`, `C` and key `k`, the value of both `(A ▷ B) ▷ C` and `A ▷ (B ▷ C)` is the rightmost defined value among `C(k)`, `B(k)`, `A(k)`. Therefore:

```text
(A ▷ B) ▷ C = A ▷ (B ▷ C)
```

The empty map is identity. The operation is not commutative, which is correct: segment order matters.

This applies directly to:

- stream heads;
- snapshot-head slots;
- single-position projection checkpoints;
- next-ID allocator values;
- “latest configuration” records where overwriting is allowed.

## 5. Semilattice checkpoint composition

A future multi-shard projection checkpoint is a frontier:

```text
F : ShardId -> Position
```

with join:

```text
(F ⊔ G)(s) = max(F(s), G(s))
```

Pointwise maximum is associative, commutative, and idempotent. A checkpoint update can therefore merge retries or concurrent worker progress without order sensitivity.

For single-node v1, the frontier has one coordinate. Keeping the algebra in the state type avoids a future format break.

## 6. Registry as a conflict-detecting algebra

The registry is not last-write-wins. IDs and primary names are immutable.

Let one registry state contain partial maps:

```text
id_to_object : Id ⇀ ObjectBytes
name_to_id   : Name ⇀ Id
```

Define `R1 ⊎ R2` as union only when all overlaps agree byte-for-byte and the resulting name/ID relationships remain injective. Otherwise the result is `⊥` (corruption/conflict).

Properties on valid states:

```text
R ⊎ empty = R
(R1 ⊎ R2) ⊎ R3 = R1 ⊎ (R2 ⊎ R3)
```

The operation is commutative for disjoint immutable assignments, but aliases may have explicit commit-order rules. The simplest implementation keeps primary assignments immutable and stores aliases as a separate append-only set; name resolution rejects ambiguous aliases rather than silently selecting by time.

A SegmentEffect carries registry additions exactly once. If a checkpoint’s registry base conflicts with a later canonical record, the checkpoint is invalid.

## 7. Dedupe as an indexed set with a moving predicate

Let each committed dedupe record be:

```text
x = (scope, full_key, fingerprint, position, capsule_ptr)
```

At current durable end `w` and configured span `W`, membership is:

```text
live_w(x) iff x.position >= w-W
```

The logical dedupe set is:

```text
D(w) = { x in accepted history | live_w(x) }
```

The storage representation may retain a superset because whole epochs overlap the boundary. Exact query applies the predicate and compares full keys. Therefore:

```text
representation_superset != semantic false positive
```

It only creates candidate work.

A frozen epoch effect is immutable. Composition concatenates ordered epoch descriptors and drops epochs whose maximum position is below the exact boundary. This is associative when parameterized by the final watermark and applied in order.

## 8. The product effect

A segment effect is a tuple:

```text
E = (EH, ES, EP, ER, ED, EA)
```

Composition is componentwise:

```text
E1 ⊗ E2 = (
  EH1 ;/▷ EH2,
  ES1 ▷ ES2,
  EP1 ⊔/▷ EP2,
  ER1 ⊎ ER2,
  ED1 ++ ED2 with expiry,
  EA1 ▷ EA2
)
```

The head component uses path-aware right override: if both effects touch a stream, the end of the first transition must equal the start of the second. A mismatch yields `⊥`.

On valid ordered histories, `⊗` is associative and has an empty effect as identity. This is enough for ordered tree reduction.

## 9. SegmentEffect construction

During seal, scan capsules once and build mutable temporary accumulators:

```text
for capsule in segment order:
  validate capsule transition against temporary segment-local head
  first_touch[stream] ||= incoming prior count
  last_head[stream] = capsule final head
  last_snapshot[stream] = latest snapshot control
  checkpoint[projection] = join/replace
  registry_delta += immutable assignments
  dedupe_epoch_builder += dedupe record
```

Emit only net values and continuity boundaries. No event payload decode is necessary for head/checkpoint/registry mechanics when the control prelude and subframe lengths are independently parseable.

The builder also computes a canonical effect hash over sorted logical entries. The hash is not commit authority; it detects sidecar corruption and makes differential tests concise.

## 10. Ordered parallel recovery

### 10.1 Map phase

Each sealed segment can independently produce or load `E_i`:

```text
segment bytes -> validated capsule sequence -> E_i
```

This runs in parallel because it does not need the incoming global kernel state except to validate cross-segment transition boundaries. The effect records those boundaries for the reduce phase.

### 10.2 Reduce phase

Reduce effects in segment order. A parallel tree may compute:

```text
E_1_4 = (E1 ⊗ E2) ⊗ (E3 ⊗ E4)
E_5_8 = (E5 ⊗ E6) ⊗ (E7 ⊗ E8)
E_1_8 = E_1_4 ⊗ E_5_8
```

The tree must preserve the left-to-right order; it may not arbitrarily shuffle operands because right override is noncommutative.

### 10.3 Apply phase

Apply the final effect to the checkpoint state or genesis. Since the effect already stores final per-key values, this is proportional to distinct touched keys, not event count.

## 11. Checkpoint theorem

Let:

- `L = prefix ++ suffix` be the accepted log;
- `Kp = fold(K0, prefix)`;
- manifest `M` store `Kp` and a cryptographic/structural anchor identifying exactly `prefix`.

If `M` validates against the recovered log and all checkpoint pages validate, then:

```text
fold(Kp, suffix)
= fold(fold(K0, prefix), suffix)
= fold(K0, prefix ++ suffix)
= fold(K0, L)
```

The second equality is the monoid-action law for sequential application.

This is why a checkpoint can be advisory yet safe: validation either establishes that it is exactly a fold checkpoint, or recovery ignores it.

## 12. Idempotence boundaries

Not every effect is idempotent.

- Setting a head to the same final value is idempotent.
- Joining a frontier is idempotent.
- Reapplying an identical immutable registry assignment is idempotent.
- Inserting a dedupe record into a multiset is not automatically idempotent unless keyed by capsule identity.

Therefore every control/effect record carries a stable identity derived from `(segment_epoch, batch_id, control_ordinal)`. Effect application uses that identity where replay might otherwise duplicate a logical row. The canonical log scan itself never repeats capsules, but shadow migrations and repair tools must be safe under reapplication.

## 13. Retention and algebraic certificates

Deleting canonical event prefixes changes what can be recomputed. Before retention removes a prefix, create a retention boundary object containing:

```text
stream/version boundary
head transition/fold-chain anchor
snapshot reference + state hash + fold version
registry/checkpoint state needed beyond boundary
retention certificate hash
```

The boundary becomes the new genesis for that retained stream range. This proves continuity and prefix identity, not semantic correctness of the aggregate fold. The same honesty required by existing fold certificates applies.

## 14. Formal test oracles

Property generators should create random valid and invalid capsule histories and assert:

```text
incremental_apply(history) == full_fold(history)
fold(prefix) then fold(suffix) == fold(history)
compose(segment_effects) == full_fold(history)
sequential_effect_reduce == ordered_parallel_reduce
checkpoint+suffix == full_fold
reapplying effect identities does not duplicate state
any transition gap/overlap -> corruption
any registry conflict -> corruption
any exact dedupe query == reference deque/map model
```

The in-memory oracle should use deliberately boring `BTreeMap`/`VecDeque` structures so it does not share implementation bugs with the optimized kernel.

## 15. Why this is more than “store a checkpoint”

A conventional checkpoint serializes one whole mutable database state and later replays a WAL. The SegmentEffect construction adds two useful properties:

1. **hierarchical summarization:** each immutable segment is a reusable algebraic summary, so recovery can skip event detail even without a recent global checkpoint;
2. **local repair:** a corrupt effect invalidates only its segment summary, not the entire checkpoint chain;
3. **parallel map, ordered reduce:** the expensive scan work parallelizes while semantics stay exact;
4. **compaction-free metadata:** old events do not force current-state records through repeated LSM levels; they collapse once at seal.

That combination is the mathematical core of Asterism.
