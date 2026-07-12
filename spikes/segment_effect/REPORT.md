# segment_effect — Spike D (bn-23x): algebraic segment effects + Merkle-page checkpoints

**Question.** Can compact per-segment algebraic effects (design §9) and
content-addressed Merkle-page checkpoints (design §10) reproduce full
recovery EXACTLY — under corruption, reordering, duplication, and a crash
at every install step — while changing startup asymptotics (open ≤ 10% of
full scan) and sustaining an effect-apply rate that makes design §18.3's
1.0 s open at 100M events arithmetically reachable (review/11 S3)? This is
the FAST-RECOVERY KILL POINT for the Asterism design.

**Model under test.** Logical capsules: v3-shaped user batches (exactly the
`AcceptedBatch` header fields the production scanner emits: `stream_id`,
`first_stream_version`, `frame_count`, `first_global_pos`) PLUS synthetic
v4 control stand-ins (`StreamRegistered`, `SnapshotInstalled`,
`ProjectionCheckpoint`, `DedupeKey`, `AllocatorSet`), so the full product
`K = H × S × P × R × D × A` is exercised. Two INDEPENDENT state
implementations — a deliberately boring `BTreeMap`/`VecDeque` oracle and a
dense-array kernel — share only the canonical digest SPEC (research/02
§14). `SegmentEffect` per design §9.7: path-composed head transitions with
FIRST (incoming prior-count) and LAST boundaries; right-biased
snapshot/allocator slots (allocators carry a first/last pair so
monotonicity survives composition); pointwise-max frontiers; conflict-union
registry delta (name→id AND id→name injectivity); position-window dedupe
epochs with whole-epoch expiry; canonical delta-varint encoding + CRC32C +
BLAKE3 effect hash; and the accepted-prefix anchor chain at both cursor
boundaries. Checkpoints: 4096-cell content-addressed pages
(`BLAKE3(kind || page_id || bytes)`), manifest with commit cursor + page
table + fold anchor + CRC32C, alternating `current.a`/`current.b` advisory
pointers, the §10.3 temp→verify→fsync→rename→dir-fsync install protocol —
run over a crash-modeling in-memory dir (volatile vs durable file content
AND directory entries, torn unsynced files, step-indexed fault injection)
plus a real `FsDir` for the perf run.

Per review/11 C3: production v3 segments carry EMPTY extension regions, so
the real-segment integration builds effects from batch HEADERS ONLY (heads
component), assumes no on-disk StreamHeadTable, and the control components
are proven on the synthetic model only.

**Verdict up front. PROCEED — every gate passes** (numbers below), and the
differential corpus EARNED its keep: it caught one real algebra gap at a
~4-in-100k rate that every targeted test missed (§3.1 — the frontier
semilattice needs an explicit bottom, a design-pack amendment), then passed
100k/100k after the one-line semantic fix.

---

## 1. Recovery variants (research/05 §8)

```text
v1 full sequential capsule-scan fold        (BTreeMap/VecDeque oracle)
v2 per-segment effect build + sequential ordered apply
   (every effect passes through the canonical encode/decode roundtrip)
v3 parallel effect build + ordered tree reduce (operands never permuted)
v4 full checkpoint at a random boundary + suffix fold
v5 incremental dirty-page checkpoints (2-3 ascending boundaries) + suffix fold
v6 corrupt/missing effect sidecar -> rescan THAT segment only
v7 corrupt page / missing page / torn manifest -> older manifest or effects
```

All seven run on EVERY corpus history; every digest must equal v1's.
The digest covers cursor, anchor, heads, snapshots, frontiers, registry,
allocators, and the EXACT-predicate live dedupe set (representation slack
can never leak in).

## 2. Digest-equivalence corpus (HARD GATE): PASS 100,000/100,000

Distribution (documented per plan): per-history capsule count 97% uniform
[100, 1e3], 2.9% uniform (1e3, 1e4], 0.1% uniform (1e4, 1e5] — measured
mean 743 capsules/history (74,290,628 total). Stream pool log-uniform
[1k, 100k]; 30% of batches hit a ≤64-stream hot set (multi-capsule streams
→ real path composition); segment boundaries every 16..=512 capsules;
dedupe span W ∈ {1k, 10k, 100k} positions, epoch span W/8; capsule mix
85/5/3/3/3/1 (user/registration/snapshot/frontier/dedupe/allocator), with
~10% of registrations idempotent re-registrations.

```text
CORPUS RESULT histories=100000/100000 all-digests-identical
  capsules=74290628 v6-rescans=100000 v7-effects-fallbacks=68692 wall=89.4s
```

(v6 rescanned exactly one segment per history — the victim, never more;
v7 recovered from an older manifest where one survived, else fell back to
effects, 68,692 times; digest equal in every case. Raw log:
`corpus_100k.log`.)

## 3. Injection matrix: PASS (rejected or safe fallback everywhere)

Every mutated log is RESEALED so its anchor chain is self-consistent —
rejection must come from the algebra's own validation, never from a
conveniently broken anchor. 300 seeds per class (`tests/injections.rs`),
plus targeted constructions (`tests/algebra.rs`,
`tests/checkpoint_faults.rs`); the corpus's v6/v7 add ~168k randomized
corruption/fallback cases on top.

| injection | where caught | result |
|---|---|---|
| head gap/overlap (intra-segment) | oracle fold ⊥; effect builder ⊥ | PASS |
| head gap/overlap (first touch in a segment) | oracle ⊥; compose/apply path-boundary ⊥ | PASS |
| global-position skew | oracle ⊥; builder ⊥ | PASS |
| registry conflict (name→id′ and id→name′; intra + cross segment) | oracle ⊥; builder/compose/apply ⊎-⊥ | PASS |
| allocator regression (intra + cross segment) | oracle ⊥; builder/compose/apply first/last boundary ⊥ | PASS |
| reordered effects (any pair) | compose `NotAdjacent` ⊥; apply `CursorMismatch` ⊥; state untouched (validate-before-mutate) | PASS |
| tampered same-stream effect with CONSISTENT cursors+anchors | `HeadPath` ⊥ from the path algebra itself (anchors could not have helped) | PASS |
| duplicate effect application | `OrderedApplier` identity skip = no-op; raw re-apply ⊥ `CursorMismatch`; digest unchanged both ways (research/02 §12) | PASS |
| corrupt effect sidecar (single byte flip anywhere; truncation) | CRC32C decode failure → that segment rescanned, rest from effects | PASS |
| stale anchor: manifest cursor beyond recovered log end | §10.4 reject → effects fallback, digest correct | PASS |
| stale anchor: wrong prefix (manifest from a foreign history) | §10.4 anchor/boundary mismatch → rejected (open = None) | PASS |
| wrong page hash / missing page | §10.4 page validation → older manifest or effects, digest correct | PASS |
| torn manifest | manifest CRC → older manifest chosen, digest correct | PASS |
| interrupted checkpoint install, EVERY protocol step (temp write / hash verify / file fsync / rename / dir fsync / manifest / pointer), ~40 steps × 12 histories | after crash: the old checkpoint or a completed-enough new one opens; digest == oracle at every single step | PASS |
| interrupted page GC at every delete step | BOTH retained manifests stay fully loadable (no reachable page ever deleted); digest == oracle | PASS |

### 3.1 The bug the corpus caught (design-pack amendment)

The FIRST 100k corpus run failed on 4 histories (seeds 22280, 37499,
49999, 59902 — all in v5): a `ProjectionCheckpoint` at **position 0**
CREATED a frontier map entry `(proj, shard) → 0`. That is a digest-visible
state change with NO value change, so latest-value dirty tracking never
marked the frontier component dirty, and the next INCREMENTAL checkpoint
reused the previous (entry-less) frontier blob → reconstructed state
diverged from the fold. Every targeted test missed it; the randomized
corpus found it at a ~4/100k rate.

**Amendment for design §9.4/§10.5:** the frontier semilattice needs an
explicit bottom — a checkpoint at the genesis position MUST be identified
with "absent" (⊥ = 0), OR entry *creation* must be treated as dirtying.
We chose ⊥ = 0 (cleaner: the join semilattice becomes total, every shard
implicitly at 0) and applied it uniformly in oracle, kernel, builder, and
apply. The general lesson generalizes past frontiers: **in any
latest-value component, "key now exists" is a state change that
value-comparison dirty tracking cannot see.** Heads/snapshots/allocators
are immune only because their writes unconditionally mark dirty.

## 4. Real-segment integration (review/11 C3): PASS

`tests/real_segment.rs`: a real store generated with the CURRENT engine
(`LogEngine::append_batch`, 2,500 appends, 1–4 × 512 B events each, 23
streams, 1 MiB segments → ≥3 sealed `seg-*.log` files), scanned with the
PRODUCTION scanner (`recover_segment_with_image`). Head-only
`SegmentEffect`s are built from real `AcceptedBatch` headers alone —
`frames()` is NEVER called, no extension region is read (C3: they are
empty in production). Asserted: intra-segment continuity from real
headers; ordered-tree-reduce compose + single apply == an independent
header-fold oracle == the scanner's own per-segment `stream_heads`
(modulo count-vs-last-version off-by-one convention); sequential
per-segment apply digest-equal to the composed apply; total scanned
positions == the engine's reported watermark.

## 5. Performance gates

Corpus/byte gates ran unguarded (byte-counting, not timing); `rate` and
`open` ran under the quiet guard.

### 5.1 Effect size (Measured): PASS

```text
EFFECTBYTES histories=500 touched-streams=344510
  head-bytes/stream avg=3.29  worst-effect=4.50   [gate <= 16]
  full-effect-bytes/stream=5.40  dedupe+control-bytes=574804 (excluded by gate)
```

Head component: delta-varint stream ids + varint (first_prior, added).
3.29 B/touched stream average, 4.50 B worst single effect — 3.6–4.9×
under the 16 B gate. (Dense small ids and small versions flatter this;
with 2^40-range ids like the registry stand-ins use, the id delta costs
~6 B/stream — still under gate.)

### 5.2 Incremental checkpoint proportionality (Measured): PASS

4M dense streams → 977 head pages; touch one stream on each of k% of
pages; measure incremental install's written blob bytes vs the full
checkpoint's:

```text
INCR streams=4000000 pages=977 dirty-pages=9  (1%)  ratio=1.03%   [expect ~1%]
INCR streams=4000000 pages=977 dirty-pages=48 (5%)  ratio=5.01%   [expect ~5%]
INCR streams=4000000 pages=977 dirty-pages=97 (10%) ratio=10.02%  [expect ~10%]
```

Bytes written scale with dirty pages, constant overhead = manifest +
component blobs.

### 5.3 Review-S3 apply rate (Measured): PASS — 24.3M head-transitions/s single-thread

Corpus: 10,000,000 capsules, 1M-stream pool, 37,925 segments, 7,383,052
touched-stream head transitions total (raw log: `rate_10m.log`).

```text
build effects, 1 thread     wall=1.873s   5.34M capsules/s
seq compose+apply, 1 thread wall=0.304s   24.31M head-transitions/s   [gate >= 5M: PASS, 4.9x]
tree-reduce+apply, 1 thread wall=7.041s   1.05M head-transitions/s
```

End-to-end parallel recovery (parallel effect build + ordered parallel
tree reduce + one apply; digest asserted equal to sequential at every
thread count):

| threads | build | reduce+apply | total | capsules/s | reduce transitions/s |
|---|---|---|---|---|---|
| 1 | 2.215s | 7.351s | 9.566s | 1.05M | 1.00M |
| 2 | 1.004s | 3.718s | 4.722s | 2.12M | 1.99M |
| 4 | 0.503s | 2.156s | 2.659s | 3.76M | 3.42M |
| 8 | 0.274s | 1.664s | 1.938s | 5.16M | 4.44M |

Build (the map phase) scales near-linearly (6.8x at 8t). The ordered tree
REDUCE is the slow path: composing 37,925 BTreeMap-based effects
materializes intermediate merged maps, so the single composed effect costs
~7s at 1t while direct sequential apply into the dense kernel costs 0.3s.
**Practical guidance for the adopting design:** parallelize the BUILD,
then apply per-effect sequentially into the resident dense state (or
chunk-reduce only within threads); reserve full tree composition for when
a single merged summary object is itself the product. The 5M/s gate is
against ordered compose+apply — the sequential-apply path passes at 4.9x,
and even the heavyweight full-tree path reaches 4.4M/s at 8 threads.

Derived §18.3 arithmetic: at 24.3M transitions/s, applying effects
covering 1M live streams costs ~40ms; a 100M-event log sealed into
256 MiB segments yields hundreds of effects, not tens of thousands, so
effect apply is comfortably inside the 1.0s open budget even before a
checkpoint shortcut (which the next gate shows is the dominant win).

### 5.4 Checkpoint open vs full scan (Measured): PASS — 4.90%

Largest feasible corpus per the plan: 10,000,000 capsules (~45M logical
events at the generator's 1-8 events/batch), 1M-stream pool, checkpoint at
the boundary covering 99% of capsules, suffix = 99,860 capsules (the
"active tail"), real `FsDir` on ext4, warm metadata (raw log:
`open_10m.log`).

```text
full-scan fold (oracle, BTreeMap)  wall=3.802s
full-scan fold (kernel, dense)     wall=1.589s   <- conservative denominator
install (full checkpoint)          wall=1.565s   494 blobs, 28.1 MB
checkpoint open + suffix fold      wall=0.078s
ratio vs kernel fold               4.90%          [gate <= 10%: PASS]
ratio vs oracle fold               2.05%
```

The opened state's digest is asserted equal to the full fold's. The
denominator is a PURE in-memory fold — production full recovery
additionally pays I/O + CRC + decode, so 4.90% is an upper bound on the
real ratio. Open cost decomposes as ~28 MB page/blob reads + hash
verification + 100k-capsule suffix fold; it depends on live state size +
tail, not history length (the asymptotic claim of design §10.1).

## 6. Gate table

| gate | result |
|---|---|
| all state digests identical on valid histories (≥100k) [hard] | **PASS** — 100,000/100,000, 74.3M capsules, 7 variants |
| all invalid histories rejected or safe fallback [hard] | **PASS** — full matrix incl. every-step install/GC crashes |
| checkpoint open ≤ 10% of full-scan time (largest feasible corpus) | **PASS** — 4.90% at 10M capsules / 1M streams (2.05% vs oracle fold) |
| no event payload decode for metadata recovery (structural) | **PASS** — model capsules carry no payloads; real-segment path never calls `frames()` |
| effect bytes ≤ 16 B/touched stream (+ dedupe/control separate) | **PASS** — 3.29 avg / 4.50 worst |
| incremental checkpoint bytes proportional to dirty pages | **PASS** — 1.03%/5.01%/10.02% at 1/5/10% dirty |
| review-S3: ordered compose+apply ≥ 5M head-transitions/s 1-thread | **PASS** — 24.31M/s seq-apply (4.9×); parallel build 6.8× at 8t |

## 7. Method / hygiene

Environment (Measured): AMD Ryzen 9 3900X (24 threads), rustc 1.97.0
(2d8144b78 2026-07-07), `--release`, `lto = "thin"`; Linux 7.0.12-arch1-1;
blake3 1.8, crc32c 0.6 (repo-pin majors), hashbrown 0.15. Every MEASURED
timing phase (`rate`, `open`) is preceded by the competing-load quiet
guard: no rustc/cc/ld anywhere, no cargo/bench outside our own ancestor
chain, load1 < 6.0 (this host carries a persistent ~4-5 ambient load;
floor per spikes/epoch_dedupe/REPORT.md §3). During this spike sibling
agents' build storms (load 45-60) were correctly held out by the guard:
575 guard holds logged during the rate run, 1 during the open run (both
in the committed raw logs).

Caveats:
- The "full scan" denominator for the open gate is a PURE IN-MEMORY fold
  of already-decoded capsules — the production full scan additionally pays
  I/O, CRC, and byte decode, so the measured open ratio is CONSERVATIVE
  (the production denominator is strictly larger). Both the kernel-fold
  (faster, used for the gate) and oracle-fold denominators are reported.
- The checkpoint perf run uses the real-filesystem `FsDir`
  (`$HOME/.cache/mess-bench`, ext4 — never tmpfs); open is timed warm
  (design §18.3's "warm filesystem metadata" case).
- Dedupe digests always apply the EXACT window predicate, so whole-epoch
  retention slack cannot leak into any equivalence result.
- Anchors: this model chains BLAKE3 over every capsule identity. That
  COST is included in all fold paths (conservatively inflating both
  numerator suffix-folds and denominators); a production implementation
  would anchor on existing batch CRCs/footers instead.
- The v5/v7 MemDir is a crash MODEL (volatile/durable split, torn
  unsynced files, atomic rename). It cannot prove kernel/fs semantics —
  it proves the PROTOCOL is crash-ordered correctly at every step.

## 8. Commands

```bash
cd spikes/segment_effect
CLANG_PATH=/usr/bin/clang cargo test --release      # 20 tests: algebra, corpus smoke,
                                                    # injections, checkpoint faults, real segment
CLANG_PATH=/usr/bin/clang cargo build --release
./target/release/bench corpus 100000 12             # >=100k digest gate    -> corpus_100k.log
./target/release/bench rate 10000000 1000000        # review-S3 apply-rate  -> rate_10m.log
./target/release/bench open 10000000 1000000        # open <= 10% gate      -> open_10m.log
./target/release/bench effectbytes 500              # <= 16 B/stream gate
./target/release/bench incr 4000000                 # dirty-page proportionality
./target/release/bench info
```

Raw logs for every table are committed alongside this report.
