# Seed-throughput profile — spike `bn-1jg`

**D10 measurement spike (evidence first, no production changes).** Why does the
social seeder average **~2.3 ms/command** (large tier: 54,680 events / 126 s,
pipelined k=32, from the bn-o9z run) when the hot-post bench warm path is
**~53 us/command** — a ~40x gap?

## TL;DR — the answer

**H1 dominates, and the mechanism is a per-new-stream `fdatasync`, not interning.**
The seeder makes almost every command a *brand-new stream*. On a new stream,
`LogEngine::append_batch` must durably persist the new stream **name→id**
registry mapping before the covering append can become durable (bn-150's
co-durable name flush). That persist is a real `fjall PersistMode::SyncAll`
(`crates/mess-index/src/meta/mod.rs:456`), dispatched via `spawn_blocking`
(`crates/mess-store/src/engine.rs:1775-1783`) for **every** new stream —
**regardless of the log committer's `Durability::Process`** (which itself does
no fsync).

Measured on this host, within one run:

| append target | `append_batch` mean | source |
|---|---|---|
| **new** stream (workload a) | **3.4344 ms** | one meta `SyncAll` fsync + in-mem append |
| existing stream, cold cache (workload c) | 0.0342 ms | in-mem append, no name flush |
| existing stream, warm (workload b) | 0.0294 ms | in-mem append, no name flush |

**Per-new-stream fsync cost = 3.4344 − 0.0342 ≈ 3.40 ms**, which is
**3.40 / 3.44 ≈ 98.8%** of the entire new-stream command latency. Everything
else the cold miss does — the snapshot-store lookup (H2), the empty/tail stream
read (H5), the decide/encode, the cache probe and write-through — sums to
**< 0.01 ms combined**.

The ~2.3 ms the seeder sees is this same per-new-stream fsync, weakly amortized
by its k=32 pipeline (see H4 — pipelining only bought ~1.3x here because all the
new-name persists serialize on one shared `MetaStore`).

---

## Host & provenance

- **Host:** 24-thread CPU, consumer NVMe. `/home` on `/dev/nvme0n1p3`, 78% full.
- **Load average during the run:** started 13.2, climbed to ~34 mid-run, settled
  ~16 (a busy shared host — see the cross-run caveat under H3/H4). The headline
  H1 result is a **within-run** (a)-vs-(c) delta, so it is immune to this drift.
- **Profile:** `--release` (optimized). Debug numbers are excluded (they inflate
  6-7x and are worthless here).
- **Workspace commit:** `43e4aca0192f01bb47670627f41182bca182759e` (wave-27
  trunk, includes bn-3dy registry-keyed meta).
- **Store:** `EventStore<FjallSnapshotBackend<LogEngine>>`,
  `Durability::Process`, `with_cache_capacity(4096)` — the seeder's exact
  construction (`examples/social/src/store_backend.rs::open_store`). Real fs
  under `TMPDIR=$HOME/.cache/mess-test-tmp` via `mess_testkit::sweeping_temp_dir`.
- **Date:** 2026-07-10.

### Exact command

```
CLANG_PATH=/usr/bin/clang TMPDIR=$HOME/.cache/mess-test-tmp \
  cargo test -p social --release --test seed_profile seed_profile \
  -- --ignored --nocapture --exact
```

Harness: `examples/social/tests/seed_profile.rs` (an `#[ignore]`d integration
test, hot-post-bench style — no `mess-*`/social production path touched). Sizes
are env-overridable (`SEED_PROFILE_NEW/WARM/COLD/PLAIN/PIPE/PIPE_SEQ/K`);
defaults 4,000 / 20,000 / 4,000 / 4,000 / 8,000 / 2,000 / 32. Total measured
runtime ~110 s.

---

## Method

Two independent instruments, both in the harness only:

1. **`ProfilingBackend<B>`** — a delegating wrapper implementing
   `Backend + SnapshotStore + SubscribeBackend` that records per-call latency
   histograms for `append_batch`, `read_stream`, `head`, `read_global`,
   `load_snapshot`, `save_snapshot`. One `Instant::now()` + one `Vec` push per
   call, guard never held across the delegated `await`.
2. **`timed_command`** — a faithful copy of `EventStore::command_cached`'s
   cold-miss path built only from the public surface (`cache().get` ->
   `load_cached` -> `decide` -> `append` -> write-through fold + `cache().put`),
   with a stopwatch around each phase. The phase means sum to the measured
   per-command total; the unexplained remainder is reported as an explicit
   **residue** row.

Aggregate under test: `Rel` — one tiny event per command (the seeder's
payload-light relationship/like shape). `decide` always emits one event, so a
stream can be touched repeatedly (workload c appends a second event to an
existing stream).

Workloads (all release, deterministic — stream names from a counter, fixed
shape, no `Date::now` in any comparison):

- **(a) all-new-streams, sequential** — the seeder shape.
- **(b) warm one-stream** — the hot-post-bench control (cache hit every call).
- **(c) all-existing, cold cache, first-touch** — fill N streams (setup), then a
  first-touch command each over a *fresh* cache: cache miss + snapshot lookup +
  tail load, but the append is to an already-interned stream (no name flush).
- **(d) all-new-streams over plain `LogEngine`** — H3 wrapper isolation, via the
  base `command` path (plain engine is not a `SnapshotStore`).
- **(e) all-new-streams sequential vs pipelined (k=32)** — H4.

---

## Results

### (a) all-new-streams, sequential — the seeder shape

`wall/n = 3.4444 ms/cmd` (n=4,000).

| phase | n | min | p50 | mean | p99 | max |
|---|---|---|---|---|---|---|
| 1 cache probe | 4000 | 0.0001 | 0.0005 | 0.0005 | 0.0013 | 0.0085 |
| 2 load_cached (miss) | 4000 | 0.0007 | 0.0030 | 0.0037 | 0.0110 | 0.0412 |
| 3 decide | 4000 | 0.0000 | 0.0001 | 0.0001 | 0.0003 | 0.0009 |
| **4 encode+append** | 4000 | 1.6983 | **2.6916** | **3.4363** | 8.2114 | 178.96 |
| 5 write-through+put | 4000 | 0.0002 | 0.0011 | 0.0020 | 0.0234 | 0.0533 |
| = measured total | 4000 | 1.7013 | 2.7000 | 3.4431 | 8.2235 | 178.97 |

*mean attributed = 3.4426 ms; mean total = 3.4431 ms; **residue = 0.0005 ms*** —
the decomposition adds up essentially perfectly. (ms throughout.)

Backend calls (ProfilingBackend): `append_batch` mean **3.4344 ms** (p50 2.6896),
`load_snapshot` mean 0.0019, `read_stream` mean 0.0008. So the append phase *is*
the backend `append_batch`, and the backend call is essentially all of it.

### (b) warm one-stream — the bench control

`wall/n = 0.0305 ms/cmd` (n=20,000) — reproduces the ~53 us bench claim (30 us
here on a marginally faster in-mem append).

| phase | mean | p50 | p99 |
|---|---|---|---|
| 1 cache probe | 0.0001 | 0.0001 | 0.0002 |
| 2 load_cached | (cache hit — never called) | | |
| 3 decide | 0.0000 | 0.0000 | 0.0001 |
| 4 encode+append | 0.0298 | 0.0263 | 0.0679 |
| 5 write-through+put | 0.0002 | 0.0002 | 0.0005 |
| = measured total | 0.0304 | 0.0267 | 0.0689 |

`append_batch` mean **0.0294 ms** — an existing-stream append with no name flush.

### (c) all-existing, cold cache, first-touch

`wall/n = 0.0380 ms/cmd` (n=4,000).

| phase | mean | p50 | p99 |
|---|---|---|---|
| 1 cache probe | 0.0002 | 0.0001 | 0.0007 |
| 2 load_cached (miss) | 0.0020 | 0.0016 | 0.0067 |
| 3 decide | 0.0000 | 0.0000 | 0.0001 |
| 4 encode+append | 0.0346 | 0.0294 | 0.0903 |
| 5 write-through+put | 0.0006 | 0.0003 | 0.0094 |
| = measured total | 0.0377 | 0.0325 | 0.0968 |

*residue = 0.0003 ms.* Backend: `append_batch` mean **0.0342 ms** (existing
stream, no name flush), `load_snapshot` mean 0.0003, `read_stream` mean 0.0009.

**This is the load-bearing control.** The cache miss here does the *full* cold
path — snapshot-store lookup, a 1-event tail read, fold — yet the whole command
is **0.038 ms**, ~90x cheaper than (a). The only structural difference from (a)
is that (c)'s append is to a stream whose name is **already interned**, so it
skips the durable name flush. That flush is the entire gap.

### (d) all-new-streams over plain `LogEngine` (H3)

`wall/n = 4.9595 ms/cmd`, per-command p50 2.9730, mean 4.9595 (n=4,000, ran
during the load-avg≈34 window). `append_batch` mean 4.9541 ms.

The wrapper adds nothing: the plain engine pays the **same** per-new-stream
`append_batch` fsync (it lives in `LogEngine`, below the wrapper). If anything
the unwrapped run was *slower* — a host-load artifact (it ran at peak load),
which itself confirms the wrapper is not the cost. The `FjallSnapshotBackend`
wrapper's only added work on the miss path is the `load_snapshot` point read,
measured at 0.0019 ms in (a). **H3 refuted.**

### (e) sequential vs pipelined k=32 (H4)

Both ran back-to-back at similar (high) host load, so this is a valid
within-comparison:

| variant | n | wall/n | in-run per-command p50 | mean |
|---|---|---|---|---|
| sequential (`command_cached`) | 2,000 | 7.1338 ms | 6.6316 ms | 7.1338 ms |
| pipelined (k=32) | 8,000 | **5.6311 ms** | 163.20 ms | 180.05 ms |

Pipelining k=32 improved throughput only **~1.3x** (7.13 -> 5.63 ms/cmd), and the
*in-pipeline* per-command latency **exploded** to 163 ms p50 — because all 32
concurrent new-stream commands funnel their name-persist through **one shared
`MetaStore::persist`**, which serializes on the single fjall journal fsync. The
executor is not the bottleneck; the shared durable name flush is. This is
exactly why the seeder's k=32 pipeline still lands at ~2.3 ms/cmd rather than
`fsync / 32`.

---

## Hypothesis verdicts

| # | hypothesis | verdict | evidence |
|---|---|---|---|
| **H1** | per-new-stream registry/interning cost | **CONFIRMED — dominant (98.8%)** | (a).append_batch − (c).append_batch = **3.40 ms/cmd**. But the cost is the durable **name flush fsync**, not the HashMap intern (which is ns). |
| H2 | fjall snapshot-lookup miss per cold miss | refuted (negligible) | `load_snapshot` mean **0.0019 ms** in (a), 0.0003 in (c) — 0.05% of cost. bn-3dy's registry-keyed meta change is in `save_snapshot` (never called: seeder saves no snapshots), not on this path. |
| H3 | `FjallSnapshotBackend` wrapper overhead | refuted | plain `LogEngine` (d) pays the same `append_batch` fsync; wrapper adds only the 0.0019 ms `load_snapshot`. |
| H4 | executor (seq vs pipelined) overhead | partly relevant | pipelining amortizes wall/n but only ~1.3x, because the per-new-stream `MetaStore::persist` is a **shared serialization point**. The per-command *latency* is unchanged; the executor is not the cost. |
| H5 | empty/cold stream load cost | refuted (negligible) | `read_stream` mean **0.0008 ms** (a, empty) / 0.0009 (c, 1-event); `load_cached` phase mean 0.0037 / 0.0020 ms. |

### The hypothesis nobody listed (found + measured)

**The per-new-stream durable name flush is a single-writer serialization point.**
It is not merely that each new stream costs one fsync (H1); it is that *all*
concurrent new-stream appends serialize on one `MetaStore` journal fsync
(`spawn_blocking(inner.meta.persist())` over the shared `Arc<MetaStore>`), so the
seeder's concurrency cannot parallelize the dominant cost — evidenced by the
~1.3x-only pipelining win and the 163 ms in-pipeline latency in (e). Any fix that
only adds *more* concurrency will not help; the flush itself must be coalesced or
removed from the per-stream path.

---

## Recommended follow-up bones

1. **Coalesce registry-name persists into the group-commit window** (expected
   win: **~5-20x** seeder throughput). Today every new stream triggers its own
   `MetaStore::persist(SyncAll)` (`engine.rs:1775`). The log committer already
   has a group-commit path (`Durability::Group`) that coalesces N appends behind
   one fsync; the name flush should ride the *same* window (one `SyncAll` per
   group, not per new stream). At k=32 that alone should turn ~3.4 ms/cmd into
   the ~0.1 ms range — the (c) cost plus an amortized fsync. This is the single
   highest-value change and matches `docs/perf/bulk-writes.md`'s group-commit
   thesis.

2. **Make the name-durability barrier batchable / deferrable per D-mode**
   (expected win: eliminates the flush entirely on `Durability::Process`). The
   flush exists so a covering append cannot out-live its name across power loss
   (bn-150). Under `Durability::Process` the *log append itself does not fsync*,
   so a per-name `SyncAll` is strictly stronger than the durability the operator
   asked for — the name only needs to be durable *before/with* the append it
   covers, which under `Process` is "never (until the next real barrier)". Gate
   the name flush on the engine's durability mode so `Process` batches names into
   the OS page cache like every other meta table. Expected: seeder new-stream
   append drops toward the (c) 0.034 ms floor.

3. **(smaller) Batch new-stream creation in the seeder** (expected win: ~2-4x,
   independent of engine changes). If 1 and 2 are deferred, the seeder can amortize
   by appending the *first* event of many new streams under a single up-front
   name-registration barrier (register all names for a phase, one flush, then
   fan out the appends). Lower ceiling than the engine fix, but no `mess-*`
   change and it dogfoods the batch-registration API gap.

---

## Dogfood / API-gap notes

- **No `mess-*` hook was missing** for this decomposition: `ProfilingBackend`
  slots in at the public `Backend`/`SnapshotStore`/`SubscribeBackend` seam and
  the cold-miss loop rebuilds cleanly from the public `EventStore` surface
  (`cache()`, `load_cached`, `append`). That the seam is decomposable without a
  private hook is itself a good sign for the abstraction.
- **The finding is a latent correctness-vs-cost smell worth a doc note:** a
  per-new-stream `SyncAll` firing under `Durability::Process` means the *name*
  side of a write is durably fsynced even when the operator explicitly chose the
  no-barrier mode for the *event* side. It is safe (stronger, never weaker), but
  it is a surprising and expensive asymmetry — the co-durable-flush doc comment
  (`engine.rs:1464-1485`) explains the power-loss rationale but does not flag the
  `Process`-mode cost, and that is exactly the seeder's whole bill.

## Reproduce

```
CLANG_PATH=/usr/bin/clang TMPDIR=$HOME/.cache/mess-test-tmp \
  cargo test -p social --release --test seed_profile seed_profile \
  -- --ignored --nocapture --exact
```

Smoke (normal suite, shape-only assertions, no absolute timings):

```
CLANG_PATH=/usr/bin/clang TMPDIR=$HOME/.cache/mess-test-tmp \
  cargo test -p social --release --test seed_profile seed_profile_smoke
```

`cargo clippy -p social --tests --release -- -D warnings` is clean; nightly
`cargo fmt -p social -- --check` is clean.
