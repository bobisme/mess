# examples/social

A small Twitter-clone built on `mess` — the second showcase example after
`examples/bank`. Where `bank` teaches the vocabulary on one aggregate, this
crate is the *shape of a real application at scale*: **every aggregate has
bounded state**, warm-path writes, a real rebuildable + checkpointed read
model, and a server-rendered HTML frontend, all wired to a real on-disk event
log.

This README is the demo tour: what it shows, how to run it at two scales, why
the "rebuild from the log" trick works and how the `--rebuild` proof pins it,
the numbers the warm path and the scale tiers actually produce on this box, and
a walk through the real `mess` operational CLI run against the demo's own
seeded store.

> **Every number in this README is a real captured output from this machine**
> (2026-07-10), release build, the engine's default `Durability::Process`.
> Each is labelled with the tier/params that produced it.

## What this demonstrates

- **Four aggregates, all with bounded state.** Two *entities* —
  `User` at `user-<id>` (handle, display name) and `Post` at `post-<id>`
  (author, body, delete tombstone) — and two *relationships*, `Like` at
  `like-<post>_<user>` and `Follow` at `follow-<follower>_<followee>`, each a
  tiny alternating two-state machine on its own stream. The crowds (a post's
  likers, a user's followers) are **not** aggregate state anywhere; see
  [Why relationship streams](#why-relationship-streams) below.
- **Warm-path writes.** Every write goes through
  `EventStore::command_cached` over a `FjallSnapshotBackend<LogEngine>`
  (`src/store_backend.rs`): a hot-aggregate write-through cache proves the
  stream's version by the append itself (zero event reads on a warm hit), with
  a snapshot-accelerated cold load underneath. `tests/snapshots.rs` proves this
  is byte-identical to plain `command`; `tests/hot_post_bench.rs` measures the
  speedup ([below](#hot-aggregate-benchmark)).
- **A real, rebuildable *and* checkpointed read model.** `src/projections.rs`'s
  `Projections` folds the entire log into in-memory tables by replaying
  `read_global` from position 0, then tails it live; it also persists a
  checkpoint sidecar so a restart *resumes* from where it left off instead of
  re-replaying. `social-web --rebuild` proves the resumed state is
  byte-identical to a clean from-0 rebuild ([below](#the-rebuild-proof)).
- **Two scale tiers.** `--scale demo` is the deterministic 1,488-event world
  the tour walks; `--scale large` is a ~54.7k-event world with realistic skew
  (a celebrity user with thousands of followers, viral posts with thousands of
  likes, a long tail), seeded with bounded-concurrency pipelining over distinct
  streams. See [Seeding the corpus](#seeding-the-corpus).
- **The real `mess` operational CLI** run against this app's own seeded store —
  at both tiers, not a synthetic fixture. See the [ops tour](#ops-tour).

## Quickstart

```sh
# one command: seed + serve the demo tier
just demo

# or step by step:
cargo run -p social --bin social-seed             # seed ~/.cache/mess-social-demo/store
cargo run -p social --bin social-web -- --dir ~/.cache/mess-social-demo/store
# then open http://127.0.0.1:3000

# the large tier (~54.7k events, pipelined):
cargo run -p social --release --bin social-seed -- --scale large --dir /path/to/large
```

`social-seed` refuses to run against a directory that already holds a store
(see [Seeding the corpus](#seeding-the-corpus)) — pass `--force` to wipe and
reseed, which is what `just demo` does so it is always safe to rerun.

There is also a no-setup toy mode with three hardcoded users and no
persistence, for a quick sanity check with nothing seeded first:

```sh
cargo run -p social --bin social-web   # no --dir: in-memory demo world
```

## Domain model

```
                         ┌─────────────────────────────────────────┐
                         │              axum handlers                │
                         │         (src/web/handlers.rs)              │
                         └────────────┬────────────────┬─────────────┘
                        GET (reads)   │                │  POST (writes)
                                      ▼                ▼
                      ┌────────────────────────┐  ┌─────────────────────────┐
                      │      ReadModels          │  │       WriteOps           │
                      │  (contracts.rs trait)     │  │  (contracts.rs trait)     │
                      └────────────┬─────────────┘  └────────────┬──────────────┘
                                   │ query                        │ command_cached
                                   ▼                              ▼
                      ┌────────────────────────┐  ┌─────────────────────────────┐
                      │      Projections          │  │    mess_store::EventStore    │
                      │  (projections.rs)          │  │  over FjallSnapshotBackend    │
                      │  from-0 rebuild at boot    │◀─┤       <LogEngine>             │
                      │  (or checkpoint-resume),   │  │  entities:                    │
                      │  then live-tails the log,  │  │    user-<id> / post-<id>      │
                      │  reconstructing the like   │  │  relationships:               │
                      │  & follow crowds from the  │  │    like-<post>_<user>         │
                      │  relationship streams      │  │    follow-<flwr>_<flwee>      │
                      └────────────┬───────────────┘  └────────────┬──────────────────┘
                    read_global +  │                                │ append_batch (+ warm
                    subscribe over │                                │ cache / snapshot load)
                    a LogEngine    │                                │
                    read handle    └───────────────┬─────────────────┘
                                                    ▼
                                    durable on-disk log + snapshots
                                    ($STORE/seg-*.log + meta/ + .snapshots/)
```

No aggregate reads another's state. A `Follow` edge only *records* that
`follow-<a>_<b>` is active; it never checks that `b` is a registered user
(`decide` folds exactly one stream; see `src/domain/user.rs`'s module docs on
the accept-and-reconcile posture — why that is a feature, not a gap).
`Projections` is what joins everything: it routes each record to the right
fold by `StoredRecord::category()` (the segment before the first `-`), folds
the entity streams into user/post tables *and* reconstructs the like/follow
crowds from the relationship streams into count/membership indexes, and answers
cross-aggregate queries (a post's `PostView` joins the author's current
handle/display name and its live like count) at query time.

**Ids** (bn-gt5) are UUIDv7 (`src/id.rs`'s `Id`, a newtype over `uuid::Uuid`),
shown everywhere — URLs, logs, stream names — as a fixed 26-character
lowercase Crockford base32 string with no separators, time-ordered (`Ord`
agrees with creation order) and safe to compare/sort as plain text. Stored ids
from older (pre-bn-gt5) seeds are format-incompatible with this encoding;
reseed a fresh store rather than trying to read an old one.

### The write and read backends (`src/store_backend.rs`)

The warm write path needs `SnapshotStore` (that is where `command_cached`
lives); the read model's live tail needs `SubscribeBackend`. As of this
example, **no single backend type has both**: `LogEngine` is a
`SubscribeBackend` but not a `SnapshotStore`, and `FjallSnapshotBackend<LogEngine>`
is a `SnapshotStore` but does not forward `SubscribeBackend`. The reconciliation
(`store_backend::read_handle`) is clean because `LogEngine` is `Arc`-backed: the
warm-write `Store` writes through the snapshot backend, and the read model tails
a second `EventStore` over a *clone of the very same engine*, which shares its
watermark and commit notifications. See that module's docs — and the
[Concerns](#concerns) at the end — for the dogfood note.

## Why relationship streams

The scaling problem with putting a crowd *inside* an aggregate: if `Post` held
`likes: HashSet<Id>`, a viral post with a million likes would replay a
million-element set on **every** `Post` command and write it into **every**
snapshot — O(crowd) work forever, on the hot write path. Same for a hub user's
follow graph.

The event-sourcing fix is that a crowd is not aggregate state — each
*relationship* is its own tiny aggregate. A like is one `like-<post>_<user>`
stream: a `not-liked ↔ liked` machine holding a single bit. Placing or removing
a like is O(1) no matter how many other people liked the same post, because the
command folds only *that one edge's* stream. Ditto follows. So:

- **Bounded commands.** Every `decide` folds a handful of fields, never a
  crowd. Snapshots stay tiny. This is exactly what makes the warm
  `command_cached` path and the O(1)-per-write story below hold at *any* like
  count.
- **Crowds live in the projection.** The read model reconstructs like counts,
  `liked_by_me`, and follower/following sets from the relationship streams at
  query time — exactly where a home timeline was already computed by filtering
  posts against the viewer's *current* follow set.
- **Stream naming.** A relationship stream keys on *both* ids:
  `like-<post>_<user>`. The `_` is a sound separator because an `Id` renders
  only over `[0-9a-hjkmnp-tv-z]` (lowercase Crockford base32, no separators at
  all) and contains neither `-` nor `_`; `StoredRecord::category()` splits at
  the first `-`, and the suffix splits once on `_` back into the two ids. See
  `src/lib.rs`'s `PAIR_SEP` / `parse_pair` for the format invariant.

The payoff is visible in the stream counts below: the demo tier's 1,488 events
live on **1,438 distinct streams**, and the large tier's 54,680 events on
**54,280** — almost every action is its own stream. That is the point: it is
what makes the writes O(1) and the seeder pipelinable.

## Seeding the corpus

`social-seed` (`src/bin/social-seed.rs`, generator in `src/seed.rs`) draws a
corpus deterministically, then commits it entirely through warm `WriteOps`
calls — `store.register(...)`, `store.follow(...)`, `store.create_post(...)`,
`store.like(...)`, … — each exactly one `EventStore::command_cached` call that
runs the real `Decide` impl. There is no bulk-append shortcut; the planner
tracks its own shadow state purely to avoid *drawing* a command it knows the
domain would reject.

Two tiers, `--scale demo` (default) and `--scale large`:

| tier  | users | follows | posts | likes  | deletes | unfollows | **events** | streams |
|-------|-------|---------|-------|--------|---------|-----------|-----------|---------|
| demo  | 50    | 88      | 500   | 800    | 30      | 20        | **1,488** | 1,438   |
| large | 3,500 | 12,280  | 3,500 | 35,000 | 200     | 200       | **54,680**| 54,280  |

The large tier is deliberately **skewed**, not uniform (real captured output of
`seed::tests::large_tier_has_target_scale_and_skew`):

```
large tier @seed=1337: 54680 events (3500 users, 12280 follows, 3500 posts,
35000 likes, 200 deletes, 200 unfollows); top user 2204 followers; hottest
post 2983 likes
```

— a sharper follow-target Zipf (`popularity_exponent = 1.5`) makes the top
user a **celebrity with 2,204 followers**; the post-like Zipf makes the
**hottest post go viral with 2,983 likes**, with a long tail behind both.

### Determinism, plan vs execute

`plan_corpus` draws the whole corpus (handles, bodies, the follow/like graphs,
and the `Id`s themselves) from one seeded `StdRng` with a fixed draw order,
*before any store write* — so the corpus is a pure function of `--seed`.
Execution then honours `--concurrency`:

- **Demo is sequential** (`concurrency = 1`): the global log is
  **byte-identical** run-to-run. Covered by
  `seed::tests::demo_seed_is_byte_identical_for_a_fixed_seed`, which seeds two
  stores and diffs their whole logs event-for-event.
- **Large is pipelined** (`concurrency = 32`): commands of each phase fan out
  over a bounded `JoinSet` window across **distinct** streams only (the
  `docs/perf/bulk-writes.md` pattern). Per-stream event order stays
  deterministic — only the *global interleaving* differs run-to-run — so the
  same seed yields identical event counts, final watermark, and per-stream
  heads. Proven cheaply at demo scale by
  `pipelined_execution_matches_sequential_per_stream` (per-stream sequences are
  identical sequential vs pipelined) and at scale by the `#[ignore]`d
  `large_scale_pipelined_determinism`.

### Seed wall-clock (this box, release, `Durability::Process`)

| tier  | sequential | pipelined (K=32) | speedup |
|-------|-----------:|-----------------:|--------:|
| demo  |    3.91 s  |          2.99 s  |  1.3×   |
| large |  221.03 s  |        126.27 s  |  1.75×  |

The pipelining pattern (bounded concurrency over distinct streams) is exactly
`docs/perf/bulk-writes.md`'s; its headline **7–23× win, however, is measured
under `Durability::Group`**, where each sequential command pays an
un-amortized `fdatasync` and pipelining coalesces them into shared group
commits. The engine's *default* durability is `Process` (buffered, no fsync),
so there is no barrier to coalesce here — the win above is only the CPU
parallelism of the per-command work (each cold `command_cached` still does a
snapshot-store point lookup) spread across cores, hence the more modest 1.3–1.75×.
The `A/B` is reproducible with `--sequential` and `--concurrency N`:

```sh
social-seed --scale large --dir DIR_A                 # pipelined (K=32)
social-seed --scale large --sequential --dir DIR_B    # concurrency=1
```

### Fresh-dir guard

Seeding into a directory that already holds a store would silently double the
corpus or mix two seeds, so `social-seed` refuses by default:

```
refusing to seed on non-empty --dir …/store: found a leftover store
(1 seg-*.log segment file(s), markers: [LOCK, meta, sealed]) — seeding assumes
an empty starting log, so pre-existing events would either double the corpus or
mix two seeds' worth of state, silently breaking the "same seed, same corpus"
guarantee. Pass --force to wipe the directory first, or point --dir at a fresh
path.
```

`--force` wipes `--dir` first (what `just demo` passes, so reseeding is always
one command).

## The rebuild-from-log party trick

```sh
# with the server running against a seeded store:
curl -s http://127.0.0.1:3000/ > before.html
# stop the server (Ctrl-C), then start it again, then:
curl -s http://127.0.0.1:3000/ > after.html
diff before.html after.html   # <- empty diff
```

Verified byte-for-byte for this bone: `/` + `/firehose` before and after a real
restart were **byte-identical (19,825 bytes)**, and the second boot printed

```
building read model (resume-or-rebuild) ... done (resumed from checkpoint at position 1488).
```

It works because the read model is a pure *function of the log*. On a cold boot
`Projections::new` drains `read_global` from position 0, folding every event
into the same in-memory tables the live tailer would build incrementally; the
process never owned the state — the on-disk log did. The checkpoint sidecar
(`<dir>/.social-projections.ckpt`) is a pure optimization on top: it lets a
restart *resume* from the last folded position and replay only the log suffix,
instead of re-replaying from 0. Nothing in this domain's rendering is
wall-clock-dependent (post bodies encode a "time of day" *flavor* as text,
never a real timestamp), so there is no hidden nondeterminism to break the
byte-for-byte claim.

### The rebuild proof

Because the checkpoint is an optimization, it needs a correctness proof: a
resumed read model must equal a clean rebuild. `social-web --rebuild` builds the
projection **both ways** over the same log — a full replay from 0 and a
checkpoint resume — and byte-compares their folded state (a canonical,
iteration-order-independent serialization). Real output against the large store
(54,680 events, checkpoint present):

```
$ social-web --rebuild --dir /path/to/large
rebuild-compare: PASS
  resumed from checkpoint at position 54680
  from-0 rebuild folded to position 54680 in 0.025s
  checkpoint resume folded to position 54680 in 0.007s
  counts: 3500 users, 3500 posts, 12080 follow edges, 35000 like edges
  compared 2267602 bytes of canonical state fingerprint
```

It exits non-zero on any mismatch. This is the read-path analogue of
`tests/snapshots.rs`'s cache-on == cache-off differential;
`rebuild::tests::rebuild_matches_after_checkpoint` runs the same comparison
in-process at demo scale in the normal test suite.

**Cold-start vs checkpoint-resume timing** (large tier, 54,680 events): the
from-0 rebuild folds the whole log in **0.025 s**; the checkpoint resume, which
replays only the suffix (here nothing new since the checkpoint), takes
**0.007 s**. Both are sub-100 ms at this depth — the log is only ~6.8 MB — so
the resume advantage is small in absolute terms *here*; it grows with log depth,
which is exactly why the checkpoint exists (a many-GB production log would
replay for seconds-to-minutes cold, and resume in the same few ms).

## Hot-aggregate benchmark

The bounded-state remodel is what makes the *real* `Post` command O(1) at any
like count. `tests/hot_post_bench.rs` proves the mechanism on a deliberately
*unbounded* aggregate (a `MegaPost` that keeps a growing liker set) so the cost
being removed is visible. Real captured output
(`cargo test --release -p social --test hot_post_bench -- --ignored --nocapture`,
real fs, `EventStore` over `FjallSnapshotBackend<LogEngine>`):

```
================ hot-post benchmark ================
params: events=100000 unlike_frac=0.30 seed=184594917 page_size=1000 (default)
seeded: depth=100000 events, active likers in fold=40278 ids
---------------------------------------------------
(a) plain command per like @depth (full replay each)
      n=5   min=  32.771 ms median=  34.267 ms mean=  36.057 ms
(b) command_cached warm steady-state (0 event reads/hit)
      n=300 min=   0.044 ms median=   0.051 ms mean=   0.059 ms  (depth≈100306)
      => warm median speedup vs cold command:  670.6x
(c) cold start @depth=100307
      full replay  load              32.364 ms
      snapshot+tail load_cached       5.052 ms  (tail=0 events)
      first warm-miss command         4.902 ms
      => snapshot load speedup vs full replay:  6.4x
(d) churn compaction
      snapshot blob = 1055192 bytes for 100307 events (10.52 B/event)
===================================================
```

Reading it: at depth 100k, a plain `command` pays a full replay of every event
on the stream, so its latency scales with history — **median 34.3 ms** (a). The
warm `command_cached` path proves the version by the append itself and folds the
one event it wrote into the cached state, reading **zero** events — flat
regardless of depth, **median 0.051 ms, ~671× faster** (b). On a cold process a
snapshot turns the same load from a 100,307-event replay into one blob decode +
a 0-event tail, **~6.4× faster** (c). This is precisely the cost the bounded
`Post` remodel removes from the real aggregate, whose command is O(1) and whose
snapshot is a handful of bytes at *any* like count — which is why the demo's
warm path stays fast without any of this churn.

## Ops tour

Everything below is the **real** `mess` CLI (`crates/mess-cli`), run against
this demo's own seeded stores — the **demo** store (1,488 events, 1,438 streams)
and, where noted, the **large** store (54,680 events, 54,280 streams) — output
lightly trimmed for length but otherwise unedited.

### `mess inspect` — stream heads at scale

Against the **large** store, this exercises `inspect`'s stream-head pagination
(bn-1yz) at real scale — 54,280 streams:

```
$ mess inspect /path/to/large
{"base_pos":0,"batch_count":54680,"epoch":1,"event_count":54680,"has_pcol":false,"has_pidx":false,"next_pos":54680,"safe_offset":6851995,"sealed":false,"segment_id":1,"size_bytes":54680-batch}
…
registry:
  available: true
  source: meta
  snapshots: (none)
  stream_names (54280):
    stream_id=1  name=user-xb4y6n-eqhavjqt-krkcwh
    stream_id=2  name=user-m52bsg-cqz41e46-b68gw1
    …
    ... and 54260 more (showing 20 of 54280; see --format json or a filter flag for the rest)
```

Proves: the on-disk segment chain matches the durable event count this app's own
log claims (54,680, matching `social-seed`'s report), and the registry knows all
54,280 streams — one per entity and one per relationship edge. The top-N
truncation (`showing 20 of 54280`) is what keeps the overview readable when
almost every action is its own stream; `--format json` or a `--stream` filter
gives the rest.

### `mess doctor` — is the store healthy?

```
$ mess doctor /path/to/large
ok  lock-free  lock  store is not locked by a live writer
ok  unsealed-head  trailer  segment 1: unsealed active/rolled head (no trailer)
ok  fsync-ok  fsync  store directory is writable and fdatasync succeeded
ok  no-snapshots  fold-version  no live snapshots to check for fold drift
dir: /path/to/large
epochs_seen: 1
fold_versions: (none)
```

Proves the lock is free, the segment chain's epoch/trailer state is sane, and
the store directory can durably `fdatasync`.

The `fold-version` drift check, though, reports **`no live snapshots`** — and
this is worth being precise about, because the warm-write flip does not make it
non-vacuous. The warm path's fast case is a hot-aggregate *in-memory*
write-through cache; `command_cached` only *reads* the persistent snapshot store
(on a cold miss) and never proactively *saves* to it, so a seeded/served social
store simply has **no persisted snapshots** for `doctor` to check. Two separate
reasons keep it that way, both outside this example's reach — see
[Concerns](#concerns). (The snapshot store *is* real and *is* exercised, cold-
load and all — just not visibly to `doctor`; see the
[hot-aggregate benchmark](#hot-aggregate-benchmark), which saves and re-loads a
snapshot to measure the 6.4× cold-load win.)

### `mess verify --full` — recovery scan + byte integrity

```
$ mess verify /path/to/demo --full
ok  unsealed-segment-scanned  segment-scan  segment 1: unsealed, 1488 committed batch(es), tail stop = clean
dir: …/demo
full: true
verified: true
```

Proves: every one of the 1,488 committed batches passes the acceptance-kernel
scan (CRCs, structural framing) and reaches a *clean* tail stop (no truncated
last record); `--full` also reassembles payload blocks and would recompute the
fold chain on any chain-enabled batch (this demo never opts into `crypto_chain`,
so that half is a correctly-reported no-op).

### `mess backup` + `mess restore` — the durability round trip

```
$ mess backup …/demo --to …/backup
ok  backup-complete  backup  backup complete: 24 file(s) copied, 0 skipped, watermark 1488
…
copied_bytes: 654025
copied_files: 24
watermark: 1488

$ mess restore …/backup --to …/restored
ok  restore-complete  restore  restored 24 file(s); verify --full clean; recovered watermark 1488
…
manifest_watermark: 1488
recovered_watermark: 1488
verified: true
```

Proves: `backup` took a consistent cut (a retention lease held for the copy,
manifest written last), and `restore` copied it back, re-ran `verify --full`,
and its `recovered_watermark` matches the manifest's `1488` exactly — no events
lost, none duplicated.

## At scale

The through-line of this example, from the bounded aggregates to the pipelined
seeder:

- **Bounded aggregates → O(1) warm commands.** Because a crowd is never
  aggregate state, a `Post`/`User`/`Like`/`Follow` command folds a handful of
  fields no matter how popular the entity is. That is the precondition for the
  warm `command_cached` path: a bounded aggregate has a tiny cached state and a
  tiny snapshot, so a warm hit is a version-check-plus-append (zero reads), flat
  at any like count. The hot-post benchmark is the counterfactual — it lets the
  state grow unbounded specifically to show the ~671× cost the bounded remodel
  removes.
- **Crowds reappear in projections.** Counts, `liked_by_me`, follower sets are
  reconstructed from the relationship streams at query time. The large tier's
  celebrity (2,204 followers) and viral post (2,983 likes) are folded into O(1)
  count/membership maps in the read model, never onto the write path.
- **Checkpoint resume.** The read model is a function of the log, so it is
  freely rebuildable; the checkpoint sidecar makes a *restart* pay only the log
  suffix instead of a full replay, and `--rebuild` proves the two agree
  byte-for-byte.
- **What changes at 10×.** The write path does not — it is already O(1) per
  command and pipelinable per distinct stream. What starts to bite is the
  *read* side: `Projections` is a single in-memory fold of the whole log, and a
  from-0 rebuild is linear in log length (0.025 s at 54.7k events here; seconds
  at millions). The checkpoint bounds *restart* cost; the next steps beyond this
  example are the ones `docs` already flag as later bones — sharding the
  projection, materialized per-user inboxes for the highest-fanout timelines,
  and seeding under `Durability::Group` so the committer's group-commit turns
  the pipelining pattern's 1.75× here into the 7–23× the bulk-writes guide
  measures.

## Concerns

Dogfood findings surfaced by building this at scale (reported here, not worked
around by editing `mess-*`):

- **`mess doctor`'s fold-version check now *can* see app snapshots — social
  just hasn't opted in yet.** This used to be a two-part dead end: (1) the
  social app never *persists* snapshots (`command_cached`'s fast path is an
  in-memory write-through cache that only *reads* the snapshot store on a cold
  miss), and (2) even saved snapshots were written to the sidecar meta store
  (`<dir>/.snapshots/meta`) keyed by *interim* FNV stream ids, while `mess-cli`'s
  `metaread` read only the engine's `<dir>/meta` and correlated by the engine's
  own ids — two id spaces that never intersected, so the check always read `no
  live snapshots`. The `mess-store`/`mess-cli` half of that is now fixed: a
  `FjallSnapshotBackend` records each snapshot head's stream *name* alongside
  its FNV key, `doctor`/`metaread` read the `.snapshots` sidecar and join heads
  back to names, and `EventStore::with_snapshot_policy(SnapshotPolicy::every_n_events(N))`
  lets the warm path persist a snapshot every N events straight from the folded
  state it already holds (default off — nothing changes until you opt in). What
  remains is purely an `examples/social` adoption choice: `store_backend`'s
  `open_store` does not yet set a snapshot policy, so a seeded/served store
  still persists none and `doctor` still (correctly) reports `no-snapshots`.
  Flip the policy on there and `mess doctor --expect-fold-version` reports a
  real, non-vacuous fold-version check over this store's persisted snapshots.

## Development

```sh
cargo test -p social                              # unit + integration + doc tests
cargo clippy -p social --all-targets -- -D warnings
just demo                                          # seed --force, then serve
```

`src/seed.rs`'s tests cover the demo-tier byte-identity, the pipelined-vs-
sequential per-stream equivalence, the large-tier scale + skew targets, the
fresh-dir guard, and that every generated handle and post body satisfies the
domain's own validation. `src/rebuild.rs`'s test is the in-process
checkpoint-correctness proof. Large-tier operations are `#[ignore]`d and run on
demand.

Every real-fs test/example store dir comes from `mess_testkit::sweeping_temp_dir`
(never a hand-rolled `TempDir`/`temp_dir()`), namespaced under
`<TMPDIR or $HOME/.cache/mess-test-tmp>/mess-tests/` and self-swept of
dead-process leftovers older than 24h on first use per process — the guard
against the on-disk store leaks that once filled the machine's disk.
