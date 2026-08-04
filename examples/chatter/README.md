# `examples/chatter` — the deep-stream corpus

A chat/feed service: **channels** are long-lived streams that accumulate
thousands of messages and reactions; **users** are tens of thousands of
one-event streams. One corpus, both pressures — stream depth *and* registry
pressure — with a `--segment-bytes` knob that makes even the demo scale roll and
seal a dozen segments.

```sh
cargo run --release -p chatter --bin chatter -- seed --dir /tmp/chat
cargo run --release -p chatter --bin chatter -- stats --dir /tmp/chat
```

```
done in 0.30s: 400 users, 24 channels, 20000 messages, 12000 reactions,
40 renames, 2 archives (32466 events total)
busiest channel: #support with 5975 messages; quietest has 180
store: 12.5 MiB across 12 segment file(s); sealed tier: 11 segment(s)
(11 SealPack, 0 loose, 11 with a payload accelerator)
```

## Why this example exists

`examples/social` is the mess showcase: users, posts, likes, follows, a web UI,
a rebuildable projection. It is also **maximally shallow**. Every aggregate is
its own stream, so bn-3jqg measured its demo corpus at **1.007 events per
stream** across 17 MB — and social exposes no segment-size knob. The
consequence is structural, not incidental:

> The active segment never fills, so no segment ever rolls, so nothing is ever
> sealed. `.pcol` payload sidecars, `.reg` per-segment registry deltas,
> SealPack, sealed-candidate quarantine and re-seal, retention,
> cold-open-at-scale — **none of it is reachable from `examples/`.**

Those are real, shipped parts of mess. Chatter is the complementary shape that
reaches them, deliberately:

| | `examples/social` | `examples/chatter` |
|---|---|---|
| stream shape | one aggregate per stream | channels are long-lived streams |
| events/stream | ~1.007 | ~1 for users, **tens of thousands** for hot channels |
| segment-size knob | none | `--segment-bytes` |
| segments sealed at demo scale | 0 | **11** |
| sealed reads exercised | none | scroll-back through sealed history |
| corpus ceiling | ~17 MB | ~1.9 GiB (`--scale large`), several GiB (`huge`) |

Chatter is not a replacement for social — social teaches the *vocabulary* and
carries the web layer. Chatter is the **instrument**: the corpus you point
`mess doctor`, `mess verify`, a backup/restore cycle, a retention policy, or an
A/B harness at when you need a store with something in it.

## The domain

Two aggregates, both with **bounded** folded state (`src/domain/`):

- **`channel-<id>`** — the deep stream. `Created`, `MessagePosted`,
  `ReactionAdded`, `Archived`. The fold keeps an existence bit, an archive bit,
  a slug, a topic, and two counters: a few dozen bytes whether the channel has
  ten messages or ten million. A message carries the channel-local **ordinal**
  the aggregate stamped on it, and a reaction names that ordinal as its target —
  which makes `target < messages` a complete referential check against *this
  stream's own state*, with no set of ids and no cross-stream read.
- **`user-<id>`** — the shallow stream. `Registered`, `DisplayNameChanged`. This
  is the registry-pressure half: `--users 150000` is 150,000 stream names for
  the engine to intern and carry in its per-segment registry deltas.

What is deliberately *not* checked in `decide`: that a message's author is a
registered user. `decide` is a pure function of one stream's state and cannot
load another — the accept-and-reconcile posture `examples/social`'s
`domain::user` documents at length. The projection reconciles.

## Knobs

Everything is a flag on `chatter seed` (and on `chatter bench`). `--scale`
picks a preset for all of them; an explicit flag always wins.

| flag | what it does |
|---|---|
| `--scale demo\|large\|huge` | preset for every knob below |
| `--seed N` | PRNG seed. Same seed, same corpus. |
| `--channels N` | deep streams |
| `--users N` | shallow streams (registry pressure) |
| `--messages N` | messages across all channels, Zipf-distributed |
| `--reactions F` | reactions per message, as a factor |
| `--segment-bytes N[K\|M\|G]` | **the knob social lacks** — active-segment size, i.e. the roll (and seal) cadence |
| `--seal-pack on\|off` | one consolidated `.seal` per sealed segment, or the legacy loose `.pidx`/`.filter`/`.pcol` trio |
| `--concurrency N` / `--sequential` | pipelining window; `1` is byte-identical run to run |
| `--force` | wipe `--dir` first |

The presets:

| | `demo` | `large` | `huge` |
|---|---|---|---|
| channels | 24 | 256 | 1,024 |
| users | 400 | 25,000 | 150,000 |
| messages | 20,000 | 3,000,000 | 12,000,000 |
| segment bytes | 1 MiB | 64 MiB | 128 MiB |
| measured store size | 12.5 MiB | 1.9 GiB | (several GiB) |
| measured sealed segments | 11 | 27 | — |
| measured seed wall-clock | 0.30 s | 28.5 s | — |

Message bodies are **lognormal in the 100–800 byte band**, drawn from the seeded
PRNG. That is what makes `--segment-bytes` mean something: a segment holds a
realistic number of realistically-sized records, so the roll cadence and the
sealed-segment shape resemble a real store's rather than a microbenchmark's.

The store's knobs are persisted in a `.chatter-store` sidecar next to the log,
because `EngineOptions` is per-*open* and the engine re-allocates the resumed
active segment to whatever `segment_size` it was opened with. Without that,
every reopen of a 1 MiB-segment store under the 256 MiB default would silently
stop rolling — and every cold-open measurement would be measuring a different
store than the one that was seeded. The sidecar is configuration, not data:
delete it and the store opens with engine defaults.

## The read side

```sh
chatter stats      --dir /tmp/chat   # sealed census + per-channel + timeline
chatter scrollback --dir /tmp/chat   # page BACKWARD through sealed history
chatter tail       --dir /tmp/chat   # follow the log, SUB2 semantics
chatter rebuild    --dir /tmp/chat   # checkpoint byte-compare proof
```

**Projections** (`src/projections.rs`) are the proven `examples/social` design —
one live pump over an `EventStore::subscribe` cursor, an atomic write-rename
checkpoint sidecar, a `PROJECTION_VERSION` fold gate, a canonical byte
fingerprint — with one change forced by scale: the folded state is **bounded**.
One row per user, one row per channel (counters, never contents), and a
fixed-size window of the most recent messages. A read model that kept every
message body would need gigabytes of RAM *and* would write a gigabyte checkpoint
every cadence tick, which would make the checkpoint a liability rather than an
acceleration. Message *history* lives in the log, where it belongs.

**The discardable-acceleration law is binding**, and observable:

- checkpoint **absent** → silent full rebuild. Not a warning. A store that has
  never been read has no checkpoint, and that is normal.
- checkpoint **corrupt / truncated / foreign / stale-version** → **reported** (a
  stderr line, and `Projections::checkpoint_status()` returns `Rejected`) and
  then rebuilt from the log. Reads still work. It is never an error that blocks
  a read.

`tests/checkpoint.rs` asserts all of it, including a checkpoint whose position
is *ahead* of the log.

**Scroll-back** (`src/scrollback.rs`) pages backward through one channel. This
is the read path the sealed tier exists for: a channel alive long enough has
most of its history in sealed segments, so the older pages are served through
the sealed payload accelerator. `Backend::read_stream` reads forward from an
exclusive cursor, so a backward pager computes the stream-position range it
wants and reads that range — each page is one bounded read, and paging back
through a 761,000-message channel never materialises 761,000 records.

**Tail** (`src/tail.rs`) is written directly against `Backend::read_global_page`
rather than using `EventStore::subscribe`, precisely so the two SUB2 rules are
visible in an example instead of hidden inside the store:

1. `read_global_page(after)` is **exclusive of `after`** — to deliver *from*
   cursor `c` you read after `c - 1`, never after `c`.
2. Advance by the **frontier**, not by the last record. The engine assigns
   canonical positions to its own `$registry` records and filters them from
   application reads, so a page can come back empty with positions still below
   the watermark. The frontier is how far the scan actually got.

## `chatter bench` — an honest example, not a shadow benchmark suite

```sh
cargo run --release -p chatter --bin chatter -- bench --dir /tmp/chat-bench
```

Prints one JSON blob on stdout with five cells: `seed`, `cold_reopen`,
`scroll_back`, `tail_catch_up`, `checkpoint_resume`. An A/B harness can diff two
blobs directly.

**Read this before quoting a number from it.**

- **Process-level timing only.** Every cell is an `Instant`-around-the-whole-
  operation measurement of a public API call. There are no engine-internal
  hooks, no private counters, no sampling. Nothing here can drift away from what
  an application observes, because it *is* what an application observes.
- **One sample per cell, in one process, in a fixed order.** No warmup, no
  repetition, no statistics. Later cells see caches (page cache, the engine's
  sealed block and capsule caches) warmed by earlier ones. That is realistic for
  an application and useless for a microbenchmark. Treat these as *shape*
  numbers.
- **`mess-bench` is the real instrument.** It has floors, ABBA ordering, null
  controls, and repetition. `chatter bench` exists so a human can seed a
  realistic corpus and see whether the shape moved.
- **Release builds only.** A debug build inflates these several fold. The JSON
  carries a `profile` field precisely so a debug number can never be mistaken
  for a release one, and the CLI prints a warning when it is not a release
  build.

Demo scale, release, one run on one machine (`profile: "release"`, 32,466
events, 12.5 MiB, 11 sealed segments):

| cell | ms | note |
|---|---|---|
| `seed` | 354 | 32,466 events through the warm write path (~92k events/s) |
| `cold_reopen` | 1.0 | opening a store with 11 sealed segments |
| `scroll_back` | 11.8 | 20 backward pages of 50 through a 5,975-message channel |
| `tail_catch_up` | 45.6 | one subscriber draining 32,466 records from position 0 |
| `checkpoint_resume` | 0.24 | vs 21.6 ms for the same read model rebuilt from 0 |

## Cells this unlocks

The bone (bn-1m6c) filed this example so the following became measurable from
`examples/` for the first time. Each is now one command away:

- **Cold open with many sealed segments** (bn-26pp) — `--scale large` leaves 27
  sealed segments; `chatter bench`'s `cold_reopen` cell times the open.
- **Sealed history reads through the payload accelerator** (bn-bka2) —
  `chatter scrollback` on a deep channel.
- **SealPack vs loose sidecars** (bn-1yor's matrix, reproducible) — seed twice,
  `--seal-pack on` and `--seal-pack off`, diff the two JSON blobs.
- **Backup / restore / verify / rebuild-index** on a store with something to
  lose.
- **Retention** — a corpus with a real sealed tail to expire.

### One thing it showed immediately — and the bug it found

Backward paging cost **was** linear in the channel's stream depth and
independent of page size — at `--scale large`, release build, one machine:

| channel | stream events | ms per page (20 records) |
|---|---|---|
| `#storage` | 1,370,418 | ~142 |
| `#oncall` | 595,966 | ~62 |
| `#hiring` | 160,467 | ~10 |
| `#growth` | 38,212 | ~1.9 |

A 20-record page and a 5,000-record page on `#storage` both cost ~145 ms, so
this was a fixed per-`read_stream` cost that scaled with how deep the stream is,
not with how much of it you asked for. Until this example existed, `examples/`
could not ask the question at all, because no example stream was ever more than
one event deep. It turned out to be an engine defect; the numbers above are the
*old* behaviour, kept here because they are what the example surfaced.

**bn-e93g named the mechanism and found that scroll-back was the least of it.**
`LogEngine::read_stream` pushed the cursor and the limit into the index only on
the *unsealed* branch (`ActiveIndex::stream_entries_from`, which binary-searches
to the cursor and stops once the page is covered — bn-2ib). The **sealed**
branch called `sealed_and_hot_entries(sid)`, which took neither cursor nor
limit: it coalesced *every* batch entry of the stream across *every* sealed
segment into a `BTreeMap` and returned the whole list, which `read_stream` then
skipped through linearly. At 1.37 M events that was 137 ms of entry resolution
(60 % `BTreeMap` inserts, 24 % the final `collect`) wrapped around **0.2 ms** of
actual page materialisation — and a read positioned at the head that returned
zero records still cost 135 ms. On a twin corpus seeded identically but with a
segment size large enough that nothing seals, the same page on the same
132,555-event stream cost **0.01 ms instead of 9.5 ms**.

The consequence nobody had costed was on the *write* path, not the scroll-back
path. `EventStore::load` pages from position 0 with `limit = 1000`, so a
cache-miss rehydration of a sealed stream paid that whole-stream resolve once
per page — quadratic. Rehydrating `#storage` measured **180 s**. And a snapshot
did not fix it: a snapshot bounds the *number* of `read_stream` calls, not the
cost of each, so even a correctly snapshotted deep aggregate paid a ~140 ms
floor that grew with total stream depth forever.

**bn-1u6p fixed it**: `ReplaySet::stream_replay_from` skips sealed segments
whose per-stream directory head is below the cursor without decoding their
pointer blocks, binary-searches the one block that straddles it, and stops once
the page is covered; `sealed_and_hot_entries_from` merges that with the
already-bounded hot tail instead of coalescing the whole stream through a
`BTreeMap`. Both branches of `read_stream` now take the cursor and the limit.
Measured on this corpus (release, ABBA over the two binaries reading the *same*
store, loadavg 4.5–10):

| cell (`#storage`, 1,370,418 events, 27 sealed segments) | before | after |
|---|---|---|
| `scrollback` page, 20 records | 121–145 ms | **0.01–0.03 ms** |
| one `read_stream`, limit 1000, any cursor | 119–146 ms | **0.4–0.5 ms** |
| the same read positioned at the head (418 records) | 125–146 ms | **0.05 ms** |
| cold `EventStore::load` of the aggregate | 178.3 s | **3.46 s** |

The residual 3.46 s is no longer entry resolution: paging the same 1.37 M
records with a 20× larger page (69 `read_stream` calls instead of 1,371) only
moves it to 2.73 s, so ~2.7 s of it is the per-record cost of fetching and
materialising 1.37 M records through the sealed payload accelerator — the work
the read actually asked for.

By contrast, the checkpoint resume is doing exactly what it promises: at
`--scale large` the read model rebuilds from the log in **8.15 s** and resumes
from its checkpoint in **0.007 s**, byte-identical (`chatter rebuild`).

## Layout

```
src/
  lib.rs            stream naming, the tour
  id.rs             UUIDv7 rendered as 26-char Crockford base32
  domain/
    channel.rs      the DEEP stream: bounded state, ordinals, the reaction check
    user.rs         the SHALLOW stream: registry pressure
    snapshot_codec.rs
  ops.rs            WriteOps: one command_cached call per action
  seed.rs           the deterministic, chunk-planned corpus generator
  projections.rs    per-channel + global timeline, checkpoint sidecar
  rebuild.rs        the checkpoint byte-compare proof
  scrollback.rs     backward paging through sealed history
  tail.rs           a subscriber built on read_global_page + frontier
  bench.rs          the five cells and their JSON
  store_backend.rs  the store type, the knobs, the sealed census
  bin/chatter.rs    the CLI
tests/
  segments.rs       THE assertion: a demo seed leaves >1 sealed segment
  checkpoint.rs     the discardable-acceleration law
  determinism.rs    same seed, same corpus
  read_paths.rs     scroll-back and tail over a multi-segment store
  bench_json.rs     the bench output contract
  cli.rs            the binary, end to end
```
