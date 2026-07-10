# Bulk writes: batching and pipelining guidance for bulk producers

Status: **guide** (non-normative). Companion to `docs/perf/envelope.md` (the
hand-recorded gate numbers) and `docs/perf/experiments-d10.md` (the
benchmark-gated spike ledger) — this page is neither a gate nor a spike, it's
operational guidance for anyone writing a lot of events through
[`EventStore`](../../crates/mess-store/src/store.rs) in one run: a bulk
importer, a fixture/seed generator, a backfill job. Promoted to a docs page by
bn-162.

## 0. The dogfood finding

`examples/social`'s seed generator (bn-1mw, `examples/social/src/seed.rs`)
writes its ~50-user demo corpus through real
[`EventStore::command`](../../crates/mess-store/src/store.rs) calls only — no
raw-append shortcut, so every invariant is enforced exactly as in production
(see that file's module docs). At the default shape that's **1,488
sequential command round trips**: ~8.5s on a quiet machine, 50-70s observed
under host contention. Fine at demo scale. A 10x seed, or any real bulk
importer, wants better than one-round-trip-at-a-time.

The fix does not need a new store primitive. bn-1s0 already made concurrent
commands to **distinct** streams safe and non-conflicting; this page
documents that fact (§2), demonstrates it with a measured, safety-checked
test (§6), and gives an honest answer to "should there be a first-class
batch API instead" (§5).

## 1. Why sequential command round trips are slow: latency-bound, not throughput-bound

[`EventStore::command`](../../crates/mess-store/src/store.rs) is
`load → decide → append`, with optimistic retry on conflict:

```rust
loop {
    let loaded = self.load::<A>(stream_id).await?;      // read the stream
    let events = loaded.state.decide(cmd.clone())?;      // pure fn, no I/O
    match self.backend.append_batch(stream_id, loaded.version, &records).await {
        Ok(appended) => return Ok(appended),
        Err(AppendError::Conflict { .. }) => { /* reload, redecide, retry */ }
        ...
    }
}
```

`mess-store` is an embedded/in-process library, not a client talking to a
remote server — so "latency" here isn't network RTT. It's the **durability
barrier**: a production engine under `Durability::Group` or `Durability::Os`
(`docs/spec/03-durability.md`) does a real `fdatasync` per committed group,
and the committer's early-close design has a specific, documented property
(§2.2 of that spec):

> With early-close, the design **degrades to sync-per-batch when there is
> only one writer in flight** — confirmed equal, not merely similar
> (`perf_group_commit/REPORT.md` §4 H1).

A sequential loop of `command` calls is *exactly* that case: each call is the
*only* request the committer's gather point ever sees, so each one pays a
full, un-amortized `fdatasync`. `docs/perf/envelope.md` records that barrier
at **~2.56ms mean** on this reference host's settled NVMe device
(`mess_log.durable.mean_fsync_ms`) — small in isolation, but linear in event
count: 1,488 of them is ~3.8s of pure barrier time before you've counted
scheduling, decode, or fold overhead. That's the whole story: sequential
`command` calls are bound by *how many separate durability barriers you make
the committer take*, not by CPU throughput. The fix is therefore about
**giving the committer more than one writer to coalesce**, not about making
any single round trip cheaper.

## 2. The blessed pattern: bounded concurrency over commands to DISTINCT streams

### 2.1 Why it's safe

`LogEngine`'s per-stream [`AppendGate`](../../crates/mess-store/src/engine.rs)
(bn-1s0) serialises the check-head → append → publish critical section **per
stream**, not store-wide:

> Serialises the check-head → reserve critical section **per stream**, so
> appends to different streams no longer queue behind one lock while an
> `Exact(v)` race on the *same* stream still resolves to exactly one winner.

Two commands to two different stream ids never contend for the same shard's
version check (mod a bounded, non-correctness-affecting shard collision —
`APPEND_GATE_SHARDS = 256`), so there is no `AppendError::Conflict` risk
between them at all. And the durable committer itself is a **single gather
point per store** (`docs/spec/03-durability.md` §2.4-§2.5) that never holds
the append path across the barrier — which is precisely what lets
concurrently in-flight requests, from any streams, coalesce into one
`fdatasync` "for free":

> This property ... is what gives group commit its pipelining for free ...
> an explicit pipeline stage ... produced **no additional win** over
> inline-barrier code, because a design that already never holds the append
> path across the barrier gets the overlap automatically.

`crates/mess-store/tests/engine_append_gate.rs`'s
`distinct_streams_overlap_under_durable_commit_path` proves this directly one
layer down (raw `append_batch`, not the `command` facade): K concurrent
appends to K distinct streams under a real durability barrier are at least
2x faster than the same K appends run sequentially, because the committer
coalesces them into far fewer `fdatasync` calls. §6 below proves the same
thing at the `EventStore::command` layer, plus the safety claim the raw-level
test doesn't need to make (no `decide`/retry loop down there).

### 2.2 The pattern

Bounded concurrency, not unbounded: cap how many commands are in flight at
once (`K`), and never spawn more than that. This workspace has no `futures`
crate dependency anywhere, so the idiomatic no-new-dependency version uses
[`tokio::task::JoinSet`](https://docs.rs/tokio/latest/tokio/task/struct.JoinSet.html)
as a bounded window — functionally the same shape as a bounded
`futures::stream::FuturesUnordered`/`join_all` chunk, if that crate is
already a dependency elsewhere in your project. Sketch (a slightly more
ergonomic, items-in-cmds-out wrapper around the exact primitive
`crates/mess-store/tests/bulk_write_pipelining.rs`'s `run_bounded` uses —
not shipped code):

```rust
use mess_store::{Backend, EventStore};
use mess_core::{Aggregate, Decide};
use tokio::task::JoinSet;

async fn bulk_write<A, C>(
    store: &EventStore<impl Backend>,
    items: impl IntoIterator<Item = (String, C)>,
    concurrency: usize,
) where
    A: Aggregate + Decide<C> + 'static,
    C: Clone + Send + 'static,
{
    let mut items = items.into_iter();
    let mut set = JoinSet::new();

    // Fill the window.
    for (stream_id, cmd) in items.by_ref().take(concurrency) {
        let store = store.clone();
        set.spawn(async move { store.command::<A, _>(&stream_id, cmd).await });
    }
    // Refill one-for-one as each completes.
    while let Some(res) = set.join_next().await {
        res.expect("task panicked").expect("command failed");
        if let Some((stream_id, cmd)) = items.next() {
            let store = store.clone();
            set.spawn(async move { store.command::<A, _>(&stream_id, cmd).await });
        }
    }
}
```

`crates/mess-store/tests/bulk_write_pipelining.rs`'s `run_bounded` is this
exact pattern (§6). `EventStore<B>` is cheap to `Clone` whenever `B` is
(`LogEngine` is `Arc`-backed) — see the type's doc comment — so spawning a
clone per task is the right way to share one store handle across the window,
not a `Mutex<EventStore<_>>` or similar.

Pick `K` empirically, not from a formula: it needs to be large enough that
the committer's gather window (`max_delay`, `docs/spec/03-durability.md`
§2.2, 1ms by the spec's recommended default) sees more than one writer
arrive, but there's no benefit to K far beyond what one `max_bytes` window
can hold (§4 below explains why "more" isn't "faster" past that point).
`crates/mess-store/tests/bulk_write_pipelining.rs` uses `K=16`-`32` and gets
7-23x; that's a reasonable starting range to tune from, not a promise.

### 2.3 Why NOT same-stream concurrency

The per-stream `AppendGate` makes *distinct*-stream concurrency safe; it does
**not** make *same*-stream concurrency fast. If N commands race the same
stream:

1. All N call `load`, most of them observing the *same* version (whichever
   racer's `append_batch` hasn't landed yet).
2. All N call `decide` against that state — wasted work for every loser.
3. All N call `append_batch` at the same expected version. The `AppendGate`
   still resolves this to exactly one winner (that's its whole job — see
   `engine_append_gate.rs`'s `same_stream_exact_version_race_has_exactly_one_winner`,
   which proves exactly-one-winner and exactly-correct-conflict-actual for
   32 racers on one stream) — but every loser gets
   `AppendError::Conflict` and `command`'s retry loop reloads, redecides,
   and resubmits, after a jittered backoff sleep (`RetryPolicy`).

So same-stream fan-out isn't parallelism, it's **optimistic-retry
contention wearing concurrency's clothes**: the winner is still fully
serialised by the gate (same-stream appends never actually overlap inside
it), and every loser pays a strictly *worse* cost than a sequential call
would have (a full reload + redecide + backoff, for a write that was always
going to have to wait its turn anyway). At high fan-out this can be a
genuine thundering herd — `command_cached`'s own doc comment calls out the
identical hazard for its (cheaper) delta retry: "cheap retries collide
back-to-back, where the uncached full reload incidentally spaces writers
out", and a policy without real jittered backoff "can let a thundering herd
starve one writer into conflict exhaustion." Plain `command`'s full-reload
retry pays that same collision hazard while being strictly more expensive
per collision, not less. If a single stream in your bulk job legitimately needs many
commands, keep those commands to *that* stream sequential (they'll still run
fast, riding the committer's gather window alongside whatever *other*
streams' pipelined commands are concurrently in flight) — don't fan them out
concurrently against each other.

## 3. Where `command_cached` helps, and where it doesn't

[`EventStore::command_cached`](../../crates/mess-store/src/store.rs) is a
different axis from pipelining, and the two compose rather than substitute:

- **Helps**: repeated commands against the **same** stream, warmed by a
  prior hit. A cache hit skips `load`'s full replay entirely (the version is
  proven by the append itself); a conflict retry only re-reads the *delta*
  since the cached version, not the whole stream from scratch. If your bulk
  job's shape has some streams that receive many commands over the run (a
  hot aggregate accumulating events one command at a time), routing those
  through `command_cached` cuts each one's read cost from O(stream length)
  to O(1) on the warm path.
- **Doesn't help with the fsync-bound problem this page is about**:
  `command_cached` still ends in the exact same `append_batch` call through
  the exact same committer — it changes *read* cost, not the durability
  barrier `command` and `command_cached` equally pay per append. Pipelining
  is what amortizes *that* cost; caching does nothing for it on its own.
- **Doesn't help the bulk-import shape specifically**: a seeder/importer's
  dominant pattern is usually "many streams, written once or a handful of
  times each" (`examples/social`'s seed: ~500 posts, ~50 users, each mostly
  opened and touched a few times, never thousands of times on one stream).
  There's no "warm" state to reuse when a stream is cold on every call — the
  cache-miss path degrades to the identical `load_cached` cost `command`
  already pays, so `command_cached` buys nothing over plain `command` for
  that shape.
- **Doesn't make same-stream fan-out safe**: concurrent `command_cached`
  calls against the same stream race the identical `AppendGate` version
  check as plain `command` and degrade into the same optimistic-retry
  contention (§2.3) — cheaper per retry, but not a different *pattern*, and
  the module's own doc comment says as much (quoted above).

**Net**: pipeline across distinct streams for the win this page is about;
reach for `command_cached` *in addition*, only for whichever streams in your
job are genuinely hot (many commands, same stream) — not as a substitute for
bounded concurrency, and not as a way to make same-stream fan-out safe.

## 4. What NOT to do

- **Unbounded spawn.** `for item in items { tokio::spawn(...) }` with no cap
  hands the committer an unbounded convoy at once. It doesn't buy more
  coalescing than a well-sized bounded window already gets — the group
  commit window has its own cap (`max_bytes`, 8 MiB by the spec's
  recommended default, `docs/spec/03-durability.md` §2.2) and simply closes
  and starts a new group once it's full, so a burst far beyond what one
  window can hold just becomes several groups instead of a magically bigger
  one. What unbounded spawn *does* buy is unbounded memory for in-flight
  command state and unbounded pressure on the async runtime and the
  `spawn_blocking` pool the committer bridges through
  (`crates/mess-store/src/engine.rs`'s module docs, "Durability spine").
  Bound it (§2.2).
- **Same-stream fan-out.** Covered in §2.3 — it's not faster, and can be
  actively slower than sequential once retries and backoff are counted.
- **Reaching for a batch API before measuring.** See §5 — profile first;
  the pipelining pattern above already captures the documented win.

## 5. Should there be a first-class multi-command batch API?

Evaluated for this bone (bn-162); **not implemented, and not warranted
yet.** Short version: the durable committer's group-commit design already
amortizes `fdatasync` cost across **any** concurrently in-flight
`append_batch` calls, from any streams, regardless of which API surface
issued them (`docs/spec/03-durability.md` §2.4-§2.5, quoted in §2.1). A
hypothetical `EventStore::command_batch(Vec<(stream_id, cmd)>)` that
internally ran the exact bounded-concurrency loop of §2.2 would get the
*identical* coalescing the caller already gets by calling that loop
themselves — because it would be built on the same `append_batch` calls,
funneled through the same single gather point. There is no fsync
amortization left on the table that only a new store-level API could reach;
§6's measured numbers (7-23x speedup, all in `EventStore::command`-level
code, zero new store API) are the proof.

Two things *would* be worth a real bone, if profiling ever shows the
per-task overhead below actually dominating over the durability barrier
(it does not today — see §6's numbers, where the barrier is still the
overwhelming cost even at 23x speedup):

1. **A `command_many`-shaped ergonomic wrapper** — literally §2.2's
   `bulk_write` helper, promoted into `mess-store` itself so every bulk
   producer stops hand-rolling the same `JoinSet` loop. This is a pure DX
   win (less boilerplate, one canonical bounded-concurrency
   implementation to get right and keep right), **not** a throughput win
   beyond what §2.2 already gets, since it would be implemented in exactly
   those terms. Worth doing whenever a second or third caller needs the
   pattern (`examples/social`'s seeder is a first candidate — see bn-gpq's
   child bn-o9z); not worth a dedicated performance bone on its own.
2. **A genuinely new capability, not a wrapper**: the committer's group
   commit already amortizes the barrier across concurrent callers, but each
   caller's `load` still pays its own `read_stream` round trip even for a
   brand-new (`Version::NoStream`) stream — cheap (in-process, no I/O) but
   not free, and it's paid once per command regardless of pipelining. A
   batch API that knows *up front* which streams are new (skip the read
   entirely) or that fuses many streams' `decide` outputs into one
   already-locked committer submission (skipping N separate `AppendGate`
   acquisitions in favor of one) could shave that residual per-command
   overhead further. This is speculative and unmeasured — nothing in this
   bone's numbers shows it's the bottleneck (the barrier still dominates by
   1-2 orders of magnitude at present event sizes) — and it would need its
   own design pass against `crates/mess-store/src/engine.rs`'s `AppendGate`/
   `PublishSequencer` machinery before it's more than a hunch. File-worthy
   only once a real workload's profile points at per-command scheduling/read
   overhead specifically, not before.

Filed for the lead to consider boning up if wanted: item 1 above (the
`command_many` ergonomic wrapper) is the more concrete of the two — a small,
low-risk addition once `examples/social`'s seeder (bn-gpq → bn-o9z) actually
adopts the pipelining pattern and a second call site exists to generalize
from. Item 2 is explicitly speculative and should stay unbuilt until
profiling data says otherwise.

## 6. The measured demonstration

`crates/mess-store/tests/bulk_write_pipelining.rs` is the runnable proof
behind this page's claims — both of them, not just the speed one:

- **Safety**: it builds two independent durable engines, primes both with
  the same `N` distinct streams (outside either timed phase, so
  brand-new-stream-name interning cost — a real but *different* durability
  cost, bn-150 — never confounds the measurement), then writes the same
  second event to every stream once **sequentially** on one store and once
  **pipelined** (bounded concurrency, §2.2's exact pattern) on the other.
  It then asserts, per stream, that the two stores' final heads
  (`Loaded::version`) and folded states are identical — the actual safety
  claim, not an inference from the speed number.
- **Speed**: both phases are timed and the ratio is printed. Both run
  against a real (non-tmpfs) `Durability::Group` engine at the spec's
  recommended defaults (`max_delay: 1ms`, `max_bytes: 8MiB`) — the engine's
  own default (`Durability::Process`) is buffered/no-fsync and would hide
  the exact effect this page is about.

Two variants:

| variant | N | K | scope |
|---|---|---|---|
| `pipelined_distinct_stream_commands_match_sequential_and_are_faster` | 64 | 16 | default (`cargo test -p mess-store`), fast |
| `pipelined_bulk_write_bench_at_seeder_scale` | 1,488 | 32 | `#[ignore]`d, matches bn-1mw's actual seed scale |

**Numbers (this host, 2026-07-10):**

| metric | value | conditions | source command |
|---|---|---|---|
| bulk_write.fast.sequential_ms | ~175-200 | N=64 distinct streams, sequential `command` calls, `Durability::Group` (1ms/8MiB) | `cargo test -p mess-store --test bulk_write_pipelining -- --nocapture` |
| bulk_write.fast.pipelined_ms | ~22-28 | same N=64, K=16 bounded concurrency | as above |
| bulk_write.fast.speedup | 7.2-8.1x | derived, two runs this host | as above |
| bulk_write.seeder_scale.sequential_s | 3.874 | N=1,488 (bn-1mw's exact seed scale), sequential, `--release` | `cargo test -p mess-store --release --test bulk_write_pipelining -- --ignored --nocapture` |
| bulk_write.seeder_scale.pipelined_s | 0.170 | same N=1,488, K=32 | as above |
| bulk_write.seeder_scale.speedup | 22.73x | derived | as above |

The seeder-scale sequential number (3.87s) is faster than the original
dogfood's 8.5s because this harness's commands are minimal (`decide` is a
one-liner, no growing per-stream state to replay) — it isolates the
durability-barrier cost specifically, rather than reproducing the full
heterogeneous seed (users/follows/posts/likes/deletes with real, larger
`decide` logic and growing replay cost). The point isn't to reproduce 8.5s
exactly; it's that the *same mechanism* (one un-amortized `fdatasync` per
sequential command) is what both numbers are measuring, and pipelining
removes it in both cases. `examples/social`'s own seeder gets pipelined
under goal bn-gpq's child bn-o9z, which should cite this page and these
numbers as its starting evidence rather than re-deriving them.
