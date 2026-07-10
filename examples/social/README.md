# examples/social

A small Twitter-clone built on `mess` — the second showcase example after
`examples/bank`. Where `bank` teaches the vocabulary on one aggregate, this
crate is the *shape of a real application at scale*: **every aggregate has
bounded state**, a real rebuildable read model, and a server-rendered HTML
frontend, all wired to a real on-disk event log.

This README is the demo tour: what it shows, how to run it, why the
"rebuild from the log" trick works, and a walk through the real `mess`
operational CLI run against the demo's own seeded store.

## What this demonstrates

- **Four aggregates, all with bounded state.** Two *entities* —
  `User` at `user-<id>` (handle, display name) and `Post` at `post-<id>`
  (author, body, delete tombstone) — and two *relationships*, `Like` at
  `like-<post>_<user>` and `Follow` at `follow-<follower>_<followee>`, each a
  tiny alternating two-state machine on its own stream. The crowds (a post's
  likers, a user's followers) are **not** aggregate state anywhere; see
  [Why relationship streams](#why-relationship-streams) below.
  `src/domain/*.rs` are commented in depth on *why* each invariant is enforced
  where it is (e.g. why self-like is allowed but self-follow is refused at the
  write seam).
- **A real, rebuildable read model.** `src/projections.rs`'s `Projections`
  type folds the *entire* event log into in-memory tables by replaying
  `read_global` from position 0, then keeps tailing it live. There is no
  separate "rebuild" code path — construction *is* the rebuild. That is the
  event-sourcing showcase this demo exists to make tangible; see
  [below](#the-rebuild-from-log-party-trick).
- **A write/read seam** (`src/contracts.rs`) — `WriteOps` (one method per
  user action, each exactly one `EventStore::command` call) and `ReadModels`
  (the query surface, with a `wait_for` read-your-writes barrier) — that lets
  the HTTP layer, the projections, and the seed generator all be built and
  tested independently against the same interface.
- **A deterministic demo corpus**, generated entirely through `WriteOps` —
  never a raw log append — so the same domain rules a real client hits
  (`handle_is_valid`, no self-follow, no double-like, author-only delete, …)
  also gate every seeded event. See [Seeding the corpus](#seeding-the-corpus).
- **The real `mess` operational CLI** run against this app's own seeded
  store — not a synthetic fixture. See the [ops tour](#ops-tour) below.

## Quickstart

```sh
# one command: seed + serve
just demo

# or step by step:
cargo run -p social --bin social-seed             # seed ~/.cache/mess-social-demo/store
cargo run -p social --bin social-web -- --dir ~/.cache/mess-social-demo/store
# then open http://127.0.0.1:3000
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
                                   │ query                        │ EventStore::command
                                   ▼                              ▼
                      ┌────────────────────────┐  ┌─────────────────────────────┐
                      │      Projections          │  │    mess_store::EventStore    │
                      │  (projections.rs)          │  │   over mess_store::LogEngine  │
                      │  in-memory fold, rebuilt   │◀─┤                               │
                      │  from position 0 at boot,  │  │  entities:                    │
                      │  then live-tails the log,  │  │    user-<id> / post-<id>      │
                      │  reconstructing the like   │  │  relationships:               │
                      │  & follow crowds from the  │  │    like-<post>_<user>         │
                      │  relationship streams      │  │    follow-<flwr>_<flwee>      │
                      └────────────┬───────────────┘  └────────────┬──────────────────┘
                                   │ read_global(after, limit)      │ append_batch
                                   │  routed by StoredRecord         │
                                   │  ::category() (before 1st '-')  │
                                   └───────────────┬─────────────────┘
                                                    ▼
                                    durable on-disk log
                                    ($STORE_DIR/seg-*.log + meta/)
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
  crowd. Snapshots stay tiny.
- **Crowds live in the projection.** The read model reconstructs like counts,
  `liked_by_me`, and follower/following sets from the relationship streams at
  query time — exactly where a home timeline was already computed by filtering
  posts against the viewer's *current* follow set. (The broader scale story —
  fan-out, sharding — is a later bone; this example just establishes the
  bounded-state shape.)
- **Stream naming.** A relationship stream keys on *both* ids:
  `like-<post>_<user>`. The `_` is a sound separator because an `Id` renders
  only over `[0-9a-z-]` (Crockford base32 plus two internal `-`s) and never
  contains `_`; `StoredRecord::category()` splits at the first `-`, and the
  suffix splits once on `_` back into the two ids. See `src/lib.rs`'s
  `PAIR_SEP` / `parse_pair` for the format invariant.

### Event-vocabulary reset

This rework changes the **on-disk event vocabulary**: `Post` no longer emits
`Liked`/`Unliked` and `User` no longer emits `Followed`/`Unfollowed`; those are
now `Liked`/`Unliked` on `like-*` streams and `Followed`/`Unfollowed` on
`follow-*` streams. For an example crate that is **fine and needs no
migration** — there is no persisted corpus to preserve; `social-seed`
regenerates the whole log from scratch on demand. A production system would
treat this as a real schema change (a new event version and a migration/replay
plan); here it is just a reseed.

## The rebuild-from-log party trick

```sh
# with the server running against a seeded store:
curl -s http://127.0.0.1:3000/ > /tmp/before.html

# stop the server (Ctrl-C), then start it again:
cargo run -p social --bin social-web -- --dir ~/.cache/mess-social-demo/store

curl -s http://127.0.0.1:3000/ > /tmp/after.html
diff /tmp/before.html /tmp/after.html   # <- empty diff
```

This is verified byte-for-byte as part of this bone's own testing (curled
`/` and `/u/<handle>` before and after a real restart — see the bone's final
report). It works because **`Projections::new` does not load a snapshot —
it replays**. On every boot it drains `read_global` from position 0, folding
every `Registered`/`Followed`/`Posted`/`Liked`/… event into the same
in-memory tables the *live* tailer would build incrementally. Restarting the
process does not discard any state, because the process never owned the
state to begin with — the on-disk log did. The read model is a pure,
cacheable *function of the log*, so replaying it twice from the same log
produces the same tables, which render the same HTML (nothing in this
domain's rendering is wall-clock-dependent — post bodies encode a
"time of day" *flavor* as text, never an actual timestamp — so there is no
hidden nondeterminism to break the byte-for-byte claim).

The same property is what makes `mess backup` + `mess restore` trustworthy
(see the [ops tour](#ops-tour)): a restored store, served by `social-web`,
renders the identical page too — verified below.

## Seeding the corpus

`social-seed` (`src/bin/social-seed.rs`, generator in `src/seed.rs`) drives:

- **50 users** — handles and display names from a small phrase combinator
  (`{adjective}_{noun}[digits]` handles, `{First} {Surname}` display names;
  see `src/seed.rs`'s `ADJ`/`NOUN`/`FIRST`/`SURNAME` word pools), never
  lorem ipsum.
- **A Zipf-ish follow graph** — a handful of "hub" users end up widely
  followed (popularity is weighted `1 / (rank+1)^1.0` over registration
  order), most users follow just one to three people.
- **500 posts** with varied, human-plausible bodies from a 20-template phrase
  combinator crossed with 30 topics and a set of "time of day" flavor
  prefixes (`"3am and just discovered {topic}..."`,
  `"monday morning, rabbit-holing on {topic} again."`) — the
  "timestamps-in-content" texture the domain itself has no `created_at`
  field to carry.
- **Zipf-distributed likes** — a handful of posts go semi-viral, most get
  zero or one like.
- **30 deletes** (author-authorized, after a post has accrued some likes) and
  **20 unfollows**.

Every single one of those actions is a real `WriteOps` call —
`store.register(...)`, `store.follow(...)`, `store.create_post(...)`,
`store.like(...)`, `store.delete_post(...)`, `store.unfollow(...)` — which is
to say, a real `EventStore::command` call that runs the real `Decide` impl.
There is no bulk-append shortcut; the generator tracks its own shadow state
(who follows whom, who liked what) purely to avoid *wasting* a command
attempt on a rejection it can see coming for free, not to skip validation.

**Determinism.** Every draw comes from one seeded `StdRng`
(`--seed`, default `1337`) — same seed, same corpus, byte-for-byte (including
the generated `Id`s, via `Id::from_u128` fed by the same RNG stream). This is
covered by a real test
(`seed::tests::generate_is_deterministic_for_a_fixed_seed`) that seeds two
independent stores and diffs their entire logs event-for-event.

**Fresh-dir guard.** `guard_fresh_dir` (`src/seed.rs`) mirrors
`mess_soak::resource::guard_fresh_dir` (bn-3dr): seeding into a directory
that already holds a store would silently double the corpus or mix two
seeds' worth of state, so `social-seed` refuses by default:

```
$ cargo run -p social --bin social-seed
refusing to seed on non-empty --dir /home/bob/.cache/mess-social-demo/store: found
a leftover store (1 seg-*.log segment file(s), markers: [LOCK, meta, sealed]) —
seeding assumes an empty starting log, so pre-existing events would either double
the corpus or mix two seeds' worth of state, silently breaking the "same seed,
same corpus" guarantee. Pass --force to wipe the directory first, or point --dir
at a fresh path.
```

`--force` wipes `--dir` first (what `just demo` passes, so reseeding is
always one command).

**Timing** (measured on this box, real fsync-per-append, no batching):

```
$ cargo run -p social --bin social-seed -- --force
seeding /home/bob/.cache/mess-social-demo/store (seed=1337) ...
done in 8.57s: 50 users, 88 follows, 500 posts, 800 likes, 30 deletes, 20 unfollows
next: cargo run -p social --bin social-web -- --dir /home/bob/.cache/mess-social-demo/store
```

~1,488 total `EventStore::command` calls in 8.57s — comfortably inside the
60s budget, with room to spare even on a slower disk.

## Ops tour

Everything below is the **real** `mess` CLI (`crates/mess-cli`), run against
this demo's own seeded store (`~/.cache/mess-social-demo/store`, 50 users +
500 posts + likes/deletes/unfollows, 1,488 events), output trimmed for
length but otherwise unedited.

> **Note — this transcript predates the bounded-state (relationship-stream)
> rework.** The **event** count is unchanged (each action is still exactly one
> event: 50 + 88 + 500 + 800 + 30 + 20 = 1,488), so `doctor`, `verify`, and the
> `backup`/`restore` watermarks below all still read `1,488`. What *did* change
> is the **stream** count: likes and follows are now their own streams, so the
> store holds ~1,438 streams (50 `user-` + 500 `post-` + 88 `follow-` + 800
> `like-`) rather than 550. The `inspect` numbers below (`550 entries`,
> `stream_id` 1..550) are the pre-rework capture; rerun `mess inspect` after a
> reseed to see the relationship streams. The dogfood findings the tour makes
> are unaffected.

### `mess doctor` — is the store healthy?

```
$ cargo run -p mess-cli --bin mess -- doctor ~/.cache/mess-social-demo/store --format pretty
mess doctor
  dir: "/home/bob/.cache/mess-social-demo/store"
  epochs_seen: [1]
  fold_versions: []

[OK] lock-free: store is not locked by a live writer
[OK] unsealed-head: segment 1: unsealed active/rolled head (no trailer)
[OK] fsync-ok: store directory is writable and fdatasync succeeded
[OK] no-snapshots: no live snapshots to check for fold drift

[OK] 4 finding(s); worst = ok
```

Proves: the lock is free (no crashed process still holding it), the segment
chain's epoch/trailer state is sane, the store directory can actually durably
fsync (catches read-only mounts / permission drift before an append would),
and there is no `fold_version` drift across live snapshots (none exist yet —
this demo never opts into snapshotting).

**Dogfood: doctor tolerates a live writer, but degrades one check.** Run
against the *same* store while `social-web --dir ...` is running:

```
info  lock-held  lock  store is locked by a live writer (pid 2233883)
ok  unsealed-head  trailer  segment 1: unsealed active/rolled head (no trailer)
ok  fsync-ok  fsync  store directory is writable and fdatasync succeeded
info  registry-unavailable  fold-version  could not read snapshot metadata for fold-version check: FjallError: Locked
```

Good behavior — it reports the lock holder's pid rather than erroring out —
but the `fold-version` check needs the fjall meta store, which the live
writer holds exclusively, so that one check degrades from "checked" to
"could not read" while the app is up. Worth knowing before reaching for
`doctor` as a live health probe rather than an offline one.

**Resolved (bn-ve0): the degraded finding now says why and what to do,
instead of surfacing the raw `FjallError`.** Same repro, after the fix:

```
info  lock-held  lock  store is locked by a live writer (pid 2233883)
ok  unsealed-head  trailer  segment 1: unsealed active/rolled head (no trailer)
ok  fsync-ok  fsync  store directory is writable and fdatasync succeeded
info  meta-store-locked  fold-version  fold-version check skipped: a live writer holds the metadata store's lock (pid 2233883). This is expected while the app is running, not an error. For the full check, stop the writer first, or run doctor against a `mess backup`/`mess restore` copy instead of the live directory.
```

Still `info` severity, still zero exit code — this was never a failure — but
the message no longer requires knowing what `FjallError: Locked` means or
which store it's talking about. `doctor --help` documents the same caveat up
front. A read-only fallback (open the meta store without the exclusive lock,
so this check could run against a live writer too) was investigated and
rejected for the pinned fjall version: fjall 3.1.6's `Database::open` has no
read-only/secondary open mode at all — every open path takes the same
exclusive lock — and, separately, fjall's metadata tables are
journal-buffered (not fsynced per write), so even bypassing the lock to read
the on-disk files directly could return stale/incomplete data instead of
failing loudly, which is worse for a health check than today's honest
degradation. See `crates/mess-cli/src/metaread.rs`'s module doc for the full
writeup.

### `mess inspect` — segment chain, stream heads, registry

```
$ cargo run -p mess-cli --bin mess -- inspect ~/.cache/mess-social-demo/store --format pretty
mess inspect
  dir: "/home/bob/.cache/mess-social-demo/store"
  lock: {"pid":2233883,"state":"held"}
  metrics: {"active_segment_count":1,"durable_batch_count":1488,"durable_event_count":1488,"sealed_segment_count":0,"segment_count":1,"total_size_bytes":239587}
  registry_source: "recovered"
  stream_heads: [{"head_version":11,"name":null,"stream_id":1},{"head_version":1,"name":null,"stream_id":2}, ... 550 entries total]

  {"base_pos":0,"batch_count":1488,"epoch":1,"event_count":1488,"segment_id":1,"sealed":false,"size_bytes":239587}

[OK] 0 finding(s); worst = ok
```

Proves: the on-disk segment chain matches the durable event count this app's
own log claims (1,488 events, matching `social-seed`'s report exactly), and
every one of the 550 streams (50 `user-<id>` + 500 `post-<id>`) this app ever
wrote to has a recorded head version.

**Dogfood: `inspect`'s default (agent/pipe) format drops almost everything.**
`Report::to_text()` — the non-TTY default the CLI-conventions doc calls
"token-efficient... for agents/pipes" — only renders `findings` and the named
`collection` (here, the one-row segment scan); it never renders
`Report::extra`, which is where `dir`, `lock`, `metrics`, `registry_source`,
and `stream_heads` all live for `inspect`. So plain, piped
`mess inspect <dir>` (exactly how an agent would invoke it) prints a single
JSON blob and nothing else — no lock state, no stream count, nothing — while
`--format pretty`/`--json` show the full picture. This is a real gap found by
running `inspect` against an app-shaped store rather than a synthetic
fixture with few streams: `doctor`'s useful content lives in `findings`
(text-format-visible), but `inspect`'s lives in `extra` (text-format-blind).

**Dogfood: `stream_heads` has no truncation and no human-readable id.** With
550 streams this array is enormous and every entry is `{"stream_id": <opaque
interned integer>, "name": null}` — there is no way to go from
`user-<the Id this app knows>` to its numeric `stream_id` without an
out-of-band lookup (`--stream <numeric-id>` narrows the *output*, but you
still have to already know the number). A real app with hundreds of streams
would want either pagination or a `--stream-prefix user-` filter.

### `mess verify --full` — recovery scan + byte integrity

```
$ cargo run -p mess-cli --bin mess -- verify ~/.cache/mess-social-demo/store --full --format pretty
mess verify
  dir: "/home/bob/.cache/mess-social-demo/store"
  full: true
  repair: false
  verified: true

[OK] unsealed-segment-scanned: segment 1: unsealed, 1488 committed batch(es), tail stop = clean

[OK] 1 finding(s); worst = ok
```

Proves: every one of the 1,488 committed batches passes the acceptance-kernel
scan (CRCs, structural framing) and the scan reaches a *clean* tail stop
(no truncated/torn last record) — with `--full`, this also reassembles
payload blocks and would recompute the fold chain on any chain-enabled
batch (this demo never opts into `crypto_chain`, so that half is a no-op
here, correctly reported rather than silently skipped). Ran in ~7ms against
1,488 events — the recovery scanner is not the bottleneck.

### `mess backup` + `mess restore` — the durability round trip

```
$ cargo run -p mess-cli --bin mess -- backup ~/.cache/mess-social-demo/store \
    --to ~/.cache/mess-social-demo/backup --format pretty
mess backup
  copied_bytes: 581705
  copied_files: 24
  dest: "/home/bob/.cache/mess-social-demo/backup"
  lease_id: "bkp-2239497-1783710951"
  watermark: 1488

[OK] backup-complete: backup complete: 24 file(s) copied, 0 skipped, watermark 1488

$ cargo run -p mess-cli --bin mess -- restore ~/.cache/mess-social-demo/backup \
    --to ~/.cache/mess-social-demo/restored --format pretty
mess restore
  dir: "/home/bob/.cache/mess-social-demo/restored"
  manifest_watermark: 1488
  recovered_watermark: 1488
  restored_bytes: 581705
  restored_files: 24
  verified: true

[OK] restore-complete: restored 24 file(s); verify --full clean; recovered watermark 1488
```

Proves: `backup` took a consistent cut (a retention lease held for the copy,
`BACKUP_MANIFEST` written last) of a store with a *live writer attached*
(this ran while `social-web` was serving requests against the same store);
`restore` copied it back, re-ran `verify --full`, and its
`recovered_watermark` matches the manifest's `1488` exactly — no events lost,
none duplicated. And it is not just a byte-count claim: pointing
`social-web --dir ~/.cache/mess-social-demo/restored` at the restored copy
serves **byte-identical HTML** to the original store, verified with `diff`
during this bone's own testing — the restore round-trips the *application*,
not just the files.

## Development

```sh
cargo test -p social                              # unit + integration + doc tests
cargo clippy -p social --all-targets -- -D warnings
just demo                                          # seed --force, then serve
```

`src/seed.rs`'s tests cover: the requested corpus shape, cross-run
determinism (two independently-seeded stores' logs diff byte-for-byte), the
fresh-dir guard's accept/refuse behavior, and that every generated handle
and post body satisfies the domain's own validation (`handle_is_valid`,
`BODY_MAX_LEN`).
