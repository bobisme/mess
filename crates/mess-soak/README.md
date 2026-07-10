# mess-soak (bn-1fo)

A multi-hour mixed-workload **soak** for the composed production engine
(`mess-store`'s `LogEngine` = `mess-log` durability spine + `mess-index`
tiers + the `EventStore` facade). It exists to surface the bugs that only
appear at hour three: slow leaks, seal/retention churn interactions,
watermark drift, fsync degradation on aging devices (the round-4 50x
finding), dedupe-window turnover. No unit test runs long enough to find them.

The soak drives a **real** engine on a **real** filesystem and checks it
against an in-driver shadow model on every action. The exit criteria are
invariants and resource ceilings — **not** throughput. The first violation
dumps a reproducible state bundle and aborts.

## Quick start

```bash
# Fast local sanity (20s, ~3 in-process crash cycles, all probes on):
cargo test -p mess-soak            # the CI smoke + probe unit tests

# Ad-hoc run (drop-and-reopen crashes, in-process):
cargo run -p mess-soak --release -- --duration 300 --dir "$HOME/.cache/mess-soak"

# Faithful SIGKILL crashes (forks + kills a child worker):
cargo run -p mess-soak --release -- --crash-mode sigkill --duration 300 \
    --crash-every 5 --dir "$HOME/.cache/mess-soak"
```

`--dir` **must not be tmpfs** — the driver reads `/proc/mounts` and refuses,
because `fdatasync` is a no-op on tmpfs and a durability/crash soak there
validates nothing. `/tmp` is tmpfs on the dev box; use `$HOME/.cache/...`
(ext4) or any real device.

Run `cargo run -p mess-soak -- --help` for every flag and its default.

## The 2h nightly profile

The nightly runs the faithful-crash mode for a bounded two-hour window and
exits non-zero on any invariant violation or resource-ceiling breach:

```bash
cargo build -p mess-soak --release
./target/release/mess-soak \
    --crash-mode sigkill \
    --duration 7200 \
    --crash-every 20 \
    --streams 512 \
    --writers 8 \
    --subscribers 8 \
    --zipf-skew 1.1 \
    --segment-size 262144 \
    --durability os \
    --rss-ceiling 1073741824 \
    --fd-ceiling 512 \
    --seed "$RANDOM$RANDOM" \
    --dir /var/lib/mess-soak/nightly
```

Pre-release, widen the window (`--duration 28800` for 8h) and raise
`--streams`/`--writers`. A green run means: zero invariant violations, RSS
and fd count plateaued (never crossed their ceilings), and every kill/recover
round reconciled its ack ledger against recovery. Recovery time per crash is
printed in the periodic metrics line (`reopen(last/max)=…`).

> **Note on the two crash modes.** `--crash-mode drop` (default) drops the
> engine handle in-process and reopens. That is a *graceful* shutdown in
> disguise — `Inner::drop` shuts the committer and joins the seal thread, so
> every destructor runs — but it exercises the full open → recover →
> rehydrate → resume-in-place path continuously with the shadow model
> preserved, which is what lets the index/density/subscription probes keep
> checking across a "restart". `--crash-mode sigkill` is the faithful "hour
> three, the box lost power / OOM-killer fired" crash: it forks the
> `soak-child` worker (real `LogEngine` under `Durability::Os`, so every acked
> append is `fdatasync`ed before its ack is reported on the pipe), `SIGKILL`s
> it mid-flight (no destructors run), then reopens and reconciles. Nightly and
> pre-release should use `sigkill`; the CI smoke uses `drop` because it must
> run purely in-process and deterministically.

## What each probe checks

Every probe is a **pure function** in `src/probe.rs` returning `Result<(),
Violation>`, so each is unit-tested by feeding it a doctored input (see
`probe::tests`). The driver supplies the real engine reads and the shadow
expectation; the decision logic is isolated and tested.

| Probe | Invariant | Fires when… |
|-------|-----------|-------------|
| **index == log** (`check_record`) | Every read tier returns the exact durable `(stream, type, payload, positions)`. Sampled at a random global position via **both** `read_global` and `read_stream` (hot `ActiveIndex` *or* sealed cold `ReplaySet`, whichever the stream currently routes through). | A read tier returns bytes/type/position that disagree with what was durably appended. |
| **per-stream density** (`check_density`) | A stream's `stream_position`s are exactly `0..n` — no gap, no duplicate — and the count matches the shadow (no truncated tail). | A stream develops a gap, a duplicate position, or loses its tail. |
| **head agreement** (`check_head`) | `engine.head(stream)` equals the shadow head. | A stream's version regresses or races ahead of durable history. |
| **subscription gap** (`SubCursor::observe`) | A subscriber consuming `read_global` sees **every** global position **exactly once, in order**. Subscribers join (full or at the live tail) and leave throughout the run. | A subscriber misses an event, sees one twice, or sees them out of order. |
| **RSS ceiling** (`check_rss`) | Resident set size stays under `--rss-ceiling`. | A leak marches RSS past the ceiling (the "plateau" exit criterion). |
| **fd ceiling** (`check_fd`) | Open fds (`/proc/self/fd`) stay under `--fd-ceiling`. | Aging seals / subscriptions leak file handles. |
| **fsync p99** (`check_fsync_p99`) | Append (= `fdatasync` under `Durability::Os`) p99 latency stays under `--fsync-p99-ceiling`. | The device/durability path degrades (off by default; p99 is always *printed*, the abort is opt-in). |

On top of the per-action probes, every **crash+reopen** runs a whole-store
reconciliation: the recovered engine must present exactly the shadow (same
total, dense global `0..N` prefix, matching sampled heads). In `sigkill` mode
the reconciliation is against the child's **ack ledger**: every acked
`(stream, stream_pos, global_pos)` the parent received before the kill must be
present and unchanged after recovery, the recovered prefix must be dense, and
extra un-reported tail events (acked but killed before the stdout write) are
legal — the correct crash-safety contract (`acked ⟹ durable`, no torn or
missing slot).

## Interpreting an abort dump

Any violation prints a block like:

```
================= SOAK INVARIANT VIOLATION =================
VIOLATION: INDEX!=LOG[stream-00042@17] data: expected 24B[..], got 24B[..]
----------------------------------------------------------
REPRODUCE: mess-soak --seed 0x50ac5eed --dir /var/lib/mess-soak/nightly \
    --streams 512 --writers 8 --duration <>=elapsed> --crash-every 20s
(the store dir is left intact on disk for post-mortem)
----------------------------------------------------------
CONFIG: <full config>
----------------------------------------------------------
STATE: elapsed=…  actions=…  appends=…  events=…  crashes=…
  index_checks=… density_checks=… head_checks=… subscription_reads=…
  shadow_total=…  shadow_dense=true  engine_total=…  sealed_segments=…
  reopen(last/max)=…  rss=…B  fd=…
  fsync[n=… mean=… p50=… p99=… max=…]
=================================================================
```

How to read it:

1. **VIOLATION** names the exact invariant and the offending
   stream/position/field, with a hex preview of expected vs. got bytes.
2. **REPRODUCE** is the command to re-run against the **same store dir**,
   which is **left intact on disk** (the driver never deletes it on abort).
   The seed pins the workload sequence; for a `drop`-mode abort the run up to
   `elapsed` is reproducible from `(seed, config)`. (`sigkill` mode injects
   real wall-clock nondeterminism by construction, so there the store dir
   itself is the reproduction, not the seed.)
3. **STATE** is the forensic snapshot: `shadow_total` vs `engine_total` and
   `shadow_dense` localize loss/gain; `sealed_segments` tells you whether the
   cold tier was involved; `reopen(last/max)` flags a recovery-time
   regression; `rss`/`fd` flag a leak; the `fsync[…]` histogram flags device
   degradation.
4. **Post-mortem**: inspect the intact store dir with `mess` (the operational
   CLI — `doctor`, `inspect`, `verify`, `rebuild-index`) pointed at it.

The store dir is deliberately **not** cleaned up after an abort — that is the
"reproducible state bundle" the acceptance criteria call for. A green run does
clean up its scratch dir.
