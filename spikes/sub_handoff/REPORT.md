# Spike: subscription catch-up -> live handoff (`sub_handoff`)

**Question.** A subscriber replaying history from a cursor must transition to
receiving live events with no gaps, no duplicates, in order, while a writer
keeps appending — including when the subscriber is too slow and falls off the
live buffer. Every event store gets this wrong at least once; this spike
designs the protocol and property-tests it to destruction.

**Verdict.** The *subscribe-first, dedupe-overlap* protocol survives 5,600
randomized scenarios (10,617 subscriber delivery sequences checked
position-for-position) plus targeted adversarial tests, with zero violations.
The obvious alternative ("catch up until history is empty, *then*
subscribe") loses events in **291 of 300** seeded races. The protocol is
ready to be specced as D11.

Model: `src/lib.rs`. Tests: `tests/props.rs`. Payloads are irrelevant to the
handoff problem, so an event *is* its global position; the log is an atomic
committed watermark plus a bounded `tokio::sync::broadcast` live feed and a
paged `read_from(pos, limit)` history API.

---

## 1. The protocol

```text
             subscribe to live feed FIRST (bounded buffer starts filling)
                                  |
                                  v
            +--------------> [ CatchUp ] ---- read_from(last, limit)
            |                     |             non-empty: deliver page,
            |                     |             advance last
            |          empty page (last == watermark
            |          as of that read)
            |                     v
            |               [ Switching ] ---- recv() from live buffer:
            |                     |              p <= last      -> drop (overlap dedupe)
            |                     |              p == last + 1  -> deliver, go Live
            |   Lagged            |
            +---------------------+
            |                     v
            |                 [ Live ] ------- recv(): p <= last -> drop
            |                     |                    p == last+1 -> deliver
            |   Lagged (live buffer overflowed while slow)
            +---------------------+
                 regress to CatchUp from last_delivered; nothing lost —
                 history has everything
```

1. **Subscribe to the live broadcast first.** The bounded per-receiver
   buffer starts accumulating on the subscriber's behalf before the first
   history read.
2. **CatchUp:** page through history with `read_from(last, limit)` until a
   page comes back empty (we have reached the committed watermark *as of
   that read*).
3. **Switching:** drain the live receiver. Positions `<= last` are the
   catch-up/live overlap — drop them by position. The first `p == last + 1`
   is delivered and the subscriber is Live.
4. **Live:** deliver in order. On `Lagged` (the bounded buffer overflowed
   because the consumer was slow), regress to CatchUp from
   `last_delivered`. History is authoritative; the live feed is only an
   optimization.

`Switching` and `Live` run the same code; the distinction is observability
(has a live event been delivered since the last switch).

## 2. The gapless invariant, precisely

Writer-side ordering (**W1**, the load-bearing line in `Log::append`):

> A position `p` is published on the live channel only **after** the
> committed watermark is `>= p`, and publish order equals position order
> (guaranteed by the single-writer rule / write lock, D9).

Subscriber-side consequence (**S1**):

> If the subscriber subscribes at time `T0` and finishes catch-up at
> watermark `W_end` (its last, empty `read_from`), then every position
> `p > W_end` was published **after** `T0`: by W1, `p` is published only
> when the watermark is already `>= p > W_end`, while the watermark at `T0`
> was `<= W_end`. Therefore `p` is in the subscriber's live stream — either
> returned by `recv()` in position order, or covered by an explicit
> `Lagged` signal. Never silently absent.

So every position is covered: `p <= W_end` by catch-up, `p > W_end` by the
live stream or by a `Lagged` that regresses us to catch-up (which re-covers
it from history). Duplicates — positions published after `T0` but
`<= W_end`, i.e. the overlap — are removed by the `p <= last` check.
Delivery is exactly `cursor+1, cursor+2, ..., final_watermark` across any
number of regressions.

Corollary tested to destruction: in Live/Switching, after dedupe, a received
`p > last + 1` **without** a preceding `Lagged` is impossible. The
implementation counts this as `anomaly_regressions` and every test asserts
it is 0 — it fired zero times across ~10,600 subscriber runs. If publish
ever escaped the write lock (out-of-order publish) or preceded the watermark
advance, this counter is the tripwire.

## 3. Alternatives considered

**(a) Catch up until empty, then subscribe — REJECTED, and executed as a
counterexample.** `naive::run_subscriber_gapped` implements it. The failure
mode: any event appended in the window between the last (empty) history read
and the `subscribe()` call is in neither source — the subscriber has already
decided history is drained, and the live feed never saw it. The subscriber
then either delivers with a gap (if it trusts the feed) or hangs forever
waiting for a position that will never arrive (if it doesn't). Under a
yield-paced racing writer, **291/300** seeded races lost events. The window
is small but structural; no amount of "re-check history once more" closes it
without becoming protocol (b).

**(b) Loop catch-up-until-empty, then atomic switch.** Re-checking history
after subscribing converges to the chosen protocol (you still need
overlap dedupe, because events can be both in the re-check page and the live
buffer). Making the switch genuinely atomic instead — blocking the writer
while the subscriber flips — is correct but couples subscriber attach/detach
to the append path; a stalled subscriber attach would stall commits.
Rejected: readers must never gate the writer.

**(c) watch + pull (no event broadcast at all).** Writer publishes only the
watermark on a `tokio::sync::watch`; subscribers are permanently in pull
mode: wake on watermark change, page history. Trivially gapless (one code
path, no dedupe, no lag states) and it is the honest fallback shape.
Rejected as the primary mechanism because every subscriber then pays a
storage/index read for every event even when fully caught up — N live
subscribers means N re-reads of the tail instead of one cheap in-memory
fan-out — and tail latency is wake + page-read instead of channel delivery.
Note the chosen protocol *degrades into* (c) under sustained load: a behind
subscriber lives in CatchUp, fed entirely from history. `broadcast` was
chosen over watch+pull precisely because it gives a per-receiver bounded
buffer with an **explicit** overflow signal (`RecvError::Lagged`), which is
the safe-regression trigger; watch coalesces updates and has no per-receiver
queue, so it cannot serve as an event channel at all.

## 4. Test results (real output)

`cargo test --release -- --nocapture` — full suite **13.6 s**, 8/8 passed:

```text
[randomized_4000]      scenarios=4000 subscribers=7993 delivered(history=1119772, live=57463)
                       dedupe_skips=43161 lag_regressions=11897 switches=13080 catchup_pages=167965
[overflow_storm_1000]  scenarios=1000 subscribers=2024 delivered(history=295282, live=436)
                       dedupe_skips=2049 lag_regressions=2027 switches=2061 catchup_pages=39322
[racing_switch_600]    scenarios=600  subscribers=600  delivered(history=70596, live=4998)
                       dedupe_skips=7228 lag_regressions=1264 switches=1397 catchup_pages=25658
[naive_protocol_drops_events] 291/300 seeded races lost events
[forever_slower]       final_wm=2336 stats=SubStats { history_delivered: 2336, live_delivered: 0,
                       dedupe_skips: 0, lag_regressions: 1, anomaly_regressions: 0, switches: 1,
                       catchup_pages: 147 }
[writer_idle_during_switch]   history=40 live=25 lag_regressions=0 switches=1
[start_exactly_at_watermark]  history=0  live=15 lag_regressions=0 switches=1
[start_on_empty_log]          history=10 live=0  lag_regressions=1 switches=1
test result: ok. 8 passed; 0 failed
```

Totals: **5,600 randomized scenarios** (scenario shape fully derived from a
seed: live-buffer capacity 2–64, append bursts 1–20 with random
yields/sleeps/pauses, 1–3 concurrent subscribers, cursors at 0 / mid-history
/ exactly-at-watermark, random consumer delays and sink capacities), plus
300 adversarial races against the naive protocol, plus 4 targeted
deterministic tests. **10,617 subscriber delivery sequences** checked
element-for-element against `cursor+1 ..= final_watermark`. 15,188 forced
lag regressions, 52,438 overlap dedupe drops, 16,538 switches — the
interesting paths ran, a lot. `anomaly_regressions == 0` everywhere.

## 5. Edge cases discovered (the valuable part)

1. **A burst larger than the live buffer means the subscriber never goes
   effectively live.** First version of `writer_idle_during_switch` appended
   25 events in a yield-paced burst against a capacity-8 buffer: the
   subscriber took one `Lagged`, regressed, and delivered 100% from history
   — `live_delivered: 0` — even though it was "keeping up" on average.
   Delivery stayed exactly correct; only the *path* surprised. Consequence
   for the real design: live-path delivery is opportunistic, an optimization
   with zero correctness weight, and the live buffer should be sized
   relative to expected commit-batch size if live-path latency matters.
   Subscriber wakeup latency, not average throughput, decides which path
   feeds you.
2. **Stable catch-up does not oscillate.** A subscriber permanently slower
   than the writer (writer 4x faster for 300 ms, buffer capacity 4) shows
   `switches: 1, lag_regressions: 1`: it attempts the switch once, gets
   `Lagged` on the first burst, and then *stays pinned in CatchUp* — because
   while it is behind, `read_from` never returns empty, so the switch
   condition never triggers. No CatchUp<->Live thrash, no livelock, memory
   bounded by construction (one page + the fixed broadcast buffer). The
   feared "flapping" failure mode is structurally absent: flapping requires
   repeatedly draining history, which means you are repeatedly caught up.
3. **Bounded memory, unbounded debt.** The same test initially let the
   writer sprint unthrottled and the subscriber needed longer than the 30 s
   timeout to drain millions of positions after the writer stopped. Not a
   protocol problem — memory stayed bounded — but the real system needs a
   *lag metric* (watermark − subscriber cursor) surfaced to operators,
   because the protocol will happily run behind forever.
4. **The dedupe must be `p <= last`, not `p == last`.** After a lag
   regression, catch-up can overshoot far past the positions still queued in
   the live receiver; on the next switch the receiver yields arbitrarily
   stale positions, not just the single boundary event. (43k+ dedupe drops
   in the randomized suite; up to buffer-capacity stale events per switch.)
5. **Gap-without-Lagged never occurred** (`anomaly_regressions == 0` across
   every run) — but only because `append` holds the write lock across
   *both* the watermark advance and the publish. Publishing outside the
   lock lets two appends publish out of order, which manifests to a live
   subscriber as a gap with no `Lagged`, which this protocol would
   misclassify. W1's "publish order == position order" clause is as
   load-bearing as "watermark before publish". In the real store this is
   free (single writer, D9), but the spec must say it.
6. **An idle writer parks the subscriber in Switching on an empty
   `recv()`.** Fine in steady state, but shutdown/cancellation needs an
   external wakeup (here the `fin` watch channel; in the real design,
   subscription cancellation) or the task hangs forever. Also: at-head
   subscription with an idle writer is the pure form of this — one empty
   catch-up page, then park; first-ever delivery is live
   (`start_exactly_at_watermark`: `history_delivered: 0, live_delivered:
   15`).

## 6. Proposed spec section

---

### D11 — Subscription handoff (catch-up -> live)

A subscription created at cursor `c` delivers exactly the committed
positions `c+1, c+2, ...` in order, with no gaps and no duplicates,
regardless of concurrent appends, consumer speed, or how many times the
subscriber falls behind.

**Sources.** A subscriber consumes from two sources: (1) *history* — paged
reads `read_from(cursor, limit)` that serve only positions `<= committed
watermark` (the D7 watermark; nothing unacknowledged is ever visible to a
subscription), and (2) the *live feed* — a bounded per-subscriber buffer fed
by the committing writer. History is authoritative; the live feed is an
optimization and carries no correctness weight.

**Writer obligation (the invariant everything rests on).** For every
committed position `p`: the committed watermark is advanced to `>= p`
*before* `p` is offered to any live buffer, and live-feed order equals
position order. Both halves are mandatory; the single-writer rule (D9) makes
the ordering free, but a future refactor that publishes outside the
commit critical section breaks gapless delivery undetectably. Positions in
a batch (D2) become visible together: watermark to batch end, then publish
the batch's positions in order.

**Protocol.**

```text
states: CatchUp -> Switching -> Live, with {Switching, Live} --overflow--> CatchUp

on subscribe(c):        attach to live feed FIRST, then enter CatchUp with last := c
CatchUp:                page read_from(last); deliver pages; empty page -> Switching
Switching/Live:         take next live event p:
                          p <= last  -> drop (catch-up/live overlap dedupe)
                          p == last+1 -> deliver (Switching becomes Live)
                          buffer overflowed -> back to CatchUp from last
```

Why it is gapless: attaching before the first history read means any
position beyond the final catch-up watermark was necessarily published
after attach (by the writer obligation), so it is in the live buffer or
covered by an explicit overflow signal. The overflow signal must be
*lossy-but-loud*: dropping events from a slow subscriber's buffer is
required (bounded memory), dropping them silently is forbidden.

**Slow subscribers.** A subscriber that is persistently slower than the
writer settles into CatchUp and is fed from history until it genuinely
drains it; it does not oscillate (the switch condition — an empty page — is
itself the proof of having caught up). Memory per subscriber is one page
plus the fixed live buffer. The store must expose `watermark - cursor` as
the subscription lag metric; the protocol tolerates unbounded lag silently.

**Overflow-regression counter, not error.** Live-buffer overflow is normal
operation (expected under bursts larger than the buffer), surfaced as a
counter, never as an error to the consumer.

**Interaction with D7 / `CursorRegressed`.** Under `Process` and `Group`
durability, visibility precedes durability, so after a crash the recovered
log end can be *behind* a subscriber's cursor. In the handoff protocol this
surfaces exactly at one point: `read_from(last, ...)` (or re-attach after a
connection loss) with `last > log_end` returns the typed error
`CursorRegressed { cursor, log_end }` (D7). This MUST propagate to the
consumer rather than being absorbed by an automatic re-subscribe: positions
`log_end+1 ..= cursor` were delivered but no longer exist, and only the
consumer knows whether its downstream effects must be compensated. After
the consumer acknowledges, the subscription re-enters CatchUp from
`log_end` under the same protocol. (In-process, the bounded live buffer
never spans a crash, so `CursorRegressed` cannot arise mid-Live; it is a
re-attach/recovery phenomenon.)

**Live buffer sizing.** Correctness is independent of capacity (property-
tested at capacity 2). Capacity only trades memory against how often bursty
writers push caught-up subscribers through a history round-trip; size it at
or above the typical commit-batch size.

---

## 7. Files

- `/home/bob/src/mess/spikes/sub_handoff/src/lib.rs` — log model, handoff
  subscriber (documented state machine + invariants), naive counterexample.
- `/home/bob/src/mess/spikes/sub_handoff/tests/props.rs` — seeded scenario
  generator, exact-sequence checker, 8 test suites.
- Repro: `cd spikes/sub_handoff && cargo test --release -- --nocapture`.
