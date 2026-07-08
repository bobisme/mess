# 06 — Subscriptions: catch-up → live handoff

Status: normative. Derives from decision record D11
(`notes/mess-research/12_convergence.md`), validated by the `sub_handoff`
spike (`spikes/sub_handoff/REPORT.md`, `spikes/sub_handoff/src/lib.rs`,
`spikes/sub_handoff/tests/props.rs`). This document is self-contained: an
implementer needs nothing else to build a conformant subscription runtime.

This is a Phase 4 concept (subscription runtime, per the roadmap in doc 12)
but is specced now, in Phase 0, because the protocol is a semantics
question, not an implementation detail: the invariant it depends on (write
ordering) has to be honored starting with the very first commit path
written in Phase 3.

## 1. Scope

A **subscription** delivers committed events from the log to a consumer, in
position order, starting just after a given cursor, continuing live as new
events are committed. This document specifies:

- the delivery guarantee a subscription MUST provide (§2),
- the two data sources a subscription is built from and their relative
  authority (§3),
- the obligation on the **writer** that the whole protocol depends on (§4),
- the subscriber-side state machine and protocol (§5–§7),
- overflow handling and its lossy-but-loud rule (§8),
- operational requirements: lag metric, buffer sizing (§9),
- interaction with cursor regression after a crash (§10),
- the conformance bar new implementations must clear (§11),
- alternatives considered and rejected, with rationale (§12).

Out of scope, owned by sibling docs:

- log commit atomicity, batch framing, and the durable watermark (D7)
  mechanics (also called the committed watermark) — `01-log-format.md`
  (D2) and `03-durability.md` (D7).
- crash recovery and `CursorRegressed` production — `02-recovery.md` and
  `03-durability.md` (D7).
- the single-writer process model that makes write-ordering free — D9,
  owned by `03-durability.md` / `04-registry.md` as applicable.
- wire encoding for an eventual out-of-process client protocol (D9's
  "server mode: clients speak a protocol"). No such protocol exists yet;
  this document specifies the in-process subscription contract only. See
  §13.

## 2. The guarantee

> A subscription created at cursor `c` MUST deliver exactly the committed
> positions `c+1, c+2, …` in ascending order, with no gaps and no
> duplicates, regardless of concurrent appends, consumer speed, or how many
> times the subscriber falls behind and recovers.

This is the only externally observable contract. Everything below is the
mechanism that makes it true, plus the operational consequences (overflow,
lag, cursor regression) that a caller must handle.

Positions are dense and monotone by construction: `01-log-format.md`
requires each batch's `first_global_pos` to equal the running expected
position, and `02-recovery.md`'s A1 (position contiguity) makes that check
normative and rejects any byte-valid batch that lands at the wrong
position — together these rule out both gaps and reordering. D10 (`doc 12
convergence`) restates this as settled design premise ("positions are
dense/monotone by construction") when it rejects a learned position→offset
index as unnecessary. (The architecture-thesis note I2 frames strictly-
increasing positions as a per-"writer-shard" property, anticipating a
future multi-writer design; under the current single-writer model, D9,
shard and log coincide, so this reduces to the same A1/D10 guarantee.) The
guarantee is stated over positions, not stream versions, because a
subscription is a log-wide construct, not a per-stream one.

## 3. Sources

A subscriber is fed by two sources with asymmetric authority:

| Source | Definition | Authority |
|---|---|---|
| **History** | Paged reads `read_from(cursor, limit)`, serving only positions `<=` the durable watermark (D7) (see `03-durability.md`). | **Authoritative.** Nothing unacknowledged is ever visible through it. |
| **Live feed** | A bounded, per-subscriber buffer fed by the committing writer as each position becomes committed. | **Optimization only.** Carries zero correctness weight. A subscription that never used the live feed at all — i.e., polled history exclusively — would still satisfy §2, merely with worse tail latency and higher read amplification. |

Because the live feed carries no correctness weight, every requirement in
this document that looks like it is about the live feed is really about
history remaining a complete, gap-free fallback at all times, and about
the subscriber never trusting the live feed further than it is entitled
to.

## 4. Writer obligation — invariant W1

This is the invariant everything else rests on. It binds the **writer**
(the commit path), not the subscriber:

> **W1.** For every committed position `p`: the durable watermark (D7) MUST
> be advanced to `>= p` **before** `p` is offered to any live buffer, and
> live-feed publish order MUST equal position order.

Both clauses are mandatory and independent — satisfying one does not imply
the other:

- Publishing before the watermark advance lets a subscriber observe a live
  position it cannot yet confirm via history, which breaks the "history is
  authoritative and complete up to the watermark" premise §5 depends on.
- Publishing out of position order (e.g., two commits racing past the
  write lock and completing their `live.send` calls out of sequence) lets a
  live subscriber observe a gap with **no** accompanying overflow signal —
  which the subscriber has no way to distinguish from data loss, because
  §7's protocol assumes any gap it can't explain via overflow is impossible
  (see the anomaly counter, §11).

Batch atomicity (D2, `01-log-format.md`) makes the batch, not the frame,
the unit of visibility: a batch's positions become visible together —
watermark advances to the batch's last position, *then* the batch's
positions are published to the live feed in order. A subscriber MUST NOT
observe a position from an in-flight, not-yet-committed batch.

The single-writer rule (D9) makes W1 free to satisfy in the current design
— one writer holding one lock across both the watermark advance and the
publish step trivially serializes them. **W1 is stated as an explicit MUST
regardless**, because it is the kind of invariant a refactor breaks
silently: moving the publish call outside the commit critical section (for
example, to get it off the hot path, or because the live-feed fan-out
moved to a background task) produces no compile error, no panic, and no
failing unit test — only an intermittent, load-dependent gap that a
downstream consumer sees as missing data. Any future multi-writer or
async-publish design MUST re-derive W1 by construction, not by inspection.

## 5. Subscriber state machine

```text
states:      CatchUp -> Switching -> Live
overflow:    {Switching, Live} --overflow--> CatchUp
```

| State | Meaning | Entered when |
|---|---|---|
| `CatchUp` | Paging through history from `last`. | Subscription start (`last := c`); or an overflow signal in `Switching`/`Live` (`last := last_delivered`). |
| `Switching` | History exhausted (as of the read that found it empty); draining the live buffer, has not yet delivered a live position since entering this state. | A `read_from(last, limit)` call in `CatchUp` returns an empty page. |
| `Live` | Delivering live positions in order; has delivered at least one live position since the last (re)entry into `Switching`. | The first live position `== last + 1` is delivered while in `Switching`. |

`Switching` and `Live` share one code path (§7); the distinction is purely
observational (has a live delivery happened since the last switch) and MAY
be collapsed in an implementation that has no use for it — nothing in this
document requires the states to be materially distinguishable, only that
the transition table in §7 is honored.

## 6. Protocol — normative steps

```text
subscribe(c):
    1. attach to the live feed FIRST (start := live buffer begins
       accumulating on the subscriber's behalf)
    2. THEN enter CatchUp with last := c

CatchUp:
    page := read_from(last, limit)
    if page is non-empty:
        deliver page in order; last := page's final position; repeat
    if page is empty:
        -> Switching

Switching / Live:
    on next live position p:
        p <= last     -> drop p                    (§7 overlap dedupe)
        p == last + 1 -> deliver p; last := p; (Switching -> Live)
        overflow       -> last unchanged; -> CatchUp (§8)
```

**SUB1 (subscribe-before-read order).** A subscriber MUST attach to the
live feed before issuing its first `read_from` call. This is what makes
gaplessness provable: everything published after attach is either received
on the live channel or covered by an explicit overflow signal (S1, below)
— nothing published after attach can fall in the gap between "history
already read" and "not yet subscribed", because there is no such gap.

**SUB2 (history-empty is the only switch trigger).** The `CatchUp ->
Switching` transition MUST be triggered only by an empty page from
`read_from`, not by a timeout, a position estimate, or any other heuristic.
An empty page is proof — not a guess — that the subscriber has reached the
durable watermark (D7) as of that read. This is also what makes the "no
flapping" property (§9) hold: the switch condition is self-verifying.

**SUB3 (deliver-in-order).** History pages MUST be delivered to the
consumer in position order and MUST be contiguous (`read_from` returning a
page with an internal gap is a violation of history's contract, not
something a subscriber is expected to handle).

### Correctness argument (S1)

This is the proof that SUB1 + W1 together give §2's guarantee; it is
restated here because it is short, and because an implementer who
understands *why* the protocol works is much less likely to break it under
refactoring pressure.

> **S1.** If a subscriber subscribes at time `T0` and later finishes
> catch-up at watermark `W_end` (its last, empty `read_from`), then every
> position `p > W_end` was published on the live feed **after** `T0`: by
> W1, `p`'s publish happens only once the watermark is already `>= p >
> W_end`, and the watermark at `T0` was `<= W_end` (`read_from` never
> serves positions beyond the watermark, and `W_end` is itself a watermark
> value observed at or after `T0`). Therefore `p` is in the subscriber's
> live buffer, delivered by `recv()` in position order, or the buffer
> overflowed and an explicit overflow signal was raised instead — never
> silently absent.

Combined with the overlap dedupe rule (§7), delivery is exactly `c+1, c+2,
…, final_watermark`, independent of how many times the subscriber
regresses from `Live`/`Switching` back to `CatchUp`.

## 7. Overlap dedupe — MUST be `<=`, not `==`

**SUB4 (dedupe comparator).** In `Switching`/`Live`, a received live
position `p` MUST be dropped whenever `p <= last`, not merely when `p ==
last`.

This is the second load-bearing detail in this document, alongside W1.
Getting the comparator wrong does not fail loudly — it fails as an
under-delivery that surfaces only once catch-up has run at least twice, so
it is very easy to write, test lightly, and ship.

> **Decision — why `<=` and not `==`.**
> **Rationale:** after an overflow-driven regression (§8), `CatchUp` can
> legitimately overshoot far past whatever positions are still queued in
> the live buffer from before the regression — the buffer isn't cleared on
> regression, and re-draining it is what the next `Switching` phase does.
> On the next switch, the live receiver yields arbitrarily stale
> positions, not just the single boundary position at `last + 1`. An
> `== last` comparator drops exactly one stale duplicate and then
> misclassifies every further stale position as an in-order gap (`p >
> last + 1` with no overflow signal), triggering the anomaly path (§11)
> or, worse in an implementation that doesn't have one, delivering out of
> order or throwing away real data.
> **Evidence:** the spike's randomized suite recorded 43,161 dedupe drops
> in one 4,000-scenario run alone (52,438 total across the full suite),
> many multi-position per switch after a regression — this is not a rare
> edge case, it is the normal shape of a regression's aftermath.
> **Rejected alternative:** `== last` only. Looks sufficient under a
> single dry-run switch; fails the moment a subscriber has regressed even
> once. No sources propose this as viable; it is recorded here because it
> is the natural first draft.

## 8. Overflow — lossy but loud

The live buffer is bounded (finite memory per subscriber). A slow
subscriber or a commit burst larger than the buffer's capacity will cause
the buffer to overflow.

**SUB5 (overflow is mandatory, silence is not).** Dropping a slow
subscriber's buffered live events on overflow is REQUIRED — the buffer is
bounded, something has to give. Dropping them **silently** — i.e., without
an explicit, distinguishable-from-normal-recv signal reaching the
subscriber's protocol logic — is FORBIDDEN. The channel primitive MUST
provide a distinct overflow signal (e.g. `RecvError::Lagged` in the spike's
`tokio::sync::broadcast`-based model) separate from both "value received"
and "channel closed".

**SUB6 (overflow regresses, does not fail the subscription).** On an
overflow signal, the subscriber MUST transition to `CatchUp` with
`last` unchanged (i.e. from the last position it actually delivered), and
MUST NOT terminate the subscription or surface an error to the consumer on
this path alone. History is authoritative and complete up to the current
watermark (§3); nothing is lost. Overflow is normal operation under load,
not a fault.

**SUB7 (overflow is observable, not an application error).** Implementations
MUST count/expose overflow events (a `lag_regressions`-style counter or
equivalent) for operators, and MUST NOT surface overflow to the consumer
as a delivery error — the consumer's delivered sequence is unaffected by
overflow; only the internal source of the next few deliveries (history vs.
live) changes.

This is the "lossy-but-loud" rule: lossy at the live-buffer layer (by
design, bounded memory demands it), loud at the observability layer
(operators can see it happening), invisible at the consumer-delivery layer
(the guarantee in §2 holds regardless).

### Anomaly path (defense in depth)

**SUB8.** A received live position `p` with `p > last + 1` that arrives
**without** a preceding overflow signal is impossible under W1 + SUB1 (this
is exactly what S1 proves). An implementation MUST nonetheless treat this
case defensively — regress to `CatchUp` from `last`, exactly as for a
genuine overflow — and MUST count it separately from ordinary overflow
regressions (the spike's suite calls this `anomaly_regressions`). This
counter is the tripwire for a W1 violation reaching production: it MUST be
0 across the conformance suite (§11) — a nonzero count there is a failing
build, full stop. In production telemetry, the same counter SHOULD be
monitored and alerted on rather than asserted to be exactly 0 by a MUST:
production is not a controlled property-test run, and this document has no
basis for mandating a specific telemetry/alerting regime on a deployment
it does not control. A nonzero count in production is nonetheless a strong
signal that a refactor broke W1 (most likely: publish moved outside the
commit critical section, or two writers began racing without D9's
exclusion) and warrants investigation.

## 9. Operational requirements

**No flapping (informative).** A subscriber persistently slower than the
writer settles into `CatchUp` and stays there — it does not oscillate
between `CatchUp` and `Switching`/`Live`. This falls directly out of SUB2:
while the subscriber is behind, `read_from` never returns an empty page,
so the `CatchUp -> Switching` transition never fires. Flapping would
require repeatedly reaching (and leaving) the caught-up state, which by
definition means the subscriber is not persistently behind. No additional
hysteresis or debouncing logic is needed or wanted.

**SUB9 (lag metric).** Implementations MUST expose `watermark - cursor`
as a per-subscription lag metric. The protocol tolerates unbounded lag
silently with respect to correctness (bounded memory, unbounded delivery
debt is a legal steady state — see §12's rejected alternatives for why
that is the right trade); an operator has no way to notice a permanently
behind subscriber without this metric.

> **Decision — live buffer capacity is not specified as a constant.**
> Sources establish that correctness is independent of capacity (the
> spike property-tests capacities from 2 to 64) and give qualitative
> sizing guidance — size the buffer at or above the typical commit-batch
> size, because a single burst larger than the buffer sends even a fast,
> attentive subscriber through a full history round-trip (observed
> directly in the spike's `writer_idle_during_switch` case: a 25-event
> burst against an 8-capacity buffer produced `live_delivered: 0` even
> though the subscriber was, on average, keeping up). No source gives a
> specific default number or formula, and commit-batch size itself is a
> `03-durability.md` (D7 group-commit) runtime characteristic, not a
> constant this document can cite. **Decision:** live buffer capacity is
> left as an implementation-level, per-subscription-runtime configuration
> parameter; this document only obligates that the sizing guidance above
> be documented for operators, not that a specific number be chosen.
> **Rejected:** hard-coding a capacity here would either be arbitrary or
> would silently couple this document to `03-durability.md`'s group-commit
> tuning, which is out of scope (§1).

## 10. Interaction with cursor regression (D7)

Under `Process` durability, and inside a `Group` durability window
(`03-durability.md`, D7), visibility can precede durability: a subscriber
may hold a cursor pointing past the log end that survives a crash.

**SUB10.** When `read_from(last, …)` (or a re-attach after a connection
loss) is called with `last > log_end`, the store MUST return the typed
error `CursorRegressed { cursor, log_end }` (D7; full production semantics
in `03-durability.md` / `02-recovery.md`). This MUST propagate to the
consumer and MUST NOT be silently absorbed by an automatic re-subscribe:
positions `log_end+1 ..= cursor` were delivered to the consumer but no
longer exist after recovery, and only the consumer's application logic
knows whether downstream effects of those deliveries need compensating.
After the consumer acknowledges `CursorRegressed`, the subscription
re-enters `CatchUp` from `log_end` under the ordinary protocol of §6.

This is a re-attach/recovery-time phenomenon only: within one process's
uptime, the bounded live buffer never spans a crash, so `CursorRegressed`
cannot arise mid-`Live`/`Switching` — it is only ever observed at
subscribe/re-subscribe time, when `last` is being validated against a
freshly recovered `log_end`.

## 11. Conformance bar

**SUB11.** An implementation of this protocol MUST be validated by a
property-test suite equivalent in kind to `spikes/sub_handoff/tests/props.rs`
before being considered conformant. At minimum such a suite MUST:

- generate randomized scenarios varying live-buffer capacity, append burst
  size and timing, number of concurrent subscribers, and subscription
  start cursor (including 0, mid-history, and exactly-at-watermark);
- check delivered sequences **element-for-element** against
  `cursor+1 ..= final_watermark` for every subscriber in every scenario;
- assert an anomaly counter (SUB8) is 0 across the entire run;
- include the naive "catch-up-then-subscribe" protocol (§12(a)) as an
  executable negative control, and assert that it demonstrably loses
  events under race — a suite that cannot fail this negative control is
  not exercising the race it claims to cover.

The reference numbers this bar is set against: the `sub_handoff` spike ran
**5,600 randomized scenarios** covering **10,617 subscriber delivery
sequences**, each checked position-for-position, with **0** anomaly
regressions across ~15,000 forced lag regressions and ~52,000 overlap
dedupe drops; its naive counterexample lost events in **291 of 300**
seeded races. A conformance suite for a real backend does not need to
match these exact counts, but MUST be of comparable structure (randomized,
adversarial, exhaustive-sequence-checked, with a negative control) — a
handful of hand-written example tests is not sufficient evidence for this
protocol, precisely because its failure mode (SUB4 done wrong, W1 violated
by a refactor) does not show up in small, sequential, single-subscriber
tests.

## 12. Alternatives considered and rejected

> **Decision — subscribe-first + overlap dedupe (the specified protocol),
> vs. three alternatives.**

**(a) Catch-up-until-empty, then subscribe.** Reverse the order of SUB1:
drain history to empty, *then* attach to the live feed. **Rejected.** Any
event committed in the window between the last empty history read and the
`subscribe()` call is in neither source: history has already been read as
exhausted, and the live feed had not yet started accumulating. The
subscriber either delivers with an undetectable gap (if it trusts the live
feed blindly) or hangs forever waiting for a position that will never
arrive (if it doesn't). Measured: 291/300 seeded races under a
yield-paced concurrent writer lost events. Kept as an executable
counterexample and as the negative control required by SUB11.

**(b) Loop catch-up-until-empty, then atomic switch.** Re-check history
after subscribing (converges toward the specified protocol — overlap
dedupe is still required, because positions can land in both the re-check
page and the live buffer) — but make the switch itself atomic by blocking
the writer while a subscriber attaches. **Rejected.** This is correct, but
couples subscriber attach/detach to the commit path: a stalled or slow
subscriber attach would stall commits. Readers MUST NOT be able to gate
the writer (this is a standing constraint of the log design generally, not
specific to subscriptions).

**(c) Watermark-only `watch` + pull (no event broadcast).** The writer
publishes only the current watermark on a coalescing `watch`-style
channel; subscribers stay permanently in pull mode — wake on watermark
change, page history. **Rejected as the primary mechanism**, though it is
the honest degenerate case the specified protocol falls back to under
sustained lag (a subscriber pinned in `CatchUp` is, functionally, exactly
this). Trivially gapless (one code path, no dedupe, no overflow states),
but every subscriber pays a history read for every commit even while fully
caught up — N live subscribers means N re-reads of the tail instead of one
shared in-memory fan-out — and tail latency becomes wake-plus-page-read
instead of direct channel delivery. A `watch`-style channel also
coalesces updates with no per-receiver queue, so it structurally cannot
provide the overflow signal SUB5 requires — it cannot distinguish "I was
slow and missed updates" from "nothing changed", which is exactly the
distinction the protocol's regression logic depends on. A broadcast-style
channel with a bounded per-receiver buffer and an explicit overflow error
was chosen specifically because it provides that signal.

## 13. Non-goals

- **Out-of-process client protocol.** D9 anticipates a server mode where
  "clients speak a protocol"; this document specifies the in-process
  subscriber contract (states, dedupe rule, overflow handling) that any
  such wire protocol would need to preserve, not the wire protocol itself.
  A future client-protocol spec MUST preserve W1, SUB1, and SUB4
  end-to-end — e.g. a network hop MUST NOT reorder live-feed deliveries,
  and the client-side reassembly MUST apply the same `<=` dedupe — but the
  framing/byte layout for that protocol is undesigned and out of scope
  here.
- **Retry/dedupe window for re-submitted commands.** Doc 12 lists an open,
  non-blocking item: "dedupe window: define extent (time vs. global-pos
  span) and retry-after-expiry behavior" for a crashed writer's retried
  batch (D7's write-side mirror, A6). This is a **different** dedupe rule
  from SUB4 — that one concerns a writer retrying an ambiguous append
  after a crash; SUB4 concerns a live subscriber's overlap with its own
  catch-up read. Owned by `02-recovery.md` / `03-durability.md`, not this
  document.
