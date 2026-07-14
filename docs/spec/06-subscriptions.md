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

A **subscription** delivers committed application events from the log to a
consumer in canonical global-position order, beginning at a given scan cursor
and continuing live as new events are committed. This document specifies:

- the delivery guarantee a subscription MUST provide (§2),
- the two data sources a subscription is built from and their relative
  authority (§3),
- the obligation on the **writer** that the whole protocol depends on (§4),
- the subscriber-side state machine and protocol (§5–§7),
- overflow handling and its lossy-but-loud rule (§8),
- operational requirements: lag metric, buffer sizing (§9),
- interaction with cursor regression after a crash (§10),
- the conformance bar new implementations must clear (§11),
- alternatives considered, with acceptance/rejection rationale (§12).

Out of scope, owned by sibling docs:

- log commit atomicity, batch framing, and the durable watermark (D7)
  mechanics (also called the committed watermark) — `01-log-format.md`
  (D2) and `03-durability.md` (D7).
- crash recovery and `CursorRegressed` production — `02-recovery.md` and
  `03-durability.md` (D7).
- the single-writer process model that makes write-ordering free — D9,
  owned by `03-durability.md` / `04-registry.md` as applicable.
- registry encoding and the v3 rule that `$registry` frames consume canonical
  positions while application reads filter them — `04-registry.md` §4.3.
- wire encoding for an eventual out-of-process client protocol (D9's
  "server mode: clients speak a protocol"). No such protocol exists yet;
  this document specifies the in-process subscription contract only. See
  §13.

## 2. The guarantee

> A subscription created at scan cursor `c` MUST deliver exactly every
> committed, application-visible event whose canonical global position is
> `>= c`, in ascending position order and without duplicates, regardless of
> concurrent appends, consumer speed, or how many times the subscriber falls
> behind and recovers.

This is the only externally observable contract. Everything below is the
mechanism that makes it true, plus the operational consequences (overflow,
lag, cursor regression) that a caller must handle.

Canonical positions are dense and monotone over **all v3 event frames** by
construction: `01-log-format.md` requires each batch's `first_global_pos` to
equal the running expected position, and `02-recovery.md`'s A1 rejects any
byte-valid batch that lands at the wrong position. `$registry` frames are part
of that canonical sequence and consume positions like any other v3 event.

The application-visible subset is deliberately **not dense**. Global reads
and subscriptions filter `$registry` (`stream_id == 0`), so consecutive
delivered events can have non-consecutive positions, including gaps introduced
mid-log when a new stream or event type is registered. “Without gaps” in this
document means no eligible application event is omitted; it never means that
the delivered numeric values form an integer range.

The protocol cursor is the next canonical position to **scan**, not necessarily the
position of the next event that will be delivered. It may point at or advance
past a filtered registry event. Consumers MUST treat it as an opaque monotone
ordering/resume token: compare it and pass it back to resume, but do not use it
as an array index or subtract cursors to count application events. A persisted
processing checkpoint MUST NOT advance past a visible record the consumer has
not processed. Thus a page consumer may persist the page frontier only after
processing the whole page. An API such as `mess-store`'s `Subscription::next`
may prefetch a page internally, but its public `position()` accounts for the
first unread buffered record and remains a safe resume cursor after the single
returned record is processed. Cursor `0` begins at the canonical start of the
log.

## 3. Sources

A subscriber is fed by two sources with asymmetric authority:

| Source | Definition | Authority |
|---|---|---|
| **History** | Paged scans `read_from(cursor, limit) -> { records, frontier }`, serving only canonical positions below the durable watermark (D7). `records` contains application-visible events; the exclusive `frontier` reports how far the canonical scan progressed, including filtered positions. | **Authoritative.** Nothing unacknowledged is ever visible through it, and the frontier is the only safe cursor advance across a filtered-only range. |
| **Live feed** | A bounded, per-subscriber buffer fed by the committing writer as each canonical position becomes committed. It MUST include internal positions or equivalent frontier markers even when the corresponding record is filtered. | **Optimization only.** Carries zero correctness weight. A subscription that never used the live feed at all — i.e., waited for a watermark advance and scanned history — would still satisfy §2, merely with different tail-latency/read-amplification tradeoffs. |

Here “watermark” means the protocol boundary below which history is
authoritative and readable. In the normative direct-owner design it coincides
with D7's durable watermark. The current composed `LogEngine` has an earlier
direct-owner durable counter and a later published read watermark after its
index tiers catch up; `EventStore` uses the latter for this protocol. That
stricter implementation boundary must not be described or consumed as the
direct owner's durable watermark.

Because the live feed carries no correctness weight, every requirement in
this document that looks like it is about the live feed is really about
history remaining a complete fallback at all times, and about the subscriber
never trusting the live feed further than it is entitled to. A history page's
record list alone is insufficient: an empty list with an advanced frontier
means “only filtered positions were scanned,” not “caught up.”

## 4. Writer obligation — invariant W1

This is the invariant everything else rests on. It binds the **writer**
(the commit path), not the subscriber:

> **W1.** For every committed canonical position `p`, including a filtered
> `$registry` position: the exclusive durable watermark (D7) MUST be advanced
> past `p` (`watermark > p`)
> **before** `p` (or an equivalent frontier marker) is offered to any live
> buffer, and live-feed publish order MUST equal canonical position order.

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
watermark advances to one past the batch's last position, *then* the batch's
positions are published to the live feed in order. Filtering `$registry`
from application delivery happens after this ordering step and does not
renumber later events. A subscriber MUST NOT observe a position from an
in-flight, not-yet-committed batch.

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

The state machine below is for the direct-live-feed implementation. The
conforming watermark-notify + authoritative-pull variant (§12(c)) remains in
`CatchUp`, waits only when an empty scan makes no frontier progress, and then
repeats the scan; `Switching`, `Live`, overlap dedupe, and overflow are absent
because notifications carry no records.

```text
states:      CatchUp -> Switching -> Live
overflow:    {Switching, Live} --overflow--> CatchUp
```

| State | Meaning | Entered when |
|---|---|---|
| `CatchUp` | Paging through history from the next-to-scan `cursor`. | Subscription start (`cursor := c`); or an overflow signal in `Switching`/`Live` (`cursor` unchanged). |
| `Switching` | History is caught up (an empty scan made no frontier progress); draining the live buffer, has not yet processed a new live position since entering this state. | A `read_from(cursor, limit)` call in `CatchUp` returns no records **and** `frontier == cursor`. |
| `Live` | Processing live canonical positions in order; application delivery remains filtered. | The first live position `== cursor` is processed while in `Switching` (it advances `cursor` whether visible or filtered). |

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
    2. THEN enter CatchUp with cursor := c

CatchUp:
    page := read_from(cursor, limit)
    deliver page.records in order
    if page.frontier > cursor:
        cursor := page.frontier; repeat
    if page.records is empty and page.frontier == cursor:
        -> Switching

Switching / Live:
    on next live position p:
        p < cursor  -> drop p                      (§7 overlap dedupe)
        p == cursor -> cursor := p + 1;
                       deliver only if visible; (Switching -> Live)
        p > cursor  -> anomaly; -> CatchUp         (§8)
        overflow    -> cursor unchanged; -> CatchUp (§8)
```

**SUB1 (subscribe-before-read order).** A direct-live-feed subscriber MUST
attach to the live feed before issuing its first `read_from` call. This is what
makes completeness provable: everything published after attach is either received
on the live channel or covered by an explicit overflow signal (S1, below)
— nothing published after attach can fall in the gap between "history
already read" and "not yet subscribed", because there is no such gap.

**SUB2 (no-record/no-progress is the only switch trigger).** The `CatchUp ->
Switching` transition MUST be triggered only when `read_from` returns both an
empty `records` list and `frontier == cursor`, not by an empty record list
alone, a timeout, a position estimate, or any other heuristic. An empty list
with `frontier > cursor` proves only that the scan crossed filtered system
positions; it MUST advance the cursor and remain in `CatchUp`. Empty with no
frontier progress is proof — not a guess — that the subscriber has reached the
durable watermark (D7) as of that read.

**SUB3 (deliver-in-order).** History records MUST be delivered to the consumer
in strictly ascending canonical position order. Numeric gaps are legal and
expected when every intervening position is accounted for by the page's scan
frontier and filtering policy. A record position below the incoming cursor, a
non-increasing pair of returned record positions, or a record at/above the
exclusive frontier is a history-contract violation.

### Correctness argument (S1)

This is the proof that SUB1 + W1 together give §2's guarantee; it is
restated here because it is short, and because an implementer who
understands *why* the protocol works is much less likely to break it under
refactoring pressure.

> **S1.** If a subscriber subscribes at time `T0` and later finishes catch-up
> at exclusive frontier `W_end` (its empty, no-progress `read_from`), then
> every canonical position `p >= W_end` was published on the live feed after
> `T0`: by W1, `p`'s publish happens only once the watermark covers `p`, while
> the scan observed `W_end` at or after `T0`. Therefore `p` (including a
> filtered-position marker) is in the subscriber's live buffer in canonical
> order, or the buffer overflowed and raised an explicit signal—never silently
> absent. History remains authoritative for materializing visible records.

Combined with the overlap dedupe rule (§7), delivery is exactly the ordered
subset of application-visible records at canonical positions `>= c` and below
the final watermark, independent of how many times the subscriber regresses
from `Live`/`Switching` back to `CatchUp`.

## 7. Overlap dedupe — MUST cover every position below the cursor

**SUB4 (dedupe comparator).** In `Switching`/`Live`, a received live
position `p` MUST be dropped whenever `p < cursor`, not merely when `p ==
cursor - 1`.

This is the second load-bearing detail in this document, alongside W1.
Getting the comparator wrong does not fail loudly — it fails as an
under-delivery that surfaces only once catch-up has run at least twice, so
it is very easy to write, test lightly, and ship.

> **Decision — why `< cursor` and not `== cursor - 1`.**
> **Rationale:** after an overflow-driven regression (§8), `CatchUp` can
> legitimately scan far past whatever positions are still queued in the live
> buffer from before the regression. The buffer is not cleared on regression,
> and re-draining it is what the next `Switching` phase does. On the next
> switch, the live receiver yields arbitrarily stale positions, not just the
> one immediately before the cursor. An `== cursor - 1` comparator drops
> exactly one stale duplicate and then mishandles the rest. Filtered registry
> positions do not weaken this rule: they advance the same canonical scan
> cursor even though they emit no application record.
> **Evidence:** the spike's randomized suite recorded 43,161 dedupe drops
> in one 4,000-scenario run alone (52,438 total across the full suite),
> many multi-position per switch after a regression — this is not a rare
> edge case, it is the normal shape of a regression's aftermath.
> **Rejected alternative:** `== cursor - 1` only. Looks sufficient under a
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
`cursor` unchanged (the next canonical position not yet processed), and
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

**SUB8.** A received live position `p` with `p > cursor` that arrives
**without** a preceding overflow signal is impossible under W1 + SUB1 (this
is exactly what S1 proves). An implementation MUST nonetheless treat this
case defensively — regress to `CatchUp` from `cursor`, exactly as for a
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
while the subscriber is behind, `read_from` either returns visible records or
advances the scan frontier, so the `CatchUp -> Switching` transition never
fires. Flapping would
require repeatedly reaching (and leaving) the caught-up state, which by
definition means the subscriber is not persistently behind. No additional
hysteresis or debouncing logic is needed or wanted.

**SUB9 (lag metric).** Implementations MUST expose `watermark - cursor` as
the per-subscription **canonical-position lag**. Because that span can include
filtered `$registry` positions, it is an ordering-distance/scan-debt metric,
not a count of application events awaiting delivery. Implementations MAY also
expose a visible-event backlog estimate, but MUST label it separately. The
protocol tolerates unbounded lag silently with respect to correctness (bounded
memory, unbounded delivery debt is a legal steady state — see §12's rejected
alternatives for why that is the right trade); an operator has no way to
notice a permanently behind subscriber without this metric.

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

Under `Process` durability (`03-durability.md`, D7), visibility can precede
crash durability: a subscriber may hold a cursor pointing past the log end
that survives a crash. Standard `Group` reads expose positions only after the
covering barrier and therefore do not create a separate cursor-regression
window; an internal optimistic pre-barrier path would be non-conforming.

**SUB10.** When `read_from(cursor, …)` (or a re-attach after a connection
loss) is called with `cursor > log_end`, the store MUST return the typed
error `CursorRegressed { cursor, log_end }` (D7; full production semantics
in `03-durability.md` / `02-recovery.md`). This MUST propagate to the
consumer and MUST NOT be silently absorbed by an automatic re-subscribe:
canonical positions `[log_end, cursor)` were previously scanned but no longer
exist after recovery. Some may have produced application deliveries and some
may have been filtered system events; only the consumer's application logic
knows whether downstream effects of the delivered subset need compensating.
After the consumer acknowledges `CursorRegressed`, the subscription
re-enters `CatchUp` from `log_end` under the ordinary protocol of §6.

This is a re-attach/recovery-time phenomenon only: within one process's
uptime, the bounded live buffer never spans a crash, so `CursorRegressed`
cannot arise mid-`Live`/`Switching` — it is only ever observed at
subscribe/re-subscribe time, when `cursor` is being validated against a
freshly recovered `log_end`.

## 11. Conformance bar

**SUB11.** An implementation of this protocol MUST be validated by a
property-test suite equivalent in kind to `spikes/sub_handoff/tests/props.rs`
before being considered conformant. At minimum such a suite MUST:

- generate randomized scenarios varying live-buffer capacity, append burst
  size and timing, number of concurrent subscribers, and subscription
  start cursor (including 0, mid-history, and exactly-at-watermark);
- insert hidden/system positions at genesis, between visible events, and in
  filtered-only runs, including runs longer than one history page;
- check delivered sequences **element-for-element** against the ordered subset
  of application-visible records at positions from the start cursor through
  the final watermark—not against a dense integer range;
- assert every reported cursor/frontier is monotone, never skips an eligible
  record, and can advance across an empty filtered-only page;
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

## 12. Alternatives considered

> **Decision — subscribe-first + overlap dedupe (the specified protocol),
> with watermark-notify + authoritative pull as a conforming implementation
> option.**

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
change, then page history. **Accepted as an equivalent implementation when
history returns the explicit scan frontier required by §3.** Coalescing is safe
because the watermark is only a wake signal, never a delivery source: after
every wake the subscriber scans authoritative history until it makes no
frontier progress. It therefore needs neither live overlap dedupe nor an
overflow signal. This is the shape used by `mess-store`'s application-facing
subscription, and it handles filtered `$registry` runs naturally.

The tradeoff is performance, not correctness: every subscriber pays a history
read after a commit even while fully caught up, and tail latency includes wake
plus page read. A direct broadcast remains a conforming optimization when it
obeys W1 and SUB1–SUB8, including canonical-position markers for filtered
events. Implementations MAY choose either shape and MUST expose the same §2
delivery and opaque-cursor contract.

## 13. Non-goals

- **Out-of-process client protocol.** D9 anticipates a server mode where
  "clients speak a protocol"; this document specifies the in-process
  subscriber contract (states, dedupe rule, overflow handling) that any
  such wire protocol would need to preserve, not the wire protocol itself.
  A future client-protocol spec MUST preserve W1, SUB1, and SUB4
  end-to-end — e.g. a network hop MUST NOT reorder live-feed deliveries,
  and the client-side reassembly MUST apply the same `< cursor` dedupe — but the
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
