# 03 — Durability modes, group commit, and cursor semantics

Status: normative (Phase 0). Source of truth for design intent:
`notes/mess-research/12_convergence.md` D7 (amended by round-4 measurement),
with supporting evidence in `spikes/perf_group_commit/REPORT.md` and
`spikes/crash_log/REPORT.md`. Where this document and the research notes
disagree, this document governs implementation; the research notes remain
the record of *why*.

This document is self-contained: an implementer should not need to read the
research notes to build the committer. Byte-level batch framing
(`BatchHeader`, `EventSubframe`, `CommitMarker`) is owned by
[01-log-format.md](./01-log-format.md); this document adds no new on-disk
format — group commit changes only *when* and *how many* of those
already-specified batches land in one physical write. Recovery's
accept/reject scan is owned by [02-recovery.md](./02-recovery.md); this
document defines the contract the committer must uphold so that scan is
correct, and the client-visible consequences when the two disagree after a
crash. Subscription mechanics (catch-up, live feed, overflow) are owned by
[06-subscriptions.md](./06-subscriptions.md); this document defines the
watermark that mechanism reads.

## 0. Terminology

The key words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are to be
interpreted as described in RFC 2119.

```text
position        a dense, monotone, zero-based integer (global_pos) assigned
                 to each committed batch's first frame, per D2/A1; matches
                 `first_global_pos` in BatchHeader
batch           the atomic append unit defined in 01-log-format.md (D2):
                 BatchHeader, one or more EventSubframes, CommitMarker
committer       the single thread (per store, per D9) that gathers batches,
                 assigns positions, issues the coalesced write, and drives
                 the durability barrier
barrier         an fdatasync (or O_DSYNC write, §4) that makes the bytes
                 written since the previous barrier survive the class of
                 crash the active Durability mode promises
watermark       the single integer, the durable watermark (D7) — also
                 called "the committed watermark" in 06-subscriptions.md's
                 D11 protocol; this document uses "the durable watermark
                 (D7)" as the canonical name and "watermark" as shorthand
                 throughout — such that read_from and the live feed
                 (06-subscriptions.md) MUST serve/publish position p only
                 once the watermark >= p
log end         the watermark's value at any instant; after a crash, the
                 value recovery (02-recovery.md, D1) will re-establish
```

There is exactly one watermark — not an immediately-advancing "visible"
watermark and a separate, later-catching-up "confirmed" watermark that
the reader waits for. D7 collapses that two-state distinction on purpose
("marker durable = ack. One fewer state; keep it.") into the single
durable watermark (D7) defined above — the cost of the collapse is
that the watermark's *meaning* (how strong a guarantee "past the watermark"
carries) depends on the configured `Durability` mode. Sections 1–2 make that
meaning precise per mode; Section 6 explains why the collapse still produces
a real, typed hazard (`CursorRegressed`) rather than silent data loss.

## 1. The `Durability` enum

```rust
pub enum Durability {
    Process,
    Os,
    Group { max_delay: Duration, max_bytes: u64 },
}
```

Every mode shares the same commit authority (D1): a batch is part of
history if and only if `02-recovery.md`'s scan accepts it after a crash.
What differs per mode is **when the watermark is allowed to advance past a
batch's positions**, i.e. when `append()` MUST return success to its
caller, and how strong a promise that success carries.

### 1.1 `Process`

- **Ack given:** the moment the batch's covering `write(2)` returns (bytes
  accepted into the OS page cache). No barrier is issued for this mode.
- **Watermark:** advances immediately on ack — there is no separate,
  later "actually flushed" event this mode waits for.
- **Survives:** process crash, panic, or `kill -9` only, provided the OS
  keeps running and eventually performs ordinary writeback.
- **Loss window:** unbounded and OS-governed. Any batch whose pages have
  not reached the device when the OS crashes, the machine loses power, or
  the device is unplugged is lost, even though it was acked and even
  though readers may already have observed it and advanced a cursor past
  it (Section 6). Mess imposes no cap on this window; it is exactly the
  kernel's dirty-page writeback interval.
- **Use case:** local dev, tests, and workloads that treat the store as a
  fast in-process cache with best-effort persistence.

### 1.2 `Os`

- **Ack given:** after the barrier (`fdatasync`, or the covering batch's
  share of a group barrier issued at `Group`-mode granularity — see 1.3)
  covering this batch returns.
- **Watermark:** advances only after the barrier returns, position-ordered
  (Section 3).
- **Survives:** process crash and OS crash/power loss, to the extent the
  device and filesystem honor the flush/FUA contract (a trust boundary
  mess does not attempt to independently verify beyond the `O_DSYNC`
  micro-benchmark of §4.2.1).
- **Loss window:** none for acked data, modulo that trust boundary. A
  batch is never acked before its barrier returns, and `02-recovery.md`'s
  scan is defined to accept exactly what the last completed barrier made
  durable.
- Under `Os`, every `append()` is its own group of size one — see §2.4,
  "degrades to sync-per-batch at one writer."

### 1.3 `Group { max_delay, max_bytes }`

- **Ack given:** after the *covering group's* single barrier (§2) returns
  AND the position-ordered durable watermark has advanced past this
  batch's positions (§3). This is bit-for-bit the same barrier strength
  as `Os` — `Group` is `Os` amortized across concurrently-gathered
  batches, not a weaker mode.
- **Watermark:** advances once per group, after that group's one barrier,
  covering every batch gathered into it, in position order.
- **Survives:** identical to `Os`, for every batch that received an ack.
- **Loss window:**
  - For **acked** batches: none, identical to `Os`.
  - For batches still gathered in a group whose window has **not yet
    closed** when the process or OS crashes: never acked at all — this is
    not data loss, it is the ordinary "the call never returned" case, and
    the client's retry path (§7) applies.
  - Section 6 covers a narrower, MUST-bounded hazard specific to
    `Process` and to the open-window portion of `Group` that is about
    *visibility*, not about the strength of an ack already given.

### 1.4 Summary table

| Mode | Ack point | Watermark advance | Survives | Acked-data loss window |
|---|---|---|---|---|
| `Process` | `write()` returns | immediate, no barrier | process crash only | unbounded, OS writeback-governed |
| `Os` | barrier returns | after barrier | process + OS crash (device permitting) | none |
| `Group{d,b}` | covering group's barrier returns | after group barrier, position-ordered | process + OS crash (device permitting), identical to `Os` | none for acked data; unacked in-window data is a retry case, not loss |

## 2. Group commit: final design (round 4)

Round 4 (spike `perf_group_commit`, every point crash-verified against
`02-recovery.md`'s scan) settled the committer architecture after the
round-2 finding (`vertical_slice` F2) that a **fixed-delay** window is
strictly worse than sync-per-batch at low concurrency (1 ms ≈ 4k ev/s, a
tie; 5 ms ≈ 2.7k; 25 ms ≈ 950 ev/s). The design below is what replaces
fixed-delay group commit; it is the only group-commit design this
specification permits.

### 2.1 Architecture: single file, single committer thread

```text
1. gather:   writers hand the committer their already-encoded frames
             (BatchHeader + EventSubframes + CommitMarker bytes per
             01-log-format.md); the committer does not re-encode
2. assign:   the committer assigns positions to every gathered batch,
             centrally, in the order gathered — this is simultaneously
             the D3/D9 single-writer position authority
3. write:    ONE coalesced write syscall for the whole group
4. sync:     one barrier (fdatasync by default; O_DSYNC option, §4)
             covering the coalesced write
5. advance:  the position-ordered durable watermark advances to cover
             every batch in the group
6. ack:      every gathered writer is acked, in any order (their
             positions are already watermark-covered by step 5)
```

There is exactly one segment file and exactly one committer thread per
store (consistent with D9's single-writer-process rule); striping across
multiple files is rejected for v1 (§5).

**Decision.** The committer MUST NOT re-encode frames handed to it by
writers (H2/H5 in `perf_group_commit/REPORT.md` §4: an extra pipeline
stage and parallel-encode-then-pwrite were both measured and did not win
at realistic payload sizes — lock-held encode of a ~250 B–26 KB batch is
single-digit microseconds, nothing to parallelize). This is not merely a
performance nicety: re-encoding on the committer thread would require
either holding writer-owned buffers across the barrier (forbidden, §2.5)
or copying them, adding latency with no measured benefit. Revisit only if
typical payload sizes grow into the multi-KB-per-event range where
`perf_group_commit`'s own numbers project parallel encode might start
paying (REPORT §4, H5).

### 2.2 Window closing: early-close, volume cap, delay cap

The window that decides which gathered batches form one group closes on
the **first** of three conditions:

```text
early-close:  every writer currently in flight (entered append, not yet
              submitted to the committer) is already waiting on this
              window — no point waiting longer, there is nothing left to
              gather
max_bytes:    the group's accumulated coalesced-write size reaches
              max_bytes (also the memory bound while the device is
              degraded, §2.6)
max_delay:    a CAP on how long the window may stay open, not a target to
              pad out to
```

`max_delay` MUST be implemented as a cap, never as a fixed sleep. The
round-2 regression (fixed-delay windows getting monotonically worse as the
delay grows) is exactly what a fixed sleep reproduces; early-close is what
fixes it. With early-close, the design **degrades to sync-per-batch when
there is only one writer in flight** — confirmed equal, not merely
similar, in `perf_group_commit/REPORT.md` §4 (H1).

**Decision — default values.** The sources bound but do not pin exact
defaults: `perf_group_commit/REPORT.md` §9 recommends `max_delay` "~1 ms"
and `max_bytes` "~8-16 MiB." This spec fixes:

```text
max_delay: 1 ms
max_bytes: 8 MiB
```

Rationale: `max_bytes` at 8 MiB is roughly 5× the measured knee (§2.3,
~1.5 MB/group) — enough headroom that ordinary bursts amortize well past
the knee before the cap forces a close, while keeping the memory a single
stalled window can hold to a modest, predictable amount when the device
degrades (§2.6). Rejected: defaulting to 16 MiB — it doubles the memory
held per stalled window with no measured throughput benefit above the
knee (REPORT §3's curve is already past its linear region well below
16 MB). Both values MUST be configurable; operators tuning for a faster
or slower device SHOULD re-run the §4.2.1 micro-benchmark and adjust
`max_bytes` to their measured bandwidth-bound plateau.

### 2.3 Design rationale: the measured knee

`perf_group_commit`'s scaling matrix (writers × events-per-batch, settled
device, drift-controlled — see REPORT.md §3 for the full curve and §1's
methodology note on why naive sequential benchmarking on a nearly-full
consumer SSD is untrustworthy) is the empirical basis for the window
design:

```text
W×B (events/group)   durable ev/s     ack p50    events/fsync
4×1      (4)                896        4.5 ms            2.7
64×10    (640)          100,183        5.9 ms          335.4
4×100    (400)          121,170        2.8 ms          373.5
512×10  (5,120)         362,578       14.1 ms        2,066.3
64×100  (6,400)         684,552        7.8 ms        3,321.1  (peak)
512×100 (51,200)        567,120       88.3 ms       25,026    (bandwidth-bound)
```

Below ~5–6k events per group (**≈1.5 MB at the ~250 B measured payload
size**), throughput is linear in `W×B` at flat 3–6 ms p50 latency — the
device's ~250–340 fsync/s ceiling is not yet binding, and every extra
event in a group is nearly free. Past the knee, fsync/s falls as each
barrier carries proportionally more data, events-per-fsync rises to
compensate, and latency starts buying throughput instead of the group
being free. The knee is not a call to grow `max_bytes` without bound: the
true ceiling past the knee is **sustained device write+flush bandwidth
(~150–200 MB/s on the reference hardware)**, not fsync count — CPU never
saturates (encode+CRC of a small event is ~1 μs) and fsync/s stays ≥200/s
until groups get large. `max_bytes` (§2.2) exists precisely to keep
ordinary groups on the cheap, linear side of this knee while still
amortizing well past sync-per-batch.

### 2.4 The convoy-split race, and why centralized gathering fixes it

A **decentralized** early-close implementation — each writer independently
decrementing/incrementing an in-flight counter and closing the window
when it observes zero — has a race: a momentary zero right after a group
acks can split a single arriving convoy of writers across two separate
barriers instead of one. This was measured directly: a decentralized
design achieved only ~2,400 events/fsync at 512×10 where a centralized
committer achieved ~5,070 — roughly half the amortization, for free,
purely from where the gather point lives (`perf_group_commit/REPORT.md`
§4 H1, §6). A `max_delay` cap is still conformant with the decentralized
version (nothing violates the spec), but it leaves throughput on the
table.

**Rule:** the committer's gather point (§2.1 step 1) MUST be a single
point of coordination per store (one queue/channel all writers hand
frames to), not a distributed in-flight counter each writer independently
observes. This is what closes the convoy-split race by construction
rather than by tuning.

### 2.5 Never hold the append path across the barrier

**Rule (MUST):** no writer, and no committer-held lock that a writer must
acquire to gather into the *next* group, may be held across a barrier
call. Group N+1 MUST be free to accumulate gathered batches while group
N's barrier is in flight.

This property — not an explicit double-buffering stage — is what gives
group commit its pipelining for free: `perf_group_commit/REPORT.md` §4
(H2) measured that an explicit pipeline stage (a second thread dedicated
to running the barrier while the committer moves on to gathering group
N+1) produced **no additional win** over inline-barrier code, because a
design that already never holds the append path across the barrier gets
the overlap automatically. Implementers MUST preserve the property; they
MUST NOT add a dedicated pipelining stage on the theory that it will help
— it was measured not to.

### 2.6 Operational note: barrier latency is device state, not a constant

`fdatasync` on the reference hardware measured 3.3 ms p50 on a settled
device and **150+ ms p50** under sustained load on a 95%-full consumer
SSD (a ~50× degradation), recovering only after 3–4 minutes idle
(`perf_group_commit/REPORT.md` §1, §10). Two consequences this spec
requires:

- The runtime fsync-latency metric MUST be exposed (this is the doc 09
  operational requirement this spec inherits; it is not optional
  instrumentation). A store that cannot show its own p50/p99 barrier
  latency cannot be operated.
- This document owns only the fact established above: a barrier that is
  merely slow (a multi-hundred-millisecond stall on a degraded device,
  still `Ok` when it eventually returns) is a distinct device state from
  a barrier that fails with `EIO`. The fsync-EIO poisoning policy itself
  is D8's (`notes/mess-research/12_convergence.md` D8), not yet a
  normative spec document in this series, and this document does not
  restate, anticipate, or otherwise legislate D8's policy. It records
  one constraint **on** the future D8 spec: whatever poisoning policy D8
  adopts MUST NOT conflate the degraded-but-live stall described here
  with the `EIO` condition D8 governs — poisoning a store that is merely
  slow would be a D8-spec bug against this section's evidence, not a
  hazard this document itself resolves. `max_bytes` (§2.2, §1.3) is also
  the memory bound on how much unacked data a single stalled window can
  accumulate before it is forced to close — this bounds the blast radius
  of a degraded-device stall on any one group, independent of whatever
  D8 eventually specifies.

## 3. The position-ordered durable watermark

The committer advances the watermark **once per group** (§2.1 step 5),
to the highest position covered by that group's now-durable barrier, and
only after the barrier returns. Because positions are assigned centrally
and monotonically (§2.1 step 2), "advance to the group's highest
position" is equivalent to "advance to cover every position in the
group" — there are no gaps to reason about within a single group.

This watermark is the durable watermark (D7) that 06-subscriptions.md's
protocol (D11) reads: `read_from` MUST serve, and the
live feed MUST publish, position `p` only once the watermark has reached
`p`, and a batch's positions MUST become visible together (watermark
advance, then publish the batch's positions in order) — never
interleaved with, or ahead of, the watermark advance that covers them.
This document is the definition of *when* that watermark is permitted to
move; 06-subscriptions.md is the definition of what happens once it does.

## 4. Barrier choice: `fdatasync` default, `O_DSYNC` as a verified option

### 4.1 The two options

```text
write + fdatasync (default):
  the coalesced write lands in the page cache, then fdatasync flushes
  the device's dirty cache and returns once the device confirms

O_DSYNC coalesced write (opt-in):
  the write itself carries the durability requirement (FUA on devices
  that support it); no separate flush call is issued
```

### 4.2 Measured difference and the decision

`perf_group_commit/REPORT.md` §4 (H4) and §6 measured `O_DSYNC` winning
at the top end: 2.3–4.4× the throughput of `write+fdatasync` at high
concurrency (512×10: ~457–467k ev/s @ p50 9–10 ms vs. ~104–205k @
14–26 ms), with the tightest tails of any design tested. The mechanism:
a FUA write pays only for its own bytes; a FLUSH pays for the device's
*entire* dirty-cache state, which is exactly what degrades under
sustained load (§2.6). At the bandwidth-bound end (512×100) all barrier
types converge (~800–870k ev/s) because bytes-in-flight, not barrier
type, is what binds there.

**Decision.** `write+fdatasync` MUST remain the default. `O_DSYNC` is
offered as a **device-verified** option, not a default, for two reasons
stated in the source evidence: (1) FUA correctness is a per-device trust
question in the same class as trusting FLUSH — some ext4 configurations
silently fall back to flush-equivalent behavior, and the serial 1 MiB
`O_DSYNC` micro-benchmark showed a 96 ms p99 tail that did not reproduce
under real concurrent load, meaning small isolated probes are not
trustworthy evidence either way; (2) the correct choice is a property of
the *target device*, not a compile-time or universal default.

### 4.2.1 The at-open micro-benchmark

The source material specifies that `O_DSYNC` must be "gated on a startup
micro-benchmark of the actual store device" (REPORT.md §9.4) but does not
pin the exact protocol or threshold.

**Decision (protocol left open by sources).** At store `open()`, if
`O_DSYNC` is configured as available (not forced off), the store MUST:

1. issue a small number (SHOULD be ≥30) of representative-size probe
   writes (SHOULD match the configured `max_bytes` knee target, §2.2 —
   i.e. on the order of 1 MB, not 256 B, since the serial small-write
   micro-benchmark is known to be unrepresentative, §4.2) via
   `write+fdatasync`;
2. issue the same number of probes via `O_DSYNC`;
3. enable `O_DSYNC` for the life of the open store only if its measured
   p50 latency is no worse than `write+fdatasync`'s p50 over the same
   probes; otherwise fall back to `write+fdatasync` and MAY log the
   result for operator visibility.

This is a conservative, cheap, one-time gate (probes run once at open,
not on the hot path) that turns a qualitative "device-verified" source
requirement into a testable one. Rejected alternative: a fixed universal
threshold (e.g. "enable `O_DSYNC` if p50 < 5 ms") — rejected because the
evidence explicitly shows barrier latency is device-and-load dependent
(§2.6) and a fixed absolute threshold would misfire identically on a
device that is merely momentarily busy at open time.

## 5. Striping: rejected for v1

**Decision (source-mandated, recorded verbatim below): striping across
multiple segment files is REJECTED for v1.** This section exists to
prevent re-litigating it without new evidence.

### 5.1 What was measured

Striping was fully implemented, crash-proven (deterministic recovery
merge, SIGKILL-verified), and priced (`perf_group_commit/REPORT.md` §4
H3, §7). It delivers real parallel flush capacity — up to ~4× aggregate
fsync/s at 8 independent files (REPORT.md §2) — and still **lost to
single-file group commit at every interleaved measurement point** (0.15–
0.8× the throughput of the single-file design).

### 5.2 Why it loses despite real parallel capacity

The global-order ack rule striping requires (ack batch `b` only once the
*global* durable watermark passes it — `b`'s own stripe's barrier
returned AND every batch with an earlier global position, on any stripe,
is durable) couples every ack's latency to the **slowest** stripe's
current barrier. Stripes drift out of phase; the durable watermark
advances in stutters instead of smoothly; each stripe forms smaller,
more frequent windows on its own — precisely un-amortizing what group
commit exists to amortize. Parallel flush capacity is real; the
coordination tax the correctness rule imposes eats all of it.

### 5.3 What striping would additionally cost the spec, even setting the measured loss aside

```text
- A1's in-scan contiguity check (02-recovery.md) no longer exists per
  file; its strength moves into a k-way recovery merge (gap/overlap
  detection) — a second, subtler invariant to keep correct
- "the log" stops being one byte-ordered object: global order exists
  only after the merge, and every raw-segment consumer (replay, seal,
  06-subscriptions.md's D11 protocol, 05-fold-certificates.md's chain)
  must merge first
- segment roll, retention, and manifest bookkeeping multiply by the
  stripe count k
```

### 5.4 Revisit conditions

Striping MUST NOT be revisited on throughput curiosity alone. It is
appropriate to reopen only for a target device class with **both**:
(a) barrier latency ≥20 ms, and (b) real, verified parallel flush
capacity across independent files on that device (REPORT.md §9.1). The
combination matters: (a) alone means group commit on a single file
already amortizes the cost away (§2.3); (b) alone without (a) means
there is no barrier tax large enough for parallelism to be worth the
coordination cost documented in §5.2–5.3. The likely trigger, per the
source material, is a future multi-device tier, not a faster single
NVMe device.

## 6. `CursorRegressed`: visibility can precede durability

### 6.1 The rule

Under `Durability::Process`, and — bounded to the currently-open
window — under `Durability::Group`, the watermark can advance (§3) on a
weaker guarantee than "this will survive an OS crash": `Process`'s
watermark advances on a bare page-cache write with no barrier at all
(§1.1), and `Group`'s watermark, though barrier-backed once it advances,
does not advance for a batch until that batch's group closes — meaning
between position assignment and the group's barrier, that batch is
gathered but not yet acked or watermark-covered by §3's rule.

Because the watermark is the single gate `06-subscriptions.md` reads
(§3), and `Process`'s watermark is honestly weaker than "this position
will still exist after a crash," a subscriber MAY legitimately advance a
cursor to a position that a subsequent process or OS crash then erases
from the recovered log. This is the D7-required, API-visible consequence:

```rust
pub struct CursorRegressed {
    pub cursor: u64,    // the position the subscriber had advanced to
    pub log_end: u64,   // the post-recovery watermark: log ends here now
}
```

**MUST:** a store that surfaces this condition to a resubscribing client
MUST report it as `CursorRegressed`, not as a silent truncation, an
empty page, or a generic I/O error. **MUST NOT (06-subscriptions.md,
D11):** the subscription runtime MUST NOT silently absorb
`CursorRegressed` via automatic resubscribe-from-`log_end` without
surfacing it to the application — delivered-but-revoked history is an
application-level fact the caller must be able to act on (e.g. compensate
a side effect it already took on the strength of the now-erased event).
The precise resubscription state machine (`CatchUp`/`Switching`/`Live`,
overflow) is 06-subscriptions.md's to define; this document defines only
the condition and its non-negotiable visibility to the caller.

### 6.2 Decision: closing the interpretive gap in the `Group`-window clause

The source record states the rule as "under `Process` (and inside a
`Group` window), visibility precedes durability" without pinning down
exactly which internal signal a reader might observe early during an
open `Group` window. Left unresolved, this could be read either as (a) a
description of default `read_from`/live-feed behavior, or (b) a warning
bounding what a *non-default*, optimistic read path would be allowed to
do.

**This spec adopts reading (b) and forecloses reading (a).** The
justification does not rest on interpreting the ambiguous clause itself —
that would be circular — but on two constraints that are independently
settled *elsewhere* in the spec record and that reading (a) would have to
silently override:

1. `06-subscriptions.md`'s advance-then-publish rule (§3, D11) is not
   itself the thing in question here; it was settled by the `sub_handoff`
   spike (5,600 randomized scenarios, 10,617 subscriber sequences verified
   gapless and duplicate-free) and is stated as a mode-unconditional
   writer obligation: "for every committed position `p`: advance the
   committed watermark to >= `p` *before* offering `p` to any live
   buffer" — no `Durability`-mode carve-out appears anywhere in that
   protocol. Reading (a) requires inventing a `Group`-specific exception
   to an already-verified, unconditional rule that the source material
   never states; reading (b) requires inventing nothing — it is simply
   what the unconditional rule already implies once you ask what it means
   for a still-open `Group` window. Between a reading that needs an
   unstated exception and one that needs none, the latter is the correct
   default absent evidence for the exception, and no such evidence exists
   in `perf_group_commit/REPORT.md` or doc 12.
2. Reading (a) would also contradict this document's own characterization
   of `Group` in §1.3 — "bit-for-bit the same barrier strength as `Os`,"
   not a weaker mode — because a default `read_from` that serves
   in-window positions would give `Group` *weaker* read-visibility
   guarantees than `Os` (which never has an open, readable window at all,
   §1.2) despite both modes claiming identical ack strength. Nothing in
   the source material draws that distinction, and the measurement
   methodology explicitly rejects it: `perf_group_commit/REPORT.md` §1
   defines "durable ev/s" as strictly post-barrier throughout ("append
   returns strictly after the covering fdatasync ... returns — never
   weakened anywhere"), with no separate, weaker accounting for `Group`.

Given both, the `Group`-window clause in the source material is best read
as bounding the same *class* of hazard as `Process`, but **short-lived**
(capped by `max_delay`/`max_bytes`, §2.2) rather than **unbounded**
(`Process`, capped only by OS writeback timing) — a warning to
implementers who might build an optimistic diagnostic or internal read
path that peeks at committer-internal, pre-barrier state, not a license
for the standard API to do so.

**Rule (MUST):** `read_from` and the live feed (06-subscriptions.md)
MUST NOT serve or publish any position before its covering barrier
returns and the watermark (§3) advances past it. This holds for `Group`
mode unconditionally. It is `Process` mode alone — not an API leak under
`Group` — that produces `CursorRegressed`'s hazard, and it does so by
that mode's own definition (§1.1), not by any implementation accident.

Rejected alternative: specifying that `Group` mode carries its own,
narrower `CursorRegressed` window distinct from `Process`'s. Rejected
because `perf_group_commit/REPORT.md`'s own measurement methodology
defines "durable ev/s" as strictly post-barrier throughout (REPORT.md
§1: "append returns strictly after the covering fdatasync ... returns —
never weakened anywhere") — there is no measured or specified scenario in
which `Group` acks anything before its barrier. Treating it as having its
own weaker window would contradict the evidence used to justify `Group`
mode's existence in the first place.

## 7. A6: recovery may surface unacked batches (the write-side mirror)

### 7.1 The condition

`crash_log`'s spike (12,000 randomized crash cases; rule A6, referenced
by `docs/spec/formal-model-commit-recovery.md`) found that recovery can
legitimately surface batches that were fully written to disk but never
acknowledged to any client — the crash landed between the batch's marker
becoming durable and the ack reaching the caller (observed in 171 of
12,000 crash runs; a client-side connection drop before an ack arrives
produces the identical client-observable situation without any server
crash at all). `docs/spec/formal-model-commit-recovery.md`'s exhaustive
model (finding Z1) additionally establishes that an unacked batch
discarded once (as "resync bait" past a recovery stop, per A10) can
legally **resurface and be accepted on a later crash** if the writer
later rewrites the same slot with a same-shaped batch and a further crash
loses only that rewrite. Both are spec-legal outcomes of D1's commit
authority, not bugs — but, per A6, the spec must own them explicitly
rather than leaving them emergent.

This is the write-side mirror of `CursorRegressed` (§6): there,
readers can observe a position that a crash later erases; here, a
writer can fail to observe an ack for a position that a crash-then-scan
later accepts anyway. Both are consequences of the same fact — the
watermark/ack signal and the recovery scan's acceptance are two
different mechanisms (D1 vs. D7) that agree only when no crash lands
between them.

### 7.2 The typed, client-visible outcome

`append()` MUST distinguish a positive ack from every other outcome; it
MUST NOT expose a third, silent "probably worked" state:

```rust
pub enum AppendOutcome {
    /// The watermark (§3) has passed this batch's positions under the
    /// caller's configured `Durability` mode (§1). Unconditional.
    Acked { first_position: u64, last_position: u64 },

    /// The call did not observe an ack before returning (connection
    /// drop, process crash mid-flight, timeout). The batch MAY still
    /// have been fully written and MAY be accepted by a subsequent
    /// recovery scan (A6) even though no ack was ever delivered here.
    /// The caller MUST retry using the SAME dedupe key to resolve which
    /// happened.
    Indeterminate { dedupe_key: DedupeKey },
}
```

**MUST:** a client library MUST NOT treat the absence of a timely ack as
proof of failure. It is exactly as consistent with "the batch is now
permanently part of the log" (A6) as with "the batch never reached
disk." `Indeterminate` MUST carry enough information (`dedupe_key`) for
a retry to be resolved to exactly one of "already applied" or "now
applied," never a duplicate.

**Decision — the shape of `DedupeKey`.** Neither doc 12 (D7, A6) nor
`02-recovery.md` (§6, A6 — read at
`.maw/workspaces/bn-21c/docs/spec/02-recovery.md` for this review) defines
`DedupeKey`'s fields; doc 12's "Open items" section lists the dedupe
key/window's precise shape explicitly as **not blocking** Phase 0. This
document commits to the type-level contract (§7.2 above) but left the
struct itself opaque, which is not implementable. This spec fixes it:

```rust
pub struct DedupeKey {
    /// The target stream (01-log-format.md `BatchHeader.stream_id`,
    /// u64). Batches are single-stream (D-FMT-6 in 01-log-format.md).
    pub stream_id: u64,

    /// The expected pre-append stream version the caller supplied to
    /// this append() call (the same value that becomes this batch's
    /// `first_stream_version` if and only if the batch is accepted at
    /// the position the caller expected). Retrying the SAME logical
    /// call always resupplies the SAME value.
    pub expected_version: u64,

    /// A 128-bit fingerprint (a non-cryptographic collision-resistant
    /// hash, e.g. a 128-bit SipHash or xxHash variant; this document
    /// does not pin the algorithm, only its width and inputs) of
    /// exactly the fields the writer controls before handing the batch
    /// to the committer (§2.1 step 1): `stream_id`, `category_id`,
    /// `expected_version`, `frame_count`, and every `EventSubframe`'s
    /// bytes, in order. It excludes `first_global_pos`, `segment_epoch`,
    /// and `batch_crc` — the fields the committer fills in AFTER gather
    /// (§2.1 steps 2–3) — because a writer computes its dedupe key
    /// once, at encode time, before it can know whether this call will
    /// even reach the committer, let alone whether it will resolve to
    /// `Acked` or `Indeterminate`.
    pub fingerprint: u128,
}
```

**Rationale.** `(stream_id, expected_version)` is already a unique slot:
D3/D9 assign stream versions centrally and strictly sequentially, so at
most one accepted batch can ever occupy a given `(stream_id,
expected_version)` pair — the same invariant ordinary optimistic-
concurrency conflict detection already depends on, not new bookkeeping
invented for A6 (consistent with §7.4's non-requirement: this needs no
durable ack-provenance record, only the per-stream version index that
conflict detection requires regardless of A6). A retry resolves by
looking up what (if anything) occupies that slot and comparing
`fingerprint`: nothing there yet means not-yet-applied; a match means
this exact call already succeeded (`Acked`, idempotently, no duplicate
write); a mismatch means a genuine expected-version conflict — a
different batch, not this retry's — reported through the normal
conflict path, never confused with dedupe. This resolves within
`02-recovery.md`'s existing scan/index machinery and needs no new
on-disk field, honoring this document's own stated boundary that it
"adds no new on-disk format" (preamble, p.1).

**Rejected alternatives:**

- *A caller-supplied opaque idempotency token* (Stripe-style
  `Idempotency-Key`), persisted verbatim with the batch. Rejected for
  Phase 0: durably persisting an arbitrary client token requires a new
  `01-log-format.md` field, and this document does not own byte layout
  (preamble, p.1) — a future revision MAY revisit this if `(stream_id,
  expected_version)` proves too coarse (e.g. once conditional,
  non-version-gated appends exist).
- *A key derived from the assigned position* (`first_global_pos`, or a
  hash including it). Rejected: `Indeterminate` is returned precisely
  when the caller cannot know whether position assignment ever
  happened, so a key that requires already knowing the position cannot
  be handed back on the outcome that most needs one.
- *Fingerprinting the entire encoded batch*, including
  `first_global_pos`/`segment_epoch`/`batch_crc`. Rejected: those
  fields are filled in by the committer strictly after the writer has
  already handed the batch off (§2.1 steps 1–2); a writer must be able
  to compute its own dedupe key immediately at encode time, before it
  is even known whether the call will reach the committer at all.

This decision fixes the key's *shape* only. The dedupe *window's*
extent and expiry policy remain the open item doc 12 defers (§7.3).

### 7.3 What this document commits to, and what it defers

`DedupeKey`'s shape is now fixed by the Decision in §7.2. What remains an
**open item** in the source record is the dedupe *window* (time-bounded
vs. position-span-bounded, and the accepted-duplicate-is-documented-
behavior policy on window expiry) — doc 12 lists it explicitly as "not
blocking" Phase 0. This document commits to the *type-level contract*
above (an indeterminate outcome MUST exist and MUST carry a stable,
precisely-shaped dedupe key) so that Phase 1 client code and
02-recovery.md's scan-result reporting have a stable shape to target;
02-recovery.md (or a future revision of this document) owns the window's
exact sizing and expiry policy. Until that policy lands, implementations
MUST treat the dedupe window as unbounded (never expire a dedupe key) —
the safe default, since expiring a key before its policy is specified
would silently reintroduce the duplicate this section exists to prevent.

### 7.4 Non-requirement

Nothing in A6 requires the store to durably track, per batch, "was this
one ever acked." That bookkeeping does not exist on disk and MUST NOT be
invented to "fix" A6 — a batch accepted by `02-recovery.md`'s scan is,
by D1, simply committed; A6 is about the client's uncertainty during the
crash window, not about the store owing every batch a permanent
ack-provenance record.

## 8. Cross-references

```text
01-log-format.md          BatchHeader / EventSubframe / CommitMarker byte
                           layout that the committer writes unmodified,
                           coalesced (§2.1)
02-recovery.md             the scan whose acceptance is D1's commit
                           authority; what CursorRegressed's log_end and
                           A6's Indeterminate resolution ultimately mean
04-registry.md             D3/D9 single-writer position assignment this
                           document's committer performs (§2.1 step 2)
05-fold-certificates.md    chain-hash placement that must not assume a
                           batch's positions are durable before the
                           watermark says so
06-subscriptions.md        D11's CatchUp/Switching/Live protocol reading
                           the watermark this document defines (§3, §6)
```
