# 14 — Spike results, round 2

Recorded: 2026-07-07. Round 1 ([13_spike_results.md](./13_spike_results.md)) validated the
design's components in isolation. Round 2 attacked what remained: the composed end-to-end
thesis, write reordering (the A4 gap round 1 punted on), and the subscription catch-up→live
handoff (the design's least-examined correctness area). All numbers measured on this machine;
full details in each spike's `REPORT.md`.

## Scorecard

| Spike | Question | Verdict |
|---|---|---|
| `spikes/vertical_slice` | Does the composed custom log + pointer index beat payload-in-LSM end to end? | **Not yet, honestly** — wins global replay 2.1× and crash semantics, ties durable appends, loses buffered appends 3×, stream replay 1.7×, and disk 1.31×. Every loss maps to an already-scheduled component; two spec corrections (F1, F2) |
| `spikes/torn_write` | Do D2's acceptance rules survive block-write reordering? | **Yes, with the CRC mandatory** — 24,000 cases clean; CRC proven load-bearing; one new mandatory rule (A9) |
| `spikes/sub_handoff` | Is there a provably gapless catch-up→live protocol? | **Yes** — 5,600 scenarios, 10,617 sequences gapless; naive alternative loses events in 291/300 races; specced as D11 |

## 1. vertical_slice — the thesis test

Two engines behind one API, same seeded workload (1M events, 257 B JSON payloads, 10k streams
Zipf 1.1, 4 writers, batches of 10), results cross-verified by payload checksums. Engine A =
crash_log framing + fjall per-event pointer index + durability modes; Engine B = RocksDB shaped
exactly like current mess_db (payload duplicated across global/stream CFs).

| metric | Engine A (Meridian slice) | Engine B (RocksDB) | winner |
|---|---|---|---|
| buffered append | 175k ev/s | 532k ev/s | **B, 3.0×** |
| durable append (any fsync mode) | 3.7–4.2k ev/s, p50 ≈ 5.3 ms | same | tie (fdatasync dominates) |
| disk per event (uncompressed A) | 305.5 B | 232.6 B | **B, 1.31×** |
| global replay | 3.89M ev/s (1.06 GB/s flat scan) | 1.85M ev/s | **A, 2.1×** |
| stream replay (pointer-chase vs prefix scan) | 1.05M ev/s | 1.75M ev/s | **B, 1.7×** |
| recovery | clean 30 ms; SIGKILL mid-write: 1.8 s scan, torn batch truncated exactly, index self-repaired | n/a (RocksDB internal) | A's semantics confirmed |

Reading the losses — each maps to a scheduled component, now with evidence for its priority:

```text
buffered append loss -> F1: per-event fjall inserts are the bottleneck, not
  the log (log alone sustains ~224k ev/s). Active-index writes must be
  batched per commit. Spec-level correction, folded into D5/Phase 4.
disk loss           -> uncompressed JSON vs. RocksDB SST compression. The
  measured 3-6x sealed-block compression flips this to a ~2.5-3x A win.
  D6 upgraded from "v2 luxury" to load-bearing for the thesis.
stream replay loss  -> per-event pointer chasing (one pread per event) vs.
  LSM prefix locality. Sealed packed blocks (78-95x hot-replay win from
  round 1) plus segment-order pread coalescing (F7) close it. These are
  required for the thesis, not optional acceleration.
```

Group-commit curve (both engines, 4 writers): 1 ms → ~4k ev/s, 5 ms → ~2.7k, 25 ms → ~950.
**F2: fixed-delay group commit is strictly worse than sync-per-batch at low concurrency** —
the window must close early when all in-flight writers are already pending (`max_delay` is a
cap, not a target). Folded into D7.

Other friction recorded in the report (F3–F8): segment metadata needs a home (F4),
"scan only the last segment" is an invariant chain to state explicitly (F5), recovery-time
index verification wants a watermark (F6), and position assignment currently forces a global
mutex around the log append (F8 — a real design question for multi-writer throughput).

## 2. torn_write — write reordering (closes A4)

ALICE-style simulated block device under the crash_log format: un-fsynced sector writes persist
as an arbitrary random subset at crash (modeling reordering), torn sectors included; 512 B and
4 KiB sectors; zeroed, garbage, and stale-generation disk backgrounds. 24,000 randomized cases
plus deterministic adversarial cases.

```text
full validation (magic + length echo + batch-CRC echo + A1 contiguity):
  29,873 acked batches recovered intact and in order
  0 partial / corrupt / stale / reordered-hole batches accepted
  recovery idempotent in every case

differential (CRC check disabled, everything else kept):
  348 corrupt batches wrongly accepted across 341/24,000 cases (~1.4%)
  in 7.8% of cases the CRC was the ONLY rejecting check — header + marker
  sectors persisted while a frame sector did not; a hole confined to
  payload bytes is structurally invisible without the CRC
```

New rules folded into D2:

```text
A9 (MANDATORY): segment epoch/generation in BatchHeader required before any
    segment recycling — a stale prior-generation batch at a coincident
    first_global_pos with zero new sectors persisted passes every other
    check and resurrects deleted data. Demonstrated deterministically.
A10: recovery never resynchronizes past a hole (1,248 fully-valid "resync
    bait" batches sat beyond stop points across the runs).
A11: BatchHeader needs no single-sector alignment; safety comes from the
    length cap + CRC. Stated to preempt alignment "optimizations".
A12: no CRC-off recovery fast path, ever. Structural checks catch ~80% of
    holes by luck, not by design.
```

Probes: sector size (512 B vs 4 KiB) changed no outcome; header+marker-without-frames is the
killer pattern; stale-generation backgrounds are the nastiest (~50% more CRC-last-line cases).

## 3. sub_handoff — catch-up → live protocol (new D11)

Protocol: subscribe-first, dedupe-overlap. Attach to the bounded live feed *before* the first
history read; page history until an empty page; drain live dropping positions `<= last`; on
buffer overflow regress to catch-up from the last delivered position. History is authoritative;
the live feed carries zero correctness weight. The load-bearing writer invariant: watermark
advances *before* publish, and publish order equals position order.

```text
5,600 randomized scenarios (incl. 1,000 overflow storms at buffer capacity
  2-4 and 600 appends-racing-the-switch)
10,617 subscriber sequences verified element-for-element gapless and
  duplicate-free across 15k forced regressions and 16.5k handoffs
gap-without-overflow-signal tripwire fired 0 times
rejected alternative (catch-up-then-subscribe): lost events in 291/300
  seeded races; kept in the crate as an executable counterexample
```

Findings that shaped the spec (full list in the report):

```text
- live-buffer sizing tracks commit-batch size, not throughput: one burst
  bigger than the buffer sends even a fast subscriber through history
- flapping is structurally impossible: a persistently slow subscriber takes
  exactly one overflow and settles in stable CatchUp
- the overlap dedupe must be `p <= last`, not `==` — post-regression the
  receiver holds arbitrarily stale queued positions
- bounded memory but unbounded delivery debt: expose watermark - cursor as
  the lag metric
- CursorRegressed must propagate to consumers, never be absorbed by silent
  auto-resubscribe
```

Folded into the convergence doc as **D11 — Subscription handoff**.

## What changed in doc 12 as a result

```text
D2:  rules A9-A12 added (A9 mandatory segment epoch before recycling)
D5:  active-index writes batched per commit (F1); pointer-chase stream reads
     lose 1.7x until packed blocks + coalescing exist — those components are
     required for the thesis
D6:  upgraded to load-bearing: uncompressed log loses disk to RocksDB 1.31×;
     with sealed-block compression it flips to ~2.5-3× win
D7:  Group{max_delay} closes early when all in-flight writers are pending
D11: new section — subscription handoff protocol
Phase 4: index-write batching + subscription runtime added
```

## Standing verdict after both rounds

Seven spikes, zero thesis-killers. The composed v1 loses some columns today, but every loss is
addressed by a component the plan already schedules — and the spikes converted those from
"planned improvements" into "quantified requirements with acceptance numbers." The correctness
story (crash, reordering, handoff) is now backed by ~42,000 adversarial test cases across three
harnesses, all of which are reusable seeds for the Phase 3 harness. Remaining known-unspiked:
the codec/upcaster bake-off (Phase 1 decision), fold-certificate prototype, seal pipeline, and
recovery-at-scale timing.
