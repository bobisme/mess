# flat_combined_append — Spike B (bn-28g): single-owner append/publish on v3

**Question** (research/05 §6, design.md §6, review 11 D1). Does collapsing
validation, position assignment, and publication into ONE owner — bounded
intent ring → single owner → shadow heads → completion slots, replacing the
per-stream `AppendGate`, the per-append `spawn_blocking`, the
`PublishSequencer` condvar, the `Book` mutex, and the post-commit Fjall
head write — remove enough composed overhead? This is the architecture's
first kill point: Process-mode composed < 85% of the bare log ⇒ stop or
narrow.

**Verdict up front: NARROW.** The flat-combined kernel **beats the current
`LogEngine` on every measured point** — Process-mode 1.2–2.5× (up to 3.5×
with matched producer topology), Group-mode at parity (97–107% of the
engine, PASSING review D1's ≥95% durable gate with identical fsync
amortization), p99 lower nearly everywhere — while deleting every one of
the five mechanisms it set out to delete and holding all correctness
gates (including a differential oracle against the real engine and
recovery via the standard scanner). But the **85%-of-bare Process gate
FAILS** after honest profiling effort: best variant, best cell = **77.9%**
(owner-owns-writer + OS-thread producers, 250 B × 100); the spec'd
tokio/`Backend`-shaped variant sits at 21–37% of bare. The measured reason
is not validation (owner CPU ≈ 10%, and 1024-stream round-robin ratios ==
single-stream-per-writer ratios): it is **thread-handoff hops and API
serialization**. Each producer→owner→producer round trip through the ring
adds ~8 µs over bare's direct producer→committer→producer path, and at the
envelope's 4-writer concurrency there is nothing to amortize it against.
One design-shaping result stands out: **the owner must BE the committer**
(design §6.3 as literally written — owner performs the write and the
barrier). Keeping the existing committer as a second thread behind the
owner makes the flat design strictly worse (−35% vs owner-direct).
**B1 (pipelined next-group validation) is a no-win** — ≈+10% in Process
mode but a 5–18% LOSS in durable mode at 64 writers and 0–5% loss at 4
writers — reconfirming perf_group_commit H2a; drop it and keep its
nondeterministic barrier cut out of the fault model.

Environment: AMD Ryzen 9 3900X (12C/24T, governor `performance`), Samsung
970 EVO Plus 2 TB `nvme0n1p3` ext4 (77% full), scheduler `none`, Linux
7.0.12-arch1-1, rustc 1.97.0, `--release` (lto=thin). Scratch:
`$HOME/.cache/mess-bench` (real ext4 — never tmpfs). Workspace base commit
`a3d40ff1`. All numbers below are **Measured** in one local session
(2026-07-11) unless marked Derived.

Run: `cargo test --release` (16 correctness tests), then
`./target/release/flat_combined_append matrix | shapes | b1probe`
(raw per-run rows: `flat_combined_results.csv`; full logs:
`matrix_run.log`, `shapes_run.log`, `b1probe_run.log`).

---

## 1. What was built

`FlatEngine` (src/lib.rs): async producers push an `Intent` into a bounded
MPSC ring — bounded by **bytes** (semaphore; producers await space; an
intent's reservation is released at its terminal state, so the bound
covers queued + in-flight memory) AND by **count** (channel capacity) —
and await a oneshot completion. ONE owner thread drains a group, validates
expected-version against shadow heads (`HashMap<u64, u64>`) in
deterministic dequeue order with a speculative group overlay (two
same-stream `Exact(v)` intents in one group: first wins, second
conflicts), writes accepted batches, applies head effects, publishes a
group watermark (single `AtomicU64`, Release), and completes waiters.
Conflicts / duplicates / empty batches complete WITHOUT writing. A dropped
caller future only drops its receiver: the owner runs every accepted
intent to a terminal state and ignores completion-send failures — a
committed append always publishes. v3 on-disk bytes and `mess-log` are
untouched.

Variants:

- **B0** — owner submits to the REAL `Committer` thread via the real
  `Appender` and awaits the group's acks inline.
- **B1** — owner validates/submits group N+1 against speculative state
  while group N's acks are pending (depth-2 pipeline; the submit happens on
  the first poll of each append future, before any await).
- **B0Direct** — the owner owns the `SegmentWriter` directly (research/05
  §6 scope: "one owner using the existing committer/**writer**"; design
  §6.3: "the owner performs one coalesced write and one durability
  barrier"): validate → real `BatchEncoder`/`SegmentWriter::append` per
  batch → ONE `sync()` per group under `Group`/`Os` → publish → complete.
  Identical v3 bytes; the committer's separate thread and its two wake
  hops are what disappear.

Bench-only diagnostics (src/main.rs) to decompose the gap: `b0o` (owned
records — no `&[RecordToAppend]` defensive copy), `b0th`/`b0dth`
(producers on dedicated OS threads — the exact producer topology `bare`
uses — plus owned records).

Two committer behaviors had to be REBUILT in the owner before the numbers
were honest (both measured, not assumed):

1. **Tail-first ack await** (B0/B1): the committer acks in position order,
   so the owner parks on the group's LAST ack and collects the earlier
   ones without parking — one owner wake per group instead of one per
   batch.
2. **D7 early-close for the intent ring** (all variants, barrier modes):
   without it the owner reproduced perf_group_commit H1's convoy-split
   **exactly** — 1,000 fsyncs for 2,000 batches (2 batches/barrier) vs
   bare's ~500 at 4×100 — because a momentary "ring empty" right after a
   group completes closes the window while producers are still waking.
   Fix: producers mark in-flight from `append_batch` entry until their
   send (guard-dropped on cancellation), and the owner's gather closes on
   in-flight-zero only once the group has reformed to the last group's
   width (`target`, the committer's own rule) AND a 200 µs grace has
   elapsed (producers build their next batch BEFORE re-entering
   `append_batch`, outside any in-flight signal). `max_delay` stays a
   strict cap. After the fix: ~500 barriers, ev/fsync matched with bare
   in every Group cell.

## 2. Correctness gates — all PASS

`cargo test --release`: 16/16 green, parametrized over B0, B1, and
B0Direct (tests/correctness.rs):

- same-stream concurrent `Exact(v)` races ×100: exactly one success, one
  `Conflict` carrying the winner's head;
- cross-stream (8 writers × 100 appends × 5 events): returned position
  ranges tile [0, 4000) exactly — dense, no gaps/overlap — and
  watermark == total committed events;
- dropped futures mid-flight (200 callers, every other one aborted, Group
  durability): no position gaps (standard scanner `next_pos` == shadow
  `next_global`), committed appends of dead callers fully visible, engine
  not wedged (sentinel append succeeds), recovered heads == shadow heads;
- differential oracle: 600 randomized ops (8 streams; correct / stale /
  future expected versions; empty batches) through `LogEngine` and
  `FlatEngine` sequentially — identical accept/conflict outcomes,
  identical `Appended` values, identical per-stream head sequences;
- reopen: after writes + conflicts + empties and crash-free shutdown, the
  standard `mess_log::scanner::recover_segment` reproduces exactly the
  shadow heads / `next_global`, and `accepted.len()` == accepted appends
  (conflicts and empties wrote NOTHING);
- byte-bounded ring: with a 4 KiB byte bound and a stalled owner, a third
  producer is demonstrably parked (select against a 30 ms sleep) and
  completes only after the owner drains — queue memory bounded.

Every flat bench run also asserts `position_mismatches == 0` (owner's
predicted positions vs the real committer/writer's) and
`watermark == total_events`.

## 3. Matched throughput matrix

3 reps per engine per cell, interleaved `ABCDEF FEDCBA ABCDEF`
(research/05 §15.1), 0.5 s settle between Process runs / 8 s between Group
runs, fresh store per run. 4 writers, one stream per writer,
driver-tracked exact versions. **medians** below (best-of-3 in the CSV;
best-vs-median spread ≤ ~10% everywhere).

### Process (no barrier) — median ev/s (% of bare median)

```text
cell            bare        log            b0             b1             b0d            b0dth
24B x 10     2,959,586   520,056 (18%)  744,206 (25%)  830,410 (28%) 1,237,663 (42%) 1,812,800 (61%)
24B x 100    9,521,221 1,956,646 (21%) 2,038,233 (21%) 2,393,333 (25%) 2,905,057 (31%) 6,846,221 (72%)
250B x 10    1,921,743   554,454 (29%)  709,047 (37%)  794,895 (41%) 1,162,362 (60%) 1,358,747 (71%)
250B x 100   4,638,734 1,480,965 (32%) 1,569,626 (34%) 1,811,280 (39%) 2,549,078 (55%) 3,608,087 (78%)
```

vs the CURRENT engine (log), Process medians: b0 104–143%, b1 122–160%,
**b0d 148–238%**, b0dth 244–349%.

### Group (durable, `Durability::group_default()`) — median ev/s (% of log median)

All engines converged to the same barrier count (~500 fsyncs per run;
ev/fsync ≈ 40 at batch 10, ≈ 400 at batch 100; mean fsync 2.4–2.9 ms all
runs — settled device throughout).

```text
cell            bare        log         b0            b1            b0d           b0dth
24B x 10        14,155     13,843   13,570 ( 98%) 14,696 (106%) 14,230 (103%) 14,491 (105%)
24B x 100      143,794    134,234  129,776 ( 97%) 127,488 ( 95%) 137,766 (103%) 140,185 (104%)
250B x 10       13,277     14,135   14,175 (100%) 14,265 (101%) 13,773 ( 97%) 13,380 ( 95%)
250B x 100     143,055    121,106  127,225 (105%) 116,221 ( 96%) 120,859 (100%) 129,287 (107%)
```

### p99 append latency — medians of per-rep p99 (µs)

```text
cell            log     b0     b1     b0d   b0dth
proc 24x10      195    107    106     61     33
proc 24x100     326    402*   270    204     72
proc 250x10     142     96     84     63     46
proc 250x100    497    365    394    229    132
group cells   4651-6189: all flat deltas inside barrier-tail variance
              (run-to-run p99 swings of the SAME engine exceed any
              cross-engine delta; barrier p99 ~2x mean fsync dominates)
```

`*` the one regression: b0 +23% at proc-24B×100. B0Direct is strictly
better than the current engine in every Process cell (−37% to −69% p99).

### Stream shape (Process, 250 B × 10) — median ev/s (% of bare)

```text
shape                 bare        log          b0           b0d          b0dth
perwriter (4w)     1,921,743  554,454 (29%) 709,047 (37%) 1,162,362 (60%) 1,358,747 (71%)
rr1024 (4w x 256)  1,905,218  476,601 (25%) 693,455 (36%) 1,125,812 (59%) 1,318,195 (69%)
hot1 (1 writer)      751,087  266,422 (35%) 365,949 (49%)   596,117 (79%)   704,805 (94%)
```

Validation contention shape is a NON-factor: 1024 distinct streams
round-robin ≡ per-writer streams for every flat variant (shadow-head
HashMap either way). The hot single-writer shape shows the floor: with no
cross-writer wake overlap to hide, b0dth reaches **94% of bare** — the
per-intent kernel overhead itself is that small; the matrix gap is
concurrency-topology cost, not kernel cost.

### B1 probe — 64 writers, Group mode (median ev/s)

```text
64w x 10:   b0 192,983   b1 182,165 (-6%)    b0d 197,656 (+2%)
64w x 100:  b0 793,711   b1 652,439 (-18%)   b0d 1,050,577 (+32%, 51 fsyncs vs 66)
```

B1 never wins where it was supposed to (durable, high concurrency) — the
committer/writer already overlaps gather with the in-flight barrier, so
pipelining only reorders owner work (H2a reconfirmed). B0Direct is the
best durable variant at high concurrency by a wide margin.

## 4. Where the composed overhead actually lives (profile)

perf (cycles, 64w Process run): **68.9% of all cycles are producer-side**
(tokio workers: record construction, allocation, scheduling, futexes);
owner thread ≈ 10% (top symbols: `EventInput` conversion, SipHash string
interning — memoized away — oneshot sends); committer thread ≈ 8%
(crc32c + `BatchEncoder::encode` + pwrite, i.e. the real work). The owner
is never CPU-saturated; the system is wake-latency bound.

Decomposition at 250 B × 100 Process (medians), each step isolating one
mechanism:

```text
log     1,480,965  current engine (gate+spawn_blocking+sequencer+book+fjall)
b0      1,569,626  flat kernel over the committer thread          (+6%)
b0d     2,549,078  owner owns the writer (no committer handoff)  (+62%)
b0dth   3,608,087  producers on OS threads (no tokio task wakes) (+42%)
bare    4,638,734  no ring, no oneshot, no name/records API      (+29%)
```

Derived: at 4×10 the flat round trip costs ~26.7 µs p50 vs bare's
18.8 µs — ~8 µs for ring send + owner wake + oneshot wake; bare pays two
thread wakes per append, flat pays four (two with B0Direct). At 4-writer
concurrency there is no queue depth to amortize this against; at 64
writers b0d closes to 1.05M durable ev/s with better amortization than
b0 — the design scales the right way, it just cannot reach a comparator
that does strictly less per append.

## 5. Gates

```text
gate                                              result
Process composed >= 85% of bare                   FAIL — best 77.9% (b0dth best-of, 250Bx100);
                                                  spec-shaped B0 (tokio, &[RecordToAppend]) 21-37%;
                                                  94% only on the 1-writer hot shape
Group composed >= ~95% of current engine Group    PASS — b0 97-105%, b0d 97-103% (medians);
                                                  identical fsync counts; B1 dropped (95-106% at 4w
                                                  but -6..-18% at 64w)
p99 regression vs current engine <= 10%           PASS for B0Direct (better everywhere in Process;
                                                  Group deltas inside barrier-tail variance);
                                                  B0 passes except one cell (proc 24x100, +23%)
zero per-append spawn_blocking / blocking-pool    PASS — no tokio::task::spawn_blocking anywhere in
                                                  FlatEngine (grep src/lib.rs); producers await a
                                                  oneshot, the owner is one std thread
zero post-commit condvar sequencing               PASS — publication is one AtomicU64 Release store;
                                                  ordering falls out of owner sequentiality
queue memory bounded                              PASS — byte-bounded ring demonstrably blocks
                                                  producers (test: byte_bounded_ring_blocks_producers)
```

## 6. Verdict: NARROW

Per the kill rule (<85% ⇒ stop or narrow), the honest answer is **narrow,
not stop**:

1. **The flat-combined kernel is a real win over the thing it replaces.**
   It deletes the AppendGate, per-append spawn_blocking, the
   PublishSequencer, the Book mutex, and the post-commit Fjall write —
   and is faster than the current engine on every cell of the matrix,
   with equal-or-better tails and a PASSING durable-mode gate. The safest
   high-value subset (research/08 §7, review S1) survives this kill point.
2. **Re-base the 85% target.** The current engine at matched 4-writer
   envelope workloads runs at 18–32% of bare — not the 63–84% review C4
   derived from the envelope's single-stream 5,000-event-batch rows. The
   85%-of-bare gate, at this concurrency, demands the composed engine cost
   less than two thread wakes per append — i.e. it prices the async API
   itself, not the orchestration the Asterism design proposes to remove.
   Recommend restating the Process gate against the measured matched
   baseline (e.g. "≥ 2× current engine Process at 4 writers AND ≥ 95%
   current engine Group" — b0d meets both today), or gating at a
   concurrency where a queue actually forms.
3. **Design change (adopt):** the single owner must OWN the
   writer/barrier (design §6.3 literally; B0Direct), not front a separate
   committer thread (−35%). The committer's gather/early-close/poison
   logic moves INTO the owner — this spike already reimplemented and
   validated the early-close (`target` + in-flight gate + grace) there.
4. **Design change (drop):** B1 barrier pipelining — no durable win, real
   durable losses, and it puts a nondeterministic barrier cut into the
   fault model for nothing (H2a, twice now).
5. **Next narrow spike (the actual open question):** the remaining gap to
   bare is producer-side — tokio task wakes (+42% when removed) and the
   ring/oneshot hop + `RecordToAppend` string API (+29% when removed). If
   bare-class composed Process throughput still matters after re-basing,
   prototype **producer combining** (the arriving producer becomes the
   combiner when the owner role is free — flat combining in the original
   sense) and/or an owned-record, interned-type append API, before
   freezing design §6.1's dedicated-owner-thread wording.

## 7. Friction / honesty

- The convoy-split cost a full debugging round: the naive owner gather
  split every 4-batch convoy across two barriers (2× the fsyncs) in
  exactly the way perf_group_commit's H1 note predicted for
  decentralized in-flight counting. The fix (target width + in-flight
  gate + 200 µs grace) is load-bearing for the Group gate PASS; a
  single-writer store pays ≤200 µs extra latency per barrier group
  (bounded by `max_delay`, and only in barrier modes).
- Group-mode p99 comparisons are barrier-tail noise on this device;
  don't read the group p99 table as engine signal.
- b0o/b0th/b0dth change the producer topology / API shape relative to
  the `Backend` trait — they are diagnostics that bound where the cost
  lives, not drop-in engine numbers. The honest spec-shaped number for
  "FlatEngine behind today's async Backend API" is the `b0`/`b0d` tokio
  column.
- Device stayed healthy through the matrix (mean fsync 2.4–2.9 ms; the
  drive is at 77% full vs perf_group_commit's 95%); two pre-matrix scout
  runs on a dirtied device showed 5–12 ms barriers and were discarded.
- The bare log's own numbers moved with payload/batch (1.9M–10.1M ev/s),
  so the gate ratio is workload-dependent by construction; all ratios
  here are within-cell, same-session, interleaved.
- Empty-batch semantics were matched to the current engine
  (validate-then-no-op, `last_global = len-1`) and covered by the oracle.
- `spin_intake_us`/`spin_gather_us` knobs measured ≈noise for tokio
  producers and +3–4% for thread producers; committed defaults (40/3)
  are the measured winners on this host, env-overridable
  (`FLAT_SPIN_INTAKE_US`/`FLAT_SPIN_GATHER_US`).

## Files

- `src/lib.rs` — FlatEngine (ring, owner, B0/B1/B0Direct, early-close)
- `src/main.rs` — bench harness (`matrix`/`shapes`/`b1probe`/`point`)
- `tests/correctness.rs` — the 16 correctness gates
- `flat_combined_results.csv` — every run (cell, engine, rep, ev/s, p50,
  p99, fsyncs, mean fsync)
- `matrix_run.log`, `shapes_run.log`, `b1probe_run.log` — full outputs
- `summarize.py` — CSV → per-cell best/median/ratio tables
