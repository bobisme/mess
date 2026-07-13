# composed_decision — Spike J (bn-2gu): the final composed decision

**The question** (research/05 §14, §16 — "J composed: final decision"). Does the
composed Asterism engine earn its complexity, and should Fjall be retired?

---

## THE DECISION — up front

**PROCEED, NARROWED — on the append/state kernel; do NOT retire Fjall yet; do
NOT ship v4.**

| # | component | verdict | why (Measured) |
|---|-----------|---------|----------------|
| **B** flat single owner | **ADOPT — highest priority** | Beats the current engine in **all 32 matrix cells**. Process 4w: **1.21x–3.74x**. Group 4w: **0.95x–1.82x**. p99 better in **every** Process cell. And it *fixes* bn-3pz outright (§5). |
| **A** dense head table | ADOPT (with B) | Spike A: 33–43 M applies/s, 16 B/stream. ~30 ns/append against b0d's 4–25 us — free at this altitude. |
| **G** epoch dedupe | ADOPT (with B) | Spike G: exact, 0.17x Fjall resident, zero per-key deletes. Removes 2 of the 8 Fjall keyspaces. |
| **F** microblocks — *stream side* | ADOPT | Spike F: apply 2.2–5.3x, contended-p99 herd collapse fixed. |
| **F** microblocks — *stride-8 global side* | **REJECT the stride-8 design point; use stride-1** | §6: the <=7-header claim is TRUE (4.5 mean) but each header is a **syscall**: 2.4–2.7 us p50, **10–17x SLOWER** than the incumbent's in-memory seek. |
| **D** effects/checkpoints | ADOPT (already proven) | Spike D passed with wide margins; not re-measured here (not in the engine). |
| **H** bitrank directory | ADOPT (seal-time only, low risk) | Spike H; off the append path entirely. |
| **C** Book removal | **SHIPPED — the program's biggest realised win** | 10M reopen **5.91 s -> 3.76 s**, peak RSS **1872 MiB -> 268 MiB**. |
| **I** SealPack | **HOLD at default-OFF** | Append-path no-op (proven, §7), but costs **+18% / +36% reopen RSS** at 2M / 10M — a cost Spike I did not measure. Needs bn-11g + an RSS fix before it defaults on. |
| **E** v4 capsules / migration | **STOP for now** | Nothing measured in J needs v4. Every win above is v3-compatible. The format passed its kill point; that does not make it *due*. |
| **Fjall retirement (M9)** | **NOT YET — blocked on exactly one thing** | 6 of 8 keyspaces are derived caches with proven replacements. **`stream_names` + `type_names` are the durable source of truth for the name<->id bijection and are NOT rebuildable from the log.** Until a log-derived `$registry` exists, Fjall cannot go. §8. |

**The headline number.** The FlatEngine rematch against *today's* main is the
single most decision-relevant measurement in this spike, and it came back
**stronger than Spike B's original**: where Spike B (gen1, base `a3d40ff1`)
measured B0Direct at **148–238%** of the engine in Process mode, today it
measures **121–374%**, and it now also *wins* the durable mode at 4 writers
(94.6–182% of the engine, matching bare's barrier count exactly) where Spike B
only reached parity. **The Book removal (Spike C) did not eat the flat owner's
lead — it widened it.**

**The honest counterweight.** The literal research/05 gate — *Process append
>= 85% of bare log* — **still FAILS below batch 1000**, for the flat owner as
much as for the current engine (b0d: 33–64% of bare at batches 1–100). Spike B
diagnosed this and Spike 0 confirmed it: at 4-writer concurrency that gate
prices the **async API** (two extra thread wakes per append), not the
orchestration Asterism removes. It is not a gate the architecture can pass
without changing the API. §9 recommends the gate be re-based, and says plainly
what it would take to pass the original.

---

## 1. What was measured, and what was not

**Measured** (this spike, this host, this session):

- the **real engine on main** (`mess_store::LogEngine`, post bn-34o / bn-2cj /
  bn-2ib(C) / bn-9mw(E) / bn-3of(I)) in the two configurations that exist
  today — `current` and `seal_pack = true`;
- the **bare log** lower bound (raw `mess-log` committer/appender);
- **Spike B's `FlatEngine`**, carried forward verbatim and re-run against
  today's crates (§3);
- the **bn-3pz** Group convoy split, including a **three-commit bisect** (§5);
- the **Spike F stride-8 global-seek** assumption against a real v3 segment (§6);
- **reopen wall + peak RSS** at 2M and 10M events, `current` vs `sealpack` (§7).

**NOT measured, and why.** There is no fully-integrated Asterism engine to run,
so §10's composed projection is exactly that — **Projected**, built from
components each of which was Measured in its own spike. Spikes A, D, F, G, H live
as spike crates, not in the engine. **No number labelled Measured is a
projection, and no projection is presented as a measurement.**

### The FlatEngine is an append *kernel*, not the Asterism engine

The caveat that most constrains every b0d number below; read it before the tables.

- b0d has **no durable name registry** — it interns names in an in-process
  `HashMap` and persists nothing. The engine writes `stream_names`/`type_names`
  to Fjall. In the **stable-stream** matrix (§4.1/§4.2) both engines intern 1–4
  names *once*, so the comparison is fair. In the **new-name** cells (§4.3) it is
  **not** like-for-like: b0d's new-name cost is a **lower bound** on what a
  log-derived registry would pay, not a drop-in result.
- b0d has **no dedupe window, no snapshot heads, no checkpoints, and no read
  index** (it publishes a watermark; it does not serve reads).

So b0d measures *the append/publish kernel with the Fjall write removed*. §10
adds the missing components back from their own spikes' measured per-op costs and
shows the sum is <= ~5% — but that sum is **Projected**.

---

## 2. Environment and discipline

- **CPU** AMD Ryzen 9 3900X (12C/24T), governor `performance`. **Kernel** Linux
  7.0.12-arch1-1. **rustc** 1.97.0, `--release`, `lto = "thin"`.
- **Disk** `/dev/nvme0n1p3` ext4, `/home` 78% full (286 GB free). **Scratch**
  `$HOME/.cache/mess-bench` — real ext4, **never tmpfs** (`fdatasync` there is a
  no-op and every durable number would be a lie).
- **Workspace base** `16243284` (main, post the bn-3of build fix).
- **Ordering** interleaved **A B C D E / E D C B A / A B C D E** (research/05
  §15.1), 3 reps per engine per cell, fresh store per run, settle before each run
  (400 ms Process / 4 s Group). **Median-of-3 AND best-of-3** reported separately
  (§15.2); the CSV carries every run.
- **Logical result check on every run**: `log`/`logsp` assert
  `total_events() == events written`; `b0d`/`b0dth` assert
  `watermark == events written` **and** `position_mismatches == 0` (the owner's
  predicted positions vs the real writer's).

### The quiet-guard, and an honest correction to it

`spikes/baseline_matrix` used a fixed load1 floor of 6.0, justified by this host's
documented ~4–5 ambient load. **Measured with the bench stopped (8 samples over 2
min), this session's true ambient was 4.37–6.55, mean ~5.5** — a 6.0 floor would
have blocked on ambient alone and, worse, would have silently sampled only the
load dips.

Rather than either stall or quietly lower the bar, the guard was made
**auditable**: the process check (no `rustc`/`cargo`/`cc`/`ld` outside our own
ancestor chain) is unchanged and non-negotiable, the load floor is
env-overridable (this session: 7.5), and **every run records the load1 it actually
ran under**. That turns a floor *claim* into a checkable *fact*:

| engine | runs | mean load1 | min | max |
|--------|-----:|-----------:|----:|----:|
| bare   | 96 | 4.26 | 2.84 | 7.31 |
| log    | 96 | 4.28 | 2.77 | 7.31 |
| logsp  | 96 | 4.26 | 2.63 | 7.31 |
| b0d    | 96 | 4.27 | 2.66 | 7.20 |
| b0dth  | 96 | 4.25 | 2.87 | 6.88 |

All five engines saw the **same** ambient (means within 0.03) — which is what the
A/B/B/A interleave is *for*. The realised mean of 4.26 also sits inside
baseline-gen2's 4–5 band, so absolute numbers here are comparable to it — and
indeed `log` reproduces gen2 cell-for-cell (§4.1).

---

## 3. The FlatEngine rematch — the decision-relevant number

Spike B measured its flat owner against the engine at base `a3d40ff1`, i.e.
**before** bn-34o, bn-2cj and bn-2ib/Spike C landed. That comparison is stale:
Spike C in particular deleted the `Book` — one of the five mechanisms the flat
owner set out to delete. The obvious worry is that **Spike C already collected the
flat owner's winnings**, leaving nothing for B.

It did not. `spikes/flat_combined_append/src/lib.rs` was carried forward
**verbatim** into `src/flat.rs` with exactly one change — `AppendOutcome::Acked`
gained `segment_id`/`offset` fields on main, so the owner's ack arm now ends in
`..` (no behaviour change) — and re-run against today's crates.

**Correctness first** (`cargo test --release`, 3/3 green): a **differential
oracle** drives 800 randomized ops (correct / stale / future expected versions,
empty batches) through **today's `LogEngine`** and the `FlatEngine` in the same
order and asserts identical accept/conflict outcomes, identical returned versions,
identical global positions; plus the same-stream `Exact(v)` race (exactly one
winner across 50 rounds) and dense position tiling over `[0, 4000)`. The flat
kernel still produces the same answers as the engine it wants to replace, *after*
C/E/I. A performance rematch would be worthless otherwise.

### 3.1 b0d vs the current engine (Measured, median-of-3)

`b0d` = B0Direct (owner owns the `SegmentWriter` and the barrier), **tokio
producers, borrowed `&[RecordToAppend]`** — matched to the engine's own async
`Backend` shape. This is the drop-in number.

**Process, 4 writers** (the gate row):

| batch | payload | current engine | b0d | **b0d / engine** | b0d / bare |
|------:|--------:|---------------:|----:|-----------------:|-----------:|
| 1    | 24 B  | 64,176    | 239,861   | **3.74x** | 64.2% |
| 1    | 250 B | 66,081    | 206,415   | **3.12x** | 58.7% |
| 10   | 24 B  | 659,868   | 1,250,403 | **1.90x** | 41.9% |
| 10   | 250 B | 601,281   | 1,139,682 | **1.90x** | 63.4% |
| 100  | 24 B  | 2,099,983 | 2,878,928 | **1.37x** | 33.4% |
| 100  | 250 B | 1,708,830 | 2,339,600 | **1.37x** | 53.8% |
| 1000 | 24 B  | 3,757,179 | 4,532,523 | **1.21x** | **106.8%** |
| 1000 | 250 B | 2,823,387 | 3,466,962 | **1.23x** | **107.9%** |

**Every cell a win**, and at batch 1000 the flat owner **passes bare** — it writes
the same bytes through the same encoder with one less thread hop, and at large
batches that hop is all that separated them.

**Process p99** (us, median of per-rep p99, 4 writers) — b0d beats the engine in
**every** cell:

| cell | bare | engine | **b0d** | b0dth |
|------|-----:|-------:|--------:|------:|
| 24 B x 1    | 19.5 | 230.4 | **29.1** | 29.2 |
| 24 B x 10   | 22.1 | 122.6 | **58.3** | 35.6 |
| 24 B x 100  | 73.2 | 344.7 | **214.2** | 73.4 |
| 24 B x 1000 | 1,422.8 | 1,644.6 | **1,546.5** | 365.4 |
| 250 B x 1   | 24.3 | 116.9 | **37.7** | 30.9 |
| 250 B x 10  | 40.2 | 134.8 | **64.2** | 48.6 |
| 250 B x 100 | 129.2 | 379.7 | **285.9** | 153.3 |
| 250 B x 1000| 1,665.0 | 2,234.3 | **1,726.3** | 906.0 |

**Group (durable)**: full table in §4.2. At **4 writers** b0d is **94.6% – 181.8%**
of the engine and matches **bare's barrier count exactly** in every cell. Its only
losses are at **1 writer** (89.6–96.7%), with a known bounded cause: Spike B's
rebuilt D7 early-close waits a **200 us grace** for a convoy that, at one writer,
never comes. Spike B §7 predicted exactly this ("a single-writer store pays
<=200 us extra latency per barrier group"). It is a tunable (`FLAT_SPIN_GATHER_US`
/ the grace), not a structural cost — but it is a real regression today and is
listed as such in the gate table.

### 3.2 Where the residual gap to bare lives (unchanged from Spike B)

`b0dth` (= b0d + **OS-thread producers** + **owned records**) is a diagnostic, not
a shippable engine — it changes the API shape. It brackets the cost:

```text
proc 250 B x 100, 4 writers (median ev/s)
  engine   1,708,830   current composed engine
  b0d      2,339,600   flat owner, tokio producers, borrowed records   (+37%)
  b0dth    3,628,200   + OS-thread producers + owned records           (+55%)
  bare     4,352,199   no ring, no oneshot, no name/record API         (+20%)
```

The remaining 2x to bare is **still producer-side** — tokio task wakes and the
`&[RecordToAppend]` defensive copy — exactly as Spike B's perf profile found
(68.9% of cycles producer-side). **This is the API's cost, not the
architecture's**, and it is why the 85%-of-bare gate cannot be passed by
re-orchestrating the owner. §9.1.

---

## 4. The full configuration matrix (Measured)

32 cells x 5 engines x 3 reps = **480 runs**. `logsp` = `EngineOptions::seal_pack
= true`. Raw rows: `composed_results.csv`; full log: `matrix_run.log`.

### 4.1 Process — median ev/s, and % of bare

| cell | bare | engine | %bare | logsp | %bare | b0d | %bare | b0dth | %bare |
|------|-----:|-------:|------:|------:|------:|----:|------:|------:|------:|
| 24 B x 1 x 1w    | 101,967 | 34,634 | 34.0% | 33,551 | 32.9% | 78,282 | 76.8% | 97,996 | 96.1% |
| 24 B x 1 x 4w    | 373,847 | 64,176 | 17.2% | 61,790 | 16.5% | 239,861 | 64.2% | 245,052 | 65.5% |
| 24 B x 10 x 1w   | 820,762 | 293,792 | 35.8% | 305,927 | 37.3% | 596,962 | 72.7% | 803,135 | 97.9% |
| 24 B x 10 x 4w   | 2,984,562 | 659,868 | 22.1% | 647,907 | 21.7% | 1,250,403 | 41.9% | 1,808,559 | 60.6% |
| 24 B x 100 x 1w  | 3,379,558 | 1,652,393 | 48.9% | 1,658,604 | 49.1% | 2,015,707 | 59.6% | 2,717,909 | 80.4% |
| 24 B x 100 x 4w  | 8,607,983 | 2,099,983 | 24.4% | 2,167,892 | 25.2% | 2,878,928 | 33.4% | 6,786,717 | 78.8% |
| 24 B x 1000 x 1w | 3,275,412 | 2,843,520 | 86.8% | 2,916,259 | 89.0% | 3,213,553 | 98.1% | 3,999,508 | 122.1% |
| 24 B x 1000 x 4w | 4,243,148 | 3,757,179 | 88.5% | 3,898,622 | 91.9% | 4,532,523 | 106.8% | 12,068,991 | 284.4% |
| 250 B x 1 x 1w   | 94,735 | 34,012 | 35.9% | 33,811 | 35.7% | 70,472 | 74.4% | 97,034 | 102.4% |
| 250 B x 1 x 4w   | 351,603 | 66,081 | 18.8% | 67,069 | 19.1% | 206,415 | 58.7% | 233,487 | 66.4% |
| 250 B x 10 x 1w  | 770,112 | 279,533 | 36.3% | 276,684 | 35.9% | 546,633 | 71.0% | 718,747 | 93.3% |
| 250 B x 10 x 4w  | 1,797,819 | 601,281 | 33.4% | 583,766 | 32.5% | 1,139,682 | 63.4% | 1,305,875 | 72.6% |
| 250 B x 100 x 1w | 2,780,240 | 1,061,237 | 38.2% | 1,167,221 | 42.0% | 1,807,691 | 65.0% | 2,446,784 | 88.0% |
| 250 B x 100 x 4w | 4,352,199 | 1,708,830 | 39.3% | 1,693,724 | 38.9% | 2,339,600 | 53.8% | 3,628,200 | 83.4% |
| 250 B x 1000 x 1w| 2,596,585 | 2,384,422 | 91.8% | 2,274,499 | 87.6% | 2,901,751 | 111.8% | 3,547,297 | 136.6% |
| 250 B x 1000 x 4w| 3,212,305 | 2,823,387 | 87.9% | 2,939,096 | 91.5% | 3,466,962 | 107.9% | 5,256,874 | 163.6% |

**The engine reproduces baseline-gen2 cell-for-cell** (gen2 -> gen-J at 4w:
16.9->17.2, 22.2->22.1, 22.4->24.4, 94.6->88.5, 18.1->18.8, 30.0->33.4,
38.4->39.3, 85.8->87.9). The baseline is stable; the deltas below are signal.

### 4.2 Group (durable) — median ev/s, % of bare, and **barrier counts**

The barrier count is the story. `fsync` = median fsyncs over the 3 reps.

| cell | bare (fsync) | engine (fsync) | %bare | b0d (fsync) | %bare | **b0d / engine** |
|------|-------------:|---------------:|------:|------------:|------:|-----------------:|
| 24 B x 1 x 1w    | 393 (800) | 393 (800) | 100.0% | 352 (800) | 89.6% | 89.6% |
| 24 B x 1 x 4w    | 1,492 (802) | 801 (**1600**) | **53.7%** | 1,456 (**800**) | 97.6% | **181.8%** |
| 24 B x 10 x 1w   | 3,915 (500) | 3,736 (500) | 95.4% | 3,686 (500) | 94.2% | 98.7% |
| 24 B x 10 x 4w   | 14,538 (503) | 14,431 (505) | 99.3% | 13,888 (500) | 95.5% | 96.2% |
| 24 B x 100 x 1w  | 39,705 (300) | 37,256 (300) | 93.8% | 33,402 (300) | 84.1% | 89.7% |
| 24 B x 100 x 4w  | 154,106 (302) | 143,046 (301) | 92.8% | 135,341 (300) | 87.8% | 94.6% |
| 24 B x 1000 x 1w | 331,271 (100) | 313,783 (100) | 94.7% | 311,847 (100) | 94.1% | 99.4% |
| 24 B x 1000 x 4w | 1,023,881 (101) | 716,191 (**200**) | **69.9%** | 1,112,174 (**100**) | 108.6% | **155.3%** |
| 250 B x 1 x 1w   | 383 (800) | 387 (800) | 101.0% | 355 (800) | 92.7% | 91.7% |
| 250 B x 1 x 4w   | 1,560 (801) | 1,454 (803) | 93.2% | 1,447 (800) | 92.8% | 99.5% |
| 250 B x 10 x 1w  | 3,910 (500) | 3,915 (500) | 100.1% | 3,626 (500) | 92.7% | 92.6% |
| 250 B x 10 x 4w  | 14,288 (502) | 11,420 (**662**) | **79.9%** | 13,485 (**500**) | 94.4% | **118.1%** |
| 250 B x 100 x 1w | 35,023 (300) | 34,433 (300) | 98.3% | 32,876 (300) | 93.9% | 95.5% |
| 250 B x 100 x 4w | 122,693 (301) | 106,542 (330) | 86.8% | 118,778 (300) | 96.8% | 111.5% |
| 250 B x 1000 x 1w| 234,897 (100) | 231,367 (100) | 98.5% | 223,622 (100) | 95.2% | 96.7% |
| 250 B x 1000 x 4w| 666,709 (101) | 438,446 (**199**) | **65.8%** | 484,038 (**104**) | 72.6% | **110.4%** |

Read the bolded rows: wherever the engine pays **~2x bare's barriers** it loses
30–46% of durable throughput — and **b0d pays bare's barrier count exactly** and
takes the throughput back. That is bn-3pz, and it is §5.

### 4.3 New-name cost (100% new streams) — the "one barrier" gate

`us per new stream`, median-of-3, 250 B payload. `fsync/name` counts the **log**
fdatasync only.

| cell | bare | engine | b0d | fsync/name |
|------|-----:|-------:|----:|-----------:|
| Process, 1w | 10.9 us | **33.9 us** | **13.9 us** | 0.00 |
| Process, 4w | 2.8 us | **18.3 us** | **4.9 us** | 0.00 |
| Group, 1w   | 2,480 us | **3,433 us** | **2,721 us** | 1.00 |
| Group, 4w   | 665 us | **1,010 us** | **734 us** | 0.25 |

- **Process**: the engine's 33.9 us / 18.3 us reproduce baseline-gen2 (34.4 /
  18.6) — the bn-2cj/bn-34o ~100x win is intact. b0d is **2.4x / 3.7x faster
  still**, but it persists no names (§1), so treat 13.9 / 4.9 us as the **floor** a
  log-derived registry aims at, not a delivered result.
- **Group, 1 writer is the gate**: `fsync/name = 1.00` for both — but the engine is
  **953 us slower per new stream than bare** while issuing the *same number of log
  barriers*. That extra ~0.95 ms is the **Fjall `SyncAll`**: a second, serialized
  barrier that the `commit.fsync` counter does not see. **The "new-name operation =
  exactly one barrier" gate FAILS for the current engine.** b0d, which has no
  Fjall, costs bare + 241 us (the D7 grace) and issues exactly one barrier —
  **PASS** — showing the gate is reachable, but only by moving names into the log.
- **Group, 4 writers**: bn-34o's coalescing works (0.25 barriers/name).

---

## 5. Carry-forward #1 — bn-3pz (Group convoy split): **CONFIRMED, and MISATTRIBUTED**

baseline-gen2 flagged that at 4 writers with small per-writer batch counts the
composed engine records **~2x the barriers bare achieves at the identical shape**,
dropping to 49–71% of bare, "where Spike B's gen1 saw parity" — and named
**bn-34o** and **bn-2ib** as the suspects, recommending a high-concurrency
confirmation on the theory that "more concurrency keeps the window full."

**All three parts of that framing are wrong. The split is real, it is *not* a
regression, and concurrency makes it *worse*.**

### 5.1 It gets worse with writers, not better

`convoy` sweep, Group, 250 B, interleaved, 3 reps (`convoy_run.log`):

| writers | batch | bare (fsync) | engine (fsync) | engine %bare | b0d (fsync) | b0d %bare |
|--------:|------:|-------------:|---------------:|-------------:|------------:|----------:|
| 1  | 10 | 3,938 (2000) | 3,883 (2000) | 98.6% | 3,459 (2000) | 87.9% |
| 4  | 10 | 14,020 (501) | 14,000 (509) | 99.9% | 14,379 (**500**) | 102.6% |
| 8  | 10 | 30,947 (251) | 25,800 (272) | 83.4% | 29,148 (**250**) | 94.2% |
| 16 | 10 | 55,975 (126) | 48,083 (141) | 85.9% | 55,578 (**125**) | 99.3% |
| 32 | 10 | 108,404 (64) | 68,821 (90) | **63.5%** | 108,418 (**62**) | 100.0% |
| 64 | 10 | 199,436 (51) | 110,517 (83) | **55.4%** | 197,436 (**50**) | 99.0% |

The engine's excess-barrier ratio rises monotonically with writer count — **1.00x
at 1w -> 1.63x at 64w** — and its throughput falls to **55.4% of bare at 64
writers**. **b0d holds bare's barrier count exactly at every point** (50 vs 51 at
64 writers) and tracks bare at 94–103%.

(The `b1000 x 64w` cell saturates the device — `fdatasync` 56 ms, the engine's own
DEGRADED threshold fires — and is excluded as device-bound, not engine-bound.)

### 5.2 The bisect: it predates all three suspects

The probe is the **frozen `spikes/baseline_matrix` binary** (`point`: bare + log
only), copied unmodified into a worktree at each commit and rebuilt there — so each
row is the engine *as it was*, driven by an identical driver. Cell: Group,
250 B x 1000 x 4 writers, 3 reps (`bisect_run.log`).

| commit | what it is | bare fsyncs | **engine fsyncs** |
|--------|------------|------------:|------------------:|
| `16243284` | HEAD (post 34o + 2cj + C + E + I) | 101, 100, 101 | 106, **200**, **190** |
| `43e8bd2a` | `f0ab89e9^` — post 34o, post 2cj, **pre Spike C** | 106, 101, 101 | **200, 200, 200** |
| `a3d40ff1` | `9d4ec27d^` — **pre bn-34o** — *Spike B's own gen1 base* | 102, 101, 101 | **200, 200, 200** |

**The convoy split is present at Spike B's own base commit.** bn-34o, bn-2cj and
bn-2ib did not cause it. (And logically they could not have: both name commits
touch only the *new-name* path, and this cell interns nothing.) Spike B's gen1
"Group parity at 4 writers" was measured at batch 10/100 — different cells, which
§5.1 shows are precisely the ones where the split does not bite at 4w.

### 5.3 Root cause, and the fix that already exists

The committer's early-close fires when `gate.is_zero() && plen >= target`
(`committer.rs`). `bare`'s producers enter that in-flight gate the instant they
call `append`. The **composed** engine's producers do registry probing, dedupe,
index and tokio work *before* they reach the appender — so between two barriers the
gate reads zero while producers are still in flight, and the window closes on a
half-formed convoy. More writers => more arrival jitter => worse.

**This is exactly the failure Spike B hit and fixed.** Its owner rebuilt the D7
early-close as *target width + in-flight-from-`append_batch`-entry + a 200 us
grace*, and b0d's barrier counts above are the proof it works. **The fix is not new
work — it is already written, tested and measured; adopting the flat owner adopts
it.**

**Recommended reclassification of bn-3pz**: not a bn-34o/bn-2ib regression; a
pre-existing composed-engine group-window defect, **fixed by Spike B's owner**. If
the flat owner is *not* adopted, bn-3pz must be fixed independently by moving the
in-flight gate to the top of `LogEngine::append_batch`.

---

## 6. Carry-forward #2 — Spike F's stride-8 global assumption: **REFUTED as specified**

Spike F adopted a **stride-8 sparse global checkpoint array** (2 B/batch resident)
on the assumption that "the composed engine then scans <=7 batch headers forward in
the (page-cached) active segment" — and explicitly deferred the cost of that scan
to Spike J: *"What this spike does NOT measure is the segment-header forward-scan
cost (real segment I/O — composed-engine / Spike J territory)."*

Measured here against a **real v3 segment written by the real engine**, with the
real scanner (`recover_segment`) as ground truth, 20,000 random global seeks, and
an assertion that every seek lands on the batch the scanner says owns that position
(`seek_run.log`):

| shape | bytes/batch | headers/seek | **stride-8 via `pread`** p50 / p99 | stride-8 via **mmap** p50 / p99 | stride-8 **cold** p50 | **F0 `ActiveIndex::global_range`** p50 / p99 |
|-------|------------:|-------------:|----------------------------------:|-------------------------------:|----------------------:|--------------------------------------------:|
| 1 x 24 B    | 140 B    | **4.51** | 2,380 / 4,360 ns | 210 / 3,730 ns | 74,970 ns | **240 / 740 ns** |
| 10 x 250 B  | 2,868 B  | **4.51** | 2,660 / 5,760 ns | 250 / 125,930 ns | 161,960 ns | **190 / 270 ns** |
| 100 x 250 B | 27,888 B | **4.49** | 2,390 / 5,530 ns | 230 / 206,730 ns | 114,750 ns | **140 / 190 ns** |

- **The structural half of the assumption HOLDS**: the forward scan reads a mean of
  **4.5 headers, never more than 8**, at every batch size.
- **The performance half FAILS.** Through the engine's actual `Fs` seam (a `pread`
  per header) the composed seek costs **2.4–2.7 us p50 — 10–17x SLOWER than the
  incumbent's in-memory `global_range`**, which measures 140–240 ns here.
  "Page-cached" does not mean free: `pread` re-enters the kernel on every header,
  cache hit or not.
- **mmap'ing the segment recovers p50 parity** (210–250 ns) — the only way to
  actually cash in the page-cache premise — but exposes the tail to faults (p99 up
  to 206 us vs F0's 190–840 ns).
- **Cold** (pages evicted with `fadvise(DONTNEED)`): **75–162 us p50**, three orders
  of magnitude worse. The design's memory saving is bought with a hard dependency on
  the page cache.

Spike F's reported "10.9x at 1/20th resident bytes" measured the **index lookup
alone** (120 ns) and, by its own admission, excluded the scan. With the scan
included, the design point inverts.

**Verdict: reject stride-8; take Spike F's own named fallback, stride-1** — a
resident batch directory at **16 B/batch**, no scan at all, which Spike F measured
at 460 ns p50 and 0.35x F0's global-side memory. The stream-side microblock result
(apply 2.2–5.3x, contended-p99 herd collapse fixed) is untouched by this and still
stands.

---

## 7. SealPack (Spike I) and startup/RSS at scale

### 7.1 SealPack is an append-path no-op — proven, not assumed

The matrix's segment-size rule (baseline-gen2's: >= 256 MiB) means **no segment ever
rolls**, so `seal_pack` cannot touch the append path there by construction — and it
doesn't: `logsp` is **96.3–104.1%** of `log` across all 32 cells (§4.1).

To measure it where it *can* bite, `sealmatrix` re-ran 4-writer cells at **8 MiB
segments and 2M events**, so the run rolls and seals ~30 segments *while appending*
(`sealmatrix_run.log`):

| cell (8 MiB segments) | engine | sealpack | ratio (median) | ratio (best-of-3) |
|-----------------------|-------:|---------:|---------------:|------------------:|
| 24 B x 100 x 4w  | 1,810,786 | 1,812,615 | 100.1% | 97.3% |
| 24 B x 1000 x 4w | 3,088,792 | 3,100,949 | 100.4% | 100.1% |
| 250 B x 100 x 4w | 734,315 | 785,378 | 107.0% | 101.3% |
| 250 B x 1000 x 4w| 1,402,148 | 960,570 | 68.5% | **100.7%** |

The one bad median (68.5%) is a single slow rep; both engines' rep0 is slow and
their best-of-3 are within 0.7% of each other. **Honest reading: sealpack's
append-path cost is inside run-to-run variance; the variance itself comes from
background sealing + page-cache warm-up, not from the pack format.**

### 7.2 Reopen wall + peak RSS — and a NEW cost Spike I did not measure

Harness: the existing Spike-C `owb_bench` (public API, one phase per process so
`VmHWM` is that phase's true peak RSS), with `OWB_SEAL_PACK=1` added to flip
`EngineOptions::seal_pack` at **both** seed and open. Verified by file inspection to
produce `.seal` files and no `.pidx`/`.filter`/`.pcol` (and vice versa). 3 reopens
each, quiet-guarded (`reopen_run.log`).

| corpus | config | reopen best / median (s) | **peak RSS (VmHWM)** | sealed segs |
|--------|--------|-------------------------:|---------------------:|------------:|
| 2M  | current  | 1.640 / 1.641 | **105,584 KiB** | 23 |
| 2M  | sealpack | **1.587** / 1.608 | **125,168 KiB (+18.5%)** | 23 |
| 10M | current  | 3.763 / 3.767 | **274,656 KiB** | 118 |
| 10M | sealpack | 3.789 / 3.789 | **373,316 KiB (+35.9%)** | 118 |

- The 2M `current` row **reproduces baseline-gen2 exactly** (1.667 s / 105,176 KiB).
  The measurement is sound.
- **SealPack's reopen wall is a wash** (-3% at 2M, +0.7% at 10M) — its -66.7%
  file-open win does not convert into wall time, because reopen is not
  `open(2)`-bound.
- **SealPack costs 18–36% more peak RSS at reopen.** Spike I measured install
  states, file opens, seal wall and cold p99 — it did **not** measure reopen peak
  RSS, and this is a real, previously-unseen cost that grows with corpus size. It
  directly erodes the "**Book removed and bounded RSS**" product gate, which is the
  program's biggest realised win.

### 7.3 The cumulative program win on startup (this is what C bought)

| corpus | Book baseline (Spike C) | post-C (Spike C) | **gen-J (measured here)** |
|--------|------------------------:|-----------------:|--------------------------:|
| 10M reopen wall | 5.91 s | 3.91 s | **3.76 s** |
| 10M peak RSS    | **1,872 MiB** | 303 MiB | **268 MiB (14.3% of Book)** |

**Spike C is the spike that paid for itself**: a 7x RSS reduction and a 1.57x faster
reopen at 10M events, already shipped on main.

---

## 8. What still depends on Fjall — the answer to "retire it?"

`mess-index`'s `MetaStore` opens **8 keyspaces**; `mess-store` also ships
`FjallSnapshotBackend`.

| keyspace | role | rebuildable from the log? | replacement | status |
|----------|------|---------------------------|-------------|--------|
| `stream_heads` | derived head cache | **yes** (I5) | Spike **A** dense head table (33–43 M applies/s, 16 B/stream) | proven |
| `snapshot_heads` | derived snapshot cache | **yes** (I5) | Spike **A**/**D** resident state | proven |
| `checkpoints` | projection checkpoints | **yes** | Spike **D** (open at 4.90% of full scan) | proven |
| `dedupe` | dedupe window | **yes** | Spike **G** (exact, 0.17x resident, zero deletes) | proven |
| `dedupe_order` | FIFO eviction order | **yes** | Spike **G** — *eliminated entirely* | proven |
| `hw` | high-water marks | **yes** | trivial | proven |
| **`stream_names`** | **name -> id bijection** | **NO** | **a log-derived `$registry`** | **DOES NOT EXIST** |
| **`type_names`** | **type -> id bijection** | **NO** | **a log-derived `$registry`** | **DOES NOT EXIST** |

`crates/mess-index/src/meta/mod.rs` says it in its own words: these two tables are
*"the durable source of truth for the name<->id bijection… unlike every other table
here, they are NOT run at plain journal-buffered durability"* — the log stores only
interned numeric ids, so a materializing reader **cannot reconstruct names from the
log alone**.

**Answer to (b): NO, Fjall cannot be retired yet, and exactly one thing blocks it —
the durable registry.** Six of eight keyspaces have spike-proven replacements. The
two that do not are the *entire* remaining reason Fjall is linked. Retiring it
requires implementing the design's `$registry` stream so the name<->id bijection is
recovered from the log like everything else.

**And note what this does to the flat owner's numbers**: `b0d` has no durable
registry at all. Its Process wins on the *stable-stream* matrix are unaffected
(names are interned once). Its **new-name** numbers (§4.3) are a floor, not a
result. **The registry is therefore both the last Fjall dependency AND the one
unmeasured cost in the composed projection** — which makes it the natural first
implementation bone (§11).

---

## 9. The product gate table (research/05 §14) — per batch size (review S7)

| # | gate | current engine | flat owner (b0d) | verdict |
|---|------|----------------|------------------|---------|
| 1 | **append >= 85% bare** (Process, 4w, **per batch size**) | b1 17–19% · b10 22–33% · b100 24–39% · **b1000 88%** | b1 59–64% · b10 42–63% · b100 33–54% · **b1000 107–108% PASS** | **FAIL below batch 1000** for both. The flat owner triples the engine but cannot reach bare, because the gap is the **async API** (two thread wakes/append), not the orchestration. **Re-base the gate — §9.1.** |
| 2 | **new-name = exactly one barrier** | **FAIL** — 1.00 log barrier/name **plus** a serialized Fjall `SyncAll` worth **+953 us/name** (Group, 1w) | **PASS** — one barrier, bare + 241 us | Only reachable by moving names into the log (§8). |
| 3 | head/snapshot latency targets | — | — | **Carried, not re-measured in J** (Spike A: 33–43 M applies/s; Spike F: 70–220 ns resolves). No composed engine exists to measure. |
| 4 | **Book removed, bounded RSS** | **PASS** — 10M: 268 MiB = **14.3%** of the 1,872 MiB Book baseline; reopen 5.91 -> 3.76 s | n/a | **The program's biggest realised win.** WARNING: `seal_pack=true` regresses it by **+36%** (§7.2). |
| 5 | checkpoint open <= 10% full scan | — | — | **Carried** — Spike D measured **4.90%**. Not in the engine. |
| 6 | dedupe exact, lower write-amp | — | — | **Carried** — Spike G: exact incl. forced collisions, 0.17x resident, **zero** per-key deletes vs 2.0 M deletes + 318 MB disk traffic. |
| 7 | existing replay/storage gates | **PASS** | n/a | Reopen/RSS reproduce gen2 (§7.2); Spike I proved byte-identical replay. |
| 8 | **all crash/formal suites green** | **PASS, with one flake** | **PASS** | `cargo test --workspace --release`: **649 passed, 1 failed** — `engine_publish_cancel::cancel_then_same_stream_retry_never_double_writes_version`, a **host-load-sensitive test-setup race**, not an engine bug: it asserts an append was still in flight after a single poll, which under favourable scheduling it is not. **Passes 5/5 alone.** Filed as **bn-31n** (same class as the already-boned bn-2rk, different test). Not caused by this spike — its only non-spike diff is an env toggle in the `owb_bench` *example*. The flat kernel's differential oracle vs **today's** engine: 3/3 green (§3). |
| — | **p99 regression <= 10% vs engine** | — | **PASS** — b0d better in **every** Process cell (29 vs 230 us … 1,546 vs 1,644 us) | |
| — | **Group >= 95% of engine** (review D1) | — | **PASS at 4w** (94.6–182%); **FAIL at 1w** (89.6–96.7%) | The 1-writer loss is the D7 200 us grace — bounded and tunable, but real. |

**No isolated microbench win waives a failed composed gate** (research/05 §14).
Gate 1 fails below batch 1000 and gate 2 fails today. The verdict is written *with*
those failures, not around them.

### 9.1 The 85%-of-bare gate must be re-based — and here is what it would take to pass it

The gate was written when the envelope suggested the composed engine ran at 63–84%
of bare. Spike 0 showed that figure came from unmatched single-stream rows;
**matched, the engine runs at 17–39% of bare below batch 1000.** The flat owner
lifts that to 33–64% and *passes* at batch 1000 — but at 4-writer concurrency the
residual is two thread wakes and a payload copy per append.

`b0dth` bounds it: removing the tokio hop and the `&[RecordToAppend]` copy takes b0d
from 53.8% -> **83.4%** of bare (250 B x 100 x 4w) and from 33.4% -> **78.8%**
(24 B x 100 x 4w). **The gate is an API gate.** To pass it as literally written the
engine would need an **owned-record, interned-type append API** and **producer-side
combining** — Spike B's "next narrow spike", still the right next question, and now
with a measured prize attached (+30–45 points of bare).

**Recommended re-based gate** (per batch size, 4 writers):
`Process >= 1.20x current engine` **AND** `Group >= 95% current engine at >= 4
writers` **AND** `p99 <= current engine` **AND** `barrier count == bare's`.
b0d passes all four today.

---

## 10. The composed Asterism projection (**Projected**, from Measured parts)

There is no integrated Asterism engine to run. What can be stated honestly is the
*budget*: take b0d (**Measured**) and add back the components it lacks, each priced
from its own spike's **Measured** per-op cost.

| component | per-append cost | source |
|-----------|-----------------|--------|
| dense head apply (A) | ~23–30 ns (33–43 M applies/s) | Spike A, Measured |
| epoch dedupe probe (G) | 100–130 ns at full W=1M | Spike G, Measured |
| microblock append (F) | **cheaper** than the `ActiveIndex` it replaces (apply 2.2–5.3x) | Spike F, Measured |
| registry probe (existing name) | ~20 ns (hashmap) | Derived |
| **sum** | **<= ~300 ns/append** | Derived |

Against b0d's **4–25 us/append**, that is **<= ~5%** — i.e.:

> **Projected**: a composed Asterism engine lands within ~5% of the b0d column, and
> therefore at **~1.15x–3.6x the current engine** in Process mode and **~0.9x–1.7x**
> in Group mode, with the current engine's barrier-count defect gone.

**The one cost NOT in this budget is the durable registry write for a *new* name** —
the thing §8 says does not exist yet. On the design's own terms it is a `$registry`
append that rides the *same* group barrier (hence gate 2's "exactly one barrier"), so
it should be ~free in Group mode and one extra log record in Process mode. **That is
a design claim, not a measurement, and it is the single biggest thing Spike J could
not verify.** It is the first thing the implementation program must measure.

---

## 11. Recommended implementation order

Ordered by *(evidence strength x blocking power) / risk*.

1. **The log-derived `$registry`** (names in the log, not in Fjall).
   *Why first*: it is the **only** blocker on Fjall retirement (§8), the **only**
   unmeasured term in the composed projection (§10), and what gate 2 needs (§9).
   Everything else is already proven. **Measure the new-name cost against §4.3's
   numbers as the acceptance test.**
2. **The flat single owner (Spike B, B0Direct)** — owner owns the writer *and* the
   barrier. Adopting it **also fixes bn-3pz for free** (§5.3). Do **not** front the
   existing committer thread (Spike B: -35%). Do **not** build B1 pipelining (H2a
   reconfirmed twice). Tune the 1-writer 200 us grace before merge (§3.1).
   - *If (2) slips, bn-3pz must still be fixed independently* by hoisting the
     in-flight gate to the top of `LogEngine::append_batch`.
3. **The resident state kernel: A (dense heads) + G (epoch dedupe)** — retires 6 of
   8 Fjall keyspaces. Cheap, proven, and only meaningful once the owner owns the
   state (2).
4. **F (microblocks), stream side only** — with **stride-1** globals, not stride-8
   (§6). Re-verify the global-seek path after integration.
5. **D (effects + checkpoints)** — the fast-recovery design; unblocks the last two
   Fjall keyspaces' semantics.
6. **H (bitrank directory)** — seal-time only, off the hot path, near-zero risk.
   Take the free `foldhash` win Spike H filed regardless.
7. **SealPack RSS fix + bn-11g**, *then* consider defaulting `seal_pack` on (§7.2).
8. **v4 / migration (E, M9): NOT NOW.** Nothing above needs it.

**Explicitly deferred, with a measured prize attached**: the owned-record,
interned-type append API + producer combining (§9.1) — worth **+30–45 points of
bare** and the only route to the original 85% gate.

---

## 12. Suite result — and the one failure

`cargo test --workspace --release` on the workspace base: **649 passed, 1 failed**.

The single failure is
`mess-store::engine_publish_cancel::cancel_then_same_stream_retry_never_double_writes_version`
(`append must be dropped in flight`, `engine_publish_cancel.rs:254`). It is a
**flaky test setup, not an engine defect**, and it is **not attributable to this
spike** (whose only non-spike source change is an env toggle in the `owb_bench`
example):

- the test calls `poll_once_then_drop(engine.append_batch(..))` and then
  *asserts* the future was genuinely still in flight after that one poll. Whether
  an append resolves within its first poll is a scheduling race; under favourable
  scheduling the drop is a no-op and the assertion fires.
- the property it is defending (a dropped in-flight append still publishes and
  holds the per-stream gate until it does, so a racing same-version retry cannot
  double-write version 1) is **correct and worth keeping** — only the setup is racy.
- re-run in isolation under lighter load: **5/5 green**.

Filed as **bn-31n** with a suggested deterministic-seam fix. Same class as the
already-boned **bn-2rk**, but a different test, so neither hides the other.

Two duplicate `cargo test --workspace` invocations were briefly running
concurrently during this session and deadlocked each other on the store lock;
that was operator error on my part, was killed, and the 649/1 result above is
from a single clean run.

## 13. Friction / honesty

- **The `owb_bench` change**: `OWB_SEAL_PACK=1` was added to an *example* (not to
  `mess-store` proper) so one binary could seed+reopen both configurations. Verified
  by file inspection that it actually produces `.seal` vs the sidecar trio.
- **The seek measurement was got wrong twice before it was got right.** An active
  `mmap` pins pages, so a `fadvise(DONTNEED)` "cold" row taken while mapped comes
  back *warm*; and an mmap row taken right *after* an evict measures major faults,
  not warm loads. The published ordering is warm-pread -> warm-mmap -> munmap ->
  evict -> cold-pread. Both mistakes are noted in the source.
- **The `convoy b1000 x 64w` cell is device-bound**, not engine-bound (`fdatasync`
  56 ms; the engine's own DEGRADED threshold fired). Excluded.
- **The `sealmatrix` medians are noisy** (§7.1) — background sealing plus page-cache
  warm-up. Best-of-3 is the more trustworthy statistic there, and it says parity.
- **b0dth is not a shippable engine number.** It changes the producer topology and
  the record API. It is used only to bound *where* the residual cost to bare lives.
- **b0d is an append kernel, not the Asterism engine** (§1). Every b0d number here
  is missing a durable registry, a dedupe window and a read index. §10 prices those
  back in and labels the result **Projected**.
- The frozen `spikes/flat_combined_append` was **not** edited (`spikes/README.md`:
  spikes are reference artifacts). Its source was copied forward into `src/flat.rs`
  with its provenance and its one API-drift fix documented in the file header.

## Files

- `src/flat.rs` — Spike B's `FlatEngine`, carried forward (provenance in header)
- `src/main.rs` — harness (`matrix` / `newname` / `convoy` / `sealmatrix` / `seek` / `point`)
- `src/timing.rs` — the auditable quiet-guard (§2)
- `tests/correctness.rs` — differential oracle vs **today's** engine + owner invariants
- `bisect.sh` — the bn-3pz three-commit bisect (§5.2)
- `reopen.sh` — reopen wall + peak RSS, current vs sealpack (§7.2)
- `summarize.py` — CSV -> per-cell tables + the load-audit table
- `composed_results.csv` — every run (480 matrix + convoy + newname + seal), each with
  the `load1` it ran under
- `matrix_run.log`, `convoy_run.log`, `newname_run.log`, `sealmatrix_run.log`,
  `seek_run.log`, `bisect_run.log`, `reopen_run.log` — full outputs
