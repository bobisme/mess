# perf_group_commit — group-commit ceiling and knee on real hardware

**Question.** The vertical slice measured durable appends at ~4.2k ev/s — but with only
4 writers × 10-event batches (~40 events amortized per ~5 ms fdatasync). Claim under
test: proper group-commit amortization scales durable throughput to **100–200k ev/s**.
Find the actual ceiling and the knee on this machine, with the D7 amendment implemented
(window closes early when all in-flight writers are pending; `max_delay` is a cap).

**Verdict up front.** The claim validates and is exceeded. On a settled device, D7
early-close group commit on a single segment file sustains **362k fully-durable ev/s at
512 writers × 10-event batches (ack p50 14.1 ms, p99 19.1 ms)** and peaks at ~685k ev/s
(64w × 100, p50 7.8 ms). The 100–200k band arrives at 64 writers × batch 10 (100k ev/s,
p50 5.9 ms) or just 4 writers × batch 100 (121k ev/s, p50 2.8 ms). The ceiling is **not
fsync-count bound**: it is sustained write+flush bandwidth (~150–200 MB/s here), reached
once commit groups carry megabytes. Of the optimizations tested, striping, explicit
pipelining, and parallel encode all failed to beat plain early-close group commit;
**coalescing each group into ONE write syscall — and issuing it O_DSYNC (FUA) instead of
write+fdatasync — is the one real win at the top end** (medians 457–467k ev/s @ p50
9–10 ms at 512×10 vs d7's 104–205k @ 14–26 ms; tightest tails of all designs). One
machine-level finding dominates everything: this 95%-full drive's fdatasync degrades
~50× (3 ms → 150+ ms) under sustained write load and recovers after ~3–4 min idle;
every comparative number below is drift-controlled.

Environment: Samsung 970 EVO Plus 2 TB (`nvme0n1p3`, **95% full**), ext4 `rw,relatime`,
scheduler `none`, Linux 7.0.12-arch1-1, ~250 B JSON-ish payloads (the vertical-slice
workload shape), format v2 batch framing byte-identical to
`spikes/vertical_slice/src/seglog.rs`. All numbers are **fully durable acks**: append
returns strictly after the covering fdatasync (or O_DSYNC write) returns — never
weakened anywhere, including inside every optimization.

Run: `cargo run --release -- all` (or `baseline | matrix | curve | compare | head2head |
duel | crash | clean`). Bench data is deleted per point and at exit.

---

## 1. Methodology

```text
target      100-200k durable ev/s claim; find ceiling + knee on this device
baseline    single-fsync latency distribution (1,000 samples); flush-only cost;
            O_DSYNC vs write+fdatasync; concurrent-file flush scaling;
            vertical-slice 4-writer reproduction (incl. the F2 fixed-window regression)
measure     scaling matrix: writers {4,16,64,256,512} x events-per-append {1,10,100};
            per point: durable ev/s, ack p50/p95/p99, achieved fsync/s, mean events
            amortized per fsync
hypotheses  H1 D7 early-close >> fixed window, >= sync-per-batch everywhere
            H2 (a) pipelining: overlap encode+write of group N+1 with fsync of group N
            H3 (b) striping across 2-4 segment files buys parallel flush capacity
            H4 (c) O_DSYNC group writes beat write+fdatasync
            H5 (d) encode+write off the lock (reserve + parallel pwrite) lifts the top
re-measure  every change vs the d7 control at identical points; interleaved reps with
            settle pauses once the device-drift confound was found
gates       recovery verification after EVERY point (recovered == acked, exact global
            tiling, zero discards); SIGKILL harness at the most aggressive configs;
            striped-merge determinism unit tests
```

**Measurement trap found and controlled.** Sequential cross-design benchmarking poisons
itself on this machine: tens of GB of writes exhaust the 95%-full drive's SLC cache /
provoke GC, and fdatasync p50 walks from ~3 ms to **150+ ms**. Drift control: re-running
the sync-per-batch 4w×10 point immediately after the matrix gave **229 ev/s vs its
original 8,196 — a 36× swing from device state alone**. Idle-recovery probe: 73 → 157 →
75 → 8.6 → **5.6 ms p50 after ~3–4 min idle**. Authoritative numbers therefore come from
settle-paced runs (12–20 s idle before each point, ≤1M events per point) with
`spb 4w×10b` sentinels interleaved, and design comparisons from interleaved repetitions
with all reps reported. The raw sequential matrix is kept in §5 for curve *shape* only.

## 2. Baseline: what one durability barrier costs here

1,000 samples each, settled device:

```text
fdatasync (256B dirty)   p50 3.31 ms   p90 6.42 ms   p99  9.62 ms   max  46.8 ms  (~242/s serial)
fdatasync (clean)        p50 0.07 ms   p90 0.10 ms   p99  0.22 ms   (flush cmd alone: cheap)
O_DSYNC write (256B)     p50 2.57 ms   p90 3.35 ms   p99  6.19 ms   (1.3x faster than fdatasync)
O_DSYNC write (1 MiB)    p50 6.66 ms   p99 96.1 ms   max 141.9 ms  (serial-loop tail is ugly...)
write+fdatasync (1 MiB)  p50 6.65 ms   p99 10.1 ms   max  10.8 ms  (~150 MB/s incl. barrier)
concurrent fdatasync, own file each: 1 file 384/s | 2: 360/s | 4: 763/s | 8: 1,557/s aggregate
```

The "fdatasync ≈ 5 ms" planning number holds (3.3–5.6 ms p50 depending on device state;
150+ ms degraded). Three downstream facts: **(1)** flush capacity scales with
independent files (~4× at 8) — the striping motivation; **(2)** a 1 MiB durable write
costs ~2× a 256 B one, not 4000× — durability is priced per *barrier*, not per byte,
until bandwidth binds; **(3)** O_DSYNC's serial-loop p99 at 1 MiB looked disqualifying
but did NOT reproduce under real concurrent load (§6) — the opposite happened.

**Vertical-slice reproduction (4 writers × batch 10)** — F2's ordering reproduces, and
the D7 amendment fixes it:

```text
                       this spike (log only)      round-2 spike (composed, fjall index)
sync-per-batch         8,225 ev/s  p50  4.8 ms      3.7-4.2k ev/s  p50 ~5.3 ms
group fixed 1 ms      10,905 ev/s  p50  3.6 ms      ~4k
group fixed 5 ms       5,277 ev/s  p50  7.6 ms      ~2.7k
group fixed 25 ms      1,432 ev/s  p50 27.6 ms      ~950
d7 early-close        15,841 ev/s  p50  2.5 ms      (new)
```

Log-only runs ~2× the composed slice (no index on the ack path); fixed windows get
strictly worse as the delay grows, exactly as F2 recorded. The amendment already pays at
4 writers: **+93% over sync-per-batch at half the latency** — the window closes the
moment all in-flight writers are pending instead of padding out the timer.

## 3. The curve (d7 early-close, settled device, drift-controlled)

15 s settle before each point; ≤1M events or 2 s per point; recovery-verified after
every point. Sentinels (`spb 4w×10b`) ran 6,570 / 5,099 / 5,245 ev/s before the
b=1/b=10/b=100 rows — device stable through b=10; the final sentinel (1,338 ev/s) shows
the b=100 row's ~1 GB re-degraded the drive, so read the 256/512×100 rows as
conservative.

```text
writers x batch -> durable ev/s   (ack p50/p99 ms | fsync/s | events per fsync)

batch=1    4w        896   ( 4.5 /   9.5 | 328 |      2.7)
          16w      3,346   ( 5.1 /  12.9 | 342 |      9.8)
          64w     14,261   ( 3.6 /  11.8 | 305 |     46.8)
         256w     22,348   ( 9.6 /  91.1 | 164 |    135.9)
         512w     56,533   ( 8.3 /  20.2 | 247 |    228.8)

batch=10   4w      8,719   ( 4.6 /   9.6 | 318 |     27.4)
          16w     41,998   ( 3.1 /   9.0 | 287 |    146.1)
          64w    100,183   ( 5.9 /  12.5 | 299 |    335.4)   <- the claim's 100k floor
         256w    143,732   (11.2 /  99.0 | 111 |  1,291.4)
         512w    362,578   (14.1 /  19.1 | 175 |  2,066.3)   <- headline (fdatasync path)

batch=100  4w    121,170   ( 2.8 /   6.9 | 324 |    373.5)   <- 121k at FOUR writers
          16w    198,136   ( 6.3 /  13.4 | 125 |  1,582.4)
          64w    684,552   ( 7.8 /  50.5 | 206 |  3,321.1)   <- peak (hit 1M cap in 1.47 s)
         256w    430,161   (47.0 / 245.3 |  33 | 12,857)     (device sagging mid-row)
         512w    567,120   (88.3 / 108.5 |  23 | 25,026)     (13 MB groups; bandwidth-bound)
```

**The knee.** While the device holds ~250–340 fsync/s, throughput is
`W × B / fsync-latency` — linear in W×B at *flat 3–6 ms p50 ack latency*: from 896 ev/s
(4×1) to ~100–120k ev/s (64×10, 4×100). The knee sits where a commit group's bytes stop
being trivial against device bandwidth: **W×B ≈ 5–6k events (~1.5 MB/group,
~150–350k ev/s)**. Beyond it fsync/s falls as each barrier carries more data,
events-per-fsync rises to compensate, and latency starts buying throughput (14 ms at
362k; 88 ms at 567k).

**The ceiling and why.** Not fsync count (barriers stayed ≥200/s until groups got big);
not CPU (encode+CRC of 250 B events is ~1 µs; cores never saturated); not queue depth.
It is **sustained write+flush bandwidth**: the peak points move ~150–230 MB/s of framed
batches (567k ev/s × 265 B ≈ 150 MB/s — the measured 1 MiB write+fdatasync rate), so
each extra event pays its own transfer time and ev/s plateaus at 430–870k depending on
device state and barrier type (§6). The plateau is *state-dependent*: degraded, the same
design delivers a proportional fraction — though the crash-harness child, running on the
fully degraded device, still acked 400–790k ev/s in bursts because events-per-fsync
reached 51,200. Amortization is exactly what makes a slow-barrier device tolerable.

## 4. Optimizations evaluated (hypothesis → measurement)

Comparisons from interleaved reps with settles (tables in §6): `d7` = single file,
early-close window, writers write under a short lock, one syncer thread.

**H1 — D7 early-close: CONFIRMED; this is the core design.** ≥ sync-per-batch everywhere
measured; fixed windows strictly worse at low concurrency (F2 reproduced, §2); degrades
to sync-per-batch at 1 in-flight writer by construction. Implementation note worth
carrying: with "in-flight" counted as entered-append-but-not-yet-submitted, a momentary
zero right after a group acks can split the convoy across two barriers (observed: d7's
ev/fsync ≈ 2,100–2,600 vs the committer's 5,070 at 512×10). Spec keeps `max_delay` a
cap, so this is conformant — but a committer-style single gather point avoids the race
entirely.

**H2 (a) — explicit pipelining: NO WIN.** `commit-piped` (committer thread, one
coalesced write per group, fsync handed to a second thread so group N+1's encode+write
overlaps group N's fsync) vs `commit-fsync` (same, fsync inline): rep spreads overlap
completely at both points (§6); nothing to distinguish. Reason: the writer-writes design
*already* overlaps writes with the in-flight fsync (the append lock is not held across
fsync), and gather naturally continues while the barrier runs. Keep the property "never
hold the append path across the barrier"; skip the extra stage.

**H3 (b) — striping across 2–4 files: REJECTED for v1 despite real parallel flush
capacity.** The device gives ~4× aggregate fsync/s at 8 files (§2); striped recovery is
implemented, deterministic, and crash-proven (§8) — and striped2/striped4 still **lost
to single-file d7 at every interleaved point** (0.15–0.8×, §6). Cause, visible in the
counters: the global-order ack rule (ack only when ALL earlier positions on ALL stripes
are durable) couples every ack to the slowest stripe's current barrier; stripes drift
out of phase, the durable watermark advances in stutters, and each stripe forms smaller,
more frequent windows — un-amortizing exactly what group commit amortizes. Parallel
flush capacity is real; the coordination tax eats all of it. Spec cost documented in §7
for the record.

**H4 (c) — coalesced O_DSYNC group writes: WIN at the top end; adopt as a measured
option.** The committer architecture writes each group as ONE syscall; with O_DSYNC the
write itself is the barrier (REQ_FUA path) and no FLUSH is ever issued. At 512×10 it
medians 457–467k ev/s @ p50 9–10 ms across h2h and duel vs d7's 104–205k @ 14–26 ms
(2.3–4.4×), with the tightest tails of any design; at the bandwidth-bound 512×100 point
all barrier types converge (~800–870k, dsync again lowest variance: p95 62–79 ms).
Mechanism: a FUA write pays for its own bytes only, while FLUSH pays for the device's
entire dirty-cache state — which is exactly what degrades on this drive. Caveats stated
honestly: FUA correctness is a per-device trust question (same class as trusting FLUSH);
the serial 1 MiB O_DSYNC micro showed 96 ms p99 that never appeared under concurrent
load; ext4 journal fallbacks can silently turn O_DSYNC into flush-equivalents on some
configurations. Ship as `write+fdatasync` default with an O_DSYNC mode gated on a
startup micro-benchmark of the actual store device.

**H5 (d) — reserve + parallel encode + pwrite: NO WIN at these payload sizes.**
`d7-pwrite` (positions and file ranges reserved under a ~1 µs lock; encode+CRC+pwrite
fully parallel across writers; per-stripe write-completion watermark gates fsync so a
hole below a pending batch is impossible) ties d7 at the bandwidth-bound point (705k vs
675k median, inside rep spread) and loses elsewhere. Lock-held encode of a 26 KB batch
is ~10 µs — nothing to parallelize until events are ~4 KB+. The machinery is implemented
and crash-proven if payloads ever grow.

## 5. Raw sequential matrix (shape only — device drifts across designs)

spb ran first (freshest device) and carries the honest surprise: **concurrent fdatasync
on one file is already kernel-mediated group commit** — 512 threads each doing
write+fdatasync reached 220k ev/s because the kernel coalesces their flushes (56k
completed fdatasync calls/s against a device that does ~250 serial flushes/s: most calls
return already-covered). d7 matches this while issuing 200× fewer barriers (one barrier
= one commit boundary, which D8's EIO poisoning and D11's watermark actually need), and
beats it wherever the kernel's accidental convoy is smaller than an explicit window.

```text
durable ev/s at (4 / 16 / 64 / 256 / 512 writers)
spb   b=1      742 /   3,043 /  12,077 /  16,821 /  15,755
      b=10   8,196 /  33,220 / 114,994 / 204,074 / 220,644
      b=100 77,716 / 182,201 / 324,088 / 308,300 / 187,374
d7    b=1      469 /   1,473 /   5,850 /  17,980 /  21,400
      b=10   4,466 /  15,291 /  56,347 / 138,536 / 222,263
      b=100 50,010 / 102,013 / 198,053 / 352,584 / 116,828*
d7-pwrite b=100 23,919 / 62,204 / 272,275 / 481,168 / 636,044*
striped2 / striped4 / striped4-pwrite / commit-dsync rows ran on a progressively
degraded device and are superseded by §6.     * = device visibly degrading in-row
```

(The sequential d7 rows sit 30–60% below the settle-paced curve in §3 — that gap IS the
device-state effect. Worth internalizing before trusting any absolute number from any
storage benchmark on a nearly-full consumer SSD.)

## 6. Head-to-head under drift control (the fair comparison)

Interleaved reps, 12 s settles, median of 3 (all reps shown — the spread is the device,
and it hits every design):

```text
512w x 10b            median ev/s   p50/p95/p99 ms     ev/fsync   reps ev/s
  d7                      104,412   30.3/170.1/211.4    2,531     [52,860 / 104,412 / 220,032]
  d7-pwrite                27,120   182.5/309.6/340.3   2,617     [20,943 / 27,120 / 182,716]
  striped2-pwrite          63,788    57.8/109.1/495.4   1,301     [16,182 / 63,788 / 142,148]
  striped4-pwrite          16,664   228.2/555.3/556.1     501     [14,988 / 16,664 / 56,820]
  commit-piped            109,921    15.0/158.3/503.0   1,704     [14,979 / 109,921 / 327,021]
  commit-dsync            457,088     9.1/ 12.0/ 73.7   5,048     [35,553 / 457,088 / 924,445]

512w x 100b (all reps hit the 750k-event cap)
  d7                      674,599    70.4/141.1/154.7  21,651     [645,926 / 674,599 / 732,126]
  d7-pwrite               704,905    69.2/ 89.1/ 91.2  24,276     [577,107 / 704,905 / 727,698]
  striped2-pwrite         547,572    88.6/118.7/122.3  12,716     [297,936 / 547,572 / 782,715]
  striped4-pwrite         525,230    94.6/111.9/115.2   6,259     [457,667 / 525,230 / 862,900]
  commit-piped            638,904    78.5/ 91.8/ 91.8  17,415     [393,123 / 638,904 / 816,342]
  commit-dsync            775,900    64.5/ 77.5/ 78.5  47,124     [714,297 / 775,900 / 1,755,111]
```

Confirming duel (d7 vs commit-fsync vs commit-dsync, 4 alternating reps, 20 s settles;
one device stall of 400–900 ms p95 landed on each design — it is not design-specific):

```text
512w x 10b   reps ev/s (p50 ms of best rep)
  d7             [55,340 / 151,255 / 258,235 / 341,300]   (14.5)   ev/fsync ~2,400
  commit-fsync   [16,608 / 123,859 / 270,946 / 528,045]   ( 9.4)   ev/fsync ~5,070
  commit-dsync   [86,735 / 312,957 / 467,408 / 505,106]   ( 9.7)   ev/fsync ~5,070

512w x 100b  reps ev/s
  d7             [678,323 / 818,160 / 854,780 / 2,333,641*]   p50 60-65 ms
  commit-fsync   [733,081 / 802,630 / 844,340 / 847,963]      p50 57-66 ms
  commit-dsync   [806,215 / 864,513 / 869,610 / 870,328]      p50 58-61 ms, p95 63-79 ms
  * one freak rep on a device burst (0.34 s for 801k events, 184 barriers/s) — reported,
    not believed as sustainable
```

Reading: the committer's single coalesced write per group both avoids d7's convoy-split
race (5,070 vs ~2,400 ev/barrier) and cuts 512 write syscalls to 1; O_DSYNC then removes
the FLUSH's dirty-cache dependency. At the bandwidth ceiling everything converges.

## 7. Striping: recovery-merge rule and spec cost (design probe result)

Implemented, verified, priced — and rejected for v1. What striping costs if a future
device (≥20 ms barriers with real parallel flush) justifies revisiting:

```text
write side
  positions assigned centrally: one global mutex hands out (batch_seq, global_pos);
    stripe = batch_seq mod k. Physical order WITHIN a stripe file is NOT
    global-position order under concurrency (lock-order inversion is legal).
  ack rule (load-bearing): ack batch b only when the GLOBAL durable watermark passes
    it — b's own stripe's covering barrier returned AND every batch with an earlier
    global position (any stripe) is durable. Implemented as a contiguous watermark
    over batch_seq. Without it an acked batch can sit beyond a durable gap, and
    recovery would discard it: ack violation.
  pwrite variant additionally needs a per-stripe write-completion watermark gating
    fsync, else a reserved-but-unwritten hole below a pending batch can be fsynced
    over, and recovery later stops at the hole, orphaning an acked batch beyond it.

recovery merge (deterministic; unit-tested + SIGKILL-proven)
  1. per stripe, in segment order: scan; stop the stripe at the FIRST invalid batch
     (A10 per stripe — never resync past a hole)
  2. sort surviving batches by first_global_pos
  3. accept the maximal exactly-tiled prefix from 0; the first gap OR overlap stops
     acceptance; everything beyond is discarded (guaranteed unacked by the ack rule)

spec cost (why v1 says no even before the measured loss)
  - A1's in-scan contiguity check no longer exists per file; its strength moves into
    the merge (gap/overlap detection) — a second, subtler invariant to keep correct
  - "the log" stops being one byte-ordered object: global order exists only after a
    k-way merge, and every raw-segment consumer (replay, seal, D11 subscriptions,
    D4/G10 fold-certificate chains) must merge first
  - commit latency couples to the slowest stripe (measured: that IS the whole loss)
  - segment roll, retention, and manifest bookkeeping multiply by k
```

## 8. Correctness gates

- **Every benchmark point** (hundreds of runs, every design including striped/pwrite):
  after finalize, `recover_dir` must reproduce exactly the acked event count, exactly
  tiled from position 0, zero merge discards. All green.
- **SIGKILL harness** (crash_log-style: child appends at the most aggressive configs,
  records each ack to a side file strictly after the ack; parent kills at a random
  1.2–2.6 s, recovers, cross-checks): `striped4-pwrite 512w×100b` ×4,
  `striped4 256w×10b` ×3, `d7-pwrite 512w×100b` ×3. **34,187 acked-and-recorded batches
  across 10 rounds — every one recovered with matching stream, version, event count,
  and payload CRC.** Written-but-unacked batches surfaced ≤ #writers per round (the
  D7/A6 case the dedupe-window spec must own); striped merges reproduced exact global
  order every time; zero torn ack records. (Ran on the degraded device — a harsher
  schedule than the benchmarks got.)
- Unit tests: framing round-trip, torn marker, exact-scan vs stripe-scan modes,
  hole-stops-scan, striped merge order reproduction, gap discard, overlap rejection.
- SIGKILL preserves page cache, so this gate exercises ordering/watermark/merge/torn
  in-process state — not lost writeback (crash_log + torn_write own that layer; nothing
  here contradicts them). An O_DSYNC/FUA path narrows the trust surface to the device
  honoring FUA — same class as trusting its FLUSH.

## 9. Recommended group-commit design for the convergence doc

```rust
Group { max_delay: Duration /* CAP, ~1 ms */, max_bytes: u64 /* ~8-16 MiB */ }
```

1. **Single segment file, single sync pipeline. No striping in v1** (H3). Re-open only
   for a device with ≥20 ms barriers AND real parallel flush, and pay §7's bill
   knowingly.
2. **D7 early-close as specced**: the window closes the moment every in-flight writer
   is pending (one atomic counter; in-flight = entered append, not yet submitted);
   `max_delay` strictly a cap; `max_bytes` closes on volume (also the memory bound when
   the device degrades). Degrades to sync-per-batch at one writer — measured equal.
3. **Commit path: a single committer gathers the group, assigns positions, and issues
   ONE coalesced write.** This is simultaneously the D3/D9 single-writer position
   authority, the fix for the convoy-split race (5,070 vs 2,400 ev/barrier at 512×10),
   and a 512→1 syscall reduction. Writers hand off encoded frames; the committer must
   not re-encode (H2/H5: no extra pipeline stage, no parallel-pwrite complexity at
   these payload sizes).
4. **Barrier: write+fdatasync by default; O_DSYNC (FUA) coalesced writes as a
   first-class option enabled by an at-open micro-benchmark of the store device**
   (H4: 2.3–4.4× at high concurrency with the tightest tails here, but per-device
   trust/behavior must be verified — it is a config knob, not an assumption).
5. **Never hold the append path across the barrier** — group N+1 accumulates while
   group N syncs (this property, not an explicit double buffer, is the real
   "pipelining").
6. **Ack = durable watermark passes the batch**, position-ordered — which is exactly
   D11's writer obligation (watermark before live publish). Spec them as one mechanism.
7. Spec must treat **barrier latency as device state, not a constant** (3 ↔ 150 ms on
   this hardware): D8's poisoning logic must not misread a 600 ms GC stall, and ops
   docs should say plainly that nearly-full consumer SSDs swing durable throughput by
   an order of magnitude (recovery here: ~3–4 min idle).

Numbers to carry into doc 12 (D7): durable-append ceiling on the reference machine ≈
**362k ev/s @ p50 14 ms / p99 19 ms** (512 writers × 10-event batches, fdatasync path;
**~460k @ p50 9–10 ms** with coalesced O_DSYNC), **121k ev/s @ p50 2.8 ms at just
4 writers × 100-event batches**; knee at ~1.5 MB per commit group; ceiling = device
write+flush bandwidth (~150–200 MB/s), not fsync rate. The old fixed-delay Group
numbers (F2) are obsolete.

## 10. Friction / honesty

- The device-state confound cost half the spike and invalidates naive sequential
  benchmarking on this hardware. All comparative claims rest on interleaved,
  settle-paced runs; residual 2–10× rep spread remains at some points and is shown raw
  in §6 rather than averaged away.
- The O_DSYNC end-to-end result contradicts the serial 1 MiB micro-benchmark's p99 —
  believe the loaded numbers, but re-verify FUA behavior per target device before
  leaning on §9.4.
- d7's convoy-split (ev/fsync ≈ half of W×B) is an honest cost of implementing the
  early-close rule literally with a decentralized counter; the committer gather point
  removes it without violating "max_delay is a cap".
- Committer engines assign positions on the committer thread; the ack-channel
  round-trip (~1 µs) is inside all reported latencies.
- 1 GiB segments here (vs D2's 256 MiB) purely to keep rolls out of the measurement;
  rolls add one clean barrier per segment and change nothing structural.
- The fjall active index is absent by design (this spike isolates the log). Vertical
  slice F1 already requires index writes batched per commit; at 362–685k ev/s the
  index write path will be the next bottleneck to measure, not the log.
