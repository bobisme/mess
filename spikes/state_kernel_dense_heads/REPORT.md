# state_kernel_dense_heads — Spike A (bn-hzc): direct head tables vs fjall/HashMap

**Question.** Can a workload-shaped direct table over dense stream IDs materially beat
fjall and `HashMap<u64, Head>` for `Head { version, global_position }` point reads while
staying safe under concurrent readers — clearing p50 ≤ 50 ns / p99 ≤ 150 ns under one
writer, ≥ 20 M cell-updates/s, ≤ 20 B resident/stream, and bounded reader retries?

**Verdict up front.** The direct designs win decisively; the "keep HashMap" fallback is
OFF the table. The decision rule (direct ≥ 5× fjall AND ≥ 2× HashMap on read latency)
passes: under one continuous writer at 1M streams, **A4 (packed `AtomicU128`) reads at
p50 70 ns / p99 210 ns vs fjall 3,190 / 7,340 ns (46× / 35×) and RwLock-HashMap
180 / 19,410 ns (2.6× / 92×)**. Four readers + one writer sustain **207 M reads/s
uniform, 511 M reads/s Zipf(1.1)** on A4 vs **3.3 M** on the RwLock HashMap (63–147×)
and 1.2 M on fjall. Writer apply is 33–43 M cells/s (gate ≥ 20 M: PASS), resident cost
is exactly **16.02 B/stream at both 1M and 10M** (gate ≤ 20 B: PASS; HashMap is 42–53,
fjall 109). A2 (seqlock pages) is within ~10–50% of A4 everywhere and is the portable
fallback where 128-bit atomics aren't lock-free; its reader retries stay bounded
(p99.9 ≤ 1 retry in the normal writer regime, ≤ 59 in the deliberately pathological
same-page-hammer regime). The absolute p50 ≤ 50 ns / p99 ≤ 150 ns gate numbers hold
quiet (p50 20 ns) but NOT under a writer with uniform-random reads at 1M+ streams — for
ANY candidate — because reads of freshly-invalidated lines pay cross-core transfer +
L3/DRAM misses (~70–100 ns floor); that is physics of the 16 MB+ footprint, not a
design defect, and the relative rule is what the bone makes decisive. **Recommendation:
adopt the chunked direct-page table (design.md §7.1) with the cell representation
selected by capability: packed 128-bit (A4) where `AtomicU128::is_lock_free()`, page
seqlock (A2) otherwise. A3 (double-buffer + selector) is rejected: 40 B/stream (memory
gate FAIL), slowest writer, and an unproven ABA torn window.**

Environment (Measured): AMD Ryzen 9 3900X, governor `performance`, 24 threads; readers
pinned to cores 1–4, writer to core 5 (same CCD); rustc 1.97.0 (2d8144b78 2026-07-07),
`--release`, `lto = "thin"`; Linux 7.0.12-arch1-1; fjall 3.1.6 (repo pin), hashbrown
0.15 (default hasher = foldhash), portable-atomic 1.x (**`AtomicU128::is_lock_free() =
true` on this host** — cmpxchg16b); disk for A0: nvme0n1p3 ext4 at 78% full (fjall is
journal-buffered here, no fsync per group — I5 contract, so the perf_group_commit
fsync-drift trap does not apply). TSC 0.25 ns/tick.

Run: `cargo run --release -- all` (or `info | latency [n] | throughput [n] | apply [n]
| mem-all | torn`). `cargo test --release` for the growth tests;
`RUSTFLAGS="--cfg loom" cargo test --release --test loom_model` for the loom model.
Bench data lives under `./skdh_bench_data`, deleted at exit.

---

## 1. Candidates

```text
A0  fjall 3.1.6 point table        8-B BE key -> 16-B LE value; Database::builder +
                                   db.keyspace, one batch per commit group — the same
                                   API shape as crates/mess-index MetaStore
A1  hashbrown HashMap<u64, Head>   measured raw (no lock, quiet only) AND behind
                                   std::sync::RwLock — the minimum a shared HashMap
                                   needs to serve readers concurrent with the writer
A2  chunked pages + page seqlock   4096 cells/page; cell = 2x AtomicU64 (16 B);
                                   writer: odd seq (Relaxed) -> Release fence -> cell
                                   stores (Relaxed) -> even seq (Release); reader:
                                   Acquire seq, Relaxed cells, Acquire fence, re-check;
                                   bounded spin then yield escalation
A3  double-buffered cell           [[AtomicU64;2];2] + AtomicU8 selector (40 B/cell);
                                   writer fills inactive copy, flips selector Release
A4  packed 128-bit cell            portable_atomic::AtomicU128 (16 B), Acquire load /
                                   Release store; lock-free on this host
```

All direct kinds share design.md §7.1's grow-only two-level shape: a directory of
`AtomicPtr<Page>`, doubling growth publishes a copied directory via `Release` store,
retired directories stay alive until drop (8 B/page, bounded by doubling), pages never
move. Single-writer `apply` sorts each batch by page so A2 pays one odd/even seqlock
cycle per touched page per commit group.

**Torn-pair invariant, checked on EVERY read in every scenario for every candidate:**
writers only ever store pairs satisfying `global == version << 32 | stream_id`
(version 0 ⇒ never written ⇒ global must be 0). Any observed violation aborts the run.
Zero violations were observed anywhere, across hundreds of millions of checked reads.

## 2. Method

```text
target      p50 <= 50 ns, p99 <= 150 ns under 1 writer; apply >= 20M cells/s;
            resident <= 20 B/stream; bounded retry p99.9; relative rule:
            >= 5x fjall AND >= 2x HashMap on read latency, else keep HashMap
measure     per-op t0=rdtsc .. t1=rdtscp samples (2M/scenario; 400k for fjall),
            sorted-sample quantiles, measured empty-pair overhead (80 ticks =
            20 ns) subtracted; all key streams pre-generated outside timed loops
            (uniform, Zipf s=1.1 via exact CDF binary search, one-page)
scenarios   (a) quiet single reader; (b) 1 writer applying 100-cell groups
            continuously (uniform keys); (c) pathological: reader AND writer
            confined to the same 4096-cell page
            throughput: 4 pinned readers + 1 writer (batch 100), 4 s per point
            apply: batches 1 / 100 / 10k over uniform keys; the update stream is
            pre-generated so the timed region is pure table apply
            memory: fresh child process per point, VmRSS delta + analytic bytes
correctness loom model of the REAL A2 read/write functions; threaded page-growth
            test; invariant check on every read everywhere
```

**Measurement trap found and controlled.** With a plain `rdtsc` closing each sample,
A3/A4 reported p50 = 0 ns at 1M streams: the CPU retires the second `rdtsc` before an
independent in-flight load completes, flattering exactly the single-load candidates.
Closing samples with `rdtscp` (waits for prior instructions and load visibility)
restored honest numbers; every table below is rdtscp-based. Residual caveat: the
timed region still carries serialization slack ≈ the 20 ns subtracted overhead, so
sub-30 ns p50s are upper-bound-ish; the throughput section is the overlap-honest
cross-check (A4 Zipf: 511 M/s ÷ 4 readers ⇒ ~7.8 ns/read effective — Derived).

## 3. Results

All numbers **Measured** unless marked Derived. Latency = single pinned reader,
ns/op, 20 ns rdtsc-pair overhead subtracted; `retries` = seqlock re-reads
(p99.9/max per op). Writer = continuous 100-cell groups.

### 3.1 Read latency, 1M streams

```text
candidate        scenario     p50        p99        p99.9        max         mean    retries      writer M/s
A0-fjall         a:quiet     1800.0     2690.0     9050.0       28040        1773.2  -            -
A0-fjall         b:writer    3190.0     7340.0    12670.0    12962316        3204.9  -            0.58
A0-fjall         c:samepage  1070.0     2450.0     6940.0    80878302        1361.3  -            0.61
A1-hash-raw      a:quiet       90.0      380.0      499.8       38110          93.2  -            -
A1-hash-rwlock   a:quiet       99.8      400.0      530.0       13340         100.9  -            -
A1-hash-rwlock   b:writer     180.0    19410.0   114740.2     2899156        1403.0  -           14.18
A1-hash-rwlock   c:samepage   190.0     8050.0    13780.0     1256493         807.3  -           38.10
A2-seqlock       a:quiet       20.0      270.0      440.0       11780          43.2  0/0          -
A2-seqlock       b:writer      80.0      319.8      510.0       12930          96.3  1/32        17.52
A2-seqlock       c:samepage    79.8      880.0     1070.0       36700         192.8  50/285     115.93
A3-dblbuf        a:quiet       70.0      360.0      470.0       10920          78.5  0/0          -
A3-dblbuf        b:writer      80.0      370.0      480.0       26630          83.3  0/0         10.49
A3-dblbuf        c:samepage    70.0      160.0      180.0       10140          56.4  0/0         83.22
A4-u128          a:quiet       20.0      270.0      430.0       23730          42.6  0/0          -
A4-u128          b:writer      70.0      209.8      430.0       10210          54.9  0/0         24.54
A4-u128          c:samepage    70.0       90.0      110.0        9490          57.1  0/0        142.00
```

Notes: fjall's c:samepage is *faster* than quiet because the 4096-key working set is
block-cache-hot. A1-rwlock's b:writer tail (p99 19 µs, p99.9 115 µs) is readers
stalling behind the writer's `RwLock` write lock — the structural cost no hashing
speed can fix. A2's quiet p50 20 ns vs under-writer 80 ns is the cross-core line
transfer on freshly-written cells; A4 shows the same floor (70 ns).

### 3.2 Read latency, 10M streams

```text
candidate        scenario     p50        p99        p99.9        max         mean    retries      writer M/s
A0-fjall         a:quiet     3780.0     6290.0    13510.0       38370        3772.6  -            -
A0-fjall         b:writer    6439.8    14110.0    20120.0   144269878        7137.2  -            0.51
A0-fjall         c:samepage  1930.0     4270.0    14800.0     3040465        2113.3  -            0.38
A1-hash-raw      a:quiet      170.0      570.0      830.0       30070         199.1  -            -
A1-hash-rwlock   a:quiet      170.0      510.0      680.0       25610         181.7  -            -
A1-hash-rwlock   b:writer     350.0   821291.5  2996745.3    17003490       28030.8  -            9.59
A1-hash-rwlock   c:samepage   210.0    12180.0    28700.1     4961909        1156.8  -           24.93
A2-seqlock       a:quiet      100.0      450.0      620.0     2032084         129.6  0/0          -
A2-seqlock       b:writer     160.0      469.8      610.0       31530         150.4  0/27        12.97
A2-seqlock       c:samepage    70.0      970.0     1250.0     2018924         160.9  59/75878    85.04
A3-dblbuf        a:quiet      130.0      560.0      770.0     5480360         186.8  0/0          -
A3-dblbuf        b:writer     110.0      490.0      680.0      207810         146.3  0/0          5.77
A3-dblbuf        c:samepage    70.0      160.0      180.0       25210          48.6  0/0         43.27
A4-u128          a:quiet       80.0      380.0      480.0      247790          93.0  0/0          -
A4-u128          b:writer      89.8      380.0      490.0       29730          95.3  0/0         18.59
A4-u128          c:samepage    70.0       99.8      170.0       24040          58.0  0/0        123.27
```

At 10M streams (160 MB direct table, DRAM regime) A4 under a writer holds p50 90 ns /
p99 380 ns while the RwLock HashMap collapses to **p99 = 821 µs, p99.9 = 3.0 ms** (its
384 MB+ table makes each insert slower, so the write lock is held longer) and fjall is
6.4 µs p50 with a 144 ms max (compaction stall). The A2 c:samepage retry max of 75,878
is one descheduled-writer incident (max latency 2.0 ms); retry p99.9 stayed at 59 and
p99.9 latency at 1.25 µs — bounded in the quantile sense the gate asks for.

### 3.3 Read throughput, 4 readers + 1 writer (batch 100), 1M streams, 4 s/point

```text
candidate        uniform            zipf(1.1)          writer during uniform/zipf
A0-fjall           1.22 Mreads/s      1.36 Mreads/s     0.50 / 0.42 Mcells/s
A1-hash-rwlock     3.28 Mreads/s      3.48 Mreads/s    10.49 / 10.67 Mcells/s
A2-seqlock       164.33 Mreads/s    375.47 Mreads/s    18.11 / 18.81 Mcells/s
A3-dblbuf        126.67 Mreads/s    367.06 Mreads/s    11.06 / 11.13 Mcells/s
A4-u128          207.19 Mreads/s    510.99 Mreads/s    20.57 / 21.39 Mcells/s
```

Derived: A4 Zipf = ~128 M reads/s/core ≈ 7.8 ns/read effective; A2/A4 direct tables
serve 50–147× the RwLock HashMap's aggregate while the writer simultaneously sustains
~19–21 M cells/s. The RwLock number is reader-starvation, not hash cost (raw map reads
are ~90 ns).

### 3.4 Writer apply throughput, 1M streams, no readers (pure pre-generated apply)

```text
candidate        batch 1        batch 100      batch 10k      gate >= 20M cells/s
A0-fjall          0.40 M/s       0.57 M/s       0.55 M/s      FAIL  (~50x short)
A1-hash-rwlock   15.17 M/s      30.53 M/s      31.67 M/s      PASS at >=100, FAIL at 1
A2-seqlock       34.17 M/s      29.92 M/s      41.26 M/s      PASS
A3-dblbuf        14.24 M/s      14.09 M/s      25.56 M/s      FAIL at <=100
A4-u128          42.90 M/s      32.79 M/s      42.67 M/s      PASS
```

(A0 measured over 0.2–2 M cells per point vs 20 M for resident kinds.) The batch-100
dip for A2/A4 vs batch-1 is the per-batch page-grouping sort; at 10k the sort amortizes
and locality improves.

### 3.5 Resident memory per stream (fresh child process, VmRSS delta; analytic = allocated bytes)

```text
candidate   n=1M RSS       n=1M analytic     n=10M RSS      n=10M analytic    gate <= 20 B
A0-fjall    108.63 B/s     (+ disk)          not run        -                 FAIL
A1-hash      52.67 B/s     45.88 B/s          41.96 B/s     36.70 B/s         FAIL
A2-seqlock   16.02 B/s     16.06 B/s          16.02 B/s     16.01 B/s         PASS
A3-dblbuf    41.16 B/s     40.15 B/s          41.02 B/s     40.02 B/s         FAIL
A4-u128      16.02 B/s     16.06 B/s          16.02 B/s     16.02 B/s         PASS
```

A2/A4 sit at the 16-byte information floor (research/03 §2.3); page + directory
overhead is noise (0.06% at 1M, 0.01% at 10M). HashMap growth was organic (no
pre-capacity), as live usage would be.

### 3.6 Correctness

```text
loom      RUSTFLAGS="--cfg loom" cargo test --release --test loom_model — PASS (482 s,
          preemption_bound = 3). Models the REAL SeqCell::write_batch / try_read
          functions (src/shim.rs swaps std/loom atomics): 1 writer publishing 2 cells
          under the page sequence, 2 concurrent bounded-retry readers. Asserts no torn
          pair on any successful read, termination via bounded retry (readers may
          return None mid-publication), and a quiescent bounded read succeeding with
          the published value.
growth    cargo test --release: growth_under_readers_{seqlock,u128,dblbuf} (1M streams
          grown from a 1-page directory under 4 readers, invariant-checked) +
          sequential_read_back — 4/4 PASS.
torn      cargo run --release -- torn: growth 0 -> 4M streams per kind under 4 readers:
          A2 56.7M reads, A3 57.0M, A4 62.8M — 0 invariant violations.
inline    every read in every latency/throughput scenario verified the invariant:
          zero torn pairs anywhere, including A2 c:samepage at 116 M writer cells/s.
          A3's theoretical ABA window (reader stalled across two same-cell updates)
          never fired, but nothing here proves it can't — another reason to reject A3.
```

## 4. Gates

Evaluated at 1M streams, scenario (b) — one continuous writer, uniform reads —
unless stated.

```text
gate                                  A0-fjall    A1-rwlock       A2-seqlock     A3-dblbuf     A4-u128
p50 <= 50 ns (quiet)                  1800 FAIL   90/100 FAIL     20   PASS      70  FAIL      20   PASS
p50 <= 50 ns (under writer)           3190 FAIL   180    FAIL     80   FAIL*     80  FAIL*     70   FAIL*
p99 <= 150 ns (under writer)          7340 FAIL   19410  FAIL     320  FAIL*     370 FAIL*     210  FAIL*
apply >= 20M cells/s                  0.6  FAIL   15-32  MARGINAL 30-41 PASS     14-26 FAIL    33-43 PASS
resident <= 20 B/stream (1M & 10M)    109  FAIL   42-53  FAIL     16.02 PASS     41  FAIL      16.02 PASS
bounded reader retry, p99.9 bounded   n/a         n/a (lock)      PASS (p99.9    PASS (no      PASS (no
                                                                  <=1 normal,    retries)      retries)
                                                                  <=59 patho)
decision rule: >=5x fjall AND >=2x    --          --              40x / 2.3x     40x / 2.3x    46x / 2.6x
HashMap on read latency (b, p50)                                  PASS           PASS          PASS
```

\* The absolute under-writer numbers fail for EVERY candidate including both baselines:
a read of a line the writer just invalidated costs one cross-core transfer, and a
uniform-random probe over a ≥16 MB footprint misses L2 — a ~70–100 ns floor no shared-
memory layout can duck on this part. In the hot regimes the gate contemplates, A4
passes outright: quiet p50 20 ns; same-page-under-writer p99 90–110 ns; Zipf service
rate ~7.8 ns/read (Derived). The bone's own decision rule (relative) is decisive and
passes with margin.

## 5. Verdict

1. **Adopt the direct chunked-page head table.** Do NOT keep HashMap: under any
   concurrent writer the RwLock-wrapped map is 2.6× worse at p50, ~90× worse at p99,
   and 50–147× worse in aggregate read throughput, while costing 2.6–3.3× the memory.
   fjall as a *read path* is 40×+ off and stays as the durable/rebuild layer only.
2. **Cell representation: A4 packed `AtomicU128` where lock-free, A2 page-seqlock
   otherwise** (research/03 §2.2-C's capability-detection caveat). Both are 16.02
   B/stream and share the identical page/directory shell, so this is a per-cell
   `#[cfg]`/runtime choice, not two tables. A4 is uniformly fastest (no retry path, no
   seqlock cache-line bounce: p99 210 vs 320 ns under writer; 511 vs 375 M Zipf
   reads/s; 33–43 M cells/s apply).
3. **Reject A3** (double-buffer + selector): 40 B/stream fails the memory gate, its
   writer is the slowest direct kind (14 M cells/s at batch ≤ 100 — under the gate),
   and its no-torn-pair argument is empirical, not proved (the reader-stall ABA window
   from research/03 §2.2 is real in theory; loom-proving it safe would need a reader
   epoch, i.e., more memory still).
4. **Publication protocol is sound**: loom passes on the real A2 functions; page
   growth under readers is invariant-clean at 4M streams × 3 kinds; zero torn pairs
   across every scenario. A2's pathological retry tail (p99.9 = 59 retries when writer
   and reader share one page at 85–116 M cells/s) is bounded and 1000× below any
   plausible real per-page update rate; the yield escalation prevents livelock.
5. **Carry-forward for bn-28g / the kernel design**: expose the writer as "one
   odd/even publication per touched page per commit group" (already how `apply`
   works); the same-page hammer scenario shows why per-group (not per-cell)
   publication matters for A2 — and why A4 doesn't care.
