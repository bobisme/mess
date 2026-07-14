# Owner-ring fairness experiment — DECLINED

Bone: `bn-3eg0`

Decision: **DECLINED**. The owner-ring production change was not accepted.
This file is a historical experiment and decision record, not a description of
the deployed policy. No candidate engine code, tests, benchmark example,
metrics surface, scheduling policy, or measurement guard from `bn-3eg0` is in
production.

The decision workspace starts from main commit
`248148316b68fc2318e9ec8d8345320196a5134a`. Merging this report changes only
documentation. Production source and behavior remain unchanged.

## Production source baseline

At main `248148316b68fc2318e9ec8d8345320196a5134a`, the flat owner uses:

- a Tokio MPSC channel bounded to 1,024 intents;
- a 64 MiB Tokio semaphore acquired before channel send;
- one `OwnerIntent` holding its byte permit until the owner reaches a terminal
  result; and
- producer-side preparation for representable batches at or above the 16 KiB
  crossover, followed by ordinary byte/channel admission.

The production cost is clamped to the 64 MiB semaphore capacity. Production
does **not** contain the experimental 256 MiB preparation pool, preparation or
owner tickets, owner-ring diagnostic metrics, a priority scheduler, or a
two-class waiter deque.

The original historical performance baseline was commit `7aea2488`. A later
contemporary control used main `248148316b68fc2318e9ec8d8345320196a5134a`
because direct-outcome callbacks and reusable owner scratch had landed after
the historical baseline. Neither control carried candidate production code.

## Workload and frozen acceptance rules

All accepted performance rows used release builds, the real `LogEngine`,
Process durability, fresh stores, AMD Ryzen 9 3900X hardware, the performance
governor, rustc 1.97.0 (`2d8144b78`, LLVM 22.1.6), and the real
`$HOME/.cache/mess-test-tmp` filesystem.

The full harness contained:

- 50,000 serial empty appends after 2,000 warmups on an existing stream;
- four 60 MiB near-limit appends submitted before four tiny writers; and
- 1,000 total tiny appends, with exact final-head assertions.

Later causal studies used the mixed cell only and the balanced sequence
`A,B,B,A,A,B`, three invocations per side, with no retry or replacement row.

The original historical gates were queue-only throughput at least 78,015
ops/s, mixed-tiny throughput at least 8,242 ops/s, queue-only p99 at most
20,273 ns, mixed-tiny p99 at most 524,238 ns, and mixed near-limit p99 at most
111,327,453 ns. Paired gates required candidate throughput at least 95% of
control, p99 at most 110% of control, and mixed wall at most 105% of control.

Before the final late-ticket study, commit `99936161` froze stricter mixed-only
gates: candidate tiny p99 at most 124,872 ns and at most 90% of early-ticket;
tiny throughput at least 8,242 ops/s and at least 95% of early-ticket;
near-limit p99 at most 111,253,621 ns and at most 110% of early-ticket; and
mixed wall at most 121.27395 ms and at most 105% of early-ticket. No result was
used to move a gate.

## Experimental designs

These designs existed only in the rejected candidate workspace.

### Strict admission before preparation

Candidate `1fbf6667` moved byte admission ahead of prepared construction and
made the 64 MiB byte gate strict. It bounded admitted owner memory but charged
each near-limit request about 60 MiB before preparation, so only one of four
large preparations could run. The production baseline had built those final
buffers concurrently before owner admission.

### Separate preparation pool, early owner ticket

The next design separated a 256 MiB preparation pool from the unchanged 64 MiB
owner pool. It assigned a global owner ticket before construction, admitted
preparation leases FIFO, built concurrently, and carried the early ticket into
the combined owner byte-and-channel turn. Cancellation and close used RAII for
preparation wait, preparation-active, owner wait, and admitted phases.

The early-ticket evidence commit was
`75970e609ecd470f3abf7f0c399aade3a2f71a5a`; the focused candidate binary was
built from `443f2cd0`.

### Separate preparation pool, late owner ticket

Candidate `b199e786e36d034b6c5a954ea51a7a983227e988` retained the preparation
pool and accounting, but assigned the owner ticket only after the immutable
prepared batch, type-name table, slots, and retained shape were complete. FIFO
remained strict from owner readiness through byte reservation and bounded
channel admission. Faster preparation could pass unfinished preparation, but
no post-ready bypass was permitted.

Focused debug tests covered preparation FIFO, owner-ready FIFO, cancellation
and close at every wait phase, no post-ready bypass, same-stream readiness
winner behavior, registry/type-name resolution and reopen, exact four-near
high-water bounds, publish cancellation, and owner outcome scratch integration.

### Two-class bounded bypass — analyzed, not implemented

A final policy analysis considered FIFO latency and bulk classes, classifying
latency by conservative owner cost at most 64 KiB. A ready bulk could be
bypassed by at most 1,024 latency admissions and 1 MiB cumulative owner bytes
before one bulk admission was forced.

The policy was not implemented. It required explicit waiter deques rather than
the scalar ticket/turn mechanism, changed concurrent cross-class same-stream
conflict winners, complicated cancellation and close, and was expected to
trade bulk latency for tiny latency without removing enough total work to fix
the absolute throughput and wall failures. It was declined for insufficient
product value relative to semantic and implementation risk.

## Frozen artifact provenance

The following hashes identify archival experiment artifacts. They are not
production assets.

### Strict candidate

- candidate commit: `1fbf6667`
- frozen harness SHA-256:
  `7e186dcf8bd1c1218eb5ec618fd823ea40631a63d3279f2832e565986a90e98b`

### Early-ticket paired study

- control base: `248148316b68fc2318e9ec8d8345320196a5134a`
- unmerged control harness commit:
  `add025f106be52193c695a39586c421391ced540`
- control harness SHA-256:
  `3a810f5a437bb131e0869eb06cefca6e3feeb255463204a5f80849c83407645c`
- early-ticket harness SHA-256:
  `9d981dd9b6d745fd1accaceb832ecfa75c579095a3c981af732489cae4866019`
- control binary SHA-256:
  `a0bcb056581c83bf3a9b316db4f600f75ea7849a2ec747a67bce790fac14c8c2`
- early-ticket binary SHA-256:
  `f36f4c390e50b9d9f85910b17ad146302e01ed7d43a1edd6cb6d4389272d042f`

### Late-ticket paired study

The disposable early control was based on
`75970e609ecd470f3abf7f0c399aade3a2f71a5a` and carried only unmerged
measurement commit `098f3126ba3fc65214f8a68a6c93b2a095db89b5`.
Its measurement-only diff SHA-256 was
`009aea60a5c6b42ac5d97421de081f01720df005ce7065254cf11b1b420e7bc3`.

| side            | `engine.rs` SHA-256                                                | harness SHA-256                                                    | binary SHA-256                                                     |     bytes |
| --------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------ | ------------------------------------------------------------------ | --------: |
| A, early ticket | `e3048a81f525f4449fca306f169857fc7ddc8f54a9ed36004b2be986577c9260` | `a8e916f272303c52bdfd4c24322af1187068c1a200ec744f865758596eb8c0fb` | `079d6af30968069cf61172f21b41f6a781cfea61b01c7903781886776cc63270` | 3,637,728 |
| B, late ticket  | `4e431a14d7498504780c8ca2093c259eefc7b55d896a937324e218c6d3a9efa6` | `09085452531e8b8268a05809f267b2a08675e77c9e0ab56fc07641c18471a38b` | `80eb945dd5512920e9470535ca40ad009d8297a58ca273b8d0790d23aa4db03b` | 3,664,408 |

The binaries were built once into separate targets on 2026-07-14 using Linux
7.0.12-arch1-1 x86-64, rustc 1.97.0, cargo 1.97.0, the performance governor,
and ext4 scratch on `/dev/nvme0n1p3`. The late control was destroyed without
merge after the final evidence commit. Its recovery object is
`098f3126ba3fc65214f8a68a6c93b2a095db89b5` under
`refs/manifold/recovery/bn-3eg0-early-control/2026-07-14T21-11-29.559997790Z`.

The isolated-core protocol was recorded at `971ed7bc`; its archival guard and
fixture-runner SHA-256 values were respectively
`811061f144b659b21dd815ee03bd85cc19c4fc1b2e16f50d84c6f733d1d7f5a9`
and `15e570a7888469fb1dd3d592a0591db70f69d26c0100e4b092f6db28833ade66`.
Those scripts are intentionally absent from this decision workspace.

## Measurement history

“Accepted” below means the rows passed their environmental admission and were
eligible for literal gate evaluation. It does not mean the candidate passed.

### Historical baseline — accepted

Three rows at `7aea2488` passed `load1 < 6`. Literal medians were:

| metric                |         median |
| --------------------- | -------------: |
| queue-only throughput |   82,121 ops/s |
| queue-only p99        |      18,430 ns |
| mixed-tiny throughput |    8,676 ops/s |
| mixed-tiny p99        |     476,580 ns |
| mixed near-limit p99  | 101,206,775 ns |
| mixed wall            |     115.260 ms |

One earlier warmed baseline invocation at `load1=10.93` was rejected and did
not contribute a row or gate.

### Strict candidate — accepted rows, failed gates

Exactly three candidate invocations at `1fbf6667` passed the load guard and
logical checks. Medians were queue-only throughput 80,313.766 ops/s and p99
19,220 ns; mixed-tiny throughput 6,040.549 ops/s and p99 652,661 ns;
near-limit p99 148,565,530 ns; mixed wall 165.548 ms.

Queue-only throughput and p99 passed. Mixed-tiny throughput was 69.62% of
baseline, mixed-tiny p99 was 136.95%, and near-limit p99 was 146.79%; all three
failed their predeclared gates. The study stopped before the broad matrix.

### Early-ticket paired study — accepted rows, failed gate

The contemporary main control and early-ticket candidate ran the exact
`A,B,B,A,A,B` sequence with all harness-start loads between 3.09 and 3.30.
There were no rejected rows, retries, rebuilds, or extra cells.

| metric                |     main control | early-ticket candidate | candidate/control | verdict  |
| --------------------- | ---------------: | ---------------------: | ----------------: | -------- |
| queue-only throughput | 80,644.180 ops/s |       80,889.355 ops/s |       100.304021% | pass     |
| queue-only p99        |        20,630 ns |              19,600 ns |        95.007271% | pass     |
| mixed-tiny throughput |  8,658.061 ops/s |        8,354.487 ops/s |        96.493741% | pass     |
| mixed-tiny p99        |       113,520 ns |             540,139 ns |       475.809549% | **fail** |
| mixed near-limit p99  |   101,139,656 ns |          99,193,169 ns |        98.075446% | pass     |
| mixed wall            |       115.499 ms |             119.696 ms |       103.633798% | pass     |

The candidate also missed the historical 524,238 ns mixed-tiny p99 ceiling.
All resource, fairness, and logical diagnostics passed, but one failed cell
stopped acceptance.

### Late-ticket attempt 1 — invalidated by load

Setup evidence was committed at `ba76d56d`. Invocation 1 (`A`) reported
harness-start load1 5.09. Immediately before invocation 2 an external sample
was 5.56, but invocation 2 (`B`) reported authoritative harness-start load1
6.72. The `<6` guard failed. The remaining four invocations were not run; the
two emitted timing rows were retained but excluded, with no median or gate.
Invalidation evidence was committed at `5d384b2`.

### Late-ticket attempt 2 — invalidated by guard tooling

The environmental protocol was committed at `7d7a457`. Before invocation 1,
the first external sample was 3.18, then gawk rejected the variable name
`load` as reserved. The guard exited before its second sample and before either
binary ran. Attempt 2 produced zero timing rows. Evidence was committed at
`73ca11a`.

The corrected numeric and process guards were fixture-tested and recorded at
`046dd757`: 3.18 exited 0, 4.51 exited 1, a clean process fixture exited 0,
and a Cargo fixture exited 1.

### Late-ticket attempt 3 — invalidated by external admission

Invocations 1 through 3 completed in `A,B,B` order. Before invocation 4, the
first external load1 was 3.95; the second sample after ten seconds was 4.86,
above the frozen 4.5 limit. Invocation 4 was not launched and rows 5–6 were not
run. All partial rows were excluded with no median or gate. Evidence was
committed at `245f6f7`.

### Late-ticket attempt 4 — accepted rows, failed gates

Attempt 4 replaced machine-wide load admission with a variant-independent,
preselected cpuset because global load included unrelated desktop work. A
single ten-second `/proc/stat` survey selected the three least-busy complete
SMT pairs: `8+20` at 11.501%, `7+19` at 12.227%, and `6+18` at 13.086%.
The frozen cpuset was `6,7,8,18,19,20`, matching six Tokio workers. Each row
required a five-second aggregate busy value at most 15%, every sibling pair at
most 20%, and clean process checks before and after. Global load was recorded
but not gated.

All six guards and harnesses passed in exact `A,B,B,A,A,B` order. There was no
retry, resample, replacement, rebuild, or extra row. Result evidence was
committed at `1cf90fe5212691034d29691802d7165afb6dc764`.

These are the literal raw three-row medians:

| metric                |  A early ticket |   B late ticket |           B/A |
| --------------------- | --------------: | --------------: | ------------: |
| mixed-tiny throughput | 6,808.931 ops/s | 6,847.811 ops/s |   100.571015% |
| mixed-tiny p99        |      143,500 ns |    1,949,070 ns | 1,358.236934% |
| mixed near-limit p99  |  128,993,855 ns |  130,848,225 ns |   101.437565% |
| mixed wall            |      146.866 ms |      146.032 ms |    99.432135% |

Literal gate evaluation:

| gate                                |                 candidate median |                             required | verdict  |
| ----------------------------------- | -------------------------------: | -----------------------------------: | -------- |
| mixed-tiny p99 absolute             |                     1,949,070 ns |                   at most 124,872 ns | **fail** |
| mixed-tiny p99 material improvement |               1,358.236934% of A |        at most 90% of A (129,150 ns) | **fail** |
| mixed-tiny throughput absolute      |                  6,847.811 ops/s |                 at least 8,242 ops/s | **fail** |
| mixed-tiny throughput paired        |                 100.571015% of A |  at least 95% of A (6,468.484 ops/s) | pass     |
| mixed near-limit p99 absolute       |                   130,848,225 ns |               at most 111,253,621 ns | **fail** |
| mixed near-limit p99 paired         |                 101.437565% of A | at most 110% of A (141,893,240.5 ns) | pass     |
| mixed wall absolute                 |                       146.032 ms |                 at most 121.27395 ms | **fail** |
| mixed wall paired                   |                  99.432135% of A |      at most 105% of A (154.2093 ms) | pass     |
| logical/resource/fairness           | exact heads/bounds/zero counters |                         all required | pass     |

The late-ticket median tiny p99 was 13.5824 times the early-ticket median, not
a material improvement. Absolute throughput, near-limit p99, and wall gates
also failed. No paired headline waived an absolute or tail failure.

## Correctness and resource evidence

The final late-ticket checkpoint ran 21 focused tests with zero failures:
15 finite-model/unit tests, two owner-ring integrations, two publish-cancel
integrations, one outcome-scratch unit, and one direct-owner metrics
integration. `cargo check --workspace --all-targets` and `just fmt-check` also
passed at candidate commit `b199e786`.

Across accepted early- and late-ticket mixed studies:

- preparation active high-water was exactly 4;
- preparation byte high-water was exactly 251,660,304 bytes, below 256 MiB;
- owner reserved-byte high-water was 62,920,352 bytes for early-ticket and
  62,915,728 bytes for late-ticket, below 64 MiB;
- current preparation, owner, and waiter counters returned to zero;
- preparation/owner cancellations, FIFO invariant violations, and starvation
  observations were zero; and
- every harness passed exact final heads for 1,000 tiny and four near-limit
  appends.

The model and integrations also exercised preparation-wait, active-preparation,
owner-wait, admitted cancellation, close, same-stream winner, registry/type
resolution, and reopen behavior. This establishes that the rejected prototype
could enforce its experimental bounds; it does not make those metrics or that
policy production behavior.

A concurrent broad correctness run was stopped by the unrelated
`mess-soak::smoke` timeout after 873 of 961 tests had run. The exact soak test
passed alone with both crash cycles. This was diagnosed as shared-host I/O
contention and was never counted as owner-ring performance evidence. The broad
`BN-2SU-FINAL` matrix and review were never authorized because every candidate
design failed an earlier gate.

## Final decision

`bn-3eg0` is declined:

1. Strict admission serialized the large construction phase and failed mixed
   throughput and both tiny and near-limit tail gates.
2. The early-ticket preparation pool satisfied resource bounds but made tiny
   p99 4.758 times its contemporary control.
3. Moving the ticket to owner readiness preserved correctness and resource
   bounds, but the final isolated-core study made tiny p99 13.5824 times the
   early-ticket control and still failed absolute throughput, near-limit p99,
   and wall gates.
4. A two-class scheduler could prioritize tiny work but would change
   cross-class same-stream outcomes, require a substantially more complex
   explicit waiter scheduler, risk bulk and Group-barrier regressions, and was
   unlikely to remove enough total work to recover the absolute wall and
   throughput deficits.

Therefore none of the strict, early-ticket, late-ticket, or bounded-bypass
policies is accepted for production. The candidate workspace is archival
evidence only. **Production remains unchanged.**

## Follow-up: memory safety without scheduling changes

The remaining memory-safety concern is tracked separately as `bn-1gn1`,
“Bound prepared construction memory without priority scheduling.” It depends
on `bn-2yye` so the deployed owned-append path, not borrowed-only scaffolding,
is measured.

That future work must preserve production admission/order behavior and
same-stream winner semantics; introduce no priority queue or size-class
reordering; account conservatively for `PreparedBatch` vectors, names, slots,
subframes, and hash scratch; release exact permits on cancellation and close;
cover owned and borrowed submissions; and pass paired Process/Group throughput,
p99, barrier-parity, and bounded-high-water gates. Failure again means decline
with production unchanged.
