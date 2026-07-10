# odsync_probe spike — bn-1hl experiment 2

O_DSYNC vs fdatasync on THIS device, plus a startup device-capability probe design
(code, not wire — see `src/probe.rs`). Round 4
(`notes/mess-research/16_spike_results_round4.md`, H4) claimed coalesced O_DSYNC
group writes beat write+fdatasync 2.3-4.4x under 512-writer concurrent load, and
flagged FUA correctness as a per-device trust question needing a startup probe. This
spike is that probe's design plus a fresh single-writer reproduction attempt.

This file is a project deliverable (spike convention: every `spikes/*` directory carries
a verdict writeup, per `spikes/README.md`), not a conversational summary.

## 1. Group-commit-shaped write latency (`src/bench.rs`)

### Method

Serial single-writer probe: **one durability barrier per call**, group-sized payloads
(4/16/32/64 KiB) — matching what round 4's chosen committer architecture issues per
commit group ("single committer with ONE coalesced write per group"). This reproduces
round 4's own baseline-section methodology (§2: "O_DSYNC write (1 MiB)" vs
"write+fdatasync (1 MiB)", a per-call latency comparison), **not** the 512-concurrent-
writer throughput number (round 4 §6/H4) — that number's 2.3-4.4x also folds in a
kernel-mediated-flush-coalescing effect this serial probe cannot exercise by
construction. Both claims are distinguished explicitly here rather than conflated.

Three modes per payload size:
- `fdatasync`: buffered `write_all` + `File::sync_data()` (current default).
- `odsync`: `O_DSYNC`-opened file, buffered `write_all` (the write IS the barrier).
- `odsync+odirect`: `O_DSYNC|O_DIRECT`, a 4 KiB-page-aligned buffer written via a raw
  `write(2)` (`std::fs::File::write_all` doesn't guarantee alignment, which `O_DIRECT`
  requires). All four sizes are 4 KiB-aligned and written sequentially from file
  offset 0, so file-offset alignment holds throughout — "if alignment permits" does
  permit here.

Drift control, following round 4's hard-won lesson (this device's fdatasync degrades
~50x under sustained load and needs minutes to recover): 3 interleaved reps x 150
samples per (size, mode) point, a 1.5 s idle settle before every point, and every rep
runs all three modes back-to-back per size before advancing — no mode systematically
lands on a fresher or more worn device state than another.

Run: `TMPDIR=$HOME/.cache/mess-bench-scratch cargo run --release -- bench` (or `all`,
which also runs the probe demo below). **Never point `TMPDIR` at `/tmp`** — it is
tmpfs on this host and `fdatasync`/`O_DSYNC` are meaningless there.

### Results (this host, 2026-07-10)

Machine/device: same as round 4 (`nvme0n1p3`, Samsung 970 EVO Plus 2TB, ext4), now 86%
full (was 95% in round 4). **Measurement environment differs materially from round 4**:
`uptime` showed load average 7.89/7.23/8.06 (24 threads) and `free -h` showed 22 GiB of
30 GiB swap in use during this run — an unrelated set of other processes (browser tabs,
`rust-analyzer` x2, a concurrent `cbmc`/Kani proof run, other agent sessions) were
competing for CPU and memory on this shared, interactive machine. This is disclosed
because it directly explains the result below, not because it was controllable within
this bone's scope — the probe was run as-is on the machine as found, matching how a
real capability probe would have to cope with a busy host anyway.

| size | mode | p50 (us) | p95 (us) | p99 (us) | mean (us) |
|---|---|---|---|---|---|
| 4096 | fdatasync | 11622.2 | 13727.9 | 18085.1 | 10296.7 |
| 4096 | odsync | 11322.1 | 12944.7 | 18376.8 | 10083.9 |
| 4096 | odsync+odirect | 11406.2 | 13020.4 | 16024.7 | 10133.4 |
| 16384 | fdatasync | 11518.5 | 13050.5 | 18736.9 | 10416.6 |
| 16384 | odsync | 11777.1 | 15062.4 | 20390.6 | 11418.1 |
| 16384 | odsync+odirect | 11568.3 | 13319.6 | 21737.4 | 10913.6 |
| 32768 | fdatasync | 11962.7 | 19930.5 | 41416.0 | 12399.9 |
| 32768 | odsync | 11582.7 | 14696.4 | 18708.5 | 10560.9 |
| 32768 | odsync+odirect | 11709.7 | 13905.0 | 16878.7 | 10370.9 |
| 65536 | fdatasync | 12251.5 | 16952.1 | 23289.8 | 11957.5 |
| 65536 | odsync | 12104.6 | 15739.2 | 18698.2 | 11453.2 |
| 65536 | odsync+odirect | 12598.6 | 16348.1 | 20820.5 | 12488.3 |

All three modes land within ~2-6% of each other at every payload size — no
meaningful separation, well inside noise.

**Cross-check against a bug in this new code**: `spikes/perf_group_commit`'s own
existing `baseline` mode (unmodified, written in round 4, previously measured
fdatasync p50 ~3.3-5.6 ms / O_DSYNC p50 2.57 ms — a 1.3x win) was re-run on this same
contended host and now reproduces the same collapse: `fdatasync (256B dirty)` p50
4.818 ms vs `O_DSYNC write (256B)` p50 4.747 ms (no win), and at 1 MiB `O_DSYNC` was
measured *slower* than `write+fdatasync` (6.844 ms vs 5.129 ms p50) — the opposite of
round 4's finding. Since this is round 4's own unmodified harness, the collapse is
confirmed to be a property of **today's contended host state**, not a bug in this
spike's new probe code.

### Verdict: REFUTED on this run (inconclusive — confounded, not contradicted)

The round-4 claim (2.3-4.4x under concurrent load) is **not reproduced** by this
serial single-writer probe on this run, and round 4's own harness, re-run unmodified
right now, shows the same collapse the new probe shows. The most defensible reading:
under heavy CPU/memory contention from unrelated processes, whatever latency
differences a barrier's underlying I/O path has become small relative to
scheduling/allocation jitter this host is currently under — the write path is no
longer the bottleneck the round-4 measurement isolated. This is a genuine, disclosed
confound, not a refutation of the underlying FUA-vs-FLUSH mechanism round 4
documented (see round 4 §6, H4, for the mechanism argument, which this spike does not
re-derive). **Re-running this probe on a quiet host is the follow-up this bone leaves
open** — do not treat either the original 2.3-4.4x or this run's ~1.0x as the settled
number for this device; both are real measurements of different (and very different)
system states on the same hardware. No production code change is indicated by
*either* run: D10's rule is "admitted only by evidence," and the evidence gathered so
far is contested by environment, which is itself evidence that any O_DSYNC adoption
must be gated on a per-host, per-runtime-conditions capability probe rather than a
one-time historical benchmark — which motivates part 2.

## 2. Startup capability probe design (`src/probe.rs`)

**Design sketch in code, not wired into any production crate.** If ever adopted, it
belongs in `mess-store`'s engine-open path, behind `ProbeConfig::enabled` (default
`false`) — nothing in mess-store, mess-log, or mess-index was touched by this bone.

### What it does

`run_probe(path, cfg)` runs three real-fs measurements against a scratch file **in the
store's own data directory** (the only filesystem whose behavior matters):

1. **fdatasync floor** — `write+fdatasync` latency distribution at the store's
   group-commit payload size (the trusted baseline; this is what mess already ships).
2. **O_DSYNC latency** — same payload size, `O_DSYNC` mode.
3. **Bandwidth sanity trap** — a larger (`trap_size`, default 4 MiB) write done BOTH
   ways, `write+fdatasync` and `O_DSYNC`, and their implied throughputs compared. This
   is deliberately NOT derived from the small-payload measurement in (1): at
   group-commit sizes, fdatasync latency is barrier-dominated, not
   bandwidth-dominated ("durability is priced per barrier, not per byte, until
   bandwidth binds" — round 4), so `payload_size / latency` at that size is not a
   bandwidth number at all. An earlier draft of this probe made exactly that category
   error (see the git history of `src/probe.rs` in this workspace, and the doc
   comment's explicit callout) — it flagged every `O_DSYNC` result as "lying" on this
   host with a 133x trap ratio, purely from the wrong denominator. Comparing two
   same-size large writes cancels the fixed per-barrier cost from both sides and
   isolates the real question: is `O_DSYNC` completing a multi-megabyte "durable"
   write in dramatically less time than an equally-sized write the kernel's
   universally-honored `fdatasync` just took to actually reach stable media? A ratio
   past `lying_multiplier` (default 4x, round 4's own observed FUA-vs-FLUSH ceiling
   under real concurrent load) refuses `O_DSYNC` regardless of its latency win.

Verdict logic (`Capability`):
- `UseFdatasync { reason }` — default and safe: no opt-in, probe error/timeout,
  `O_DSYNC`/`O_DIRECT` unsupported on this filesystem (common on overlayfs, some
  network filesystems, tmpfs), no meaningful speedup, or a lying-device signal.
- `UseOdsync { speedup }` — only when the opt-in is set, the probe completed cleanly,
  the speedup exceeds `min_speedup` (default 1.15x — a serial probe cannot see the
  concurrency-driven part of round 4's win, so this bar is deliberately much lower
  than 2.3x), and the bandwidth trap found no lying signal.

Even a `UseOdsync` recommendation is **not self-executing**: the module docs are
explicit that a real integration must additionally require the caller's own
persistent opt-in config, so an operator can force the conservative default even on a
device the probe likes — D10's "explicit opt-in config" kept as a second, independent
gate from "the probe said yes."

### What it deliberately does NOT prove

Latency/bandwidth heuristics cannot *prove* power-loss durability — only a real
SIGKILL/power-cut crash harness (`spikes/perf_group_commit`'s `crash` mode) builds
that evidence, and even that cannot rule out every device lying about a write cache
with no battery/capacitor backup. This is a **plausibility gate**, documented as such
in `src/probe.rs`'s module docs, meant to be combined with — not replace — periodic
crash-harness verification in a real deployment.

### Demo run (this host, this run — `cargo run --release -- probe`)

```text
probe: fdatasync p50=13221.0us  odsync p50=13231.4us  speedup=1.00x  trap_bw_ratio=0.88x  lying_signal=false
verdict: UseFdatasync { reason: "O_DSYNC showed no meaningful latency win over fdatasync on this device" }
```

Consistent with part 1: on this contended host, the probe correctly declines
`O_DSYNC` (no false "lying" flag after the bandwidth-trap fix, and a correctly
conservative `UseFdatasync` given the measured ~1.0x speedup). This is the probe
behaving exactly as designed — on a device/host state where O_DSYNC shows no real
win, it should not recommend switching, independent of what round 4 measured on a
quiet host.

## Tests and lint

`cargo test --release` (2 tests: `disabled_by_default_returns_none`,
`probe_runs_and_is_self_consistent_on_real_fs`) and
`cargo clippy --all-targets -- -D warnings` both pass clean in this spike.
