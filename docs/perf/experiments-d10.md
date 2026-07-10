# D10 benchmark-gated experiments — bn-1hl

Doc 12 D10 ("Learned indexes: demoted, not deleted") says accelerators are admitted
only by evidence: `stream-name / registry string directories: MAYBE — benchmark`. This
bone runs those two deferred experiments as **spikes** (following the existing
`spikes/*` convention — see `spikes/README.md`): full methodology and raw numbers live
in each spike's own `REPORT.md`; this doc is the cross-referenced summary the D10 rule
asks for ("each experiment has a ledger entry with verdict"). Nothing in any production
crate (`mess-log`, `mess-index`, `mess-store`, `mess-cli`, `mess`) was touched by either
experiment — both are pure measurement, run from standalone spike crates that opt out
of the repo's `[workspace]`.

## Experiment 1 — sealed stream-directory lookup

**Spike:** `spikes/perfect_hash_directory/` (full methodology, all numbers, source in
`spikes/perfect_hash_directory/REPORT.md` and `src/`).

**Question:** does a hand-rolled perfect hash (or a sorted-array + binary-search
baseline) beat the `HashMap<u64, DirEntry>` the sealed segment sidecar
(`crates/mess-index/src/sealed/segment.rs`) already uses for the directory lookup that
sits behind the `BinaryFuse16` membership pre-filter
(`crates/mess-index/src/sealed/filter.rs`)? The bone also asked for a comparison
against `fjall` (mess-index's `MetaStore` on-disk KV backend,
`crates/mess-index/src/meta/mod.rs`) as the named "current ... fjall lookup path".

**Method:** four structures (`HashMap`, sorted `Vec` + `binary_search_by_key`, a
hand-rolled fixed-seed FKS two-level perfect hash, `fjall` on real fs) over 1,000 /
100,000 / 1,000,000 pseudo-random `u64` stream ids, 200,000 hit + 200,000 miss probes
each, measuring build time, per-lookup latency, and memory/disk bytes-per-key. Run:
`TMPDIR=$HOME/.cache/mess-bench-scratch cargo run --release` from the spike directory.

**Numbers (this host, 2026-07-10, release):**

| n | structure | hit (ns) | miss (ns) | bytes/key |
|---|---|---|---|---|
| 1,000,000 | **hashmap (current)** | **62.4** | 16.4 | **60.6** |
| 1,000,000 | sorted + binary search | 381.3 | 310.0 | 32.0 |
| 1,000,000 | hand-rolled MPH | 77.4 | 50.4 | 80.0 |
| 1,000,000 | fjall (real fs) | 1,865.2 | 570.1 | 107.4 |

(1k/100k rows and full build-time/on-disk-size numbers in the spike's `REPORT.md`.)

**Verdict: DECLINE.** The current `HashMap` beats the hand-rolled perfect hash on both
latency and memory at every tested size — the MPH's O(1) worst-case guarantee does not
beat `hashbrown`'s SIMD-probed open addressing in practice, and its FKS-style
slot-table blowup (measured 1.96-2.00x the key count) costs more memory than
`HashMap`'s load-factor-bounded overhead. Sorted-array binary search is more
memory-compact (32 B/key flat) but 1.7-6x slower at every size — a real trade, not
enough to displace `HashMap` for this lookup-dominated path. `fjall` costs 30-280x more
per lookup (expected — it is doing a fundamentally different, durability-focused job,
and already is the right backend for the piece of the system that needs it, the
registry `MetaStore`). **No change to `crates/mess-index/src/sealed/segment.rs`.**

## Experiment 2 — O_DSYNC vs fdatasync capability probe

**Spike:** `spikes/odsync_probe/` (full methodology, all numbers, source in
`spikes/odsync_probe/REPORT.md` and `src/`).

**Question:** does round 4's claimed 2.3-4.4x coalesced-`O_DSYNC`-over-`fdatasync` win
(`notes/mess-research/16_spike_results_round4.md`, H4) reproduce on this device at
group-commit payload sizes (4-64 KiB), including an `O_DSYNC+O_DIRECT` variant where
alignment permits? And: design (code, not wire) a startup device-capability probe that
detects devices where `O_DSYNC` is a no-op ("lying"), gated behind an explicit opt-in.

**Method (part 1 — reproduction):** a serial single-writer probe (one durability
barrier per call — the shape a coalescing committer thread issues per commit group),
150 samples x 3 interleaved reps per (payload size, mode) point, 1.5 s settle pauses
(round 4's drift-control recipe), real fs under `$HOME/.cache/mess-bench-scratch`
(never `/tmp`, which is tmpfs on this host). Run:
`TMPDIR=$HOME/.cache/mess-bench-scratch cargo run --release -- bench`.

**Numbers (this host, 2026-07-10, release, all three modes at all four sizes, p50):**

| size | fdatasync | odsync | odsync+odirect |
|---|---|---|---|
| 4 KiB | 11,622 us | 11,322 us | 11,406 us |
| 16 KiB | 11,519 us | 11,777 us | 11,568 us |
| 32 KiB | 11,963 us | 11,583 us | 11,710 us |
| 64 KiB | 12,252 us | 12,105 us | 12,599 us |

All three modes within ~2-6% of each other at every size — no meaningful separation.

**Verdict: REFUTED on this run — confounded, not contradicted.** This run's host was
materially contended (load average 7.89/7.23/8.06 on 24 threads, 22 GiB of 30 GiB swap
in use from unrelated processes) — a wholly different system state from round 4's quiet
machine. Cross-check: `spikes/perf_group_commit`'s own unmodified round-4 harness,
re-run right now on this same contended host, shows the identical collapse (256B
`fdatasync` p50 4.818 ms vs `O_DSYNC` p50 4.747 ms — no win; at 1 MiB `O_DSYNC` was
measured *slower*), confirming this is a property of today's host state, not a bug in
the new probe. The underlying FUA-vs-FLUSH mechanism round 4 documented is not
contradicted (this spike does not re-derive it) but its magnitude on THIS device
depends on host contention the original 2.3-4.4x number did not disclose measuring
against. **Open follow-up:** re-run `spikes/odsync_probe -- bench` on a quiet host to
get a clean number; neither the original 2.3-4.4x nor this run's ~1.0x should be
treated as the settled figure for this device.

**Part 2 — capability probe design (code, not wire):** `spikes/odsync_probe/src/probe.rs`
implements `ProbeConfig` / `run_probe` / `Capability`: measures the `fdatasync` floor
and `O_DSYNC` latency at the store's payload size, then runs a same-size
`write+fdatasync` vs `O_DSYNC` bandwidth "trap" (a large write done both ways — NOT
derived from the small-payload latency, which is barrier-dominated and would be a
category error as a bandwidth ceiling; an earlier draft made exactly that mistake and
flagged every `O_DSYNC` result as "lying" from the wrong denominator, documented in the
module's doc comment as a cautionary note). A same-size `O_DSYNC` write completing
&gt;4x faster than the `fdatasync` equivalent is treated as a lying-device signal and
forces `UseFdatasync` regardless of latency. `ProbeConfig::enabled` defaults to
`false`; even a `UseOdsync` verdict is documented as requiring the caller's own
persistent opt-in as a second, independent gate — D10's "explicit opt-in config"
kept separate from "the probe said yes". **FUA semantics caveat** (from round 4,
restated in the module docs): `O_DSYNC` asks for the device's FUA write path, but
FUA correctness is a per-device trust question — ext4 journal fallbacks and some
virtualized/misconfigured block stacks can silently turn `O_DSYNC` into a
flush-equivalent (safe, no speedup) or, in the worst case, into a no-op (unsafe); the
probe's bandwidth trap is a plausibility gate against the latter, not a durability
proof — only a real crash harness (`spikes/perf_group_commit`'s `crash` mode) builds
that evidence, and this module's docs say so explicitly. On this run, the probe demo
correctly declined `O_DSYNC` (`speedup=1.00x`, `lying_signal=false`) — the conservative,
correct call given the measured ~1.0x win on this contended host.

**Not wired into production.** `crates/mess-store` was not touched; if ever adopted,
the probe belongs in the engine-open path behind the opt-in described above — a
separate, evidence-gated decision per D10, not a consequence of writing this design.

## Summary

| experiment | verdict | production change |
|---|---|---|
| perfect-hash directory | **decline** — current `HashMap` already wins on speed and memory | none |
| O_DSYNC capability probe | **refuted on this run** (host-contention-confounded); probe design delivered as code, not wired | none |

Both spikes are runnable via the commands above; `cargo clippy --all-targets -- -D
warnings` is clean in both (`spikes/perfect_hash_directory`, `spikes/odsync_probe`).
