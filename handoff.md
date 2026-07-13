# Handoff — Asterism implementation program

**Written:** 2026-07-13 by the outgoing lead agent.
**For:** the next lead agent.
**Read this first, then `bn show bn-agy` (the goal bone — its comments are the running decision log).**

---

## 1. Where we are in one paragraph

The **Asterism spike program is complete** — 12 of 12 spikes done, zero kill criteria tripped, all merged (goal `bn-3fn`, closed). It proved a log-derived state kernel can replace Fjall, and produced a final go/no-go (Spike J): **PROCEED, NARROWED**. The **implementation program** (goal `bn-agy`) is now underway. **Step 1 is merged** (`05af6861`): stream/type names now live in the log, and **Fjall no longer holds anything authoritative** — every remaining Fjall keyspace is a derived cache with a spike-proven replacement. **Step 2 (the flat-combined append owner, `bn-2su`) is the next task and has not been started** — its worker died on a usage limit before writing a line. Everything else is follow-up.

---

## 2. Immediate next action

**Dispatch Step 2: the flat-combined owner (`bn-2su`).**

State: bone is `doing`, claim staked by `mess-dev`, workspace `.maw/workspaces/bn-2su` exists and is **empty/clean**. Either reuse it or destroy and recreate. Nothing is lost.

The brief I would give (it is the same one whose worker died — reuse it):

> Replace the composed engine's append orchestration with ONE owner that validates, assigns positions, encodes, writes, barriers, applies, and publishes. Prototyped and proven in Spike B (`spikes/flat_combined_append/REPORT.md`) — beats the current engine in **all 32 measured cells**.

**Three hard constraints (measured; violating any of them throws away the win):**

1. **The owner MUST BE the committer** — it performs the write and the `fdatasync` itself. An owner that *fronts* the existing committer thread measured **−35%**.
2. **Rebuild the committer's D7 early-close inside the owner** (target width + in-flight-producer gate + ~200 µs grace). Without it the commit convoy splits into ~2× fsyncs — which is exactly the pre-existing defect **`bn-3pz`**, so doing this correctly **fixes `bn-3pz` for free**. Verify it does and report fsync counts.
3. **Do NOT implement B1 pipelining** (validate/encode group N+1 during group N's barrier). Measured: no win in Process, **5–18% LOSS** in durable mode at 64 writers. Reconfirmed the `perf_group_commit` H2a result twice. Keeping it out also keeps a nondeterministic barrier cut out of the fault model.

**It replaces** (all in `crates/mess-store/src/engine.rs`): the 256-shard `AppendGate`, both per-append `spawn_blocking` tasks, the `PublishSequencer` (mutex+condvar), and the post-commit Fjall `CommitGroup` head write.

**It MUST preserve** — these are load-bearing and were earned the hard way:
- **The registry unit** (just landed — read `git show 05af6861` first): a `$registry` batch and the domain batch that first uses its ids are ONE ordered unit that fails atomically (`UnitTag`/`failed_units` in `committer.rs`) and must **publish everything that landed before surfacing any error** (a committed-but-never-published position stalls every higher position forever — the `bn-3nz` hazard). Ids are **staged**: only committed to the `Book` after the record is irrevocably queued.
- **Cancellation**: dropping the caller's future must never prevent a committed batch from publishing (today guaranteed by the non-cancellable `spawn_blocking`; the owner must give the same guarantee).
- Dense global positions *within the log*, exactly-one-winner on same-stream `Exact(v)` races, conflict/empty-append API semantics, EIO/poison (D8) stickiness.

**Bar:** full `just test`; `cargo check --workspace --all-targets`; differential oracle vs the pre-change engine; matched benchmark using `spikes/baseline_matrix/` (baseline-gen2: batch 1/10/100/1000 × 24 B/250 B × 1/4 writers × Process/Group, interleaved ABBA, median-of-3, quiet-guarded). **Adversarial review is mandatory before merge.**

---

## 3. The one process rule that matters most

**Every production change gets an adversarial review before merge, from a separate agent, told to REFUTE merge-readiness — not to "review".**

This is not ceremony. In this program it caught, in production code that passed its author's full test suite:

| What review caught | Severity |
|---|---|
| A reader panic that poisoned the engine-wide mutex, killing the store (Spike C) | blocking |
| A recovery path that trusted an index covering data that never reached disk (Spike C) | blocking |
| A corruption test that "passed" only because it recomputed the checksum after corrupting the byte (Spike I) | bogus gate |
| **Permanent store corruption from the public API, no crash, no fault injection** (Step 1) | **REJECT** |

That last one: an oversized (>64 MiB) batch to a new stream minted ids into memory, failed, and never rolled back — the *next* append saw them as registered, emitted no registry record, and committed events referencing ids that existed nowhere. Store unopenable forever, with a successfully-acked event inside it. **Reproduced from the public API.** The fix was structural (stage ids; commit them only after the record is irrevocably queued), and the same reviewer then re-verified against its own repros.

Practical notes: reuse **the same reviewer agent** for re-checks after fixes (it holds its own reproductions). Give reviewers a concrete attack list, not "please review". Expect FIX-FIRST or REJECT on first pass for anything touching durability or recovery — that has been the norm, not the exception.

---

## 4. What is done (and where the evidence lives)

All spike code and REPORTs are merged under `spikes/`. Each REPORT has method, machine info, Measured/Derived labels, gates, and a verdict.

| Spike | Verdict | Evidence |
|---|---|---|
| 0 baseline | baseline-gen2 locked; **gate per batch size, never one headline** | `spikes/baseline_matrix/` |
| A dense heads | Direct pages win: 46× fjall, 2.6× HashMap, 16.02 B/stream | `spikes/state_kernel_dense_heads/` |
| B flat owner | NARROW: beats engine everywhere; **owner must BE committer**; B1 dead | `spikes/flat_combined_append/` |
| C no-Book | **MERGED INTO ENGINE.** RSS 16–25% of baseline, zero open decodes, hot loads 2× faster | `spikes/open_without_book/` |
| D effects+checkpoint | PROCEED: 100k/100k digests identical, open 4.9% of full scan, 24.3M transitions/s | `spikes/segment_effect/` |
| E v4 capsules | **MERGED, flag-OFF.** 96k crash states + 24k torn + 30M fuzz, zero violations | `spikes/capsule_v4_prelude/` |
| F microblocks | ADOPT-F3 stream side; **stride-8 global REFUTED** (use stride-1) | `spikes/active_microblocks/` |
| G epoch dedupe | ADOPT G2: exact under forced collisions, 12.6× on window misses, zero deletes | `spikes/epoch_dedupe/` |
| H directory | ADMIT bitvector+rank (real segments are all dense); PtrHash disqualified | `spikes/directory_tournament/` |
| I SealPack | **MERGED, flag-OFF** (`EngineOptions::seal_pack`) | `spikes/seal_pack/` |
| J composed | **PROCEED NARROWED** — the decision | `spikes/composed_decision/` |
| Step 1 registry | **MERGED** (`05af6861`) — Fjall now holds nothing authoritative | — |

---

## 5. Spike J's decision (this is the plan; don't re-litigate it)

- **(a) Integrate the flat owner: YES**, highest priority. → Step 2, `bn-2su`.
- **(b) Retire Fjall: NOT YET** → *this is now unblocked.* Step 1 removed the sole blocker (names). All remaining keyspaces (`stream_heads`, `snapshot_heads`, `checkpoints`, `dedupe`, `dedupe_order`, `hw`) are **derived caches**. Deleting Fjall is now mechanical, not dangerous.
- **(c) v4 / migration: STOP for now.** The v3-compatible subset is the right stopping point. v4 is merged, proven, and flag-off — available the day there's a reason. **See §7: the reason may now exist.**

**Implementation order (from Spike J, with Step 1 done):**
1. ~~log-derived `$registry`~~ ✅ **DONE** (`bn-2di`)
2. **flat owner** ← **YOU ARE HERE** (`bn-2su`)
3. dense heads (A) + epoch dedupe (G) — replaces `stream_heads`, `dedupe`
4. microblocks (F) **stream side only, stride-1 global**
5. segment effects + checkpoints (D)
6. bitrank sealed directory (H)
7. SealPack RSS fix (`bn-dbz`) + footer identity binding (`bn-11g`) **before** `seal_pack` may default ON
8. v4: not now (but see §7)

**The known ceiling — do not be surprised by it:** the literal "≥85% of bare log" gate **fails below batch 1000 for BOTH engines**. It prices *the async API*, not the orchestration. ~69% of cycles are producer-side (tokio wakeups + record allocation; two thread wakes per append). Spike J bounded the prize precisely: **dropping the tokio hop + record copy takes the flat owner from 53.8% → 83.4% of bare.** That is the single largest lever remaining after the current work — an owned-record / interned-type append API. Note `mess-log` itself is runtime-agnostic (its own `Runtime` trait, `RealRuntime`/`SimRuntime`); tokio lives only above it, in `mess-store` and up.

---

## 6. Design-pack amendments the spikes produced

The `notes/mess-asterism/` pack is **not** fully updated with these. They live in `bn-3fn`'s comments. If you revise the pack, these are the corrections:

- **Frontier semilattice needs an explicit bottom** (or entry *creation* must count as dirtying). A `ProjectionCheckpoint` at position 0 creates a digest-visible entry with no value change — invisible to latest-value dirty tracking, so an incremental checkpoint reuses a stale blob. Found by the corpus at ~4/100k. General rule: **in any latest-value component, "key now exists" is invisible to value-comparison dirty tracking.**
- **Parallelize the effect BUILD, apply sequentially.** Full ordered tree composition materializes intermediate maps (7.0 s vs 0.3 s at 1 thread). Build parallelizes 6.8× at 8 threads; apply runs at 24.3M transitions/s sequentially.
- **Allocator components need first/last boundaries** so monotonicity survives composition (design §9.7).
- **The owner must be the committer** (design §6.3 as literally written). Fronting the existing committer: −35%.
- **B1 pipelining is dead.** Measured twice.
- **The v4 spec must mandate the no-alloc validate/materialize split.** A naive materialize-everything `decode_capsule` costs **+24.5%/byte** over v3; splitting it into an allocation-free `validate_capsule` for the scanner hot path brings it to **+0.47%**. Without this the <2% scan gate is unreachable.
- **A5 framing (design §1.1 is wrong as written):** v4 does not "preserve all A1–A12". Say: *"preserves A1–A4 and A6–A12; replaces A5 with a safety-preserving nonempty-capsule rule (`control_count + event_count >= 1`) and promotes contiguous `batch_id` to recovery-significant."*
- **Gate per batch size, never on one headline ratio** (the composed/bare ratio is fixed-overhead-bound and rises monotonically with batch size).
- **research/06 (migration plan) is MOOT** — see §7. Bone `bn-3m3` tracks annotating it.

---

## 7. IMPORTANT — a mid-flight scope change you must know about

**Bob confirmed (2026-07-13): mess has NO users and NO existing stores. Early development.**

Consequences, already actioned:
- The **entire migration program is moot** — research/06's M0–M9 phased cutover, shadow compares, dedupe/snapshot import, rollback matrix, `mess migrate` CLI. None of it will be built. Risk R3 ("authoritative name mappings lost during migration") drops out entirely. Bone `bn-3m3` tracks annotating the doc (keep it — its analysis of *why* names were the sole non-rebuildable authority is what produced Step 1).
- **Fjall retirement is a straight deletion, not a migration.**
- **This weakens the main argument for holding v4.** Spike J's caution was substantially about migration danger. With no stores to migrate, v4 can be judged on its own merits — and there is now a *positive* reason to want it (below). I did not re-open that decision; the v3 registry path works and I did not want to churn it. **But it is a live question for you.**

**The positive reason for v4 — a real semantic cost of the v3 registry path (bone `bn-25c`):**
`$registry` batches are real v3 event batches, so **they consume global positions**. The engine filters stream 0 out of `read_global` (both tiers), so users never *see* registry events — but the `global_position` values on the user events they *do* see now have **gaps**. **Global positions are no longer dense over user events.** This already broke four test suites whose oracles assumed "position N == the Nth user event" (fixed in `bn-2di`). Any external cursor or projection assuming density will break the same way.

This was an explicit, anticipated trade (research/06 M4 option 1; design §21.7: *"accept the extra global positions rather than weakening recovery"*). **But v4's control-only capsules do not consume domain positions — by design (§5.2). That is precisely the problem they were invented to solve.** So: v3 registry works and is merged; v4 would restore dense user positions. Worth deciding deliberately once the state kernel lands.

---

## 8. Open bones, triaged

**Blocks flag-default-ON (do before turning `seal_pack` on):**
- `bn-dbz` — SealPack costs **+18%/+36% reopen peak RSS** (2M/10M). Erodes the exact bounded-RSS win Spike C bought. Likely: the pack is materialized whole into RAM at open; leave `PAYLOAD_COLUMNS` on the pread/bounded-cache path.
- `bn-11g` — footer does not name the pack hash (deviates from research/04 §6.2 trust chain). Accepted for the flag-off period only.

**Real bugs / gaps:**
- `bn-3pz` — Group-mode convoy split. **CONFIRMED but MISATTRIBUTED** (bisect showed it pre-dates the recent merges; present at Spike B's own base commit). **Step 2's early-close fixes it for free** — do not fix separately; close it out as part of `bn-2su`.
- `bn-30u` — refuted sidecar candidates are re-evaluated every reopen; rolled-but-refuted segments are never re-queued for re-seal.
- `bn-2r8` — v4 follow-ups (SIGKILL scenario when a v4 committer exists; v3-binary open-time refusal; `marker_flags == 0` read check; validate-vs-decode differential fuzz).

**Free wins:**
- `bn-dcr` — swap the sealed directory HashMap to **foldhash**: +53% median real-segment batch lookup, one line. Independent of whether bitrank ships.

**Docs:**
- `bn-25c` — document that global positions are no longer dense over user events (§7 above). **Do this one; it is API-visible.**
- `bn-3m3` — annotate research/06 as moot.

**Flaky tests — these actively erode signal; three separate reviewers have burned time proving they aren't regressions:**
- `bn-2nd` — `fold_chain_overhead::chain_append_overhead_is_in_envelope` (fails under parallel load; passes 3/3 in isolation at 0.26 s). **Ignore it in suite runs; do not chase it.**
- `bn-31n` — `engine_publish_cancel` (same class).
- `bn-2rk` — `engine_append_gate` (same class). *Note: this one may be deleted outright by Step 2, which removes the AppendGate.*

**Process:**
- `bn-1qa` — **worker verification must run `cargo check --workspace --all-targets`**, not just `-p <touched crates>`. A merge broke the build because `SealInput` gained a field and two call sites (`mess-bench`, `mess-cli`) sat outside the verified scope. I hotfixed it (`16243284`). **Enforce this on every worker brief.**
- `bn-tkd` — tracking a bones projection glitch filed upstream (`bn-2nxw` in `~/src/bones`): created items went briefly unfindable mid-batch; workaround is to re-create (same IDs come back).

---

## 9. Operational notes that will save you time

- **Models:** Bob asked for **Opus workers** (Fable got expensive). Lead stays on the session model. Reviewers on Opus have been excellent.
- **Workers die on usage limits mid-task.** It has happened four times. Their work is usually *committed or at least written* in the workspace — **always inspect `.maw/workspaces/<bone>/` (`git log`, `git status`, `git diff`) before re-spawning.** Twice I finished the job myself from what was already on disk rather than burning a new worker.
- **Bench hygiene is real on this host.** Ambient load is ~4–5 from unrelated processes, so a fixed "load < 6" quiet-guard is unusable. Have workers **record the load1 each measured run executed under** rather than trusting a floor. Concurrent spikes must stagger measured phases — and beware `pgrep -f <name>` **self-matching** the guard's own shell (it deadlocked one spike for an hour).
- **Merging:** `.bones/events/*.events` conflicts on nearly every merge (append-only). Resolve with `maw ws resolve <ws> --keep union`. If merge says the epoch is stale, `maw epoch sync` then `maw ws sync <ws>`.
- **`just test` is long** (crash harnesses). `snapshot_law` alone is ~3000 fsync-bound iterations; the documented override is `MESS_SNAPSHOT_LAW_ITERS=25`.
- **Do not chase the three flaky tests.** See §8.

---

## 10. What "the promised land" looks like from here

Fjall holds nothing authoritative *today*. After **Step 2** (flat owner) the append path is one owner, one barrier, no `spawn_blocking`, no publish sequencer, no second database on the write path. After **steps 3–6** the remaining Fjall keyspaces are replaced by the structures the spikes already proved (dense head pages, epoch dedupe, microblocks, segment effects, bitrank directories) and **Fjall is deleted**. What remains after that is the tokio/API overhead (§5) — a known, bounded, measured 54% → 83%-of-bare opportunity.

Nothing in the plan is speculative. Every component has a merged spike with measured gates behind it.

Good luck.
