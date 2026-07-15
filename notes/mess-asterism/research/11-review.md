# Research 11: review — Asterism against the engine as built

**Reviewed:** `design.md` + `research/01`–`10` at the hashes in `MANIFEST.sha256`
**Against:** working tree at `b3455fec` (post bn-1jg seed-profile spike)
**Date:** 2026-07-11
**Revision:** v1.1 — incorporates the corrections from `12-response.md`, each verified against the source before adoption (the invalid C4 ratio, the perf_group_commit H2a pipelining no-win, the Phase-3 empty extension region, and the D2/D4/C1 refinements). Points of disagreement: none remaining.
**Reviewer verdict:** the architecture and the spike-gated plan are sound and worth pursuing. The pack's biggest strength — grounding every mechanism in the actual code — is also where the errors are: several "current engine" claims are wrong or already stale, one baseline the plan measures against does not exist in production, and two internal inconsistencies would confuse whoever turns this into bones. All are fixable without touching the core design. Issues are numbered `V-*` (verification against code), `C-*` (internal consistency), `D-*` (design gaps), `S-*` (suggestions to make it stronger).

---

## 1. Claims verified as correct

Before the issues: the load-bearing characterizations were checked against the source and hold up.

- **Full-scan open, dense all-history Book, no checkpoint** — confirmed verbatim. `LogEngine::recover` enumerates every segment, CRC-scans every batch, and pushes one `Payload` per event into `book.payloads` with a dense-index assert (`crates/mess-store/src/engine.rs:953-1097`, assert at `:1028`). Sealed/cold segments are *also* fully re-materialized into the Book — the `is_cold` gate at `engine.rs:1003` only skips hot-index seeding, not payload residency. Startup is event-count proportional exactly as claimed.
- **The publish-tail machinery** — all five pieces exist as described: 256-shard per-stream `AppendGate` (`engine.rs:284-346`), up to two `spawn_blocking` per append (`engine.rs:1777`, `:1844`), `PublishSequencer` (mutex + condvar, `engine.rs:348-440`), global `Arc<Mutex<Book>>` on both publish and every read (`engine.rs:1890`, `:1600`, `:1662`), Fjall `CommitGroup` after ack (`engine.rs:1921-1932`).
- **The new-name barrier** — confirmed, and now Measured: `persist_new_names` + `spawn_blocking(meta.persist())` with `PersistMode::SyncAll` strictly before the covering append (`engine.rs:1757-1783`). The bn-1jg spike (`spikes/seed_profile/REPORT.md`) measured 3.4344 ms per new-stream append vs 0.0342 ms existing-stream — the fsync is 98.8% of the new-stream path, it serializes all concurrent new-stream appends through one shared persist, and it fires even under `Durability::Process`.
- **56-byte sealed DirEntry rebuilt into a `HashMap` on open** — confirmed (`crates/mess-index/src/sealed/segment.rs:101`, rebuild loop `:477-499`). (Drive-by: the comment at `segment.rs:98-100` still says "48-byte"; worth a one-line fix.)
- **D10 FKS result** — accurately reported: 62.4 ns/60.6 B per key for `HashMap` vs 77.4 ns/80.0 B for the hand-rolled FKS at 1M keys. The pack's framing ("rejects naive FKS, not all static layouts") is fair.
- **The algebra** — right-biased override associativity (§9.3), path-composition of head transitions (§9.2), and the checkpoint theorem (research/02 §11) are correct as stated. The explicit non-commutativity and ordered-tree-reduction constraints are exactly the right guardrails.

---

## 2. V — claims that are wrong or outdated against the code

### V1 (major). Exact dedupe is dormant — the engine never uses it

The pack consistently describes the dedupe machinery as part of the current composed engine: design §0 ("it maintains an exact sliding dedupe window as mutable KV rows plus an order index…"), research/01 §7, Spike G's baseline ("current Fjall primary+order tables"), and kill criterion §21.8 ("exact dedupe is slower than Fjall by >20%").

Reality: the `dedupe`/`dedupe_order` partitions exist in `MetaStore` (`crates/mess-index/src/meta/mod.rs:85-86`, capacity-FIFO eviction at `:321-347`, `DEFAULT_DEDUPE_CAPACITY = 65_536` at `:123`), but **nothing in the append path populates or queries them**. The per-append `CommitGroup` carries `stream_heads` only (`engine.rs:1921-1932`); no caller of `dedupe_lookup` exists outside `MetaStore` itself. Dedupe is a dormant capability, not a production behavior.

Consequences, mostly good ones:

1. **Spike G's "current Fjall dedupe" baseline is synthetic.** It's fine as a component bench, but the §21.8 kill criterion compares against a path with zero production users and zero composed-workload measurements. Reframe it as "epoch dedupe must beat a best-effort Fjall implementation we build for the comparison" — or drop the criterion and gate on the absolute latency/write-amp targets only.
2. **Migration phase M6 can shrink dramatically.** In `LogEngine`-created stores there are no live Fjall dedupe rows to export, no `DedupeWindowImportedV1` seeding, no dual-query soak against a legacy answer. Make M6 **conditional on an inventory check** ("dedupe partitions empty?") in M0 rather than asserting universal emptiness — `MetaStore` is public, so experimental or directly constructed stores may contain rows even though `LogEngine` never creates them. When the check passes, M6 reduces to "dedupe ships as a new Asterism feature," deleting one of the trickiest rollback rows in research/06 §14 and the "dedupe key represented only by Fjall" data-loss trap.
3. **The window semantics are unconstrained.** The dormant implementation is entry-count-bounded (last 65,536 keys); Asterism proposes `WindowByGlobalPosition{span}`. Since nobody depends on the old semantics, the position-span definition can be adopted cleanly — say so, and stop describing the FIFO-capacity behavior as something being *replaced* rather than *superseded before first use*.

### V2 (major). The seal pipeline depends on the Book — Spike C is bigger than written

Research/05 Spike C ("remove the Book") scopes the work as read-path changes plus dropping rehydration. It misses that **sealing consumes the Book**: the roll-sealer waits until `book.payloads.len() >= end` as its readiness gate (`engine.rs:1146-1163`) and then copies the segment's payloads out of the Book in global-position order to build `.pcol` (`with_payloads`, `engine.rs:1180-1198`).

Removing the Book therefore forces a seal-pipeline rework at build-order step 3 (design §23), three steps before the SealPack consolidation at step 6 where you'd naturally touch this code. The fix is straightforward — source seal payloads by re-reading the raw sealed segment (verify-on-seal already proves byte-exact reassembly against raw bytes, so the read path exists) and gate readiness on the canonical published/durable watermark instead of Book length — but it must be *in* Spike C's scope and gates, not discovered mid-implementation. The correctness gate should be **semantic, not compressed-byte identity** (compression output may legitimately change with encoder/library version or tuning):

```text
reassembled event payloads are byte-identical
pointer/version/global-position results are identical
the new pack validates and falls back correctly on corruption
```

with byte-identical `.pcol` output retained only as an auxiliary same-build regression check.

### V3 (moderate). The production hot read path doesn't touch the ActiveIndex

Research/01 §5 and design §8.1 motivate microblocks by describing reader/writer contention on the sharded ActiveIndex. In the engine as built, `read_stream`'s hot path slices `book.stream_events[sid]` and `read_global` slices `book.payloads`, both under the **global Book mutex** (`engine.rs:1640-1652`, `:1662-1670`); `head` reads `book.heads` (`engine.rs:1599-1605`). The ActiveIndex is written by the publish step and exercised by tests/benches, but production reads bypass it.

Two implications:

- The current-state description is actually *worse* than the pack says — every read serializes on one global mutex, not on 64 shards. This strengthens the Asterism case; state it.
- Spike F's baseline ("current ActiveIndex read/update rates") is a component bench, not the production read path. The composed comparison for microblocks must be against Book-mutex reads (before Spike C) or block-native reads (after), and the plan should say which.

### V4 (moderate). Snapshot `state_hash`/`event_prefix_hash` don't exist yet — but v4 makes them mandatory

Design §7.3/§15.1 and `SnapshotInstalledV1` (research/09 §13) carry `state_hash: Hash256` and `event_prefix_hash: Hash256` as fixed fields. In the current code these are **reserved and always `None`** — the fold-chain/BLAKE3 machinery is a later phase (`crates/mess-store/src/snapshot.rs:29-38`); only `fold_version` is live. The production snapshot backend stores blobs one-file-per-version under `blobs/<stream>/<version>.blob` with a magic+len+checksum header (`crates/mess-store/src/fjall_snapshot.rs:34`, `:122-129`) and no cryptographic state hash anywhere.

So `SnapshotInstalledV1` as sketched has a hidden dependency on shipping fold-certificate hashing first. Either (a) make the two hash fields explicitly zeroable with a flags bit ("hashes present"), so snapshot migration (M7) doesn't block on Phase-5 fold certs, or (b) add fold-cert hashing as a named prerequisite in the migration plan. (a) is cheaper and consistent with K6 — the hashes are verification aids, not commit authority.

### V5 (moderate). The name-barrier motivation is being eroded right now — re-frame the v4 win

Two bones filed off the bn-1jg spike directly target the barrier the v4 capsule design leads with: **bn-2cj** (gate the name `SyncAll` on the engine's `Durability` mode — under `Process`, buffer instead of fsync) and **bn-34o** (coalesce name persists into the group-commit window, est. 5–20× on stream-creation-heavy loads). Neither has landed — bn-2cj is `doing` as of wave 29 and bn-34o is blocked behind it — but both will likely merge well before any Asterism spike.

After those land, the "two barriers for a new name" cost mostly collapses on the paths people actually measure, and design §2.3's latency framing will read as stale. The v4 capsule's *real* advantages survive untouched and should lead instead: (1) **atomicity** — no name-durable-but-event-absent state, one authority instead of two ordered persistence domains; (2) **mode inheritance** — registration durability automatically matches the append's durability mode, which is precisely the property bn-2cj is hand-patching in; (3) **no serialization point** — registration rides the group commit natively instead of funneling through one shared `MetaStore::persist`. Recommend rewriting §2.3 to lead with atomicity and cite bn-2cj/bn-34o as the interim mitigation whose existence proves the two-domain design is being fought.

Also: re-lock the Spike B baseline *after* those bones merge, or the composed-engine baseline will shift mid-program.

### V6 (minor). An event-sourced registry implementation already exists — use it or supersede it

Research/06 M4 designs registry canonicalization from scratch. But `crates/mess-store/src/registry/{mod,state,codec}.rs` already implement the normative spec-04 registry: writer-assigned dense IDs, a pure `RegistryState` replay machine, categories/streams/event-types/dicts/aliases (`registry/mod.rs:191-263`) — currently referenced only by tests, and the engine explicitly notes the log-carried `$registry` is "not implemented here" (`engine.rs:1458-1462`). The v4 control prelude should either reuse `RegistryState` as the recovery-side registry fold (the control records become an alternative encoding feeding the same state machine) or explicitly declare the module superseded. Silently building a third registry representation would be the worst outcome. This is also a cheap de-risking asset: the replay machine and its tests exist today.

### V7 (minor). Small factual corrections

- "One coalesced write": distinguish current implementation from Asterism target. The **current** committer issues k `pwrite`s + one barrier, per its own deviation note (`crates/mess-log/src/committer.rs:31-39`) — descriptions of the current engine should say so. "One gathered/coalesced write where size permits, with `writev` or bounded chunks for oversized groups" remains a legitimate **Asterism target** (the group-commit spike found actual coalescing to be the useful top-end optimization); just label which is which.
- The engine module doc claiming recovery uses `recover_whole_log` + advisory manifest (`engine.rs:47`) is stale — the code hand-enumerates segments and never implements spec-02's R2 fast path. The research pack describes actual behavior correctly; just don't cite that comment.
- Research/01 §2's keyspace table should mark `dedupe`/`dedupe_order` as *present but unused* (see V1) — as written it implies eight active keyspaces.
- Per-append Fjall work: the group carries `stream_heads` only, journal-buffered with an explicit no-fsync commit (`meta/mod.rs:355`). Design §2.2 gets this right; make sure summaries elsewhere never say "persisted" without the buffered qualifier.

---

## 3. C — internal consistency issues in the pack

### C1. "Asterism does not weaken any A1–A12 rule" contradicts zero-event capsules

Design §1.1 makes this claim flatly, but **A5 is `frame_count >= 1` — no empty batches** (spec 01, header rule at offset 8). v4's control-only capsules with `event_count == 0` are a direct amendment of A5, replaced by the stronger conjunction `control_count + event_count >= 1` plus mandatory batch-ID contiguity. That replacement is well-designed (research/04 §3.3 and the R1 risk entry handle the actual hazard), but the blanket "does not weaken" sentence will be quoted against you in review. Note the amendment is not "strictly stronger" by set inclusion either — v4 accepts control-only capsules v3 rejects. Amend to:

> Asterism preserves A1–A4 and A6–A12. Format v4 replaces A5 with a safety-preserving nonempty-capsule rule, `control_count + event_count >= 1`, and promotes contiguous `batch_id` validation to a recovery-significant rule.

— noting that promoting `batch_id` from informational (D-FMT-5, `01-log-format.md:404-411`) to recovery-significant is likewise a v4-only semantic change to an existing field.

### C2. Spike lettering diverges between design.md §19 and research/05

Design §19: A dense-heads, B flat-owner, C no-Book, D effects, E v4, **F succinct-directory, G dedupe, H seal-pack, I composed** — nine spikes, no microblock spike despite §8 being a major mechanism.
Research/05: A–E identical, then **F microblocks, G dedupe, H directory tournament, I SealPack, J composed** — ten spikes.

The stop/go table in research/05 §16 uses the second scheme. Whoever creates bones from these documents will mislabel something. Unify on the research/05 lettering (it's the complete one), fix design §19, and add microblocks to design §19 explicitly since design §8 depends on it.

### C3. The v3 trailer extension region and R2 manifest are design ancestors the pack never mentions

Spec 01 §3.3.2 defines an optional extension region before the segment trailer that can carry **`StreamHeadTable` (48-byte entries) and `SnapshotAnchor`** records (`01-log-format.md:240-299`), and spec 02 defines the **R2 advisory manifest**. Neither appears anywhere in the pack, and both are conceptual ancestors of `SegmentEffect` and the kernel checkpoint manifest respectively. Two qualifications keep this claim honest:

- A `StreamHeadEntry` is only `(stream_id, last_version, head_hash)` — no incoming version boundary, final global position, registry delta, snapshot transition, checkpoint effects, or dedupe epochs. It is **partial evidence** toward a `SegmentEffect`'s head component, not a proto-effect, and it is specified as durable fold-certificate material, not a rebuildable accelerator.
- The production sealer currently emits a **Phase-3 empty extension region** — the trailer sits directly at `content_len` (`crates/mess-log/src/writer.rs:637-639`, `engine.rs:1620`). No v3 segment in the wild carries a `StreamHeadTable` today, so M5.2's synthetic effects cannot count on one.

Recommended resolution: acknowledge the ancestry; keep `SegmentEffect` in the rebuildable SealPack and fold anchors in the durable extension region; never create two authoritative copies of the same logical field; where a valid `StreamHeadTable` *does* exist, use it as partial evidence, not as a scan waiver. The checkpoint manifest should share R2's anchor/validation concepts, but need not physically replace the segment-catalog manifest if the two have distinct lifetimes and failure domains.

### C4. State the measured composed/bare ratio — but only from matched workloads

Design §18.1 says the composed engine is "below that ceiling" without a number. The envelope has one **valid matched pair**: bare log 2.038M ev/s vs composed 1.710M ev/s at 4 writers × 10-event batches — **~84% of bare log** on that workload. Publishing it anchors the 85% gate against a measurement instead of an abstract ratio.

*Correction from `12-response.md`, verified:* an earlier draft of this review divided the 4.71M bare-log row (4×100 batches, ~250 B payloads) by the 2.96M composed row — but that composed row is **400 sequential 5,000-event single-stream appends at ~24 B payloads** (`docs/perf/envelope.md:108`). Different concurrency, batch size, payload size, and stream distribution; the quotient is meaningless. Do not publish a large-batch ratio, or any diagnosis of where the gap lives, until the pack's baseline lock-in (research/05 §2) produces a matched matrix over batch size × payload × concurrency. Batch-size-stratified gates (S7) remain a good idea — the matched matrix is their prerequisite.

---

## 4. D — design gaps

### D1. The single owner stalls on the durability barrier — add a durable-mode gate to Spike B, and benchmark pipelining as a variant

Design §6 has the owner do validate → encode → write → **barrier** → apply → publish on one OS thread. Under `Os`/`Group` durability the barrier is the dominant term (measured 2.56 ms mean `fdatasync` on the reference box, `envelope.md:46-48`) — and during it the owner does no validation, encoding, or dedupe checks for the next group. The current engine, for all its machinery, *does* overlap: the committer thread syncs while producer tasks validate elsewhere. A flat-combined implementation could **lose** durable-mode throughput while winning Process-mode, and Spike B's gate is Process-only, so the regression wouldn't be caught until Spike J.

The non-negotiable fix is the gate: **add a durable-mode gate to Spike B** (e.g. "Group-mode composed ≥ 95% of current engine Group-mode") so a durable regression dies at the kill point.

The remedy, however, should be measured rather than prescribed. The perf_group_commit spike already implemented "encode/write group N+1 while group N's fsync runs" and found **no distinguishable win** (`spikes/perf_group_commit/REPORT.md`, H2a) — largely because the existing committer already overlaps appender writes with the in-flight fsync. That result isn't conclusive for Asterism (the owner absorbs more validation work than the bare committer), so Spike B should carry two variants:

```text
B0: one owner, inline barrier
B1: one owner, pipelined next-group validation/encoding against speculative state
```

B1 survives only if it measurably improves matched durable throughput or tails. Note its correctness cost too: if group N+1's bytes are written to the same file while group N's `fdatasync` is in flight, the barrier may persist some or all of N+1 — safe under A6, but it makes the exact barrier cut nondeterministic and must be represented in the fault model. Encoding-without-writing avoids that but overlaps only the CPU work H2a found non-bottleneck. Either way, ordering stays assigned by one thread and publish/ack for group N stays strictly after its barrier (K3 intact); poison-on-EIO (K8) composes: barrier failure poisons before any group N+1 ack.

### D2. Name the state frontiers

The pipelined variant above makes explicit what design §6.2 leaves implicit: the kernel has multiple state frontiers, not one overlay. The accurate model needs **four**, because Process mode makes "durable" unknowable at runtime:

```text
speculative:       accepted by validation; may still be discarded on I/O failure
written/accepted:  write returned; ack-eligible under Process
crash-stable:      covered by a successful Os/Group barrier; under Process the exact
                   persisted prefix is unknowable until recovery
published:         reader-visible effects
```

For `Os` and closed `Group`: `published <= crash_stable`, and a `Success` completion implies the capsule is both crash-stable and published (there is still a short interval after barrier success where `published < crash_stable`; they are not equal at every instant). For `Process`, `published` can exceed the eventual recovered prefix — do not describe a runtime-known "durable frontier" in that mode; recovery truncates published to what actually persisted.

Defining the taxonomy buys three things: the TLA+ model in research/10 §2.1 gets its state variables named consistently with the design (align design.md's terminology with the formal model, not the other way around); cancellation semantics (§6.5) become statements about which frontier an intent has crossed; and the Process-mode "cursor regression" rule falls out of the definitions.

### D3. Retention can eat the dedupe window's canonical keys

Design §13.2/§13.5 rest on "full keys remain in canonical capsules until the dedupe window expires." Research/02 §13 designs retention boundary objects for prefix deletion — but nothing connects the two: if retention deletes segments whose positions are still inside span `W`, the exact-verification path (`canonical_key(candidate.ptr)`) dereferences deleted bytes, and a lost checkpoint can no longer rebuild the window (§13.5's guarantee breaks). Add an explicit invariant:

```text
window_start = watermark.saturating_sub(W)

a segment containing canonical dedupe keys is deletable only when:
    segment.end_position <= window_start
```

Otherwise the retention boundary object must carry forward the still-live full keys the way it carries snapshots. Add to the risk register; it's a silent-corruption class (S=5, D=5) that none of R1–R20 currently covers.

### D4. Registration retry idempotency under A6 deserves a stated rule

A6 permits unacked-but-committed batches to surface after recovery. For a v4 capsule that *registered a stream* and crashed before ack: recovery applies the registration (ID allocated, name mapped); the client retries "append to stream X, new". The live path must resolve the already-existing name to the existing ID before deciding whether to emit a registration control — not treat it as a registry conflict. Research/02 §12 covers only effect-identity idempotence, not this API-level case.

But registration idempotency does **not** make the whole retried append idempotent. The precise retry sequence is:

```text
resolve name -> existing stream ID
check exact dedupe key
check expected version
```

with outcomes:

```text
same live dedupe key:            absorb retry, return the original commit
no dedupe key / expired key:     ExpectedVersion::NoStream conflicts with the
                                 recovered version — append nothing
different registration mapping:  corruption/conflict
```

It must never silently append the events a second time merely because the registration resolved. Note the canonical replay rule stays strict: `RegistryState` intentionally rejects a second registration for an already-registered ID; the v4 **writer** avoids constructing the duplicate record rather than the replay rule being weakened. Two Spike E crash tests close this: (1) recovered-unacked registration+event with a matching dedupe key returns the original commit; (2) the same retry without a dedupe key returns an expected-version conflict and appends nothing.

### D5. One dedupe key per capsule is a new API decision — document it as such

v4 allows at most one `DedupeKeyV1` per user capsule covering the whole batch (research/09 §12). Since production dedupe is dormant (V1) and the public append path carries no per-event idempotency key today, this is not a narrowing of an existing contract — it is a **new API decision**. Document it as batch/capsule-level idempotency, and state explicitly that per-event idempotency requires one capsule per event or a future vector-valued control record.

### D6. Bounded-ring fairness under byte backpressure

§6.6 bounds the ring by bytes and has producers await space. Unaddressed: wake order when space frees (FIFO? arbitrary? — a large intent can starve behind a stream of small ones admitted around it), and what happens when a producer awaiting *space* (not completion) is dropped. Loom plan §5.3 models completion-slot cancellation but not space-waiter cancellation. Strict FIFO admission avoids starvation but creates head-of-line blocking behind an oversized intent — that trade should be a deliberate, tested policy decision, plus one Loom case for space-waiter drop (which releases nothing, because nothing was reserved).

---

## 5. S — suggestions to make it stronger

**S1. Elevate the "safest high-value subset" to the headline.** Research/08 §7 quietly names the best strategic insight in the pack: single owner on v3 + resident heads + no Book + effects/checkpoints is valuable *even if v4 is never built*, and everything in it is reversible. That subset is the same shape as bn-2cj/bn-34o's direction and could plausibly be the next quarter of engine work with Fjall retained for names/snapshots. The README's proposed decision says this, but design.md buries it at §21/§23 — restate it in §0 so a skimming reader doesn't think the wager is v4-or-nothing.

**S2. Run the 100M-event open measurement early.** The 29 s extrapolation (research/01 §4) is the pack's single most user-visible motivation, and `spikes/recovery_scale/` infrastructure already exists. Turning that Derived number into a Measured one (even on a synthetic corpus) costs a day and either strengthens the pitch or recalibrates it before anyone commits to Spike D.

**S3. Add "effect-apply throughput during recovery" to Spike D's gates.** §18.3's 1.0 s open target implicitly budgets checkpoint load + suffix fold. The gates check equivalence and effect size, but not the fold *rate* — a correct-but-slow ordered reduce could pass every listed gate and still miss the startup target. One line: "compose+apply ≥ N effects/s such that the §18.3 corpus opens in budget."

**S4. Reuse `RegistryState` as the single fold implementation** (per V6) — and make the maintainability guardrail "one canonical `Effect` definition" (research/08 §6.1) explicitly include the registry component, since that's the one place a second implementation already exists to drift against. One boundary condition: the effect layer promises idempotent reapplication by stable control identity, while canonical `RegistryState::apply` correctly rejects duplicate registrations. Keep both — the effect layer discards already-applied control identities, then feeds each canonical assignment **exactly once** into `RegistryState`. Do not weaken `AlreadyRegistered` globally to satisfy effect idempotence.

**S5. Cite the actually-measured Fjall numbers as the honest floor.** Warm `stream_head` ≈ 0.49 µs and `dedupe_lookup` ≈ 0.55 µs (`meta/mod.rs:56-61`) are respectable component floors — though note neither is the current production `head()` path, which reads the Book under its mutex. Add the corollary the pack dances around: **head lookup has not been demonstrated as a bottleneck in any measured composed workload**; the wins are startup asymptotics, RSS, the publish tail, and the name barrier. Direct heads earn their place primarily because they enable Book removal, compact checkpoints, deterministic ownership, and low resident memory — the nanoseconds are a side effect, not the thesis. Saying this plainly inoculates the design against "you built a custom engine to shave 440 ns" criticism.

**S6. Add a `mess doctor dedupe-status` precheck to M0** (per V1) — one command that proves the dedupe partitions are empty across a fleet turns the M6 simplification from an assumption into a checked invariant.

**S7. Consider batch-size-stratified acceptance gates throughout §18** (per C4). The one matched pair available today sits at ~84%; whether other batch-size/payload points sit nearer or farther from the gate is unknown until the matched baseline matrix exists — which is itself the argument for stratifying rather than trusting one ratio.

**S8. Update `MANIFEST.sha256`** when this review and any revisions land, or state that the manifest covers only the original ten documents. Right now a reader can't tell whether an unlisted file is an addition or tampering — which is exactly the ambiguity the manifest exists to prevent.

---

## 6. Priority order

If the pack gets one revision pass before bones are cut:

1. **V1** (dedupe dormancy) — corrects a false premise, deletes migration work, and re-scopes Spike G.
2. **V2** (seal-pipeline Book dependency) — prevents a mid-implementation surprise in the highest-value spike.
3. **D1 + D2** (durable-mode gate on Spike B, B0/B1 pipeline benchmark, state-frontier taxonomy) — the one place the proposed architecture could genuinely regress vs. the current engine, and it's invisible to Spike B's Process-only gate.
4. **C1 + C2** (A5 claim, spike lettering) — cheap, and they'll be found by any careful reviewer if not fixed.
5. **V5 + S1** (re-frame the name-barrier win; headline the reversible subset) — keeps the pack persuasive as bn-2cj/bn-34o land.
6. **D3** (retention × dedupe window) — new risk-register entry; silent-corruption class.
7. Everything else as editorial follow-up.

None of these threaten the central wager. The event log as the sole transition authority, state as an associative fold, checkpoints as discardable caches, and the spike-gated reversible build order are all the right calls — and the pack's willingness to predeclare kill criteria is rarer and more valuable than any individual mechanism in it.
