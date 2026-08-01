# ADR 0003: Decline the v4 commit-capsule format

- Status: Proposed — RECOMMENDATION PENDING LEAD RATIFICATION
- Date: 2026-08-01
- Bone: bn-1ojm (Decide v4 admission at the post-Fjall control-plane gate)
- Deciders: mess-dev (ratifying); drafted by mess-w-1ojm
- Amends: [ADR 0002 §4](0002-asterism-capability-authority.md) — the interim
  v3/v4 control contract this record resolves
- Evidence: `spikes/capsule_v4_prelude/REPORT.md`,
  `notes/mess-asterism/research/09-wire-format-v4-sketch.md`, the recorded
  `bn-11mk` and `bn-2ctq` decisions (2026-07-28), and the measurements in §5
  below

## Decision

**DECLINE.** The v4 commit-capsule format is not admitted as the production log
format. v3 remains the normative format; `$registry` remains its sole control
encoding and that contract is now **frozen and closed**, not interim. The v4
prototype in `crates/mess-log/src/v4/` stays exactly what
`crates/mess-log/src/lib.rs:62-65` already says it is — non-normative,
write-off-by-default spike code — and is not promoted, extended, or wired into
`mess-store`. The conditional Phase 9 implementation chain (`bn-ro0i`,
`bn-5rc6`, `bn-2wpj`, `bn-2r8`, `bn-3ew9`, `bn-31f8`, `bn-1zm0`) closes without
speculative production code.

This is a **product** decline, not a format rebuttal. Spike E's verdict stands:
the v4 byte contract is admissible and the crash story is as boring as v3's
(96,654 exhaustive states, 24,000 sector-reorder cases, three 10M-exec fuzz
soaks, zero safety violations). Nothing here disputes that. What the spike
proved is that v4 *could* be built safely. This gate asks whether it *should*
be, and every capability it was going to serve has since been individually
declined or found to have no consumer — while its costs are permanent, and the
one cost that can be stated as a number today points the wrong way (§5.2).

The exact evidence that reverses this decision is in **Revisit when** (§9). It
is deliberately narrow and deliberately reachable.

## Context

The Asterism roadmap deferred a production format change until the flat owner
and the log-derived state kernel existed. That condition is now met, and the
landscape v4 was designed for has changed underneath it:

- **Phase 3** (`bn-k8qd`) produced ADR 0002, which classified the four
  candidate control capabilities and routed each to its own gate. It did not
  pull v4 forward; §4 explicitly left v4 unadmitted and named this bone as the
  only v4 product-admission gate (`0002-…:637-638`).
- **`bn-11mk`** (2026-07-28) DECLINED engine-owned projection checkpoints.
- **`bn-2ctq`** (2026-07-28) DECLINED exact batch idempotency for v3 and
  deferred the "does v4 change this?" question to this record.
- **Phase 7** (`bn-fj34`) deleted `MetaStore`, `FjallSnapshotBackend`, and the
  fjall dependency outright.
- **Phase 8** shipped pack snapshot sidecars (`bn-ozi5`, `bn-3l8n`, `bn-ccx1`)
  and per-segment registry deltas (`bn-26pp`).

The v4 sketch's own §23 (`research/09`) states the position this record now
settles: the format resolutions "do not decide whether snapshot, projection, or
idempotency controls are product capabilities."

There are still no users and no existing stores. Migration cost is therefore
not an input on either side of this decision, and this record does not use it
as one. What *is* an input is that ADR 0002 §4's escape hatch survives a
DECLINE intact: a future capability that genuinely needs v4 amends this ADR and
rewires through a fresh gate, and the tested format artifact is still on trunk
to make that cheap.

## 1. The constituency

Format support alone is not product admission — ADR 0002 §4's own words
(`0002-…:484`). v4's value is entirely derivative of the capabilities it would
carry. As of this date the roster is empty:

| capability | v4's offer | decided fate | deciding record |
|---|---|---|---|
| Snapshot install | `SnapshotInstalledV1` control-only capsule | **DECLINED** — snapshot discovery is a discardable pack sidecar; "No `SnapshotInstalled` record is emitted in v3 or v4" | ADR 0002 §1, `0002-…:363-365`; shipped and default (`bn-ozi5`, `bn-3l8n`, `bn-ccx1`) |
| Projection checkpoints | `ProjectionCheckpointV1` control | **DECLINED** — zero production callers; the one real consumer owns a discardable sidecar and proves it correct with `examples/social/src/rebuild.rs` | `bn-11mk`, 2026-07-28 |
| Exact batch idempotency | one `DedupeKeyV1` per capsule, atomic with its events | **DECLINED for v3**, revisit routed here | `bn-2ctq`, 2026-07-28; disposed of in §6 below |
| Registry encoding | registration control co-committed with the first event that uses it | **KEEP v3** — `$registry` is canonical, live, and correct; its position cost is contractual (§2) | ADR 0002 capability table, `0002-…:48`; this record |
| Control/event atomicity | one capsule commits controls and events or neither | **no admitted control to be atomic with** — the sole live control is `$registry`, whose REG12 ordering is already correct and crash-safe in v3 | this record |

The last row is the load-bearing one. Control/event atomicity is not a
capability; it is a *property* that becomes valuable only once there is a
control whose split from its events would be a bug. ADR 0002 §3 correctly
identified that a v3 dedupe bridge would be genuinely hard because "V3 has no
existing field that atomically covers a dedupe key and its domain events"
(`0002-…:459-463`) — but `bn-2ctq` then declined dedupe on grounds that had
nothing to do with encoding. v4 makes atomicity cheap for a feature nobody
asked for.

`$registry` needs no such atomicity. REG12 requires the registry batch to
occupy *earlier* positions than the first domain batch referencing its ids
(`docs/spec/04-registry.md:440-445`), and a torn registry batch is simply not
accepted, so the domain batch that follows it is never reached. The failure
mode v4's atomicity prevents — a committed event referencing an id whose
registration did not commit — is already structurally impossible in v3.

### Nothing in the pack-sealed world creates a new constituency

`bn-26pp` (Phase 8) is the closest thing to a new argument, and it cuts the
other way. Registry rebuild was 89.9% of a 10.5 s cold open at 8 GiB/250k
streams; the fix was a discardable per-segment `.reg` delta
(`crates/mess-index/src/sealed/regdelta.rs`), which cut cold open 38.5x with
"+0.12% store size" and left the log as sole authority. That is the *same*
problem v4's control plane would have been sold as addressing, solved inside
v3 by a discardable accelerator, already merged, already default. It removes a
constituency rather than adding one.

## 2. Position semantics: v3 gaps versus v4 control-only capsules

**v3.** Every accepted event frame consumes one canonical global position,
including `$registry` frames (`docs/spec/01-log-format.md:527-541`). The
application-visible subset is deliberately not dense: global reads and
subscriptions filter `stream_id == 0`, so delivered positions have holes
(`docs/spec/04-registry.md:447-460`, `docs/spec/06-subscriptions.md:60-73`).
Consumers MUST treat a position as an opaque monotone ordering/resume cursor
and advance across a filtered-only range via the page's exclusive scan
frontier.

**v4.** A control-only capsule advances `CommitCursor`/`batch_id` but not the
domain global position (`REPORT.md` §2, safety property P4). Registrations
would consume no domain position, and visible positions would be dense.

**Is the v3 contract actually honored, or is it prose?** It is honored, in
code, on the paths that matter:

- `crates/mess-store/src/engine.rs:5055-5077` — `read_global_page` documents
  the holes as "the accepted cost of landing the registry on v3 rather than
  waiting for v4 control capsules", skips stream 0, and returns `frontier: pos`
  (`engine.rs:5192`), the exclusive position the scan actually reached.
- `crates/mess-store/src/subscription.rs:184-220` — the subscriber advances
  `cursor` to `max(last_delivered + 1, frontier)` on a non-empty page and to
  `frontier` on a page that was empty only because the scan crossed
  engine-internal positions. It never jumps to the watermark. This is exactly
  SUB2 (`docs/spec/06-subscriptions.md:210-218`).
- `crates/mess-store/src/engine.rs:5113,5168` — a stream-0 directory entry is
  skipped by advancing `pos` past it; the batch bytes are never `pread`. The
  doc comment's "skipping is free, not merely cheap" is accurate.

So the gap "problem" is contractually solved and costs nothing on the read
path. Removing gaps buys a nicer-looking public API, not a capability.

**But the gaps are not rare.** This record does not want to hide behind an
adjective, so §5.1 measures them.

## 3. Integration, validation, recovery, and downgrade cost

| surface | v3 today | v4 if admitted |
|---|---|---|
| Owner integration | one append path; one stream per batch (D-FMT-6) | flat owner must plan/validate user-only, control-only, and mixed capsules and assign domain positions only to user records (`bn-5rc6`) |
| Validation | one scanner; A1–A12 | two scanners; an allocation-free `validate_capsule` fast path plus on-demand materialize, and a **mandatory** differential proof that the two accept the same corpus (`bn-2r8`) |
| Recovery / crash model | one exhaustive model, one torn matrix, one SIGKILL harness | two of each, permanently; plus mixed v3/v4 replay proving identical registry/head/position state against a reference model (`bn-2wpj`) |
| Downgrade refusal | not needed | a typed, actionable, before-mutation refusal when a v3-only binary meets a v4 directory — **not implemented today**: `crates/mess-log/src/scanner.rs:763-772` returns a bare `None` for a non-v3 segment version, indistinguishable from a corrupt or absent header |
| Reserved-field policy | v3 batch flags checked (`scanner.rs:417`) | header flags and reserved words are checked (`v4/capsule.rs:507-516`), but the 32-byte marker's `flags` and `reserved` words are written zero (`v4/capsule.rs:311,315`) and **never validated on decode** (`v4/capsule.rs:557-565` checks magic and the three echoes only) |
| Long-term burden | one format | two formats, forever: fixture lifetime, rollback policy, feature negotiation, two golden corpora, two fuzz families, and a permanent "which format is this store?" question in every offline tool |

The last two rows are `bn-2r8`'s scope. Its four gaps are real and remaining;
this record confirms each against trunk:

1. **Real-writer SIGKILL coverage** — open. `REPORT.md` §6 defers it explicitly:
   the existing `sigkill_harness` is wired to the v3 writer/committer, and a v4
   scenario needs a v4 committer that does not exist.
2. **v3-only open-time refusal** — open, and worse than "not written": today it
   would be a silent `None` (`scanner.rs:770`), not a typed error.
3. **Reserved marker validation** — partially open: header covered, marker
   `flags`/`reserved` words not (`v4/capsule.rs:311,315` vs `557-565`).
4. **validate-vs-decode differential fuzz** — open. The three committed targets
   (`fuzz_v4_capsule`, `fuzz_v4_control`, `fuzz_v4_scan`) are parse-only and
   parse-then-reencode; none differentially pins `validate_capsule` against
   `decode_capsule`.

Note for the record: `bn-2r8`'s description says these four are "documented in
`spikes/capsule_v4_prelude/REPORT.md`". Only gap 1 is documented there as such;
gaps 2–4 are traceable to the bn-9mw adversarial review, not to REPORT.md. The
bone text overstates the report. All four are nonetheless genuine, as verified
above.

## 4. What Spike E's gates do and do not show

The spike's headline gates are sound and this record relies on them for what
they claim. Two need precise reading before they are used as an admission
argument.

**"Scan overhead within a 2% gate" is a per-byte gate, and by construction it
cancels v4's larger frames.** `crates/mess-log/tests/v4_scan_bench.rs:13-18`
says so in its own words: the per-byte ratio isolates "scanning efficiency from
v4's larger fixed header/marker framing (which moves more CRC bytes per capsule
by design)". The assertion at `v4_scan_bench.rs:157-160` is on
`per_byte_overhead` alone (computed at `v4_scan_bench.rs:137-139`). The
measured +0.47% means *the v4 scanner is as efficient per byte as v3's* — it
does not mean a v4 store costs the same to scan. It costs more, because it is
bigger (§5.2).

**The best case at that gate is "not worse".** A gate of the form "overhead
< 2%" cannot produce a benchmark-visible win. Judged on composed engine
benefit, v4's measured contribution today is zero: nothing in
`mess-bench --mode full --floors` moves, because no production path can be
pointed at v4 without first building `bn-ro0i` + `bn-5rc6` + `bn-2wpj`. And the
two bones that would supply real numbers — `bn-3ew9` (certification) and
`bn-31f8` (benchmark) — are **PUNTED** as end-of-project attestation that
"cannot produce a meaningful answer until the engine it certifies actually
exists" (mess-dev, 2026-07-28). The ADMIT path's own evidence gates are not
runnable under the current evidence regime.

**What the spike genuinely de-risks** is the byte contract and the crash model.
That value is preserved by this DECLINE, not destroyed: the code, the golden
fixtures, the exhaustive model, the torn matrix, the fuzz targets, and the D4
retry tests all remain on trunk and green.

## 5. Measurements

### 5.1 Gap density — how much of the position space is `$registry`?

Method: `social-seed` (release build of workspace `bn-1ojm`, epoch `d473744a`)
into a fresh directory, then `mess inspect --format json`. Registry positions
are computed as `durable_event_count - Σ(head_version + 1)` over every
`stream_id != 0`, cross-checked against the `$registry` stream's own head
version and against the folded registry's `stream_names + type_names` counts.

| corpus | global positions | domain events | `$registry` positions | gaps |
|---|---|---|---|---|
| social demo (`--scale demo`, seed 1337) | 3,008 | 1,526 | 1,482 (1,476 streams + 6 event types) | **49.27%** |
| social large (`--scale large`, seed 1337) | 108,810 | 54,602 | 54,208 (54,202 streams + 6 event types) | **49.82%** |

Half the position space. That is far more than an aesthetic blemish, and this
record will not pretend otherwise.

It is also fully explained by one ratio. Registry positions are
`streams + event_types + categories + dicts` — one record, one batch, one
position per `RegistryLog` append
(`crates/mess-store/src/registry/mod.rs:197-270`). So

```text
gap fraction ≈ 1 / (1 + events_per_stream)
```

The social example is a stream-per-aggregate design in which every user, post,
follow edge, and like edge is its own stream: 54,202 streams carrying 54,602
events, i.e. **1.007 events per stream**. That pins the gap fraction at ~50% by
arithmetic. At 10 events/stream it is 9%; at 100, 1%. The measured 49.8% is the
maximum of the range, not a typical value — but it is the number the project's
own flagship example produces, so it is the number this record uses.

### 5.2 The framing cost v4 would add

v4's frames are strictly larger than v3's:

| | header | marker | framing/frame |
|---|---|---|---|
| v3 batch | 72 (`crates/mess-log/src/format.rs:55`) | 16 (`format.rs:116`) | **88 B** |
| v4 capsule | 96 (`crates/mess-log/src/v4/format.rs:50`) | 32 (`v4/format.rs:160`) | **128 B** |

**+40 B per frame, a +45.5% increase in fixed framing.** Neither encoder pads,
so this is exact: v3 `total_len = 72 + chain + frames + 16`
(`crates/mess-log/src/encode.rs:370-371`); v4
`total_len = 96 + chain + control_len + event_region_len + 32`
(`crates/mess-log/src/v4/capsule.rs:209-213`).

On the spike bench's own corpus (one event, 64 B payload, 28 B subframe header)
v3 writes 180 B per event and v4 writes 220 B: **+22.2% bytes for identical
events**. Combining that with the measured per-byte parity, v4's per-event scan
cost on that corpus is ≈ +22.8%, not +0.47%. The gate is not wrong; it simply
measures a different thing than "what does a v4 store cost".

**Where v4 wins the bytes back, and where it stops winning.** v4's real byte
argument is not gap removal, it is co-committing a registration with the first
event that uses it, which deletes a whole v3 batch. For a stream that receives
`E` single-event appends, with registry payload `R` and event payload `P`:

```text
v3 = (72+28+R+16) + E·(72+28+P+16)        = 116 + R + 116E + E·P
v4 =  (96+8+R+28+P+32) + (E-1)·(96+28+P+32) =   8 + R + 156E + E·P
v4 - v3 = 40E - 108
```

`R` and `P` cancel. **v4 is smaller only while `E < 2.7` — fewer than three
appends per stream. At `E ≥ 3` v4 is strictly larger, and it grows by 40 B per
append forever.** On the social large corpus (`E = 1.007`) v4 would be ~21%
smaller; on any store whose streams accumulate history it is larger without
bound. With batched appends the +40 B amortizes over the batch, but so does the
one-time registration saving, and the sign does not change for a long-lived
store.

Two honest caveats, both against this record's own verdict: the co-commit
saving is an *estimate* from format constants, not a measurement; and the
stream-per-aggregate/short-stream regime where v4 wins is a real and common
event-sourcing shape, not a strawman. The counter is that measuring it is
exactly `bn-31f8`, which is punted, and that the regime where v4 wins is
precisely the regime a seeded demo corpus occupies and a running application
grows out of.

## 6. Batch identity and idempotency — the normative rule

**v3 is the normative format, and it has no batch identity contract and no
engine-level idempotency.** Stated positively, and now closed:

1. `batch_id` is **per-segment and informational only**. Recovery's identity and
   ordering come from `first_global_pos` (A1) and `segment_epoch` (A9), never
   from `batch_id` (`docs/spec/01-log-format.md` D-FMT-5, lines 555-562). It is
   not recovery-significant and no contiguity rule applies to it.
2. **A5 stands unamended**: `frame_count ≥ 1`; empty batches are rejected
   (`docs/spec/01-log-format.md:508,819`; `docs/spec/02-recovery.md:98`;
   `crates/mess-log/src/acceptance.rs:16,125`).
3. `Backend::append_batch` carries **no idempotency key**, and none is added.
   The supported and sufficient retry story is caller-level: optimistic
   concurrency via `expected: Version` makes a duplicate retry observable — the
   retry conflicts, the caller re-reads and decides. This costs the engine
   nothing permanent.
4. There is **no `$dedupe` v3 record and no v3 format extension** for one. ADR
   0002 §3's prohibition on approximating atomicity with two ordered v3 batches
   remains in force and is now moot rather than pending.

**A5 amendment disposition.** Spike E replaced A5 with the nonempty-capsule
rule `control_count + event_count ≥ 1` and promoted `batch_id` to
recovery-significant with mandatory +1 contiguity (`REPORT.md` §2, "A5 → §6").
Under this DECLINE that design work becomes a **frozen, tested spike
capability, not production format**. It is not adopted into any spec, it does
not amend A5 or D-FMT-5, and `docs/spec/01-log-format.md` and
`docs/spec/02-recovery.md` remain the sole normative statements of batch
acceptance. The rule's proof value is preserved in
`crates/mess-log/tests/v4_model.rs` (properties P1–P7) and in `REPORT.md`; it
is available unchanged to any future amendment that reopens v4.

**`bn-2ctq` revisit disposition — stated plainly, not dodged.** `bn-2ctq`
scoped its DECLINE to v3 and said "admitting v4 re-opens this by construction",
routing the question here. This record closes that door: **v4 is not admitted,
so the dedupe revisit-under-v4 does not occur, and `bn-2ctq`'s DECLINE becomes
the standing decision rather than an interim one.** The reasoning is that
`bn-2ctq`'s rationale was encoding-independent. Its finding was "no identified
consumer" plus "caller-level idempotency via expected-version is free and
sufficient"; its cost objection was a retention frontier outliving segment
deletion, collision exactness under adversarial keys, namespace abuse limits,
and memory/disk caps. v4 addresses exactly one item on that list — crash
atomicity of key-plus-events — and none of the others. Making the hard part
cheap does not create the consumer, and the consumer was the missing piece.
`bn-2ctq` inherits the same escape hatch as everything else in §9: a named
consumer that cannot get correct retry behavior from `expected: Version`
reopens it, and *that* consumer's needs — not the format — would then decide
the encoding.

## 7. Wire-format and security review: the frozen v3 control contract

Under DECLINE the reviewable wire surface is v3's, and `$registry` is its only
control encoding. The following are now **normative and closed**, not interim:

- **`$registry` is `stream_id == 0`** (`crates/mess-store/src/registry/log.rs:33`),
  an ordinary v3 event stream for framing, CRC, recovery, ordering, and
  watermark advancement. It is authority; nothing derived may contradict it.
- **Registry frames consume canonical global positions** and are filtered from
  application delivery, so visible positions have gaps. Positions are opaque
  monotone cursors; `GlobalPage::frontier` is the only safe advance across a
  filtered-only range (`docs/spec/01-log-format.md:527-548`,
  `docs/spec/04-registry.md:447-459`, `docs/spec/06-subscriptions.md:60-86`,
  SUB2 at `06-subscriptions.md:210-218`).
- **REG12 ordering is the atomicity mechanism.** A registration occupies
  strictly earlier positions than the first domain batch referencing its ids.
  A torn registry batch is not accepted, so no accepted domain batch can
  reference an unregistered id. No control/event capsule is required to make
  this safe.
- **Any future canonical v3 system record inherits ADR 0002 §4's rule**: it
  consumes a position, can create a visible gap, must be included in recovery
  while remaining skippable by application delivery, and the ADR proposing it
  must state that cost explicitly. This record adds no such record.
- **Registry accelerators are discardable and cross-checked.** The `.reg`
  per-segment delta (`crates/mess-index/src/sealed/regdelta.rs`) is used only
  when its CRC, magic/version/flags, segment identity, and exact
  `(first_global_pos, frame_count)` list all match the independently validated
  pointer sidecar's directory; otherwise the unchanged `pread` path runs. It
  can supply payload bytes for batches the sidecar already agrees exist, at
  positions it already agrees they occupy — it cannot invent, drop, or move a
  batch. It is read once and never retained.
- **Unknown canonical versions remain fail-closed.** ADR 0002's requirement is
  unchanged. `crates/mess-log/src/scanner.rs:763-772` rejects a segment whose
  header version is not 3, and `scanner.rs:417` rejects a batch header whose
  format version is not 3. Both currently fail closed by returning "not a valid
  segment/batch" rather than a typed "newer format" error. That is safe (no
  mutation, no misinterpretation) but not actionable. Because v4 is declined
  and no writer emits a non-v3 version, this stays a latent diagnostic-quality
  issue rather than a correctness one; §9's ADMIT path makes it a prerequisite.

**Threat notes preserved from the declined branch.** The v4 control TLV's
security properties — every length capped before it drives a slice or
allocation, all controls critical with unknown `(kind, version)` rejecting, the
mandatory split-coverage CRC as the load-bearing check (a CRC-off decoder
wrongly accepted ~1.6% of torn cases, matching v3's ~1.63%) — remain valid and
tested but are not part of the production attack surface, because no production
path decodes a capsule: `crates/mess-store/src` contains zero references to
`mess_log::v4`. The only reference outside `mess-log` is the spike's own D4
retry test (`crates/mess-store/tests/v4_d4_retry.rs`, six references), which
drives the `RegistryView` seam against the real `RegistryState` and rides the
frozen artifact. It is test-only and reaches no production path.

## 8. Reconciliation with ADR 0002

This record is the amendment and closure that ADR 0002 §4 anticipated. ADR 0002
— **`docs/adr/0002-asterism-capability-authority.md`**, authored under
`bn-k8qd` whose parent is `bn-ogn`, "Phase 3: reconcile Asterism authority,
contracts, and evidence" — is the document `bn-1ojm` refers to as "the Phase 3
ADR". There is no other v3/v4 control contract in the repository; ADR 0001 is
the Phase 2 `mess_db` disposition record and is unrelated. ADR 0002 §4 said v4
"is not pulled forward by this decision", that pulling it into any capability
"requires an amendment to this ADR", and that `bn-1ojm` "remains the only v4
product-admission gate" (`0002-…:465-484`, `0002-…:637-638`). This ADR exercises
that clause in the negative direction: v4 is not pulled forward, and §4's
interim framing is replaced by a settled one. ADR 0002 §4 is amended by a
pointer paragraph only; its history is not rewritten. After this record there is
exactly one control contract in force — v3 `$registry` as frozen in §7 — and no
competing one.

ADR 0002's other clauses are unaffected: §1 (snapshot packs) shipped, §2 and §3
were resolved by `bn-11mk` and `bn-2ctq`, and its "Revisit when" entry — "the
v4 admission gate demonstrates product value sufficient to pay its format,
downgrade, and operational costs" (`0002-…:660`) — is answered here: it does
not, today.

## 9. Revisit when — the exact stop criteria

This DECLINE forecloses nothing. Any **one** of the following reopens v4
through a fresh gate that amends this ADR:

1. **A named consumer for an admitted control.** A real caller needs
   engine-owned dedupe, projection checkpoints, snapshot install records, or a
   new control, *and* its correctness requires that control to commit
   atomically with domain events. Then encoding follows the capability, and v4
   is the leading candidate because `bn-2ctq` and ADR 0002 §3 already
   established that a v3 bridge is genuinely hard. A consumer that only needs a
   control to *exist* does not qualify — v3 can carry it as a system record at
   the cost §7 names.
2. **A measured composed win.** A runnable A/B on a production-shaped corpus
   shows v4 better on a metric `mess-bench --mode full --floors` reports, by a
   margin outside this host's noise (`bn-1gn1` showed 5% is inside it). Per
   §5.2 the plausible candidate is log size and cold-open time for
   stream-per-aggregate workloads with short streams; the honest prior is that
   the +40 B/frame framing cost cancels it at `E ≥ 3` appends per stream, so
   the measurement must cover the steady state and not only the seeding phase.
   Reopening on this ground requires unpunting `bn-31f8`.
3. **A gap-visibility defect in the wild.** A real consumer is demonstrably
   harmed by non-dense visible positions in a way the opaque-cursor plus
   `GlobalPage::frontier` contract cannot fix. Note that §2 verified the
   contract is honored in code, so this requires a defect in the contract, not
   in an implementation.
4. **A pre-1.0 public-API freeze that wants dense positions as a permanent
   promise.** Contracts written at 1.0 bind forever. If the project decides
   before 1.0 that "visible positions are dense" must be a public guarantee
   rather than "positions are opaque cursors", that is a legitimate reason to
   revisit — and it must be decided *before* 1.0, because after 1.0 it becomes
   a breaking change rather than a format choice.

An ADMIT reached through any of these inherits `bn-2r8`'s four gaps (§3) as
prerequisites, not follow-ups: real-writer SIGKILL coverage, a typed
before-mutation v3-only refusal, marker flags/reserved validation, and the
validate-vs-decode differential fuzz target.

## Consequences

- **Closed as conditional work that will not happen**: `bn-ro0i` (normative v4
  contract), `bn-5rc6` (owner-path v4 encoding), `bn-2wpj` (mixed v3/v4 read,
  recovery, downgrade refusal), `bn-2r8` (four review gaps), `bn-3ew9` (v4
  certification), `bn-31f8` (v4 benchmark), `bn-1zm0` (v4 default policy).
  `bn-5hcg`'s exit gate is satisfied by this record: the decision is made and
  the declined conditional bones are closed.
- **`bn-2ctq`'s DECLINE becomes standing** rather than v3-scoped (§6). No
  dormant dedupe API or table is left behind — `bn-fj34` already removed them.
- **The v4 prototype is retained, frozen, and demoted.** It stays in
  `crates/mess-log/src/v4/` under the existing non-normative header
  (`lib.rs:62-65`), which should now cite this ADR. Recommendation: **retain,
  do not delete.** It is the artifact that makes every path in §9 cheap to
  re-exercise — deleting it would convert a preserved option into a from-scratch
  rebuild for no benefit beyond a smaller test run. Its extent, stated exactly:
  2,323 lines of `crates/mess-log/src/v4/`, 2,295 lines across the five
  `crates/mess-log/tests/v4_*.rs` suites, 182 lines across the three
  `fuzz_v4_*` targets, and 419 lines in
  `crates/mess-store/tests/v4_d4_retry.rs` — **5,219 lines total**. That last
  file is the one **cross-crate** dependent: `mess-store`'s test target links
  `mess_log::v4`, so a later deletion of the module must remove
  `crates/mess-store/tests/v4_d4_retry.rs` in the same change or `mess-store`
  will not build its tests. `crates/mess-store/src` has no such dependency. If
  the module ever obstructs a refactor, delete it then; `REPORT.md` and git
  history preserve the result either way. This is the one place where the "no
  users, no migration" argument genuinely applies: the option is cheap to
  *keep*, which is precisely why it does not need to be *exercised* now.
- **v3 control semantics are frozen** as stated in §7. `$registry` is the sole
  control encoding; positions are opaque cursors; gaps are legal and
  contractual; `GlobalPage::frontier` is the advance mechanism.
- **Specs are reconciled**: `docs/spec/01-log-format.md` and
  `docs/spec/04-registry.md` no longer describe v4 as a pending alternative;
  they cite this ADR as its declined-with-escape-hatch disposition.
- **One format, one crash model, one scanner, one fixture family.** The
  ongoing engine work keeps a single durable byte contract to defend.
- **What is given up, explicitly**: gap-free domain positions as a public API
  property; a ~21% smaller log for short-stream/stream-per-aggregate corpora
  (estimated, §5.2, unmeasured); and structurally trivial admission of any
  future control. The first is answered by an existing contract, the second is
  unmeasured and reverses sign for long-lived streams, and the third is
  recoverable via §9 at the cost of one ADR amendment.

## Adversarial failure analysis

| challenge to this decision | response |
|---|---|
| "Zero stores today; adopting later is much more expensive" | The premium is smaller than it looks. `bn-2wpj`'s own scope says mixed v3/v4 read machinery, format detection, and downgrade refusal are required **even for adopt-now** — "test fixtures, rollback, and staged rollout require exact format detection". Adopting now buys avoidance of *user data* migration only, and there is no 1.0 commitment yet. Meanwhile the tested format artifact stays on trunk, so the option itself is preserved at ~zero marginal cost. |
| "Contracts written today bind forever; opaque cursors are a worse public API" | True, and §9.4 makes it a named reopening ground with a deadline (pre-1.0). It is not being dismissed — it is being deferred to the moment when the public contract is actually frozen, which is the moment the argument becomes decisive. |
| "The spike said PROCEED with zero safety violations" | It said the *format* is admissible. This record agrees. `REPORT.md`'s scope is format admissibility; §23 of `research/09` says explicitly that the format resolutions "do not decide whether snapshot, projection, or idempotency controls are product capabilities." |
| "Half the positions are registry records — that is not aesthetics" | Measured and conceded (§5.1, 49.8%). But the read path skips them without a `pread` (`engine.rs:5113,5168`), the cursor contract absorbs them (§2), and the *byte* cost they represent is addressed by v4 only via a co-commit optimization that reverses sign at ≥3 appends per stream (§5.2). The position count is large; the harm it causes is not. |
| "v4 makes future controls structurally trivial" | Yes, and that is an argument for reopening when a control is admitted (§9.1), not for paying the format cost against a roster that is currently empty. |
| "DECLINE leaves 5,219 lines of dead code" | Conceded as a real maintenance cost, and it is the price of keeping the option. It is nearly decoupled — `crates/mess-store/src` has zero `mess_log::v4` references; the single cross-crate dependent is the spike's own `crates/mess-store/tests/v4_d4_retry.rs`, which a later deletion must remove alongside the module. It is tested, and deletable at any time without reopening this decision. |
| "The decision was made without running the benchmarks" | The benchmarks that would answer it (`bn-3ew9`, `bn-31f8`) are punted and, per their own punt rationale, cannot produce a meaningful answer until the v4 engine exists — which is the thing under decision. Building it to measure it is the circularity this gate exists to break. §9.2 states exactly what measurement would reverse the verdict and what it must cover. |
