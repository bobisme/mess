Thanks—this is an excellent review. I accept the central verdict and nearly all the code-verification findings. Please incorporate V1–V6, C2, D3, D6, and most of S1–S8. I have the following corrections and qualifications.

### 1. C4’s claimed 63% composed/bare ratio is not valid

Please do **not** publish `2.96M / 4.71M ≈ 63%` as the current composed/bare ratio. Those are not equal workloads:

- `4.71M` is the bare log at **4 writers × 100-event batches**, approximately 250-byte payloads.
- `2.96M` is the composed engine doing **400 sequential 5,000-event appends to one stream**, approximately 24-byte payloads.

The composed benchmark’s loop is plainly sequential and uses 5,000-event batches, so dividing those numbers is apples-to-oranges.

The matched **4-writer × 10-event** comparison is valid:

```text
bare log:       2.038M ev/s
composed:       1.710M ev/s
ratio:          ~84%
```

Those rows explicitly share the same concurrency and batch size.

So I agree with batch-size-stratified gates, but the pack should add a fresh matched matrix before asserting the large-batch ratio or diagnosing the missing 37 percentage points. The inference that the apparent 63%→84% difference proves per-event publish-tail cost is also unsupported until payload size, concurrency, stream distribution, and batch size are controlled.

### 2. D1 correctly identifies a risk, but pipelining should remain an experimental variant

I accept the important part of D1: **Spike B needs an `Os`/`Group` gate**, not only a Process-mode gate. A single-owner implementation that wins buffered mode but regresses durable mode should die at Spike B.

I do not agree that speculative cross-barrier pipelining should immediately become normative architecture. The existing group-commit spike explicitly implemented “encode/write N+1 while N’s fsync runs” and measured no distinguishable win. Its conclusion was to retain the property that appenders can continue gathering while the barrier runs, but skip the extra pipeline stage.

That result is not conclusive for Asterism—the owner would absorb more validation work than the existing committer—but it is sufficient reason to **benchmark two variants rather than prescribe one**:

```text
B0: one owner, inline barrier
B1: one owner, pipelined next-group validation/encoding
```

Only B1 should survive if it measurably improves matched durable throughput or tails.

There is also a correctness cost the review should acknowledge: if group N+1 writes to the same file while group N’s `fdatasync` is in flight, the barrier may persist some or all of N+1. That can still be safe under A6, but it makes the exact barrier cut nondeterministic and must be represented in the fault model. Encoding N+1 without writing it avoids that issue but overlaps only CPU work—which the earlier spike found was not the bottleneck.

So: **accept the durable gate and model the pipelined option; do not make pipelining mandatory before measurement.**

### 3. C3 identifies useful ancestry, but `StreamHeadTable` is not a proto-`SegmentEffect`

The pack should absolutely acknowledge the v3 extension region and R2 manifest as design ancestors. But the proposed equivalence is too strong.

A v3 `StreamHeadEntry` contains only:

```text
stream_id
last_version
head_hash
```

It does not contain the incoming version boundary, final global position, registry delta, snapshot-head transition, checkpoint effects, or dedupe epochs required by a complete independently composable `SegmentEffect`. The extension is also specified as durable fold-certificate material, not as a rebuildable state accelerator.

More importantly, the current production sealer does not emit a `StreamHeadTable`: it writes a `phase3` trailer directly at `content_len`, with no extension payload inserted.

I recommend revising C3 to say:

- acknowledge the extension-region framing and R2 validation discipline;
- keep `SegmentEffect` in the rebuildable SealPack;
- keep fold anchors in the durable extension region;
- never create two authoritative copies of the same logical field;
- when a valid v3 `StreamHeadTable` actually exists, use it as **partial evidence**, not as a complete synthetic effect or a guarantee that no scan is required.

Likewise, the checkpoint manifest should share anchor and validation concepts with R2, but it need not physically replace the segment-catalog manifest if the two have distinct lifetimes and failure domains.

### 4. D4 needs a more precise retry outcome

I agree that recovery of an unacknowledged registration capsule must not cause a registry conflict when the client retries the same stream name. The live path should resolve the already-existing name to the existing ID before deciding whether to emit a registration control.

But registration idempotency does **not** by itself make the whole event append idempotent. The correct retry sequence is:

```text
resolve name -> existing stream ID
check exact dedupe key
check expected version
```

Outcomes:

```text
same live dedupe key:
    absorb retry and return original commit

no dedupe key, or expired key:
    ExpectedVersion::NoStream conflicts with the recovered version

different registration mapping:
    corruption/conflict
```

It must never silently append the event a second time merely because the registration resolved.

This distinction matters because the existing `RegistryState` intentionally rejects a second registration for an already-registered ID. The v4 writer should avoid constructing that duplicate record, not weaken the canonical replay rule.

Please add both crash tests:

1. recovered-unacked registration+event with a matching dedupe key returns the original commit;
2. the same retry without dedupe returns an expected-version conflict and appends nothing.

### 5. V2 is correct, but the seal gate should be semantic, not compressed-byte identity

I accept the Book dependency completely. The current sealer waits for both ActiveIndex and Book publication and then copies payloads out of `book.payloads` to construct `.pcol`.

Spike C must therefore include:

- reading payloads back from the rolled raw segment;
- gating seal readiness on the canonical published/durable watermark rather than Book length;
- constructing the payload pack with no Book present.

However, “identical `.pcol` bytes” should be an auxiliary same-build regression check, not the correctness contract. Compression output may legitimately change with encoder version, library version, block tuning, or a future parallel implementation.

The normative gate should be:

```text
reassembled event payloads are byte-identical
pointer/version/global-position results are identical
the new pack validates and falls back correctly on corruption
```

### 6. V7 must distinguish current implementation from Asterism’s target

The current production committer does indeed issue `k` positioned writes followed by one barrier, despite its introductory shorthand saying “one coalesced write.” Its own deviation section documents this precisely.

Please correct descriptions of the **current engine** accordingly.

But retain “one coalesced write” as an explicit Asterism target. That is intentional, not a mistaken description of current code, and the performance spike found actual group coalescing to be the useful top-end optimization. The revised wording should be:

```text
current:
    k pwrite calls + one group barrier

Asterism target:
    one gathered/coalesced write where size permits,
    with writev or bounded chunks for oversized groups
```

### 7. C1 should say “safety-preserving A5 amendment,” not “strictly stronger”

I accept the contradiction: “does not weaken any A1–A12 rule” is wrong because v4 intentionally changes A5.

But the review’s suggested phrase “strictly stronger acceptance rule” is also mathematically awkward. The v4 rule accepts control-only capsules that v3 A5 rejects, so it is not stronger by set inclusion.

Please use:

> Asterism preserves A1–A4 and A6–A12. Format v4 replaces A5 with a safety-preserving nonempty-capsule rule, `control_count + event_count >= 1`, and promotes contiguous `batch_id` validation to a recovery-significant rule.

That says exactly what changed without overselling it.

### 8. D2’s frontier taxonomy is right in spirit, but Process mode needs a fourth distinction

Naming the state versions is a good improvement. I would not force everything into exactly three frontiers, though. The accurate model is closer to:

```text
speculative:
    accepted by validation; may still be discarded on I/O failure

written/accepted:
    write returned; in Process mode this can be ack-eligible

crash-stable:
    covered by a successful Os/Group barrier
    under Process, the exact persisted prefix is unknowable until recovery

published:
    reader-visible effects
```

For `Os` and closed `Group`:

```text
published <= crash_stable
Success completion => capsule is both crash-stable and published
```

There can still be a short interval after barrier success where `published < crash_stable`; they are not literally equal at every instant.

For `Process`, `published` can exceed the eventual recovered prefix. Describing a runtime-known “durable frontier” in that mode would imply knowledge the process does not possess.

This should align design terminology with the formal model rather than simplifying the formal model to fit three names.

### 9. V1, D3, and D5 should be revised together

I accept V1: production `LogEngine` does not use the dedupe partitions. Its post-commit `CommitGroup` contains only a stream-head update.

Therefore:

- describe current Fjall dedupe as a **dormant component**, not composed behavior;
- label the Fjall comparison in Spike G as a synthetic component baseline;
- make M6 conditional on an inventory check;
- treat position-span dedupe as a new feature, not a migration of established runtime semantics.

I would keep the inventory check rather than assert that every existing directory is empty: `MetaStore` is public, and experimental or directly constructed stores may contain rows even though `LogEngine` does not create them.

D3’s retention invariant should be written without the ambiguous “floor ≥ boundary” terminology:

```text
window_start = watermark.saturating_sub(W)

a segment containing canonical dedupe keys is deletable only when:
    segment.end_position <= window_start
```

Otherwise the retention boundary must carry the still-live full keys.

For D5, “one dedupe key per capsule” is not a narrowing of an existing production contract because production dedupe is dormant and the public append path currently carries no per-event idempotency key. It is a **new API decision**. Document it as batch/capsule-level idempotency and state that per-event idempotency requires one capsule per event or a future vector-valued control record.

### 10. Reuse `RegistryState`, but preserve its strict canonical semantics

I accept V6/S4. `RegistryState` is already the backend-independent replay state machine and should be the source of registry rules rather than creating another implementation.

One qualification: the SegmentEffect layer promises idempotent reapplication by stable control identity, while canonical `RegistryState::apply` correctly rejects duplicate registration records. Do not make canonical double registration legal to satisfy effect idempotence.

Instead:

```text
effect layer:
    discard already-applied control identity

then:
    feed each canonical assignment exactly once into RegistryState
```

Shared low-level assignment logic is welcome; weakening `AlreadyRegistered` globally is not.

---

## Accepted without further pushback

The following should be incorporated essentially as written:

- **V1:** dormant dedupe, conditional M6 simplification, explicit new semantics.
- **V2:** Book removal must redesign sealing.
- **V3:** production reads currently bypass ActiveIndex and serialize through Book. The code confirms `head`, hot stream reads, cold materialization, and global reads all take the Book mutex.
- **V4:** add hash-presence flags to `SnapshotInstalledV1`; current hashes are explicitly reserved `None`.
- **V5:** lead with one-authority atomicity and mode inheritance, not the raw two-barrier latency claim. `bn-2cj` is currently only marked `doing`, and `bn-34o` is blocked behind it; neither production optimization has landed yet.
- **V6:** reuse or explicitly supersede the existing registry implementation.
- **C2:** unify the spike lettering on the complete A–J sequence in research/05.
- **D3:** add the dedupe-retention invariant and risk-register entry.
- **D6:** specify bounded-ring fairness and cancellation. Strict FIFO avoids starvation but may create head-of-line blocking; that trade should be deliberate and tested.
- **S1–S4, S6, and S8:** all good.
- **S7:** good after replacing the invalid 63% number with matched measurements.

For **S5**, soften “head reads were never the bottleneck” to:

> Head lookup has not been demonstrated as a bottleneck in the measured composed workloads.

The warm Fjall number is a useful component floor, but it is not the current production `head()` path—that path reads Book—and there is no matched high-concurrency head-only profile proving “never.” Direct heads remain valuable primarily because they enable Book removal, compact checkpoints, deterministic ownership, and low resident memory; the nanoseconds are not the whole thesis.

The review’s priority order remains broadly right after replacing “D1 pipeline it” with “D1 add a durable gate and benchmark the pipeline variant.”
