# Observability: authority, backlog, and fallback state

`bn-11ba`. This is the normative list of what the composed engine reports,
what each name means, what unit it carries, and what bounds its cardinality.

There are two surfaces, and the split is not negotiable:

| surface | scope | who can read it |
|---|---|---|
| `LogEngine::observability()` (`mess-store`) | **in-process**, live counters | the writer process itself |
| `mess doctor`'s `authority` section (`mess-cli`) | **offline**, file state | any reader, no lock, live writer or not |

A separate process cannot observe another process's counters. Anything that
looks like a live number in an offline report would be a lie, so the offline
view reports installed state and *what the engine would do*, and says so in an
`authority-scope` advisory.

Both surfaces render one shared classification table,
`mess_store::observability::AUTHORITY_MODEL`. That is deliberate: if the
engine's report and the CLI's report could drift about which bytes are
authoritative, one of them would mislead an operator mid-incident.

## 1. Authority

From ADR 0002 (`docs/adr/0002-asterism-capability-authority.md`). Every
artifact in the store has exactly one role.

| artifact | role | on loss |
|---|---|---|
| `seg-*.log` | **canonical** | data loss; recovery accepts only the durable prefix that validates |
| `$registry` | **canonical** | the name space, and only with the log records that carry it; re-folded from the log on every open |
| `.seal` | discardable accelerator | log scan on open + an owed re-seal |
| `.pidx` | discardable accelerator | log scan on open + an owed re-seal |
| `.pcol` | discardable accelerator | payloads reassembled from log frames |
| `.filter` | discardable accelerator | every segment probed through its pointer directory |
| `.reg` | discardable accelerator | `$registry` batches point-read from the log (`O(#names)` instead of `O(#segments)`) |
| `.par` | discardable accelerator | no repair option; detection is unaffected |
| snapshot pack | discardable accelerator | a load miss; the aggregate is rebuilt by full replay |

Rules a report must obey:

- **Never present an accelerator as authoritative.** `Role::as_str()` is
  either `canonical` or `discardable-accelerator`; there is no third value and
  no field left blank.
- **Every class states its loss consequence.** A list of files without "what
  does losing this cost me" is the thing an operator cannot act on.
- **Format version is v3 only.** ADR 0003 declined v4, so
  `state.log_format_version` is always `3` and no v4 row exists.

## 2. Naming and units

- Units are in the name: `*_bytes`, `*_nanos`, `*_secs`, `*_count`, `*_hwm`
  (high-water mark). A bare plural (`conflicts`, `groups`, `seals_skipped`) is
  a dimensionless monotone total **since this engine handle opened** — not
  since the store was created.
- Percentile fields come from `mess_log::metrics::LatencySnapshot`:
  `count`, `p50_nanos`, `p95_nanos`, `p99_nanos`, `max_nanos`, `mean_nanos`.
  Percentiles are bucket estimates with a bounded ~6% relative error; `count`,
  `max_nanos` and `mean_nanos` are exact.
- `owner.group_width` reuses the same histogram type but its unit is
  **intents per group**, not nanoseconds. It is the one exception and it is
  named in its doc comment.
- Ratios (`cache_hit_rate`, `slot_saturation()`, `byte_saturation()`) are
  `[0, 1]`, not percentages.
- Snapshot semantics: each field is loaded independently from a relaxed
  atomic or a short read lock. One report is internally coherent only while
  appends are quiescent; deltas between two reports are exact for the monotone
  counters.

## 3. Cardinality

**Bounded by the segment count. Nothing scales with the workload.**

The only per-item collections in either surface are:

| collection | one entry per | bound |
|---|---|---|
| `accelerators.segments` | installed sealed segment | segments on disk |
| `fallbacks.refutations` | refuted candidate at this open | segments on disk |
| `backlog.pending_reseal` | owed re-seal at this open | segments on disk |
| `authority.segments` (CLI) | segment on disk | segments on disk |
| `authority.canonical` / `.accelerators` (CLI) | artifact class | a fixed `const` table |

No stream name, stream id, event type name, event type id, category id, or
batch id is ever a key, a label, or a field name. `stream_count` on a segment
row is a *number*. `registry_*_hwm` are *numbers*. A store with a million
streams produces exactly the same number of report entries as a store with
one.

Text/pretty rendering of `authority.segments` truncates at 20 rows;
`--format json` always carries the complete array (`Report::limit_display`).

## 4. `LogEngine::observability()` — field reference

### `owner` — append-owner saturation, group shape, outcomes

| field | unit | meaning |
|---|---|---|
| `durability_mode` | — | `process` \| `os` \| `group` (+ `max_delay_nanos`, `max_bytes`) |
| `queue_slots_in_use` / `queue_slots_capacity` | intents | instantaneous occupancy of the bounded owner channel and its fixed bound |
| `queue_bytes_in_use` / `queue_bytes_capacity` | bytes | byte permits held (admitted intents **and** producer preparation in flight) and the ring bound |
| `slot_saturation()` / `byte_saturation()` | ratio | the two above, divided |
| `group_width` | **intents** | distribution of gathered group size, one sample per group |
| `group_wait` | nanos | gather-window duration per group — the owner-side queue delay |
| `ack_latency` | nanos | admission to outcome, one sample per **append batch** |
| `commit_latency` | nanos | the owner's durable-commit span per group (write + barrier) |
| `conflicts` | count | appends refused for an expected-version mismatch |
| `cancellations` | count | appends whose caller dropped its future before the outcome landed (the events still committed — `bn-3nz`) |
| `groups` / `batches` / `events` / `bytes` | count / bytes | committer throughput, `$registry` records included |
| `outcome_scratch_slots` / `_bytes` / `_trims` | slots / bytes / count | owner result-scratch retention |
| `append_input` | counts / bytes | `AppendInputMetrics` — owned vs borrowed submissions and defensive copies |

**Write latency** is not measured separately. `commit_latency` is the write
plus barrier span as the owner sees it and `durability.barrier` is the barrier
alone, so the difference is the write half; a dedicated write timer would have
to live in `mess-log`'s committer, which this surface deliberately does not
modify.

### `durability`

`mode`, `barrier` (nanos), `barrier_degraded` (sticky, spec 03 §2.6),
`barrier_degraded_trips`, `barrier_threshold_nanos`, `seal_barrier`,
`seal_barrier_degraded`, `seal_barrier_degraded_trips`, `poisoned`,
`fold_chain_enabled`.

`barrier_degraded` means **slow**. `poisoned` means a barrier **fault** froze
the store: writes fail fast, reads clamp to the frozen watermark. They are
different fields because they are different incidents.

### `state`

`log_format_version` (always 3), `published_watermark`, `durable_watermark`,
`active_index_applied_end`, `active_segment_age_secs`,
`sealed_segment_count`, `sealed_install_generation`,
`sealed_index_resident_bytes`, `block_cache_{entries,bytes,hits,misses,hit_rate}`,
`registry_{stream,category,event_type,dict}_hwm`, `recover_payload_decodes`.

The three watermarks are three different frontiers and none substitutes for
another: `durable_watermark` (the owner acked it) ≥ `published_watermark` (a
reader can see it) ≥ `active_index_applied_end` is the ordering under load.
All three count `$registry` positions, so none of them is an
application-event count.

### `accelerators`

`seal_pack_enabled`, `seal_pack_segments`, `loose_sidecar_segments`, and one
`SealedSegmentReport` per installed sealed segment: `segment_id`, `base_pos`,
`event_count`, `stream_count`, `representation` (`seal-pack` \|
`loose-sidecar`), `pack_identity_hex`, `pack_format_version`, `dir_codec`,
`dir_codec_name`, `resident_bytes`, `sections_resident`, `has_payload_index`,
`has_event_types`, `has_registry_delta`, `install_generation`,
`active_evicted`.

`state.sealed_index_resident_bytes` is the sum of the rows' `resident_bytes`,
not a separate estimate.

### `fallbacks`

`sealed_candidates_refuted`, `sealed_candidates_quarantined`,
`quarantine_failures`, `refutations[]` (with the reason string per candidate),
`registry_delta_admitted`, `registry_delta_fallback`, `seals_skipped`.
`any()` is true when any of these ran.

None of these is data loss. Each names a place where the engine declined to
trust an accelerator and used the canonical log instead. What matters
operationally is whether they keep growing **across reopens** — that means the
store has not converged. `quarantine_failures > 0` (a read-only or full
filesystem) is the one that cannot converge on its own.

### `backlog`

`reseals_owed_at_open`, `pending_reseal[]`, `seal_queue_depth`,
`seal_jobs_dequeued`, `seals_completed`, `seals_skipped`, `seal_duration`.
`draining()` is `seal_queue_depth > 0`.

`seal_queue_depth` counts jobs queued or in progress: rolls reported by the
committer plus owed re-seals enqueued at open, minus jobs the sealer thread
has finished. It returns to zero on a quiescent store. Known inaccuracy: a
roll whose next-segment open then fails (`StoreFull`/`EIO` — an error the
append also surfaces) leaves the gauge one high until the next completion. The
decrement side saturates, so it can never wrap.

## 5. `mess doctor` — the `authority` section

`mess doctor <dir>` (all formats; `--format json` for the complete payload).

```
authority.scope                     = "offline"
authority.log_format_version        = 3
authority.canonical[]               = { name, role, on_loss }
authority.accelerators[]            = { name, role, on_loss }
authority.summary                   = { segments, served_by_seal_pack,
                                        served_by_loose_sidecar,
                                        served_by_log_scan,
                                        unsealed_segments,
                                        artifacts_present, artifacts_absent,
                                        artifacts_degraded }
authority.segments[]                = { segment_id, sealed, serving,
                                        serving_role, canonical_source,
                                        fallback, sealed_artifact,
                                        pack_identity, dir_codec,
                                        dir_codec_name,
                                        artifacts{ ".seal", ".pidx", ".pcol",
                                                   ".filter", ".reg", ".par" } }
```

`ArtifactClass::what` ("what the artifact holds") is in §1 and in the
`AUTHORITY_MODEL` doc comments, not in the report rows: it is reference
material, and carrying it would roughly double a `text`-format doctor run.
`on_loss` is the half an operator cannot look up fast enough mid-incident, so
that one is always in the payload.

`serving` is `seal-pack` \| `pidx` \| `log-scan` \| `unsealed` — which shape a
reader would actually use, following the engine's own dual-read preference
(`LogEngine::load_sealed`: the pack wins).

`sealed` (bn-3m62) is whether the segment's `.log` carries a footer trailer,
and it is what separates the last two values. A segment that is **not** sealed
yet — the live head, or one rolled but not yet roll-sealed — has no sealed
index *by design*: nothing is missing and no re-seal is owed, because sealing
writes the footer and the engine excludes the head from the re-seal queue
unconditionally. That is `unsealed`. `log-scan` is reserved for a segment that
**is** sealed and has no usable index anyway — the accelerator was lost,
refuted, or never landed — which is a real, actionable state.

Keeping them apart matters twice over. Every store has a live head, so folding
it into `log-scan` meant a perfectly healthy store could never report `ok`, and
the permanent count hid the one segment an operator actually needs to see.
`unsealed_segments` counts the first kind; `served_by_log_scan` counts only the
second.

Per-artifact state is four-way, and the distinction is the useful part:

| state | meaning |
|---|---|
| `present` | on disk and structurally valid |
| `absent` | not on disk — ordinary and expected (nothing sealed this yet; parity is opt-in; a pack carries `.pcol`/`.filter`/`.reg` as sections, not files) |
| `degraded` | on disk but unreadable, wrong-segment, or failing its own checksums — the only one worth waking up for |
| `shadowed` | inert because a higher-preference artifact serves this segment (a `.pidx` under a healthy `.seal`) |

One finding, `authority-accelerators`: `ok` when everything installed
validated (unsealed segments do not lower it — they are accounted for in the
`ok` message), `info` when some **sealed** segment is served by a log scan,
`warn` when any artifact is `degraded`. It is a *summary* — the per-segment
`sidecar-missing`
/ `seal-pack-corrupt` findings from the other doctor checks are the
per-artifact voice, and a second finding at a different severity for the same
file would make the report argue with itself. The severity never rises to
`error`, so the section cannot change `mess doctor`'s exit code on its own.

Validation depth matches the engine's: `SealedSegmentIndex::open_pack` /
`open`, i.e. header + section directory + trailer — "would the next open
install this?". Byte-level reassembly is `mess verify`'s question, not
doctor's.

## 6. What the engine cannot answer

Reported honestly rather than faked:

- **Active-index memory.** `mess-index`'s `ActiveIndex` has no byte
  accounting, and `snapshot()` is `O(N)` — unusable from a metrics call. Only
  the sealed tier reports exact `resident_bytes`. The active tier reports its
  watermark (`active_index_applied_end`) and nothing about its footprint.
- **Dedupe epochs / dedupe memory.** There is no dedupe. ADR 0002 §3 leaves
  exact batch idempotency undecided (`bn-2ctq`); `Backend::append_batch`
  carries no key. There is no state to report, so no field pretends there is.
- **Projection checkpoint / frontier lag.** ADR 0002 §2 leaves engine-level
  projection checkpoints undecided (`bn-11mk`); the supported pattern is an
  application-owned sidecar. The engine holds no checkpoint, so it reports no
  checkpoint lag. Per-subscription lag is `mess_log::subscription::SubMetrics`,
  a different (per-subscriber) surface.
- **Separate write latency.** See §4.
- **Snapshot-sidecar counters.** `pack_snapshot::SidecarMetrics`
  (`degraded_loads`, `roots_rejected`, `packs_rolled`, …) is a
  `PackSnapshotBackend` fact, not a `LogEngine` fact, and lives one layer up.
  It is reachable through `PackSnapshotBackend::sidecar().metrics` and is not
  folded into `EngineObservability`, which reports the engine.
