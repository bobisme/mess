# Snapshots: the public API and its laws

- Status: current
- Bones: `bn-ozi5` (storage core), `bn-3l8n` (adoption), `bn-2gns` (this API)
- Normative source: [ADR 0002 §1](adr/0002-asterism-capability-authority.md)

This is the contract page for `mess_store::snapshot`. It states what each public
value means and what a caller may rely on. Where it and ADR 0002 §1 disagree,
the ADR wins and this page is a bug.

## 0. The one law

**A snapshot is discardable acceleration. The event log is the sole authority.**

Absence, corruption, an unknown format, a deleted pack, a truncated root, an
identity this binary does not recognize, a whole sidecar directory removed —
every one of them is a **miss**, and a miss is a full replay. None of them is an
error, and none of them can make the event store unavailable. Nothing in the
snapshot subsystem ever authorizes deleting a log prefix.

Two consequences worth stating outright, because they are what make everything
else simple:

- **There is no snapshot migration, ever.** A snapshot written by an older build
  under an older identity or an older on-disk format is not converted, not
  repaired, and not read. It misses, and the aggregate is rebuilt from the log.
  See §5.
- **Correctness never rests on a stored blob.** It rests on the
  snapshot-equivalence law — `fold(s0, all) == fold(snapshot, tail)` — which the
  `snapshot_law` property test exercises over thousands of random prefixes.

There is exactly one loud failure in the whole subsystem, and it is not about
snapshot data: see §2.

## 1. Identity

A record's identity is a `SnapshotCompatibility`, and **stream name plus that
value is the complete lookup key**.

```rust
pub struct SnapshotCompatibility {
    pub aggregate_schema_id: StableSnapshotId,
    pub fold_version:        u32,
    pub codec_id:            StableSnapshotId,
    pub codec_version:       u32,
}
```

| field | what it names | bump it when |
|---|---|---|
| `aggregate_schema_id` | the aggregate and its state schema, for all time | never |
| `fold_version` | the semantics of `Aggregate::apply` | `apply` changes, including newly handling a previously-ignored event type |
| `codec_id` | the state codec | the state representation is swapped for a different one |
| `codec_version` | that codec's byte shape | `decode_state` can no longer read what the previous `encode_state` wrote |

Rules:

- **Two records are interchangeable iff their compatibilities are equal.** A
  store never falls back from one identity to another, never upgrades a record
  in place, and never lets one identity hide or delete another's head. A
  different identity is a different key, full stop.
- **A `StableSnapshotId` is author-supplied and never derived.** Not from
  `type_name`, not from `TypeId`, not from a hash, not from anything the
  compiler or the build decides — those change under a rename, a refactor, a
  dependency bump, or a rebuild, and an identity that changes by accident is an
  identity that stops working by accident. `StableSnapshotId::new` is a
  `const fn` whose validation runs at compile time, so the spelling is a literal
  in the source.
- **One canonical encoding.** Lowercase ASCII letters, digits and `. - _ : /`,
  1..=64 bytes. No case folding and no normalization, so two ids are equal
  exactly when their bytes are equal.

The smallest honest implementation declares two things; the codec fields default
to the aggregate's own name and version 1:

```rust
impl Snapshottable for User {
    const AGGREGATE_SCHEMA_ID: StableSnapshotId =
        StableSnapshotId::new("social.user");
    const FOLD_VERSION: u32 = 1;

    fn encode_state(&self) -> Result<Vec<u8>, StateCodecError> { … }
    fn decode_state(bytes: &[u8]) -> Result<Self, StateCodecError> { … }
}
```

Override `CODEC_ID`/`CODEC_VERSION` when the codec has a life of its own — the
`examples/social` aggregates all serialize through one shared hand-rolled format
(`social.length-prefixed`), so its wire shape can be versioned once for all four
without touching any `FOLD_VERSION`.

## 2. The one loud failure

Two aggregates in one process claiming one complete `SnapshotCompatibility`
share a head key, so they would overwrite and mis-decode each other's snapshots.
That is a program bug, not a storage condition, and it is reported —
`StoreError::SnapshotIdentity` — at the first snapshot save or accelerated load
of the second aggregate, naming both Rust types.

The check is on the **complete** identity, not the schema id alone, because the
harm is a shared key. Two types sharing a schema id at different fold versions
are what a fold bump looks like from inside one process (the old fold and the
new one, or a migration test holding both); they cannot touch each other's
heads, and flagging them would make honest deploy modelling impossible while
catching nothing.

`register_snapshot_identity` keys its registry by `TypeId`. That is a runtime
diagnostic only; no `TypeId` is ever persisted or hashed into an identity.

## 3. Coverage

```rust
pub enum SnapshotCoverage { Empty, Through(u64) }
```

The total order is exactly

```text
Empty < Through(0) < Through(1) < … < Through(u64::MAX)
```

Compare `SnapshotCoverage`; never compare a raw covered version. "Summarizes
nothing" and "summarizes event 0" are different values and can never collide —
sequentially or under concurrent publication, both of which are tested in both
directions.

## 4. Publication

Within one `(stream, compatibility)` key, publication is monotone in coverage,
and a save never destroys a *valid* record it cannot prove obsolete:

1. a strictly **higher** coverage supersedes the head → `Published`;
2. a strictly **lower** coverage is refused → `CoverageRegressed`;
3. at **equal** coverage the current record is read and fully validated *first*:
   - invalid or unreadable bytes may be superseded → `Repaired`,
   - identical bytes → `Idempotent`,
   - different valid bytes → `Conflict`, and the current record stays.

Validating before superseding is what lets a corrupt head be healed without
weakening rule 3.

### Why rule 3 is a conflict and not last-write-wins

`bn-ozi5` shipped last-write-wins at equal coverage, and the lead ruling on
`cr-2ywo0t` accepted that for v1 while transferring the ADR's conflict rule to
this bone as a requirement. It is now implemented, for two reasons beyond the
ruling:

- **The seam can finally say so.** LWW was chosen partly because `save_snapshot`
  returned `()` and had no way to report a refusal. It now returns a
  `SnapshotSaveOutcome`, so a conflict is visible instead of silent.
- **LWW was hiding a real bug class.** Two records claiming to fold the same
  prefix of the same stream under the same identity to different states cannot
  both be right. The cause is a non-deterministic fold, or a state encoding
  changed without a `codec_version` bump. Alternating between the two answers
  makes that undiagnosable; keeping the older evidence and reporting
  `Conflict` (plus a `SidecarMetrics::conflicts` counter) makes it obvious.

Nothing depended on LWW: a fold bump no longer needs to overwrite anything,
because it writes under a different key.

## 5. What happens to old snapshots

They miss, and the aggregate is rebuilt. That is the entire disposition, and it
is a deliberate one rather than an omission.

`bn-2gns` bumped both on-disk formats — the record body and the root descriptor
— to carry the stable identity. A decoder that meets a format it does not
understand returns "no", which every caller turns into a miss. So:

- every root written by an older build is rejected, which means an older sidecar
  resolves to **no heads at all**;
- every record written by an older build is unreadable even if a leaf named it;
- a read-only open of such a sidecar creates nothing, repairs nothing, renames
  nothing and deletes nothing — it simply has no snapshots;
- a writer opening the same directory misses too, and publishes new-format
  artifacts alongside the old ones without touching them.

This is tested end to end against a hand-planted, internally consistent
pre-identity sidecar (`a_pre_identity_sidecar_misses_cleanly_and_is_never_touched`),
and at the byte level against frozen v1 record and root layouts
(`a_record_body_written_by_the_previous_format_never_decodes`,
`a_root_written_by_the_previous_format_never_decodes`). The cost of the choice
is one cold rebuild per stream after an upgrade; the benefit is that no
migration code exists to be wrong.

## 6. Trust

```rust
#[non_exhaustive]
pub enum SnapshotTrust { UnverifiedCache }
```

`UnverifiedCache` means both semantic hashes are absent. Compatibility,
coverage, bounds, codec validation and the record hash protect routing and
physical integrity; an *honest producer* is assumed. The record hash is never
evidence that the state is the result of the fold.

ADR 0002 §1 defines a second mode, `CertifiedSnapshotRef`, carrying a
`state_hash` over the encoded state and an `event_prefix_hash` — the canonical
event-prefix certificate for `(stream, coverage)`. **It is deferred, and no
variant for it exists yet**, because its precondition does not: `mess-log`'s
`certificates` module owns the verification algorithm over an in-memory
`StreamCert` view, but nothing wires a stream's on-disk chain value `h[v]` out
to the snapshot layer — `Backend` exposes no way to ask for one. Writing a
record that claimed certification without both hashes would be a lie, and
building the variant with nothing able to populate it would be a stub.

The enum is `#[non_exhaustive]` precisely so adding that variant later is not a
source break. That is the whole forward-compatibility provision, and it is why
this deferral does not cost a second trait break.

## 7. Administrative enumeration

`Snapshottable` and `SnapshotStore` are the seams application code uses.
Enumeration is deliberately **not** on them: it is a property of a store that
has a discovery root, its only callers are offline tools that hold such a store
concretely, and `EventStore` must never enumerate heads on the ordinary path.

```rust
Sidecar::pin_root() -> Option<PinnedSnapshotRoot>
Sidecar::scan(&pin, cursor, limit) -> SnapshotScanPage
```

- A **pin** names exactly one validated root generation and holds a shared,
  non-mutating lease on it. Pinning takes no lock and creates nothing, so a
  read-only tool may pin a live store's root.
- A **cursor** binds the root it was minted against plus the last key returned.
  Presented against a different root it is `Rejected`, never reinterpreted — the
  key may mean something else there. It is intentionally not serializable: paging
  happens inside one command, so a wire form would be public API with no caller.
- A **page** holds at most `limit` entries, capped at `MAX_SNAPSHOT_SCAN_LIMIT`.
  Every entry is validated (frame read, leaf binding checked, record decoded); a
  head that does not validate is skipped and *counted*, never fabricated and
  never repaired.
- `SnapshotScanDiagnostic::Complete` is the **only** value that licenses a
  destructive caller to act on what it read. `Partial` and `Rejected` fail
  closed. A destructive caller must also revalidate the same pin before
  mutating.

Scan order is lexicographic by stream name then by compatibility, so one stream
snapshotted under two identities is two entries.

`mess doctor` / `inspect` / `retention explain` read the sidecar through this
path (`mess_cli::metaread`): peak memory is one page rather than one entry per
head, the inventory comes from one root generation rather than smeared across
concurrent publications, and skipped heads surface as `MetaFacts::degraded`.

### The honest cost note

`pin_root` is `O(1)`, but the thing it pins is v1's **flat** root, which keeps
every head resident (`bn-ozi5`'s documented deviation). So a pin borrows an
`O(N)` list instead of walking an `O(height)` page path, and the sidecar holds
each head twice — once for `O(1)` point lookup, once in scan order. ADR 0002's
copy-on-write discovery tree removes both together, and **no signature on this
page changes when it does**.
