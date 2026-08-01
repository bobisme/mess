# Golden store directories (format-stability e2e)

On-disk compatibility **is** the contract. A golden is a small, complete store
written by a released format version and committed to the repo; every future
version must open it, recover it, replay it byte-exact, and verify it — forever.

Each `vN/` holds:

| File | What it is |
|---|---|
| `store.tar.zst` | A complete store directory (segments + sealed `.pidx`/`.pcol` sidecars + finalized trailers + the fjall `meta` name registry + a nested app snapshot sidecar), packed with the system `tar` + `zstd`. |
| `expected-events.json` | The manifest: full global event order (byte-exact payloads as hex), per-stream heads, the cold-tier stream, and the chained stream's frozen fold-chain hashes + snapshot. |

`vN` tracks the **on-disk format version** (spec `docs/spec/01` — currently
format v3). The generator and check live in
[`crates/mess-cli/tests/golden.rs`](../../crates/mess-cli/tests/golden.rs).

## What the check proves (runs in normal CI)

`golden_v3_opens_and_verifies` unpacks each committed golden and, with the
**current** code:

- runs full recovery over the whole segment chain (`LogEngine::open`);
- confirms the registry / interned names hydrated (stream + type strings);
- replays every event byte-exact against the manifest (global order);
- serves a fully-sealed stream from the **cold** tier, byte-exact;
- proves the committed **pre-pack** snapshot sidecar misses safely and is left
  untouched (see the compatibility statement below);
- runs `load_verified` green on the chained stream and checks its
  genesis/chain/head hashes against the frozen manifest (fold-chain
  format-stability);
- runs `mess verify --full` and requires exit 0.

The check is parameterized over `(version, chain)`; `golden_v3_opens_and_verifies`
and `golden_v4_opens_and_verifies` are thin wrappers. `v4` (`bn-3l0`) opens the
engine with `chain: true`, so its segments carry the **real on-disk**
`crypto_chain` — `mess verify --full` recomputes the fold chain against the
actual stored bytes, and the check additionally asserts every batch on disk
carries the chain (flag bit 0). `v3` stays plain-frame (chain off) and immutable.

## Reviewed compatibility statement: the snapshot sidecar (bn-3l8n)

`v3` and `v4` were generated when the app snapshot sidecar was a fjall head
table plus a positional blob directory at `store/snapshots/{meta,blobs}`.
`bn-3l8n` replaced that sidecar with the **pack sidecar**
(`<store>/.snapshots.packs/`: immutable packs plus a discovery root). Goldens
are immutable, so the committed bytes keep the old layout, and the current
reader does not understand them.

That is not a broken contract — it is the contract:

> A snapshot is **discardable acceleration**. An absent, foreign, unknown, or
> corrupt sidecar is a **miss**, never an error and never a wrong answer, and
> the store answers by replaying the events.

So the check did not lose an assertion, it changed which one it makes:

- step 6 now proves the committed `store/snapshots/` directory yields `None`
  (not an error) through the pack reader, and that the read-only open creates
  nothing in it (no `LOCK`, no `IDENTITY`, no repair);
- the snapshot's *format stability* — the state blob and both semantic hashes —
  is pinned in step 7, derived from the committed payloads via `build_cert` /
  `take_snapshot` rather than read back out of a sidecar. A change to the
  state-blob encoding still breaks the golden.

Every other assertion (recovery, registry hydration, byte-exact replay, cold
tier, fold chain, `verify --full`) is untouched and still runs on the untouched
bytes. The generator writes a **pack** sidecar, so the next `vN` minted will
pin the new layout positively and step 6 becomes a load assertion for it.

## The rule: OLD GOLDENS ARE IMMUTABLE

A committed `vN/` is **frozen**. Never regenerate, re-pack, or edit an existing
golden to make a change pass — that silently redefines the very contract the
golden exists to defend. The only permitted edits to an existing `vN/` are (a)
this README and (b) a check that got *stricter* while still passing on the
untouched bytes.

## When to add a new `vN`

Add a new golden (bump `N`) on **any format-affecting change**, e.g.:

- a batch / subframe / segment-header / trailer / extension-section layout
  change (spec `01`);
- a sealed sidecar (`.pidx` / `.pcol` / `.filter`) format change;
- a registry / meta-table (`stream_names`, `type_names`, `snapshot_heads`)
  encoding change;
- a snapshot-record or state-blob layout change;
- a fold-chain / certificate hash-derivation change (spec `05`).

If a format change ships without either a new `vN` **or** a migration that keeps
old goldens opening, the check fails by construction — that is the point.

> Scope note: `v3`'s composed `LogEngine` backend emitted PLAIN frames (no
> per-batch `crypto_chain`), so its fold-certificate contract is pinned against
> the chained stream's committed payloads via `mess-log`'s spec-conformant
> `build_cert` construction (hashes frozen in the manifest), not against on-disk
> chain bytes. As of `bn-3l0` the engine emits a real on-disk chain when opened
> with `EngineOptions { chain: true }`; **`v4`** is the golden whose segments
> carry it, so there `mess verify --full` validates the actual stored
> `crypto_chain`. Both goldens keep the `build_cert` payload-derived hashes in
> their manifests for hash-derivation stability.

## Regenerating (only when creating a NEW `vN`)

```bash
cd <workspace>
# Point tmp at a non-tmpfs dir so durable writes are honest.
TMPDIR=$HOME/.cache/mess-test-tmp \
  cargo test -p mess-cli --test golden -- --ignored --nocapture generate_golden_v3
```

The generator is deterministic: fixed corpus, no wall-clock in payloads,
single-threaded sequential appends. Two runs produce an identical
`expected-events.json` and two stores that both pass the check. The store
**bytes** are not byte-compared (fjall embeds internal timestamps) — goldens are
opened and verified, never diffed whole. Keep each golden under ~2 MiB
compressed. To mint a new version, copy the generator to target `v{N}` and
adjust it to the new format.
