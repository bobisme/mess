# Golden store directories (format-stability e2e)

On-disk compatibility **is** the contract. A golden is a small, complete store
written by a released format version and committed to the repo; every future
version must open it, recover it, replay it byte-exact, and verify it — forever.

Each `vN/` holds:

| File | What it is |
|---|---|
| `store.tar.zst` | A complete store directory (segments + sealed `.pidx`/`.pcol` sidecars + finalized trailers + the fjall `meta` name registry + a nested `snapshots/` store), packed with the system `tar` + `zstd`. |
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
- loads the snapshot with its `fold_version` and byte-exact blob;
- runs `load_verified` green on the chained stream and checks its
  genesis/chain/head hashes against the frozen manifest (fold-chain
  format-stability);
- runs `mess verify --full` and requires exit 0.

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

> Scope note: the composed `LogEngine` backend does not yet emit the optional
> per-batch `crypto_chain` bytes into its segments, so the fold-certificate
> contract is pinned against the chained stream's committed payloads via
> `mess-log`'s spec-conformant `build_cert` construction (hashes frozen in the
> manifest), not against on-disk chain bytes. When the engine grows a real
> on-disk chain, add a new `vN` whose segments carry it.

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
