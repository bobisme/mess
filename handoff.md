# Handoff — Asterism Phase 4 integration and terminal rebaseline

**Written:** 2026-07-17
**Outgoing lead:** `mess-dev`
**Repository:** `/home/bob/src/mess`

This document supersedes the July 13 handoff that described the beginning of the
Asterism implementation program. Much more has landed since then. The immediate
job is no longer “implement the flat owner”; it is to finish the sealed,
independently reviewed Phase 4 rebaseline pipeline, run its single-use terminal
measurement, and publish the current-vs-Fjall-era performance budget.

## 1. Executive summary

The flat-owner/owned-append kernel and its supporting Phase 4 work are integrated
in the active Maw workspace and pass the full repository suite. The current
source checkpoint has independent code, lock-authority, and source-authority
approvals. A fresh current-child build also proved that the production-test
overlay compiles completely out of the release binary.

The final prepared A/B/C/D artifact build is the only immediate blocker. Its
first attempt, `bn-znj5-prepared-v3-r1`, failed closed before emitting any
benchmark rows:

```text
prepare-overlays: [Errno 2] No such file or directory:
'/home/bob/src/mess/.maw/workspaces/bn-znj5/None'
```

That root is diagnostic and must never be reused or promoted. The next lead
should locate which serialized optional value is being converted into the
repository-relative path `None`, fix it through a new bone/Maw workspace if it
is a product-code defect, independently review the fix, regenerate all
source-bound authority, finish the prepared build, and only then start the
single-use terminal runner.

There is no manual action required from Bob. Do not ask him to stop unrelated
processes. Authority and builds now use a physically separate offline
`CARGO_HOME`, so unrelated local Cargo work cannot perturb the lineage.

## 2. Live objective and bone chain

The immediate dependency chain is:

```text
bn-3ef   Phase 4 goal
  └─ bn-2l3n  Rebaseline flat-owner kernel vs Fjall-era and bare engines
       ├─ bn-znj5  Integrate/review rebaseline state machine        DOING
       └─ bn-3nl7  Terminal rebaseline and published budget        OPEN
                    depends on bn-znj5
```

- `bn-znj5` is the current implementation/authority task.
- `bn-3nl7` is the next execution task. It alone may emit the final correctness
  and performance rows from the approved, frozen artifact set.
- `bn-2l3n` is the umbrella rebaseline task and completes after `bn-3nl7`.
- `bn-3ef` is the Phase 4 goal.
- `bn-1gn1` (“Bound prepared construction memory without priority scheduling”)
  is another ready Phase 4 bone, but it is not part of this immediate authority
  chain. Do not silently switch to it before the rebaseline is finished.

The broader roadmap is intentional, not active clutter. After this handoff bone
is closed, the Asterism-labelled inventory should be approximately 118 bones:
63 done, 2 doing, and 53 open. Most open bones are dependency-gated Phase 5–10
work under `bn-o5b`, “Complete the post-flat-owner Asterism storage-engine
roadmap.” They preserve the rest of the Asterism integration plan; do not treat
all 53 as concurrently active.

## 3. Maw workspaces

At handoff time:

| Workspace | Meaning | Action |
|---|---|---|
| `bn-znj5` | Active exact Phase 4 source and authority checkpoint; intentionally stale relative to trunk | Continue here; **do not advance, sync, or rebase it**, and verify exact HEAD/tree after every trunk merge |
| `bn-2l3n` | Clean umbrella workspace | Retain until the rebaseline closes |
| `bn-1qsq` | Done review of old commit `86027c…` | Historical only; never merge. It may be recovery-snapshotted/destroyed |

The current source workspace is:

```text
/home/bob/src/mess/.maw/workspaces/bn-znj5
commit  2bd1dcc5bb2ff2ae7c291fe7a83ab3bc32b47381
tree    0087e1f4de9fc3608107d2291b6b7de6d1cc5573
status  clean, detached HEAD
```

`maw ws list` should label `bn-znj5` stale because trunk gained this handoff
after the authority was approved. That staleness is intentional. Do **not**
follow Maw's generic advance/sync hint: advancing or rebasing rewrites the exact
commit/tree and invalidates the current approval lineage. The bone is not done:
prepared artifacts and the terminal handoff remain outstanding.

The first handoff merge briefly auto-rebased the then-ephemeral workspace to
`bc65c131f9dd784388efa9484bc5c6e45f0aae8b`, changing only `handoff.md`.
That rebased state was recovery-snapshotted at:

```text
refs/manifold/recovery/bn-znj5/2026-07-17T22-23-33.291552008Z
```

The workspace was then safely recreated from epoch `664e0f987ebb` and
fast-forwarded to the exact approved `2bd1dcc5` checkpoint, restoring tree
`0087e1f4`. No source work was lost. It was created persistent, but a subsequent
documentation merge still auto-rebased it. Therefore **do not trust the
persistent flag as protection** on the installed Maw version. After every trunk
merge, and immediately before every authority-bound command, require:

```bash
test "$(maw exec bn-znj5 -- git rev-parse HEAD)" = \
  "2bd1dcc5bb2ff2ae7c291fe7a83ab3bc32b47381"
test "$(maw exec bn-znj5 -- git rev-parse 'HEAD^{tree}')" = \
  "0087e1f4de9fc3608107d2291b6b7de6d1cc5573"
```

If either check fails, stop. Inspect `maw ws history bn-znj5` and
`maw ws recover bn-znj5`; do not generate authority from the rewritten commit.

The trunk has unrelated/operational metadata changes in `.bones/` and `.seal/`.
Preserve them. Do not reset or clean the trunk.

All three previously spawned reviewers are finished; no subagent is currently
running:

- canonical handoff/code review: approved
- lock r23 review: approved
- source r2 review: approved

## 4. What is integrated and verified

The last integrated repair is commit `2bd1dcc5`:

- separates builder-local ASCII-canonical JSON replay from the strict UTF-8
  canonical parser used by reviewed/prepared authorities;
- fixes the real non-ASCII system-closure case
  `/usr/lib/go/test/fixedbugs/issue27836.dir/Þfoo.go`;
- keeps generic reviewed/prepared authority parsing strict UTF-8;
- rejects all 10 cross-encoding hostile cases;
- refreshes every live release-line-neutral overlay binding:
  - overlay SHA-256
    `db060c902d7d1a2664dcaea44525adac727b1bef1de32bb01b33be2561143a39`
  - reviewed overlay commit
    `86027c98605d9ea01c3e385b0702741723d5a538`
  - validator counts: 11 production, 17 self-test;
  - profile adapter binding updated and directly tested.

The repair originated as commit
`8e8b4f9936d47c62d16691872d7d90246fe4b9ca`. Seal review `cr-244cxl`
approved it with zero findings and zero threads. Review event-log SHA-256:

```text
908e41bc59aae38a77bf1132ba513bbed5800ef2330f0d3535cbfd1f2af8eb76
```

After integration, the exact current tree passed:

- `just fmt-check`
- focused Python, overlay, and build static tests
- all 58 profile-adapter tests
- `MESS_SNAPSHOT_LAW_ITERS=25 CARGO_NET_OFFLINE=true CARGO_HOME=<private> just test`
- result: 981 passed, 27 skipped, 2 slow; 151.509 seconds of test runtime
  after the clean build

No benchmark rows were emitted by these checks.

## 5. Private Cargo authority

Every authority or build producer must use:

```bash
export PYTHONDONTWRITEBYTECODE=1
export CARGO_NET_OFFLINE=true
export CARGO_HOME=/home/bob/.cache/mess-bench/asterism-rebaseline/bn-znj5-cargo-home-v3-r1
```

The private Cargo home is 7.2 GiB with 225,566 entries. It is physically
distinct from `~/.cargo` and has zero inode overlap. It contains the offline
registry/git/cache material required by the pipeline, not user credentials or
installed bins. Tool identities are pinned separately.

This isolation was added after unrelated local Cargo work mutated
`~/.cargo/.global-cache` during earlier lineages. It solves the interference
without weakening any guard. Do not revert to shared `~/.cargo`, stop unrelated
user processes, or loosen the authority checks.

The parent claims may have expired by the time this is read. Check first:

```bash
rite claims list --agent "$AGENT"
```

When continuing `bn-znj5`, stake only these scoped claims:

```bash
rite claims stake --agent "$AGENT" \
  "resource://mess/cargo-home-authority" \
  "bone://mess/bn-znj5" \
  "workspace://mess/bn-znj5" \
  -m "bn-znj5 isolated Cargo-home authority and Phase 4 integration" \
  --ttl 3600
```

Never use `rite claims release --all`; release only claims owned by your task.

## 6. Fresh approved authority lineage for `2bd1dcc5`

These artifacts are immutable evidence. Preserve all diagnostic and successful
roots. Never overwrite an existing output root.

### 6.1 Base tools r22

```text
root:
/home/bob/.cache/mess-bench/asterism-rebaseline/bn-znj5-base-tools-v3-r22

manifest:
asterism-rebaseline-tools.json

SHA-256:
68b9a132d7d554afbb462f827317adf54442315671182a9cff3297123fe47bdc
```

`base-tools-v3-r21` is diagnostic/non-reusable. Its output argument accidentally
created a nested
`asterism-rebaseline-tools.json/asterism-rebaseline-tools.json`.

### 6.2 Resolution-only locks r22

```text
root:
/home/bob/.cache/mess-bench/asterism-rebaseline/bn-znj5-locks-v3-r22

manifest SHA-256:
5768c74f1aab0c07de7703601ba3319d1697bc5eb8bac26561eb4ccf3308df75

final lock SHA-256:
A/B  9c24189940d9b43d7798c6680c8aeab6ddc270ef9b450390334d9327405cbea0
C    36c49c89776c15aafdbdee969c25a5289e4860548c4c4005bb4535786d5b93fd
D    a51304c875ed957aa0dbb21c65cd1fbf6ba5df166786d69c5b6819e493d072d1
```

### 6.3 Lock review/authority r23

```text
root:
/home/bob/.cache/mess-bench/asterism-rebaseline/bn-znj5-lock-review-v3-r23

assertion SHA-256:
9d7560c87d9ab66e767bdc42ff63d5c4f7bba27dca38ab4801be7994da96d495

Seal:
cr-2x9pjy

Seal events SHA-256:
ea846bc9aa8db9468e433220161c5ef5e0dca7b2306bc84521161aee1c3bf314

source-review bundle SHA-256:
d969547dab415b1cb7a0185f8d279d5c1fb6ce57312e8df37005fed4f0bcdf11

authority SHA-256:
25412caafcfd59028f42c678766b7e2c73b015ec49dab429b20c87b69e66f978
```

Independent authority validation returned `status=ok` and the same authority
and manifest hashes. The Seal review was bound to exact commit/tree
`2bd1dcc5…` / `0087e1f4…` with zero findings and zero threads.

### 6.4 Current children r9

```text
root:
/home/bob/.cache/mess-bench/asterism-rebaseline/bn-znj5-current-children-v3-r9

attestation SHA-256:
e22fbbf5ade6649287954abc452b014ba1c2d1e384a5ed5ebf72d6e2aa7b5f9e

final tools SHA-256:
7a409b4772ec000962d7b4aeb9468cc41d53da6f070acc49d21268d5b0cffa01
```

Release compile-out proof:

```text
pristine == overlay-without-test
size:    5,688,448 bytes
SHA-256: 30c8c7eb2de304ff18967eca315c8e36d0d869c11dccab5b08736de044c04550

exact nm inventory SHA-256:
f2d6dda0328722cfd7a1f1116bc6f0dea9c6a1c68a14463beafa8c70b922c997

forbidden hook strings:
absent

correctness tool SHA-256:
2449c4ac3e4861ecd7401f2718bd02f7dfe6b071c0c70f33e1768e28091f7eaa

fault tool SHA-256:
e5a4cdd1ee20b882fed404f709834064fb729023df29c4e3e32cf6f193d866ef
```

r9 release/fault hashes differ from the older r8 lineage. That is expected;
within-r9 equality and fresh authority passed. r9 is the source-bound evidence
for the current commit. No benchmark rows were emitted.

### 6.5 Source review r2

```text
root:
/home/bob/.cache/mess-bench/asterism-rebaseline/bn-znj5-source-review-v3-r2

source assertion SHA-256:
314f628d7adaed56c5758513b955267629a330e63e3fda9cf76a270e56c7807a

Seal:
cr-gr7zun

Seal events SHA-256:
2d8807f324c07c3388d870838b0f4e76d563a62b440c6fceda661908c2df52d0

source bundle SHA-256:
e4d8691bc8cfb62a8190eb3a975c488f1004fd5208f65d1e0a7292ddad5ab237

source approval:
/home/bob/.cache/mess-bench/asterism-rebaseline/bn-znj5-source-review-v3-r2/source-approval.json

source approval SHA-256:
3d2f86d6b2a9f13447bca6629348ecca13bbd92ad583f387622d9427daeb09e5
```

The approval is mode `0444`, size 383,056 bytes, schema
`bn-2l3n-source-approval-v3`, status `approved`, and binds exact commit/tree
`2bd1dcc5…` / `0087e1f4…` and review `cr-gr7zun`.

The independent reviewer reproduced the assertion byte-for-byte in 299.636
seconds and replayed all r9 semantic live roots. This successful source
assertion conclusively proves the earlier `Þfoo.go` handoff failure is fixed.

## 7. Current failure: prepared build r1

The failed root is:

```text
/home/bob/.cache/mess-bench/asterism-rebaseline/bn-znj5-prepared-v3-r1
```

It ran for roughly 8.5 minutes and failed closed before any benchmark rows. It
materialized part of variant A and its product overlay, so the exact stopping
point can be narrowed from the files already present:

- `materialized/A`
- `materialized/A-product-overlay`
- A and A-product-overlay source archives/manifests
- semantic source/toolchain/Cargo-home manifests
- build/contract logs
- frozen binding copies for tools, locks, children, and source approval

The terminal error was:

```text
[Errno 2] No such file or directory:
'/home/bob/src/mess/.maw/workspaces/bn-znj5/None'
```

Treat the entire root as diagnostic/non-reusable. Do not delete it, edit it,
resume it, or use its partial artifacts as authority.

Likely class of defect: a serialized optional field is being passed through
`str(value)` and then resolved relative to the repository, yielding
`repository / "None"`. This is only a hypothesis; prove the exact consumer
before editing.

Primary code:

```text
spikes/asterism_rebaseline/tooling/prepare_overlays.py
spikes/asterism_rebaseline/tooling/authority_inputs.py
spikes/asterism_rebaseline/tooling/current/prepare_children.py
spikes/asterism_rebaseline/tooling/current/build_children.py
```

Recommended bounded diagnosis:

1. Inspect the diagnostic root filenames and timestamps to identify the exact
   build phase. Do not rerun the full build blindly.
2. Enumerate `null`/`None` values in:
   - source r2 `source-approval.json`, assertion, and bundle;
   - current-child r9 attestation and tools manifest;
   - lock r22/r23 manifest, bundle, and authority;
   - the frozen copies under prepared r1 `bindings/`.
3. Search `prepare_overlays.py` and helpers for:
   - `resolve_repository_input`
   - `Path(str(...))`
   - `repository / str(...)`
   - path-bearing fields read from source approval, overlay descriptors,
     adapter descriptors, support files, or tool identities.
4. Reproduce only the validation/materialization substep that consumes the
   suspected field. The top-level tool catches the exception and suppresses a
   useful traceback, so importing the module and calling bounded substeps may be
   faster than another full producer run.
5. Distinguish legitimate optional `null` fields from fields whose schema
   requires a path. Do not globally reject or stringify all nulls.

If the cause is product code:

1. Create a new bug bone.
2. Create a fresh Maw workspace from exact `bn-znj5` commit `2bd1dcc5`.
3. Implement a narrow repair plus positive and hostile tests.
4. Run focused gates and an independent Seal review in another fresh Maw
   workspace.
5. Integrate only the reviewed tree into `bn-znj5`.
6. Run `just fmt-check` and the proportional/full test gate.
7. Regenerate the entire source-bound r22/r23/r9/r2 lineage under the private
   Cargo home. Old authority remains immutable regression evidence but becomes
   stale immediately after any source change.

If the cause is only an incorrect producer argument or non-source artifact
binding, document the proof on `bn-znj5` and retry into a fresh, absent prepared
root. Never “fix” an immutable artifact in place.

## 8. What happens after prepared construction succeeds

The successful prepared root must:

- bind the exact approved source, locks, toolchain, and child tools;
- freeze all A/B/C/D artifacts;
- pass its own independent validation;
- contain no diagnostic residue or benchmark rows;
- be made immutable according to the protocol.

Then finish/merge `bn-znj5` according to the Edict protocol and start
`bn-3nl7`. Read:

```text
spikes/asterism_rebaseline/BN-2L3N-PROTOCOL.md
spikes/asterism_rebaseline/run_rebaseline.py
spikes/asterism_rebaseline/evaluate.py
spikes/asterism_rebaseline/verify_terminal.py
spikes/asterism_rebaseline/evidence_schema.py
```

`bn-3nl7` is a single-use terminal run:

- start from row zero;
- acquire the global lease;
- run correctness/fault/reopen gates first;
- run all frozen balanced matrices and focused profiles exactly as specified;
- retain raw evidence;
- evaluate once and terminal-verify;
- publish a plain-English comparison against both the Fjall-era engine and the
  fresh bare engine;
- update later Asterism budgets and admitted paths from the terminal result.

Do not emit exploratory “almost final” rows with the terminal runner. Failed,
partial, or diagnostic attempts must remain clearly non-authoritative.

## 9. Current performance interpretation

There is not yet a fresh terminal Phase 4 number that can honestly be published.
Do not promote older spike results or partial authority runs as the final
current-vs-Fjall comparison.

What is already established:

- the flat-owner design beat the old composed engine across the original 32
  measured spike cells;
- the owner must itself be the committer—fronting the old committer lost about
  35%;
- speculative B1 pipelining was rejected because it lost 5–18% at high durable
  concurrency;
- the owned/interned append path and release integration are now real engine
  paths, not spike-only code;
- full correctness remains green after integration.

The terminal rebaseline exists because the implementation has evolved
substantially since those measurements. The honest plain-English summary today
is: the architecture has already removed the expensive Fjall-era orchestration
from the critical append path and prior controlled evidence says it should win,
but the exact size of the current win—and whether every batch-size/tail-latency
budget passes—remains pending `bn-3nl7`.

## 10. Safety and workflow rules

- Read `AGENTS.md` and use the Edict workflow.
- Make every source change in a bone-named Maw workspace, never directly in the
  trunk.
- Use `maw exec <workspace> -- <command>` for child workspaces.
- Run `maw ws merge <name> --into default --check` before any destructive merge.
- Never merge or destroy `default`.
- Never manually create Git branches.
- Preserve unrelated dirty trunk/workspace changes.
- Before assuming a destroyed workspace lost work, run `maw ws recover`.
- Post progress comments to the active bone for crash/session recovery.
- For risk-high source or authority changes, use a separate adversarial reviewer
  bound to the exact commit/tree.
- Keep builds local/offline. Do not use Cyber/security scans or external
  security tooling for this work unless explicitly authorized.
- Do not weaken namespace, mount, descriptor, toolchain, canonicalization,
  compile-out, or immutable-authority guards to get a run through.
- Use fresh absent output directories for every retry.
- Do not delete diagnostic evidence roots.
- Run `just fmt-check`; for the final source checkpoint run
  `MESS_SNAPSHOT_LAW_ITERS=25 just test` under the private offline Cargo home.
- Workers do not push. The lead owns integration.

## 11. First commands for the next lead

```bash
cd /home/bob/src/mess
cat handoff.md
edict protocol resume --agent "$AGENT"
bn show bn-znj5
bn show bn-2l3n
bn show bn-3nl7
maw ws list
maw exec bn-znj5 -- git status --short --branch
maw exec bn-znj5 -- git log --oneline -5
rite claims list --agent "$AGENT"
```

Then continue the bounded prepared-r1 diagnosis in
`/home/bob/src/mess/.maw/workspaces/bn-znj5`; do not start a new implementation
from trunk, do not advance/rebase the persistent workspace, and do not rerun the
terminal measurement.
