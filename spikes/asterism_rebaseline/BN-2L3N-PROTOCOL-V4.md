# bn-2l3n Asterism production rebaseline protocol — version 4

## Status and decision boundary

The canonical protocol identity is `bn-2l3n-asterism-rebaseline-v4`.

Version 4 supersedes the version-3 document (SHA-256 of the frozen v3 copy in
`bn-znj5-prepared-v3-r8`: recorded in `SUPERSEDES.json` alongside this file)
before any accepted timing row was produced. No rebaseline benchmark evidence
exists from versions 1–3; eight v3 terminal attempts ended pre-decision
(seven fail-closed pre-lease/pre-claim, one INCONCLUSIVE at the first
cpu_profiles smoke after claim consumption, zero timing rows accepted).

**What v4 changes and why.** Versions 1–3 wrapped the measurement in a sealed
evidence-custody chain: staged authority lineages with per-stage independent
review, byte-canonical vote strings, single-use prepared artifacts, frozen
interpreter/comm identity binding, and a rule that any source change
invalidates every downstream artifact. In five days of operation, the majority
of failures were the custody machinery disagreeing with itself (manifest
ordering conventions, ASCII/UTF-8 canonicalization splits, validator
expectations diverging from reviewed producer output), each costing a
multi-hour lineage regeneration. The custody chain defended against evidence
tampering and silent substitution — threats disproportionate to this
artifact's actual audience (this repository's own roadmap decisions) and
already substantially mitigated by pinned sources, deterministic builds, and
recorded hashes. Version 4 keeps every element that makes the *measurement*
trustworthy and removes the elements that only made the *custody* elaborate.

**Unchanged from v3 (normative by reference).** The following v3 sections are
adopted verbatim, with only the mechanical substitutions listed under
*Adaptations* below:

- **Exact variants and source binding** — the A/B/C/D variant table, product
  commits/trees, timed surfaces, lockfile hashes, adapter constraints, and the
  C/D comparator rules, including the prohibition on synthetic Fjall
  substitutes and on the superseded pre-optimization checkpoint as `D`.
- **Primary 32-cell matrix** — cells, Williams blocks, work counts, fresh
  stores, segment sizing, the three exact measurement phases, and latency
  accounting.
- **Focused public topologies** — all-new-name, fairness/owner-saturation,
  CPU and syscall sentinel profiles, reopen/peak-RSS, exactly as specified.
- **Metrics and row invariants**, **Statistics and comparisons**, and every
  **Locked performance gate** (A/D preservation, A/C Fjall-era comparison,
  fairness/boundedness/write-shape), with unchanged thresholds.
- **Correctness, recovery, and fault gates** — the full suite, including the
  reviewed `cfg(test)`-only fault seam and its isolation from performance
  binaries.
- **Terminal outcomes** — `ADMIT` / `NARROW` / `REVERT` / `INCONCLUSIVE`
  definitions and their consequences, including that reversion is a reviewed
  product action and that partial rows of a failed accepted run are retained
  as non-decision evidence.
- The **report requirements**: per-cell gates, medians and best-of-four,
  degraded rows, admitted-versus-declined mechanisms, the residual A/B budget
  by batch size, and the plain-English comparison against both the Fjall-era
  engine and bare.

## Build and run discipline (replaces the v3 custody chain)

1. **Pinned, offline, locked builds.** Each variant builds from a read-only
   `git archive` materialization of its pinned commit, `--locked --offline`,
   with the approved lockfile hashes from the variant table, in a fresh
   sandbox with a unique target directory and no network. Each binary embeds
   protocol/source/tree/lock hashes and exposes the non-timed contract mode.
   Binary SHA-256 values are recorded at build time and re-checked by the
   runner before row zero. The sandboxing (bwrap, read-only host root,
   tmpfs overlays) is retained as build hygiene; its argv is recorded but not
   semantically validated field-by-field.

2. **One provenance file, not a lineage.** A single `provenance.json` records:
   toolchain identities and flags, kernel/boot ID, CPU topology and governor,
   filesystem/device, the four variant commit/tree/lock/binary hashes, adapter
   file hashes, runner and evaluator script hashes, exact commands and
   environment, seed, and timestamps. No staged authority files, no bundles,
   no assertions, no approval objects, no per-stage review binding. The
   producing checkout's `git status` must be clean and its HEAD recorded;
   review of tooling changes happens through the repository's ordinary
   bone/Seal workflow at ordinary strength (title, description, one approving
   review — no byte-canonical vote strings, no event-log topology
   requirements).

3. **Rehearsal is unlimited and encouraged.** A rehearsal run is identical to
   an accepted run except that its output directory is named
   `rehearsal-*` and its rows are non-evidence by construction. Rehearsals
   may run any subset: contract smokes, single cells, profile handshakes
   (perf/strace control-fd protocol against the real pinned helpers),
   correctness gates, or the full matrix. There is no claim to consume and
   no limit on attempts. The intended workflow is: rehearse until green,
   then declare.

4. **Declared accepted runs.** Before an accepted run, the operator (human)
   declares it: a one-line `DECLARED.txt` in the fresh output directory
   naming the date, the runner script hash, and the four binary hashes,
   plus `MESS_BENCH_COORDINATION_CONFIRMED=true` in the environment as the
   host-quiet attestation (unchanged from v3 semantics). An accepted run's
   rows are the evidence. If it fails mid-run, the partial output is retained,
   the failure is recorded, and a new accepted run may be declared after the
   cause is fixed and rehearsed — there is no single-use claim and no
   lineage regeneration. What prevents retry-until-pass is the declaration
   record itself: every declared run's outcome (including failures) must be
   listed in the final report's provenance section. Cherry-picking among
   multiple *successful* declared runs is prohibited; the first fully
   successful declared run is the evidence.

5. **Quiet-host and lease rules (retained, simplified).** The runner holds the
   exclusive lease `$HOME/.cache/mess-bench/global-measurement.lock` for the
   duration of an accepted run; builds, tests, and other benchmarks may not
   overlap it. Load, free-space, and inode floors, settle times, and the
   pre-spawn quiet guard are unchanged from v3. The `/proc` process guard is
   retained with its comm allowlist and fail-stop on unexplained processes,
   but a fail-stop ends the run as INCONCLUSIVE without burning anything —
   fix, rehearse, redeclare.

6. **Evaluator replay (retained).** The evaluator independently recomputes row
   statistics, gate outcomes, and hash checks from the raw outputs and
   provenance. The terminal verifier confirms the report matches the
   evaluator's outcomes. Both run from the same checkout as the runner; their
   script hashes appear in provenance. The v3 requirement that the executing
   runner file *be* a specific frozen inode under a specific interpreter with
   a specific process comm is dropped; recording and re-checking script hashes
   suffices.

7. **No self-validating canonicalization layers.** Canonical-JSON byte
   equality is required only where two independent programs must agree on a
   hash (row files, provenance, ledger entries), and in exactly one
   convention: UTF-8 `json.dumps(..., sort_keys=True, ensure_ascii=False)`
   with `\n` terminators. The ASCII/UTF-8 dual-convention machinery is
   removed.

## Adaptations

Where a v3 section incorporated by reference says "source approval",
"prepared artifacts", "single-use", "attestation", or "authority", read
"provenance.json and the declared-run record". Where it requires an
independent risk-high review of a tooling checkpoint, read "ordinary
bone/Seal review". Where it binds evidence to frozen support-file identity,
read "script hashes recorded in provenance". The measurement-phase text,
gate arithmetic, and outcome definitions require no adaptation.

## Outstanding known defect carried into v4

The perf control-fd ACK framing fix (bn-h521: the pinned perf binary answers
`b"ack\n\0"`, five bytes; both the runner's `_read_ack_line` and the overlay
`control.rs` `disable_after_t1` must consume exactly that framing) must land
through ordinary review before the first v4 rehearsal of the cpu_profiles
track, and the perf/strace handshake rehearsal must pass against the real
pinned helpers before an accepted run is declared.

## Terminal decision

Unchanged from v3: the single accepted run's evaluator-verified outcome is
`ADMIT`, `NARROW`, `REVERT`, or `INCONCLUSIVE`, with the same definitions,
and the report writes the budget against which later Asterism work is judged.
