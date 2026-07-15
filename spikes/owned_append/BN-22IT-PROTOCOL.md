# bn-22it Process-only owned append admission protocol

## Status and scope

This document was frozen as the pre-measurement decision contract for
`bn-22it`. The contract has now completed with an independently terminal-
verified `ADOPT`. The admissible comparison used reviewed tooling baseline
`fa6bc0cc2d533a9ef5e9fb54755007e2287ad28d`, neutral control
`0890cec2d734b047a71c3236db16faaeb38654df`, and candidate
`e5c3da658abd619c240851d384642fe741d93047`. It started from zero rows and
completed all 80 physical observations. The prior recoverable reference
`75694cb483844cbd18f1e1b0ee4164adf278ef58` and its Attempt-6 timing rows
remain non-admissible.

The candidate is deliberately narrow:

- `EventStore` encodes into an owned batch and capable wrappers forward it.
- `LogEngine` consumes that ownership directly only for
  `Durability::Process`.
- `Durability::Group`, `Durability::Os`, `$registry`, and the borrowed
  `Backend::append_batch` surface retain the borrowed-compatible validation,
  preparation, admission, barrier, error-ordering, and completion behavior.
- Batch-local type slots never become registry authority. The owner resolves
  every distinct name against the current `RegistryState` while holding its
  authority lock immediately before commit.

The exact candidate above is authorized for a separate production merge,
followed by the public product matrix. Later product changes cannot inherit
this result without a new measurement. A post-measurement commit may retain
this contract and its evidence, but must not change the measured product
patch before integration.

The final evaluator result is retained as `adopt-20260715/result.json` with
SHA-256
`6714401d9db3b6c329ff299f772f757d17e9352ce8ca18516feb83db8edd9086`.
The released terminal and independent terminal verification are retained as
`adopt-20260715/terminal.json` and
`adopt-20260715/terminal-verification.json`, with SHA-256 values
`94e1660c9e76c01508f7f43a63df6c2f48e3acdfea18a9e2a7b52984678041b1`
and
`67e1ad0dc0c2e235727c25fed76de9634a4377c490b695415f50e846e76b5fe3`.

## Required correctness checkpoint

Before a source checkpoint can become a benchmark candidate, focused tests
must prove all of the following on the real public composition:

- append, command, and cached-command traverse
  `EventStore -> FjallSnapshotBackend -> LogEngine` by ownership under Process;
- borrowed backends remain source-compatible and preserve mixed-record order;
- Group and Os owned submissions select the borrowed compatibility behavior;
- registry names and aliases are owner-resolved and revalidated in every mode;
- empty, conflict, oversize, and reserved-registry error ordering is unchanged;
- cancellation after ownership transfer still obeys
  committed-capsule-always-publishes, including a fresh-name append;
- live reads, reopen, prepared chain/roll/cache, and exact owner-cohort proofs
  remain green through both borrowed and owned submissions; and
- deterministic input counters report the selected boundary path exactly.

Only static and focused correctness checks run before the benchmark source is
frozen. They may not overlap any sibling build, test, review execution, or
measurement workload.

## Frozen performance matrix and gates

The decision matrix and thresholds are unchanged from the retained bn-2yye
Process-only contract:

- modes: `Process` only;
- batches: `1`, `10`, `100`, `1000`;
- writers: `4`;
- payload: `250` bytes/event;
- five cycles per cell;
- odd cycles: `control,candidate,candidate,control` (`ABBA`);
- even cycles: `candidate,control,control,candidate` (`BAAB`);
- 20 physical rows per cell, 80 rows total; and
- fresh ext4 store and fixed work for every observation.

Every cell must independently satisfy:

- throughput candidate/control at least `0.97`;
- append p99 candidate/control at most `1.10`;
- allocation calls/event candidate/control at most `1.05`;
- allocated bytes/event candidate/control at most `1.05`;
- zero fsyncs and no durability degradation in every row;
- exact event, append, batch, writer, payload, and work totals;
- candidate `owned_batches == measured appends`;
- candidate `owned_records == measured events`;
- candidate `owned_payload_bytes == measured events * 250`; and
- candidate borrowed-batch, borrowed-record, copied-record, and copied-byte
  deltas all exactly zero.

The exact Process counter identities are admission requirements, not
diagnostic hints. For each row, `appends = writers * bpw` and
`events = batch * appends`; `batches == appends`, `groups == 0`, `fsyncs == 0`,
`fsync_p99_ns == 0`, and `fsync_degraded == false`. The candidate has the
owned counts above and all borrowed/copy counts at zero. The control has all
owned counts at zero, `borrowed_batches == appends`, and
`borrowed_records == events`. Its copy counts are `events` records and
`events * 261` bytes for batch 1 and 10, and zero for batch 100 and 1000.
Allocation calls/event must equal raw `allocs / events` formatted to four
decimal places, and bytes/event must equal raw `alloc_bytes / events` formatted
to two decimal places. Gates use values recomputed from those raw integers; a
self-consistent-looking ratio column cannot override them. Latencies must be
finite and nonnegative with p50 no greater than p99.

The complete Process workload must additionally satisfy all three geometric
mean gates:

- throughput candidate/control at least `1.10`;
- allocation calls/event candidate/control at most `0.90`; and
- allocated bytes/event candidate/control at most `0.90`.

The evaluator uses the median of the two observations per variant within each
cycle, then the median of the five cycle ratios. Thresholds, work, order, and
aggregation may not change after row zero.

## Reviewed source, fresh artifact, and provenance contract

Source approval, artifact preparation, and measurement are separate one-way
stages. The common baseline is the final reviewed tooling commit. The
preparer and runner require their own checkout to be clean and require its
exact HEAD and tree to equal that approved baseline. Control and candidate
source commits are clean descendants whose exact merge base is that baseline.
A canonical `bn-22it-source-approval-v1` JSON object binds:

- the review identity, review timestamp, and explicit `approved` status;
- the exact baseline commit and tree;
- the exact control and candidate commits and trees;
- per variant, a sorted no-renames name/status allowlist, the SHA-256 of its
  canonical JSON, and the SHA-256 of `git diff --binary --full-index
  --no-ext-diff --no-renames baseline..source -- <sorted paths>`; and
- identical harness and `Cargo.lock` SHA-256 values.

Unknown keys, non-canonical serialization, a dirty worktree, a different live
diff or patch, an inexact merge base, a changed common input, or tooling that
does not exactly equal the approved baseline fails before a build. Review
status is never inferred or created by the preparer.

The separate preparer acquires the global lease and builds control then
candidate, never concurrently. It does not build from either live worktree.
For each approved source commit it records the exact Git archive, rejects
symlinks, gitlinks/submodules, non-regular entries, duplicate paths, and unsafe
paths, then materializes only regular `100644` and `100755` blobs into a fresh
directory. Canonical tree and materialized manifests bind every sorted path,
mode, Git object ID, size, and SHA-256. The source is made read-only and is
replayed byte-for-byte and mode-for-mode both before and after build and
contract execution.

The build runs in a fresh `bwrap` sandbox with the materialized source and host
root read-only, a private temporary directory, and only its random, previously
absent target directory writable. It removes inherited `CARGO_TARGET_DIR` and
uses the exact locked release command. Each compilation embeds the protocol,
baseline/source commits and trees, common-input hashes, approval hash, and a
unique 256-bit nonce. Contract mode must read that identity back without
writing CSV. Each canonical `bn-22it-build-attestation-v2` binds the exact Git
archive and manifests, materialized root, sandbox executable and argv, Cargo
argv, target, binary, build and contract logs, child PID/start/wait/reap/group
lifecycle and cwd, toolchain, source proofs, tool hashes, and embedded
contract. Monotonic timestamps prove build and contract execution are
sequential, non-overlapping, and bounded by the preparation lease.

Preparation uses a publish-last lifecycle. While the lease remains held it
durably publishes a canonical pre-release proof that binds both attestations,
the exact tooling and approval, the lease acquisition, and the absence of
`failure.json`. It then actually unlocks, durably publishes a release record,
and binds both in a preparation terminal. Only after all of those steps does it
publish the canonical `bn-22it-prepared-pair-v2`. That pair binds the complete
lifecycle and both attestations. A preparation failure is published while its
lease is still held; a failure, partial lifecycle, or pair published before
release is inadmissible.

The measurement runner accepts only that prepared pair. Before row zero and
again before evaluation it replays the approved source graph, exact archive
and materialization manifests, artifact paths/hashes, attestations, child
lifecycles, embedded contracts, and live clean source state. Live Rust/Cargo
identity must still equal both build attestations. The control retains
borrowed behavior; the candidate contains only the reviewed owned-append
patch. After preflight the runner acquires the global lease and atomically
creates a canonical `bn-22it-pair-consumption-v1` claim next to the pair using
exclusive creation. The claim binds the pair, attempt output, and live lease
identity. Any prior claim rejects reuse permanently. Only then does the runner
perform the exact transition smoke and permit row zero. Provenance binds:

- the baseline, approval, prepared pair, build attestations and logs, and
  canonical diff/patch hashes;
- clean, distinct full source commits and trees, original roots, immutable
  materialized roots, archives, manifests, and sandbox executable;
- distinct binary SHA-256 values and unique build nonces;
- identical harness and `Cargo.lock` SHA-256 values;
- preparer, runner, and evaluator path/SHA-256 values;
- exact toolchain, kernel, CPU/topology, governor, filesystem/device, free
  bytes, page size, command, and timestamp;
- the preparation pre-release, release, terminal, failure absence, and the
  single-use pair claim;
- the physical 80-row order; and
- after row 80, final CSV SHA-256, byte count, row count, and column count.

The evaluator independently re-reads every frozen path and rejects historical
rows, dirty worktrees, missing or extra manifest fields, duplicates, extra or
reordered rows, stale hashes, tool/source/build/materialization disagreement,
a missing or multiply-linked consumption claim, or a non-fresh output
directory. Preparation artifacts and decision output must be outside both
source workspaces and outside one another. The runner enforces the exact
pre-release, release, and terminal schemas before row zero, requires each
materialized root itself to remain a `0555` directory, and proves the complete
control-build, control-contract, candidate-build, candidate-contract monotonic
sequence is non-overlapping and bounded by lease acquisition and pre-release.
The evaluator repeats those checks independently before any terminal decision.

## Global exclusive lease

The runner must hold an exclusive non-blocking `flock` for the entire
measurement interval from post-build preflight through evaluator exit. The
canonical host-wide lock path is:

`$HOME/.cache/mess-bench/global-measurement.lock`

The lock is opened once and its file descriptor remains live across every
row, final provenance write, and evaluator invocation. Failure to acquire it
is a pre-row fail-stop. Provenance records the resolved path, device/inode,
holder PID, holder `/proc` start-time ticks, UID, hostname, and acquisition
time. A fresh random nonce and Linux boot ID disambiguate holder identity and
host reboot. The immutable acquisition event is evaluator input. Evaluator
exit and lease release are later append-only events and are included with the
evaluator result in a terminal manifest; a field that has not happened is
never predeclared as observed. All cooperating sibling build, test, formatter,
reviewer-execution, and benchmark work is prohibited while the lease is held;
coordination must be confirmed before row zero. The process guard remains
mandatory because this path and `flock` only exclude cooperating work, not
arbitrary activity by every host user.

The evaluator does not trust the acquisition record alone. While it is the
runner's direct child it requires the recorded holder to be its exact parent,
rechecks PID/start time, UID, boot ID, hostname, and canonical device/inode,
finds a matching `FLOCK ADVISORY WRITE` owned by that parent in `/proc/locks`,
and proves that a second nonblocking exclusive lock on the same inode fails.
Absence of either kernel proof invalidates the attempt.

## Observable process guard and child reaping

Before every benchmark child, the runner scans exact process names for
`cargo`, `rustc`, `cc`, `ld`, `collect2`, and `owned_append_be`. The last value
is the exact Linux `comm` for `owned_append_bench`, truncated to
`TASK_COMM_LEN - 1`; full executable and command-line fields remain separately
observable. The runner first writes and flushes a same-directory temporary
snapshot, atomically renames it into the attempt's immutable guard directory,
and syncs the directory before deciding whether to continue. Every match
records, from `/proc`, at least:

- observation timestamp;
- PID;
- `comm`;
- process state;
- PPID;
- `/proc/<pid>/stat` start-time ticks;
- NUL-decoded command line; and
- executable path or its read error;
- one enumerated owner classification: `runner`, `current_child`,
  `expected_helper`, `forbidden_unexplained`, or `vanished_unresolved`.

An empty match set is also recorded. All fields for one scan come from that
single scan; the runner does not compose a verdict from sequential `pgrep`
calls. `/proc/<pid>/stat` is the identity linearization point because it
establishes `comm`, PID, state, PPID, and start time together. If the directory
entry vanishes with `ENOENT` before stat establishes any identity, the snapshot
records that PID and error in `preidentity_vanished` but does not invent an
exact-name match. Non-forbidden identities need no further reads. Once stat
establishes a forbidden `comm`, any partial UID, command-line, or executable
read is recorded with all available fields and the read error as
`vanished_unresolved`, never silently discarded. `forbidden_unexplained` and
`vanished_unresolved` are fail-stops after the snapshot is durable in the
result directory. `expected_helper` is allowed only for an exact, predeclared
PID/start-time identity owned and reaped by the runner; name-only allowances
are forbidden.

Each benchmark invocation is launched as an explicit child with PID and start
time recorded before waiting. SIGINT and SIGTERM are masked across `Popen`,
exact child-identity capture, and durable `active-child.json` publication, then
restored before the wait. Thus no interrupt can land after process creation but
before the runner has published the identity it must clean up. Its stdout and
stderr go directly to a row-local file, so no pipeline or output-drainer
lifecycle can obscure `$!` or the wait status. The runner explicitly waits,
records the exit status, and verifies that the exact `(PID, start-time)`
identity is absent before the next guard. If that identity remains, it records
`prior_child_not_reaped` and fail-stops. A reused PID with a different start
time proves the prior child is absent; the new identity is independently
classified under the same forbidden-name policy. Child/reaping evidence is
append-only and its final SHA-256/cardinality is bound into the evaluator-input
manifest. The preparer uses the same masked-spawn, durable-identity, whole-group
cleanup rule for build and contract children.

Every child owns a new process group. Timeout, SIGINT, SIGTERM, or any exception
while waiting sends TERM and then KILL if needed to the whole group, explicitly
waits the child, proves the exact process group absent, and publishes failure
before lease release. The tooling gate injects a live-child signal and requires
the child identity and group to be gone, its record to say terminated and
reaped, and all failure/release artifacts to remain unchanged after a delay.

For each row the mandatory order is: load cooldown, frozen-input recheck,
atomic pre-child snapshot and verdict, immediate child spawn, explicit wait,
exact reaping proof, and atomic post-child snapshot and verdict. There is no
unobserved cooldown window between the final guard and spawn. The child
manifest records the CSV byte and complete-row counts immediately before and
after the invocation, plus SHA-256 of both prefixes. Each before prefix must
equal the prior child's after prefix, and its after prefix must equal the
actual final CSV prefix at that byte count. Wall-clock and monotonic timestamps
must prove `pre guard -> child -> post guard` for every row and
`pre-evaluator guard -> evaluator child`. The final child hash and byte count
must bind the actual final CSV. Exactly one complete row must be appended. A
failed child or a child that appends zero, partial, or multiple rows terminates
the attempt without retry.

The same atomic process snapshot and fail-closed verdict run once more
immediately before evaluator invocation. The evaluator cannot inherit a blind
post-row window.

## Exact transition smoke

The tooling checkpoint first runs a fixture-backed, non-timed transition smoke;
it requires no release build but traverses the exact runner finalizer and
evaluator entry point. After both real release binaries are frozen and the
global lease is acquired, the runner repeats that smoke with the attempt's
frozen paths and hashes before row zero. Both forms use the same shared
finalization function as a real attempt. The smoke exercises:

1. fresh output creation and use of the already-held exclusive lease;
2. atomic guard snapshot publication with full fields;
3. explicit child PID/start-time capture, direct row-local output, wait, and
   exact reaping proof;
4. final CSV hash and cardinality append to provenance;
5. guard and child-manifest hash/cardinality append to provenance;
6. evaluator invocation, exit-status capture, and successful readback; and
7. evaluator-result, under-lease pre-release proof, actual lease release, and
   terminal-manifest publication; and
8. an automatically spawned, explicitly waited terminal verifier that rereads
   and binds the released terminal, release, pre-release, result, provenance,
   and one-time pair claim before any decision exit is returned.

The later pre-row smoke additionally invokes each real frozen binary through
its non-timed contract mode, using the exact PID/start-time/wait/reap path, then
invokes and reads back the exact frozen evaluator. Its artifact hash and status
are bound into attempt provenance. If that mode is absent or any transition
fails, no timed row may start. Fixture and pre-row smoke outputs live outside
the decision CSV and can never satisfy a decision row.

Negative smoke cases must fail closed for a held lease, reused pair, unreaped
child, unidentified process match, missing final CSV binding, wrong
cardinality, wrong physical order, missing guard/child binding, and evaluator
or terminal verifier non-executable, malformed, or non-zero. Source/preparation
synthetic checks additionally reject non-canonical or unexpected approval
fields, dirty or mismatched tooling, mutated allowlist, patch, common input,
archive, materialized source, pair binding, preparation lifecycle, or
attestation lifecycle. Evaluator self-test traverses every live admission
helper and fails on unresolved globals. Smoke artifacts are never decision
evidence, may not report an admission or performance outcome, and never
substitute for a real fresh release build.

The evaluator writes a machine-readable outcome before returning. Distinct
exit codes separate `ADOPT`, a complete-and-valid `DECLINED`, invalid or
incomplete evidence, and evaluator failure. Malformed numeric input is handled
as invalid evidence rather than an uncaught exception. The runner always
publishes an outcome or failure record and may never translate invalid evidence
or an evaluator crash into `DECLINED`. It returns an `ADOPT` or `DECLINED` exit
only after the independent terminal verifier publishes the exact
`TERMINAL_VERIFIED` result. Missing, invalid, stale, or non-zero verification
creates `post_release_failure.json` and returns a fatal infrastructure result.

Every abnormal runner exit after output creation atomically publishes
`failure.json` before releasing the lease. It records the protocol and phase,
next physical ordinal/cell, runner and active-child identities, lease identity,
latest guard snapshot, reason and exit status, plus hashes and cardinalities
for all artifacts available at failure time. Missing evidence is represented
explicitly rather than invented. This includes exceptions during runner
construction after the fresh output directory exists: before a complete runner
object or lease exists, a minimal fatal record binds the constructor phase,
exception type and reason, and timestamp instead of inventing unavailable
state. The protocol identifier is
`bn-22it-process-owned-v1` throughout runner, provenance, evaluator, smoke, and
terminal artifacts.

## Attempt outcomes

- `ADOPT`: all 80 rows, every exact logical/provenance check, every cell gate,
  and all three material gates pass, and the released terminal chain is
  independently verified.
- `DECLINED`: a complete valid matrix fails any gate and the released terminal
  chain is independently verified.
- `INCONCLUSIVE_INFRASTRUCTURE`: lease, guard, child, load, provenance, or
  evaluator transition prevents a complete valid matrix.
- `INCONCLUSIVE_FATAL`: a harness/tool/runtime failure prevents evaluation.

There are no retries, replacements, substitutions, manual evaluator runs, or
historical-row carry-forward after row zero. Only `ADOPT` authorizes a separate
reviewed production integration step and a rerun of the public product matrix.
Every other outcome retains evidence only and removes the candidate from the
merge tree.

## Final outcome

The fresh T1 comparison returned `ADOPT` with `valid_evidence: true`, no
evaluator errors, and no failed gates. All four cells passed. The overall
geometric means were:

- throughput candidate/control: `1.2325020228246115`;
- allocation calls/event candidate/control: `0.549768173765176`; and
- allocated bytes/event candidate/control: `0.8157565909622126`.

The terminal verifier returned `TERMINAL_VERIFIED` with no errors after the
measurement lease was released. This satisfies the contract's sole
performance authorization path. Integration is limited to the exact measured
candidate product patch; retained evidence and explanatory documentation may
be added afterward without changing that patch. The merged public product
matrix remains the final correctness gate.
