# Source-bound Asterism rebaseline overlays

This directory realizes the approved `bn-2l3n` measurement contract without
changing a product source file or Cargo manifest. It does not run the decision
matrix and it contains no accepted timing result.
Its canonical identity and every protocol-owned tooling/artifact schema are
version 3. Version-2 plans, approvals, lock candidates, contracts, prepared
artifacts, tool manifests, rows, and smoke objects fail closed and cannot be
replayed or promoted.

`source-plan.json` binds public `A`, bare `B`, Fjall-era public `C`, and flat
public `D` to exact Git commits, trees, lock inputs, binary kinds, and timed
surfaces. `prepare_overlays.py stage-locks` materializes those commits with
`git archive`, injects examples only, attempts the current lock offline for
the historical comparators, resolves their candidate locks offline, and
retains complete lock diffs. Resolution is bound to the resolved cargo/rustc
paths, executable hashes, verbose versions, and Rust host triple; these are the
actual binaries selected by one exact rustup toolchain, not rustup proxies.
Git and bubblewrap are absolute and hash-bound too; every Git identity,
archive, lock-readback, and overlay operation executes through its retained
hash-checked descriptor. Every Cargo invocation
uses a complete allowlisted environment, disabled global/system Git config,
an offline read-only Cargo home, and a canonical replayed manifest of every
Cargo config search path. The exact same identity is required for every later
sequential build. Resolution itself runs in a no-network bubblewrap sandbox
whose only system mounts are `/usr/bin`, `/usr/lib`, and `/usr/include`.
It creates empty `/dev` and `/proc` directories and exposes no device or proc
interface, host root, `/etc`, or rustup home. The sandboxed Git global-config
path is a deliberately absent path under `/asterism`, not `/dev/null`.
Merged-`/usr` aliases are explicit guest symlinks.
Every system-tree symlink must either remain in that closure or resolve to a
guest-inaccessible path; links into `/asterism`, `/dev`, `/proc`, `/run`,
`/sys`, or `/tmp` fail closed. All closure entries are root-owned and neither
group/world nor process-writable, are recursively watched, and are replayed at
the child boundary.

Lock resolution is replayable authority, not a best-effort log. `A` and `B`
record the exact retained-Git `show <commit:path>` readback. `C` and `D` first
run the exact offline locked-metadata command with the current lock and require
both success and an unchanged lock, then run the exact offline
`generate-lockfile` command from an absent lock and require the reviewed final
hash. Validation reconstructs the complete argv and descriptor order, fixed
guest cwd, canonical materialized source root, retained bubblewrap lease,
environment, successful exit, UTF-8 stdout/stderr hashes, and both lock
boundaries. Its fixed descriptor order is three system roots, four ordinary
core bindings (source, toolchain root, Cargo, and rustc), one retained-FD
Cargo-home temporary overlay, and four Cargo config files, plus the separately
retained bubblewrap executable: thirteen passed descriptors in total. The
overlay makes absent config targets mountable without modifying the retained
Cargo-home tree; the exact config files are then mounted and the overlay is
remounted read-only before Cargo starts. The resolution source is recursively
frozen except for the root needed to create `Cargo.lock`; inotify and a second
manifest allow only that lock and its atomic temporary names to mutate.
This command
performs dependency resolution but does not compile Rust or emit a measurement
row.

The public example is byte-identical for `A`, `C`, and `D`. Their only
difference is a minimal reviewed adapter: `A` reads its exported append-input
counters, while `C` and `D` label their historical borrowed API and report
unexported defensive-copy counters as `not_available`. `B` uses a separate
bare `mess-log` example. All four receive byte-identical shared workload,
payload, digest, allocation, timing, fairness, schema, contract, control, and
narrow exact-PID ptrace-authorization modules.
The payload contract is the historical byte sequence `i & 0xff`, checked
literally at both 24 and 250 bytes. Writer-result capacity is reserved before
the allocation/CPU/wall snapshots. Public and bare fairness rows report the
five unavailable queue/group-width diagnostics as the literal
`not_available`, never a fabricated zero.

The child protocol covers the ordinary append/profile order and the special
single-open order. Every child embeds the protocol/tool/source/tree/adapter/
lock/approval identity. Public reopen corpus creation and verification use
the public `LogEngine -> FjallSnapshotBackend -> EventStore` composition.
The common oracle is likewise a mode of each exact prepared public binary,
never a generic helper-selected target. Its frozen
`--correctness-oracle` invocation creates a fresh store through that same
composition, checks append/load/subscription order and a generation-neutral
logical digest, exact conflict and common error outcomes, stream heads and
high-water, generation-specific registry accounting, and Process/Group
barriers before emitting the canonical correctness-child object. The binary
contract, source approval, and prepared variant record bind whether that mode
is available. Admission uses this mode from `C` and `D`; `A` remains assigned
to its separate focused current-product child. Canonical row modes separately
require their runner-supplied track, ordinal, and config identity on argv and
in the environment.
Reopen freezes wall, allocation, process-CPU, and external-profile end markers
at successful open. Event-count/recovery observations and the retained
2,000,000-event verification run only after the parent has taken its immediate
post-open profile snapshot and released the child.

CPU-profile children, including the exact `smoke`/`cpu_profiles` pairing,
receive the exact preflight permission result. Only an available CPU row may
additionally inherit the reviewed perf command-write,
ACK-read, and shared-offset ACK-ledger descriptors. At `t1` the child writes
`disable`, reads the pinned perf wire frame exactly as `ack\n\0`, strips only
the asserted trailing NUL, appends `ack\n` through the inherited ledger
description, and includes that exact nonce/timestamp event in
`measured` before the final counter snapshot is serialized. An unavailable CPU
row receives no perf descriptors and emits `perf_disable: null`; every non-CPU
mode rejects all four perf environment bindings and omits that field.

After the exact tooling commit is final, an independent Seal review approves
one structured assertion over the protocol, the tooling commit/tree, the
candidate locks, the final tools manifest, the lock-review authority and
bundle, and the immutable preapproval current-child attestation. The canonical
`bn-3hch-source-review-bundle-v1` contains the detached Git anchor and exact
`lgtm` verdict; `write-approval` derives `review_id` and `reviewed_at` from
those events. There are no free-form review-provenance arguments.

The assertion also freezes a release compile-out requirement. That requirement
binds the preapproval A/product-test-overlay equality proof and requires it to
be repeated under the eventual real source-approval hash. It intentionally
does not bind the later proof hash: the proof binds the approval, in one
direction, so neither the approval nor its review has a hash cycle. The
approval embeds the complete canonical tools object and its file hash;
`build --tools` requires both to be byte-identical before it then:

1. rematerializes every exact approved source commit and retains its tar
   archive and manifest in the prepared root;
2. injects only the approved overlay paths and approved root lock;
3. makes the complete source tree read-only;
4. builds ordinary `A`, then a physically distinct proof-only `A` source with
   the exact reviewed product test overlay, under the same real approval,
   contract, nonce, lock, toolchain, Cargo environment, normalized sandbox,
   package/example and fixed guest paths, with no workspace wrapper or
   `cfg(test)`. Bubblewrap, Cargo, rustc, Git, both source roots, both target
   roots, the exact toolchain root, and the Cargo home are retained by
   descriptor across their use. The sandbox binds the three ordered system
   roots first, then five ordinary core bindings (source, target, toolchain
   root, Cargo, and rustc), exposes Cargo home through one retained-FD temporary
   overlay, and then mounts four exact config files into that overlay before
   remounting it read-only. Bubblewrap is a separate retained executable lease.
   All thirteen sandbox descriptor operands are authority-bound and their
   numbers are normalized, never a guest destination; with the bubblewrap
   lease, the child inherits fourteen descriptors. Cargo-config evidence names
   the guest cwd/home and all eight searched guest paths. Source and Cargo-home
   config files are private
   descriptor mounts containing either the retained reviewed bytes or an
   immutable empty substitute; ancestor `.cargo` directories are empty
   read-only mounts. The config-manifest hash is part of the sandbox hash;
5. proves the two release binaries and their retained-`nm` symbol inventories
   are byte-identical and contain none of the six reviewed hook strings, emits
   `manifests/release-compile-out.json`, and publishes only ordinary `A`;
6. continues the measurement build chronology as `B`, `C`, and `D`, preserving
   the public `build_order` of exactly `A`, `B`, `C`, `D`;
7. proves every materialized manifest and lock is unchanged;
8. runs contract mode only, verifies exact canonical output, and retains child
   identity/reaping records; and
9. copies binaries, source approval, the canonical tools manifest, and every
   source-approved tool/support file into one immutable, single-use
   prepared-artifact root, rechecking every hash, mode, comm, exact set, and
   relocated claim.

The prepared root carries immutable local copies at
`bindings/source-review-bundle.json`,
`bindings/current-children-attestation.json`,
`bindings/lock-review-authority.json`, and
`bindings/lock-review-bundle.json`. Its prepared manifest binds those four
copies and `manifests/release-compile-out.json`. The proof-only overlay binary
is reachable only through that proof: it is absent from variants, prepared
tools, executable argument plans, and measurement child plans. The ordinary A
hash in the proof must equal the A hash published by the prepared manifest.

The preapproval current-child attestation is
`bn-ecm1-current-children-build-v2`. Each current and release build carries one
`bn-ecm1-semantic-input-authority-v1` object: recursively immutable filesystem
authority for source, toolchain, and Cargo-home tree bindings plus the narrow
trusted-system closure.
Every referenced manifest is canonical immutable `0444` evidence with exact
entry/watch counts and is deeply replayed against the live tree. Regular files
in source/toolchain/Cargo-home are content-hashed; the trusted system closure
uses exact metadata under its explicit root-owned non-writable trust boundary.
The path-free runtime digest intentionally excludes relocatable evidence paths
and the source component, while binding the exact ordered
host/resolved/guest system-mount tuples, so independently materialized builds
must still prove one identical Cargo/toolchain/system runtime. Downstream
source approval must consume and replay this object; merely preserving its
hash is not a valid integration.

The prepared manifest also binds immutable copies of the exact approved
protocol document and the historical `BN-2SU-FINAL.csv` input. Downstream
runner/evaluator copies must consume these prepared bindings, never infer a
checkout-relative file next to their relocated support code.

The source approval also freezes the complete, sorted Linux `comm` tracking
set: all four prepared variants, every exact prepared tool name, and the
compiler, linker, build-script, formatter, and test-orchestrator names that
must be treated as overlapping work. A tools manifest may only repeat that
set and the exact reviewed name for each tool; it cannot expand or narrow
process-guard authority.
The prepared tools-manifest binding is exactly `path`, `sha256`, and mode
`0444`. Source approval and each prepared variant bind typed relative trace
templates to the exact `ASTERISM_REBASELINE_STORE` root. The runner must derive
canonical absolute `{kind,path}` markers for the row context and profile input;
it may not pass the relative templates through as evidence. Variant `C`
additionally classifies `log/meta/` as a metadata directory prefix while its
`log/seg-` file-family prefix remains nonoverlapping; `A` and `D` retain their
existing metadata set. Bare `B` has one exact `segment-1.log` marker and an
intentionally empty metadata family.

Variant `C` also carries one exact role-lifetime object in both source approval
and binary contract. Approval statically replays its measured public append
path, requires one `spawn_blocking` site and zero other thread-birth sites, and
requires every public multi-thread Tokio builder to retain blocking workers
for one hour. The separately reviewed runner caps every profile child at two
minutes, so a measured publication worker cannot age out before terminal role
capture.

Both resolution and build independently require an ext4 scratch filesystem
with at least 128 GiB and one million inodes initially available. The completed
prepared root is read-only except for its dedicated single-use claims
directory. Build validation requires every other directory to be exactly
`0555`, rejects extra support paths, and proves that both relocated Python
runtimes can import the shared evidence schema without creating `__pycache__`
or changing any support byte. Those imports are the last child executions;
the producer then freezes the tree and terminally replays a whole-tree
file/directory manifest plus every release compile-out binding and proof.

`authority_inputs.py` is the fail-closed producer for the operational inputs
that were previously hand assembled. `write-base-tools` creates immutable,
role-distinct Python runtime copies, retained `perf`/`strace` copies, the six
fixed support files, and the exact correctness/fault placeholders, then runs
the downstream base-tools validator. Every producer output must be a fresh
path outside the reviewed repository. The two review phases use this sequence:

1. `write-lock-review-assertion` or `write-source-review-assertion` freezes the
   exact assertion before review.
2. Create a fresh Seal review at that detached tooling commit and approve it
   with
   `seal lgtm <review-id> -m "APPROVED assertion_sha256=<hash>; open_findings=0"`.
3. `write-lock-review-bundle` or `write-source-review-bundle` consumes the
   immutable assertion and exact per-review `events.jsonl`. It accepts only the
   minimal terminal lifecycle `ReviewCreated`, optional `ReviewersRequested`,
   one matching `ReviewerVoted(lgtm)`, and `ReviewApproved`; any thread,
   abandonment, merge, block, duplicate vote, or other lifecycle event fails
   closed. It recomputes all authority, publishes with an absent-output
   requirement, and resamples every input after publication.

Seal review events should be created in a separate clean worktree at the exact
tooling commit. That keeps the source/build workspace clean while preserving
the detached commit anchor required by both assertion validators. No author,
timestamp, verdict reason, assertion body, commit, or tree is supplied as a
free-form producer argument. Produce the bundle from that worktree's
`.seal/reviews/<review-id>/events.jsonl` immediately after `seal lgtm`; do not
add threads, abandon, or mark the review merged before bundle production.

Contract and transition smokes are not performance evidence. Only the
separately reviewed runner may drive evidence modes after all prepared hashes
are frozen.
