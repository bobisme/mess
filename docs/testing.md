# Testing: real-fs suites, `TMPDIR`, and temp-dir hygiene

Status: **informational** (process documentation, not a format/protocol
spec — those live in `docs/spec/`).

Most of `mess`'s test suite is pure/in-memory or runs against the
deterministic-simulation `SimFs` (`mess-log`'s `runtime` module) and needs
no special setup — plain `cargo test -p <crate>` is enough. This doc covers
the minority that isn't: suites that open a **real** `LogEngine` (and its
pack snapshot sidecar) on a **real** filesystem, because the property under test only means
something there (a durability barrier, a `fallocate`d segment, an actual
crash/`SIGKILL`).

## Why `TMPDIR` matters here

`std::env::temp_dir()` (and `tempfile::tempdir()`, which uses it) honors
`$TMPDIR`, which on most dev boxes and CI runners defaults to `/tmp` — often
`tmpfs`. Two things break on `tmpfs`:

- `fdatasync`/`fsync` are a no-op there, so any test asserting a durability
  barrier (`Durability::Os`/`Group`, the SIGKILL harness's acked-implies-
  recovered contract, …) would pass vacuously regardless of whether the real
  code is correct.
- Segment preallocation (`fallocate`, typically 256MiB) can fail outright
  with `os error 122` (`EDQUOT`) against a quota-limited `tmpfs`.

The convention this repo uses is to point `TMPDIR` at a real, persistent
device instead:

```bash
TMPDIR=$HOME/.cache/mess-test-tmp cargo test -p mess-store
TMPDIR=$HOME/.cache/mess-test-tmp cargo test -p mess-log
```

Several suites (`mess-store`'s `engine_append_gate.rs`, `mess-log`'s
`sigkill_harness.rs`, `mess-soak`'s driver, …) need this to be a real
filesystem to mean anything; a couple additionally hard-refuse to run (or
hard-code a `$HOME/.cache/...` fallback) rather than silently run
dishonestly fast against `tmpfs`.

## The self-sweeping temp-dir helper (bn-cxr)

Because `TMPDIR` here is a **persistent** directory (not the usual ephemeral
`/tmp`, which typically gets swept on reboot), nothing sweeps it on its own.
Two things leak real, multi-MiB store directories there over time:

1. Crash/`SIGKILL`-harness tests kill child processes by design — the
   child's `TempDir::drop` never runs for whatever was mid-flight. If the
   *parent* test process itself is interrupted (`Ctrl-C`, a CI timeout, an
   OOM-kill) mid-run, the same applies to it.
2. Any aborted run leaks the same way — a hard panic/`abort`, a killed
   `cargo test` process, etc.

This accumulated 302GB / 737 leaked dirs in two days on the dev box before
this was addressed.

`mess_testkit::sweeping_temp_dir(name)` (`crates/mess-testkit/src/tempdir.rs`)
is the fix: a drop-in replacement for `tempfile::tempdir()` in real-fs
suites. It creates a fresh directory under one shared namespace —
`<TMPDIR, or $HOME/.cache/mess-test-tmp if unset>/mess-tests/<name>-<pid>-<nonce>/`
— and, once per process (a cheap guard on every subsequent call), sweeps
*sibling* directories under that same namespace that are safely
identifiable as abandoned: their name encodes a pid, that pid is no longer
alive, **and** the directory is at least 24h old by mtime. Any one of those
three conditions failing leaves the entry untouched — in particular, a live
concurrent run's directory is never removed, no matter its age. The sweep
never follows symlinks and is entirely best-effort: I/O errors are
swallowed, never propagated, so it can't fail or panic a calling test. See
that module's doc comment for the full contract.

Adopted today in `mess-log`'s `sigkill_harness.rs` and `mess-store`'s
`engine_append_gate.rs` (the two suites that previously hand-rolled their
own never-swept scratch-dir helpers). New real-fs suites that need their own
store directory should reach for `mess_testkit::sweeping_temp_dir` rather
than hand-rolling another one-off `$HOME/.cache/...` helper.

## Cleaning up manually

```bash
just clean-test-tmp
```

Force-sweeps every directory under the shared `mess-tests` namespace
regardless of age (still skips a directory whose encoded pid is currently
alive, so it's safe to run while another suite is mid-run elsewhere). Use
this to reclaim disk immediately rather than waiting on the 24h auto-sweep.
