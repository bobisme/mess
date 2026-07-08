# Spikes

The 15 directories here are **frozen reference artifacts**, not workspace
members. Each `spikes/*` opts out of the root `[workspace]` and each carries
its `REPORT.md` verdict, which fed the design decisions recorded in
`notes/mess-research/12_convergence.md` and `notes/mess-research/13_spike_results.md`.

Do not edit spike contents to "fix" them — if a spike no longer builds, that
is expected and does not need to be repaired. Read the spike for its
`REPORT.md` findings, not to run it.

## Local `.cargo/config.toml` overrides

All 15 spikes carry a local `.cargo/config.toml` that overrides the (former)
repo-root linker configuration. At the time the spikes were written, the
root `.cargo/config.toml` hard-required `clang` + `-fuse-ld=lld`, which is
not installed on every machine; each spike neutralized that with its own
config (either dropping the override or redirecting to a shim, e.g.
`spikes/dx_api/ld-shim.sh`).

The root `.cargo/config.toml` no longer sets a `[target.*]` linker override
(see bn-igp) — a fresh clone now builds with the platform's stock linker.
That makes the spikes' local overrides vestigial. They are left in place
rather than removed, since spikes are frozen and not meant to be
re-touched.

## Spikes referencing pre-restructure paths

The workspace crates moved from repo-root (`mess_db/`, `mess_ecs/`) to
`crates/` (bn-2gq). Spikes reference their dependencies by relative path
from *when they were written*, so any spike that pointed at the old
root-level location may no longer build:

- `spikes/dx_api` — `Cargo.toml` depends on `mess_db = { path = "../../mess_db" }`,
  which pointed at the old repo-root crate. That path is now
  `crates/mess_db`, so `spikes/dx_api` will fail to resolve its dependency
  as-is. This is expected breakage, not a regression to fix; consult
  `spikes/dx_api/REPORT.md` for its findings instead of building it.

No other spike's `Cargo.toml` references a moved path (checked via
`rg -n 'path = ' spikes/*/Cargo.toml`).
