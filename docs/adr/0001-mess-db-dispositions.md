# ADR 0001: Dispositions for pre-research `mess_db` code

- Status: Accepted
- Date: 2026-07-08
- Bone: bn-17h (Phase 2 — "Boring backend: prove the API on the RocksDB actor")
- Deciders: mess-dev

## Format

This is the first ADR in the repo; it also establishes the format. ADRs live
in `docs/adr/`, are numbered sequentially (`NNNN-kebab-title.md`), and carry at
least: **Status**, **Context**, **Decision**, **Consequences**. Add
**Revisit-when** conditions when a decision is deliberately provisional.

## Context

`crates/mess_db` predates the v1 research convergence (see
`notes/mess-research/` doc 12, which mandates *no silent carry* of
pre-research code into Phase 2). Three pieces of that code need an explicit,
recorded disposition rather than being quietly kept or quietly dropped:

1. **`StreamPos` 1-bit encoding.** `StreamPos` is an
   `enum { Sequential(u64), Relaxed(u64) }` whose `encode()` packed the
   discriminant into the low bit of the stored `u64`
   (`Sequential(p) -> p << 1`, `Relaxed(p) -> (p << 1) | 1`). The intent was to
   let a stream key record, in-band, whether the stream used strict serial
   ordering or causal ordering via a hybrid logical clock, and to stop the two
   from being mixed in one stream. This bit-flag conflicts with the v1 model,
   in which a stream version is a plain `u64` and ordering *mode* is not a
   property smuggled into the position integer.

2. **Hybrid Logical Clock** (`rocks/clock.rs`). A complete, unit-tested
   `Clock`/`Tick` implementation (20 ticks/sec since a 2020 epoch, 16-bit
   logical counter in the low bits, monotonic `observe`/`next`). It is **not
   wired into any read or write path** — nothing constructs a `Clock` outside
   its own tests.

3. **rusqlite backend** (`rusqlite/`). The more feature-complete of the two
   historical backends (connection pool, migrations, SQL-trigger-enforced
   invariants). The v1 plan standardises on the RocksDB actor. The module is
   gated behind a `rusqlite` feature that was never finished — it does not
   build standalone, and `lib.rs` carried `#[cfg_attr(feature = "sqlx", ...)]`
   remnants for a `sqlx` feature that does not exist (raising
   `unexpected_cfgs` warnings).

Relaxed / causal streams are still wanted eventually, but doc 06 specifies they
return **post-v1 via algebraic ordering modes**, layered on top of the store —
not via an encoding trick in the position integer.

## Decision

### 1. Retire the `StreamPos` bit-flag encoding; `StreamPos` becomes a plain `u64`

`StreamPos` is now a transparent newtype `struct StreamPos(pub u64)`. The
`Sequential`/`Relaxed` variants are removed. `encode()`/`decode()` remain as the
explicit DB-serialization boundary but are the **identity** on `u64`
(`encode(self) == self.0`). `position()` and `next()` are preserved.

`Error::UnsupportedRelaxed` is removed: with no `Relaxed` variant there is no
way to *request* relaxed ordering in v1, so the runtime rejection path is dead.
The write path (`write_mess`, `write_mess_async`) no longer branches on the
discriminant.

Relaxed/causal ordering returns post-v1 through doc 06's algebraic modes, not
this encoding.

### 2. Delete the HLC clock (`rocks/clock.rs`)

It is unwired and its design is entangled with the relaxed-ordering story that
is being deferred. Git history preserves it. It will be reconsidered when
relaxed streams are designed.

### 3. Delete the rusqlite backend, keep its ideas noted here

Delete `crates/mess_db/src/rusqlite/`, the `rusqlite`/`dep:rusqlite` feature and
dependency, the `write_sqlite_rusqlite` bench, the rusqlite `ToSql`/`TryFrom<Row>`
impls, and the `RusqliteError`/`MigrationFailed`/`PreparedStmtError` error
variants. Remove the dead `sqlx` `cfg_attr`s.

Ideas worth carrying forward (recorded so the code can die):

- **Storage-enforced optimistic concurrency.** A `BEFORE INSERT` trigger
  (`check_stream_position`) rejected a write unless `NEW.position` equalled the
  stream's last position + 1, enforcing the expected-version check *in the
  store* rather than via an app-level read-modify-write. The RocksDB backend
  currently does this check in `next_stream_pos`; if we ever want the guarantee
  at the storage layer (e.g. multi-writer), this trigger is the reference.
- **DB-assigned HLC ordering.** An `AFTER INSERT` trigger (`clock_timestamp`)
  set `ord = max(-NEW.ord, (select max(ord)+1))`, i.e. the store minted a
  monotonic HLC timestamp. Tick layout matched `clock.rs`:
  `((unixepoch - 2020_epoch) * 20) << 16`, low 16 bits a logical counter.
- **Virtual/derived columns.** `category`, `stream_id`, `cardinal_id` were
  derived from `stream_name` by splitting on `-` and `+` — a naming/addressing
  convention to preserve if we reintroduce stream categories.

## Consequences

- **On-disk key semantics change for stream positions ≥ 1** (documented per the
  bone's requirement). Previously a Sequential position `n` was stored in the
  `StreamKey` as the big-endian bytes of `n << 1`; it is now stored as the
  big-endian bytes of `n`. Position `0` is unchanged (`0 << 1 == 0`), and the
  `StreamKey::max` sentinel is unchanged (`Relaxed(u64::MAX)` and
  `StreamPos(u64::MAX)` both encode to all-ones). This is **safe now**: there is
  no persisted v1 data — `mess_db` is a pre-v1 prototype being brought up to
  prove the API, so no migration is required. Key-encoding tests were updated to
  the identity layout. Should a future change need to read pre-existing data
  written by the old shifted encoding, it must account for the `<< 1`.
- Positions now use the full 64-bit range instead of 63 bits.
- `mess_ecs` (a legacy pre-v1 crate) kept its own `Version { Sequential, Relaxed }`
  enum but its `From`/`Into<StreamPos>` shims were updated: both `Version`
  variants map to `StreamPos(x)`, and `StreamPos` maps back to
  `Version::Sequential`. Relaxed is thus collapsed to sequential at the boundary,
  consistent with relaxed being unavailable in v1.
- The `rusqlite` and `sqlx` feature surface is gone; `unexpected_cfgs` warnings
  are resolved.
- HLC and rusqlite code is recoverable from git if the deferred designs need it.

## Revisit-when

- Relaxed/causal streams are designed (doc 06 algebraic modes): reconsider the
  HLC clock and whether a mode needs to be represented in or beside the key.
- A multi-writer or storage-enforced-invariant requirement appears: revisit the
  SQL-trigger approach captured above.
