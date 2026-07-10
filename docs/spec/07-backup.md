# 07 — Online backup, restore, and the retention lease

Status: **normative**. This document specifies the *consistent cut* an online
backup copies while the writer is live, the on-disk backup layout, the
detectability of a torn backup, and the **retention lease** that stops
compaction from deleting a segment out from under a running backup. It builds
directly on the durable watermark of [03-durability.md](03-durability.md), the
sealed-segment trailer / manifest of [02-recovery.md](02-recovery.md §8), and
the retention-blocking rule of [05-fold-certificates.md](05-fold-certificates.md)
§8.2 (`bn-2ug`).

Key words (MUST, MUST NOT, SHOULD, MAY) are per RFC 2119 / RFC 8174.

Backups **are** the single-node durability story: device death is the dominant
failure mode and same-device parity was rejected on that basis (`bn-2za`). The
value of this document is that "consistent online backup" is defined
*precisely* — undefined-but-easy is how operators lose data.

---

## 1. The consistent cut

A backup captures a **cut**: a prefix of the committed log that is itself a
legal durable image — exactly what recovery ([02](02-recovery.md)) would accept
after a crash at the instant of the cut. The cut is the disjoint union of:

1. **Every sealed segment**, whole. A sealed segment is immutable once its
   `SegmentFooter` trailer is durable ([01 §3.3](01-log-format.md)); its bytes
   never change again, so copying the whole file — concurrently with a live
   writer — is safe and its content is **stable by name** (the `segment_id` and
   the trailer's `footer_crc` never change). Sealed sidecars
   (`.pidx`/`.pcol`/`.filter`) are copied alongside when present; they are
   rebuildable caches (D1/I5) but content-stable, so copying them is a
   restore-speed optimisation, not a correctness requirement.

2. **The active segment up to the durable watermark.** The active (unsealed)
   segment is the only file a live writer mutates, and it only ever *grows*
   past the committed frontier. The cut copies its bytes `[0, safe_offset)`,
   where `safe_offset` is the recovery scan's committed-prefix end (the durable
   watermark expressed as a byte offset, [02 §1](02-recovery.md);
   [03 durable watermark](03-durability.md)). Bytes at or beyond `safe_offset`
   are the uncommitted tail and are **not** part of the cut.

3. **The backup manifest** (`BACKUP_MANIFEST`, §3), written **last**, naming
   every copied file with its byte length and CRC32C and recording the cut's
   durable watermark.

### 1.1 A torn tail beyond the watermark is harmless by design

The cut deliberately copies the active segment only up to `safe_offset`. Even
if an implementation chose to copy the *whole* active file (tail included), the
result would still restore correctly: recovery scans from the segment start and
**truncates at the first invalid/incomplete batch** (A10, [02 §3](02-recovery.md)),
discarding any torn tail. The cut is therefore robust to a writer that is
appending *during* the copy — every event acked before the cut is fully inside
it, and every event not yet durable at the cut is cleanly beyond it. There is
no torn middle: batches are atomic (A4 CRC over the whole batch), so a
partially-copied trailing batch fails recovery and is dropped, never
half-accepted.

### 1.2 What is rebuildable, and the one part of `meta/` that is not (I5)

The store's own advisory manifest ([02 R2](02-recovery.md)) and **most** of the
`meta/` fjall tables (stream/snapshot heads, dedupe window, high-water marks)
are **rebuildable** from the log alone (D1; I5, `mess rebuild-index --meta`). A
backup does not need them for correctness: restore runs full recovery, which
re-derives every index and catalog from the segment bytes.

**The one exception is the name↔id interner** (`stream_names` / `type_names`,
`bn-20b` / `bn-150`). The log stores only interned numeric ids (a `stream_id`
per batch, an `event_type_id` per event) — never their names — so these two
tables are the **durable source of truth** for the bijection and are **NOT
rebuildable from the log**. A restored store cannot resolve any stream/type
name without them. The cut therefore **MUST** include `meta/`. This is safe
even under a live writer: `bn-150` fsyncs a newly-interned name's row durable
**before** its covering append can become durable, so every stream present in
the committed cut already has its name durable in `meta/` at cut time; a `meta/`
copy that skews slightly ahead of or behind the log cut can only differ by
names for streams whose events are themselves beyond the cut (harmless), and
fjall's own recovery handles a torn LSM copy the same way it handles a crash.
`meta/` is copied **after** the log cut to minimise that skew. The rest of
`meta/` riding along is a restore-speed bonus (recovery would rebuild it
anyway); the interner is the load-bearing part.

`BACKUP_MANIFEST` is a *separate* artifact — it describes the backup, not the
store — and it is the only file whose presence gates a restore (§4).

### 1.3 The recovered watermark

The cut's **watermark** is the exclusive durable end position (D7,
[03](03-durability.md)): the global position one past the last committed frame
in the cut. It equals the active segment's `base_pos + Σ frame_count` over its
accepted prefix, or — for a fully-sealed store with no live active tail — the
highest sealed trailer's `end_pos`. Restore re-derives and reports it (§4);
byte-exact replay of the restored store reproduces every frame up to, and none
beyond, this watermark.

---

## 2. `mess backup <dir> --to <dest> [--incremental]`

`backup` copies the cut of the store at `<dir>` into `<dest>`, atomically and
crash-safely, while the writer at `<dir>` stays live (it does **not** take the
D9 writer lock — it reads files directly; sealed files are immutable and the
active prefix is append-only).

Procedure (the order is normative):

1. **Register a retention lease** (§5) protecting every segment in the cut, so
   compaction cannot delete a sealed segment mid-copy. The lease is written and
   fsync'd **before** any segment is copied.
2. **Compute the cut**: enumerate segments, read each sealed trailer, scan the
   active segment for `safe_offset`, and fix the watermark (§1.3).
3. **Copy each cut file** into `<dest>` with **temp + rename** discipline: copy
   to `<name>.tmp` under `<dest>`, fsync it, then `rename` to `<name>` (rename
   is atomic within a filesystem). Sealed `.log` files and their sidecars copy
   whole; the active `.log` copies exactly `[0, safe_offset)`.
   - **`--incremental`**: a sealed file already present at `<dest>` whose byte
     length **and** CRC32C match the source is skipped (content-stable names
     make the match trivial — but the match is verified by **size + CRC**, not
     by name alone, so a truncated or corrupted prior copy is re-copied). The
     active segment is always re-copied (its prefix grows between backups).
4. **Write `BACKUP_MANIFEST` last** (temp + rename), listing every file in the
   backup with its length + CRC32C and the cut watermark. Because it is written
   last and atomically, its presence is proof the copy completed: a backup that
   crashed mid-copy has no manifest and is detectably torn (§4).
5. **Release the lease** (delete the lease file).

Exit codes and `--format {text,pretty,json}` follow the CLI conventions
(`.agents/edict/design/cli-conventions.md`): `0` success, `2` system/IO error,
`3` the backup completed but a finding (e.g. a source segment failed to scan)
demands attention.

### 2.1 The no-CLI (plain-rsync) procedure

The "boring operation" promise includes operators with existing backup tooling.
A correct backup can be taken with `rsync` alone **if the ordering rules hold**:

- **Copy sealed segments and sidecars first, the active segment last.** Sealed
  files are immutable, so their copy is always consistent. Copying the active
  segment last minimises the window in which its tail grows during the run.
- **The active segment MAY be copied whole.** A tail appended during the copy
  is a torn tail beyond the watermark and is truncated by recovery (§1.1) — it
  never corrupts the restore. No lock, no quiesce.
- **Do not delete at the destination on this pass** (`rsync` without
  `--delete`), so a sealed segment that was compacted at the source between two
  runs is not removed from a good backup.
- **Guard against compaction racing the copy** by taking a lease first
  (`mess backup` does this automatically; a pure-rsync operator instead
  disables retention for the run, or accepts that a segment deleted mid-run is
  simply absent from — and re-fetched on — the next run).
- **The backup is complete only once the active segment has been copied**; an
  rsync interrupted before then is a partial backup. `mess backup`'s
  `BACKUP_MANIFEST` makes this explicit; a pure-rsync operator MUST treat an
  interrupted run as incomplete and re-run it.

---

## 3. Backup layout and `BACKUP_MANIFEST`

`<dest>` mirrors the store's own layout so a restore is a copy-back:

```text
<dest>/
  BACKUP_MANIFEST            # JSON, written LAST; presence ⇒ complete backup
  seg-00000001.log          # sealed + active .log files (active = prefix)
  sealed/
    seg-...pidx / .pcol / .filter
  meta/                      # REQUIRED for the name interner (§1.2); rest is bonus
```

`BACKUP_MANIFEST` is JSON (stable field names):

```json
{
  "format": "mess-backup-v1",
  "created_unix": 1751971200,
  "watermark": 42,
  "incremental": false,
  "files": [
    { "path": "seg-00000001.log", "len": 8192, "crc32c": 3735928559,
      "role": "active", "copied_len": 8192 }
  ]
}
```

`role` is `sealed` | `active` | `sidecar` | `meta`. `copied_len` is the number
of bytes copied for that file (for the active segment, the cut `safe_offset`;
for a sealed file, its whole length). Restore verifies every listed file's
length + CRC before running recovery (§4).

---

## 4. `mess restore <src> --to <dir>`

`restore` reconstructs a store at `<dir>` from a backup at `<src>`:

1. **Refuse a non-empty `<dir>`** — never overwrite an existing store (a
   `target-not-empty` refusal, non-zero exit).
2. **Refuse a torn/absent backup** — if `<src>/BACKUP_MANIFEST` is missing or
   fails to parse, the backup is incomplete or corrupt; restore refuses (a
   `torn-backup` refusal). This is the torn-backup detection: because the
   manifest is written
   last (§2), its absence proves the copy never finished.
3. **Verify the backup against its manifest** — every listed file MUST exist at
   `<src>` with matching length + CRC32C, else refuse (`backup-file-mismatch`).
4. **Copy every file** into `<dir>` (temp + rename), reproducing the layout.
5. **Run full recovery + `verify --full`** over `<dir>` (the same machinery
   `mess verify --full` uses): scan every segment through the acceptance kernel,
   cross-check sealed trailers, reassemble payload sidecars, and recompute the
   fold chain. A restore that does not pass `verify --full` fails (non-zero).
6. **Report** what was restored (file count, bytes) and the **recovered
   watermark** (§1.3), which MUST equal the manifest's `watermark`.

Restore relies on the backed-up `meta/` for the name interner (§1.2): the
derived tables are re-derived by recovery, but the `stream_names`/`type_names`
bijection is the durable source of truth and is restored verbatim. Every
`meta/` file is verified against the manifest (size + CRC) like any other file.

Exit codes: a clean restore exits `0`; a refusal (non-empty target, torn/absent
manifest, a backup file that fails its size/CRC check, a failed `verify --full`,
or a watermark mismatch) exits non-zero (an `Error` finding, exit code `3` per
the report model).

---

## 5. The retention lease (`bn-2ug` integration)

Retention (v1: whole-segment deletion, [05 §8.2](05-fold-certificates.md)) must
never delete a segment a running backup still needs to copy. The mechanism is a
**backup lease**: a durable marker, written before the copy and removed after,
that pins a range of segment ids.

### 5.1 The lease as a retention blocker

A backup lease pins **every segment id present in the cut**, i.e. the inclusive
range `[protect_min_segment_id, protect_max_segment_id]`. The retention
decision function (`bn-2ug`,
`mess_index::sealed::retention`) is extended so that, in addition to a live
snapshot's certification frames, **an active backup lease whose range covers a
segment blocks that segment's deletion**. Any code path that would unlink a
sealed segment MUST consult the decision function with the current live-lease
set and MUST NOT proceed while the segment is lease-pinned.
`mess retention explain` surfaces active leases as `lease-hold` blockers.

### 5.2 Lease liveness — a crashed backup MUST NOT leak the lease forever

A lease is a file under `<dir>/leases/<backup_id>.lease` (JSON: `backup_id`,
`pid`, `created_unix`, `expires_unix`, `protect_min_segment_id`,
`protect_max_segment_id`, `watermark`). A lease is **active** iff
`now < expires_unix`. `mess backup` sets a short TTL (default 60 s) and
**renews** it (rewrites `expires_unix`) periodically while copying, so a
long-running backup stays protected while a **crashed** backup's lease simply
**expires** and is thereafter ignored — the retention reader treats an expired
lease as absent and MAY unlink its stale file opportunistically. TTL is the
primary liveness signal; an implementation MAY additionally treat a lease whose
`pid` is provably dead as inactive early, but MUST NOT treat a live-but-slow
backup as dead (the TTL renewal is what distinguishes them). This is the same
"a stale file does not block" discipline as the D9 writer lock
([01 lock semantics](01-log-format.md)).

Because the lease is advisory and self-expiring, it is safe under crash: losing
the lease file loses only the protection window, never committed data; a leaked
lease self-heals at TTL.

---

## 6. Rule index

| Rule | Where | Gist |
|---|---|---|
| B1 | §1 | The cut = sealed segments (whole) + active prefix to `safe_offset` + backup manifest |
| B2 | §1.1 | A torn tail beyond the watermark is harmless; recovery truncates it (A10) |
| B3 | §2 | Copy order: lease → cut → files (temp+rename) → manifest LAST → release |
| B4 | §4 | Restore refuses a non-empty target and an absent/torn manifest; passes `verify --full` |
| B5 | §5 | An active backup lease pins its cut's segment ids against retention; TTL-expiring so a crash cannot leak it |
