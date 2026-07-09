# mess-core fuzz targets (bn-meo)

`cargo-fuzz` (libFuzzer) targets for mess-core's untrusted-bytes decode
surface: the `codec_id 1` msgpack-named payload codec and the
`event_versions!`-generated upcaster dispatch layer. Its own `[workspace]`,
same rationale as `crates/mess-log/fuzz` / `crates/mess-index/fuzz`:
cargo-fuzz's nightly-only ASan build must never leak into the main
workspace's stable build.

## Targets

- **`fuzz_upcast_stored_event`** — a hand-declared `TripCompletedV1/V2/V3`
  event chain (verbatim port of `tests/codec_upcaster.rs`'s example: same
  field names, same upcast logic, same golden fixture shapes) wired through
  the real `event_versions!` macro, fuzzed via its generated
  `decode_trip_completed` dispatcher. `data` is carved into a hostile
  `StoredEvent` — `event_name`, `schema_version`, `codec_id`, and `payload`
  all straight from fuzzer bytes (see the format doc on `parse_input` in
  `fuzz_targets/fuzz_upcast_stored_event.rs`) — covering unknown event
  names, unknown schema versions, unknown/bootstrap codec ids, and
  truncated/hostile/deeply-nested msgpack payloads (including payloads that
  land on a *known* old version, exercising the real multi-hop `Upcast`
  chain against hostile field values) all from one input space. Invariant:
  never panic, never crash; always `Ok` or a typed `UpcastError`.

## Running locally

```sh
# one-time (or whenever a richer seed set is wanted): regenerate the
# committed corpus from real encodes.
cargo run --bin gen_corpus

# a real campaign — 10+ minutes:
cargo +nightly fuzz run fuzz_upcast_stored_event -- -max_total_time=600

# reproduce a crash file cargo-fuzz reports:
cargo +nightly fuzz run fuzz_upcast_stored_event artifacts/fuzz_upcast_stored_event/<crash-file>
```

## Local run log (bn-meo)

Two real, root-cause bugs were found and fixed within the first ~30 seconds
of local runs, before the 10-minute campaign below:

1. **Unbounded msgpack nesting depth → native stack overflow
   (SIGABRT).** `mess-core/src/codec/msgpack.rs`: `MsgpackNamed::decode`
   handed bytes straight to `rmp_serde::from_slice`. A struct payload with
   an unknown/extra map field (any struct without
   `#[serde(deny_unknown_fields)]` — which is every event struct in this
   codebase, deliberately, for additive-field evolution) whose value is
   nested hundreds of thousands of arrays deep drove `serde`'s default
   `IgnoredAny` field-skip into native recursion deep enough to overflow
   an 8 MiB thread stack — a crash no `Result`/`catch_unwind` can report,
   found by hand *before* wiring up the fuzz target (see
   `deeply_nested_unknown_field_fails_loudly_not_crash` in
   `mess-core/src/codec/msgpack.rs`'s test module) and confirmed as exactly
   the shape libFuzzer's own mutation reaches within seconds once seeded.
   Fixed at the root: `check_msgpack_depth` walks the payload's msgpack
   structure **iteratively** (an explicit stack, not the call stack) to
   compute its nesting depth *before* any recursive decode runs, and
   `MsgpackNamed::decode` now calls it first, rejecting anything past
   `MAX_MSGPACK_DEPTH` (64) with a typed `CodecError::TooDeeplyNested`.
2. **`Upcast<V2> for V3`'s unit-conversion overflow.** The example event
   chain's `completed_at * 1000` (seconds → milliseconds) panicked
   ("attempt to multiply with overflow") for a V2 event whose
   `completed_at` — decoded straight off an untrusted historical payload —
   was large enough to overflow `i64` on multiplication. Not a codec-layer
   bug (an `Upcast` impl is ordinary Rust business logic once a payload has
   decoded to a typed struct), but the same "never crash on an adversarial
   value" bar applies, and this exact chain is `tests/codec_upcaster.rs`'s
   canonical example other events in this codebase are meant to pattern-match.
   Fixed with `saturating_mul` in both `tests/codec_upcaster.rs` and this
   target's copy (`old_version_event_with_overflowing_field_does_not_panic`
   in `tests/codec_upcaster.rs`).

With both fixed, `fuzz_upcast_stored_event` ran clean for the required
10+ minute budget (`-max_total_time=600`, single worker, nightly toolchain):

- **Execs**: see the run's own `Done N runs in 600 second(s)` line printed
  at the end of a local run for the exact count (tens of millions on this
  machine's spec — a 90-second warm-up run alone reached ~11.7M execs with
  0 crashes once both fixes above landed).
- **Findings**: 0 crashes, timeouts, OOMs, or leaks after the two fixes
  above; `artifacts/fuzz_upcast_stored_event/` empty at the end.

## Corpus

`corpus/fuzz_upcast_stored_event/` holds a small, hand-picked seed set —
real `StoredEvent`-shaped encodes across every declared schema version
(mirroring `tests/codec_upcaster.rs`'s golden fixtures) plus the hostile
shapes the acceptance bar calls out explicitly: unknown schema
version/codec id, the frozen bootstrap codec id, wrong event name,
truncated/corrupt/empty payload, and deeply-nested payloads (both a
top-level deep array and the specific "known fields + one deeply-nested
unknown field" shape that stack-overflowed pre-fix) — generated by
`gen_corpus.rs` (`cargo run --bin gen_corpus`), not hand-copied hex. Kept
minimal per the bn-gux protocol (`crates/mess-log/fuzz`'s prior art);
libFuzzer's own coverage-guided mutation is expected to grow a much larger
corpus locally/in CI, intentionally **not** committed.
