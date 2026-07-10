set positional-arguments

@test *args='':
	env CLICOLOR_FORCE=1 cargo nextest run --workspace --failure-output=final "$@"

# bn-3mc: rustfmt.toml uses nightly-only options (see its header comment), so
# these always target +nightly rather than the default toolchain — a stable
# `cargo fmt` silently drops those options and reports spurious diffs. No
# `*args` pass-through here (unlike `test`/`bench` above): `just`'s empty
# default for a variadic parameter passes cargo a bare "" when no args are
# given, which nextest tolerates but `cargo fmt` rejects as an unexpected
# argument.
@fmt:
	cargo +nightly fmt --all

@fmt-check:
	cargo +nightly fmt --all --check

@bench *args='':
	cargo bench --workspace "$@"

@flame-bench *args='':
  rm flamegraph.svg*
  flamegraph -- cargo bench --workspace "$@"

@watch-test *args='':
	env CLICOLOR_FORCE=1 cargo watch -x "nextest run --workspace --failure-output=final $@"

alias wt := watch-test

# bn-25j: Miri over the mess-log/mess-core encode/decode/codec unit +
# integration subset. Real-fs, real-thread-BFS, and hot-path-timing tests
# are excluded in-source via #[cfg_attr(miri, ignore)] (Miri cannot do real
# fs I/O; see the notes on those tests). Requires: rustup component add
# miri --toolchain nightly.
@miri *args='':
	cargo +nightly miri test -p mess-log "$@"
	cargo +nightly miri test -p mess-core "$@"

# bn-25j: AddressSanitizer over mess-log's full test binary set (unit +
# integration; doctests excluded with --tests because rustdoc does not
# thread sanitizer RUSTFLAGS into the doctest binary the same way cargo
# does, causing a false-positive ABI-mismatch error — mess-log has zero
# doctests today so nothing is lost). Requires: rustup component add
# rust-src llvm-tools --toolchain nightly.
@asan *args='':
	env RUSTFLAGS="-Zsanitizer=address" cargo +nightly test -p mess-log --target x86_64-unknown-linux-gnu -Z build-std --tests "$@"

# bn-25j: ThreadSanitizer, same scope/caveats as `asan` above. Verified
# green locally (full mess-log suite, including the real-thread stateright
# BFS checker) — see the bn-25j bone summary for the invocation history.
@tsan *args='':
	env RUSTFLAGS="-Zsanitizer=thread" cargo +nightly test -p mess-log --target x86_64-unknown-linux-gnu -Z build-std --tests "$@"

# bn-py3: the full A4 torn-write/sector-reorder matrix (>=20k cases,
# `#[ignore]`d in-source; the ~1k-case `torn_matrix_fast` already runs in the
# ordinary `just test` / `cargo test -p mess-log` gate). Also runs nightly
# via .github/workflows/torn-matrix.yml. Measured locally: ~1.7s debug.
@torn-matrix-full *args='':
	cargo test -p mess-log --test torn_matrix -- --ignored --nocapture "$@"

# bn-1gx: loom memory-ordering interleaving models (src/loom_tests.rs) for the
# cross-thread atomic protocols — watermark publish/wakeup + committer group
# handoff. loom replaces std::sync, so the module is gated on `--cfg loom` and
# only compiled here. Release build (checked models are slow in debug);
# LOOM_MAX_PREEMPTIONS=3 bounds the search (every model fully explores at that
# bound — see each test's printed interleaving count). The `loom_` filter runs
# only the loom tests (the rest of the suite is skipped, not rebuilt away).
# Also runs in CI via .github/workflows/loom.yml.
@loom *args='':
	env RUSTFLAGS="--cfg loom" LOOM_MAX_PREEMPTIONS=3 cargo test -p mess-log --release --lib -- --nocapture loom_ "$@"

# bn-1mw: examples/social one-command demo. Seeds a deterministic corpus
# (~50 users, ~500 posts, Zipf-ish follows/likes — see `social-seed`) into
# $HOME/.cache/mess-social-demo/store (--force wipes a prior run so this is
# safe to rerun), then serves it at http://127.0.0.1:3000. See
# examples/social/README.md for the full tour, including the CLI ops walk.
@demo:
	cargo run -p social --bin social-seed -- --force
	cargo run -p social --bin social-web -- --dir "$HOME/.cache/mess-social-demo/store"
