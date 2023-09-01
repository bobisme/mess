set positional-arguments

@test *args='':
	env CLICOLOR_FORCE=1 cargo nextest run --workspace --failure-output=final "$@"

@bench *args='':
	cargo bench --workspace "$@"

@flame-bench *args='':
  rm flamegraph.svg*
  flamegraph -- cargo bench --workspace "$@"

@watch-test *args='':
	env CLICOLOR_FORCE=1 cargo watch -x "nextest run --workspace --failure-output=final $@"

alias wt := watch-test
