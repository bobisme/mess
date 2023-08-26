set positional-arguments

default:
    echo 'Hello, world!'

test:
	cargo nextest run

@watch-test *args='':
	env CLICOLOR_FORCE=1 cargo watch -x "nextest run --workspace --failure-output=final $@"

alias wt := watch-test
