#!/bin/sh
# Linker shim for this spike only.
#
# The repo root .cargo/config.toml sets rustflags with -Clink-arg=-fuse-ld=lld
# and -Clink-arg=-Wl,--no-rosegment. lld is not installed on this machine, and
# cargo MERGES rustflags arrays from parent config files (a nested
# .cargo/config.toml cannot remove flags, only append). So this spike points
# the linker at this wrapper, which strips the lld-specific flags and calls cc.
for arg in "$@"; do
    shift
    case "$arg" in
        -fuse-ld=lld) ;;
        -Wl,--no-rosegment) ;;
        *) set -- "$@" "$arg" ;;
    esac
done
exec cc "$@"
