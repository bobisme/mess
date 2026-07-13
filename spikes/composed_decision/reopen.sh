#!/usr/bin/env bash
# Spike J: reopen wall time + peak RSS at scale, `current` vs `sealpack`.
#
# Harness is the EXISTING Spike-C `owb_bench` example (public API only), with
# one addition for this spike: `OWB_SEAL_PACK=1` flips
# `EngineOptions::seal_pack` at BOTH seed and open, so the same binary seeds
# and reopens a corpus through the consolidated `.seal` pack (Spike I) instead
# of the `.pidx`/`.filter`/`.pcol` sidecar trio.
#
# One phase per PROCESS so `/proc/self/status VmHWM` is that phase's true peak
# RSS. Reopen is run 3x (best AND median reported, research/05 §15.2).
#
# Comparators:
#   spikes/open_without_book/REPORT.md  — the pre-C Book baseline + post-C
#   spikes/baseline_matrix/REPORT.md §6 — gen2 post-C (2M: 1.667 s, 105 MiB)
set -u
BIN=/home/bob/src/mess/.maw/workspaces/bn-2gu/target/release/examples/owb_bench
ROOT="${MESS_BENCH_DIR:-$HOME/.cache/mess-bench}/spikeJ-reopen"
rm -rf "$ROOT"; mkdir -p "$ROOT"

quiet() { # hold until load1 < 7.5 and no compiler running
  while :; do
    l=$(cut -d' ' -f1 /proc/loadavg)
    if ! pgrep -x rustc >/dev/null && ! pgrep -x cargo >/dev/null \
       && [ "$(echo "$l < 7.5" | bc -l)" = 1 ]; then return; fi
    echo "[quiet-guard] load1=$l; sleeping 5s" >&2
    sleep 5
  done
}

for events in 2000000 10000000; do
  for sp in 0 1; do
    dir="$ROOT/e${events}-sp${sp}"
    export OWB_SEAL_PACK=$sp
    quiet
    # seed: events, streams, per_batch, payload_bytes, segment_mb
    "$BIN" seed "$dir" "$events" 1000 10 64 8 | sed "s/^/[e=$events sp=$sp] /"
    for i in 1 2 3; do
      quiet
      "$BIN" open "$dir" | sed "s/^/[e=$events sp=$sp] /"
    done
    du -sh "$dir" | sed "s/^/[e=$events sp=$sp] disk /"
    ls "$dir" | grep -c '\.seal$' | sed "s/^/[e=$events sp=$sp] seal_files /"
    ls "$dir" | grep -cE '\.(pidx|filter|pcol)$' \
      | sed "s/^/[e=$events sp=$sp] sidecar_files /"
  done
done
rm -rf "$ROOT"
