#!/usr/bin/env bash
# bn-3pz bisect: is the Group-mode convoy split (composed engine pays ~2x the
# barriers bare achieves at the same shape) a REGRESSION introduced by
# bn-34o / bn-2cj / bn-2ib(C), or a structural property of the composed
# producer path that predates all three?
#
# The probe is the FROZEN spikes/baseline_matrix binary (`point` subcommand:
# bare + log only, no flat engine), copied unmodified into a worktree at each
# commit and built there — so each row is the engine AS IT WAS at that commit,
# driven by an identical driver.
#
#   HEAD      16243284  today (post 34o + 2cj + C + E + I)
#   pre-c     43e8bd2a  f0ab89e9^  (post 34o, post 2cj, PRE Spike C)
#   pre-34o   a3d40ff1  9d4ec27d^  (post 2cj, PRE bn-34o) == Spike B's gen1 base
#
# Cell: the sharpest, most reproducible convoy-split cell from the Spike J
# convoy sweep — Group, 250 B, batch 1000, 4 writers, 100 batches/writer, where
# today's engine pays 194-200 barriers against bare's 100.
set -u
CELL_ARGS="group 250 1000 4 100"
SP=/tmp/claude-1000/-home-bob-src-mess/aadc1558-5638-4541-b43b-481a77f94f96/scratchpad
HEAD_BIN=/home/bob/src/mess/.maw/workspaces/bn-2gu/spikes/baseline_matrix/target/release/baseline_matrix

run3() { # $1 = binary, $2 = label
  for rep in 0 1 2; do
    for eng in bare log; do
      sleep 4
      out=$("$1" point "$eng" $CELL_ARGS 2>/dev/null | tail -1)
      echo "$2 rep$rep $out"
    done
  done
}

echo "=== bn-3pz bisect: Group 250B x 1000 x 4w (bpw 100) ==="
run3 "$HEAD_BIN" "HEAD-16243284"
run3 "$SP/pre-c/spikes/bisect_probe/target/release/bisect_probe" "pre-C-43e8bd2a"
run3 "$SP/pre-34o/spikes/bisect_probe/target/release/bisect_probe" "pre-34o-a3d40ff1"
