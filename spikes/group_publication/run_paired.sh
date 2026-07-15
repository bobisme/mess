#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: $0 <control-bin> <candidate-bin> <control-source> <candidate-source> <output-dir>" >&2
  exit 2
fi

control_bin=$(realpath "$1")
candidate_bin=$(realpath "$2")
control_source=$3
candidate_source=$4
output_dir=$(realpath -m "$5")
mkdir -p "$output_dir"
csv="$output_dir/paired.csv"
log="$output_dir/run.log"
provenance="$output_dir/provenance.txt"
if [[ -e "$csv" ]]; then
  echo "refusing to append to existing $csv" >&2
  exit 2
fi

control_sha=$(sha256sum "$control_bin" | awk '{print $1}')
candidate_sha=$(sha256sum "$candidate_bin" | awk '{print $1}')
control_root=$(realpath "$(dirname "$control_bin")/../../..")
candidate_root=$(realpath "$(dirname "$candidate_bin")/../../..")
control_head=$(git -C "$control_root" rev-parse HEAD)
candidate_head=$(git -C "$candidate_root" rev-parse HEAD)
control_dirty=$(git -C "$control_root" status --porcelain --untracked-files=no)
candidate_dirty=$(git -C "$candidate_root" status --porcelain --untracked-files=no)

if [[ $control_head != "$control_source" || $candidate_head != "$candidate_source" ]]; then
  echo "source argument does not match a benchmark workspace HEAD; fail-stop" >&2
  exit 2
fi
if [[ -n $control_dirty || -n $candidate_dirty ]]; then
  echo "benchmark workspace has tracked changes; fail-stop" >&2
  exit 2
fi

{
  date --iso-8601=seconds
  uname -a
  rustc -Vv
  cargo -V
  lscpu
  findmnt -T "${MESS_BENCH_DIR:-$HOME/.cache/mess-bench}"
  df -h "${MESS_BENCH_DIR:-$HOME/.cache/mess-bench}"
  echo "control_source=$control_source"
  echo "candidate_source=$candidate_source"
  echo "control_tracked_dirty=false"
  echo "candidate_tracked_dirty=false"
  echo "control_harness_sha256=$(sha256sum "$control_root/crates/mess-store/examples/owned_append_bench.rs" | awk '{print $1}')"
  echo "candidate_harness_sha256=$(sha256sum "$candidate_root/crates/mess-store/examples/owned_append_bench.rs" | awk '{print $1}')"
  echo "control_binary_sha256=$control_sha"
  echo "candidate_binary_sha256=$candidate_sha"
  echo "cargo_lock_sha256=$(sha256sum Cargo.lock | awk '{print $1}')"
  printf 'governors='
  for governor in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    if [[ -r $governor ]]; then
      printf '%s ' "$(<"$governor")"
    fi
  done
  printf '\n'
  echo "command=$0 $*"
} >"$provenance"

guard() {
  if pgrep -x cargo >/dev/null || pgrep -x rustc >/dev/null || \
    pgrep -x cc >/dev/null || pgrep -x ld >/dev/null || \
    pgrep -x collect2 >/dev/null || pgrep -x owned_append_be >/dev/null; then
    echo "foreign build or benchmark process present; fail-stop" | tee -a "$log" >&2
    exit 3
  fi
  local x deadline=$((SECONDS + 120))
  while true; do
    x=$(awk '{print $1}' /proc/loadavg)
    if awk -v x="$x" 'BEGIN { exit !(x < 6.0) }'; then
      break
    fi
    if (( SECONDS >= deadline )); then
      echo "load1=$x did not fall below 6.0 in 120s; fail-stop" | \
        tee -a "$log" >&2
      exit 4
    fi
    echo "waiting for load1=$x to fall below 6.0" | tee -a "$log"
    sleep 2
  done
}

run_one() {
  local variant=$1 mode=$2 batch=$3 cycle=$4 slot=$5 binary source sha
  if [[ $variant == control ]]; then
    binary=$control_bin
    source=$control_source
    sha=$control_sha
  else
    binary=$candidate_bin
    source=$candidate_source
    sha=$candidate_sha
  fi
  guard
  OWNED_APPEND_VARIANT=$variant \
    OWNED_APPEND_MODE=$mode \
    OWNED_APPEND_BATCH=$batch \
    OWNED_APPEND_CYCLE=$cycle \
    OWNED_APPEND_SLOT=$slot \
    OWNED_APPEND_SOURCE=$source \
    OWNED_APPEND_BINARY_SHA256=$sha \
    OWNED_APPEND_CSV=$csv \
    "$binary" 2>&1 | tee -a "$log"
}

for mode in process group; do
  for batch in 1 10 100 1000; do
    for cycle in 1 2 3 4 5; do
      if (( cycle % 2 == 1 )); then
        order=(control candidate candidate control)
      else
        order=(candidate control control candidate)
      fi
      slot=0
      for variant in "${order[@]}"; do
        slot=$((slot + 1))
        run_one "$variant" "$mode" "$batch" "$cycle" "$slot"
      done
    done
  done
done

"$(dirname "$0")/evaluate.py" "$csv" | tee "$output_dir/evaluation.txt"
