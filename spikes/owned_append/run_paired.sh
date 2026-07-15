#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "usage: $0 <control-bin> <candidate-bin> <control-source> <candidate-source> <output-dir>" >&2
  exit 2
fi

script_dir=$(realpath "$(dirname "${BASH_SOURCE[0]}")")
runner="$script_dir/$(basename "${BASH_SOURCE[0]}")"
evaluator="$script_dir/evaluate.py"
control_bin=$(realpath "$1")
candidate_bin=$(realpath "$2")
control_source=$3
candidate_source=$4
output_dir=$(realpath -m "$5")
if [[ -e $output_dir || -L $output_dir ]]; then
  echo "refusing non-fresh output directory $output_dir" >&2
  exit 2
fi
if [[ ! $control_source =~ ^[0-9a-f]{40}$ || \
      ! $candidate_source =~ ^[0-9a-f]{40}$ ]]; then
  echo "source arguments must be full lowercase commit ids" >&2
  exit 2
fi
if [[ ! -x $control_bin || ! -x $candidate_bin || ! -x $evaluator ]]; then
  echo "control, candidate, and evaluator must be executable" >&2
  exit 2
fi

control_root=$(realpath "$(dirname "$control_bin")/../../..")
candidate_root=$(realpath "$(dirname "$candidate_bin")/../../..")
if [[ $output_dir == "$control_root" || $output_dir == "$control_root/"* ||
      $output_dir == "$candidate_root" || $output_dir == "$candidate_root/"* ]]; then
  echo "output directory must be outside both frozen source workspaces" >&2
  exit 2
fi
control_harness="$control_root/crates/mess-store/examples/owned_append_bench.rs"
candidate_harness="$candidate_root/crates/mess-store/examples/owned_append_bench.rs"
control_lock="$control_root/Cargo.lock"
candidate_lock="$candidate_root/Cargo.lock"
for frozen_file in \
  "$runner" "$evaluator" "$control_harness" "$candidate_harness" \
  "$control_lock" "$candidate_lock"; do
  if [[ ! -f $frozen_file ]]; then
    echo "missing frozen input $frozen_file" >&2
    exit 2
  fi
done

control_head=$(git -C "$control_root" rev-parse HEAD)
candidate_head=$(git -C "$candidate_root" rev-parse HEAD)
control_dirty=$(git -C "$control_root" status --porcelain)
candidate_dirty=$(git -C "$candidate_root" status --porcelain)
if [[ $control_head != "$control_source" || $candidate_head != "$candidate_source" ]]; then
  echo "source argument does not match a benchmark workspace HEAD; fail-stop" >&2
  exit 2
fi
if [[ -n $control_dirty || -n $candidate_dirty ]]; then
  echo "benchmark workspace is dirty; fail-stop" >&2
  exit 2
fi

control_sha=$(sha256sum "$control_bin" | awk '{print $1}')
candidate_sha=$(sha256sum "$candidate_bin" | awk '{print $1}')
control_harness_sha=$(sha256sum "$control_harness" | awk '{print $1}')
candidate_harness_sha=$(sha256sum "$candidate_harness" | awk '{print $1}')
control_lock_sha=$(sha256sum "$control_lock" | awk '{print $1}')
candidate_lock_sha=$(sha256sum "$candidate_lock" | awk '{print $1}')
runner_sha=$(sha256sum "$runner" | awk '{print $1}')
evaluator_sha=$(sha256sum "$evaluator" | awk '{print $1}')
if [[ $control_harness_sha != "$candidate_harness_sha" ]]; then
  echo "control and candidate harnesses differ; fail-stop" >&2
  exit 2
fi
if [[ $control_lock_sha != "$candidate_lock_sha" ]]; then
  echo "control and candidate Cargo.lock files differ; fail-stop" >&2
  exit 2
fi
if [[ $control_source == "$candidate_source" || $control_sha == "$candidate_sha" ]]; then
  echo "control and candidate must be distinct frozen artifacts; fail-stop" >&2
  exit 2
fi

mkdir -p "$(dirname "$output_dir")"
mkdir "$output_dir"
csv="$output_dir/paired.csv"
log="$output_dir/run.log"
provenance="$output_dir/provenance.txt"

{
  echo "protocol=bn-2yye-process-successor-v1"
  date --iso-8601=seconds
  uname -a
  rustc -Vv
  cargo -V
  lscpu
  findmnt -T "${MESS_BENCH_DIR:-$HOME/.cache/mess-bench}"
  df -h "${MESS_BENCH_DIR:-$HOME/.cache/mess-bench}"
  echo "control_source=$control_source"
  echo "candidate_source=$candidate_source"
  echo "control_dirty=false"
  echo "candidate_dirty=false"
  echo "control_root=$control_root"
  echo "candidate_root=$candidate_root"
  echo "control_binary_path=$control_bin"
  echo "candidate_binary_path=$candidate_bin"
  echo "control_harness_path=$control_harness"
  echo "candidate_harness_path=$candidate_harness"
  echo "control_cargo_lock_path=$control_lock"
  echo "candidate_cargo_lock_path=$candidate_lock"
  echo "runner_path=$runner"
  echo "evaluator_path=$evaluator"
  echo "control_harness_sha256=$control_harness_sha"
  echo "candidate_harness_sha256=$candidate_harness_sha"
  echo "control_binary_sha256=$control_sha"
  echo "candidate_binary_sha256=$candidate_sha"
  echo "control_cargo_lock_sha256=$control_lock_sha"
  echo "candidate_cargo_lock_sha256=$candidate_lock_sha"
  echo "runner_sha256=$runner_sha"
  echo "evaluator_sha256=$evaluator_sha"
  printf 'governors='
  for governor in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    if [[ -r $governor ]]; then
      printf '%s ' "$(<"$governor")"
    fi
  done
  printf '\n'
  echo "command=$0 $*"
} >"$provenance"

frozen_fail() {
  echo "frozen provenance changed: $1; fail-stop" | tee -a "$log" >&2
  exit 3
}

verify_frozen() {
  [[ $(git -C "$control_root" rev-parse HEAD) == "$control_source" ]] || \
    frozen_fail "control HEAD"
  [[ $(git -C "$candidate_root" rev-parse HEAD) == "$candidate_source" ]] || \
    frozen_fail "candidate HEAD"
  [[ -z $(git -C "$control_root" status --porcelain) ]] || \
    frozen_fail "control worktree"
  [[ -z $(git -C "$candidate_root" status --porcelain) ]] || \
    frozen_fail "candidate worktree"
  [[ $(sha256sum "$control_bin" | awk '{print $1}') == "$control_sha" ]] || \
    frozen_fail "control binary"
  [[ $(sha256sum "$candidate_bin" | awk '{print $1}') == "$candidate_sha" ]] || \
    frozen_fail "candidate binary"
  [[ $(sha256sum "$control_harness" | awk '{print $1}') == "$control_harness_sha" ]] || \
    frozen_fail "control harness"
  [[ $(sha256sum "$candidate_harness" | awk '{print $1}') == "$candidate_harness_sha" ]] || \
    frozen_fail "candidate harness"
  [[ $(sha256sum "$control_lock" | awk '{print $1}') == "$control_lock_sha" ]] || \
    frozen_fail "control Cargo.lock"
  [[ $(sha256sum "$candidate_lock" | awk '{print $1}') == "$candidate_lock_sha" ]] || \
    frozen_fail "candidate Cargo.lock"
  [[ $(sha256sum "$runner" | awk '{print $1}') == "$runner_sha" ]] || \
    frozen_fail "runner"
  [[ $(sha256sum "$evaluator" | awk '{print $1}') == "$evaluator_sha" ]] || \
    frozen_fail "evaluator"
}

guard() {
  verify_frozen
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

mode=process
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

verify_frozen
csv_sha=$(sha256sum "$csv" | awk '{print $1}')
csv_rows=$(awk 'END { print NR - 1 }' "$csv")
csv_columns=$(awk -F, 'NR == 1 { print NF; exit }' "$csv")
{
  echo "paired_csv_path=$csv"
  echo "paired_csv_sha256=$csv_sha"
  echo "paired_csv_data_rows=$csv_rows"
  echo "paired_csv_columns=$csv_columns"
} >>"$provenance"
"$evaluator" "$csv" "$provenance" | tee "$output_dir/evaluation.txt"
