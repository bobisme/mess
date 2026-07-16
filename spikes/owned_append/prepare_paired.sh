#!/usr/bin/env bash
set -euo pipefail

script_dir=$(realpath "$(dirname "${BASH_SOURCE[0]}")")
exec python3 "$script_dir/prepare_paired.py" "$@"
