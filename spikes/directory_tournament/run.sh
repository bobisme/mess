#!/usr/bin/env bash
# Spike H full tournament run. Every measured phase is quiet-guarded inside
# the binary (loadavg < 6, no compilers, no active_microblocks bench); this
# script just sequences the phases and tees raw logs.
set -euo pipefail
cd "$(dirname "$0")"

STORE="${STORE:-$HOME/.cache/mess-bench-scratch/directory_tournament_store}"
STORE_BIG="${STORE_BIG:-$HOME/.cache/mess-bench-scratch/directory_tournament_store_big}"
STAMP="$(date +%Y%m%d-%H%M%S)"

CLANG_PATH=/usr/bin/clang cargo build --release

./target/release/directory_tournament info 2>&1 | tee "logs/info-$STAMP.log"

# Synthetic matrix (1k..10M x dense/sparse/zipfclust) + 30M.
./target/release/directory_tournament bench --with-30m \
  > "logs/synthetic-$STAMP.csv" 2> >(tee "logs/synthetic-$STAMP.err" >&2)

# Real sealed segments from both generated stores.
./target/release/directory_tournament bench --sizes '' --dists '' --real "$STORE" \
  > "logs/real-$STAMP.csv" 2> >(tee "logs/real-$STAMP.err" >&2) || true
./target/release/directory_tournament bench --sizes '' --dists '' --real "$STORE_BIG" \
  > "logs/real-big-$STAMP.csv" 2> >(tee "logs/real-big-$STAMP.err" >&2) || true

echo "done: logs/*-$STAMP.*"
