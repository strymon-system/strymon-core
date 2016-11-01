#!/bin/bash
set -euo pipefail

PREFIX=${1?"Usage: $0 [PREFIX] [THREADS] [TOPO]"}
THREADS=${2?"Usage: $0 [PREFIX] [THREADS] [TOPO]"}
TOPO=${3?"Usage: $0 [PREFIX] [THREADS] [TOPO]"}
PROCESSES=1

LOGS="logs/$(hostname)_$(date +%Y%m%d-%H%M%S)_p${PROCESSES}_t${THREADS}/"
PATH="$PATH:~/release"
export RUST_LOG=debug

mkdir -p "$LOGS"

taskset -ca "$TOPO" monolith "$PREFIX" "$LOGS" -w ${THREADS} &> "$LOGS/monolith.log" &

python2 ~/monitor.py monolith | tee "$LOGS/monitor.log"
