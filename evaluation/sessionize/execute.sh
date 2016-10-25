#!/bin/bash
set -euo pipefail

PREFIX=${1?"Usage: $0 [PREFIX]"}

LOGS="logs/$(hostname)_$(date +%Y%m%d-%H%M%S)/"
PATH="$PATH:~/release"
export RUST_LOG=debug

mkdir -p "$LOGS"

coordinator &>"$LOGS/coordinator.log" &
sleep 3
executor &>"$LOGS/executor.log" &
sleep 3

queries='txdepth servicetop10 txsigtop10 txns msgspan msgcount sessionize'

for query in $queries; do 
    (cd "$LOGS" ; ~/syrupy/syrupy.py --quiet --interval=0.5 --separator=, --no-align -t "$query" -c "$query") &
    submit ~/release/$query "$PREFIX"
    sleep 0.1
done

tail -f "$LOGS/executor.log" || true
pkill -P $$
