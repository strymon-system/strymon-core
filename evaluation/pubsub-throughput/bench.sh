#!/usr/bin/env bash
set -euo pipefail

FEATURES="pubsub"
MSGSIZES="10 100 1000"
MSGCOUNT="1000"

TARGET="../../target/release/"
PATH=$PATH:$(realpath $TARGET)

getpath() {
    BINARY="$TARGET/$1"
    echo $BINARY
}

spawn() {
    NAME=$1
    shift
    "$NAME" "$@" &>$NAME.log &
}

export RUST_LOG=debug

(cd ../.. && RUST_LOG=error cargo build --release)

for feature in $FEATURES ; do
    RUST_LOG=error cargo build --release --features "$feature"

    for msgsize in $MSGSIZES ; do
        spawn coordinator
        coordinator=$!
        spawn executor

        submit `which consumer` $MSGCOUNT $msgsize
        submit `which producer` $MSGCOUNT $msgsize

        wait $coordinator
    done
done
