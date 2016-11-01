#!/bin/bash
set -eu
CURRENT="$(dirname "$(readlink -f "$0")")"
TARGET="${CURRENT}/../../target"

queries=$(find "$CURRENT/src/bin" -iname '*.rs' -exec basename -s .rs '{}' ';')

(cd $CURRENT/../.. ; cargo build --release)
(cd $CURRENT ; cargo build --release)

for bin in submit coordinator executor $queries ; do
    rsync -v "$TARGET/release/$bin" cisco1.ethz.ch:~/release/
done

scp monitor.py cisco1.ethz.ch:~
scp execute.sh cisco1.ethz.ch:~
scp execute_mono.sh cisco1.ethz.ch:~
