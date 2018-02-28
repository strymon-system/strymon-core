#!/bin/sh
set -eu
BASEDIR=$(dirname "$0")
INDEX="${BASEDIR}/../target/doc/index.html"

CRATES="strymon_communication strymon_coordinator strymon_executor strymon_job strymon_model strymon_rpc"
for crate in $CRATES ; do
  cargo doc --no-deps --package $crate
done

echo "<meta http-equiv=refresh content=0;url=strymon_job/index.html>" > "${INDEX}"
