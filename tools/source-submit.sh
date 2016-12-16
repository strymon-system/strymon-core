#!/bin/sh
export PATH=$PATH:$(realpath "$(dirname $0)")
export PATH=$PATH:$(realpath "$(dirname $0)/../target/release/")
export TIMELY_QUERY_HOSTNAME="ThinkPad.local"
