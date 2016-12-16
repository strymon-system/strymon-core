#!/bin/sh
set -eu

USAGE="Usage: $(basename $0) HOST QUERY_ID"

parse_args() {
  if [ "$#" -ne 2 ] || [ "$1" == "-h" ] ; then
      echo $USAGE >&2
      exit 1
  fi
  
  HOST="${1}"
  QUERY_ID="${2}"
}

parse_args "$@"

ssh "${HOST}" "tail -n +1 -f ~/executor.log | sed -u -n 's/timely_query::executor::executable: QueryId(${QUERY_ID}) |\(.*\)/\1/p'"
