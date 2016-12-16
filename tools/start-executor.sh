#!/bin/sh
set -eu

USAGE="Usage: $(basename $0) HOST COORD"

PORTS=""
HOST="localhost"

BASEDIR=$(dirname "$0")

parse_args() {
  if [ "$#" -ne 2 ] || [ "$1" == "-h" ] ; then
      echo $USAGE >&2
      exit 1
  fi
  
  HOST="${1}"
  COORD="${2}"
}

locate_binary() {
  NAME="$1"
  BINARY="${BASEDIR}/../target/release/${NAME}"

  if ! [ -x "${BINARY}" ] ; then
    echo "Unable to locate binary: ${NAME}" >&2
    exit 1
  fi
  
  echo "${BINARY}"
}

parse_args "$@"
BINARY=$(locate_binary executor)

scp "${BINARY}" "${HOST}:"
ssh "${HOST}" "RUST_LOG=debug nohup ~/executor -e '${HOST}' -c '${COORD}' > ~/executor.log 2>&1 < /dev/null &"
