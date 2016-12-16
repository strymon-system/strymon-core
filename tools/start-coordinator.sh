#!/bin/sh
set -eu

USAGE="Usage: $(basename $0) [-p PORT] HOST"

PORT=9189
HOST="localhost"

BASEDIR=$(dirname "$0")

parse_args() {
  while getopts 'p:' OPT; do
    case "$OPT" in
      'p')
        PORT="${OPTARG}"
        ;;
      'h')
        echo $USAGE
        exit 0
        ;;
      '?')
        echo $USAGE >&2
        exit 1
        ;;

    esac
  done
  shift $((${OPTIND} - 1))

  if [ "$#" -ne 1 ] ; then
      echo $USAGE >&2
      exit 1
  fi
  
  HOST="${1}"
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
BINARY=$(locate_binary coordinator)

scp "${BINARY}" "${HOST}:"
ssh "${HOST}" "RUST_LOG=debug nohup ~/coordinator -p ${PORT} -e ${HOST} > ~/coordinator.log 2>&1 < /dev/null &"
