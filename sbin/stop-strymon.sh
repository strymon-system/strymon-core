#!/bin/sh
set -eu -o pipefail

BASEDIR=$(dirname "$0")
. "${BASEDIR}/.common.sh"

#
# main
#
parse_args "$@"
shift $((OPTIND-1))

# ensure paths are absolute
LOGDIR="$(abspath "${LOGDIR}")"

# extract coordinator hostname
case "${COORDINATOR}" in
*:*)
  coord_host=$(echo "${COORDINATOR}" | cut -d":" -f1)
  ;;
*)
  coord_host="${COORDINATOR}"
  ;;
esac

while read host; do
  stop_service "executor" "${host}" "${LOGDIR}"
done < "${EXECUTORS}"
stop_service "coordinator" "${coord_host}" "${LOGDIR}"

