#!/usr/bin/env bash
set -eu -o pipefail

BASEDIR=$(dirname "$0")
. "${BASEDIR}/.common.sh"

## Starts a coordinator process
# $1: Address of the coordinator to be spawned (e.g. localhost:9189)
# $2: Working directory for pid and log files
# $3: Full path to the the strymon binary
start_coordinator() {
  case "${1}" in
    *:*)
      coord_host=$(echo "${1}" | cut -d":" -f1)
      coord_port=$(echo "${1}" | cut -d":" -f2)
      ;;
    *)
      coord_host="${1}"
      coord_port="9189"
      ;;
  esac

  coord_basedir="${2}"
  coord_binary="${3}"

  spawn_service "coordinator" "${coord_host}" "${coord_basedir}" \
    "${coord_binary}" --log-level info manage start-coordinator \
    --external-hostname "${coord_host}" --port "${coord_port}"
}

## Starts an executor process
# $1: Hostname of the machine on which the executor will be started
# $2: Working directory for pid and log files
# $3: Full path to the the strymon binary
start_executor() {
  exec_host="$(echo ${1} | tr -d '[:space:]')"
  exec_basedir="${2}"
  exec_binary="${3}"

  # TODO(swicki): This currently does not support the port range option
  spawn_service "executor" "${exec_host}" "${exec_basedir}" \
    "${coord_binary}" --log-level info \
    manage start-executor --external-hostname "${exec_host}"
}

#
# main
#
parse_args "$@"
shift $((OPTIND-1))

# ensure paths are absolute
LOGDIR="$(abspath "${LOGDIR}")"
BINARY="$(locate_binary)"
FULL_BINARY="$(abspath "${BINARY}")"

# create working directory and spawn cluster
mkdir -p "${LOGDIR}"
start_coordinator "${COORDINATOR}" "${LOGDIR}" "${FULL_BINARY}"
while read host; do
  start_executor "${host}" "${LOGDIR}" "${FULL_BINARY}"
done < "${EXECUTORS}"

