#!/usr/bin/env bash
set -eu -o pipefail

BASEDIR="$(dirname "${0}")"
BINDIR="${BASEDIR}/../bin"
OUTDIR=$(mktemp -d 2>/dev/null || mktemp -d -t 'strymon_tests')

STRYMON="${BINDIR}/strymon"

## Starts a local strymon tests instance, keeping artifacts in $OUTDIR
start_strymon() {
    echo "localhost" > "${OUTDIR}/executors"
    "${BINDIR}/start-strymon.sh" -l "${OUTDIR}" -w "${OUTDIR}" -e "${OUTDIR}/executors"
}

## Stops the strymon test instance
stop_strymon() {
    "${BINDIR}/stop-strymon.sh" -l "${OUTDIR}" -w "${OUTDIR}" -e "${OUTDIR}/executors"
}

## Spawns a binary and extracts its job id
# $@: Arguments passed down to `strymon submit`
submit() {
    $STRYMON submit "${@}" | grep "Successfully spawned job:" | cut -d':' -f2  | tr -d ' '
}

## Teminates a job
# $1: The job id of the job to terminate
terminate() {
    $STRYMON terminate "${1}"
}

## Waits for certain output to occur in the job output.
## Times out with a failure after 10 seconds without matching the regex.
# $1: Job id
# $2: Output regex to block on
wait_job_output() {
    local executor_log="${OUTDIR}/executor_localhost.log"
    for i in $(seq 10); do
        if grep -F "QueryId(${1}) |" "${executor_log}" | grep -qE "${2}" ; then
            return 0
        fi
        sleep 1
    done

    echo "Timed out waiting for job $1"
    return 1
}

## Basic integration test for the publish-subscribe protocol
test_pubsub() {
     sub_id=$(submit --bin subscriber "${BASEDIR}/simple-pubsub")
     pub_id=$(submit --bin publisher "${BASEDIR}/simple-pubsub")
     # wait for subscriber to receive some tuples
     wait_job_output "${sub_id}" 'Subscriber received [0-9]+ batches'
     terminate "${pub_id}"
     terminate "${sub_id}"
}

## Partitioned publish-subscribe protocol
test_partitioned_pubsub() {
     sub_id=$(submit --bin multisub "${BASEDIR}/simple-pubsub" -- 4)
     pub_id=$(submit --bin multipub --workers 4 "${BASEDIR}/simple-pubsub")
     # wait for subscriber to receive some tuples
     wait_job_output "${sub_id}" 'Subscriber received [0-9]+ batches'
     terminate "${pub_id}"
     terminate "${sub_id}"
}

## Test for the example from the documentation
test_example() {
     # start the topology generator with a small fat-tree
     topo_id=$(submit "${BASEDIR}/../apps/topology-generator")
     wait_job_output "${topo_id}" 'Hosts: 1024, Switches: 320, Ports: 16, Links: 2048'
     # run connected components and wait for the result
     cc_id=$(submit "${BASEDIR}/../apps/connected-components")
     wait_job_output "${cc_id}" 'All nodes in the graph are now connected.'
     # disconnect a random switch
     "${BASEDIR}/../apps/topology-generator/inject-fault.sh" disconnect-random-switch
     wait_job_output "${topo_id}" 'Disconnecting randomly chosen switch \#[0-9]+.'
     wait_job_output "${cc_id}" 'There are now 2 disconnected partitions in the graph\!'
     terminate "${cc_id}"
     terminate "${topo_id}"
}

#
# main
#
echo "Building everything in release mode..."
cargo build --release --all

echo "Test artifacts in: ${OUTDIR}"

start_strymon
trap stop_strymon EXIT

TESTS=(test_example test_pubsub test_partitioned_pubsub)

for test in ${TESTS[@]}; do
    echo "===== Running '$test' ====="
    $test
done

echo "Tests successful."
