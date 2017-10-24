#
# configuration variables
#
LOGDIR="${BASEDIR}/../logs"
EXECUTORS="${BASEDIR}/../conf/executors"
COORDINATOR="localhost:9189"

# Platform-independent implementation for generating an absolute path
# $1: relative or absolute filesystem path
abspath() {
    case ${1} in
    /*)
        ;;
    *)
        echo "$(pwd)/${1}"
    esac
}

## Prints the command line usage and quits
# $0: Name of the script
# $COORDINATOR, $EXECUTORS, $LOGDIR: Default values
usage() {
  printf "Usage: $0 [-c <HOSTNAME:PORT>] [-e <HOSTFILE>] [-l <LOGDIR>]\n">&2
  printf "\n">&2
  printf "    -c <ADDR>   Address of the coordinator to be spawned (default: \"${COORDINATOR}\").\n"
  printf "    -e <FILE>   Executor hosts file (default: \"${EXECUTORS}\").\n"
  printf "    -l <DIR>    Working directory for log and pid files (default: \"${LOGDIR}\").\n"
  exit 1
}

## Parses the command line arguments according to the usage function above
## Writes result to $COORDINATOR, $EXECUTORS and $LOGDIR
# $1...: command line arguments
parse_args() {
  while getopts '?hc:e:l:' opt; do
    case "${opt}" in
      c)
        COORDINATOR="${OPTARG}"
        ;;
      e)
        EXECUTORS="${OPTARG}"
        ;;
      l)
        LOGDIR="${OPTARG}"
        ;;
      *)
        usage
        ;;
    esac
  done
}

## Executes a command on a remote host using ssh
# $1: Remote host name (or: "localhost")
# $2...: Command to execute
remote_cmd() {
  remote_host="${1}"
  remote_args="${2}"

  case "${remote_host}" in
    "localhost"|"127.0.0.1"|"::1")
      sh -c "${remote_args}"
      ;;
    *)
      ssh "${remote_host}" "${remote_args}"
  esac
}

## Spawns a binary as a deamon, keeping its log and pid file
# $1: name of service to spawn
# $2: host on which the service will be spawned
# $3: working directory for log and pid files
# $4...: command to spawn
spawn_service() {
  service="${1}"
  host="${2}"
  basedir="${3}"
  shift 3
  command="$@"

  # artifacts
  logfile="${basedir}/${service}_${host}.log"
  pidfile="${basedir}/${service}_${host}.pid"

  # basic sanity check
  if [ -e "${pidfile}" ] ; then
    echo "error: ${service} seems to be already running on ${host}" >&2
    echo "remove '${pidfile}' to override this check" >&2
    exit 1
  fi

  cmd="nohup ${command[@]} >> \"${logfile}\" 2>&1 & echo \$! > \"${pidfile}\""
  remote_cmd "$host" "$cmd"
}

## Spawns a binary as a deamon, keeping its log and pid file
# $1: name of service to stop
# $2: host on which the service is running
# $3: working directory for log and pid files
stop_service() {
  service="${1}"
  host="${2}"
  basedir="${3}"

  pidfile="${basedir}/${service}_${host}.pid"

  if ! [ -e "${pidfile}" ] ; then
    echo "error: ${service} does not seem to be running on ${host}" >&2
    exit 1
  fi
  
  pid="$(cat "${pidfile}")"

  cmd="kill ${pid}; rm \"${pidfile}\""
  remote_cmd "$host" "$cmd"
}
