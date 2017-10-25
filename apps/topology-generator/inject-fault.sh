#!/usr/bin/env bash
# this script is a simple bash tcp client for systems without telnet/netcat
set -o errexit

CMD=${1?"Usage: $0 <disconnect-random-switch|disconnect-random-link> [HOSTNAME]"}
HOST=${2:-localhost}
PORT=9201

exec 5<>/dev/tcp/${HOST}/${PORT}
echo "${CMD}" >&5
head -1 <&5
