#!/usr/bin/env bash
set -u

role="$1"
initializer="$2"
doris_home="${DORIS_HOME:-/opt/apache-doris}"

case "$role" in
    fe|be) ;;
    *) exit 2 ;;
esac

shutdown() {
    if ! "$doris_home/$role/bin/stop_$role.sh" --grace; then
        exit 1
    fi
    wait "$initializer_pid"
    exit 0
}

trap shutdown TERM INT
bash "$initializer" &
initializer_pid=$!
wait "$initializer_pid"
