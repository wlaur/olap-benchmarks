#!/usr/bin/env bash
set -u

role="$1"
initializer="$2"
doris_home="${DORIS_HOME:-/opt/apache-doris}"

case "$role" in
    fe|be) ;;
    *) exit 2 ;;
esac

if [[ "$role" == "fe" && -n "${OLAP_DORIS_FE_HEAP_MAXIMUM_MB:-}" ]]; then
    initial="${OLAP_DORIS_FE_HEAP_INITIAL_MB:-}"
    maximum="$OLAP_DORIS_FE_HEAP_MAXIMUM_MB"
    if [[ ! "$initial" =~ ^[1-9][0-9]*$ || ! "$maximum" =~ ^[1-9][0-9]*$ ]] || (( initial > maximum )); then
        exit 2
    fi
    fe_config="$doris_home/fe/conf/fe.conf"
    if ! awk -v initial="$initial" -v maximum="$maximum" '
        /^JAVA_OPTS_FOR_JDK_17=/ {
            if (gsub(/-Xms[0-9]+[mMgG]/, "-Xms" initial "m") != 1 ||
                gsub(/-Xmx[0-9]+[mMgG]/, "-Xmx" maximum "m") != 1) exit 2
            matches++
        }
        { print }
        END { if (matches != 1) exit 2 }
    ' "$fe_config" > "$fe_config.benchmark"; then
        printf '%s\n' 'Unsupported Doris FE JVM heap configuration' >&2
        exit 2
    fi
    mv "$fe_config.benchmark" "$fe_config" || exit 1
fi

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
