#!/usr/bin/env bash
# Run the same suite/scale-factor matrix the Dec2025-SP3 ARM baseline covers.
# usage: run-matrix.sh REVISION

set -uo pipefail

revision="${1:?usage: run-matrix.sh REVISION}"
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
olap="$repo/.venv/bin/olap"
cd "$repo"

run() {
    local suite="$1"
    shift
    echo "=== $(date +%H:%M:%S) START $suite $* (revision=$revision) ==="
    "$olap" benchmark monetdb "$suite" --revision "$revision" --cleanup "$@" 2>&1 \
        | grep -E "INFO olap_benchmarks\.(dbs|__main__|suites)|ERROR|Traceback|Error"
    echo "=== $(date +%H:%M:%S) END $suite $* (exit ${PIPESTATUS[0]}) ==="
}

run kaggle_airbnb
run time_series --scale-factor 1
run chat_threads --scale-factor 1
run tpc_ds --scale-factor 1
run rtabench
run tpc_h --scale-factor 10
run clickbench

echo "=== $(date +%H:%M:%S) MATRIX DONE (revision=$revision) ==="
