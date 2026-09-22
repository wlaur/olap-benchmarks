#!/usr/bin/env bash
# Run the SP3-baseline matrix twice: pipeline off, then pipeline on.
set -uo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "############ $(date +%H:%M:%S) PASS 1/2: pipeline OFF (revision=master2)"
"$here/run-matrix.sh" master2

echo "############ $(date +%H:%M:%S) PASS 2/2: pipeline ON (revision=master2-pp)"
OLAP_BENCHMARKS_MONETDB_GDK_DEBUG=524288 "$here/run-matrix.sh" master2-pp

echo "############ $(date +%H:%M:%S) BOTH PASSES DONE"
