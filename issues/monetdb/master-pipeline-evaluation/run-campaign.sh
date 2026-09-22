#!/usr/bin/env bash
# Run the full matrix twice against one server build: pipeline off, then on.
#
# usage: run-campaign.sh REVISION [PIPELINE_REVISION]
#
#   REVISION           results revision for the pipeline-OFF pass (MonetDB's default)
#   PIPELINE_REVISION  results revision for the pipeline-ON pass; defaults to REVISION-pp
#
# Each pass writes results/<revision>.db. Pick a revision name that identifies the
# server build, e.g. the short commit: run-campaign.sh ee305e4
#
# Prerequisites, in order:
#   1. An image for the build under test, tagged the way the harness expects
#      (see this directory's README).
#   2. MONETDB_RELEASE in olap_benchmarks/dbs/monetdb/__init__.py repinned to it.
#   3. Nothing else holding port 50000 -- stop other MonetDB containers first.
set -uo pipefail

revision="${1:?usage: run-campaign.sh REVISION [PIPELINE_REVISION]}"
revision_pp="${2:-${revision}-pp}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"

pin=$(grep -oE 'MonetDBRelease\(.*\)' "$repo/olap_benchmarks/dbs/monetdb/__init__.py")
echo "############ $(date +%H:%M:%S) server pin: $pin"

for r in "$revision" "$revision_pp"; do
    if [[ -e "$repo/results/$r.db" ]]; then
        echo "refusing to overwrite an existing results/$r.db -- pick another revision" >&2
        exit 1
    fi
done

echo "############ $(date +%H:%M:%S) PASS 1/2: pipeline OFF (revision=$revision)"
"$here/run-matrix.sh" "$revision"

echo "############ $(date +%H:%M:%S) PASS 2/2: pipeline ON (revision=$revision_pp)"
OLAP_BENCHMARKS_MONETDB_GDK_DEBUG=524288 "$here/run-matrix.sh" "$revision_pp"

echo "############ $(date +%H:%M:%S) BOTH PASSES DONE"
echo "compare with: $here/compare.sh $revision $revision_pp"
