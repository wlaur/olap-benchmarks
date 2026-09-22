#!/usr/bin/env bash
# Compare a campaign against the Dec2025-SP3 baseline in results/default.db.
#
# usage: compare.sh REVISION [PIPELINE_REVISION]
#
#   compare.sh ee305e4              two-way:   SP3 vs the pipeline-off pass
#   compare.sh ee305e4 ee305e4-pp   three-way: SP3 vs pipeline off vs pipeline on
set -uo pipefail

revision="${1:?usage: compare.sh REVISION [PIPELINE_REVISION]}"
revision_pp="${2:-}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
olap="$repo/.venv/bin/olap"
cd "$repo"

[[ -e results/default.db ]] || { echo "results/default.db (the SP3 baseline) is missing" >&2; exit 1; }
[[ -e "results/$revision.db" ]] || { echo "results/$revision.db is missing" >&2; exit 1; }

run_sql() {  # run_sql <file> <title>
    echo
    echo "### $2"
    local sql
    sql=$(cat "$here/$1")
    if [[ -n "$revision_pp" ]]; then
        sql=${sql//\{\{PP\}\}/$revision_pp}
    fi
    "$olap" results query --revision "$revision" "$sql"
}

if [[ -n "$revision_pp" ]]; then
    [[ -e "results/$revision_pp.db" ]] || { echo "results/$revision_pp.db is missing" >&2; exit 1; }
    run_sql compare-all.sql  "suite totals: SP3 vs $revision vs $revision_pp"
    run_sql perquery-all.sql "per-query: SP3 vs $revision vs $revision_pp"
    run_sql ingest-mem.sql   "ingest and peak server memory"
else
    run_sql compare.sql  "suite totals: SP3 vs $revision"
    run_sql perquery.sql "per-query: SP3 vs $revision"
fi
