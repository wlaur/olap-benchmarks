#!/usr/bin/env bash
# Time MonetDB master's native parquet reader ingesting TPC-H SF10 lineitem,
# against the ADBC ingest time the benchmark harness records for the same table.
#
# usage: parquet-vs-adbc.sh [GDK_DEBUG]

set -uo pipefail

gdk_debug="${1:-}"
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
data="$repo/data/input/tpc_h_sf10"
image=wlaur/monetdb-container:56.0.0-1
name=mdb-parquet-bench

extra="--loadmodule=parquet"
if [[ -n "$gdk_debug" ]]; then
    extra="$extra -d$gdk_debug"
fi

docker rm -f "$name" >/dev/null 2>&1
docker volume rm "$name-vol" >/dev/null 2>&1
docker volume create "$name-vol" >/dev/null

docker run -d --name "$name" --platform linux/arm64 \
    -v "$data:/data:ro" \
    -v "$name-vol:/var/monetdb5/dbfarm" \
    -e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=pq \
    -e MSERVER5_EXTRA_ARGS="$extra" \
    "$image" >/dev/null

n=0
until docker exec "$name" sh -c 'printf "user=monetdb\npassword=monetdb\n" > /tmp/.m 2>/dev/null; DOTMONETDBFILE=/tmp/.m mclient -h127.0.0.1 -p50000 -dpq -fraw -s "select 1"' >/dev/null 2>&1 || [ $n -ge 60 ]; do
    n=$((n + 1))
    sleep 2
done

q() {
    docker exec -e DOTMONETDBFILE=/tmp/.m "$name" \
        mclient -h127.0.0.1 -p50000 -dpq -fcsv -tperformance -s "$1" 2>&1
}

echo "### server: gdk_debug=${gdk_debug:-unset}"
q "select value from sys.env() where name = 'monet_version'"

echo
echo "### 1. parquet -> table WITH primary key (same DDL as the harness)"
q "create table lineitem_pk (
    l_orderkey BIGINT NOT NULL, l_partkey BIGINT NOT NULL, l_suppkey BIGINT NOT NULL,
    l_linenumber INTEGER NOT NULL, l_quantity DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL,
    l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR(1) NOT NULL,
    l_linestatus VARCHAR(1) NOT NULL, l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL,
    l_shipinstruct VARCHAR(25) NOT NULL, l_shipmode VARCHAR(10) NOT NULL,
    l_comment VARCHAR(44) NOT NULL,
    PRIMARY KEY (l_orderkey, l_linenumber))"
q "insert into lineitem_pk select * from '/data/lineitem.parquet'"

echo
echo "### 2. parquet -> table WITHOUT constraints"
q "create table lineitem_plain (
    l_orderkey BIGINT NOT NULL, l_partkey BIGINT NOT NULL, l_suppkey BIGINT NOT NULL,
    l_linenumber INTEGER NOT NULL, l_quantity DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL,
    l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR(1) NOT NULL,
    l_linestatus VARCHAR(1) NOT NULL, l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL,
    l_shipinstruct VARCHAR(25) NOT NULL, l_shipmode VARCHAR(10) NOT NULL,
    l_comment VARCHAR(44) NOT NULL)"
q "insert into lineitem_plain select * from '/data/lineitem.parquet'"

echo
echo "### 3. scan only (no table write)"
q "select count(*), sum(l_quantity) from '/data/lineitem.parquet'"

echo
echo "### 4. row counts and a checksum against the written tables"
q "select count(*) as pk_rows, sum(l_orderkey) as pk_sum from lineitem_pk"
q "select count(*) as plain_rows, sum(l_orderkey) as plain_sum from lineitem_plain"

echo
echo "### peak container memory"
docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' "$name"
