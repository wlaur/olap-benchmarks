# TDengine and 4,094-metric range comparisons

Working laptop measurements collected on 2026-10-11, with other user work active.
Revision: `2026-10-timeseries-4094-tdengine-working`. Only `time_series_ranges`
was refreshed; the historical 5,000-metric campaigns remain in
[CALCULATION_RESULTS.md](CALCULATION_RESULTS.md) and
[WORKING_RESULTS.md](WORKING_RESULTS.md).

All six engines use 100 days at ten-minute intervals: 14,400 timestamps,
100/1,000/2,000/4,094 nullable Float32 metrics, paired wide and dense EAV layouts.
The largest EAV table has 58,953,600 observations, including nulls. The range
suite's largest width reserves a timestamp and a future device tag within
TDengine's 4,096-column physical limit. Other suites retain their prior widths.

Each engine runs 120 queries: fetch five/ten metrics, average those metrics,
and calculate an hourly derived signal from ten aligned metrics, over 1/10/100
days. See the [workload definition](README.md) for the arithmetic and null
semantics. Tables show median end-to-end client milliseconds: **wide / EAV**.
Each query has one warmup and three measured iterations. Fetch and simple
average retain their native wide/long shapes; calculated outputs have the same
shape and include EAV alignment in the timer.

TDengine is competitive with ClickHouse on the 100-day wide average, but is
slower for full-range fetches and derived calculations in this configured
client workflow. Wide beats EAV for those long-range ten-metric cases on every
engine in this campaign. Short selected-metric ClickHouse queries still often
favor EAV; the 100-day ten-metric averages are approximately equal. These noisy
working measurements support keeping the aligned wide model for the stated
plant workload rather than migrating to EAV for a short-query advantage.

## Fetch and average, ten of 4,094 metrics

| Engine | Fetch 1 day | AVG 1 day | Fetch 10 days | AVG 10 days | Fetch 100 days | AVG 100 days |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Arc | 8.94 / 7.43 | 8.71 / 7.35 | 30.57 / 46.22 | 29.59 / 44.38 | 234.18 / 434.84 | 229.81 / 415.59 |
| ClickHouse (tuned) | 9.40 / 6.89 | 9.83 / 6.35 | 11.41 / 8.21 | 10.77 / 6.62 | 13.78 / 21.97 | 11.23 / 11.74 |
| DuckDB | 1.04 / 2.32 | 0.93 / 2.18 | 1.07 / 16.70 | 0.93 / 16.27 | 1.90 / 155.41 | 1.16 / 149.03 |
| MonetDB | 7.96 / 13.98 | 1.35 / 12.77 | 9.13 / 300.58 | 1.60 / 399.05 | 11.04 / 3473.87 | 1.84 / 3415.35 |
| Polars 2 | 30.62 / 5.93 | 30.36 / 5.97 | 30.58 / 17.62 | 30.63 / 17.18 | 33.54 / 126.65 | 33.61 / 123.39 |
| TDengine | 9.92 / 8.51 | 9.18 / 6.38 | 14.57 / 37.23 | 14.62 / 14.23 | 45.07 / 245.98 | 10.91 / 15.24 |

## Fetch and average, five of 4,094 metrics

| Engine | Fetch 1 day | AVG 1 day | Fetch 10 days | AVG 10 days | Fetch 100 days | AVG 100 days |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Arc | 8.80 / 7.45 | 8.66 / 7.18 | 30.44 / 45.30 | 30.03 / 44.68 | 234.45 / 425.35 | 232.50 / 414.29 |
| ClickHouse (tuned) | 9.42 / 4.66 | 8.79 / 4.16 | 10.79 / 5.59 | 10.14 / 5.63 | 11.94 / 10.95 | 10.35 / 9.37 |
| DuckDB | 0.90 / 2.26 | 0.88 / 2.24 | 0.96 / 16.50 | 0.83 / 16.00 | 1.48 / 151.75 | 0.96 / 159.68 |
| MonetDB | 5.68 / 10.92 | 1.09 / 11.47 | 4.88 / 93.54 | 1.25 / 91.96 | 8.88 / 1692.83 | 1.55 / 1796.40 |
| Polars 2 | 30.84 / 6.00 | 30.79 / 5.58 | 30.26 / 16.85 | 30.18 / 16.72 | 32.15 / 122.77 | 31.81 / 121.65 |
| TDengine | 8.20 / 9.27 | 8.91 / 6.12 | 9.92 / 20.14 | 9.62 / 6.49 | 33.59 / 146.23 | 16.83 / 16.56 |

## Derived calculation, ten of 4,094 metrics

| Engine | 1 day | 10 days | 100 days |
| --- | ---: | ---: | ---: |
| Arc | 10.19 / 10.23 | 31.85 / 47.84 | 235.30 / 422.94 |
| ClickHouse (tuned) | 11.55 / 7.62 | 12.54 / 8.18 | 13.19 / 16.84 |
| DuckDB | 1.35 / 2.94 | 1.37 / 17.58 | 2.11 / 156.04 |
| MonetDB | 3.25 / 62.46 | 6.17 / 272.35 | 6.15 / 3481.16 |
| Polars 2 | 30.50 / 6.72 | 30.75 / 18.22 | 34.13 / 125.93 |
| TDengine | 6.18 / 12.02 | 14.98 / 25.22 | 59.22 / 178.42 |

## 100-day average as stored width increases

| Engine | 100 metrics | 1,000 metrics | 2,000 metrics | 4,094 metrics |
| --- | ---: | ---: | ---: | ---: |
| Arc | 27.06 / 28.14 | 69.91 / 118.90 | 119.25 / 212.14 | 229.81 / 415.59 |
| ClickHouse (tuned) | 6.01 / 7.29 | 7.05 / 10.52 | 7.95 / 14.32 | 11.23 / 11.74 |
| DuckDB | 0.68 / 4.21 | 0.76 / 37.35 | 0.89 / 73.23 | 1.16 / 149.03 |
| MonetDB | 1.13 / 4.60 | 1.51 / 73.85 | 1.61 / 179.11 | 1.84 / 3415.35 |
| Polars 2 | 4.75 / 6.65 | 10.40 / 39.55 | 17.85 / 65.45 | 33.61 / 123.39 |
| TDengine | 8.65 / 12.13 | 9.94 / 20.79 | 10.99 / 12.68 | 10.91 / 15.24 |

## 100-day calculation as stored width increases

| Engine | 100 metrics | 1,000 metrics | 2,000 metrics | 4,094 metrics |
| --- | ---: | ---: | ---: | ---: |
| Arc | 28.41 / 34.96 | 73.50 / 128.03 | 125.34 / 222.78 | 235.30 / 422.94 |
| ClickHouse (tuned) | 6.33 / 13.06 | 8.72 / 15.18 | 9.58 / 22.21 | 13.19 / 16.84 |
| DuckDB | 1.60 / 10.62 | 1.76 / 41.89 | 2.01 / 78.97 | 2.11 / 156.04 |
| MonetDB | 4.85 / 19.28 | 6.64 / 85.27 | 5.92 / 146.49 | 6.15 / 3481.16 |
| Polars 2 | 5.16 / 8.94 | 11.18 / 42.07 | 18.79 / 67.87 | 34.13 / 125.93 |
| TDengine | 61.87 / 196.69 | 59.57 / 227.78 | 59.21 / 175.42 | 59.22 / 178.42 |

## TDengine configuration and interpretation

TDengine OSS 3.4.2.8 runs in a native ARM64 container through the official
`taos-ws-py` 0.7.0 WebSocket connector. Wide uses a timestamp plus FLOAT columns
in a basic table. EAV uses a timestamp/value supertable with `metric_name` as a
tag and one child table per metric. This supports identical timestamps across
metrics while respecting TDengine's per-table timestamp identity. Both layouts
return every observation, including nulls; no EAV case was skipped.

The database uses two vgroups, 32 MiB write buffers per vgroup and an extended
retention period for the historical fixture. Prepared STMT2 inserts use bounded
480-timestamp batches; EAV binds at most 128 child tables per execution. EAV is
generated from the identical wide Parquet, so its populate path avoids decoding
the redundant long input. Population flushes before the select restart.

Reported latency includes the connector. TDengine materializes Python rows
into Polars; ClickHouse and Arc use Arrow paths, MonetDB uses ADBC/Arrow, and
DuckDB fetches Polars directly. These results compare the configured end-to-end
workflows, not isolated server execution times. Polars 2.0.0 is pinned and used
for this campaign. They do not measure one-minute samples over ten years,
continuous ingestion, late corrections or production memory behavior.

## Tested Parquet interoperability

The OSS container's `taosdump -Fparquet` exported real standard Parquet files
with the `.par` extension and `PAR1` magic. A separate small fixture contained
144 ten-minute timestamps in 100- and 4,094-metric wide tables, plus 100 EAV
child tables. Polars read both wide exports with exact schemas, timestamps,
Float32 values and nulls; DuckDB `read_parquet` returned matching canonical
hashes. All 100 EAV sample files also matched the source timestamps, values and
nulls. Restoring the dump under a new database name produced identical SQL
answer hashes for both wide tables and the EAV supertable.

EAV sample files contain only time/value: metric tags live in separate Parquet
metadata files. Preserve and join those tags when using the files outside
TDengine. Wide exports map directly to the aligned DataFrame workflow. This is
an interoperability proof, not a large-scale export/restore throughput test.
The dump package includes schema SQL and tag metadata; this does not establish
that arbitrary standalone external Parquet files can be restored with taosdump.
The benchmark ingests source Parquet through PyArrow and prepared STMT2 inserts.

TDengine has its own storage/query engines and engine-specific live files;
their implementation is open source. Interchange Parquet exports are separate
from that live storage format. See the official
[storage architecture](https://docs.tdengine.com/internals/storage/),
[Python connector](https://docs.tdengine.com/developer-guide/connectors-reference/python/)
and [taosdump documentation](https://docs.tdengine.com/operations-and-tooling/tools/taosdump/).
The [interop archive](https://github.com/wlaur/olap-benchmarks/releases/download/runs/tdengine-parquet-interop-3.4.2.8.tar.gz)
contains the tested Parquet package, metadata and export/restore logs.

## Resource bounds and provenance

Engines ran sequentially under `nice -n 15`, with a two-thread query budget
where supported and 6 GiB engine memory limits. Container CPU quotas were two;
MonetDB and TDengine native worker pools were not separately pinned. Arc uses
two flush workers and a 1 GiB Go heap target. Server charge includes file cache;
client and server peaks occur at different times and should not be added.

| Engine | Peak sampled store MiB | Peak server charge MiB | Peak client RSS MiB |
| --- | ---: | ---: | ---: |
| Arc | 2998 | 4406 | 1844 |
| ClickHouse (tuned) | 1222 | 3399 | 1213 |
| DuckDB | 1025 | — | 2944 |
| MonetDB | 3129 | 3219 | 891 |
| Polars 2 | — | — | 1458 |
| TDengine | 2251 | 2742 | 892 |

The retained range inputs occupy approximately 899 MiB. Suite-owned engine
stores and staging files were removed after each engine. No other suite was
rerun. All twelve populate/select runs record clean implementation commit
`62e4aac5e5520a1ed97f5f8b9f8000ba3148dfda`, together with engine versions,
settings, SQL and input fingerprints. All 120 queries agree across six engines
and measured iterations; paired calculation answers also agree across layouts.

Partial publication replaced only the range-suite cells and added TDengine.
Exact row comparisons verified the 80 other-suite runs and their steps,
resource samples and SQL records were unchanged. The
[raw revision](https://github.com/wlaur/olap-benchmarks/releases/download/runs/2026-10-timeseries-4094-tdengine-working.db)
and [per-query CSVs](https://github.com/wlaur/olap-benchmarks/releases/download/runs/2026-10-timeseries-4094-diagnostics.tar.gz)
retain the complete working measurements; the
[published explorer](https://wlaur.github.io/olap-benchmarks/#/explorer/time_series_ranges)
shows the latest suite cells.

To repeat only one engine, use a fresh revision and the publication merge:

```bash
uv run olap matrix run --db tdengine --suite time_series_ranges --revision tdengine-rerun
uv run olap publish --revision tdengine-rerun --merge
```

The merge preserves each original run's commit; the report/publication commit
is separate from the implementation commit that produced the measurements.
