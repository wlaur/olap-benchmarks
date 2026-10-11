# Wide Parquet schema query cost

Investigation on Arc 26.09.3, whose query engine reports DuckDB 1.5.5, compared
with Python DuckDB 1.5.6 on macOS ARM64. This records observed storage-path costs;
it does not establish an engine defect. Measurements were collected during
concurrent laptop use on 2026-10-11.

## Connector and physical layout

The harness uses typed Parquet imports and the native `/api/v1/query/arrow`
endpoint with `x-arc-database: olap`, materializing Arrow results into Polars.
It waits for visibility, runs compaction, verifies row counts, and restarts the
server before select. The final sweep uses two query threads; `current_setting`
confirmed two threads and a 4.7 GiB internal DuckDB memory limit under the 6 GB
container cap.

EAV measurements configure
`ARC_INGEST_SORT_KEYS='eav100:metric_name eav1000:metric_name eav2000:metric_name'`.
After daily compaction there were 15 Parquet files per measurement. Sampled EAV
files retained metric ordering, with sequential metric min/max ranges across row
groups. The sampled 2,000-metric file had 24 row groups, each covering roughly
85 metrics. Wide files had 2,001 columns and one row group per sampled daily file.
Arc stores the imported Float32 measurements as Double.

The [Arc data-model guide](https://docs.basekick.net/arc/guides/data-model/)
describes directory partition pruning and metric sort keys. Its warning about
older custom-sort-key compaction behavior should not be assumed to describe
this pinned image: the compacted files were inspected directly here.
The [querying guide](https://docs.basekick.net/arc/guides/querying/) documents
the Arrow endpoint and time filters used by the harness.

## Matched-file diagnostic

A separate diagnostic ran the same SQL through Arc and through Python DuckDB
reading Arc's own compacted Parquet files, with two threads on both. The local
control used `union_by_name=true, hive_partitioning=false`, as Arc does, and the
one-day control selected that day's file. Each query ran one warmup and three
measured iterations. Arc used Arrow IPC; the local control materialized Python
rows, so sub-millisecond differences should not be interpreted precisely.

| 2,000-metric query | Arc Arrow ms | DuckDB over Arc Parquet ms | DuckDB loaded table ms |
| --- | ---: | ---: | ---: |
| Wide, one-day single-metric aggregate | 5.7 | 2.6 | 0.6 |
| Wide, full-history single-metric statistics | 18.5 | 18.7 | 0.8 |
| EAV, one-day single-metric aggregate | 2.7 | 3.2 | 4.4 |
| EAV, full-history single-metric statistics | 14.8 | 12.9 | 60.3 |

Arc's profiled JSON endpoint reported approximately the same server execution
time as its Arrow request latency on the full-history queries. `EXPLAIN ANALYZE`
showed Parquet scans. For the wide single-metric query, planning/binding took about
6 ms and scan work was also substantial despite projecting only one metric.
The matching Parquet control reproduces the cost: the gap versus a loaded table
is primarily the storage/query path in this experiment, not HTTP transfer.

The wide all-metric override originally constructed lists for every row to fit
Arc's 10,000-character SQL limit. It now aggregates columns first, then combines
the aggregate results into count and sum. This reduced the working 2,000-metric
median from about 820 ms to 617 ms. The optimized EAV all-metric query was 216 ms.
The all-metric diagnostic is omitted from the matched-file table because the
DuckDB versions and the very wide aggregate plan have additional performance
differences; its cost cannot be assigned solely to Arc's service wrapper.

## Reproduction

Generate and populate only the bounded time-series extension; omit cleanup so
the files remain available for inspection:

```sh
export POLARS_MAX_THREADS=2
export OLAP_BENCHMARKS_BENCHMARK_THREADS=2
export OLAP_BENCHMARKS_BENCHMARK_MEMORY_GB=6
uv run olap prepare time_series_metrics
uv run olap benchmark arc time_series_metrics --operation populate --revision arc-diagnostic
uv run olap docker arc time_series_metrics start
```

Run `wide2000_05_full_aggregate.sql` and `eav2000_05_full_aggregate.sql` from
`olap_benchmarks/suites/time_series_metrics/queries/` through the Arrow endpoint.
For server profiling, send the same SQL to `/api/v1/query` with
`x-arc-profile: true`; `EXPLAIN ANALYZE` works on that endpoint too.

For the direct DuckDB control, replace `FROM wide2000` with
`FROM read_parquet('<database directory>/arc/time_series_metrics/arc/olap/wide2000/**/*.parquet',
union_by_name=true, hive_partitioning=false)`; use the analogous EAV directory.
Set `threads=2`, `memory_limit='6GB'`, and `TimeZone='UTC'`. Preserve the existing
metric/time filters. Stop only the benchmark container after inspection:
`uv run olap docker arc time_series_metrics stop`.
