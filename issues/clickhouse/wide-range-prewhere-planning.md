# Automatic PREWHERE preparation cost on very wide tables

Investigation on ClickHouse 26.9.8.3, macOS ARM64 with the pinned ARM64 Linux
container, collected on 2026-10-11 during concurrent laptop use. This records a
removable preparation cost, not an established regression against an older
ClickHouse version.

## Workload and observation

The bounded `time_series_ranges` fixture has 100 days of ten-minute samples:
14,400 aligned timestamps, nullable Float32 metrics, and widths of 100, 1,000,
2,000 and 5,000 metrics. Wide uses `MergeTree ORDER BY time`, DoubleDelta/ZSTD
for timestamps and Gorilla/ZSTD for values. EAV uses the same codecs and
`ORDER BY (metric_name, time)`. Only five or ten requested metrics are read.

With 5,000 stored columns and ten selected columns, a 100-day average took
17.59 ms and a derived calculation took 20.55 ms in the initial working run.
The average's query log read 14,400 rows and 835,200 logical bytes: it was not
scanning every metric. EAV read 377,344 rows and 5,660,160 logical bytes,
including primary-index overscan, yet its average was faster. A planning-only
`EXPLAIN indexes = 1` of the wide calculation also took about 17 ms.
These are warm measurements, not a cold-disk comparison.

A later diagnostic separated SQL parsing from preparation on the same table:

| Diagnostic | Median client ms |
| --- | ---: |
| `EXPLAIN AST` | 1.39 |
| `EXPLAIN QUERY TREE` | 8.93 |
| `EXPLAIN indexes = 1` | 14.39 |

These calls construct and return different plans; their timings should not be
subtracted as exact CPU phase counters. They demonstrate substantial work
before scanning the measurements.

## Targeted tuning

An interleaved sweep checked every wide query at every width against its
original answer hash: 60 queries, one warmup for each variant and five measured
iterations, shuffled with seed 731. Query result caching was disabled. The
variants were automatic WHERE-to-PREWHERE movement, WHERE with
`optimize_move_to_prewhere = 0`, and an explicit timestamp PREWHERE with all
other optimizer settings left enabled.

| Stored metrics | Explicit PREWHERE / automatic latency, geometric mean |
| --- | ---: |
| 100 | 1.001× |
| 1,000 | 0.893× |
| 2,000 | 0.802× |
| 5,000 | 0.733× |

For the 5,000-column queries, interleaved medians were:

| Query, ten selected metrics | Automatic ms | Automatic disabled ms | Explicit PREWHERE ms |
| --- | ---: | ---: | ---: |
| Fetch 1 day | 16.29 | 11.91 | 11.70 |
| Fetch 100 days | 21.49 | 15.20 | 16.09 |
| AVG 100 days | 18.63 | 12.67 | 13.49 |
| Calculation, hourly statistics, 100 days | 20.31 | 15.28 | 15.04 |

The suite now uses explicit `PREWHERE time >= ... AND time < ...` for all wide
ClickHouse range queries. This retains early filtering and primary-index
pruning without disabling the optimizer globally. The subsequent complete
suite rerun measured 15.38 ms fetch, 12.84 ms average and 14.34 ms calculation
at 5,000 columns over 100 days. EAV was 20.48, 10.91 and 15.17 ms respectively.
The small calculation lead is a working result, not a stable ranking claim.

The pinned [automatic PREWHERE pass](https://github.com/ClickHouse/ClickHouse/blob/v26.9.8.3-stable/src/Processors/QueryPlan/Optimizations/optimizePrewhere.cpp)
constructs `MergeTreeWhereOptimizer`. Its
[`getTableColumns`](https://github.com/ClickHouse/ClickHouse/blob/v26.9.8.3-stable/src/Storages/MergeTree/MergeTreeWhereOptimizer.cpp)
requests all columns with subcolumns, then builds a name set. This is consistent
with the cost growing with stored schema width. The ablation localizes the
avoidable cost to automatic PREWHERE preparation; no CPU flamegraph was
collected to assign all elapsed time to a specific C++ function.

[PREWHERE documentation](https://clickhouse.com/docs/reference/statements/select/prewhere)
explains how to supply the filter explicitly. Disabling the analyzer is not an
option in this pinned release: it is mandatory from 26.9 according to the
[analyzer documentation](https://clickhouse.com/docs/guides/clickhouse/performance-and-monitoring/analyzer).

## Physical layout and other settings

Another interleaved sweep used one warmup and nine measured iterations per
query/setting, shuffled with seed 710. These measurements use unchanged
baseline SQL with automatic PREWHERE movement, so the part-layout table is a
separate comparison from the final tuned suite. All tested answers matched.

| Wide 5,000-column variant | Fetch 1 day ms | Fetch 100 days ms | AVG 100 days ms | Calculation 100 days ms |
| --- | ---: | ---: | ---: | ---: |
| Baseline, adaptive granules and Wide parts | 16.18 | 20.31 | 17.32 | 20.20 |
| Fixed 1,024-row granules, Wide parts | 16.19 | 22.62 | 18.64 | 20.66 |
| Fixed 8,192-row granules, Wide parts | 16.45 | 21.76 | 18.80 | 20.94 |
| Forced Compact parts | 23.95 | 53.91 | 45.06 | 48.89 |
| Ten selected columns in a separate wide table | 6.97 | 10.08 | 7.84 | 9.59 |

Each physical control retained time-series codecs, nullable value types and
time ordering. The narrow control also had two Wide parts, matching the source;
its adaptive granules were larger because the row was narrower. Source and
cloned full-width tables each had two parts and identical values. Mark totals
were 38 for the source, 15 for fixed 1,024, 3 for fixed 8,192 and 36 for Compact;
these include the part's terminal marks. Each full-width copy occupied about
260–261 MiB. Only one full-width copy was retained at a time, then dropped.

One query thread, a 4,096-row block with JIT disabled, aggregation in order,
and disabling lazy materialization gave no consistently useful wide-query
improvement. Disabling all query-plan optimizations reduced latency, but the
specific explicit PREWHERE rewrite achieves the desired improvement while
retaining other optimizations.

This does not establish that a different MergeTree-family engine is needed.
The current ordering already supports the range predicate. ReplacingMergeTree
or AggregatingMergeTree would change update/aggregation semantics without
addressing this observed preparation cost. Production monthly partitions,
batch sizes, merge pressure, and longer histories remain separate tuning
questions; the current fixture is too small to select production settings for
those operations.

## Reproduction

Prepare and populate only this suite, leaving its disposable store for inspection:

```sh
export OLAP_BENCHMARKS_BENCHMARK_THREADS=2
export OLAP_BENCHMARKS_BENCHMARK_MEMORY_GB=6
export POLARS_MAX_THREADS=2
uv run olap prepare time_series_ranges
nice -n 15 uv run olap benchmark clickhouse time_series_ranges --operation populate --revision prewhere-repro
uv run olap docker clickhouse time_series_ranges start
```

The common `queries/wide5000_12_avg_10_metrics_100d.sql` is the baseline SQL;
the ClickHouse override replaces its WHERE with PREWHERE. Run those through
`Clickhouse.fetch` with `ClickhouseTimeSeriesMetrics.QUERY_SETTINGS`,
`max_threads = 2`, `use_query_cache = 0`, and `time_columns=["time", "hr"]`.
Use the common query with `optimize_move_to_prewhere = 0` for the third control.
Keep answer hashing outside the latency timer. The raw diagnostic archive
linked in the suite's calculation report contains the executable sweeps,
samples, settings, plans and part measurements. Stop only the benchmark
container when finished: `uv run olap docker clickhouse time_series_ranges stop`.

ClickHouse also has a [TimeSeries engine](https://clickhouse.com/docs/reference/engines/table-engines/integrations/time-series),
but it models metric/tag series with separate id/timestamp/value sample tables.
Its sample storage still uses MergeTree. It is a different data model, not a
drop-in tuning option for this fixed-grid wide schema.
