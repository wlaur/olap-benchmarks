# Working selected-metric range measurements

This records the earlier 96-query campaign. See
[the expanded calculation and tuning results](CALCULATION_RESULTS.md) for the
latest 120-query campaign and explicit PREWHERE on ClickHouse wide queries.

Collected on 2026-10-11 during concurrent laptop use. Revision:
`2026-10-timeseries-ranges-working`. Only the new `time_series_ranges` suite
was run; all 80 previously published runs are retained unchanged.

This targets fetching five or ten metrics over a time range and computing
the average of each selected metric over that same range. It avoids the
EAV pivot in the earlier four-metric hourly query and avoids aggregating
thousands of unrequested metrics.

## Dataset and timings

Wide and dense EAV contain identical nullable Float32 measurements for one entity,
including about 3% null gaps. Stored
widths are 100, 1,000, 2,000 and 5,000 metrics. Each table covers exactly
100 days at ten-minute intervals, ending before 2025-01-01: 14,400 timestamps
per wide table, up to 72 million EAV rows. Selected keys are evenly spread
across each width. Filters select the last 1, 10 or 100 days, with an inclusive
start and exclusive end.

Each layout has 48 queries: four widths × three ranges × two selection sizes
× raw fetch/average. One warmup and three measured iterations use the existing
`resident-hot-v1` profile. Tables report the median end-to-end client fetch
latency in milliseconds, including SQL planning, ordered result delivery,
transfer and full materialization as a Polars DataFrame.

Raw answers retain their native layout: wide returns time plus five or ten
columns, while EAV returns time, metric name and value. At 100 days and ten
selected metrics, those are 14,400 × 11 and 144,000 × 3 cells respectively.
Average answers are one wide row versus five or ten metric-keyed EAV rows.
Tests reshape only for semantic verification, outside benchmark timing.
These are comparisons of complete supported query paths, including their
different answer representations.

ClickHouse and Arc EAV are ordered by metric and time. DuckDB and MonetDB
load the time-major inputs into persistent tables; Polars scans the source
Parquet. Arc promotes imported Float32 to Double. These physical choices
are part of the systems measured. Ten-minute samples make the longer
history fit the prior resource budget; this does not measure one-minute
sampling over 100 days.

## Ten selected metrics at 5,000 stored metrics

Each entry is **wide / EAV**, in milliseconds.

| Engine | Fetch 1 day | AVG 1 day | Fetch 10 days | AVG 10 days | Fetch 100 days | AVG 100 days |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Arc | 9.65 / 7.11 | 8.71 / 6.98 | 35.58 / 56.64 | 34.23 / 53.13 | 284.00 / 519.23 | 278.29 / 491.73 |
| ClickHouse | 17.13 / 5.49 | 16.91 / 5.15 | 17.06 / 7.44 | 17.76 / 5.96 | 22.40 / 21.43 | 19.05 / 8.29 |
| DuckDB | 1.09 / 2.93 | 1.02 / 2.93 | 1.17 / 22.82 | 1.07 / 22.32 | 1.85 / 226.26 | 1.26 / 221.42 |
| MonetDB | 4.64 / 9.48 | 0.84 / 14.31 | 7.41 / 66.04 | 1.07 / 63.25 | 10.39 / 487.59 | 1.52 / 405.24 |
| Polars | 36.08 / 7.22 | 36.27 / 7.19 | 36.64 / 23.22 | 36.00 / 22.68 | 40.49 / 161.79 | 39.36 / 161.08 |

## Reading these results

- DuckDB favors wide in these range-fetch and average cases at every tested
  width. Its EAV penalty grows with history and the number of stored metrics.
- MonetDB favors wide for averages at every tested width and for raw fetches
  at larger widths. At 100 stored metrics its raw fetch is slightly faster
  with EAV on the combined range/selection cases.
- ClickHouse favors EAV for averages in the combined cases. Wide raw fetches
  win at 100 stored metrics; EAV becomes more competitive as width grows.
  At 5,000 stored metrics, fetching ten metrics for 100 days is nearly tied.
- Arc favors wide across the combined cases at every width. At 5,000 stored
  metrics EAV has a modest advantage for one day, while wide wins for 10 and
  100 days. This workload does not show a general Arc EAV advantage.
- Polars depends on the range: at 5,000 stored metrics EAV wins for 1–10 days,
  while wide is about four times faster for 100 days. Across the smaller
  widths the combined results are close.

These conclusions describe the measured physical layouts and sampling interval.
The earlier suite selected the first one or four adjacent metrics, or aggregated
every metric. This suite selects five or ten keys spread across the stored width.
It also averages only the requested metrics: the 5,000-column wide query has ten
aggregates instead of thousands. The earlier many-aggregate planning results
therefore should not decide which layout to choose for this workload.

## Effect of stored metric count

Geometric mean **EAV/wide** latency across the six range/selection cases
for each operation. Below 1 means EAV is faster. These are raw latency
ratios, rather than the website's smoothed ranking scores.

| Engine | Operation | 100 stored | 1,000 stored | 2,000 stored | 5,000 stored |
| --- | --- | ---: | ---: | ---: | ---: |
| Arc | Fetch | 1.35× | 1.57× | 1.45× | 1.31× |
| Arc | AVG | 1.12× | 1.54× | 1.37× | 1.29× |
| ClickHouse | Fetch | 1.40× | 0.82× | 0.91× | 0.42× |
| ClickHouse | AVG | 0.85× | 0.74× | 0.86× | 0.31× |
| DuckDB | Fetch | 2.62× | 6.91× | 10.35× | 19.04× |
| DuckDB | AVG | 2.86× | 8.90× | 12.48× | 23.13× |
| MonetDB | Fetch | 0.85× | 1.69× | 2.46× | 7.75× |
| MonetDB | AVG | 2.06× | 7.12× | 11.00× | 40.93× |
| Polars | Fetch | 1.01× | 1.03× | 0.83× | 0.79× |
| Polars | AVG | 0.96× | 1.03× | 0.85× | 0.79× |

## Complete per-query comparisons

### Fetch: 5 selected metrics

| Engine | Stored metrics | 1 day wide / EAV ms | 10 days wide / EAV ms | 100 days wide / EAV ms |
| --- | ---: | ---: | ---: | ---: |
| Arc | 100 | 2.72 / 2.70 | 4.55 / 5.93 | 23.67 / 42.81 |
| Arc | 1,000 | 3.35 / 3.81 | 9.10 / 15.51 | 69.05 / 129.98 |
| Arc | 2,000 | 4.67 / 4.49 | 13.82 / 23.86 | 119.11 / 224.94 |
| Arc | 5,000 | 8.95 / 7.39 | 33.89 / 53.60 | 278.89 / 507.55 |
| ClickHouse | 100 | 4.48 / 4.46 | 3.90 / 5.53 | 6.20 / 9.99 |
| ClickHouse | 1,000 | 7.16 / 3.70 | 7.05 / 5.21 | 8.80 / 9.45 |
| ClickHouse | 2,000 | 8.68 / 4.33 | 8.66 / 6.87 | 10.87 / 11.78 |
| ClickHouse | 5,000 | 16.85 / 4.51 | 15.39 / 4.79 | 20.65 / 10.85 |
| DuckDB | 100 | 0.48 / 0.55 | 0.47 / 1.10 | 0.89 / 6.65 |
| DuckDB | 1,000 | 0.53 / 0.91 | 0.65 / 4.00 | 0.91 / 37.82 |
| DuckDB | 2,000 | 0.69 / 1.23 | 0.63 / 7.59 | 1.06 / 74.25 |
| DuckDB | 5,000 | 1.11 / 2.98 | 1.05 / 22.29 | 1.72 / 223.34 |
| MonetDB | 100 | 5.67 / 2.68 | 5.29 / 3.66 | 6.32 / 14.21 |
| MonetDB | 1,000 | 5.03 / 5.07 | 5.91 / 6.43 | 8.05 / 25.45 |
| MonetDB | 2,000 | 5.09 / 4.72 | 4.99 / 7.83 | 6.81 / 53.48 |
| MonetDB | 5,000 | 2.57 / 7.13 | 4.79 / 17.06 | 6.92 / 178.10 |
| Polars | 100 | 1.85 / 1.10 | 2.01 / 1.62 | 3.98 / 8.11 |
| Polars | 1,000 | 8.44 / 3.20 | 8.27 / 6.18 | 10.35 / 39.96 |
| Polars | 2,000 | 15.59 / 3.78 | 15.60 / 9.07 | 17.39 / 65.17 |
| Polars | 5,000 | 36.57 / 6.96 | 36.04 / 20.66 | 38.53 / 170.03 |

### Fetch: 10 selected metrics

| Engine | Stored metrics | 1 day wide / EAV ms | 10 days wide / EAV ms | 100 days wide / EAV ms |
| --- | ---: | ---: | ---: | ---: |
| Arc | 100 | 3.26 / 2.77 | 5.02 / 8.16 | 27.88 / 51.50 |
| Arc | 1,000 | 3.80 / 4.39 | 9.11 / 16.70 | 71.37 / 138.92 |
| Arc | 2,000 | 5.63 / 4.74 | 14.67 / 26.92 | 122.87 / 237.99 |
| Arc | 5,000 | 9.65 / 7.11 | 35.58 / 56.64 | 284.00 / 519.23 |
| ClickHouse | 100 | 5.11 / 5.55 | 4.76 / 6.62 | 7.49 / 16.57 |
| ClickHouse | 1,000 | 8.43 / 4.81 | 8.46 / 7.11 | 11.70 / 18.00 |
| ClickHouse | 2,000 | 9.46 / 5.50 | 9.84 / 10.83 | 11.93 / 24.30 |
| ClickHouse | 5,000 | 17.13 / 5.49 | 17.06 / 7.44 | 22.40 / 21.43 |
| DuckDB | 100 | 0.50 / 0.55 | 0.52 / 1.32 | 1.40 / 8.05 |
| DuckDB | 1,000 | 0.64 / 0.98 | 0.63 / 4.27 | 1.73 / 40.74 |
| DuckDB | 2,000 | 0.73 / 1.25 | 0.72 / 8.03 | 1.80 / 77.30 |
| DuckDB | 5,000 | 1.09 / 2.93 | 1.17 / 22.82 | 1.85 / 226.26 |
| MonetDB | 100 | 8.21 / 3.01 | 7.82 / 7.01 | 12.58 / 20.31 |
| MonetDB | 1,000 | 8.01 / 5.60 | 5.11 / 9.77 | 11.25 / 56.13 |
| MonetDB | 2,000 | 4.77 / 5.25 | 8.43 / 11.22 | 6.95 / 90.99 |
| MonetDB | 5,000 | 4.64 / 9.48 | 7.41 / 66.04 | 10.39 / 487.59 |
| Polars | 100 | 2.00 / 1.21 | 2.20 / 1.87 | 5.25 / 11.23 |
| Polars | 1,000 | 8.60 / 3.47 | 8.59 / 6.52 | 12.17 / 43.27 |
| Polars | 2,000 | 15.32 / 4.12 | 15.80 / 10.01 | 18.38 / 68.31 |
| Polars | 5,000 | 36.08 / 7.22 | 36.64 / 23.22 | 40.49 / 161.79 |

### AVG: 5 selected metrics

| Engine | Stored metrics | 1 day wide / EAV ms | 10 days wide / EAV ms | 100 days wide / EAV ms |
| --- | ---: | ---: | ---: | ---: |
| Arc | 100 | 2.55 / 2.79 | 4.16 / 4.64 | 20.14 / 27.29 |
| Arc | 1,000 | 3.30 / 3.88 | 8.13 / 13.41 | 64.98 / 116.48 |
| Arc | 2,000 | 5.44 / 4.35 | 13.35 / 22.98 | 112.25 / 211.69 |
| Arc | 5,000 | 9.03 / 7.13 | 35.60 / 52.48 | 278.11 / 492.77 |
| ClickHouse | 100 | 4.10 / 4.25 | 3.62 / 3.77 | 4.73 / 5.17 |
| ClickHouse | 1,000 | 6.94 / 3.52 | 6.83 / 5.25 | 7.07 / 6.19 |
| ClickHouse | 2,000 | 8.18 / 4.12 | 8.20 / 7.03 | 8.85 / 8.96 |
| ClickHouse | 5,000 | 16.60 / 4.11 | 15.30 / 4.49 | 17.71 / 5.06 |
| DuckDB | 100 | 0.34 / 0.53 | 0.34 / 0.86 | 0.46 / 4.00 |
| DuckDB | 1,000 | 0.53 / 0.98 | 0.49 / 3.80 | 0.52 / 34.58 |
| DuckDB | 2,000 | 0.58 / 1.18 | 0.54 / 7.37 | 0.78 / 70.71 |
| DuckDB | 5,000 | 0.99 / 2.94 | 0.94 / 21.85 | 1.05 / 220.57 |
| MonetDB | 100 | 1.29 / 2.09 | 1.16 / 3.04 | 1.37 / 3.33 |
| MonetDB | 1,000 | 1.37 / 4.47 | 1.70 / 5.48 | 1.50 / 23.14 |
| MonetDB | 2,000 | 1.00 / 4.36 | 1.45 / 5.93 | 1.99 / 76.96 |
| MonetDB | 5,000 | 0.75 / 8.63 | 1.11 / 10.92 | 1.13 / 176.52 |
| Polars | 100 | 1.68 / 1.11 | 1.92 / 1.47 | 3.52 / 6.44 |
| Polars | 1,000 | 8.24 / 3.34 | 8.37 / 5.96 | 9.71 / 38.28 |
| Polars | 2,000 | 15.74 / 4.12 | 15.68 / 9.13 | 15.93 / 63.42 |
| Polars | 5,000 | 36.32 / 6.89 | 36.62 / 22.01 | 38.10 / 164.38 |

### AVG: 10 selected metrics

| Engine | Stored metrics | 1 day wide / EAV ms | 10 days wide / EAV ms | 100 days wide / EAV ms |
| --- | ---: | ---: | ---: | ---: |
| Arc | 100 | 2.67 / 2.74 | 4.79 / 4.44 | 21.92 / 26.97 |
| Arc | 1,000 | 3.83 / 5.16 | 8.34 / 13.52 | 66.85 / 115.99 |
| Arc | 2,000 | 5.67 / 4.63 | 13.97 / 23.19 | 115.98 / 213.09 |
| Arc | 5,000 | 8.71 / 6.98 | 34.23 / 53.13 | 278.29 / 491.73 |
| ClickHouse | 100 | 23.17 / 5.40 | 4.25 / 4.92 | 5.77 / 7.04 |
| ClickHouse | 1,000 | 8.37 / 4.52 | 8.22 / 6.10 | 8.12 / 9.39 |
| ClickHouse | 2,000 | 9.22 / 5.79 | 9.81 / 8.61 | 9.91 / 16.08 |
| ClickHouse | 5,000 | 16.91 / 5.15 | 17.76 / 5.96 | 19.05 / 8.29 |
| DuckDB | 100 | 0.46 / 0.56 | 0.43 / 0.94 | 0.66 / 4.05 |
| DuckDB | 1,000 | 0.58 / 1.01 | 0.55 / 3.91 | 0.86 / 36.49 |
| DuckDB | 2,000 | 0.67 / 1.26 | 0.68 / 7.51 | 1.01 / 71.90 |
| DuckDB | 5,000 | 1.02 / 2.93 | 1.07 / 22.32 | 1.26 / 221.42 |
| MonetDB | 100 | 1.34 / 1.85 | 1.19 / 3.19 | 2.38 / 4.73 |
| MonetDB | 1,000 | 1.54 / 4.58 | 1.59 / 6.52 | 1.26 / 82.79 |
| MonetDB | 2,000 | 0.97 / 5.83 | 1.92 / 9.62 | 1.27 / 107.69 |
| MonetDB | 5,000 | 0.84 / 14.31 | 1.07 / 63.25 | 1.52 / 405.24 |
| Polars | 100 | 1.79 / 1.30 | 2.02 / 1.69 | 5.12 / 6.99 |
| Polars | 1,000 | 8.34 / 3.47 | 8.35 / 6.23 | 11.88 / 39.77 |
| Polars | 2,000 | 15.92 / 4.14 | 15.65 / 9.76 | 17.26 / 65.37 |
| Polars | 5,000 | 36.27 / 7.19 | 36.00 / 22.68 | 39.36 / 161.08 |

## Resource bounds and provenance

Engines ran one at a time under `nice -n 15`, with two query threads and a
6 GB engine memory limit. Arc also uses two flush workers and a 1 GiB Go
heap target. Server charge includes reclaimable file cache; Python client
RSS is separate, so these peaks occur at different times and should not
be added. The engine limit is not a hard cap on in-process client RSS.
For MonetDB, the thread setting enforces a two-CPU container quota; its native
worker pool is not explicitly pinned to two threads.

| Engine | Peak sampled store MiB | Peak server charge MiB | Peak client RSS MiB |
| --- | ---: | ---: | ---: |
| Arc | 3,614 | 4,831 | 2,132 |
| ClickHouse | 1,585 | 4,777 | 1,318 |
| DuckDB | 1,185 | — | 3,410 |
| MonetDB | 3,566 | 3,403 | 887 |
| Polars | — | — | 1,294 |

Polars has no additional engine store; it scans the retained input files.

New inputs total 1,012.6 MiB. Engine stores and temporary files were cleaned
up after each engine. Existing 14-day inputs remain available for reuse.

All measured answers agree across the five engines and across iterations.
Every engine completed all 96 queries. Every retained run records clean-tree
repository provenance, input/query/schema fingerprints, engine version,
settings and image digests. Benchmark commit: `317eaa39c4615034448f534c64000993da812125`.

The raw revision is distributed as a [GitHub release asset](https://github.com/wlaur/olap-benchmarks/releases/download/runs/2026-10-timeseries-ranges-working.db).
Publication merges its ten populate/select cells into the previous database.
Row-by-row comparisons verify that existing run, step, resource and SQL
execution records are unchanged. A later one-engine update can use
`olap matrix run --db ENGINE --suite time_series_ranges --revision UNIQUE_NAME`,
followed by `olap publish --revision UNIQUE_NAME --merge` and upload of the
matching release asset. Each retained run keeps its own benchmark commit.
