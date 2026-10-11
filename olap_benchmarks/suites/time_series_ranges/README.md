# Time Series Ranges

Fetch five or ten selected metrics over the last 1, 10 or 100 days, or average
each selected metric over that same range. Each operation is paired between
wide and dense EAV at 100, 1,000, 2,000 and 5,000 stored metrics. Another 24
queries calculate a derived signal from ten aligned metrics and summarize it
hourly over the same three ranges: 120 queries in total.

The dataset holds exactly 100 days at ten-minute intervals, ending immediately
before 2025-01-01: 14,400 timestamps per wide table. The largest EAV table has
72 million rows. This sampling interval keeps the input and engine-store budget
similar to the existing 14-day, one-minute `time_series_metrics` suite. It does
not represent 100 days of one-minute data. Existing suites and inputs are kept.

Selected metric names are evenly spaced across the stored width, including the
first and last metrics. Range predicates are inclusive at the start and exclusive
at the end. Raw fetches return every sample, including nulls, in chronological
order; no LIMIT truncates the range. Averages ignore null values and promote
Float32 to Double for arithmetic.

Outputs retain their native layout. Wide fetches have one timestamp plus five or
ten value columns; EAV fetches have timestamp, metric name and value columns.
Wide averages return one row with five or ten columns; EAV averages return five
or ten metric-keyed rows. SQL planning, transfer, sorting and materializing the
complete answer are included in client latency. No EAV pivot is required. Small
tests reshape raw/average answers only to verify that the same measurements are returned.

The derived calculation uses ten evenly spaced metrics as `v1` through `v10`:

```text
balance = (v1 * (v2 - v3) + v4 * (v5 - v6) - abs(v7 * v8)) / (1 + abs(v9))
score = balance * (1.25 if v10 > 0 else 0.75)
```

This is a synthetic balance calculation, not a physical model of a plant. Every
metric participates, and any missing input makes the score null. Each hour
returns sample count, valid-score count, mean/min/max score and positive-score
count. Empty-score hours remain in the answer. Wide reads the ten columns;
EAV first aligns them with conditional aggregation by timestamp. This alignment
is included in the timer. Both return the same hourly shape, so no client pivot
is needed. The fixture has exactly one observation per metric and timestamp;
it does not benchmark duplicate resolution or asynchronous alignment.

Physical loading and ordering follow `time_series_metrics`: time ordering for
wide, metric/time ordering for Arc and ClickHouse EAV, loaded tables for DuckDB
and MonetDB, and source Parquet scans for Polars. Arc compacts before verifying
imports and uses two ingest flush workers plus a 1 GiB Go heap target. ClickHouse
uses nullable casts, native time decoding and increased SQL/AST limits for wide
table creation. These queries have only five or ten aggregates and need none of
the all-metric JIT/block overrides from the earlier suite.

ClickHouse wide queries put the timestamp range in an explicit `PREWHERE`.
This retains early filtering and primary-index pruning while avoiding the
automatic WHERE-to-PREWHERE selection cost on thousands of columns. MergeTree,
time ordering, adaptive granules and the time-series codecs stay the same.

Run only this suite with `olap matrix run --suite time_series_ranges --revision
UNIQUE_NAME`; add `--db ENGINE` for one database. Publish with `olap publish
--revision UNIQUE_NAME --merge` to retain other suites and their provenance.

See [the working measurements](WORKING_RESULTS.md) for the per-engine,
per-range comparison and resource usage, and
[the calculation and tuning results](CALCULATION_RESULTS.md) for the expanded
120-query campaign.
