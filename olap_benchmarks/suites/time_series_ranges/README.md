# Time Series Ranges

Fetch five or ten selected metrics over the last 1, 10 or 100 days, or average
each selected metric over that same range. Each operation is paired between
wide and dense EAV at 100, 1,000, 2,000 and 5,000 stored metrics: 96 queries.

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
tests reshape answers only to verify that the same measurements are returned.

Physical loading and ordering follow `time_series_metrics`: time ordering for
wide, metric/time ordering for Arc and ClickHouse EAV, loaded tables for DuckDB
and MonetDB, and source Parquet scans for Polars. Arc compacts before verifying
imports and uses two ingest flush workers plus a 1 GiB Go heap target. ClickHouse
uses nullable casts, native time decoding and increased SQL/AST limits for wide
table creation. These queries have only five or ten aggregates and need none of
the all-metric JIT/block overrides from the earlier suite.

Run only this suite with `olap matrix run --suite time_series_ranges --revision
UNIQUE_NAME`; add `--db ENGINE` for one database. Publish with `olap publish
--revision UNIQUE_NAME --merge` to retain other suites and their provenance.
