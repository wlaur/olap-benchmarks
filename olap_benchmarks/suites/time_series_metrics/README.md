# Time Series Metrics

This bounded extension isolates schema layout and metric count from time-series length.
The existing `time_series` suite remains available with its historical tall/wide/large
datasets and mutation/concurrency workloads.

| Property | Value |
| --- | --- |
| Layouts | Wide and dense EAV (`time`, `metric_name`, nullable Float32 `value`) |
| Metric counts | 100, 1,000, 2,000 |
| Timestamp count | 20,160 per table, one-minute intervals over 14 days ending 2025-01-01 |
| EAV rows | 2,016,000 / 20,160,000 / 40,320,000 |
| Values | Deterministic smooth signals, metric-specific offsets and periodic null gaps |
| Width control | First N metrics are identical at every width and in both layouts |
| Generation | 480 timestamps per batch; EAV batches contain at most 960,000 rows |
| Iterations | Standard resident-hot profile: one warmup plus three measured iterations |
| Operations | Populate and select |

The same 12 logical queries run against all six tables: latest 50 observations,
one-day filtered aggregate, hourly aggregate of one metric and four metrics,
full-history statistics of one metric, 1,000 raw observations, scalar four-metric
lookup, filter one metric by another, 60-row moving average, first differences,
null-gap detection, and count/sum across every metric. Queries return bounded
answers. Float32 measurements are promoted to Float64 for arithmetic in all engines.

EAV retains a row for every observation, including nulls. Removing null rows would
change window frames, gaps and timestamp domains, so this workload does not measure
sparse EAV. A metric name identifies a signal within one entity; multiple entities
and varying sparsity are separate workload dimensions.

ClickHouse uses time ordering for wide tables and `(metric_name, time)` ordering
with LowCardinality metric names for EAV, plus the existing time-series codecs.
Arc receives time-sorted input, sorts EAV storage by `(metric_name, time)` using its
per-measurement ingest sort keys, and completes its compaction cycle before select.
MonetDB analyzes the timestamp and EAV metric-name columns after loading.
DuckDB loads persistent tables; Polars scans the same Parquet inputs lazily.
These compare supported storage paths, not identical physical indexes or ingestion.

MonetDB's known 60-row `AVG` window defect is recorded as unsupported in both
layouts. Other queries still run. Small-data tests compare every SQL query between
wide and EAV and against the native Polars expressions at every metric count.

Run this suite alone with `olap matrix run --suite time_series_metrics`; add `--db`
to select an engine. Use a fresh `--revision` and `olap publish --merge` to preserve
unrelated published results and each run's own repository provenance.
