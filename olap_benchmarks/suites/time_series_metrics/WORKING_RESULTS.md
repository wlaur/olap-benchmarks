# Working metric-width measurements

Collected on 2026-10-11 on `macbook-m4-pro` while the laptop was in use.
These are working results, not an isolated performance run. Concurrent-work noise
was accepted without retries for timing variance. Only the time-series extension
was run; historical suites were preserved.

Revision: `2026-10-timeseries-metrics-working`. Profile: `resident-hot-v1`, one
warmup and three measured iterations. Times are medians of the client fetch timer,
including full result materialization. Each engine ran separately with a two-thread
budget and a 6 GB memory limit, under `nice -n 15`. Polars also had two threads.
Benchmark containers were cleaned up after use; unrelated containers were untouched.

The workload has 20,160 timestamps and identical values in wide and dense EAV
layouts at 100, 1,000 and 2,000 metrics. There is one entity and about 3% null gaps.
It tests increasing metric width, not increasing history length or sparse EAV.

## Selective workloads

Geometric mean of the EAV/wide latency ratio across queries 01–11. Smaller than
1 means EAV is faster. MonetDB omits the declared unsupported rolling average,
so its mean includes ten queries. These ratios are not the site's ranking scores.

| Engine | 100 metrics | 1,000 metrics | 2,000 metrics |
| --- | ---: | ---: | ---: |
| Arc | 1.68× | 1.54× | 0.98× |
| ClickHouse | 1.09× | 1.09× | 0.80× |
| DuckDB | 3.08× | 17.55× | 23.47× |
| MonetDB | 2.24× | 8.17× | 9.58× |
| Polars | 2.54× | 2.48× | 2.14× |

Arc's metric sort keys matter: the initial time-only EAV layout had a 4.41×
selective-workload penalty at 2,000 metrics. Sorting by `(metric_name, time)`
reduced that to 0.98× in the corrected run. This is a physical-design rerun;
the change and its commit are recorded. ClickHouse also orders EAV by metric and
time. DuckDB's persistent tables retain time-major ingestion order, and Polars
scans time-major input Parquet. Layout and configured physical design both affect
these comparisons.

## Reading every metric

Query 12 counts non-null observations and sums every measurement. Each entry is
**wide / EAV**, in milliseconds. EAV wins this query on every engine, including
MonetDB; the selective-workload conclusion does not apply to an all-metric scan.

| Engine | 100 metrics | 1,000 metrics | 2,000 metrics |
| --- | ---: | ---: | ---: |
| Arc | 26.4 / 16.1 | 251.3 / 115.3 | 617.2 / 215.7 |
| ClickHouse | 35.1 / 10.7 | 541.7 / 112.5 | 1,426.8 / 158.1 |
| DuckDB | 7.4 / 2.9 | 87.2 / 26.9 | 185.2 / 53.1 |
| MonetDB | 14.0 / 7.2 | 718.7 / 190.6 | 2,789.7 / 387.8 |
| Polars | 13.5 / 6.2 | 124.7 / 45.9 | 252.3 / 80.7 |

## Resource bounds and validation

Inputs occupy about 583 MiB. The largest EAV table has 40.32 million rows, generated
in batches of at most 960,000 rows. The largest sampled engine-store footprint was
1,187 MiB (MonetDB). Arc's largest sampled server charge, including file cache,
was 4,737 MiB during populate; DuckDB's largest client RSS was 3,063 MiB. Server
and client peaks occur at different times and should not be summed. The machine
still had about 151 GiB free disk space after cleanup.

All measured answers agree across the five engines and across iterations. MonetDB
records six unsupported rolling-average queries, one per table, for its known
window defect. Every other engine runs all 72 queries. Small-data tests also
compare wide/EAV SQL, native Polars expressions, and Arc's compact SQL at each width.

The merged publication has 80 runs: all 70 original runs plus ten latest
populate/select cells. A row-by-row comparison found zero changes to the original
run, step, resource, and SQL-execution records. Failed and superseded new runs were
excluded. Each retained run keeps its own clean-tree commit hash, query/schema
hashes, input fingerprints, engine version, settings, and image digests.

| Engine | Version | Populate commit | Select commit |
| --- | --- | --- | --- |
| Arc | 26.09.3 | `3cba4bb2f3` | `4ee1e13ce3` |
| ClickHouse | 26.9.8.3 | `93304cd012` | `93304cd012` |
| DuckDB | 1.5.6 | `2d728f7976` | `2d728f7976` |
| MonetDB | Dec2025-SP4, ADBC | `93304cd012` | `93304cd012` |
| Polars | 2.0.0 | `2d728f7976` | `2d728f7976` |

For a later one-engine update, fetch the current published database, run
`olap matrix run --db arc --suite time_series_metrics --revision UNIQUE_NAME`,
and stage `olap publish --revision UNIQUE_NAME --merge`. Commit the updated manifest
and upload its matching release asset. Other cells retain their original provenance.
