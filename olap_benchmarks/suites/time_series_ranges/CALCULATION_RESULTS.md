# Working aligned calculations and ClickHouse tuning

The later [six-engine campaign](TDENGINE_RESULTS.md) includes TDengine and
uses 4,094 metrics at the largest width. This report retains the historical
5,000-metric measurements.

Collected on 2026-10-11 during concurrent laptop use. Revision:
`2026-10-timeseries-calculations-working`. Only `time_series_ranges` was run.
These replace the previous range-suite cells; the 80 previously published runs
from other suites remain unchanged. The earlier untuned measurements remain in
[WORKING_RESULTS.md](WORKING_RESULTS.md) as a historical campaign.

## Workload

The retained input is unchanged: 100 days at ten-minute intervals, 14,400
aligned timestamps, 100/1,000/2,000/5,000 nullable Float32 metrics per entity,
and up to 72 million dense EAV rows. Inputs total about 1,013 MiB. Each layout
has the original selected-five/ten raw fetch and average queries, plus three
new ten-metric calculations covering 1, 10 and 100 days: 120 queries total.

For ten evenly spaced metrics `v1` through `v10`, the calculation is:

```text
balance = (v1 * (v2 - v3) + v4 * (v5 - v6) - abs(v7 * v8)) / (1 + abs(v9))
score = balance * (1.25 if v10 > 0 else 0.75)
```

Any missing input makes the score null. Each hour returns total samples, valid
scores, mean/min/max score and positive-score count, retaining all-null hours.
This synthetic arithmetic/conditional workload is not a thermodynamic plant
model. Double arithmetic avoids accidental Float32 precision differences.

Wide reads the ten columns directly. EAV filters the metric keys, then aligns
them by timestamp with ten conditional aggregates before calculating the
score. Alignment, sorting, transfer and full Polars materialization are timed.
The fixture has unique metric/time pairs on a common grid; it does not simulate
asynchronous observations or resolve duplicates. Both layouts return the same
hourly answer shape. Raw and simple-average queries keep their native wide
versus long result shapes, as described in the earlier report.

Tables below show median client latency in milliseconds, one warmup and three
measured iterations under `resident-hot-v1`. Each entry is **wide / EAV**.
ClickHouse wide now uses an explicit timestamp PREWHERE; EAV retains its original
query path. Other engines' query paths and the input data are unchanged.

## Derived calculation, ten of 5,000 metrics

| Engine | 1 day wide / EAV ms | 10 days wide / EAV ms | 100 days wide / EAV ms |
| --- | ---: | ---: | ---: |
| Arc | 10.77 / 10.13 | 40.15 / 58.97 | 292.61 / 513.61 |
| ClickHouse (tuned) | 12.63 / 8.09 | 12.83 / 9.66 | 14.34 / 15.17 |
| DuckDB | 1.42 / 3.09 | 1.65 / 23.59 | 2.13 / 227.67 |
| MonetDB | 2.32 / 86.08 | 4.06 / 283.42 | 3.85 / 3996.17 |
| Polars | 36.12 / 8.26 | 39.29 / 21.27 | 42.52 / 163.23 |

## Calculation over 100 days as stored width increases

| Engine | 100 metrics | 1,000 metrics | 2,000 metrics | 5,000 metrics |
| --- | ---: | ---: | ---: | ---: |
| Arc | 26.63 / 35.30 | 73.79 / 125.58 | 125.62 / 222.02 | 292.61 / 513.61 |
| ClickHouse (tuned) | 6.60 / 13.08 | 8.43 / 14.97 | 9.94 / 23.92 | 14.34 / 15.17 |
| DuckDB | 1.64 / 10.16 | 1.59 / 41.14 | 1.75 / 78.45 | 2.13 / 227.67 |
| MonetDB | 6.21 / 21.09 | 6.99 / 85.42 | 5.79 / 189.05 | 3.85 / 3996.17 |
| Polars | 4.83 / 8.81 | 11.10 / 41.03 | 18.41 / 66.79 | 42.52 / 163.23 |

## Refreshed fetch/average, ten of 5,000 metrics

| Engine | Fetch 1 day | AVG 1 day | Fetch 10 days | AVG 10 days | Fetch 100 days | AVG 100 days |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Arc | 9.49 / 7.60 | 8.78 / 7.46 | 37.71 / 56.97 | 36.24 / 53.45 | 293.57 / 527.35 | 283.73 / 499.75 |
| ClickHouse (tuned) | 11.15 / 6.85 | 11.12 / 6.26 | 11.52 / 8.29 | 11.66 / 6.55 | 15.38 / 20.48 | 12.84 / 10.91 |
| DuckDB | 1.11 / 2.39 | 1.04 / 2.41 | 1.23 / 22.77 | 1.05 / 22.13 | 1.93 / 224.39 | 1.24 / 219.16 |
| MonetDB | 8.26 / 17.01 | 2.86 / 14.57 | 5.97 / 471.49 | 1.93 / 402.94 | 7.68 / 3979.51 | 1.52 / 4277.31 |
| Polars | 35.28 / 7.59 | 35.94 / 7.45 | 35.82 / 20.73 | 36.12 / 20.76 | 39.44 / 161.53 | 38.83 / 160.78 |

## Why MonetDB is faster here

The MonetDB physical plan for the wide average binds just time and the ten
requested value columns, selects the timestamp range, projects those aligned
columns, converts them to Double and averages them. The wide calculation adds
column arithmetic and hourly grouping, without reconstructing rows from metric
keys. In a separate warm diagnostic its logical EXPLAIN took 0.73–0.79 ms and
EXPLAIN PHYSICAL took 0.85–1.16 ms for these two queries. ClickHouse's original
wide planning-only call was already around 17 ms before scanning data.

Both engines are columnar. MonetDB's aligned BAT representation and
memory-mapped fixed-width columns support these direct operations; ClickHouse
uses compressed part streams, marks and a block pipeline. On this small warm
fixture, the measured preparation overhead explains much of the wide-query
gap. It does not show that MonetDB has a universally better optimizer or that
ClickHouse has an unsuitable disk layout. See MonetDB's
[query journey](https://www.monetdb.org/blogs/query-journey-in-monetdb/) and
[memory footprint documentation](https://www.monetdb.org/documentation-Dec2025/admin-guide/system-resources/memory-footprint/)
for the architecture; neither diagnoses the reported production memory leaks.

MonetDB's EAV plan first filters the long table, then builds aligned values
with conditional aggregation. Its generated MAL repeats metric-key selection
and merges candidates across many fragments before the hourly arithmetic.
This is substantial extra work compared with direct aligned columns. The
baseline EAV average is also slow in this working campaign, so the 100-day
calculation's entire penalty cannot be assigned to pivoting alone. The
four-second EAV result differs markedly from the earlier campaign's simple
averages; these noisy, resource-constrained runs are not a regression claim.

## ClickHouse tuning

The [engine investigation](../../../issues/clickhouse/wide-range-prewhere-planning.md)
contains the interleaved sweeps, preparation evidence and reproduction.
Replacing automatic WHERE-to-PREWHERE movement with explicit PREWHERE reduced
wide-query geometric mean latency by about 11% at 1,000 columns, 20% at 2,000
and 27% at 5,000; 100 columns was unchanged. Every tested answer matched.

The complete suite rerun at 5,000 columns and ten selected metrics over 100 days
changed wide fetch from 21.17 to 15.38 ms, average from 17.59 to 12.84 ms and
calculation from 20.55 to 14.34 ms. Tuned wide beats EAV for fetch and narrowly
for the calculation in this campaign; EAV still wins the simple average.
The interleaved sweep is stronger evidence for the tuning than comparing two
separate noisy suite runs. The small final calculation difference needs a
quieter rerun before treating it as a stable ranking.

Fixed 1,024-row or 8,192-row granules did not improve this workload. Forced
Compact parts made the 100-day wide calculation about 49 ms versus about 20 ms
with the baseline Wide parts. The useful change is query preparation; the
suite retains MergeTree, time ordering, adaptive granules and its existing
DoubleDelta/Gorilla/ZSTD codecs. Query result caching was not used to produce
these improvements. No other suite's settings were changed.

## Examples of wide time-series models

Very wide time-aligned tables exist, though these examples do not establish
that a single multi-thousand-column plant table is common in production:

| System | Relevant example | What it establishes |
| --- | --- | --- |
| [ArcticDB](https://docs.arcticdb.io/latest/notebooks/arcticdb_demo_lmdb/) | The official notebook writes 10,000 hourly timestamps × 10,000 measurement columns, then reads six selected columns over a date range. | A close match to the aligned-wide model, exposed as a versioned DataFrame store rather than a SQL table. ArcticDB is a different product from Arc in this benchmark. |
| [TDengine](https://docs.tdengine.com/tdengine-sql/ddl/stable/) | Timestamp plus typed measurement columns per device; a regular supertable supports up to 4,096 total columns including tags. | A dedicated TSDB that uses a wide metric schema. It is not evidence for a physical 5,000-column table. |
| [ClickHouse/Altinity](https://altinity.com/blog/too-wide-or-not-too-wide-that-is-the-clickhouse-question) | A monitoring benchmark stores timestamp/device plus 10,000 metric columns and tests selected-column averages. | A concrete wide SQL-table experiment; its synthetic data and extreme insert/merge resource use are not a production deployment or an EAV comparison. |
| [QuestDB](https://community.questdb.com/t/high-dimensional-tables/817) | An IoT user evaluates up to 5,000 sensors as columns at 10 Hz, including out-of-order ingestion. | A public high-dimensional workload evaluation; it reports ingestion/WAL costs and discusses splitting the table. It does not prove a successful production deployment. |
| [InfluxDB 3](https://docs.influxdata.com/influxdb3/core/write-data/best-practices/schema-design/) | Fields are typed columns, so multiple metrics can occupy the same timestamped row. | Influx is not uniformly an EAV physical schema. Core's documented [500-column limit](https://docs.influxdata.com/influxdb3/core/admin/databases/) and warning about wide-schema costs make it a poor example for thousands of columns. |

PostgreSQL's hard limit is [1,600 columns](https://www.postgresql.org/docs/current/limits.html),
with the row also constrained to fit on its normally 8 KiB page. Fixed-width
Double values make the practical limit closer to the cited roughly 1,000
columns; TOAST does not remove this fixed-width constraint. Timescale therefore
cannot provide a single 5,000-Double-column hypertable. Per-device/subsystem wide
tables are more feasible than assuming the entire plant must be one SQL row.

For an already aligned grid, known metric types and cross-signal arithmetic,
these results support keeping a wide model for the MonetDB-to-ClickHouse
migration. They do not justify changing to EAV solely because of the earlier
ClickHouse aggregate latency. Separate operational tests are needed for
one-minute samples over 5–10 years: this 100-day, ten-minute fixture does not
measure long-term ingestion, retention, schema evolution or production memory
behavior.

## Resource bounds and provenance

Engines ran one at a time under `nice -n 15`. Server containers had two-CPU
quotas and 6 GiB memory caps; native query threads were set to two where the
connector supports it. MonetDB's native worker pool was not explicitly pinned;
its CPU quota is the enforced limit. Arc also uses two flush workers and a
1 GiB Go heap target. In-process/client RSS is separate from server charge,
and the peaks below occur at different times. They should not be added.

| Engine | Peak sampled store MiB | Peak server charge MiB | Peak client RSS MiB |
| --- | ---: | ---: | ---: |
| Arc | 3614 | 4945 | 2095 |
| ClickHouse | 1550 | 4904 | 2446 |
| DuckDB | 1185 | — | 3296 |
| MonetDB | 3566 | 3393 | 927 |
| Polars | — | — | 1384 |

Polars scans the retained input and has no additional engine store. All suite
engine stores and temporary staging files were cleaned after their runs.
Physical-layout diagnostics retained at most one extra full-width ClickHouse
copy, about 260 MiB, then dropped it; diagnostic controls are outside matrix
resource sampling and the final published query measurements.

Every engine completed all 120 queries with agreeing answers across engines
and iterations. The 12 paired calculation cases also have identical wide/EAV
answer hashes across all five engines. All 11 raw runs record clean trees; the
older ClickHouse select is retained in the raw revision and omitted from the
published latest-cell set. Calculation code: `51fbe18835a7c01b6d935069fb233ef90cefc0b1`.
Tuned ClickHouse select and DuckDB/Polars/Arc runs:
`5c47af3e1e097e3b7302df35aa5c3ce38f94261c`. MonetDB runs and the ClickHouse
populate retain the calculation-code commit. Per-run commits, settings, SQL
and input fingerprints are preserved by partial publication.

The raw revision is a [release asset](https://github.com/wlaur/olap-benchmarks/releases/download/runs/2026-10-timeseries-calculations-working.db).
The [diagnostic archive](https://github.com/wlaur/olap-benchmarks/releases/download/runs/2026-10-timeseries-calculations-diagnostics.tar.gz)
contains individual samples, plans, sweeps and the complete per-query median CSV.
The [published explorer](https://wlaur.github.io/olap-benchmarks/#/explorer/time_series_ranges)
uses the latest ten populate/select cells. Exact row comparisons after merging
verified that all 80 other-suite runs, 7,877 steps, 39,358 resource samples and
9,157 SQL-execution records remain unchanged. No other suite was rerun.
