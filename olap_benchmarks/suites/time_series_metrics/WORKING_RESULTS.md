# Working metric-width measurements

Collected on 2026-10-11 on `macbook-m4-pro` during concurrent laptop use. Timing
variance was accepted. Only `time_series_metrics` was run; other suites were
preserved. Revision: `2026-10-timeseries-metrics-5000-working`.

The dataset has 20,160 timestamps at one-minute intervals over 14 days, one
entity, and about 3% null gaps. Wide and dense EAV contain identical Float32
measurements at 100, 1,000, 2,000 and 5,000 metrics. The largest EAV table has
100.8 million rows. Every width has the same history length; this does not
measure longer histories or sparse EAV.

The `resident-hot-v1` profile uses one warmup and three measured iterations.
Tables below report median client fetch latency, including SQL planning and full
result materialization. Engines ran separately under `nice -n 15`, with two query
threads and a 6 GB engine memory limit. That limit controls server containers or
DuckDB's buffer manager; it is not a hard limit on in-process Python RSS.

The earlier 100/1,000/2,000-metric revision remains available as a
[release asset](https://github.com/wlaur/olap-benchmarks/releases/download/runs/2026-10-timeseries-metrics-working.db).
These measurements supersede its metric-suite cells in the publication.

## Selective workloads

Geometric mean EAV/wide latency ratio across queries 01–11. Below 1 means EAV
is faster. MonetDB omits the unsupported rolling average, so its mean includes
ten queries. These are workload ratios, not site ranking scores.

| Engine | 100 metrics | 1,000 metrics | 2,000 metrics | 5,000 metrics |
| --- | ---: | ---: | ---: | ---: |
| Arc | 1.76× | 1.52× | 0.92× | 0.39× |
| ClickHouse | 1.10× | 1.01× | 0.89× | 0.69× |
| DuckDB | 3.70× | 17.31× | 26.26× | 40.78× |
| MonetDB | 1.67× | 4.67× | 6.46× | 31.43× |
| Polars | 2.45× | 2.44× | 2.09× | 2.11× |


MonetDB strongly favors wide for the selective cases. Arc and ClickHouse favor
EAV at larger widths in their configured storage layouts. Both order EAV by
metric and time. DuckDB's loaded tables and Polars' source Parquet retain
time-major EAV order. Arc's stored Parquet promotes Float32 to Double. These
physical choices are part of the measured systems.

## Per-metric aggregates

Query 13 counts and sums each metric over the full history. Each entry is
**wide / EAV**, in milliseconds. Wide returns one row with two columns per
metric; EAV returns one row per metric. Both outputs are fully fetched. Tests
reshape wide outputs solely to verify semantic equivalence.

| Engine | 100 metrics | 1,000 metrics | 2,000 metrics | 5,000 metrics |
| --- | ---: | ---: | ---: | ---: |
| Arc | 27.7 / 23.5 | 271.8 / 168.5 | 686.7 / 323.3 | 2,520.2 / 798.4 |
| ClickHouse | 29.3 / 20.1 | 510.9 / 168.2 | 1,755.3 / 321.5 | 5,810.5 / 803.6 |
| DuckDB | 6.4 / 3.8 | 73.5 / 38.4 | 161.8 / 84.2 | 497.3 / 327.6 |
| MonetDB | 7.4 / 8.9 | 317.4 / 796.5 | 1,344.3 / 1,071.1 | 11,262.9 / 6,986.1 |
| Polars | 13.0 / 14.9 | 124.1 / 131.2 | 248.4 / 269.6 | 631.8 / 685.5 |


MonetDB wide wins at 100 and 1,000 metrics here, matching the expected advantage
for some grouped workloads. At 5,000 metrics, large-plan cost changes the result.
Polars wide is slightly faster throughout this case. These short histories give
wide tables only 20,160 observations per column.

## Hourly aggregates across all metrics

Query 14 counts and sums each metric per hour for one day. Wide returns 24 rows
with 2 × metric-count + 1 columns; EAV returns 24 × metric-count rows and four
columns. The largest answer is 480,000 cells. Each entry is **wide / EAV**, in
milliseconds.

| Engine | 100 metrics | 1,000 metrics | 2,000 metrics | 5,000 metrics |
| --- | ---: | ---: | ---: | ---: |
| Arc | 15.1 / 9.2 | 133.9 / 30.4 | 406.9 / 55.8 | 2,016.3 / 133.9 |
| ClickHouse | 28.2 / 12.0 | 395.8 / 62.8 | 1,236.5 / 276.0 | 5,279.1 / 251.7 |
| DuckDB | 4.4 / 2.2 | 51.0 / 13.1 | 110.7 / 27.1 | 364.1 / 62.3 |
| MonetDB | 15.7 / 8.0 | 468.5 / 64.6 | 1,767.4 / 123.2 | 15,398.7 / 482.4 |
| Polars | 4.4 / 2.1 | 51.8 / 14.0 | 127.7 / 27.7 | 557.3 / 71.5 |


The short filtered window makes planning especially visible. These results do
not establish which layout wins with a much longer time range, more entities,
other aggregate combinations, or reusable prepared plans.

## Grand total

Query 12 returns a single count and sum across every measurement. EAV needs two
aggregates over one value column. This is a whole-matrix reduction rather than
per-metric output. Each entry is **wide / EAV**, in milliseconds.

| Engine | 100 metrics | 1,000 metrics | 2,000 metrics | 5,000 metrics |
| --- | ---: | ---: | ---: | ---: |
| Arc | 33.1 / 17.2 | 309.6 / 113.7 | 707.3 / 220.9 | 2,578.6 / 540.4 |
| ClickHouse | 30.8 / 14.1 | 395.3 / 97.9 | 929.6 / 190.4 | 2,551.9 / 455.1 |
| DuckDB | 7.0 / 2.8 | 81.1 / 26.3 | 175.0 / 52.7 | 477.1 / 134.0 |
| MonetDB | 12.1 / 5.2 | 726.2 / 307.9 | 2,989.1 / 590.8 | 9,567.3 / 1,489.7 |
| Polars | 13.4 / 6.1 | 124.3 / 44.8 | 251.1 / 80.2 | 637.6 / 198.9 |


ClickHouse's wide query now reduces row arrays before aggregating, avoiding
10,000 aggregate states for a two-value answer. Queries 12–14 disable expression
and aggregate JIT in both layouts and cap blocks at 4,096 rows. Settings are
recorded with each run. Initial 5,000-column attempts hit the 6 GiB container
limit during LLVM compilation. At 1,000 metrics, a fourth execution spent about
9.5 seconds in compilation; at 2,000, about 24.9 seconds. The corrected run has
no compilation events in the sampled query log.

## Planning diagnostics

Even with JIT disabled, ClickHouse's 5,000-metric query 13 spent roughly 3.1–3.3
seconds in `QueryPlanBuildMicroseconds`, against 5.6–5.7 seconds server duration.
It read 504 MB, the same value bytes as the EAV grand-total scan. The cost is
not explained by extra measurement bytes or Compact parts: its 5,000-column
wide table used Wide parts. Active part bytes were about 355 MiB wide and
324 MiB EAV at that width.

MonetDB's `EXPLAIN` for query 13 took 12.36 seconds and returned only five plan
rows. A native `mclient` control also reported `sql:11865.235 ms`, with client
clock 13.91 seconds. Reusing an ADBC cursor did not remove the wide slowdown:
query 13 still took about 11.9 seconds. The pinned driver treats parameter-free
ADBC prepare as a literal execution path; it does not create a server-side
prepared plan. An explicit native SQL `PREPARE` followed by four `EXECUTE` calls
completed in a roughly 12-second batch, showing the value of amortizing that
planning work. That batch uses `mclient` with discarded rendering and is not a
prepared Arrow-fetch median or a published ranking result.

See [the MonetDB planning investigation](../../../issues/monetdb/wide-many-aggregate-planning.md)
and [the Arc storage-path investigation](../../../issues/arc/wide-parquet-query-cost.md).
Arc is queried through native Arrow HTTP over compacted, partitioned Parquet;
DuckDB is queried over loaded persistent tables. Shared DuckDB implementation
does not make those storage paths equivalent.

## Resource bounds

Inputs use 1,417 MiB (1.38 GiB), of which the new 5,000-metric pair uses 864 MiB.
Existing input widths were reused without changing their bytes or mtimes.
Generation batches contain at most 480 timestamps / 2.4 million EAV rows.

Peak sampled engine-store sizes and memory below cover the whole eight-table
suite. Store peaks include temporary files inside the engine store. Server
charge includes reclaimable file cache; client RSS is separate. Peaks occur at
different times and should not be summed.

| Engine | Peak store MiB | Final store MiB | Peak server charge MiB | Peak client RSS MiB |
| --- | ---: | ---: | ---: | ---: |
| Arc | 1,478 | 1,023 | 5,229 | 2,664 |
| ClickHouse | 2,163 | 1,363 | 5,391 | 1,378 |
| DuckDB | 1,600 | 1,600 | — | 7,158 |
| MonetDB | 7,975 | 4,954 | 6,143 | 1,062 |
| Polars | 0 | 0 | — | 5,756 |

Polars reads the shared inputs rather than maintaining another store. MonetDB's
high server charge includes file cache; its largest measured anonymous/shared
server memory was 1,614 MiB. The largest in-process client RSS was 7.0 GiB on this
48 GiB machine. All benchmark stores and engine temp files were removed after
use; the inputs remain for reuse. About 147 GiB remained free immediately after
the runs, with concurrent user disk activity.

Arc needed two ingest flush workers, a 1 GiB Go heap target, and compaction before
verifying each table's row count. A count over hundreds of raw 5,000-column
Parquet schemas exhausted the cap. The final populated/compacted path completed
within the same 6 GiB container budget; no memory limit was increased.

## Validation and provenance

All measured answers agree across engines and across iterations. Four engines
run all 112 queries; MonetDB records eight unsupported rolling averages and
runs the other 104. Small-data checks compare all widths, layouts, native Polars
expressions, Arc overrides, and the equivalent ClickHouse grand-total reduction.

The merged publication keeps 80 runs: the 70 original runs plus ten latest
metric-suite populate/select cells. Row-by-row comparisons found zero changes
to other suites' run, step, resource, or SQL-execution records. Failed and
superseded attempts remain in the raw revision archive but are excluded from
the publication. Each retained run preserves its own clean-tree commit, input
fingerprints, query/schema hashes, versions, settings, and image digests.

| Engine | Version | Populate commit | Select commit |
| --- | --- | --- | --- |
| Arc | 26.09.3 | `76357fe0fa` | `defa30251d` |
| ClickHouse | 26.9.8.3 | `6a75266435` | `6a75266435` |
| DuckDB | 1.5.6 | `6a75266435` | `6a75266435` |
| MonetDB | Dec2025-SP4, ADBC | `defa30251d` | `6a75266435` |
| Polars | 2.0.0 | `6a75266435` | `6a75266435` |

For a later one-engine update, fetch the published database, run
`olap matrix run --db arc --suite time_series_metrics --revision UNIQUE_NAME`,
then stage `olap publish --revision UNIQUE_NAME --merge`. Commit the manifest
and upload its matching release asset. Adding query names makes an older cell
`stale`, so `--missing-only` reruns an expanded suite. The merge preserves
unrelated cells and their original provenance.
