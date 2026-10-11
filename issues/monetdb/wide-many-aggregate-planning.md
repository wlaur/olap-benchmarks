# Planning thousands of wide aggregates

Investigation on MonetDB Dec2025-SP4 / 11.55.9, container
`wlaur/monetdb-container:11.55.9-1`, ARM64, two threads and a 6 GiB container
limit. Client packages: ADBC 0.12.10 and sqlalchemy-monetdb-adbc 0.3.10.
Measurements were collected on 2026-10-11 during concurrent laptop use.
This records a planning cost, not a demonstrated correctness defect.

## Observed workload

The bounded `time_series_metrics` suite has 20,160 timestamps over 14 days,
5,000 nullable Float32 metric columns, and an equivalent dense EAV table with
100.8 million rows. The wide per-metric query returns one row with 10,000
count/sum columns. The EAV query returns 5,000 rows with a metric key, count
and sum. Both are fully materialized through Arrow in the benchmark.

| Metric count | Wide per-metric fetch ms | EAV per-metric fetch ms |
| --- | ---: | ---: |
| 100 | 7.4 | 8.9 |
| 1,000 | 317.4 | 796.5 |
| 2,000 | 1,344.3 | 1,071.1 |
| 5,000 | 11,262.9 | 6,986.1 |

Wide is expected to benefit from column-local aggregates and does win at some
widths. However, `EXPLAIN` of the 5,000-metric SQL took 12.36 seconds while
returning only five logical-plan rows. A native `mclient` execution also
reported `sql:11865.235 ms`, `opt:69.263 ms`, `run:13850.573 ms`, and batch client
clock `13908.504 ms`. These raw counters should not be added to estimate total
latency. They show substantial work in the SQL layer outside the Arrow fetch
helper. The [mclient manual](https://www.monetdb.org/documentation-Dec2025/admin-guide/manpages/mclient/)
documents its planning, optimization and execution timers and batch clock scope.

Reusing an ADBC cursor did not remove this cost: three repeated wide per-metric
fetches remained about 11.9 seconds. The pinned driver's parameter-free
`prepare()` takes the literal path, as shown in
[its implementation](https://github.com/wlaur/adbc-driver-monetdb/blob/v0.12.10/crates/adbc-monetdb/src/driver.rs).
This is different from explicit server SQL `PREPARE`. A native batch containing
`PREPARE` plus four `EXECUTE` calls completed in about 12 seconds, amortizing
planning across executions. That control discards rendering and does not
measure a prepared Arrow-fetch median. The published benchmark uses literal
SQL fetches and includes planning.

## Reproduction

Use a checkout containing the 5,000-metric extension. Prepare and populate only
this suite, retaining the engine store for inspection:

```sh
export POLARS_MAX_THREADS=2
export OLAP_BENCHMARKS_BENCHMARK_THREADS=2
export OLAP_BENCHMARKS_BENCHMARK_MEMORY_GB=6
export OLAP_BENCHMARKS_HOST_PORTS='{"monetdb":50010}'
uv run olap prepare time_series_metrics
uv run olap benchmark monetdb time_series_metrics --operation populate --revision planning-diagnostic
uv run olap docker monetdb time_series_metrics start
```

Run the SQL in
`olap_benchmarks/suites/time_series_metrics/queries/wide5000_13_per_metric.sql`
through the suite's Arrow fetch path, then run the same statement prefixed with
`EXPLAIN`. Use `eav5000_13_per_metric.sql` for the grouped EAV counterpart.
The query consists of one `COUNT(metric)` and one `SUM(CAST(metric AS DOUBLE
PRECISION))` per wide column, with no filter or final scalar addition tree.

For a native control, connect `mclient` to the disposable `monetdb-benchmark`
container's `benchmark` database using its `monetdb` / `monetdb` credentials.
Use `--timer=performance --format=trash --interactive` to obtain per-statement
clock measurements without output rendering. Feed the wide SQL, then an
explicit `PREPARE` of that SQL and repeated `EXECUTE <returned-id>()` commands
in the same session. Split the generated statement over short physical lines.
Stop only this container when done:

```sh
uv run olap docker monetdb time_series_metrics stop
```

The result is specific to two aggregates per metric and a short history.
Longer histories, fewer projections, parameterized queries and reusable
server-side plans can shift the wide/EAV comparison considerably.
