# OLAP Benchmarks

A reproducible benchmarking framework for comparing analytical (OLAP) database engines on real-world workloads.

## What this project measures

Each benchmark suite runs a fixed set of SQL queries against multiple database engines under identical conditions — same hardware, same data, same queries. Results capture end-to-end wall-clock latency including data loading (populate) and query execution (run), along with system-level resource metrics (CPU, memory, on-disk size).

The goal is **not** to declare a single winner, but to provide transparent, apples-to-apples comparisons across engines so you can make informed decisions for your own workloads.

---

## Time Series

Scale-sensitive latency comparisons for wide time-series ingestion and analytical queries.

**Databases tested:** ClickHouse, MonetDB (more coming)

| Metric                | ClickHouse | MonetDB |
| --------------------- | ---------- | ------- |
| Populate (data load)  | ~45s       | ~120s   |
| Median SELECT latency | ~0.02s     | ~0.08s  |
| Median MUTATE latency | ~0.15s     | ~0.40s  |
| Peak memory           | ~1.2 GB    | ~2.8 GB |

ClickHouse leads on raw query throughput while MonetDB trades speed for a more traditional SQL interface. Mutation workloads show the largest divergence.

[Explore Time Series results](#/explorer/time_series)

---

## RTABench

Operational event-analytics workloads — counts, sums, and group-bys over event streams with realistic cardinality. Based on [RTABench](https://github.com/timescale/rtabench) by Timescale.

**Status:** Benchmark defined, results pending.

Expected to stress group-by and distinct-count performance across engines with varying index strategies.

[Explore RTABench results](#/explorer/rtabench)

---

## ClickBench

Analytical scan-heavy queries derived from [ClickBench](https://github.com/ClickHouse/ClickBench) by ClickHouse — full-table scans, filters, and aggregations over a single wide table.

**Status:** Benchmark defined, results pending.

This suite focuses on raw scan throughput and predicate pushdown efficiency. Results will cover DuckDB, ClickHouse, and others.

[Explore ClickBench results](#/explorer/clickbench)

---

## Kaggle Airbnb

Join-oriented analytics over a multi-table Airbnb listings dataset — testing join strategies, subquery optimization, and mixed aggregation patterns. Based on ["Testing query speed for DuckDB vs ClickHouse vs StarRocks databases"](https://medium.com/@marvin_data/testing-query-speed-for-duckdb-vs-clickhouse-vs-starrocks-databases-fecc6614d1ef) by Vitaliy.

**Status:** Benchmark defined, results pending.

This suite stresses multi-table join performance and optimizer quality across engines with different join implementations.

[Explore Kaggle Airbnb results](#/explorer/kaggle_airbnb)

---

## Methodology

### Hardware isolation

All benchmarks for a given **system** (e.g. `macbook-pro-m4`) run on the same physical machine. Results from different systems are never compared directly — the system selector in the navigation lets you view results scoped to a single machine.

### Reproducibility

- Every run is fully automated: schema creation, data loading, query execution, and metric collection
- Database versions, configuration, and query SQL are pinned per revision
- Raw results are stored in a DuckDB database and checked into version control

### Metrics collected

- **Populate latency** — time to create tables and load data
- **Run latency** — wall-clock time for each query (median of multiple iterations)
- **Resource traces** — CPU utilization, memory usage, and on-disk size sampled throughout each phase

## Navigating the results

Use the **Explorer** link in the navigation bar to dive into detailed results. Inside the explorer you can:

- Switch between benchmark suites with the suite selector
- Filter which databases are shown
- Toggle between linear and logarithmic duration scales
- Switch between Select and Mutate operation tabs
- Click on individual queries to see per-database breakdowns and the SQL source
- Inspect resource trends (CPU, memory, disk) for specific steps
