# OLAP Benchmarks

A reproducible benchmarking framework for comparing analytical (OLAP) database engines on real-world workloads.

## What this project measures

Each benchmark suite runs a fixed set of SQL queries against multiple database engines under identical conditions — same hardware, same data, same queries. Results capture end-to-end wall-clock latency including data loading (populate) and query execution (run), along with system-level resource metrics (CPU, memory, on-disk size).

The goal is **not** to declare a single winner, but to provide transparent, apples-to-apples comparisons across engines so you can make informed decisions for your own workloads.

## Benchmark suites

| Suite                                       | Workload                               | Focus                                              |
| ------------------------------------------- | -------------------------------------- | -------------------------------------------------- |
| [Time Series](#/benchmarks/time_series)     | Wide time-series ingestion and queries | Scale-sensitive latency, columnar scan performance |
| [RTABench](#/benchmarks/rtabench)           | Operational event-analytics            | Real-time aggregation patterns                     |
| [ClickBench](#/benchmarks/clickbench)       | Analytical scan-heavy queries          | Full-table scan and filter performance             |
| [Kaggle Airbnb](#/benchmarks/kaggle_airbnb) | Join-oriented analytics                | Multi-table join performance                       |

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

### Scoring

Databases are ranked by a composite score that weights both median query latency and the number of individual query wins. This balances overall throughput against best-case performance on specific query shapes.

## Navigating the results

Use the **benchmark tabs** in the navigation bar to switch between suites, and the **system selector** on the right to choose which machine's results to view.

Within each benchmark page you can:

- Filter which databases are shown
- Toggle between linear and logarithmic duration scales
- Click on individual queries to see per-database breakdowns and the SQL source

## Links and navigation examples

Internal links use hash-based routing. Here are some examples:

- Link to a benchmark suite: [Time Series results](#/benchmarks/time_series)
- Link to the home page: [Home](#/)
- External links work normally: [DuckDB documentation](https://duckdb.org/docs/)
