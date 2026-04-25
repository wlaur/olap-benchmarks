## Time Series

Time-series ingestion and analytical queries across table shapes (wide, tall, large).

**Databases tested:** ClickHouse, MonetDB (more coming)

| Metric                | ClickHouse | MonetDB |
| --------------------- | ---------- | ------- |
| Populate (data load)  | ~45s       | ~120s   |
| Median SELECT latency | ~0.02s     | ~0.08s  |
| Median MUTATE latency | ~0.15s     | ~0.40s  |
| Peak memory           | ~1.2 GB    | ~2.8 GB |

[Explore Time Series results](#/explorer/time_series)

---

## RTABench

Group-bys, distinct counts, and filtered aggregations over a normalized order-tracking schema. Based on [RTABench](https://github.com/timescale/rtabench) by Timescale.

**Status:** Benchmark defined, results pending.

[Explore RTABench results](#/explorer/rtabench)

---

## ClickBench

Full-table scans, filters, and aggregations over a single wide table. Based on [ClickBench](https://github.com/ClickHouse/ClickBench) by ClickHouse.

**Status:** Benchmark defined, results pending.

[Explore ClickBench results](#/explorer/clickbench)

---

## Kaggle Airbnb

Multi-table joins and mixed aggregations over Airbnb listings data. Based on ["Testing query speed for DuckDB vs ClickHouse vs StarRocks"](https://medium.com/@marvin_data/testing-query-speed-for-duckdb-vs-clickhouse-vs-starrocks-databases-fecc6614d1ef) by Vitaliy.

**Status:** Benchmark defined, results pending.

[Explore Kaggle Airbnb results](#/explorer/kaggle_airbnb)

---

## Methodology

All benchmarks for a given **system** (e.g. `macbook-pro-m4`) run on the same machine. Results from different systems are never compared directly.

- Runs are fully automated: schema creation, data loading, query execution, metric collection
- Database versions, config, and SQL are pinned per revision
- Raw results are stored in DuckDB and checked into git

### Metrics

- **Populate** — time to create tables and load data
- **Query latency** — wall-clock time per query (median of multiple iterations)
- **Resources** — CPU, memory, and disk usage sampled throughout each phase

## Using the explorer

- Switch between suites and filter databases
- Toggle linear / log duration scales
- Click queries for per-database breakdowns and SQL source
- Inspect CPU, memory, and disk trends per step
