## Suites

- **Time Series** — time-series ingestion and analytical queries across wide, tall, and large table shapes. [Results](#/explorer/time_series)
- **RTABench** — group-bys, distinct counts, and filtered aggregations over a normalized order-tracking schema. Based on [RTABench](https://github.com/timescale/rtabench) by Timescale. [Results](#/explorer/rtabench)
- **ClickBench** — full-table scans, filters, and aggregations over a single wide table. Based on [ClickBench](https://github.com/ClickHouse/ClickBench) by ClickHouse. [Results](#/explorer/clickbench)
- **Kaggle Airbnb** — multi-table joins and mixed aggregations over Airbnb listings data. Based on [a comparison](https://medium.com/@marvin_data/testing-query-speed-for-duckdb-vs-clickhouse-vs-starrocks-databases-fecc6614d1ef) by Vitaliy. [Results](#/explorer/kaggle_airbnb)

## Methodology

All results for a given system (e.g. `macbook-pro-m4`) come from the same machine. Results from different systems are not compared.

- Runs are automated end to end: schema creation, data loading, query execution, metric collection
- Database versions, configuration, and SQL are pinned per revision
- Raw results are stored in DuckDB files checked into git

### Metrics

- **Populate** — time to create tables and load data
- **Query latency** — wall-clock time per query, median over multiple iterations
- **Resources** — CPU, memory, and disk usage sampled throughout each phase

## Explorer

Each suite links to an explorer with per-query latency breakdowns, SQL source, and per-step resource usage. Databases can be filtered and the duration scale toggled between linear and log.
