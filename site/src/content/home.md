## Suites

- **Time Series** — time-series ingestion and analytical queries across wide, tall, and large table shapes. [Results](#/explorer/time_series)
- **RTABench** — group-bys, distinct counts, and filtered aggregations over a normalized order-tracking schema. Based on [RTABench](https://github.com/timescale/rtabench) by Timescale. [Results](#/explorer/rtabench)
- **ClickBench** — full-table scans, filters, and aggregations over a single wide table. Based on [ClickBench](https://github.com/ClickHouse/ClickBench) by ClickHouse. [Results](#/explorer/clickbench)
- **Kaggle Airbnb** — multi-table joins and mixed aggregations over Airbnb listings data. Based on [a comparison](https://medium.com/@marvin_data/testing-query-speed-for-duckdb-vs-clickhouse-vs-starrocks-databases-fecc6614d1ef) by Vitaliy. [Results](#/explorer/kaggle_airbnb)
- **TPC-H** — the 22 ad-hoc decision-support queries over a normalized order/lineitem schema, derived from the [TPC-H benchmark](https://www.tpc.org/tpch/) (not comparable to published TPC-H results). [Results](#/explorer/tpc_h)
- **TPC-DS** — the 99 decision-support queries over a retail snowflake schema (24 tables), derived from the [TPC-DS benchmark](https://www.tpc.org/tpcds/) (not comparable to published TPC-DS results). [Results](#/explorer/tpc_ds)

## Methodology

All results for a given system (e.g. `macbook-pro-m4`) come from the same machine. Results from different systems are not compared.

- Runs are automated end to end: schema creation, data loading, query execution, metric collection
- Database versions, configuration, and SQL are pinned per revision
- Raw results are stored in DuckDB files checked into git

### Metrics

- **Populate** — time to create tables and load data
- **Query latency** — wall-clock time per query, median over all recorded iterations, including the first iteration
- **Resources** — CPU, memory, and disk usage sampled throughout each phase

### Tuning policy

Vendor-recommended server settings, storage layout, and per-suite physical design are allowed when they are documented here. Results should be read as configured benchmark runs, not stock-default engine comparisons.

- **DuckDB** runs in process with the installed Python package and no server process.
- **ClickHouse** uses the pinned Docker image and suite schemas/query variants without extra server tuning.
- **MonetDB** uses the pinned Docker image; selected large-result queries use MonetDB's binary fetch path.
- **Postgres** uses explicit server settings for memory, WAL, and parallel workers, plus suite-specific indexes where defined by the schema.
- **TimescaleDB** uses Timescale tuning, hypertables, compression/chunk options where defined, and the same Postgres-family server settings.
- **QuestDB** uses the pinned Docker image; ClickBench input is sorted by event time before ingest.
- **StarRocks** uses the pinned Docker image and suite schemas/query variants without extra server tuning.

## Explorer

Each suite links to an explorer with per-query latency breakdowns, SQL source, and per-step resource usage. Databases can be filtered and the duration scale toggled between linear and log.
