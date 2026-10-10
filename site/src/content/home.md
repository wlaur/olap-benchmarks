## Suites

- **Time Series** — time-series ingestion and analytical queries across wide, tall, and large table shapes. [Results](#/explorer/time_series)
- **RTABench** — group-bys, distinct counts, and filtered aggregations over a normalized order-tracking schema. Based on [RTABench](https://github.com/timescale/rtabench) by Timescale. [Results](#/explorer/rtabench)
- **ClickBench** — full-table scans, filters, and aggregations over a single wide table. Based on [ClickBench](https://github.com/ClickHouse/ClickBench) by ClickHouse. [Results](#/explorer/clickbench)
- **JSONBench** — analytical queries over semi-structured Bluesky event JSON. Based on [JSONBench](https://github.com/ClickHouse/JSONBench) by ClickHouse. [Results](#/explorer/jsonbench)
- **Chat Threads** — queries over generated AI chat histories stored as rich JSON documents. Original to this project; not comparable to any published benchmark. [Results](#/explorer/chat_threads)
- **Kaggle Airbnb** — small smoke/example coverage for multi-table joins and mixed aggregations over Airbnb listings data. Based on [a comparison](https://medium.com/@marvin_data/testing-query-speed-for-duckdb-vs-clickhouse-vs-starrocks-databases-fecc6614d1ef) by Vitaliy. Shown, but excluded from the overall ranking. [Results](#/explorer/kaggle_airbnb)
- **TPC-H** — the 22 ad-hoc decision-support queries over a normalized order/lineitem schema, derived from the [TPC-H benchmark](https://www.tpc.org/tpch/) (not comparable to published TPC-H results). [Results](#/explorer/tpc_h)
- **TPC-DS** — the 99 decision-support queries over a retail snowflake schema (24 tables), derived from the [TPC-DS benchmark](https://www.tpc.org/tpcds/) (not comparable to published TPC-DS results). [Results](#/explorer/tpc_ds)

## Methodology

Every run records the system it ran on, and results from different systems are not comparable. The ranking above is scoped to the selected system; explorer pages can also compare the same suite and scale factor across systems, versions, and scale factors. The engines shown are the ones with published runs for that system and suite.

- Runs are automated end to end: container start, schema creation, data loading, query execution, metric collection
- Engine versions are pinned in the repository and recorded on every run, together with the query text and container image
- Queries run through each engine's own SQL dialect; most suites share one query text, with per-engine variants where the dialect requires it
- Raw results are DuckDB database files. The published one is the `results.db` asset on the repository's `data` release, and this site queries it in your browser with DuckDB-WASM

### What is timed

- **Query latency** is end-to-end client latency: a `perf_counter` timer around the client's `fetch()`, from sending the query until the full result is materialized as a Polars DataFrame. It includes driver, transfer, and conversion time, and is not the engine's internal execution time.
- Each select query runs a fixed number of iterations defined per suite (mostly 3–5). The reported value is the median of the warm iterations (all iterations after the first); a query with a single recorded iteration falls back to that value.
- The first iteration is not a true cold run. Each benchmark operation (populate, select, …) runs in a fresh process, so embedded engines such as DuckDB and Polars are reopened per operation, and server engines are restarted after populate. Operating-system file caches are never dropped, so the first iteration may still read warm pages.
- Populate time, mutation and concurrency workloads, and resource usage (CPU, server and client memory, disk) are recorded with every run but not yet shown on this site.

### Scores

A suite score is the geometric mean, over the suite's queries, of each engine's latency divided by the fastest latency for that query, with 10 ms added to both sides so very fast queries do not dominate. A missing or failed query counts as 10× or twice the slowest engine's ratio for that query, whichever is larger. The overall ranking is the geometric mean across benchmark suites. 1.0× means fastest on every query.

Scores are relative to the fastest engine **in the current selection**, not absolute. Adding or removing an engine (or changing the explorer comparison) can change every other engine's score, including the penalty for missing queries; compare absolute latencies in the explorer for anything that must hold across selections.

### Engine configuration

Vendor-recommended settings, storage layout, and per-suite physical design are allowed when documented here. Results should be read as configured benchmark runs, not stock-default engine comparisons.

- **ClickHouse** uses the pinned Docker image without server configuration changes. Suites use MergeTree tables with suite-specific sort keys and codecs, and query-level settings from ClickHouse's own TPC-H/TPC-DS kits where those queries need them. Results are streamed as Arrow into Polars.
- **DuckDB** runs in process with the installed Python package and no server. Results are fetched as Polars DataFrames.
- **MonetDB** uses the pinned Docker image and is accessed only through ADBC ([adbc-driver-monetdb](https://pypi.org/project/adbc-driver-monetdb/)): data is ingested as Arrow streams and results are read as Arrow tables converted to Polars.
- **Polars** runs in process over Parquet files, using its LazyFrame API instead of SQL, for the suites it implements.
- Other connectors in the repository (PostgreSQL, TimescaleDB, QuestDB, StarRocks, Doris) appear only once they have published runs.

## Explorer

Each suite links to an explorer with overall rankings, per-query latency breakdowns, and database-specific SQL.
