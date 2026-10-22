## October 2026 · M4 Pro

**DuckDB** has the strongest results overall, with lower latency, memory use and storage than Polars on [TPC-H SF50](#/explorer/tpc_h?mode=database&system=macbook-m4-pro&scale=50&databases=duckdb,clickhouse,starrocks,polars). **StarRocks** also does well on that workload, beating ClickHouse on [18 of the 22 queries](#/explorer/tpc_h?mode=database&system=macbook-m4-pro&scale=50&databases=starrocks,clickhouse&query=15_top_supplier&sql_database=starrocks).

**ClickHouse** wins all five [JSONBench queries](#/explorer/jsonbench?mode=database&system=macbook-m4-pro&scale=10), although its [global chat keyword search](#/explorer/chat_threads?mode=database&system=macbook-m4-pro&scale=10&databases=clickhouse,duckdb&query=09_keyword_search_global&sql_database=clickhouse) takes 6.49 s against DuckDB’s 0.24 s. **Doris** still wins [ClickBench Q21](#/explorer/clickbench?mode=database&system=macbook-m4-pro&scale=1&databases=doris,duckdb,clickhouse&query=Q21&sql_database=doris) with result caching disabled, at 38 ms against DuckDB’s 315 ms.

**MonetDB** wins 32 [TPC-DS queries](#/explorer/tpc_ds?mode=database&system=macbook-m4-pro&scale=1), but struggles with [large string group-bys](#/explorer/clickbench?mode=database&system=macbook-m4-pro&scale=1&databases=monetdb,duckdb,clickhouse&query=Q33&sql_database=monetdb). **Polars** has the lowest total [metrics latency](#/explorer/time_series_metrics?mode=database&system=macbook-m4-pro&scale=1&databases=polars,duckdb&query=wide5000_12_all_metrics&sql_database=polars) among complete runs, though DuckDB wins 87 of their 112 individual queries. **TimescaleDB’s** [compressed EAV storage](#/explorer/time_series_ranges?mode=database&system=macbook-m4-pro&scale=1&databases=timescaledb,postgres,duckdb&query=eav4094_15_calc_10_metrics_100d&sql_database=timescaledb) helps reads, though loading and updates remain expensive here.

## Memory and storage

On TPC-H SF50, DuckDB’s sampled peak RSS is 13.2 GiB and its final store is 13.8 GiB, compared with Polars’ 24.7 and 19.0 GiB. The memory limits differ, and neither resource use nor loading and writes contribute to the scores above; see the [resource notes](https://github.com/wlaur/olap-benchmarks/blob/main/docs/campaigns/2026-10-expanded-analysis.md#memory-and-storage-change-the-interpretation) for those comparisons.

## How to read these results

Each query runs once to warm up, then three times for measurement; we report the median, including driver, transfer and conversion time. Runs are serial with warm OS caches, and [documented layouts and query tuning](https://github.com/wlaur/olap-benchmarks/blob/main/docs/campaigns/2026-10-tuning-protocol.md) are allowed.

Missing queries and suites lower an engine’s standing, so specialist engines can rank poorly despite good results in the workloads they support. The [campaign report](https://github.com/wlaur/olap-benchmarks/blob/main/docs/campaigns/2026-10-configuration-correction.md) lists the exclusions, and the [catalog](#/catalog) covers all ten suites. Airbnb is excluded from the overall score, and TPC-derived results are not comparable to published TPC benchmarks.
