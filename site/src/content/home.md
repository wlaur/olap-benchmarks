## October 2026 · M4 Pro

- **DuckDB 2** leads overall. On [TPC-H SF50](#/explorer/tpc_h?mode=database&system=macbook-m4-pro&scale=50&databases=duckdb,clickhouse,starrocks,polars), it is faster than Polars and uses less RAM and disk.
- **ClickHouse** wins all five [JSONBench queries](#/explorer/jsonbench?mode=database&system=macbook-m4-pro&scale=10). Chat is mixed: [global keyword search](#/explorer/chat_threads?mode=database&system=macbook-m4-pro&scale=10&databases=clickhouse,duckdb&query=09_keyword_search_global&sql_database=clickhouse) takes 6.49 s versus DuckDB’s 0.24 s.
- **StarRocks** beats ClickHouse on [18 of 22 TPC-H SF50 queries](#/explorer/tpc_h?mode=database&system=macbook-m4-pro&scale=50&databases=starrocks,clickhouse&query=15_top_supplier&sql_database=starrocks).
- **MonetDB** wins 32 [TPC-DS queries](#/explorer/tpc_ds?mode=database&system=macbook-m4-pro&scale=1), but [large string group-bys](#/explorer/clickbench?mode=database&system=macbook-m4-pro&scale=1&databases=monetdb,duckdb,clickhouse&query=Q33&sql_database=monetdb) remain slow.
- **Polars** has the lowest summed [metrics latency](#/explorer/time_series_metrics?mode=database&system=macbook-m4-pro&scale=1&databases=polars,duckdb&query=wide5000_12_all_metrics&sql_database=polars) among complete runs. DuckDB still wins 87 of their 112 queries.
- **Doris** keeps some wins with result caching disabled: [ClickBench Q21](#/explorer/clickbench?mode=database&system=macbook-m4-pro&scale=1&databases=doris,duckdb,clickhouse&query=Q21&sql_database=doris) takes 38 ms versus DuckDB’s 315 ms.
- **TimescaleDB** improves with [compressed EAV storage](#/explorer/time_series_ranges?mode=database&system=macbook-m4-pro&scale=1&databases=timescaledb,postgres,duckdb&query=eav4094_15_calc_10_metrics_100d&sql_database=timescaledb). Loading and updates remain costly.

## Costs beyond latency

TPC-H SF50: DuckDB’s sampled peak RSS is 13.2 GiB and final store 13.8 GiB; Polars uses 24.7 and 19.0 GiB. Memory limits differ. Loading, RAM, disk and writes are recorded, but absent from these scores. [Resource notes](https://github.com/wlaur/olap-benchmarks/blob/main/docs/campaigns/2026-10-expanded-analysis.md#memory-and-storage-change-the-interpretation).

## Method

One warmup, three measured fetches; median reported. Timing includes driver, transfer and conversion. OS caches stay warm. Runs are serial. Native layouts and bounded query tuning are allowed. [Tuning rules](https://github.com/wlaur/olap-benchmarks/blob/main/docs/campaigns/2026-10-tuning-protocol.md).

Scores reward latency and coverage. Missing queries and suites carry penalties; narrow coverage can sink a specialist. Airbnb is excluded from the overall score. [Coverage and exclusions](https://github.com/wlaur/olap-benchmarks/blob/main/docs/campaigns/2026-10-configuration-correction.md).

[Browse all ten suites](#/catalog). TPC-derived results are not comparable to published TPC benchmarks. [Data and code](https://github.com/wlaur/olap-benchmarks).
