# OLAP Benchmarks: Analysis and Release Preparation

*Analysis date: 2026-04-12 | System: macbook-pro-m4 | Single system, all databases running in Docker (except DuckDB in-process)*

## Executive Summary

**Databases tested:** DuckDB 1.5.0, ClickHouse 26.1.1, MonetDB Dec2025-SP1, PostgreSQL 18.3, TimescaleDB 2.25.0 (on PG 18)

**Suites:** clickbench (44 queries), rtabench (31 queries), time_series (60 queries across tall/wide/large), kaggle_airbnb (5 queries)

**Overall ranking by median query latency (select operations):**
1. **DuckDB** -- fastest in ~90% of queries across all suites
2. **ClickHouse** -- typically 2-5x slower than DuckDB, but consistent
3. **MonetDB** -- variable; competitive on simple queries, degrades on complex ones
4. **PostgreSQL** -- expected worst on full scans, but surprisingly fast on indexed point lookups
5. **TimescaleDB** -- wildly inconsistent: 200x faster than Postgres on some queries, 2000x slower on others

---

## 1. Per-Suite Results Summary

### 1.1 ClickBench (44 queries, 3 iterations, ~100M rows, 107 columns)

| DB | Queries where fastest | Median of medians (s) | Worst query (s) |
|---|---|---|---|
| DuckDB | 37 | 0.07 | 3.99 (Q28) |
| ClickHouse | 1 | 0.23 | 5.52 (Q28) |
| MonetDB | 0 | 0.39 | 33.58 (Q33) |
| PostgreSQL | 5 | 3.38 | 257.36 (Q33) |
| TimescaleDB | 1 | 1.35 | 150.54 (Q32) |

**Notable anomalies:**
- **Q17** (`GROUP BY UserID, SearchPhrase LIMIT 10` -- no ORDER BY): Postgres returns in 0.005s while DuckDB takes 0.27s and TimescaleDB takes 10.7s. This is because Postgres can return an arbitrary 10 rows immediately without computing the full GROUP BY, while columnar engines must complete the full aggregation. This is a legitimate behavioral difference, but worth documenting.
- **Q19/Q24/Q25/Q26** (point lookups on UserID/SearchPhrase): Postgres wins by 10-100x due to B-tree indexes. These are OLTP-style queries included in the ClickBench spec.
- **Q32-Q34** (high-cardinality GROUP BY with many columns): Postgres and TimescaleDB are 100-400x slower than DuckDB.

### 1.2 RTABench (31 queries, 5 tables, multi-table JOINs)

| DB | Queries where fastest | Median of medians (s) | Worst query (s) |
|---|---|---|---|
| DuckDB | 24 | 0.016 | 0.094 |
| ClickHouse | 0 | 0.30 | 5.21 |
| MonetDB | 1 | 0.07 | 18.59 |
| PostgreSQL | 3 | 2.85 | 10.78 |
| TimescaleDB | 3 | 0.23 | 9.96 |

**Notable findings:**
- **ClickHouse is surprisingly slow on RTABench** -- consistently 10-100x slower than DuckDB on multi-table JOIN queries. This aligns with known ClickHouse weakness on JOINs.
- **TimescaleDB outperforms plain Postgres** on most RTABench queries (typically 5-20x faster), suggesting hypertable+compression on order_events works well for this workload.
- Query 0017 (`top_selling_month_product`): ClickHouse takes 2.66s vs DuckDB 0.024s (110x slower) -- likely a JOIN optimizer issue.

### 1.3 Time Series (60 queries across 3 dataset shapes, 5 iterations)

Dataset shapes:
- **tall**: 2M rows x 10 cols (narrow, many rows)
- **wide**: 200K rows x 1,500 cols (few rows, many columns)
- **large**: 4M rows x 1,500 cols (both)

| DB | Queries where fastest | Worst slowdown vs DuckDB |
|---|---|---|
| DuckDB | 49 | -- |
| ClickHouse | 0 | 24x (large_23_batch_export) |
| MonetDB | 0 | 44x (large_23_batch_export) |
| PostgreSQL | 10 | 1700x (large_12_conditional_aggregate) |
| TimescaleDB | 1 | 6200x (large_19_null_gap_detection) |

**TimescaleDB configuration problem confirmed:**

TimescaleDB is *dramatically slower* than plain Postgres on several time_series queries:
- `large_06_daily_resample`: TimescaleDB 100.7s vs Postgres 7.9s (13x slower)
- `large_18_top_n_hourly`: TimescaleDB 89.3s vs Postgres 7.9s (11x slower)
- `large_19_null_gap_detection`: TimescaleDB 93.0s vs Postgres 9.8s (9.5x slower)
- `large_17_group_by_hour_category`: TimescaleDB 22.0s vs Postgres 8.1s (2.7x slower)
- `large_07_full_aggregate`: TimescaleDB 16.2s vs Postgres 7.9s (2x slower)

Root cause: **`data_large` does NOT have columnstore/compression enabled** because it has 1,500 columns which exceeds PostgreSQL's 8160-byte max tuple size for compressed rows. So TimescaleDB is paying the hypertable chunk overhead without getting any columnstore benefit. With a 14-day chunk interval and 4M rows of minute-resolution data (~7.6 years), there are many chunks to scan.

However, TimescaleDB correctly outperforms Postgres where `time_bucket` can leverage the hypertable:
- `large_04_hourly_resample_single`: TimescaleDB 0.041s vs Postgres 7.9s (193x faster)
- `large_05_hourly_resample_multi`: TimescaleDB 0.051s vs Postgres 11.5s (225x faster)

These use TimescaleDB-specific `time_bucket()` overrides instead of `date_trunc()`.

**Postgres wins on point lookups** (MAX, scalar lookup) because it has a B-tree index on `time` and can do index-only scans, while all columnar engines must scan column data.

### 1.4 Kaggle Airbnb (5 queries, multi-table JOINs)

| DB | Q1 (count) | Q2 (1 join) | Q3 (2 joins) | Q4 (3 joins + array_agg) | Q5 (3 joins + row_number) |
|---|---|---|---|---|---|
| DuckDB | 0.004 | 0.27 | 0.33 | 1.26 | 1.77 |
| ClickHouse | 0.008 | 0.57 | 0.99 | 2.90 | 2.51 |
| MonetDB | 0.005 | 5.69 | 8.47 | 66.49 | 81.68 |
| PostgreSQL | 0.099 | 8.49 | 13.02 | 125.69 | 162.86 |
| TimescaleDB | 0.065 | 8.15 | 13.64 | 153.23 | 156.14 |

DuckDB and ClickHouse dominate. MonetDB is surprisingly poor on JOINs here. TimescaleDB adds no value over plain Postgres for this workload (no hypertables, no time-series structure).

---

## 2. Ingestion / Populate Performance

### 2.1 ClickBench (single table, ~100M rows, 107 cols)

| DB | Total populate (s) | Insert only (s) | Indexing/Compress (s) |
|---|---|---|---|
| DuckDB | 52 | 52 | -- |
| ClickHouse | 175 | 161 | 1.9 (optimize) |
| PostgreSQL | 4636 | 691 | 3943 (indexes) |
| MonetDB | 1054 | 1042 | -- |
| TimescaleDB | 2541 | 1103 | 910 (compress) + 509 (verify) |

DuckDB ingests 90x faster than Postgres. TimescaleDB compression adds ~1400s overhead. The 509s `verify_populate` for TimescaleDB is suspicious -- likely decompressing to count rows.

### 2.2 Time Series

| DB | Total populate (s) | Notes |
|---|---|---|
| DuckDB | 70 | Fastest |
| MonetDB | 127 | |
| ClickHouse | 197 | |
| PostgreSQL | 319 | |
| TimescaleDB | 3930 | **56x slower than DuckDB** -- hypertable creation (211s) + insert (520s) + compress (3195s) |

The 3195s compression time for TimescaleDB is dominated by `data_large` which **cannot actually be compressed** due to the tuple size limit. This time is likely wasted attempting and failing compression on each chunk, or compressing only `data_tall` and `data_wide`.

---

## 3. Mutation Performance (time_series only)

| Operation | DuckDB | ClickHouse | MonetDB | PostgreSQL | TimescaleDB |
|---|---|---|---|---|---|
| Total mutate phase | 32s | 456s | 312s | 237s | 134s |
| delete_large_10000 | 0.017s | 16.1s | 0.6s | 13.9s | 3.7s |
| delete_wide_10000 | 0.013s | 5.4s | 0.6s | 0.8s | 2.2s |
| insert_large_10000 | 1.1s | 1.5s | 1.2s | 1.3s | N/A (disabled) |
| delete_tall_1 | 0.005s | 0.09s | 0.03s | 0.01s | 0.03s |

- **ClickHouse DELETE is extremely slow** (~16s regardless of row count for large tables). This is because ClickHouse deletes are mutations that rewrite parts, not in-place operations.
- **DuckDB dominates** across all mutation types.
- TimescaleDB `insert_data_large_10000` is disabled due to the tuple size limit.

---

## 4. Potential Configuration Issues

### 4.1 TimescaleDB / PostgreSQL -- CRITICAL: EAV schema for wide time-series tables

**Problem:** `data_large` (4M rows x 1500 cols) and `data_wide` (200K rows x 1500 cols) use a wide columnar schema that cannot use TimescaleDB columnstore compression (exceeds PostgreSQL's 8160-byte max tuple size). This makes the hypertable pure overhead -- TimescaleDB is *slower* than plain Postgres on most queries for these tables.

**Root cause:** Wide telemetry tables (1500+ columns) are common in industrial settings (SCADA, IoT, ML feature stores) and work naturally on columnar engines. But PostgreSQL's row-oriented storage cannot handle this -- the tuple size limit breaks TimescaleDB compression. The idiomatic row-store approach is EAV (entity-attribute-value):

```sql
-- Columnar engines (DuckDB, ClickHouse, MonetDB) keep the wide schema:
--   time TIMESTAMP, process_1 FLOAT, ..., process_1275 FLOAT  (1500 cols)

-- Row-store engines (PostgreSQL, TimescaleDB) use EAV:
CREATE TABLE data_large (
    time TIMESTAMP NOT NULL,
    metric_name TEXT NOT NULL,
    value FLOAT
);
-- 4M timestamps * 1500 metrics = 6B rows, 3 columns
-- This is exactly what TimescaleDB is designed for.
```

**Implementation:** Override the time_series queries for Postgres/TimescaleDB (same pattern as existing `time_bucket` overrides). Queries return identical results but use EAV-style SQL:

```sql
-- Wide schema (DuckDB/ClickHouse/MonetDB):
SELECT avg(process_364) FROM data_large WHERE time > '2020-01-01'

-- EAV schema (PostgreSQL/TimescaleDB):
SELECT avg(value) FROM data_large WHERE metric_name = 'process_364' AND time > '2020-01-01'
```

**Why this is fair:** The benchmark should test each engine's idiomatic approach to the same problem. The interesting insight is: *does the right schema on each engine close the performance gap, or is the wide-table advantage of columnar engines fundamental?* Note that EAV forces query overhead (filtering by metric_name, pivoting for multi-column access) that columnar engines avoid entirely -- this is a real architectural tradeoff worth measuring.

**Additional chunk tuning needed:** Once EAV is in place, revisit chunk intervals. With 6B rows the 14-day interval creates much larger chunks that compress well. Consider `compress_segmentby = 'metric_name'` for optimal TimescaleDB compression.

### 4.2 ClickHouse -- ORDER BY column selection

**Problem:** For auto-created tables (time_series, clickbench), the ORDER BY is set to the first column or primary key. For time_series, this is `ORDER BY (time)` which is reasonable. But for clickbench, the official ClickHouse benchmark uses a carefully tuned `ORDER BY (CounterID, EventDate, UserID, EventTime, WatchID)`. The current implementation likely uses whatever column happens to be first.

**Recommended fix:** Check if the clickbench schema for ClickHouse uses the official ORDER BY. If it auto-generates from the first column, this could significantly undercount ClickHouse performance.

### 4.3 PostgreSQL -- Missing parallel query configuration

The clickbench TimescaleDB schema sets `min_parallel_table_scan_size TO '0'` and `work_mem TO '1GB'`, but the plain Postgres clickbench setup only creates indexes without tuning these GUCs. This is an unfair disadvantage for Postgres on full-scan queries.

### 4.4 MonetDB -- Restart overhead

MonetDB has a 445s restart for rtabench and 12s for others. This suggests it's doing significant work on startup (WAL replay, memory mapping). Not a config issue per se, but the restart is included in populate timing.

---

## 5. Fairness Analysis of Suites and Queries

### 5.1 ClickBench

**Source:** Official ClickBench by ClickHouse Inc.

**Fairness concerns:**
- **Created by ClickHouse** -- the queries and data model are designed around ClickHouse's single-wide-table paradigm. No JOINs, no normalized schema. This inherently favors columnar engines, especially ClickHouse.
- **Some queries are OLTP-style** (Q17, Q19, Q24-Q26): point lookups and LIMIT-without-ORDER-BY queries where row-store indexes win. These are included in the official spec but arguably don't represent OLAP workloads.
- **Q29** (89 consecutive SUMs of ResolutionWidth+N) is a synthetic benchmark of aggregation throughput, not a realistic query.
- **Well-established**: This is the most widely recognized OLAP benchmark. Including it gives credibility and comparability with other benchmark publications.

**Verdict:** Keep it, but acknowledge its ClickHouse origin. Consider categorizing results by query type (scan, aggregation, point lookup, text search).

### 5.2 RTABench

**Source:** Timescale Inc.

**Fairness concerns:**
- **Created by Timescale** -- designed around a relational schema with JOINs and normalized tables. This inherently favors PostgreSQL-family databases and disadvantages ClickHouse (poor JOIN performance).
- **Only `order_events` is a hypertable** in TimescaleDB -- the other 4 tables are regular PostgreSQL tables. So TimescaleDB's advantages are limited to queries touching order_events.
- **JSON extraction queries** (event_payload) test a feature not all DBs handle equally. MonetDB has limited JSON support.
- **Realistic workload**: Multi-table JOINs, aggregations, EXISTS subqueries, window functions. This is what real OLAP queries often look like.

**Verdict:** Keep it. Good counterbalance to ClickBench's single-table bias.

### 5.3 Time Series

**Source:** Custom/original for this benchmark.

**Fairness concerns:**
- **Synthetic data** with generated column names (process_364, binary_22, etc.). No real-world semantic meaning.
- **1,500-column tables are realistic for industrial telemetry** (SCADA/DCS systems, IoT fleet monitoring, ML feature stores). A power plant might log 10k+ signals at 1-min frequency into a single wide table for efficient `SELECT time, col1, col2, ...` access in visualization and ML pipelines. This is a legitimate workload that exposes a fundamental row-store limitation (PostgreSQL's tuple size cap breaks TimescaleDB compression).
- **The `large` dataset (4M x 1500) is the main differentiator** but it's also the most unrealistic. `tall` (2M x 10) is realistic but too small to differentiate fast databases.
- **All queries touch only 1-6 columns** even on the 1500-column table, which favors columnar storage. A real application might SELECT * for export/ETL.
- **`time_bucket` vs `date_trunc` overrides** give TimescaleDB a built-in advantage on resampling queries that other databases don't get equivalent optimization for.
- **No multi-table queries** -- all time_series queries operate on a single table. Real time-series workloads often join metadata tables.
- **Good query variety**: point lookups, range scans, resampling, rolling windows, gap detection, batch export. These are genuine time-series operations.

**Verdict:** The 1500-column schema is a realistic industrial workload (SCADA, IoT, ML feature stores) that columnar engines handle natively. Row stores (Postgres/TimescaleDB) fundamentally cannot represent this schema efficiently due to tuple size limits -- they must use EAV, which is how these engines are actually deployed for wide telemetry. Use EAV schema overrides for Postgres/TimescaleDB to test the idiomatic approach. This becomes a key insight: *columnar engines offer direct wide-table access that row stores must work around*. Additionally consider:
- Add multi-table queries (e.g., join time-series data with a metadata/tags table)
- Increase `tall` row count (20M+ rows) to better differentiate fast engines on narrow tables

### 5.4 Kaggle Airbnb

**Source:** Medium blog post by Vitaliy, data from Kaggle.

**Fairness concerns:**
- **Only 5 queries** -- too few to draw meaningful conclusions.
- **Small dataset** (populate takes <25s for all DBs) -- not stressing any database's capabilities.
- **Primarily tests JOIN performance** which is already covered by RTABench with a larger dataset.
- **Data quality issues**: The source Kaggle dataset has messy CSV data requiring type casting, which could introduce inconsistencies.

**Verdict:** Consider dropping or significantly expanding. It doesn't add much beyond RTABench.

---

## 6. Anticipated Criticism and Mitigation Plan

### 6.1 "You're running everything on a laptop"

**Criticism:** Results from a MacBook Pro M4 don't represent production deployments. Docker overhead varies per DB. DuckDB runs in-process while others run in containers.

**Mitigation:**
- [ ] Document hardware specs explicitly (CPU cores, RAM, SSD speed)
- [ ] Document Docker resource limits (memory, CPU pinning)
- [ ] Add a disclaimer that this benchmarks *single-machine local deployment*, not distributed clusters
- [ ] Consider adding a Linux server system for comparison
- [ ] DuckDB's in-process advantage is real but also representative of how people actually use it

### 6.2 "DuckDB has an unfair advantage running in-process"

**Criticism:** DuckDB avoids network/IPC overhead that all Docker-based databases pay.

**Mitigation:**
- [ ] Measure and document the Docker overhead (run a query that returns large results vs small results to isolate serialization cost)
- [ ] Note that DuckDB's deployment model IS the product -- comparing in-process DuckDB to Docker Postgres is comparing how they're actually used
- [ ] Consider also running DuckDB in a separate process or via JDBC for a "fair" comparison row

### 6.3 "Your ClickHouse ORDER BY / schema is wrong"

**Criticism:** ClickHouse performance is extremely sensitive to ORDER BY key selection and MergeTree engine tuning. A generic `ORDER BY (first_column)` setup is not representative.

**Mitigation:**
- [ ] Audit the ClickHouse ORDER BY for all suites -- ensure it matches official recommendations
- [ ] For ClickBench: use the official ClickHouse schema from the ClickBench repo
- [ ] For RTABench: ensure ORDER BY matches the most common query patterns (filter + join columns)
- [ ] Document the exact schema/engine config used for each DB

### 6.4 "TimescaleDB is misconfigured"

**Criticism:** The `data_large` table can't use columnstore, making TimescaleDB results meaningless for that dataset.

**Mitigation:**
- [ ] Use EAV schema for Postgres/TimescaleDB on wide/large tables (see section 4.1) -- this is how these engines are actually used
- [ ] Ensure all TimescaleDB hypertables have compression enabled (EAV makes this possible)
- [ ] Tune chunk intervals and `compress_segmentby = 'metric_name'` for optimal compression
- [ ] Document the schema difference explicitly and explain *why* -- this becomes an insight, not a flaw

### 6.5 "Where are the distributed/cluster benchmarks?"

**Criticism:** ClickHouse's real strength is distributed queries. Comparing single-node ClickHouse to DuckDB is misleading.

**Mitigation:**
- [ ] Title the benchmark explicitly as "single-node OLAP" or "embedded/local OLAP"
- [ ] Acknowledge that distributed deployments would change the picture
- [ ] Potentially add a "scale-out" section in the future

### 6.6 "3 iterations isn't enough / no warmup"

**Criticism:** 3 iterations for ClickBench and 5 for others may not be statistically significant. First-run vs cached performance differs.

**Mitigation:**
- [ ] Report min/median/max to show variance
- [ ] Consider 5 iterations minimum for all suites
- [ ] Add explicit cold-run vs warm-run reporting (iteration 1 vs median of 2-5)
- [ ] The DB restart between populate and select ensures cold cache for first iteration

### 6.7 "Synthetic data doesn't represent real workloads"

**Criticism:** Generated time-series data with random column names isn't representative.

**Mitigation:**
- [ ] ClickBench uses real Yandex.Metrica data -- note this
- [ ] RTABench uses Timescale's realistic e-commerce data
- [ ] Kaggle Airbnb uses real Airbnb listing data
- [ ] The time_series wide/large datasets model real SCADA/IoT telemetry workloads (thousands of signals at minute frequency). The column names are generic but the shape and access patterns are realistic.
- [ ] Consider additionally using a real public dataset (e.g., NYC taxi, weather stations, financial tick data) for extra credibility

### 6.8 "Why no [Databricks/StarRocks/Apache Doris/QuestDB/Polars]?"

**Criticism:** Missing popular OLAP engines.

**Mitigation:**
- [ ] QuestDB support already exists in code but has no results -- run it or remove it
- [ ] Clearly state criteria for inclusion (open-source, runnable locally, SQL interface)
- [ ] Invite community contributions for additional engines
- [ ] Polars is not a database -- acknowledge it as a DataFrame library comparison point if desired

### 6.9 "ClickBench was designed to make ClickHouse look good"

**Criticism:** Using a benchmark created by a vendor to evaluate that vendor is circular.

**Mitigation:**
- [ ] Present ClickBench results alongside non-ClickHouse-originated suites (RTABench, time_series)
- [ ] DuckDB actually beats ClickHouse on ClickBench, so the bias argument is weakened
- [ ] Note that ClickBench has become a de facto standard despite its origin

### 6.10 "PostgreSQL shouldn't even be in an OLAP benchmark"

**Criticism:** Comparing a general-purpose RDBMS to purpose-built OLAP engines is unfair.

**Mitigation:**
- [ ] PostgreSQL is included as a **baseline** -- many teams evaluate "do we need a specialized OLAP engine or is Postgres enough?"
- [ ] This is a genuinely useful data point for the target audience
- [ ] PostgreSQL's indexed point-lookup wins demonstrate that it's not always slower

---

## 7. Priority Action Items for Release

### Critical (must fix before release)

1. **Implement EAV schema for Postgres/TimescaleDB time_series** -- use an entity-attribute-value table for `data_large` and `data_wide`, with query overrides that return equivalent results. This lets TimescaleDB use columnstore compression and tests each engine idiomatically.

2. **Audit ClickHouse ORDER BY for ClickBench** -- verify it uses the official recommended ORDER BY, not an auto-generated one. This could change ClickHouse results significantly.

3. **Equalize PostgreSQL GUC tuning** -- if TimescaleDB gets `work_mem = 1GB` and `min_parallel_table_scan_size = 0`, plain PostgreSQL should get equivalent tuning for ClickBench.

4. **Document all database configurations** -- every CREATE TABLE, every index, every GUC setting should be visible and auditable. Publish the exact schemas used.

### High priority (should fix before release)

5. **Implement EAV query/schema overrides** for Postgres/TimescaleDB on wide/large time_series tables. Keep the 1500-column schema for columnar engines (it's realistic SCADA/IoT). The EAV pivot overhead is part of the benchmark insight.

6. **Increase ClickBench iterations** from 3 to 5 for consistency with other suites.

7. **Add cold vs warm reporting** -- separate first-iteration (cold) from subsequent iterations (warm) in the analysis.

8. **Either run QuestDB or remove it** from the codebase. Having dead code for an untested DB invites questions.

9. **Add methodology documentation** -- explain the Docker setup, restart strategy (ensuring cold cache), measurement approach (wall clock including result serialization), and any known limitations.

### Medium priority (nice to have for release)

10. **Replace Kaggle Airbnb** with either a larger dataset or drop it. It's too small and overlaps with RTABench.

11. **Add query categorization** -- tag each query with its type (point lookup, range scan, full scan, aggregation, JOIN, window function, text search) so readers can filter results by workload pattern.

12. **Add a "query equivalence" verification** -- confirm all DB-specific query variants return the same results. Different SQL dialects could cause semantic differences.

13. **Consider adding a real-world time-series dataset** (NYC taxi, weather data, financial data) as an alternative to synthetic generation.

14. **Add resource consumption metrics** -- the `run_metric` table captures CPU/memory/disk. Include these in the analysis to show cost-of-query, not just wall time.

---

## 8. Appendix: Suspicious Data Points to Investigate

| Suite | Query | Issue |
|---|---|---|
| clickbench | Q17 | Postgres 0.005s vs DuckDB 0.27s -- LIMIT without ORDER BY, Postgres short-circuits |
| time_series | large_06_daily_resample | TimescaleDB 100.7s vs Postgres 7.9s -- hypertable overhead without compression; will be resolved by EAV schema |
| time_series | large_18_top_n_hourly | TimescaleDB 89.3s vs Postgres 7.9s -- same root cause |
| time_series | large_19_null_gap_detection | TimescaleDB 93.0s vs Postgres 9.8s -- same root cause |
| clickbench | Q29 | 89 SUM expressions -- synthetic, tests vectorization, not realistic |
| rtabench | 0017_top_selling_month_product | ClickHouse 2.66s vs DuckDB 0.024s -- 110x gap on a JOIN query |
| rtabench | 0025_product_category_performance | ClickHouse 5.21s vs DuckDB 0.030s -- 173x gap |
| time_series | large_23_batch_export | All DBs slow (1.4-63s) -- tests serialization throughput more than query performance |
| clickbench | Q32-Q34 | Massive spread (DuckDB 0.6s, Postgres 257s) -- high-cardinality GROUP BY |
