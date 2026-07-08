# OLAP Benchmark Relevance Review

Date: 2026-07-08

This document synthesizes the interrupted Claude research workflow at
`/Users/williamlauren/.claude/projects/-Users-williamlauren-repos-olap-benchmarks/3f342f80-45e0-4b07-9bf7-b408cc2d714b/subagents/workflows/wf_a61ac559-645`.
The workflow completed scoping, search, fetch, and most verification work, but
stopped before the final synthesis. The useful raw materials are `journal.jsonl`,
the `agent-*.jsonl` transcripts, and the workflow script under
`workflows/scripts/deep-research-wf_a61ac559-645.js`.

## Bottom Line

The project is still relevant in 2026, but its current benchmark mix is tilted
toward classic 2022-2024 OLAP coverage. It should not be replaced wholesale.
The most valuable next work is to update methodology and versions before the
next full run, then add workloads and engines that reflect where analytical
systems have moved: semi-structured JSON, real-time analytics, dataframe engines,
and optimizer-heavy joins.

The recommended order is:

1. Fix reporting methodology: separate cold and warm iterations, record failed
   query status explicitly, add stronger correctness checks, and capture
   hardware/container metadata.
2. Bump stale database versions before any new public run.
3. Add JSONBench as the next suite.
4. Add Polars and Apache Doris as the next two important engines.
5. Add JOB after that if the goal is optimizer and join-order coverage.
6. Add TSBS/InfluxDB 3 only when the time-series workload is expanded beyond the
   current custom suite.

Postgres and TimescaleDB should stay, but they should be treated as row-store and
hybrid baselines rather than equal full-matrix peers for every large analytical
suite.

## Current Project Snapshot

The local benchmark currently includes these engines:

- ClickHouse
- DuckDB
- MonetDB
- PostgreSQL
- QuestDB
- StarRocks
- TimescaleDB

The current suites are:

- ClickBench
- Kaggle Airbnb
- RTABench
- custom time-series
- TPC-H
- TPC-DS

Default scale factors are small to moderate: ClickBench 1, Kaggle Airbnb 1,
RTABench 1, time-series 1, TPC-H 10, and TPC-DS 1. The custom time-series suite
also supports scale factor 10.

The runner times `fetch(...)`, so query timing includes client round trip and
result materialization into Polars. That is defensible because ClickBench also
measures request/response time, but the report should say this explicitly.

The default Docker platform is `linux/amd64`, while DuckDB is in-process and
StarRocks has Apple Silicon-specific handling. This means local macOS/ARM
results are useful for development and relative smoke testing, but they should
not be presented as clean public performance numbers without a clear host and
container-platform disclaimer.

## Priority Decisions

### 1. Improve Methodology Before a Full Rerun

The current iteration model mixes first-run and warm-run effects. ClickBench
style reporting separates cold behavior from hot behavior and reports the best
warm run. The benchmarking literature also recommends medians and confidence
intervals, and stresses checking result correctness against reference answers.

Recommended changes:

- Record iteration role: `cold`, `warm`, or `steady_state`.
- Report cold and warm numbers separately.
- For warm numbers, report median and best warm run, not just an average.
- Store query status explicitly: success, timeout, wrong result, unsupported,
  or error.
- Keep failed queries in aggregate score calculations as missing/null results
  instead of silently dropping them.
- Add reference answer hashes for deterministic suites where practical.
- Capture host CPU, memory, OS, Docker version, image tag, container platform,
  database version, and whether the engine is in-process or server-based.

True cold-cache measurements are hard on macOS because Docker runs through a
Linux VM. For public results, use an x86_64 Linux host where database processes
can be restarted and OS caches can be controlled. On the local M-series Mac,
label first-iteration results as first-run or lukewarm rather than true cold.

### 2. Bump Versions Before Comparing Engines

Several pins are already stale enough to matter. Recommended version actions:

| Engine | Current local pin | Recommendation |
| --- | --- | --- |
| DuckDB | 1.5.0 | Bump to 1.5.4 before the next run. |
| QuestDB | 9.3.5 | Bump to 9.4.3. It includes Parquet-native table work and query performance fixes. |
| TimescaleDB | 2.25.0 | Bump to 2.28.2 if a PostgreSQL 18-compatible image is available. |
| StarRocks | 4.0.9 | Keep locally if needed for Apple Silicon Docker constraints; use a newer 4.0.x or 4.1.x image for a real x86_64 Linux run. |
| ClickHouse | 26.1.1.912 | Check current stable/LTS tags before rerunning and update to the latest appropriate 26.x release. |
| PostgreSQL | 18.3 | Keep for the next full run. PostgreSQL 19 is still beta as of June 2026 and expected later in 2026. |
| MonetDB | Dec2025-SP2 | Lower priority; update only if a newer service pack is straightforward. |
| Polars | 1.42.1 dependency only | Current enough as a dependency, but it is not yet modeled as an engine. |

The PostgreSQL 19 beta is worth tracking because it includes async I/O,
planner/executor work, eager aggregation, default LZ4 TOAST compression, and JIT
changes. It is not the right baseline for the next stable benchmark run.

### 3. Add JSONBench Next

JSONBench is the strongest next suite candidate. It covers semi-structured JSON
analytics, includes engines already in this repo, and reflects a major real-world
direction for OLAP systems that ClickBench and TPC-H do not cover well.

Recommended implementation:

- Add JSONBench at 10M rows first.
- Add 100M rows if storage and runtime are acceptable.
- Do not start with the full 1B-row dataset on a 200 GB budget.
- Implement the overlapping engines first: ClickHouse, DuckDB, PostgreSQL, and
  StarRocks.
- Add Doris when the Doris engine lands.
- Reuse upstream rules: default settings, no flattening that sidesteps native
  JSON support, and explicit unsupported/null query statuses.

This suite should likely replace Kaggle Airbnb as the small real-world-ish
coverage point in public reporting. Kaggle Airbnb can remain as a smoke or
example suite, but it should not carry much interpretive weight.

### 4. Add Polars as an Engine

Polars is important enough to benchmark directly. It is not a SQL database, so it
should be modeled as an in-process dataframe/query engine rather than forced into
the same server-database category as ClickHouse or PostgreSQL.

Recommended implementation:

- Add a Polars expression/LazyFrame engine first.
- Use streaming collection where applicable.
- Keep SQL mode separate if added later.
- Do not manually rewrite query plans to make Polars look better; use the normal
  Polars API for each query.
- Label it as in-process and single-node.

The Polars PDS-H results show Polars and DuckDB are now the main in-process
analytical comparators. That makes Polars more valuable than adding another
minor server database first.

### 5. Add Apache Doris as the Next Server OLAP Engine

Doris should be the next server-style OLAP engine to add. It appears across
ClickBench, JSONBench, and RTABench ecosystems, targets interactive real-time
analytics, and gives a better modern MPP comparison set alongside ClickHouse and
StarRocks.

Recommended scope:

- Start with ClickBench and JSONBench.
- Add RTABench after the suite is aligned with upstream's current query count.
- Defer deep TPC-DS coverage until Docker setup and load process are stable.

### 6. Keep Row Stores, But Narrow Their Matrix

PostgreSQL and TimescaleDB remain useful because many real systems still use
Postgres-compatible stacks for user-facing analytics, real-time dashboards, and
operational reporting. Timescale's TigerData rebrand does not make TimescaleDB
irrelevant; it clarifies the positioning around real-time analytical workloads on
PostgreSQL.

Do not run row stores as equal peers across every large workload by default.
Recommended matrix:

- Always run: RTABench, custom time-series, JSONBench small/medium, and
  ClickBench with timeouts/null statuses.
- Optional low-scale runs: TPC-H SF10 and TPC-DS SF1.
- Avoid by default: expensive large TPC-H/TPC-DS/JSON/JOB runs that only prove
  the row store is outside the intended operating envelope.

This keeps the baselines meaningful without letting slow row-store runs dominate
disk, runtime, and report interpretation.

## Engine Recommendations

| Engine | Recommendation | Rationale |
| --- | --- | --- |
| Apache Doris | Add next server OLAP engine. | Modern MPP/real-time analytics engine; overlaps with ClickBench, JSONBench, and RTABench. |
| Polars | Add as in-process dataframe engine. | Now a major analytical baseline; compare against DuckDB as a local execution engine. |
| chDB | Add after Polars/Doris. | In-process ClickHouse-family engine; useful bridge between DuckDB and ClickHouse server. |
| CedarDB | Experimental add only. | Community Edition is self-hostable and free without signup, but capped at 64 GiB compressed and proprietary. |
| InfluxDB 3 | Add only with TSBS or a stronger time-series suite. | Relevant for time-series ingest/recent-query workloads, not for TPC-style analytics. |
| DataFusion | Defer or put in a library-engine category. | Important ecosystem engine, but primarily a Rust query engine/library rather than a standalone DB. |
| pg_duckdb | Defer. | Useful hybrid PostgreSQL extension, but not an independent OLAP database baseline. |
| Databend | Defer. | Relevant cloud/lakehouse-style engine, but less urgent than Doris or Polars. |
| Druid/Pinot | Defer. | Better fit for high-concurrency streaming dashboard benchmarks than the current suite mix. |
| GlareDB | Defer. | Federation focus and project maturity make it a lower-priority addition. |
| MonetDB | Keep as legacy baseline; deprioritize expansion. | Historically important column store, but less central to current OLAP comparisons. |

## Suite Recommendations

### Keep and Adjust Existing Suites

ClickBench should remain the main simple analytical scan/filter/group benchmark.
It is widely recognized and has direct overlap with several target engines.

TPC-H and TPC-DS should remain, but the report should emphasize that they are
synthetic decision-support suites and not complete coverage for modern JSON,
real-time, or dataframe workloads. Keep their scale factors modest unless running
on a dedicated benchmark host.

RTABench should remain because it exercises normalized real-time analytics with
joins, selective filters, and pre-aggregated views. The local implementation
appears to have 31 query files, while upstream RTABench documents 33 queries.
Align the local suite before making strong claims.

The custom time-series suite should remain as a project-specific workload. Its
value would increase if the TODO around concurrent writers/readers is completed.

Kaggle Airbnb should be demoted to smoke/example status. It is convenient and
human-readable, but it is too small and weakly standardized to anchor public
engine comparisons.

### Add JSONBench

JSONBench is the top suite addition. It tests native JSON handling and
semi-structured analytics on a realistic Bluesky-derived dataset. Start at 10M
rows, then consider 100M rows.

### Add JOB

JOB is the best next optimizer-heavy suite. It uses real IMDb-derived data and is
designed to stress join ordering and cardinality estimation. It complements
ClickBench and TPC-H because it is not primarily a flat analytical scan
benchmark.

Costs:

- The original repo is PostgreSQL-oriented.
- Queries and load paths will need porting for each analytical engine.
- Some systems may require unsupported/null status handling for specific SQL
  features.

JOB-Complex is a later follow-up. It expands the optimizer stress surface with
more complex predicates and strings, but it is less mature as a standard
benchmark target than JOB.

### Add TSBS Only With a Time-Series Expansion

TSBS is useful if the benchmark wants to compare ingest, compression, and
recent-window time-series query behavior. It pairs naturally with InfluxDB 3,
QuestDB, TimescaleDB, ClickHouse, and possibly DuckDB.

Do not add TSBS just to increase suite count. Add it when the project is ready to
represent time-series behavior as a first-class category, including ingest and
concurrent read/write behavior.

## Methodology Details to Fix

### Cold vs Warm Runs

Do not average first-run and warm-run behavior into one number. Store each
iteration separately and classify it.

Suggested reporting:

- Cold or first-run latency.
- Warm median latency.
- Best warm latency for ClickBench-style comparison.
- Optional p95 when running enough iterations.

### Correctness

Add correctness checks beyond successful execution. At minimum, deterministic
query suites should store expected row counts and result hashes for reference
engines. Wrong-result queries should not be treated the same as successful slow
queries.

### Timeouts and Unsupported Queries

A public benchmark needs a first-class result status. This also makes it easier
to include row stores and experimental engines without hand-editing failed
queries out of the report.

Suggested statuses:

- `ok`
- `timeout`
- `unsupported`
- `wrong_result`
- `error`
- `skipped`

### Client Timing

Keep timing through `fetch(...)`, but document that it includes client round
trip, result transfer, and Polars materialization. This is reasonable for
end-to-end user-perceived latency, but it is not the same as server-only
execution time.

### Local vs Public Runs

Treat local Apple Silicon Docker results as development data. For public results,
prefer a stable x86_64 Linux host with pinned images, fixed CPU governor, enough
RAM to avoid incidental swapping, and explicit cache-control rules.

## Suggested Implementation Sequence

1. Extend the results schema/reporting for query status, iteration role, engine
   execution mode, Docker platform, and hardware metadata.
2. Add correctness hashes for ClickBench, TPC-H, TPC-DS, and RTABench where
   practical.
3. Bump DuckDB, QuestDB, TimescaleDB, ClickHouse, and public-run StarRocks pins.
4. Align RTABench query coverage with upstream's 33-query set.
5. Add JSONBench at 10M rows for ClickHouse, DuckDB, PostgreSQL, and StarRocks.
6. Add Polars as an in-process LazyFrame engine.
7. Add Doris for ClickBench and JSONBench.
8. Add JOB once result-status handling is in place.
9. Expand time-series coverage with TSBS and InfluxDB 3 only if time-series
   becomes a core benchmark category.

## Source Notes

Local project files inspected:

- `olap_benchmarks/settings.py`
- `olap_benchmarks/dbs/*/__init__.py`
- `olap_benchmarks/benchmark.py`
- `olap_benchmarks/suites/*`
- `pyproject.toml`
- `README.md`

External and upstream sources checked during synthesis:

- ClickBench: https://github.com/ClickHouse/ClickBench
- JSONBench: https://github.com/ClickHouse/JSONBench
- Polars PDS-H benchmark: https://pola.rs/posts/benchmarks/
- RTABench: https://github.com/timescale/rtabench
- JOB: https://github.com/gregrahn/join-order-benchmark
- JOB-Complex paper: https://arxiv.org/abs/2507.07471
- Fair Benchmarking Considered Difficult: https://mytherin.github.io/papers/2018-dbtest.pdf
- DuckDB 1.5.0 release notes: https://duckdb.org/2026/03/09/announcing-duckdb-150
- DuckDB PyPI releases: https://pypi.org/project/duckdb/
- PostgreSQL 19 beta announcement: https://www.postgresql.org/about/news/postgresql-19-beta-1-released-3313/
- Timescale/TigerData rebrand: https://www.tigerdata.com/blog/timescale-becomes-tigerdata
- TimescaleDB releases: https://github.com/timescale/timescaledb/releases
- CedarDB Community Edition: https://cedardb.com/docs/community_edition/
- Apache Doris: https://doris.apache.org/
- chDB: https://github.com/chdb-io/chdb
- Apache DataFusion: https://datafusion.apache.org/
- pg_duckdb: https://github.com/duckdb/pg_duckdb
- InfluxDB 3: https://www.influxdata.com/products/influxdb3/
- QuestDB releases: https://github.com/questdb/questdb/releases
- StarRocks releases: https://github.com/StarRocks/starrocks/releases

## Caveats

The recovered Claude workflow had 53 result records: one scope result, five
search results, 23 fetch results, and 24 verification verdicts. Three verdicts
refuted the same incorrect CedarDB claim; the corrected finding is that CedarDB
Community Edition is available without signup but is proprietary and capped at
64 GiB compressed data. The remaining claims were not all equally deep, so this
document prioritizes recommendations that were supported by both local code
inspection and current upstream sources.

Vendor-published benchmark claims should be used as workload-discovery signals,
not as proof of comparative performance. The project's own results should remain
the source of truth after methodology and version updates.
