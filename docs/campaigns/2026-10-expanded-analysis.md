# October 2026 campaign: performance interpretation

Analysis dated 2026-10-21, against the campaign merged on 2026-10-19 in commit
`1b4ea18`. The [campaign report](2026-10-expanded.md) documents execution and
correctness; this note interprets its measured performance. No new benchmarks
were run and no published results were changed.

## Judgment

For broad single-machine OLAP latency in this harness, my ordering is **DuckDB,
ClickHouse, StarRocks, MonetDB / Polars, QuestDB, Arc, PostgreSQL / TimescaleDB**.
MonetDB has broader implemented coverage and stronger relational results; Polars
has fewer supported suites but consistently completes them. Their relative
placement depends on whether breadth or performance on implemented workloads
matters more. PostgreSQL and TimescaleDB are effectively tied overall.

**Doris needs a cache-controlled rerun before assigning it an execution-speed
rank. TDengine has too little workload coverage for a general rank.** Neither
should be called slow because it has few registered suites.

These are judgments about the configured runs, not universal engine rankings.
The host is an M4 Pro with 48 GiB RAM. Server engines run in OrbStack; DuckDB and
Polars run in the native client process. The configured budget is ten CPUs and
24 GiB server/embedded memory, with a separate client guard. The measured boundary
includes driver, transfer and Polars materialization. One warmup precedes three
measured iterations; the query statistic is their median. This is warm,
single-client latency, not service throughput, cold-storage performance, or a
cluster comparison. The nominally similar budgets do not imply identical
whole-system resource accounting across these execution modes.

## What the published score actually says

The published asset contains 99 completed select cohorts, 5,206 planned query/cell
pairs and 4,776 pairs with measured `ok` results. The other 430 pairs are explicitly
skipped or unsupported. Counts include the Airbnb smoke suite here.

| Published rank | Engine      |  Score | Ranked suite/scale cells with a cohort |
| -------------- | ----------- | -----: | -------------------------------------: |
| 1              | DuckDB      |  2.189 |                                  12/12 |
| 2              | ClickHouse  |  2.426 |                                  12/12 |
| 3              | StarRocks   |  4.047 |                                  10/12 |
| 4              | MonetDB     |  5.686 |                                  11/12 |
| 5              | Doris       |  6.844 |                                   3/12 |
| 6              | Polars      |  8.017 |                                   6/12 |
| 7              | QuestDB     | 10.119 |                                   6/12 |
| 8              | Arc         | 12.638 |                                   7/12 |
| 9              | PostgreSQL  | 14.570 |                                  12/12 |
| 10             | TimescaleDB | 14.571 |                                  12/12 |
| 11             | TDengine    | 18.355 |                                   1/12 |

Lower is better. These scores are not direct runtime multipliers between engines.
The site smooths query latency with 10 ms and geometrically averages ratios to
the fastest engine per query. Missing queries receive at least a 10x penalty,
or twice the slowest measured ratio if greater. An entirely absent suite/scale
cell receives the worst participating engine's suite score. Thus an unimplemented
adapter affects the overall rank even without evidence that the engine is slow.

The actual homepage aggregates **12 suite/scale columns**, not nine distinct suite
families: core time series, chat and TPC-H each contribute two scales. Airbnb is
excluded. RTABench also combines 31 raw queries with ten precomputed-aggregate
queries that most engines do not implement.

Sensitivity checks preserve DuckDB, ClickHouse and StarRocks as the top three:

- Removing Doris and recomputing every baseline gives scores 1.673, 1.854 and
  3.093 respectively; the remaining broad ordering is unchanged.
- Averaging scales within each suite first, then the nine suites equally, still
  puts those three first, but changes some middle positions and the order of the
  PostgreSQL pair.
- Removing the 10 ms smoothing also preserves those three positions. Raw ratios
  emphasize the large relative differences among sub-millisecond queries.

These checks establish stability under these scoring choices, not statistical
confidence intervals. Three repeats in one cohort do not establish run-to-run
uncertainty. A single overall score should be accompanied by coverage and
workload-specific rankings.

## Evidence behind the interpretation

The following are **sums of per-query measured medians in seconds**, over complete
cohorts. They describe running every query once at its median latency; they are
not operation wall times or geometric-mean scores. Dashes indicate no complete
cohort, not zero runtime.

| Workload                  | DuckDB | ClickHouse | StarRocks | MonetDB | Polars |
| ------------------------- | -----: | ---------: | --------: | ------: | -----: |
| TPC-H SF10, 22 queries    |   1.65 |       5.14 |      2.74 |    2.97 |   3.97 |
| TPC-H SF50, 22 queries    |   8.16 |      25.56 |     17.03 |       — |  22.58 |
| TPC-DS SF1, 99 queries    |   1.22 |       4.89 |      5.90 |    2.59 |      — |
| ClickBench, 43 queries    |  11.10 |      10.99 |     23.67 |       — |  20.52 |
| JSONBench SF10, 5 queries |   3.54 |       0.21 |      0.30 |       — |   2.84 |

**DuckDB exceeds my expectations for breadth.** It wins 67/99 TPC-DS queries,
13/22 TPC-H SF10 queries, 14/22 TPC-H SF50 queries and 103/120 range queries
against all measured engines. Against ClickHouse specifically it wins 35/43
ClickBench queries even though their total times are almost identical. Its
advantage is not confined to tiny point queries: it also leads the complete
TPC-H SF50 workload. Native in-process execution and efficient materialization
are part of that advantage. JSONBench is its conspicuous weak spot.

**ClickHouse is the most convincing broad server performer.** It measures all
634 planned non-smoke query/cell pairs, including every RTABench aggregate query.
It leads both chat scales and, setting Doris aside, every JSONBench query.
On matching queries, DuckDB's geometric-mean latency is about 20.3 times
ClickHouse's on JSONBench. However, this compares physical designs too:
ClickHouse explicitly stores important JSON paths as typed subcolumns and sorts
by JSON fields, whereas DuckDB stores JSON objects and extracts fields at query
time. This is useful evidence for these configurations, not proof of a universal
JSON-processing ratio. ClickHouse's TPC-H performance is less dominant than I
would have expected of a broad OLAP leader: DuckDB, StarRocks, MonetDB and Polars
all beat it on most SF10 queries.

**StarRocks is a positive result for join-heavy analytics.** It beats ClickHouse
on 18/22 TPC-H queries at both scales; its matched-query geometric-mean latency
is 0.46x ClickHouse at SF10 and 0.44x at SF50. It completes all 22 SF50 queries.
Its weakness here is small-query overhead, very wide expressions and export:
`wide5000_12_all_metrics` takes 34.34 s versus DuckDB's 0.50 s and Polars' 0.37 s.
Core time-series batch export times out at both scales. This is a capable
analytical engine with uneven performance, rather than uniformly third-fastest.

**MonetDB is the biggest mixed result.** Its TPC-DS performance is excellent:
31 query wins and the second-best suite score. Its TPC-H SF10 and ordinary
time-series results are also strong. In contrast, its 39 completed ClickBench
queries take 93.11 s in total, while DuckDB and ClickHouse each finish all 43 in
about 11 s. Chat SF1 totals 35.69 s against DuckDB's 1.95 s and ClickHouse's
1.09 s; SF10 only retains seven primary queries. Very wide aggregates and
rolling-AVG exclusions also hurt. Its incomplete TPC-H SF50 load was stopped by
the disk reserve, so that partial is a capacity/loading limitation under this
budget, not a measurement of slow execution of the missing joins.

**Polars is better than sixth place suggests.** It completes all 324 query/cell
pairs across its six ranked cells. It beats ClickHouse on 20/22 TPC-H SF10 and
19/22 SF50 queries. It performs well on metrics/ranges, including the 5,000-column
aggregate above. The overall score heavily reflects absent suite implementations.
These results make it a strong analytical execution option, without measuring
database-server capabilities it does not provide.

**QuestDB is less impressive than I expected on these time-series reads.**
It completes all core time-series, metrics and ranges queries, including core
SF10. But DuckDB beats it on all 62 core queries at each scale and all 120 range
queries. On matching queries, the unsmoothed geometric-mean gaps are 4.84x,
4.35x and 6.96x respectively. Some absolute differences are only milliseconds.
RTABench's join-heavy workload is much worse: the matched-query geometric mean
over its 28 primary queries is 48x DuckDB's. Its ingestion, latest-per-device and
streaming strengths are not comprehensively tested by this select leaderboard.

**Arc underperforms as a low-latency way to run these workloads.** Its use of an
embedded DuckDB does not yield the standalone DuckDB timings: matched-query
geometric means are about 4.1x slower on ClickBench, 7.9x on ranges and 31.6x on
core SF1. Core SF10 has incomplete loading coverage. HTTP/Arrow service overhead,
Parquet layout and planning are plausible contributors, but this dataset does
not isolate their causal shares. Arc can still be competitive on individual wide
metrics queries; it is not uniformly slow.

**PostgreSQL and TimescaleDB are the weakest broad analytical performers here.**
TPC-DS SF1 retains 98/99 queries for each, yet their totals are 151.03 s and
142.27 s versus DuckDB's 1.22 s for all 99. Large-load and timeout limits further
restrict coverage. That does not surprise me for PostgreSQL; Timescale's failure
to separate clearly on the headline result is more disappointing, but the
metrics and ranges adapters use ordinary rowstores, not Timescale hypertables.
Core time series does use hypertables and compression, so the caveat must not be
generalized to every Timescale result. On the 31 raw RTABench queries, Timescale's
matched geometric-mean latency is 0.44x PostgreSQL's, a real positive pocket.

**TDengine is unranked overall.** It completes all 120 range queries, with a
smoothed suite score of 2.21 versus DuckDB's 1.05, ClickHouse's 1.41, QuestDB's
2.12 and Arc's 2.20. That is a middle-of-pack result on one workload, not evidence
that it is the worst engine in general.

## Doris: apparent wins need a different interpretation

Doris wins 34/43 ClickBench queries and all five JSONBench queries on the measured
repeats. But the timings strongly indicate SQL-result-cache reuse:

- ClickBench run 416, Q33: warmup **4,335.456 ms**; measured iterations
  **4.067, 2.616, 1.910 ms**.
- Across its 42 completed ClickBench queries, the geometric mean of
  warmup/median ratios is **160.5x**; 40/42 drop by more than 10x.
- The corresponding geometric means are **152.5x** for JSONBench and **61.0x**
  for raw RTABench.

The connector does not explicitly disable SQL-result caching on its connections.
The [Doris 4.0 release notes](https://doris.apache.org/releases/v4.0/release-4.0.0/)
say SQL cache became enabled by default, and
[upstream branch-4.1 source](https://github.com/apache/doris/blob/branch-4.1/fe/fe-core/src/main/java/org/apache/doris/qe/SessionVariable.java)
sets `enableSqlCache = true`. The separate `enableQueryCache` flag is false in
that source. The SQL-cache manual's default description conflicts with the
release notes/source, so default assumptions should not substitute for recording
the actual running server's session values.

This is **strong evidence, not a captured cache-hit profile or a controlled
cache-off reproduction**. No live server was started for this analysis. The
warmup cannot simply replace the measured samples: it has a different execution
state and only one observation. Correct answers do not resolve the execution
comparability issue. Cached repeat latency is useful, but it belongs in a
separately labelled profile. Existing published evidence has been left intact.

## Additions that would improve fairness most

1. **Larger TPC-DS plus concurrent analytical streams.** Extend the existing
   SF1 suite to SF10 and SF100 where storage permits; use declared memory tiers
   and 1/4/16 clients with varied parameters and query order. Report throughput,
   p50/p95/p99 latency, failures and spill. This tests large joins, windows,
   memory contention and server scheduling that the current fast SF1 reads
   largely miss. TPC decision-support benchmarks already include multi-user
   throughput concepts; this harness should remain labelled TPC-derived, not
   claim official compliance. [TPC-H scope](https://www.tpc.org/tpch/),
   [TPC-DS](https://www.tpc.org/tpcds/).
2. **Join Order Benchmark (JOB).** Add the real-data IMDb join workload to expose
   optimizer behavior under correlated predicates and complex joins, complementing
   synthetic TPC data. Keep common statistics/index rules and preserve query
   semantics. [Original query repository](https://github.com/gregrahn/join-order-benchmark).
3. **A native telemetry suite based on TSBS.** Use many devices/hosts with modest
   metric width, tag filters, last point per device, downsampling, sparse/missing
   observations and late arrivals. Measure analytical reads during sustained
   ingestion, including ingest-to-query freshness and tail latency. Allow native
   hypertables, partitioning and compression with documented tuning budgets.
   TSBS supplies host-oriented aggregation and last-point workloads; freshness
   and late-arrival tests would be explicit harness extensions. This gives the
   time-series specialists a more representative test than column-width stress
   alone. [TSBS](https://github.com/timescale/tsbs).
4. **Star Schema Benchmark (SSB).** Add dimensional joins and aggregation with
   several scales/selectivities. Keep the canonical star schema as one track;
   an equivalent flattened table can be a separately scored physical-design
   experiment. This fills the gap between single-table ClickBench and complex
   TPC queries. [Original specification](https://www.cs.umb.edu/~poneil/StarSchemaB.PDF).
5. **A common Parquet analytics track.** Use identical files, row groups, codecs
   and partitioning, and add high-cardinality group-bys and joins inspired by
   [db-benchmark](https://github.com/duckdblabs/db-benchmark). Test both small
   aggregate results and large exports, with hot and cold runs separated.
   Keep loaded-native-table results as their own track. This helps distinguish
   execution performance from ingest/layout advantages for Polars and Arc.

If only three additions fit, prioritize larger/concurrent TPC-DS, JOB and native
telemetry. Add SSB next. More variants of the existing wide-column suites would
add less new evidence.

Before extending the leaderboard, explicitly control and record result caches
on every engine, starting with Doris, and rerun its affected suites. Keep OS/buffer
caches warm in the hot profile; disabling result caching need not make it a cold
test. Separate raw queries, precomputed aggregates and cached repeats. Add paired
raw-JSON and typed/extracted-column tracks with preprocessing/storage costs.
Vary query parameters while retaining deterministic expected answers.

Balance a broad leaderboard by workload family before scale, schema shape and
query template, so adding more widths/scales does not silently increase a
family's weight. Report shared-query speed and completion/implementation coverage
separately. Preserve the 45 s / 900 s bounded-budget findings as censored outcomes;
use a separate longer-budget tier if testing eventual completion. Repeat cohorts
in randomized engine order, record actual session settings and resource use, and
add an isolated Linux server run with comparable client/engine limits before
generalizing this laptop ranking to deployment choices. Existing populate and
resource measurements should become visible alongside query latency.

## Reproduction and audit limits

The analysis uses the published `results.db`, whose SHA-256 matches the committed
manifest:
`013b198a4e7fb89a5a5afddde94977460f57b3bfa5adbc03f44e468a69945dc2`.
It has 244 operation records. The raw revision has 496, including failed and
invalidated diagnostics; those were not mixed into primary timings.

Run the companion SQL **against this published asset**, not the unfiltered raw
revision. From a checkout with its venv and downloaded asset:

```sh
OLAP_BENCHMARKS_RESULTS_DIRECTORY="$PWD/site/public/data" \
  ./.venv/bin/olap results query --revision results \
  "$(cat docs/campaigns/2026-10-expanded-scores.sql)"
```

The SQL reproduces the displayed overall scores using the full recorded plans.
For this publication their query-name sets match the query manifest. Setting
`include_doris` to `false` recalculates the exclusion sensitivity check.

Per-query medians come from `run_step.metadata.duration_ms` for measured
iterations with `status='completed'` and `result_status='ok'`, not timestamp
differences or `query_execution` spans. Every published planned query has three
measured-role rows. Independent SQL and in-memory calculations were compared.

One documentation discrepancy: the campaign narrative says PostgreSQL ClickBench
has five primary queries, but published cohort **397 has four** measured queries:
Q25, Q27, Q38 and Q39. This analysis uses the actual asset. Additional validated
observations in earlier/failed attempts remain useful diagnostic evidence, not
substitutes for these primary cohorts. The campaign also documents peer-hash
and oversized-answer exceptions, so `ok` does not imply an independent hash
comparison for every answer.
