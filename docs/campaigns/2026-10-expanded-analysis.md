# October 2026 campaign: corrected performance interpretation

Updated 2026-10-22 after the configuration audit, bounded AI tuning and serial
replacement runs. This analysis uses the corrected selected campaign, not the
original October 19 publication or exploratory timings. The
[correction report](2026-10-configuration-correction.md) records changes and
failures; the [78-pair audit ledger](2026-10-audit-coverage.md) states review depth
and deferrals.

## Ranking and judgment

**DuckDB is the strongest broad single-machine OLAP performer here; ClickHouse
is the strongest broad server performer.** StarRocks, MonetDB and Polars form
an interesting next group: StarRocks handles large joins well, MonetDB excels
on many relational queries but has severe outliers, and Polars is competitive
on its smaller implemented workload set. The remaining order depends heavily
on workload and coverage.

The historical coverage-adjusted score gives this ordering. Lower is better; the numbers
are scores, not direct runtime multipliers between engines. Airbnb is excluded
from the ranking but included in campaign validation.

The subsequent `fixed-workloads-v1` publication separates performance eligibility,
coverage and correctness evidence using the same recorded measurements. Its
checksum-bound `comparison.json` freezes the comparison query lists and scales,
separates raw RTABench from prepared aggregates, and excludes ClickBench Q28 from
comparable performance while retaining its coverage position. This is a
retrospective policy; the table below preserves the historical score and must not
be read as the new family-ranking policy. Neither the policy change nor its
metadata export requires rerunning the 99 SELECT combinations.

| Rank | Engine | Score | Ranked cells with a cohort | Measured queries |
| ---: | --- | ---: | ---: | ---: |
| 1 | DuckDB | 1.666 | 12/12 | 624 |
| 2 | ClickHouse | 1.917 | 12/12 | 634 |
| 3 | StarRocks | 3.215 | 10/12 | 576 |
| 4 | MonetDB | 4.391 | 11/12 | 582 |
| 5 | Polars | 6.301 | 6/12 | 324 |
| 6 | QuestDB | 7.942 | 6/12 | 427 |
| 7 | Arc | 9.920 | 7/12 | 422 |
| 8 | TimescaleDB | 10.090 | 12/12 | 487 |
| 9 | Doris | 10.877 | 3/12 | 78 |
| 10 | PostgreSQL | 11.626 | 12/12 | 481 |
| 11 | TDengine | 14.605 | 1/12 | 120 |

The score adds 10 ms to query times, compares each with the fastest engine,
and geometrically averages ratios. Missing queries receive at least a 10×
penalty, or twice the slowest measured ratio if larger. Entirely absent cells
receive the worst participating engine's cell score. Consequently **TDengine's
last place is mostly a coverage penalty**, not evidence that it is the slowest
database. Doris's three cells and Polars's six also limit broad conclusions.

The headline averages 12 suite/scale cells, representing nine workload families.
Giving the nine families equal weight after averaging scales keeps DuckDB,
ClickHouse, StarRocks, MonetDB and Polars in that order, but moves Doris ahead
of TimescaleDB. Removing 10 ms smoothing keeps DuckDB and ClickHouse first and
second, while MonetDB narrowly passes StarRocks: 6.490 versus 6.531. Thus the
first two positions are robust to these checks; the middle order is less so.
These are sensitivity checks, not confidence intervals from independent cohorts.

The experiment measures warm, single-client, end-to-end fetch latency on an M4
Pro with 48 GiB RAM. Server engines run in OrbStack; DuckDB and Polars run in
the native client process. Fetch includes driver, transfer and full Polars
materialization. One warmup precedes three measured iterations, summarized by
their median. All live experiments and reruns were serial. Internal parallelism
and the separately defined concurrent workload retain their declared behavior.

## What exceeds or falls short of expectations

These are sums of per-query medians in seconds, not operation wall times.
Only complete cohorts appear in this comparison; partial workloads are discussed
separately.

| Workload | DuckDB | ClickHouse | StarRocks | MonetDB | Polars |
| --- | ---: | ---: | ---: | ---: | ---: |
| TPC-H SF10, 22 queries | 1.30 | 4.83 | 2.74 | 2.97 | 3.65 |
| TPC-H SF50, 22 queries | 6.34 | 25.02 | 17.03 | — | 21.14 |
| TPC-DS SF1, 99 queries | 1.11 | 4.89 | 5.90 | 2.59 | — |
| ClickBench, 43 queries | 10.22 | 10.99 | 23.67 | — | 20.42 |
| JSONBench SF10, 5 queries | 3.52 | 0.21 | 0.30 | — | 2.77 |

**DuckDB exceeds expectations for breadth.** The final 2.0 cohorts win 66/99
TPC-DS queries and 15/22 and 17/22 TPC-H queries at SF10/SF50 against all measured
engines. It beats ClickHouse on all 22 TPC-H queries at both scales and on 35/43
ClickBench queries. JSON extraction remains a conspicuous weakness: matched-query
geometric-mean latency is 21.54× ClickHouse's on JSONBench. Their physical designs
differ: ClickHouse uses typed JSON subcolumns and sorting, while DuckDB extracts
from stored JSON at query time. The result is about those configurations.

DuckDB 2 was tested on native storage and Parquet across every registered cell.
Native remains selected for all ten suites. Read-only suite comparisons favored
native, while core time series and chat also require its write contract. Airbnb
was close and metric-sensitive: native won the equal-query geometric mean while
Parquet used less storage and had a slightly lower summed latency. The
[storage report](2026-10-duckdb-storage-comparison.md) gives all ratios, load costs,
resource observations and large-output correctness limits. These local warm
reads do not isolate the causal benefit of async I/O or predict remote-storage
behavior. Provenance records ten compute threads, ten async threads and automatic
read-ahead; ten CPUs does not mean ten total engine threads.

**ClickHouse remains the most convincing broad server result.** It measures all
634 ranked query positions, including RTABench's prepared aggregates, and wins
all five JSONBench queries. It wins 12 of 23 chat queries at each scale; DuckDB
wins the other eleven. SF10 chat's total favors DuckDB despite ClickHouse winning
slightly more queries and the matched geometric mean. TPC-H remains weaker than
expected even after the proven Q15 rewrite: StarRocks beats it on 18/22 queries
at each scale, and Polars on 21/22 and 20/22.

**StarRocks is strong on larger relational analytics.** It completes every
TPC-H SF50 query and wins five against all measured engines. Its TPC-H matched
geometric-mean latencies are 0.495×/0.445× ClickHouse's at SF10/SF50. Small-query
overhead, wide expressions and exports remain uneven; core batch exports time
out. Its headline third place should not suggest uniformly third-fastest reads.

**MonetDB is both a positive surprise and the largest mixed result.** It wins
32 TPC-DS queries and has the second-smallest complete suite total. Q5 tuning
reduced its final ClickBench distinct-string count to about 2.20 seconds; Q08
filter placement brought chat user keyword search to about 97/101 ms at SF1/SF10.
Those fixes do not remove its broader weaknesses: 40 ClickBench queries still
total 89.54 seconds, versus DuckDB's 10.22 for all 43. Chat SF1 totals 31.35 seconds;
SF10 completes 20/23, with a Q15 crash and independent Q16/Q17 timeouts retained.
Q16 and Q17 candidate rewrites were rejected after controlled measurements.
Its five-query TPC-H SF50 partial follows a disk-reserve failure during loading;
missing joins are a capacity outcome, not measured slow query times.

**Polars performs better than its coverage-weighted rank suggests.** All 324
queries in its six ranked cells complete. It is competitive on wide metrics
and TPC-H, with Q17's validated semijoin reducing final medians to 149 ms at SF10
and 722 ms at SF50. It lacks several suite implementations and database-server
contracts. Its required Parquet input files are now included in storage totals;
a small reference file is not the entire database footprint.

**TimescaleDB's corrected telemetry is materially better than the original
result suggested.** On matching PostgreSQL queries, its geometric-mean latency
is 0.225× for metrics and 0.266× for ranges, approximately 4.45× and 3.75× better.
The selected scope remains 82/112 and 90/120 queries. Seven-day hypertables and
compressed EAV tables now have physical readiness proof. `wide1000` cannot be
compressed under the observed tuple-size limit and remains an explicit rowstore
hypertable. The same live limit justifies core's EAV layout; a comment records
it for future audits. This improvement does not generalize to relational TPC
rowstores, chat, or the incompletely prepared ClickBench store. PostgreSQL and
TimescaleDB still have weak broad analytical latency and capacity outcomes here.
The TPC-DS Q11 proposals remain inconclusive after original-query timeouts.

**QuestDB's configured reads disappoint, but the missing native layout matters.**
DuckDB beats it on all 62 core queries at both scales and all 120 range queries.
Matched geometric-mean gaps are 4.38×, 4.16× and 4.78× respectively. A designated
timestamp/DAY/WAL proposal demonstrated interval scans on tiny nonempty fixtures,
but failed correctness: empty `MAX(time)` returned zero rows instead of one NULL.
It was not adopted. These rankings describe the existing layout, not a fair proof
of QuestDB's best native performance. The [issue](../../issues/questdb/empty-designated-timestamp-max.md)
explains the limitation and the uncompleted gates.

**Arc underperforms as a low-latency route to these workloads.** Relative to
standalone DuckDB, matched geometric-mean gaps are 4.23× on ClickBench, 5.42× on
ranges and 28.61× on core SF1. Service overhead, layout and planning are plausible
contributors; their shares were not isolated. Its embedded DuckDB is also not
the newly upgraded standalone 2.0 configuration. Individual wide metrics remain
competitive.

**Doris's earlier apparent dominance was not comparable execution.** Controlled
Q33 diagnostics proved SQL-answer-cache reuse on the campaign's Python path.
Final cohorts disable SQL and aggregate-result caches, retain data/predicate
caches, and use a verified FE 6 GiB / BE 18 GiB split within the same total budget.
They complete 42/43 ClickBench queries, all five JSONBench queries and 31 raw
RTABench queries. Matched geometric-mean latency is 3.54×, 17.98× and 1.48×
ClickHouse respectively. Its 42 ClickBench query medians total 40.72 seconds.
That is substantially less competitive than the old cached repeats implied.
No causal cache claim is extended to other engines without evidence.

**TDengine remains too narrowly covered for a general judgment.** It completes
all 120 range queries, with a total of 3.87 seconds, versus QuestDB's 3.20,
ClickHouse's 0.93 and DuckDB's 0.54. Its ingestion and native telemetry strengths
need a more representative workload before a broad ranking is useful.

## Memory and storage change the interpretation

Latency alone is insufficient. For the four engines completing TPC-H SF50,
whole-SELECT sampled peaks and last observed required store size are:

| Engine | Sum of medians, s | Server charge, MiB | Server anon + shmem, MiB | Client RSS, MiB | Required store endpoint, MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| DuckDB | 6.34 | — | — | 13,556 | 14,096 |
| ClickHouse | 25.02 | 17,165 | 9,344 | 277 | 16,704 |
| StarRocks | 17.03 | 19,991 | 11,810 | 284 | 25,173 |
| Polars | 21.14 | — | — | 25,255 | 19,494 |

DuckDB combines strong latency and a smaller measured store here. Polars's
required Parquet storage and process footprint are substantial despite cheap
population. ClickHouse's store fell from a sampled 42,669 MiB maximum during
background activity to 16,704 MiB; endpoint size omits this transient footprint.
These columns are observations from distinct runs, not a causal memory experiment.

Server charge includes file cache and kernel charges; anon + shmem excludes
file-backed working sets such as MonetDB's mappings. Client RSS is the execution
process for embedded engines but only the client for servers. Independent maxima
must not be added. The nominal 24 GiB engine budget is not a common whole-job
limit: Polars has no enforced 24 GiB heap cap and uses the 28 GiB client guard.
A performance-per-GiB league table would imply equivalence these measurements
cannot establish.

The [query resource SQL](2026-10-query-resources.sql) keeps each client sampling
family's preceding interval separate and reports missing short-query observations
as NULL. In this export, 13,683 of 15,618 planned measured query iterations lack
a fully contained RSS interval, including skipped/unsupported positions. Do not
turn those gaps into zeros. [Operation resource SQL](2026-10-suite-resources.sql)
is more useful for complete-suite capacity. Store/WAL endpoints are not peak
spill; physical-design comparisons must include load, conversion, compaction,
external source files and temporary-space peaks.

## Additions that would improve fairness most

1. **Larger TPC-DS and concurrent analytical streams.** Add SF10/SF100 where
   storage permits, declared memory tiers and 1/4/16 clients. Report throughput,
   tail latency, failures and measured temporary-space peaks. This targets joins,
   windows and resource contention absent from a fast SF1 latency comparison.
   Keep it labelled TPC-derived, without claiming official compliance.
   [TPC-DS scope](https://www.tpc.org/tpcds/).
2. **Join Order Benchmark.** IMDb-based correlated predicates and multi-join
   queries would expose optimizer weaknesses beyond synthetic TPC data. Use
   declared statistics/index rules and preserve exact semantics.
   [Original JOB paper](https://www.vldb.org/pvldb/vol9/p204-leis.pdf).
3. **Native telemetry based on TSBS.** Many devices, moderate metric width,
   tag filters, last point per device and downsampling would better represent
   the specialists. Add late arrivals and analytical reads during ingestion as
   explicit harness extensions, measuring freshness and tail latency; TSBS does
   not itself supply that entire concurrent protocol.
   [TSBS](https://github.com/timescale/tsbs).
4. **Star Schema Benchmark.** Add dimensional aggregation and selective joins
   between single-table ClickBench and complex TPC workloads. Keep canonical
   star and equivalent flattened layouts in separately labelled tracks.
   [Original SSB specification](https://www.cs.umb.edu/~poneil/StarSchemaB.PDF).
5. **Common-file analytics and raw/typed JSON tracks.** Give engines identical
   Parquet files, codecs, row groups and partitions; separate warm local, cold
   local and remote-object-store tests. Compare native loads separately, charging
   conversion and storage. Likewise compare raw JSON with explicitly materialized
   typed fields instead of allowing that physical-design difference to disappear
   inside a single JSON score.

AI-assisted tuning is a reasonable benchmark capability. Keep a declared search
budget, publish accepted and rejected proposals, require exact correctness and
repeatable gains, and charge preparation and storage. Equal opportunity does not
mean every query has received equal attention; the coverage ledger exposes that
limit. Keep canonical and prepared-aggregate tracks separate. Balance families
before adding scales or widths, and report speed, completed coverage and resources
alongside the aggregate score. Repeat isolated cohorts and add a Linux host run
before generalizing this laptop result to deployment choices.

## Reproduction and retained evidence

The selected asset has 244 operations, 99 SELECT cohorts, 5,206 planned query
positions, 4,790 measured positions and 416 explicit skips/unsupported positions.
Its SHA-256 is
`6e12f959af14dfefafd8d8d8c8c75032640f70d610813f3c559ba8ff54446b22`,
with 116,404,224 bytes. It was written by Python DuckDB 2.0 in explicit v1.4
storage format for the browser. The pinned Wasm package embeds engine 1.5.4,
not 2.0; Node tooling and benchmark execution use 2.0.

The raw revision retains 324 operations after deleting 250 fully superseded
successes. All 86 failed operations remain, along with successful 314, 392 and
395 carrying additional queries absent from selected cohorts. The
[removal mapping](2026-10-superseded-runs.csv) records replacements. Historical
failure evidence and diagnostic timings are not mixed into selected medians.

Run the score SQL against the selected asset:

```sh
OLAP_BENCHMARKS_RESULTS_DIRECTORY="$PWD/site/public/data" \
  ./.venv/bin/olap results query --revision results \
  "$(cat docs/campaigns/2026-10-expanded-scores.sql)"
```

Per-query medians use `run_step.metadata.duration_ms` from measured-role `ok`
steps, not timestamp differences or inner `query_execution` spans. Independent
Python aggregation agrees with the SQL scores within 0.000001. Large-output
hash-cap unknowns and documented cross-engine comparison exceptions remain
explicit; an `ok` fetch is not proof of independent comparison for every cell.
Three measured iterations do not establish between-cohort uncertainty.
