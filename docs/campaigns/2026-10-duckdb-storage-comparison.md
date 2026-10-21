# DuckDB 2 storage comparison

This comparison was requested after the October campaign correction. The Python
benchmark and results tools now target DuckDB 2.0.0. Native Node tooling targets
`@duckdb/node-api` 2.0.0-r.1. The browser WASM package has a separate release
schedule; no published bundle embedding final DuckDB 2.0 was verified on October
21. The site export deliberately targets storage compatibility `v1.4.0` while
benchmark stores use fresh 2.0 files. Historical runs retain their actual engine
versions. Integration and browser validation precede performance measurements.

The initial comparison is complete. The decision rules below were committed
before measurement; results and remaining correctness limits follow them. See
also the [tuning protocol](2026-10-tuning-protocol.md).

## Scope and execution

Both alternatives use DuckDB 2.0.0 and the same SQL and fetch boundary. Parquet
means DuckDB querying files directly, with explicit casts to the native schema.
It does not mean Polars execution. There is no sorting, partitioning,
preaggregation, typed-JSON substitution or query rewrite between alternatives.
JSONBench's Parquet conversion retains raw repaired JSON text and is charged to
population. Every required external file counts toward the Parquet footprint.

| Suite | Scales | Planned queries per scale | Parquet eligibility |
| --- | --- | ---: | --- |
| Time Series | 1, 10 | 62 | SELECT control; native required for writes |
| Metrics | 1 | 112 | Full suite |
| Ranges | 1 | 120 | Full suite |
| Chat Threads | 1, 10 | 23 | SELECT control; native required for writes |
| RTABench | 1 | 41 | Full suite; existing 10 unsupported aggregates remain |
| Airbnb | 1 | 5 | Full suite |
| TPC-H | 10, 50 | 22 | Full suite |
| TPC-DS | 1 | 99 | Full suite |
| JSONBench | 10 | 5 | Full suite, subject to raw JSON correctness |
| ClickBench | 1 | 43 | Full suite, subject to all canonical column casts |

The 13 scale cells contain 639 planned query positions, including ten declared
unsupported RTABench queries. Each alternative gets fresh population and a full
SELECT cohort. The initial order alternates native/Parquet and Parquet/native
across cells in table order, with scales ascending. Each query has one warmup
and three measured iterations under the existing resident-hot profile.

Only one benchmark operation runs at a time. Both alternatives use ten regular
compute threads, ten async I/O threads, memory-governed read-ahead (`-1`), a
24 GiB engine limit, 28 GiB client RSS guard, 40 GiB free-space reserve,
45-second query limit and 900-second operation limit. These thread pools are
recorded separately; ten regular workers do not imply ten total process threads.
Effective settings, engine/build identity, source files and physical layout must
be recorded. No tests, builds, installations or other engine work overlap
measurements. Reopening the engine does not make the OS page cache cold.

## Correctness and selection

Storage readiness requires the requested mode, canonical schema and complete
preparation, as well as expected row counts. A matching count alone cannot
certify an old or unfinished store. Fixtures cover strict casts, NULLs, ordered
types, JSON strings and supported mutations. Full runs require stable answers
and matching counterpart hashes and row counts. ClickBench Q28 is compared
between these two implementations despite its existing cross-engine exclusion.
An unavailable hash is unknown, not a match; large export controls must disclose
that limitation. No incomplete correctness evidence can establish a new winner.

Original operation support, correctness, resource compliance and supported-query
coverage take precedence over speed. Time Series and Chat Threads retain native
storage for their writable contracts; their Parquet SELECT results are reported
separately. Failed, missing and unstarted queries stay in the coverage denominator.

For complete eligible alternatives, calculate the geometric mean of each
query's Parquet/native median latency ratio, giving queries equal weight within
each scale and scales equal weight within a suite. A latency winner needs at
least a 5% difference and the same direction beyond that threshold in all three
aggregate measured-iteration comparisons. This is a repeatability rule, not a
statistical confidence interval.

If latency is inconclusive, prefer lower required disk, then lower worst observed
SELECT RSS, then lower total population time, each requiring at least a 5%
difference and comparable observed evidence. Otherwise retain native. Report
that decision as a resource tradeoff, without calling it a latency win. Opposite
repeatable scale winners permit one reversed-order confirmation of both complete
cohorts at every scale of that suite. Use all scheduled samples; inconsistent
confirmation remains inconclusive. No per-query storage choices are made.

Report ingestion cost, latency, coverage, sampled RSS, required storage and
temporary-space observations separately. Missing short-query samples remain
NULL. Independent memory peaks are not added, whole-store changes are not spill
measurements, and retained source files are distinguished from files required
to execute queries. Local resident-hot measurements do not establish remote
object-store performance.

Comparison revisions are separate from final campaign records. After selecting
and committing defaults, rerun all thirteen full campaign cells, including the
original native mutation/concurrency operations for the writable suites.
Supersede old completed cohorts only after validated replacements; preserve
meaningful failure evidence.

## Initial comparison results

All 52 populate/SELECT operations completed serially at commit `6778832`. Both
variants ran all 629 executable query positions with one warmup and three measured
iterations: 5,032 successful fetches. The ten RTABench unsupported queries stayed
explicit. Across all four iterations, 625 positions matched canonical SQL, exact
output schemas, dimensions and canonical-v11 answer hashes. ClickBench Q28 matched.

Native storage is retained for every fully certified read-only suite below.
Ratios are equal-query geometric means of Parquet/native median latency, with
equal weighting of TPC-H scales. They are not total suite-duration ratios. All
three corresponding measured-iteration aggregates exceeded the 5% threshold in
the same direction, so no reversal confirmation was indicated.

| Suite | Parquet/native latency | Selected storage |
| --- | ---: | --- |
| time_series_metrics | 9.081× | Native |
| time_series_ranges | 9.954× | Native |
| rtabench | 3.458× | Native |
| tpc_h | 2.878× | Native |
| tpc_ds | 2.431× | Native |
| jsonbench | 1.308× | Native |
| clickbench | 4.147× | Native |

Time Series and Chat Threads retain native storage for their writable contracts.
All Chat SELECT controls also passed the full equality gates. Time Series batch
exports at both scales exceeded the existing 5M-cell hash cap (172,039,680 and
328,538,880 cells), so those SELECT controls remain uncertified. Airbnb
`02_join_one_table` and `03_join_two_tables` also exceed the cap (39,578,392 and
84,810,840 cells); the initial comparison alone cannot certify a storage winner
for that suite. Counts and schemas are not substitutes for full answer equality.

### Supplementary Airbnb output validation

A separate serial diagnostic rebuilt both layouts from the same input
fingerprint and ran all five unchanged queries, with one warmup and three
controls per layout: 40 full fetches. Both large joins returned 5,654,056 rows.
Exact output schemas, values, NULLs and multiplicity matched. Q02 orders only by
`listing_id`; raw order within ties varied across repeats in both layouts. The
comparison verified that ordering and unique, non-NULL `(listing_id, date)`
pairs, then normalized only within equal-`listing_id` groups by date. Q03's
complete ordering was compared directly. Ten full Arrow answers are retained.

Native is conservatively retained for Airbnb, completing the native selection
for all ten suites. Using every original timing, its equal-query advantage is
1.305×; all three original aggregate comparisons agree (1.257×, 1.286×, 1.312×).
The very short count query receives the same predeclared weight as each join.
Parquet instead has a slightly lower sum of query medians, 4.037 versus 4.101
seconds, and about half the required storage. Those are useful tradeoffs, not
contradictory measurements of the same score.

The new proof does not recover the original missing hashes: the four historical
query/layout positions, comprising sixteen fetch hashes, remain unknown. The
initial comparison is not retrospectively certified by the later diagnostic,
and its timings are not replaced. Evidence is retained under
`data/duckdb2-upgrade/airbnb-full-answer/`, including `complete.json`,
`original-timing-summary.json`, `summary.json` and the Arrow answers. Both
diagnostic stores were removed after validation; final campaign reruns remain
required.

### Resource and loading tradeoffs

Each pair below lists **native / Parquet**. Loading is full recorded population
wall time, including readiness work and JSONBench conversion where applicable.
RSS is the sampled peak of the SELECT client process, including retained engine
allocations and materialized results. Required storage includes external Parquet
files exactly once; original input files retained beside a native import are not
query-required native storage. GiB uses 2³⁰ bytes.

| Suite | SF | Population seconds N/P | SELECT RSS GiB N/P | Required storage GiB N/P |
| --- | ---: | ---: | ---: | ---: |
| time_series | 1 | 12.14 / 1.08 | 3.31 / 4.37 | 1.33 / 1.03 |
| time_series | 10 | 92.06 / 11.99 | 6.92 / 8.27 | 10.49 / 10.45 |
| time_series_metrics | 1 | 9.75 / 8.58 | 7.22 / 12.39 | 1.52 / 1.39 |
| time_series_ranges | 1 | 6.41 / 5.58 | 1.19 / 0.95 | 0.95 / 0.88 |
| chat_threads | 1 | 1.40 / 0.23 | 3.52 / 3.04 | 0.84 / 0.32 |
| chat_threads | 10 | 6.21 / 0.93 | 17.04 / 10.53 | 7.45 / 2.67 |
| rtabench | 1 | 45.60 / 1.59 | 2.88 / 1.06 | 8.47 / 3.77 |
| kaggle_airbnb | 1 | 1.74 / 0.18 | 4.86 / 4.89 | 0.13 / 0.06 |
| tpc_h | 10 | 16.95 / 0.46 | 3.02 / 2.06 | 2.67 / 3.65 |
| tpc_h | 50 | 63.09 / 1.13 | 13.27 / 6.11 | 13.77 / 19.04 |
| tpc_ds | 1 | 3.28 / 1.00 | 1.11 / 1.05 | 0.33 / 0.25 |
| jsonbench | 10 | 29.00 / 15.06 | 4.14 / 4.72 | 2.70 / 1.96 |
| clickbench | 1 | 122.80 / 0.45 | 17.14 / 8.57 | 18.06 / 13.77 |

Native wins latency while Parquet often reduces loading time, disk, or RSS. For
example, ClickBench native is 4.147× faster under equal-query weighting, but its
SELECT RSS is about twice Parquet’s and required storage is larger. TPC-H native
is faster and smaller on disk, while Parquet uses less SELECT RSS. These are
separate tradeoffs; a latency choice does not establish a memory-efficiency win.

All operations stayed within the configured guards. The largest sampled
individual-operation RSS was 21,229 MiB; the smallest observed boundary reserve
was 142,053,228,544 bytes. Sampling can miss short peaks. Temporary/spill boundary
observations do not establish peak temporary use, and no remote-I/O conclusion
is supported by this local resident-hot experiment. Native shutdown emitted
nanobind reference warnings, but processes exited with code zero and absence
was verified; the warnings alone do not establish a retained-memory leak.

### Evidence and final cohort separation

The separate revisions are `duckdb2-ab-storage-ab-20261021-native-r1` and
`duckdb2-ab-storage-ab-20261021-parquet-r1`. Each contains populate/select pairs
1/2 through 25/26 in the matrix order above. These selection experiments do not
replace the final campaign records. Local retained evidence is indexed by
`data/duckdb2-upgrade/comparison/evidence-index-initial-completed.json`;
`analysis-initial-completed.json` retains every query’s timings and equality
status. The runner hashes, plan, inputs, physical manifests, effective runtime
settings, strict fixture proof, and worker-release observations are retained
there. Successful temporary stores were removed; input files and result
revisions remain. Fresh final campaign measurements are still required.
