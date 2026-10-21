# DuckDB 2 storage comparison

This comparison was requested after the October campaign correction. The Python
benchmark and results tools now target DuckDB 2.0.0. Native Node tooling targets
`@duckdb/node-api` 2.0.0-r.1. The browser WASM package has a separate release
schedule; no published bundle embedding final DuckDB 2.0 was verified on October
21. The site export deliberately targets storage compatibility `v1.4.0` while
benchmark stores use fresh 2.0 files. Historical runs retain their actual engine
versions. Integration and browser validation precede performance measurements.

No storage winner has been measured yet. The following decision rules are fixed
before the first comparison, building on the [tuning protocol](2026-10-tuning-protocol.md).

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
