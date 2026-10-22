# October publication follow-up protocol

The initial protocol was recorded before follow-up measurements. Ordering
amendments below explicitly record later observations and policy changes. The
protocol extends the existing macOS campaign with workloads and evidence; it
does not repeat the 99 original SELECT cells to implement the publication
policy. Linux replication and new database engines are outside this pass.

Execution modes remain those of the original campaign: server databases use
their pinned ARM64 Docker containers on the macOS host; DuckDB and Polars run
embedded. An engine execution check means exercising its actual SQL/loader,
not adding a macOS build or a separate operating-system comparison.

The original selected asset has SHA-256
`6e12f959af14dfefafd8d8d8c8c75032640f70d610813f3c559ba8ff54446b22`.
Its fixed-workload analysis is explicitly retrospective. Original measurements,
failed attempts, provenance and documented limitations remain retained.

## Fixed workloads and participants

| Workload | Scales | Query set | Existing engine candidates |
| --- | --- | --- | --- |
| Device Telemetry | 1, 10 | All 18 declared queries | DuckDB, ClickHouse, PostgreSQL, TimescaleDB |
| Join Order Benchmark | Original May 2013 snapshot | All 113 upstream queries | DuckDB, ClickHouse, MonetDB, PostgreSQL, StarRocks |
| TPC-DS | 10 | All 99 canonical queries | DuckDB, ClickHouse, MonetDB, PostgreSQL, TimescaleDB, StarRocks, Doris |
| Doris relational coverage | TPC-H 10, 50; TPC-DS 1 | All 22 / 99 queries | Doris |
| Common Parquet Analytics | TPC-H 10 | All 22 canonical queries over the same eight files | DuckDB, Polars, ClickHouse |
| JSON Representation | 10 | Five raw and five prepared variants | DuckDB, ClickHouse |

Candidate registration is implementation coverage, not evidence of successful
native execution. Run native fixture gates before full measurement; record any
failed gates or unavailable dialects explicitly. Canonical lists do not shrink
after a timeout or wrong answer. A failed or incomplete workload has coverage
and evidence, but no full-workload performance score.

JOB retains the pinned upstream queries and original archive. Dialect-only
overrides are recorded separately. It initially uses the documented primary-key
layouts without the optional upstream secondary foreign-key indexes. Telemetry
uses one reading per device/time identity, twelve nullable metrics, skewed
cadences and explicit metadata. Simulated arrival timestamps do not constitute
late-write or ingestion-freshness measurements.

Representation tracks are supplementary. They do not give TPC-H or JSON an
additional vote in any future overall score. Raw and prepared JSON are shown
separately. Preparation and combined-store disk costs are reported; shared disk
samples cannot attribute storage separately to the two JSON tables. Common-file
engines must count referenced Parquet bytes and retain the exact shared files.

## Measurement and correctness

Use the pinned engine versions, ten CPU threads, 24 GiB server/internal engine
budget, separate 28 GiB operation-process RSS guard, 40 GiB disk reserve,
45-second query limit and 900-second operation limit. These have different
enforcement scopes across container and embedded engines; they are not a uniform
whole-job memory cap. Do not claim a general memory-efficiency ranking.

Only the parent schedules live work. Start one engine and one benchmark group
at a time, including diagnostics. No other benchmark, test suite, build or data
preparation may compete with final measurements. Use one warmup and three full
materialized SELECT iterations, with answer hashing outside query latency.
Record preparation phases, input fingerprints, runtime settings and actual
physical-layout evidence. Retain timeouts, resource stops and unstarted queries
as their recorded outcomes. Do not substitute a fast subset for a failed suite.

Fixture gates compare every output value, NULL, duplicate and ordering against
an independent oracle where available. Full-data peer hashes supplement those
gates; they do not establish exact-schema equivalence or universal correctness.
Unresolved mismatches block the affected performance comparison. The existing
ClickBench Q28 semantic exclusion remains visible in coverage and query detail.

### Ordering-evidence amendment after the first SF10 cohort

The first DuckDB TPC-DS SF10 SELECT, run 589, completed all 99 queries but the
order-sensitive `canonical-v11` hashes varied for Q18, Q65 and Q71. Six full-data
diagnostic repetitions of each query retained identical row multisets. The
varying rows have equal declared ORDER BY keys: Q18's four projected dimensions,
Q65's store name/item description, and Q71's amount/brand ID/hour. The SQL does
not prescribe an order inside those ties. This is not evidence of a wrong
answer.

Before measuring the other SF10 engines, introduce a separate, versioned hash
policy for those three query identities. Normalize only the order within
consecutive equal ORDER BY key groups, retaining every row and duplicate, all
values, and the sequence of distinct key groups. Changed LIMIT-boundary row
membership must still disagree. Record the policy and key positions with the
answer evidence. Default hashes and all timed SQL remain unchanged; no query
is excluded from coverage or the performance set.

Run 589 remains retained as the initial diagnostic observation. Repeat the
complete DuckDB SF10 SELECT under the corrected evidence policy; the other
five completed DuckDB cells and the original 99 cells need no rerun for this
change. This amendment follows observed evidence and is not presented as part
of the original premeasurement declaration.

### Q44 amendment after the first ClickHouse SF10 cohort

The selected ClickHouse TPC-DS SF10 SELECT, run 604, subsequently exposed Q44
order variation. Its complete ten-row outputs show only the two rank-8 rows
swapping positions, with identical ranks and product names. The final ORDER BY
uses only `asceding.rnk`, projected at zero-based output position 0. Six further
full-data repetitions each on DuckDB and ClickHouse had identical exact Python
row multisets, including duplicate counts, and one shared ordered-ties hash.
These later repetitions were themselves order-stable; the observed variation
is retained in run 604's log. Driver schemas remain recorded separately.

Add only `("tpc_ds", "44"): (0,)` to the existing
`canonical-v11-ordered-ties-v1` registry, making four entries with Q18/Q65/Q71.
Apply the same consecutive full-key tie-group normalization, preserving all
rows, duplicates and values, distinct-group order and LIMIT membership. Default
`canonical-v11`, all other query identities and all timed SQL remain unchanged.
Static review of all 99 queries also identified 25 other candidates with
complete projected order keys, but no observed instability justifies adding
them. Q36/Q70/Q86 have hidden conditional order expressions and remain outside
this projected-key policy.

Retain DuckDB run 591 and ClickHouse run 604 as diagnostics, alongside run 589.
After committing the validated combined source, the parent must rerun both
complete 99-query DuckDB and ClickHouse TPC-DS SF10 SELECT workloads under the
four-entry policy before selecting replacements. These full reruns are pending;
the other completed cells and all 99 original baseline cells need no rerun.
This second amendment follows measurement and is not a premeasurement claim.

## Analytical streams

Measure TPC-H SF10 for DuckDB, Polars, PostgreSQL and ClickHouse at 1, 4 and 16
closed-loop clients, with a 60-second request-issuing window and seed 0. Use the
full 22-query manifest, a full warmup per client, synchronized measured release,
and deterministic rotations of one seeded cyclic query order. Keep the total
engine budget fixed across client counts. Stop issuing at the deadline and
drain in-flight fetches. Record actual elapsed time, counts, errors, throughput
and per-query latency samples separately from resident-hot SELECT medians.

One request failure stops new requests across clients; unfinished work and
operation watchdog aborts remain visible. Report p50 only with at least five
completed observations, p95 with 100, and p99 with 1,000. Otherwise show
insufficient samples. A single timed window is exploratory evidence, not a
confidence interval or a production concurrency capacity guarantee.

## Tuning and adoption

Apply the [bounded tuning protocol](2026-10-tuning-protocol.md): at most three
candidate patterns and twenty minutes of live diagnostics per engine/suite,
shared across scales. Existing exhausted budgets remain exhausted unless a new
mechanism and a new budget are declared. Required readiness and result-cache
corrections are recorded separately from tuning.

Prioritize completing native preparation, attesting effective settings and
statistics, then query plans for the new relational and telemetry workloads.
Compare original/candidate controls in both orders, preserve exact answers and
schemas, and adopt only repeatable practical improvements. A physical or common
configuration change requires rerunning every affected operation; a SQL change
requires the full affected SELECT workload at every applicable scale. Record
rejected proposals, no-change decisions and deferrals. Commit validated code
before final measurements, and retain the original measurements when a proposed
replacement has incomplete coverage or conflicting answers.
