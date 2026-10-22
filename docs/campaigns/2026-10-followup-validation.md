# Follow-up validation evidence

This records correctness and lifecycle gates for the
[declared follow-up](2026-10-followup-protocol.md), plus explicitly identified
ordering evidence observed after measurement. Initial fixture checks precede
the publication measurements. Tiny fixture durations are not benchmark results.
Only the parent starts engines, one at a time, using isolated stores. Server
databases retain their original ARM64 Docker execution; DuckDB and Polars retain
their embedded execution. No additional macOS builds or separate OS comparison
are introduced by these checks.

## Input preparation

Device Telemetry contains 484,318 readings at SF1 and 4,863,454 at SF10, with
1,000 and 5,000 devices respectively. Its deterministic generator, nullable
metrics and fixture oracle are covered by offline tests.

The pinned original JOB archive produced 21 tables and 74,190,187 rows.
Preparation and subsequent reuse verified every input fingerprint, schema,
row count and text-width constraint. The archive acquisition checksum is frozen;
upstream supplied no independent checksum. See the
[suite provenance](../../olap_benchmarks/suites/join_order/README.md).

TPC-DS SF10 preparation produced all 24 canonical tables. Common Parquet
Analytics reuses the eight original TPC-H SF10 files after validating their
schemas, footer row counts and representation contract. JSON Representation
reuses the existing JSONBench SF10 inputs.

## Engine execution fixture gates

| Workload | Native engines checked | Scope and oracle |
| --- | --- | --- |
| JOB | DuckDB, ClickHouse, PostgreSQL, MonetDB, StarRocks | All 113 queries on populated and empty fixtures, including native ingestion; SQLite reference and selected hand-checked goldens |
| Device Telemetry | DuckDB, ClickHouse, PostgreSQL, TimescaleDB | All 18 queries against the Python fixture oracle; TimescaleDB checked before and after compression |
| JSON Representation | ClickHouse | Five raw/prepared query pairs on populated and empty fixtures through the shared loading path; DuckDB reference with lossless timestamp-unit normalization |
| Doris TPC-H | Doris 4.1.5 | All 22 queries on populated and empty canonical-typed fixtures against DuckDB; exact row counts and full persisted/cached column-statistics readiness |
| Doris TPC-DS | Doris 4.1.5 | All 99 queries on populated and empty canonical-typed fixtures against DuckDB; native loading and strict column-statistics readiness |

DuckDB JSON fixture tests also compare all raw/prepared variants offline.
Fixture agreement establishes the tested cases, not universal correctness or
full-data preparation. Full-scale JSON loading and common-file query execution
remain separate gates until their results are recorded here.

JOB required four DuckDB alias renames and two StarRocks quoted output aliases.
Canonical upstream files remain unchanged. Doris TPC-H Q13 required explicit
derived-column aliases. TPC-DS native investigation identified singleton sample
deviation, NULL ordering, alias case, grouping-dependent sorting, calendar-day
arithmetic and empty-input ROLLUP differences. Explicit timed SQL adaptations
passed the complete populated/empty fixture before measurement; no answer is
repaired after execution. Nonempty grouping sets are combined with a scalar
grand total before ranking, preserving the empty-input row and duplicate
grouping rows. Additional offline fixtures cover NULLs, duplicates, filtering,
rank ties and limits.

The Doris fixture first generates tiny data, then inserts it into the repository's
canonical DuckDB DDL before exporting and comparing. This matters because the
extension generator has different types for some address columns and a different
spelling for the store tax column. Comparing unnormalized generator schemas
would incorrectly attribute those differences to Doris.

## Analytical lifecycle gates

The real analytical CLI is exercised with canonical-typed tiny TPC-H inputs,
all 22 queries, 1/4/16 clients and a one-second issuing window. These runs use
isolated inputs, stores and results under `data/fixtures/`; they are excluded
from the follow-up publication revision. They check independent workers,
runtime readback, synchronized release, request evidence and cleanup. Their
throughput and latency have no publication meaning.

DuckDB, Polars, PostgreSQL and ClickHouse passed all three client counts. All
22 observed query hashes agree across the latest runs from all four engines.
ClickHouse's first gate rejected a runtime readback discrepancy before warmup:
the settings table reports `auto(10)` while numeric `getSetting('max_threads')`
reports ten effective threads. The probe now retains both representations and
validates the effective value. The container CPU quota remains ten.

The fixture also exposed ClickHouse's zero-valued SUM over an empty matching
input in Q17, against the canonical NULL. New analytical and common-Parquet
tracks explicitly use `aggregate_functions_null_for_empty=1`; the native
analytical fixture then agrees with its peers. This setting change applies to
the new tracks. Existing resident-hot TPC-H measurements remain unchanged.
Offline failure tests additionally cover partial-store
rejection, cancellation, worker initialization, request deadlines and watchdog
cleanup. A real CLI negative test also rejected a store with its orders removed
before starting analytical warmup. Engine execution success does not establish cache-hit behavior, which remains
uninstrumented.

## Verification boundary

Before the first full-scale follow-up measurement, repository-wide Ruff
formatting/lint and strict Pyright passed. The non-live pytest suite passed
1,876 tests, with one skipped and fourteen live tests excluded. Separately
scheduled engine execution checks are described above.

Bun formatting, lint, types and production build passed, along with 33 unit
tests and 23 browser tests including publication-reader and desktop/mobile/iPad
fixtures. Database code generation matches the selected asset schema. The
original published asset still matches its declared SHA-256 and byte length.
Full-scale outcomes belong in the follow-up campaign report once measurement
and publication validation finish.

## First full-data checks and ordering evidence

The first DuckDB batch completed telemetry SF1/SF10, all 113 JOB queries, both
JSON tracks, all 22 common-Parquet queries and all 99 TPC-DS SF10 queries.
The five raw/prepared JSON pairs agree, and common-Parquet hashes agree with
the retained DuckDB TPC-H SF10 answers.

TPC-DS Q18/Q65/Q71 exposed SQL-permitted output-order ties at SF10. Six full-data
repetitions per query had one row-multiset hash each, while their ordered hashes
varied. Applying `canonical-v11-ordered-ties-v1` produced one hash per query
without changing SQL or row membership. The policy preserves distinct-key-group
order, values and duplicate counts, and does not equate different LIMIT-boundary
memberships. At that point the registry covered only those three query
identities. Static review verified their complete order keys and output positions across all
registered SQL contracts; dedicated tests cover the failure cases.

The [protocol amendment](2026-10-followup-protocol.md#ordering-evidence-amendment-after-the-first-sf10-cohort)
retains the initial run 589 diagnostically and requires a complete DuckDB SF10
SELECT replacement before ranking. The other five completed cells and the
original 99 measurements retain their evidence.

After the scoped hashing change, repository-wide Ruff and Pyright passed again;
the full non-live suite passed 1,933 tests, with one skipped and fourteen live
tests excluded. The separate static review found no actionable defect.

The first ClickHouse common-file attempt exited during container initialization,
before recording an operation. Its default entrypoint recursively changes
ownership beneath the writable data directory, conflicting with a nested
read-only input mount. Move the same read-only source mount outside that tree
and explicitly configure `user_files_path` for this suite. A fresh isolated
container then started, read the five canonical region rows through `file()`,
and reported Docker mount `RW=false`. Query SQL, input files and resource limits
remain unchanged. Ruff, Pyright and 53 targeted tests passed for this startup
correction. Preserve the unsuccessful startup log separately from the retry.

### Q44 evidence and fourth registry entry

The selected ClickHouse TPC-DS SF10 SELECT, run 604, exposed a further permitted
tie in Q44. `results/followup-logs/clickhouse-tpc_ds-sf10.log`, lines 3433–3504,
prints all ten rows for the warmup and three measured iterations. Only the two
rank-8 rows swap positions; the ranks, both product-name values, duplicate
counts and membership remain identical. The final SQL orders solely by
`asceding.rnk`, the first projected column.

The parent then retained six new full-data repetitions from each engine in
`data/temp/q44-duckdb-evidence.log` and
`data/temp/q44-clickhouse-evidence.log`, with all twelve complete frames under
`results/followup-tuning/q44-order/*.arrow`. Exact Python row-multiset `Counter`
comparison passed for every frame against the DuckDB reference, covering the
integer ranks and both string values without hash precision conversion. All
twelve frames retain the rank-8 tie and share the policy hash
`blake2b128:34a9b2b80f7306aaafe2f06d94bcadcb`. These diagnostics themselves
returned a stable row order; they do not independently reproduce the swap.
ClickHouse reports `asceding.rnk` as `UInt64`; DuckDB reports `rnk` as `Int64`.
Those schema differences remain recorded, so value agreement is not a claim
of exact-schema equivalence or universal correctness.

A static audit of all 99 query SQL contracts found Q44's complete final order
key projected at index 0. Tests check Q44's projection and final ORDER BY/LIMIT
across all eleven registered engines' SQL resolution paths, using registered
TPC-DS implementations where available and the shared suite otherwise. This
static check does not establish native support for engines without a registered
TPC-DS suite. The 25 other complete-projected-key candidates had no observed
instability and remain outside the minimal policy. Q36/Q70/Q86 have hidden
conditional order expressions and explicit negative execution-path guards.

The registry now contains only Q18/Q44/Q65/Q71. The existing
`canonical-v11-ordered-ties-v1` algorithm and default `canonical-v11` are
unchanged. Registry tests check both SELECT and analytical paths, including
unchanged default ordering for Q36/Q70/Q86 and identities outside TPC-DS;
existing hash tests cover full-key grouping, separated groups, values,
duplicates and LIMIT-boundary membership.

The [second protocol amendment](2026-10-followup-protocol.md#q44-amendment-after-the-first-clickhouse-sf10-cohort)
retains runs 591 and 604 diagnostically and requires complete DuckDB and
ClickHouse 99-query SF10 SELECT reruns after the validated combined source is
committed. Those replacements remain pending. No original baseline cohort is
rerun for this policy change.

With engines paused, repository-wide Ruff format checking and lint passed,
strict Pyright reported zero errors and warnings, and the focused registry and
hashing tests passed all 215 cases on Python 3.14.3. An offline reread of the
twelve retained Arrow frames also confirmed exact `Counter` agreement, the
rank-8 tie, one shared policy hash and both preserved schemas. No engine was
started or benchmark rerun for these implementation checks.
