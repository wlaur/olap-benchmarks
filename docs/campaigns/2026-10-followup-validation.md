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
full-data preparation. Full-scale JSON and common-file executions are recorded
below; final selection and source/artifact binding remain separate gates.

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

## Full-scale analytical outcomes

All twelve TPC-H SF10 attempts finished under parent scheduling. Nine completed;
PostgreSQL failed warmup at all client counts and has unknown QPS.

| Engine | 1 client QPS | 4 clients QPS | 16 clients QPS | Run IDs (1/4/16 clients) |
| --- | ---: | ---: | ---: | --- |
| DuckDB | 15.598 | 21.616 | 21.542 | 662 / 663 / 664 |
| Polars | 5.587 | 6.366 | 6.243 | 666 / 667 / 668 |
| ClickHouse | 4.155 | 4.792 | 4.898 | 674 / 675 / 676 |
| PostgreSQL | Unknown | Unknown | Unknown | 670 / 671 / 672; warmup failed |

Completed measured requests are 936/1,298/1,308 for DuckDB, 336/384/383 for
Polars and 250/290/305 for ClickHouse. All nine completed runs record zero failed
or unfinished request steps. QPS divides completed measured requests by actual
elapsed time, including drain, hashing and logging; windows span 60.008–62.271
seconds. Request latency instead covers fetch through DataFrame materialization,
excluding hashing and logging. A complete cyclic order visits all 22 queries,
but timing determines completed counts and mix. One window per configuration
supports neither confidence intervals nor a production-capacity guarantee.

PostgreSQL 670/671/672 failed under the parent's 45-second request watchdog
during warmup, before measured release. Labels such as `client-N:warmup:13`
identify sequence positions, not TPC-H Q13. Run 672 retains twelve unfinished
request records; run 671 lacks budget readback and 672 lacks worker readback,
so those facts remain unknown. Export fixes `d7fe4e7` and `be8eafc` represent unknown work only
for terminal failed operations with an explicit error, preserving raw rows and
NULL times. Logical request order and both warmup-completion boundaries are
checked. Independent review found no further issues. Final export acceptance
remains pending.

All twelve input manifests share prefix `1ff2594823ab`. Completed readbacks
record ten shared threads and a configured 24 GiB engine budget without scaling
resources with client count. Polars has no hard 24 GiB native allocator cap;
the separate 28 GiB operation-process RSS guard applies. The parent's read-only
CLI comparison found zero peer-hash conflicts between successful completed
analytical fetches and TPC-H SF10 SELECT at matching input and hash versions.
This supports peer answer agreement, not exact-schema or universal correctness.

Evidence is retained in `data/temp/followup-analytical-{summary,request-check,budget-check,answer-check}.log`
and the engine/client CLI logs under `results/followup-logs/`. Final public
evidence binding and export validation remain pending.

## Verification boundary

Before the first full-scale follow-up measurement, repository-wide Ruff
formatting/lint and strict Pyright passed. The non-live pytest suite passed
1,876 tests, with one skipped and fourteen live tests excluded. Separately
scheduled engine execution checks are described above.

Bun formatting, lint, types and production build passed, along with 33 unit
tests and 23 browser tests including publication-reader and desktop/mobile/iPad
fixtures. Database code generation matches the selected asset schema. The
original published asset still matches its declared SHA-256 and byte length.
Full-scale outcomes recorded below and in the analytical section remain
distinct from final publication validation. Export fixes `d7fe4e7` and `be8eafc`
passed 162 focused tests and independent source review. After the Doris numerical
translations, the complete non-live suite passed **2,125 tests**, with one
skipped and fourteen live tests excluded. Final Ruff/Pyright checks and export
verification are recorded separately; tests do not establish final asset acceptance.

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
retains the initial run 589 diagnostically. Complete DuckDB SF10 replacement 619
now agrees with ClickHouse 620 across all 99 query identities under the final
four-entry policy. The other five completed cells and original 99 measurements
retain their evidence; final selection remains pending.

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
retains runs 591 and 604 diagnostically. After committing validated combined
source, complete 99-query SF10 replacements 619 and 620 agree on all 99 query
identities. No original baseline cohort is rerun for this hash-policy change.

With engines paused, repository-wide Ruff format checking and lint passed,
strict Pyright reported zero errors and warnings, and the focused registry and
hashing tests passed all 215 cases on Python 3.14.3. An offline reread of the
twelve retained Arrow frames also confirmed exact `Counter` agreement, the
rank-8 tie, one shared policy hash and both preserved schemas. No engine was
started or benchmark rerun for these implementation checks.

### Full-data portability diagnostics

Complete SF10 output capture exposed additional query differences in MonetDB
628, StarRocks 624 and Doris 632. The diagnostic frames retain every column,
row, duplicate, original SQL, candidate SQL, engine version, input fingerprint,
schema, Arrow-file digest and normal answer-policy hash. A separate diagnostic
multiset hash helps distinguish order changes; it is not a publication hash or
a new correctness exemption.

MonetDB Q12/Q45/Q50 require explicit reference NULL placement. Q44 requires
DOUBLE inputs to all six AVG expressions, including its HAVING thresholds.
Its original ranks differ in value and membership, so the Q44 ordering policy
cannot reconcile that observation. The candidate output retains the genuine
rank-8 tie and matches the complete reference at both SF1 and SF10.

MonetDB Q67 loses boundary peers under a direct rank predicate. The complete
unfiltered NULL category has 13,084 exact matching rows and ranks in both
engines. A subsequent Python rank filter retains 102 qualifying occurrences,
including four tied at rank 99. MonetDB's direct SQL predicate retains 100.
The equivalent CASE predicate preserves the complete 1,107-row qualifying pool,
with exact row-multiset and sequence agreement, and the final limited result.
An independent eight-row reproduction establishes the same defect without
aggregation or LIMIT. The [engine issue](../../issues/monetdb/rank-filter-drops-boundary-ties.md)
contains the complete tested SQL. An equivalent ROLLUP expansion and a rank
cast did not correct the observation and are not adopted.

All five MonetDB, seven StarRocks and two Doris candidate outputs agree with the
normal reference policy and diagnostic multiset hash at both SF1 and SF10.
The [28-capture evidence summary](2026-10-followup-tpcds-evidence.json) binds
the source SQL, input and Arrow fingerprints to each comparison. The Doris
changes make Q12's five and Q50's ten final NULL positions explicit. StarRocks
changes widen three monetary AVG inputs, widen revenue/return-ratio numerators
before division and ranking, make NULL placement explicit, and express Q72's
offset as five calendar days. Rank values, selected membership and complete
outputs are compared; neither displayed samples nor sorting alone supplies
this gate. These checks establish the stated full-data comparisons within the
existing numeric precision contract, not exact-schema or universal correctness.

After the [SQL amendment](2026-10-followup-protocol.md#sql-portability-amendment-after-the-other-sf10-cohorts),
validation and a source commit, MonetDB 644/SF10 and 646/SF1 and StarRocks
648/SF10 and 650/SF1 completed all 99 queries with all 99 peer hashes matching
at each scale. Only 646 and 650 replace original SF1 cells; all 99 original
cells remain retained as raw evidence. Doris 652/SF1 and 654/SF10 also executed
all 99, but 654 retains Q44 instability. The original observations remain
retained. No diagnostic duration substitutes for a measured SELECT duration.

The subsequent full offline check identified JOB's SELECT override as a harness
contract violation. Its PostgreSQL-only row-count and index checks now use the
existing `before_benchmark_operation` hook, after result storage is initialized
and before the shared SELECT loop. The same checks and SQL still precede timed
queries. Populate and other engine/workload paths are guarded separately. This
placement correction changes neither the indexed layout nor the measured JOB
queries and does not replace run 636's measurements.

With all engines stopped, repository-wide Ruff formatting/lint and strict
Pyright passed. The focused portability, Doris, registry and hashing checks
passed 429 tests. After the JOB hook correction, the complete non-live suite
passed 2,054 tests, with one skipped and fourteen live tests excluded. These
offline checks precede the final corrected TPC-DS measurements.

### Doris Q44 and pricing diagnostic status

The earlier report of Doris 632 omitted Q44 instability: its warmup/measured
outputs follow A/B/B/A. Corrected full-suite 654 again changes Q44 answers,
with three of four materializations reporting rank 9 where the reference
reports rank 8. This changes rank values and membership, beyond the permitted
normalization of order within equal final keys.

Later Q44 diagnostics passed their candidate gates at both SF1 and SF10.
Each scale retains six original and six candidate full-query outputs plus
three aggregate captures. At SF10 one original in six changes rank 8 to 9 and
item membership; all six candidates match the exact Fraction oracle and policy
hash. At SF1 all six originals and all six candidates pass. All item and
threshold SUM/COUNT values match. Independent SF10 native-aggregate capture
across three repetitions finds varying original floating AVG in 33,145 groups,
with zero varying SUM, COUNT or proposed-mean groups. This supports a numerical
explanation without proving an optimizer mechanism.

The candidate replaces all six Q44 AVG expressions, including thresholds, with
exact decimal SUM followed by `CAST(... AS DOUBLE) / NULLIF(COUNT(...), 0)`.
Complete results, SQL and Arrow fingerprints remain in
`data/temp/doris-q44-{sf1,sf10}-r1/`; final summaries are in
`results/followup-logs/doris-q44-{sf1,sf10}-diagnostic.log`. Those logs record
completed diagnostics and confirmed shutdown. These are separate from the
earlier 28-capture portability evidence and from final SELECT measurements.

Doris SF50 population 655 verified all eight table counts, including 300,005,811
lineitems; SELECT 656 executed all 22 TPC-H queries. ClickHouse 577, DuckDB 567,
Polars 529, StarRocks 467 and Doris 656 share input manifest SHA-256
`252a574eb1dc2cdf36f2460d8d074cbf91c040040cf2e8286c16c518406888aa`.
The first four agree on Q01 hash
`blake2b128:fc42e22f683c4e39c64e63eab843ddfe`; Doris records
`blake2b128:6a326039c5330434cc387e83de0aa441`. Pricing captures subsequently passed at SF10 and SF50: three original, three
candidate and three diagnostic captures per scale. At SF50 all originals
reproduce the historical differing hash; all candidates reproduce the four-peer
reference hash. The policy-level difference is the N/O group's `avg_disc`,
`0.04999790773844881` versus reference `0.049997907733309185`. Exact diagnostic
sums and non-NULL counts match. Candidates preserve all other values, native
schemas and NULL masks; raw floating means may still differ from the reference
by a last bit within the unchanged comparison contract. SF10 originals and
candidates both match the reference policy.

Adopt exact decimal SUM followed by DOUBLE division by NULLIF(COUNT(...), 0)
for all six Q44 means and the three pricing means after both-scale gates and
offline checks. These are numerical SQL translations, not a change to hashing
or a demonstrated optimizer defect. The [four-capture summary](2026-10-followup-doris-means.json)
binds source and artifact hashes. Complete TPC-DS SF1/SF10 and TPC-H SF10/SF50
SELECT replacements remain required after committing validated source.

### Completed common-file and JOB replacements

ClickHouse common-Parquet replacement 602 completed all 22 queries after the
scoped Q5 join-order change. Preserve 601 and its Q5 timeout diagnostically;
final complete-cohort answer/source/configuration binding remains pending.

PostgreSQL fresh JOB population 635 and indexed SELECT 636 completed with all
113 query hashes matching DuckDB, ClickHouse, MonetDB and StarRocks. Preserve
primary-key-only 612, its 112 completed queries and Q29c timeout. Final 635
index/readiness readback, preparation timings and storage accounting remain
to be bound to publication. Diagnostic timings do not substitute for these
costs or final SELECT measurements.
