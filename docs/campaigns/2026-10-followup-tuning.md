# Follow-up tuning ledger

The [follow-up protocol](2026-10-followup-protocol.md) fixes workload membership,
participants and resource limits before measurement. This ledger declares the
initial candidates before observing full-data timings. The initial table records
planned investigations; completed investigations are recorded separately below.

| Engine / suite | Candidate pattern | Physical-plan gate | Correctness and cost gate |
| --- | --- | --- | --- |
| PostgreSQL / Device Telemetry | Reverse the internal DISTINCT ON device order in queries 02/03, then restore ascending output order outside | Check whether a backward scan of the existing composite key removes incremental/full sorting without adding larger scans | Exact original/candidate rows and schema at SF1/SF10, including latest-row NULLs and inactive devices; original/candidate controls in both orders |
| TimescaleDB / Device Telemetry | The same ordering rewrite against the actual compressed hypertable | Inspect chunk scans, decompression and sorting; no adoption based only on PostgreSQL's plan | Verify before/after compressed results and both scales independently; count compressed chunks and retain memory/storage evidence |
| PostgreSQL / JOB | Add the pinned upstream secondary foreign-key indexes | Check whether representative canonical queries use the new indexes and change join/scan work | All 113 query answers and schemas; charge index building and ANALYZE to preparation, include disk cost, and rerun the full workload on adoption |

The telemetry rewrite changes only row traversal order. The outer ORDER BY
restores the original output. It does not introduce a metadata-dimension lookup,
drop devices, substitute older non-NULL metrics, or reduce the queried history.
The JOB candidate is exactly `upstream/fkindexes.sql`; no precomputed join or
aggregate results are allowed. A faster selected query alone does not justify
an overall workload claim.

Before observing the first PostgreSQL JOB timings, fix representative plan and
paired-control queries to `1a`, `6a`, `17a` and `28a`. Those controls supplement
the complete 113-query answer and final measurement requirements; they do not
replace the fixed workload. Telemetry controls remain queries 02 and 03 at both
declared scales.

Use at most three candidate patterns and twenty minutes of live diagnostics per
engine/suite across scales, following the existing bounded protocol. Record
unchanged plans, noisy controls, failed correctness gates and rejected physical
designs. Population and final suite measurements are outside the diagnostic
allowance. Existing exhausted October budgets are not reset by this ledger.

Doris relational work first requires native loading, full statistics collection,
cache attestation and dialect correctness. Its overview statistics can lag
physical data; readiness must record the actual evidence used rather than equate
a successful ANALYZE statement with complete preparation. No Doris performance
tuning is claimed before that gate passes.

Common-file and JSON tracks hold their representation contracts fixed. Changing
row groups, repartitioning files, indexing prepared JSON, or adding materialized
aggregates would create a different track and is outside these initial candidates.

## ClickHouse common-Parquet Q5: preserve the written join order

The first full common-file cohort, SELECT 601, timed out on Q5 and completed the
other 21 queries. Retain that failed cohort. Its plan had no cardinality
statistics for the file views and formed a customer/supplier join on nation
before applying the selective lineitem/orders chain. This reproduced the
large intermediate relation already described in the existing ClickHouse Q5
adaptation, despite its explicit written join chain.

Investigate three patterns within the new suite's twenty-minute budget:

| Pattern | Outcome | Disposition |
| --- | --- | --- |
| Force `join_algorithm=hash` | The bad join order remains; server reports its 45-second timeout | Rejected |
| Hash joins plus `query_plan_optimize_join_order_limit=0` | Exact DuckDB rows and schema; successful controls around 307–338 ms | Correct, but the extra algorithm override is unnecessary |
| Only `query_plan_optimize_join_order_limit=0` | Exact DuckDB rows and schema in three controls around 330–343 ms; effective readback is zero | Adopt for common-Parquet Q5 only |

The last setting preserves the explicit lineitem/orders/customer/supplier/nation/
region chain. Plans show the large customer/supplier intermediate disappearing.
The [documented join-order setting](https://clickhouse.com/docs/reference/settings/session-settings/query-plan#query_plan_optimize_join_order_limit)
controls this optimization. Original controls before and after candidates still
timed out; these are censored observations, not finite latency measurements.
The abandoned hash-only repetition and interrupted work remain in the logs.
Diagnostics ran serially at the same ten-CPU/24-GiB container limits, with native
45-second query limits and external sixty-second process deadlines, between
09:48:55 and 09:52:43 UTC. No table, source file, row group, index, precomputed
answer or materialized relation changed.

The scoped change was followed by complete 22-query SF10 SELECT replacement
602. Retain 601 and its Q5 timeout diagnostically. Final complete-cohort
answer/source/configuration binding remains pending; record configured
per-query settings in operation metadata. The other ClickHouse workloads and
the original 99 measurements are unaffected. The censored baseline supplies
no finite speedup ratio; successful controls do not establish an overall
workload speedup.

## PostgreSQL and TimescaleDB telemetry: retain the original traversal

Reject the proposed internal descending-device traversal at both declared
scales. Exact original/candidate rows and schemas agree, including the empty
and latest-row-NULL fixtures, but the performance and plan gates do not support
adoption across scales. No telemetry SQL or physical design changes follow
these diagnostics; completed SELECT cohorts 608/610 and 614/616 stand.

The following are diagnostic median fetch times in milliseconds, from six
observations per form in three original/candidate/candidate/original blocks.
They are separate from the publication's three-iteration SELECT medians.

| Engine | Query | SF1 original / candidate | SF10 original / candidate | Decision |
| --- | --- | --- | --- | --- |
| PostgreSQL | 02 latest per device | 158.9 / 143.7 | 611.9 / 1496.3 | Reject: the modest small-scale improvement reverses at SF10 |
| PostgreSQL | 03 latest in region | No paired controls | No paired controls | Reject at plan gate: the same bitmap scan and inner sort remain, with an additional outer sort |
| TimescaleDB | 02 latest per device | 65.7 / 72.8 | 370.7 / 442.9 | Reject: slower at both scales |
| TimescaleDB | 03 latest in region | 37.3 / 45.5 | 149.8 / 195.8 | Reject: slower at both scales |

PostgreSQL Q02 replaces a sequential scan and sort with a backward primary-key
scan. At SF10 the original uses a parallel scan with Gather Merge, and the
backward index traversal loses substantially. TimescaleDB's original plans use
SkipScan over the compressed chunks; the candidate replaces that access with
columnar scans and per-chunk sorting. Avoiding one apparent sort does not imply
less total work.

PostgreSQL diagnostics ran from 10:17:31 to 10:18:32 UTC on October 22;
TimescaleDB diagnostics ran from 10:20:05 to 10:21:07 UTC. Each engine's two
scales share its twenty-minute allowance. Both used the same ten-CPU/24-GiB
container settings, a separate 28-GiB client RSS guard, 45-second session query
limits and 60-second external process deadlines. Runtime settings, plans,
input fingerprints, exact comparison results, storage and Timescale compression
readback are retained in `results/followup-tuning/{postgres,timescaledb}-telemetry.json`
and their accompanying directories. No recorded timeout or wrong answer was
discarded. These bounded controls do not establish confidence intervals.

## PostgreSQL JOB: adopt the upstream indexes with explicit regressions

Adopt exactly the pinned upstream 23 secondary foreign-key indexes for JOB
preparation. The primary-key-only SELECT 612 completed 112 of 113 queries;
Q29c reached its 45-second deadline. Preserve that cohort and its timeout.
The indexed diagnostic completed every query, including Q29c in 0.548 seconds.
The 112 available original answers and DBAPI schemas matched exactly; Q29c's
candidate DataFrame matched a separately executed DuckDB reference exactly.
The timed-out original PostgreSQL Q29c DBAPI schema remains unproven. This is
112 same-engine comparisons plus one cross-engine reference, not 113
same-engine proofs or independent correctness certification.

The predeclared controls used original-forward, candidate-forward,
candidate-reverse and original-reverse blocks, with three retained observations
per block and six per form. All four candidate plans used new indexes.

| Query | Original median (ms) | Indexed median (ms) | Observation |
| --- | ---: | ---: | --- |
| 1a | 113.8 | 50.3 | Faster in both block orders |
| 6a | 243.4 | 26.9 | Faster in both block orders |
| 17a | 1087.1 | 4402.8 | About four times slower in both block orders |
| 28a | 621.7 | 366.6 | Faster in both block orders |

The single-fetch checks across 112 comparable queries have a candidate/original
geometric ratio of 0.4754, but their summed fetch time increases from 75.61 to
107.31 seconds. Q26c regresses from 0.515 to 28.111 seconds in those checks.
Neither number is a full-workload SELECT score: the original Q29c is censored,
and these correctness probes are not the final repeated measurements. The
adoption favors complete workload coverage and typical-query improvement;
it does not establish an improvement for every query or every workload mix.

Index construction took 23.035 seconds and the candidate ANALYZE took 2.545
seconds. Observed database size increased from 8,293,947,071 to 10,027,595,455
bytes. The one candidate pattern consumed 325.212 diagnostic seconds, including
startup, controls, validation, index construction, ANALYZE, restoration and
shutdown, within the shared twenty-minute allowance. All 23 indexes were
dropped afterward and the original primary-key layout was verified; ANALYZE
statistics are not restored, a limitation of the reverse controls.

Adoption in `96ecbf5` was followed by fresh population 635 and complete
113-query SELECT 636. All 113 query hashes match DuckDB, ClickHouse, MonetDB
and StarRocks. Preserve primary-key-only 612 and its timeout. Final 635
preparation timings, charged index construction/ANALYZE, storage and exact
index-definition/readiness readback remain to be bound to publication,
separately from the diagnostic costs above. The implementation is scoped to
PostgreSQL JOB; other PostgreSQL workloads, TimescaleDB and original cells are
unaffected by this physical-design change. Retain the diagnostic details under
`results/followup-tuning/postgres-job.json` and `postgres-job/`.

## TPC-DS correctness corrections and remaining Doris gates

The completed portability corrections preserve the fixed workloads. MonetDB
644/SF10 and 646/SF1 and StarRocks 648/SF10 and 650/SF1 completed all 99
queries, with all 99 peer hashes matching at each scale. The only replacements
of original SF1 cells are MonetDB 646 and StarRocks 650; retain all original
measurements and affected diagnostics. Doris 652/SF1 and 654/SF10 executed all
99, but 654 remains blocked by Q44 answer instability. These are correctness
outcomes, not evidence of a workload speedup or final eligibility.

Run 632 already has Q44 answers in A/B/B/A warmup/measured order, omitted from
the earlier report. In 654, three of four outputs report rank 9 instead of the
reference's rank 8. Later parent-scheduled diagnostics retain six original
and six candidate full results plus three aggregate captures at each scale.
SF10 has one unstable original in six; all six candidates match the exact
Fraction oracle and policy hash. At SF1 all six originals and candidates pass.
All item and threshold SUM/COUNT values match. Three independent SF10 aggregate
repetitions show varying original floating AVG in 33,145 groups, while SUM,
COUNT and proposed mean vary in zero groups. This supports a numerical
explanation without proving an optimizer mechanism.

Pricing captures subsequently passed at SF10 and SF50: three original, three
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

## Analytical export verification

All twelve analytical attempts finished: nine completed and PostgreSQL
670/671/672 failed warmup with unknown QPS. Run 672 retains twelve unfinished
request records. Export fixes `d7fe4e7` and `be8eafc` retain unknown work only
for terminal failed operations with an explicit error, preserving raw rows and
NULL finish times. Logical request order and warmup-completion claims remain
checked. Ruff/Pyright and 162 focused tests passed; independent review found
no further issues. The full non-live suite subsequently passed 2,125 tests,
with one skipped and fourteen live tests excluded. Final export acceptance
remains pending. This is an evidence-export correction, not engine tuning.
