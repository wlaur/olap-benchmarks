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

Record configured per-query settings in operation metadata. Commit this change
and rerun the complete 22-query SF10 SELECT before replacing cohort 601. The
other ClickHouse workloads and the original 99 measurements are unaffected.
Successful controls do not establish an overall workload speedup; the complete
replacement cohort and answer comparison provide the publication evidence.

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
