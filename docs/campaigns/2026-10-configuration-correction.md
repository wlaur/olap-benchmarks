# October campaign configuration correction

The October 21 audit identified two changes requiring measured replacements:
Doris SQL-result caching was uncontrolled, and TimescaleDB metrics/ranges used
ordinary PostgreSQL tables. MonetDB's chat SF10 failures were confirmed server
OOM events; no worker, optimizer, or memory configuration error was established
by the static audit.

## Cache boundary

Doris disables `enable_sql_cache` and `enable_query_cache` on every Python and
ConnectorX connection. Effective values and runtime version strings are captured
before and after operations. Publication rejects select cohorts without these
observations. Predicate, inverted-index, file, page, and segment caches remain
available. This boundary preserves the hot profile and avoids asymmetrically
disabling predicate caches while ClickHouse uses them by default.

The initial subagent proposed disabling predicate caches too. That proposal was
narrowed before measured runs. Final-result and aggregate-result reuse are the
controlled variables; this campaign is not a cache-free comparison.

The running image identifies itself as `doris-4.1.5-release-917b20be838`.
A serial Q33 experiment used off → SQL-only-on → aggregate-cache-on → off,
with four executions per condition and an identical answer hash throughout.
On the campaign's Python path, uncached execution took 4.15–5.71 seconds;
SQL-cached repeats took 5.84–8.97 milliseconds. The SQL-on plan contained
`LogicalSqlCache` / `PhysicalSqlCache`; the off plans did not. This confirms
cached-answer reuse, rather than merely inferring it from warmup differences.
Enabling the aggregate cache made this query slower (about 11–12 seconds).
ConnectorX did not show the SQL-cache speedup despite enabled flag readback;
the causal conclusion applies to the Python path actually used by the campaign.

Uncached ClickBench run 506 exposed a separate resource issue: Q18 exceeded the
backend's 10.8 GiB process limit within its 12 GiB container. The frontend also
had a 12 GiB cap, leaving too little of the combined allowance available to
execution. The corrected allocation is FE 6 GiB / BE 18 GiB, retaining FE one
CPU / BE nine CPUs and exactly 24 GiB combined. FE heap flags become
`-Xms1024m -Xmx4096m`; the backend retains its normal 90% process-memory policy.
All three Doris suites completed fresh populate and select measurements under
this allocation: ClickBench 510/511, JSONBench 512/513, and RTABench 514/515.
Answers validated. ClickBench Q18 completed all four iterations, with measured
times 3.767, 3.783 and 4.090 seconds. The original Q32 timeout exclusion remains.
Runtime readback confirmed the container limits, FE heap and 16.2 GiB BE process
limit. Run 506 remains failure evidence, not a replacement cohort.

## TimescaleDB storage proof

Live tests used TimescaleDB 2.30.2 / PostgreSQL 18, ten workers and a 24 GiB
container limit, with the first 256 original Parquet rows and original dtypes/nulls.
The 785- and 1,500-metric core fixtures load into a wide hypertable, but
`compress_chunk` fails with tuple sizes 14,216 and 27,088 bytes versus the
8,160-byte limit. Their EAV layout therefore remains justified for compression.
The static inference that the EAV workaround was unnecessary was rejected.

Metrics `wide1000` likewise fails compression (18,088 bytes). It is explicitly a
rowstore hypertable. `wide100` and EAV fixtures compress successfully and produce
identical ordered results, including nulls, before and after compression. Both
telemetry suites now use seven-day hypertables; EAV tables segment by metric name
and order by time. The loader compresses supported layouts and analyzes them.
Wide tables exceeding PostgreSQL's column limit remain unsupported.

The live server reported ten parallel workers, nine workers per gather, 6 GiB
shared buffers and 139 MiB work_mem, all from command-line configuration.

Validated telemetry replacements preserve the original measured scope:

| Suite | Populate / select | Measured queries | Old sum of medians | New sum of medians |
| --- | --- | ---: | ---: | ---: |
| Metrics SF1 | 500 / 501 | 82 | 18.103 s | 4.135 s |
| Ranges SF1 | 502 / 503 | 90 | 11.543 s | 1.693 s |

Physical readback confirmed all 15 metrics chunks and all 75 ranges chunks in
the five compressible tables were compressed. `wide1000` remained an explicit
rowstore hypertable. Representative plans used columnar scans, vectorized
aggregation and metric/time pruning. A readiness guard now rejects old stores
whose row counts match but required hypertables or compressed chunks are absent.

## Replacement procedure

### MonetDB query investigation

Q08 evaluated JSON over the full content column before applying its user filter.
A filtered derived table with `OFFSET 0` changes the physical plan so selected
content is projected before JSON evaluation. SF1 and adversarial fixtures match
the original values, schemas and hashes. The full SF1 select run 508 validated
all 23 queries; Q08's three measured times were 96.62, 97.18 and 96.46 ms versus
the old approximately 3.33 seconds. The SF1 concurrency rerun 509 completed.

Fresh SF10 populate 517 completed within the existing setup budget. Select 518
completed queries 01–14, including Q08 at 96.18, 103.66 and 96.24 ms, then the
server crashed during Q15's warmup. The sampled container charge reached exactly
24,576 MiB, anonymous/shared memory 24,563 MiB, and client RSS only 250 MiB.
The daemon log records the server crash/restart. This is consistent with memory
exhaustion; the removed container's OOM counter was not captured. Q15's failure
does not establish that unattempted Q16–23 fail. Independent attempts then
exceeded the 45-second limit for Q16 (519) and Q17 (520). Run 521 completed and
validated all six remaining queries, Q18–23. Final select 522 completed the full
23-query plan with 20 measured queries and explicit exclusions for those three
observed failures. Row counts and answer hashes validated, and hashes were
stable across iterations. Q08's measured median was 101.11 ms. The SF10
concurrency replacement 523 also completed, with four readers and 20 writer
batches. These replace the former seven-query primary and failed concurrency
coverage without asserting that the three excluded queries now complete.

Two further chat rewrites passed exact fixture and full-SF1 correctness gates
but were rejected. Filtering assistants before Q17's JSON expansion was slower
in both orders. Hoisting nested content extraction in Q16 first appeared faster,
but counterbalanced warmup/three-repeat comparisons produced medians 9.961 s
for the original and 11.589 s for the candidate. Its memory samples also do not
establish a repeatable saving: retained allocations and cache rose across the
shared-process schedule. Neither rewrite is adopted.

ClickBench Q5's distinct-string count uses a serial whole-column `algebra.unique`
path in MonetDB 11.55.9. Grouping SearchPhrase in an `OFFSET 0` derived table and
counting its non-NULL representatives changes this to partitioned grouping and
reconciliation. Empty, NULL-only, empty-string and mixed duplicate fixtures
passed exact value/schema/Arrow/canonical-hash comparisons. All eight full-store
executions returned the same 6,019,103 distinct non-NULL phrases over 99,997,497
input rows. Counterbalanced measured medians were 21.532 s for the original and
2.207 s for the candidate, with paired speedups of 9.803×, 9.510× and 9.844×.
The rewrite is adopted; these diagnostic timings are not campaign measurements.
Fresh populate 524 validated the full 105-column input. Select replacement 525
completed in 392.3 seconds with all 43 planned queries: 40 measured and explicit
Q4/Q9/Q32 timeout exclusions. Answers and repeat hashes validated. Q5's measured
median was 2.205 seconds. Q33 now has complete primary-cohort observations
(17.725-second median); Q18/Q28/Q34 remained about 13.129/8.922/16.946 seconds.
The whole select run's sampled peaks were 19,347 MiB server charge, 252 MiB
client RSS and 48,597 MiB store allocation; independent maxima are not summed
or attributed exclusively to Q5.

Q5's sampled total container charge was lower for the candidate (1.68–1.80 GiB
versus 2.33–2.42 GiB), while its anonymous-memory peaks were higher and allocations
remained after execution. This establishes a latency improvement in the tested
cohort, not an unconditional memory saving. No OOM, timeout or resource guard
fired. Required storage remained 44.805 GiB; endpoint sizes do not establish
temporary-space peaks or spill. The three ClickBench candidate investigations
used 255.076 seconds of cumulative live diagnostic time, excluding population.

Actual plans rejected the proposed Q28 and Q34 rewrites: all 56 regex operators
already consume filtered projections, and Q34 already groups only by URL before
projecting its constant output. Q18's physical-plan fetch completed but exceeded
the diagnostic's 2,000-row artifact guard; its detailed localization remains
incomplete. Its original query and all existing failure evidence are retained.

### Polars correctness and storage accounting

Adversarial fixtures exposed NULL-distinct, empty-average and duplicate-pivot
differences from canonical SQL. The adapter now excludes NULL from the affected
distinct aggregates, returns NULL for the empty ClickBench average, and uses
MAX for the metrics EAV alignment. The generated EAV inputs already have unique
time/metric pairs; this correction does not imply those published answers were
wrong. Seven new fixture cases failed before the changes and pass afterward.
TPC-H Q17 also now returns NULL for an empty or all-NULL qualifying SUM while
preserving legitimate zero and decimal accumulation. Fourteen fixture variants
failed before that correction; all 26 pass afterward.

The separately evaluated Q17 semijoin is now adopted. Both scales' plans show
that the original computes average quantities for every part, while the rewrite
restricts that aggregation to selected part keys. It preserves all quantities
for those keys, the final join's multiplicity and the corrected NULL sum.
All 26 fixture variants passed for both implementations; all eight full fetches
per scale matched exact schemas, values, Arrow IPC hashes and canonical hashes.
Counterbalanced measured medians were 0.448 to 0.150 seconds at SF10 and 2.704
to 0.702 seconds at SF50. Every measured pair favored the candidate, with
speedups of 2.929–3.045× and 3.830–3.906× respectively.

Sampled process RSS peaks were 3.53–3.65 versus 1.55–2.20 GiB at SF10, and
10.75–10.83 versus 1.27–1.55 GiB at SF50. These include retained allocations;
the fast SF10 candidate had only two or three samples per measured fetch.
Required storage stayed 3,734/19,494 MiB, including all eight external Parquet
tables once. Zero spill at the endpoints does not establish peak temporary
usage or prove absence of spill. One pattern used 121.871 seconds of cumulative
diagnostic time across both scales, including review and cleanup. Populates
526/527 verified every table. Full-suite select replacements 528/529 validated
all 22 queries at each scale. Q17's measured medians were 149.337 ms at SF10
and 721.919 ms at SF50, with stable hashes across all four iterations.

Polars registers most tables as links to external Parquet. The previous store
directory measurement counted link entries without their targets. Metrics
version 8 includes required referenced files and records their allocated-MiB
baseline separately from the imported store. Those bytes are already included
in the total and must not be added again. All six Polars scale cells now have
validated replacements: 324 planned queries completed one warmup and three
measured iterations, with no skips or unsupported queries. Existing ClickBench
Q28 cross-engine comparison policy is unchanged; its repeat hashes are stable.
Historical near-zero store values do not establish a storage advantage.

| Suite | Populate / select | Queries | Sum of measured medians | Required disk | Sampled peak client RSS |
| --- | --- | ---: | ---: | ---: | ---: |
| TPC-H SF10 | 526 / 528 | 22 | 3.647 s | 3,734 MiB | 6,815 MiB |
| TPC-H SF50 | 527 / 529 | 22 | 21.145 s | 19,494 MiB | 25,255 MiB |
| ClickBench SF1 | 530 / 531 | 43 | 20.422 s | 14,095 MiB | 18,359 MiB |
| JSONBench SF10 | 532 / 533 | 5 | 2.769 s | 992 MiB | 3,049 MiB |
| Metrics SF1 | 534 / 535 | 112 | 4.165 s | 1,417 MiB | 5,885 MiB |
| Ranges SF1 | 536 / 537 | 120 | 1.419 s | 898 MiB | 1,536 MiB |

These RSS peaks cover the full select operation. Polars does not enforce the
nominal 24 GiB heap cap; the existing 28 GiB client RSS guard remained in force.
SF10 Q17 has no fully contained RSS samples, so its per-query reading remains
NULL. Required disk includes referenced immutable inputs once and excludes
client staging and temporary spill; endpoint sizes do not establish peak spill.

The [tuning protocol](2026-10-tuning-protocol.md) describes the common bounded
review and resource interpretation. Query-window resource SQL reports observed
footprints and sample counts; it does not claim exclusive query allocations or
spill bytes from whole-store measurements.

### DuckDB 2 JSON extraction

DuckDB's Q02/Q03 overrides now extract two JSON string paths together after the
original filters. The verified 2.0.0 build (`d54cbbe3df`) retains a single shared
list extraction, with the Q03 timestamp cast above its filters. NULL handling,
distinct counts, numeric conversion, UTC hours and output ordering are unchanged.
Q04/Q05 retain their original lexical timestamp aggregation.

Both rewrites independently passed physical-plan review, 46 fixture executions
in UTC and Helsinki, and eight exact full-data answer/schema comparisons per
query. Four fixture executions raised the same expected invalid-timestamp error.
Counterbalanced measured medians fell from 1.095 to 0.741 seconds for Q02 and
1.143 to 1.035 seconds for Q03; every measured pair favored the candidate.
These are tuning diagnostics, separate from final campaign measurements.

The full diagnostic phase sampled a peak process RSS of 4,366,467,072 bytes,
with three to six samples per query window. No material candidate footprint
increase was observed, but retained allocations prevent exclusive attribution.
Store allocation remained 2,902,470,656 bytes; empty WAL/temp endpoints do not
establish peak spill. Native DESCRIBE and actual returned Polars/Arrow schemas
were checked separately: the connection's prior cursor metadata is stale after
relation fetch and was explicitly excluded as fetch metadata.

One pattern consumed 266.361 seconds of the 1,200-second exploration allowance.
Native populate 538 verified 10,000,000 rows. Full five-query SELECT replacement
539 passed normal validation, with measured medians of 742.754 ms for Q02 and
1,045.558 ms for Q03. Detailed diagnostic evidence is retained under
`data/fairness-audit/query-proposals/validation/duckdb2-live-r5/`. The separate
[storage comparison](2026-10-duckdb-storage-comparison.md) selected native for
every suite. A separate complete native/Parquet comparison with the adopted
JSON queries confirmed native at 1.348× under the same selection criteria.
All thirteen fresh DuckDB 2 campaign cells are now validated, with runs 538–571
covering 34 operations and 2,516 successful SELECT fetches. Ten unsupported
RTABench aggregates and the existing large-output hash limits remain explicit.
The storage report lists each replacement cohort and its resource limitations.

### PostgreSQL and TimescaleDB Q11 investigation

The proposed TPC-DS Q11 `NOT MATERIALIZED` directive remains unapplied. Both
engines independently passed exact edge and empty fixtures, preserving native
VARCHAR metadata, NULLs, ordered values and duplicate output rows. Plans showed
earlier year/channel filtering but also repeated fact scans, so the net benefit
required a full-data comparison.

Each original full-data warmup reached its native 45-second statement timeout.
The candidate full runs and measured repetitions were therefore not executed.
This leaves full answer equivalence, latency and resource benefit inconclusive;
small fixture timings are not evidence of a campaign speedup. PostgreSQL used
127.536 diagnostic seconds and TimescaleDB 106.002 seconds, one pattern each.

Fresh normal populations 572/573 verified all 24 tables and completed ANALYZE.
Actual query sessions reported PostgreSQL 18.6, UTC, 139 MiB work_mem, ten
parallel workers and nine per gather; TimescaleDB's extension version was 2.30.2.
Neither client memory nor disk-reserve guard fired. Sampled server footprints
and store/WAL endpoints are retained separately, without claims of peak spill
or candidate resource savings. Evidence is under
`data/fairness-audit/query-proposals/validation/relational/live-r5/`.

### ClickHouse TPC-H Q15

The adopted Q15 rewrite computes supplier revenue once, then obtains its global
maximum with a window before joining suppliers. That order preserves the empty
answer when the maximum belongs to a supplier absent from the dimension. Both
scales passed all seven adversarial fixture pairs and eight complete full-data
answer comparisons, including native Decimal/FixedString types, actual Arrow
transport schemas, NULLs, duplicate rows and exact padded bytes.

The original executes a separate revenue aggregation for its scalar maximum.
ClickHouse evaluated that scalar during EXPLAIN, leaving a literal in the
displayed plan; query-log read counters confirmed the otherwise hidden fact
scan. The candidate has one aggregation followed by the global window and
supplier join. Counterbalanced measured medians fell from 266.095 to 138.798 ms
at SF10 and from 1,680.884 to 835.959 ms at SF50. Every measured pair improved.
These diagnostics remain separate from the required full-suite reruns.

Sampled resource evidence does not establish lower memory use: SF10 server
samples are sparse, and SF50 candidate anonymous-memory observations can be
modestly higher. Store endpoints include logs, background state and temporary
files; they do not measure peak spill or exclusive query storage. Effective
`use_query_cache=0` disabled answer caching while
`use_query_condition_cache=1` retained predicate caching. No cache setting was
changed, and this observation is not evidence of historical contamination.

One pattern used 551.739 seconds across both scales. Populates 574/575 verified
all eight full tables under the original limits on ClickHouse 26.9.19.9.
Evidence is retained in
`data/fairness-audit/query-proposals/validation/relational/clickhouse-live-r6/`.
The complete 22-query SELECT cohort at each scale must validate before replacing
earlier results.

### Rerun policy

Reruns use the original immutable inputs, engine pins, ten-CPU/24-GiB budget,
45-second query limit, 900-second operation limit, one warmup, three measured
iterations, and serial engine execution. TimescaleDB telemetry initially retains
the original table/query scope. Doris reruns all three registered select suites.
MonetDB diagnostics first verify runtime settings and the anomalous chat and
ClickBench workloads. Answers must validate before replacement.

Only runs covered by validated replacements are superseded. Distinct timeout,
OOM, disk-limit and additional-query evidence must survive. Benchmark databases
remain release assets and are not committed.
