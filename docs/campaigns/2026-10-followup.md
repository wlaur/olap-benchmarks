# October 2026 follow-up campaign

The follow-up adds complete measurements for four new suites, demonstrates that geometric scores and summed query medians can reverse participant order, and exposes correctness and completion limits in larger relational workloads. PostgreSQL JOB indexes enable complete coverage but introduce substantial regressions. Doris numerical translations passed both-scale diagnostic gates and were adopted in `d3a6ccb`; complete replacement runs 681/682 (TPC-DS SF1/SF10) and 683/684 (TPC-H SF10/SF50) have finished with matching peer hashes. Nine analytical configurations completed; three PostgreSQL configurations failed warmup.

The resulting publication policy scores complete, answer-consistent fixed workloads. Failed, incomplete and correctness-blocked attempts retain coverage and query detail without a full-workload score. Latest failed attempts remain visible, with no fallback to older successes. Diagnostics and superseded measurements remain raw evidence.

## New-suite measurements

These **20 workload/scale/track summaries come from 18 SELECT cohorts**; each JSON cohort supplies both tracks. Matching-input peer hashes agree. Final publication eligibility and selection are checked separately below. Every declared query has three measured iterations. “Gmean ratio” geometrically averages each query's median divided by its fastest participant median within that workload, scale and track, without smoothing. Lower is better; this is not an elapsed-runtime multiplier. “Sum medians” adds query medians, excludes preparation and is not full-run elapsed time.

| Workload / scale / track | Engine      | Queries | Gmean ratio | Sum medians (ms) |
| ------------------------ | ----------- | ------: | ----------: | ---------------: |
| Telemetry SF1            | DuckDB      |      18 |       1.000 |             29.2 |
| Telemetry SF1            | ClickHouse  |      18 |       3.988 |            115.0 |
| Telemetry SF1            | PostgreSQL  |      18 |       9.079 |            373.4 |
| Telemetry SF1            | TimescaleDB |      18 |      11.161 |            370.6 |
| Telemetry SF10           | DuckDB      |      18 |       1.000 |             56.6 |
| Telemetry SF10           | ClickHouse  |      18 |       3.094 |            159.2 |
| Telemetry SF10           | PostgreSQL  |      18 |      12.630 |           1680.7 |
| Telemetry SF10           | TimescaleDB |      18 |      13.779 |           1072.5 |
| JOB, original snapshot   | DuckDB      |     113 |       1.045 |           3302.9 |
| JOB, original snapshot   | StarRocks   |     113 |       2.528 |           7235.4 |
| JOB, original snapshot   | ClickHouse  |     113 |       4.239 |          13919.9 |
| JOB, original snapshot   | MonetDB     |     113 |       4.868 |          20154.7 |
| JOB, original snapshot   | PostgreSQL  |     113 |       6.805 |          73355.8 |
| Common Parquet SF10      | Polars      |      22 |       1.104 |           3688.4 |
| Common Parquet SF10      | DuckDB      |      22 |       1.191 |           3376.7 |
| Common Parquet SF10      | ClickHouse  |      22 |       2.736 |           8737.8 |
| JSON SF10 raw            | DuckDB      |       5 |       1.000 |           4012.3 |
| JSON SF10 raw            | ClickHouse  |       5 |       1.907 |           7251.7 |
| JSON SF10 prepared       | DuckDB      |       5 |       1.000 |            203.9 |
| JSON SF10 prepared       | ClickHouse  |       5 |       3.312 |            570.6 |

At telemetry SF10, TimescaleDB has lower summed medians than PostgreSQL but a higher geometric ratio. Common Parquet reverses similarly: Polars leads geometrically, while DuckDB has the smaller sum. Equal query weighting in log space and sums dominated by expensive queries answer different questions.

Telemetry measures warm reads over 484,318/4,863,454 static readings at SF1/SF10. Twelve nullable metrics and latest-row semantics include NULLs; simulated arrival metadata measures neither freshness nor late-write cost. [JOB](../../olap_benchmarks/suites/join_order/README.md) uses the pinned May 2013 snapshot: 21 tables, 74,190,187 rows and all 113 upstream queries. Its frozen acquisition checksum has no independent upstream checksum.

[Common Parquet](../../olap_benchmarks/suites/parquet_analytics/README.md) reads the same eight immutable TPC-H files without native import or optimized duplicates. [JSON preparation](../../olap_benchmarks/suites/json_representation/README.md) extracts fields one-to-one, preserving rows, NULLs and timestamp extrema. Field materialization cost 1.529 s in DuckDB and 3.242 s in ClickHouse; total recorded population cost 29.909/21.096 s, including repair (13.212/13.440 s), loading, verification and applicable restart. Read savings exclude those charges; shared raw/prepared storage cannot be allocated separately. These representation tracks have zero overall weight.

## Bounded tuning

The [tuning ledger](2026-10-followup-tuning.md) records controls, rejected candidates and costs under the shared three-pattern/twenty-minute allowance per engine/suite across scales.

ClickHouse common-Parquet Q5 adopts only `query_plan_optimize_join_order_limit=0`, preserving written join order and avoiding the large intermediate. Replacement 602 completed all 22 queries; retain 601's Q5 timeout. Original controls before and after candidates still timed out, so no finite speedup ratio follows.

Reject the PostgreSQL/TimescaleDB telemetry traversal rewrites. PostgreSQL Q02's modest SF1 gain reversed at SF10; Q03 failed its plan gate. TimescaleDB candidates regressed at both scales and displaced SkipScan. Original cohorts 608/610/614/616 stand.

PostgreSQL JOB adopts exactly 23 upstream foreign-key indexes (`96ecbf5`). Fresh population 635 and SELECT 636 completed; all 113 hashes match four peers. Diagnostic Q17a regressed from 1087.1 to 4402.8 ms; Q26c's single-fetch check rose from 0.515 to 28.111 seconds. Across 112 comparable probes, the geometric candidate/original ratio was 0.4754 while summed time increased from 75.61 to 107.31 seconds. Original Q29c timed out, preventing a complete before/after ratio.

Diagnostic index construction cost 23.035 seconds, ANALYZE 2.545 seconds, and storage rose from 8.294 to 10.028 GB. Reverse controls restored the primary-key layout but not sampled ANALYZE statistics, leaving a confound. Adoption improves coverage without guaranteeing latency improvement. Final fresh population 635 cost 123.484 seconds, including loading, indexes, statistics and verification; this is separate from the diagnostic construction timings.

## Correctness and relational coverage

The [protocol](2026-10-followup-protocol.md) and [validation record](2026-10-followup-validation.md) retain the detailed chronology. `canonical-v11-ordered-ties-v1` covers only TPC-DS Q18/Q44/Q65/Q71: reorder within consecutive complete-key ties, preserving values, duplicates, distinct-group order and LIMIT membership. Numeric precision and default hashing remain unchanged. DuckDB 619 and ClickHouse 620 completed all 99 SF10 queries with matching hashes.

Explicit NULL placement, numeric promotion and calendar-day translations corrected further portability differences. MonetDB 644/SF10 and 646/SF1 and StarRocks 648/SF10 and 650/SF1 completed all 99 queries with matching peer hashes at each scale. **646 and 650 replace two original SF1 cells**; retain their predecessors. MonetDB's separate [rank-filter defect](../../issues/monetdb/rank-filter-drops-boundary-ties.md) has an eight-row reproducer returning four qualifying rows instead of six. Generic portability differences alone do not establish engine defects.

Doris 632 already had Q44 instability in A/B/B/A warmup/measured order; 654 again changed ranks, with three of four outputs reporting rank 9 instead of 8. Tie-order normalization cannot reconcile changed values or membership.

At each of SF1 and SF10, later diagnostics retained six original and six candidate Q44 outputs plus three aggregate captures. **All six candidates per scale match exact final-row Counters from a `Fraction` oracle and the policy hash.** SF1 originals also pass; one of six SF10 originals changes rank and membership. All item and threshold SUM/COUNT values match. Across three SF10 aggregate captures, native floating AVG varies in **33,145 groups**; exact SUM, COUNT and proposed means each vary in zero groups.

Doris SF50 population 655 verified all eight tables, including 300,005,811 lineitems. SELECT 656 executed all 22 TPC-H queries but Q01 disagreed with four same-input peers. Three subsequent SF50 originals all reproduce `6a326039c5330434cc387e83de0aa441`; three candidates all match the four-peer hash `fc42e22f683c4e39c64e63eab843ddfe`. At SF10, three originals and three candidates all match the normal reference hash.

The SF50 policy difference is group `(N,O)`'s `avg_disc`: original `0.04999790773844881`, reference `0.049997907733309185`. About **5.14e-12** crosses the existing ten-significant-digit boundary; precision was not relaxed. Exact diagnostic sums/counts match across three diagnostic captures per scale. All six pricing candidate captures preserve other fields, native schema and NULL masks. Candidate means can retain last-bit raw floating differences within the existing contract; whole raw frames are not asserted exactly equal to the reference.

After both-scale gates and offline checks, **`d3a6ccb` adopted both translations**: exact decimal SUM followed by DOUBLE division by `NULLIF(COUNT(...), 0)` for all six Q44 averages, including thresholds, and three pricing averages. The [four-capture evidence summary](2026-10-followup-doris-means.json) binds artifacts and SQL. Observations support a numerical explanation without proving a Doris engine bug or optimizer mechanism. Full replacements from clean `d3a6ccb` completed: 681/682 cover all 99 TPC-DS queries at SF1/SF10; 683/684 cover all 22 TPC-H queries at SF10/SF50. Every query has warmup plus three measured observations. The latest cohorts have no matching-input/hash-version peer conflicts; superseded 632/654/656 remain raw evidence.

PostgreSQL 658 and TimescaleDB 660 failed at the external 45-second TPC-DS SF10 Q11 watchdog after complete 24-table populations 657/659. Partial and skipped positions remain retained and unscored. Historical TPC-H SF50 limitations for MonetDB/PostgreSQL/TimescaleDB concern incomplete engine stores, not globally incomplete common inputs.

## Exploratory analytical streams

Twelve TPC-H SF10 configurations attempted 1/4/16 closed-loop clients with one outstanding fetch each. All 22 queries receive warmup per client; seed 0 fixes cyclic permutations and client rotations. Synchronized measurement issues requests for 60 seconds, then drains. Total engine resources stay fixed.

| Engine     | 1 client QPS | 4 clients QPS | 16 clients QPS |
| ---------- | -----------: | ------------: | -------------: |
| DuckDB     |       15.598 |        21.616 |         21.542 |
| Polars     |        5.587 |         6.366 |          6.243 |
| ClickHouse |        4.155 |         4.792 |          4.898 |
| PostgreSQL |      Unknown |       Unknown |        Unknown |

Nine configurations completed with zero recorded failed/unfinished request steps. QPS divides completed measured requests by actual elapsed time, including drain, hashing and logging; windows span 60.008–62.271 seconds. Request latency covers fetch/materialization, excluding hashing/logging. Timing determines completed query mix. Gains flatten between four and sixteen clients; one window per configuration supplies no confidence interval or production-capacity guarantee. Nearest-rank p50/p95/p99 require 5/100/1,000 completed samples respectively.

PostgreSQL 670/671/672 failed warmup before measurement. Run 672 retains **12 unfinished records**, unknown and unscored, plus a native timeout that finished 292 ms after the run was marked failed during cancellation. Raw timestamps remain preserved. Exporter fixes `d7fe4e7`, `be8eafc` and `276b8c2` retain this evidence without supplying missing summaries: explicit unscored failures may finish after the failure marker, while successful requests keep the strict cutoff. Logical request order, warmup boundaries and duration checks remain enforced. **191 focused tests and independent source review passed; the actual campaign export accepted all twelve attempts.** Watchdog sequence positions do not identify query numbers; missing readbacks remain unknown. Successful analytical fetches have zero peer-hash conflicts against same-input/version SF10 SELECT answers; this proves neither exact schemas nor universal correctness. Streams remain separate from resident-hot SELECT rankings.

## Publication boundaries and final acceptance

All **244 original selected operations, including 99 SELECT cohorts, were copied and fingerprint-verified for raw retention**. Original asset SHA-256: `6e12f959af14dfefafd8d8d8c8c75032640f70d610813f3c559ba8ff54446b22`. The 28 added cells give a **127-cell publication target**; replacements and diagnostics add no unique cells. Fixed-workload analysis of original measurements is retrospective. [Baseline limitations](2026-10-audit-coverage.md) remain bound to their recorded runs.

Final eligibility requires a completed, non-invalidated cohort without run error, successful warmup and three positive finite measured durations per required query, matching known input fingerprints, no documented incomplete preparation and no conflicting answers. Missing latency is never imputed. ClickBench Q28 remains coverage-only; Airbnb remains smoke-only; RTABench raw/prepared share a family with only raw weighted. No overall winner is established.

This is an M4 Pro macOS/48 GiB campaign using pinned ARM64 Docker servers and embedded DuckDB/Polars; no Linux replication, new engines or macOS server ports. Ten threads, nominal 24 GiB engine budgets and a separate 28 GiB process RSS guard enforce different scopes; Polars has no hard native 24 GiB allocator cap. There is no uniform whole-job cap or general memory-efficiency ranking. Limits also include a 40 GiB disk reserve, 45 seconds per query and 900 seconds per operation. Preparation, storage and sampled memory stay separate; missing RSS stays unknown. Latency includes planning, transport and materialization, so deployment overhead remains included. Three SELECT repetitions establish no independent uncertainty estimate.

The final export retains **312 operations**, including **127 SELECT cohorts: 125 completed and two failed**. Thirty-four cohorts have partial execution coverage. Across **6,837 planned query positions**, 6,244 have complete warmup and measured observations, 515 are skipped, 72 unsupported, and six partial or failed. A completed cohort or query is not automatically eligible for ranking. The selected analytical evidence contains nine completed and three failed attempts. The exporter validated pins, drivers, recorded provenance, preparation/cache evidence and the single system snapshot; selected follow-up answers have no matching-input/hash-version conflicts. An absence of `wrong_result` flags is not an independent correctness proof.

The raw revision retains **351 operations**. The [retention record](2026-10-followup-retention.json) verifies all four original table row counts and fingerprints. [Run-specific annotations](2026-10-followup-annotations.json) supply 52 evidence records and are explicitly regenerated for this revision; baseline annotations apply only to their recorded runs. Superseded measurements and diagnostic failures stay in raw storage, rather than contributing publication scores.

The application reader identifies **92 distinct cohorts eligible in at least one performance track**. Track counts overlap, so their sum is not a distinct-cohort count. The [publication evidence record](2026-10-followup-publication.json) binds these counts to the selected database checksum.

| Family / track                    | Eligible / selected at each scale |
| --------------------------------- | --------------------------------- |
| Time Series                       | SF1: 4/8; SF10: 3/8               |
| Metrics; Ranges                   | SF1: 6/9; 8/10 respectively       |
| RTABench raw; prepared            | SF1: 7/8; 1/8 respectively        |
| ClickBench; JSONBench             | SF1: 6/10; SF10: 7/7 respectively |
| Chat Threads                      | SF1: 6/6; SF10: 5/6               |
| Airbnb                            | SF1: 0/7, smoke only              |
| TPC-H                             | SF10: 7/8; SF50: 5/8              |
| TPC-DS                            | SF1: 5/7; SF10: 5/7               |
| Telemetry                         | SF1: 4/4; SF10: 4/4               |
| JOB                               | 5/5                               |
| Common Parquet                    | SF10: 3/3                         |
| JSON Representation raw; prepared | SF10: 2/2 in each track           |

For adopted source `d3a6ccb`, **2,125 non-live pytest tests passed, one skipped and fourteen live tests deselected; Ruff formatting/lint and strict Pyright passed**. The final cancellation-evidence amendment `276b8c2` additionally passed 191 focused tests and independent review. Against the frozen final asset, Bun type/lint/format/build, schema generation and hash/size checks passed, with **33 unit tests, 15 fetch-script tests and 25 browser tests** (zero failures, skips or flakes). Browser coverage includes the real database reader, all twelve analytical attempts, fixed-family eligibility and mobile/iPad WebKit fixtures. Databases are GitHub release assets, never committed. Asset upload, PR review, merge and deployment are separate outcomes.

## Reproduction and release assets

Download the retained raw asset below to `results/2026-10-followup.db` as the publication source. Campaign selection replaces the dataset; it does not merge into an existing campaign. Generate exact-run annotations explicitly for this revision:

```sh
./.venv/bin/olap publish --revision 2026-10-followup --campaign \
  --report-url https://github.com/wlaur/olap-benchmarks/blob/main/docs/campaigns/2026-10-followup.md
./.venv/bin/olap results publication-metadata \
  --output-directory site/public/data \
  --annotations-path docs/campaigns/2026-10-followup-annotations.json
cd site
bun run fmt
bun run verify-data
bun run codegen:check
bun run verify
bun run test:unit
bun test tests/scripts/fetch-data.test.mjs
bun run test:e2e
```

The manifest, SQL, suite inventory and comparison metadata form one reviewable bundle with the tested selected database. [The release record](2026-10-followup-release.json) binds all three assets to GitHub asset IDs, remote digests and exact byte lengths. Remote verification passed for every upload; all prior selected, legacy and original raw assets retain their previous sizes and digests.

| Asset                                                                                                                                                                                               |       Bytes | SHA-256                                                            |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------: | ------------------------------------------------------------------ |
| [Selected browser database](https://github.com/wlaur/olap-benchmarks/releases/download/data/results-832e28599501d125f2f4e0e35bb5251803955fbe1f6066f02c949c9df59f833f.db)                            | 143,929,344 | `832e28599501d125f2f4e0e35bb5251803955fbe1f6066f02c949c9df59f833f` |
| [Retained raw revision](https://github.com/wlaur/olap-benchmarks/releases/download/runs/2026-10-followup-raw-4b549128c0cead5ca43734b14ea15ccd1ce6b1e9d2e05b6694118aef6ebde2b0.db)                   | 251,146,240 | `4b549128c0cead5ca43734b14ea15ccd1ce6b1e9d2e05b6694118aef6ebde2b0` |
| [Diagnostic and tuning evidence](https://github.com/wlaur/olap-benchmarks/releases/download/runs/2026-10-followup-evidence-6f3ae03ea492f8562a60c89af810667eb0dacfb63918e86feba3caff4fc7ce11.tar.gz) |  21,000,221 | `6f3ae03ea492f8562a60c89af810667eb0dacfb63918e86feba3caff4fc7ce11` |

The evidence archive contains 2,716 files with a per-file SHA-256 inventory: complete Arrow/SQL diagnostic outputs, tuning controls, readbacks, CLI logs and reproduction helpers. All archive members passed integrity verification. Historical failed probes remain evidence, not scoring observations. The archive omits source datasets, engine stores and draft reports. Its paths preserve the original repository-relative layout, allowing the compact evidence summaries to identify captured files.

Final review corrected the JSON prepared-track label to “prepared fields” and reconciled completed preparation gates. The metadata generator passed 46 focused publication tests, Ruff and Pyright; regenerated metadata passed hash/schema checks, Bun verification and both real publication E2Es. Neither the label correction nor the evidence-export amendments change measured durations or ranking policy. Uploading fresh assets does not deploy Pages. Review and merge of matching code and metadata into `main` triggers the normal fetch, verification and deployment workflow.
