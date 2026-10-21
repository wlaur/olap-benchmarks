# October 2026 AI optimization audit coverage

Source/artifact consolidation at `a1d391b`, with the parent's October 22 status
updates. **All 78 unique registered database/suite pairs are accounted for,
covering 99 registered scale cells.** This is bounded source-review coverage,
not a claim that every query was tuned, explained, or live-tested. Final selected rankings and the removal of 250 fully superseded successful
runs are recorded in the correction report; release delivery remains a separate
publication step.

The authoritative [tuning protocol](2026-10-tuning-protocol.md),
[configuration correction](2026-10-configuration-correction.md), and
[DuckDB storage comparison](2026-10-duckdb-storage-comparison.md) supersede stale
proposal statuses in the ignored research drafts. The
[original scope](2026-10-expanded.md) preserves historical limitations; the [updated analysis](2026-10-expanded-analysis.md) interprets the corrected
selected cohorts. This consolidation read instructions, source,
and existing artifacts only; it performed no engine work, SQL, results-database
reads, input scans, installs, tests, or commits.

## Scope and interpretation

Registration comes from [`Database.suite_registry`](../../olap_benchmarks/dbs/__init__.py),
the overrides under [`dbs/`](../../olap_benchmarks/dbs/), and
[`SUITE_SPECS`](../../olap_benchmarks/settings.py), read without runtime imports.
The [`default matrix`](../../olap_benchmarks/matrix.py) is a selection, not the
registration inventory: notably StarRocks metrics/ranges remain registered.
Airbnb is registered as a smoke workload. Missing implementations are coverage
gaps, not measured failures.

Each pair received the same source-review criteria through shared adapter and
suite paths: native storage/readiness, answer-cache policy, predicates,
expressions/CTEs, joins/statistics, casts, aggregates/sorts/windows, and timed
fetch/materialization. Review depth varies explicitly below. Unlisted individual
queries, generated expansions, runtime plans, and exact equivalence checks remain
deferred unless a specific proof is recorded. An inventory or structural reading
is not a detailed optimizer or correctness audit. Source loader hooks, counts,
and upstream defaults do not establish a particular run's completed setup.

The opportunity is **at most three candidate patterns and 1,200 cumulative live
diagnostic seconds per engine/suite across scales**, not identical optimization
of every query or a requirement to consume every allowance. Earlier source
handoffs used a stricter three-per-engine shortlist; MonetDB ClickBench's legacy
900-second diagnostic limit remains as recorded, with 255.076 seconds used.
That legacy diagnostic limit is distinct from the common 900-second operation
limit. Population/final measurements and required cache/readiness corrections
are separate from exploratory tuning. Unknown budget totals remain unknown;
unused allowance is not authorization to run more work.

All live proof belongs to the parent's serial schedule: immutable inputs and
engine pins, ten CPUs, original memory/client/disk guards, no container swap,
45-second queries, 900-second operations, diagnostic watchdogs, one warmup and
three measured fetches. Plan evidence gates candidates; exact values, schemas,
NULLs, duplicates, order/ties, and adversarial/empty cases gate adoption. Reject
unchanged physical work, semantic changes, regressions, or incomplete proof.
Keep planning, transport, conversion, and complete materialization timed. Charge
physical design to preparation, storage, and all affected operations. No
precomputed answers, narrowed inputs, or unjustified rewrites.

## Pair ledger

Counts by engine: ClickHouse 10, StarRocks 9, Doris 3, PostgreSQL 10,
TimescaleDB 10, QuestDB 6, TDengine 1, DuckDB 10, Polars 5, Arc 5, MonetDB 9.
The three source inventories cover 22 + 27 + 29 = 78 pairs, with no duplicates.
Scales below are registered possibilities, not proof all were measured.

Core `time_series` groups tall/wide/large shapes. Metrics groups 14 templates ×
wide/EAV × 100/1000/2000/5000 metrics (112 names); ranges groups 15 templates ×
wide/EAV × 100/1000/2000/4094 (120 names), including 1/10/100-day spans.
Grouping shares source review, not every expansion's proof. Table/output limits
and the selected cohort's actual scope still apply. TPC-H has 22 queries,
TPC-DS 99, ClickBench 43, JSONBench five, and chat 23. RTABench's 31 raw queries
and ten aggregate-view variants are distinct workloads.

All pairs register populate/select. Core and chat also register
mutate/concurrent except Arc; QuestDB's unsupported upsert/delete steps remain
explicit. Detailed mutation/concurrency review is deferred beyond the cited
proofs. **N** means no supported change from bounded review, **D** deferred,
**A** adopted, **C** corrected, **R** rejected, **I** inconclusive, **STOP**
correctness failure. N and D do not mean no improvement is possible.

| # | Engine | Suite | SF | Source depth, current disposition, remaining deferrals |
| ---: | --- | --- | --- | --- |
| 1 | ClickHouse | time_series | 1,10 | Three shapes/shared families and 18 overrides; ordered MergeTree/codecs. N query change; D ordered-read/pruning plans and detailed writes/concurrency. Cache provenance remains partial. |
| 2 | ClickHouse | time_series_metrics | 1 | Representative wide/EAV, overrides, 14 families; metric/time EAV ordering. N obvious pushdown fix; D wide Q12 allocation/native-aggregate probe and every width's plans/equality. |
| 3 | ClickHouse | time_series_ranges | 1 | All 15 families, PREWHERE/filtered calculation overrides. N missing pushdown; D physical score reuse/pruning across variants, preserving MAX/NULL semantics. |
| 4 | ClickHouse | chat_threads | 1,10 | All 23 overrides read structurally; typed JSON/FINAL semantics. D Q21 restricted message aggregation, remaining physical plans and writes/concurrency; Q08 already filtered. |
| 5 | ClickHouse | rtabench | 1 | Schema/load order and JSON/join/group families; raw/view tracks separated. N setup rewrite; D view completion/content and remaining individual plans; charge all view preparation/storage. |
| 6 | ClickHouse | kaggle_airbnb | 1 | All five forms, including Q04/05 overrides. D preaggregation before calendar/reviews fanout, preserving arrays, NULLs, multiplicity and ties; no live benefit established. |
| 7 | ClickHouse | tpc_h | 10,50 | 22 files inventoried; Q02/11/15/17/18/20/21/22 targeted. A Q15 single revenue aggregation/window after both-scale proof; final 576/577. D remaining plans and Q05/07 workaround revalidation. |
| 8 | ClickHouse | tpc_ds | 1 | 24-table loader, 99 names/14 overrides inventoried; Q36/49/78 targeted. D Q36 repeated scans/reuse/grouping-sets gate and remaining detailed queries; preserve NULL/group/rank semantics. |
| 9 | ClickHouse | jsonbench | 10 | All five overrides/native typed-JSON loader. N query fix; D actual design completion and extraction plans, exact distinct/timestamp contracts. |
| 10 | ClickHouse | clickbench | 1 | All 43 forms structurally read; native load/100 partitions/post-load OPTIMIZE. N rewrite; D setup completion and individual plans; Q28 cross-engine exclusion retained. |
| 11 | StarRocks | time_series | 1,10 | Three-shape families, PRIMARY KEY write contract, stddev adaptation. N query fix; D ordered/window plans and detailed writes/concurrency; cache provenance open. |
| 12 | StarRocks | time_series_metrics | 1 | Representative 112-query families; DUPLICATE KEY/FILES, time-based EAV order. N proven setup defect; D key/pruning/wide5000 support and all-width proof; outside default matrix. |
| 13 | StarRocks | time_series_ranges | 1 | All 15 grouped families/native loader; predicates precede pivot. N pushdown fix; D key pruning, score reuse, variant proof; outside default matrix. |
| 14 | StarRocks | rtabench | 1 | 31 overrides inventoried; selected JSON/date/window/join families. N rewrite; D cardinalities/statistics/remaining SQL; ten views unsupported, restaging charged to populate. |
| 15 | StarRocks | kaggle_airbnb | 1 | All five forms including timed array decoding. D Q04/05 review preaggregation before fanout, independently gated on exact output and plan change. |
| 16 | StarRocks | tpc_h | 10,50 | Native schema/FILES; 21 overrides plus shared Q15, selected correlated/reuse forms. D Q17 statistics/decorrelation; automatic-statistics failure is unproven; remaining detailed queries deferred. |
| 17 | StarRocks | tpc_ds | 1 | 99 names/14 overrides; Q27/49/78/70 targeted. D Q27 repeated CTE scans/statistics completion and remaining SQL; grouping alternatives must preserve averages, NULLs and ties. |
| 18 | StarRocks | jsonbench | 10 | All five forms, JSON/sort-key loader. N rewrite; D actual FlatJSON completion, extraction/CSE plans and exact timestamp/NULL behavior. |
| 19 | StarRocks | clickbench | 1 | All 43 forms structurally read; FILES/date-cast ingestion. N query fix; D runtime plans/readiness; explicit cache/readback patch remains unapplied. |
| 20 | Doris | rtabench | 1 | 31 overrides inventoried, selected JSON/date/window/join families. C 514/515: 31 supported plus ten unsupported views. D batching-hook wiring, Q25/27 cardinalities and remaining SQL. |
| 21 | Doris | jsonbench | 10 | All five overrides/native JSON, duplicate key/hash16. C 512/513: five queries. D filtered extraction/CSE plans; current native design is not failed preparation. |
| 22 | Doris | clickbench | 1 | All 43 forms structurally read/native hash16. C 510/511: 42/43, Q32 skipped. N SQL rewrite; D Python/ConnectorX Q23 fetch localization; preserve full timed materialization and failure 506. |
| 23 | PostgreSQL | rtabench | 1 | Indexed rowstores; selected JSON/age/join forms. N rewrite; D index/statistics completion and remaining queries; preserve Delivered-event multiplicity; ten views unsupported. |
| 24 | PostgreSQL | clickbench | 1 | All 43 forms plus Btree/partial/trigram preparation. N rewrite; D failed-populate/index completion before tuning; verified count precedes indexes and is insufficient readiness proof. |
| 25 | PostgreSQL | time_series | 1,10 | Representative resampling/raw/lookup/rolling/lag/pivot families; indexed EAV. N EAV removal; D remaining templates/write plans; retain anchor domain, LEFT JOINs and unsupported large export. |
| 26 | PostgreSQL | time_series_metrics | 1 | 14 grouped families, heaps/COPY/UNNEST width adaptation. D unselected EAV(metric_name,time)/wide(time) indexes with charged build/ANALYZE/storage; all-width plans; column/output limits retained. |
| 27 | PostgreSQL | time_series_ranges | 1 | 15 grouped families; generic heaps/COPY, early filters/Float64 casts. D same unselected selective-index hypothesis and variant plans; score reuse lower priority. |
| 28 | PostgreSQL | kaggle_airbnb | 1 | All five forms; PK/FK tables, existing fact preaggregation. N rewrite; D actual indexes/plans; preserve DISTINCT arrays, NULL representation, outer group and full timed fetch. |
| 29 | PostgreSQL | tpc_h | 10,50 | Schema/indexes and Q02/15/17/20/21/22 targeted. N rewrite without plans; D other 16 detailed queries and incomplete SF50 lineitem/dependency-safe subset provenance. |
| 30 | PostgreSQL | tpc_ds | 1 | Q01/04/11/74/78 and override resolution; remainder structural. I Q11 NOT MATERIALIZED: fixtures/plans pass, original full warmup times out; candidate full unexecuted, rewrite unapplied. Population 572 retained. |
| 31 | PostgreSQL | jsonbench | 10 | All five overrides, JSONB/LZ4/expression index. N rewrite; D index completion/use and integer-versus-fractional time-expression proof. |
| 32 | PostgreSQL | chat_threads | 1,10 | Representative Q02–06/08/10/11/13–18/20–22; indexed rowstores. N rewrite; D remaining serving/export/write plans and JSON placement; retain UNION ALL and all-role LAG. |
| 33 | TimescaleDB | rtabench | 1 | Native hooks, one continuous-aggregate refresh/matching Q1004 read. D native completion/remaining definitions; two aggregates recorded prepared, eight preparation-budget exclusions, not eight query failures. |
| 34 | TimescaleDB | clickbench | 1 | Native schema/compression phase and 43-query family; individual deltas deferred. D required preparation: 398 failed before compression; 404 does not prove native compressed performance. |
| 35 | TimescaleDB | time_series | 1,10 | Representative time_bucket/EAV families/compression hooks. N redesign: core 785/1500-wide compression fails at 14,216/27,088 > 8,160 bytes; EAV retained. D cohort chunk/write completion and remaining plans. |
| 36 | TimescaleDB | time_series_metrics | 1 | 14 grouped families/native readiness correction. C 500/501 within original 82-query scope; compressible EAV/native hypertables complete, wide1000 explicit rowstore after 18,088 > 8,160 failure. D other widths/plans. |
| 37 | TimescaleDB | time_series_ranges | 1 | 15 grouped families/filtered pivots/casts. C 502/503 within original 90-query scope. D Q13–15 score reuse only if duplicated physical work exists; all-variant equality/plans open. |
| 38 | TimescaleDB | kaggle_airbnb | 1 | Five forms, preaggregating Q04/05; plain tables without PostgreSQL schema PKs. N rewrite; D costed indexed relational design; layout parity is unproven. |
| 39 | TimescaleDB | tpc_h | 10,50 | Deliberate indexed relational rowstore; same six targeted queries as PostgreSQL. N hypertable/rewrite requirement; D remaining detailed SQL/plans and incomplete-input dependency scope. |
| 40 | TimescaleDB | tpc_ds | 1 | Deliberate rowstore, own Q01/04 plus PostgreSQL fallback; bounded Q11 review. I Q11 independently: fixture/plan pass, original full timeout, no candidate full, unapplied. Population 573 retained; other detailed SQL deferred. |
| 41 | TimescaleDB | jsonbench | 10 | All five own forms/inherited indexed JSONB/LZ4 rowstore. N hypertable/rewrite requirement; D index completion, actual sessions and time-expression plans. |
| 42 | TimescaleDB | chat_threads | 1,10 | Representative JSON/window/reuse forms; 30-day compressed messages/thread rowstore. N rewrite; D chunk/native completion and remaining serving/export/write proof; no borrowed MonetDB barrier. |
| 43 | QuestDB | rtabench | 1 | Selected date/JSON/existence/cent-arithmetic forms; other SQL deferred. D unselected Q0003/Q0015 LIMIT-before-count existence probe preserving one boolean on empty; ten views unsupported. |
| 44 | QuestDB | clickbench | 1 | 43 structural forms, designated EventTime/MONTH/WAL/SYMBOL schema. N rewrite; D actual layout/settings/plans; charge host sort and preserve byte-length/NULL/regex semantics. |
| 45 | QuestDB | time_series | 1,10 | Representative three-shape families/generic CTAS. STOP native DAY/WAL candidate: r5 empty designated-timestamp MAX answer mismatch; not adopted. Tiny nonempty wide proof only; EAV/boundaries/NULL-time/full-scale/write proof incomplete. |
| 46 | QuestDB | time_series_metrics | 1 | 14 grouped families/generic CTAS. D same native-layout proposal blocked before independent metrics proof; no adoption or speedup. Width/type/WAL/full-answer gates open; preserve DOUBLE CASE workaround. |
| 47 | QuestDB | time_series_ranges | 1 | 15 grouped families/filtered ten-metric calculations. D native layout blocked before independent ranges proof; optional score reuse and all-variant gates unexecuted; no further Quest live work planned this campaign. |
| 48 | QuestDB | kaggle_airbnb | 1 | Five families, existing preaggregation/ordered string-array emulation. N rewrite; D review-scan consolidation only with order/NULL/duplicate proof; broad export remains timed. |
| 49 | TDengine | time_series_ranges | 1 | 15 templates/representative generated forms; tagged EAV subtables/wide FLOAT, STMT2/FLUSH. D unselected score reuse and VGROUPS=2 parallelism probes; configuration/plan gate first, all 120 individual proofs deferred. |
| 50 | DuckDB | time_series | 1,10 | Shared three-shape families/native import; storage comparison at both scales, native required for writes. Final 540–547 includes writes/concurrency. D detailed interleavings/timestamp-PK workaround; large-export hash limits remain. |
| 51 | DuckDB | time_series_metrics | 1 | 14 grouped families/targeted wide/EAV; full storage comparison, native selected. Final 548/549. D Q07 time-before-MAX-pivot probe only if original lacks pushdown; all-width individual plans deferred. |
| 52 | DuckDB | time_series_ranges | 1 | 15 families/representative calc/raw; full storage comparison, native selected. Final 550/551. N missing pushdown; D per-variant pruning/plans; retain full history/output. |
| 53 | DuckDB | chat_threads | 1,10 | Focus Q01–03/07–08/13/16–18/21/23/shared families; full storage controls, native write contract. Final 552–559 includes writes/concurrency. D remaining detailed parsing/write plans; no generic barrier. |
| 54 | DuckDB | rtabench | 1 | Native schema/load; Q0000/0003/0006/0007/0010/0013/0028/0030 focused, storage comparison complete. Final 560/561. N rewrite; D remaining SQL/JSON/join plans; ten views unsupported. |
| 55 | DuckDB | kaggle_airbnb | 1 | All five source families; separate full-output native/Parquet proof passed, native retained. Final 562/563. D Q05 preaggregation/Q04 array semantics; initial missing hashes remain unknown. |
| 56 | DuckDB | tpc_h | 10,50 | Native PK/schema; Q02/17/21 targeted plus Polars branch comparison; full storage comparison. Final 564–567. N rewrite without plans; D remaining detailed decorrelation/statistics audit. |
| 57 | DuckDB | tpc_ds | 1 | Native 24 tables; Q01/04/23 targeted, full storage comparison. Final 568/569. D CTE duplication/cardinality and remaining detailed 99-query review; no forced statistics patch. |
| 58 | DuckDB | jsonbench | 10 | All five forms/native repaired JSON. A Q02/03 list-path extraction after fixtures/plans/full answers; final 538/539. Optimized full storage confirmation retains native; Q04/05 lexical timestamp extrema unchanged. |
| 59 | DuckDB | clickbench | 1 | All 43 forms structurally read/native nullable ingest; full storage comparison. Final 570/571. N query rewrite; D detailed runtime/NULL/arithmetic plans; Q28 matched storage controls, peer exclusion retained. |
| 60 | Polars | tpc_h | 10,50 | All 22 branch structures, targeted Q02/04/11/17/21/22. C Q17 empty/all-NULL SUM; A Q17 restricted-key semijoin after exact both-scale proof. Final 526/528 and 527/529; other detailed optimizer checks deferred. |
| 61 | Polars | jsonbench | 10 | All five branches versus SQL/native structs. C NULL-distinct/storage accounting; final 532/533. N parsing rewrite; D Q04/05 numeric-versus-lexical timestamp edge/type contract, not resolved by campaign hashes. |
| 62 | Polars | clickbench | 1 | All 43 branch structures. C NULL-distinct/empty AVG/storage accounting; final 530/531. N performance patch; D Q29/Q35 arithmetic/NULL-order edges and optional fetch localization; Q28 peer policy retained. |
| 63 | Polars | time_series_metrics | 1 | All 14 branches across eight grouped layouts. C EAV Q04/07/08 MAX and storage accounting; final 534/535. D separate Q07 pushdown, comparing MAX before/after; generated uniqueness does not prove earlier answers wrong. |
| 64 | Polars | time_series_ranges | 1 | Entire 15-template builder, early filters/MAX/score. C required external-file accounting; final 536/537. N query change; D individual scan-pruning/variant plans and equality beyond recorded validation. |
| 65 | Arc | time_series | 1,10 | Three-shape shared families/typed import/compaction hooks; populate/select only. N speculative cache flag; D deployment/budget attestation, compaction/pruning/settings; no writes reenabled. |
| 66 | Arc | time_series_metrics | 1 | All 14 grouped families and compact Q12–14 list overrides; native sorted EAV import. N per-row-list rewrite; D actual compaction/row-group/metric pruning and runtime budgets across widths. |
| 67 | Arc | time_series_ranges | 1 | All 15 grouped families/representative calc/raw/native import. N obvious pushdown fix; D actual partition/file/metric pruning and completion across widths; projection casts do not prove lost pruning. |
| 68 | Arc | chat_threads | 1,10 | Ten overrides inventoried, Q08/18/21 plus shared expansions targeted; typed time-sorted import, populate/select only. D Q08/Q17 role/relational-before-JSON plan gate and remaining override/equality review. |
| 69 | Arc | clickbench | 1 | All 43 adapted forms alongside DuckDB; time-sorted import/compaction. N replacing EventDate with time bounds; D deployment/Arrow-path/layout/settings attestation and EventDate partition pruning. |
| 70 | MonetDB | time_series | 1,10 | Representative shapes/cast/stddev overrides, native BAT/ANALYZE hooks. N global-worker/query patch; D remaining overrides/write proof; three wrong-answer rolling AVG forms remain unsupported. |
| 71 | MonetDB | time_series_metrics | 1 | 14 grouped families/eight native tables/targeted ANALYZE. D Q07 time-before-pivot plan/statistics provenance and all-width proof; eight rolling AVG queries unsupported; full planning/output retained. |
| 72 | MonetDB | time_series_ranges | 1 | 15 families/inherited ANALYZE/early filtered pivot and score. N obvious rewrite; D actual CSE/pruning/variant plans; mutually exclusive CASE text is not proof of duplicated evaluation. |
| 73 | MonetDB | chat_threads | 1,10 | Focused MAL/JSON review plus serial proofs. A Q08 both scales; final SF1 508/509, SF10 517/522/523. R Q16/Q17; all three patterns spent. D Q13, remaining query/write audit; Q15/16/17 failures retained. |
| 74 | MonetDB | rtabench | 1 | Native schema/19 overrides inventoried; Q0000/0003/0007/0010/0019/0030 focused. N selected patch; D remaining SQL/statistics/JSON placement; absent ANALYZE hook alone is not failed preparation. |
| 75 | MonetDB | kaggle_airbnb | 1 | PK/FK schema; both preaggregating Q04/05 fully read plus shared forms. N further tuning; D group_concat-versus-array type/NULL/no-review contract; preserve DISTINCT/order. |
| 76 | MonetDB | tpc_h | 10,50 | Four overrides Q01/08/14/17 and shared Q02/21 targeted. N patch; D Q17 restricted-key average only after plan proof and remaining detailed 22-query review; deliberate DOUBLE casts retained. |
| 77 | MonetDB | tpc_ds | 1 | 30 overrides inventoried; Q72 detailed, Q28/49/83 structural plus shared Q01/04/23. N patch; D remaining 99-query/override proofs, CTE/rank/outer-join cardinality and statistics provenance. |
| 78 | MonetDB | clickbench | 1 | Static handoff plus live plans. A Q5 grouped distinct, final 524/525: 40/43, Q4/9/32 excluded, Q33 included. R Q28/Q34 already-optimized plans; D Q18 localization after artifact guard, remaining per-query proof. |

## Selected proofs and cohort references

Run IDs below refer to `2026-10-expanded`; they identify validated replacement
scopes without declaring release publication or record deletion complete.
Diagnostic comparisons are separate from final benchmark samples.

- **Doris:** both `enable_sql_cache` and `enable_query_cache` are off on query
  connections; predicate/data caches remain available. FE 6 GiB/one CPU plus
  BE 18 GiB/nine CPUs fits the combined 24 GiB/ten-CPU allowance. Python Q33's
  controlled off/on/off proof established SQL answer reuse; ConnectorX did not
  show that speedup. Final 510–515 cover all three registered suites as listed
  above. Preserve run 506's backend-limit failure.
- **TimescaleDB:** metrics 500/501 and ranges 502/503 prove native hypertables
  and complete compression for the selected compressible tables, with explicit
  wide1000 rowstore. They cover 82/90 queries, not all 112/120 registered names.
  Core wide-compression failures justify EAV; the source comment records that
  constraint. ClickBench 398/404 remains incomplete-preparation evidence.
- **MonetDB:** Q08 adopted after SF1/SF10 proof. SF10 522 validates 20 of 23
  planned queries with Q15/16/17 resource exclusions; 523 validates concurrency.
  Preserve 518's crash at sampled 24 GiB and 519/520 timeouts, plus 521's extra
  answers. The removed container's OOM counter was not captured: the sampled
  charge and daemon crash support memory exhaustion without recovering that
  missing counter. Q16's counterbalanced median regressed 16.35%; Q17 also
  regressed, so both were rejected. Q13 stays deferred. ClickBench Q5's eight
  full answers and adversarial fixtures passed; diagnostic medians improved
  21.532 → 2.207 seconds (about 9.76×). Final 525 is a full planned cohort with
  explicit exclusions. Q28's 56 regex operators were already filtered and Q34
  already grouped on URL alone. Q18's plan exceeded the 2,000-row artifact guard;
  that incomplete localization is not a query failure. No global worker change.
  Chat's three-pattern count is known, but a reconciled cumulative diagnostic
  seconds total was not established by this consolidation; no budget-compliance
  total is inferred from fragmentary timings.
- **Polars:** NULL-distinct, empty AVG/SUM, EAV MAX, and required-file accounting
  corrections are distinct from performance tuning. Q17 semijoin passed 26
  fixture variants and eight exact full fetches per scale; diagnostic medians
  improved 0.448 → 0.150 seconds at SF10 and 2.704 → 0.702 at SF50. One pattern
  used 121.871329 seconds across scales. Six final cells 526–537 contain 324
  queries and 1,296 successful fetches, with no skips/unsupported queries.
- **DuckDB 2:** commit `6778832` compared native/Parquet across all 13 cells,
  including 629 executable positions; 625 had complete initial hash equality.
  Native is selected for all ten suites, with core/chat writable contracts
  preserved. Separate full Airbnb proof passed without retroactively filling
  initial missing hashes. JSON Q02/03 was adopted at `beb1320`: one pattern,
  266.360578 diagnostic seconds, about 32%/9% diagnostic wins; the optimized
  five-query storage confirmation still favored native at 1.348×. Final 538–571
  comprise 34 operations, 13 validated SELECTs, 639 planned/629 executable
  positions, 2,516 fetches, four mutate and four concurrent runs. The linked
  storage report has the full matrix and limits. Benchmark DuckDB 2 is distinct
  from browser WASM's prerelease core 1.5.4 and the compatible v1.4 asset.
- **PostgreSQL / TimescaleDB Q11:** independently passed fixtures and plan gates,
  but each original full warmup hit 45 seconds. Neither candidate full run was
  attempted; the shared NOT MATERIALIZED rewrite remains unapplied and benefit
  inconclusive. One pattern each used 127.536114 / 106.002379 diagnostic seconds;
  fresh populations 572/573 remain useful preparation evidence.
- **ClickHouse Q15:** adopted at `b6363e6` after seven fixture pairs and eight
  exact full answers at each scale. Diagnostic medians improved
  0.266095 → 0.138798 seconds at SF10 and 1.680884 → 0.835959 at SF50; one pattern
  used 551.739346 seconds across scales. Populates 574/575 and SELECTs 576/577
  validate all 44 query positions/176 fetches. Lower memory use is unproven.
  Diagnostic readbacks showed answer cache off and condition cache on; those
  observations do not attest every final HTTP request or historical run.
- **QuestDB native layout:** r5 stopped on correctness, with 22 original states
  and eight nonempty candidate wide states completed. Empty designated-timestamp
  `MAX(time)` returned zero rows instead of one NULL row. DAY/WAL/catalog/interval
  evidence exists for those fixtures; EAV, boundary, NULL-time rejection,
  full-scale performance/resources and write/concurrency proof remain incomplete.
  The r4 helper's NULL-sort oracle defect was corrected in r5 and is distinct
  from this engine answer mismatch. One pattern consumed 423.288579 cumulative
  seconds; no adoption and no further Quest live work planned this campaign.
  The [separate issue write-up](../../issues/questdb/empty-designated-timestamp-max.md)
  is owned by the issue task; its empty candidate catalog was not captured after
  the answer gate stopped, so nonempty catalog proof must not be extended to it.

CH/StarRocks explicit cache-control patches remain **unapplied**. Defaults and
source gaps do not prove historical answer-cache contamination, and do not
justify blanket reruns. StarRocks aggregate-intermediate reuse is disallowed;
ClickHouse predicate/condition caching is allowed. Arc's pinned QueryCache
transforms SQL rather than storing answers; no speculative disable flag is
justified. Arc deployment/build/arcx/configuration/Arrow-path attestation remains
open. Deferred PostgreSQL indexes, TDengine vgroups, QuestDB existence probes,
and other table entries are unselected hypotheses, not failed optimizations.

## Resource and fairness limits

Latency, completion, preparation, memory, and required storage must stay separate.
The nominal 24 GiB allowance is not an equal whole-job cap: container engines
have a server limit plus a separate 28 GiB client RSS guard; DuckDB has an internal
allocator limit; Polars relies on the process guard and can exceed 24 GiB RSS.
Report effective settings and sampled server anonymous/shared memory,
mapped-file/page-cache/container charge, and client RSS independently. Do not
sum independent maxima or treat these footprints as exclusive query allocation.

Polars Q17 showed lower observed process RSS with unchanged storage, but samples
include retained allocations and short windows. MonetDB Q5's lower sampled total
charge coexisted with higher anonymous peaks; ClickHouse Q15's memory saving is
unproven. DuckDB native wins the predeclared latency rule while Parquet often
reduces loading time, disk, or RSS. None establishes a universal efficiency rank.

Count shared external input files required at query time exactly once. Polars's
external-Parquet baseline is already included in its disk totals; historical
symlink-only near-zero sizes are not comparable. Native imports need not retain
optional source files. Report persistent required storage and peak observed
whole-store sizes, preparation, and staging scope explicitly. Temporary/WAL/store
endpoints do not measure peak spill; zero endpoints do not prove no spill.

Short-query RSS can be missing and must remain NULL, never zero. Strict
per-query summaries exclude intervals crossing query boundaries; DuckDB's final
report records 1,796 of 1,917 measured positions without a fully contained client
RSS interval. Operation peaks do not fill those missing query readings. Preserve
timeouts, OOM/crash, disk-reserve, unsupported, unstarted, preparation, hash-cap,
and additional-query evidence. Three repeats and resident-hot local runs support
this bounded campaign, not broad statistical, cold-cache, remote-I/O, or memory-
pressure conclusions. Equal bounded opportunity is not uniform query tuning.

## Evidence retained outside committed documentation

These are exact repository-relative paths to ignored local evidence, not claims
that the artifacts are published or available in a clean checkout:

- Source inventories: `data/fairness-audit/public-ledger-draft.txt`,
  `data/fairness-audit/olap-engines.txt`,
  `data/fairness-audit/postgres-timeseries.txt`,
  `data/fairness-audit/embedded-engines.txt`. Earlier adoption/pending wording
  is historical; the current committed reports and parent updates govern.
- MonetDB: `data/monetdb-diagnostics/20261021-research-proposals/research-handoff.md`,
  `data/monetdb-diagnostics/20261021-live-q17-q16/handoff.json`,
  `data/monetdb-diagnostics/20261021-q16-counterbalanced/summary.json`,
  `data/monetdb-clickbench-static-20261021/HANDOFF.txt`,
  `data/monetdb-clickbench-static-20261021/validation/HANDOFF-r8.txt`, and
  `data/monetdb-clickbench-static-20261021/validation/live-r8-q5/summary.json`.
- Polars: `data/fairness-audit/native-polars-proposals/q17-serial-proof/live-r4/summary.json`
  and `data/fairness-audit/polars-six-cohort-reruns-r5/summary.json`.
- DuckDB: `data/fairness-audit/query-proposals/validation/duckdb2-live-r5/REPORT.txt`,
  `data/fairness-audit/query-proposals/validation/duckdb2-live-r5/diagnostic-budget-final.json`,
  `data/duckdb2-upgrade/comparison/evidence-index-initial-completed.json`,
  `data/duckdb2-upgrade/airbnb-full-answer/complete.json`, and
  `data/duckdb2-upgrade/final-cohorts-r7/merged-final-index.json`.
- Relational: `data/fairness-audit/query-proposals/validation/relational/live-r5/REPORT.md`,
  `data/fairness-audit/query-proposals/validation/relational/clickhouse-live-r6/REPORT.md`,
  `data/fairness-audit/query-proposals/validation/relational/clickhouse-live-r6/budget-closure.json`,
  and `data/fairness-audit/query-proposals/validation/relational/clickhouse-final-r7/REPORT.md`.
- QuestDB: `data/fairness-audit/native-polars-proposals/questdb-serial-proof/runs/quest-native-fixture-resume-20261022-r5/final-ledger.json`
  and the adjacent `evidence-index.json`; preserve the separate r4 directory too.
- Cache source/proposals: `data/fairness-audit/cache-proposals/README.txt` and
  `data/fairness-audit/cache-proposals/arc-cache-audit.txt`.

Relevant committed investigations retain context without replacing campaign
proof: [MonetDB JSON memory](../../issues/monetdb/json-query-memory-amplification.md),
[wide planning](../../issues/monetdb/wide-many-aggregate-planning.md),
[QuestDB CASE casts](../../issues/questdb/double-precision-cast-inside-case.md),
[ClickHouse hash joins](../../issues/clickhouse/fixedstring-too-large-under-parallel-hash.md),
[StarRocks full joins](../../issues/starrocks/bucket-shuffle-full-join-loses-matches.md),
and [Arc wide queries](../../issues/arc/wide-parquet-query-cost.md).

Replacement selection must match revision, immutable inputs, configuration,
query/table/operation scope and validation before retiring old completed runs.
Do not erase distinct failure or extra-query evidence. The 78-pair source ledger
does not close the stated deferrals or certify final rankings/publication.
