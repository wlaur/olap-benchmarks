# October 2026 expanded local campaign

Completed on 2026-10-19, branch `t3/expand-suite-db-benchmarks`, local revision
`2026-10-expanded`: **99 combinations, 75 full supported operation groups and 24
measured partials**. Every combination has a nonempty completed select cohort.
None is wholly excluded, failed-only, or unfinished. Failed attempts remain
recorded; a successful partial does not erase them.

Full means every **registry-supported** query and operation, excluding declared
unsupported work. In particular, the existing MonetDB 11.55.9 rolling-AVG defect
excludes three core queries at each scale (59/62 measured) and eight metrics
queries (104/112 measured). Those excluded queries are not claimed as validated;
the exact conditions are in `olap_benchmarks/query_registry.py`.

This covers every registered suite/database/supported-scale combination: 78
default-scale and 21 additional-scale candidates across ten suites and eleven
engines. Relative to the existing 70-pair matrix, this adds eight default
registrations and 21 larger-scale candidates. Replays do not increase the count.
Unregistered pairs, including MonetDB JSONBench, remain unimplemented; this is
not an engine-capability conclusion.

| Suite | Full | Partial | Total |
| --- | ---: | ---: | ---: |
| Kaggle Airbnb | 7 | 0 | 7 |
| Chat threads | 11 | 1 | 12 |
| Time-series ranges | 8 | 2 | 10 |
| Time-series metrics | 7 | 2 | 9 |
| TPC-DS | 4 | 2 | 6 |
| TPC-H | 10 | 4 | 14 |
| JSONBench | 7 | 0 | 7 |
| Core time series | 9 | 7 | 16 |
| RTABench | 6 | 2 | 8 |
| ClickBench | 6 | 4 | 10 |
| **Total** | **75** | **24** | **99** |

Results and proofs are local, not published or committed. The site data was not
replaced. Measurements span multiple clean commits and snapshots, so the whole
collection does not satisfy the single-commit publication gate as one release.

## Method and material corrections

The audit preceded expansion (`b8b9666`). Host: `macbook-m4-pro`, 48 GiB RAM,
fourteen logical CPUs, 44 GiB OrbStack allowance. Benchmarks ran strictly serially
with ten configured CPUs/threads, actual `POLARS_MAX_THREADS=10`, 24 GiB
server/embedded limits, a 28 GiB client-tree guard and a 40 GiB disk reserve.
Doris shared its budget: FE 1 CPU / 12 GiB, BE 9 CPUs / 12 GiB. Container CPU quotas
do not guarantee a ten-thread internal worker pool.

`resident-hot-v1` restarts servers before selects, retains OS caches and measures
one warmup plus three measured iterations. Latency includes fetch, transfer and
complete Polars materialization; hashing is outside that boundary. Normal limits
were 900 seconds per operation and 45 seconds per query iteration. Early resource
stops are identified separately. Tests/builds ran at quiet boundaries, with one
retained exception: diagnostic populate run 211 overlapped brief static checks at
final verification/restart. Its populate latency is unsuitable for strict serial
comparison; its selects did not overlap.

Material corrections and affected baseline replays include:

- Resource/failure handling: PostgreSQL-family container and buffer budgets,
  binary DuckDB GiB limits, combined Doris limits, canonical mutation seeds, and
  mutation failures that previously appeared successful.
- Faithful answers: canonical-v11 mixed-null arrays; native FLOAT/list decoding;
  decimal transport, rounding, bytewise ordering, character/byte lengths, raw
  column order and deterministic ties. Representative commits: `e55b553`,
  `4231541`, `2f9ad4e`, `810aa02`, `e8d4755` through `84f0905`.
- Loading/statistics: success-only PostgreSQL `ANALYZE` (`32909a3`), bounded native
  COPY preserving quoted records and CR/LF (`b5f9153`), and external Parquet sorting
  in a short-lived child (`a401944`, `9423051`). The reproduced external COPY
  splitter defect belongs to its driver, not an engine.
- SQL semantics: RTABench incremental aggregates and independent raw oracles
  (`af10d7a`), QuestDB null/cent arithmetic (`d618dfb`), StarRocks sample deviation
  (`3204f09`), and TPC-DS precision/full-join execution (`9471618`). The
  [StarRocks reproduction](../../issues/starrocks/bucket-shuffle-full-join-loses-matches.md)
  documents the demonstrated engine defect and semantics-preserving plan hint.
- Lifecycle: 120-second graceful shutdown (`a8bd52e`), native Doris stop scripts
  and backend-first restart (`ba6c955`), supported by native logs and Docker events.
  See the [Doris investigation](../../issues/doris/container-stop-bypasses-graceful-shutdown.md).
- Recovery validity: the local controller retains dependency-safe query includes.
  Committed invalidation support (`2a1af7d`) preserves diagnostic records and
  original failures while excluding invalid experiments from validation.

All 17 native-COPY, seven shutdown and seven DuckDB memory-policy replays are
complete, with no pending journal entries. Other required correctness replays are
complete. PostgreSQL/Timescale metrics and ranges use ordinary rowstores, not
Timescale hypertables; core time series uses their existing EAV designs. Engines
retain their native indexing, compression, column layouts or Parquet scans.

## Partial coverage and resource evidence

Inputs retain full deterministic row counts and values. Tables or exact queries
are selected; rows are not truncated. Up to four select explorations are followed,
where needed, by a real completed cohort whose conservative
`4 × maximum observed iteration` estimate fits 600 seconds. A primary cohort and
additional fully observed queries are separate evidence.

| Partial combinations | Measured coverage and limitation |
| --- | --- |
| PostgreSQL + Timescale ranges SF1 | 90/120 queries; `wide100`, `wide1000`, EAV100/1000/2000/4094. Larger wide layouts exceed PostgreSQL's table-column limit. |
| PostgreSQL + Timescale metrics SF1 | 82/112 queries; `wide100`, `wide1000`, EAV100/1000/2000/5000. Genuine 2,000/2,001-column outputs exceed the separate result-column limit; the two-column all-metrics rewrite works. |
| PostgreSQL + Timescale TPC-DS SF1 | 98/99; Q11 exceeded 45 seconds. |
| PostgreSQL TPC-H SF10 | 21/22; Q18 exceeded 45 seconds. |
| MonetDB TPC-H SF50 | Five dependency-verified queries after the disk reserve stopped incomplete `lineitem` loading. |
| PostgreSQL + Timescale TPC-H SF50 | Four queries: Q02/Q11/Q16/Q22. Their seven required tables have full verified counts; `lineitem` is incomplete. Q13 separately timed out on verified dependencies. |
| PostgreSQL + Timescale core time series SF1 | 61/60 selects respectively; mutation failures retained, concurrent recoveries completed. Timescale rolling average also exceeded 45 seconds. |
| StarRocks core time series SF1 + SF10 | 61/62 selects; batch export exceeded 45 seconds. Mutations/concurrency completed separately. SF10 export timeout also broke driver rollback; an exact 61-query replay preserved tall/wide coverage. |
| PostgreSQL + Timescale core time series SF10 | 39 tall/wide queries each; large EAV table omitted. PostgreSQL mutation reached 900 seconds; Timescale's first wide point upsert was stopped after more than 190 seconds without completing. Canonical concurrency requires the large table. |
| Arc core time series SF10 | 39 tall/wide queries; full large-table import stopped using measured throughput. |
| Timescale RTABench SF1 | 33 queries: 31 raw plus two continuous aggregates; eight costly aggregate preparations excluded. |
| QuestDB RTABench SF1 | 28-query primary cohort plus two independently validated observations; Q0025 timed out. Ten materialized-view queries are registry-unsupported. |
| MonetDB ClickBench | 39-query primary plus independently validated Q33; Q4/Q9/Q32 timed out. |
| PostgreSQL ClickBench | Five-query primary; 35 fully observed queries validated across attempts. Full table verified after failed populate; indexes may be incomplete. |
| Timescale ClickBench | Eight-query primary; 26 fully observed queries validated. Q35–Q42 lack complete four-iteration observations after bounded exploration. Full table verified; indexes may be incomplete. |
| Doris ClickBench | 42/43; Q32 timed out. |
| MonetDB chat SF10 | Seven-query primary; sixteen tail queries lack complete observations after failed exploration. Mutation completed; concurrency failed. |

PostgreSQL's SF10 large EAV model was excluded up front: six billion observations
imply roughly 370 GiB heap/index storage before WAL/scratch, based on the existing
model and SF1 evidence. This is an estimate, not a measured SF10 store. Timescale's
136 million-row batches at median 0.94 seconds projected about 94 minutes for its
large table alone. Arc's 26 completed imports projected about 29 minutes for 751
bounded batches. Both projected completions exceeded the 900-second budget, so the
attempts were stopped early at about 514 seconds and 270 seconds respectively,
then reloaded as tall/wide partials. Early stops are not labelled 900-second
timeouts.

## Invalid diagnostics, sleep and validation limits

A controller bug dropped the SF50 recovery include. PostgreSQL runs 454–457 and
Timescale runs 461–464 are retained as diagnostics from recovery on that incomplete
store, including the invalid awake Q07 retry. Across these attempts, 29 fully
observed queries against unverified `lineitem` disagreed with full-data references.
These eight runs retain invalidation metadata, timings, hashes, SQL,
metrics and original failure errors. They provide no valid full-data coverage or
performance inference. PostgreSQL's explicit four-query run 458 remains valid;
fresh Timescale four-query run 465 completed and validated.
The database retains 496 operation records, including 81 failed attempts (eight
marked as invalid diagnostics), with zero running records. Failed attempts
are distinct from the 99 combination outcomes above.

On October 19 the laptop entered clamshell sleep at **09:09:49 Helsinki time**,
made brief maintenance-wake progress through **09:11:18**, and finally woke at
**17:24:53**, confirmed by retained `pmset` evidence. Run 461 crossed this interval
and is diagnostic. Awake-only Q07 run 463 also used incomplete input and was
invalidated; it does not establish full-data awake performance. No OrbStack hang
is inferred from the sleep interval. Task-scoped idle-sleep prevention ended with
the final controller; unrelated power settings/processes were left alone.

Final official select validation reports matching row counts/hashes and stable iteration
hashes. The broader audit checks **4,828 query/cell pairs**, including complete
queries in failed select runs: all 99 cell audits pass with zero missing target
queries or disagreements. **4,789 pairs have peer hash comparisons**. The other
39 lack a peer hash comparison because of declared exclusions, missing peers or
answer-size limits. Answers above five million cells retain row counts and
explicit hash-skip reasons. The existing ClickBench Q28 regex exclusion remains;
raw RTABench aggregate oracles add evidence where no second engine registers the
same incremental query.

## Dependencies, checks and local artifacts

Python/frontend/tooling dependencies were refreshed with uv and Bun; the local
Python runtime was 3.14.3. Engine pins:
ClickHouse 26.9.19.9, Doris 4.1.5, DuckDB 1.5.6, MonetDB runtime 11.55.9, Polars
2.0.0, PostgreSQL 18.6, QuestDB 10.0.1, StarRocks 4.1.6, TimescaleDB 2.30.2,
Arc 26.09.3, TDengine 3.4.2.8. SQLAlchemy is locked at 2.0.55 within the ClickHouse
dialect's transitive constraint; stable DuckDB-WASM is 1.32.0. The verified native
ARM MonetDB image works here; the absent official amd64 SP4 image remains a cloud
distribution blocker.

Final Python checks at `2a1af7d`: Ruff/format and strict Pyright pass;
**853 pytest tests pass, one is skipped, six live tests are deselected**.
Controller/filter/checkpoint regression checks and ignored-helper lint/type checks
pass. After the dependency refresh, frontend `bun run verify`, five E2E tests,
manifest/data verification and generated-schema checks passed; frontend code is
unchanged since those checks.

Local evidence is intentionally ignored by git:

- `results/2026-10-expanded.db`: measurements, including retained failures.
- `results/expanded-campaign/{plan,state,audit-notes}.json`: inventory,
  selections, exclusions, checkpoints and replay provenance.
- `validation.log`, `final-all-observed-audit.{json,log}` and
  `observed-answer-audit-*-proof.json` in that campaign directory.
- `sf50-recovery-filter-proof.json`, `sleep-interruption-evidence.json`,
  `*-resource-stop.json`, per-cell logs and raw Docker lifecycle events there.
- Replay journals: `native-copy-baseline-refresh.json`,
  `shutdown-baseline-refresh.json`, `duckdb-memory-refresh.json`.
- Local operational helpers: `data/serial_campaign.py`,
  `data/audit_observed_answers.py`, `data/check_campaign_checkpoint.py`.

Inspect results through
`./.venv/bin/olap results query --revision 2026-10-expanded "<SQL>"`.
Owned engine containers and populated stores are cleaned; immutable inputs,
proofs and results remain. Final free disk space is approximately **150 GiB**.
No PR, upload, deployment, release publication or site-data replacement was made.
