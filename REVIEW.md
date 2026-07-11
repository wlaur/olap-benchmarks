# Project review - 2026-07-11

Status after fixes through the 2026-07-11 methodology, version, warm-reporting,
suite-registry, TPC-DS, and scale-factor-discovery work. Completed findings
from the prior review have been removed. This document tracks only remaining
work and caveats.

**Current answer:** no, not all review items are addressed. The code fixes closed
the db-label/keying, score-universe, time-series SF1/SF10, TPC-DS normalization,
preflight-error masking, ConnectorX recording, validation partitioning, Docker
platform-flag, runtime-version verification, version-bump, warm-reporting, home
scale-factor, suite-registry, TPC-DS decimal-normalization hardening, failed
scale-factor discovery, and system-metadata normalization issues. TPC-H now fans
out SF10 and SF50 for `suite=all`.
The time-series suite now includes an explicit concurrent read/write operation.
The version-bump work now includes MonetDB Dec2025-SP3, the site now surfaces
run methodology metadata when present, and the home page labels the current
Apple Silicon public data as development data. Row-count validation now marks
consensus outliers as `wrong_result`, and query steps now record bounded answer
hashes for value-level validation. Shared-revision migrations now reject
unknown legacy suite names instead of guessing scale factors, dedupe duplicate
natural run keys before adding the unique index, clean dependent orphan rows
from deduped runs, and implement the scale-factor downgrade. The explorer now
includes a cross-system comparison panel for the selected suite and scale
factor. Kaggle Airbnb is now marked as a smoke suite and excluded from the home
aggregate ranking while staying explorable. RTABench now schedules the
upstream 1000-series pre-aggregated query variants where db-specific files
exist and records explicit unsupported steps elsewhere. The default benchmark
matrix now keeps row-store TPC-H/TPC-DS runs opt-in while preserving explicit
low-scale commands. Doris is now wired as a split FE/BE Docker engine for
ClickBench and JSONBench, with combined FE+BE CPU and memory metric samples.
Stable host, Python, Docker, and benchmark-methodology context now lives in
deduplicated `system_snapshot` rows linked to runs by a nullable scalar ID;
legacy run metadata is backfilled by migration, and results merges remap and
deduplicate snapshots.
Remaining blockers are public rerun quality, correctness checks,
published-data hygiene, and the larger suite/engine roadmap from the research
notes.

---

## 1. Public-Run Methodology

- [ ] **Full native rerun is still pending.** Docker starts now use the
      host-native platform by default, but `site/public/data/results.db` still
      contains the old published runs from `macbook-pro-m4`, before the platform
      and ConnectorX fixes. The next full rerun should use the intended
      `OLAP_BENCHMARKS_SYSTEM=macbook-m4-pro` value and then regenerate the
      published DB. The rerun should also live-confirm each engine's runtime
      version query and the bumped image/package pins.
## 2. Correctness And Validation

- [ ] **Investigate/fix divergent query results, then rerun.**

      | Suite | Query | Divergence in current published DB |
      | --- | --- | --- |
      | clickbench | Q28 | MonetDB and StarRocks return 11 rows vs 25 elsewhere (`REGEXP_REPLACE` handling) |
      | time_series | `wide_10/11_scalar_lookup` | TimescaleDB returns 0 rows vs 1 |
      | time_series | `*_19_null_gap_detection` | TimescaleDB differs materially; ClickHouse is also off by 1-2 rows |
      | time_series | `*_09/22_raw_filtered`, `large_23_batch_export` | TimescaleDB is short by dozens |
      | time_series | `*_06_daily_resample` | ClickHouse and TimescaleDB disagree on date-bucket boundaries |

      Partially fixed 2026-07-08: ClickBench Q28 now uses `$1` replacement
      backreferences for MonetDB and StarRocks; the published row counts still
      need the final rerun. Time-series `operation=all` now runs select before
      mutate so the final rerun records select correctness against the clean
      populated dataset instead of post-mutation state. Postgres/TimescaleDB
      EAV time-series queries now anchor row-per-timestamp semantics on
      `binary_1` before left-joining selected metric values, so null-valued
      metrics no longer remove scalar, raw, bucket, rolling, rate, or null-gap
      rows. TimescaleDB marks the all-column `large_23_batch_export` query
      unsupported because its EAV layout cannot return the same wide-row shape
      without a static full-table pivot. ClickHouse now normalizes `hr`/`d`
      bucket aliases on fetch for answer hashing.

      Isolated Doris ClickBench validation on 2026-07-09 initially returned 11
      rows for Q28; Doris needs a SQL literal of `'\\1'` so
      `REGEXP_REPLACE` receives a backreference. The Doris query file was fixed
      and the isolated select rerun returned 25 rows for all Q28 iterations.

      Continuation 2026-07-09: isolated time-series SF1 validation under
      revision `correctness-timeseries-final` now passes row-count and
      answer-hash validation across DuckDB, ClickHouse, and PostgreSQL latest
      select runs. Fixes made during that validation include stable ClickHouse
      standard deviation for `*_07_full_aggregate`, deterministic answer
      hashing over canonical NDJSON, decimal/float and bool/integer hash
      canonicalization, `NULLS LAST` for hourly top-N queries, and explicit
      row-store EAV binary casts. TimescaleDB SF1 populate also completed in
      the isolated environment and verified `data_tall`, `data_wide`, and
      `data_large` counts (`599,999,835` EAV rows for `data_large`); the next
      backend step is the TimescaleDB SF1 select run plus row-count/hash
      validation.

      QuestDB's ClickBench rewrites still deserve value-level checks even though
      their published row counts match.
- [ ] **Add value-level validation against a reference engine.** Row counts are
      too weak for TPC-DS because 84 of 99 queries end in `LIMIT 100`; a wrong
      rewrite can still return 100 rows. Add deterministic answer hashes or
      equivalent reference checks for ClickBench, TPC-H, TPC-DS, and RTABench
      where practical.
      Partially fixed 2026-07-08: query steps now record deterministic
      answer hashes for results up to the configured cell limit, and validation
      compares latest-run hashes after row counts agree. Answer hashing now
      canonicalizes equivalent integer widths, float widths, timestamp
      precision/time zones, and categorical string values before hashing. The
      published data needs the final rerun before these hashes exist across
      engines.
      Partially fixed 2026-07-09: answer hashing now writes canonical NDJSON
      instead of IPC, ignores engine-specific column labels, rounds small
      floating roundoff, and canonicalizes booleans and decimals where engines
      return semantically equivalent values with different physical types.
- [ ] **Make unrecorded steps impossible or visible.** TimescaleDB's published
      time-series mutate run silently lacks all three
      `insert_data_large_10000` iterations (79 mutation steps vs 82 elsewhere).
      A missing step should be recorded as an explicit status.
      Partially fixed 2026-07-08: configured disabled time-series mutation
      steps are now written as `result_status='skipped'` instead of omitted, and
      suite-excluded query steps are now recorded as `skipped`/`unsupported`.
      Select-query loops now isolate per-query failures across suites and record
      skipped remaining iterations after a failed iteration. The historical
      TimescaleDB missing insert iterations still need root-cause analysis
      during the rerun.

      Continuation 2026-07-11: time-series mutation loops now isolate a failed
      mutation step, roll back its transaction, record every remaining
      iteration for that step as `skipped`, and continue with later planned
      steps. This prevents an ordinary per-step exception from leaving the rest
      of the mutation plan invisible. Interrupted processes and the historical
      TimescaleDB run still need confirmation during the rerun.

## 3. Dimensions, UI, And Data Hygiene

- [ ] **Clean published data after the rerun.** Remove old `macbook-pro-m4`
      runs once `macbook-m4-pro` data is published, delete the 9 shadowed
      duplicate runs, resolve the Postgres TPC-DS populate-only run, and confirm
      TimescaleDB mutate coverage. Do not delete `site/public/data/results.db`
      until the replacement has been generated and validated.

## 4. Suite And Engine Roadmap From Research

- [ ] **Add JSONBench next, starting small.** Use 10M rows first, then 100M only
      if storage/runtime are acceptable; avoid the full 1B-row dataset on the
      current 200 GB free-space budget. Start with ClickHouse, DuckDB,
      PostgreSQL, and StarRocks; add Doris once the engine exists.
      Partially fixed 2026-07-08: the suite scaffold, 10M scale-factor
      metadata, upstream five-query workload, DuckDB load path, and ClickHouse
      native-JSON load/query path are in place. PostgreSQL JSONB and StarRocks
      native-JSON load/query paths are also wired. The suite still needs
      engine smoke runs before the final full rerun, and 100M should wait until
      10M storage/runtime are known.

      Partially fixed 2026-07-09: the upstream 10M files contain three raw
      newline-split JSON objects, which strict readers either reject or filter.
      JSONBench loaders now share a normalizing input stream that repairs those
      objects, removes escaped NULs, and preserves the documented physical row
      count with inert `{}` continuation rows. The normalized stream validates
      as 10,000,000 parseable JSON rows with no tab characters.
- [ ] **Add Polars as an in-process engine.** Model it as a dataframe/LazyFrame
      engine, use normal Polars APIs, keep SQL mode separate if added later,
      and label it as in-process/single-node.
      Partially fixed 2026-07-08: Polars is registered as an in-process engine
      for JSONBench using NDJSON-to-Parquet populate and LazyFrame expression
      queries. ClickBench/TPC-H style Polars API ports are still open.
- [ ] **Add Apache Doris as the next server OLAP engine.** Start with
      ClickBench and JSONBench, then RTABench after query coverage is aligned;
      defer TPC-DS until Docker/load stability is proven.

      Investigation progress 2026-07-08: official Doris 4.x docs confirm the
      quick-start MySQL endpoint on port 9030, FE HTTP Stream Load on port
      8030, Stream Load support for JSON and Parquet, and JSON extraction
      functions such as `GET_JSON_STRING`. Docker Hub currently exposes
      split 4.1.x images (`fe-4.1.3`, `be-4.1.3`, `ms-4.1.3`) and a 4.x
      single-container tag `4.0.3-all-slim`; the obvious `all-in-one-4.1.3`
      tag is not published. Next continuation should decide whether to keep
      the repo's one-container lifecycle by starting with `4.0.3-all-slim`, or
      add first-class multi-container lifecycle/metrics for the split 4.1.x
      FE/BE setup. No Doris code was committed yet.

      Partially fixed 2026-07-09: Doris now uses the split `apache/doris`
      `fe-4.1.3` and `be-4.1.3` images, first-class command-sequence
      lifecycle hooks, and metric targets that aggregate FE+BE CPU and memory
      into one run metric sample. Doris is registered only for the initial
      ClickBench and JSONBench scope. JSONBench loads native Doris `JSON`
      values through Stream Load and uses `json_extract_*` query overrides.
      Live smoke 2026-07-09: FE/BE startup, runtime-version verification,
      native-JSON Stream Load, all five JSONBench query overrides, generic
      Parquet Stream Load, FE+BE metric aggregation, lifecycle cleanup, and an
      `olap benchmark doris jsonbench select` run all completed against a tiny
      smoke table. The smoke found and fixed two integration issues:
      ConnectorX is not the default Doris fetch path because live Doris rejects
      ConnectorX's MySQL `socket` variable, and Doris JSON timestamp queries now
      use fractional `from_unixtime(...)` instead of unsupported microsecond
      `timestampadd`. Full JSONBench 10M validation also completed on
      2026-07-09 in the isolated `jsonbench-doris` environment: FE/BE images
      were pulled and already current, populate loaded all ten 1M-row files
      with zero filtered rows after shared input normalization, row-count
      verification passed at 10,000,000 rows, select completed all 25 query
      iterations, latest row-count and answer-hash validation passed, and run
      metrics were recorded as aggregate FE+BE CPU and memory samples. Full
      ClickBench validation completed the same day in the isolated
      `clickbench-doris` environment: the official 99,997,497-row parquet was
      downloaded, populate loaded all 100 partitioned Parquet chunks plus the
      final 97-row chunk with zero filtered rows, row-count verification
      passed, select completed all 215 query iterations, and metrics were
      recorded as aggregate FE+BE samples. The first select run exposed Doris'
      Q28 backreference syntax difference; after changing the query to pass
      `'\\1'` through the SQL literal, a second full select run completed with
      Q28 returning 25 rows for all iterations, and latest row-count and
      answer-hash validation passed. RTABench and TPC-DS remain deferred.
- [ ] **Add JOB only after status handling.** It is the best optimizer-heavy
      follow-up, but unsupported/null statuses need to exist before porting it
      across engines.
- [ ] **Add TSBS/InfluxDB 3 only with a broader time-series push.** TSBS should
      come with ingest, compression, recent-window queries, and concurrent
      read/write behavior; otherwise it is just more suite surface area.

## 5. Suggested Priority Order

1. Fix known divergent queries and add value-level validation before trusting
   the rerun.
2. Run the full matrix within the disk budget, validate the normalized system
   snapshots, and publish `macbook-m4-pro` data.
3. Clean published-data leftovers and make unexpected missing steps visible.
4. Smoke JSONBench for the already-wired engines and smoke the new Doris
   ClickBench/JSONBench paths. Handle JOB and TSBS after
   value-level checks and status-aware reporting have been exercised on the
   rerun.

## Reference Facts

- Timed unit: `perf_counter` around `fetch()` into Polars, so timings include
  client round trip, result transfer, and materialization.
- Current default SFs: ClickBench 1, Kaggle Airbnb 1, RTABench 1, time-series 1
  (also configured at SF10), TPC-H 10, TPC-DS 1.
- With `suite=all`, PostgreSQL and TimescaleDB skip optional TPC-H/TPC-DS runs
  by default; explicit `tpc_h`/`tpc_ds` commands still include those engines.
- Current published coverage is still the old `macbook-pro-m4` data:
  clickbench SF1 x 7 DBs; kaggle_airbnb/rtabench/time_series SF1 x 6 DBs;
  TPC-H SF10 x 6 DBs; TPC-DS SF1 with DuckDB and ClickHouse selects plus a
  Postgres populate-only run.
- Current validation: populate table row counts vs source parquet, then
  cross-engine per-query row counts after select runs within one
  `(system, suite, scale factor)` scope. Consensus row-count outliers are marked
  as `wrong_result`; ambiguous splits still fail validation without guessing.
  New select runs also store bounded answer hashes, and consensus hash outliers
  are marked as `wrong_result` when row counts agree.
- Multi-container engines are represented as one benchmark database; run
  metrics store total CPU and memory across the engine's measured containers,
  not per-service breakdown rows. Doris currently measures FE+BE totals.
- Runs retain the operator-provided `system` label for grouping and natural
  keys, while `system_snapshot_id` identifies the concrete host, Python, Docker,
  and methodology context. Snapshot rows are deduplicated by label plus their
  raw metadata rather than assuming a label permanently identifies one system.
- RTABench upstream currently documents 33 queries in its README, but the
  current upstream Postgres query directory has 31 base files; TimescaleDB and
  ClickHouse also include 10 `1000+` pre-aggregated variants, and upstream
  `run.sh` executes every `*.sql` file in the database query directory.
