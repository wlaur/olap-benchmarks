# Project review - 2026-07-08

Status after fixes through the 2026-07-08 methodology, version, warm-reporting,
suite-registry, TPC-DS, and scale-factor-discovery work. Completed findings
from the prior review have been removed. This document tracks only remaining
work and caveats.

**Current answer:** no, not all review items are addressed. The code fixes closed
the db-label/keying, score-universe, time-series SF1/SF10, TPC-DS normalization,
preflight-error masking, ConnectorX recording, validation partitioning, Docker
platform-flag, runtime-version verification, version-bump, warm-reporting, home
scale-factor, suite-registry, TPC-DS decimal-normalization hardening, and failed
scale-factor discovery issues. Remaining blockers are public-rerun quality,
correctness checks, query status/correctness semantics, metadata surfacing,
published-data hygiene, and the larger suite/engine roadmap from `RESEARCH.md`.

---

## 1. Public-Run Methodology

- [ ] **Full native rerun is still pending.** Docker starts now use the
      host-native platform by default, but `site/public/data/results.db` still
      contains the old published runs from `macbook-pro-m4`, before the platform
      and ConnectorX fixes. The next full rerun should use the intended
      `OLAP_BENCHMARKS_SYSTEM=macbook-m4-pro` value and then regenerate the
      published DB. The rerun should also live-confirm each engine's runtime
      version query and the bumped image/package pins.
- [ ] **Surface methodology metadata on the site.** New runs now persist
      `run.metadata` with host OS/arch/memory, Python version, Docker
      version/context/server platform, execution mode, container image/digest
      when available, start command, and timing/cache-policy notes. Existing
      published runs naturally have null metadata until rerun, and the site does
      not yet expose this metadata or caveat missing metadata directly.
- [ ] **Add first-class query status.** Results need explicit `ok`, `timeout`,
      `unsupported`, `wrong_result`, `error`, and `skipped` states. Non-`ok`
      queries should remain in aggregate scoring as missing/penalized instead
      of disappearing from validation or report surfaces.
      Partially fixed 2026-07-08: query/mutation steps now store
      `result_status`, existing completed/failed rows were backfilled, disabled
      time-series mutation steps are recorded as `skipped`, and site timing
      queries exclude non-`ok` steps. Suite-excluded query steps are now
      recorded as `skipped`, and TPC-DS unsupported-list queries are recorded as
      `unsupported`. `wrong_result` still needs correctness-validation wiring.
- [ ] **Make local-vs-public run policy explicit.** `RESEARCH.md` recommends a
      stable x86_64 Linux host for clean public comparisons. If the public set
      remains Apple Silicon Docker data, the site should show host and container
      platform caveats directly.

## 2. Correctness And Validation

- [ ] **Investigate/fix divergent query results, then rerun.**

      | Suite | Query | Divergence in current published DB |
      | --- | --- | --- |
      | clickbench | Q28 | MonetDB and StarRocks return 11 rows vs 25 elsewhere (`REGEXP_REPLACE` handling) |
      | time_series | `wide_10/11_scalar_lookup` | TimescaleDB returns 0 rows vs 1 |
      | time_series | `*_19_null_gap_detection` | TimescaleDB differs materially; ClickHouse is also off by 1-2 rows |
      | time_series | `*_09/22_raw_filtered`, `large_23_batch_export` | TimescaleDB is short by dozens |
      | time_series | `*_06_daily_resample` | ClickHouse and TimescaleDB disagree on date-bucket boundaries |

      QuestDB's ClickBench rewrites still deserve value-level checks even though
      their published row counts match.
- [ ] **Add value-level validation against a reference engine.** Row counts are
      too weak for TPC-DS because 84 of 99 queries end in `LIMIT 100`; a wrong
      rewrite can still return 100 rows. Add deterministic answer hashes or
      equivalent reference checks for ClickBench, TPC-H, TPC-DS, and RTABench
      where practical.
- [ ] **Make unrecorded steps impossible or visible.** TimescaleDB's published
      time-series mutate run silently lacks all three
      `insert_data_large_10000` iterations (79 mutation steps vs 82 elsewhere).
      A missing step should be recorded as an explicit status.
      Partially fixed 2026-07-08: configured disabled time-series mutation
      steps are now written as `result_status='skipped'` instead of omitted, and
      suite-excluded query steps are now recorded as `skipped`/`unsupported`.
      The historical TimescaleDB missing insert iterations still need root-cause
      analysis during the rerun, and unexpected non-TPC-DS aborts still need
      broader per-step fault isolation.

## 3. Dimensions, UI, And Data Hygiene

- [ ] **TPC-H SF10+SF50 fanout is still not configured.** `suite=all` now fans
      out configured suite scale factors, and time-series has SF1/SF10, but
      TPC-H still only participates at its default SF unless invoked separately.
- [ ] **Cross-system comparison is still absent from the UI.** The data model
      supports comparing the same `(db, version, suite, SF)` across systems, but
      every current site view is scoped to one selected system.
- [ ] **Migration hardening is still missing for shared revisions.** The
      unique-index migration has no dedup/remediation path for legacy duplicate
      keys; the scale-factor migration keeps unknown legacy suite names as SF1;
      its downgrade is a no-op.
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
- [ ] **Add Polars as an in-process engine.** Model it as a dataframe/LazyFrame
      engine, use normal Polars APIs, keep SQL mode separate if added later,
      and label it as in-process/single-node.
- [ ] **Add Apache Doris as the next server OLAP engine.** Start with
      ClickBench and JSONBench, then RTABench after query coverage is aligned;
      defer TPC-DS until Docker/load stability is proven.
- [ ] **Align RTABench with upstream coverage.** Research found the local suite
      appears to have 31 base query files while upstream documents 33; verify
      the difference before making strong RTABench claims.
- [ ] **Demote Kaggle Airbnb in public interpretation.** Keep it as a smoke or
      example suite, but do not let it anchor engine-comparison conclusions.
- [ ] **Narrow row-store matrix.** Keep PostgreSQL and TimescaleDB as useful
      row-store/hybrid baselines, but default them to RTABench, custom
      time-series, JSONBench small/medium, and ClickBench with status handling;
      treat TPC-H SF10 and TPC-DS SF1 as optional low-scale runs.
- [ ] **Add JOB only after status handling.** It is the best optimizer-heavy
      follow-up, but unsupported/null statuses need to exist before porting it
      across engines.
- [ ] **Add TSBS/InfluxDB 3 only with a broader time-series push.** TSBS should
      come with ingest, compression, recent-window queries, and concurrent
      read/write behavior; otherwise it is just more suite surface area.
- [ ] **Add a concurrent time-series mutate+select workload.** The README TODO
      called for a workload where one writer inserts rows into the large table
      as quickly as possible while multiple clients run a small fixed set of
      selects against it. Decide the operation name before changing the CLI,
      result schema, and site display.

## 5. Suggested Priority Order

1. Finish query-status semantics: wrong-result wiring, status-aware validation,
   and broader per-step fault isolation outside TPC-DS.
2. Fix known divergent queries and add value-level validation before trusting
   the rerun.
3. Finalize public-run policy and metadata surfacing, then run the full matrix
   within the disk budget and publish `macbook-m4-pro` data.
4. Clean published-data leftovers and make unexpected missing steps visible.
5. Finish remaining site/data architecture work: cross-system comparison and
   shared-revision migration hardening.
6. Add JSONBench, then Polars and Doris; handle JOB and TSBS after the result
   status model exists.

## Reference Facts

- Timed unit: `perf_counter` around `fetch()` into Polars, so timings include
  client round trip, result transfer, and materialization.
- Current default SFs: ClickBench 1, Kaggle Airbnb 1, RTABench 1, time-series 1
  (also configured at SF10), TPC-H 10, TPC-DS 1.
- Current published coverage is still the old `macbook-pro-m4` data:
  clickbench SF1 x 7 DBs; kaggle_airbnb/rtabench/time_series SF1 x 6 DBs;
  TPC-H SF10 x 6 DBs; TPC-DS SF1 with DuckDB and ClickHouse selects plus a
  Postgres populate-only run.
- Current validation: populate table row counts vs source parquet, then
  cross-engine per-query row counts after select runs within one
  `(system, suite, scale factor)` scope. There is no value-level comparison.
