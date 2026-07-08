# Project review - 2026-07-08

Status after fixes through `124160d` and the `RESEARCH.md` synthesis. Completed
findings from the prior review have been removed. This document tracks only
remaining work and caveats.

**Current answer:** no, not all review items are addressed. The code fixes closed
the db-label/keying, score-universe, time-series SF1/SF10, TPC-DS normalization,
preflight-error masking, ConnectorX recording, validation partitioning, and
Docker platform-flag issues. Remaining blockers are methodology/public-run
quality, correctness checks, version/metadata verification, published-data
hygiene, and the larger suite/engine roadmap from `RESEARCH.md`.

---

## 1. Public-Run Methodology

- [ ] **Full native rerun is still pending.** Docker starts now use the
      host-native platform by default, but `site/public/data/results.db` still
      contains the old published runs from `macbook-pro-m4`, before the platform
      and ConnectorX fixes. The next full rerun should use the intended
      `OLAP_BENCHMARKS_SYSTEM=macbook-m4-pro` value and then regenerate the
      published DB.
- [ ] **Record methodology metadata on each run.** The `run` table still lacks
      host CPU/memory/OS, Docker version, image tag/digest, container platform,
      execution mode (in-process vs server), tuning settings, iteration config,
      and cache/cold/warm policy. `RESEARCH.md` makes this a prerequisite for
      public results so future platform and iteration drift are visible.
      Partially fixed 2026-07-08: new runs now persist `run.metadata` with host
      OS/arch/memory, Python version, Docker version/context/server platform,
      execution mode, container image/digest when available, start command, and
      timing/cache-policy notes. The checked-in published DB was migrated, but
      existing runs naturally have null metadata until rerun; the site does not
      surface this metadata yet.
- [ ] **Verify `db_version` against the running server.** Connector constants
      are still hardcoded (`clickhouse`, `postgres`, `monetdb`, `questdb`,
      `starrocks`, `timescaledb`) and are not checked against `SELECT version()`
      or equivalent. DuckDB derives its value from the installed package.
      Fixed in code 2026-07-08: benchmark runs now call
      `verify_runtime_version()` before recording a run; ClickHouse, MonetDB,
      Postgres, QuestDB, StarRocks, and TimescaleDB each query their runtime
      version and fail on mismatch. DuckDB keeps its package/runtime assertion.
      Caveat: this was unit-tested but not live-smoke-tested against every
      container; the full rerun should confirm each engine's version query.
- [ ] **Decide version bumps before the rerun.** `RESEARCH.md` recommends
      reviewing/updating DuckDB 1.5.0, QuestDB 9.3.5, TimescaleDB 2.25.0,
      ClickHouse 26.1.1.912, and the public-run StarRocks image; PostgreSQL
      18.3 can stay, PostgreSQL 19 is still a beta-track item, and MonetDB is
      lower priority unless a newer service pack is straightforward.
      Fixed in code 2026-07-08: DuckDB is pinned to 1.5.4, QuestDB to 9.4.3,
      TimescaleDB to 2.28.2 with the verified `2.28.2-pg18` image, and
      ClickHouse to the verified 26.x jammy tag `26.6.1.1193`. PostgreSQL stays
      at 18.3, MonetDB stays at Dec2025-SP2, and StarRocks stays at 4.0.9 for
      the Apple Silicon Docker constraint noted in `RESEARCH.md`.
- [ ] **Classify iteration roles.** Store and report first-run/cold,
      warm/steady-state, warm median, and best warm timings separately. On an
      M-series Mac, label first-iteration numbers as first-run/lukewarm rather
      than true cold-cache unless the run environment can actually control OS
      caches.
      Partially fixed 2026-07-08: query/mutation steps now store
      `iteration_role` (`first_run` for iteration 1, `warm` for later
      iterations), with backfill for existing steps. Site scoring and per-query
      latency now use warm/steady-state medians when available, with first-run,
      all-iteration median, and best-warm values exposed in query details.
      Caveat: true cold-cache measurement still requires a controlled public
      benchmark host; local macOS first iterations remain first-run/lukewarm.
- [ ] **Add first-class query status.** Results need explicit `ok`, `timeout`,
      `unsupported`, `wrong_result`, `error`, and `skipped` states. Non-`ok`
      queries should remain in aggregate scoring as missing/penalized instead
      of disappearing from validation or report surfaces.
      Partially fixed 2026-07-08: query/mutation steps now store
      `result_status`, existing completed/failed rows were backfilled, disabled
      time-series mutation steps are recorded as `skipped`, and site timing
      queries exclude non-`ok` steps. Unsupported/wrong-result statuses still
      need to be wired into query execution and correctness validation.
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
      steps are now written as `result_status='skipped'` instead of omitted.
      The historical TimescaleDB missing insert iterations still need root-cause
      analysis during the rerun, and unexpected aborts still need broader
      per-step fault isolation.
- [ ] **Add per-query fault isolation for `TpcDs.select()`.** The first failing
      query still aborts the whole 99-query DB run and the enclosing multi-DB
      command; the post-run validation never runs. Verify MonetDB/StarRocks
      TPC-DS query support or add explicit unsupported lists before running
      them.
- [ ] **Harden TPC-DS decimal normalization.** `_normalization_exprs` still
      blanket-casts decimals to `(7,2)` except `p_cost`; assert source widths so
      out-of-range values fail loudly.

## 3. Dimensions, UI, And Data Hygiene

- [ ] **Home still shows each suite at its default scale factor only.** A system
      with data only at a non-default SF appears "not run"; home column headers
      also do not expose which SF is being scored. Fold this into the suite
      registry work.
- [ ] **TPC-H SF10+SF50 fanout is still not configured.** `suite=all` now fans
      out configured suite scale factors, and time-series has SF1/SF10, but
      TPC-H still only participates at its default SF unless invoked separately.
- [ ] **Cross-system comparison is still absent from the UI.** The data model
      supports comparing the same `(db, version, suite, SF)` across systems, but
      every current site view is scoped to one selected system.
- [ ] **Suite registry remains duplicated.** Python `SuiteName` and scale-factor
      maps, `site/src/lib/benchmarks.ts`, `site/src/lib/suiteConfig.ts`, and
      `home.md` still duplicate suite metadata. Publish a `suites.json` next to
      `queries.json` and drive the site from it.
- [ ] **Migration hardening is still missing for shared revisions.** The
      unique-index migration has no dedup/remediation path for legacy duplicate
      keys; the scale-factor migration keeps unknown legacy suite names as SF1;
      its downgrade is a no-op.
- [ ] **Clean published data after the rerun.** Remove old `macbook-pro-m4`
      runs once `macbook-m4-pro` data is published, delete the 9 shadowed
      duplicate runs, resolve the Postgres TPC-DS populate-only run, and confirm
      TimescaleDB mutate coverage. Do not delete `site/public/data/results.db`
      until the replacement has been generated and validated.
- [ ] **Minor site data edge cases.** `fetchSuiteScaleFactors` requires
      `finished_at` non-null, so an SF with only failed attempts is
      unselectable; `suite_scale_factor` is integer-only, which is fine unless
      sub-SF1 workloads are added.

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

1. Methodology schema first: query status, iteration role, execution mode,
   platform/host metadata, and server-version verification.
2. Decide version bumps and rerun policy, then run the full matrix within the
   disk budget and publish `macbook-m4-pro` data.
3. Fix known divergent queries and add value-level validation before trusting
   the rerun.
4. Clean published-data leftovers and make missing/unsupported steps visible.
5. Finish site/data architecture work: scale-factor home display,
   cross-system comparison, `suites.json`, and shared-revision migration
   hardening.
6. Add JSONBench, then Polars and Doris; handle JOB and TSBS after the result
   status model exists.

## Reference Facts

- Timed unit: `perf_counter` around `fetch()` into Polars, so timings include
  client round trip, result transfer, and materialization.
- Current default SFs: ClickBench 1, Kaggle Airbnb 1, RTABench 1, time-series 1
  and 10, TPC-H 10, TPC-DS 1.
- Current published coverage is still the old `macbook-pro-m4` data:
  clickbench SF1 x 7 DBs; kaggle_airbnb/rtabench/time_series SF1 x 6 DBs;
  TPC-H SF10 x 6 DBs; TPC-DS SF1 with DuckDB and ClickHouse selects plus a
  Postgres populate-only run.
- Current validation: populate table row counts vs source parquet, then
  cross-engine per-query row counts after select runs within one
  `(system, suite, scale factor)` scope. There is no value-level comparison.
