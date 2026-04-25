# Release Plan

Concrete, ordered tasks to implement the OUTPUT.md action items.

**Scope notes**

- We will re-run all benchmarks from scratch. Cross-stack changes (schema files,
  query files, populate code, db wrappers, web app) are fair game; we don't
  need migration paths or backwards compatibility shims.
- Iterate against `--revision test` (`./.venv/bin/olap benchmark <db> <suite>
  --revision test --cleanup`). Inspect with `./.venv/bin/olap results query
  --revision test "<sql>"`. Use `./.venv/bin/olap docker <db> <suite> start|
  stop|restart` for ad-hoc container control.
- QuestDB work is **out of scope** for this plan. A separate QuestDB benchmark
  is being run against the `default` revision in parallel; do not touch
  questdb code, schemas, queries, or `default.db`. When re-running benchmarks
  for the release, use a fresh revision (e.g. `release`) instead of `default`.
- `system = macbook-pro-m4` (single-system release).

---

## 0. Working setup (do once)

- [ ] Confirm `OLAP_BENCHMARKS_SYSTEM` matches the system you intend to publish
      from.
- [ ] Pick the release revision name (recommendation: `release`). Use it for
      every full re-run.
- [ ] Decide whether to keep `default.db` (the in-flight QuestDB run) or merge
      relevant rows out later. Easiest: leave `default.db` alone, publish from
      `release.db` once it's complete.

---

## 1. Critical configuration fixes

These are pre-conditions for a credible re-run. Each lands as one or more
edits in `olap_benchmarks/...`; iterate per-DB with `--revision test` until
the affected queries look right.

### 1.1 Postgres / TimescaleDB time-series: EAV schema for wide tables

**Goal:** Replace the unindexable wide schema (`data_wide`, `data_large`) with
an EAV schema (`time, metric_name, value`) on Postgres and TimescaleDB only.
Keep the wide schema unchanged on DuckDB / ClickHouse / MonetDB. Override the
time-series queries on Postgres/TimescaleDB to read EAV and produce the same
result columns as the wide queries.

**Files to touch**

- `olap_benchmarks/dbs/postgres/__init__.py`
  - New `PostgresTimeSeries.populate`: build the wide DataFrames as today via
    `pl.scan_parquet`, then `.unpivot()` (or `melt`) to `(time, metric_name,
    value)` with one CSV-staged COPY per dataset. Drop `binary_*` cast to
    float (or store as a separate boolean column if we don't want to lose the
    type — see open question below).
  - `index_tables`: replace per-table `(time)` indexes with composite
    `(metric_name, time)` btree on `data_wide` and `data_large` (covers the
    typical filter `metric_name = 'X' AND time BETWEEN ...`). Keep the
    existing `(time)` index on `data_tall`.
- `olap_benchmarks/dbs/timescaledb/__init__.py`
  - `TimescaleTimeSeries.populate` mirrors the Postgres version (unpivot →
    COPY via `timescaledb-parallel-copy`).
  - Drop `SKIP_COMPRESS`. With EAV both wide tables fit and we get
    columnstore.
  - Update `POST_INSERT_SCHEMA_FILES`: every table gets compression with
    `timescaledb.compress_segmentby = 'metric_name'`,
    `timescaledb.compress_orderby = 'time'`. Pick chunk intervals that keep
    chunk count modest at 6B rows for `data_large` (try `7 days`, tune via
    `pg_stat_user_tables` after a `--revision test` populate).
  - Remove `DISABLED_MUTATION_STEPS["time_series"]["insert_data_large_10000"]`
    — EAV makes this work.
- `olap_benchmarks/suites/time_series/schemas/timescaledb/{wide,large}_post_insert.sql`
  - Rewrite as EAV-aware: hypertable on `(time)` with appropriate chunk
    interval, `enable_columnstore`, `compress_segmentby = 'metric_name'`.
- `olap_benchmarks/suites/time_series/queries/postgres/` and `.../timescaledb/`
  - **New directory** `postgres/`. Add EAV overrides for every wide/large query
    that touches a named column. Pattern:
    - `select avg(process_364) from data_large where time > 'X'`
      → `select avg(value) from data_large where metric_name = 'process_364' and time > 'X'`
    - For multi-metric queries (e.g. `large_05_hourly_resample_multi`,
      `large_20_raw_multi_limit`, `large_23_batch_export`) use
      `metric_name IN (...)` and a conditional aggregate / pivot via `FILTER`:
      ```sql
      select date_trunc('hour', time) as hr,
             avg(value) filter (where metric_name = 'process_667') as value_1,
             avg(value::int) filter (where metric_name = 'binary_22') as value_2,
             ...
      from data_large
      where metric_name in ('process_667','binary_22',...)
      group by hr order by hr limit 100
      ```
    - For `large_23_batch_export` (the SELECT * variant): emit
      `select time, metric_name, value from data_large where time >= 'X' and time < 'Y' order by time, metric_name`.
      Document that this is the EAV-equivalent — a true `SELECT *` doesn't
      exist on EAV.
  - Migrate each existing TimescaleDB override (currently in
    `queries/timescaledb/`) to the EAV form. Add EAV overrides for the
    queries that don't yet have a `timescaledb/` override (most of them).
    Where the Postgres EAV form is identical to the TimescaleDB one, keep
    them in sync but separate (so we can tweak `time_bucket` independently).
  - For the `tall` queries (10 cols, fits in a row), **keep wide schema**
    and don't add EAV overrides — `data_tall` stays `(time, binary_1, ...,
    process_N)`. This matches OUTPUT.md §4.1 and is the realistic case for
    `data_tall`.
- `olap_benchmarks/suites/time_series/config.py`
  - `expected_table_row_counts` is keyed by table; for Postgres/TimescaleDB
    `data_wide` and `data_large` it should now equal `n_rows × n_cols`
    instead of `n_rows`. Either: (a) override `expected_table_row_counts` in
    `PostgresTimeSeries`/`TimescaleTimeSeries` to compute the EAV count, or
    (b) have those subclasses use a different table-name layout. Prefer (a)
    to keep the SuiteName/TableName mapping uniform across DBs.
  - Update `_generate_insert_data` / `_generate_upsert_data` /
    `_generate_delete_keys` to also emit EAV rows when the target db is
    Postgres/TimescaleDB. Cleanest split: move that logic onto the suite
    subclass via `insert_table` / a new `_to_eav` helper invoked only by the
    PG/TS subclasses' mutate path.

**Open questions to resolve while implementing**

- `binary_*` columns: bool. Store EAV `value` as `double precision` and cast
  bools to 0/1 (lossy but consistent with the wide schema's
  `cast(binary_22 as int)` queries). Or use a separate `data_*_bool` table.
  Default: cast to numeric, document.
- Chunk interval tuning: start at 7 days for `data_large` (≈86 chunks across
  ~7.6 years × 1500 metrics = ~7M rows/chunk), 1 day for `data_wide`
  (current value). Re-tune via `--revision test` if compression ratio /
  query latency disappoint.

**Validation per query**

- After implementing, run `olap benchmark postgres time_series --revision
  test --cleanup` and `olap benchmark timescaledb time_series --revision
  test --cleanup`.
- Spot-check 3 representative queries against DuckDB for result equivalence:
  ```bash
  ./.venv/bin/olap results query --revision test \
      "select query_name, db, row_count from run_step rs join run r on r.id=rs.run_id
       where suite='time_series' and step_type='query' and iteration=1
       order by query_name, db"
  ```
  `row_count` should match across DBs for same query.
- Sanity-check key OUTPUT.md anomalies:
  - `large_06_daily_resample` and `large_18_top_n_hourly` should drop from
    ~100 s to a few seconds on TimescaleDB.
  - `large_19_null_gap_detection` may still be slow but should at least
    track Postgres rather than be 9× worse.

### 1.2 ClickHouse ClickBench ORDER BY audit

`olap_benchmarks/suites/clickbench/schemas/clickhouse.sql` already uses
`PRIMARY KEY (CounterID, EventDate, UserID, EventTime, WatchID)` with
`ENGINE = MergeTree`, which matches the official ClickBench. Action:

- [ ] Confirm `PRIMARY KEY` here is interpreted as both the primary key and
      `ORDER BY` (it is, in MergeTree, when `ORDER BY` is omitted). If we
      want to be explicit/auditable, change to:
      ```sql
      ENGINE = MergeTree
      ORDER BY (CounterID, EventDate, UserID, EventTime, WatchID)
      SETTINGS old_parts_lifetime = 5;
      ```
      and drop the inline `PRIMARY KEY`.
- [ ] Verify auto-created tables (time_series via
      `Clickhouse.insert._get_order_by_columns`) use a sensible ORDER BY.
      `time_series` ends up `ORDER BY (time)` because `not_null = {"time"}`,
      which is fine for that suite's queries. Document this.
- [ ] Add a short comment block at the top of each clickhouse schema file
      explaining the ORDER BY choice and citing the official ClickBench
      schema URL.

### 1.3 PostgreSQL GUC parity for ClickBench

`schemas/postgres.sql` for ClickBench currently has no GUC tuning (just the
`CREATE TABLE`); `index_table()` runs the indexes. The TimescaleDB schema
sets `work_mem = 1GB` and `min_parallel_table_scan_size = 0` at the
**database** level. We want Postgres to get the same treatment.

- [ ] Edit `olap_benchmarks/suites/clickbench/schemas/postgres.sql` to append:
      ```sql
      ALTER DATABASE postgres SET work_mem TO '1GB';
      ALTER DATABASE postgres SET min_parallel_table_scan_size TO '0';
      ```
- [ ] Audit other Postgres schemas (`rtabench/schemas/postgres.sql`,
      `kaggle_airbnb/schemas/postgres.sql`, time_series Postgres) — apply
      the same `ALTER DATABASE` settings if they aren't there. Goal: any
      tuning given to TimescaleDB also given to plain Postgres so the
      comparison is fair.
- [ ] Note in CLAUDE.md/OUTPUT.md that the Postgres container is
      otherwise stock; we are not tuning shared_buffers / wal_buffers /
      effective_cache_size beyond what TimescaleDB does automatically.
      (If we later want to: add a startup `-c` to the docker command in
      `olap_benchmarks/dbs/postgres/__init__.py:start`.)

### 1.4 Increase ClickBench iterations 3 → 5

- [ ] `olap_benchmarks/suites/clickbench/config.py` — change `ITERATIONS =
      3` to `ITERATIONS = 5`. Matches time_series and rtabench.

### 1.5 Drop or expand kaggle_airbnb (decision)

OUTPUT.md §5.4 / §7.10: too small, too few queries, overlaps with rtabench.

- [ ] **Decision needed**: drop the suite or expand it. Default
      recommendation: drop. It will save ~30 minutes per re-run and remove a
      data point that doesn't carry signal.
- [ ] If dropping: remove `kaggle_airbnb` from `SuiteName` literal in
      `settings.py`, delete `olap_benchmarks/suites/kaggle_airbnb/`, remove
      `kaggle_airbnb` property from `Database`, drop the directory in
      `_build_queries_manifest`, update `__main__.py` preparer registry,
      drop site references.
- [ ] If keeping: add at least 5–10 more queries (analytical aggregations,
      window functions, geographic filtering on listings) and document the
      data prep step.

---

## 2. Documentation, methodology, transparency

### 2.1 Methodology section in README / new METHODOLOGY.md

- [ ] Hardware specs: MacBook Pro M4, RAM, SSD model + speed, macOS version,
      Docker / OrbStack version.
- [ ] Container resource limits (currently none — document that, including
      the implication that Docker can use the full machine).
- [ ] Restart strategy: every populate ends with `restart_event()` to ensure
      cold cache for the first select iteration.
- [ ] Measurement: wall-clock from `perf_counter` around `db.fetch`,
      including result serialization to Polars; this is what
      `run_step.metadata.duration_ms` records.
- [ ] DB-by-DB CREATE TABLE / index / GUC summary — render automatically
      from `schemas/*.sql` if possible (write a small one-shot script in
      `scripts/` that walks `olap_benchmarks/suites/*/schemas/*` and dumps a
      markdown table). This is the `Document all database configurations`
      action.
- [ ] Acknowledge that DuckDB runs in-process; everything else runs in
      Docker; document the implication.
- [ ] Acknowledge ClickBench's ClickHouse origin and RTABench's Timescale
      origin. Note that DuckDB still wins ClickBench, undercutting the
      "biased benchmark" criticism (OUTPUT.md §6.9).

### 2.2 README "What's measured" / "Limitations"

- [ ] Add a short up-front summary: single-node, single-system, embedded /
      local OLAP. Not distributed. Not cloud. Polars-DataFrame round-trip
      included in measurement.
- [ ] List the explicit per-engine schema choices (wide vs EAV, ORDER BY,
      indexes, compression).

### 2.3 Document the EAV split

- [ ] In README or METHODOLOGY.md, add a "Time-series schema: wide vs EAV"
      paragraph explaining why columnar engines keep the 1500-col schema
      while Postgres/TimescaleDB are tested in EAV. Frame it as the headline
      insight, per OUTPUT.md §5.3.

---

## 3. Web app (`site/`) updates

These follow the data-model changes above.

- [ ] If `data_wide`/`data_large` row counts diverge per-engine (EAV change),
      ensure any "rows ingested" / "rows/sec" UI handles per-engine variance
      without crashing or implying inequivalence. The `expected_row_counts`
      change in §1.1 is the single source of truth — re-derive whatever the
      UI shows from `run_step.metadata` / `Run.row_count`.
- [ ] Add a brief in-app note next to time-series large/wide results
      explaining the EAV schema for Postgres/TimescaleDB. Use existing
      `Typography` primitives; don't introduce a one-off styled box.
- [ ] (Optional) Query-category tagging UI per OUTPUT.md §7.11: each query
      gets a tag like `scan | aggregate | join | window | text | point`.
      Define the tag map in code (e.g. `site/src/content/queryCategories.ts`
      or a JSON manifest emitted by `_build_queries_manifest`) and let the
      results page filter by category. Defer if scope is tight; this is a
      "nice to have," not a release blocker.

---

## 4. Re-run procedure

After §1 changes are in and tested individually with `--revision test`:

- [ ] Drop test data: `olap docker all all stop`, `rm -rf data/dbs data/temp`.
- [ ] `olap prepare all` (regenerate inputs as needed; should be idempotent).
- [ ] `olap benchmark-all --revision release --cleanup` — full re-run, every
      db × every suite × every operation, with per-(db,suite) cleanup so
      each combo starts fresh. Expect this to take many hours.
- [ ] During the run, **leave the QuestDB → `default` revision benchmark
      alone**. Do not run questdb in `release` until the user has merged or
      retired the QuestDB results.
- [ ] On completion, sanity-check with `olap results runs --revision
      release --status failed`. Resolve any failed runs (most likely OOM,
      docker disk pressure, port collisions) and re-run only the failed
      `(db, suite, operation)` combos via individual `olap benchmark`
      invocations.

---

## 5. Post-run analysis

- [ ] Regenerate the OUTPUT.md tables from `release.db`. The structure can
      stay; only the numbers change. Keep the EAV anomaly section as
      historical context but mark it "resolved by EAV — see methodology."
- [ ] Cold-vs-warm reporting (OUTPUT.md §7.7): in the analysis, separate
      iteration 1 (cold) from iterations 2–5 (warm). Implementation: the
      query already records `iteration`; the page / analysis just needs to
      pivot on it. Add a column or toggle to the results page.
- [ ] Resource consumption: `run_metric` already captures CPU/RAM/disk per
      run. Add a small section to the results page summarizing peak RSS and
      mean CPU per (db, suite, operation). Useful for the cost-of-query
      narrative, OUTPUT.md §7.14.

---

## 6. Out of scope for this release (recorded so we don't forget)

- Distributed/cluster benchmarks (OUTPUT.md §6.5).
- Real-world public time-series dataset (NYC taxi etc.) as a fifth suite
  (§7.13).
- New engines beyond the existing six (§6.8).
- The README TODO "concurrent insert + select" stress test for time_series.
- QuestDB integration changes — the user has an in-flight benchmark.

---

## 7. Suggested execution order (single sitting)

1. §1.3 (GUC) — one-line edits, low risk, lets us start re-running while
   bigger changes are still in flight.
2. §1.4 (ClickBench iterations) — one-line edit.
3. §1.2 (ClickHouse ORDER BY) — schema-only edit, validate with `olap
   benchmark clickhouse clickbench --revision test --cleanup` for one
   query.
4. §1.5 (kaggle_airbnb decision + drop, if dropping) — clears out one
   benchmark dimension before the bigger lift.
5. §1.1 (EAV) — the largest change. Implement Postgres first, validate,
   then mirror to TimescaleDB. Compress tuning is iterative; expect 2–3
   `--revision test` cycles.
6. §2 (docs) — once schema/code are settled.
7. §3 (web app) — once data shape is settled.
8. §4 (full re-run on `release`).
9. §5 (analysis + OUTPUT.md regen).
