# Release Plan

Status of the work to make `OUTPUT.md` action items real before the
release-revision re-run.

**Scope notes**

- We will re-run all benchmarks from scratch on a fresh revision (recommend
  `release`); `default.db` is reserved for the parallel QuestDB run and
  should not be touched.
- Single-system release: `system = macbook-pro-m4`.

---

## Done

- §1.1 EAV schema for Postgres + TimescaleDB time_series (`data_wide`,
  `data_large`). `data_tall` stays wide. Smoke-tested on synthetic data:
  all 62 time_series queries parse and return correct shapes; mutate
  insert/upsert/delete on EAV tables works; populate (chunked unpivot →
  COPY) verified against real Postgres and TimescaleDB containers.
- §1.2 ClickHouse ClickBench schema: explicit `ORDER BY (CounterID,
  EventDate, UserID, EventTime, WatchID)` with citation comment. Verified
  by applying the schema to a fresh container.
- §1.3 Postgres GUC parity: `work_mem = 1 GB` + `min_parallel_table_scan_size
  = 0` for ClickBench, `work_mem = 50 MB` for RTABench, matching what
  TimescaleDB already gets. Verified GUC values persist on a fresh
  connection after schema apply.
- §1.4 ClickBench iterations 3 → 5.
- CLI: `--omit <db>` flag added to `benchmark` and `benchmark-all`. Single
  command for the release run is
  `olap benchmark-all --revision release --omit questdb --cleanup`.
- QuestDB rtabench: passes end-to-end on `--revision test` (populate=137 s,
  select=491 s).
- QuestDB kaggle_airbnb: q04 has a QuestDB-specific override (no
  `array_agg` in QuestDB); the override drops the `reviewer_ids` column and
  preserves the JOIN+GROUP BY workload. q01–03 and q05 pass unchanged.
- QuestDB time_series mutate: upsert + delete steps disabled via
  `DISABLED_MUTATION_STEPS` because QuestDB has no row-level DELETE
  (verified against 9.3.5). Insert remains enabled. Rationale and
  rejected alternatives are captured in the block comment in
  `dbs/questdb/__init__.py`.

## Open

- §1.5 kaggle_airbnb keep/drop/expand decision — left as-is.
- §2 Methodology / README documentation — not started. Once written, this
  is where the EAV split, per-engine schema choices, and the in-process
  vs. Docker disclaimer should live.
- §3 Web app updates — not started. The EAV change makes
  `data_wide`/`data_large` row counts diverge per-engine; any UI showing
  raw row counts should be re-derived from `run_step.metadata` /
  `Run.row_count` rather than assuming uniformity.
- QuestDB time_series populate + select smoke run — pending; deferred while
  a manual `questdb-benchmark` container is in use. Static review expects
  it to work (1500-col tables fit under QuestDB's 2048-col limit;
  date_trunc / lag / stddev are all supported in 9.3.5).

## Pre-flight checklist for the release run

- [ ] Free disk: ≥150 GB available before starting. The EAV CSV staging
      for `data_large` peaks around 6 GB per chunk; previous attempts
      failed with "No space left on device" when QuestDB's 88 GB results
      directory plus the 9 GB `data_large.parquet` left ~67 GB free.
- [ ] Use `--cleanup` so per-(db, suite) state is removed after each
      combo, otherwise total disk use balloons.
- [ ] Pick a fresh revision (`release` recommended); leave `default.db`
      alone until QuestDB is retired or merged.
- [ ] After completion, check
      `olap results runs --revision release --status failed` and re-run
      only the failed combos with individual `olap benchmark` invocations.

## Post-run

- Regenerate `OUTPUT.md` tables from `release.db`. Structure stays; numbers
  refresh.
- Cold-vs-warm reporting: separate iteration 1 from 2–5 in the analysis.
  The `iteration` column already exists on `run_step`; just a pivot.
- Resource consumption: `run_metric` already captures CPU/RAM/disk per
  run. Surface peak RSS and mean CPU per (db, suite, operation) in the
  results page.

## Out of scope for this release

- Distributed/cluster benchmarks.
- Real-world public time-series dataset as a fifth suite.
- New engines beyond the existing six.
- README TODO "concurrent insert + select" stress test for time_series.
