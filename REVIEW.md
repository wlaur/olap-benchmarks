# Project review — 2026-07-06

Holistic review of architecture, ease of use, and benchmark methodology fairness.
Covers the Python harness, the DB connectors, the suites, and the published
`site/public/data/results.db` (analyzed directly with DuckDB).

**TL;DR:** the architecture is solid, but the published results are not currently
apples-to-apples. The biggest problem is that five of seven databases run under x86
emulation on the M4 while StarRocks and DuckDB run natively, and the published data
contains several queries where engines return *different results* for the "same" query.

---

## 1. Fairness audit — issues that materially skew results

### 1.1 Platform asymmetry (critical, affects every suite)

- [ ] Fix platform flags and re-run all benchmarks natively

`docker_run_command` defaults to `platform="linux/amd64"` (`olap_benchmarks/dbs/__init__.py:102`),
so ClickHouse, MonetDB, and QuestDB inherit emulation, and Postgres/TimescaleDB hardcode it
(`postgres/__init__.py:456`, `timescaledb/__init__.py:313`). StarRocks alone runs
`linux/arm64` (`starrocks/__init__.py:214`, pinned to 4.0.9 because the 4.1.0 BE segfaults),
and DuckDB is in-process native.

On an M4, Rosetta/QEMU costs tens of percent — worse for SIMD-heavy engines like ClickHouse,
which loses its AVX paths entirely. This plausibly explains why ClickHouse looks weak vs
StarRocks/DuckDB in the tpch_sf10 numbers. ClickHouse, MonetDB, Postgres, TimescaleDB, and
QuestDB all ship arm64 images — switch them to native, re-run, and record the platform on
each run so this can't silently recur.

### 1.2 Engines disagree on query results — nothing validates them

- [ ] Add cross-engine row-count assertion after suite runs
- [ ] Investigate/fix the divergent queries below, then re-run

Found by comparing `row_count` per query step in `site/public/data/results.db`:

| Suite | Query | Divergence |
| --- | --- | --- |
| clickbench | Q28 | MonetDB and StarRocks return **11 rows vs 25** everywhere else, despite byte-identical SQL — their `REGEXP_REPLACE` handles the `(?:…)` group / `\1` backreference differently, so they group on a different key |
| time_series | `wide_10/11_scalar_lookup` | TimescaleDB returns **0 rows** (timed on finding nothing) vs 1 elsewhere |
| time_series | `*_null_gap_detection` | TimescaleDB: 4542 vs 6573 (large), 4687 vs 5217 (tall), 3717 vs 3529 (wide) |
| time_series | `*_raw_filtered`, `large_23_batch_export` | TimescaleDB off by dozens of rows (e.g. 9997 vs 10012) |
| time_series | `*_06_daily_resample` | ClickHouse: 2780 vs 2808 (large), 1394 vs 1420 (tall), 140 vs 169 (wide) — looks like date-bucket boundary/timezone semantics |

Something is wrong in the TimescaleDB EAV/hypertable query variants specifically.

The harness only checks row counts against the source parquet at populate
(`suites/__init__.py:42-128`); there is no cross-engine result comparison anywhere.
Cheapest meaningful fix: assert per-query `row_count` matches across DBs after a suite run
(the data is already stored). Value-level validation against a golden engine (DuckDB) was
done by hand for TPC-H per commit `3898f29` — automate it.

QuestDB's ClickBench queries are also semantically risky rewrites (`<> ''` → `IS NOT NULL`,
`EventDate` range → `EventTime` range, `SAMPLE BY`) and unvalidated — though its published
row counts currently match.

### 1.3 Tuning/physical-design asymmetry with no stated policy

- [ ] Decide and document a tuning policy; publish per-db tuning notes on the site

Individually defensible (some mirror upstream ClickBench/StarRocks practice), but
collectively there is no rule for who gets optimized:

- **ClickBench Postgres**: 16 btree + 2 GIN trigram indexes (`postgres/__init__.py:121-162`) —
  the trigram indexes directly serve the `LIKE '%…%'` queries — plus reordered columns.
  ClickHouse, MonetDB, QuestDB, StarRocks: no indexes, image defaults.
- **Server tuning only for the Postgres family**: `shared_buffers=8GB`, `work_mem=4GB`,
  `synchronous_commit=off`, parallel workers (`postgres/__init__.py:471-483`;
  timescale via `timescaledb-tune` + flags at `timescaledb/__init__.py:330-343`).
  ClickHouse/MonetDB/QuestDB/StarRocks get zero server config.
- **TPC-H**: `lineitem(l_partkey, l_suppkey)` index only for Postgres + TimescaleDB
  (`schemas/postgres.sql:99`, `schemas/timescaledb.sql:98`, commit `c01719f`).
- **TPC-H Q05**: hand-ordered join chain for ClickHouse only
  (`queries/clickhouse/05_local_supplier_volume.sql`, commit `3898f29`); everyone else's
  planner is trusted with the comma-join form.
- **MonetDB**: hand-picked binary fetch path on exactly its expensive queries
  (`monetdb/__init__.py:46-53`, `:75-87`), while Postgres/StarRocks stay on slow
  row-by-row fetch for the same queries.
- **QuestDB**: ClickBench parquet pre-sorted by EventTime on the host before ingest
  (`questdb/__init__.py:74-79`).
- **RTABench**: Postgres gets 3 targeted secondary indexes (`postgres/__init__.py:100-108`),
  TimescaleDB gets hypertable + compression + chunk skipping; the rest get nothing.
- **time_series**: Postgres/TimescaleDB use an EAV layout (structurally different schema);
  Postgres additionally skips `data_large` entirely (`postgres/__init__.py:308`).

Suggested policy: "vendor-recommended config plus per-suite physical design is allowed,
and every deviation is listed on the site."

### 1.4 DuckDB's structural advantages

- [ ] Cold-start DuckDB before select (reopen file / drop caches) or document the difference
- [ ] Consider Arrow-native fetch paths for Postgres/TimescaleDB/StarRocks where possible

- Never restarted (`start/stop/restart` are `None`, `duckdb/__init__.py:66-76`), so it is the
  only engine whose buffer manager stays warm from populate into select; every container DB
  gets a deliberate cold restart after populate (`restart_event`, `dbs/__init__.py:337`).
- Zero-copy `con.pl()` fetch vs everyone else's network round-trip. Fetch/deserialization is
  **inside the timed region** for all engines (`dbs/__init__.py:301-335`); Postgres,
  TimescaleDB, and StarRocks pay row-by-row Python materialization, ClickHouse gets
  HTTP+Arrow, QuestDB gets connectorx→Arrow.
- Its cpu/mem metrics come from the main Python process via psutil, not `docker stats`
  (`metrics/measure.py:18`, `:110-115`) — not comparable with the container DBs.

### 1.5 Published-data hygiene (found in `site/public/data/results.db`)

- [ ] Don't insert a run row when `should_populate()` skips; delete run 77 and republish
- [ ] Re-run older suites so all DBs share iteration counts and MonetDB versions
- [ ] Run QuestDB on rtabench/time_series/kaggle_airbnb or note why absent

- **ClickHouse tpch_sf10's displayed populate is a 0.5s "populate skipped" run** (run id 77;
  the real load was 35.7s in run 68). The site shows the latest completed run per
  (db, version, operation), so skip-runs mask real ones.
- **Iteration-count vintage drift**: March/early-April ClickBench runs used 3 iterations;
  the April 25 QuestDB/StarRocks runs used 5 (current code: `ITERATIONS = 5`,
  `clickbench/config.py:16`). Medians over 3-with-cold vs 5-with-cold are not comparable.
- **MonetDB is Dec2025-SP1 in four suites and Dec2025-SP2 in tpch_sf10**; the home page's
  cross-suite geomean keys rows by label, which collapses to just "monetdb" when only one
  version exists per suite scope (`queries.ts:82-86`) — silently mixing versions.
- **QuestDB exists only in ClickBench results** (TPC-H is legitimately unsupported —
  no correlated subqueries — but rtabench/time_series/kaggle_airbnb are just unrun).
- Run-to-run variance is real: the two QuestDB ClickBench select runs total 152s vs 245s;
  "latest run wins" has no stability guard.

### 1.6 Site aggregation and display

- [x] Fix TPC-H SQL panel: `queries.json` keys TPC-H as `"tpch"` but the site looks up
      `queriesManifest[suiteConfig.id]` (`site/src/components/explorer/OperationTab.tsx:78`,
      `QueryHeatmapPanel.tsx:58`) — SQL never displays for tpch_sf10/tpch_sf50
      *(fixed 2026-07-07: `SuiteConfig.queriesKey` maps suite ids to queries.json keys,
      covers tpch and the new tpcds suite)*
- [ ] Document cold-iteration inclusion (or exclude iteration 1) in scoring
- [ ] Penalize (or at least flag) skipped/missing queries in the score
- [ ] Distinguish "failed" from "never attempted" in the UI
- [x] Update `site/src/content/home.md` — TPC-H suites are missing from the suite list
      *(fixed 2026-07-07: TPC-H and TPC-DS suites added to the home suite list)*

Mechanics (from `site/src/lib/queries.ts`, `score.ts`, `useHomeOverview.ts`):

- Run selection: latest completed run per (db, db_version[, operation]) by `finished_at`
  (`queries.ts:97-107`). Failed runs are invisible — the DB just vanishes from the suite.
- Headline metric: **median over all iterations including the cold first one**
  (no `iteration > 1` filter; `queries.ts:189-226`). With 3 iterations the cold run is one
  of three median inputs. `home.md` says only "median over multiple iterations."
- Score: geometric mean of per-query ratios vs the fastest, **only over queries the DB
  completed** (`score.ts:41-52`) — so Postgres skipping the `data_large` time_series
  queries *improves* its score with no penalty and no visible flag.
- Suite-level worst-score substitution for entirely missing suites (`useHomeOverview.ts:60-76`)
  doesn't catch partial query skips.
- Populate/select bars use whole-run wall time (`finished_at − started_at`), which includes
  schema, verification, index build, and post-load optimize — fine, but engines do very
  different amounts of post-load work (Timescale compress+vacuum, ClickHouse OPTIMIZE,
  Postgres indexes+VACUUM ANALYZE, DuckDB/StarRocks/QuestDB ~nothing).
- Minor: `wins` uses exact float equality (`score.ts:42`); median over 3 samples is fragile.

---

## 2. Architecture / ease of use

The CLI design itself is good — `prepare/benchmark/publish` with `all` matrices is the right
shape. The forgetting-commands problem was documentation, not design (README now has the
runbook). Worthwhile improvements:

- [ ] **Make db_version genuinely first-class.** It's in the schema and the merge natural key
      (`results/merge.py:14`), but versions are hardcoded constants scattered across seven
      connector files, never verified against the server, and there's no way to select one at
      run time. Suggest: one `versions.toml` (or env overrides), verify `SELECT version()`
      against the pin at startup, fail the run on mismatch. DuckDB's version follows the
      installed package (`duckdb/__init__.py:19-21`) — fine, but document it.
- [x] **Make suite scale factor first-class.** The run schema now stores non-null
      `suite_scale_factor`, merge keys include it, legacy `tpch_sf*` / `tpcds_sf1`
      rows migrate to `tpc_h` / `tpc_ds` plus scale, and the web UI filters by
      both database version and suite scale factor.
- [ ] **Record methodology metadata on the run**: platform (arm64/amd64/emulated), iteration
      config, tuning applied. This is what would have caught the emulation asymmetry and the
      3-vs-5 iteration drift.
- [ ] **Document and support a remote-run merge workflow.** There should be a clean way to run
      benchmarks on another host, commit or transfer that revision's results DB through a normal
      Git workflow, and merge those runs into the published `site/public/data/results.db` without
      overwriting local/published history.
- [x] **`olap prepare all` crashes** on clickbench's `NotImplementedError`
      (`clickbench/config.py:19-23`) — make manual-download suites skip with an
      instructive warning instead.
      *(fixed 2026-07-07: manual-preparation suites raise `ManualPreparationRequired`;
      `prepare all` skips them with a warning, explicit suite preparation still fails
      with the manual download instructions)*
- [ ] **Suite registry is quadruplicated**: Python `SuiteName` (`settings.py:23`),
      `site/src/lib/benchmarks.ts`, `site/src/lib/suiteConfig.ts`, and hand-written
      `home.md`. A mismatch silently yields "No completed runs". Publish a `suites.json`
      next to `queries.json` and drive the site from it.
- [ ] No unique constraint on the run natural key (`results/models.py:41-45`) — merge logic
      is the only guard against duplicates.
- [ ] `results/new.db` looks like a stale revision; delete or rename meaningfully.

---

## 3. Suggested priority order

1. Fix platform flags → re-run everything natively (invalidates most current numbers anyway;
   also fixes iteration drift and MonetDB version mixing in one sweep).
2. Add the cross-engine row-count assertion; fix the TimescaleDB time_series and
   Q28 regex divergences before trusting those suites.
3. Fix skip-populate run recording; delete run 77; republish.
4. Fix the `tpch` queries.json key mismatch in the site.
5. Decide + document the tuning policy and cold-iteration handling on the site
   (`home.md` + score explainer).
6. DX items: version pins in one place with server-side verification, run metadata,
   `prepare all` robustness, `suites.json`.

---

## Reference: methodology facts (as of this review)

- Timed unit: `perf_counter` around `fetch()` into polars — client materialization included
  (`dbs/__init__.py:301-335`).
- Iterations (all recorded, none discarded): clickbench 5; tpch 3; rtabench 5 (two queries
  3/2); kaggle_airbnb 3 (calendar_count 10); time_series 5. Restart only after populate;
  select runs warm; no restarts between queries/iterations.
- Mutate grid (time_series only): {insert, upsert, delete} × {tall, wide, large} × {1, 100,
  10k rows} × 3 iterations. QuestDB: inserts only; Postgres: no data_large.
- Verification: per-table row count vs source parquet at populate only.
- No docker `--memory`/`--cpus` limits for any DB; no query-result caches enabled anywhere.
- Site: DuckDB-WASM reads `results.db`; latest completed run wins; median (cold included)
  drives heatmap/score; geomean-of-ratios score over completed queries only.
