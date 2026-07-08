# Project review — 2026-07-08

Follow-up to the 2026-07-06 review, covering everything since `c337047`: suite
scale factor as a first-class dimension (`898705d`), the TPC-DS suite
(`e52828a`…`d0d7173`), run natural-key enforcement (`fae3a29`), post-select
row-count validation (`f973381`), failed-query coverage in the site (`349ebef`),
and the ConnectorX fetch switch (`8703ed0`). Items from the previous review that
are verified done have been removed (see the list at the bottom); the published
`site/public/data/results.db` was re-audited directly.

**TL;DR:** the scale-factor and TPC-DS work is solid. The run identity
(system, db, db_version, suite, suite_scale_factor, operation) is enforced
end-to-end — schema, merge, row-count validation, data directories, and every
site query — and no code path in the harness or web app can compare results
across different scale factors or systems. The TPC-DS ClickHouse rewrites are
semantically equivalent, not hand-tuning. What remains: the platform asymmetry
from the last review is untouched and still undermines the published numbers,
every previously found divergent query result is still present, and the site
has db-version labeling/keying gaps that bite once multiple versions or
failed-only variants coexist in a scope.

---

## 1. Dimension handling (db+version, suite+scale factor, system)

### Verified correct

- Run identity: unique index `uq_run_natural_key` on (system, db, db_version,
  suite, suite_scale_factor, operation, started_at)
  (`results/models.py:45-55`), the same tuple as the merge natural key
  (`results/merge.py:14`). Runs at different scale factors always merge as
  additions, never replacements (`tests/test_results_merge.py:176-192`).
- Row-count validation partitions and joins on (system, suite,
  suite_scale_factor) at every stage (`results/validation.py:84,116,131-135`) —
  no cross-scale or cross-system comparison is possible. Cross-db and
  cross-version comparison within one scope is the intended check and fires
  only when ≥2 distinct (db, version) disagree.
- Data/DB/temp directories are keyed per scale factor (`settings.py:56-60`,
  `dbs/__init__.py:72-80`), so populate verification counts always come from
  the same-SF parquet as the recorded run.
- Every site query goes through shared CTEs filtered by exactly one (system,
  suite, suite_scale_factor) (`site/src/lib/queries.ts:113-118,159-163`); the
  explorer has a per-suite scale selector, and the home table scores each suite
  at its default SF only and says so in the blurb. Nothing in the web app mixes
  scale factors or systems.
- The published DB is clean on the new dimensions: zero legacy
  `tpch_sf*`/`tpcds_sf1` suite names, zero NULL/invalid scale factors, no db
  with two versions inside one (suite, SF) scope.

### Open issues

- [ ] **Verify db_version against the running server.** Pins are still
      hardcoded constants in the connector files (`clickhouse/__init__.py:29`,
      `postgres/__init__.py:29`, `monetdb/__init__.py:29`, etc.); the stored
      `db_version` is never checked with `SELECT version()`, so an image-tag
      drift silently corrupts the natural key. One `versions.toml` (or env
      overrides) plus a startup assertion still recommended. DuckDB correctly
      derives its version from the installed package.
- [x] **Site: `db_label` is computed over two different run scopes.**
      `withLatestRuns` builds the version-disambiguation window over *completed
      populate/mutate/select* runs (`queries.ts:104-108`) while
      `withLatestAttemptedSelectRuns` builds it over *attempted select* runs
      (`queries.ts:148-152`). A version present in one scope but not the other
      (failed-only select, or populate-only) gets different labels in query
      summaries vs coverage: `coverageByDb` lookups miss (`score.ts:26,90-95`),
      phantom all-missing score rows appear (coverage dbs are added to the
      scored set, `score.ts:34-36`), and the filter chips list the same variant
      twice (`useSuiteData.ts:250-259`). Compute the label window once over a
      shared scope.
      Fixed 2026-07-08: labels now come from one non-running run-label CTE
      shared by completed-run and attempted-select paths. `queries.json` is now
      required for the explorer score path instead of silently falling back.
- [x] **Home table still keys rows by scope-local label.** `useHomeOverview`
      keys scores by `entry.db` (`useHomeOverview.ts:52-60`). With one version
      per suite scope the label collapses to bare `monetdb`, silently mixing
      Dec2025-SP1 (the four sf1 suites) with Dec2025-SP2 (tpc_h sf10) in a
      single row and in the overall geomean — confirmed still present in
      published data. Conversely, two versions inside one suite would split
      into rows that don't align across suites. Key by (db, db_version) and
      derive the label at render time.
      Fixed 2026-07-08: score rows carry stable `(db, db_version)` keys, the
      home overview indexes by those keys, and home labels are rendered from
      the global home-scope version set. Existing `site/public/data/results.db`
      was not regenerated.
- [ ] **Home shows each suite at its default scale factor only**
      (`useHomeOverview.ts:43-44`); a system whose data exists at a non-default
      SF shows "not run". The per-suite default SF is also now duplicated
      between `benchmarks.ts` and `suiteConfig.ts` (plus Python
      `DEFAULT_SUITE_SCALE_FACTORS` and `home.md` prose) — fold into the
      registry consolidation in §4 and consider showing the SF in the home
      column headers.
- [ ] **`benchmark|prepare|docker all … --scale-factor N` crashes** on the
      first fixed-SF suite: `resolve_suite_scale_factor` raises for SF≠1
      (`settings.py:50-51`) and nothing catches it
      (`__main__.py:128,165-167,241`). There is also no SF fan-out in the `all`
      matrix (`settings.py:30-37,70-79`) — running tpc_h at SF10 and SF50 takes
      two invocations. Decide whether "requested SF where supported, default
      elsewhere" should work.
      Partially fixed 2026-07-08: `suite=all` now fans out configured suite scale
      factors, currently adding time_series SF1 and SF10, and global
      `--scale-factor` uses fixed-suite defaults instead of crashing. Explicit
      invalid fixed-suite or time_series scale factors still fail. time_series
      SF10 is the previous implicit dataset size; SF1 is 10% of the previous
      total cell count with both rows and columns reduced, while retaining the
      referenced query columns. Static time-series lookup/filter dates were
      moved inside the new SF1 ranges. No benchmark data was generated in this
      fix. Caveat: TPC-H SF10+SF50 fan-out is still not configured.
- [ ] **Cross-system comparison is not possible in the UI.** Everything is
      scoped to the single selected system and `home.md` states systems are not
      compared — right as the default, but comparing the same (db, version,
      suite, SF) across systems is a stated goal. Needs an explicit compare
      view; the data model already supports it.
- [ ] **Row-count validation compares every (db, version) with a completed
      run, not the latest version per db** (`validation.py:82-88`). An old run
      from a since-fixed version will keep failing validation against fresh
      runs with no way to retire it. Confirm the intent; add a
      same-db-different-version test either way.
- [ ] **Test gaps on the new dimensions.** Nothing asserts that different
      scale factors or systems are *not* compared by validation
      (`tests/test_results_validation.py` fixtures all use one system and
      SF=1), and the SF migration tests cover only the `tpch_sf50` and
      `tpcds_sf1` branches (`tests/test_results_schema_updates.py:162-190`).
- [ ] **Migration hardening for the shared-revision lane.** The unique-index
      migration hard-fails with no dedup/remediation on legacy duplicate keys
      (`alembic/versions/4d9c2b7e6f10…:30-31`); the SF migration's ELSE branch
      keeps unknown legacy suite names verbatim with SF=1
      (`9c1e5f0a7b6d…:42-52`); its `downgrade()` is a silent no-op. Local
      history is clean, but `results/shared/*.db` files from other hosts will
      run these migrations.

Minor: `fetchSuiteScaleFactors` requires `finished_at` non-null
(`queries.ts:62`) while coverage tolerates failed runs without it, so an SF
with only such runs is unselectable; `suite_scale_factor` is INTEGER ≥ 1, so
sf0.1 is unrepresentable (fine unless sub-SF1 is ever wanted);
Fixed 2026-07-08: `current_suite_scale_factor` now guards on the SF attribute
(`dbs/__init__.py:67-70`), and score `wins` now uses a small duration tolerance
instead of exact float equality.

---

## 2. Fairness and methodology

### 2.1 Platform asymmetry (critical — unchanged since last review)

- [ ] Fix platform flags and re-run all benchmarks natively

Still: `platform="linux/amd64"` default (`dbs/__init__.py:123`), Postgres and
TimescaleDB hardcode it (`postgres/__init__.py:457`,
`timescaledb/__init__.py:313`), StarRocks alone runs arm64
(`starrocks/__init__.py:228`), DuckDB is in-process native. Everything else in
the published numbers sits on top of this. The native re-run also collapses
three other drifts in one sweep: the clickbench iteration mix, the MonetDB
SP1/SP2 split (both in §2.5), and the fetch-path discontinuity — `8703ed0`
switched Postgres/TimescaleDB/StarRocks to ConnectorX, so their timed client
path no longer matches the already-published runs.

### 2.2 Engines still disagree on query results

- [ ] Investigate/fix the divergent queries, then re-run

Re-audited against the current published DB — every previously reported
divergence is still present, plus small new ClickHouse ones:

| Suite | Query | Divergence |
| --- | --- | --- |
| clickbench | Q28 | MonetDB and StarRocks return **11 rows vs 25** everywhere else (`REGEXP_REPLACE` `(?:…)`/`\1` handling) |
| time_series | `wide_10/11_scalar_lookup` | TimescaleDB **0 rows vs 1** |
| time_series | `*_19_null_gap_detection` | TimescaleDB 4542/4687/3717 vs 6573/5217/3529; ClickHouse now also off by 1–2 (large 6575, wide 3530) |
| time_series | `*_09/22_raw_filtered`, `large_23_batch_export` | TimescaleDB short by dozens (e.g. 9997 vs 10012; 215799 vs 215833) |
| time_series | `*_06_daily_resample` | ClickHouse 2780/1394/140 vs 2808/1420/169; TimescaleDB also drifts (large 2785, wide 170) — date-bucket boundary semantics |

The TimescaleDB EAV/hypertable time_series variants remain the biggest cluster.
QuestDB's ClickBench rewrites (`<> ''` → `IS NOT NULL`, `EventTime` ranges,
`SAMPLE BY`) are still semantically risky but its published row counts match.
TPC-DS is the counterexample: ClickHouse vs DuckDB agree on all 99 queries.

### 2.3 Row-count validation is too weak for TPC-DS

- [ ] Add value-level validation against a golden engine (DuckDB)

84 of 99 TPC-DS queries end in `LIMIT 100`, so the post-select row-count check
(`validation.py:108-118`) only asserts "both returned 100 rows" for them — a
semantically wrong rewrite passes as long as it yields ≥100 rows. All eight
ClickHouse TPC-DS adaptations returned exactly 100 rows in published data.
Manual review finds the rewrites equivalent (§3), but the automated net barely
constrains them. Also: a query skipped via `UNSUPPORTED_QUERIES` records no
step and is invisible to validation — it relies on the scoring penalty alone.

### 2.4 Score universe is "queries somebody completed"

- [x] Score against the suite's full query list from `queries.json`

`computeDatabaseScores` builds the query universe from observed summaries
(`score.ts:24-33`), so a query that *no* scored db completed silently drops
out — no penalty, no display. With sparse suites (TPC-DS currently has two
engines) one shared unsupported query would simply vanish. `queries.json`
already carries the canonical per-suite query list.
Fixed 2026-07-08: `computeDatabaseScores` now requires the suite query list,
and both the home overview and explorer score cards pass the manifest-derived
names. Verified with `bun run verify` in `site/`.

### 2.5 Published-data hygiene (re-audited)

- [ ] Re-run older suites (folds into 2.1): clickbench iteration drift is
      confirmed still present (clickhouse/duckdb/monetdb/postgres/timescaledb
      at 3 iterations, questdb/starrocks at 5), as is the MonetDB version
      split (SP1 in the four sf1 suites, SP2 in tpc_h)
- [ ] Delete the 9 shadowed duplicate runs (7 natural keys with an older
      sibling: clickbench clickhouse/duckdb/questdb populate+select,
      time_series duckdb/postgres populate) — legal under the unique key since
      `started_at` differs, but dead weight that latest-wins must skip over
- [ ] TimescaleDB's time_series mutate run silently lacks all 3
      `insert_data_large_10000` iterations (79 mutation steps vs 82 on every
      other engine) — no failed step was recorded; find the cause and make
      unrecorded steps impossible or visible
- [ ] Finish and publish the Postgres TPC-DS run (populate exists, select
      missing) and note current TPC-DS coverage (duckdb + clickhouse only) in
      `home.md`
- [ ] **Record methodology metadata on the run**: platform (arm64/amd64/
      emulated), iteration config, tuning applied — the `run` table has no such
      columns (`models.py:21-58`). This is what would permanently catch
      2.1-style asymmetry and iteration drift.

Run-to-run variance still has no stability guard (latest run wins), and
DuckDB's cpu/mem metrics still come from psutil on the host process vs
`docker stats` for the container DBs — not comparable; both unchanged.

---

## 3. TPC-DS suite

Verified: all eight ClickHouse adaptations (q18/35/36/47/49/57/66/75) are
forced by ClickHouse limitations and semantically equivalent to the shared base
queries — decorrelations, alias-collision renames, and cast/COALESCE typing;
none is a hand-ordered join like the TPC-H Q05 case. The session settings
(`dbs/clickhouse/__init__.py:120-130`) restore standard-SQL semantics and come
from the official kit, documented in `tpc_ds/config.py:14-20`. q35 is adapted,
not skipped: the `f4935db` skip was reverted by `31d8142`, and published data
shows q35 completed on both engines with matching counts. The
Postgres/Timescale q01/q04 decorrelations are equivalent, documented in-file,
and applied to both engines symmetrically. Schemas are fair: spec primary keys
only, no secondary indexes for anyone, TimescaleDB deliberately without
hypertables, QuestDB unregistered with a documented reason. Prepare has
disk-space guards, stray-file rejection, and atomic per-table normalization,
covered by `test_tpcds_prepare.py`.

- [x] **`_is_normalized()` only checks `income_band`**
      (`tpc_ds/config.py:135-137`). A crash mid-normalization (income_band is
      table 10 of 24) leaves later tables unrenamed/uncast, and the rerun then
      skips normalization because the marker table passes. Check all 24 tables
      or write a completion marker after the loop.
      Fixed 2026-07-08: `_is_normalized()` now checks every TPC-DS table for
      expected spec renames and normalized decimal widths. Verified with
      `uv run pyright`, `uv run ruff check ...`, and
      `uv run pytest olap_benchmarks/tests/test_tpcds_prepare.py`.
- [ ] **No per-query fault isolation in `TpcDs.select()`**
      (`tpc_ds/config.py:282-304`): the first failing query aborts that db's
      whole 99-query run *and* the enclosing multi-db command
      (`dbs/__init__.py:338-345,560-567`), and the post-run row-count check
      never runs. `UNSUPPORTED_QUERIES` is empty for every engine, and
      MonetDB/StarRocks are registered with schemas but zero query overrides
      and zero runs — whether the base ROLLUP/INTERSECT/EXISTS queries run
      there is untested. Verify them or give them a skip list before someone
      loses a multi-hour run.
- [ ] Minor: `_normalization_exprs` blanket-casts every decimal to (7,2)
      except `p_cost` (`tpc_ds/config.py:119-125`) — correct per spec today,
      but an out-of-range value would overflow silently; assert source widths
      instead.

---

## 4. Architecture / refactor cleanups

- [x] **`should_populate()` swallows all exceptions**
      (`dbs/__init__.py:516-519`): any error → "populate anyway", hiding real
      schema/connection faults behind the fallback. Catch only the expected
      "no tables yet" cases.
      Fixed 2026-07-08: the outer blanket catch was removed; suite-level
      missing-table detection still returns `True`, while real
      `should_populate()` errors now propagate before a populate run is
      recorded. Verified with `uv run pyright`, `uv run ruff check ...`, and
      `uv run pytest olap_benchmarks/tests/test_database_benchmarks.py`.
- [ ] **Suite registry still quadruplicated, now with scale factors.** Python
      `SuiteName` + `DEFAULT_SUITE_SCALE_FACTORS` (`settings.py:23-37`),
      `site/src/lib/benchmarks.ts`, `site/src/lib/suiteConfig.ts` (both now
      also carry per-suite default SFs), and hand-written `home.md`. A mismatch
      still silently yields "No completed runs". Publish a `suites.json` next
      to `queries.json` and drive the site from it.
- [x] Minor: `questdb` deregisters TPC suites with `registry.pop(tpc_suite)`
      and no default (`questdb/__init__.py:364`) — a registry change turns it
      into a KeyError; `fetch_connectorx` records the unstripped query text
      while executing the stripped SQL (`postgres/__init__.py:517-519`).
      Fixed 2026-07-08: QuestDB uses tolerant `pop(..., None)`, and both
      Postgres-family and StarRocks ConnectorX paths record the stripped SQL
      they execute. Verified with `uv run pyright`, `uv run ruff check ...`,
      and `uv run pytest olap_benchmarks/tests/test_connectorx_fetch.py`.

---

## 5. Suggested priority order

1. Platform flags → full native re-run (also collapses the iteration drift,
   the MonetDB version split, and the ConnectorX timing discontinuity).
2. Fix the TimescaleDB time_series and Q28 divergences before that re-run;
   add value-level validation against DuckDB so the TPC-DS/TPC-H rewrites stay
   honest despite `LIMIT 100`.
3. db_version server verification + methodology metadata on runs.
4. Site correctness: unify the `db_label` scope, key the home table by
   (db, version), score against the `queries.json` universe.
5. TPC-DS robustness: `_is_normalized()` completeness, per-query fault
   isolation, then run MonetDB/StarRocks TPC-DS and publish Postgres select.
6. DX: `all --scale-factor` handling, `suites.json` consolidation, migration
   dedup path for shared revisions, cross-system compare view.

---

## Verified fixed since the 2026-07-06 review (removed from this document)

Cross-engine row-count assertion after select runs; tuning policy + per-engine
notes and DuckDB warm-cache caveat on the site; ConnectorX/Arrow fetch paths;
skip-populate runs no longer recorded and the masking run deleted; QuestDB
coverage note; TPC-H/TPC-DS SQL panel key fix (`queriesKey`); cold-iteration
scoring documented; missing-query score penalty; failed vs never-attempted
distinction in the UI; TPC suites in the home suite list; suite scale factor
first-class end-to-end; remote-run merge workflow (`results/shared/` +
`publish --merge`); `prepare all` manual-suite skip; unique run natural key;
stale `results/new.db` removed.

---

## Reference: methodology facts (as of this review)

- Timed unit: `perf_counter` around `fetch()` into polars — client
  materialization included. Fetch paths: ConnectorX for
  Postgres/TimescaleDB/StarRocks/QuestDB, HTTP+Arrow for ClickHouse, selected
  binary paths for MonetDB, zero-copy `con.pl()` for DuckDB.
- Iterations (all recorded, cold first iteration included in medians):
  clickbench 5; tpc_h 3; tpc_ds 3; rtabench 5 (two queries 3/2);
  kaggle_airbnb 3 (calendar_count 10); time_series 5. Restart only after
  populate; select runs warm; no restarts between queries/iterations; DuckDB
  never restarted.
- Published coverage (macbook-pro-m4, all 82 runs completed): clickbench sf1
  × 7 dbs; kaggle_airbnb/rtabench/time_series sf1 × 6 (no questdb); tpc_h sf10
  × 6 (monetdb SP2); tpc_ds sf1 — duckdb + clickhouse select, postgres
  populate-only.
- Verification: per-table row counts vs source parquet at populate;
  cross-engine per-query row counts after select runs within one
  (system, suite, scale factor); no value-level comparison anywhere.
- Mutate grid (time_series only): {insert, upsert, delete} × {tall, wide,
  large} × {1, 100, 10k rows} × 3 iterations. QuestDB: inserts only; Postgres:
  no data_large.
- No docker `--memory`/`--cpus` limits for any DB; no query-result caches.
- Site: DuckDB-WASM reads `results.db`; latest completed run per (system,
  suite, scale factor, db, db_version) wins; median over all iterations drives
  heatmap/score; geometric mean of per-query ratios with a ≥10×/2×-worst
  penalty for missing queries; failed vs never-attempted shown separately.
