# Remaining project work - 2026-10-10

Only unfinished work. Remove items when they are done; history lives in git.

## Where things stand

- Published: one system (`macbook-m4-pro`), three engines — ClickHouse, DuckDB, MonetDB (Dec2025-SP4, ADBC) —
  across all eight suites at SF1 (TPC-H and JSONBench at SF10). MonetDB has no JSONBench queries and skips
  `chat_threads` concurrent (server crash). Answer hashes are `canonical-v10`; the published DB is at the Alembic head.
- Connectors with no published results: Polars, Postgres, TimescaleDB, QuestDB, StarRocks, Doris. They carry ~2.7k
  lines and ~370 SQL files that every interface change has to touch.
- Higher scale factors (time_series/chat_threads SF10, TPC-H SF50) do not fit this machine's RAM and disk; they
  belong to the cloud campaign.

**Releasable** means: a published set produced by one commit, one system snapshot and one benchmark profile; every
published cell validated; a site that loads fast and states exactly what is being compared; and a repeatable
one-command path from "bump a version" to "published".

## 1. Release blockers

- [ ] **Publish one coherent campaign, and enforce it.** The current set mixes five commits (one dirty tree), two
      OS snapshots and three dates: ClickHouse/DuckDB RTABench were rerun on 2026-10-10, the rest is from 2026-08-04.
      Run the published matrix into a fresh revision from one clean commit, and make `publish` reject a set with more
      than one commit, dirty trees, more than one system snapshot or mixed profiles.
- [ ] **Extend the publish gate to every published engine.** `_validate_publishable_runs` only checks MonetDB
      (ADBC matrix, session baselines, hashes). Generalise it to "every cell of the declared matrix is present,
      completed, hashed and in cross-engine agreement" (see 3.1).
- [ ] **Make wrong-result marking order-independent and self-healing.** After every select, `__main__.py:324-343`
      compares the latest run per `(system, suite, sf, db, driver)` *ignoring `db_version`*, then writes
      `result_status='wrong_result'` permanently — and can mark the *other* engines when one stale run sits in the
      majority. Nothing clears it. Validate once at the end of a matrix run or at publish; recompute marks
      idempotently (clear, then apply); add `olap results validate [--mark|--clear]`.
- [ ] **Fix non-deterministic queries.** RTABench `1008_most_week_delayed_order` returns a different answer hash on
      each ClickHouse iteration (`ORDER BY delayed_orders DESC LIMIT 1` with ties). Add tie-breakers, and add a check
      that flags any query whose hash differs between iterations of one run.
- [ ] **Redeploy Pages when the data asset changes.** `publish --upload` replaces the release asset, but Pages only
      rebuilds on a push to `main`. Trigger the workflow from `publish --upload` (`gh workflow run`). The build
      already verifies the asset against `results_db_sha256`/`results_db_bytes` in `manifest.json`
      (`bun run verify-data`), so a mismatched asset now fails the build instead of deploying.
- [ ] **Record image digests for every engine.** The locally built MonetDB image has no RepoDigest, so
      `container_image_digest` is null for all MonetDB runs. Pull from the registry before a campaign (now that the
      image is published) or record the registry digest explicitly; refuse to publish runs without one.

## 2. Methodology

- [ ] **Define and version a benchmark profile.** Today every suite has its own fixed iteration count (ClickBench 5,
      TPC-H/TPC-DS 3, RTABench 2-5 per query, Kaggle 3-10) and `classify_iteration_role` only produces `first_run` and
      `warm` (`steady_state` is declared but never set). Define one profile: restart/reopen once before select, N
      unscored warm-up iterations, M measured; store a profile id on each run; have the site refuse to compare runs
      with different profiles.
- [ ] **Name the timed boundary.** The timer covers `fetch()` through materialisation into Polars. Label it
      end-to-end latency on the site, stop showing `first_run_duration_s` as if it were cold, and add engine-internal
      timing only as a separate, clearly labelled metric.
- [ ] **Show absolute latency next to the home score.** The ranking now says scores are relative to the engines
      shown; the home table still has no absolute latency beside each score.
- [ ] **Finish the reproducibility identity.** Already recorded: repo commit (editable installs only), image IDs,
      input manifest hashes, query text. Missing: query/schema hashes, profile revision, iteration policy, and a repo
      commit for non-editable installs. Scope site comparisons by system snapshot, not just `system`.
- [ ] **Decouple input hashing from populate.** Hashing the inputs right before populate warms the page cache, which
      favours engines that read the files directly. Hash in a separate step (or cache hashes by size+mtime) and drop
      the cache before populate on Linux.
- [ ] **Account for planned-but-never-started steps.** An interrupt marks running steps failed, but planned steps
      that never started leave no row; coverage then undercounts the denominator.

## 3. CLI and architecture

Ordered by how much they reduce the cost of rerunning and publishing.

- [ ] **3.1 Declare the matrix in code and add `olap matrix`.** The intended matrix exists only for MonetDB
      (`dbs/monetdb/manifest.py`); everything else is implied by `db × suite` plus hidden rules
      (row-store engines skip TPC-H/TPC-DS, scale-factor fan-out differs with `suite=all`). Add one declaration
      (`{db: [(suite, sf, operations)]}`, with a `published` flag per engine) and:
      - `olap matrix status [--db X] [--revision R]` — planned vs latest completed run per cell: missing, failed,
        stale version, stale commit.
      - `olap matrix run [--db X] [--missing-only] [--keep-going]` — replaces hand-written shell loops, resumes a
        partial campaign, validates once at the end, prints a summary table and exits non-zero on any failed cell.
- [ ] **3.2 Replace superseded runs without run-id lists.** The merge key includes `started_at`, so reruns are added
      next to old runs; the site hides them, the file keeps them. Add `olap results prune --superseded [--dry-run]`
      (keep the latest completed run per cell) and a `publish --merge` mode keyed on the cell, not the start time.
- [ ] **3.3 One version manifest.** Pins are `VERSION` constants in six connector modules, a dataclass for MonetDB,
      and `pyproject.toml` for DuckDB/Polars. Move them to one `versions.toml` (version, image tags, expected digest)
      and add `olap versions [--check] [--outdated]` that pulls/verifies images and lists newer upstream releases.
- [ ] **3.4 One way to declare unsupported queries.** There are four: reasoned dicts (`UNSUPPORTED_QUERIES`), a bare
      frozenset (`tpc_ds/config.py:263`), a Postgres-only set, and "the SQL file is missing" (generic reason). Answer
      comparison exclusions live in yet another list (`validation.py:17`). Use one registry
      `{(suite, db, query): reason}` that `matrix status` and the site both read.
- [ ] **3.5 Deduplicate query SQL.** RTABench has no shared base: 268 files, 159 distinct texts. chat_threads has 11
      overrides identical to the shared file. The "engine file, else shared file" lookup is reimplemented in six suite
      configs. Move resolution into `BenchmarkSuite`, give RTABench a shared directory, and add a test that fails on an
      override identical to its base.
- [ ] **3.6 Collapse suite metadata into one spec.** Suite properties are ~10 parallel dicts held together by asserts
      (`settings.py:43-120`) plus per-suite branches in `resolve_suite_scale_factor`; TPC-H/TPC-DS scale factors are
      not validated. Use one `SuiteSpec` per suite.
- [ ] **3.7 Load settings lazily.** `SETTINGS` is validated at import from `.env` in the current directory, so any
      command outside the repo root fails and pytest from a subdirectory errors at collection. Resolve `.env` from the
      repo root, and record the effective benchmark-affecting settings (e.g. `OLAP_BENCHMARKS_MONETDB_*`) in run
      metadata.
- [ ] **3.8 Live smoke tests.** Only one test can touch a live engine (MonetDB ADBC, env-gated). Add an opt-in
      `pytest -m live` that starts each published engine and runs every select query at the smallest scale, so a
      broken dialect file or engine upgrade fails in minutes instead of mid-campaign.
- [ ] **3.9 Decide the unpublished engines' future.** Either put them behind an `experimental` flag excluded from
      `all` and from interface-change obligations, or publish them. Known state after the 2026-10-10 version bump:
      - StarRocks 4.1.6 now starts on Apple Silicon (the 4.1.0 segfault is gone), but TPC-DS 49, 70 and 86 fail
        with new analyzer errors (`ORDER BY clause cannot contain grouping`, ambiguous column).
      - Doris 4.1.4.1 RTABench populate fails: stream load cannot reach the BE on port 8040 — readiness only waits
        for the FE.
      - Postgres, TimescaleDB and QuestDB: smoke-tested on time_series (see the 2026-10 campaign notes).

## 4. Site: drop DuckDB-WASM for publish-time JSON

The site runs six fixed queries (systems, suite/scale factors, per-query summaries, coverage, catalog dimensions,
explorer metrics) over `run` and `run_step` only; there is no ad-hoc SQL (CodeMirror only highlights static query
text). Yet a cold visit downloads ~12.4 MB before showing a number — `duckdb-eh.wasm` 7.9 MB and `results.db`
4.1 MB on the wire (GitHub Pages gzips the file, so DuckDB cannot use range reads and fetches all of it) — and
holds 38 MB in WASM memory. Everything actually displayed is ~331 KB of JSON (35 KB gzipped). The published file is
also half free blocks (38 MB; 15.5 MB when compacted).

- [ ] **Precompute the views at publish time.** Move the six queries (and the "latest run per cell" and `db_label`
      logic) into Python, emit `index.json`, `summary/<system>/<suite>-<sf>.json` and `explorer/<suite>.json` from
      `olap publish`, define the payloads once as pydantic models and test them with pytest.
- [ ] **Generate TS types from those models** and turn `queries.ts` into `fetch().json()` with the same return
      types, so hooks and pages are unchanged. Delete `duckdb.ts`, Kysely, `generate-db-types.mjs`,
      `generated/db.ts`, and the duckdb-wasm / node-duckdb dependencies.
- [ ] **Ship the raw database only as a download.** Keep a compacted `results.db` on the release for people who want
      the raw data; serve per-run metric series as small JSON/Parquet on demand if memory charts are added later. An
      interactive SQL page, if ever wanted, can lazy-load DuckDB-WASM on its own route.

Expected result: ~400 KB first load, data visible after one small request, a ~2 MB `dist/` instead of 111 MB, and
schema changes caught by Python tests at publish time instead of in the browser.

## 5. Engine coverage

- [ ] **Arc on time_series.** Arc (Basekick Labs, AGPL-3.0, DuckDB + Parquet, multi-arch `basekicklabs/arc` image)
      is a dedicated time-series database: MessagePack columnar ingest, SQL over HTTP with Arrow IPC results,
      API deletes, no documented UPDATE. Add it for time_series populate/select (mutate/concurrent only where Arc's
      delete API matches the workload), declared as a time_series-only engine in the matrix. Not every engine needs
      every suite.
- [ ] **Polars in the published matrix** for the suites it implements (ClickBench, JSONBench, TPC-H).
- [ ] **MonetDB JSONBench**, or record why not in the unsupported registry.
- [ ] **Re-test MonetDB `chat_threads` concurrent** on SP4; the SIGSEGV is unexplained and was last seen on 11.55.7.

## 6. Resource reporting

- [ ] **Stop calling cgroup charge server memory.** `measure.py:223` uses `memory_stats.usage`, which includes page
      cache. Record anon and file separately (cgroup `memory.stat`) and report anon (or engine RSS) as server memory.
- [ ] **Split disk into data and WAL**, and report peak and final separately (currently one `du` over the store).
- [ ] **Fix the client-memory claim for MonetDB.** `monetdb/adbc.py:28-30` keeps the Arrow table and the Polars
      frame alive together, and README:116 still says Polars adopts them zero-copy.
- [ ] **Show memory on the site** only once the above is fixed, with in-process engines (DuckDB, Polars) labelled as
      client-only.

## 7. Deferred

- [ ] **Cloud campaign** on one x86-64 Linux `i4i` host per `CLOUD.md`, including the higher scale factors this
      machine cannot run. Do it after sections 1-3 so the campaign is a single `olap matrix run`.
- [ ] **JOB**, after the profile from section 2 has been exercised by a public run.
- [ ] **TSBS and InfluxDB 3**, only as a complete time-series expansion (Arc first).

## Suggested order

1. Section 3.1-3.3 (matrix, prune, versions): they turn the rest into single commands.
2. Section 1, then a fresh campaign published through the new gate.
3. Section 4 (site), independent of the harness work and the biggest visible improvement.
4. Section 2 profile, then the cloud campaign; sections 5-6 fit around these.
