# Repository Review: `olap-benchmarks`

## Scope
- Reviewed benchmark orchestration, suite/database architecture, results persistence model, and current static site implementation.
- Goal aligned to your stated direction: many suites × many DBs, comparable outputs, DuckDB/JSON artifacts, static interactive results UI.

## Findings (ordered by severity)

### Critical

1. **Some DB-specific benchmark customizations are never executed due method-name mismatch**
- `ClickhouseClickbench` defines `populate_clickbench()` instead of overriding `populate()`, so `optimize_clickbench_table()` never runs.
  - `olap_benchmarks/dbs/clickhouse/__init__.py:96`
- `PostgresTimeSeries` defines `populate_time_series()` instead of `populate()`, so indexing flow is skipped.
  - `olap_benchmarks/dbs/postgres/__init__.py:311`
- `TimescaleTimeSeries` defines `populate_time_series()` instead of `populate()`, so custom schema/compression flow is skipped.
  - `olap_benchmarks/dbs/timescaledb/__init__.py:101`
- The framework calls `benchmark.populate` directly.
  - `olap_benchmarks/dbs/__init__.py:225`

Impact: benchmark comparability is likely wrong for affected DB/suite combinations.

2. **Failure-state accounting is inconsistent: failed runs can be marked completed**
- Sampler always records `finished_at` when stop event is set, even if benchmark body raised.
  - `olap_benchmarks/metrics/sampler.py:47`
  - `olap_benchmarks/metrics/sampler.py:48`
- Status logic treats `finished_at != NULL` as `completed`.
  - `olap_benchmarks/results.py:43`

Impact: downstream analysis and UI can report false positives.

3. **Delete semantics are contradictory (`deleted_at` exists but deletes are hard deletes)**
- Schema includes `deleted_at`.
  - `olap_benchmarks/metrics/schema.sql:10`
- `results delete` physically removes benchmark/event/metric rows.
  - `olap_benchmarks/results.py:483`
  - `olap_benchmarks/results.py:485`
- Status supports a `deleted` state that cannot occur after hard delete.
  - `olap_benchmarks/results.py:41`

Impact: lineage/auditability is lost, and status model is internally inconsistent.

### High

4. **Current extension model does not scale cleanly to many suites and many DBs**
- Suite and DB types are hardcoded in `Literal` unions.
  - `olap_benchmarks/settings.py:13`
  - `olap_benchmarks/settings.py:22`
- DB registry is hardcoded.
  - `olap_benchmarks/__main__.py:25`
- Suite registry per DB is hardcoded.
  - `olap_benchmarks/dbs/__init__.py:209`

Impact: each new suite or DB requires edits in multiple core files, raising coupling and maintenance cost.

5. **Event model is string-encoded, which makes analysis/filtering brittle**
- Event table stores `name` text only.
  - `olap_benchmarks/metrics/schema.sql:28`
- Query identity and iteration are embedded in strings (e.g. `query_<name>_iteration_<n>`).
  - `olap_benchmarks/suites/rtabench/config.py:192`

Impact: every visualization must parse strings instead of joining normalized dimensions.

6. **Results-site pipeline is missing**
- GH Pages workflow builds `site/` only; no export step from benchmark DB to site artifacts.
  - `.github/workflows/pages.yml:35`
  - `.github/workflows/pages.yml:42`
- Site is still starter/template content.
  - `site/src/pages/index.astro:8`
  - `site/README.md:1`

Impact: results presentation is not reproducible from benchmark output.

7. **Writer process lifecycle lacks explicit shutdown protocol**
- Writer loop is infinite and only exits on queue EOF.
  - `olap_benchmarks/metrics/storage.py:36`
- There is no explicit “stop writer” message.
  - `olap_benchmarks/metrics/storage.py:17`
- Bench entrypoint starts writer but does not explicitly terminate/join it.
  - `olap_benchmarks/__main__.py:42`

Impact: process lifecycle is fragile and behavior depends on manager/GC semantics.

### Medium

8. **CLI contract mismatch (`create` accepted in type but not implemented)**
- `run(..., command: Literal["start", "stop", "restart", "create"])`
  - `olap_benchmarks/__main__.py:54`
- Match block handles only start/stop/restart.
  - `olap_benchmarks/__main__.py:58`

9. **Testing gap in core execution path**
- No normal `tests/` suite found; only a debug concurrency script.
  - `olap_benchmarks/debug/test_result_concurrency.py:1`

Impact: regressions in benchmark semantics are likely to slip through.

## Architecture recommendations

### 1. Split into clear layers

- **Layer A: Benchmark definitions (declarative)**
  - Suite metadata (dataset refs, query list, iteration policy, optional DB overrides).
  - DB capability metadata (supports_upsert, supports_httpfs, needs_restart, etc.).

- **Layer B: Execution engine (imperative)**
  - Matrix runner: `for suite in suites, for db in dbs`.
  - Uniform lifecycle hooks: `prepare`, `populate`, `run`, `teardown`.
  - Deterministic result IDs and run metadata.

- **Layer C: Results model + exporters**
  - Canonical DuckDB artifact.
  - Optional JSON/Parquet exports for the site.

### 2. Normalize results schema for analytics-first UX

Recommended minimum tables:
- `run` (run metadata, git commit, host/system, started/finished, status, error)
- `database` (db_id, name, version, config fingerprint)
- `suite` (suite_id, name, version)
- `query_def` (suite_id, query_id, canonical_name, sql_hash)
- `query_run` (run_id, query_id, iteration, start/end, duration_ms, row_count)
- `resource_sample` (run_id/query_run_id, ts, cpu_percent, mem_mb, disk_mb)
- `event` (structured `event_type`, optional payload JSON)

Keep `event.name` free-form only as supplemental text, not primary key material.

### 3. Artifact contract between backend and site

Publish under `site/public/data/`:
- `results.duckdb` (canonical)
- `results_manifest.json` (schema version, generated_at, commit SHA, row counts)
- optional `summary/*.json` for fast first paint

This gives repeatable, cacheable deploys and easy rollbacks.

## DuckDB in browser feasibility (for your exact use case)

Short answer: **yes, feasible** for a static results presentation layer.

Why:
- DuckDB-Wasm supports loading/querying remote files via URL (`registerFileURL`, SQL directly from URL).
- DuckDB can attach remote `.duckdb` files over `httpfs` in read-only mode; DuckDB’s own benchmark-over-time page demonstrates this pattern with Wasm shell.

Constraints to plan for:
- Wasm memory cap is effectively bounded (4 GB WebAssembly ceiling; browsers may be stricter).
- Single-thread by default in browser client.
- Browser CORS and static host HTTP behavior must allow remote file reads.

Practical guidance:
- If `results.duckdb` stays reasonably small (typically tens to low hundreds of MB), querying in-browser is straightforward.
- If it grows large, pre-aggregate to summary tables/materialized exports and keep raw detail optional (lazy-loaded drill-down).

## Astro vs React for results UI

Recommendation for your stated scope: **keep Astro**, add **React islands** for interactive charts/filters.

Why this fits:
- You want static deployment and presentation-only scope.
- Astro’s island model lets you keep most of the page static and hydrate only heavy interactive components.
- Astro has official React integration (`@astrojs/react`), so your visualization layer can still be TypeScript/React-heavy.

When to choose pure React/Vite instead:
- If the whole site becomes an SPA with app-like client routing/state everywhere.
- If almost every page region is interactive and SSR/static HTML benefits are minimal.

Given your current goal, Astro + React islands is the cleaner default.

## Proposed next steps

1. **Fix correctness issues first**
- Rename/override the misnamed `populate_*` methods so intended DB-specific flows run.
- Introduce explicit run status (`success|failed|aborted`) and error field.
- Decide on soft-delete vs hard-delete and align schema + CLI behavior.

2. **Define v2 results schema and migration**
- Add normalized query/run/resource tables.
- Keep compatibility view(s) for existing tooling.

3. **Add artifact export pipeline**
- New command (e.g. `python -m olap_benchmarks export_site_data`) to produce `results.duckdb` + manifest + optional summary JSON.
- Wire this into CI before `site` build.

4. **Implement UI as Astro + React islands**
- Keep Astro pages/layout/routes static.
- Build interactive dashboard components in React/TS.
- Use DuckDB-Wasm as query engine over static artifact; precompute heavy summaries.

5. **Add minimal regression tests**
- Lifecycle/status tests for benchmark failure handling.
- Schema/event contract tests.
- One golden export test for site artifacts.

## External references

- DuckDB-Wasm data ingestion and URL-based file registration/querying: https://duckdb.org/docs/stable/clients/wasm/data_ingestion.html
- DuckDB-Wasm overview and browser limitations (single thread by default, 4 GB Wasm memory ceiling): https://duckdb.org/docs/stable/clients/wasm/overview
- DuckDB example showing Wasm shell attaching remote `.duckdb` over HTTP (`LOAD httpfs; ATTACH ...`): https://duckdb.org/2024/06/26/benchmarks-over-time
- Astro React integration docs (`@astrojs/react`): https://docs.astro.build/en/guides/integrations-guide/react/
- Astro islands architecture and selective hydration (`client:*`): https://docs.astro.build/en/concepts/islands/
- Astro client directives reference (`client:load`, `client:visible`, etc.): https://docs.astro.build/en/reference/directives-reference/
