# Remaining project work - 2026-10-10

Only unfinished work. Remove items when they are done; history lives in git.

## Where things stand

- Published: one system (`macbook-m4-pro`), three engines — ClickHouse, DuckDB, MonetDB (Dec2025-SP4, ADBC) —
  across all eight suites at SF1 (TPC-H and JSONBench at SF10). MonetDB has no JSONBench queries and skips
  `chat_threads` concurrent (server crash). Answer hashes are `canonical-v10`; the published DB is at the Alembic head.
- The intended matrix is declared in `olap_benchmarks/matrix.py` (Polars is declared published for TPC-H, JSONBench
  and ClickBench but has no runs yet). `olap matrix status|run`, `olap results validate|prune` and the generalised
  publish gate (one commit, clean tree, one snapshot, current pins, complete and validated cells) are in place.
  RTABench queries gained deterministic tie-breakers, so their stored answer hashes are no longer comparable to new
  runs.
- Connectors with no published results: Polars, Postgres, TimescaleDB, QuestDB, StarRocks, Doris. They carry ~2.7k
  lines and ~370 SQL files that every interface change has to touch.
- Higher scale factors (time_series/chat_threads SF10, TPC-H SF50) do not fit this machine's RAM and disk; they
  belong to the cloud campaign.

**Releasable** means: a published set produced by one commit, one system snapshot and one benchmark profile; every
published cell validated; a site that loads fast and states exactly what is being compared; and a repeatable
one-command path from "bump a version" to "published".

## 1. Release blockers

- [ ] **Publish one coherent campaign.** The current set mixes five commits (one dirty tree), two OS snapshots and
      three dates, and predates `metadata.repository`, so `olap publish` now refuses it. Run
      `olap matrix run --published-only` into a fresh revision from one clean commit (this adds Polars) and publish
      it through the gate.
- [ ] **No amd64 image for the MonetDB pin.** `monetdb/monetdb:Dec2025-SP4` does not exist on Docker Hub (newest
      tag: Dec2025-SP3), and `wlaur/monetdb-container` is arm64-only, so `olap versions --check` fails for the amd64
      variant and an amd64 host (the cloud campaign) cannot start MonetDB. Push an amd64 build of the wlaur image and
      point `images.server` at it, or wait for the upstream tag.

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
- [ ] **Finish the reproducibility identity.** Already recorded: `metadata.repository` (commit, dirty flag, diff
      hash) for every run, image IDs, input manifest hashes, query text. Missing: query/schema hashes, profile
      revision, iteration policy. Scope site comparisons by system snapshot, not just `system`.
- [ ] **Decouple input hashing from populate.** Hashing the inputs right before populate warms the page cache, which
      favours engines that read the files directly. Hash in a separate step (or cache hashes by size+mtime) and drop
      the cache before populate on Linux.
- [ ] **Account for planned-but-never-started steps.** An interrupt marks running steps failed, but planned steps
      that never started leave no row; coverage then undercounts the denominator.

## 3. CLI and architecture

Ordered by how much they reduce the cost of rerunning and publishing.

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
      - TimescaleDB RTABench `0008` orders by `count desc` (the most-delayed order) while every other engine orders
        ascending, so its answer cannot agree.
      - Postgres, TimescaleDB and QuestDB: smoke-tested on time_series (see the 2026-10 campaign notes).

## 4. Site

Decision (2026-10-10): keep DuckDB-WASM. An arbitrary-query UI is planned, which needs an in-browser engine, and a
~12-15 MB first load is acceptable. The six fixed views stay as SQL over `run`/`run_step`.

- [ ] **Keep the typed query layer in step with migrations.** `bun run codegen` introspects the published file, so a
      schema change reaches the site only when someone regenerates; run codegen from `olap publish` (or check in CI
      that `generated/db.ts` matches the current schema).
- [ ] **Arbitrary-query UI** (later): a SQL editor page over the same DuckDB instance, with the schema documented.

## 5. Engine coverage

- [ ] **Arc** (implemented, pending a real run). `olap_benchmarks/dbs/arc/` pins `basekicklabs/arc:26.09.3` and
      registers time_series (all four operations), clickbench (populate/select) and chat_threads (all four operations).
      Every select query executed on a tiny probe slice of each, which says nothing about answers or timings at scale.
      Arc requires a non-null `time` column, so `hits.EventTime` and both chat tables' `created_at` are imported as
      `time` and the `queries/arc` overrides refer to it by that name. Arc has no other timestamp or date type, so
      further timestamps are stored as BIGINT microseconds and dates as midnight timestamps; the overrides convert back
      with `make_timestamp()` where a query returns one. Writes use the typed Parquet import because the MessagePack
      path corrupts types and timestamps. ClickBench and chat input is sorted by time on the host before import
      (Arc writes one file per hour partition per flush). Upsert is delete (file rewrite) then insert, and populate
      ends with an explicit compaction. rtabench, kaggle_airbnb, tpc_h and tpc_ds have tables with no timestamp,
      and jsonbench has placeholder rows without one, so those stay unregistered (reasons in
      `Arc.suite_registry`; defects in [`issues/arc/`](issues/arc/)). Watch for the partition pruner: the first
      time_series `large_03` (a 4.4-year range) took ~10 s cold while it checked ~40k hourly directories. Close once
      a campaign has run each registered suite on Arc and the answers match the other engines.
- [ ] **Polars in the published matrix** for the suites it implements (ClickBench, JSONBench, TPC-H).
- [ ] **MonetDB JSONBench**, or record why not in the unsupported registry.
- [ ] **Re-test MonetDB `chat_threads` concurrent** on SP4; the SIGSEGV is unexplained and was last seen on 11.55.7.

## 6. Resource reporting

- [ ] **Show memory on the site.** Metric schema v7 now records the cgroup anon/file/shmem split and a data/WAL disk
      split, and `olap results resources` labels DuckDB and Polars `client_only`. Publish runs recorded at v7 first
      (older runs only carry the page-cache-inclusive charge), then show server memory (anon + shmem) for container
      engines and client memory for in-process engines, labelled as such.
- [ ] **Validate the v7 sampling on a live run.** The cgroup v2 parsing is unit-tested against Docker payloads but no
      benchmark has run with it yet; check that one ClickHouse and one PostgreSQL run record plausible anon, shmem
      and WAL figures.

## 7. Deferred

- [ ] **Cloud campaign** on one x86-64 Linux `i4i` host per `CLOUD.md`, including the higher scale factors this
      machine cannot run. Do it after sections 1-3 so the campaign is a single `olap matrix run`.
- [ ] **JOB**, after the profile from section 2 has been exercised by a public run.
- [ ] **TSBS and InfluxDB 3**, only as a complete time-series expansion (Arc first).

## Suggested order

1. Section 1: a fresh campaign published through the new gate.
2. Section 4 (site), independent of the harness work.
3. Section 2 profile, then the cloud campaign; sections 5-6 fit around these.
