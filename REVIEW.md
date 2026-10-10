# Remaining project work - 2026-10-10

Only unfinished work. Remove items when they are done; history lives in git.

## Where things stand

- Published: one system (`macbook-m4-pro`), three engines — ClickHouse, DuckDB, MonetDB (Dec2025-SP4, ADBC) —
  across all eight suites at SF1 (TPC-H and JSONBench at SF10). MonetDB has no JSONBench queries; its published set
  lacks `chat_threads` concurrent, which is registered again since it passed on SP4 and still needs a campaign run.
  Answer hashes are `canonical-v10`; the published DB is at the Alembic head.
- The intended matrix is declared in `olap_benchmarks/matrix.py` (Polars is declared published for TPC-H, JSONBench
  and ClickBench but has no runs yet). `olap matrix status|run`, `olap results validate|prune` and the generalised
  publish gate (one commit, clean tree, one snapshot, current pins, complete and validated cells) are in place.
  RTABench queries gained deterministic tie-breakers, so their stored answer hashes are no longer comparable to new
  runs.
- Experimental (unpublished) connectors: Postgres, TimescaleDB, QuestDB, StarRocks and Doris. `olap benchmark all`
  and `olap matrix status|run` leave them out unless named or `--include-experimental` is passed; their code stays.
  Polars and Arc are published but have no runs yet.
- Unsupported queries are declared in one registry (`olap_benchmarks/query_registry.py`), which also holds the
  answer-comparison exclusions; a missing SQL file is an error. SQL resolution lives in `BenchmarkSuite`, RTABench
  has a shared base, suite metadata is one `SuiteSpec` per suite, settings load lazily from the repository's `.env`,
  and `pytest -m live` smoke-tests each published engine (only DuckDB has been run so far).
- Higher scale factors (time_series/chat_threads SF10, TPC-H SF50) do not fit this machine's RAM and disk; they
  belong to the cloud campaign.

**Releasable** means: a published set produced by one commit, one system snapshot and one benchmark profile; every
published cell validated; a site that loads fast and states exactly what is being compared; and a repeatable
one-command path from "bump a version" to "published".

## 1. Release blockers

- [ ] **Publish one coherent campaign.** The current set mixes five commits (one dirty tree), two OS snapshots and
      three dates, predates `metadata.repository` and is `legacy-per-suite` rather than `resident-hot-v1`, so
      `olap publish` now refuses it. Run
      `olap matrix run` into a fresh revision from one clean commit (this adds Polars) and publish
      it through the gate.
- [ ] **No amd64 image for the MonetDB pin.** `monetdb/monetdb:Dec2025-SP4` does not exist on Docker Hub (newest
      tag: Dec2025-SP3), and `wlaur/monetdb-container` is arm64-only, so `olap versions --check` fails for the amd64
      variant and an amd64 host (the cloud campaign) cannot start MonetDB. Push an amd64 build of the wlaur image and
      point `images.server` at it, or wait for the upstream tag.

## 2. Methodology

The `resident-hot-v1` profile, query/schema hashes, decoupled input hashing and planned-step accounting are in
place (README "Benchmark profile"). Remaining, all optional:

- [ ] **Engine-internal timing** (optional). Latency is end-to-end client latency only. If engine execution time is
      wanted, record it as a separate, clearly labelled step metric (e.g. ClickHouse `query_log`, DuckDB profiling,
      MonetDB `sys.queue`), never in place of `duration_ms`.
- [ ] **Cold-cache profile** (optional). `resident-hot-v1` never drops the page cache between operations;
      `--drop-caches` only covers populate on Linux. A cold profile would need a drop (or container restart with
      dropped caches) before every measured iteration and its own profile id.

## 3. Site

Decision (2026-10-10): keep DuckDB-WASM. An arbitrary-query UI is planned, which needs an in-browser engine, and a
~12-15 MB first load is acceptable. The six fixed views stay as SQL over `run`/`run_step`.

- [ ] **Keep the typed query layer in step with migrations.** `bun run codegen` introspects the published file, so a
      schema change reaches the site only when someone regenerates; run codegen from `olap publish` (or check in CI
      that `generated/db.ts` matches the current schema).
- [ ] **Explorer e2e expects an overflowing latency list.** `tests/e2e/explorer.spec.ts` asserts that the per-query
      latency bars overflow their frame, which three engines do not; it fails on the current data independent of
      any change. Make the assertion conditional on the row count or drop it.
- [ ] **Arbitrary-query UI** (later): a SQL editor page over the same DuckDB instance, with the schema documented.

## 4. Engine coverage

- [ ] **Arc** (published for populate/select, pending a campaign). `olap_benchmarks/dbs/arc/` pins
      `basekicklabs/arc:26.09.3` and registers time_series, clickbench and chat_threads with populate and select only.
      Mutate and concurrent are not measured: every row-targeted upsert or delete rewrites the immutable hourly
      Parquet files it touches, so the time_series SF1 mutate run took 88 minutes (10k-row iterations up to ~6 minutes,
      slowing as files fragment) and the concurrent run after it was OOM-killed within 40 s. Arc requires a non-null
      `time` column, so `hits.EventTime` and both chat tables' `created_at` are imported as `time` and the
      `queries/arc` overrides refer to it by that name; other timestamps are stored as BIGINT microseconds and dates as
      midnight timestamps, converted back with `make_timestamp()` where a query returns one. Writes use the typed
      Parquet import (the MessagePack path corrupts types and timestamps), split below Arc's 500 MiB import limit.
      Populate sorts by time on the host and ends with Arc's manual compaction, which runs the hourly and daily tiers
      (one file per day for this historical data). rtabench, kaggle_airbnb, tpc_h and tpc_ds have tables with no
      timestamp and jsonbench has placeholder rows without one, so those stay unregistered (reasons in
      `Arc.suite_registry`; defects in [`issues/arc/`](issues/arc/)). Close once a campaign has run each registered
      suite and the answers match the other engines.
- [ ] **Polars in the published matrix** for the suites it implements (ClickBench, JSONBench, TPC-H).
- [ ] **MonetDB JSONBench**, or record why not: MonetDB does not register the suite at all, so nothing states why.
- [ ] **Run the live smoke tests for the other published engines.** `pytest -m live` has only been run for DuckDB;
      run it for ClickHouse, MonetDB, Polars and Arc once and fix what it finds.
- [ ] **Experimental engines: publish or keep parked.** They are excluded from `all` (README "olap benchmark"). Known
      state after the 2026-10-10 version bump:
      - StarRocks 4.1.6 runs all 99 TPC-DS queries at SF1 (49, 70 and 86 have StarRocks variants that match
        DuckDB), but 11 answers still differ from DuckDB (09, 18, 27, 31, 51, 58, 59, 66, 78, 83, 98) and 51 is
        non-deterministic across iterations. Not investigated.
      - Doris 4.1.4.1 RTABench populate and select complete (stream loads go to the BE's published port; the FE
        redirect points at a Docker-internal address). Seven answers differ from DuckDB/Postgres/TimescaleDB
        (0005, 0006, 0010, 0011, 0012, 0017, 0022). Doris registers no time_series.
      - Postgres 18.6 and TimescaleDB 2.30.2 RTABench complete and agree with DuckDB. TimescaleDB now creates the
        upstream continuous aggregates its 1000-series queries read and sorts 0008 like the others; the 0028 age
        bucketing fix it shares with Postgres was verified on Postgres only. In the 2026-10 time_series smoke run
        the TimescaleDB server went away at `upsert_data_large_1` and every later mutation failed, yet the mutate
        run is recorded as completed and concurrent never ran; not investigated.
      - QuestDB 10.0.1 runs all time_series operations at SF1 and agrees with DuckDB (version check reads
        `build()`; top-N hourly has a QuestDB variant). RTABench, ClickBench and Kaggle are untested on 10.x.
      - TimescaleDB's SQL is mostly a byte copy of Postgres's (all 23 chat_threads and 29 time_series overrides). An
        engine-level SQL fallback (timescaledb -> postgres) would remove ~54 files but needs a few new TimescaleDB
        copies where it deliberately runs the shared SQL (e.g. JSONBench); worth doing only if they stay.

## 5. Resource reporting

- [ ] **Show memory on the site.** Metric schema v7 now records the cgroup anon/file/shmem split and a data/WAL disk
      split, and `olap results resources` labels DuckDB and Polars `client_only`. Publish runs recorded at v7 first
      (older runs only carry the page-cache-inclusive charge), then show server memory (anon + shmem) for container
      engines and client memory for in-process engines, labelled as such.
- [ ] **Validate the v7 sampling on a live run.** The cgroup v2 parsing is unit-tested against Docker payloads but no
      benchmark has run with it yet; check that one ClickHouse and one PostgreSQL run record plausible anon, shmem
      and WAL figures.

## 6. Deferred

- [ ] **Cloud campaign** on one x86-64 Linux `i4i` host per `CLOUD.md`, including the higher scale factors this
      machine cannot run. Do it after section 1 so the campaign is a single `olap matrix run`.
- [ ] **JOB**, after `resident-hot-v1` has been exercised by a public run.
- [ ] **TSBS and InfluxDB 3**, only as a complete time-series expansion (Arc first).

## Suggested order

1. Section 1: a fresh campaign published through the new gate.
2. Section 3 (site), independent of the harness work.
3. The cloud campaign under `resident-hot-v1`; sections 4-5 fit around these.
