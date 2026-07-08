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
scale-factor discovery issues. TPC-H now fans out SF10 and SF50 for `suite=all`.
The version-bump work now includes MonetDB Dec2025-SP3, the site now surfaces
run methodology metadata when present, and the home page labels the current
Apple Silicon public data as development data. Row-count validation now marks
consensus outliers as `wrong_result`, and query steps now record bounded answer
hashes for value-level validation. Shared-revision migrations now reject
unknown legacy suite names instead of guessing scale factors, dedupe duplicate
natural run keys before adding the unique index, clean dependent orphan rows
from deduped runs, and implement the scale-factor downgrade. The explorer now
includes a cross-system comparison panel for the selected suite and scale
factor. Kaggle Airbnb is now marked as a smoke suite and excluded from the home
aggregate ranking while staying explorable. RTABench now schedules the
upstream 1000-series pre-aggregated query variants where db-specific files
exist and records explicit unsupported steps elsewhere. The default benchmark
matrix now keeps row-store TPC-H/TPC-DS runs opt-in while preserving explicit
low-scale commands. Remaining blockers are public rerun quality, correctness
checks, published-data hygiene, and the larger suite/engine roadmap from
`RESEARCH.md`.

---

## 1. Public-Run Methodology

- [ ] **Full native rerun is still pending.** Docker starts now use the
      host-native platform by default, but `site/public/data/results.db` still
      contains the old published runs from `macbook-pro-m4`, before the platform
      and ConnectorX fixes. The next full rerun should use the intended
      `OLAP_BENCHMARKS_SYSTEM=macbook-m4-pro` value and then regenerate the
      published DB. The rerun should also live-confirm each engine's runtime
      version query and the bumped image/package pins.

## 2. Correctness And Validation

- [ ] **Investigate/fix divergent query results, then rerun.**

      | Suite | Query | Divergence in current published DB |
      | --- | --- | --- |
      | clickbench | Q28 | MonetDB and StarRocks return 11 rows vs 25 elsewhere (`REGEXP_REPLACE` handling) |
      | time_series | `wide_10/11_scalar_lookup` | TimescaleDB returns 0 rows vs 1 |
      | time_series | `*_19_null_gap_detection` | TimescaleDB differs materially; ClickHouse is also off by 1-2 rows |
      | time_series | `*_09/22_raw_filtered`, `large_23_batch_export` | TimescaleDB is short by dozens |
      | time_series | `*_06_daily_resample` | ClickHouse and TimescaleDB disagree on date-bucket boundaries |

      Partially fixed 2026-07-08: ClickBench Q28 now uses `$1` replacement
      backreferences for MonetDB and StarRocks; the published row counts still
      need the final rerun. Time-series `operation=all` now runs select before
      mutate so the final rerun records select correctness against the clean
      populated dataset instead of post-mutation state.

      QuestDB's ClickBench rewrites still deserve value-level checks even though
      their published row counts match.
- [ ] **Add value-level validation against a reference engine.** Row counts are
      too weak for TPC-DS because 84 of 99 queries end in `LIMIT 100`; a wrong
      rewrite can still return 100 rows. Add deterministic answer hashes or
      equivalent reference checks for ClickBench, TPC-H, TPC-DS, and RTABench
      where practical.
      Partially fixed 2026-07-08: query steps now record deterministic
      answer hashes for results up to the configured cell limit, and validation
      compares latest-run hashes after row counts agree. The published data
      needs the final rerun before these hashes exist across engines.
- [ ] **Make unrecorded steps impossible or visible.** TimescaleDB's published
      time-series mutate run silently lacks all three
      `insert_data_large_10000` iterations (79 mutation steps vs 82 elsewhere).
      A missing step should be recorded as an explicit status.
      Partially fixed 2026-07-08: configured disabled time-series mutation
      steps are now written as `result_status='skipped'` instead of omitted, and
      suite-excluded query steps are now recorded as `skipped`/`unsupported`.
      Select-query loops now isolate per-query failures across suites and record
      skipped remaining iterations after a failed iteration. The historical
      TimescaleDB missing insert iterations still need root-cause analysis
      during the rerun.

## 3. Dimensions, UI, And Data Hygiene

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

1. Fix known divergent queries and add value-level validation before trusting
   the rerun.
2. Finalize public-run policy and metadata surfacing, then run the full matrix
   within the disk budget and publish `macbook-m4-pro` data.
3. Clean published-data leftovers and make unexpected missing steps visible.
4. Add JSONBench, then Polars and Doris; handle JOB and TSBS after value-level
   checks and status-aware reporting have been exercised on the rerun.

## Reference Facts

- Timed unit: `perf_counter` around `fetch()` into Polars, so timings include
  client round trip, result transfer, and materialization.
- Current default SFs: ClickBench 1, Kaggle Airbnb 1, RTABench 1, time-series 1
  (also configured at SF10), TPC-H 10, TPC-DS 1.
- With `suite=all`, PostgreSQL and TimescaleDB skip optional TPC-H/TPC-DS runs
  by default; explicit `tpc_h`/`tpc_ds` commands still include those engines.
- Current published coverage is still the old `macbook-pro-m4` data:
  clickbench SF1 x 7 DBs; kaggle_airbnb/rtabench/time_series SF1 x 6 DBs;
  TPC-H SF10 x 6 DBs; TPC-DS SF1 with DuckDB and ClickHouse selects plus a
  Postgres populate-only run.
- Current validation: populate table row counts vs source parquet, then
  cross-engine per-query row counts after select runs within one
  `(system, suite, scale factor)` scope. Consensus row-count outliers are marked
  as `wrong_result`; ambiguous splits still fail validation without guessing.
  New select runs also store bounded answer hashes, and consensus hash outliers
  are marked as `wrong_result` when row counts agree.
- RTABench upstream currently documents 33 queries in its README, but the
  current upstream Postgres query directory has 31 base files; TimescaleDB and
  ClickHouse also include 10 `1000+` pre-aggregated variants, and upstream
  `run.sh` executes every `*.sql` file in the database query directory.
