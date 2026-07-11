# Remaining project work - 2026-07-11

This document contains only unfinished work. Completed findings and implementation history have been removed.

## 1. Validate before the public rerun

- [ ] **Exercise value-level validation across the remaining public matrix.**
  Run latest-select row-count and `canonical-v4` answer-hash validation for ClickBench, TPC-H, TPC-DS, RTABench, and
  the time-series engines not covered by the isolated DuckDB/ClickHouse/PostgreSQL/TimescaleDB SF1 validation.
  Review every result that exceeds the answer-hash cell limit instead of treating an absent hash as a pass.

- [ ] **Live-confirm the remaining known correctness risks.**
  Verify ClickBench Q28 on MonetDB and StarRocks after the replacement-backreference fixes, and perform value-level
  checks on QuestDB's ClickBench rewrites. Treat the old published divergences as historical; determine correctness
  from clean latest runs.

- [ ] **Confirm complete step accounting during failure and mutation runs.**
  Rerun TimescaleDB time-series mutation and verify all planned iterations, especially
  `insert_data_large_10000`, have an explicit result status. Also confirm that interrupted runs leave visible failed
  or skipped steps rather than silently missing work.

- [ ] **Smoke the wired JSONBench 10M paths before including them in the matrix.**
  Validate populate row counts, all five queries, answer hashes, runtime versions, and cleanup for ClickHouse,
  DuckDB, PostgreSQL, StarRocks, and Polars. Doris 10M is already validated and does not need another isolated smoke.

## 2. Replace the published benchmark data

- [ ] **Run the full host-native benchmark matrix as `macbook-m4-pro`.**
  Use `OLAP_BENCHMARKS_SYSTEM=macbook-m4-pro`, confirm every engine's live runtime version against its pin, retain the
  current opt-in policy for row-store TPC-H/TPC-DS runs, and monitor disk throughout. Do not start a run whose peak
  storage cannot preserve a safe free-space reserve.

- [ ] **Validate the completed matrix before publishing.**
  Require populate source-row checks, latest-select row-count checks, `canonical-v4` answer-hash checks, explicit
  unsupported/skipped statuses, complete run metrics, and valid `system_snapshot_id` links. Resolve every ambiguous
  split or `wrong_result` before replacing the public database.

- [ ] **Publish a clean replacement database.**
  Replace `site/public/data/results.db` only after validation. Exclude the old `macbook-pro-m4` runs, the nine
  shadowed duplicates, the PostgreSQL TPC-DS populate-only leftover, failed/orphaned runs, and unreferenced system
  snapshots. Regenerate the manifests and confirm TimescaleDB mutation coverage in the published artifact.

## 3. Finish the near-term suite and engine roadmap

- [ ] **Decide whether JSONBench should add a 100M scale.**
  Make the decision only after measuring the validated 10M storage and runtime costs. Do not use the upstream 1B
  dataset on the current machine, and do not attempt 100M without enough space for source data, database expansion,
  temporary files, and a safety reserve.

- [ ] **Extend Polars beyond JSONBench.**
  Add normal DataFrame/LazyFrame implementations for ClickBench and TPC-H. Keep any future SQL-mode implementation
  separate and continue labeling Polars as in-process/single-node.

- [ ] **Extend Doris to RTABench.**
  Align query coverage and unsupported statuses first. Consider TPC-DS only after the full rerun demonstrates stable
  Doris lifecycle, loading, metrics, and cleanup behavior.

## 4. Deferred larger additions

- [ ] **Add JOB after status-aware reporting is exercised by the public rerun.**
  Port the optimizer-heavy workload only after unsupported, skipped, timeout, error, and wrong-result states are
  proven end to end in published data.

- [ ] **Add TSBS and InfluxDB 3 only as a complete time-series expansion.**
  Include ingest throughput, compression/storage, recent-window queries, and concurrent read/write behavior; do not
  add a query-only comparison.

## Execution order

1. Complete the JSONBench 10M smokes and resolve any cross-engine discrepancies.
2. Add Polars ClickBench/TPC-H and Doris RTABench, then validate the new paths.
3. Complete the remaining correctness checks across the intended public matrix.
4. Run and validate the host-native `macbook-m4-pro` matrix within the disk budget.
5. Publish the cleaned replacement database and manifests.
6. Continue with JOB and the broader TSBS/InfluxDB work.
