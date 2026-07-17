# Remaining project work - 2026-07-17

This document contains only unfinished work. Completed findings and implementation history have been removed.

The current published data is development evidence, not a defensible cross-engine leaderboard. The checked-in public
database contains only `macbook-pro-m4` runs, all 82 runs predate run methodology metadata, none of its 5,215 query
steps has a `canonical-v4` answer hash, and it is one migration behind the current results schema. Its ClickBench
comparison also mixes three-iteration DuckDB/ClickHouse runs with five-iteration QuestDB/StarRocks runs. The deployed
GitHub Pages database is an older, different artifact and does not contain QuestDB.

## 1. Lock the benchmark methodology before another public campaign

- [ ] **Define and version a resident-hot execution profile.**
      Start select runs from the same process state by reopening embedded engines and restarting server engines once, then
      keep every process and connection resident for the query phase. Use the same warm-up and measurement window for every
      engine. Store a profile identifier on each run and prevent the site from comparing runs with different profiles.

- [ ] **Replace the fixed five-iteration ClickBench policy with a demonstrated steady-state policy.**
      The published QuestDB run is still accelerating from a 0.3248 s per-query geometric mean at iteration 2 to 0.2544 s
      at iteration 5. Use a common longer warm-up, such as ten unscored iterations followed by a fixed measured window, or
      a documented convergence rule. Record warm-up and measured iterations separately instead of classifying every
      iteration after the first as equally warm.

- [ ] **Keep end-to-end, engine-execution, and cold results distinct.**
      The current timer measures `fetch()` through materialization into Polars, so label it end-to-end DataFrame latency.
      Add engine-internal timing only as a separate metric if direct ClickBench comparison is required. Do not call the
      first iteration cold: OS caches are not cleared, containers are restarted only once, and DuckDB is not restarted.
      Publish a cold profile only on controlled Linux with an engine restart/reopen and OS-cache clearing before each query.

- [ ] **Make the relative score's comparison scope explicit.**
      The 10 ms-smoothed score is recalculated against the fastest selected engine for each query, so an unchanged database
      receives a different score when another engine is added or removed. Keep absolute latency visible, state that scores
      are contender-set-relative wherever they appear, and never compare scores copied from different selections.

- [ ] **Record enough identity to reproduce a run exactly.**
      In addition to system snapshots, versions, and image digests, store the repository commit, benchmark-profile revision,
      input-data checksum, query/schema hashes, iteration policy, and timed boundary. Scope site comparisons to a single
      system snapshot and benchmark profile.

## 2. Validate correctness and failure accounting

- [ ] **Exercise value-level validation across the intended public matrix.**
      Run latest-select row-count and `canonical-v4` answer-hash validation for ClickBench, TPC-H, TPC-DS, RTABench, and
      time-series coverage not included in the isolated SF1 validation. Review every result above the answer-hash cell
      limit rather than treating an absent hash as a pass.

- [ ] **Live-confirm the remaining known correctness risks on the campaign commit.**
      Verify ClickBench Q28 on MonetDB and StarRocks after the replacement-backreference fixes, and perform value-level
      checks on QuestDB's dialect rewrites. Treat the old published divergences as historical and accept only clean results
      produced by the exact code and version pins used for the campaign.

- [ ] **Confirm complete step accounting during failure and mutation runs.**
      Rerun TimescaleDB time-series mutation and verify all planned iterations, especially
      `insert_data_large_10000`, have an explicit result status. Also interrupt a disposable run and confirm that failed or
      skipped steps remain visible instead of silently disappearing.

## 3. Replace and deploy the public benchmark data

- [ ] **Run one controlled x86-64 Linux campaign using `CLOUD.md`.**
      Use a single AWS `i4i` machine and system label for the complete comparison, with native-architecture containers,
      local NVMe, fixed CPU/RAM, and one pinned repository commit. Confirm every live runtime version and container digest,
      retain the opt-in policy for row-store TPC-H/TPC-DS runs, and preserve enough disk headroom for cleanup and compaction.
      Do not use the MacBook runs for the main cross-engine ranking.

- [ ] **Validate the complete campaign before publishing.**
      Require source and populated row-count checks, latest-select row-count checks, `canonical-v4` answer-hash checks,
      explicit unsupported/skipped statuses, complete run metrics, current-schema `system_snapshot_id` links, and one
      methodology profile across compared runs. Resolve every ambiguous consensus split or `wrong_result` first.

- [ ] **Publish a clean replacement rather than merging the development runs.**
      Build `site/public/data/results.db` at the current Alembic head using only the validated Linux campaign. Regenerate the
      manifest, suite registry, and query catalog; compact the file; and verify the site against the exact generated
      artifact before committing it.

- [ ] **Verify deployment integrity.**
      After GitHub Pages deploys, download `/olap-benchmarks/data/results.db` and compare its checksum, size, schema revision,
      manifest timestamp, run count, and database coverage with the committed artifact. Automate this check so a stale Pages
      database cannot silently diverge again.

## 4. Deferred larger additions

- [ ] **Add JOB only after the new methodology is exercised by the public rerun.**
      Port the optimizer-heavy workload after profile-aware comparison and unsupported, skipped, timeout, error, and
      wrong-result reporting have been proven end to end in published data.

- [ ] **Add TSBS and InfluxDB 3 only as a complete time-series expansion.**
      Include ingest throughput, compression/storage, recent-window queries, and concurrent read/write behavior; do not
      add a query-only comparison.

## Execution order

1. Define the profile, warm-up policy, timing boundaries, and score presentation.
2. Complete the remaining correctness and failure-accounting checks on the pinned campaign commit.
3. Run and validate the controlled Linux campaign.
4. Publish the clean replacement and verify that the deployed database matches it.
5. Continue with JOB and the broader TSBS/InfluxDB work.
