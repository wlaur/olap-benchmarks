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

## 5. Fix how memory is reported

Measured 2026-08-02/03 while comparing MonetDB ADBC driver versions. Every item below was
observed, not inferred, and each one has already produced a wrong conclusion at least once.

- [ ] **Stop reporting cgroup charge as server memory.**
      `metrics/measure.py` sets `server_mem_mb` from `memory_stats.usage`, the container's total
      cgroup charge. That includes file-backed page cache, so it rises when the kernel caches the
      data the engine just wrote and falls when the kernel reclaims it, neither of which is an
      engine memory change. It is also higher than the figure `docker stats` prints, because the
      CLI subtracts `inactive_file` first and the raw API field does not. Record the engine
      process RSS separately, keep the cgroup charge under its own name, and never present the
      cgroup number alone as "server memory".

- [ ] **Record the cgroup anon/file split.**
      Read `memory.stat` from the container and store `anon` and `file` as distinct fields. Without
      them there is no way to tell an allocation regression from page cache, which is exactly the
      ambiguity that made a measured 4x rise in MonetDB read-path server memory impossible to
      interpret.

- [ ] **Split disk into data and write-ahead log, and keep peak separately from final.**
      `disk_mb` is the whole engine directory, so a 58 GB MonetDB WAL beside 46 GB of data reads as
      one number, and the failure it causes is invisible. Peak also matters more than final: TPC-H
      SF10 ends at a 9.7 GB dbfarm after peaking near 30 GB, so a final-size metric would miss the
      transient cost entirely.

- [ ] **Do not treat `client_mem_mb` as the driver's client memory.**
      `dbs/monetdb/adbc.py::fetch_adbc` calls `fetch_arrow_table()` and then `pl.from_arrow()`,
      holding both alive. Measured on a 218,880 x 786 result: the Arrow table alone peaks at 768 MB
      and the pair at 1,458 MB, because `pl.from_arrow()` copies rather than adopting the buffers
      (`rechunk=False` does not avoid it). The harness figure is therefore roughly 3x the driver's
      own peak. This does not distort cross-engine comparison, since every engine converts to
      Polars the same way, but the column must not be quoted as a driver measurement. The README
      claim that Polars adopts these Arrow tables zero-copy is wrong and should be corrected.

- [ ] **Never sum server and client memory.**
      They fund different budgets and the client usually runs on another host. Summing separately
      computed maxima also assumes the peaks coincide: on one measured ClickBench run
      `max(server) + max(client)` overstated `max(server + client)` by 31 GB. If a combined figure
      is ever wanted, sum per sample and then take the maximum.

- [ ] **Keep per-operation process isolation, and document why.**
      Peak RSS is a high-water mark that does not fall when memory is freed, so operations sharing
      a process charge each later operation with the earlier one's peak. Before `operation_runner`
      existed, a ClickBench select returning at most 25 rows was credited with 1,327 MB inherited
      from its populate, which was reported as an 890% driver regression before being traced to the
      harness. Anything that reintroduces shared-process operations reintroduces that error.

- [ ] **Make in-process engines explicit wherever memory is presented.**
      `duckdb` and `polars` have no container, so `server_mem_mb` is structurally 0 for them and
      their whole footprint sits in `client_mem_mb`. `results/resource_usage.py` already resolves
      this for reads, but any chart or export that touches memory needs the same treatment or those
      two engines will appear to use none.

- [ ] **Sample often enough to see transient peaks, and keep the last sample on failure.**
      A `docker stats --no-stream` call takes about a second, so it cannot sample a sub-second
      query at all; streaming `docker stats` emits ANSI control codes that break naive parsing and
      silently yield zero. Disk can be sampled more slowly than memory because walking a large
      directory perturbs the run. A failed run's final sample is often the interesting one and must
      not be discarded.

## Execution order

1. Define the profile, warm-up policy, timing boundaries, and score presentation.
2. Complete the remaining correctness and failure-accounting checks on the pinned campaign commit.
3. Run and validate the controlled Linux campaign.
4. Publish the clean replacement and verify that the deployed database matches it.
5. Continue with JOB and the broader TSBS/InfluxDB work.

Section 5 is independent of the campaign order and should land before the next public run, since it
changes what the published resource numbers mean.
