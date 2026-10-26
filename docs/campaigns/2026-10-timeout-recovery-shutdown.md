# Recovery shutdown — 26 October 2026

Stopped at the user's request so the system can be closed. Task-owned runners,
containers, database writers, WAL and native stores are absent. Frozen
measurement worktrees remain intact. The unrelated user caffeinate and four
pre-existing stopped containers were left alone.

The site, readability changes, timeout fixes, SQL portability contract and plans
are merged to main (PRs 27–34). Full source validation last passed 2,283 tests,
Ruff and strict Pyright. The site still serves the previous reviewed dataset;
recovered raw measurements have not been curated or published as a new dataset.

## Preserved progress

The corrective seven-cell campaign settled with three completed cells, one
reviewed population failure and three reviewed SELECT failures. TimescaleDB
ClickBench completed all 43 queries and 129 measured executions, with clean
validation. Q18's client normalization is verified live; Q28 retains its
cross-engine answer-comparison exclusion.

In the final campaign, TimescaleDB TPC-DS SF1 completed population. Its SELECT
keeps a Q11 timeout and 98 successful queries / 294 measurements, with clean
validation and a reviewed failed-SELECT disposition. PostgreSQL SF1 population
completed, but SELECT `2026-10-timeout-recovery-final/run688` was interrupted by
the user during Q81 at 04:12:58.346959 UTC. It retains 79 completed queries /
237 measurements and the Q11 timeout. Its validation is incomplete; the raw
status is failed with `KeyboardInterrupt: Interrupted by user`. A missing SELECT
ID in the journal reflects the interrupted audit, not an unattempted SELECT.

TimescaleDB TPC-DS SF10, PostgreSQL TPC-DS SF10 and the native-timeout MonetDB
ClickBench cohort remain unstarted. All earlier raw failures and validation
blocks remain preserved. No retry or acknowledgment of the interrupted run
was performed.

## Work left in progress

1. Review the interrupted PostgreSQL evidence and a safe continuation plan;
   finish the remaining cohorts. Do not run plain `--resume` or acknowledge
   this SELECT while journal validation is false.
2. Implement the user's selected overall-score policy: count **validated
   successful queries**; failed or missing queries score zero. This choice is
   recorded, but the policy is not implemented or published. Require a clean
   whole-attempt answer/iteration/provenance audit and explicit versioned
   eligibility; keep the formula, fixed weights and denominators unchanged.
3. Curate whole attempts with revision-qualified IDs, deterministic lineage
   maps and a run-bound SQL audit; review and publish one coherent dataset,
   metadata and score fixtures. Do not destructively merge seed-cloned raw
   databases or select per-query winners.
4. Complete final cleanup after publication and verified archive restoration.
   Local worktrees and evidence are retained for resumption.

## Remote checkpoint

All five raw database namespaces, campaign logs/reports/checkpoints, baseline
proof and correctness diagnostics are backed up as unpublished evidence. The
archive contains 1,707 files stored as 827 unique SHA-256-addressed payloads;
every payload was read back and verified. Inputs, virtual environments, native
stores and one private environment-file payload are excluded. The exact six
source/preparation heads were restored successfully from a standalone Git
bundle. GitHub's uploaded sizes and SHA-256 digests match local files.

- [timeout-recovery-20261026-source-468549d42d1d.bundle](https://github.com/wlaur/olap-benchmarks/releases/download/runs/timeout-recovery-20261026-source-468549d42d1d.bundle) — SHA256 `468549d42d1d7a7bfff33acacac00043879a2343f14ca9392dafa5029a6d642e`
- [timeout-recovery-20261026-evidence-a8192c23b118.tar.gz](https://github.com/wlaur/olap-benchmarks/releases/download/runs/timeout-recovery-20261026-evidence-a8192c23b118.tar.gz) — SHA256 `a8192c23b11807fe9d7733c0f7b8197494d1be9aa750b728628661d5fed147aa`
- [timeout-recovery-20261026-checkpoint-e4e49329dea2.json](https://github.com/wlaur/olap-benchmarks/releases/download/runs/timeout-recovery-20261026-checkpoint-e4e49329dea2.json) — SHA256 `e4e49329dea20f531a3ce1010155670328f82052b41fd8e5ac2cb29770b065a8`

The checkpoint JSON explains the content-addressed archive and pending work.
Never restore over existing sealed evidence automatically.
