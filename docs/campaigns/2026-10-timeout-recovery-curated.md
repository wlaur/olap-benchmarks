# Timeout recovery publication, 26 October 2026

This publication selects whole attempts from the sealed timeout-recovery campaigns, on top of one copy of the previously published database. It contains 312 runs, including 127 SELECT attempts (119 completed, eight failed) and 12 separate analytical attempts. Selection follows source generation, provenance and validated attempt order; it never chooses individual queries or faster measurements from different attempts.

The published database is [`results-c47a7f7c04963684a2125a63578cf7fdca84c4c028ed2e05ceece1ed7ec4ac0c.db`](https://github.com/wlaur/olap-benchmarks/releases/download/data/results-c47a7f7c04963684a2125a63578cf7fdca84c4c028ed2e05ceece1ed7ec4ac0c.db), 152,842,240 bytes. Its SHA-256 is `c47a7f7c04963684a2125a63578cf7fdca84c4c028ed2e05ceece1ed7ec4ac0c`. No original database, error, journal, timing, query result or JSON provenance was edited.

## Whole-attempt selection

Every source ID is qualified by its revision. The [machine-readable selection](2026-10-timeout-recovery-selection.json) provides exact original IDs, raw database hashes, selected IDs and replaced baseline IDs. The table lists population → SELECT IDs; selected IDs belong exclusively to `2026-10-timeout-recovery-curated`.

| Source revision | Cell | Original population → SELECT | Selected population → SELECT |
| --- | --- | --- | --- |
| `2026-10-timeout-recovery` | Doris ClickBench SF1 | 685 → 686 | 685 → 686 |
| `2026-10-timeout-recovery` | MonetDB Chat Threads SF10 | 689 → 690 | 687 → 688 |
| `2026-10-timeout-recovery` | PostgreSQL ClickBench SF1 | 691 → none | 689; baseline SELECT 397 retained separately |
| `2026-10-timeout-recovery` | PostgreSQL TPC-DS SF1 | 692 → 693 | 690 → 691 |
| `2026-10-timeout-recovery-continuation` | PostgreSQL TPC-H SF10 | 685 → 686 | 692 → 693 |
| `2026-10-timeout-recovery-remaining` | StarRocks Time Series SF1 | 689 → 690 | 697 → 698 |
| `2026-10-timeout-recovery-remaining` | StarRocks Time Series SF10 | 691 → 692 | 699 → 700 |
| `2026-10-timeout-recovery-corrective` | QuestDB RTABench SF1 | 685 → 686 | 701 → 702 |
| `2026-10-timeout-recovery-corrective` | TimescaleDB RTABench SF1 | 687 → none | 703; baseline SELECT 387 retained separately |
| `2026-10-timeout-recovery-corrective` | TimescaleDB Time Series SF1 | 688 → 689 | 704 → 705 |
| `2026-10-timeout-recovery-corrective` | MonetDB ClickBench SF1 | 690 → 691 | 706 → 707 |
| `2026-10-timeout-recovery-corrective` | PostgreSQL TPC-H SF50 | 692 → 693 | 708 → 709 |
| `2026-10-timeout-recovery-corrective` | TimescaleDB TPC-H SF50 | 694 → 695 | 710 → 711 |
| `2026-10-timeout-recovery-corrective` | TimescaleDB ClickBench SF1 | 696 → 697 | 712 → 713 |

The two population-only replacements failed at the disk reserve. PostgreSQL ClickBench SELECT 397 retains its incomplete-preparation annotation and zero credit. TimescaleDB RTABench SELECT 387 remains scored under the legacy rules using its original baseline population 386, preserved in the baseline asset. Population 386 is replaced in the curated database by failed new population 703; there is no claim that 703 prepared SELECT 387. Each new SELECT has its actual same-attempt population identified in the proof bundle; a newer population is never substituted as proof for an older SELECT.

The continuation's analytical runs `2026-10-timeout-recovery-continuation/run687`, `/run688` and `/run689` replace baseline analytical runs 670–672 as curated runs 694–696, preserving the one-, four- and sixteen-client configurations. Sixteen clients failed. Analytical evidence remains outside resident-hot SELECT scoring.

## Certification and failed attempts

The comparison manifest uses schema 2, overall policy `relative-speed-v2`, and certificate policy `certified-terminal-partial-v1`. The [ranking method](../overall-ranking.md) retains 871 fixed query positions and 11 equally weighted raw families. Failed and missing queries score zero. The strict workload ranking still excludes failed runs.

Four whole failed SELECT attempts satisfy the population, lineage, source SQL, full answer and iteration certificate contract:

- Curated 686, from `2026-10-timeout-recovery/run686`: Doris ClickBench, Q32 memory failure; 42 other queries completed, with the existing Q28 comparison exclusion retained.
- Curated 691, from `2026-10-timeout-recovery/run693`: PostgreSQL TPC-DS SF1, 98 of 99 queries completed under the old SQL generation; sealed answer validation passed.
- Curated 709, from `2026-10-timeout-recovery-corrective/run693`: PostgreSQL TPC-H SF50, Q9 failed; the other 21 queries completed all three measured iterations.
- Curated 711, from `2026-10-timeout-recovery-corrective/run695`: TimescaleDB TPC-H SF50, the same Q9 failure and 21 complete queries.

Four selected failed attempts remain uncertified and earn zero overall credit:

- Curated 688, from `2026-10-timeout-recovery/run690`, explicitly excluded `15_part_type_distribution`. Its 14 successful queries remain visible, but this is not a full canonical certificate. The later Q16 failure is preserved. No fallback to older run 522 is used.
- Curated 707, from `2026-10-timeout-recovery-corrective/run691`, retained 96 successful measurements for Q0–Q31. Q32 reached the 300-second deadline plus 30-second cancellation grace; the outer watchdog cut off the post-SELECT `session_baseline_after` check before it could observe session state. This is not a finding of a dirty session. Independent cleanup succeeded and answer validation was clean. This is **not** a wrong-answer finding or quarantine. The current certificate contract rejects that failed phase; no fallback to older run 525 is used.
- Retained baseline PostgreSQL/TimescaleDB TPC-DS SF10 runs 658/660 have no located archived full canonical population proof suitable for this certificate. Their earlier Q11 outer-watchdog failures, successful measurements and missing positions remain unchanged. No certificate is fabricated.

QuestDB's corrected RTABench run completed 31 supported queries, including Q0025 above 60 seconds and below the configured 300-second budget. TimescaleDB's corrected Time Series SF1 completed 61 supported queries plus one unsupported position. Its corrected ClickBench attempt completed all 43 queries and passed Q18 normalization validation; Q28 remains excluded from comparison. Both StarRocks Time Series replacements retain the existing answer-hash cap disclosure.

The final plan's native MonetDB ClickBench 43-query cohort was **unstarted** and remains deferred. The native fix has only a two-second synthetic proof, with no claim of measured 300-second expiry or Q32 recovery. PostgreSQL ClickBench Q18 has not been live-validated; the corrected Q18 proof is TimescaleDB run697 only. The continuation's one-/four-client analytical runs recorded only 14/34 measured requests, insufficient for reliable p95/p99; sixteen-client warmup had 103 requests, 101 successes and two timeouts, with no measured phase and null QPS. StarRocks' large results contained 172,039,680/328,538,880 cells: answer hashing was skipped by the existing 5,000,000-cell cap, so successful completion does not imply hash validation for those results.

## Deferred SQL generation

The entire unfinished TPC-DS generation measured at `479fab502e0a2ca97e27b74473f3f68773092bf6` is deferred: `2026-10-timeout-recovery-final/run685`–`/run688`, and both unstarted PostgreSQL/TimescaleDB SF10 cells. TimescaleDB SF1 SELECT 686 was partial; PostgreSQL SF1 SELECT 688 was interrupted by the user's shutdown during Q81 at `2026-10-26T04:12:58.346959Z`. Q11 had already timed out after 300.027296 seconds; 79 queries had 237 measured / 316 total successful iterations. Its validation audit did not finish. This is not evidence of wrong answers or an engine defect, and it is not resumed here.

The selected TPC-DS generation comes from the frozen published `queries.json` at `efaae1c9f31a9c917b074e3c60da74c0572d34e6`. All 99 planned positions per selected TPC-DS run were audited against that run's recorded source commit, including runs whose final query-hash metadata is absent. No new Q31/Q49/Q65/Q85 SQL is introduced.

The publication materializes three inherited **old** PostgreSQL overrides as explicit TimescaleDB Q46/Q72/Q78 entries, matching its measured adapter behavior. Arc Chat Q06 receives an Arc-only override from `e55b553407307d804d14e9d22fb0830e5fdfc4e2`, matching both retained Arc scales. All other frozen SQL is preserved. The SQL audit covers all 6,837 selected planned positions and every recorded query SQL string; recorded execution text matches after only the adapters' whitespace and terminal-semicolon handling. The proof bundle includes the exact manifest adjustment list and hashes.

`2026-10-timeout-recovery/run695` and `2026-10-timeout-recovery-remaining/run694` are excluded because their sealed validation did not pass. The latter is superseded by the corrected TimescaleDB ClickBench attempt. Earlier or slower attempts are not substituted to improve a score.

## Preservation and validation

The task-scoped [curator](../../scripts/curate_timeout_recovery.py) copies the baseline once, excludes every raw clone's seed rows, and replaces only the explicit whole attempts above. Relational IDs are remapped in `system_snapshot`, `run`, `run_step`, `run_metric` and `query_execution`; arbitrary JSON is never rewritten. CSV maps qualify every origin by revision. Baseline retained IDs use identity maps. Bidirectional `EXCEPT ALL` checks prove all retained and selected scientific fields are exact, including metadata, errors and timings. All eight non-ID snapshot fields, including JSON, match exactly; one snapshot is justified.

Read-only full answer and iteration validation passed with zero findings and zero marks before the actual `olap publish --campaign --report-url` staging export. All five table multisets were unchanged after publication and compaction: zero unexpected marks. The frozen SQL manifest was restored and the four source-matching overrides materialized. Certificates were generated and independently validated only after the final database hash was fixed. No engine or benchmark ran during curation.

Validation includes Ruff, strict Pyright, the complete non-live pytest suite (2,318 passed, one skipped, 14 deselected), 154 Bun unit tests, site type checking/lint/format/build, and 17 browser reader/overall/certificate tests (all passed). The pinned fixture is generated from the actual final v2 database; the previous fixture is retained as `overall-v1.json`. Browser tests also verify Explorer displays the selected old SQL generation. Final command results and artifact hashes are recorded in the proof bundle and [artifact index](2026-10-timeout-recovery-artifacts.json).

## Reproduction and evidence

The [original evidence archive](https://github.com/wlaur/olap-benchmarks/releases/download/runs/timeout-recovery-20261026-evidence-a8192c23b118.tar.gz) preserves all five raw databases and sealed reports. Its SHA-256 is `a8192c23b11807fe9d7733c0f7b8197494d1be9aa750b728628661d5fed147aa`. The [source bundle](https://github.com/wlaur/olap-benchmarks/releases/download/runs/timeout-recovery-20261026-source-468549d42d1d.bundle) preserves their exact commits. Its SHA-256 is `468549d42d1d7a7bfff33acacac00043879a2343f14ca9392dafa5029a6d642e`. Existing assets are retained.

The artifact index names the separate portable curation proof archive. It contains deterministic ID maps, selected/removed-run lineage, exact baseline preservation, all-answer/iteration/provenance and SQL audits, population evidence, accepted/rejected certificate candidates, final metadata, and checks. It contains no database symlinks or private environment files. Its restore index maps each certificate's raw revision to the original archive's checksum-addressed object, so worktree paths are unnecessary for verification.

To verify an existing publication, download the final database and metadata, restore the raw objects as `<revision>.db` beside the extracted proofs, and configure the canonical input directory for read-only Parquet row-count checks. Run `olap results publication-metadata <publication-directory> --annotations-path <proof-directory>/annotations.json --certificates-path <proof-directory>/certificates.json --evidence-directory <proof-directory>`. The certificate validator checks raw and selected full-run fingerprints, canonical population counts, lineage references, and clean final-database validation. It does not rewrite the database.

To reconstruct selection in a fresh checkout, restore source trees/evidence using the original archive manifest, then run `uv run python scripts/curate_timeout_recovery.py build --worktrees <restored-roots> --archive-manifest <original-manifest>` and `audit`. The `export` command performs the audited staging publication; `uv run python scripts/certify_timeout_recovery.py` creates certificates after that export. Rebuilding database files can change their binary hash; regenerated certificates must bind to the new final bytes. Never republish or compact the already-frozen release database merely to rerun an audit.
