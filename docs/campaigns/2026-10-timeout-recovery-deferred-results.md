# Deferred timeout-recovery results — 26 October 2026

All authorized deferred attempts are terminal. This publication selects whole
attempts, preserves raw failures and publishes validated successful serial
queries under the existing `relative-speed-v2` certificate policy. It does not
claim that every workload completed successfully.

The measurement source was clean commit
`743cc81f1b3d0e689d86aabbc2d6c1019ef7a962`, with frozen plan SHA256
`3f5826b437df924e14204a811689a448b5f7e4b48a3dbc83346ab725f671b64d`.
The [protocol](2026-10-timeout-recovery-deferred.md) retained the original inputs,
SQL, settings, images and limits: 10 server CPUs, 24 GiB server memory, 28 GiB
sampled client RSS, 300 seconds per query, a 30-second SELECT cancellation grace,
14,400 seconds per operation and a 40 GiB runtime disk reserve. No measurement
source changes occurred after START.

## Serial outcomes

All five fresh native populations completed their canonical row-count gates.
The fresh serial campaign covered 362 planned positions: 350 queries completed
all four iterations, producing 1,050 successful measured fetches. Twelve warmups
timed out; their 36 measured iterations were recorded as skipped, never as zero
millisecond observations. Every other planned query continued within its same
SELECT. Read-only answer and iteration validation reported zero findings and
zero marks.

Raw IDs in the following table belong to
`2026-10-timeout-recovery-deferred-20261026` unless explicitly qualified.
Publication IDs belong to `2026-10-timeout-recovery-deferred-publication`.

| Whole SELECT | Raw population / SELECT | Publication SELECT | Result |
|---|---|---|---|
| TimescaleDB TPC-DS SF1, prior complete attempt | `final/685` / `final/686` | 715 | Failed Q11; 98 of 99 queries completed |
| PostgreSQL TPC-DS SF1 | 685 / 686 | 717 | Failed Q11; 98 of 99 completed |
| TimescaleDB TPC-DS SF10 | 687 / 688 | 719 | Failed Q06, Q11, Q30, Q81, Q95; 94 of 99 completed |
| PostgreSQL TPC-DS SF10 | 689 / 690 | 721 | Same five warmup timeouts; 94 of 99 completed |
| MonetDB ClickBench SF1 | 691 / 692 | 723 | Failed Q32; 42 of 43 completed |
| PostgreSQL TPC-H SF10 | 693 / 694 | 725 | Completed all 22 queries |

`final` denotes the sealed `2026-10-timeout-recovery-final` namespace. Its
TimescaleDB SF1 attempt used the same approved TPC-DS SQL generation and completed
all 99 planned positions before its failed-ending SELECT. It was selected whole,
without rerunning it or splicing individual queries. The user-interrupted
PostgreSQL `final/run688` remains archived, unselected and incompletely validated;
it was replaced by the fresh PostgreSQL SF1 cohort above.

## TPC-DS generation and correctness

All four PostgreSQL/TimescaleDB TPC-DS cells now use the reviewed Q31/Q49/Q65/Q85
source together. Q31/Q65/Q85 add byte-order `COLLATE "C"` only to approved outer
sort keys. Q49 converts all twelve already-summed `DECIMAL(15,4)` operands to
binary64 before division, defining binary64 ranks explicitly. This changes the
numeric contract and the measured work; these are new timing cohorts, not
performance improvements inferred from old-to-new timings.

All four queries completed warmup and three measurements at both scales on both
engines. At SF10, the PostgreSQL and TimescaleDB answers match these exact hashes:

| Query | BLAKE2b-128 answer hash |
|---|---|
| 31 | `c3bc5e143b1b15f4d8f6cdc82ac4734d` |
| 49 | `3aa2e4c29ca4495b945113f9b9b87d77` |
| 65 | `0f6a6f4927c565f1eafb1d528462688f` |
| 85 | `b2b8237eb850ca77b3473ccd38ea2103` |

The earlier exact-decimal PostgreSQL Q49 answer remains valid evidence for its
own arithmetic contract. It is not relabeled as an engine defect or wrong result.
The [portability record](2026-10-tpcds-pg-collation-portability.md) discloses the
48 currency-rank changes in the bounded proof, with one projected output cell
changing from 340 to 342. Historical original `run695` and remaining `run694`
retain their original failed validation; no raw marks or statuses were rewritten.

The publication SQL audit checks 6,837 planned SELECT positions, all recorded
execution SQL and every TPC-DS statement against its actual measured source
commit. There are zero mismatches. The Explorer manifest changes exactly the four
approved overrides for PostgreSQL and TimescaleDB while preserving the prior
Arc Q06 and inherited TimescaleDB Q46/Q72/Q78 source-matching materializations.
No retained cohort is labeled with a different SQL generation.

## MonetDB deadline and recovery

The new ClickBench population verified exactly 99,997,497 `hits` rows. The actual
SELECT session read back `querytimeout=300`, explicitly labeled seconds. Q32's
warmup surfaced the harness `TimeoutError` after 300.784037 seconds. Its three
measurements were skipped; Q33–Q42 then completed all forty iterations in the
same SELECT. The final session baseline completed with zero tagged sessions and
zero staging tables. There was no 330-second outer-watchdog intervention or
cascade of connection failures.

This establishes the configured 300-second cap and production recovery for this
cohort. The Python alarm was also active, so it is not a separate isolated proof
of a 300-second native expiry. The isolated native-cancellation proof remains the
earlier bounded two-second synthetic query. No classifier or SQL changes were
made for this rerun. Q28 ran and was timed in all four iterations, retaining its
suite-wide cross-engine answer-comparison and overall-performance exclusion.
Thus the failed Q32 leaves 41 of 42 comparable positions eligible for overall
credit after certification; strict whole-workload ranking remains ineligible.

## Final analytical retry

The authorized 16-client PostgreSQL TPC-H SF10 retry used the new full population
and its freshly validated 22-query serial SELECT. Raw `run695`, publication 726,
failed during warmup: 150 requests were offered, 149 completed, and one Q9 request
hit PostgreSQL's native 300-second statement timeout. The raw exception is retained
as `OperationalError` with `QueryCanceled`; warmup failure is a hard stop.

Measurement never started. Completed measured requests are zero, throughput is
null, and no p50/p95/p99 performance claim is made. The failed warmup and its
successful warmup answers passed structural/answer checks without changing the
failed status. No further retry is authorized or scheduled.

The earlier 1- and 4-client measurements remain separate configurations with only
14 and 34 completed measured requests and insufficient samples for p95/p99. Their
actual population remains the original continuation population archived in the
previous publication; the new population 724 did not prepare those old runs.
Analytical results carry no overall-ranking weight.

## Curation, certification and score

The previous published database, SHA256
`c47a7f7c04963684a2125a63578cf7fdca84c4c028ed2e05ceece1ed7ec4ac0c`,
was copied once. The [exact selection](2026-10-timeout-recovery-deferred-selection.json)
replaces thirteen whole runs: six associated populations, six SELECTs and the
single 16-client analytical configuration. No generic sequential merge, latency
selection, older-query fallback or per-query winner selection was used.

Deterministic maps cover all five tables and every known relational ID/FK.
Bidirectional `EXCEPT ALL` checks preserve all non-ID scientific content and all
retained baseline rows, including embedded JSON IDs. The single eight-field
system snapshot matches exactly. The publication has 312 runs, including 127
SELECTs (118 completed, 9 failed) and 12 analytical configurations. Export and
compaction leave all five tables exactly equal to staging; unexpected marks are
zero. Raw source namespaces remain separate immutable archives.

Eight terminal failed SELECTs have whole-run certificates: retained 686, 709 and
711, plus new 715, 717, 719, 721 and 723. Each certificate binds the final DB bytes,
actual population, origin namespace, unchanged whole-run content, full canonical
plan, provenance and clean all-answer/iteration validation. The retained proofs
are independently revalidated against the new export. MonetDB Chat Threads SF10
688 still has an explicit Q15 exclusion, no certificate and zero whole-cell
credit; its fourteen successful queries remain visible. No certificate is issued
for analytical warmup results or interrupted attempts.

The formula, fixed 871 positions, eleven equally weighted raw families, peer
minimums, semantic exclusions and strict workload eligibility are unchanged.
References recalculate from this frozen publication, so score differences between
editions are not independent performance claims. The checked overall fixture now
records PostgreSQL 8.5 with 738 scored queries, TimescaleDB 7.6 with 667, and
MonetDB 29.2 with 770. The [method](../overall-ranking.md) explains the distinction
between certified successful-query credit and strict completed-workload ranking.

Previous limits remain: PostgreSQL ClickBench Q18 is not live validated;
TimescaleDB's existing Q18 normalization proof remains selected. QuestDB's earlier
queries beyond sixty seconds do not prove isolated 300-second enforcement.
StarRocks' two large time-series exports retained their original hash-cap skips
at 172,039,680 and 328,538,880 cells against the five-million-cell limit. Existing
failed population replacements do not certify retained older SELECTs: PostgreSQL
ClickBench 397 remains preparation-blocked; TimescaleDB RTABench 387 remains
scored on its original baseline population 386 preserved in the baseline archive,
not on the later failed population 703.

## Archives and reproducibility

The [artifact index](2026-10-timeout-recovery-deferred-artifacts.json) pins the
selected database, comparison metadata, certificates, proof archive and both
new raw/source archives. Assets use unique checksum names on the existing
[`data`](https://github.com/wlaur/olap-benchmarks/releases/tag/data) and
[`runs`](https://github.com/wlaur/olap-benchmarks/releases/tag/runs) releases.

The deferred raw database is 212,348,928 bytes, SHA256
`6e6baccfde803dc6a098f4bbdc7c90e269bf9b489f0d787eb51793aa55d6ffe2`.
The new evidence archive SHA256 is
`582821cffb5b3b872703ae10e15c3d71074232e30964ac74668110b69afa7b9d`;
the restore-tested source bundle SHA256 is
`a0576ddc17b7b582dca4c2141087367a7d61df06513c8a0f36653f477b0be3df`.
The previous evidence/source/proof releases remain required for the retained
origins and certificates. Neither raw archive is a merged publication database.

The typed curator and certificate validator reproduce the new selection,
read-only audits, exact table preservation and metadata. Logs retain the passing
Python, Ruff, Pyright, Bun, browser-reader and overall-fixture checks. At terminal
closure all owned stores, benchmark containers, processes, writers and WAL were
absent. The unrelated user caffeinate process and unowned canonical inputs were
preserved. Cleanup happens only after remote archive verification and deployment.
