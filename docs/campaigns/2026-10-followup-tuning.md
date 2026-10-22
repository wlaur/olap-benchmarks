# Follow-up tuning ledger

The [follow-up protocol](2026-10-followup-protocol.md) fixes workload membership,
participants and resource limits before measurement. This ledger declares the
initial candidates before observing full-data timings. All entries below are
planned investigations, not adopted improvements.

| Engine / suite | Candidate pattern | Physical-plan gate | Correctness and cost gate |
| --- | --- | --- | --- |
| PostgreSQL / Device Telemetry | Reverse the internal DISTINCT ON device order in queries 02/03, then restore ascending output order outside | Check whether a backward scan of the existing composite key removes incremental/full sorting without adding larger scans | Exact original/candidate rows and schema at SF1/SF10, including latest-row NULLs and inactive devices; original/candidate controls in both orders |
| TimescaleDB / Device Telemetry | The same ordering rewrite against the actual compressed hypertable | Inspect chunk scans, decompression and sorting; no adoption based only on PostgreSQL's plan | Verify before/after compressed results and both scales independently; count compressed chunks and retain memory/storage evidence |
| PostgreSQL / JOB | Add the pinned upstream secondary foreign-key indexes | Check whether representative canonical queries use the new indexes and change join/scan work | All 113 query answers and schemas; charge index building and ANALYZE to preparation, include disk cost, and rerun the full workload on adoption |

The telemetry rewrite changes only row traversal order. The outer ORDER BY
restores the original output. It does not introduce a metadata-dimension lookup,
drop devices, substitute older non-NULL metrics, or reduce the queried history.
The JOB candidate is exactly `upstream/fkindexes.sql`; no precomputed join or
aggregate results are allowed. A faster selected query alone does not justify
an overall workload claim.

Use at most three candidate patterns and twenty minutes of live diagnostics per
engine/suite across scales, following the existing bounded protocol. Record
unchanged plans, noisy controls, failed correctness gates and rejected physical
designs. Population and final suite measurements are outside the diagnostic
allowance. Existing exhausted October budgets are not reset by this ledger.

Doris relational work first requires native loading, full statistics collection,
cache attestation and dialect correctness. Its overview statistics can lag
physical data; readiness must record the actual evidence used rather than equate
a successful ANALYZE statement with complete preparation. No Doris performance
tuning is claimed before that gate passes.

Common-file and JSON tracks hold their representation contracts fixed. Changing
row groups, repartitioning files, indexing prepared JSON, or adding materialized
aggregates would create a different track and is outside these initial candidates.
