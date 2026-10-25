# PostgreSQL-family TPC-DS sort and ratio portability

This patch starts from merged main `dedca0bbcc7511ff69b4daa4cd18da49bb48563a`.
It adds PostgreSQL overrides for Q31, Q65, and Q85 using the exact candidate SQL
saved by the correctness-only `probes-collation-rank-v1` captures. Only these
existing outer `ORDER BY` expressions gain `COLLATE "C"`:

| Query | Text expressions |
| --- | --- |
| Q31 | `ss1.ca_county` |
| Q65 | `s_store_name`, `i_item_desc` |
| Q85 | `SUBSTRING(r_reason_desc,1,20)` |

Both PostgreSQL and TimescaleDB use `PostgresTpcDs.query_path`: engine-specific
file, then shared PostgreSQL override, then base SQL. These three new files in
`olap_benchmarks/suites/tpc_ds/queries/postgres/` therefore serve both engines;
there are no Timescale-specific files for these queries. Base SQL and the query
resolver remain unchanged. The projections, grouping, filters, arithmetic,
numeric sort keys, sort directions, limits, and existing null ordering remain
unchanged, including Q65's two `NULLS FIRST` clauses.
Explicit `C` collation changes the server's sort work and cost. Q31/Q65/Q85
timings belong to a new source cohort and must not be compared directly with
the earlier `2026-10-timeout-recovery/run695` timings as if the SQL work were unchanged.

## Saved evidence

Read-only evidence is in the frozen `t3-416ee4f7` worktree under
`data/temp/tpcds-sf10-row-diagnostics/`. The original manifest's 70 files were
reverified against their recorded sizes and SHA256s before copying candidates.
The original `result.json` SHA256 is
`f9fa46cc3d4125a558e24f9ff1886fd7ae99be5a25689768ffd5f8072508b166`.
Candidate SQL comes from `probes-collation-rank-v1/captures/postgres/<query>/probe.sql`
and matches the sealed probe plan byte for byte. Its `preparation.json` and
`test_probe.py` record the earlier AST equivalence checks; its `result.json`
records these outcomes:

| Query | Rows | Matching seed-peer answer hash (`blake2b128`) |
| --- | ---: | --- |
| Q31 | 288 | `c3bc5e143b1b15f4d8f6cdc82ac4734d` |
| Q65 | 100 | `0f6a6f4927c565f1eafb1d528462688f` |
| Q85 | 19 | `b2b8237eb850ca77b3473ccd38ea2103` |

The captured PostgreSQL database used `en_US.utf8` for `lc_collate` and
`lc_ctype`, with collation version `2.41`. Q31 and Q85 changed row sequence
without changing the row multiset. Q65's changed sort affected the limited
selection: 13 rows differed from the original PostgreSQL result. Its candidate
matches the seed-peer hash using the existing `ordered-ties-v1` policy with
output key indices `[0, 1]`; raw ordering within equal keys need not match.
Captured PostgreSQL driver values and frame values had no changed cells.
These observations support explicit sort portability, not an engine-defect
claim. The saved candidate execution evidence is PostgreSQL-only; TimescaleDB
coverage here verifies source resolution and query structure offline.

## Source SQL SHA256

| Query | Unchanged base SQL | New PostgreSQL-family override |
| --- | --- | --- |
| Q31 | `d1d1ee84af35662f8e0b766c0c7bbc38c1277c243a87433ee10c1e63c30c4d8c` | `bc24b32a90c24664424e9cec95ea34974b544c5c192cfdc4875d80cb352a4160` |
| Q65 | `a2f14a96bb6280a725a18e84a94b99fda0a299e0c1aacbdb56a07b40d364ceb3` | `82150b95ee4de318010fc68956317cbcc856cfcc5f73c0ff2db869c88a8efbcc` |
| Q85 | `09f2031d466106cfe8a42db595bf2184d0db3144f1d323b74556b02911bde766` | `526af09fbdce11d928ff75dc9a4902b3a3f049a2e6064ddfb921e4d8ed5e5da6` |

## Scope and validation

Offline regressions pin the saved candidate hashes for both engine classes,
check exact collation placement on the outer sort keys, and compare complete
parsed query trees after removing only those wrappers and source locations.
The existing all-query resolution test includes the three new shared overrides.
The registry's exact Q65 `ORDER BY` expectation includes both `COLLATE "C"`
clauses only for PostgreSQL and TimescaleDB; other engines and order keys keep
their existing expectations.

The combined seven-path patch's full non-live Python validation passed:
**2,264 passed, 1 skipped, 14 live tests deselected**, in 86.14 seconds (exit 0).
All repository Python tests were
collected from `olap_benchmarks/tests/`, with `--basetemp` in a task-specific
`/tmp` directory outside Git and no Git discovery ceiling override. Full strict
Pyright reported zero errors and warnings; Ruff lint/format and whitespace checks
passed. Existing DuckDB/nanobind shutdown diagnostics recurred (30 instances,
2 keep-alive records, 6 types, 85 functions), without changing the successful
exit status. SQL parsing uses an in-memory DuckDB connection with one thread;
the new regression tests require no captured data or external engine.

## Adopted Q49 arithmetic contract

Following final independent review, `queries/postgres/49.sql` adopts an explicit
arithmetic contract: the binary64 quotient of exactly summed operands
first cast to `DECIMAL(15,4)`, then individually converted to `DOUBLE PRECISION`.
All 12 operands are covered: numerator and denominator of return and currency
ratios in the web, catalog, and store branches. These conversions occur after
`SUM`; aggregation remains exact. The existing inner decimal casts, joins,
filters, grouping, rank expressions, rank filters, output ordering, and limit
are preserved. The same existing PostgreSQL-family resolver serves TimescaleDB.

Candidate SHA256 (4,666 bytes):
`4d408183a0884a31edbb341f8ff1ab0f159ed00615f5044e52fb787fd4c3797b`.
Unchanged base Q49 SHA256:
`2a4a143b28d1dda71b3113b04d4d2e264e95fa9fbe314bbfd7b13c2c060e174e`.
The four existing engine overrides remain byte-identical. DuckDB's native
division and the ClickHouse, MonetDB, Doris, and StarRocks overrides use binary64
quotients. The overrides convert after summation; Doris/StarRocks convert the
numerator to promote division, while ClickHouse/MonetDB convert both sums.
The PostgreSQL candidate explicitly retains `DECIMAL(15,4)` on both operands.

Historical evidence does not make PostgreSQL's original arithmetic wrong.
Catalog items 31015, 44507, and 87925 have the same exact rational ratio `80/81`.
PostgreSQL decimal division retains their tie at rank 340. Binary64 operand
conversion and division produce `0x1.f9add3c0ca458p-1` for the first two and
`0x1.f9add3c0ca459p-1` for item 87925, which has rank 342 in the captured DuckDB
population. Fractional decimal values generally cannot be represented exactly
in binary64 even when their unscaled coefficients fit below `2^53`; conversion
rounding is part of the numeric behavior verified by the bounded probes.

The parent's historical review found StarRocks' pre-`37daaae` exact-decimal
answer hash `blake2b128:63559e6e707ae105fb454dfe98e4dee5`, matching PostgreSQL
`2026-10-timeout-recovery/run695`. Commit `37daaae`
introduced `DOUBLE` and changed StarRocks' own ranks to the peer result
`blake2b128:3aa2e4c29ca4495b945113f9b9b87d77`. The earlier protocol's rank
preservation claim was relative to the DuckDB reference, not preservation of
StarRocks' previous decimal rank semantics. Each historical result remains
evidence under its own arithmetic model; the earlier records are not rewritten.

The new regressions require exactly 12 outer double casts and equality with
the complete original AST after removing only those wrappers. They also pin
the base/other-engine SQL and use the three saved aggregate operand pairs to
demonstrate the decimal tie versus binary64 split without a database query.
This small offline fixture predicts tie groups, not the full population's ranks.

The separate bounded seven-SELECT probe passed all predictions. Its sealed
artifacts are under the original frozen worktree's
`data/temp/tpcds-sf10-row-diagnostics/probes-q49-binary64-contract-v1/`:

| Artifact | SHA256 |
| --- | --- |
| `findings.json` | `6e0f184407ef1fb52aeece735503c52a9299e9c8d42536a322de8cbcaee68ccd` |
| `result.json` | `f61f3e3ce9e0d4e5bc6686e33be35752dfb31b6599fe160ac847a065f84e0b71` |
| `evidence-manifest.json` (74 files) | `6926ec985caeaf75e36b10200a44cd5347d9e4b1bbde54b3e0f806afb13ac093` |
| `all-rank-differences.json` | `ade5d840c119982b38d84f5caca2e3aab5343ea7032b8e1bf07958c0b11f8d08` |

All 74 manifest entries were reverified against their recorded sizes and hashes.
Full pre-rank aggregate captures cover all 696 groups, with all four exact
decimal operands equal between PostgreSQL and DuckDB:

| Channel | Groups | Currency-rank changes | Return-rank changes |
| --- | ---: | ---: | ---: |
| Web | 173 | 6 | 0 |
| Catalog | 397 | 38 | 0 |
| Store | 126 | 4 | 0 |
| Total | 696 | 48 | 0 |

Offline exact-Fraction ranking reconstructs the original PostgreSQL answer
`blake2b128:63559e6e707ae105fb454dfe98e4dee5`. Binary64 operand division
reconstructs the DuckDB answer
`blake2b128:3aa2e4c29ca4495b945113f9b9b87d77`; the frozen PostgreSQL candidate
produced that same hash and all 41 predicted ordered normalized rows. Neither
the top-10 OR-filter membership nor the final 41-row membership changed. Of
the 48 currency-rank changes, only catalog item 87925's `340` to `342` change
appears in the final projection; the other 47 changed groups are not projected.
The complete list remains in `all-rank-differences.json`.

All 63 previously observed DuckDB ratios and all 41 candidate projected
`return_ratio` values were bitwise equal to `float(numerator) / float(denominator)`.
This covers 104 observed ratios, not full engine ratio arrays for every group.
All 3,685 PostgreSQL driver scalars, including 2,784 Decimals, survived frame
conversion unchanged. These results validate the explicit arithmetic model and
the candidate's predictions; majority agreement is not a truth criterion.
The probes measured correctness only, and all their engines were stopped.
Final independent Opus review approved the combined four SQL files, two test
files, and scientific contract, with 48 focused checks passing. Adoption does
not clear any historical validation status; publication requires fresh cohorts.

Live evidence for the new SQL covers PostgreSQL SF10 only. PostgreSQL SF1 and
TimescaleDB at both scales remain predictions until fresh full cohorts verify
them. `2026-10-timeout-recovery/run693` is old-source PostgreSQL SF1 evidence:
its returned answers validated, while Q11 failed. That validation does not
verify the new SQL.

All four replacement cohorts must execute the full 99-query suite from the
new commit; the four changed queries must not be spliced into old cohorts:

- `postgres:tpc_ds:sf1`
- `postgres:tpc_ds:sf10`
- `timescaledb:tpc_ds:sf1`
- `timescaledb:tpc_ds:sf10`

These conversions change SQL work on grouped sums and have unmeasured cost;
there is no zero-overhead claim. Q49 timings, like the three explicit-collation
queries, require a new source cohort rather than direct comparison with
`2026-10-timeout-recovery/run695`
as unchanged work. Hashing, canonicalization precision, validation,
and historical rows remain unchanged. Original `2026-10-timeout-recovery/run695`
and its journal stay sealed and blocked. The separate seven-cell safe plan
excludes all four replacement TPC-DS cohorts for source-cohort consistency.
No engine services or benchmarks were started by this implementation. The
seven-path commit is approved; publication requires the fresh full cohorts.
