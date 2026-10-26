# October 2026: final five-cell timeout-recovery plan

**Prepared, not started.** The [plan](2026-10-timeout-recovery-final-plan.json)
and [manifest](2026-10-timeout-recovery-final-manifest.json) define revision
`2026-10-timeout-recovery-final` in bound worktree `t3-69104cbd`, branch
`t3/monetdb-select-timeout`. Adopted source is
`ad9f6a18b1812e0e86fef83ed4508ba6c2a5275b`, tree
`40490461cdb6f055e18bcd66e2f3e8967cdd34f4`; PR33 merged to main at
`0abcf0fb9067663dab506a56757745cb79c2bcaf`. Preparation does not move HEAD.
The eventual clean documentation commit must retain this exact executable
source and be bound with all three document hashes in the ignored pre-START
handoff. Independent review and the parent's explicit START remain required.

This plan supersedes the **unstarted, prepared-only** four-cell
`2026-10-timeout-recovery-tpcds` plan at
`b83648cee849e0e9e3707e3b2da781c2a512019e`. That worktree and its documents remain
untouched. It produced no measurements and is not another completed campaign.

## Five fresh cohorts and preserved contracts

Run serially in this exact order, with a fresh full native population per cell:

1. `timescaledb:tpc_ds:sf1` — all 99 queries.
2. `postgres:tpc_ds:sf1` — all 99 queries.
3. `timescaledb:tpc_ds:sf10` — all 99 queries.
4. `postgres:tpc_ds:sf10` — all 99 queries.
5. `monetdb:clickbench:sf1` — all 43 queries, last.

There are **439 query slots and zero analytical runs**. All four TPC-DS Cell
objects are raw-byte/deep equal to the approved b836 plan. Relative to the
original plan, only their SQL hashes for Q31/Q49/Q65/Q85 differ. The other
95 hashes per cell, all names, schemas, source-run IDs, reasons, input keys and
exclusions are unchanged. The MonetDB Cell is raw-byte/deep equal to the
original. Empty query/table filters, no registry exclusions, original warmups
and three measured iterations per supported query remain unchanged. A timeout
can prevent measurements; 439 slots is a planned scope, not a success claim.
Never splice replacement queries or old successful iterations into these cohorts.

ClickBench Q28 retains its unchanged suite-wide cross-engine answer-comparison
exclusion: it still runs, is timed and counts among the 439 planned positions,
but its row count and answer hash are not compared across engines.
`registry_exclusions={}` describes unsupported-query skips only; it does not
remove this answer-comparison exclusion.

Retain all original pins, all nine input manifests, the shared input directory,
and the original selected source asset. `analytical.clients=[]` is the sole
analytical change. Budgets remain 10 CPUs, 24 GiB server memory, 28 GiB sampled
client RSS, 300 seconds per query, outer query supervision of 300+30 seconds,
14,400 seconds per operation, and a 40 GiB runtime free-disk reserve. Driver,
image, CPU/memory settings, global hashing, validation and measured iteration
boundaries are unchanged. No scoring-policy change is authorized.

## Adopted TPC-DS contract and evidence

The reviewed [SQL contract](2026-10-tpcds-pg-collation-portability.md) applies to
both PostgreSQL-family engines. Q31/Q65/Q85 add `COLLATE "C"` only to existing
text sort expressions. Q49 uses binary64 division of exactly summed operands
after the existing `DECIMAL(15,4)` casts: all 12 operands convert to double
**after SUM**, preserving exact aggregation. The manifest records all four
before/after hashes and unchanged schema contracts.

Three sealed diagnostic indexes cover 201 artifacts. PostgreSQL SF10 probes
verified all 696 pre-rank groups' exact operands, 48 currency-rank changes,
zero return-rank changes, and unchanged top-10 OR membership and final 41-row
membership. Only catalog item 87925's rank 340 to 342 appears in the final
projection; the other 47 changes remain disclosed. The original exact-decimal
answer is valid under its own model; majority agreement alone is not truth.
Independent Opus review approved the source contract and implementation.
Live new-SQL evidence covers PostgreSQL SF10 only; PostgreSQL SF1 and both
Timescale scales still require actual full cohorts. Never apply SF10 hashes to
SF1. Explicit collation and conversions change server work, so all four full
99-query cohorts need new timings under this source.

## Adopted MonetDB deadline and bounded proof

The reviewed [native-timeout record](2026-10-monetdb-native-select-timeout.md)
describes configuration once per actual SELECT SQLAlchemy/ADBC connection,
including cached sessions and reconnects, before measured work. Configuration
fails loudly and cleans up on failure. Population/mutation semantics remain
unlimited; this adds no population cap or per-iteration setup. Runtime evidence
records the requested native cap and checks its actual session readback.

The pinned Dec2025-SP4 source and procedure references in that record establish
`CALL sys.setquerytimeout(seconds)` and raw `sys.sessions.querytimeout` in
**seconds**, with zero unlimited. Label only this verified field's unit; do
not relabel historical metadata or infer other fields' units.

The parent-owned fixed 8,192-row correctness probe has 30 sealed artifacts.
Direct ADBC `fetch_arrow_table`, native cap 2, with no Python alarm raised
`adbc_driver_manager.OperationalError`, SQLSTATE `HYT00`,
`AdbcStatusCode.TIMEOUT` (12), after 2.137776 seconds. Native cap 2 plus existing
main-thread `query_deadline(2)` surfaced `builtins.TimeoutError` after
2.391715 seconds: SIGALRM interrupted native error construction, with **no
preserved native cause/context**. Neither observation is a performance claim.
Strict 300-to-300 readback, explicit rollback survival, None-to-zero, rollback
and `SELECT 1`, same-session current-cap readback, reconnect recovery and zero
tagged-session/staging baselines passed. Parent completion was 11.459877 seconds,
with cleanup within its bound and no intervention; its owned container/dbfarm
were removed. A 1-second retry was not needed. Independent Opus evidence review
approved adoption without a shared-classifier change or engine-defect claim.

This proves neither 300-second expiry nor ClickBench Q32 recovery. Checks in
MonetDB are cooperative. The fresh full 43-query cohort must retain actual
runtime `querytimeout=300` seconds, `session_baseline_after=0`, and no outer
watchdog intervention. If Q32 raises a clean `TimeoutError`, the existing
`execute_query_with_isolation` rolls back, calls the inherited no-op
`reset_session`, skips remaining Q32 iterations and returns false;
`run_select_queries` then continues Q33–Q42 in the **same SELECT attempt** and
raises `FailedQueriesError` only at the end. This is conditional behavior, not
a guarantee. Rollback/recovery failure, outer 330-second watchdog intervention,
resource guards or a validation conflict stop for review. Cascading Q33+
dead-session errors mean recovery failure, not independent per-query outcomes.
An acknowledgement advances the next cell only; it cannot resume an aborted
SELECT. Never auto-bypass a blocked outcome or retry via plain `--resume`.

## Ownership, corrective closure and START gates

The seed remains the original selected asset, 143,929,344 bytes, SHA256
`832e28599501d125f2f4e0e35bb5251803955fbe1f6066f02c949c9df59f833f`.
Only after corrective terminal closure and a quiet-host gate, stage an
independent regular mode0444 copy at this worktree's
`data/frozen/2026-10-selected-source.db`; never seed from a recovery database.
No copying, input inspection or large rehash occurs during this preparation.
Shared inputs are read-only. Owned outputs are
`results/2026-10-timeout-recovery-final.db` and its seed/WAL sidecars,
`data/temp/timeout-recovery/`, `data/temp/<engine>/` scratch and these five
actual stores (ClickBench has `scalable=False`):

- `data/dbs/timescaledb/tpc_ds_sf1/`
- `data/dbs/postgres/tpc_ds_sf1/`
- `data/dbs/timescaledb/tpc_ds_sf10/`
- `data/dbs/postgres/tpc_ds_sf10/`
- `data/dbs/monetdb/clickbench/`

Preparation and handoff records belong
in ignored `data/temp/final-preparation/`; sealed native probe evidence is
separate and must be retained. Never clean protected worktrees or unowned data.

Corrective source remains `9acda00c6b40df50f405103e04f335c782452cd5`. The r3
report/seals are a **historical stopped prefix at blocked run693**, referenced
only through independent immutable checkpoint `run693-before-ack` (95 files).
The manifest pins its seal, mapping and four mirrored r3 references. Index
source paths can name now-mutating files: later verification must resolve them
through `source-to-checkpoint.json` and hash **mirror copies only**. Never read
active original DB/journal paths to verify that prefix.

The parent subsequently acknowledged the failed SELECT: raw
`2026-10-timeout-recovery-corrective/run693` remains failed, the journal records
`accepted_failed_select`, and Timescale TPC-H SF50
`2026-10-timeout-recovery-corrective/run694` started at
2026-10-25T23:45:40.255Z. This is a parent-reported active milestone, not final
closure evidence. This preparation reads no active outputs. Before any checker
or START, require a reviewed FINAL corrective terminal outcome, cleanup and
zero writers/engines/heavy processes; bind the FINAL report, seals, terminal
envelope and zero-writer evidence paths/hashes in a **new ignored**
`data/temp/final-preparation/pre-start-handoff.json`. Prefix seals cannot
discharge those gates. Terminal blocked outcomes remain blocked in history.

At START require **at least 155 GiB free**, targeting **158 GiB**, after
authorized cleanup. This is a separate parent-controlled START gate; the
runner's runtime reserve remains **40 GiB**. Recheck free space, source/branch,
clean documentation commit, plan/document hashes, expected existing images,
pins/environment, seed identity, seals and process/container absence in the
quiet window. No pulls, installs, budget increases or concurrent campaigns.
From START through terminal closure, no commits, branch moves or tree changes
are permitted in this bound worktree.
The runner verifies full input hashes before each population. Require complete
actual native population and all table row-count gates before SELECT,
including exactly **99,997,497** MonetDB ClickBench `hits` rows, the expected
count from the frozen `hits.parquet`. Empty filters do not alone prove
complete data. Failed population cannot authorize a dependent SELECT.

Reuse the original eight clean seed baselines, with TPC-DS SF1/SF10 and
ClickBench SF1 needed here; keep `include_failed=True`, `mark=False`, unchanged
validator and controls. Do not regenerate heavy baselines. Verify the 304 older
campaign seals, 201 diagnostic artifacts, 30 native artifacts and immutable
95-file prefix through their indexes in the future quiet window. Resolve FINAL
corrective closure separately. Any new validation finding blocks review;
never clear old or new validation flags to continue.

These are **future commands**, from this exact bound worktree. The checker
hashes the seed and checks containers, so it is also deferred. Explicit `--plan`
is mandatory; the default points to another workspace. Once quiet preflight
and seed staging are authorized:

```sh
uv run --no-sync python scripts/timeout_recovery.py check --plan docs/campaigns/2026-10-timeout-recovery-final-plan.json
```

After independent review, the frozen clean documentation commit, complete
ignored pre-START handoff and the parent's explicit START, restate and execute:

```sh
caffeinate -i uv run --no-sync python scripts/timeout_recovery.py run --plan docs/campaigns/2026-10-timeout-recovery-final-plan.json
```

No command here authorizes START or a resume. Existing supported
acknowledgement paths require concrete terminal evidence and parent review;
they do not turn failed or unstarted measurements into successful ones.

## Validation performed and final accounting

Preparation performs only stdlib JSON/Git-blob comparisons and small source
hashes: raw/deep Cell equality, allowed SQL substitutions, all 439 source SQL
hashes, schemas, unchanged settings/pins/inputs/analytical fields and ownership
paths. It does not execute the runtime checker, tests, installs, engine/image
commands, probes, seed copies or large hashes. Prior source validation remains
2,283 passed, 1 skipped, 14 live deselected; full Ruff and strict Pyright passed.
Those prior checks do not discharge the new runtime or campaign gates.

Join measured histories from `2026-10-timeout-recovery`,
`2026-10-timeout-recovery-continuation`, `2026-10-timeout-recovery-remaining`,
`2026-10-timeout-recovery-corrective` and, only if executed, this final revision.
Retain the superseded TPC-DS plan separately as prepared/unstarted. Numeric IDs
collide from run685 again: qualify every run/step ID by revision; original Cell
`source_run_id` remains selected-seed scoped. Preserve all failed, blocked,
interrupted, unstarted and successful attempts and all actual analytical results.

In particular, original `2026-10-timeout-recovery/run695` stays
validation-blocked; old-source original run693 is not proof of new TPC-DS SQL;
`2026-10-timeout-recovery-continuation/run689` analytical16 failed in warmup
with no measurements; and `2026-10-timeout-recovery-remaining/run694` retains
its validation failure despite raw SELECT completion. Corrective MonetDB
run691 remains the old native-cap-zero timeout attempt. This five-cell plan
does not resolve every historical failure or imply all 17 original cells
succeeded. Do not publish this database alone, alter historical statuses,
splice cohorts or change scoring policy. Final publication/cleanup disposition
belongs to the parent after reviewed combined accounting.
