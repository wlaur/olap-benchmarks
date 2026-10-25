# October 2026: four complete TPC-DS cohorts with explicit SQL semantics

Preparation only. The [plan](2026-10-timeout-recovery-tpcds-plan.json) and
[manifest](2026-10-timeout-recovery-tpcds-manifest.json) define revision
`2026-10-timeout-recovery-tpcds` in bound worktree `t3-13d41a82`, branch
`t3/timeout-recovery-fixes`. Its source is PR30 merge
`dc96d03e526eeb9cdd31fd4534e72a20a0ffe3ca`, tree
`4800c815c652fb021de7cb07d43f34617e233731`. Only this branch was fast-forwarded;
prior workspaces and the current corrective measurement branch remain untouched.

## Scope and preserved contracts

Run one campaign serially, with a fresh full native population and the full
99-query SELECT cohort for each cell, in this exact order:

1. `timescaledb:tpc_ds:sf1`
2. `postgres:tpc_ds:sf1`
3. `timescaledb:tpc_ds:sf10`
4. `postgres:tpc_ds:sf10`

There are 396 query slots, empty query/table filters, no registry exclusions,
the original warmup policy, and three measured iterations per supported query.
Do not splice four replacement queries into older cohorts. The four original
Cell objects retain every field except `sql_sha256` entries `31`, `49`, `65`,
and `85`. The other 95 hashes per cell, all 99 names, schemas, source-run IDs,
reasons, input keys, and exclusions are unchanged. Source-run IDs remain scoped
to the selected seed, not joined to recovery revisions by numeric ID alone.

Original settings, engine pins, input directory and all nine input manifests
are retained. Budgets remain 10 CPUs, 24 GiB engine memory, 28 GiB sampled client
RSS, 300 seconds per query, 14,400 seconds per operation and a 40 GiB runtime
free-disk reserve. `analytical.clients=[]`; all other original analytical fields
are retained but no analytical work is scheduled. There is no resource increase.

`PostgresTpcDs.query_path` resolves engine-specific SQL, then a shared PostgreSQL
override, then base SQL. Both engines resolve the four changes to committed
`queries/postgres/{31,49,65,85}.sql`. Q31/Q65/Q85 explicitly use `COLLATE "C"`
only on their existing text sort expressions. Q49 divides binary64 conversions
of exactly summed operands after their existing `DECIMAL(15,4)` casts, across
all 12 operands. Other query structure and all global hash/validation policies
are unchanged. Before/after SQL hashes and unchanged schema hashes are recorded
in the manifest. Sort costs and grouped-sum conversions change server work;
new timings are not direct unchanged-work comparisons with old SQL cohorts.

## Evidence and validation

The adopted [source contract](2026-10-tpcds-pg-collation-portability.md) records
the complete correctness evidence. Three immutable diagnostic manifest
references cover 201 artifacts; their entries are not duplicated here. The
bounded PostgreSQL SF10 probes matched peer hashes for Q31/Q65/Q85 and Q49.
All 696 pre-rank aggregate groups had identical exact operands across PostgreSQL
and DuckDB. Exact and binary64 models reconstruct their respective historical
results: 48 currency ranks change, zero return ranks change, and no top-10 OR
membership or final 41-row membership changes. Only item 87925's rank change
from 340 to 342 appears in the final projection. The other 47 changes remain
disclosed. The model comparison does not make original exact-decimal SQL wrong.

Live evidence for new SQL covers PostgreSQL SF10 only. PostgreSQL SF1 and both
TimescaleDB scales remain predictions until these full cohorts run. Expected
answers follow the explicit contract and should match the selected seed's
current peer consensus within each scale; majority agreement alone is not a
truth criterion. SF10 probe hashes must not be applied to SF1. The unchanged
validator, with `include_failed=True` and `mark=False`, retains all findings.

Reuse the original eight clean seed baselines, needing only TPC-DS SF1 and SF10
here. Their pinned manifest, validator and positive/negative controls remain
unchanged; do not regenerate heavy baselines. Any new validation finding blocks
the runner. Never suppress findings, change historical/new validation flags,
or automatically retry with plain `--resume`. Existing reviewed failure
acknowledgment gates, complete-population requirements and cleanup checks apply.

`2026-10-timeout-recovery/run695` stays raw and validation-blocked; its original
exact-decimal result is not wrong under its own arithmetic model.
`2026-10-timeout-recovery/run693` is old-source PostgreSQL SF1 evidence whose
returned answers validated while Q11 failed; it does not verify new SQL.
Historical journals, statuses and hash policy remain immutable.

## Output ownership and serialized START

The sole seed is the original frozen workspace's `site/public/data/results.db`,
143,929,344 bytes, SHA256
`832e28599501d125f2f4e0e35bb5251803955fbe1f6066f02c949c9df59f833f`.
After the quiet-host gate, copy and verify it as an independent regular mode0444
file at this worktree's `data/frozen/2026-10-selected-source.db`. Never seed from
any recovery raw database. No seed copy or large rehash is performed now.

Owned outputs are `results/2026-10-timeout-recovery-tpcds.db` (and its seed/WAL
sidecars), `data/temp/timeout-recovery/`, `data/dbs/<engine>/tpc_ds_sf{1,10}/`
and `data/temp/<engine>/`. The manifest records these as absent during light
preparation. Ignored preparation records belong in `data/temp/tpcds-preparation/`.
The existing environment is reused without installation or synchronization;
the runner supplies plan-specific settings. Shared original inputs remain read-only.

The seven-cell corrective campaign is START-authorized and active at preparation
according to the parent. Its frozen source is
`9acda00c6b40df50f405103e04f335c782452cd5`, plan SHA256
`ce313ff8cf3d5e666f25ec3b4192944686906a9ad44488fa2ff27a242b9d14c4`.
No active result DB, journal, final status or terminal seal is read or inferred.
Never start this four-cell campaign until the corrective campaign has a reviewed
terminal outcome, all its writers/engines are stopped, cleanup is confirmed,
and the parent explicitly signals START. A terminal blocked outcome remains
blocked in the combined history; it is not converted into success.

Before START, require at least **155 GiB free**, targeting approximately
**158 GiB**, after authorized cleanup. The runtime reserve stays **40 GiB**.
Recheck at START; a preparation disk observation cannot discharge this gate.
Do not lower the floor, increase budgets or delete unowned data.

## Deferred checks and final accounting

Light preparation checks JSON structure, cell equality except the four allowed
hashes, all 99 resolved source SQL hashes, static schema hashes, exact output
absence, and unchanged small runner/validator files. It imports no benchmark
runtime and performs no engine checks, database reads, large rehashes, installs
or tests. Prior 2,264 full Python tests and 33 recovery-runner tests remain valid;
no source code or SQL is changed by these documents.

In a quiet window, stage/verify the owned seed; verify referenced 304 campaign
and 201 diagnostic seal entries and wrappers; confirm retained seed-baseline
scope, installed environment and original PostgreSQL/Timescale image IDs;
verify original input manifests without changing inputs. No image pulls or
dependency changes are authorized. Full input hashes remain checked before
each population by the unchanged runner. Run the existing plan checker only
after the corrective campaign is terminal and the seed is ready; its seed hash
and container checks are intentionally deferred now.

These are future commands, run from this bound worktree. Explicit `--plan` is
mandatory for both: the runner's default plan points to a different workspace.
Do not execute either command while the corrective campaign is active. Once
the quiet-window checks above permit the plan checker:

```sh
uv run --no-sync python scripts/timeout_recovery.py check --plan docs/campaigns/2026-10-timeout-recovery-tpcds-plan.json
```

After concrete review, record the actual clean documentation commit and frozen
plan digest in a NEW ignored `data/temp/tpcds-preparation/pre-start-handoff.json`.
Bind the corrective terminal report/seal paths and hashes there when available,
without changing committed documents or executable source. No commit may occur
during measurement. The parent then controls the explicit runner START; no
additional human approval question is part of this preparation.

Only after the fresh final pre-START handoff and the parent's explicit START,
re-state and execute this exact campaign command with the required plan path:

```sh
caffeinate -i uv run --no-sync python scripts/timeout_recovery.py run --plan docs/campaigns/2026-10-timeout-recovery-tpcds-plan.json
```

The final report must join all five distinct revisions: original
`2026-10-timeout-recovery`, `2026-10-timeout-recovery-continuation`,
`2026-10-timeout-recovery-remaining`, `2026-10-timeout-recovery-corrective`, and
`2026-10-timeout-recovery-tpcds`. Always qualify run/step IDs by revision and
retain every failed, blocked, interrupted, unstarted and successful attempt.
A new four-cell completion summary describes only this plan. Do not publish
its database alone, rewrite historical evidence, or splice queries across runs.
