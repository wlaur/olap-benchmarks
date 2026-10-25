# October 2026 timeout recovery: seven corrective cells

Prepared only: no commit, container start or performance START. The executable
[plan](2026-10-timeout-recovery-corrective-plan.json) and compact
[manifest](2026-10-timeout-recovery-corrective-manifest.json) are the concrete draft for
Opus review and parent/Astra disposition. The parent will authorize the reviewed
documentation commit and START after its quiet-host and disk-cleanup gates.

The bound workspace is `/Users/williamlauren/.t3/worktrees/olap-benchmarks/t3-f9877fbb`,
branch `t3/prepare-timeout-recovery-cells`. After confirming the exact binding, clean
tracked/untracked state, initial HEAD `efaae1c9f31a9c917b074e3c60da74c0572d34e6` and no
task-owned commits, only this fresh branch was realigned to PR29 merge
`dedca0bbcc7511ff69b4daa4cd18da49bb48563a`, as explicitly authorized by the parent.
A fast-forward was impossible because PR28 squash-merged the original feature.
No prior branch or worktree was moved. Only these three new documents may differ
from PR29. Record the future clean documentation commit in the ignored handoff and
runner journal; no commit may be made after START.

## Frozen execution scope

Execute serially in this order, with a fresh complete population for every cell:

1. `questdb:rtabench:sf1` — 41 query slots.
2. `timescaledb:rtabench:sf1` — 41.
3. `timescaledb:time_series:sf1` — 62.
4. `monetdb:clickbench:sf1` — 43.
5. `postgres:tpc_h:sf50` — 22.
6. `timescaledb:tpc_h:sf50` — 22.
7. `timescaledb:clickbench:sf1` — 43.

These 274 slots retain the original registry exclusions. Each Cell object is
deep-equal and raw-JSON-byte-equal by key to the original 17-cell plan. All settings,
pins, nine input manifests, input directory, SQL/schema hashes, source identities,
reasons and cell exclusions remain unchanged. The only top-level changes are
workspace, branch, revision, source path, ordered cell subset, provenance and
`analytical.clients=[]`; all other analytical fields remain identical. The original
plan remains the authority for the full analytical contract.

MonetDB ClickBench is included because `2026-10-timeout-recovery/run688` records
Q32's in-process `Query exceeded 300 seconds`, then the parent watchdog's
`Query Q32 exceeded 300 seconds`, with Q33–Q42 unstarted. PR29's SELECT cancellation
grace applies to this path too, so the parent accepted Opus's recommendation for a
new complete attempt. Preserve the original failed/partial run and its raw errors;
never splice its successful queries into the new cohort. TimescaleDB ClickBench
runs last to limit how much unrelated work a validation block could leave untouched;
its validation and stopping rules remain unchanged. PostgreSQL ClickBench's
population disk failure in `2026-10-timeout-recovery/run691` is unaffected by PR29,
remains an acknowledged failure, and is not retried here.

Budgets remain 10 CPUs, 24 GiB engine memory, 28 GiB sampled client RSS, a 40 GiB
free-disk reserve, 300 seconds per query and 14,400 seconds per operation. Empty
query/table filters, full input populations, original warmup and three measured
iterations per supported query are mandatory. No scheduler, retry bypass, validator,
hash policy, SQL or input changes are proposed. The frozen runner and its 33 tests
are unchanged. TimescaleDB TPC-DS SF1 and SF10 remain outstanding for new SQL
cohorts, along with prospective PostgreSQL TPC-DS SF1 and SF10 cohorts. None is
waived or included here. The parent reports that initial Opus numeric review approved
COLLATE and recommends an explicit Q49 binary64 contract consistent with five
existing engine paths; agreement by majority does not establish truth. The next
correctness-only ratio-contract probe and prospective production SQL changes are
separate work. PR29 remains this campaign's frozen source base; those pending
changes do not affect these seven cells and are not adopted here. Retained diagnostic
stores remain needed for that probe and must not be modified or deleted by this
campaign. The parent will clear its owned stores before performance START under
its quiet-host and disk gates.

## Ownership and evidence

Revision: `2026-10-timeout-recovery-corrective`. Writable outputs belong only to this
workspace: `results/2026-10-timeout-recovery-corrective.db`,
`data/temp/timeout-recovery/{state.json,summary.json,runner.lock,<attempt-id>/}`,
`data/dbs/<engine>/<suite-scale>/` and `data/temp/<engine>/`. Preparation evidence
lives in ignored `data/temp/corrective-preparation/`. No campaign result database,
seed staging file or journal directory may exist before START.

The sole seed is the original workspace's `site/public/data/results.db`, copied
independently to owned `data/frozen/2026-10-selected-source.db`, a regular mode0444
file with a distinct inode: 143,929,344 bytes, SHA256
`832e28599501d125f2f4e0e35bb5251803955fbe1f6066f02c949c9df59f833f`.
This value was read from the original committed plan and verified against both
files. Never seed from any recovery database. Shared inputs remain read-only at
`/Users/williamlauren/.t3/worktrees/olap-benchmarks/t3-3dcf1cb2/data/input`.
No prior input hash cache is copied. The runner verifies full input hashes before
each population; preparation verifies names and byte sizes.

The manifest references existing seal indexes instead of duplicating their entries:
75 original artifacts, 102 baseline artifacts, 34 second-campaign artifacts and
93 third-campaign artifacts, plus the two second-campaign wrappers and the third
terminal report/seal wrapper. Rehash every referenced entry before START and after
completion, blockage or interruption. Stop on mismatch; never refresh unexplained
seals. The eight existing zero-finding seed baselines cover the four required new
suite/scale scopes; their unchanged validator, pins, seed and artifacts allow reuse.
Do not regenerate the heavy baseline evidence. Ad hoc results inspection uses only
the read-only `./.venv/bin/olap results query --revision <revision> "<sql>"` CLI.

Prior workspaces `t3-416ee4f7`, `t3-01c9b611` and `t3-f4c709e8` remain frozen at
`efaae1c9f31a9c917b074e3c60da74c0572d34e6`,
`ed78de3b03153036735fec2a414c6556e3a63dcf` and
`f5b24e2307684866b13dda289ac62b82289cf287`. Their raw databases, journals, reports,
acknowledgments and diagnostic evidence must remain untouched.

## PR29 behavior and readback

QuestDB's new startup command must contain `-e QDB_QUERY_TIMEOUT=300s`. The generated
command and run metadata are configuration evidence, not effective readback. As part
of the parent's authorized START sequence, before the performance runner, start one
owned configuration-only QuestDB instance using the same PR29 RTA startup command,
image and budgets. Capture container ID, image ID, owned mounts, environment and the
following read-only response; preserve it under `data/temp/corrective-preparation/`:

```sql
(SHOW PARAMETERS) WHERE property_path = 'query.timeout';
```

Require `env_var_name=QDB_QUERY_TIMEOUT`, `value_source=env`, and a value representing
300 seconds. Preserve the raw value and units; do not label an environment listing
as server readback. This response proves the configured value, not runtime timeout
enforcement. The [pinned 10.0.1 SHOW implementation](https://github.com/questdb/questdb/blob/10.0.1/core/src/main/java/io/questdb/griffin/engine/functions/catalogue/ShowParametersCursorFactory.java)
exposes these fields. Stop and remove only that verified owned container; confirm no
benchmark containers remain. The unchanged runner then cleans the owned store and
performs a fresh full population. No benchmark query or row input is executed by the
configuration-only check. This check is pending, not claimed as preparation evidence.
Missing or contradictory readback blocks performance START. Retain the actual
population/SELECT startup commands to prove they use the same setting.
After measurement, inspect `0025_product_category_performance` for any native abort
near the prior 60-second limit. If it aborts at 300 seconds, preserve the actual
duration and raw runtime error as a failure. If it is unstarted or finishes before
60 seconds, do not infer that 300-second enforcement was demonstrated. A readback
alone cannot establish that the old native timeout was removed from this query path.

For PostgreSQL TPC-H SF50, the native statement timeout and in-process query alarm
remain 300 seconds. The SELECT parent watchdog begins abort/cleanup only after
300 + 30 = 330 seconds for an active query, subject to its polling interval and
existing SIGINT/TERM/KILL escalation waits. This is not another 30-second query
budget after 330. Disk, RSS and the 14,400-second operation limit still apply.
A query exceeding 300 seconds remains a failure with its raw native exception;
successful fetches after budget receive no success credit. Later queries continue
when the connection recovers. If cleanup cannot recover, preserve interruption and
unstarted queries. The same SELECT parent grace applies to MonetDB ClickBench,
without changing its 300-second in-process query limit or crediting late successes.
The analytical watchdog has no added grace.

Both PostgreSQL and TimescaleDB retain unchanged ClickBench Q18 SQL. Their PR29
client guard verifies finite integral minutes before the Int64 cast, preserves
nulls, and executes inside the recorded fetch duration. No claim of zero timing
overhead is made; the global timing boundary and hash policy remain unchanged.

## Honest failures and combined reporting

The three sealed campaigns together retain 17 original cells: 2 complete,
7 acknowledged failures, 3 blocked and 5 untouched. Acknowledged failures are honest
partial/failed outcomes, never successful measurements. The original campaign's six
attempts span `2026-10-timeout-recovery/run685` through
`2026-10-timeout-recovery/run695`; the latter retains its sealed TPC-DS SF10
validation block. Its Q49 exact-decimal result is equivalent under the original
arithmetic semantics and must not be marked wrong; the proposed binary64 contract
applies prospectively to new SQL cohorts. The second spans `2026-10-timeout-recovery-continuation/run685`
through `2026-10-timeout-recovery-continuation/run689`. The third spans
`2026-10-timeout-recovery-remaining/run685` through
`2026-10-timeout-recovery-remaining/run694`; the latter completed raw SELECT but
retains `validation_ok=false`. PR29 and any new successes do not rewrite these facts.
The original 17-cell accounting remains unchanged until new measurements exist;
prospective TPC-DS cohorts do not change historical dispositions.

No new analytical runs are scheduled. Prior PostgreSQL TPC-H SF10 clients1 and4,
`2026-10-timeout-recovery-continuation/run687` and
`2026-10-timeout-recovery-continuation/run688`, completed and validated with only
14 and 34 measured requests; p95/p99 remain null for insufficient samples. Clients16,
`2026-10-timeout-recovery-continuation/run689`, failed warmup: 103 offered,
101 completed, two native 300-second Q9 timeouts. Measurement never started,
measured requests are zero and QPS is null. This original cell remains blocked.

Any validation finding stops the new runner. Never acknowledge failed SELECT
validation, edit old or new correctness flags, suppress findings, or use plain
`--resume` to retry a blocked cell. Existing failure acknowledgment paths require
parent review and their unchanged terminal-evidence/provenance/cleanup gates;
SELECT acknowledgment additionally requires clean answer validation. Population
acknowledgment cannot invent a SELECT attempt. No automatic continuation is added.

All reported run/step identities must include revision. Immutable plan
`source_run_id` fields retain their selected-seed context; they are not cross-revision
join keys. Combine final accounting with the sealed third report, preserving every
prior attempt and all analytical limits. A new `measurements_complete` summary means
only these seven cells; both TimescaleDB TPC-DS scales, prospective PostgreSQL TPC-DS
cohorts and prior blockers remain outstanding. Do not splice
queries across attempts or publish the new database alone. No publication is part
of this task.

## Checks and parent handoff

Preparation uses `uv sync --frozen --python 3.14`, the same installed dependency set
after editable-path normalization, a mode0600 owned environment, and read-only
inspection of the four pinned engine image IDs/digests. MonetDB's image ID and
RepoDigests must match both original `2026-10-timeout-recovery/run687` population
and `2026-10-timeout-recovery/run688` SELECT metadata. Its image tag and engine pin
remain unchanged; no image pull or version change is permitted. The existing
ClickBench SF1 seed baseline already covers this added cell.
The prior 33 recovery tests, Ruff lint/format and strict Pyright results remain
valid because source and the ignored verifier are unchanged. This scope-only
revision rechecks contract byte/deep equality, recursive seals, source integrity,
JSON/whitespace and:

```sh
uv run --no-sync python scripts/timeout_recovery.py check \
  --plan docs/campaigns/2026-10-timeout-recovery-corrective-plan.json
```

It must report `7 cells + 0 analytical runs; no measurements`. Full-source PR29
integrity and runner/validator parity are checked separately. Record actual results
in the manifest and ignored handoff; do not infer performance from these checks.

Before performance START, the parent must record at least **155 GiB free** after
cleaning only its owned diagnostic stores; the target is approximately 158 GiB.
Opus's `run_metric.disk_mb` review found the largest prior peak in TimescaleDB
ClickBench `2026-10-timeout-recovery-remaining/run693`: 107.6 GiB. PostgreSQL SF50
`2026-10-timeout-recovery-remaining/run685` peaked at 97.5 GiB and MonetDB ClickBench
`2026-10-timeout-recovery/run687` at 45.7 GiB. TimescaleDB SF50 remains unmeasured;
an uncompressed PostgreSQL-like footprint is a planning assumption, not evidence.
The 155 GiB floor budgets 107.6 GiB plus the unchanged 40 GiB runtime reserve and
approximately 7.4 GiB for results, logs and sampling headroom.
This is an additional parent START-headroom gate only: the plan setting and runner's
runtime reserve remain 40 GiB. If 155 GiB is unavailable, stop; do not delete unowned
data. Preparation's free-space observation does not discharge this gate. Diagnostic
stores remain needed while the correctness probe is active; no cleanup is part of
this preparation.

After concrete review, the parent's reviewed docs-only commit, quiet-host/disk gates,
fresh evidence/environment/image verification, and the QuestDB readback above,
the exact performance command is:

```sh
caffeinate -i uv run --no-sync python scripts/timeout_recovery.py run \
  --plan docs/campaigns/2026-10-timeout-recovery-corrective-plan.json
```

The explicit plan path is mandatory. Retain console output in owned preparation
storage. Only the parent signals START; no additional human confirmation is needed.
The current free-disk observation does not discharge the parent's cleanup gate or
authorize deletion of diagnostic stores. Keep host-intensive work and other
schedulers stopped during measurement; report any terminal blockage before further
execution decisions.
