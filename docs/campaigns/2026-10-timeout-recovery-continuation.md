# October 2026 timeout recovery continuation — prepared, not started

This is a separate campaign for exactly the untouched 11 cells, in the original order.
Its declaration is [the continuation plan](2026-10-timeout-recovery-continuation-plan.json).
[The cross-campaign manifest](2026-10-timeout-recovery-cross-campaign-manifest.json) seals the
first campaign's evidence and defines how both campaigns must be reported together.
Final Opus review and parent review approved the concrete plan subject to the recorded
checks and documentation corrections. The parent authorized a commit of exactly these
three documentation files. Its actual clean HEAD must be verified and recorded, and the
parent must explicitly send START before measurement. Preparation and commit approval are
not START. No push, PR, population, or measurement is authorized yet.

## Scope and isolation

The workspace is `/Users/williamlauren/.t3/worktrees/olap-benchmarks/t3-01c9b611`, branch
`t3/timeout-recovery-continuation`, based on
`efaae1c9f31a9c917b074e3c60da74c0572d34e6`. The revision is
`2026-10-timeout-recovery-continuation`. The original campaign in
`/Users/williamlauren/.t3/worktrees/olap-benchmarks/t3-416ee4f7` remains stopped. Its sealed
evidence, source files and journal remain unchanged. The parent created its baseline
scratch directory outside the sealed recovery evidence directory; the whole original
workspace is therefore not claimed to be unchanged.

The new `cells` array is a deep-equal and raw-JSON-byte-equal copy of the original plan's
`cells[6:]`, without reordering. These are its 575 query slots:

1. `postgres:tpc_h:sf10`
2. `postgres:tpc_h:sf50`
3. `questdb:rtabench:sf1`
4. `starrocks:time_series:sf1`
5. `starrocks:time_series:sf10`
6. `timescaledb:clickbench:sf1`
7. `timescaledb:rtabench:sf1`
8. `timescaledb:time_series:sf1`
9. `timescaledb:tpc_ds:sf1`
10. `timescaledb:tpc_ds:sf10`
11. `timescaledb:tpc_h:sf50`

Every cell retains its source identity, query names, SQL hashes, physical schema hashes,
registry exclusions, explicit exclusions, input key, and reason. The settings, pins, full
input manifests and analytical object are identical to the original plan. The sole explicit
MonetDB Q15 exclusion remains in the original attempted chat cell; no MonetDB cell belongs
in this continuation and every continuation `query_exclude` stays empty.

The only changed top-level plan fields are `workspace`, `branch`, `revision`,
`source_database`, `provenance`, and `cells`. The parent-approved `source_database` change
relocates the identical selected asset to an owned, read-only relative path; it changes
neither its bytes nor its SHA256. The runner resolves that path under the frozen workspace
and uses its parent directory and filename stem for the read-only source query CLI.

Budgets remain 300 seconds per query, 14,400 seconds per operation, 10 engine CPUs,
24 GiB engine memory, 28 GiB sampled client RSS and a 40 GiB free-disk reserve.
PostgreSQL TPC-H SF10 analytical clients1,4,16 remain serial, 60 seconds each, seed0,
with all 22 queries and the unchanged warmup contract. Complete fresh population,
complete SELECT measurements and successful answer validation remain prerequisites.

The shared input path is read-only for this campaign:
`/Users/williamlauren/.t3/worktrees/olap-benchmarks/t3-3dcf1cb2/data/input`.
Only this worktree's `results/`, `data/dbs/` and `data/temp/` are writable campaign paths.
No symlink redirects or input hash cache are copied. The existing runner's fixed journal
location, `data/temp/timeout-recovery/state.json`, is isolated by the new workspace.

## Seed and unresolved correctness evidence

The only seed is a byte-for-byte copy of the original workspace's selected
`site/public/data/results.db`, stored locally as the ignored, read-only file
`data/frozen/2026-10-selected-source.db`: 143,929,344 bytes, SHA256
`832e28599501d125f2f4e0e35bb5251803955fbe1f6066f02c949c9df59f833f`.
The read-only CLI found 312 runs, maximum ID684, no running runs, and no ID695 in this seed.
The local source revision name is `2026-10-selected-source`; historical source IDs below
refer to that selected asset. No fresh campaign results database exists during preparation.

At START the unchanged runner clones this seed to
`results/2026-10-timeout-recovery-continuation.db`. It never seeds from the first recovery
raw database. This deliberately excludes the first campaign's newly appended observations,
including `2026-10-timeout-recovery/run695`, from the new validation candidate database.
That exclusion is a disclosed cohort boundary, not a resolution of its correctness finding.
Its failed rows, marks, reports and journal entry remain untouched in the original revision.

`2026-10-timeout-recovery/run694` completed PostgreSQL TPC-DS SF10 population.
`2026-10-timeout-recovery/run695` failed five of 99 queries: Q06, Q11, Q30, Q81 and Q95.
Q30's raw error is `OperationalError`; its PostgreSQL statement timeout is a 300-second
deadline outcome. Retain the raw error classification while explaining that deadline in
the report. The other four failures record `TimeoutError`.
The run also has 16 answer-hash mismatch observations across Q31/Q49/Q65/Q85, with no
row-count mismatches or iteration inconsistencies. Its report used `include_failed=True`,
changed no marks, and left `validation_ok=false`. It remains blocked and unresolved.

Historical `2026-10-selected-source/run658` and `2026-10-selected-source/run660` did not
reach those four SF10 queries. SF1 observations from `2026-10-selected-source/run365`,
`2026-10-selected-source/run368` and `2026-10-timeout-recovery/run693` agree with peers;
that does not resolve the SF10 discrepancy. The original SQL, 25-file input manifest,
and clean executable provenance are preserved; no changed query or input is proposed.

The runner still requires `validation_ok` before acknowledging a failed SELECT.
There is no acknowledgment or status edit for `2026-10-timeout-recovery/run695`.
Future TimescaleDB TPC-DS SF10 mismatches must stop this campaign for review. A PostgreSQL
TPC-H SF10 prerequisite failure may also stop it at the first cell. Keep the required cell
order. If a gate prevents further work, untouched cells need another separately reviewed
plan; do not reorder, auto-accept, edit flags or reports, splice successful queries, or
weaken validation to reach them. The existing reviewed terminal-failure procedures remain
limited to their original eligibility rules; a validation failure never qualifies.

## Sealed evidence and reporting

The manifest records sizes and SHA256 hashes for the original selected seed, original plan,
raw recovery database, journal, summary, runner, all six attempt directories and every file
under the original recovery evidence directory. This includes operation/resource logs,
audits and validation reports, console logs, resume evidence, and
`supervisor-decisions.jsonl`, which contains the four SELECT acknowledgment reasons missing
from the journal's null acknowledgment fields. The separate
`data/temp/timeout-recovery-investigation-695.json` is also sealed. Evidence filenames are
preserved verbatim and their run IDs belong to the original revision.

Reverify every seal immediately before START and after the campaign, including after a
blocked or interrupted ending. A mismatch requires investigation; do not refresh the
manifest to erase an unexplained change. Retain both raw databases as distinct assets.

Every run and step identity is `(revision, id)`. For example,
`2026-10-timeout-recovery/run685` and `2026-10-timeout-recovery-continuation/run685` would
be different runs despite their colliding numeric IDs. Source IDs in unchanged frozen
objects remain scoped to the selected source asset; never treat them as global IDs.
Do not concatenate revisions by bare IDs, overwrite evidence, or assemble a replacement
cell from successful queries taken from different attempts.

The new runner's summary covers only its 11 cells and three analytical clients. Even a
`measurements_complete` summary for this continuation does not complete the original
17-cell recovery request. The combined report must still show the six original attempts:
four acknowledged failed SELECTs, one acknowledged failed population with SELECT
unattempted, and the unresolved validation blocker. There are zero completed measurement
cells in the original summary. Report analytical clients1,4,16 independently, including
unperformed, interrupted or blocked outcomes.

Publication remains blocked pending separate correctness and cohort review. Any future
publication decision must consider both campaigns, never the continuation database alone.
Passing validation against the selected seed does not excuse the excluded original
SF10 finding or establish a valid combined replacement cohort.

## Executable and environment parity

All 1,987 original tracked files in both worktrees were byte-compared against their frozen
Git blobs. Both initial HEAD trees were
`89adf2a2d61e86713f3c24db4dae2a651a6dbe0d`. The manifest records tree/blob identities for
the runner, all benchmark code and canonical SQL/schema files, dependency files, site,
CI configuration and other root configuration.

Only the three new continuation documentation files may differ from the base:
the plan, this protocol and the cross-campaign manifest. Before the approved commit and
again before START, compare all remaining paths, including staged changes, file modes,
untracked files and the original plan. The original plan SHA256 remains
`bebaa46abd731d1b36f1a0a59472005a45bcc97e6402502ee1a703b73a2007c9`.
The runner and benchmark code must remain byte-identical. No executable edits are needed.

`uv sync --frozen --python 3.14` produced the same Python 3.14.3 interpreter as the parent.
`uv pip freeze --python <workspace>/.venv/bin/python` matched all 129 entries after only
normalizing the editable project's workspace path. The ignored local `.env` has mode0600,
explicit identical budgets, empty include/exclude/table filters and only owned output paths.
No site code changed; no Bun or browser work is needed.

Read-only `docker image inspect` matched all four continuation engine image IDs and
RepoDigests against all 11 selected source runs and the recorded PostgreSQL evidence in
`2026-10-timeout-recovery/run692` through `2026-10-timeout-recovery/run695`.
PostgreSQL matches
`postgres@sha256:74935e72241653ca55e0414067e6d8763aceb8a810eb51b452253ec3dcfc4336`.
The manifest contains the other digests. No images were pulled or containers started.
Recheck immediately before START and disclose any drift rather than relying on tags alone.

The committed protocol cannot contain its own commit hash. After parent approval and the
documentation-only commit, record the actual new clean HEAD in the parent handoff before
START. The unchanged runner records it in the new journal and each run's metadata. Make no
commits after measurement START. The post-run report must name that actual measurement
HEAD, verify source-tree equality, and distinguish it from the older implementation base.

## Checks, commands and remaining review

Run commands from this continuation workspace. The explicit `--plan` is mandatory because
the unchanged runner defaults to the original plan and its different workspace.

```sh
uv run --no-sync python scripts/timeout_recovery.py check \
  --plan docs/campaigns/2026-10-timeout-recovery-continuation-plan.json
uv run --no-sync pytest -q olap_benchmarks/tests/test_timeout_recovery.py \
  --basetemp data/temp/continuation-preparation/pytest
uv run --no-sync ruff check scripts/timeout_recovery.py olap_benchmarks/tests/test_timeout_recovery.py
uv run --no-sync ruff format --check scripts/timeout_recovery.py olap_benchmarks/tests/test_timeout_recovery.py
uv run --no-sync pyright scripts/timeout_recovery.py olap_benchmarks/tests/test_timeout_recovery.py
```

The safe `check` passed for 11 cells and three analytical runs. All 33 existing recovery
tests passed. Ruff lint/format and strict Pyright passed. Test fixture files stay under the
owned preparation temp directory; no campaign revision or campaign journal was initialized.
The check verifies input names and sizes; full input hashing remains mandatory before each
population at START. The original input hash cache is not copied.

The parent completed seed baseline validation for the eight unique runner scopes:
TPC-H SF10/SF50, RTABench SF1, Time Series SF1/SF10, ClickBench SF1, and TPC-DS SF1/SF10.
All eight scopes had zero row-count, answer-hash and iteration findings. The checks used
SQL extracted from the frozen validator AST and its pure `_Scope` generator, the frozen
pins and system, `include_failed=True` and `mark=False`. Database access used only the
read-only `olap results query` CLI on scratch copies. The positive control reproduced all
16 original SF10 mismatch groups with exact step identities and no row-count or iteration
findings. TPC-DS SF1 and ClickBench SF1 negative controls were clean. The generator passed
Ruff format/lint and strict Pyright. Original source files and both scratch DB hashes
remained unchanged.

The parent-owned baseline scratch directory is
`/Users/williamlauren/.t3/worktrees/olap-benchmarks/t3-416ee4f7/data/temp/timeout-recovery-continuation-baseline-verified/`,
outside the sealed `data/temp/timeout-recovery/` directory. It contains `manifest.json`,
`verify_baseline.py`, the generated SQL, JSON outputs/execution records, and
`databases/seed.db` plus `databases/control.db`. The final baseline manifest's absolute path is
`/Users/williamlauren/.t3/worktrees/olap-benchmarks/t3-416ee4f7/data/temp/timeout-recovery-continuation-baseline-verified/manifest.json`.
This 20,635-byte manifest has SHA256
`34dfbaee363f735db7eb45c1e5f40ef7bf88c6baf46d8ddbddb581542b013dfe` and seals all its
artifacts. The cross-campaign manifest seals this manifest too. Reverify this nested
evidence with the other seals before START and after the campaign. Do not use a direct
ad hoc database connection. Final independent and parent reviews approved the plan subject
to these checks and corrections; commit approval covers only the three documentation
files. Recording the resulting clean HEAD, fresh pre-START verification and explicit parent
START remain gates. A zero baseline count is scoped to the seed and is not a correctness
clearance for the quarantined first-campaign observations.

Only after all checks, the approved clean documentation commit, fresh seal/image/host
verification and explicit parent START, the start command is:

```sh
caffeinate -i uv run --no-sync python scripts/timeout_recovery.py run \
  --plan docs/campaigns/2026-10-timeout-recovery-continuation-plan.json
```

Preserve its console output in this worktree's ignored temp directory. Keep all other
benchmark schedulers and host-intensive work stopped throughout measurement. The normal
disk, RSS, clean-provenance, serial-execution, full-population, answer-validation and
cleanup gates remain active. No permission to execute this command is implied by its
presence here.
