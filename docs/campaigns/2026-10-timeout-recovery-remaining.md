# October 2026 timeout recovery: ten remaining cells — prepared, not started

This third, separate campaign covers exactly the ten untouched cells in the original
order. The [plan](2026-10-timeout-recovery-remaining-plan.json) declares its executable
scope. The [combined manifest](2026-10-timeout-recovery-remaining-manifest.json) preserves
the full 17-cell request, three actual analytical outcomes, and the evidence chain.
Opus and the parent approved the analytical scope described below. Final evidence review,
explicit parent commit approval and explicit parent START remain separate gates.
Preparation does not authorize a commit, population, measurement, push, PR or publication.

## Scope and unchanged contract

The owned worktree is
`/Users/williamlauren/.t3/worktrees/olap-benchmarks/t3-f4c709e8`, branch
`t3/plan-timeout-recovery-cells`, revision `2026-10-timeout-recovery-remaining`.
Only this bound branch was fast-forwarded with `git merge --ff-only` from implementation
base `efaae1c9f31a9c917b074e3c60da74c0572d34e6` to documentation base
`ed78de3b03153036735fec2a414c6556e3a63dcf`. Those commits differ only in the three prior
continuation documents. All executable code and tracked configuration remain byte-identical
to the implementation base; all 1,990 files tracked at the documentation base remain intact.

The new `cells` array must satisfy both deep equality and raw JSON object byte equality:
`new.cells == original.cells[7:] == continuation.cells[1:]`. Its 553 query slots are:

1. `postgres:tpc_h:sf50` — 22
2. `questdb:rtabench:sf1` — 41
3. `starrocks:time_series:sf1` — 62
4. `starrocks:time_series:sf10` — 62
5. `timescaledb:clickbench:sf1` — 43
6. `timescaledb:rtabench:sf1` — 41
7. `timescaledb:time_series:sf1` — 62
8. `timescaledb:tpc_ds:sf1` — 99
9. `timescaledb:tpc_ds:sf10` — 99
10. `timescaledb:tpc_h:sf50` — 22

Settings, pins, full input manifests, input directory, SQL and physical schema hashes,
source run identities, cell reasons and all registry/explicit exclusions remain unchanged.
Every new cell has empty `query_exclude`; the original MonetDB Q15 exclusion remains only
in its already attempted original cell. Budgets remain 300 seconds per query, 14,400 seconds
per operation, 10 engine CPUs, 24 GiB engine memory, 28 GiB sampled client RSS and a 40 GiB
free-disk reserve. No filters or partial populations are introduced.

The only permitted top-level plan changes from the original are `cells`, `analytical`,
`workspace`, `branch`, `revision`, `source_database` and `provenance`. Within `analytical`,
only `clients` changes from `[1, 4, 16]` to `[]`. PostgreSQL TPC-H SF10 is absent from the
new cells, and all three configurations were already attempted. Every other analytical
field is identical, including SQL manifest, seed0, 60-second duration, synchronized warmup
and recorded budgets. The full original analytical contract is retained verbatim as a JSON
object in both provenance and the combined manifest.

Zero new analytical runs are scheduled. The prior actual outcomes remain:

- `2026-10-timeout-recovery-continuation/run687`: clients1 completed and validated.
- `2026-10-timeout-recovery-continuation/run688`: clients4 completed and validated.
- `2026-10-timeout-recovery-continuation/run689`: clients16 failed during warmup with
  `AnalyticalWorkloadError` and 300-second PostgreSQL statement timeouts. Its measurement
  did not complete. There is no acknowledgment or automatic retry.

The clients16 warmup offered 103 requests, completed 101 and recorded two Q9 statement
timeouts. Measurement never started, measured requests are zero and QPS is null. Preserve
the raw `OperationalError` / `QueryCanceled` evidence for
`2026-10-timeout-recovery-continuation/step86368` (client15, 300.020188458 seconds) and
`2026-10-timeout-recovery-continuation/step86387` (client14, 300.008550250 seconds).

The second campaign's PostgreSQL TPC-H SF10 attempt remains blocked at `analytical-16`,
despite complete population in `2026-10-timeout-recovery-continuation/run685`, validated
full 22-query/three-iteration SELECT in `2026-10-timeout-recovery-continuation/run686`,
and confirmed cleanup. Its frozen summary is not rewritten to claim a completed cell.
The validated Q18 median in `2026-10-timeout-recovery-continuation/run686` is 62.789258
seconds, from measured iterations 60.755288750, 62.789258000 and 63.395317542 seconds.

## Ownership, seed and prior campaigns

The original worktree `/Users/williamlauren/.t3/worktrees/olap-benchmarks/t3-416ee4f7`
remains clean at `efaae1c9f31a9c917b074e3c60da74c0572d34e6`; the first continuation
`/Users/williamlauren/.t3/worktrees/olap-benchmarks/t3-01c9b611` remains clean at actual
measurement HEAD `ed78de3b03153036735fec2a414c6556e3a63dcf`. No source, database,
journal, status, report, cleanup or branch mutation is permitted in either workspace.
The parent finalized the second evidence set and confirmed its owned runner, caffeinate,
benchmark processes and any event watchers stopped before sealing. Its process audit
reports no benchmark containers or open evidence handles and confirms owned store/scratch
cleanup. Existing baseline scratch evidence outside the original sealed campaign directory
remains intact.

The only seed is the original selected `site/public/data/results.db`, copied to this
worktree's ignored `data/frozen/2026-10-selected-source.db`: 143,929,344 bytes, SHA256
`832e28599501d125f2f4e0e35bb5251803955fbe1f6066f02c949c9df59f833f`.
This explicitly authorized source-path relocation uses an owned regular mode0444 copy,
with no symlink or hard link. The unchanged runner resolves the relative path beneath
the plan workspace and uses its parent directory and filename stem for source CLI queries.
Neither recovery raw database may be used as a seed. Excluding their appended observations
from the new candidate database does not resolve any prior finding.

The shared full input directory is read-only for this campaign:
`/Users/williamlauren/.t3/worktrees/olap-benchmarks/t3-3dcf1cb2/data/input`.
Writable campaign paths are exclusively this worktree's `results/`, `data/dbs/` and
`data/temp/`. Local preparation evidence lives in `data/temp/remaining-preparation/`.
No input hash cache is copied. The new revision database
`results/2026-10-timeout-recovery-remaining.db` and the entire campaign journal directory
`data/temp/timeout-recovery/` must not exist during preparation. The runner creates these
only after all START gates pass.

All run and step identities are `(revision, id)`, rendered as `revision/runN` or
`revision/stepN`. The seed has maximum run ID684; the first new ID will again be
`2026-10-timeout-recovery-remaining/run685`. This is distinct from both
`2026-10-timeout-recovery/run685` and `2026-10-timeout-recovery-continuation/run685`.
Never combine revisions by bare numeric IDs or splice successful queries across attempts.

## Correctness, stopping and combined reporting

The original campaign has six attempts: four acknowledged failed SELECTs, one acknowledged
failed population with SELECT unattempted, and an unresolved blocked SELECT. It has zero
completed measurement cells. Its five acknowledgment reasons, including the four stored
in `supervisor-decisions.jsonl` rather than the journal, remain in the evidence chain.

`2026-10-timeout-recovery/run695` remains blocked with `validation_ok=false`. It contains
16 answer-hash mismatch observations across Q31/Q49/Q65/Q85 and no row-count or iteration
findings. Its five failed queries are Q06/Q11/Q30/Q81/Q95. Preserve Q30's raw
`OperationalError` while explaining its 300-second PostgreSQL statement timeout; the other
four failures record `TimeoutError`. Neither historical SF10 source coverage nor SF1 peer
agreement resolves the four stable SF10 hash disagreements.

Keep the declared order. Any validation failure must stop, including a TimescaleDB TPC-DS
SF10 mismatch. Passing against the selected seed does not resolve the excluded original
`2026-10-timeout-recovery/run695` observations. Never accept `validation_ok=false`, edit
flags/statuses/marks/reports, suppress findings, substitute a success-only cohort or change
SQL/input/budgets to reach another cell.

Plain `--resume` must not retry a blocked cell without a separately reviewed decision.
The original eligible failure acknowledgment flags remain available only after explicit
review under their unchanged validation, frozen-provenance, terminal-evidence and cleanup
gates. They do not apply to the prior analytical failure or unresolved correctness blocker.
An unacknowledgeable stop anywhere requires another separately reviewed plan for untouched
cells. If TimescaleDB TPC-DS SF10 blocks, the final TimescaleDB TPC-H SF50 cell stays
unattempted until such a plan is approved. Never reorder to avoid that stop.

The new runner summary covers ten cells and zero analytical runs. A future
`measurements_complete` in that summary can mean only those ten cells completed. Empty
analytical lists neither erase the original 1/4/16 contract nor mean clients16 succeeded.
The combined report must show all 17 original cells and all three actual analytical
outcomes across the three distinct revisions, retaining every prior failure and blocker.
Publication remains blocked pending separate correctness and cohort review; the new asset
alone must never be published as the recovered campaign.

## Evidence, environment and START gates

The manifest must chain all 75 original seals, the baseline manifest and all 102 nested
baseline artifacts, and every finalized second-campaign artifact: raw database, attempt
files, state, summary, console, terminal report, selected seed, runner and the three actual
`ed78de3` documentation files. The second terminal report is
`data/temp/continuation-preparation/postrun/combined-report.json` under the second worktree,
25,509 bytes, SHA256 `00e793799f708f7c4691cbede00f9bffcbb4f88476434517b85948a665a6aef3`.
Its adjacent `campaign-seals.json` is 8,293 bytes, SHA256
`d101b988affadda290759dd92f81eabfda8338e690823d71d57c462c1174acd8`, and seals 34 artifacts.
The new manifest seals both wrapper files and those 34 entries, with explicit roots.
The prior input hash cache is sealed as evidence only and is never copied or reused.
The manifest records the second actual measurement HEAD separately
from the original implementation base. Reverify every seal immediately before START and
after completion, blockage or interruption. On mismatch stop and investigate; do not reseal
unexplained drift. Confirm both prior raw databases have no WAL or running runs using the
read-only CLI, and confirm prior owned benchmark processes and containers have stopped.

The existing baseline manifest is
`/Users/williamlauren/.t3/worktrees/olap-benchmarks/t3-416ee4f7/data/temp/timeout-recovery-continuation-baseline-verified/manifest.json`,
20,635 bytes, SHA256 `34dfbaee363f735db7eb45c1e5f40ef7bf88c6baf46d8ddbddb581542b013dfe`.
Its eight clean scopes cover the seven required new scopes: TPC-H SF50, RTABench SF1,
Time Series SF1/SF10, ClickBench SF1 and TPC-DS SF1/SF10. Reuse requires unchanged seed,
validator, source tree, pins and all 102 artifacts. Its positive control reproduces the
16 original SF10 mismatch groups and exact step identities; the SF1 and ClickBench
negative controls are clean. It used frozen validator SQL, `include_failed=True`,
`mark=False` and the read-only CLI. No ad hoc direct DB opens are allowed. All further
ad hoc inspection uses `./.venv/bin/olap results query --revision <revision> "<sql>"`.

`uv sync --frozen --python 3.14` installed Python 3.14.3 and 129 packages. Dependency
entries match both prior worktrees after normalizing only the editable project path.
The owned ignored mode0600 `.env` sets identical budgets, empty query/table filters and
owned output paths. Read-only `docker image inspect` verifies the four frozen image IDs
and RepoDigests; never rely on mutable tags. No container start or image pull is part of
preparation. No source or site change is proposed, so no Bun/browser work is needed.

The safe checks are:

```sh
uv run --no-sync python scripts/timeout_recovery.py check \
  --plan docs/campaigns/2026-10-timeout-recovery-remaining-plan.json
uv run --no-sync pytest -q olap_benchmarks/tests/test_timeout_recovery.py \
  --basetemp data/temp/remaining-preparation/pytest
uv run --no-sync ruff check scripts/timeout_recovery.py olap_benchmarks/tests/test_timeout_recovery.py
uv run --no-sync ruff format --check scripts/timeout_recovery.py olap_benchmarks/tests/test_timeout_recovery.py
uv run --no-sync pyright scripts/timeout_recovery.py olap_benchmarks/tests/test_timeout_recovery.py
```

The final safe check must report `10 cells + 0 analytical runs; no measurements`.
Input names and sizes are checked here; full input content hashing remains mandatory
before every population at START. Only isolated test fixtures may write temporary DBs;
the campaign DB and journal directory remain absent. No live test is authorized.

Only these three new documentation files may be added relative to `ed78de3`: the plan,
this protocol and the manifest. Compare all existing tracked bytes, modes, staged changes
and untracked files before commit and START. Final evidence/contract review must precede
explicit parent approval of that documentation-only commit. Record its actual clean HEAD
in the parent handoff and later terminal report; the unchanged runner records it in the
journal and run metadata. The committed manifest cannot self-hash. No commits after START.

After those gates, fresh seal/image/environment/host verification, confirmed stopped prior
owned processes, sufficient disk/RSS headroom and explicit parent START, the command is:

```sh
caffeinate -i uv run --no-sync python scripts/timeout_recovery.py run \
  --plan docs/campaigns/2026-10-timeout-recovery-remaining-plan.json
```

The explicit `--plan` is mandatory; the runner defaults to the original workspace's plan.
Preserve console output in owned temp storage. Keep benchmark schedulers and host-intensive
work stopped; perform no CPU-heavy work before START or during measurement. Preserve the
serial, full-population, answer-validation, clean-provenance, resource and cleanup gates.
This documented command is not execution authorization.
