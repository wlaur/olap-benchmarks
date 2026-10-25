# October 2026 timeout recovery — prepared, not started

The frozen declaration is [2026-10-timeout-recovery-plan.json](2026-10-timeout-recovery-plan.json).
The serial runner is [scripts/timeout_recovery.py](../../scripts/timeout_recovery.py).
This protocol prepares 17 resident-hot cells and three analytical runs. No live benchmark has
started as part of preparation. The coordinating parent must first commit the plan, runner,
tests and UI changes, finish host validation, and explicitly signal the quiet measurement phase.
Historical campaign documents and results remain unchanged.

## Why these cells

The 45-second deadline came from the October 18 campaign environment accompanying resource
guards in commits `b8b9666` / `f93cf80`. It was not a Python setting default:
`Settings.query_timeout_seconds` still defaults to `None`. This campaign deliberately changes
the query guard to 300 seconds and the operation guard from 900 to 14,400 seconds. These are
finite guard budgets, not promises that every query or population will finish.

MonetDB illustrates why the old failures do not establish an intrinsic query limitation.
October 11 revision `2026-10`, run58, completed all 43 ClickBench queries. Q4/Q9/Q32 had measured
medians of 3.379/5.808/30.087 seconds. Their SQL hashes match October 18 runs335/336/337, which
hit 45 seconds during warmup. Selected follow-up run525 explicitly excluded all three after
fresh population524, so it did not test their recovery. Those medians describe the earlier
run only; they are not substituted for new measurements.

| Engine / suite / scale | Selected run | Recovery reason |
| --- | ---: | --- |
| Doris / ClickBench / 1 | 511 | Q32 deadline |
| MonetDB / ClickBench / 1 | 525 | Q4/Q9/Q32 exclusions after warmup deadlines |
| MonetDB / chat_threads / 10 | 522 | Q16/Q17 deadlines; retain Q15 server-memory-crash exclusion |
| PostgreSQL / ClickBench / 1 | 397 | Four-query cohort after incomplete index preparation and exploration deadlines |
| PostgreSQL / TPC-DS / 1 | 365 | Q11 deadline |
| PostgreSQL / TPC-DS / 10 | 658 | Q11 failure |
| PostgreSQL / TPC-H / 10 | 372 | Q18 and conservative cohort; analytical prerequisite |
| PostgreSQL / TPC-H / 50 | 458 | Incomplete lineitem and Q13/dependency-only subset |
| QuestDB / RTABench / 1 | 316 | Q0025 deadline and 28-query cohort |
| StarRocks / time_series / 1 | 282 | large_23_batch_export deadline |
| StarRocks / time_series / 10 | 494 | large_23_batch_export deadline |
| TimescaleDB / ClickBench / 1 | 404 | Multiple query deadlines and incomplete compression |
| TimescaleDB / RTABench / 1 | 387 | Bounded raw/prepared-query cohort |
| TimescaleDB / time_series / 1 | 381 | large_13_rolling_avg deadline |
| TimescaleDB / TPC-DS / 1 | 368 | Q11 deadline |
| TimescaleDB / TPC-DS / 10 | 660 | Q11 failure |
| TimescaleDB / TPC-H / 50 | 465 | Q13 and incomplete lineitem/dependency subset |

MonetDB TPC-H SF50 run449 is deliberately absent: its incomplete scope follows the disk-reserve
failure documented by run448, not the 45-second query deadline.

Every cell uses fresh population and its full canonical SELECT plan. No old query includes,
table restrictions or timeout exclusions are inherited. The full plans contain 925 query
slots across the 17 cells. Registry exclusions retain their existing reason and `unsupported`
status. MonetDB chat keeps the 23-query plan with only `15_part_type_distribution` explicitly
excluded for the unrelated memory crash; queries16/17 are retried. Skipped, unsupported,
failed, unstarted and unknown results remain distinct evidence.

## Frozen inputs and analytical contract

The source is this worktree's ignored `site/public/data/results.db`: 143,929,344 bytes,
SHA256 `832e28599501d125f2f4e0e35bb5251803955fbe1f6066f02c949c9df59f833f`.
Only at START will the runner clone it to the new ignored revision
`results/2026-10-timeout-recovery.db`. Existing reference cohorts, selected failures and
historical metadata stay in that copy; new attempts append. Original selected and raw
revisions in other worktrees are never changed. Historical raw failures remain at their
original locations; cloning the selected asset does not claim to import every raw run.

Ad hoc source inspection used the read-only `olap results query` CLI. Its `COPY (SELECT ...)`
exports went to `/tmp/olap-timeout-recovery-source-all.json` and related preparation files;
the source database was not opened for direct ad hoc inspection. The tracked plan retains
the relevant source identities, full input manifests, canonical SQL hashes, schema hashes,
registry exclusions and engine pins. Existing measured-query hashes matched current SQL
where recorded; all 17 source schema fingerprints matched. The preparation check compares
every input filename and byte size. At START, full input fingerprints must match before each
population; hashing uses the harness cache in this worktree's temp directory.

Analytical runs670/671/672 explicitly record clients1/4/16, 60.0 seconds, seed0, no query
exclusions and all 22 canonical TPC-H queries. Their shared manifest digest is
`1aceb1b1f21ad379edda0976a55c5ea8c126eb15ca04f92e85d346025ed57022`.
Every client warms each query once before synchronized measurement. Shared budgets remain
10 CPUs / 24 GiB engine and a separate sampled 28 GiB operation-process RSS guard, not budgets
multiplied by clients. Runs670/672 contain effective cgroup evidence; run671 has matching
configured settings and start command but no completed budget object. No missing effective
measurement is imputed. Run-level errors in670/672 are `KeyboardInterrupt`; their query-step
evidence records warmup timeouts, so the protocol does not relabel every run error as
`TimeoutError`.

After a complete fresh PostgreSQL TPC-H SF10 population and SELECT, the runner validates
SELECT answers and all three measured iterations for each supported query. Only then does
it run analytical clients1,4,16 serially against that populated store, with the audited
manifest/duration/seed. The relaxed 300/14,400-second guards are the intentional changes.
Analytical evidence and answers are checked by the existing analytical validator against
the latest current-pin SELECT reference cohorts. Analytical evidence never enters
resident-hot medians or an invented aggregate score.

## Execution and recovery boundaries

The runner defaults to `check`, which verifies paths, branch, asset checksum, pins, SQL,
schemas, input sizes, disk reserve and absence of benchmark containers. It does not clone
the asset, start containers or measure queries. `run` additionally requires a clean committed
checkout. Each operation rechecks the commit and clean tree; recorded provenance and budgets
must agree. Resumption requires the same plan checksum, commit and revision. An exclusive
file lock prevents simultaneous recovery runners.

All engine stores and scratch paths are restricted to this worktree's `data/dbs` and
`data/temp`; symlink redirects are refused. Inputs are read from
`/Users/williamlauren/.t3/worktrees/olap-benchmarks/t3-3dcf1cb2/data/input`. No input rows,
scales, SQL or physical schemas are reduced. The runner refuses existing benchmark
containers rather than stopping an unknown owner's container. Parent coordination must keep
other benchmark schedulers and host-intensive UI validation stopped throughout measurement.

Each cell has a unique attempt directory and run note. Fresh cleanup precedes population;
SELECT requires a recorded completed population with no failed or unfinished steps. The
harness verifies complete input table counts and performs its normal finalization. All
commands execute serially. SIGINT/SIGTERM request a controlled stop; the runner waits for
the harness process and its descendants before attempting cleanup. Cleanup checks container
absence, removes only the owned cell store and engine temp directory, verifies removal,
and preserves logs, run plans, failures and result revisions.

Disk/RSS guards, failed population, incomplete query measurement, validation findings,
unconfirmed shutdown or cleanup, and command errors stop the scheduler. There is no automatic
query filtering, deadline escalation or dependent SELECT on incomplete input. After an
interruption, `--resume` preserves the attempt and marks remaining running rows failed only
in the new revision, then freshly populates the unfinished cell. The scheduler distinguishes
settled recovery attempts from completed measurements. A cell is settled only after success
or explicit reviewed acknowledgment of a terminal failure, with confirmed cleanup and
matching result run IDs/notes. An unacknowledged failure always blocks progress.
Analytical interruption also repeats the whole PostgreSQL cell, preserving earlier attempts.

A terminal SELECT failure may remain a legitimate recovery outcome even at 300 seconds.
After reviewing its saved logs and validation, the coordinating parent can use
`--resume --accept-failed-select 'db:suite:sfN'` to retain that attempt as failed/partial and
continue to the next cell. This requires recorded population and SELECT, clean shutdown,
successful reference validation and confirmed cleanup. It never changes failure records
or treats the cell as a successful measurement. Population failure cannot be acknowledged
this way. PostgreSQL TPC-H SF10 cannot use this escape because the analytical prerequisite
must succeed. A disk/resource failure needs investigation before any continuation.

A terminal **population** failure has a separate reviewed continuation path:
`--resume --accept-failed-populate 'db:suite:sfN' --acknowledgment-reason 'review reason'`.
The supervisor must first inspect the saved population log/audit and restored free disk.
The runner rechecks the 40 GiB reserve, absence of benchmark containers, removal of the owned
store/scratch paths and the journal's cleanup confirmation. Both the saved audit and a fresh
read-only database audit must identify the same terminal failed population, with matching
clean committed provenance, inputs and settings. Missing, running, successful, contradictory
or unrecorded population evidence cannot be acknowledged. No dependent run may exist.

The journal records `accepted_failed_populate`, the failed run ID, review reason and time.
The database retains its failed status and raw evidence. This settles only the scheduling
decision: SELECT remains unattempted, and unrelated later cells can proceed. If PostgreSQL
TPC-H SF10 population is acknowledged this way, its SELECT and all analytical clients remain
unperformed; it is never counted as completed measurements. No automatic skip is introduced.

`summary.json`, also printed after a scheduler pass or interruption, separately lists
completed measurement cells, acknowledged population/SELECT failures, cells without a SELECT
attempt and outstanding analytical clients. A traversal with reviewed failures ends with
`attempted_with_failures`, never `measurements_complete`. Remaining unacknowledged work is
reported as `blocked_or_not_attempted`.

Answer validation is read-only (`mark=False`, including terminal failed attempts). Reports
remain in each attempt directory; no historical marks are cleared. Findings block progress
for review rather than silently choosing a success-only cohort. No publish, prune, upload,
asset replacement, site metadata update or score-fixture update is performed.

## Commands and artifacts

Run from `/Users/williamlauren/.t3/worktrees/olap-benchmarks/t3-416ee4f7`.

Preparation check, safe before the quiet measurement phase:

```sh
uv run --no-sync python scripts/timeout_recovery.py check
```

Exact start command, only after the parent's explicit START and committed provenance:

```sh
caffeinate -i uv run --no-sync python scripts/timeout_recovery.py run
```

Resume after investigating an interruption:

```sh
caffeinate -i uv run --no-sync python scripts/timeout_recovery.py run --resume
```

Example continuation after reviewing a recorded PostgreSQL TPC-H SF50 population failure
and confirming free disk has recovered (only after START; do not run during preparation):

```sh
caffeinate -i uv run --no-sync python scripts/timeout_recovery.py run --resume \
  --accept-failed-populate 'postgres:tpc_h:sf50' \
  --acknowledgment-reason 'Reviewed saved terminal population failure; shutdown and cleanup confirmed; disk reserve restored.'
```

Use the actual failed cell key and a reason describing the evidence reviewed. Each
acknowledgment is explicit; the flag can be repeated for independently reviewed failures.

`caffeinate` holds an idle-sleep assertion only for the command's lifetime; global power
settings are unchanged. The operation logs themselves are written by the runner. The
coordinator should retain its top-level console output as well.

| Artifact | Path relative to this worktree |
| --- | --- |
| New appended result revision | `results/2026-10-timeout-recovery.db` |
| Atomic resume journal | `data/temp/timeout-recovery/state.json` |
| Attempt/measurement status report | `data/temp/timeout-recovery/summary.json` |
| Exclusive scheduler lock | `data/temp/timeout-recovery/runner.lock` |
| Operation logs and raw run/step audits | `data/temp/timeout-recovery/<attempt-id>/` |
| SELECT answer report | `<attempt-id>/validation.json` and `.txt` |
| Analytical evidence/answer report | `<attempt-id>/analytical-<clients>-validation.json` |
| Per-cell fresh persistent store | `data/dbs/<engine>/<suite-scale-directory>/` |
| Input hash cache | `data/temp/input-sha256-cache.json` |

If the process dies during the initial seed copy before a matching journal exists, the runner
refuses to adopt an orphan `.seed` or `.db`. Inspect that local artifact before removing or
archiving it and starting again; never point the runner at an original revision. If a forced
kill leaves a benchmark container, inspect its mounts/ownership and stop the owned container
before resuming. The runner will not guess ownership from the name alone.

## Preparation validation and limits

Scoped Ruff formatting/lint and strict Pyright pass. All 33 focused pytest cases pass and cover the new runner, serial
order, population and analytical gates, fresh resumption, immutable seeding, missing evidence,
inherited filters, path isolation, failed cleanup, subprocess interruption, explicit population
acknowledgment, independent continuation and truthful incomplete analytical status. No live test
or full/global UI test is part of this preparation. The safe `check` command passes; a clean
commit remains an intentional prerequisite for `run`. The read-only audit projection also
parsed the frozen source's run525 and its 177 steps successfully. No recovery result revision
exists yet, and the preparation host check found no benchmark containers.

The host had approximately 162 GiB free when preparation began. The 40 GiB reserve is enforced
again before each operation and by the harness during it; SF50 stores and index preparation
can still exhaust usable headroom. Cleanup between cells avoids accumulating those stores.
Successful full population of the previously incomplete large row-store cells remains
unmeasured, so there is no defensible precise completion estimate. Expect hours to multiple
days. The 34 populate/SELECT operations plus three analytical operations allocate up to
148 hours of operation-watchdog time for one uninterrupted pass, excluding input hashing,
container startup/shutdown, validation, cleanup and retries. That is a budget ceiling for
operation execution, not a forecast or a whole-campaign timeout.

Publication acceptance is separate: review all requested attempts and qualifications,
successful complete populations for usable SELECT replacements, answer validation,
clean provenance, preserved raw failure evidence, and the analytical results or explicit
remaining blockers. A 300-second timeout remains a measured failure; it is not missing data,
unsupported capability or a success with a substituted latency.
