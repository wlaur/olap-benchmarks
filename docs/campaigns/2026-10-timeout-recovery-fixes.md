# Timeout recovery and Q18 implementation review

This is an implementation record, not an execution plan or authorization to restart.
The isolated branch `t3/timeout-recovery-fixes` was fast-forwarded from `efaae1c` to
`f5b24e2`; that base adds only six documentation files. The prior workspaces and their
results remain untouched. No benchmark server was started or probed for this work.

Changed implementation files are `olap_benchmarks/dbs/questdb/__init__.py`,
`olap_benchmarks/dbs/__init__.py`, `olap_benchmarks/operation_runner.py`,
`olap_benchmarks/run_metadata.py`, `olap_benchmarks/analytical_streams.py`, and
`olap_benchmarks/dbs/{postgres,timescaledb}/__init__.py`.
The six test files are `test_benchmark_limits.py`, `test_analytical_streams.py`,
`test_questdb_budgets.py`, `test_postgres_budgets.py`, `test_run_metadata.py` and
`test_postgres_clickbench_queries.py`, all under `olap_benchmarks/tests/`.
This review record is the only documentation change.

## Native QuestDB budget

`QuestDB.start` now supplies `QDB_QUERY_TIMEOUT=<seconds>s` when
`query_timeout_seconds` is configured. For the 300-second budget, the generated
command includes `-e QDB_QUERY_TIMEOUT=300s`. CPU, memory, image, mounts, COPY root
and the metrics suite's small-map page size are unchanged.

`QDB_QUERY_TIMEOUT` is server-global, so it also applies during populate and mutate,
not just SELECT. A 300-second setting loosens the previous native 60-second limit for
those operations; configured budgets below 60 seconds would tighten that limit.

The [official configuration overview](https://questdb.com/docs/configuration/overview/#environment-variables)
specifies the environment-variable mapping and startup requirement. The
[query timeout reference](https://questdb.com/docs/configuration/cairo-engine/#querytimeout)
specifies a 60-second default, duration units, plain numbers in milliseconds and
non-reloadability. These details were also checked against the pinned 10.0.1 source:

- [PropServerConfiguration](https://github.com/questdb/questdb/blob/10.0.1/core/src/main/java/io/questdb/PropServerConfiguration.java)
  initializes the legacy default to 60 seconds, then reads `QUERY_TIMEOUT` with `getMillis`.
- [Numbers.parseMillis](https://github.com/questdb/questdb/blob/10.0.1/core/src/main/java/io/questdb/std/Numbers.java)
  multiplies an `s` suffix by 1,000 and leaves an unsuffixed number in milliseconds.
- [DynamicPropServerConfiguration](https://github.com/questdb/questdb/blob/10.0.1/core/src/main/java/io/questdb/DynamicPropServerConfiguration.java)
  does not include either query-timeout property in the reloadable set.

With `None`, no environment override is added. The pre-existing native 60-second
default therefore remains a known gap; disabling the harness limit does not establish
unlimited QuestDB execution. The fix does not depend on PGwire options or ConnectorX
forwarding options through its `redshift` URI.

Existing `metadata.execution.start_command` records the configured environment, and
`metadata.settings` records the harness budget. Those are configuration evidence, not
runtime readback. No new schema or speculative effective-cap metadata was added.
Other engines' native caps have not been audited by this change and remain unknown.

## Fixed SELECT cancellation

The SELECT parent watchdog now expires an active query only after its query budget
plus a fixed 30-second cancellation grace. At a 300-second budget it starts process
cleanup after 330 seconds, subject to the existing one-second polling interval and
existing SIGINT/TERM/KILL escalation waits. It does not extend the operation budget,
disk guard or client-memory guard.

The in-process alarm and PostgreSQL native statement timeout still use the original
query budget. A fetch returning successfully after that budget is explicitly failed
before answer metadata, row count or successful duration can be recorded. Native
failures retain their original exception type and message. Normal query isolation
still skips the failed query's remaining iterations, runs later queries and ultimately
fails a suite containing failed queries.

The grace covers the active query marker, including failure recording before its
`finally` removal. Session rollback/reset after marker removal retains its existing
operation-level supervision. This is not a new per-reset deadline. Unresponsive
native work may still require whole-operation cleanup after the grace expires.

The analytical parent watchdog retains its original query-budget threshold. No
analytical deadline, issuance, draining or success-credit behavior was changed.
Failed analytical warmup still prevents measurement from starting.

## Classification

Fixed and analytical requests share recognition of the confirmed PostgreSQL
statement-timeout and QuestDB timeout-abort diagnostics. QuestDB's pinned
[CairoException](https://github.com/questdb/questdb/blob/10.0.1/core/src/main/java/io/questdb/cairo/CairoException.java)
distinguishes `timeout, query aborted` from `cancelled by user`.
Classification checks deadline text at diagnostic boundaries on the first line,
excluding SQL appended on later lines and quoted syntax-error fragments.
SQLSTATE `57014`, `QueryCanceled`, manual cancellations, connection timeouts
and lock timeouts alone are not evidence of the query budget expiring. Unrecognized
native diagnostics remain errors. Raw exception evidence is preserved, and no
historical row is rewritten.

## Validation

Deterministic tests use mocked clocks and processes, not 300-second sleeps:

- QuestDB startup commands with a 300-second budget and `None`, for RTA and metrics,
  retaining 10 CPUs, 24 GiB and the existing image and suite-specific environment.
- Native cancellation or SIGALRM delivered at simulated 300.4 seconds records failure,
  removes the marker, runs subsequent queries and leaves the suite failed.
- Parent polling at 300.1 and 300.4 seconds allows cooperative marker removal; a
  query still active at 330.1 seconds enters process cleanup.
- A successful fetch at 299.9 seconds is credited; at 300.4 seconds it is failed;
  disabling the configured budget preserves unbounded harness behavior.
- Native timeout positives and manual-cancellation negatives preserve raw errors.
- Analytical watchdog scope and failed-warmup behavior remain covered separately.

Validation uses Python 3.14.3, `uv`, Ruff, strict Pyright and nonlive pytest. The local
ignored `.env` points only to this worktree's empty `temp/unit-tests` directories;
campaign settings and data are not used by the tests.

Final validation results:

- `uv run --no-sync ruff check olap_benchmarks`: passed.
- `uv run --no-sync ruff format --check olap_benchmarks`: 191 files formatted.
- `uv run --no-sync pyright`: zero errors or warnings in strict mode.
- Focused deadline/native-budget/classification coverage: 183 passed.
- Focused ClickBench client-fetch/SQL/hash/registry coverage: 303 passed.
- `uv run --no-sync pytest -q`: 2,250 passed, one skipped, 14 live tests deselected,
  exit code zero. DuckDB/nanobind emitted shutdown diagnostics for 30 leaked instances,
  two keep-alive records, six types and 85 functions; these were not test failures.
- `git diff --check`: passed. No result databases, settings, image pins or hash-policy
  files are changed.

## ClickBench Q18 client normalization

The parent subsequently supplied an offline reconstruction explaining the Q18 hash
disagreement. The original workspace's
`data/temp/clickbench-q18-offline-reconstruction.json` was read without modification;
its 1,970-byte length and SHA-256
`dc97be6e7a47c09ac71a5cbd8d14ac0c444d77fff60efb38d622b3e99ffeb748` match the handoff.

The ten displayed rows have user ID `1313338681122956954`, empty search phrases,
minutes `31/28/29/33/27/32/30/34/26/10` and counts
`589/578/572/567/557/554/552/546/540/539`, in that order. The unchanged hashing code
reproduces `blake2b128:7aae3990077bd97f5d5562be7a989683` with only the minute column
represented as `Decimal(38,10)`, and
`blake2b128:fb1308d0569108347f102270ada59b03` with an integer minute column.
Decimal values with nonzero scale take the floating-point canonicalization path;
integer values take the exact decimal-string path. The values and row order are equal.

The parent reports independent Opus approval of the reconstruction against the sealed
preview, all four iteration metadata records and eight peers. The accepted change is
client-only: `PostgresClickbench` and `TimescaleClickbench` each declare their own
Q18-only `query_fetch_kwargs` override, `{"schema": {"m": pl.Int64}}`. No SQL is changed.

Only TimescaleDB run694 supplies prior live Q18 evidence. PostgreSQL run397 skipped
all four Q18 iterations, so PostgreSQL Q18 has no prior live evidence. Its identical
SQL and shared fetch path support the fix, but live PostgreSQL behavior remains
unvalidated.

Both Q18 query texts retain SHA-256
`c9e33b31c1ce1dc970a00642717176b6730dc9cff46d9a1665dd86c6faad72d4`; both complete SQL files
retain SHA-256 `1f149020fb3ff3875ae8b68dc75d892be24fe1bd2ccf8210cd4dc5fe256d3703`.

The PostgreSQL schema application helper, shared by ConnectorX and Python fetches,
checks explicitly requested Decimal/float-to-integer conversions for finite, integral
values before calling Polars' cast. Values such as Decimal `59.5` and `0.0000000001`
raise rather than being rounded or truncated. Nulls and integral values are preserved;
there is no clamping. Empty Python results retain every returned column before applying
the partial schema, instead of keeping only the columns named in the override.
Other query/suite fetch kwargs remain unchanged. No changes were
made to `canonical-v11`, global hashing, precision, order policy or the query registry.

This small guard and cast happen before `fetch` returns and are included in the
recorded fetch duration, just like existing schema overrides. Engine SQL and work are
unchanged; recorded timing is not claimed to be completely unaffected. The global
measurement boundary has not been moved to exclude client normalization.

Thirty regression cases exercise both classes and both mocked fetch methods.
They reproduce the full sealed ten-row Decimal baseline hash and the exact peer hash
after normalization, verify fractional rejection, null preservation and empty result
columns, preserve non-Q18 fetches and pin both original SQL hashes. Existing SQL, result-hash, registry,
ConnectorX, PostgreSQL TPC-DS and engine-contract tests also pass. These are offline
checks; they do not attest a fresh PostgreSQL or TimescaleDB execution.

A fresh full TimescaleDB ClickBench cohort remains a future target after source review
and authorization; no new campaign plan is created here. Run694 and its sealed journal
remain blocked with their original validation failure.

## Remaining evidence and objections

The following campaign observations are inherited from the parent handoff and existing
campaign records; this work did not reopen the campaign databases:

- `2026-10-timeout-recovery-remaining/run688`: QuestDB RTA SF1 Q0025 failed with
  runtime 60,332 ms and native timeout 60,000 ms despite configured 300 seconds;
  30 executable queries completed and 10 original registry queries were unsupported.
- The same revision's PostgreSQL TPC-H SF50 run686 had the full 300,005,811-row
  lineitem population, three measured iterations for Q1–Q8, a Q9 warmup timeout,
  interrupted Q10 and unstarted Q11–Q22. The deterministic tests reproduce a possible
  watchdog/cancellation interleaving; they do not prove that interleaving caused the
  historical interruption.
- The prior 16-client analytical warmup failure at the native 300-second deadline
  remains failed. It receives no benefit from the SELECT-only grace.
- `2026-10-timeout-recovery-remaining/run694`: all 43 TimescaleDB ClickBench queries
  completed, but stable Q18 hashes disagree with eight peers at row count 10. The
  offline type-only explanation and candidate above do not resolve its historical
  validation status; the campaign remains stopped.
- `2026-10-timeout-recovery/run695`: the TPC-DS SF10 disagreements on Q31/Q49/Q65/Q85
  remain unresolved. No TPC-DS SQL, answer precision, ordering or validation policy changed.
- TimescaleDB RTA SF1, time-series SF1, TPC-DS SF1/SF10 and TPC-H SF50 remain untouched.

Native configuration and live cancellation behavior still need independent parent
review and a separately authorized frozen execution plan. No commits, campaign plans,
live benchmarks, publication, pushes or pull requests are part of this work.
