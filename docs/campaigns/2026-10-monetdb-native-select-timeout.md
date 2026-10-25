# MonetDB native SELECT timeout: implementation and bounded evidence

The candidate starts from `906d9d1783a6b71714edd16f0f50fc99c0e119d5` and
adds `CALL sys.setquerytimeout(configured_seconds)` to the actual SQLAlchemy
ADBC connection used by MonetDB SELECT benchmarks. It covers connections cached
before SELECT and reconnects. Normal preflight configures the session before
measured queries; repeated fetches do not repeat the configuration SQL.
Fresh `None` and non-SELECT sessions retain their unlimited baseline; reusing a
previously capped session with either clears the cap to zero. Configuration
failure closes/disposes the connection and propagates. Runtime capture verifies
the effective SELECT cap and labels only `querytimeout` as seconds; run options
record the requested native cap.

The [Dec2025 procedure documentation](https://www.monetdb.org/documentation-Dec2025/admin-guide/monitoring/session-procedures/)
defines the argument in seconds, with zero disabling the limit. Pinned
Dec2025-SP4 source `e12e577f3a87ec496799caf116ab09db5fba1d92`
[stores microseconds](https://github.com/MonetDB/MonetDB/blob/e12e577f3a87ec496799caf116ab09db5fba1d92/monetdb5/modules/mal/clients.c#L478-L480)
and [divides by 1,000,000 in the session view](https://github.com/MonetDB/MonetDB/blob/e12e577f3a87ec496799caf116ab09db5fba1d92/sql/backends/monet5/sql.c#L3730-L3738),
so raw `sys.sessions.querytimeout` reports seconds despite the catalog prose's
milliseconds label. Numeric session IDs are reusable client-slot indices;
reconnect evidence records raw session ID/login and a new client connection,
with zero tagged sessions verified after closing the old connection.

The parent executed the fixed correctness probe on October 25, 2026 UTC
(October 26 in Helsinki), using MonetDB 11.55.9 and the existing pinned ADBC
packages. Both cases used the same 8,192-integer seed and synthetic aggregate.
The resource envelope was 10 CPUs, 24 GiB server memory, 28 GiB client guard,
40 GiB disk reserve, separate 30-second worker deadlines, and a parent
150-second global bound reserving 45 seconds for cleanup. No benchmark data or
ClickBench query was used.

- Direct `fetch_arrow_table` with a native 2-second cap and no Python alarm
  raised `adbc_driver_manager.OperationalError` after **2.137776 seconds**:
  `TIMEOUT: HYT00!Query aborted due to timeout. SQLSTATE: HYT00`.
  The raw attributes were SQLSTATE `HYT00` and `AdbcStatusCode.TIMEOUT` (12).
- Native cap 2 plus the existing main-thread `query_deadline(2)` raised
  `builtins.TimeoutError("Query exceeded 2 seconds")` after **2.391715 seconds**.
  Its traceback shows SIGALRM interrupting native-error construction; no native
  cause/context was preserved. The surfaced error must not be presented as a
  retained native exception.
- Both cases confirmed raw `300 -> 300`, survival of the 300-second cap across
  explicit rollback, and `None -> 0`. After cancellation, rollback, `SELECT 1`,
  same-session identity/current-cap readback, reconnect recovery, and zero
  tagged-session/staging-table baselines passed. The current recovery cap was
  2 seconds. No 1-second retry was needed or executed.
- Parent completion took **11.459877 seconds**, with cleanup successful and
  within the bound, no supervisor intervention, and no logging errors. The
  parent removed only the owned probe container and dbfarm.

These elapsed values describe this bounded correctness observation, not
benchmark performance. The probe did not test 300-second expiry, ClickBench
Q32 recovery, or completion of the remaining queries. Timeout checks are
[cooperative in the interpreter](https://github.com/MonetDB/MonetDB/blob/e12e577f3a87ec496799caf116ab09db5fba1d92/monetdb5/mal/mal_interpreter.c#L590-L607)
and [instrumented GDK loops](https://github.com/MonetDB/MonetDB/blob/e12e577f3a87ec496799caf116ab09db5fba1d92/gdk/gdk.h#L1573-L1718);
this result does not establish a hard deadline through every native operation.
It supports harness session wiring and recovery, without an engine-defect claim
or shared timeout-classifier change.

If Q32 raises a clean `TimeoutError`, the existing
`execute_query_with_isolation` calls `rollback()` and MonetDB's inherited no-op
`reset_session`, skips Q32's remaining iterations, and returns false.
`run_select_queries` continues Q33–Q42 in the **same SELECT attempt**, raising
`FailedQueriesError` only at the end. A campaign acknowledgement advances to the
next cell; it cannot resume the remainder of an aborted SELECT attempt. This
continuation is not established until the fresh full cohort runs: rollback or
connection-recovery failure, the 330-second outer watchdog, or resource guards
can still abort it. Cascading dead-session errors in Q33 onward are recovery
failure, not independent query outcomes. The cohort must retain runtime
`querytimeout=300`, `session_baseline_after=0`, and evidence that no outer
watchdog intervention occurred.

Evidence is retained locally under
`data/temp/monetdb-native-timeout-probe/abs_3tym/`, including exact candidate,
helper, controller, and plan snapshots. All 30 manifest entries were verified
against their recorded sizes and hashes before this record was written:

- `findings.json`: SHA256
  `97414510d7913203f490361b8293a9a39834207900e34f9648b5042d950199ec`.
- `evidence-manifest.json`: SHA256
  `e455e5ca887f99f04b5c52e61a454f324081b2dd3139f22562b042cbe77c8b46`.

The native deadline changes execution behavior, so new timings require a fresh,
reviewed **full 43-query MonetDB cohort** under the new source and plan, even
though benchmark SQL, input, driver/image pins, CPU/memory settings, hashes,
three-iteration timing boundaries, the outer 300+30-second query supervision,
and the 14,400-second operation budget remain unchanged. Historical run 691,
its unlabeled runtime metadata, and all earlier statuses remain intact. Any
combination with the four unstarted TPC-DS cells requires a separately reviewed
plan; the prepared `b836` plan remains unrun and is not measurement evidence.

Full non-live validation of the unchanged three-file candidate passed:
**2,283 passed, 1 skipped, 14 live tests deselected**, in 85.86 seconds (exit 0).
Ruff reported all 254 Python files formatted and no lint errors; full strict
Pyright reported zero errors and warnings. Pytest settings and basetemp were
isolated under a task-owned `/tmp` directory. Existing nanobind shutdown
diagnostics recurred (30 instances, 2 keep-alive records, 6 types, 85 functions)
without changing the successful exit status. No source corrections were needed.
