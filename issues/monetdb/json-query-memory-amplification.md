# JSON query memory amplification in the expanded campaign

Static investigation on 2026-10-21 of the retained October 2026 campaign,
MonetDB Dec2025-SP4 / 11.55.9, native ARM64 image
`wlaur/monetdb-container:11.55.9-1`, ADBC 0.13.1 and
sqlalchemy-monetdb-adbc 0.4.1. No new server or benchmark was run for this note.
The host had ten configured benchmark threads, a 24 GiB container limit and no
container swap. This is evidence of a server memory failure, not yet a minimal
upstream reproduction or proof of its specific allocation cause.

## Observed failures and timings

Raw revision `2026-10-expanded`, select runs 429 and 430, both failed on the
measured iteration of chat SF10 `08_keyword_search_user` with
`IO: unexpected end of file`. The subsequent rollback produced the recorded
`INVALID_STATE: connection has been closed` operation error. Run 429's warmup
completed in 38,162.69 ms with only 43 rows and three output columns.

The retained `monetdb-chat_threads-sf10.docker-events.jsonl` and
`monetdb-chat_threads-sf10-partial1-1792381514128428000.docker-events.jsonl`
contain `oom` events at Unix seconds 1792381510 and 1792381579 respectively.
Peak server charges in runs 429/430 were 24,576/24,575 MiB; peak anonymous
memory was 24,556/24,440 MiB. Client RSS stayed at 248/243 MiB. These are
independent sampled maxima and must not be added. The server's daemon survived
long enough for the container's eventual shutdown to report exit code zero.
That exit code does not disprove the OOM events.

Concurrency run 433 also failed on `08_keyword_search_user`, with a retained
OOM event at 1792381646, server charge 24,576 MiB and client RSS 263 MiB.
Completed primary run 431 contains only queries 01–07; the other sixteen
queries have no completed primary timings. It does not show that all sixteen
are individually too slow or unsupported. The first failing query prevented
their exploration.

Published chat SF1 run 81 has these measured medians:

- `16_attachment_bytes_by_mime`: 9,088.69 ms.
- `15_part_type_distribution`: 5,126.08 ms.
- `17_tool_call_leaderboard`: 3,891.69 ms.
- `10_threads_with_visualization`: 3,445.23 ms.
- `09_keyword_search_global`: 3,368.17 ms.
- `08_keyword_search_user`: 3,334.30 ms.
- `18_response_latency`: 2,957.87 ms.
- `13_token_usage_by_model`: 1,815.47 ms.
- `14_top_users_by_tokens`: 1,204.52 ms.
- `21_first_response_by_day`: 692.58 ms.

The similar global/user keyword latencies warrant checking predicate placement
around JSON extraction, but timings alone do not establish a pushdown defect.
SF1's server anonymous-memory peak was 15,618 MiB with a 260 MiB client.

ClickBench uses zero-based query IDs. Published run 339's main outliers are
Q5 (distinct SearchPhrase), 23,180.83 ms; Q34 (URL grouping), 16,890.42 ms;
Q18 (UserID/minute/SearchPhrase grouping), 12,920.20 ms; Q28 (regex-derived
grouping), 9,102.70 ms; and Q16/Q13/Q17, approximately 4,813–4,883 ms.
Raw runs 335/336/337 timed out on Q4/Q9/Q32 respectively. Q33 has separately
validated observations and was omitted from the bounded primary cohort;
it must not be counted as a fourth demonstrated timeout. The primary run's
server file-memory peak was 18,163 MiB, anonymous-memory peak 4,801 MiB and
client RSS 251 MiB, on a roughly 48,597 MiB store. Memory pressure and costly
grouping remain plausible; missing indexes or a worker cap are unproven.

The wide-metrics outliers in run 130 are
`wide5000_14_hourly_all_metrics` (13,981.18 ms),
`wide5000_13_per_metric` (10,260.41 ms), and
`wide5000_12_all_metrics` (9,091.11 ms). The existing
[wide-aggregate investigation](wide-many-aggregate-planning.md) independently
demonstrates substantial SQL planning time through native mclient and EXPLAIN.
It is not evidence for a driver-only materialization problem. Preparing only
MonetDB queries outside the timed boundary would change the benchmark profile.

TPC-H SF50 populate run 448 was stopped at 39.6 GiB free by the 40 GiB reserve,
with a sampled store size of 121,800 MiB. This is a disk-capacity outcome.
Run 449 correctly limits execution to dependency-verified Q02/Q11/Q13/Q16/Q22;
queries requiring the incomplete lineitem table must remain unmeasured.

## Configuration and implementation audit

The campaign start command passed Docker `--cpus 10 --memory 24g
--memory-swap 24g`; it supplied no MonetDB optimizer or per-user/session cap.
It used native ARM64, not emulation. Actual server/session settings were not
retained, so defaults are not proof of the live values.

The [11.55.9 source archive](https://www.monetdb.org/downloads/sources/Dec2025-SP4/MonetDB-11.55.9.tar.bz2),
SHA-256 `95f5aeee9f3cb70ac462ca7c5ee74e7c01a35a7dde69c1501f5ddb80b9d8a468`,
contains cgroup-v2 CPU quota and memory detection in `gdk/gdk_utils.c`.
It derives the default GDK memory threshold using 0.815 of detected physical
memory. A missing explicit `nthreads` flag therefore does not demonstrate
oversubscription. Verify `sys.env()` values in the actual container.

The pinned [container entrypoint](https://github.com/wlaur/monetdb-container/blob/30ad454d6faa82e6e2ddca49cccf90029eeb4aee/scripts/entrypoint.sh)
applies `MDB_DB_PROPERTIES` only during first initialization. Adding that
environment variable alone would leave existing campaign stores unchanged.
MonetDB's [administration manual](https://www.monetdb.org/documentation-Dec2025/admin-guide/manpages/monetdb/)
documents `nthreads` and recommends the default optimizer; there is no evidence
here supporting a pipeline switch.

`monetdb5/modules/atoms/json.c:JSONfilterInternal` parses its JSON argument on
every scalar call. Current chat queries use nested filters because MonetDB's
array projection is limited. Queries 15–17 already extract parts into CTEs;
whether the physical plan reuses that work must be checked. A SQL CTE alone
does not prove physical materialization. Source-level parsing/allocation costs
and the server-only memory growth motivate investigation, without proving a
memory leak, a particular optimizer defect, or a safe JSON-path replacement.

Chat tables deliberately lack primary keys because of the documented ingest
regression; changing that choice requires an ingest and mutation comparison.
Time-series loaders already analyze relevant predicate columns. Chat and
ClickBench lack explicit post-load ANALYZE, which warrants a controlled plan
comparison rather than an unmeasured indexing/statistics change. No SQL,
schema, correctness exclusions, driver settings or memory thresholds changed.

The connector now records a `monetdb_runtime_settings` phase before each
operation, outside timed query iterations. It records GDK worker/memory values,
runtime version, current optimizer, session worker/memory/time limits and user
worker/memory caps. It queries `sys.current_sessionid()` on the benchmark
connection, retaining restrictive values instead of silently overriding them.
Missing settings and capture failures produce a failed phase. This closes the
configuration evidence gap; it is not a claimed performance fix.

## Bounded diagnostic recipe for the serial benchmark owner

Use a disposable store populated from the full immutable input, serially, with
the original 10-thread/24-GiB policy and 45-second query/900-second operation
limits. Keep the external client/disk guards. Do not rerun SF50 merely to test
the JSON issue. Nothing below was executed in this investigation.

1. On the benchmark connection collect the connector's
   `MONETDB_RUNTIME_ENVIRONMENT_SQL` and `MONETDB_RUNTIME_SESSION_SQL` constants.
   Separately retain `monetdb get all benchmark`, the server startup log and
   `/sys/fs/cgroup/{cpu.max,memory.max,memory.swap.max,memory.events}`.
   Check worker count against ten, memory against the 24-GiB cgroup, and the
   effective optimizer against `default_pipe`. A session cap of zero means no
   additional session limit, not zero usable workers/memory.
2. At SF1 first, capture `EXPLAIN PHYSICAL <query>` and `TRACE <query>` for chat
   08/13/15/16/17, plus EXPLAIN PHYSICAL 18/22, using a 45-second watchdog per
   statement. In the physical plan and execution trace check where user/time filters run relative to
   `json.filter`, and whether repeated CTE references repeat JSON work.
   [Explain/trace](https://www.monetdb.org/documentation-Dec2025/admin-guide/debugging-features/)
   results are diagnostics, not publishable benchmark samples.
   Run the same SQL through native mclient with `--timer=performance
   --format=trash` and through the normal ADBC fetch, one warmup plus three
   measured fetches, retaining correctness hashes. Different client output
   handling makes native timing a localization control, not a replacement score.
3. Capture at most one 45-second plan/execution diagnostic each for ClickBench
   Q4/Q5/Q9/Q18/Q28/Q32/Q34 and wide-metrics
   `wide5000_13_per_metric`. Compare SQL planning, optimization and execution
   counters without summing overlapping timers. For statistics diagnostics,
   start with `SELECT * FROM sys.statistics('sys', 'chat_message')` or
   `sys.statistics('sys', 'hits')`; collect plans before considering ANALYZE.
   Keep any ANALYZE comparison separate and account for its populate cost.
4. For SF10, isolate query 08 rather than immediately repeating concurrency.
   Sample cgroup memory and retain `memory.events` before/after and native
   logs before cleanup. Stop if the 24-GiB limit/OOM recurs. To explore the
   unobserved tail, run 09–23 in separate diagnostic operations with the same
   45-second per-query bound, stopping after the first resource failure.
   Do not infer all sixteen queries fail from query 08's failure.

The source exposes `CALL sys.setquerytimeout(45)` in seconds, but engine
cancellation is not a substitute for the external watchdog and resource guard.
Any SQL improvement must pass the full answer audit, including attachments
nested inside tool results and visualization objects with unrelated `data`
keys. Preserve the rolling-AVG and regex comparison exclusions.

## Rerun and retention proposal

With only settings capture implemented, old timings are not proven invalid.
The first new runs should be the bounded diagnostics above. No immediate
publication replacement or deletion is justified by this static work.

If a chat SQL/physical-design fix is demonstrated, rerun MonetDB chat SF1 and
SF10 select with all 23 planned queries; include populate if schema/statistics
change, and mutate/concurrent at both scales if physical design or connection
behavior changes. Verify SF10's previously unobserved tail independently.
Expected effects are lower server memory and broader coverage if the JSON
evaluation diagnosis is confirmed; no numeric speedup is established.

If a ClickBench fix is demonstrated, rerun MonetDB ClickBench SF1 populate and
select, all 43 queries under the existing exclusions and budgets. If a common
worker/memory/optimizer setting changes, rerun every registered MonetDB cell:
kaggle_airbnb SF1, chat SF1/SF10, time_series SF1/SF10,
time_series_metrics SF1, time_series_ranges SF1, tpc_ds SF1, tpc_h SF10/SF50,
rtabench SF1 and ClickBench SF1. Chat and core time_series require populate,
select, mutate and concurrent; the other cells require populate and select.
SF50 requires
enough disk to preserve the reserve and verified complete dependencies before
any newly measured joins.

For eventual replacement, retain failures 335–337, 429–430, 433 and 448 as
timeout/OOM/capacity evidence. Completed primaries 81, 339 and 431 become
superseded only after validated replacements with adequate coverage exist.
Do not delete 338 blindly: its Q33 observations add coverage absent from 339.
Earlier chat SF1 runs 36/60 and completed ClickBench run 338 need per-query
answer/provenance comparison before any pruning. No records were modified.
