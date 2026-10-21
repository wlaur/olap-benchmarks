# October campaign configuration correction

The October 21 audit identified two changes requiring measured replacements:
Doris SQL-result caching was uncontrolled, and TimescaleDB metrics/ranges used
ordinary PostgreSQL tables. MonetDB's chat SF10 failures were confirmed server
OOM events; no worker, optimizer, or memory configuration error was established
by the static audit.

## Cache boundary

Doris disables `enable_sql_cache` and `enable_query_cache` on every Python and
ConnectorX connection. Effective values and runtime version strings are captured
before and after operations. Publication rejects select cohorts without these
observations. Predicate, inverted-index, file, page, and segment caches remain
available. This boundary preserves the hot profile and avoids asymmetrically
disabling predicate caches while ClickHouse uses them by default.

The initial subagent proposed disabling predicate caches too. That proposal was
narrowed before measured runs. Final-result and aggregate-result reuse are the
controlled variables; this campaign is not a cache-free comparison.

## TimescaleDB storage proof

Live tests used TimescaleDB 2.30.2 / PostgreSQL 18, ten workers and a 24 GiB
container limit, with the first 256 original Parquet rows and original dtypes/nulls.
The 785- and 1,500-metric core fixtures load into a wide hypertable, but
`compress_chunk` fails with tuple sizes 14,216 and 27,088 bytes versus the
8,160-byte limit. Their EAV layout therefore remains justified for compression.
The static inference that the EAV workaround was unnecessary was rejected.

Metrics `wide1000` likewise fails compression (18,088 bytes). It is explicitly a
rowstore hypertable. `wide100` and EAV fixtures compress successfully and produce
identical ordered results, including nulls, before and after compression. Both
telemetry suites now use seven-day hypertables; EAV tables segment by metric name
and order by time. The loader compresses supported layouts and analyzes them.
Wide tables exceeding PostgreSQL's column limit remain unsupported.

The live server reported ten parallel workers, nine workers per gather, 6 GiB
shared buffers and 139 MiB work_mem, all from command-line configuration.

## Replacement procedure

Reruns use the original immutable inputs, engine pins, ten-CPU/24-GiB budget,
45-second query limit, 900-second operation limit, one warmup, three measured
iterations, and serial engine execution. TimescaleDB telemetry initially retains
the original table/query scope. Doris reruns all three registered select suites.
MonetDB diagnostics first verify runtime settings and the anomalous chat and
ClickBench workloads. Answers must validate before replacement.

Only runs covered by validated replacements are superseded. Distinct timeout,
OOM, disk-limit and additional-query evidence must survive. Benchmark databases
remain release assets and are not committed.
