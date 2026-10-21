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

The running image identifies itself as `doris-4.1.5-release-917b20be838`.
A serial Q33 experiment used off → SQL-only-on → aggregate-cache-on → off,
with four executions per condition and an identical answer hash throughout.
On the campaign's Python path, uncached execution took 4.15–5.71 seconds;
SQL-cached repeats took 5.84–8.97 milliseconds. The SQL-on plan contained
`LogicalSqlCache` / `PhysicalSqlCache`; the off plans did not. This confirms
cached-answer reuse, rather than merely inferring it from warmup differences.
Enabling the aggregate cache made this query slower (about 11–12 seconds).
ConnectorX did not show the SQL-cache speedup despite enabled flag readback;
the causal conclusion applies to the Python path actually used by the campaign.

Uncached ClickBench run 506 exposed a separate resource issue: Q18 exceeded the
backend's 10.8 GiB process limit within its 12 GiB container. The frontend also
had a 12 GiB cap, leaving too little of the combined allowance available to
execution. The corrected allocation is FE 6 GiB / BE 18 GiB, retaining FE one
CPU / BE nine CPUs and exactly 24 GiB combined. FE heap flags become
`-Xms1024m -Xmx4096m`; the backend retains its normal 90% process-memory policy.
All three Doris suites require fresh populate and select measurements under
this allocation. Run 506 remains failure evidence, not a replacement cohort.

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

Validated telemetry replacements preserve the original measured scope:

| Suite | Populate / select | Measured queries | Old sum of medians | New sum of medians |
| --- | --- | ---: | ---: | ---: |
| Metrics SF1 | 500 / 501 | 82 | 18.103 s | 4.135 s |
| Ranges SF1 | 502 / 503 | 90 | 11.543 s | 1.693 s |

Physical readback confirmed all 15 metrics chunks and all 75 ranges chunks in
the five compressible tables were compressed. `wide1000` remained an explicit
rowstore hypertable. Representative plans used columnar scans, vectorized
aggregation and metric/time pruning. A readiness guard now rejects old stores
whose row counts match but required hypertables or compressed chunks are absent.

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
