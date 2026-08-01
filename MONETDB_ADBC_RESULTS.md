# MonetDB ADBC versus staged binary ingestion

This is the release decision based on the preserved 2026-07-29 result databases. The slow suites
were not rerun. Times are complete population-run wall times, so ADBC Parquet decoding and staged
binary-file serialization are both inside the measured boundary.

The measured release stack is `adbc-driver-monetdb` 0.10.0 and
`sqlalchemy-monetdb-adbc` 0.3.0, both installed from their released PyPI
artifacts by the benchmark lockfile. Patch releases 0.10.1 and 0.3.1 retain
the measured ingest tuning and add correctness and bounded-memory hardening.

For this decision, latency within 10% is parity. A resource difference is material only when it
exceeds both 15% and 256 MB of client memory, 512 MB of server memory, or 1 GB of disk. Those
absolute bands keep small fixed buffers from deciding an otherwise equivalent run.

## Result

| Suite | ADBC | Staged binary | ADBC/staged | Decision |
|---|---:|---:|---:|---|
| Time Series SF1, release-wheel median of three | 19.884 s | 20.432 s | 0.973 | parity; ADBC 2.7% faster |
| TPC-DS SF1 | 9.534 s | 9.720 s | 0.981 | parity |
| Kaggle Airbnb | 7.292 s | 10.056 s | 0.725 | ADBC 27.5% faster |
| TPC-H SF1 | 13.092 s | 17.794 s | 0.736 | ADBC 26.4% faster |
| ClickBench | 305.699 s | 1054.889 s | 0.290 | ADBC 3.45× faster |
| RTABench, corrected experimental path | 100.280 s | 1519.301 s | 0.066 | ADBC 15.2× faster |

The current-era resource splits support the same conclusion:

| Suite | ADBC client / server / disk peak | Staged client / server / disk peak | Decision |
|---|---:|---:|---|
| Time Series SF1, release-wheel medians | 565 / 5408 / 4908 MB | 479 / 4806 / 4689 MB | resource parity by the declared relative and absolute bands |
| TPC-DS SF1 | 1271 / 1477 / 1564 MB | 1480 / 1483 / 1565 MB | ADBC lower client peak; server and disk parity |
| Kaggle Airbnb | 781 / 786 / 528 MB | 999 / 896 / 589 MB | ADBC lower on every resource |

The older staged TPC-H, ClickBench, and RTABench files predate the separate client/server memory
columns. Their combined peaks were respectively 1496 MB, 42824 MB, and 11941 MB, compared with
ADBC client peaks of 985 MB, 1717 MB, and 5030 MB. The staged ClickBench validation stopped as
failed after 813.618 seconds and 66.2 GB of peak disk; the separate completed historical run took
1054.889 seconds.

## RTABench release blocker and resolution

The released 0.9.1 default split a constrained append into repeated target COPY statements.
`order_items` then spent more than 138 seconds revalidating its growing composite primary key. An
otherwise identical diagnostic with one 2 GiB target window completed that table in 14.077
seconds, proving that repeated target validation—not Arrow production—caused the regression.

Driver 0.10.0 keeps the normal bounded windows but sends COPY-sized constrained appends to an
unconstrained session-local table, followed by one `INSERT … SELECT` into the target. Public ingest
telemetry distinguishes staging COPYs, target COPYs, and the final move. Live regression tests
verify multiple bounded staging windows, exactly one target move, constraint rollback, caller-work
preservation, temporary-object cleanup, and prepared-cache reuse after failed staging. The
`adbc.monetdb.constrained_append=direct` diagnostic escape hatch retains the old direct behavior.

This fixes the causal defect without the unacceptable 2 GiB client window and without recognizing
RTABench-specific tables. Unconstrained and tiny INSERT workloads remain on their existing direct
paths.

## Evidence locations

| Measurement | Result revision |
|---|---|
| Time Series release-wheel ADBC / staged, three paired runs | `monetdb-final-release5-adbc-20260729` / `monetdb-final-release5-staged-20260729` |
| TPC-DS ADBC / staged | `monetdb-cross-suite-final-tpcds-20260729` / `monetdb-cross-suite-final-default-20260729` |
| Kaggle Airbnb ADBC / staged | `monetdb-review2-adbc-final` / `monetdb-review-staged-final` |
| TPC-H ADBC / staged | `monetdb-review2-adbc-final` / `monetdb-staged-full-validation` |
| ClickBench ADBC / staged completed | `monetdb-final-release5-clickbench-20260729` / `default` |
| ClickBench staged failed resource run | `monetdb-clickbench-staged-validation` |
| RTABench ADBC / staged | `monetdb-cross-suite-dense-final-20260729` / `monetdb-staged-final-validation` |
| RTABench 2 GiB causal control | `monetdb-rtabench-control-2g-20260729` |

Reproduce the aggregate rows without opening a database file directly:

```bash
./.venv/bin/olap results query --revision REVISION \
  "select r.id, r.suite, round(epoch(r.finished_at-r.started_at), 3) run_s,
          max(m.client_mem_mb) client_mb, max(m.mem_mb) server_mb,
          max(m.disk_mb) disk_mb
   from run r left join run_metric m on m.run_id=r.id
   group by all order by r.id"
```

Historical files without `client_mem_mb` use `max(m.mem_mb)` as the combined peak. The comparison
names every source revision rather than selecting a best run, and the repeated Time Series result
reports medians. The earlier pre-final physical-window pair measured 17.378 seconds for ADBC versus
19.679 seconds staged; it remains useful tuning evidence but is not the release headline. Runs
record `db_driver`, package/editable revision provenance, container image
IDs and digests, effective MonetDB options, and SHA-256 input fingerprints; at
decision time publication rejected running runs, a release matrix without
same-system ADBC/staged pairs, missing or disagreeing correctness results,
mismatched query coverage, and unclean session baselines.

The staged binary path was removed after this decision. MonetDB now connects
only through ADBC, and publication requires a complete ADBC release matrix
with the same correctness and session-baseline gates.
