# Device Telemetry

A custom multi-device workload with 12 nullable metrics, a small device dimension, mixed reporting cadences, selective tags, and 18 analytical queries. It implements `populate` and `select` using the existing suite, Parquet loader, phase logging, answer hashing, input fingerprinting, and resource measurement paths. It does not implement or register `mutate` or `concurrent`.

## Data contract

Both scales end at **2025-01-15 00:00:00 UTC, exclusive**. Timestamps are naive UTC with millisecond precision. The fixed arithmetic seed is **20251022**; generation never reads the wall clock or uses process-local randomness.

| Scale | Devices | History | Exact reading rows | Device rows |
| ----- | ------: | ------: | -----------------: | ----------: |
| 1     |   1,000 |  7 days |            484,318 |       1,000 |
| 10    |   5,000 | 14 days |          4,863,454 |       5,000 |

SF10 multiplies cardinality by five and history by two. It preserves the final seven days of every SF1 device exactly. These scales change device/history cardinality, rather than column width. The largest scale has approximately 467 MB of raw metric doubles before null masks, identifiers, strings, indexes, and engine overhead; this is a size calculation, not a measured memory or storage result. Generation processes 24 five-minute slots per batch, filtering activity before computing metrics. It does not materialize the full dataset in memory. Full-scale warm-read measurements and preparation observations are recorded in the [follow-up report](../../../docs/campaigns/2026-10-followup.md); the size calculation is not a peak-memory measurement.

`telemetry_device` has one row per `device_id`. Its columns are `device_id`, `region`, `tenant`, `model`, nullable `rack`, `cadence_slots`, and `rated_power`. Tags are immutable: four regions, 100 tenants, three models, and sixteen rack labels. `rack` is NULL when `device_id % 11 = 0`. Tenants and reporting cadence are intentionally correlated; tenant 7 is a selective cold-device cohort. This is a tag-selectivity workload, not a claim of statistically independent tags.

`telemetry_reading` has 20 columns: `time`, `device_id`, `sample_no`, `ingested_at`, four denormalized tags (`region`, `tenant`, `model`, `rack`), and these twelve Float64 metrics:

`cpu_user`, `cpu_system`, `memory_used`, `disk_used`, `net_rx`, `net_tx`, `temperature`, `battery`, `voltage`, `current`, `signal`, `errors`.

The names represent common telemetry fields; their values are synthetic dimensionless quarter units in `[0, 99.75]`. They do not model physical units, monotonic counters, or realistic correlations between metrics.

A sample's identity is **(device_id, time)**. Many devices share an event timestamp. There is exactly one row per identity and no duplicate-update workload. PostgreSQL and TimescaleDB enforce the composite key; DuckDB and ClickHouse preserve the same generated multiplicity without a unique index. No schema uses a timestamp-only primary key.

Let `d` be the device ID, `s` the five-minute slot since 2025-01-01 00:00 UTC, and `i` the metric's zero-based position:

- Devices with `d % 10 = 0` report every slot; residues 1–3 report every three slots; residues 4–9 report every twelve slots. Before outages, 10% of devices produce about 40% of readings, 30% produce about 40%, and 60% produce about 20%.
- A scheduled row is missing when `(s + 13*d + 20251022) % 97 < 3`. Devices divisible by 17 emit nothing in the final day. Missing rows are absent, not rows of zeroes or all-NULL metrics.
- A metric is NULL when `(s + 7*d + 11*i + 20251022) % 101 < 5`. Otherwise its exact value is `((s*(2*i+1) + d*(7*i+11) + 20251022) % 400) / 4`. There are no NaNs or infinities.
- `sample_no = s` and `time = origin + s*5 minutes`.
- `ingested_at` is a **simulated arrival timestamp**: `time + 7200 seconds` when `(s+d) % 20 = 0`, otherwise `time + ((s+d) % 31) seconds`. Some values extend beyond the event-time cutoff. All such rows are retained.

Bulk input is ordered by `(time, device_id)`. Simulated arrival timestamps do not make this an out-of-order ingestion benchmark. Actual late-write cost, duplicate conflict/update behavior, and concurrent freshness are outside the current measured operations.

Preparation writes two Zstd Parquet files and a versioned `generation.json` contract under `device_telemetry_sf1/` or `device_telemetry_sf10/`. Reuse validates the generation contract, schemas, and exact row counts. Partial or stale inputs fail with an instruction to regenerate using `overwrite=True`. The harness's existing `fingerprint_input_directory` supplies the content hashes; this suite does not introduce a separate hashing cache or results format.

## Queries and exact semantics

| Query | Operation                                                                                  |
| ----- | ------------------------------------------------------------------------------------------ |
| 01    | Time, CPU, and memory from the latest row for device 42                                    |
| 02    | Time, CPU, and memory from the latest row for each observed device                         |
| 03    | Time, CPU, and memory from the latest row per device in region 1                           |
| 04    | Tenant 7 statistics in the final hour                                                      |
| 05    | Region 1 statistics by model in the final day                                              |
| 06    | Fleet averages across all twelve metrics in the final hour                                 |
| 07    | Ordered raw readings for device 42 in the final day                                        |
| 08    | Hourly downsampling of device 42 in the final day                                          |
| 09    | Region/model hourly downsampling in the final day                                          |
| 10    | Daily fleet aggregates by region over all stored history                                   |
| 11    | Top twenty devices by average CPU, requiring twelve valid samples in the final day         |
| 12    | CPU and joint CPU/memory threshold counts by region                                        |
| 13    | Six-observation moving average for device 42                                               |
| 14    | Reporting gaps relative to each device's configured cadence                                |
| 15    | CPU, battery, and nullable-rack coverage by region                                         |
| 16    | Many-to-one metadata join and synthetic power estimate, grouped by region/rack             |
| 17    | Inventory devices whose latest event precedes the final day, or which have no observations |
| 18    | Simulated arrivals after the cutoff among events from the final hour                       |

All time ranges are half open. Buckets align to UTC hour/day boundaries, with no gap filling. Aggregates ignore NULL values, return NULL for an all-NULL numeric input, and count actual rows. A threshold comparison against NULL is not a breach. Latest means the latest **row**, preserving NULL metrics from that row; it never substitutes an older non-NULL value. Identity uniqueness makes latest ordering deterministic.

Query 13 uses `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW` after applying the final-day filter. It is six observed rows, not thirty elapsed minutes. Query 14 likewise considers only final-day observations: the first observation has no predecessor, gaps are measured in five-minute slots, and missing slots must exceed the device's cadence. Entirely inactive devices are covered by query 17. Query 16 retains a NULL rack group. Every ordered LIMIT has a device-ID tie breaker where needed.

## Engine contracts and validation limits

The registry and matrix include **DuckDB, PostgreSQL, TimescaleDB, and ClickHouse** at SF1 and SF10, with `populate` and `select` operations. No queries are marked unsupported or excluded from answer comparison for these engines. Other engines fail closed if the class is instantiated manually. The full-scale campaign completed all eight engine/scale cohorts, with successful warmup plus three measured observations for each query and matching-input peer hashes agreeing.

- DuckDB uses shared SQL and the existing native/Parquet storage loader and manifest. Its schema declares required values without persistent primary-key indexes, matching the repository's existing DuckDB storage convention.
- PostgreSQL uses `(device_id, time)` and time, tenant/time, and region/time indexes. Latest-per-device queries use `DISTINCT ON`. Data loads through the existing COPY path, followed by `VACUUM FREEZE ANALYZE` on both tables.
- TimescaleDB uses the PostgreSQL schema plus one-day hypertable chunks and columnstore segmented by device, ordered by descending time. Populate compresses all chunks, runs `VACUUM FREEZE ANALYZE`, and reuses `verify_telemetry_storage` for the reading hypertable on completion, reuse, and select. Device metadata remains a regular table. Latest uses `DISTINCT ON`; bucket queries use `time_bucket`. No retention policy is installed, so fixed historical events and simulated late-arrival metadata are retained.
- ClickHouse uses MergeTree, daily partitions, and `(region, tenant, device_id, time)` ordering. Metrics and rack remain nullable. `argMax(tuple(time, cpu_user, memory_used), time)` preserves NULLs in the latest row. `*OrNull` aggregates preserve all-NULL/empty semantics, an explicitly nullable `lagInFrame` avoids a fabricated first-row gap, and `join_use_nulls=1` preserves absent metadata-join matches. Timestamp outputs use the existing normalization path. No ReplacingMergeTree or background deduplication changes row identity.

All 18 queries passed the populated native fixture against an independent Python oracle on DuckDB, ClickHouse, PostgreSQL, and TimescaleDB. These engine gates ran serially; TimescaleDB passed both before and after chunk compression. The fixture uses 60 devices and two days of history, not the complete SF1 or SF10 dataset. This fixture evidence covers its tested answers. Separate full-scale cohorts 579/581 (DuckDB), 593/595 (ClickHouse), 608/610 (PostgreSQL), and 614/616 (TimescaleDB) supply SF1/SF10 warm-read timings and recorded preparation evidence in the follow-up report.

Offline tests also execute shared, PostgreSQL, and TimescaleDB query texts in in-memory DuckDB against the Python oracle, and check ClickHouse SQL structurally. They cover batch-independent generation, scale overlap, schema/null/multiplicity, half-open boundaries, latest-row NULLs, failed population/reuse, and the append-visibility probe. The native fixture above provides separate evidence of execution on each engine.

QuestDB and TDengine are intentionally deferred. QuestDB needs a dedicated designated-timestamp/partitioned loader with appropriate symbol tags, a native latest-row path, and verified window/join/null behavior. Its generic Parquet CTAS is insufficient evidence for that contract. TDengine's existing driver only loads the metric-oriented range suite; reusing its timestamp-primary basic table for multiple devices could overwrite simultaneous samples. A proper device supertable/subtable loader, metadata relation strategy, and native query contracts must be implemented and checked before registration. No unsupported dialect is silently mapped to shared SQL.

## Ingestion visibility extension hook

`visibility_batch(spec, batch_number)` returns a frame, probe SQL, and the **exact expected ordered probe result**. It emits one row for each of the first eight device IDs (or the whole inventory if smaller), at `END + batch_number*5 minutes`; values use the same deterministic formula, and simulated `ingested_at` is one second later. This is an explicit probe cadence that can resume inactive devices. No baseline identity is reused. Batch numbers must be nonnegative and must never repeat within a run.

A future shared concurrent runner can precompute a batch, call `db.insert(batch.frame, 'telemetry_reading')` on an existing worker connection, and probe `batch.query` from a distinct reader connection. It must compare every row and NULL against `batch.expected`, not just a maximum timestamp or row count. Duplicate rows must fail the comparison. Record writer send/ack times, first complete visibility on a monotonic clock, freshness deadlines, failures, and resource use with the existing run APIs. The probe's event time is deterministic and is not a wall-clock latency measurement. Before insertion its result is empty; after insertion the exact result is independent of later batches. Rebuild/clean the store before normal benchmark reuse, which checks baseline row counts. DuckDB's existing dirty-store rules must remain in force.

This hook does not itself create threads, measure latency, or register an ingestion/concurrent operation. Actual backdated writes and duplicate updates need separate contracts in the common runner.

## Sources and provenance

This is **custom Device Telemetry**, not an official TSBS workload or a TSBS-compatible score. No upstream source code or datasets were copied. The query families were informed by the [TSBS overview](https://github.com/timescale/tsbs) and inspected against its [TimescaleDB Devops query generator at commit 8323e59c74027b108f4ad5ec5d3e498b0101a02e](https://github.com/timescale/tsbs/blob/8323e59c74027b108f4ad5ec5d3e498b0101a02e/cmd/tsbs_generate_queries/databases/timescaledb/devops.go): time-bucket aggregation, latest row per host, and high-CPU filters. The arithmetic generator, moderate metric schema, cadence skew, sparse outages, NULL/tag coverage, metadata power join, simulated arrival field, and visibility probe are custom extensions.

Dialect design was checked against primary documentation for [PostgreSQL windows](https://www.postgresql.org/docs/18/functions-window.html), [Timescale hypertables](https://docs.tigerdata.com/api/latest/hypertable/create_hypertable/), [Timescale chunk compression](https://docs.tigerdata.com/api/latest/compression/compress_chunk/), [ClickHouse argMax NULL behavior](https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/argmax), [ClickHouse lagInFrame](https://clickhouse.com/docs/sql-reference/window-functions/lagInFrame), [QuestDB latest-row requirements](https://questdb.com/docs/query/sql/latest-on/), and [TDengine primary-key ingestion semantics](https://docs.tdengine.com/tdengine-sql/data-write/insert/).

## Reproduction and validation

From the repository root, prepare and run one scale at a time:

```sh
uv run --no-sync olap prepare device_telemetry --scale-factor 1
uv run --no-sync olap benchmark duckdb device_telemetry populate --scale-factor 1
uv run --no-sync olap benchmark duckdb device_telemetry select --scale-factor 1
```

Use `--scale-factor 10` for the larger dataset and substitute another registered engine as needed. Run engines and measured operations serially. These are reproduction commands, not a record of completed full-scale measurements.

Offline validation:

```sh
uv run --no-sync ruff format --check olap_benchmarks/suites/device_telemetry olap_benchmarks/tests/test_device_telemetry*.py
uv run --no-sync ruff check olap_benchmarks/suites/device_telemetry olap_benchmarks/tests/test_device_telemetry*.py
uv run --no-sync pyright olap_benchmarks/suites/device_telemetry olap_benchmarks/tests/test_device_telemetry*.py
uv run --no-sync pytest olap_benchmarks/tests/test_device_telemetry*.py -q
```

The native fixture gate is marked `live` and excluded from default pytest runs. It requires one already provisioned, empty scratch database, uses the shared Parquet ingestion path, and drops its fixture tables afterward. It does not start or restart an engine or record benchmark timings. Select one of the four registered engines per invocation:

```sh
OLAP_BENCHMARKS_TELEMETRY_FIXTURE_DATABASE=duckdb uv run --no-sync pytest -m live -q \
  olap_benchmarks/tests/test_device_telemetry_native.py
```
