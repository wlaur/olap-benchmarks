# olap-benchmarks

> OLAP database benchmarks

[Results](https://wlaur.github.io/olap-benchmarks/)

## Requirements

- Python 3.13+ and [uv](https://docs.astral.sh/uv/)
- Docker (OrbStack on macOS)
- [Bun](https://bun.sh) for the web app in `site/`
- The TPC data generators, both from `tpchgen-rs` and both pinned to the same commit (the crates.io
  release lags `main`, and `tpcgen-cli` is not published at all):
  `cargo install --git https://github.com/clflushopt/tpchgen-rs --rev e53dea45345d3c934c724147e393983a53a40986 tpchgen-cli tpcgen-cli`

## Setup

```bash
uv sync
```

Configuration lives in `.env` (see `olap config` for the resolved values):

| Variable                             | Purpose                                              |
| ------------------------------------ | ---------------------------------------------------- |
| `OLAP_BENCHMARKS_INPUT_DATA_DIRECTORY` | Suite input data (Parquet/CSV) written by `prepare` |
| `OLAP_BENCHMARKS_DATABASE_DIRECTORY` | Persistent database files, one subdir per (db, suite) |
| `OLAP_BENCHMARKS_TEMPORARY_DIRECTORY` | Scratch space used during populate                  |
| `OLAP_BENCHMARKS_RESULTS_DIRECTORY`  | DuckDB results databases (`<revision>.db`)           |
| `OLAP_BENCHMARKS_SYSTEM`             | System label stored on every run (e.g. `macbook-m4-pro`); results from different systems are not comparable |
| `OLAP_BENCHMARKS_HOST_PORTS`         | JSON object remapping the host ports the benchmark containers bind, e.g. `{"monetdb": 50010}`, to avoid colliding with other local containers. Only the listed names are overridden; unknown names are rejected. Container-side ports are fixed by the images. Names: `monetdb`, `clickhouse_http`, `clickhouse_native`, `postgres`, `timescaledb`, `questdb_http`, `questdb_pg`, `doris_fe_mysql`, `doris_fe_http`, `doris_be_http`, `starrocks_fe_mysql`, `starrocks_fe_http`, `starrocks_be_http` |
| `OLAP_BENCHMARKS_MONETDB_WRITE_WINDOW_BYTES` | Optional ADBC COPY window byte budget; unset uses the driver's latency- and width-adaptive default |
| `OLAP_BENCHMARKS_MONETDB_WIRE_COMPRESSION` | ADBC upload compression: sampled `auto` (default), client-only `none`, or forced `lz4` |
| `OLAP_BENCHMARKS_MONETDB_CONSTRAINED_APPEND` | `auto` (default) stages bounded COPY windows and validates constrained targets once; use `direct` only for a diagnostic comparison or a measured server/workload exception |

Container images follow the Docker engine architecture automatically, including
OrbStack's Linux engine on Apple Silicon. The pinned ClickHouse, TimescaleDB,
QuestDB, PostgreSQL, StarRocks, and Doris references are multi-architecture.
MonetDB uses the official `monetdb/monetdb` image on amd64 and
`wlaur/monetdb-container` on ARM64. A connector marked as lacking an ARM64 image
still runs its amd64 image but emits a prominent warning that CPU virtualization
overhead will affect the result. The selected image platform and virtualization
status are saved in run metadata.

Install zsh completions once (rerun after CLI changes):

```bash
uv run olap --install-completion --shell zsh && exec zsh
```

## Workflow

The full loop for one suite/db combination:

```bash
# 1. generate or download input data (one-time per suite, see table below)
uv run olap prepare tpc_h --scale-factor 10

# 2. run the benchmark (populate + select [+ mutate/concurrent] by default)
uv run olap benchmark clickhouse tpc_h --scale-factor 10

# 3. inspect the results
uv run olap results runs --suite tpc_h --db clickhouse
uv run olap results query "select * from run order by started_at desc limit 5"

# 4. publish to the site and upload the database to the `data` release
uv run olap publish --merge --upload

# 5. view locally
cd site && bun install && bun run dev
```

### `olap prepare <suite|all>`

| Suite                  | Data source                                                                              |
| ---------------------- | ---------------------------------------------------------------------------------------- |
| `time_series`          | Generated locally                                                                        |
| `rtabench`             | Downloaded automatically from rtadatasets.timescale.com                                  |
| `tpc_h`                | Generated locally with `tpchgen-cli`; use `--scale-factor` for SF10/SF50/etc.         |
| `tpc_ds`               | Generated locally with `tpcgen-cli`; use `--scale-factor` (decimals cast to spec precision and four columns renamed to spec names after generation) |
| `jsonbench`            | Downloaded automatically from ClickHouse's public Bluesky JSONBench dataset; currently supports SF10 |
| `clickbench`           | Manual: download [hits.parquet](https://datasets.clickhouse.com/hits_compatible/hits.parquet) to `data/input/clickbench/` |
| `kaggle_airbnb`        | Manual: download the Austin CSVs from [Kaggle](https://www.kaggle.com/datasets/konradb/inside-airbnb-usa) to `data/input/kaggle_airbnb/`, then run `prepare` to convert to Parquet |

### `olap benchmark <db|all> <suite|all> [operation] [--revision NAME] [--cleanup] [--omit DB] [--scale-factor N]`

- `db`: `monetdb`, `clickhouse`, `timescaledb`, `duckdb`, `polars`, `questdb`, `postgres`, `starrocks`, `doris`, or `all`
- `suite`: `rtabench`, `time_series`, `clickbench`, `jsonbench`, `kaggle_airbnb`, `tpc_h`, `tpc_ds`, or `all`
- `operation`: `populate`, `select`, `mutate`, `concurrent`, or `all` (default). `mutate` and `concurrent` are only supported by `time_series`.
- `--revision`: which results database to write to (`results/<revision>.db`, default `default`)
- `--cleanup`: delete the database files for the (db, suite) combination after the run
- `--omit`: skip databases when using `db=all`, e.g. `--omit questdb --omit starrocks`
- `--scale-factor`: suite scale factor (`>= 1`). Time-series supports SF1/SF10, JSONBench currently supports SF10, TPC-H supports SF10/SF50, and TPC-DS supports SF1; fixed-size suites require `1`.

The container is started and stopped automatically. Populate is skipped when
existing tables already match the expected row counts, so `olap benchmark <db>
<suite> select` reuses previously loaded data. With `suite=all`, PostgreSQL and
TimescaleDB skip optional TPC-H/TPC-DS runs by default; select those suites
explicitly to include row-store low-scale runs. Examples:

```bash
uv run olap benchmark all tpc_h --scale-factor 10  # every db, explicit suite/scale
uv run olap benchmark duckdb all                # one db, every default suite/scale
uv run olap benchmark starrocks clickbench select   # re-run only the select queries
uv run olap benchmark all all --omit questdb    # the default matrix, minus one db
```

MonetDB connects exclusively through ADBC (`adbc-driver-monetdb` +
`sqlalchemy-monetdb-adbc`). Ingest streams Arrow data over the wire — eager
Polars frames, lazy frames, and Parquet files all take the same path — and
reads return Arrow tables that Polars adopts zero-copy:

```python
from pathlib import Path

import polars as pl
from adbc_driver_monetdb import ParquetArrowStream, PolarsArrowStream
from sqlalchemy import create_engine
from sqlalchemy_monetdb_adbc import fetch_arrow_table, ingest_arrow

engine = create_engine("monetdb+adbc://monetdb:monetdb@localhost:50000/benchmark")
with engine.connect() as connection:
    # bulk-load a Polars frame (any Arrow PyCapsule source works)
    ingest_arrow(connection, "events", pl.DataFrame({"id": [1, 2]}), mode="create_append")

    # stream a lazy frame or a Parquet file without materializing it
    ingest_arrow(connection, "events", PolarsArrowStream(pl.scan_parquet("events.parquet")))
    with ParquetArrowStream(Path("events.parquet")) as stream:
        ingest_arrow(connection, "events", stream)
    connection.commit()

    # read a query result as Arrow and hand it to Polars
    frame = pl.from_arrow(fetch_arrow_table(connection, "select * from events"))
```

The benchmark adapter in `olap_benchmarks/dbs/monetdb/adbc.py` wraps these
primitives with constraint-preserving table creation (primary keys, not-null
columns), ingest row-count validation, and query recording.

MonetDB runs persist `adbc` as a typed run dimension; publication requires a
completed ADBC run for all 20 matrix cells and rejects running results,
missing query row counts and answer hashes, and unclean session baselines.
Each MonetDB operation records `session_baseline_before` and
`session_baseline_after` phases. Connections are tagged as `olap-benchmarks`
and scoped to the benchmark process ID. The post-operation phase disposes the
SQLAlchemy engine and requires zero matching server sessions and zero residual
ADBC staging tables.

Input fingerprints, package provenance, and container-image metadata are collected before the
recorded run start, so populate wall time excludes harness hashing and metadata probes. Reading
the inputs for SHA-256 may warm the operating-system page cache; populate results are therefore
warm-input end-to-end measurements, not cold-cache storage benchmarks.

Populate timings cover each connector's supported end-to-end loading path, which is not always
the same client pipeline. Current MonetDB and StarRocks paths can load source Parquet directly,
while connectors without an equivalent path may materialize or transform data through Polars.
In particular, StarRocks time-series populate results from before the direct `INSERT FROM FILES`
path are not comparable with current results. ClickBench populate comparisons likewise compare
database loading paths, not identical client preprocessing.

#### Resource metrics: server side vs client side

Each `run_metric` row separates the database server from the benchmark client:

| Column | Side | Meaning |
| --- | --- | --- |
| `server_mem_mb` | server | Sum of database-container memory. **Always 0 for in-process engines** (`duckdb`, `polars`), which have no server. |
| `client_mem_mb` | client | Peak RSS of the benchmark Python process, sampled every 10 ms. |
| `client_uss_mb` | client | Peak unique set size of the same process, sampled every 100 ms, so shared and reclaimable file-backed pages can be separated from private memory. The USS scan is the expensive one, hence the lower rate; it would otherwise materially perturb client CPU on Linux. |
| `cpu_percent` | combined | Benchmark client process **plus** every database container, summed. It cannot be decomposed into a server and a client share. |
| `disk_mb` | server | The database's own storage directories (`Database.metric_directories`, today just its data directory). Client staging files under the shared temporary directory and the benchmark client's own disk use are not measured. |

Server and client memory are two independent peak series and are never summed.
They fund different budgets (the client normally runs on a different host), and
adding two separately computed maxima overstates the true combined peak because
the peaks do not coincide in time — on one recorded run `max(server) + max(client)`
exceeded `max(server + client)` by 31 GB.

For cross-engine comparison use the *comparable* peak memory: `server_mem_mb` for
containerised engines and `client_mem_mb` for in-process engines. Reading
`server_mem_mb` alone would rank DuckDB and Polars as using no memory at all.
`olap results resources` resolves this per run and refuses to report a number it
cannot interpret:

```bash
uv run olap results resources --revision default --suite clickbench
```

These are metric schema v5 semantics (v4 renamed to `server_mem_mb`) and must not
be compared as identical measurements with v2/v3 client-memory data. The
definitions and the metric-schema version are stored with each run so results
remain interpretable after methodology changes.

To start a database manually (e.g. to poke at loaded data):

```bash
uv run olap docker clickhouse tpc_h start --scale-factor 10
uv run olap docker clickhouse tpc_h stop --scale-factor 10
```

### `olap results ...`

```bash
uv run olap results revisions                        # list results/*.db
uv run olap results runs --status failed             # filters: --status/--suite/--db/--db-driver
uv run olap results query "<sql>"                    # read-only SQL against a revision
uv run olap results resources --db duckdb            # peak server/client memory, disk and CPU per run
uv run olap results delete --status failed           # delete failed + orphaned runs
uv run olap results delete --run-id 12 --run-id 13   # delete specific runs
uv run olap results rename-db old_name new_name      # rename a db in stored runs
```

All `results` commands accept `--revision` (default `default`).

### `olap publish [--revision NAME] [--merge] [--upload]`

Copies `results/<revision>.db` to `site/public/data/results.db` along with
`manifest.json` and `queries.json`. With `--merge`, runs are merged into the
already-published file instead of replacing it: runs matching an existing
(system, db, db_version, db_driver, suite, suite_scale_factor, operation,
started_at) are replaced, new runs are added, everything else is kept.

**`results.db` is not committed.** It is large and rewritten on every publish, so
git would keep every version forever. It ships as the `results.db` asset on the
`data` release: `--upload` replaces that asset via the GitHub CLI, and the site
build fetches it with `bun run fetch-data` (the Pages workflow runs this before
`bun run build` and fails the build if the asset is missing or truncated). The
small `manifest.json`, `queries.json` and `suites.json` stay in git.

A fresh clone therefore needs the database before the site will run:

```bash
cd site && bun run fetch-data && bun run dev
```

### Remote benchmark runs

Use a named revision when running benchmarks on another host, and keep the
portable revision under `results/shared/` so it can move through Git without
mixing with local scratch revisions.

On the benchmark host:

```bash
export OLAP_BENCHMARKS_RESULTS_DIRECTORY="$PWD/results/shared"
export OLAP_BENCHMARKS_SYSTEM="cloud-c7i-4xlarge"

uv run olap benchmark clickhouse tpc_ds --revision cloud-c7i-tpcds-sf1 --scale-factor 1
uv run olap results migrate --revision cloud-c7i-tpcds-sf1

git add results/shared/cloud-c7i-tpcds-sf1.db
git commit -m "add cloud c7i tpc ds results"
```

After merging that branch on the publishing machine:

```bash
export OLAP_BENCHMARKS_RESULTS_DIRECTORY="$PWD/results/shared"

uv run olap results migrate --revision cloud-c7i-tpcds-sf1
uv run olap publish --revision cloud-c7i-tpcds-sf1 --merge --upload

git add site/public/data/manifest.json site/public/data/queries.json
git commit -m "publish cloud c7i tpc ds results"
```

`publish --merge` verifies that the source and published databases are at the
current schema revision before merging. Use a unique revision name per host/run
so independent benchmark branches do not overwrite each other's result files.

## Database versions

`db_version` is recorded on every run and is part of the run identity, so runs
against different versions of the same database coexist in the results. The
version pins live in each connector:

| Database    | Pinned in                                          |
| ----------- | -------------------------------------------------- |
| clickhouse  | `olap_benchmarks/dbs/clickhouse/__init__.py` (`VERSION`, Docker image tag) |
| doris       | `olap_benchmarks/dbs/doris/__init__.py` (`VERSION`, split FE/BE Docker image tags) |
| monetdb     | `olap_benchmarks/dbs/monetdb/__init__.py`          |
| polars      | `pyproject.toml` (runs in-process; version follows the installed `polars` package) |
| postgres    | `olap_benchmarks/dbs/postgres/__init__.py`         |
| timescaledb | `olap_benchmarks/dbs/timescaledb/__init__.py`      |
| questdb     | `olap_benchmarks/dbs/questdb/__init__.py`          |
| starrocks   | `olap_benchmarks/dbs/starrocks/__init__.py`        |
| duckdb      | `pyproject.toml` (runs in-process; version follows the installed `duckdb` package) |

To benchmark a new version: bump the pin, rerun `olap benchmark`, and publish —
old-version runs remain in the results database.

## Results schema migrations

Install dev dependencies (includes Alembic):

```bash
uv sync --group dev
```

Create a new migration from SQLAlchemy models:

```bash
uv run --group dev alembic revision --autogenerate -m "describe_change"
```

Apply migrations to a results revision:

```bash
uv run olap results migrate --revision default
```

Or directly with Alembic (default db from `.env`, or an explicit path):

```bash
uv run --group dev alembic upgrade head
uv run --group dev alembic -x db=/absolute/path/to/results.db upgrade head
```

## Site development

```bash
cd site
bun install
bun run dev       # local dev server against site/public/data/results.db
bun run codegen   # regenerate Kysely types after a results schema change
bun run verify    # typecheck + lint + format check + build
```

## Testing

```bash
uv run pyright
uv run ruff check && uv run ruff format --check
uv run pytest
```

## Attribution

- **ClickBench** suite is based on [ClickBench](https://github.com/ClickHouse/ClickBench) by ClickHouse
- **JSONBench** suite is based on [JSONBench](https://github.com/ClickHouse/JSONBench) by ClickHouse
- **RTABench** suite is based on [RTABench](https://github.com/timescale/rtabench) by Timescale
- **Kaggle Airbnb** suite is based on ["Testing query speed for DuckDB vs ClickHouse vs StarRocks databases"](https://medium.com/@marvin_data/testing-query-speed-for-duckdb-vs-clickhouse-vs-starrocks-databases-fecc6614d1ef) by Vitaliy
- **TPC-H** suite (`tpc_h`) is derived from the [TPC-H benchmark](https://www.tpc.org/tpch/); results are not comparable to published TPC-H results. Data is generated with [tpchgen-rs](https://github.com/clflushopt/tpchgen-rs) (requires `cargo install tpchgen-cli`). Base queries come from the [DuckDB tpch extension](https://github.com/duckdb/duckdb/tree/main/extension/tpch/dbgen/queries), with per-database adaptations from [ClickHouse](https://github.com/ClickHouse/ClickHouse/tree/master/tests/benchmarks/tpc-h) and [StarRocks](https://docs.starrocks.io/docs/benchmarking/TPC-H_Benchmarking/)
- **TPC-DS** suite (`tpc_ds`) is derived from the [TPC-DS benchmark](https://www.tpc.org/tpcds/); results are not comparable to published TPC-DS results. Data is generated with the `tpcgen-cli` from [tpchgen-rs](https://github.com/clflushopt/tpchgen-rs) at commit `09d609d1` (`--compat c`, conformance-tested against the reference dsdgen). Queries come from the [DuckDB tpcds extension](https://github.com/duckdb/duckdb/tree/main/extension/tpcds), with schema adaptations from [ClickHouse](https://github.com/ClickHouse/ClickHouse/tree/master/tests/benchmarks/tpc-ds) and [StarRocks](https://docs.starrocks.io/docs/3.4/benchmarking/TPC_DS_Benchmark/)
