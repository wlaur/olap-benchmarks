# olap-benchmarks

> OLAP database benchmarks

[Results](https://wlaur.github.io/olap-benchmarks/)

## Requirements

- Python 3.13+ and [uv](https://docs.astral.sh/uv/)
- Docker (OrbStack on macOS)
- [Bun](https://bun.sh) for the web app in `site/`
- `tpchgen-cli` for the TPC-H suites: `cargo install tpchgen-cli`
- `tpcgen-cli` for the TPC-DS suite (not yet published to crates.io):
  `cargo install --git https://github.com/clflushopt/tpchgen-rs --rev 09d609d13b7b45a2aa06d2b86e5a4bfa29aacb1a tpcgen-cli`

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
| `OLAP_BENCHMARKS_SYSTEM`             | System label stored on every run (e.g. `macbook-pro-m4`); results from different systems are not comparable |

Install zsh completions once (rerun after CLI changes):

```bash
uv run olap --install-completion --shell zsh && exec zsh
```

## Workflow

The full loop for one suite/db combination:

```bash
# 1. generate or download input data (one-time per suite, see table below)
uv run olap prepare tpch_sf10

# 2. run the benchmark (populate + select [+ mutate] by default)
uv run olap benchmark clickhouse tpch_sf10

# 3. inspect the results
uv run olap results runs --suite tpch_sf10 --db clickhouse
uv run olap results query "select * from run order by started_at desc limit 5"

# 4. publish to the site (merge new runs into site/public/data/results.db)
uv run olap publish --merge

# 5. view locally
cd site && bun install && bun run dev
```

### `olap prepare <suite|all>`

| Suite                  | Data source                                                                              |
| ---------------------- | ---------------------------------------------------------------------------------------- |
| `time_series`          | Generated locally                                                                        |
| `rtabench`             | Downloaded automatically from rtadatasets.timescale.com                                  |
| `tpch_sf10` / `tpch_sf50` | Generated locally with `tpchgen-cli`                                                  |
| `tpcds_sf1`            | Generated locally with `tpcgen-cli` (decimals cast to spec precision and four columns renamed to spec names after generation) |
| `clickbench`           | Manual: download [hits.parquet](https://datasets.clickhouse.com/hits_compatible/hits.parquet) to `data/input/clickbench/` |
| `kaggle_airbnb`        | Manual: download the Austin CSVs from [Kaggle](https://www.kaggle.com/datasets/konradb/inside-airbnb-usa) to `data/input/kaggle_airbnb/`, then run `prepare` to convert to Parquet |

### `olap benchmark <db|all> <suite|all> [operation] [--revision NAME] [--cleanup] [--omit DB]`

- `db`: `monetdb`, `clickhouse`, `timescaledb`, `duckdb`, `questdb`, `postgres`, `starrocks`, or `all`
- `suite`: `rtabench`, `time_series`, `clickbench`, `kaggle_airbnb`, `tpch_sf10`, `tpch_sf50`, `tpcds_sf1`, or `all`
- `operation`: `populate`, `select`, `mutate`, or `all` (default). `mutate` is only supported by `time_series`.
- `--revision`: which results database to write to (`results/<revision>.db`, default `default`)
- `--cleanup`: delete the database files for the (db, suite) combination after the run
- `--omit`: skip databases when using `db=all`, e.g. `--omit questdb --omit starrocks`

The container is started and stopped automatically. Populate is skipped when
existing tables already match the expected row counts, so `olap benchmark <db>
<suite> select` reuses previously loaded data. Examples:

```bash
uv run olap benchmark all tpch_sf10             # every db, one suite
uv run olap benchmark duckdb all                # one db, every suite
uv run olap benchmark starrocks clickbench select   # re-run only the select queries
uv run olap benchmark all all --omit questdb    # the full matrix, minus one db
```

To start a database manually (e.g. to poke at loaded data):

```bash
uv run olap docker clickhouse tpch_sf10 start
uv run olap docker clickhouse tpch_sf10 stop
```

### `olap results ...`

```bash
uv run olap results revisions                        # list results/*.db
uv run olap results runs --status failed             # list runs (filters: --status/--suite/--db)
uv run olap results query "<sql>"                    # read-only SQL against a revision
uv run olap results delete --status failed           # delete failed + orphaned runs
uv run olap results delete --run-id 12 --run-id 13   # delete specific runs
uv run olap results rename-db old_name new_name      # rename a db in stored runs
```

All `results` commands accept `--revision` (default `default`).

### `olap publish [--revision NAME] [--merge]`

Copies `results/<revision>.db` to `site/public/data/results.db` along with
`manifest.json` and `queries.json`. With `--merge`, runs are merged into the
already-published file instead of replacing it: runs matching an existing
(system, db, db_version, suite, operation, started_at) are replaced, new runs
are added, everything else is kept.

## Database versions

`db_version` is recorded on every run and is part of the run identity, so runs
against different versions of the same database coexist in the results. The
version pins live in each connector:

| Database    | Pinned in                                          |
| ----------- | -------------------------------------------------- |
| clickhouse  | `olap_benchmarks/dbs/clickhouse/__init__.py` (`VERSION`, Docker image tag) |
| monetdb     | `olap_benchmarks/dbs/monetdb/__init__.py`          |
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
- **RTABench** suite is based on [RTABench](https://github.com/timescale/rtabench) by Timescale
- **Kaggle Airbnb** suite is based on ["Testing query speed for DuckDB vs ClickHouse vs StarRocks databases"](https://medium.com/@marvin_data/testing-query-speed-for-duckdb-vs-clickhouse-vs-starrocks-databases-fecc6614d1ef) by Vitaliy
- **TPC-H** suites (`tpch_sf10`, `tpch_sf50`) are derived from the [TPC-H benchmark](https://www.tpc.org/tpch/); results are not comparable to published TPC-H results. Data is generated with [tpchgen-rs](https://github.com/clflushopt/tpchgen-rs) (requires `cargo install tpchgen-cli`). Base queries come from the [DuckDB tpch extension](https://github.com/duckdb/duckdb/tree/main/extension/tpch/dbgen/queries), with per-database adaptations from [ClickHouse](https://github.com/ClickHouse/ClickHouse/tree/master/tests/benchmarks/tpc-h) and [StarRocks](https://docs.starrocks.io/docs/benchmarking/TPC-H_Benchmarking/)
- **TPC-DS** suite (`tpcds_sf1`) is derived from the [TPC-DS benchmark](https://www.tpc.org/tpcds/); results are not comparable to published TPC-DS results. Data is generated with the `tpcgen-cli` from [tpchgen-rs](https://github.com/clflushopt/tpchgen-rs) at commit `09d609d1` (`--compat c`, conformance-tested against the reference dsdgen). Queries come from the [DuckDB tpcds extension](https://github.com/duckdb/duckdb/tree/main/extension/tpcds), with schema adaptations from [ClickHouse](https://github.com/ClickHouse/ClickHouse/tree/master/tests/benchmarks/tpc-ds) and [StarRocks](https://docs.starrocks.io/docs/3.4/benchmarking/TPC_DS_Benchmark/)

## TODO

- Additional step for time series suite with mutate+select
    - Concurrently: insert one row as quickly as possible to large table + run a small number of selects against this table (multiple clients)
    - Simulates actual workloads (single writer + multiple readers)
    - What should this operation be named?
