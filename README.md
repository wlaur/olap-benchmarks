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
| `OLAP_BENCHMARKS_SYSTEM`             | System label stored on every run (e.g. `macbook-m4-pro`); results from different systems are not comparable |

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
| `tpc_h`                | Generated locally with `tpchgen-cli`; use `--scale-factor` for SF10/SF50/etc.         |
| `tpc_ds`               | Generated locally with `tpcgen-cli`; use `--scale-factor` (decimals cast to spec precision and four columns renamed to spec names after generation) |
| `jsonbench`            | Downloaded automatically from ClickHouse's public Bluesky JSONBench dataset; currently supports SF10 |
| `clickbench`           | Manual: download [hits.parquet](https://datasets.clickhouse.com/hits_compatible/hits.parquet) to `data/input/clickbench/` |
| `kaggle_airbnb`        | Manual: download the Austin CSVs from [Kaggle](https://www.kaggle.com/datasets/konradb/inside-airbnb-usa) to `data/input/kaggle_airbnb/`, then run `prepare` to convert to Parquet |

### `olap benchmark <db|all> <suite|all> [operation] [--revision NAME] [--cleanup] [--omit DB] [--scale-factor N]`

- `db`: `monetdb`, `clickhouse`, `timescaledb`, `duckdb`, `polars`, `questdb`, `postgres`, `starrocks`, or `all`
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

To start a database manually (e.g. to poke at loaded data):

```bash
uv run olap docker clickhouse tpc_h start --scale-factor 10
uv run olap docker clickhouse tpc_h stop --scale-factor 10
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
(system, db, db_version, suite, suite_scale_factor, operation, started_at) are
replaced, new runs are added, everything else is kept.

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
uv run olap publish --revision cloud-c7i-tpcds-sf1 --merge

git add site/public/data/results.db site/public/data/manifest.json site/public/data/queries.json
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
