# olap-benchmarks

> OLAP database benchmarks

[Results](https://wlaur.github.io/olap-benchmarks/)

## Structure

This repo is divided by database and benchmark suite.

## Running locally

Requires Python + uv and Docker.

Install the necessary Python dependencies

```bash
uv sync
```

Install zsh completions once:

```bash
uv run olap --install-completion --shell zsh && exec zsh
```

Run it again if the CLI structure changes and you want the generated completions refreshed.

## Results schema migrations

Install dev dependencies (includes Alembic):

```bash
uv sync --group dev
```

Create a new migration from SQLAlchemy models:

```bash
uv run --group dev alembic revision --autogenerate -m "describe_change"
```

Apply migrations to the default results database from `.env`:

```bash
uv run --group dev alembic upgrade head
```

Apply migrations to a named results revision through the app CLI:

```bash
uv run olap results migrate --revision default
```

Apply migrations to an explicit database path:

```bash
uv run --group dev alembic -x db=/absolute/path/to/results.db upgrade head
```

## Attribution

- **ClickBench** suite is based on [ClickBench](https://github.com/ClickHouse/ClickBench) by ClickHouse
- **RTABench** suite is based on [RTABench](https://github.com/timescale/rtabench) by Timescale
- **Kaggle Airbnb** suite is based on ["Testing query speed for DuckDB vs ClickHouse vs StarRocks databases"](https://medium.com/@marvin_data/testing-query-speed-for-duckdb-vs-clickhouse-vs-starrocks-databases-fecc6614d1ef) by Vitaliy

## TODO

- Additional step for time series suite with mutate+select
    - Concurrently: insert one row as quickly as possible to large table + run a small number of selects against this table (multiple clients)
    - Simulates actual workloads (single writer + multiple readers)
    - What should this operation be named?
- StarRocks: not comparable to the other engines on the current Apple Silicon
  / OrbStack-without-Rosetta system.
    - StarRocks 4.x BE requires AVX2; OrbStack's QEMU-based amd64 emulation
      only exposes sse4_1/sse4_2, so the BE SIGSEGVs on startup under
      `--platform linux/amd64` (the platform the rest of the suite uses).
    - As a workaround, `dbs/starrocks/__init__.py` runs the container under
      `--platform linux/arm64` (native), which is faster than what the other
      six engines get and is therefore an unfair advantage for StarRocks
      results from this system. It also crashes on the current 4.1.0 arm64
      image with no useful diagnostics in be.out, so this is not currently
      working at all.
    - Fix path when the bench is rerun on a real x86_64 Linux host (or with
      OrbStack's "Use Rosetta to run amd64 containers" turned on so AVX2 is
      exposed to amd64 containers): switch the platform flag back to
      `--platform linux/amd64` and drop this note. Until then,
      StarRocks numbers from the M-series MacBook should be excluded from
      cross-engine comparisons.
