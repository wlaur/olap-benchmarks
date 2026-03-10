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

Apply migrations to an explicit database path:

```bash
uv run --group dev alembic -x db=/absolute/path/to/results.db upgrade head
```
