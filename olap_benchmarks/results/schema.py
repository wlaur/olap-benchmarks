from __future__ import annotations

from pathlib import Path

from sqlalchemy import inspect
from sqlalchemy.engine import Engine

RESULT_TABLES = [
    "run",
    "run_step",
    "run_metric",
    "query_execution",
    "debug",
]


def _get_db_path(engine: Engine) -> Path:
    database = engine.url.database
    if database is None:
        raise RuntimeError("Results engine does not expose a database path")

    return Path(database).expanduser().resolve()


def _existing_tables(engine: Engine) -> set[str]:
    inspector = inspect(engine)
    return set(inspector.get_table_names())


def _get_alembic_revision(engine: Engine) -> str | None:
    if "alembic_version" not in _existing_tables(engine):
        return None

    with engine.begin() as connection:
        value = connection.exec_driver_sql("select version_num from alembic_version").scalar_one_or_none()

    return None if value is None else str(value)


def ensure_results_schema(engine: Engine, allow_create: bool = True) -> None:
    existing_tables = _existing_tables(engine)
    alembic_revision = _get_alembic_revision(engine)
    from . import get_results_head_revision, migrate_results

    head_revision = get_results_head_revision()

    if allow_create:
        migrate_results(db_path=_get_db_path(engine))
        existing_tables = _existing_tables(engine)
        alembic_revision = _get_alembic_revision(engine)

    if alembic_revision != head_revision:
        raise RuntimeError(
            "Results schema is inconsistent with the current Alembic head. "
            "Run `olap results migrate` or update `alembic_version` manually."
        )

    missing_tables = set(RESULT_TABLES).difference(existing_tables)
    if missing_tables:
        raise RuntimeError(f"Results schema is missing expected tables: {', '.join(sorted(missing_tables))}.")
