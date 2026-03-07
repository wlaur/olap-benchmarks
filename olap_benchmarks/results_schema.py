from __future__ import annotations

from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy.schema import DropTable

from .results_models import LEGACY_TABLES, Base, ResultsMeta

SCHEMA_VERSION = 3

RESULT_TABLES = [
    "run",
    "run_step",
    "run_metric",
    "debug",
    "results_meta",
]


def _existing_tables(engine: Engine) -> set[str]:
    inspector = inspect(engine)
    return set(inspector.get_table_names())


def _drop_schema_objects(engine: Engine) -> None:
    with engine.begin() as connection:
        # drop known v2/v3 tables in FK-safe reverse dependency order
        for table in reversed(Base.metadata.sorted_tables):
            connection.execute(DropTable(table, if_exists=True))

        for seq_name in ["seq_run", "seq_run_step", "seq_run_metric", "seq_debug", "seq_benchmark"]:
            connection.exec_driver_sql(f"drop sequence if exists {seq_name}")

        for table_name in LEGACY_TABLES:
            connection.exec_driver_sql(f'drop table if exists "{table_name}"')


def _create_schema(engine: Engine) -> None:
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        session.merge(ResultsMeta(key="schema_version", value=str(SCHEMA_VERSION)))
        session.commit()


def _get_schema_version(engine: Engine) -> int | None:
    existing_tables = _existing_tables(engine)

    if "results_meta" not in existing_tables:
        return None

    with Session(engine) as session:
        value = session.get(ResultsMeta, "schema_version")

        if value is None:
            return None

        try:
            return int(value.value)
        except ValueError:
            return None


def ensure_results_schema(
    engine: Engine,
    reset_if_mismatch: bool = True,
    allow_create: bool = True,
) -> None:
    existing_tables = _existing_tables(engine)
    schema_version = _get_schema_version(engine)

    has_legacy_tables = bool(existing_tables.intersection(LEGACY_TABLES))

    if has_legacy_tables:
        if reset_if_mismatch and allow_create:
            _drop_schema_objects(engine)
            _create_schema(engine)
            return

        raise RuntimeError(
            "Legacy results schema detected. Re-run benchmarks with the new runner "
            "or open the database in write mode and run schema initialization."
        )

    if schema_version is None:
        if not allow_create:
            raise RuntimeError(
                f"Results schema is not initialized. Run a benchmark first to create schema version {SCHEMA_VERSION}."
            )

        _create_schema(engine)
        return

    if schema_version == SCHEMA_VERSION:
        return

    if not reset_if_mismatch:
        raise RuntimeError(
            f"Results schema version mismatch: found {schema_version}, expected {SCHEMA_VERSION}. "
            "Set reset_if_mismatch=True to recreate the schema."
        )

    _drop_schema_objects(engine)
    _create_schema(engine)
