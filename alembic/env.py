from __future__ import annotations

import logging
from logging.config import fileConfig
from pathlib import Path
from typing import Any, cast

from alembic.ddl import impl
from alembic.ddl.postgresql import PostgresqlImpl
from alembic.runtime.migration import MigrationContext
from sqlalchemy import String, Text, create_engine, inspect, pool
from sqlalchemy.engine import Connection

from alembic import context
from olap_benchmarks.results import get_results_db_path
from olap_benchmarks.results.duckdb_sqlalchemy import patch_duckdb_sqlalchemy_compat
from olap_benchmarks.results.models import Base

_LOGGER = logging.getLogger(__name__)

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = Base.metadata
_existing_tables_for_autogen: set[str] = set()

impl._impls["duckdb"] = PostgresqlImpl

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def _database_url() -> str:
    db_path_attr = config.attributes.get("db_path")
    if db_path_attr is not None:
        path = Path(str(db_path_attr)).expanduser().resolve()
        return f"duckdb:///{path}"

    x_args = context.get_x_argument(as_dictionary=True)
    db_path = x_args.get("db")
    path = Path(db_path).expanduser().resolve() if db_path else get_results_db_path()

    return f"duckdb:///{path}"


def _include_object(
    object_: object,
    name: str | None,
    type_: str,
    reflected: bool,
    compare_to: object | None,
) -> bool:
    del name, compare_to

    if type_ != "index":
        return True

    if reflected:
        return False

    table = getattr(object_, "table", None)
    table_name = getattr(table, "name", None)

    if isinstance(table_name, str):
        return table_name not in _existing_tables_for_autogen

    return False


def _compare_type(
    context_: MigrationContext,
    inspected_column: object,
    metadata_column: object,
    inspected_type: object,
    metadata_type: object,
) -> bool | None:
    del context_, inspected_column, metadata_column

    if isinstance(inspected_type, String) and isinstance(metadata_type, Text):
        return False

    if isinstance(inspected_type, Text) and isinstance(metadata_type, String):
        return False

    return None


def _patch_duckdb_reflection_for_autogenerate() -> None:
    try:
        import duckdb_engine
    except Exception:
        return

    dialect_cls = getattr(duckdb_engine, "Dialect", None)

    if dialect_cls is None:
        return

    if cast(bool, getattr(dialect_cls, "_olap_alembic_reflection_patched", False)):
        return

    original_get_multi_columns = getattr(dialect_cls, "get_multi_columns", None)

    if not callable(original_get_multi_columns):
        return

    def _get_multi_columns_with_fallback(
        self: object,
        connection: Connection,
        schema: str | None = None,
        filter_names: list[str] | set[str] | None = None,
        scope: object | None = None,
        kind: object | None = None,
        **kw: object,
    ) -> object:
        try:
            return original_get_multi_columns(
                self,
                connection,
                schema=schema,
                filter_names=filter_names,
                scope=scope,
                kind=kind,
                **kw,
            )
        except Exception as exc:
            if "pg_collation" not in str(exc):
                raise

            if filter_names is None:
                table_names = [
                    str(row[0])
                    for row in connection.exec_driver_sql(
                        "select table_name from information_schema.tables "
                        "where table_schema = current_schema() and table_type = 'BASE TABLE'"
                    ).fetchall()
                ]
            else:
                table_names = [str(table_name) for table_name in filter_names]

            reflected_rows: list[dict[str, object]] = []

            for table_name in table_names:
                escaped_table_name = table_name.replace("'", "''")
                pragma_sql = f"pragma table_info('{escaped_table_name}')"

                try:
                    pragma_rows = connection.exec_driver_sql(pragma_sql).mappings().all()
                except Exception:
                    _LOGGER.debug(f"Skipping {table_name}: pragma table_info failed", exc_info=True)
                    continue

                for row in pragma_rows:
                    reflected_rows.append(
                        {
                            "name": row["name"],
                            "table_name": table_name,
                            "format_type": row["type"] or "varchar",
                            "default": row["dflt_value"],
                            "not_null": bool(row["notnull"]),
                            "comment": None,
                            "generated": None,
                            "identity_options": None,
                            "collation": None,
                        }
                    )

            columns = cast(Any, self)._get_columns_info(reflected_rows, domains={}, enums={}, schema=schema)
            return columns.items()

    dialect_cls.get_multi_columns = _get_multi_columns_with_fallback
    dialect_cls._olap_alembic_reflection_patched = True


def run_migrations_offline() -> None:
    """Run migrations in offline mode."""
    url = _database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=_compare_type,
        compare_server_default=True,
        include_object=_include_object,
        transaction_per_migration=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in online mode."""
    patch_duckdb_sqlalchemy_compat()
    _patch_duckdb_reflection_for_autogenerate()
    connectable = create_engine(
        _database_url(),
        connect_args={"read_only": False},
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        global _existing_tables_for_autogen
        _existing_tables_for_autogen = set(inspect(connection).get_table_names())
        connection.commit()

        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=_compare_type,
            compare_server_default=True,
            include_object=_include_object,
            transaction_per_migration=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
