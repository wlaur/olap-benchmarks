import logging
import shutil
import uuid
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from textwrap import dedent
from time import perf_counter
from typing import Any, cast

import polars as pl
from sqlalchemy import Connection, text

from ...settings import TableName
from ..utils import record_query_execution_context, tracked_commit
from .binary import serialize_binary_column_data, write_binary_column_data
from .settings import SETTINGS as MONETDB_SETTINGS
from .utils import (
    MONETDB_TEMPORARY_DIRECTORY,
    create_table,
    ensure_downloader_uploader,
    get_pymonetdb_connection,
    get_table,
)

_LOGGER = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 500_000
SQL_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
WIDE_COLUMN_GROUP_SIZE = 10
WIDE_SCHEMA_COLUMN_THRESHOLD = 512


@dataclass(frozen=True)
class RowBatchWrite:
    pass


@dataclass(frozen=True)
class ColumnGroupWrite:
    group_size: int = WIDE_COLUMN_GROUP_SIZE


type LazyWrite = RowBatchWrite | ColumnGroupWrite
DEFAULT_LAZY_WRITE = RowBatchWrite()


def staged_write_for_column_count(column_count: int) -> LazyWrite:
    if column_count >= WIDE_SCHEMA_COLUMN_THRESHOLD:
        return ColumnGroupWrite()
    return RowBatchWrite()


def _raise_insert_error(table: TableName, columns: Sequence[str], exc: Exception) -> None:
    col_indexes = dict(enumerate(columns))
    raise ValueError(f"Could not insert binary data for '{table}', columns:\n{col_indexes}\n") from exc


@contextmanager
def _temporary_binary_directory() -> Generator[Path]:
    temp_dir = MONETDB_TEMPORARY_DIRECTORY / "data" / str(uuid.uuid4())[:4]
    temp_dir.mkdir()

    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir)


def _copy_binary_files(
    connection: Connection,
    table: TableName,
    column_files: Sequence[Path],
    subdir: str,
    commit: bool,
) -> None:
    con = get_pymonetdb_connection(connection)
    path_prefix = "" if MONETDB_SETTINGS.client_file_transfer else "/"
    files_clause = ", ".join(f"'{path_prefix}{subdir}/{path.name}'" for path in column_files)
    copy_query = (
        f"copy little endian binary into {table} from {files_clause} "
        f"on {'client' if MONETDB_SETTINGS.client_file_transfer else 'server'}"
    )

    with record_query_execution_context(copy_query, connection):
        cast(Any, con).execute(copy_query)

    if commit:
        tracked_commit(con, recorder_source=connection)


def _write_eager_column_files(df: pl.DataFrame, temp_dir: Path) -> list[Path]:
    column_files: list[Path] = []

    for idx, col in enumerate(df.columns):
        path = temp_dir / f"{idx}.bin"
        write_binary_column_data(df[col], path)
        column_files.append(path)

    return column_files


def _insert_eager_files(
    df: pl.DataFrame,
    table: TableName,
    connection: Connection,
    temp_dir: Path,
    commit: bool,
) -> None:
    column_files = _write_eager_column_files(df, temp_dir)
    subdir = temp_dir.relative_to(MONETDB_TEMPORARY_DIRECTORY).as_posix()
    _copy_binary_files(connection, table, column_files, subdir, commit)


def _write_row_batches(
    df: pl.LazyFrame,
    columns: Sequence[str],
    column_files: Sequence[Path],
    batch_size: int,
) -> int:
    total_rows = 0

    for batch_idx, batch in enumerate(df.collect_batches(chunk_size=batch_size)):
        batch_rows = batch.shape[0]
        total_rows += batch_rows

        for col_name, path in zip(columns, column_files, strict=True):
            with path.open("ab") as fh:
                fh.write(serialize_binary_column_data(batch[col_name]))

        _LOGGER.info(f"Wrote batch {batch_idx + 1:_} ({batch_rows:_} rows, {total_rows:_} total)")

    return total_rows


def _write_column_groups(
    df: pl.LazyFrame,
    columns: Sequence[str],
    column_files: Sequence[Path],
    group_size: int,
) -> int:
    total_rows = 0
    num_groups = (len(columns) + group_size - 1) // group_size

    for group_idx, start in enumerate(range(0, len(columns), group_size), start=1):
        group_columns = list(columns[start : start + group_size])
        group_files = column_files[start : start + group_size]
        group_df = df.select(group_columns).collect()

        if total_rows == 0:
            total_rows = group_df.shape[0]

        for col_name, path in zip(group_columns, group_files, strict=True):
            with path.open("wb") as fh:
                fh.write(serialize_binary_column_data(group_df[col_name]))

        _LOGGER.info(f"Wrote column group {group_idx:_}/{num_groups:_} ({len(group_columns):_} columns)")

    return total_rows


def _write_lazy_column_files(
    df: pl.LazyFrame,
    columns: Sequence[str],
    column_files: Sequence[Path],
    batch_size: int,
    lazy_write: LazyWrite,
) -> int:
    if isinstance(lazy_write, RowBatchWrite):
        return _write_row_batches(df, columns, column_files, batch_size)
    return _write_column_groups(df, columns, column_files, lazy_write.group_size)


def _insert_lazy_files(
    df: pl.LazyFrame,
    table: TableName,
    connection: Connection,
    temp_dir: Path,
    columns: Sequence[str],
    batch_size: int,
    lazy_write: LazyWrite,
    commit: bool,
) -> int:
    column_files = [temp_dir / f"{idx}.bin" for idx in range(len(columns))]
    total_rows = _write_lazy_column_files(df, columns, column_files, batch_size, lazy_write)
    subdir = temp_dir.relative_to(MONETDB_TEMPORARY_DIRECTORY).as_posix()
    _copy_binary_files(connection, table, column_files, subdir, commit)
    return total_rows


def _insert_eager(
    df: pl.DataFrame,
    table: TableName,
    connection: Connection,
    create: bool,
    commit: bool,
    primary_key: str | list[str] | None,
    not_null: str | list[str] | None,
) -> None:
    t0 = perf_counter()
    columns = list(df.columns)

    if create:
        create_table(table, df.schema, connection, primary_key, not_null)
        _LOGGER.info(f"Created table '{table}' with {len(df.columns):_} columns")

    ensure_downloader_uploader(get_pymonetdb_connection(connection))

    with _temporary_binary_directory() as temp_dir:
        try:
            _insert_eager_files(df, table, connection, temp_dir, commit)
        except Exception as exc:
            _raise_insert_error(table, columns, exc)

    _LOGGER.info(
        f"Inserted dataset with shape ({df.shape[0]:_}, {df.shape[1]:_}) "
        f"into table {table} in {perf_counter() - t0:_.2f} seconds"
    )


def _insert_lazy(
    df: pl.LazyFrame,
    table: TableName,
    connection: Connection,
    create: bool,
    commit: bool,
    primary_key: str | list[str] | None,
    not_null: str | list[str] | None,
    batch_size: int,
    lazy_write: LazyWrite,
) -> None:
    t0 = perf_counter()
    schema = df.collect_schema()

    if create:
        create_table(table, schema, connection, primary_key, not_null)
        _LOGGER.info(f"Created table '{table}' with {len(schema):_} columns")

    columns = list(schema.names())
    ensure_downloader_uploader(get_pymonetdb_connection(connection))
    total_rows = 0

    with _temporary_binary_directory() as temp_dir:
        try:
            total_rows = _insert_lazy_files(
                df,
                table,
                connection,
                temp_dir,
                columns,
                batch_size,
                lazy_write,
                commit,
            )
        except Exception as exc:
            _raise_insert_error(table, columns, exc)

    _LOGGER.info(
        f"Inserted {total_rows:_} rows ({len(columns):_} columns) "
        f"into table {table} in {perf_counter() - t0:_.2f} seconds"
    )


def insert(
    df: pl.DataFrame | pl.LazyFrame,
    table: TableName,
    connection: Connection,
    primary_key: str | list[str] | None = None,
    not_null: str | list[str] | None = None,
    create: bool = True,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    lazy_write: LazyWrite = DEFAULT_LAZY_WRITE,
) -> None:
    if isinstance(df, pl.DataFrame):
        _insert_eager(df, table, connection, create, commit, primary_key, not_null)
    else:
        _insert_lazy(
            df,
            table,
            connection,
            create,
            commit,
            primary_key,
            not_null,
            batch_size,
            lazy_write,
        )


def delete(table: TableName, connection: Connection, primary_key: str | list[str], keys: pl.DataFrame) -> None:
    t0 = perf_counter()
    primary_keys = [primary_key] if isinstance(primary_key, str) else list(primary_key)

    def _format_literal(val: object) -> str:
        if isinstance(val, datetime):
            return f"'{val:{SQL_TIMESTAMP_FORMAT}}'"
        if isinstance(val, str):
            return f"'{val}'"
        return str(val)

    if len(primary_keys) == 1:
        pk = primary_keys[0]
        literals = ", ".join(_format_literal(v) for v in keys.get_column(pk).to_list())
        delete_sql = f'DELETE FROM "{table}" WHERE "{pk}" IN ({literals})'
    else:
        rows: list[str] = []
        for row in keys.iter_rows():
            parts = ", ".join(_format_literal(val) for val in row)
            rows.append(f"({parts})")
        pk_cols = ", ".join(f'"{pk}"' for pk in primary_keys)
        delete_sql = f'DELETE FROM "{table}" WHERE ({pk_cols}) IN (VALUES {", ".join(rows)})'

    with record_query_execution_context(delete_sql, connection):
        connection.execute(text(delete_sql))
    tracked_commit(connection)

    _LOGGER.info(f"Deleted from table {table} using {keys.shape[0]:_} key rows in {perf_counter() - t0:_.2f} seconds")


def upsert(df: pl.DataFrame, table: TableName, connection: Connection, primary_key: str | list[str]) -> None:
    t0 = perf_counter()
    dest = get_table(table, df.schema)

    temp_table_name = f"_temporary_{str(uuid.uuid4())[:4]}"
    source = create_table(temp_table_name, df.schema, connection, primary_key=primary_key, temporary=True)

    insert(df, source.name, connection, create=False, commit=False)

    primary_keys = [primary_key] if isinstance(primary_key, str) else list(primary_key)
    shared_cols = sorted({c.name for c in dest.columns} & {c.name for c in source.columns})

    if not shared_cols:
        raise ValueError("No overlapping columns to upsert")

    update_cols = [col for col in shared_cols if col not in primary_keys]

    if not update_cols:
        raise ValueError("No non-PK columns to upsert")

    on_clause = " and ".join(f'dest."{pk}" = source."{pk}"' for pk in primary_keys)
    update_assignments = ", ".join(f'"{col}" = source."{col}"' for col in update_cols)

    insert_cols = ", ".join(f'"{col}"' for col in shared_cols)
    insert_values = ", ".join(f'source."{col}"' for col in shared_cols)

    merge_statement = dedent(f"""
        merge into "{dest.name}" as dest
        using "{source.name}" as source
            on {on_clause}
        when matched then
            update set {update_assignments}
        when not matched then
            insert ({insert_cols}) values ({insert_values})
    """)

    with record_query_execution_context(merge_statement, connection):
        connection.execute(text(merge_statement))
    tracked_commit(connection)

    _LOGGER.info(
        f"Upserted dataset with shape ({df.shape[0]:_}, {df.shape[1]:_}) "
        f"into table {table} in {perf_counter() - t0:_.2f} seconds"
    )
