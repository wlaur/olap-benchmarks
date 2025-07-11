import logging
import shutil
import uuid
from pathlib import Path
from textwrap import dedent

import polars as pl
from sqlalchemy import Connection, text

from ..settings import TableName
from .binary import (
    write_blob_column,
    write_date_column,
    write_datetime_column,
    write_decimal_column,
    write_json_column,
    write_numeric_column,
    write_string_column,
    write_time_column,
)
from .settings import SETTINGS as MONETDB_SETTINGS
from .utils import (
    MONETDB_TEMPORARY_DIRECTORY,
    create_table,
    ensure_downloader_uploader,
    get_pymonetdb_connection,
    get_table,
)

_LOGGER = logging.getLogger(__name__)


def write_binary_column_data(series: pl.Series, path: Path) -> None:
    dtype = series.dtype

    match dtype:
        case (
            pl.Int8
            | pl.Int16
            | pl.Int32
            | pl.Int64
            | pl.UInt8
            | pl.UInt16
            | pl.UInt32
            | pl.UInt64
            | pl.Float32
            | pl.Float64
            | pl.Boolean
        ):
            write_numeric_column(series, path)
        case pl.Decimal:
            write_decimal_column(series, path, dtype)
        case pl.Date:
            write_date_column(series, path)
        case pl.Time:
            write_time_column(series, path)
        case pl.Datetime:
            write_datetime_column(series, path)
        case pl.String:
            write_string_column(series, path)
        case pl.Struct | pl.Object:
            write_json_column(series, path)
        case pl.Binary:
            write_blob_column(series, path)
        case _:
            raise ValueError(f"Unsupported Polars dtype for binary export: {dtype}, {series.name=}")


def insert(
    df: pl.DataFrame,
    table: TableName,
    connection: Connection,
    primary_key: str | list[str] | None = None,
    create: bool = True,
    commit: bool = True,
) -> None:
    if create:
        # NOTE: when inserting into an existing table, the column order and types must match exactly
        create_table(table, df.schema, connection, primary_key)
        _LOGGER.info(f"Created table '{table}' with {len(df.columns):_} columns")

    con = get_pymonetdb_connection(connection)
    ensure_downloader_uploader(con)

    temp_dir = MONETDB_TEMPORARY_DIRECTORY / "data" / str(uuid.uuid4())[:4]
    temp_dir.mkdir()

    subdir = temp_dir.relative_to(MONETDB_TEMPORARY_DIRECTORY).as_posix()
    column_files: list[Path] = []
    path_prefix = "" if MONETDB_SETTINGS.client_file_transfer else "/"

    try:
        for idx, col in enumerate(df.columns):
            path = temp_dir / f"{idx}.bin"
            write_binary_column_data(df[col], path)
            column_files.append(path)

        files_clause = ", ".join(f"'{path_prefix}{subdir}/{path.name}'" for path in column_files)
        con.execute(
            f"copy little endian binary into {table} from {files_clause} "
            f"on {'client' if MONETDB_SETTINGS.client_file_transfer else 'server'}"
        )

        if commit:
            con.commit()

    except Exception as e:
        col_indexes = dict(enumerate(df.columns))
        raise ValueError(f"Could not insert binary data for '{table}', columns:\n{col_indexes}\n") from e
    finally:
        shutil.rmtree(temp_dir)


def upsert(
    df: pl.DataFrame, table: TableName, connection: Connection, primary_key: str | list[str] | None = None
) -> None:
    if primary_key is None:
        raise ValueError("primary_key must be provided when upserting data")

    dest = get_table(table, df.schema)

    temp_table_name = f"_temporary_{str(uuid.uuid4())[:4]}"
    source = create_table(temp_table_name, df.schema, connection, primary_key=primary_key, temporary=True)

    insert(df, source.name, connection, create=False, commit=False)

    primary_keys = [primary_key] if isinstance(primary_key, str) else list(primary_key)
    shared_cols = sorted({c.name for c in dest.columns} & {c.name for c in dest.columns})

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

    connection.execute(text(merge_statement))
    connection.commit()
