import shutil
import uuid
from pathlib import Path

import polars as pl
from sqlalchemy import Connection

from ..settings import TableName
from .binary import (
    write_blob_column,
    write_boolean_column,
    write_date_column,
    write_datetime_column,
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
)


def write_binary_column_data(series: pl.Series, path: Path) -> None:
    dtype = series.dtype

    match dtype:
        case pl.Boolean:
            write_boolean_column(series, path)
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
        ):
            write_numeric_column(series, path)
        case pl.String:
            write_string_column(series, path)
        case pl.Date:
            write_date_column(series, path)
        case pl.Time:
            write_time_column(series, path)
        case pl.Datetime:
            write_datetime_column(series, path)
        case pl.Binary:
            write_blob_column(series, path)
        case _:
            raise ValueError(f"Unsupported Polars dtype for binary export: {dtype}, {series.name=}")


def insert(
    df: pl.DataFrame,
    table: TableName,
    connection: Connection,
    primary_key: str | list[str] | None = None,
    json_columns: str | list[str] | None = None,
) -> None:
    # NOTE: when inserting into an existing table, the column order and types must match exactly
    create_table(table, df.schema, connection, primary_key, json_columns)

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
        con.commit()

    finally:
        shutil.rmtree(temp_dir)


def upsert(
    df: pl.DataFrame, table: TableName, connection: Connection, primary_key: str | list[str] | None = None
) -> None:
    # insert into unlogged temp table, use merge statement to update target
    raise NotImplementedError
