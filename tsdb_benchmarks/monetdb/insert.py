import shutil
import struct
import uuid
from pathlib import Path

import numpy as np
import polars as pl
from sqlalchemy import Connection

from ..settings import TableName
from .settings import SETTINGS as MONETDB_SETTINGS
from .utils import (
    MONETDB_TEMPORARY_DIRECTORY,
    POLARS_NUMPY_TYPE_MAP,
    create_table,
    ensure_downloader_uploader,
    get_pymonetdb_connection,
)


def write_boolean_column(series: pl.Series, path: Path) -> None:
    encoded = (
        series.to_frame()
        .select(
            pl.when(pl.col(series.name).is_null())
            .then(128)
            .when(pl.col(series.name))
            .then(1)
            .otherwise(0)
            .cast(pl.UInt8)
        )
        .to_series()
    )
    with path.open("wb") as f:
        f.write(encoded.to_numpy().tobytes())


def write_numeric_column(series: pl.Series, path: Path) -> None:
    np_dtype = POLARS_NUMPY_TYPE_MAP[series.dtype]

    values: np.ndarray

    if np.issubdtype(np_dtype, np.integer):
        sentinel = np.iinfo(np_dtype).min
        values = series.fill_null(sentinel).to_numpy().astype(np_dtype)
    elif np.issubdtype(np_dtype, np.floating):
        values = series.fill_null(np.nan).to_numpy().astype(np_dtype)
    else:
        raise ValueError(f"Unsupported numeric type: {series.dtype}")

    with path.open("wb") as f:
        f.write(values.tobytes())


def write_string_column(series: pl.Series, path: Path) -> None:
    with path.open("wb") as f:
        for val in series:
            if val is None:
                f.write(b"\x80\x00")
            else:
                f.write(val.encode("utf-8") + b"\x00")


def write_date_column(series: pl.Series, path: Path) -> None:
    with path.open("wb") as f:
        for val in series:
            if val is None:
                f.write(struct.pack("<BBh", 255, 255, -1))
            else:
                f.write(struct.pack("<BBh", val.day, val.month, val.year))


def write_time_column(series: pl.Series, path: Path) -> None:
    with path.open("wb") as f:
        for val in series:
            if val is None:
                f.write(struct.pack("<I4B", 0xFFFFFFFF, 255, 255, 255, 255))
            else:
                ms = val.microsecond // 1000
                seconds = val.second
                minutes = val.minute
                hours = val.hour
                f.write(struct.pack("<I4B", ms, seconds, minutes, hours, 0))


def write_datetime_column(series: pl.Series, path: Path) -> None:
    with path.open("wb") as f:
        for val in series.cast(pl.Datetime("ms")):
            if val is None:
                f.write(
                    struct.pack(
                        "<I4B2Bh",
                        0xFFFFFFFF,
                        255,
                        255,
                        255,
                        255,
                        255,
                        255,
                        -1,
                    )
                )
            else:
                ms = val.microsecond // 1000
                seconds = val.second
                minutes = val.minute
                hours = val.hour
                day = val.day
                month = val.month
                year = val.year
                f.write(
                    struct.pack(
                        "<I4B2Bh",
                        ms,
                        seconds,
                        minutes,
                        hours,
                        0,
                        day,
                        month,
                        year,
                    )
                )


def write_binary_column(series: pl.Series, path: Path) -> None:
    with path.open("wb") as f:
        for val in series:
            if val is None:
                f.write((0xFFFFFFFFFFFFFFFF).to_bytes(8, byteorder="little"))
            else:
                f.write(len(val).to_bytes(8, byteorder="little"))
                f.write(val)


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
            write_binary_column(series, path)
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

    column_files: list[Path] = []

    path_prefix = "" if MONETDB_SETTINGS.client_file_transfer else "/"

    try:
        for idx, col in enumerate(df.columns):
            path = temp_dir / f"{idx}.bin"
            write_binary_column_data(df[col], path)
            column_files.append(path)

        files_clause = ", ".join(
            f"'{path_prefix}{path.relative_to(MONETDB_TEMPORARY_DIRECTORY).as_posix()}'" for path in column_files
        )

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
