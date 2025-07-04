import shutil
import uuid
from pathlib import Path

import numpy as np
import polars as pl
from sqlalchemy import Connection

from ..settings import TableName
from .settings import SETTINGS as MONETDB_SETTINGS
from .utils import (
    MONETDB_DATE_RECORD_TYPE,
    MONETDB_DATETIME_RECORD_TYPE,
    MONETDB_TEMPORARY_DIRECTORY,
    MONETDB_TIME_RECORD_TYPE,
    POLARS_NUMPY_TYPE_MAP,
    create_table,
    ensure_downloader_uploader,
    get_pymonetdb_connection,
)

BLOB_NULL_MARKER = (0xFFFFFFFFFFFFFFFF).to_bytes(8, byteorder="little")
STRING_NULL_MARKER = b"\x80\x00"

DATETIME_NULL_RECORD = {
    "ms": 0xFFFFFFFF,
    "seconds": 255,
    "minutes": 255,
    "hours": 255,
    "padding": 255,
    "day": 255,
    "month": 255,
    "year": -1,
}

DATE_NULL_RECORD = {
    "day": 255,
    "month": 255,
    "year": -1,
}

TIME_NULL_RECORD = {
    "ms": 0xFFFFFFFF,
    "seconds": 255,
    "minutes": 255,
    "hours": 255,
    "padding": 255,
}


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
    buffer = bytearray()

    for val in series:
        if val is None:
            buffer.extend(STRING_NULL_MARKER)
        else:
            buffer.extend(val.encode("utf-8"))
            buffer.append(0)

    path.write_bytes(buffer)


def write_date_column(series: pl.Series, path: Path) -> None:
    null_mask = series.is_null().to_numpy()
    n = len(series)
    data = np.zeros(n, dtype=MONETDB_DATE_RECORD_TYPE)

    for k, v in DATE_NULL_RECORD.items():
        data[k][null_mask] = v

    valid_mask = ~null_mask

    if valid_mask.any():
        parts = (
            series.to_frame("dt")
            .with_columns(
                day=pl.col("dt").dt.day(),
                month=pl.col("dt").dt.month(),
                year=pl.col("dt").dt.year(),
            )
            .filter(pl.col("dt").is_not_null())
            .drop("dt")
        )
        parts_np = parts.to_numpy()

        data["day"][valid_mask] = parts_np[:, 0]
        data["month"][valid_mask] = parts_np[:, 1]
        data["year"][valid_mask] = parts_np[:, 2]

    path.write_bytes(data.tobytes())


def write_time_column(series: pl.Series, path: Path) -> None:
    null_mask = series.is_null().to_numpy()
    n = len(series)
    data = np.zeros(n, dtype=MONETDB_TIME_RECORD_TYPE)

    for k, v in TIME_NULL_RECORD.items():
        data[k][null_mask] = v

    valid_mask = ~null_mask
    if valid_mask.any():
        parts = (
            series.to_frame("dt")
            .with_columns(
                ms=pl.col("dt").dt.millisecond(),
                seconds=pl.col("dt").dt.second(),
                minutes=pl.col("dt").dt.minute(),
                hours=pl.col("dt").dt.hour(),
            )
            .filter(pl.col("dt").is_not_null())
            .drop("dt")
        )
        parts_np = parts.to_numpy()

        data["ms"][valid_mask] = parts_np[:, 0]
        data["seconds"][valid_mask] = parts_np[:, 1]
        data["minutes"][valid_mask] = parts_np[:, 2]
        data["hours"][valid_mask] = parts_np[:, 3]
        data["padding"][valid_mask] = 0

    path.write_bytes(data.tobytes())


def write_datetime_column(series: pl.Series, path: Path) -> None:
    series = series.cast(pl.Datetime("ms"))
    null_mask = series.is_null().to_numpy()
    n = len(series)

    data = np.zeros(n, dtype=MONETDB_DATETIME_RECORD_TYPE)
    for k, v in DATETIME_NULL_RECORD.items():
        data[k][null_mask] = v

    valid_mask = ~null_mask
    if valid_mask.any():
        parts = (
            series.to_frame("dt")
            .with_columns(
                ms=pl.col("dt").dt.millisecond(),
                seconds=pl.col("dt").dt.second(),
                minutes=pl.col("dt").dt.minute(),
                hours=pl.col("dt").dt.hour(),
                day=pl.col("dt").dt.day(),
                month=pl.col("dt").dt.month(),
                year=pl.col("dt").dt.year(),
            )
            .filter(pl.col("dt").is_not_null())
            .drop("dt")
        )

        parts_np = parts.to_numpy()

        data["ms"][valid_mask] = parts_np[:, 0]
        data["seconds"][valid_mask] = parts_np[:, 1]
        data["minutes"][valid_mask] = parts_np[:, 2]
        data["hours"][valid_mask] = parts_np[:, 3]
        data["padding"][valid_mask] = 0
        data["day"][valid_mask] = parts_np[:, 4]
        data["month"][valid_mask] = parts_np[:, 5]
        data["year"][valid_mask] = parts_np[:, 6]

    path.write_bytes(data.tobytes())


def write_blob_column(series: pl.Series, path: Path) -> None:
    buffer = bytearray()

    for val in series:
        if val is None:
            buffer += BLOB_NULL_MARKER
        else:
            length = len(val)
            buffer += length.to_bytes(8, byteorder="little")
            buffer += val

    path.write_bytes(buffer)


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
