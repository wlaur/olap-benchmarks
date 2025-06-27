import contextlib
import shutil
import uuid
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
import polars as pl
import pymonetdb  # type: ignore[import-untyped]
from pymonetdb import Connection as MonetDBConnection
from sqlalchemy import Connection

from ..settings import SETTINGS

DOWNLOAD_DIRECTORY = SETTINGS.temporary_directory / "monetdb"


def get_pymonetdb_connection(connection: Connection) -> MonetDBConnection:
    return cast(MonetDBConnection, connection._dbapi_connection)


MONETDB_POLARS_TYPE_MAP: dict[str, pl.DataType | type[pl.DataType]] = {
    "tinyint": pl.Int8,
    "smallint": pl.Int16,
    "int": pl.Int32,
    "bigint": pl.Int64,
    "hugeint": pl.Int128,
    "char": pl.String,
    "blob": pl.Binary,
    "real": pl.Float32,
    "double": pl.Float64,
    "decimal": pl.Decimal,
    "boolean": pl.Boolean,
    "timestamp": pl.Datetime("ms"),
    "time": pl.Time,
}


def _get_type(type_code: str) -> pl.DataType | type[pl.DataType]:
    try:
        return MONETDB_POLARS_TYPE_MAP[type_code]
    except KeyError:
        raise ValueError(f"Unknown type code: '{type_code}'") from None


def fetch_pymonetdb(query: str, connection: Connection) -> pl.DataFrame:
    con = get_pymonetdb_connection(connection)
    c = con.cursor()
    c.execute(query)
    ret = c.fetchall()

    description = c.description
    assert description is not None

    return pl.DataFrame(ret, schema={n.name: _get_type(n.type_code) for n in description}, orient="row")


def _ensure_downloader(connection: MonetDBConnection) -> None:
    DOWNLOAD_DIRECTORY.mkdir(exist_ok=True)

    transfer_handler = pymonetdb.SafeDirectoryHandler(DOWNLOAD_DIRECTORY)
    connection.set_downloader(transfer_handler)


def read_timestamp_column(path: Path) -> pl.Series:
    with path.open("rb") as f:
        data = f.read()

    record_dtype = np.dtype(
        [
            ("ms", "<u4"),
            ("seconds", "u1"),
            ("minutes", "u1"),
            ("hours", "u1"),
            ("padding", "u1"),
            ("day", "u1"),
            ("month", "u1"),
            ("year", "<i2"),
        ]
    )

    records = np.frombuffer(data, dtype=record_dtype)

    dt_index = pd.to_datetime(
        {
            "year": records["year"],
            "month": records["month"],
            "day": records["day"],
            "hour": records["hours"],
            "minute": records["minutes"],
            "second": records["seconds"],
            "microsecond": records["ms"] * 1000,
        },
        errors="coerce",
    )

    return pl.Series(dt_index)


def read_column_bin(path: Path, dtype: pl.DataType) -> pl.Series:
    if dtype == pl.Datetime("ms"):
        return read_timestamp_column(path)

    with path.open("rb") as f:
        data = f.read()

    # 128-bit int is not supported
    np_dtype = {
        pl.Boolean: "<u1",
        pl.Int8: "<i1",
        pl.Int16: "<i2",
        pl.Int32: "<i4",
        pl.Int64: "<i8",
        pl.UInt8: "<u1",
        pl.UInt16: "<u2",
        pl.UInt32: "<u4",
        pl.UInt64: "<u8",
        pl.Float32: "<f4",
        pl.Float64: "<f8",
    }.get(dtype)

    if np_dtype is None:
        raise ValueError(f"Unsupported dtype: {dtype}")

    values = np.frombuffer(data, dtype=np_dtype)
    s = pl.Series(values).cast(dtype)

    with contextlib.suppress(Exception):
        s = s.fill_nan(None)

    return s


def fetch_binary(query: str, connection: Connection, schema: dict[str, pl.DataType] | None = None) -> pl.DataFrame:
    con = get_pymonetdb_connection(connection)

    _ensure_downloader(con)

    if schema is None:
        schema = fetch_pymonetdb(f"{query} limit 1", connection).schema

    temp_dir = DOWNLOAD_DIRECTORY / str(uuid.uuid4())[:4]
    temp_dir.mkdir()

    output_files = [temp_dir / f"{idx}.bin" for idx in range(len(schema))]
    output_files_repr = ",".join(f"'{temp_dir.name}/{n.name}'" for n in output_files)

    try:
        con.execute(f"copy {query} into little endian binary {output_files_repr} on client")

        columns = {}
        for (col_name, dtype), path in zip(schema.items(), output_files, strict=True):
            columns[col_name] = read_column_bin(path, dtype)

        df = pl.DataFrame(columns, orient="row")
    finally:
        shutil.rmtree(temp_dir)

    return df
