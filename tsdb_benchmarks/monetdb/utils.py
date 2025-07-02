import re
from typing import cast

import numpy as np
import polars as pl
import pymonetdb  # type: ignore[import-untyped]
from pydantic import BaseModel
from pymonetdb import Connection as MonetDBConnection
from pymonetdb.sql.cursors import Description  # type: ignore[import-untyped]
from sqlalchemy import Connection, text

from ..settings import SETTINGS, TableName

BOOLEAN_NULL = 128

# NOTE: the order matters, see get_monetdb_type
MONETDB_POLARS_TYPE_MAP: dict[str, pl.DataType | type[pl.DataType]] = {
    "tinyint": pl.Int8,
    "smallint": pl.Int16,
    "int": pl.Int32,
    "bigint": pl.Int64,
    "hugeint": pl.Int128,
    "blob": pl.Binary,
    "real": pl.Float32,
    "double": pl.Float64,
    "boolean": pl.Boolean,
    "timestamp": pl.Datetime("ms"),
    "timestamptz": pl.Datetime("ms"),  # tz info is not stored in binary data, this will be dumped as UTC
    "time": pl.Time,
    "date": pl.Date,
    "timetz": pl.Time,
    "varchar": pl.String,
    "char": pl.String,
    "json": pl.Object,  # would be better to use pl.String and convert to pl.Struct as needed
    "sec_interval": pl.Int64,  # int64 ms
    "day_interval": pl.Int64,  # int64 ms
    "month_interval": pl.Int32,
}

POLARS_NUMPY_STRUCT_PACKING_CODE_MAP: dict[pl.DataType | type[pl.DataType], str] = {
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
}

POLARS_NUMPY_TYPE_MAP: dict[pl.DataType | type[pl.DataType], type] = {
    pl.Boolean: np.uint8,
    pl.Int8: np.int8,
    pl.Int16: np.int16,
    pl.Int32: np.int32,
    pl.Int64: np.int64,
    pl.UInt8: np.uint8,
    pl.UInt16: np.uint16,
    pl.UInt32: np.uint32,
    pl.UInt64: np.uint64,
    pl.Float32: np.float32,
    pl.Float64: np.float64,
}

MONETDB_TEMPORARY_DIRECTORY = SETTINGS.temporary_directory / "monetdb"


class SchemaMeta(BaseModel):
    size: int | None = None
    tz: str | None = None


def get_schema_meta(description: Description) -> SchemaMeta:
    meta = SchemaMeta()

    if description.type_code == "varchar" and description.internal_size > 0:
        meta.size = description.internal_size

    return meta


def get_polars_type(type_code: str) -> pl.DataType | type[pl.DataType]:
    try:
        return MONETDB_POLARS_TYPE_MAP[type_code]
    except KeyError:
        raise ValueError(f"Unknown type code: '{type_code}'") from None


def get_monetdb_type(dtype: pl.DataType | type[pl.DataType]) -> str:
    for k, v in MONETDB_POLARS_TYPE_MAP.items():
        if dtype == v:
            return k

    raise ValueError(f"Unsupported type: {dtype}")


def get_limit_query(query: str) -> str:
    query = query.rstrip().rstrip(";")
    limit_regex = re.compile(r"\s+limit\s+\d+\s*$", re.IGNORECASE)
    query = re.sub(limit_regex, "", query)
    return f"{query} limit 1"


def ensure_downloader_uploader(connection: MonetDBConnection) -> None:
    MONETDB_TEMPORARY_DIRECTORY.mkdir(exist_ok=True)

    if connection.mapi.downloader is not None and connection.mapi.uploader is not None:
        return

    transfer_handler = pymonetdb.SafeDirectoryHandler(MONETDB_TEMPORARY_DIRECTORY)
    connection.set_downloader(transfer_handler)
    connection.set_uploader(transfer_handler)


def get_pymonetdb_connection(connection: Connection) -> MonetDBConnection:
    return cast(MonetDBConnection, connection._dbapi_connection)


def create_table_if_not_exists(
    table: TableName,
    df: pl.DataFrame,
    connection: Connection,
    primary_key: str | tuple[str, ...] | None = None,
    json_columns: list[str] | str | None = None,
) -> None:
    col_defs: list[str] = []

    if json_columns is None:
        json_columns = []

    if isinstance(json_columns, str):
        json_columns = [json_columns]

    for name, dtype in zip(df.columns, df.dtypes, strict=True):
        sql_type = "json" if name in json_columns else get_monetdb_type(dtype)

        col_defs.append(f"{name} {sql_type}")

    if primary_key:
        if isinstance(primary_key, str):
            primary_key = (primary_key,)

        pk = ", ".join(f'"{n}"' for n in primary_key)
        col_defs.append(f"primary key ({pk})")

    col_def_clause = ", ".join(col_defs)
    sql = f'create table if not exists "{table}" ({col_def_clause})'
    connection.execute(text(sql))
    connection.commit()


def drop_table(table: TableName, connection: Connection) -> None:
    connection.execute(text(f'drop table if exists "{table}"'))
    connection.commit()
