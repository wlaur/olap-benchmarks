import re
from collections.abc import Mapping
from typing import cast

import numpy as np
import polars as pl
import pymonetdb  # type: ignore[import-untyped]
from pydantic import BaseModel
from pymonetdb import Connection as MonetDBConnection
from pymonetdb.sql.cursors import Description  # type: ignore[import-untyped]
from sqlalchemy import (
    Column,
    Connection,
    MetaData,
    Table,
    text,
)
from sqlalchemy.types import UserDefinedType

from ..settings import SETTINGS, TableName
from .settings import SETTINGS as MONETDB_SETTINGS

# MonetDB exports booleans as uint8, 128 means null (0 is false and 1 is true)
BOOLEAN_NULL = 128


MONETDB_DATETIME_RECORD_TYPE = np.dtype(
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

MONETDB_TIME_RECORD_TYPE = np.dtype(
    [
        ("ms", "<u4"),
        ("seconds", "u1"),
        ("minutes", "u1"),
        ("hours", "u1"),
        ("padding", "u1"),
    ]
)


MONETDB_DATE_RECORD_TYPE = np.dtype(
    [
        ("day", "u1"),
        ("month", "u1"),
        ("year", "<i2"),
    ]
)

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


class MonetDBType(UserDefinedType):
    def __init__(self, type_name: str):
        self.type_name = type_name

    def get_col_spec(self, **kwargs):
        return self.type_name


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
    if not MONETDB_SETTINGS.client_file_transfer:
        return

    if connection.mapi.downloader is not None and connection.mapi.uploader is not None:
        return

    MONETDB_TEMPORARY_DIRECTORY.mkdir(exist_ok=True, parents=True)

    transfer_handler = pymonetdb.SafeDirectoryHandler(MONETDB_TEMPORARY_DIRECTORY)
    connection.set_downloader(transfer_handler)
    connection.set_uploader(transfer_handler)


def get_pymonetdb_connection(connection: Connection) -> MonetDBConnection:
    return cast(MonetDBConnection, connection._dbapi_connection)


def get_table(
    table: TableName,
    schema: Mapping[str, pl.DataType | type[pl.DataType]],
    metadata: MetaData | None = None,
    primary_key: str | list[str] | None = None,
    json_columns: str | list[str] | None = None,
) -> Table:
    if json_columns is None:
        json_columns = []

    if isinstance(json_columns, str):
        json_columns = [json_columns]

    if primary_key is None:
        primary_key = []

    if isinstance(primary_key, str):
        primary_key = [primary_key]

    if metadata is None:
        metadata = MetaData()

    columns: list[Column] = []

    for name, dtype in schema.items():
        # SQLAlchemy does not have all types that exist in MonetDB (e.g. tinyint)
        # can use custom types instead, this causes issues if accessing data via the ORM but this is not done here
        col_type_name = "json" if name in json_columns else get_monetdb_type(dtype)

        columns.append(
            Column(
                name=name,
                type_=MonetDBType(col_type_name),
                primary_key=name in primary_key,
            )
        )

    return Table(table, metadata, *columns)


def create_table(
    table: TableName,
    schema: Mapping[str, pl.DataType | type[pl.DataType]],
    connection: Connection,
    primary_key: str | list[str] | None = None,
    json_columns: str | list[str] | None = None,
) -> None:
    metadata = MetaData()
    tbl = get_table(
        table=table,
        schema=schema,
        metadata=metadata,
        primary_key=primary_key,
        json_columns=json_columns,
    )

    metadata.create_all(connection, tables=[tbl], checkfirst=False)


def drop_table(table: TableName, connection: Connection) -> None:
    connection.execute(text(f'drop table if exists "{table}"'))
    connection.commit()
