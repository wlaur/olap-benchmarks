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
)
from sqlalchemy.types import UserDefinedType

from ...settings import SETTINGS, TableName
from .settings import SETTINGS as MONETDB_SETTINGS

# MonetDB exports booleans as uint8, 128 means null (0 is false and 1 is true)
BOOLEAN_NULL = 128

MONETDB_DEFAULT_DECIMAL_PRECISION = 18
MONETDB_DEFAULT_DECIMAL_SCALE = 3

MONETDB_MAX_DECIMAL_PRECISION = 18
MONETDB_MAX_DECIMAL_SCALE = 3

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


JSON_POLARS_DTYPE: type[pl.Object] | type[pl.String] | type[pl.Struct]

match MONETDB_SETTINGS.json_polars_dtype:
    case "object":
        JSON_POLARS_DTYPE = pl.Object
    case "string":
        JSON_POLARS_DTYPE = pl.String
    case "struct":
        JSON_POLARS_DTYPE = pl.Struct
    case _:
        raise ValueError(f"Invalid value for setting 'json_polars_dtype': {MONETDB_SETTINGS.json_polars_dtype}")

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
    "json": JSON_POLARS_DTYPE,
    "sec_interval": pl.Int64,  # int64 ms
    "day_interval": pl.Int64,  # int64 ms
    "month_interval": pl.Int32,
}


POLARS_NUMPY_TYPE_MAP: dict[pl.DataType | type[pl.DataType], type] = {
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
    pl.Boolean: np.uint8,
}


MONETDB_TEMPORARY_DIRECTORY = SETTINGS.temporary_directory / "monetdb"


class MonetDBType(UserDefinedType):
    def __init__(self, type_name: str) -> None:
        self.type_name = type_name

    def get_col_spec(self, **kwargs) -> str:  # noqa: ANN003
        return self.type_name


class SchemaMeta(BaseModel):
    size: int | None = None
    tz: str | None = None

    precision: int | None = None
    scale: int | None = None


def get_schema_meta(description: Description) -> SchemaMeta:
    meta = SchemaMeta(precision=description.precision, scale=description.scale)

    if description.type_code == "varchar" and description.internal_size > 0:
        meta.size = description.internal_size

    return meta


def get_polars_type(type_code: str, precision: int | None, scale: int | None) -> pl.DataType | type[pl.DataType]:
    if type_code == "decimal":
        return pl.Decimal(precision or MONETDB_DEFAULT_DECIMAL_PRECISION, scale=scale or MONETDB_DEFAULT_DECIMAL_SCALE)

    if type_code in MONETDB_POLARS_TYPE_MAP:
        return MONETDB_POLARS_TYPE_MAP[type_code]

    raise ValueError(f"Unknown type code: '{type_code}'") from None


def get_monetdb_type(dtype: pl.DataType | type[pl.DataType]) -> str:
    if isinstance(dtype, pl.Decimal):
        return f"decimal({dtype.precision or MONETDB_DEFAULT_DECIMAL_PRECISION},{dtype.scale})"

    if dtype == pl.Decimal:
        return f"decimal({MONETDB_DEFAULT_DECIMAL_PRECISION},{MONETDB_DEFAULT_DECIMAL_SCALE})"

    if dtype == pl.Struct or dtype == pl.Object:
        return "json"

    for k, v in MONETDB_POLARS_TYPE_MAP.items():
        if dtype == v:
            return k

    raise ValueError(f"Could not determine MonetDB type for Polars type: {dtype}")


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
    prefixes: list[str] | None = None,
) -> Table:
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
        col_type_name = get_monetdb_type(dtype)

        columns.append(
            Column(
                name=name,
                type_=MonetDBType(col_type_name),
                primary_key=name in primary_key,
            )
        )

    return Table(table, metadata, *columns, prefixes=prefixes)


def create_table(
    table: TableName,
    schema: Mapping[str, pl.DataType | type[pl.DataType]],
    connection: Connection,
    primary_key: str | list[str] | None = None,
    temporary: bool = False,
    commit: bool = False,
) -> Table:
    metadata = MetaData()
    tbl = get_table(
        table=table,
        schema=schema,
        metadata=metadata,
        primary_key=primary_key if MONETDB_SETTINGS.use_primary_key else None,
        prefixes=["LOCAL", "TEMPORARY"] if temporary else None,
    )

    metadata.create_all(connection, tables=[tbl], checkfirst=False)

    if commit:
        connection.commit()

    return tbl
