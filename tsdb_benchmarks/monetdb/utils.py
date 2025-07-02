import re
from typing import cast

import polars as pl
import pymonetdb  # type: ignore[import-untyped]
from pydantic import BaseModel
from pymonetdb import Connection as MonetDBConnection
from pymonetdb.sql.cursors import Description  # type: ignore[import-untyped]
from sqlalchemy import Connection

from ..settings import SETTINGS

BOOLEAN_TRUE = 128

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
    "timestamptz": pl.Datetime("ms"),  # tz info is not stored in binary data, this will be dumped as UTC
    "timetz": pl.Time,
    "varchar": pl.String,
    "json": pl.String,
    "sec_interval": pl.Int64,  # int64 ms
    "day_interval": pl.Int64,  # int64 ms
    "month_interval": pl.Int32,
}


UPLOAD_DOWNLOAD_DIRECTORY = SETTINGS.temporary_directory / "monetdb"


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


def get_limit_query(query: str) -> str:
    query = query.rstrip().rstrip(";")
    limit_regex = re.compile(r"\s+limit\s+\d+\s*$", re.IGNORECASE)
    query = re.sub(limit_regex, "", query)
    return f"{query} limit 1"


def ensure_downloader_uploader(connection: MonetDBConnection) -> None:
    UPLOAD_DOWNLOAD_DIRECTORY.mkdir(exist_ok=True)

    if connection.mapi.downloader is not None and connection.mapi.uploader is not None:
        return

    transfer_handler = pymonetdb.SafeDirectoryHandler(UPLOAD_DOWNLOAD_DIRECTORY)
    connection.set_downloader(transfer_handler)
    connection.set_uploader(transfer_handler)


def get_pymonetdb_connection(connection: Connection) -> MonetDBConnection:
    return cast(MonetDBConnection, connection._dbapi_connection)
