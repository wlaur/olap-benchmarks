from typing import cast

import polars as pl
import pymonetdb  # type: ignore[import-untyped]
from pymonetdb import Connection as MonetDBConnection
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
    "varchar": pl.String,
}


def get_polars_type(type_code: str) -> pl.DataType | type[pl.DataType]:
    try:
        return MONETDB_POLARS_TYPE_MAP[type_code]
    except KeyError:
        raise ValueError(f"Unknown type code: '{type_code}'") from None


UPLOAD_DOWNLOAD_DIRECTORY = SETTINGS.temporary_directory / "monetdb"


def ensure_downloader_uploader(connection: MonetDBConnection) -> None:
    UPLOAD_DOWNLOAD_DIRECTORY.mkdir(exist_ok=True)

    if connection.mapi.downloader is not None and connection.mapi.uploader is not None:
        return

    transfer_handler = pymonetdb.SafeDirectoryHandler(UPLOAD_DOWNLOAD_DIRECTORY)
    connection.set_downloader(transfer_handler)
    connection.set_uploader(transfer_handler)


def get_pymonetdb_connection(connection: Connection) -> MonetDBConnection:
    return cast(MonetDBConnection, connection._dbapi_connection)
