from collections.abc import Mapping
from typing import Any

import polars as pl
from sqlalchemy import (
    Column,
    Connection,
    MetaData,
    Table,
)
from sqlalchemy.schema import CreateTable
from sqlalchemy.types import UserDefinedType

from ...settings import TableName
from ..utils import record_query_execution_context, tracked_commit

# NOTE: don't change this, possible that MonetDB assumed ms in some places
PL_DEFAULT_DATETIME = pl.Datetime("ms")

MONETDB_DEFAULT_DECIMAL_PRECISION = 18
MONETDB_DEFAULT_DECIMAL_SCALE = 3

# Polars dtype -> MonetDB type name, used to build DDL. This is a one-way lookup: reads go
# through ADBC and take their dtype from the Arrow schema the driver reports, never from here.
# One entry per dtype, so the lookup cannot depend on ordering.
MONETDB_TYPE_NAMES: dict[str, pl.DataType | type[pl.DataType]] = {
    "tinyint": pl.Int8,
    "smallint": pl.Int16,
    "int": pl.Int32,
    "bigint": pl.Int64,
    "hugeint": pl.Int128,
    "blob": pl.Binary,
    "real": pl.Float32,
    "float": pl.Float64,
    "boolean": pl.Boolean,
    "timestamp": PL_DEFAULT_DATETIME,
    "time": pl.Time,
    "date": pl.Date,
    "varchar": pl.String,
}

assert len({str(dtype) for dtype in MONETDB_TYPE_NAMES.values()}) == len(MONETDB_TYPE_NAMES), (
    "MONETDB_TYPE_NAMES maps two names to the same Polars dtype; the second would be unreachable"
)


class MonetDBType(UserDefinedType[Any]):
    def __init__(self, type_name: str) -> None:
        self.type_name = type_name

    def get_col_spec(self, **kwargs: object) -> str:
        return self.type_name


def get_monetdb_type(dtype: pl.DataType | type[pl.DataType]) -> str:
    if isinstance(dtype, pl.Decimal):
        return f"decimal({dtype.precision or MONETDB_DEFAULT_DECIMAL_PRECISION},{dtype.scale})"

    if dtype == pl.Decimal:
        return f"decimal({MONETDB_DEFAULT_DECIMAL_PRECISION},{MONETDB_DEFAULT_DECIMAL_SCALE})"

    if dtype == pl.Struct or dtype == pl.Object:
        return "json"

    # map unsigned integer to their signed counterparts
    # an unsigned int will always fit in the signed counterpart
    if dtype == pl.UInt8:
        dtype = pl.Int8

    if dtype == pl.UInt16:
        dtype = pl.Int16

    if dtype == pl.UInt32:
        dtype = pl.Int32

    if dtype == pl.UInt64:
        dtype = pl.Int64

    for k, v in MONETDB_TYPE_NAMES.items():
        if dtype == v:
            return k

    raise ValueError(f"Could not determine MonetDB type for Polars type: {dtype}")


def get_table(
    table: TableName,
    schema: Mapping[str, pl.DataType | type[pl.DataType]],
    metadata: MetaData | None = None,
    primary_key: str | list[str] | None = None,
    not_null: str | list[str] | None = None,
    prefixes: list[str] | None = None,
) -> Table:
    if not_null is None:
        not_null = []

    if isinstance(not_null, str):
        not_null = [not_null]

    if primary_key is None:
        primary_key = []

    if isinstance(primary_key, str):
        primary_key = [primary_key]

    if metadata is None:
        metadata = MetaData()

    columns: list[Column[Any]] = []

    for name, dtype in schema.items():
        # SQLAlchemy does not have all types that exist in MonetDB (e.g. tinyint)
        # can use custom types instead, this causes issues if accessing data via the ORM but this is not done here
        col_type_name = get_monetdb_type(dtype)

        columns.append(
            Column(
                name=name,
                type_=MonetDBType(col_type_name),
                primary_key=name in primary_key,
                nullable=name not in not_null,
            )
        )

    return Table(table, metadata, *columns, prefixes=prefixes)


def create_table(
    table: TableName,
    schema: Mapping[str, pl.DataType | type[pl.DataType]],
    connection: Connection,
    primary_key: str | list[str] | None = None,
    not_null: str | list[str] | None = None,
    temporary: bool = False,
    commit: bool = False,
) -> Table:
    metadata = MetaData()
    tbl = get_table(
        table=table,
        schema=schema,
        metadata=metadata,
        primary_key=primary_key,
        not_null=not_null,
        prefixes=["local", "temporary"] if temporary else None,
    )

    create_query = str(CreateTable(tbl).compile(connection))
    with record_query_execution_context(create_query, connection):
        metadata.create_all(connection, tables=[tbl], checkfirst=False)

    if commit:
        tracked_commit(connection)

    return tbl
