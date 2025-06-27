import polars as pl
from sqlalchemy import Connection

from ..settings import TableName


def insert(df: pl.DataFrame, table: TableName, connection: Connection) -> None:
    raise NotImplementedError


def upsert(df: pl.DataFrame, table: TableName, connection: Connection) -> None:
    raise NotImplementedError
