import polars as pl

from ..settings import TableName


def insert(df: pl.DataFrame, table: TableName) -> None:
    raise NotImplementedError


def upsert(df: pl.DataFrame, table: TableName) -> None:
    raise NotImplementedError
