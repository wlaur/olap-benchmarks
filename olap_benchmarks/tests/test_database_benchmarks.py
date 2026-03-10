from __future__ import annotations

from collections.abc import Mapping
from typing import get_args

import polars as pl
from sqlalchemy import Connection

from ..dbs import Database
from ..settings import DatabaseName, SuiteName, TableName


class DummyDatabase(Database):
    name: DatabaseName = "duckdb"
    version: str = "test"
    connection_string: str = "dummy://"

    @property
    def start(self) -> None:
        return None

    def connect(self, reconnect: bool = False) -> Connection:
        raise NotImplementedError

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
    ) -> pl.DataFrame:
        raise NotImplementedError

    def insert(
        self,
        df: pl.DataFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        raise NotImplementedError

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        raise NotImplementedError


def test_database_benchmarks_resolves_all_suites() -> None:
    db = DummyDatabase()

    benchmarks = db.benchmarks

    assert set(benchmarks) == set(get_args(SuiteName))
    assert benchmarks["time_series"].db is db
