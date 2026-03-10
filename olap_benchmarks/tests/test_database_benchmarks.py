from __future__ import annotations

from collections.abc import Mapping
from typing import get_args

import polars as pl
import pytest
from sqlalchemy import Connection

from ..dbs import Database
from ..settings import DatabaseName, SuiteName, TableName
from ..suites import BenchmarkSuite


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


class CountingDatabase(DummyDatabase):
    row_counts: dict[str, int] = {}
    row_count_errors: dict[str, str] = {}

    def get_row_count(self, table: TableName) -> int:
        if table in self.row_count_errors:
            raise RuntimeError(self.row_count_errors[table])

        return self.row_counts[table]


class DummySuite(BenchmarkSuite[CountingDatabase]):
    name: SuiteName = "clickbench"

    def expected_table_row_counts(self) -> dict[TableName, int]:
        return {"hits": 10, "events": 20}

    def populate(self) -> None:
        raise NotImplementedError

    def run(self) -> None:
        raise NotImplementedError


def test_database_benchmarks_resolves_all_suites() -> None:
    db = DummyDatabase()

    benchmarks = db.benchmarks

    assert set(benchmarks) == set(get_args(SuiteName))
    assert benchmarks["time_series"].db is db


def test_suite_should_skip_populate_when_expected_data_exists() -> None:
    suite = DummySuite(db=CountingDatabase(row_counts={"hits": 10, "events": 20}))

    assert suite.should_populate() is False


def test_suite_should_populate_when_expected_tables_are_missing() -> None:
    suite = DummySuite(db=CountingDatabase(row_count_errors={"hits": "missing", "events": "missing"}))

    assert suite.should_populate() is True


def test_suite_should_fail_when_existing_data_is_inconsistent() -> None:
    suite = DummySuite(db=CountingDatabase(row_counts={"hits": 10}, row_count_errors={"events": "missing"}))

    with pytest.raises(RuntimeError, match="existing data does not match expected row counts"):
        suite.should_populate()


def test_suite_verify_populated_data_requires_expected_counts() -> None:
    suite = DummySuite(db=CountingDatabase(row_counts={"hits": 10, "events": 19}))

    with pytest.raises(RuntimeError, match="Populated data verification failed"):
        suite.verify_populated_data()
