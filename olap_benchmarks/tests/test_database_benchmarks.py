from __future__ import annotations

from collections.abc import Mapping
from typing import Any, get_args

import polars as pl
import pytest
from sqlalchemy import Connection

from ..dbs import Database
from ..settings import DatabaseName, SuiteName, TableName
from ..suites import BenchmarkSuite
from ..suites.time_series.config import TimeSeries


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

    def get_table_names(self) -> set[TableName]:
        raise NotImplementedError

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        raise NotImplementedError

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        raise NotImplementedError

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        raise NotImplementedError


class CountingDatabase(DummyDatabase):
    row_counts: dict[str, int] = {}
    row_count_errors: dict[str, str] = {}
    table_names: set[str] = set()
    rollback_calls: int = 0

    def get_row_count(self, table: TableName) -> int:
        if table in self.row_count_errors:
            raise RuntimeError(self.row_count_errors[table])

        return self.row_counts[table]

    def get_table_names(self) -> set[TableName]:
        return set(self.table_names)

    def rollback(self) -> None:
        self.rollback_calls += 1


class DummySuite(BenchmarkSuite[CountingDatabase]):
    name: SuiteName = "clickbench"

    def expected_table_row_counts(self) -> dict[TableName, int]:
        return {"hits": 10, "events": 20}

    def populate(self) -> None:
        raise NotImplementedError

    def select(self) -> None:
        raise NotImplementedError


class SkipPopulateSuite(BenchmarkSuite["SkipPopulateDatabase"]):
    name: SuiteName = "clickbench"

    def should_populate(self) -> bool:
        return False

    def populate(self) -> None:
        raise AssertionError("populate should not run when should_populate is false")

    def select(self) -> None:
        raise NotImplementedError


class SkipPopulateDatabase(DummyDatabase):
    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        return {"clickbench": SkipPopulateSuite}


def test_suite_supported_operations_defaults_to_populate_and_select() -> None:
    assert DummySuite.supported_operations == ("populate", "select")


def test_time_series_suite_declares_mutate_support() -> None:
    assert TimeSeries.supported_operations == ("populate", "mutate", "select")


def test_database_benchmarks_resolves_all_suites() -> None:
    db = DummyDatabase()

    benchmarks = db.benchmarks

    assert set(benchmarks) == set(get_args(SuiteName))
    assert benchmarks["time_series"].db is db
    assert benchmarks["time_series"].scale_factor == 1
    assert benchmarks["tpc_h"].scale_factor == 10
    assert benchmarks["tpc_ds"].scale_factor == 1


def test_database_benchmark_rejects_unsupported_operation() -> None:
    db = DummyDatabase()

    with pytest.raises(ValueError, match="does not support operation 'mutate'"):
        db.benchmark("clickbench", "mutate")


def test_database_benchmark_does_not_record_skipped_populate() -> None:
    db = SkipPopulateDatabase()

    db.benchmark("clickbench", "populate")

    assert db._run_id is None


def test_suite_should_skip_populate_when_expected_data_exists() -> None:
    suite = DummySuite(db=CountingDatabase(row_counts={"hits": 10, "events": 20}))

    assert suite.should_populate() is False


def test_suite_should_populate_when_expected_tables_are_missing() -> None:
    db = CountingDatabase(row_count_errors={"hits": "missing", "events": "missing"})
    suite = DummySuite(db=db)

    assert suite.should_populate() is True
    assert db.rollback_calls == 2


def test_suite_should_fail_when_expected_tables_are_missing_but_db_is_not_empty() -> None:
    suite = DummySuite(
        db=CountingDatabase(
            row_count_errors={"hits": "missing", "events": "missing"},
            table_names={"legacy_table"},
        )
    )

    with pytest.raises(RuntimeError, match="database is not empty"):
        suite.should_populate()


def test_suite_should_fail_when_existing_data_is_inconsistent() -> None:
    suite = DummySuite(db=CountingDatabase(row_counts={"hits": 10}, row_count_errors={"events": "missing"}))

    with pytest.raises(RuntimeError, match="existing data does not match expected row counts"):
        suite.should_populate()


def test_suite_verify_populated_data_requires_expected_counts() -> None:
    suite = DummySuite(db=CountingDatabase(row_counts={"hits": 10, "events": 19}))

    with pytest.raises(RuntimeError, match="Populated data verification failed"):
        suite.verify_populated_data()
