from __future__ import annotations

from collections.abc import Generator, Mapping
from contextlib import contextmanager
from datetime import datetime
from typing import Any, cast

import polars as pl
import pytest
from sqlalchemy import Connection

from ..dbs import Database
from ..settings import DatabaseName, TableName
from ..suites.time_series.config import MutateStep, TimeSeries


class FakeMutationDB(Database):
    name: DatabaseName = "timescaledb"
    version: str = "test"
    connection_string: str = "dummy://"

    disabled_steps: set[str] = set()
    executed_steps: list[tuple[str, int, str | None]] = []
    skipped_steps: list[tuple[str, int, str, str]] = []
    inserted_tables: list[str] = []
    upserted_tables: list[str] = []
    deleted_tables: list[str] = []
    query_iterations: list[tuple[str, int, str]] = []
    rollback_calls: int = 0

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

    def execute_query_iteration(
        self,
        query_name: str,
        iteration: int,
        query: str,
        fetch_kwargs: Mapping[str, Any] | None = None,
    ) -> tuple[pl.DataFrame, float]:
        _ = fetch_kwargs
        self.query_iterations.append((query_name, iteration, query))
        return pl.DataFrame({"value": [1]}), 0.001

    def get_table_names(self) -> set[TableName]:
        raise NotImplementedError

    def is_mutation_step_enabled(self, suite: str, step_name: str) -> bool:
        assert suite == "time_series"
        return step_name not in self.disabled_steps

    @contextmanager
    def mutation_context(self, query_name: str, iteration: int, table_name: str | None = None) -> Generator[None]:
        self.executed_steps.append((query_name, iteration, table_name))
        yield

    def record_skipped_mutation_step(self, query_name: str, iteration: int, table_name: str, reason: str) -> None:
        self.skipped_steps.append((query_name, iteration, table_name, reason))

    def rollback(self) -> None:
        self.rollback_calls += 1

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        self.inserted_tables.append(table)

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        self.upserted_tables.append(table)

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        self.deleted_tables.append(table)


def _fake_insert_data(self: TimeSeries[Any], step: MutateStep, seed: int) -> pl.DataFrame:
    _ = (self, step, seed)
    return pl.DataFrame({"time": [datetime(2025, 1, 1)]})


def _fake_delete_keys(self: TimeSeries[Any], step: MutateStep, seed: int) -> pl.DataFrame:
    _ = (self, step, seed)
    return pl.DataFrame({"time": [datetime(2025, 1, 1)]})


def _fake_load_time_series_query(self: TimeSeries[Any], query_name: str) -> str:
    _ = self
    return f"select '{query_name}'"


def _same_concurrent_worker_suite(self: TimeSeries[Any]) -> TimeSeries[Any]:
    return self


@pytest.mark.parametrize("disabled_step", ["insert_data_large_1", "insert_data_wide_1"])
def test_time_series_mutate_skips_disabled_steps(
    monkeypatch: pytest.MonkeyPatch,
    disabled_step: str,
) -> None:
    db = FakeMutationDB(
        disabled_steps={disabled_step},
        executed_steps=[],
        skipped_steps=[],
        inserted_tables=[],
        upserted_tables=[],
        deleted_tables=[],
        query_iterations=[],
    )
    suite = cast(TimeSeries[FakeMutationDB], TimeSeries.model_construct(db=db, name="time_series", scale_factor=1))

    monkeypatch.setattr(
        "olap_benchmarks.suites.time_series.config.TIME_SERIES_MUTATE_STEPS",
        [
            MutateStep(action="insert", table="data_large", row_count=1),
            MutateStep(action="insert", table="data_wide", row_count=1),
            MutateStep(action="delete", table="data_tall", row_count=1),
        ],
    )
    monkeypatch.setattr("olap_benchmarks.suites.time_series.config.MUTATE_ITERATIONS", 1)
    monkeypatch.setattr(TimeSeries, "_generate_insert_data", _fake_insert_data)
    monkeypatch.setattr(TimeSeries, "_generate_delete_keys", _fake_delete_keys)

    suite.mutate()

    executed_query_names = [query_name for query_name, _iteration, _table_name in db.executed_steps]

    assert disabled_step not in executed_query_names
    assert [query_name for query_name, _iteration, _table_name, _reason in db.skipped_steps] == [disabled_step]
    assert len(executed_query_names) == 2
    assert db.deleted_tables == ["data_tall"]

    if disabled_step == "insert_data_large_1":
        assert db.inserted_tables == ["data_wide"]
    else:
        assert db.inserted_tables == ["data_large"]


def test_time_series_concurrent_runs_writer_and_readers(monkeypatch: pytest.MonkeyPatch) -> None:
    db = FakeMutationDB(
        disabled_steps=set(),
        executed_steps=[],
        skipped_steps=[],
        inserted_tables=[],
        upserted_tables=[],
        deleted_tables=[],
        query_iterations=[],
    )
    suite = cast(TimeSeries[FakeMutationDB], TimeSeries.model_construct(db=db, name="time_series", scale_factor=1))

    monkeypatch.setattr("olap_benchmarks.suites.time_series.config.CONCURRENT_QUERY_NAMES", ("large_01_max_time",))
    monkeypatch.setattr("olap_benchmarks.suites.time_series.config.CONCURRENT_READER_CLIENTS", 2)
    monkeypatch.setattr("olap_benchmarks.suites.time_series.config.CONCURRENT_READER_ITERATIONS", 2)
    monkeypatch.setattr("olap_benchmarks.suites.time_series.config.CONCURRENT_WRITER_BATCH_ROWS", 1)
    monkeypatch.setattr("olap_benchmarks.suites.time_series.config.CONCURRENT_WRITER_ITERATIONS", 2)
    monkeypatch.setattr(TimeSeries, "_generate_insert_data", _fake_insert_data)
    monkeypatch.setattr(TimeSeries, "load_time_series_query", _fake_load_time_series_query)
    monkeypatch.setattr(TimeSeries, "_create_concurrent_worker_suite", _same_concurrent_worker_suite)

    suite.concurrent()

    assert db.inserted_tables == ["data_large", "data_large"]
    assert db.deleted_tables == []
    assert [step for step, _iteration, _table in db.executed_steps] == [
        "concurrent_insert_data_large_1",
        "concurrent_insert_data_large_1",
    ]
    assert sorted(db.query_iterations) == [
        ("large_01_max_time", 1, "select 'large_01_max_time'"),
        ("large_01_max_time", 1, "select 'large_01_max_time'"),
        ("large_01_max_time", 2, "select 'large_01_max_time'"),
        ("large_01_max_time", 2, "select 'large_01_max_time'"),
    ]


def test_time_series_mutate_records_remaining_iterations_after_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    db = FakeMutationDB(
        disabled_steps=set(),
        executed_steps=[],
        skipped_steps=[],
        inserted_tables=[],
        upserted_tables=[],
        deleted_tables=[],
        query_iterations=[],
    )
    suite = cast(TimeSeries[FakeMutationDB], TimeSeries.model_construct(db=db, name="time_series", scale_factor=1))
    failed_step = MutateStep(action="insert", table="data_large", row_count=1)
    succeeding_step = MutateStep(action="delete", table="data_tall", row_count=1)

    monkeypatch.setattr(
        "olap_benchmarks.suites.time_series.config.TIME_SERIES_MUTATE_STEPS",
        [failed_step, succeeding_step],
    )
    monkeypatch.setattr("olap_benchmarks.suites.time_series.config.MUTATE_ITERATIONS", 2)
    monkeypatch.setattr(TimeSeries, "_generate_insert_data", _fake_insert_data)
    monkeypatch.setattr(TimeSeries, "_generate_delete_keys", _fake_delete_keys)

    def fail_insert(self: TimeSeries[Any], step: MutateStep, df: pl.DataFrame) -> None:
        _ = (self, step, df)
        raise RuntimeError("insert failed")

    monkeypatch.setattr(TimeSeries, "_apply_insert", fail_insert)

    suite.mutate()

    assert db.executed_steps == [
        (failed_step.name, 1, "data_large"),
        (succeeding_step.name, 1, "data_tall"),
        (succeeding_step.name, 2, "data_tall"),
    ]
    assert db.skipped_steps == [
        (
            failed_step.name,
            2,
            "data_large",
            "mutation aborted after RuntimeError: insert failed",
        )
    ]
    assert db.deleted_tables == ["data_tall", "data_tall"]
    assert db.rollback_calls == 1


def test_time_series_concurrent_skips_disabled_writer(monkeypatch: pytest.MonkeyPatch) -> None:
    db = FakeMutationDB(
        disabled_steps={"insert_data_large_1"},
        executed_steps=[],
        skipped_steps=[],
        inserted_tables=[],
        upserted_tables=[],
        deleted_tables=[],
        query_iterations=[],
    )
    suite = cast(TimeSeries[FakeMutationDB], TimeSeries.model_construct(db=db, name="time_series", scale_factor=1))

    monkeypatch.setattr("olap_benchmarks.suites.time_series.config.CONCURRENT_QUERY_NAMES", ())
    monkeypatch.setattr("olap_benchmarks.suites.time_series.config.CONCURRENT_READER_CLIENTS", 1)
    monkeypatch.setattr("olap_benchmarks.suites.time_series.config.CONCURRENT_WRITER_BATCH_ROWS", 1)
    monkeypatch.setattr("olap_benchmarks.suites.time_series.config.CONCURRENT_WRITER_ITERATIONS", 2)
    monkeypatch.setattr(TimeSeries, "_create_concurrent_worker_suite", _same_concurrent_worker_suite)

    suite.concurrent()

    assert db.inserted_tables == []
    assert db.executed_steps == []
    assert [step for step, _iteration, _table, _reason in db.skipped_steps] == [
        "concurrent_insert_data_large_1",
        "concurrent_insert_data_large_1",
    ]
