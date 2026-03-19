from __future__ import annotations

from collections.abc import Iterator, Mapping
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
    inserted_tables: list[str] = []
    upserted_tables: list[str] = []
    deleted_tables: list[str] = []

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

    def is_mutation_step_enabled(self, suite: str, step_name: str) -> bool:
        assert suite == "time_series"
        return step_name not in self.disabled_steps

    @contextmanager
    def mutation_context(self, query_name: str, iteration: int, table_name: str | None = None) -> Iterator[None]:
        self.executed_steps.append((query_name, iteration, table_name))
        yield

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


@pytest.mark.parametrize("disabled_step", ["insert_data_large_1", "insert_data_wide_eav_1"])
def test_time_series_mutate_skips_disabled_steps(
    monkeypatch: pytest.MonkeyPatch,
    disabled_step: str,
) -> None:
    db = FakeMutationDB(
        disabled_steps={disabled_step},
        executed_steps=[],
        inserted_tables=[],
        upserted_tables=[],
        deleted_tables=[],
    )
    suite = cast(TimeSeries[FakeMutationDB], TimeSeries.model_construct(db=db, name="time_series"))

    monkeypatch.setattr(
        "olap_benchmarks.suites.time_series.config.TIME_SERIES_MUTATE_STEPS",
        [
            MutateStep(action="insert", table="data_large", row_count=1),
            MutateStep(action="insert", table="data_wide_eav", row_count=1),
            MutateStep(action="delete", table="data_tall", row_count=1),
        ],
    )
    monkeypatch.setattr("olap_benchmarks.suites.time_series.config.MUTATE_ITERATIONS", 1)
    monkeypatch.setattr(TimeSeries, "_generate_insert_data", _fake_insert_data)
    monkeypatch.setattr(TimeSeries, "_generate_delete_keys", _fake_delete_keys)

    suite.mutate()

    executed_query_names = [query_name for query_name, _iteration, _table_name in db.executed_steps]

    assert disabled_step not in executed_query_names
    assert len(executed_query_names) == 2
    assert db.deleted_tables == ["data_tall"]

    if disabled_step == "insert_data_large_1":
        assert db.inserted_tables == ["data_wide_eav"]
    else:
        assert db.inserted_tables == ["data_large"]
