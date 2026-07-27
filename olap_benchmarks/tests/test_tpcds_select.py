from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal, cast

import polars as pl
import pytest
from sqlalchemy import Connection

from ..dbs import Database
from ..settings import DatabaseName, TableName
from ..suites.tpc_ds import config as tpcds_config
from ..suites.tpc_ds.config import TpcDs


class FakeTpcDsDatabase(Database):
    name: DatabaseName = "duckdb"
    version: str = "test"
    connection_string: str = "dummy://"

    executed_iterations: list[tuple[str, int]] = []
    skipped_iterations: list[tuple[str, int, str, str]] = []
    fail_query_names: set[str] = set()
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

    def get_table_names(self) -> set[TableName]:
        return set()

    def execute_query_iteration(
        self,
        query_name: str,
        iteration: int,
        query: str,
        fetch_kwargs: Mapping[str, Any] | None = None,
    ) -> tuple[pl.DataFrame, float]:
        _ = (query, fetch_kwargs)
        self.executed_iterations.append((query_name, iteration))
        if query_name in self.fail_query_names:
            raise RuntimeError(f"{query_name} failed")
        return pl.DataFrame({"query_name": [query_name], "iteration": [iteration]}), 0.001

    def record_skipped_query_step(
        self,
        query_name: str,
        iteration: int,
        reason: str,
        result_status: Literal["skipped", "unsupported"] = "skipped",
    ) -> None:
        self.skipped_iterations.append((query_name, iteration, result_status, reason))

    def rollback(self) -> None:
        self.rollback_calls += 1

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


def _suite(db: FakeTpcDsDatabase) -> TpcDs[FakeTpcDsDatabase]:
    return cast(TpcDs[FakeTpcDsDatabase], TpcDs.model_construct(db=db, name="tpc_ds", scale_factor=1))


def _fake_load_tpcds_query(self: TpcDs[FakeTpcDsDatabase], query_name: str) -> str:
    _ = self
    return f"select '{query_name}'"


def _patch_tpcds_queries(monkeypatch: pytest.MonkeyPatch, query_names: list[str], iterations: int = 2) -> None:
    monkeypatch.setattr(tpcds_config, "TPCDS_QUERY_NAMES", query_names)
    monkeypatch.setattr(tpcds_config, "TPCDS_ITERATIONS", iterations)
    monkeypatch.setattr(TpcDs, "load_tpcds_query", _fake_load_tpcds_query)


def test_tpcds_select_records_unsupported_query_steps(monkeypatch: pytest.MonkeyPatch) -> None:
    db = FakeTpcDsDatabase()
    suite = _suite(db)
    _patch_tpcds_queries(monkeypatch, ["01", "02", "03"])
    monkeypatch.setattr(TpcDs, "UNSUPPORTED_QUERIES", {"duckdb": frozenset({"02"})})

    suite.select()

    assert db.executed_iterations == [("01", 1), ("01", 2), ("03", 1), ("03", 2)]
    assert db.skipped_iterations == [
        ("02", 1, "unsupported", "query is unsupported by duckdb"),
        ("02", 2, "unsupported", "query is unsupported by duckdb"),
    ]


def test_tpcds_select_continues_after_query_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    db = FakeTpcDsDatabase(fail_query_names={"01"})
    suite = _suite(db)
    _patch_tpcds_queries(monkeypatch, ["01", "02"])
    monkeypatch.setattr(TpcDs, "UNSUPPORTED_QUERIES", {})

    suite.select()

    assert db.executed_iterations == [("01", 1), ("02", 1), ("02", 2)]
    assert db.skipped_iterations == [("01", 2, "skipped", "query aborted after RuntimeError: 01 failed")]
    assert db.rollback_calls == 1
    assert db.current_query_name is None
