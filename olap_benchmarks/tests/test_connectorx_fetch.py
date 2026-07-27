from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import cast

import polars as pl
import pytest
from sqlalchemy import Connection

from ..dbs.doris import Doris
from ..dbs.postgres import POSTGRES_CONNECTION_STRING, Postgres
from ..dbs.starrocks import StarRocks


def test_postgres_fetch_uses_connectorx_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str]] = []
    recorded_queries: list[str] = []

    class RecordingPostgres(Postgres):
        @contextmanager
        def record_query_execution(self, query: str) -> Generator[None]:
            recorded_queries.append(query)
            yield

    def fake_read_database_uri(query: str, uri: str) -> pl.DataFrame:
        calls.append((query, uri))
        return pl.DataFrame({"one": [1]})

    monkeypatch.setattr(pl, "read_database_uri", fake_read_database_uri)

    df = RecordingPostgres().fetch("select 1 as one;", schema={"one": pl.Int64})

    assert df.to_dict(as_series=False) == {"one": [1]}
    assert calls == [("select 1 as one", POSTGRES_CONNECTION_STRING)]
    assert recorded_queries == ["select 1 as one"]


def test_starrocks_fetch_uses_python_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    executed_queries: list[str] = []
    recorded_queries: list[str] = []

    class RecordingStarRocks(StarRocks):
        @contextmanager
        def record_query_execution(self, query: str) -> Generator[None]:
            recorded_queries.append(query)
            yield

    class FakeResult:
        def keys(self) -> list[str]:
            return ["one"]

        def fetchall(self) -> list[tuple[int]]:
            return [(1,)]

    class FakeConnection:
        def execute(self, query: object) -> FakeResult:
            executed_queries.append(str(query))
            return FakeResult()

    def fake_connect(self: StarRocks, reconnect: bool = False) -> Connection:
        _ = self, reconnect
        return cast(Connection, FakeConnection())

    monkeypatch.setattr(RecordingStarRocks, "connect", fake_connect)

    df = RecordingStarRocks().fetch("select 1 as one;", schema={"one": pl.Int64})

    assert df.to_dict(as_series=False) == {"one": [1]}
    assert executed_queries == ["select 1 as one"]
    assert recorded_queries == ["select 1 as one;"]


def test_doris_fetch_uses_connectorx_mysql_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str]] = []
    recorded_queries: list[str] = []

    class RecordingDoris(Doris):
        @contextmanager
        def record_query_execution(self, query: str) -> Generator[None]:
            recorded_queries.append(query)
            yield

    def fake_read_database_uri(query: str, uri: str) -> pl.DataFrame:
        calls.append((query, uri))
        return pl.DataFrame({"one": [1]})

    monkeypatch.setattr(pl, "read_database_uri", fake_read_database_uri)

    df = RecordingDoris().fetch("select 1 as one;", schema={"one": pl.Int64}, method="connectorx")

    assert df.to_dict(as_series=False) == {"one": [1]}
    assert calls == [("select 1 as one", "mysql://root@localhost:9030/benchmark")]
    assert recorded_queries == ["select 1 as one"]
