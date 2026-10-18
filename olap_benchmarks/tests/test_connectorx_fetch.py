from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import cast

import polars as pl
import pytest
from pymysql.constants import FIELD_TYPE
from sqlalchemy import Connection

from ..dbs.doris import Doris
from ..dbs.postgres import Postgres, _postgres_connection_string
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
    assert calls == [("select 1 as one", _postgres_connection_string())]
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


@pytest.mark.parametrize("value", [0.1, None])
@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("override", [False, True])
def test_doris_python_fetch_recovers_float32_before_the_cursor_closes(
    monkeypatch: pytest.MonkeyPatch, value: float | None, empty: bool, override: bool
) -> None:
    class FakeCursor:
        description: tuple[tuple[str, int], ...] = (
            ("single", FIELD_TYPE.FLOAT),
            ("double", FIELD_TYPE.DOUBLE),
            ("count", FIELD_TYPE.LONGLONG),
        )

    class FakeResult:
        cursor: FakeCursor | None = FakeCursor()

        def keys(self) -> list[str]:
            return ["single", "double", "count"]

        def fetchall(self) -> list[tuple[float | None, float, int]]:
            self.cursor = None
            return [] if empty else [(value, 0.1, 7)]

    class FakeConnection:
        def execute(self, _query: object) -> FakeResult:
            return FakeResult()

    def connect(_self: Doris, reconnect: bool = False) -> Connection:
        return cast(Connection, FakeConnection())

    monkeypatch.setattr(Doris, "connect", connect)

    df = Doris().fetch_python("select single, double, count", schema={"single": pl.Float64} if override else None)

    assert df.columns == ["single", "double", "count"]
    assert df.schema["single"] == (pl.Float64 if override else pl.Float32)
    assert df.height == (0 if empty else 1)
    if not empty:
        assert df["single"].item() == (None if value is None else 0.10000000149011612)
        assert df["double"].item() == 0.1
        assert df["count"].item() == 7
