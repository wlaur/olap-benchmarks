from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import cast

import polars as pl
import pytest
from pymysql.constants import FIELD_TYPE
from sqlalchemy import Connection

from ..dbs.doris import DORIS_CACHE_INIT_SQL, Doris
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

    class FakeCursor:
        description: tuple[tuple[str, int], ...] = (("one", FIELD_TYPE.LONGLONG),)

    class FakeResult:
        cursor: FakeCursor | None = FakeCursor()

        def keys(self) -> list[str]:
            return ["one"]

        def fetchall(self) -> list[tuple[int]]:
            self.cursor = None
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

    def fake_read_database_uri(query: str, uri: str, *, pre_execution_query: list[str]) -> pl.DataFrame:
        assert pre_execution_query == [DORIS_CACHE_INIT_SQL]
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
@pytest.mark.parametrize("database_class", [Doris, StarRocks])
def test_mysql_python_fetch_recovers_float32_before_the_cursor_closes(
    monkeypatch: pytest.MonkeyPatch,
    value: float | None,
    empty: bool,
    override: bool,
    database_class: type[Doris | StarRocks],
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

    def connect(_self: Doris | StarRocks, reconnect: bool = False) -> Connection:
        return cast(Connection, FakeConnection())

    monkeypatch.setattr(database_class, "connect", connect)

    df = database_class().fetch("select single, double, count", schema={"single": pl.Float64} if override else None)

    assert df.columns == ["single", "double", "count"]
    assert df.schema["single"] == (pl.Float64 if override else pl.Float32)
    assert df.height == (0 if empty else 1)
    if not empty:
        assert df["single"].item() == (None if value is None else 0.10000000149011612)
        assert df["double"].item() == 0.1
        assert df["count"].item() == 7


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ([], []),
        ([None, None], [None, None]),
        (
            ["[]", "[null]", "[7,null,11]", "[9007199254740993,9223372036854775807]", None],
            [[], [None], [7, None, 11], [9007199254740993, 9223372036854775807], None],
        ),
    ],
)
def test_starrocks_airbnb_decodes_only_declared_arrays_without_losing_nulls_or_integers(
    monkeypatch: pytest.MonkeyPatch, values: list[str | None], expected: list[list[int | None] | None]
) -> None:
    class FakeCursor:
        description: tuple[tuple[str, int], ...] = (
            ("reviewer_ids", FIELD_TYPE.STRING),
            ("text", FIELD_TYPE.STRING),
        )

    class FakeResult:
        cursor: FakeCursor | None = FakeCursor()

        def keys(self) -> list[str]:
            return ["reviewer_ids", "text"]

        def fetchall(self) -> list[tuple[str | None, str | None]]:
            self.cursor = None
            return [(value, value) for value in values]

    class FakeConnection:
        def execute(self, _query: object) -> FakeResult:
            return FakeResult()

    def connect(_self: StarRocks, reconnect: bool = False) -> Connection:
        return cast(Connection, FakeConnection())

    monkeypatch.setattr(StarRocks, "connect", connect)

    database = StarRocks()
    suite = database.suite_registry()["kaggle_airbnb"](db=database, name="kaggle_airbnb", scale_factor=1)
    df = database.fetch("select reviewer_ids, text", **suite.query_fetch_kwargs("04_join_three_tables_array_agg"))

    assert df.columns == ["reviewer_ids", "text"]
    assert df.schema["reviewer_ids"] == pl.List(pl.Int64)
    assert df["reviewer_ids"].to_list() == expected
    assert df["text"].to_list() == values
    assert suite.query_fetch_kwargs("05_join_three_tables_row_number") == {}
