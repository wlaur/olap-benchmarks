from __future__ import annotations

import polars as pl
import pytest

from ..dbs.postgres import POSTGRES_CONNECTION_STRING, Postgres
from ..dbs.starrocks import StarRocks


def test_postgres_fetch_uses_connectorx_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_read_database_uri(query: str, uri: str) -> pl.DataFrame:
        calls.append((query, uri))
        return pl.DataFrame({"one": [1]})

    monkeypatch.setattr(pl, "read_database_uri", fake_read_database_uri)

    df = Postgres().fetch("select 1 as one;", schema={"one": pl.Int64})

    assert df.to_dict(as_series=False) == {"one": [1]}
    assert calls == [("select 1 as one", POSTGRES_CONNECTION_STRING)]


def test_starrocks_fetch_uses_connectorx_mysql_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_read_database_uri(query: str, uri: str) -> pl.DataFrame:
        calls.append((query, uri))
        return pl.DataFrame({"one": [1]})

    monkeypatch.setattr(pl, "read_database_uri", fake_read_database_uri)

    df = StarRocks().fetch("select 1 as one;", schema={"one": pl.Int64})

    assert df.to_dict(as_series=False) == {"one": [1]}
    assert calls == [("select 1 as one", "mysql://root@localhost:9030/benchmark")]
