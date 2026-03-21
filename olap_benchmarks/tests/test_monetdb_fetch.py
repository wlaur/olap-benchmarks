from __future__ import annotations

from typing import Any

from sqlalchemy.sql.elements import TextClause

from ..dbs.monetdb.fetch import _escape_literal_colons_for_sqlalchemy_text, fetch_pymonetdb


def test_escape_literal_colons_for_sqlalchemy_text_escapes_regex_group_syntax_only() -> None:
    query = (
        "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') "
        "FROM hits WHERE CounterID = :counter_id"
    )

    escaped = _escape_literal_colons_for_sqlalchemy_text(query)

    assert escaped == (
        "SELECT REGEXP_REPLACE(Referer, '^https?://(?\\:www\\.)?([^/]+)/.*$', '\\1') "
        "FROM hits WHERE CounterID = :counter_id"
    )


class _StubResult:
    def keys(self) -> list[str]:
        return ["value"]

    def fetchall(self) -> list[tuple[str]]:
        return [("ok",)]


class _StubConnection:
    def __init__(self) -> None:
        self.info: dict[str, Any] = {}
        self.executed_statement: TextClause | None = None

    def execute(self, statement: TextClause) -> _StubResult:
        self.executed_statement = statement
        return _StubResult()


def test_fetch_pymonetdb_preserves_regex_literals_without_creating_bind_params() -> None:
    connection = _StubConnection()
    query = "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS value"

    result = fetch_pymonetdb(query, connection)  # pyright: ignore[reportArgumentType]

    assert connection.executed_statement is not None
    assert list(connection.executed_statement._bindparams) == []
    assert str(connection.executed_statement) == query
    assert result.to_dicts() == [{"value": "ok"}]
