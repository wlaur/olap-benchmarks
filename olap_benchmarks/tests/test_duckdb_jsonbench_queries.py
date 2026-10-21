from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime

import duckdb
import pytest

from olap_benchmarks.dbs.duckdb import DuckDB


@pytest.fixture
def duckdb_jsonbench() -> Iterator[duckdb.DuckDBPyConnection]:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'Europe/Helsinki'")
    con.execute("CREATE TABLE bluesky (j JSON)")
    rows = [
        {
            "did": "u1",
            "kind": "commit",
            "time_us": 1_700_000_000_000_900,
            "commit": {"operation": "create", "collection": "app.bsky.feed.post"},
        },
        {
            "did": "u1",
            "kind": "commit",
            "time_us": 1_700_003_600_000_100,
            "commit": {"operation": "create", "collection": "app.bsky.feed.post"},
        },
        {
            "did": "u2",
            "kind": "commit",
            "time_us": 1_700_000_600_000_000,
            "commit": {"operation": "create", "collection": "app.bsky.feed.like"},
        },
        {
            "did": "u3",
            "kind": "commit",
            "time_us": 1_700_007_200_000_000,
            "commit": {"operation": "delete", "collection": "app.bsky.feed.post"},
        },
    ]
    con.executemany("INSERT INTO bluesky VALUES (?)", [(json.dumps(row),) for row in rows])
    yield con
    con.close()


def run_query(con: duckdb.DuckDBPyConnection, query_name: str) -> list[tuple[object, ...]]:
    suite = DuckDB().benchmarks["jsonbench"]
    query = suite.load_query(query_name)
    return con.execute(query).fetchall()


def test_duckdb_jsonbench_json_filters(duckdb_jsonbench: duckdb.DuckDBPyConnection) -> None:
    assert run_query(duckdb_jsonbench, "02_create_events_by_collection") == [
        ("app.bsky.feed.post", 2, 1),
        ("app.bsky.feed.like", 1, 1),
    ]


def test_duckdb_jsonbench_uses_utc_millisecond_boundaries(
    duckdb_jsonbench: duckdb.DuckDBPyConnection,
) -> None:
    assert run_query(duckdb_jsonbench, "03_create_events_by_hour") == [
        ("app.bsky.feed.like", 22, 1),
        ("app.bsky.feed.post", 22, 1),
        ("app.bsky.feed.post", 23, 1),
    ]
    assert run_query(duckdb_jsonbench, "04_first_post_users") == [("u1", datetime(2023, 11, 14, 22, 13, 20))]
    assert run_query(duckdb_jsonbench, "05_longest_post_activity") == [("u1", 3_600_000)]


@pytest.mark.parametrize("query_name", ["02_create_events_by_collection", "03_create_events_by_hour"])
def test_duckdb_jsonbench_resolves_native_list_extraction(query_name: str) -> None:
    suite = DuckDB().benchmarks["jsonbench"]
    assert suite.query_path(query_name) == suite.queries_directory / "duckdb" / f"{query_name}.sql"


@pytest.mark.parametrize("query_name", ["02_create_events_by_collection", "03_create_events_by_hour"])
@pytest.mark.parametrize("timezone", ["UTC", "Europe/Helsinki"])
@pytest.mark.parametrize("empty", [False, True])
def test_duckdb_jsonbench_extraction_preserves_answers(query_name: str, timezone: str, empty: bool) -> None:
    suite = DuckDB().benchmarks["jsonbench"]
    original = (suite.queries_directory / f"{query_name}.sql").read_text()
    candidate = suite.load_query(query_name)
    with duckdb.connect() as con:
        con.execute("SET TimeZone = ?", [timezone])
        con.execute("CREATE TABLE bluesky (j JSON)")
        if not empty:
            rows: list[str | None] = [None, "null", "[]", "{}"]
            for did in (None, "null", 17, "17", True, {"b": 2, "a": 1}, ["x", None], 'a\\b\n"雪'):
                for time_us in (None, -1, "10000", 1_700_000_000_000_900):
                    row = json.dumps(
                        {
                            "did": did,
                            "kind": "commit",
                            "time_us": time_us,
                            "commit": {"operation": "create", "collection": "app.bsky.feed.post"},
                        }
                    )
                    rows.extend((row, row))
            rows.extend(
                json.dumps({"kind": kind, "time_us": "invalid", "commit": {"operation": operation}})
                for kind, operation in (("identity", "create"), ("commit", "delete"))
            )
            con.executemany("INSERT INTO bluesky VALUES (?)", [(row,) for row in rows])
        assert con.sql("DESCRIBE " + original).fetchall() == con.sql("DESCRIBE " + candidate).fetchall()
        expected = con.sql(original).pl()
        actual = con.sql(candidate).pl()
        assert actual.schema == expected.schema
        assert actual.equals(expected, null_equal=True)


def test_duckdb_jsonbench_preserves_qualifying_timestamp_error() -> None:
    suite = DuckDB().benchmarks["jsonbench"]
    name = "03_create_events_by_hour"
    original = (suite.queries_directory / f"{name}.sql").read_text()
    with duckdb.connect() as con:
        con.execute("CREATE TABLE bluesky (j JSON)")
        con.execute(
            "INSERT INTO bluesky VALUES (?)",
            [
                json.dumps(
                    {
                        "kind": "commit",
                        "time_us": "invalid",
                        "commit": {"operation": "create", "collection": "app.bsky.feed.post"},
                    }
                )
            ],
        )
        for query in (original, suite.load_query(name)):
            with pytest.raises(duckdb.ConversionException, match="invalid"):
                con.sql(query).pl()
