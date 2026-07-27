from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime

import duckdb
import pytest

from olap_benchmarks.suites.jsonbench.config import JSONBENCH_QUERIES_DIRECTORY


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
    query = (JSONBENCH_QUERIES_DIRECTORY / f"{query_name}.sql").read_text()
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
