from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import duckdb
import pytest
from polars.testing import assert_frame_equal

from ..dbs.clickhouse import Clickhouse
from ..dbs.duckdb import DuckDB
from ..suites.json_representation.config import (
    JSON_SETTINGS,
    QUERY_NAMES,
    SCHEMAS_DIRECTORY,
    JSONRepresentation,
    prepared_projection,
)
from ..suites.jsonbench.config import JSONBENCH_QUERY_NAMES


def fixture_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = [{}, {"did": None, "commit": None}, {"commit": {"collection": ""}}]
    for did, timestamps in (
        ("lexical", ["9000", "10000"]),
        (None, [None]),
        ('u\\\n"雪', ["1000", "2000"]),
        ("null", ["1000000"]),
        ("negative", ["-1001", "-2001"]),
    ):
        rows.extend(
            {
                "kind": "commit",
                "did": did,
                "time_us": timestamp,
                "commit": {"operation": "create", "collection": "app.bsky.feed.post"},
            }
            for timestamp in timestamps
        )
    rows.extend(
        [
            {"kind": "identity", "time_us": "invalid"},
            {"kind": "commit", "did": None, "commit": {"operation": "create", "collection": None}},
            {"kind": "commit", "did": "", "commit": {"operation": "create", "collection": ""}},
        ]
    )
    return rows


@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("timezone", ["UTC", "Europe/Helsinki"])
def test_all_tracks_match_canonical_rows_and_schema(empty: bool, timezone: str) -> None:
    suite = JSONRepresentation(db=DuckDB(), scale_factor=10)
    with duckdb.connect() as con:
        con.execute("SET TimeZone = ?", [timezone])
        con.execute((SCHEMAS_DIRECTORY / "duckdb.sql").read_text())
        con.execute("CREATE VIEW bluesky AS SELECT CAST(j AS JSON) AS j FROM json_raw")
        if not empty:
            con.executemany("INSERT INTO json_raw VALUES (?)", [(json.dumps(row),) for row in fixture_rows()])
        con.execute("INSERT INTO json_prepared " + prepared_projection("duckdb"))
        for name in JSONBENCH_QUERY_NAMES:
            canonical = (Path(__file__).parents[1] / "suites/jsonbench/queries" / f"{name}.sql").read_text()
            expected = con.sql(canonical).pl()
            for track in ("raw", "prepared"):
                query = suite.load_query(f"{track}_{name}")
                actual = con.sql(query).pl()
                assert_frame_equal(actual, expected, check_exact=True)
                assert con.sql("DESCRIBE " + query).fetchall() == con.sql("DESCRIBE " + canonical).fetchall()


def test_timestamp_extrema_remain_lexical_and_distinct_excludes_null() -> None:
    suite = JSONRepresentation(db=DuckDB(), scale_factor=10)
    with duckdb.connect() as con:
        con.execute((SCHEMAS_DIRECTORY / "duckdb.sql").read_text())
        selected = [row for row in fixture_rows() if row.get("did") in ("lexical", None)]
        con.executemany("INSERT INTO json_raw VALUES (?)", [(json.dumps(row),) for row in selected])
        con.execute("INSERT INTO json_prepared " + prepared_projection("duckdb"))
        for track in ("raw", "prepared"):
            first = con.sql(suite.load_query(f"{track}_04_first_post_users")).fetchall()
            assert first == [("lexical", datetime(1970, 1, 1, 0, 0, 0, 10000)), (None, None)]
            longest = con.sql(suite.load_query(f"{track}_05_longest_post_activity")).fetchall()
            assert longest == [("lexical", -1), (None, None)]
            collection = con.sql(suite.load_query(f"{track}_02_create_events_by_collection")).fetchall()
            assert ("app.bsky.feed.post", 3, 1) in collection


def test_clickhouse_queries_preserve_nullable_extraction_exact_counts_and_lexical_extrema() -> None:
    suite = JSONRepresentation(db=Clickhouse(), scale_factor=10)
    assert len(QUERY_NAMES) == 10
    assert JSON_SETTINGS["function_json_value_return_type_allow_nullable"] == 1
    for name in QUERY_NAMES:
        query = suite.load_query(name)
        assert suite.query_path(name) == suite.queries_directory / "clickhouse" / f"{name}.sql"
        assert "JSON_VALUE" in query if name.startswith("raw_") else "JSON_VALUE" not in query
        if name.startswith("raw_"):
            assert "JSONType" in query
        if "_02_" in name:
            assert "uniqExact(did)" in query
        if "_04_" in name or "_05_" in name:
            assert "min(time_us_text)" in query
            assert "min(time_us)" not in query


def test_prepared_projection_extracts_fields_without_aggregation() -> None:
    for engine in ("duckdb", "clickhouse"):
        projection = prepared_projection(engine)
        assert "FROM json_raw" in projection
        assert "time_us_text" in projection
        assert all(keyword not in projection.upper() for keyword in ("GROUP BY", "COUNT(", "MIN(", "MAX(", "LIMIT"))
