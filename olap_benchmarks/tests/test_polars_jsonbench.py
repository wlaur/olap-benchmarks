from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from olap_benchmarks.dbs.polars import Polars
from olap_benchmarks.settings import SETTINGS


@pytest.fixture
def polars_jsonbench_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Polars:
    monkeypatch.setattr(SETTINGS, "database_directory", tmp_path)

    db = Polars()
    db._current_suite = "jsonbench"
    db._current_suite_scale_factor = 10
    db.database_directory.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "did": ["u1", "u1", "u2", "u3"],
            "kind": ["commit", "commit", "commit", "commit"],
            "time_us": [1_700_000_000_000_900, 1_700_003_600_000_100, 1_700_000_600_000_000, 1_700_007_200_000_000],
            "commit": [
                {"operation": "create", "collection": "app.bsky.feed.post"},
                {"operation": "create", "collection": "app.bsky.feed.post"},
                {"operation": "create", "collection": "app.bsky.feed.like"},
                {"operation": "delete", "collection": "app.bsky.feed.post"},
            ],
        }
    ).write_parquet(db.table_path("bluesky"))

    return db


def test_polars_jsonbench_row_count(polars_jsonbench_db: Polars) -> None:
    assert polars_jsonbench_db.get_table_names() == {"bluesky"}
    assert polars_jsonbench_db.get_row_count("bluesky") == 4


def test_polars_jsonbench_collection_counts(polars_jsonbench_db: Polars) -> None:
    polars_jsonbench_db.current_query_name = "01_events_by_collection"

    df = polars_jsonbench_db.fetch("")

    assert df.to_dicts() == [
        {"event": "app.bsky.feed.post", "count": 3},
        {"event": "app.bsky.feed.like", "count": 1},
    ]


def test_polars_jsonbench_activity_span(polars_jsonbench_db: Polars) -> None:
    polars_jsonbench_db.current_query_name = "05_longest_post_activity"

    df = polars_jsonbench_db.fetch("")

    assert df.to_dicts() == [{"user_id": "u1", "activity_span": 3_600_000}]


def test_polars_jsonbench_first_post_uses_utc_milliseconds(polars_jsonbench_db: Polars) -> None:
    polars_jsonbench_db.current_query_name = "04_first_post_users"

    df = polars_jsonbench_db.fetch("")

    assert df.to_dicts() == [{"user_id": "u1", "first_post_ts": datetime(2023, 11, 14, 22, 13, 20)}]
