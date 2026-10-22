from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl
import pytest

from olap_benchmarks.dbs.polars import Polars
from olap_benchmarks.dbs.polars.clickbench import execute_clickbench_query
from olap_benchmarks.dbs.polars.time_series_metrics import execute_time_series_metrics_query
from olap_benchmarks.results.hashing import build_answer_metadata
from olap_benchmarks.settings import REPO_ROOT, get_settings


@pytest.mark.parametrize("values, expected", [([None, "", "x", "x", None], 2), ([None, None], 0), ([], 0)])
def test_clickbench_distinct_nullable_phrase(values: list[str | None], expected: int) -> None:
    frame = pl.DataFrame({"SearchPhrase": pl.Series(values, dtype=pl.String)})
    result = execute_clickbench_query(frame.lazy(), "Q5").collect()
    assert result.schema == {"SearchPhrase": pl.Int64}
    assert result.item() == expected


@pytest.mark.parametrize(
    "values, expected",
    [([], None), ([2, 2, 8], 4.0), ([435090932899640449] * 3, 435090932899640449.0)],
)
def test_clickbench_average_empty_and_large_ids(values: list[int], expected: float | None) -> None:
    frame = pl.DataFrame({"UserID": pl.Series(values, dtype=pl.Int64)})
    result = execute_clickbench_query(frame.lazy(), "Q3").collect()
    assert result.schema == {"UserID": pl.Float64}
    assert result.item() == expected


def test_jsonbench_distinct_missing_users_retains_events(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(get_settings(), "database_directory", tmp_path)
    db = Polars()
    db._current_suite = "jsonbench"
    db._current_suite_scale_factor = 10
    db.current_query_name = "02_create_events_by_collection"
    db.database_directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "did": pl.Series([None, "u1", "u1", None, None, None, "u2"], dtype=pl.String),
            "kind": ["commit"] * 7,
            "commit": [
                {"operation": "create", "collection": "post"},
                {"operation": "create", "collection": "post"},
                {"operation": "create", "collection": "post"},
                {"operation": "create", "collection": "like"},
                {"operation": "create", "collection": None},
                {"operation": "create", "collection": None},
                {"operation": "delete", "collection": "post"},
            ],
        }
    ).write_parquet(db.table_path("bluesky"))
    result = db.fetch("")
    assert result.schema == {"event": pl.String, "count": pl.Int64, "users": pl.Int64}
    assert result.to_dicts() == [
        {"event": "post", "count": 3, "users": 1},
        {"event": None, "count": 2, "users": 0},
        {"event": "like", "count": 1, "users": 0},
    ]


@pytest.mark.parametrize("number, suffix", [(4, "hourly_multi"), (7, "scalar_multi"), (8, "cross_metric_filter")])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("empty", [False, True])
def test_eav_alignment_matches_canonical_max(number: int, suffix: str, reverse: bool, empty: bool) -> None:
    target = datetime(2024, 12, 29, 12, 30)
    later = datetime(2024, 12, 29, 13, 30)
    rows: list[tuple[datetime, str, float | None]] = [
        (target, "metric_0001", None),
        (target, "metric_0001", 1.0),
        (target, "metric_0001", 9.0),
        (target, "metric_0001", 9.0),
        (target, "metric_0002", 5.0),
        (target, "metric_0002", 12.0),
        (target, "metric_0003", None),
        (target, "metric_0003", None),
        (later, "metric_0002", 20.0),
        (later, "metric_0004", -4.0),
        (later, "metric_0004", -2.0),
        (target, "metric_0099", 999.0),
    ]
    if reverse:
        rows.reverse()
    frame = pl.DataFrame(
        [] if empty else rows,
        schema={"time": pl.Datetime("ms"), "metric_name": pl.String, "value": pl.Float32},
        orient="row",
    )
    query_name = f"eav100_{number:02d}_{suffix}"
    query = (REPO_ROOT / "olap_benchmarks/suites/time_series_metrics/queries" / f"{query_name}.sql").read_text()
    with duckdb.connect() as connection:
        connection.register("eav100", frame)
        expected = connection.sql(query).pl()
    result = execute_time_series_metrics_query(frame.lazy(), query_name).collect()
    assert build_answer_metadata(result)["answer_hash"] == build_answer_metadata(expected)["answer_hash"]
    assert result.columns == expected.columns
    for name, dtype in result.schema.items():
        if name in ("time", "hr"):
            assert dtype == pl.Datetime("ms")
        else:
            assert dtype == (pl.Int64 if name == "cnt" else pl.Float64)
