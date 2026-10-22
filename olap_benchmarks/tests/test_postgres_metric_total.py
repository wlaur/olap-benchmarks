import re
from typing import Any, Literal, cast

import duckdb
import polars as pl
import pytest

from ..dbs.postgres import Postgres, PostgresTimeSeriesMetrics
from ..dbs.timescaledb import TimescaleDB
from ..results.hashing import build_answer_metadata
from ..settings import get_settings
from ..suites.time_series_metrics.config import QUERIES_DIRECTORY, QUERY_NAMES, metric_name


@pytest.mark.parametrize("db_type", [Postgres, TimescaleDB])
def test_postgres_family_preserves_metric_plan_and_unrelated_queries(
    monkeypatch: pytest.MonkeyPatch,
    db_type: type[Postgres],
) -> None:
    monkeypatch.setattr(get_settings(), "tables", ())
    suite = db_type().benchmarks["time_series_metrics"]

    assert isinstance(suite, PostgresTimeSeriesMetrics)
    assert suite.planned_query_names() == QUERY_NAMES
    assert suite.query_skip("wide1000_12_all_metrics") is None
    for query_name in ("wide1000_13_per_metric", "wide1000_14_hourly_all_metrics"):
        skip = suite.query_skip(query_name)
        assert skip is not None
        assert skip[0] == "unsupported"
        assert "1664 result-column limit" in skip[1]
    for query_name in QUERY_NAMES:
        if query_name == "wide1000_12_all_metrics":
            continue
        path = suite.query_path(query_name)
        assert path is not None
        assert suite.load_query(query_name) == path.read_text(), query_name

    sql = suite.load_query("wide1000_12_all_metrics")
    columns = re.findall(r"CAST\((metric_\d{4}) AS DOUBLE PRECISION\)", sql)
    assert columns == [metric_name(index) for index in range(1, 1001)]
    assert "CROSS JOIN LATERAL UNNEST(ARRAY[" in sql
    assert sql.count("COUNT(") == 1
    assert sql.count("SUM(") == 1


@pytest.mark.parametrize("db_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize("case", ["mixed", "all_null", "empty"])
def test_postgres_native_metric_total_matches_the_canonical_query(
    db_type: type[Postgres],
    case: Literal["mixed", "all_null", "empty"],
) -> None:
    data: dict[str, pl.Series] = {}
    for index in range(1, 1001):
        values: list[float | None]
        if case == "empty":
            values = []
        elif case == "all_null" or index == 1000:
            values = [None, None, None]
        else:
            values = [(index % 7 - 3) / 4, None, -(index % 5) / 2]
        data[metric_name(index)] = pl.Series(metric_name(index), values, dtype=pl.Float32)
    frame = pl.DataFrame(data)
    suite = db_type().benchmarks["time_series_metrics"]
    canonical = (QUERIES_DIRECTORY / "wide1000_12_all_metrics.sql").read_text()
    connection: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(config={"threads": 1})
    try:
        connection.register("wide1000", frame)
        expected = connection.sql(canonical).pl()
        actual = connection.sql(suite.load_query("wide1000_12_all_metrics")).pl()
    finally:
        connection.close()

    assert actual.columns == ["cnt", "total"]
    assert actual.shape == (1, 2)
    assert build_answer_metadata(actual)["answer_hash"] == build_answer_metadata(expected)["answer_hash"]
    assert actual.item(0, "cnt") == (1998 if case == "mixed" else 0)
    if case == "mixed":
        assert actual.item(0, "total") < 0
    else:
        assert actual.item(0, "total") == 0.0
