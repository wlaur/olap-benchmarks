from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import duckdb
import polars as pl
import pytest

from ..dbs.polars.time_series_ranges import execute_time_series_ranges_query
from ..results.hashing import build_answer_metadata
from ..suites.time_series_metrics.config import METRIC_COUNTS, generate_batch, to_eav, write_datasets
from ..suites.time_series_ranges.config import END, INTERVAL_MINUTES, QUERIES_DIRECTORY, QUERY_NAMES, RANGE_QUERIES


@pytest.mark.parametrize("metrics", METRIC_COUNTS)
def test_ranges_match_layouts_and_native_polars_at_filter_boundaries(metrics: int) -> None:
    times = [END - timedelta(days=days, minutes=offset) for days in (0, 1, 10, 100, 101) for offset in (-10, 0, 10)]
    wide = generate_batch(100, 100 + len(times), rows=120, metrics=metrics).with_columns(
        pl.Series("time", times, dtype=pl.Datetime("ms"))
    )
    eav = to_eav(wide)
    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(config={"threads": 2})
    try:
        con.register(f"wide{metrics}", wide)
        con.register(f"eav{metrics}", eav)
        for wide_name in QUERY_NAMES:
            if not wide_name.startswith(f"wide{metrics}_"):
                continue
            query = RANGE_QUERIES[int(wide_name.split("_", 2)[1]) - 1]
            eav_name = wide_name.replace("wide", "eav", 1)
            wide_result = con.execute((QUERIES_DIRECTORY / f"{wide_name}.sql").read_text()).pl()
            eav_result = con.execute((QUERIES_DIRECTORY / f"{eav_name}.sql").read_text()).pl()
            for frame, name, expected in ((wide, wide_name, wide_result), (eav, eav_name, eav_result)):
                actual = execute_time_series_ranges_query(frame.lazy(), name).collect()
                assert build_answer_metadata(actual)["answer_hash"] == build_answer_metadata(expected)["answer_hash"]
            if query.operation == "raw":
                comparable = wide_result.unpivot(index="time", variable_name="metric_name", value_name="value").sort(
                    "time", "metric_name"
                )
                expected_rows = wide.filter((pl.col("time") >= query.start) & (pl.col("time") < END)).height
                assert wide_result.height == expected_rows
                assert eav_result.height == expected_rows * query.selected_metrics
            else:
                comparable = (
                    wide_result.unpivot(variable_name="metric_name", value_name="avg_value")
                    .with_columns(pl.col("metric_name").str.strip_prefix("avg_"))
                    .sort("metric_name")
                )
                assert eav_result.height == query.selected_metrics
            assert build_answer_metadata(comparable)["answer_hash"] == build_answer_metadata(eav_result)["answer_hash"]
    finally:
        con.close()


def test_generation_uses_exact_history_and_reuses_metric_prefixes(tmp_path: Path) -> None:
    rows = 100 * 24 * 60 // INTERVAL_MINUTES
    write_datasets(
        tmp_path, rows=rows, counts=(10, 20), interval_minutes=INTERVAL_MINUTES, end=END - timedelta(minutes=10)
    )
    wide = pl.read_parquet(tmp_path / "wide20.parquet")
    assert wide.height == rows
    assert wide["time"].min() == END - timedelta(days=100)
    assert wide["time"].max() == END - timedelta(minutes=10)
    assert wide["time"].diff().drop_nulls().unique().to_list() == [timedelta(minutes=10)]
    narrow = pl.read_parquet(tmp_path / "wide10.parquet")
    assert narrow.equals(wide.select(narrow.columns))
    eav = pl.read_parquet(tmp_path / "eav10.parquet")
    assert eav.height == rows * 10
    assert eav["value"].null_count() == narrow.select(pl.sum_horizontal(pl.all().exclude("time").null_count())).item()
