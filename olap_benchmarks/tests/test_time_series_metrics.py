from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any, cast

import duckdb
import polars as pl
import pytest

from ..dbs.polars.time_series_metrics import execute_time_series_metrics_query
from ..results.hashing import build_answer_metadata
from ..settings import get_settings
from ..suites.time_series_metrics import config
from ..suites.time_series_metrics.config import (
    METRIC_COUNTS,
    QUERIES_DIRECTORY,
    QUERY_NAMES,
    generate_batch,
    metric_name,
    to_eav,
    write_datasets,
)


@pytest.mark.parametrize("metrics", METRIC_COUNTS)
def test_all_queries_agree_between_layouts_and_polars(metrics: int) -> None:
    wide = generate_batch(0, 240, metrics=metrics).with_columns(
        pl.datetime_range(
            datetime(2024, 12, 29, 12), datetime(2024, 12, 29, 15, 59), interval="1m", time_unit="ms", eager=True
        ).alias("time")
    )
    eav = to_eav(wide)
    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(config={"threads": 2})
    try:
        con.register(f"wide{metrics}", wide)
        con.register(f"eav{metrics}", eav)
        for wide_name in QUERY_NAMES:
            if not wide_name.startswith(f"wide{metrics}_"):
                continue
            eav_name = wide_name.replace("wide", "eav", 1)
            wide_result = con.execute((QUERIES_DIRECTORY / f"{wide_name}.sql").read_text()).pl()
            eav_result = con.execute((QUERIES_DIRECTORY / f"{eav_name}.sql").read_text()).pl()
            expected = build_answer_metadata(wide_result)["answer_hash"]
            override = QUERIES_DIRECTORY / "arc" / f"{wide_name}.sql"
            if override.is_file():
                arc_result = con.execute(override.read_text()).pl()
                assert build_answer_metadata(arc_result)["answer_hash"] == expected, wide_name
            clickhouse_override = QUERIES_DIRECTORY / "clickhouse" / f"{wide_name}.sql"
            if wide_name.endswith("_12_all_metrics"):
                clickhouse_result = con.execute(clickhouse_override.read_text().replace("arraySum", "list_sum")).pl()
                assert build_answer_metadata(clickhouse_result)["answer_hash"] == expected, wide_name
            assert build_answer_metadata(eav_result)["answer_hash"] == expected, eav_name
            for frame, name in ((wide, wide_name), (eav, eav_name)):
                result = execute_time_series_metrics_query(frame.lazy(), name).collect()
                assert build_answer_metadata(result)["answer_hash"] == expected, name
    finally:
        con.close()


def test_generation_keeps_null_rows_and_identical_metric_prefixes(tmp_path: Path) -> None:
    write_datasets(tmp_path, rows=480, counts=(10, 20))
    narrow = pl.read_parquet(tmp_path / "wide10.parquet")
    wide = pl.read_parquet(tmp_path / "wide20.parquet")
    assert narrow.equals(wide.select(narrow.columns))
    for count in (10, 20):
        frame = pl.read_parquet(tmp_path / f"eav{count}.parquet")
        assert frame.height == 480 * count
        assert frame["value"].null_count() > 0
        restored = frame.pivot(on="metric_name", index="time", values="value").sort("time")
        expected = wide.select("time", *[metric_name(index) for index in range(1, count + 1)])
        assert restored.select(expected.columns).equals(expected)
    assert not list(tmp_path.glob("*.part"))


def test_expanding_metric_counts_preserves_existing_inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = tmp_path / "time_series_metrics"
    write_datasets(directory, rows=480, counts=(10,))
    existing = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in directory.glob("*.parquet")}
    settings = get_settings().model_copy(update={"input_data_directory": tmp_path})
    monkeypatch.setattr(config, "get_settings", lambda: settings)
    monkeypatch.setattr(config, "METRIC_COUNTS", (10, 20))
    monkeypatch.setattr(config, "write_datasets", partial(write_datasets, rows=480))

    config.prepare_data()

    for path, original in existing.items():
        assert (path.read_bytes(), path.stat().st_mtime_ns) == original
    assert pl.scan_parquet(directory / "wide20.parquet").select(pl.len()).collect().item() == 480
    assert pl.scan_parquet(directory / "eav20.parquet").select(pl.len()).collect().item() == 9_600
