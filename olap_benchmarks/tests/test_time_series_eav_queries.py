from __future__ import annotations

from typing import Any, cast

import duckdb

from ..suites.time_series.config import TIME_SERIES_QUERIES_DIRECTORY


def _query(name: str) -> str:
    return (TIME_SERIES_QUERIES_DIRECTORY / f"postgres/{name}.sql").read_text()


def _connect_with_rows(
    rows: list[tuple[str, str, float | None]], table: str = "data_wide"
) -> duckdb.DuckDBPyConnection:
    con = cast(Any, duckdb).connect(":memory:")
    con.execute(f"create table {table} (time timestamp, metric_name varchar, value real)")
    con.executemany(f"insert into {table} values (?, ?, ?)", rows)
    return cast(duckdb.DuckDBPyConnection, con)


def test_eav_scalar_lookup_preserves_timestamp_when_metric_row_is_missing() -> None:
    con = _connect_with_rows(
        [
            ("2024-12-15 12:30:00", "binary_1", 1.0),
            ("2024-12-15 12:30:00", "ratio_1", 0.5),
        ]
    )

    try:
        rows = con.execute(_query("wide_10_scalar_lookup")).fetchall()
    finally:
        con.close()

    assert len(rows) == 1
    assert rows[0][1] is None


def test_eav_raw_filtered_uses_anchor_timestamp_domain() -> None:
    con = _connect_with_rows(
        [
            ("2024-12-11 00:00:00", "binary_1", 1.0),
            ("2024-12-11 00:00:00", "process_545", 10.0),
            ("2024-12-12 00:00:00", "binary_1", 1.0),
        ]
    )

    try:
        rows = con.execute(_query("wide_09_raw_filtered")).fetchall()
    finally:
        con.close()

    assert [row[1] for row in rows] == [10.0, None]


def test_eav_daily_resample_keeps_all_null_metric_bucket() -> None:
    con = _connect_with_rows(
        [
            ("2024-12-11 00:00:00", "binary_1", 1.0),
            ("2024-12-11 00:00:00", "process_545", 10.0),
            ("2024-12-12 00:00:00", "binary_1", 1.0),
        ]
    )

    try:
        rows = con.execute(_query("wide_06_daily_resample")).fetchall()
    finally:
        con.close()

    assert [row[1] for row in rows] == [10.0, None]


def test_eav_null_gap_detection_uses_complete_timestamp_domain() -> None:
    con = _connect_with_rows(
        [
            ("2024-12-11 00:00:00", "binary_1", 1.0),
            ("2024-12-11 00:00:00", "process_545", 10.0),
            ("2024-12-11 00:01:00", "binary_1", 1.0),
            ("2024-12-11 00:02:00", "binary_1", 1.0),
            ("2024-12-11 00:02:00", "process_545", 12.0),
        ]
    )

    try:
        rows = con.execute(_query("wide_19_null_gap_detection")).fetchall()
    finally:
        con.close()

    assert len(rows) == 1
    assert rows[0][1:] == (None, 10.0)
