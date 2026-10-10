import io
from datetime import datetime

import polars as pl
import pyarrow as pa
import pytest
from pyarrow import ipc

from olap_benchmarks.dbs.arc import (
    Arc,
    ArcTimeSeries,
    build_time_key_predicate,
    decode_arrow_stream,
    encode_parquet,
    iter_write_batches,
)


def _frame(rows: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "time": pl.datetime_range(
                datetime(2024, 1, 1), datetime(2024, 1, 1, 0, rows - 1), "1m", eager=True, time_unit="ms"
            ),
            "value": pl.Series([None] * rows, dtype=pl.Float32),
            "flag": [True] * rows,
        }
    )


def test_iter_write_batches_splits_by_cell_budget() -> None:
    df = _frame(10)

    eager = list(iter_write_batches(df, batch_cells=9))
    lazy = list(iter_write_batches(df.lazy(), batch_cells=9))

    assert [batch.height for batch in eager] == [3, 3, 3, 1]
    assert pl.concat(eager).equals(df)
    assert pl.concat(lazy).equals(df)


def test_encode_parquet_keeps_types_of_all_null_columns() -> None:
    df = _frame(3)

    decoded = pl.read_parquet(io.BytesIO(encode_parquet(df)))

    assert decoded.schema == df.schema
    assert decoded.equals(df)


def test_encode_parquet_keeps_timestamps_beyond_nanosecond_range() -> None:
    df = pl.DataFrame({"time": pl.Series([datetime(3927, 1, 1)], dtype=pl.Datetime("ms"))})

    assert pl.read_parquet(io.BytesIO(encode_parquet(df))).item(0, 0) == datetime(3927, 1, 1)


def test_build_time_key_predicate_deduplicates_and_sorts_keys() -> None:
    keys = pl.DataFrame(
        {"time": [datetime(2024, 10, 15, 12, 31), datetime(2024, 10, 15, 12, 30), datetime(2024, 10, 15, 12, 31)]}
    )

    assert build_time_key_predicate(keys, ["time"]) == "time IN ('2024-10-15 12:30:00', '2024-10-15 12:31:00')"


def test_build_time_key_predicate_rejects_other_keys() -> None:
    with pytest.raises(ValueError, match="time column only"):
        build_time_key_predicate(pl.DataFrame({"id": [1]}), ["id"])


def test_decode_arrow_stream_returns_naive_utc_timestamps() -> None:
    table = pa.table(
        {
            "time": pa.array([1_704_067_200_000_000], type=pa.timestamp("us", tz="UTC")),
            "value": pa.array([1.5]),
        }
    )
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)

    df = decode_arrow_stream(sink.getvalue())

    assert df.schema == pl.Schema({"time": pl.Datetime("us"), "value": pl.Float64})
    assert df.item(0, 0) == datetime(2024, 1, 1)


def test_arc_registers_only_time_series_with_all_operations() -> None:
    db = Arc()

    assert dict(db.suite_registry()) == {"time_series": ArcTimeSeries}
    assert ArcTimeSeries.supported_operations == ("populate", "select", "mutate", "concurrent")
    assert not db.DISABLED_MUTATION_STEPS


def test_arc_connect_is_not_a_sql_connection() -> None:
    with pytest.raises(NotImplementedError):
        Arc().connect()
