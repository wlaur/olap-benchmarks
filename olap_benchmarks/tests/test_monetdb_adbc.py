import polars as pl

from ..dbs.monetdb.adbc import MAX_ARROW_BATCH_ROWS, get_rows_per_batch


def test_arrow_batches_use_driver_aligned_row_count() -> None:
    assert get_rows_per_batch({"value": pl.Float64}) == MAX_ARROW_BATCH_ROWS


def test_arrow_batches_use_driver_aligned_row_count_for_current_wide_table() -> None:
    schema = {f"value_{index}": pl.Float64 for index in range(786)}

    assert get_rows_per_batch(schema) == MAX_ARROW_BATCH_ROWS


def test_arrow_batches_bound_very_wide_frames_by_memory() -> None:
    schema = {f"value_{index}": pl.Float64 for index in range(2_000)}

    assert get_rows_per_batch(schema) == 67_108
