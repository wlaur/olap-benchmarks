from __future__ import annotations

import polars as pl
import pytest

from ..results import hashing


def test_answer_metadata_hashes_small_results_deterministically() -> None:
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    left = hashing.build_answer_metadata(df)
    right = hashing.build_answer_metadata(df)
    changed = hashing.build_answer_metadata(pl.DataFrame({"id": [1, 2], "name": ["a", "c"]}))

    assert left["answer_rows"] == 2
    assert left["answer_columns"] == 2
    assert left["answer_cells"] == 4
    assert left["answer_hash_version"] == "canonical-v4"
    assert left["answer_hash"] == right["answer_hash"]
    assert left["answer_hash"] != changed["answer_hash"]


def test_answer_metadata_canonicalizes_equivalent_numeric_dtypes() -> None:
    narrow = pl.DataFrame(
        {
            "id": pl.Series([1, 2], dtype=pl.Int32),
            "count": pl.Series([3, 4], dtype=pl.UInt32),
            "value": pl.Series([1.5, 2.5], dtype=pl.Float32),
        }
    )
    wide = pl.DataFrame(
        {
            "id": pl.Series([1, 2], dtype=pl.Int64),
            "count": pl.Series([3, 4], dtype=pl.Int64),
            "value": pl.Series([1.5, 2.5], dtype=pl.Float64),
        }
    )

    assert hashing.build_answer_metadata(narrow)["answer_hash"] == hashing.build_answer_metadata(wide)["answer_hash"]


def test_answer_metadata_canonicalizes_booleans_to_integer_values() -> None:
    bools = pl.DataFrame({"flag": [True, False, None]})
    ints = pl.DataFrame({"flag": pl.Series([1, 0, None], dtype=pl.Int64)})

    assert hashing.build_answer_metadata(bools)["answer_hash"] == hashing.build_answer_metadata(ints)["answer_hash"]


def test_answer_metadata_canonicalizes_decimal_float_equivalents() -> None:
    decimals = pl.DataFrame({"value": pl.Series(["0.8974358974", "0.5500000000"], dtype=pl.Decimal(38, 10))})
    floats = pl.DataFrame({"value": [0.8974358974, 0.55]})

    assert (
        hashing.build_answer_metadata(decimals)["answer_hash"] == hashing.build_answer_metadata(floats)["answer_hash"]
    )


def test_answer_metadata_quantizes_float_roundoff() -> None:
    left = pl.DataFrame({"stddev": [43.242039026201894]})
    right = pl.DataFrame({"stddev": [43.24203902622548]})

    assert hashing.build_answer_metadata(left)["answer_hash"] == hashing.build_answer_metadata(right)["answer_hash"]


def test_answer_metadata_ignores_engine_specific_column_names() -> None:
    expression_label = pl.DataFrame({"max(time)": [1]})
    quoted_expression_label = pl.DataFrame({'max("time")': [1]})

    assert (
        hashing.build_answer_metadata(expression_label)["answer_hash"]
        == hashing.build_answer_metadata(quoted_expression_label)["answer_hash"]
    )


def test_answer_metadata_hashes_equal_nullable_frames_deterministically() -> None:
    first = pl.DataFrame({"time": [1, 2, 3], "delta": [None, 0.1, -0.2]})
    second = pl.DataFrame({"time": [1, 2, 3], "delta": [None, 0.1, -0.2]})

    assert hashing.build_answer_metadata(first)["answer_hash"] == hashing.build_answer_metadata(second)["answer_hash"]


def test_answer_metadata_canonicalizes_datetimes_to_naive_milliseconds() -> None:
    utc = pl.DataFrame(
        {
            "ts": pl.Series(
                ["2026-01-01T00:00:00.123456+00:00"],
                dtype=pl.Datetime("us", time_zone="UTC"),
            )
        }
    )
    naive_ms = pl.DataFrame(
        {
            "ts": pl.Series(
                ["2026-01-01T00:00:00.123"],
                dtype=pl.Datetime("ms"),
            )
        }
    )

    assert hashing.build_answer_metadata(utc)["answer_hash"] == hashing.build_answer_metadata(naive_ms)["answer_hash"]


def test_answer_metadata_skips_large_results(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hashing, "MAX_ANSWER_HASH_CELLS", 1)

    metadata = hashing.build_answer_metadata(pl.DataFrame({"a": [1], "b": [2]}))

    assert "answer_hash" not in metadata
    assert metadata["answer_hash_skipped_reason"] == "result has 2 cells, above the 1-cell hash limit"
