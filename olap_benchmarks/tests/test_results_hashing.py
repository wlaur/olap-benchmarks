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
    assert left["answer_hash_version"] == "canonical-v1"
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
