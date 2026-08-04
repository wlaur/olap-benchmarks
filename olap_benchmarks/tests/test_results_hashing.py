from __future__ import annotations

from decimal import Decimal

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
    assert left["answer_hash_version"] == "canonical-v10"
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


def test_answer_metadata_canonicalizes_fixed_width_text_to_strings() -> None:
    # ClickHouse declares TPC-H's CHAR(n) columns as FixedString(n), which arrives as binary
    # padded to the declared width with NUL. Writing that to NDJSON panics polars-json, and the
    # padded bytes would never match a VARCHAR engine's plain string.
    fixed_width = pl.DataFrame({"name": pl.Series([b"BRAZIL\x00\x00\x00", b"A"], dtype=pl.Binary)})
    varchar = pl.DataFrame({"name": pl.Series(["BRAZIL", "A"], dtype=pl.String)})

    assert (
        hashing.build_answer_metadata(fixed_width)["answer_hash"]
        == hashing.build_answer_metadata(varchar)["answer_hash"]
    )


def test_answer_metadata_canonicalizes_arrays_to_their_bracketed_elements() -> None:
    # MonetDB has no array type and builds the same content as a string with group_concat, so an
    # aggregated array has to compare on its elements rather than on its container type
    arrays = pl.DataFrame({"ids": pl.Series([[3, 1, 2], [7]], dtype=pl.List(pl.Int64))})
    strings = pl.DataFrame({"ids": pl.Series(["[3,1,2]", "[7]"], dtype=pl.String)})

    assert hashing.build_answer_metadata(arrays)["answer_hash"] == hashing.build_answer_metadata(strings)["answer_hash"]


def test_answer_metadata_keeps_array_element_order_significant() -> None:
    left = pl.DataFrame({"ids": pl.Series([[3, 1, 2]], dtype=pl.List(pl.Int64))})
    right = pl.DataFrame({"ids": pl.Series([[1, 2, 3]], dtype=pl.List(pl.Int64))})

    assert hashing.build_answer_metadata(left)["answer_hash"] != hashing.build_answer_metadata(right)["answer_hash"]


def test_answer_metadata_canonicalizes_an_all_null_array_to_null() -> None:
    # array_agg() over an unmatched outer join yields [NULL]; MonetDB's group_concat over no rows
    # yields NULL, and concatenating that into brackets stays NULL
    null_element = pl.DataFrame({"ids": pl.Series([[None]], dtype=pl.List(pl.Int64))})
    null_value = pl.DataFrame({"ids": pl.Series([None], dtype=pl.String)})

    assert (
        hashing.build_answer_metadata(null_element)["answer_hash"]
        == hashing.build_answer_metadata(null_value)["answer_hash"]
    )


def test_answer_metadata_canonicalizes_scale_zero_decimals_to_integers() -> None:
    # drivers disagree on how they report an exact integer: MonetDB narrows an aggregate's declared
    # type by context (int8/int32/int64) and reports hugeint as DECIMAL(38, 0), while a driver that
    # erases width reports int64 throughout. The same number must hash identically either way.
    integers = pl.DataFrame({"total": pl.Series([12345, -7, 0], dtype=pl.Int64)})
    decimals = pl.DataFrame({"total": pl.Series([Decimal(12345), Decimal(-7), Decimal(0)], dtype=pl.Decimal(38, 0))})

    assert (
        hashing.build_answer_metadata(integers)["answer_hash"] == hashing.build_answer_metadata(decimals)["answer_hash"]
    )


def test_answer_metadata_preserves_integers_beyond_float64_precision() -> None:
    # sum(bigint) can exceed int64, arriving as DECIMAL(38, 0); canonicalizing through Float64
    # would collapse neighbouring values onto the same hash
    low = pl.DataFrame({"total": pl.Series([Decimal(18000000000000000000)], dtype=pl.Decimal(38, 0))})
    high = pl.DataFrame({"total": pl.Series([Decimal(18000000000000000001)], dtype=pl.Decimal(38, 0))})

    assert hashing.build_answer_metadata(low)["answer_hash"] != hashing.build_answer_metadata(high)["answer_hash"]


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


def test_answer_metadata_canonicalizes_json_rendering() -> None:
    # MonetDB's json column normalises the stored document, a text column echoes it verbatim, and
    # object key order is not meaningful; none of that should decide whether two engines agree
    spaced = pl.DataFrame({"payload": ['{"status": ["Pending"], "terminal": "Berlin"}']})
    compact = pl.DataFrame({"payload": ['{"terminal":"Berlin","status":["Pending"]}']})

    assert hashing.build_answer_metadata(spaced)["answer_hash"] == hashing.build_answer_metadata(compact)["answer_hash"]


def test_answer_metadata_keeps_json_content_significant() -> None:
    left = pl.DataFrame({"payload": ['{"terminal": "Berlin"}']})
    right = pl.DataFrame({"payload": ['{"terminal": "Hamburg"}']})

    assert hashing.build_answer_metadata(left)["answer_hash"] != hashing.build_answer_metadata(right)["answer_hash"]


def test_answer_metadata_leaves_ordinary_text_untouched() -> None:
    # only columns whose first value opens a JSON object or array take the normalisation path
    text = pl.DataFrame({"name": ["Berlin", "Hamburg"]})
    braced = pl.DataFrame({"name": ["{not json", "also not"]})

    assert "answer_hash" in hashing.build_answer_metadata(text)
    assert "answer_hash" in hashing.build_answer_metadata(braced)


def test_float_rounding_separates_real_divergence_from_summation_noise() -> None:
    """Pins FLOAT_SIGNIFICANT_DIGITS between the cases this suite actually produces.

    Every pair here was observed while making the engines agree. Loosening the constant hides the
    defects; tightening it reports float summation order as a disagreement.
    """
    real_divergences = [
        # MonetDB's avg() over a ROWS window frame wider than 16 rows
        (156.5231018066, 156.5604739189),
        # engines evaluating a decimal division at the numerator's scale
        (0.6559139785, 0.6559),
        (3295493.512857143, 3295493.512),
        (552580.1678651213, 552580.167),
    ]
    summation_noise = [
        # the same value summed in a different order
        (38257.8106600811, 38257.8106600812),
        (156.5656321738, 156.5656321737),
        # stddev over 183k float32 values, DuckDB against ClickHouse
        (0.5407961018807453, 0.540796101881521),
    ]

    for left, right in real_divergences:
        assert (
            hashing.build_answer_metadata(pl.DataFrame({"v": [left]}))["answer_hash"]
            != hashing.build_answer_metadata(pl.DataFrame({"v": [right]}))["answer_hash"]
        ), f"{left} and {right} are a real divergence and must not collapse"

    for left, right in summation_noise:
        assert (
            hashing.build_answer_metadata(pl.DataFrame({"v": [left]}))["answer_hash"]
            == hashing.build_answer_metadata(pl.DataFrame({"v": [right]}))["answer_hash"]
        ), f"{left} and {right} are summation noise and must not be reported"
