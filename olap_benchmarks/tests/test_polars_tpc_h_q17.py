from __future__ import annotations

from decimal import Decimal

import polars as pl
import pytest

from olap_benchmarks.dbs.polars.tpc_h import execute_tpc_h_query
from olap_benchmarks.results.hashing import build_answer_metadata
from olap_benchmarks.suites.tpc_h.config import TPCH_TABLES

type PartRow = tuple[int, str, str]
type LineRow = tuple[int, str | None, str | None]

SELECTED: list[PartRow] = [(1, "Brand#23", "MED BOX")]


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "parts, lines, expected",
    [
        pytest.param([], [], None, id="typed-empty-both"),
        pytest.param(SELECTED, [], None, id="typed-empty-lineitem"),
        pytest.param([], [(1, "1.00", "7.00"), (1, "100.00", "700.00")], None, id="empty-part"),
        pytest.param(
            [(1, "Brand#12", "MED BOX"), (2, "Brand#23", "SM BOX")],
            [(1, "1.00", "7.00"), (1, "100.00", "700.00")],
            None,
            id="no-selected-part",
        ),
        pytest.param(SELECTED, [(1, "2.00", "700.00"), (1, "18.00", "700.00")], None, id="strict-threshold"),
        pytest.param(SELECTED, [(1, "1.00", None), (1, "100.00", "700.00")], None, id="all-null-qualified-prices"),
        pytest.param(SELECTED, [(1, "1.00", "0.00"), (1, "100.00", "700.00")], 0.0, id="real-zero"),
        pytest.param(
            SELECTED,
            [(1, "1.00", "-7.00"), (1, "1.00", "7.00"), (1, "100.00", "700.00")],
            0.0,
            id="cancelling-non-null-prices",
        ),
        pytest.param(
            SELECTED,
            [(1, "1.00", None), (1, "1.00", "7.00"), (1, "100.00", "700.00")],
            1.0,
            id="sum-ignores-null-price",
        ),
        pytest.param(SELECTED, [(1, None, "7.00"), (1, None, "7.00")], None, id="all-null-quantities"),
        pytest.param(
            SELECTED,
            [(1, None, "7000.00"), (1, "1.00", "7.00"), (1, "100.00", "700.00")],
            1.0,
            id="average-ignores-null-quantities",
        ),
        pytest.param(
            [
                (1, "Brand#23", "MED BOX"),
                (1, "Brand#23", "MED BOX"),
                (2, "Brand#12", "MED BOX"),
                (3, "Brand#23", "MED BOX"),
                (1, "Brand#12", "MED BOX"),
            ],
            [
                (1, "1.00", "7.00"),
                (1, "1.00", "7.00"),
                (1, "100.00", "700.00"),
                (2, "1.00", "7000.00"),
                (2, "1000.00", "7000.00"),
                (4, "1.00", "70000.00"),
            ],
            4.0,
            id="duplicate-lines-and-selected-parts",
        ),
        pytest.param(
            SELECTED,
            [(1, "0.10", "0.01"), (1, "0.10", "0.02"), (1, "99.80", "700.00")],
            float(Decimal("0.03")) / 7.0,
            id="fixed-decimal-sum-before-float",
        ),
    ],
)
def test_q17_sql_sum_contract(
    parts: list[PartRow], lines: list[LineRow], expected: float | None, reverse: bool
) -> None:
    part = pl.DataFrame(
        list(reversed(parts)) if reverse else parts,
        schema={"p_partkey": pl.Int64, "p_brand": pl.String, "p_container": pl.String},
        orient="row",
    )
    line_rows = list(reversed(lines)) if reverse else lines
    lineitem = pl.DataFrame(
        [
            (key, Decimal(qty) if qty is not None else None, Decimal(price) if price is not None else None)
            for key, qty, price in line_rows
        ],
        schema={"l_partkey": pl.Int64, "l_quantity": pl.Decimal(15, 2), "l_extendedprice": pl.Decimal(15, 2)},
        orient="row",
    )
    tables = {name: pl.DataFrame().lazy() for name in TPCH_TABLES}
    tables.update(part=part.lazy(), lineitem=lineitem.lazy())
    result = execute_tpc_h_query(tables, "17_small_quantity_revenue", 10).collect()
    expected_frame = pl.DataFrame({"avg_yearly": pl.Series([expected], dtype=pl.Float64)})
    assert part.schema == {"p_partkey": pl.Int64, "p_brand": pl.String, "p_container": pl.String}
    assert lineitem.schema == {
        "l_partkey": pl.Int64,
        "l_quantity": pl.Decimal(15, 2),
        "l_extendedprice": pl.Decimal(15, 2),
    }
    assert result.schema == {"avg_yearly": pl.Float64}
    assert result.shape == (1, 1)
    assert result.rows() == expected_frame.rows()
    assert result.rows().count((expected,)) == 1
    assert build_answer_metadata(result) == build_answer_metadata(expected_frame)
