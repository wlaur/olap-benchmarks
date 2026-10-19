from pathlib import Path

import duckdb
import pytest
from polars.testing import assert_frame_equal

QUERY_DIRECTORY = Path(__file__).parents[1] / "suites/tpc_h/queries"


@pytest.mark.parametrize("empty", [False, True])
def test_pricing_summary_averages_preserve_nullable_and_empty_inputs(empty: bool) -> None:
    with duckdb.connect() as connection:
        connection.execute(
            """
            CREATE TABLE lineitem (
                l_returnflag VARCHAR, l_linestatus VARCHAR, l_shipdate DATE,
                l_quantity DECIMAL(15, 2), l_extendedprice DECIMAL(15, 2),
                l_discount DECIMAL(15, 2), l_tax DECIMAL(15, 2)
            )
            """
        )
        if not empty:
            connection.execute(
                """
                INSERT INTO lineitem VALUES
                    ('A', 'F', '1998-09-01', 1.00, 10.01, 0.01, 0.00),
                    ('A', 'F', '1998-09-02', 2.00, 10.02, 0.02, 0.05),
                    ('A', 'F', '1998-09-02', NULL, NULL, NULL, NULL),
                    ('N', 'O', '1998-09-02', NULL, NULL, NULL, NULL),
                    ('A', 'F', '1998-09-03', 50.00, 99.99, 0.10, 0.05)
                """
            )
        canonical = (QUERY_DIRECTORY / "01_pricing_summary.sql").read_text()
        override = (QUERY_DIRECTORY / "starrocks/01_pricing_summary.sql").read_text()
        reference = connection.execute(canonical).pl()
        actual = connection.execute(override).pl()

    assert_frame_equal(actual, reference)
    if not empty:
        assert actual.item(0, "avg_qty") == 1.5
        assert actual.item(0, "avg_price") == 10.015
        assert actual.item(0, "avg_disc") == 0.015
        assert actual.item(0, "count_order") == 3
        assert actual.item(1, "avg_qty") is None
        assert actual.item(1, "avg_price") is None
        assert actual.item(1, "avg_disc") is None
