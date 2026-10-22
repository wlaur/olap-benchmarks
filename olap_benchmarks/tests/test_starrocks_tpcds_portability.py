import re
from collections import Counter
from collections.abc import Iterator
from decimal import Decimal
from typing import cast

import duckdb
import pytest
from polars.testing import assert_frame_equal

from ..query_registry import SUITES_DIRECTORY

TPCDS = SUITES_DIRECTORY / "tpc_ds"
QUERIES = ("07", "12", "20", "45", "49", "50", "72")
NULLS_LAST_KEYS = {
    "12": ("i_category", "i_class", "i_item_id", "i_item_desc", "revenueratio"),
    "45": ("ca_zip", "ca_city"),
    "50": (
        "s_store_name",
        "s_company_id",
        "s_street_number",
        "s_street_name",
        "s_street_type",
        "s_suite_number",
        "s_city",
        "s_county",
        "s_state",
        "s_zip",
    ),
}
SORT_KEYS = NULLS_LAST_KEYS | {"20": NULLS_LAST_KEYS["12"]}
NUMERIC_SORT_KEYS = {"revenueratio", "s_company_id", "s_street_number"}
RETURN_NUMERATORS = (
    "wr.wr_return_quantity",
    "wr.wr_return_amt",
    "cr.cr_return_quantity",
    "cr.cr_return_amount",
    "sr.sr_return_quantity",
    "sr.sr_return_amt",
)
type Row = tuple[object, ...]


def _query(name: str, *, adapted: bool) -> str:
    directory = TPCDS / "queries"
    return ((directory / "starrocks" if adapted else directory) / f"{name}.sql").read_text()


def _compact(sql: str) -> str:
    return " ".join(re.sub(r"--[^\n]*", "", sql).split())


@pytest.fixture
def connection() -> Iterator[duckdb.DuckDBPyConnection]:
    with duckdb.connect(":memory:", config={"threads": 1}) as database:
        database.execute("SET default_null_order = 'NULLS_LAST'")
        database.execute((TPCDS / "schemas/duckdb.sql").read_text())
        yield database


def _equivalent_rows(connection: duckdb.DuckDBPyConnection, name: str) -> list[Row]:
    reference = connection.sql(_query(name, adapted=False)).pl()
    actual = connection.sql(_query(name, adapted=True)).pl()
    assert_frame_equal(actual, reference, rel_tol=1e-12, abs_tol=1e-12)
    return cast(list[Row], actual.rows())


@pytest.mark.parametrize("name", QUERIES)
def test_only_proven_portability_changes_preserve_full_query_contract(name: str) -> None:
    expected = _query(name, adapted=False)
    if name == "07":
        for column in ("ss_list_price", "ss_coupon_amt", "ss_sales_price"):
            expected = expected.replace(f"avg({column})", f"avg(CAST({column} AS DOUBLE))")
    if name in {"12", "20"}:
        column = "ws_ext_sales_price" if name == "12" else "cs_ext_sales_price"
        numerator = f"sum({column})*100.0000"
        expected = expected.replace(numerator, f"CAST({numerator} AS DOUBLE)")
    if name in NULLS_LAST_KEYS:
        body, separator, _ = expected.rpartition("\nORDER BY ")
        assert separator
        order = ", ".join(f"{key} NULLS LAST" for key in NULLS_LAST_KEYS[name])
        expected = f"{body}\nORDER BY {order}\nLIMIT 100;"
    if name == "49":
        for column in RETURN_NUMERATORS:
            numerator = f"cast(sum(coalesce({column},0)) AS decimal(15,4))"
            expected = expected.replace(numerator, f"CAST({numerator} AS DOUBLE)")
        expected = expected.replace(") in_cat) CATALOG", ") in_cat) catalog")
    if name == "72":
        expected = expected.replace("d1.d_date + 5", "DATE_ADD(d1.d_date, INTERVAL 5 DAY)")
    assert _compact(_query(name, adapted=True)) == _compact(expected)


@pytest.mark.parametrize("name", QUERIES)
def test_empty_queries_preserve_output_names_types_and_rows(connection: duckdb.DuckDBPyConnection, name: str) -> None:
    assert _equivalent_rows(connection, name) == []


@pytest.mark.parametrize(("name", "key"), [(name, key) for name, keys in SORT_KEYS.items() for key in keys])
def test_final_sort_limit_preserves_null_selection_and_duplicate_rows(
    connection: duckdb.DuckDBPyConnection, name: str, key: str
) -> None:
    connection.execute(f"CREATE TABLE sort_input AS {_query(name, adapted=False)}")
    connection.execute("ALTER TABLE sort_input ADD COLUMN fixture_id INTEGER")
    keys = SORT_KEYS[name]
    inputs: list[Row] = []
    for identity in range(102):
        values: list[object] = [1 if column in NUMERIC_SORT_KEYS else "A" for column in keys]
        values[keys.index(key)] = (
            None if identity >= 100 else identity if key in NUMERIC_SORT_KEYS else f"{identity:02}"
        )
        inputs.append((*values, identity))
    columns = ", ".join((*keys, "fixture_id"))
    placeholders = ", ".join("?" for _ in range(len(keys) + 1))
    connection.executemany(f"INSERT INTO sort_input ({columns}) VALUES ({placeholders})", inputs)
    canonical_order = _query(name, adapted=False).rpartition("\nORDER BY ")[2]
    reference = cast(
        list[Row], connection.execute(f"SELECT fixture_id FROM sort_input ORDER BY {canonical_order}").fetchall()
    )
    expected = [*range(98), 100, 101] if name == "20" else list(range(100))
    assert Counter(reference) == Counter((identity,) for identity in expected)
    adapted_order = _query(name, adapted=True).rpartition("\nORDER BY ")[2]
    for null_order in ("NULLS_FIRST", "NULLS_LAST"):
        connection.execute(f"SET default_null_order = '{null_order}'")
        actual = cast(
            list[Row], connection.execute(f"SELECT fixture_id FROM sort_input ORDER BY {adapted_order}").fetchall()
        )
        assert Counter(actual) == Counter(reference)
        if name == "20":
            assert Counter(actual[:2]) == Counter([(100,), (101,)])
            assert actual[2:] == [(identity,) for identity in range(98)]
        else:
            assert actual == [(identity,) for identity in range(100)]


def test_decimal_means_preserve_fractional_cents_nulls_and_duplicate_facts(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    connection.execute(
        """
        INSERT INTO date_dim (d_date_sk, d_year) VALUES (1, 2000), (2, 1999);
        INSERT INTO item (i_item_sk, i_item_id) VALUES (1, 'a'), (2, NULL), (3, 'filtered');
        INSERT INTO customer_demographics (cd_demo_sk, cd_gender, cd_marital_status, cd_education_status)
            VALUES (1, 'M', 'S', 'College'), (2, 'F', 'S', 'College');
        INSERT INTO promotion (p_promo_sk, p_channel_email, p_channel_event)
            VALUES (1, 'N', NULL), (2, NULL, 'N'), (3, NULL, NULL);
        INSERT INTO store_sales (ss_item_sk, ss_ticket_number, ss_sold_date_sk, ss_cdemo_sk, ss_promo_sk,
                                ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price) VALUES
            (1, 1, 1, 1, 1, 3, 1.00, 0.01, 2.00),
            (1, 1, 1, 1, 1, 3, 1.00, 0.01, 2.00),
            (1, 2, 1, 1, 2, 9, 1.01, 0.02, 2.01),
            (1, 3, 1, 1, 1, NULL, NULL, NULL, NULL),
            (2, 4, 1, 1, 2, NULL, NULL, NULL, NULL),
            (3, 5, 2, 1, 1, 100, 99.99, 99.99, 99.99),
            (3, 6, 1, 2, 1, 100, 99.99, 99.99, 99.99),
            (3, 7, 1, 1, 3, 100, 99.99, 99.99, 99.99),
            (3, 8, 1, 1, NULL, 100, 99.99, 99.99, 99.99);
        """
    )
    rows = _equivalent_rows(connection, "07")
    assert len(rows) == 2
    assert rows[0][0] == "a"
    assert rows[0][1:] == pytest.approx((5, 3.01 / 3, 0.04 / 3, 6.01 / 3), rel=1e-12)
    assert rows[1] == (None, None, None, None, None)


@pytest.mark.parametrize(("name", "channel", "prefix"), [("12", "web", "ws"), ("20", "catalog", "cs")])
def test_revenue_ratios_preserve_decimal_sums_null_partitions_and_date_boundaries(
    connection: duckdb.DuckDBPyConnection, name: str, channel: str, prefix: str
) -> None:
    connection.execute(
        """
        INSERT INTO date_dim (d_date_sk, d_date) VALUES
            (1, '1999-02-22'), (2, '1999-03-24'), (3, '1999-02-21'), (4, '1999-03-25');
        INSERT INTO item (i_item_sk, i_item_id, i_item_desc, i_category, i_class, i_current_price) VALUES
            (1, 'a', 'one', 'Books', 'same', 1.00), (2, 'b', 'two', 'Books', 'same', 2.00),
            (3, 'c', NULL, 'Books', 'same', NULL), (4, 'd', 'four', 'Home', NULL, NULL),
            (5, 'e', 'five', 'Home', NULL, 0.00), (6, 'f', 'six', 'Sports', 'empty', 3.00),
            (7, 'filtered', 'seven', 'Other', 'same', 4.00);
        """
    )
    connection.execute(
        f"INSERT INTO {channel}_sales ({prefix}_item_sk, {prefix}_order_number, "
        f"{prefix}_sold_date_sk, {prefix}_ext_sales_price) VALUES "
        "(1, 1, 1, 0.01), (1, 1, 1, 0.01), (2, 2, 2, 0.05), (3, 3, 1, NULL), "
        "(4, 4, 1, 0.01), (5, 5, 2, 0.02), (6, 6, 1, NULL), (7, 7, 1, 99.99), "
        "(1, 8, 3, 99.99), (1, 9, 4, 99.99)"
    )
    rows = _equivalent_rows(connection, name)
    assert [row[0] for row in rows] == list("abcdef")
    assert [row[5] for row in rows] == [
        Decimal("0.02"),
        Decimal("0.05"),
        None,
        Decimal("0.01"),
        Decimal("0.02"),
        None,
    ]
    assert rows[0][6] == pytest.approx(200 / 7, rel=1e-12)
    assert rows[1][6] == pytest.approx(500 / 7, rel=1e-12)
    assert rows[2][6] is None
    assert rows[3][6] == pytest.approx(100 / 3, rel=1e-12)
    assert rows[4][6] == pytest.approx(200 / 3, rel=1e-12)
    assert rows[5][6] is None


def test_return_ratios_are_widened_before_ranking_with_boundary_ties_in_all_channels(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    connection.execute("INSERT INTO date_dim (d_date_sk, d_year, d_moy) VALUES (1, 2001, 12), (2, 2001, 11)")
    for channel, sales, returns, order, return_amount in (
        ("web", "ws", "wr", "order_number", "return_amt"),
        ("catalog", "cs", "cr", "order_number", "return_amount"),
        ("store", "ss", "sr", "ticket_number", "return_amt"),
    ):
        connection.execute(
            f"INSERT INTO {channel}_sales ({sales}_item_sk, {sales}_{order}, {sales}_sold_date_sk, "
            f"{sales}_net_profit, {sales}_net_paid, {sales}_quantity) "
            "SELECT i, i, 1, 2.00, 33003.00, 3000000 FROM range(1, 26) fixture(i)"
        )
        connection.execute(
            f"INSERT INTO {channel}_returns ({returns}_item_sk, {returns}_{order}, "
            f"{returns}_return_quantity, {returns}_{return_amount}) "
            "SELECT i, i, CASE WHEN i = 11 THEN 10 ELSE i END, "
            "(CASE WHEN i = 15 THEN 10 ELSE 26 - i END) * 1000 + 10001.00 FROM range(1, 26) fixture(i)"
        )
        connection.execute(f"INSERT INTO {channel}_sales SELECT * FROM {channel}_sales WHERE {sales}_item_sk = 1")
        connection.execute(f"INSERT INTO {channel}_returns SELECT * FROM {channel}_returns WHERE {returns}_item_sk = 2")
        connection.execute(
            f"INSERT INTO {channel}_sales ({sales}_item_sk, {sales}_{order}, {sales}_sold_date_sk, "
            f"{sales}_net_profit, {sales}_net_paid, {sales}_quantity) VALUES "
            "(26, 26, 2, 2.00, 33003.00, 3000000), (27, 27, 1, 2.00, 33003.00, 3000000), "
            "(28, 28, 1, 1.00, 33003.00, 3000000), (29, 29, 1, 2.00, 0.00, 3000000), "
            "(30, 30, 1, 2.00, 33003.00, 0), (31, 31, 1, 2.00, 33003.00, 3000000), "
            "(32, 32, 1, 2.00, 33003.00, 3000000), (33, 33, 1, 2.00, NULL, 3000000), "
            "(34, 34, 1, 2.00, 33003.00, NULL), (35, 35, NULL, 2.00, 33003.00, 3000000)"
        )
        connection.execute(
            f"INSERT INTO {channel}_returns ({returns}_item_sk, {returns}_{order}, "
            f"{returns}_return_quantity, {returns}_{return_amount}) VALUES "
            "(26, 26, 1, 10001.00), (27, 27, 1, 10000.00), (28, 28, 1, 10001.00), "
            "(29, 29, 1, 10001.00), (30, 30, 1, 10001.00), (32, 32, NULL, NULL), "
            "(33, 33, 1, 10001.00), (34, 34, 1, 10001.00), (35, 35, 1, 10001.00)"
        )
    rows = _equivalent_rows(connection, "49")
    expected = sorted(
        (
            (
                channel,
                item,
                (10 if item == 11 else item) / 3000000,
                10 if item == 11 else item,
                10 if item == 15 else 26 - item,
            )
            for channel in ("web", "catalog", "store")
            for item in (*range(1, 12), *range(15, 26))
        ),
        key=lambda row: (row[0], row[3], row[4], row[1]),
    )
    assert len(rows) == 66
    for actual, reference in zip(rows, expected, strict=True):
        assert actual[:2] == reference[:2]
        assert actual[2] == pytest.approx(reference[2], rel=1e-12, abs=1e-15)
        assert actual[3:] == reference[3:]


def test_shipping_delay_preserves_calendar_days_nulls_and_both_left_join_multiplicities(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    dates: list[tuple[str | None, str | None]] = [
        ("1999-01-28", "1999-02-02"),
        ("1999-01-28", "1999-02-03"),
        ("1999-02-25", "1999-03-02"),
        ("1999-02-25", "1999-03-03"),
        ("1999-12-29", "2000-01-03"),
        ("1999-12-29", "2000-01-04"),
        ("1999-06-10", "1999-06-16"),
        ("1999-06-10", "1999-06-15"),
        (None, "1999-06-16"),
        ("1999-06-10", None),
        ("1999-03-01", "1999-02-28"),
    ]
    connection.execute(
        """
        INSERT INTO warehouse (w_warehouse_sk, w_warehouse_name) VALUES (1, 'Warehouse'), (2, NULL);
        INSERT INTO customer_demographics (cd_demo_sk, cd_marital_status) VALUES (1, 'D');
        INSERT INTO household_demographics (hd_demo_sk, hd_buy_potential) VALUES (1, '>10000');
        INSERT INTO promotion (p_promo_sk) VALUES (1), (1);
        """
    )
    for item, (sold, shipped) in enumerate(dates, start=1):
        connection.executemany(
            "INSERT INTO date_dim (d_date_sk, d_date, d_week_seq, d_year) VALUES (?, ?, ?, ?)",
            [(item * 2, sold, item, 1999), (item * 2 + 1, shipped, item + 100, 2000)],
        )
        connection.execute(
            "INSERT INTO item (i_item_sk, i_item_desc) VALUES (?, ?)",
            [item, None if item == 7 else f"Item {item:02}"],
        )
        connection.execute(
            "INSERT INTO inventory (inv_item_sk, inv_date_sk, inv_warehouse_sk, inv_quantity_on_hand) "
            "VALUES (?, ?, ?, 1)",
            [item, item * 2, 2 if item == 6 else 1],
        )
        connection.execute(
            "INSERT INTO catalog_sales (cs_item_sk, cs_order_number, cs_sold_date_sk, cs_ship_date_sk, "
            "cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_promo_sk, cs_quantity) VALUES (?, ?, ?, ?, 1, 1, ?, 2)",
            [item, item, item * 2, item * 2 + 1, None if item == 7 else 1 if item % 4 == 0 else 2],
        )
    connection.execute("INSERT INTO catalog_sales SELECT * FROM catalog_sales WHERE cs_item_sk = 2")
    connection.execute(
        "INSERT INTO catalog_returns (cr_item_sk, cr_order_number) VALUES (2, 2), (2, 2), (4, 4), (6, 6)"
    )
    assert _equivalent_rows(connection, "72") == [
        ("Item 02", "Warehouse", 2, 4, 0, 4),
        ("Item 04", "Warehouse", 4, 0, 2, 2),
        (None, "Warehouse", 7, 1, 0, 1),
        ("Item 06", None, 6, 1, 0, 1),
    ]
