import math
from collections.abc import Iterator
from decimal import Decimal
from typing import cast

import duckdb
import pytest

from ..query_registry import SUITES_DIRECTORY
from ..suites.tpc_ds.config import TPCDS_TABLES

TPCDS = SUITES_DIRECTORY / "tpc_ds"
RANKED_QUERIES = ("70", "86")
QUERIES = ("05", "14", "18", "22", "67", "77", "80", *RANKED_QUERIES)
type Row = tuple[object, ...]


def _query(name: str, *, adapted: bool) -> str:
    directory = TPCDS / "queries"
    return ((directory / "doris" if adapted else directory) / f"{name}.sql").read_text()


def _assert_equivalent(connection: duckdb.DuckDBPyConnection, name: str) -> list[Row]:
    expected = cast(list[Row], connection.execute(_query(name, adapted=False)).fetchall())
    actual = cast(list[Row], connection.execute(_query(name, adapted=True)).fetchall())
    assert len(actual) == len(expected), name
    if name == "18":
        expected = sorted(expected, key=repr)
        actual = sorted(actual, key=repr)
    for left, right in zip(actual, expected, strict=True):
        assert len(left) == len(right)
        for value, canonical in zip(left, right, strict=True):
            if isinstance(value, (int, float, Decimal)) and isinstance(canonical, (int, float, Decimal)):
                assert math.isclose(float(value), float(canonical), rel_tol=1e-12, abs_tol=1e-9), (name, left, right)
            else:
                assert value == canonical, (name, left, right)
    return actual


@pytest.fixture
def empty_tpcds() -> Iterator[duckdb.DuckDBPyConnection]:
    with duckdb.connect(config={"threads": 1}) as connection:
        connection.execute((TPCDS / "schemas/duckdb.sql").read_text())
        yield connection


@pytest.fixture(scope="module")
def generated_tpcds() -> Iterator[duckdb.DuckDBPyConnection]:
    with duckdb.connect(config={"threads": 1}) as connection:
        connection.execute("LOAD tpcds")
        assert connection.execute(
            "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'tpcds'"
        ).fetchone() == (f"v{duckdb.__version__}",)
        connection.execute("CALL dsdgen(sf=0.01)")
        connection.execute("ALTER TABLE store RENAME COLUMN s_tax_percentage TO s_tax_precentage")
        connection.execute("CREATE SCHEMA fixture")
        connection.execute("SET schema = 'fixture'")
        connection.execute((TPCDS / "schemas/duckdb.sql").read_text())
        for table in TPCDS_TABLES:
            columns = [str(row[0]) for row in connection.execute(f"DESCRIBE fixture.{table}").fetchall()]
            names = ", ".join(f'"{column}"' for column in columns)
            connection.execute(f"INSERT INTO fixture.{table} SELECT {names} FROM main.{table}")
            connection.execute(f"DROP TABLE main.{table}")
        yield connection


@pytest.mark.parametrize("name", QUERIES)
def test_rollup_preserves_canonical_sort_and_limit(name: str) -> None:
    original = _query(name, adapted=False)
    adapted = _query(name, adapted=True)
    order = " ".join(original[original.rindex("\nORDER BY") :].split())
    if name in RANKED_QUERIES:
        parent, child, key = (
            ("s_state", "s_county", "order_state") if name == "70" else ("i_category", "i_class", "order_category")
        )
        order = order.replace(f"CASE WHEN grouping({parent})+grouping({child}) = 0 THEN {parent} END", key)
    assert "".join(adapted[adapted.rindex("\nORDER BY") :].split()) == "".join(order.split())


@pytest.mark.parametrize("name", QUERIES)
def test_empty_rollup_keeps_one_null_grand_total(empty_tpcds: duckdb.DuckDBPyConnection, name: str) -> None:
    rows = _assert_equivalent(empty_tpcds, name)
    assert len(rows) == 1
    if name == "67":
        assert rows == [(None,) * 9 + (1,)]
    elif name in RANKED_QUERIES:
        assert rows == [(None, None, None, 2, 1)]
    else:
        assert all(value is None for value in rows[0])


@pytest.mark.parametrize("name", QUERIES)
def test_rollup_matches_canonical_typed_generated_data(generated_tpcds: duckdb.DuckDBPyConnection, name: str) -> None:
    _assert_equivalent(generated_tpcds, name)


@pytest.fixture
def adversarial_tpcds(empty_tpcds: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    empty_tpcds.execute(
        """
        INSERT INTO date_dim (d_date_sk, d_date, d_year, d_month_seq, d_qoy, d_moy) VALUES
            (1, DATE '2000-08-24', 1998, 1200, 1, 1),
            (2, DATE '2001-11-01', 2001, 1234, 4, 11);
        INSERT INTO item (i_item_sk, i_item_id, i_current_price, i_brand_id, i_class_id, i_category_id,
                          i_brand, i_class, i_category, i_product_name) VALUES
            (1, 'item', 60, 1, 1, 1, 'brand', 'class', 'category', 'product'),
            (2, NULL, 60, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
        INSERT INTO store (s_store_sk, s_store_id, s_state, s_county) VALUES
            (1, 'store', 'AA', 'county'), (2, NULL, NULL, NULL);
        INSERT INTO catalog_page (cp_catalog_page_sk, cp_catalog_page_id) VALUES (1, 'page'), (2, NULL);
        INSERT INTO web_site (web_site_sk, web_site_id) VALUES (1, 'site'), (2, NULL);
        INSERT INTO web_page (wp_web_page_sk) VALUES (1), (2);
        INSERT INTO promotion (p_promo_sk, p_channel_tv) VALUES (1, 'N');
        INSERT INTO customer_demographics
            (cd_demo_sk, cd_gender, cd_education_status, cd_dep_count) VALUES (1, 'F', 'Unknown', 2);
        INSERT INTO customer_address (ca_address_sk, ca_country, ca_state, ca_county) VALUES
            (1, 'US', 'MS', 'county'), (2, NULL, 'MS', NULL);
        INSERT INTO customer
            (c_customer_sk, c_current_cdemo_sk, c_current_addr_sk, c_birth_month, c_birth_year) VALUES
            (1, 1, 1, 1, 1960), (2, 1, 2, 1, NULL);
        INSERT INTO inventory VALUES (1, 1, 1, 4), (1, 1, 1, 4), (1, 1, 2, 9), (1, 2, 1, NULL);
        INSERT INTO store_sales
            (ss_sold_date_sk, ss_item_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity,
             ss_list_price, ss_sales_price, ss_ext_sales_price, ss_net_profit) VALUES
            (1, 1, 1, 1, 1, 2, 10.25, 5.25, 10.50, 3.25),
            (1, 1, 1, 1, 1, 2, 10.25, 5.25, 10.50, 3.25),
            (1, 2, 2, 1, 2, NULL, NULL, NULL, NULL, NULL),
            (2, 1, 1, 1, 3, 2, 10.25, 5.25, 10.50, 3.25),
            (2, 1, 1, 1, 3, 2, 10.25, 5.25, 10.50, 3.25);
        INSERT INTO store_returns
            (sr_returned_date_sk, sr_item_sk, sr_store_sk, sr_ticket_number, sr_return_amt, sr_net_loss) VALUES
            (1, 1, 1, 1, 1.25, 0.25), (1, 1, 1, 1, 1.25, 0.25), (1, 2, 2, 2, NULL, NULL);
        INSERT INTO catalog_sales
            (cs_sold_date_sk, cs_item_sk, cs_catalog_page_sk, cs_call_center_sk, cs_promo_sk, cs_order_number,
             cs_bill_customer_sk, cs_bill_cdemo_sk, cs_quantity, cs_list_price, cs_coupon_amt, cs_sales_price,
             cs_ext_sales_price, cs_net_profit) VALUES
            (1, 1, 1, 1, 1, 1, 1, 1, 2, 10.25, 0.25, 5.25, 10.50, 3.25),
            (1, 1, 1, 1, 1, 1, 1, 1, 2, 10.25, 0.25, 5.25, 10.50, 3.25),
            (1, 2, 2, NULL, 1, 2, 2, 1, NULL, NULL, NULL, NULL, NULL, NULL),
            (2, 1, 1, 1, 1, 3, 1, 1, 2, 10.25, 0.25, 5.25, 10.50, 3.25);
        INSERT INTO catalog_returns
            (cr_returned_date_sk, cr_item_sk, cr_catalog_page_sk, cr_call_center_sk,
             cr_order_number, cr_return_amount, cr_net_loss) VALUES
            (1, 1, 1, 1, 1, 1.25, 0.25), (1, 1, 1, 1, 1, 1.25, 0.25), (1, 2, 2, NULL, 2, NULL, NULL);
        INSERT INTO web_sales
            (ws_sold_date_sk, ws_item_sk, ws_web_site_sk, ws_web_page_sk, ws_promo_sk,
             ws_order_number, ws_quantity, ws_list_price, ws_sales_price, ws_ext_sales_price,
             ws_net_profit, ws_net_paid) VALUES
            (1, 1, 1, 1, 1, 1, 2, 10.25, 5.25, 10.50, 3.25, 10.50),
            (1, 1, 1, 1, 1, 1, 2, 10.25, 5.25, 10.50, 3.25, 10.50),
            (1, 2, 2, 2, 1, 2, NULL, NULL, NULL, NULL, NULL, NULL),
            (2, 1, 1, 1, 1, 3, 2, 10.25, 5.25, 10.50, 3.25, 10.50);
        INSERT INTO web_returns
            (wr_returned_date_sk, wr_item_sk, wr_web_page_sk, wr_order_number, wr_return_amt, wr_net_loss) VALUES
            (1, 1, 1, 1, 1.25, 0.25), (1, 1, 1, 1, 1.25, 0.25), (1, 2, 2, 2, NULL, NULL);
        """
    )
    return empty_tpcds


@pytest.mark.parametrize("name", QUERIES)
def test_rollup_keeps_null_keys_null_measures_and_duplicate_facts(
    adversarial_tpcds: duckdb.DuckDBPyConnection, name: str
) -> None:
    rows = _assert_equivalent(adversarial_tpcds, name)
    assert len(rows) > 1


@pytest.mark.parametrize("name", QUERIES)
def test_nonempty_tables_with_no_qualifying_input_keep_one_grand_total(
    adversarial_tpcds: duckdb.DuckDBPyConnection, name: str
) -> None:
    adversarial_tpcds.execute("UPDATE date_dim SET d_date = NULL, d_year = NULL, d_month_seq = NULL")
    assert len(_assert_equivalent(adversarial_tpcds, name)) == 1


@pytest.mark.parametrize("name", ["22", "67"])
@pytest.mark.parametrize("values", [[None], [3], [3, 3], [None, 3, 9]])
def test_rollup_retains_duplicate_null_groups_at_every_level(
    empty_tpcds: duckdb.DuckDBPyConnection, name: str, values: list[int | None]
) -> None:
    empty_tpcds.execute("INSERT INTO date_dim (d_date_sk, d_month_seq) VALUES (1, 1200)")
    empty_tpcds.execute("INSERT INTO item (i_item_sk) VALUES (1)")
    if name == "22":
        empty_tpcds.executemany("INSERT INTO inventory VALUES (1, 1, 1, ?)", [(value,) for value in values])
    else:
        empty_tpcds.execute("INSERT INTO store (s_store_sk) VALUES (1)")
        empty_tpcds.executemany(
            "INSERT INTO store_sales (ss_sold_date_sk, ss_item_sk, ss_store_sk, ss_ticket_number, "
            "ss_quantity, ss_sales_price) VALUES (1, 1, 1, 1, ?, 1)",
            [(value,) for value in values],
        )
    rows = _assert_equivalent(empty_tpcds, name)
    assert len(rows) == (5 if name == "22" else 9)
    assert all(row == rows[0] for row in rows)


@pytest.mark.parametrize("name", ["22", "67"])
def test_rollup_sorts_and_limits_after_combining_all_grouping_levels(
    empty_tpcds: duckdb.DuckDBPyConnection, name: str
) -> None:
    empty_tpcds.execute("INSERT INTO date_dim (d_date_sk, d_month_seq) VALUES (1, 1200)")
    empty_tpcds.execute(
        "INSERT INTO item (i_item_sk, i_product_name, i_brand, i_class, i_category) "
        "SELECT value, 'product' || lpad(CAST(value AS VARCHAR), 3, '0'), 'brand', 'class', 'category' "
        "FROM range(1, 131) fixture(value)"
    )
    if name == "22":
        empty_tpcds.execute("INSERT INTO inventory SELECT 1, i_item_sk, 1, i_item_sk FROM item")
    else:
        empty_tpcds.execute("INSERT INTO store (s_store_sk, s_store_id) VALUES (1, 'store')")
        empty_tpcds.execute(
            "INSERT INTO store_sales (ss_sold_date_sk, ss_item_sk, ss_store_sk, ss_ticket_number, "
            "ss_quantity, ss_sales_price) SELECT 1, i_item_sk, 1, i_item_sk, 1, i_item_sk FROM item"
        )
    assert len(_assert_equivalent(empty_tpcds, name)) == 100


@pytest.mark.parametrize("name", RANKED_QUERIES)
@pytest.mark.parametrize("parent", [None, "AA"])
@pytest.mark.parametrize("values", [[None], [3], [3, 3], [None, 3, 9]])
def test_ranked_rollup_distinguishes_null_keys_from_rolled_up_keys_and_keeps_duplicate_facts(
    empty_tpcds: duckdb.DuckDBPyConnection, name: str, parent: str | None, values: list[int | None]
) -> None:
    empty_tpcds.execute("INSERT INTO date_dim (d_date_sk, d_month_seq) VALUES (1, 1200)")
    if name == "70":
        empty_tpcds.execute("INSERT INTO store (s_store_sk, s_state) VALUES (1, ?)", [parent])
        empty_tpcds.executemany(
            "INSERT INTO store_sales "
            "(ss_sold_date_sk, ss_item_sk, ss_store_sk, ss_ticket_number, ss_net_profit) VALUES (1, 1, 1, 1, ?)",
            [(value,) for value in values],
        )
    else:
        empty_tpcds.execute("INSERT INTO item (i_item_sk, i_category) VALUES (1, ?)", [parent])
        empty_tpcds.executemany(
            "INSERT INTO web_sales (ws_sold_date_sk, ws_item_sk, ws_order_number, ws_net_paid) VALUES (1, 1, 1, ?)",
            [(value,) for value in values],
        )
    rows = _assert_equivalent(empty_tpcds, name)
    if name == "70" and parent is None:
        assert rows == [(None, None, None, 2, 1)]
    else:
        total = (
            sum(value for value in values if value is not None) if any(value is not None for value in values) else None
        )
        assert rows == [(total, None, None, 2, 1), (total, parent, None, 1, 1), (total, parent, None, 0, 1)]
