import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import duckdb
import polars as pl
import pytest
from polars.testing import assert_frame_equal
from pymysql.constants import FIELD_TYPE
from sqlalchemy import Connection

from ..dbs import doris as doris_module
from ..dbs.doris import (
    DORIS_RESULT_CACHE_VARIABLES,
    Doris,
    DorisFetchMethod,
    DorisStatisticsRows,
    DorisTpcDs,
    DorisTpcH,
)
from ..settings import REPO_ROOT, SuiteName
from ..suites.tpc_ds.config import TPCDS_QUERY_NAMES, TPCDS_TABLES
from ..suites.tpc_h.config import TPCH_QUERY_NAMES, TPCH_TABLES

SUITES = REPO_ROOT / "olap_benchmarks/suites"
TPCH_OVERRIDES = {
    "01_pricing_summary",
    "08_market_share",
    "13_customer_distribution",
    "14_promotion_effect",
    "17_small_quantity_revenue",
    "22_global_sales_opportunity",
}
TPCDS_RANKED_ROLLUP_OVERRIDES = {"70", "86"}
TPCDS_EMPTY_ROLLUP_OVERRIDES = {"05", "14", "18", "22", "67", "77", "80"} | TPCDS_RANKED_ROLLUP_OVERRIDES
TPCDS_OVERRIDES = {
    "01",
    "02",
    "04",
    "06",
    "07",
    "09",
    "11",
    "12",
    "13",
    "14",
    "17",
    "18",
    "20",
    "21",
    "24",
    "26",
    "27",
    "28",
    "30",
    "31",
    "32",
    "34",
    "36",
    "39",
    "44",
    "45",
    "47",
    "49",
    "50",
    "53",
    "57",
    "58",
    "59",
    "61",
    "63",
    "65",
    "66",
    "72",
    "73",
    "74",
    "75",
    "78",
    "81",
    "83",
    "85",
    "89",
    "90",
    "92",
    "98",
} | TPCDS_EMPTY_ROLLUP_OVERRIDES

type JsonValue = dict[str, JsonValue] | list[JsonValue] | str | int | float | bool | None


def _logical_schema(suite: SuiteName) -> str:
    source = (SUITES / suite / "schemas/doris.sql").read_text()
    return re.sub(r"\) ENGINE = OLAP\n.*?;", ");", source, flags=re.DOTALL)


def _column_contracts(sql: str) -> dict[str, dict[str, str]]:
    return {
        table: dict(line.strip().removesuffix(",").split(" ", 1) for line in columns.splitlines())
        for table, columns in re.findall(r"CREATE TABLE (\w+) \(\n(.*?)\n\)", sql, flags=re.DOTALL)
    }


@pytest.mark.parametrize(("suite", "scale"), [("tpc_h", 10), ("tpc_h", 50), ("tpc_ds", 1), ("tpc_ds", 10)])
def test_doris_relational_registry_uses_every_canonical_query_and_python_fetch(suite: SuiteName, scale: int) -> None:
    database = Doris()
    database._current_suite = suite
    database._current_suite_scale_factor = scale
    benchmark = database.benchmarks[suite]
    expected = TPCH_QUERY_NAMES if suite == "tpc_h" else TPCDS_QUERY_NAMES

    assert isinstance(benchmark, DorisTpcH if suite == "tpc_h" else DorisTpcDs)
    assert benchmark.planned_query_names() == expected
    assert benchmark.scale_factor == scale
    assert database.run_options["select_fetch_method"] == "python"
    assert database.run_options["relational_statistics_policy"] == "full-synchronous-v1"
    assert database.run_options["relational_statistics_readiness_policy"] == "full-columns-fe-cache-v1"
    assert database.run_options["relational_statistics_readiness_timeout_seconds"] == 60
    assert database.run_options["session_settings"] == dict.fromkeys(DORIS_RESULT_CACHE_VARIABLES, False)
    for query in expected:
        assert benchmark.query_fetch_kwargs(query) == {"method": "python"}
        overrides = TPCH_OVERRIDES if suite == "tpc_h" else TPCDS_OVERRIDES
        directory = benchmark.queries_directory / "doris" if query in overrides else benchmark.queries_directory
        assert benchmark.query_path(query) == directory / f"{query}.sql"
        assert benchmark.query_skip(query) is None
    if suite == "tpc_h":
        expected_fraction = "0.00001" if scale == 10 else "0.000002"
        assert expected_fraction in benchmark.load_query("11_important_stock")


@pytest.mark.parametrize(("suite", "tables"), [("tpc_h", TPCH_TABLES), ("tpc_ds", TPCDS_TABLES)])
def test_doris_relational_layout_preserves_all_column_types_nullability_and_rows(
    suite: SuiteName, tables: tuple[str, ...]
) -> None:
    source = (SUITES / suite / "schemas/doris.sql").read_text()
    canonical = (SUITES / suite / "schemas/duckdb.sql").read_text()
    assert _column_contracts(source) == _column_contracts(canonical)
    layouts = re.findall(
        r"CREATE TABLE (\w+) \(\n(.*?)\n\) ENGINE = OLAP\n"
        r"DUPLICATE KEY \((.*?)\)\nDISTRIBUTED BY HASH \((.*?)\) BUCKETS (\d+)\n"
        r'PROPERTIES \("replication_num" = "1"\);',
        source,
        flags=re.DOTALL,
    )
    assert {table for table, *_ in layouts} == set(tables)
    for _table, declarations, sort_key, bucket_key, bucket_count in layouts:
        columns = [line.strip().split()[0] for line in declarations.splitlines()]
        keys = sort_key.split(", ")
        assert columns[: len(keys)] == keys
        assert set(bucket_key.split(", ")) <= set(columns)
        assert 1 <= int(bucket_count) <= (32 if suite == "tpc_h" else 16)
    assert "AGGREGATE KEY" not in source
    assert "UNIQUE KEY" not in source
    assert "CREATE VIEW" not in source
    assert "colocate_with" not in source


@pytest.mark.parametrize("suite", ["tpc_h", "tpc_ds"])
def test_doris_relational_queries_bind_to_the_logical_schema_in_memory(suite: SuiteName) -> None:
    benchmark = Doris().benchmarks[suite]
    with duckdb.connect() as connection:
        connection.execute("SET threads = 1")
        connection.execute(_logical_schema(suite))
        for query in benchmark.planned_query_names():
            connection.execute("EXPLAIN " + benchmark.load_query(query))


def _without_double_casts(node: JsonValue) -> JsonValue:
    if isinstance(node, list):
        return [_without_double_casts(value) for value in node]
    if not isinstance(node, dict):
        return node
    type_expr = node.get("type_expr")
    if node.get("class") == "CAST" and isinstance(type_expr, dict) and type_expr.get("type_name") == "DOUBLE":
        return _without_double_casts(node["child"])
    return {key: _without_double_casts(value) for key, value in node.items() if not key.startswith("query_location")}


def _sql_tree(connection: duckdb.DuckDBPyConnection, query: str) -> JsonValue:
    row = connection.execute("SELECT json_serialize_sql(?)", [query]).fetchone()
    assert row is not None
    tree = cast(dict[str, JsonValue], json.loads(str(row[0])))
    assert tree["error"] is False
    return _without_double_casts(tree)


@pytest.mark.parametrize("suite", ["tpc_h", "tpc_ds"])
def test_doris_relational_overrides_only_widen_numeric_expressions(suite: SuiteName) -> None:
    directory = SUITES / suite / "queries"
    overrides = TPCH_OVERRIDES if suite == "tpc_h" else TPCDS_OVERRIDES
    assert {path.stem for path in (directory / "doris").glob("*.sql")} == overrides
    with duckdb.connect() as connection:
        for name in sorted(overrides):
            if name in {"13_customer_distribution", "12", "17", "39", "45", "49", "50", "72"}:
                continue
            if suite == "tpc_ds" and name in TPCDS_EMPTY_ROLLUP_OVERRIDES:
                continue
            queries = [(directory / f"{name}.sql").read_text(), (directory / "doris" / f"{name}.sql").read_text()]
            assert _sql_tree(connection, queries[0]) == _sql_tree(connection, queries[1]), name


def test_doris_return_ranking_only_widens_ratios_and_matches_catalog_alias_case() -> None:
    directory = SUITES / "tpc_ds/queries"
    original = (directory / "49.sql").read_text()
    adapted = (directory / "doris/49.sql").read_text()
    assert ") in_cat) catalog" in adapted
    with duckdb.connect() as connection:
        assert _sql_tree(connection, original.replace(") in_cat) CATALOG", ") in_cat) catalog")) == _sql_tree(
            connection, adapted
        )


@pytest.mark.parametrize(
    ("name", "parent", "child", "measure", "order_column"),
    [
        ("70", "s_state", "s_county", "ss_net_profit", "order_state"),
        ("86", "i_category", "i_class", "ws_net_paid", "order_category"),
    ],
)
def test_doris_ranked_rollup_only_splits_off_the_scalar_total_before_ranking(
    name: str, parent: str, child: str, measure: str, order_column: str
) -> None:
    directory = SUITES / "tpc_ds/queries"
    original = " ".join((directory / f"{name}.sql").read_text().split())
    adapted = (directory / "doris" / f"{name}.sql").read_text()
    projection, source = original.split(" FROM ", 1)
    source, grouping_order = source.rsplit(" GROUP BY rollup(", 1)
    _grouping, order = grouping_order.split(" ORDER BY ", 1)
    hierarchy = f"grouping({parent})+grouping({child})"
    parent_key = f"CASE WHEN grouping({child}) = 0 THEN {parent} END"
    sort_key = f"CASE WHEN {hierarchy} = 0 THEN {parent} END"
    ranked = (
        projection.replace(f"sum({measure}) AS total_sum", "total_sum")
        .replace(
            f"{hierarchy} AS lochierarchy",
            f"lochierarchy, CASE WHEN lochierarchy = 0 THEN {parent} END AS {order_column}",
        )
        .replace(f"PARTITION BY {hierarchy}, {parent_key}", "PARTITION BY lochierarchy, parent_key")
        .replace(f"ORDER BY sum({measure}) DESC", "ORDER BY total_sum DESC")
    )
    expected = (
        f"WITH rollup_input AS (SELECT {measure}, {parent}, {child} FROM {source}), "
        f"rollup_output AS (SELECT sum({measure}) AS total_sum, {parent}, {child}, "
        f"{hierarchy} AS lochierarchy, {parent_key} AS parent_key FROM rollup_input "
        f"GROUP BY GROUPING SETS (({parent}, {child}), ({parent})) UNION ALL "
        f"SELECT sum({measure}) AS total_sum, NULL AS {parent}, NULL AS {child}, "
        "2 AS lochierarchy, NULL AS parent_key FROM rollup_input) "
        f"SELECT total_sum, {parent}, {child}, lochierarchy, rank_within_parent "
        f"FROM ({ranked} FROM rollup_output) AS grouped ORDER BY {order.replace(sort_key, order_column)}"
    )
    with duckdb.connect() as connection:
        assert _sql_tree(connection, adapted) == _sql_tree(connection, expected)


def test_doris_shipping_delay_only_replaces_integer_date_arithmetic_with_calendar_days() -> None:
    directory = SUITES / "tpc_ds/queries"
    original = (directory / "72.sql").read_text()
    adapted = (directory / "doris/72.sql").read_text()
    assert adapted == original.replace("d1.d_date + 5", "DATE_ADD(d1.d_date, INTERVAL 5 DAY)")


@pytest.mark.parametrize("empty", [False, True])
def test_doris_return_ranking_preserves_all_channels_ties_filtering_and_join_multiplicity(empty: bool) -> None:
    benchmark = Doris().benchmarks["tpc_ds"]
    with duckdb.connect() as connection:
        connection.execute("SET threads = 1")
        connection.execute(_logical_schema("tpc_ds"))
        connection.execute("INSERT INTO date_dim (d_date_sk, d_year, d_moy) VALUES (1, 2001, 12), (2, 2001, 11)")
        if not empty:
            for sales, returns, sales_order, return_order, return_amount in [
                ("ws", "wr", "order_number", "order_number", "return_amt"),
                ("cs", "cr", "order_number", "order_number", "return_amount"),
                ("ss", "sr", "ticket_number", "ticket_number", "return_amt"),
            ]:
                channel = {"ws": "web", "cs": "catalog", "ss": "store"}[sales]
                connection.execute(
                    f"INSERT INTO {channel}_sales ({sales}_item_sk, {sales}_{sales_order}, "
                    f"{sales}_sold_date_sk, {sales}_net_profit, {sales}_net_paid, {sales}_quantity) "
                    "SELECT i, i, 1, 2.00, 10001.00, 100 FROM range(1, 26) fixture(i)"
                )
                connection.execute(
                    f"INSERT INTO {channel}_returns ({returns}_item_sk, {returns}_{return_order}, "
                    f"{returns}_return_quantity, {returns}_{return_amount}) "
                    "SELECT i, i, CASE WHEN i = 11 THEN 10 ELSE i END, (26 - i) * 1000 + 10001.00 "
                    "FROM range(1, 26) fixture(i)"
                )
                connection.execute(
                    f"INSERT INTO {channel}_sales SELECT * FROM {channel}_sales WHERE {sales}_item_sk = 1"
                )
                connection.execute(
                    f"INSERT INTO {channel}_returns SELECT * FROM {channel}_returns WHERE {returns}_item_sk = 2"
                )
                connection.execute(
                    f"INSERT INTO {channel}_sales ({sales}_item_sk, {sales}_{sales_order}, "
                    f"{sales}_sold_date_sk, {sales}_net_profit, {sales}_net_paid, {sales}_quantity) VALUES "
                    "(26, 26, 2, 2.00, 10001.00, 100), (27, 27, 1, 2.00, 10001.00, 100), "
                    "(28, 28, 1, 1.00, 10001.00, 100), (29, 29, 1, 2.00, 0.00, 100), "
                    "(30, 30, 1, 2.00, 10001.00, 0), (31, 31, 1, 2.00, 10001.00, 100)"
                )
                connection.execute(
                    f"INSERT INTO {channel}_returns ({returns}_item_sk, {returns}_{return_order}, "
                    f"{returns}_return_quantity, {returns}_{return_amount}) VALUES "
                    "(26, 26, 1, 10001.00), (27, 27, 1, 10000.00), (28, 28, 1, 10001.00), "
                    "(29, 29, 1, 10001.00), (30, 30, 1, 10001.00)"
                )
        reference = connection.sql((benchmark.queries_directory / "49.sql").read_text()).pl()
        actual = connection.sql(benchmark.load_query("49")).pl()
        assert_frame_equal(actual, reference)
        assert actual.height == (0 if empty else 63)
        if not empty:
            expected_items = {*range(1, 12), *range(16, 26)}
            for channel in ("web", "catalog", "store"):
                rows = actual.filter(pl.col("channel") == channel)
                assert set(rows["item"].to_list()) == expected_items
                assert rows.filter(pl.col("item").is_in([10, 11]))["return_rank"].to_list() == [10, 10]


@pytest.mark.parametrize("name", sorted(TPCDS_RANKED_ROLLUP_OVERRIDES))
@pytest.mark.parametrize("mode", ["empty", "filtered", "populated", "limited", "duplicates", "null_measures"])
def test_doris_rollup_ranking_preserves_empty_totals_null_groups_ties_and_ordering(name: str, mode: str) -> None:
    benchmark = Doris().benchmarks["tpc_ds"]
    with duckdb.connect() as connection:
        connection.execute("SET threads = 1")
        if name == "70":
            dimension, key, parent, child = "store", "s_store_sk", "s_state", "s_county"
            sales, sales_key, measure = "store_sales", "ss_store_sk", "ss_net_profit"
            date_key = "ss_sold_date_sk"
        else:
            dimension, key, parent, child = "item", "i_item_sk", "i_category", "i_class"
            sales, sales_key, measure = "web_sales", "ws_item_sk", "ws_net_paid"
            date_key = "ws_sold_date_sk"
        connection.execute("CREATE TABLE date_dim (d_date_sk INTEGER, d_month_seq INTEGER)")
        connection.execute(f"CREATE TABLE {dimension} ({key} INTEGER, {parent} VARCHAR, {child} VARCHAR)")
        connection.execute(f"CREATE TABLE {sales} ({sales_key} INTEGER, {date_key} INTEGER, {measure} DECIMAL(7, 2))")
        connection.execute("INSERT INTO date_dim (d_date_sk, d_month_seq) VALUES (1, 1200), (2, 1212)")
        if mode != "empty":
            connection.execute(
                f"INSERT INTO {dimension} ({key}, {parent}, {child}) VALUES "
                "(1, 'A', 'x'), (2, 'A', 'y'), (3, 'A', NULL), (4, 'B', 'x'), (5, 'B', 'y'), "
                "(6, NULL, 'x'), (7, NULL, NULL), (8, 'C', 'z'), (9, 'C', 'n')"
            )
            sold_date = 2 if mode == "filtered" else 1
            connection.execute(
                f"INSERT INTO {sales} ({sales_key}, {date_key}, {measure}) VALUES "
                f"(1, {sold_date}, 10.01), (1, {sold_date}, 10.01), (2, {sold_date}, 20.02), "
                f"(3, {sold_date}, 5.00), (4, {sold_date}, -1.00), (5, {sold_date}, NULL), "
                f"(6, {sold_date}, 2.00), (7, {sold_date}, 3.00), (8, {sold_date}, NULL), "
                f"(9, {sold_date}, NULL), (1, 2, 999.99), (999, 1, 999.99)"
            )
            if mode == "limited":
                connection.execute(
                    f"INSERT INTO {dimension} ({key}, {parent}, {child}) "
                    "SELECT i, 'A', CAST(i AS VARCHAR) FROM range(10, 130) fixture(i)"
                )
                connection.execute(
                    f"INSERT INTO {sales} ({sales_key}, {date_key}, {measure}) "
                    "SELECT i, 1, i * 100 FROM range(10, 130) fixture(i)"
                )
            elif mode == "duplicates":
                connection.execute(f"INSERT INTO {dimension} SELECT * FROM {dimension} WHERE {key} = 1")
                connection.execute("INSERT INTO date_dim VALUES (1, 1200)")
            elif mode == "null_measures":
                connection.execute(f"UPDATE {sales} SET {measure} = NULL")
        reference = connection.sql((benchmark.queries_directory / f"{name}.sql").read_text()).pl()
        actual = connection.sql(benchmark.load_query(name)).pl()
        assert_frame_equal(actual, reference, check_row_order=False)
        assert [(row[3], row[1] if row[3] == 0 else None, row[4]) for row in actual.rows()] == [
            (row[3], row[1] if row[3] == 0 else None, row[4]) for row in reference.rows()
        ]
        if mode in {"empty", "filtered"}:
            assert actual.rows() == [(None, None, None, 2, 1)]
        elif mode == "limited":
            assert actual.height == 100
        elif mode == "null_measures":
            assert actual["total_sum"].null_count() == actual.height
            assert set(actual["rank_within_parent"].to_list()) == {1}
        else:
            assert actual.filter((pl.col("lochierarchy") == 0) & pl.col(child).is_null()).height > 0
            details = actual.filter(pl.col("lochierarchy") == 0)
            a_ranks = dict(details.filter(pl.col(parent) == "A").select(child, "rank_within_parent").iter_rows())
            assert a_ranks == ({"x": 1, "y": 2, None: 3} if mode == "duplicates" else {"x": 1, "y": 1, None: 3})
            subtotals = actual.filter(pl.col("lochierarchy") == 1)
            parent_ranks = dict(subtotals.select(parent, "rank_within_parent").iter_rows())
            assert parent_ranks == ({"A": 1, "B": 2, "C": 3} if name == "70" else {"A": 1, None: 2, "B": 3, "C": 4})
            grand_total = actual.filter(pl.col("lochierarchy") == 2).rows()
            total = (
                Decimal("128.12" if name == "70" else "138.12")
                if mode == "duplicates"
                else Decimal("44.04" if name == "70" else "49.04")
            )
            assert grand_total == [(total, None, None, 2, 1)]


@pytest.mark.parametrize("empty", [False, True])
def test_doris_shipping_delay_preserves_day_boundaries_nulls_and_left_join_multiplicity(empty: bool) -> None:
    benchmark = Doris().benchmarks["tpc_ds"]
    dates = [
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
    with duckdb.connect() as connection:
        connection.execute("SET threads = 1")
        connection.execute(_logical_schema("tpc_ds"))
        if not empty:
            connection.execute("INSERT INTO warehouse (w_warehouse_sk, w_warehouse_name) VALUES (1, 'Warehouse')")
            connection.execute("INSERT INTO customer_demographics (cd_demo_sk, cd_marital_status) VALUES (1, 'D')")
            connection.execute("INSERT INTO household_demographics (hd_demo_sk, hd_buy_potential) VALUES (1, '>10000')")
            connection.execute("INSERT INTO promotion (p_promo_sk) VALUES (1)")
            for item, (sold, shipped) in enumerate(dates, start=1):
                connection.executemany(
                    "INSERT INTO date_dim (d_date_sk, d_date, d_week_seq, d_year) VALUES (?, ?, ?, ?)",
                    [(item * 2, sold, item, 1999), (item * 2 + 1, shipped, item + 100, 2000)],
                )
                connection.execute("INSERT INTO item (i_item_sk, i_item_desc) VALUES (?, ?)", [item, f"Item {item:02}"])
                connection.execute(
                    "INSERT INTO inventory (inv_item_sk, inv_date_sk, inv_warehouse_sk, inv_quantity_on_hand) "
                    "VALUES (?, ?, 1, 1)",
                    [item, item * 2],
                )
                connection.execute(
                    "INSERT INTO catalog_sales (cs_item_sk, cs_order_number, cs_sold_date_sk, cs_ship_date_sk, "
                    "cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_promo_sk, cs_quantity) VALUES (?, ?, ?, ?, 1, 1, ?, 2)",
                    [item, item, item * 2, item * 2 + 1, 1 if item % 4 == 0 else 2],
                )
            connection.execute("INSERT INTO catalog_sales SELECT * FROM catalog_sales WHERE cs_item_sk = 2")
            connection.execute(
                "INSERT INTO catalog_returns (cr_item_sk, cr_order_number) VALUES (2, 2), (2, 2), (4, 4), (6, 6)"
            )
        reference = connection.sql((benchmark.queries_directory / "72.sql").read_text()).pl()
        actual = connection.sql(benchmark.load_query("72")).pl()
        assert_frame_equal(actual, reference)
        assert actual.rows() == (
            []
            if empty
            else [
                ("Item 02", "Warehouse", 2, 4, 0, 4),
                ("Item 04", "Warehouse", 4, 0, 1, 1),
                ("Item 06", "Warehouse", 6, 1, 0, 1),
                ("Item 07", "Warehouse", 7, 1, 0, 1),
            ]
        )


def test_doris_customer_distribution_names_the_count_inside_the_derived_table() -> None:
    directory = SUITES / "tpc_h/queries"
    original = (directory / "13_customer_distribution.sql").read_text()
    adapted = (directory / "doris/13_customer_distribution.sql").read_text()
    assert adapted == original.replace("count(o_orderkey)\n", "count(o_orderkey) AS c_count\n").replace(
        ") AS c_orders (c_custkey,\n        c_count)", ") AS c_orders"
    )
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE customer (c_custkey BIGINT)")
        connection.execute("CREATE TABLE orders (o_orderkey BIGINT, o_custkey BIGINT, o_comment VARCHAR)")
        connection.execute("INSERT INTO customer VALUES (1), (2), (3)")
        connection.execute("INSERT INTO orders VALUES (1, 1, 'normal'), (2, 1, 'normal'), (3, 2, 'special requests')")
        assert connection.execute(adapted).fetchall() == connection.execute(original).fetchall() == [(0, 2), (2, 1)]


@pytest.mark.parametrize("name", ["17", "39"])
def test_doris_sample_deviation_overrides_only_guard_groups_with_fewer_than_two_values(name: str) -> None:
    directory = SUITES / "tpc_ds/queries"
    original = (directory / f"{name}.sql").read_text()
    adapted = (directory / "doris" / f"{name}.sql").read_text()
    assert adapted == re.sub(
        r"stddev_samp\((\w+)\)",
        lambda match: f"(CASE WHEN count({match[1]}) < 2 THEN NULL ELSE stddev_samp({match[1]}) END)",
        original,
    )


def test_doris_web_sales_zip_order_preserves_canonical_nulls_last() -> None:
    directory = SUITES / "tpc_ds/queries"
    original = (directory / "45.sql").read_text()
    adapted = (directory / "doris/45.sql").read_text()
    assert adapted == original.replace(
        "ORDER BY ca_zip,\n         ca_city", "ORDER BY ca_zip NULLS LAST,\n         ca_city NULLS LAST"
    )


@pytest.mark.parametrize(("name", "sort_keys"), [("12", 5), ("50", 10)])
def test_doris_portability_overrides_only_make_reference_null_placement_explicit(name: str, sort_keys: int) -> None:
    directory = SUITES / "tpc_ds/queries"
    original = (directory / f"{name}.sql").read_text()
    adapted = (directory / "doris" / f"{name}.sql").read_text()
    order = adapted.rpartition("\nORDER BY ")[2].removesuffix("\nLIMIT 100;\n")
    terms = order.split(",")
    assert len(terms) == sort_keys
    assert all(term.strip().endswith(" NULLS LAST") for term in terms)
    assert adapted.count(" NULLS LAST") == sort_keys
    with duckdb.connect() as connection:
        assert _sql_tree(connection, original) == _sql_tree(connection, adapted.replace(" NULLS LAST", ""))


@pytest.mark.parametrize("values", [[], [None], [1], [1, None], [1, 1], [1, 2, None]])
def test_sample_deviation_guard_preserves_empty_null_singleton_and_duplicate_groups(values: list[int | None]) -> None:
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE fixture (value DOUBLE)")
        if values:
            connection.executemany("INSERT INTO fixture VALUES (?)", [(value,) for value in values])
        expected = connection.execute("SELECT stddev_samp(value) FROM fixture").fetchall()
        actual = connection.execute(
            "SELECT CASE WHEN count(value) < 2 THEN NULL ELSE stddev_samp(value) END FROM fixture"
        ).fetchall()
        assert actual == expected


@pytest.mark.parametrize("empty", [False, True])
def test_doris_relational_pricing_summary_fixture_preserves_decimal_sums_and_nullable_means(empty: bool) -> None:
    benchmark = Doris().benchmarks["tpc_h"]
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
                    ('A', 'F', '1998-09-02', 2.00, 10.02, 0.02, 0.05),
                    ('A', 'F', '1998-09-02', NULL, NULL, NULL, NULL),
                    ('N', 'O', '1998-09-02', NULL, NULL, NULL, NULL),
                    ('A', 'F', '1998-09-03', 50.00, 99.99, 0.10, 0.05)
                """
            )
        reference = connection.sql((benchmark.queries_directory / "01_pricing_summary.sql").read_text()).pl()
        actual = connection.sql(benchmark.load_query("01_pricing_summary")).pl()
        assert_frame_equal(actual, reference)
        if not empty:
            assert actual.item(0, "sum_base_price") == Decimal("30.05")
            assert actual.item(0, "avg_price") == pytest.approx(30.05 / 3)
            assert actual.item(0, "count_order") == 4
            assert actual.item(1, "avg_price") is None


def test_doris_relational_promotion_fixture_preserves_decimal_revenue_and_recurring_ratios() -> None:
    benchmark = Doris().benchmarks["tpc_h"]
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE part (p_partkey BIGINT, p_type VARCHAR)")
        connection.execute(
            "CREATE TABLE lineitem (l_partkey BIGINT, l_extendedprice DECIMAL(15, 2), "
            "l_discount DECIMAL(15, 2), l_shipdate DATE)"
        )
        connection.execute("INSERT INTO part VALUES (1, 'PROMO'), (2, 'OTHER')")
        connection.execute(
            "INSERT INTO lineitem VALUES (1, 10.01, 0.00, '1995-09-01'), (2, 20.02, 0.00, '1995-09-30'), "
            "(1, 99.99, 0.00, '1995-10-01'), (1, NULL, NULL, '1995-09-01')"
        )
        reference = connection.sql((benchmark.queries_directory / "14_promotion_effect.sql").read_text()).pl()
        actual = connection.sql(benchmark.load_query("14_promotion_effect")).pl()
        assert_frame_equal(actual, reference)
        assert actual.item() == pytest.approx(100 / 3)


def test_doris_relational_tpcds_fixture_preserves_null_sort_keys_exact_money_and_duplicate_rows() -> None:
    benchmark = Doris().benchmarks["tpc_ds"]
    with duckdb.connect() as connection:
        connection.execute(_logical_schema("tpc_ds"))
        connection.execute("INSERT INTO date_dim (d_date_sk, d_moy, d_year) VALUES (1, 11, 1999), (2, 12, 1999)")
        connection.execute("INSERT INTO item (i_item_sk, i_manager_id, i_brand_id, i_brand) VALUES (1, 28, 7, 'Brand')")
        connection.execute(
            """
            INSERT INTO store_sales (ss_sold_date_sk, ss_item_sk, ss_ticket_number, ss_ext_sales_price) VALUES
                (1, 1, 10, 10.01), (1, 1, 10, 10.01), (1, 1, 11, NULL),
                (NULL, 1, 12, 99.99), (2, 1, 13, 20.02)
            """
        )
        assert connection.execute(benchmark.load_query("55")).fetchall() == [(7, "Brand", Decimal("20.02"))]
        assert connection.execute("SELECT count(*) FROM store_sales").fetchone() == (5,)
        connection.execute("INSERT INTO reason (r_reason_sk) VALUES (1)")
        connection.execute(
            """
            INSERT INTO store_sales (ss_item_sk, ss_ticket_number, ss_quantity, ss_net_paid) VALUES
                (1, 20, 1, 10.01), (1, 21, 2, 10.02), (1, 22, 3, 10.02), (1, 23, 21, NULL),
                (1, 24, 4, NULL)
            """
        )
        assert connection.execute(benchmark.load_query("09")).fetchall() == [
            (pytest.approx(30.05 / 3), None, None, None, None)
        ]


def test_doris_relational_tpcds_warehouse_fixture_preserves_exact_sales_and_floating_per_area_results() -> None:
    benchmark = Doris().benchmarks["tpc_ds"]
    with duckdb.connect() as connection:
        connection.execute(_logical_schema("tpc_ds"))
        connection.execute("INSERT INTO date_dim (d_date_sk, d_moy, d_year) VALUES (1, 1, 2001)")
        connection.execute("INSERT INTO time_dim (t_time_sk, t_time) VALUES (1, 30838)")
        connection.execute("INSERT INTO ship_mode (sm_ship_mode_sk, sm_carrier) VALUES (1, 'DHL')")
        connection.execute(
            "INSERT INTO warehouse (w_warehouse_sk, w_warehouse_name, w_warehouse_sq_ft) VALUES (1, 'Warehouse', 3)"
        )
        connection.execute(
            "INSERT INTO web_sales (ws_item_sk, ws_order_number, ws_warehouse_sk, ws_sold_date_sk, "
            "ws_sold_time_sk, ws_ship_mode_sk, ws_quantity, ws_ext_sales_price, ws_net_paid) "
            "VALUES (1, 1, 1, 1, 1, 1, 1, 10.01, 5.01)"
        )
        reference = connection.sql((benchmark.queries_directory / "66.sql").read_text()).pl()
        actual = connection.sql(benchmark.load_query("66")).pl()
        assert_frame_equal(actual, reference)
        assert actual.item(0, "jan_sales") == Decimal("10.01")
        assert actual.item(0, "jan_sales_per_sq_foot") == pytest.approx(10.01 / 3)


@pytest.mark.parametrize("suite", ["tpc_h", "tpc_ds"])
@pytest.mark.parametrize("fail", [False, True])
def test_doris_relational_streams_unmodified_source_parquet_with_explicit_columns_and_zero_filtering(
    suite: SuiteName, fail: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "source.parquet"
    frame = pl.DataFrame(
        {
            "nullable_date": [None, date(1994, 1, 1)],
            "exact_money": [Decimal("10.01"), Decimal("10.02")],
            "wide_key": [9007199254740993, 9007199254740994],
        },
        schema_overrides={"exact_money": pl.Decimal(15, 2)},
    )
    frame.write_parquet(path)
    original = path.read_bytes()
    calls: list[tuple[str, Path, Mapping[str, str]]] = []

    def stream_load_file(_self: Doris, table: str, source: Path, headers: Mapping[str, str]) -> None:
        calls.append((table, source, headers))
        if fail:
            raise RuntimeError("load failed")

    monkeypatch.setattr(Doris, "stream_load_file", stream_load_file)
    database = Doris()
    database._current_suite = suite
    if fail:
        with pytest.raises(RuntimeError, match="load failed"):
            database.insert_parquet(path, "fixture")
    else:
        database.insert_parquet(path, "fixture")
    assert calls == [
        (
            "fixture",
            path,
            {
                "format": "parquet",
                "columns": "nullable_date,exact_money,wide_key",
                "strict_mode": "true",
                "max_filter_ratio": "0",
                "timeout": "3600",
            },
        )
    ]
    assert path.read_bytes() == original


def test_doris_relational_python_fetch_preserves_decimal_date_null_and_large_integer_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Cursor:
        description = [
            ("money", FIELD_TYPE.NEWDECIMAL),
            ("day", FIELD_TYPE.DATE),
            ("key", FIELD_TYPE.LONGLONG),
            ("missing", FIELD_TYPE.VAR_STRING),
        ]

    rows = [(Decimal("1234567890123.45"), date(1994, 1, 1), 9007199254740993, None)]

    class Result:
        cursor = Cursor()

        def keys(self) -> list[str]:
            return ["money", "day", "key", "missing"]

        def fetchall(self) -> list[tuple[Decimal, date, int, None]]:
            return rows

    class PythonConnection:
        def execute(self, _query: object) -> Result:
            return Result()

    def connect(_self: Doris, reconnect: bool = False) -> Connection:
        assert not reconnect
        return cast(Connection, PythonConnection())

    monkeypatch.setattr(Doris, "connect", connect)
    assert Doris().fetch("SELECT fixture_values", method="python").rows() == rows


def _column_statistics(name: str) -> dict[str, str | None]:
    return {
        "column_name": name,
        "index_name": "fixture",
        "count": "2.0",
        "ndv": "2.0" if name == "id" else "1.0",
        "num_null": "0.0" if name == "id" else "1.0",
        "data_size": "16.0" if name == "id" else "1.0",
        "avg_size_byte": "8.0" if name == "id" else "0.5",
        "min": "1" if name == "id" else "'a'",
        "max": "2" if name == "id" else "'a'",
        "method": "FULL",
        "type": "FUNDAMENTALS",
        "trigger": "MANUAL",
        "query_times": "0",
        "updated_time": "2026-10-22 10:00:01",
        "update_rows": "2",
        "last_analyze_row_count": "0",
        "last_analyze_version": "2",
    }


class _OfflineDoris(Doris):
    table_rows: dict[str, int] = {"fixture": 2}
    actual_rows: dict[str, int] = {"fixture": 2}
    overview: dict[str, str | None] = {
        "row_count": "0",
        "columns": "[fixture:id, fixture:label]",
        "updated_time": "2026-10-22 09:59:00",
        "last_analyze_time": "2026-10-22 10:00:00",
        "updated_rows": "2",
        "new_partition": "false",
        "user_inject": "false",
    }
    column_statistics: DorisStatisticsRows = [_column_statistics("id"), _column_statistics("label")]
    cached_statistics: DorisStatisticsRows | None = None
    responses: dict[str, list[DorisStatisticsRows]] = {}
    fetch_error: str | None = None
    analyze_error: bool = False
    statements: list[str] = []
    steps: list[dict[str, Any]] = []

    def _start_step(self, *args: object, **kwargs: object) -> int:
        self.steps.append({"args": args, **kwargs})
        return len(self.steps)

    def _finish_step(self, step_id: int, *args: object, **kwargs: object) -> None:
        self.steps[step_id - 1].update(kwargs)

    def execute(self, statement: str, commit: bool = True, autocommit: bool = False, reconnect: bool = False) -> None:
        self.statements.append(statement)
        if self.analyze_error:
            raise RuntimeError("ANALYZE failed")

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
        method: DorisFetchMethod = "python",
    ) -> pl.DataFrame:
        assert method == "python"
        self.statements.append(query)
        if query == self.fetch_error:
            raise RuntimeError("Statistics query failed")
        if self.responses.get(query):
            return pl.DataFrame(self.responses[query].pop(0))
        if query.startswith("select count(*)"):
            return pl.DataFrame({"row_count": [self.actual_rows[query.rsplit(" ", 1)[1]]]})
        table = query.split("`")[1]
        if query.startswith("SHOW TABLE STATS"):
            return pl.DataFrame([self.overview])
        if query.startswith("SHOW COLUMN STATS"):
            return pl.DataFrame(self.column_statistics)
        if query.startswith("SHOW COLUMN CACHED STATS"):
            return pl.DataFrame(self.column_statistics if self.cached_statistics is None else self.cached_statistics)
        assert query == f"SHOW CREATE TABLE `{table}`"
        return pl.DataFrame({"Table": [table], "Create Table": [f"CREATE TABLE `{table}` (id BIGINT, label VARCHAR)"]})


@dataclass
class _StatisticsClock:
    now: float = 0.0
    sleeps: list[float] = field(default_factory=list[float])

    def perf_counter(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def readiness_clock(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> _StatisticsClock:
    def expected_rows(self: DorisTpcH | DorisTpcDs) -> dict[str, int]:
        return cast(_OfflineDoris, self.db).table_rows

    def input_directory(self: DorisTpcH | DorisTpcDs) -> Path:
        return tmp_path

    monkeypatch.setattr(DorisTpcH, "expected_table_row_counts", expected_rows)
    monkeypatch.setattr(DorisTpcDs, "expected_table_row_counts", expected_rows)
    monkeypatch.setattr(DorisTpcH, "input_data_directory", property(input_directory))
    monkeypatch.setattr(DorisTpcDs, "input_data_directory", property(input_directory))
    pl.DataFrame({"id": [1, 2], "label": ["a", None]}).write_parquet(tmp_path / "fixture.parquet")
    clock = _StatisticsClock()
    monkeypatch.setattr(doris_module, "perf_counter", clock.perf_counter)
    monkeypatch.setattr(doris_module, "sleep", clock.sleep)
    return clock


def _readiness_database() -> _OfflineDoris:
    database = _OfflineDoris()
    database._current_suite = "tpc_ds"
    database._current_suite_scale_factor = 1
    return database


@pytest.mark.parametrize("suite", ["tpc_h", "tpc_ds"])
def test_doris_relational_finalization_charges_synchronous_full_statistics_and_records_readiness(
    suite: SuiteName,
    readiness_clock: _StatisticsClock,
) -> None:
    database = _OfflineDoris()
    database._current_suite = suite
    database._current_suite_scale_factor = 10
    database.finalize_populate()

    assert database.statements == [
        "ANALYZE TABLE `fixture` WITH SYNC",
        "select count(*) as row_count from fixture",
        "SHOW CREATE TABLE `fixture`",
        "SHOW TABLE STATS `fixture`",
        "SHOW COLUMN STATS `fixture`",
        "SHOW COLUMN CACHED STATS `fixture`",
    ]
    assert database.steps[0]["args"] == ("phase", "doris_analyze")
    assert database.steps[0]["table_name"] == "fixture"
    assert database.steps[0]["status"] == "completed"
    evidence = database.steps[1]
    assert evidence["args"] == ("phase", "doris_statistics_readiness")
    assert evidence["status"] == "completed"
    assert evidence["metadata"] == {
        "expected_rows": 2,
        "actual_rows": 2,
        "expected_columns": ["id", "label"],
        "statistics": [database.overview],
        "column_statistics": database.column_statistics,
        "cached_column_statistics": database.column_statistics,
        "create_table_sql": "CREATE TABLE `fixture` (id BIGINT, label VARCHAR)",
        "readiness_attempts": 1,
        "readiness_elapsed_seconds": 0.0,
    }
    assert readiness_clock.sleeps == []
    assert database.overview["row_count"] == "0"
    assert database.column_statistics[0]["last_analyze_row_count"] == "0"


def test_doris_relational_readiness_attests_actual_rows_independently(readiness_clock: _StatisticsClock) -> None:
    database = _readiness_database()
    database.actual_rows["fixture"] = 1
    with pytest.raises(RuntimeError, match="actual row count differs"):
        database.verify_storage_readiness()
    assert database.steps[0]["status"] == "failed"
    assert database.steps[0]["metadata"] == {"expected_rows": 2, "actual_rows": 1}
    assert database.statements == ["select count(*) as row_count from fixture"]
    assert readiness_clock.sleeps == []


def test_doris_relational_populate_can_recollect_missing_statistics_for_already_verified_tables(
    readiness_clock: _StatisticsClock,
) -> None:
    database = _readiness_database()
    database._current_operation = "populate"
    database.verify_storage_readiness()
    assert database.statements == []
    database.finalize_populate()
    assert database.statements == [
        "ANALYZE TABLE `fixture` WITH SYNC",
        "select count(*) as row_count from fixture",
        "SHOW CREATE TABLE `fixture`",
        "SHOW TABLE STATS `fixture`",
        "SHOW COLUMN STATS `fixture`",
        "SHOW COLUMN CACHED STATS `fixture`",
    ]
    assert readiness_clock.sleeps == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("row_count", None),
        ("row_count", "NaN"),
        ("row_count", "-1"),
        ("row_count", "2.9"),
        ("columns", ""),
        ("columns", "[]"),
        ("columns", "[fixture:id]"),
        ("columns", "[another:id, another:label]"),
        ("last_analyze_time", ""),
        ("last_analyze_time", None),
        ("last_analyze_time", "not a timestamp"),
        ("last_analyze_time", "2026-10-22 09:58:00"),
        ("updated_rows", "3"),
        ("updated_rows", "NaN"),
        ("new_partition", "true"),
        ("user_inject", "true"),
    ],
)
def test_doris_relational_readiness_rejects_missing_or_stale_overview_metadata(
    field: str, value: str | None, readiness_clock: _StatisticsClock
) -> None:
    database = _readiness_database()
    database.overview[field] = value
    with pytest.raises(RuntimeError, match="missing, stale or invalid"):
        database.verify_storage_readiness()
    assert database.steps[0]["status"] == "failed"
    assert database.steps[0]["metadata"]["statistics"][0][field] == value
    assert readiness_clock.now == 60


@pytest.mark.parametrize("source", ["collected", "cached"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("count", "0.0"),
        ("count", "1.0"),
        ("count", "2.9"),
        ("count", "NaN"),
        ("count", "Infinity"),
        ("count", None),
        ("method", "SAMPLE"),
        ("method", "N/A"),
        ("type", "HISTOGRAM"),
        ("trigger", "SYSTEM"),
        ("index_name", "another"),
        ("updated_time", "2026-10-22 09:59:59"),
        ("updated_time", ""),
        ("update_rows", "1"),
        ("last_analyze_version", "N/A"),
        ("last_analyze_row_count", "-1"),
        ("num_null", "-1.0"),
        ("num_null", "0.5"),
        ("num_null", "3.0"),
        ("num_null", "2.0"),
        ("ndv", "-1.0"),
        ("ndv", "0.0"),
        ("ndv", "NaN"),
        ("data_size", "-1.0"),
        ("avg_size_byte", "Infinity"),
        ("min", "N/A"),
        ("max", None),
    ],
)
def test_doris_relational_readiness_rejects_invalid_or_stale_column_statistics(
    source: str, field: str, value: str | None, readiness_clock: _StatisticsClock
) -> None:
    database = _readiness_database()
    if source == "cached":
        database.cached_statistics = [dict(row) for row in database.column_statistics]
    rows = database.column_statistics if source == "collected" else database.cached_statistics
    assert rows is not None
    rows[0][field] = value
    with pytest.raises(RuntimeError, match="missing, stale or invalid"):
        database.verify_storage_readiness()
    evidence = database.steps[0]
    assert evidence["status"] == "failed"
    key = "column_statistics" if source == "collected" else "cached_column_statistics"
    assert evidence["metadata"][key][0][field] == value
    assert evidence["metadata"]["create_table_sql"]
    assert evidence["metadata"]["readiness_elapsed_seconds"] == 60
    assert readiness_clock.sleeps == [1.0] * 60


@pytest.mark.parametrize("source", ["collected", "cached"])
@pytest.mark.parametrize("problem", ["missing", "duplicate", "wrong_column", "empty"])
def test_doris_relational_readiness_requires_every_canonical_column_in_storage_and_cache(
    source: str, problem: str, readiness_clock: _StatisticsClock
) -> None:
    database = _readiness_database()
    rows = [dict(row) for row in database.column_statistics]
    if problem == "missing":
        rows.pop()
    elif problem == "duplicate":
        rows[1] = dict(rows[0])
    elif problem == "wrong_column":
        rows[1]["column_name"] = "other"
    else:
        rows.clear()
    if source == "collected":
        database.column_statistics = rows
    else:
        database.cached_statistics = rows
    with pytest.raises(RuntimeError, match="every canonical column"):
        database.verify_storage_readiness()
    assert database.steps[0]["status"] == "failed"
    assert readiness_clock.now == 60


@pytest.mark.parametrize(
    ("field", "value"),
    [("ndv", "1.0"), ("min", "0"), ("updated_time", "2026-10-22 10:00:02"), ("last_analyze_version", "1")],
)
def test_doris_relational_readiness_rejects_cache_disagreement_even_when_counts_match(
    field: str, value: str, readiness_clock: _StatisticsClock
) -> None:
    database = _readiness_database()
    database.cached_statistics = [dict(row) for row in database.column_statistics]
    database.cached_statistics[0][field] = value
    with pytest.raises(RuntimeError, match="Cached statistics differ"):
        database.verify_storage_readiness()
    assert readiness_clock.now == 60


@pytest.mark.parametrize("source", ["collected", "cached"])
def test_doris_relational_finalization_waits_for_actual_statistics_publication(
    source: str, readiness_clock: _StatisticsClock
) -> None:
    database = _readiness_database()
    query = "SHOW COLUMN STATS `fixture`" if source == "collected" else "SHOW COLUMN CACHED STATS `fixture`"
    stale = [dict(row) for row in database.column_statistics]
    stale[0]["count"] = "0.0"
    database.responses[query] = [[], stale]
    database.finalize_populate()
    assert readiness_clock.sleeps == [1.0, 1.0]
    assert database.steps[1]["status"] == "completed"
    assert database.steps[1]["metadata"]["readiness_attempts"] == 3
    assert database.steps[1]["metadata"]["readiness_elapsed_seconds"] == 2
    assert database.statements.count("ANALYZE TABLE `fixture` WITH SYNC") == 1
    assert database.statements.count("select count(*) as row_count from fixture") == 1


def test_doris_relational_readiness_caps_the_last_polling_sleep(
    readiness_clock: _StatisticsClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(doris_module, "DORIS_STATISTICS_TIMEOUT_SECONDS", 2.5)
    database = _readiness_database()
    database.cached_statistics = []
    with pytest.raises(RuntimeError, match="every canonical column"):
        database.verify_storage_readiness()
    assert readiness_clock.sleeps == [1.0, 1.0, 0.5]
    assert database.steps[0]["metadata"]["readiness_elapsed_seconds"] == 2.5


def test_doris_relational_readiness_allows_all_null_and_empty_columns(readiness_clock: _StatisticsClock) -> None:
    for count in (0, 2):
        database = _readiness_database()
        database.table_rows["fixture"] = count
        database.actual_rows["fixture"] = count
        for row in database.column_statistics:
            row.update(
                count=str(count),
                ndv="0.0",
                num_null=str(count),
                data_size="0.0",
                avg_size_byte="0.0",
                min="N/A",
                max="N/A",
            )
        database.verify_storage_readiness()
        assert database.steps[0]["status"] == "completed"
    assert readiness_clock.sleeps == []


def test_doris_relational_readiness_does_not_require_approximate_ndv_to_be_exact(
    readiness_clock: _StatisticsClock,
) -> None:
    database = _readiness_database()
    database.column_statistics[0]["ndv"] = "2.01"
    database.verify_storage_readiness()
    assert database.steps[0]["status"] == "completed"
    assert readiness_clock.sleeps == []


def test_doris_relational_readiness_does_not_hide_statistics_query_errors(readiness_clock: _StatisticsClock) -> None:
    database = _readiness_database()
    database.fetch_error = "SHOW COLUMN CACHED STATS `fixture`"
    with pytest.raises(RuntimeError, match="Statistics query failed"):
        database.verify_storage_readiness()
    assert database.steps[0]["status"] == "failed"
    assert database.steps[0]["metadata"]["column_statistics"] == database.column_statistics
    assert readiness_clock.sleeps == []


def test_doris_relational_failed_analyze_does_not_attest_readiness(readiness_clock: _StatisticsClock) -> None:
    database = _readiness_database()
    database.analyze_error = True
    with pytest.raises(RuntimeError, match="ANALYZE failed"):
        database.finalize_populate()
    assert len(database.steps) == 1
    assert database.steps[0]["status"] == "failed"
    assert database.statements == ["ANALYZE TABLE `fixture` WITH SYNC"]
    assert readiness_clock.sleeps == []
