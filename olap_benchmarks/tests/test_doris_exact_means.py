from collections import Counter
from collections.abc import Iterator, Sequence
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Literal, cast

import duckdb
import pytest

from ..dbs.doris import Doris
from .test_doris_relational import SUITES, _logical_schema

type Row = tuple[object, ...]
type PricingInput = tuple[str, str, date, Decimal | None, Decimal | None, Decimal | None, Decimal | None]
type Means = tuple[Fraction | None, Fraction | None, Fraction | None]


def _mean(values: Sequence[Decimal | None]) -> Fraction | None:
    nonnull = [value for value in values if value is not None]
    if not nonnull:
        return None
    return sum((Fraction(value) for value in nonnull), Fraction()) / len(nonnull)


def _assert_mean(actual: object, expected: Fraction | None) -> None:
    if expected is None:
        assert actual is None
    else:
        assert isinstance(actual, float)
        assert actual == pytest.approx(float(expected), rel=1e-15, abs=0)


@pytest.fixture
def q44_connection() -> Iterator[duckdb.DuckDBPyConnection]:
    with duckdb.connect(config={"threads": 1}) as connection:
        connection.execute(_logical_schema("tpc_ds"))
        yield connection


@pytest.fixture
def q01_connection() -> Iterator[duckdb.DuckDBPyConnection]:
    with duckdb.connect(config={"threads": 1}) as connection:
        connection.execute(_logical_schema("tpc_h"))
        yield connection


def _seed_q44(connection: duckdb.DuckDBPyConnection) -> dict[int, tuple[Decimal | None, ...]]:
    profits: dict[int, tuple[Decimal | None, ...]] = {
        1: (Decimal("10.00"), Decimal("8.00")),
        2: (Decimal("8.99"),),
        3: (Decimal("9.01"),),
        4: (Decimal("10.00"), Decimal("10.00"), Decimal("10.01"), None),
        5: (Decimal("9.99"), Decimal("10.01"), Decimal("10.01")),
        **{item: (Decimal(item + 5),) for item in range(6, 17)},
        17: (None, None),
    }
    connection.executemany(
        "INSERT INTO item (i_item_sk, i_product_name) VALUES (?, ?)",
        [(item, "shared-name" if item in {4, 5} else f"item-{item}") for item in range(1, 20)],
    )
    rows: list[tuple[int, int, int | None, int, Decimal | None]] = []
    for item, values in profits.items():
        for offset, profit in enumerate(values):
            address = None if item == 17 or (item == 1 and offset == 0) else 1
            rows.append((item, len(rows) + 1, address, 4, profit))
    connection.executemany(
        "INSERT INTO store_sales "
        "(ss_item_sk, ss_ticket_number, ss_addr_sk, ss_store_sk, ss_net_profit) VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    connection.execute(
        "INSERT INTO store_sales (ss_item_sk, ss_ticket_number, ss_addr_sk, ss_store_sk, ss_net_profit) "
        "VALUES (18, 1000, NULL, 3, 999.99)"
    )
    return profits


def test_doris_q44_fractional_means_preserve_having_rank_gaps_ties_and_duplicate_names(
    q44_connection: duckdb.DuckDBPyConnection,
) -> None:
    profits = _seed_q44(q44_connection)
    query = Doris().benchmarks["tpc_ds"].load_query("44")
    result = q44_connection.sql(query)
    assert result.columns == ["rnk", "best_performing", "worst_performing"]
    rows = cast(list[Row], result.fetchall())
    expected = [
        (1, "item-3", "item-16"),
        (2, "shared-name", "item-15"),
        (2, "shared-name", "item-15"),
        (4, "item-6", "item-13"),
        (5, "item-7", "item-12"),
        (6, "item-8", "item-11"),
        (7, "item-9", "item-10"),
        (8, "item-10", "item-9"),
        (9, "item-11", "item-8"),
        (10, "item-12", "item-7"),
    ]
    assert Counter(rows) == Counter(expected)
    assert [row[0] for row in rows] == [1, 2, 2, 4, 5, 6, 7, 8, 9, 10]
    assert _mean(profits[4]) == _mean(profits[5]) == Fraction(3001, 300)
    assert _mean(profits[1]) == Fraction(9)
    assert _mean(profits[17]) is None
    assert q44_connection.execute(
        "SELECT SUM(ss_net_profit), COUNT(ss_net_profit), COUNT(*) "
        "FROM store_sales WHERE ss_store_sk = 4 AND ss_addr_sk IS NULL"
    ).fetchone() == (Decimal("10.00"), 1, 3)
    assert q44_connection.execute(
        "SELECT SUM(ss_net_profit), COUNT(ss_net_profit), COUNT(*) "
        "FROM store_sales WHERE ss_store_sk = 4 AND ss_item_sk = 4"
    ).fetchone() == (Decimal("30.01"), 3, 4)
    grouped_sql = "SELECT ss_item_sk item_sk," + query.split("(SELECT ss_item_sk item_sk,", 1)[1].split(")V1", 1)[0]
    grouped = cast(list[tuple[int, float]], q44_connection.execute(grouped_sql).fetchall())
    assert {item for item, _mean_value in grouped} == set(range(3, 17))
    for item, mean in grouped:
        _assert_mean(mean, _mean(profits[item]))


@pytest.mark.parametrize(
    "mode", ["empty", "missing_reference", "all_null_reference", "all_null_profits", "other_store"]
)
def test_doris_q44_empty_or_null_comparisons_do_not_admit_groups(
    q44_connection: duckdb.DuckDBPyConnection,
    mode: Literal["empty", "missing_reference", "all_null_reference", "all_null_profits", "other_store"],
) -> None:
    if mode != "empty":
        _seed_q44(q44_connection)
    match mode:
        case "empty":
            pass
        case "missing_reference":
            q44_connection.execute("UPDATE store_sales SET ss_addr_sk = 1")
        case "all_null_reference":
            q44_connection.execute("UPDATE store_sales SET ss_addr_sk = 1 WHERE ss_net_profit IS NOT NULL")
        case "all_null_profits":
            q44_connection.execute("UPDATE store_sales SET ss_net_profit = NULL")
        case "other_store":
            q44_connection.execute("UPDATE store_sales SET ss_store_sk = 3")
    query = Doris().benchmarks["tpc_ds"].load_query("44")
    assert q44_connection.execute(query).fetchall() == []


def _insert_lineitems(connection: duckdb.DuckDBPyConnection, rows: Sequence[PricingInput]) -> None:
    if rows:
        connection.executemany(
            "INSERT INTO lineitem (l_shipdate, l_orderkey, l_partkey, l_suppkey, l_linenumber, "
            "l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, "
            "l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) "
            "VALUES (?, ?, 1, 1, 1, ?, ?, ?, ?, ?, ?, "
            "DATE '1998-09-01', DATE '1998-09-03', 'DELIVER IN PERSON', 'AIR', 'fixture')",
            [
                (shipdate, index, quantity, price, discount, tax, flag, status)
                for index, (flag, status, shipdate, quantity, price, discount, tax) in enumerate(rows, 1)
            ],
        )


@pytest.mark.parametrize("mode", ["empty", "populated", "nullable_extension"])
def test_doris_q01_preserves_every_sum_count_row_column_and_fractional_mean(
    q01_connection: duckdb.DuckDBPyConnection, mode: Literal["empty", "populated", "nullable_extension"]
) -> None:
    before, cutoff, after = date(1998, 9, 1), date(1998, 9, 2), date(1998, 9, 3)
    inputs: list[PricingInput] = [
        ("N", "O", cutoff, Decimal("3.25"), Decimal("4.03"), Decimal("0.03"), Decimal("0.02")),
        ("A", "F", before, Decimal("1.00"), Decimal("10.01"), Decimal("0.01"), Decimal("0.00")),
        ("A", "F", cutoff, Decimal("2.00"), Decimal("10.02"), Decimal("0.02"), Decimal("0.05")),
        ("R", "F", before, Decimal("0.00"), Decimal("-0.01"), Decimal("0.00"), Decimal("0.00")),
        ("A", "F", cutoff, Decimal("2.00"), Decimal("10.02"), Decimal("0.02"), Decimal("0.05")),
        ("A", "F", after, Decimal("999.00"), Decimal("999.99"), Decimal("0.10"), Decimal("0.05")),
        ("N", "F", after, Decimal("999.00"), Decimal("999.99"), Decimal("0.10"), Decimal("0.05")),
    ]
    expected_sums: list[Row] = [
        ("A", "F", Decimal("5.00"), Decimal("30.05"), Decimal("29.5491"), Decimal("30.531060"), 3),
        ("N", "O", Decimal("3.25"), Decimal("4.03"), Decimal("3.9091"), Decimal("3.987282"), 1),
        ("R", "F", Decimal("0.00"), Decimal("-0.01"), Decimal("-0.0100"), Decimal("-0.010000"), 1),
    ]
    expected_means: list[Means] = [
        (Fraction(5, 3), Fraction(601, 60), Fraction(1, 60)),
        (Fraction(13, 4), Fraction(403, 100), Fraction(3, 100)),
        (Fraction(0), Fraction(-1, 100), Fraction(0)),
    ]
    if mode == "nullable_extension":
        for column in ("l_quantity", "l_extendedprice", "l_discount", "l_tax"):
            q01_connection.execute(f"ALTER TABLE lineitem ALTER COLUMN {column} DROP NOT NULL")
        inputs.extend(
            [
                ("A", "F", cutoff, None, Decimal("20.04"), Decimal("0.01"), None),
                ("A", "F", cutoff, Decimal("4.00"), None, Decimal("0.03"), Decimal("0.05")),
                ("A", "F", cutoff, None, None, None, None),
                ("N", "F", cutoff, None, None, None, None),
                ("N", "F", cutoff, None, None, None, None),
            ]
        )
        expected_sums[0] = ("A", "F", Decimal("9.00"), Decimal("50.09"), Decimal("49.3887"), Decimal("30.531060"), 6)
        expected_sums.insert(1, ("N", "F", None, None, None, None, 2))
        expected_means[0] = (Fraction(9, 4), Fraction(5009, 400), Fraction(9, 500))
        expected_means.insert(1, (None, None, None))
    if mode == "empty":
        inputs = []
        expected_sums = []
        expected_means = []
    _insert_lineitems(q01_connection, inputs)
    query = Doris().benchmarks["tpc_h"].load_query("01_pricing_summary")
    canonical_sql = (SUITES / "tpc_h/queries/01_pricing_summary.sql").read_text()
    result = q01_connection.sql(query)
    canonical = q01_connection.sql(canonical_sql)
    assert (
        result.columns
        == canonical.columns
        == [
            "l_returnflag",
            "l_linestatus",
            "sum_qty",
            "sum_base_price",
            "sum_disc_price",
            "sum_charge",
            "avg_qty",
            "avg_price",
            "avg_disc",
            "count_order",
        ]
    )
    assert result.types == canonical.types
    rows = cast(list[Row], result.fetchall())
    canonical_rows = cast(list[Row], canonical.fetchall())
    assert [(*row[:6], row[9]) for row in rows] == expected_sums
    assert [(*row[:6], row[9]) for row in canonical_rows] == expected_sums
    for row, means in zip(rows, expected_means, strict=True):
        for actual, expected in zip(row[6:9], means, strict=True):
            _assert_mean(actual, expected)
