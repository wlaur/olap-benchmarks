import re
from collections import Counter
from collections.abc import Iterator
from decimal import Decimal
from typing import cast

import duckdb
import pytest

from ..query_registry import SUITES_DIRECTORY

TPCDS = SUITES_DIRECTORY / "tpc_ds"
QUERIES = ("12", "44", "45", "50", "67")
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
type Row = tuple[object, ...]


def _query(name: str, *, adapted: bool) -> str:
    directory = TPCDS / "queries"
    return ((directory / "monetdb" if adapted else directory) / f"{name}.sql").read_text()


def _compact(sql: str) -> str:
    return " ".join(sql.split())


@pytest.fixture
def connection() -> Iterator[duckdb.DuckDBPyConnection]:
    with duckdb.connect(":memory:", config={"threads": 1}) as database:
        database.execute((TPCDS / "schemas/duckdb.sql").read_text())
        yield database


@pytest.mark.parametrize("name", QUERIES)
def test_only_proven_portability_changes_preserve_full_query_contract(name: str) -> None:
    expected = _query(name, adapted=False)
    if name == "12":
        expected = expected.replace(
            "sum(ws_ext_sales_price)*100.0000",
            "cast(sum(ws_ext_sales_price) as double)*100.0000",
        )
    if name == "44":
        expected = expected.replace("avg(ss_net_profit)", "avg(CAST(ss_net_profit AS DOUBLE))")
    if name == "67":
        expected = expected.replace("WHERE rk <= 100", "WHERE CASE WHEN rk <= 100 THEN 1 ELSE 0 END = 1")
    if name in NULLS_LAST_KEYS:
        body, _, _ = expected.rpartition("\nORDER BY ")
        order = ", ".join(f"{key} ASC NULLS LAST" for key in NULLS_LAST_KEYS[name])
        expected = f"{body}\nORDER BY {order}\nLIMIT 100;"
    assert _compact(_query(name, adapted=True)) == _compact(expected)


@pytest.mark.parametrize("name", tuple(NULLS_LAST_KEYS))
def test_final_sort_explicitly_preserves_reference_null_placement_and_limit(name: str) -> None:
    order = _query(name, adapted=True).rpartition("\nORDER BY ")[2]
    expected = ", ".join(f"{key} ASC NULLS LAST" for key in NULLS_LAST_KEYS[name])
    assert _compact(order) == f"{expected} LIMIT 100;"


@pytest.mark.parametrize(
    "position",
    range(6),
    ids=(
        "ascending-projection",
        "ascending-having",
        "ascending-scalar-threshold",
        "descending-projection",
        "descending-having",
        "descending-scalar-threshold",
    ),
)
def test_every_query44_avg_casts_its_input_before_aggregation(position: int) -> None:
    sql = _compact(_query("44", adapted=True)).lower()
    inputs = re.findall(r"\bavg\((cast\([^()]*\)|[^()]*)\)", sql)
    assert len(inputs) == 6
    assert inputs[position] == "cast(ss_net_profit as double)"


def test_query44_preserves_fractional_average_ranks_and_item_identities(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    connection.execute(
        """
        INSERT INTO item (i_item_sk, i_product_name) VALUES
            (1, 'lower'), (2, 'upper'), (3, 'threshold'), (4, 'null');
        INSERT INTO store_sales (ss_store_sk, ss_item_sk, ss_ticket_number, ss_addr_sk, ss_net_profit) VALUES
            (4, 1, 1, 1, 1.00), (4, 1, 2, 1, 1.00), (4, 1, 3, 1, 1.01),
            (4, 2, 4, 1, 1.00), (4, 2, 5, 1, 1.01), (4, 2, 6, 1, 1.01),
            (4, 3, 7, NULL, 0.01), (4, 4, 8, 1, NULL);
        """
    )
    expected = [(1, "threshold", "upper"), (2, "lower", "lower"), (3, "upper", "threshold")]
    for adapted in (False, True):
        assert connection.execute(_query("44", adapted=adapted)).fetchall() == expected


@pytest.mark.parametrize(("cutoff", "preceding", "ties"), [(2, 1, 2), (100, 98, 4)])
@pytest.mark.parametrize("duplicate_labels", [False, True])
def test_rank_predicates_keep_all_boundary_ties_including_null_partitions_and_duplicates(
    connection: duckdb.DuckDBPyConnection,
    cutoff: int,
    preceding: int,
    ties: int,
    duplicate_labels: bool,
) -> None:
    connection.execute("CREATE TABLE rank_input (g VARCHAR(8), v INTEGER, label INTEGER)")
    inputs: list[tuple[str | None, int, int]] = []
    expected: list[Row] = []
    for group in (None, "a"):
        for rank, value in enumerate(range(preceding + 8, 8, -1), start=1):
            inputs.append((group, value, rank))
            expected.append((group, value, rank, rank))
        for index in range(ties):
            label = preceding + 1 + (0 if duplicate_labels else index)
            inputs.append((group, 8, label))
            expected.append((group, 8, label, preceding + 1))
        inputs.append((group, 7, preceding + ties + 1))
    connection.executemany("INSERT INTO rank_input VALUES (?, ?, ?)", inputs)
    base = "SELECT g, v, label, RANK() OVER (PARTITION BY g ORDER BY v DESC) rk FROM rank_input"
    assert len(expected) == (6 if cutoff == 2 else 204)
    if duplicate_labels:
        assert max(Counter(expected).values()) == ties
    for adapted in (False, True):
        sql = _query("67", adapted=adapted)
        predicate = sql.rpartition("\nORDER BY ")[0].rsplit("\nWHERE ", maxsplit=1)[1]
        predicate = predicate.replace("100", str(cutoff))
        rows = cast(
            list[Row],
            connection.execute(
                f"SELECT * FROM ({base}) ranked WHERE {predicate} ORDER BY g NULLS FIRST, v DESC, label"
            ).fetchall(),
        )
        assert Counter(rows) == Counter(expected)
        assert rows == expected


@pytest.mark.parametrize("category", [None, "a"])
@pytest.mark.parametrize("quantities", [[None], [3], [3, 3], [None, 3, 9]])
def test_native_rollup_keeps_null_keys_duplicate_facts_and_duplicate_output_rows(
    connection: duckdb.DuckDBPyConnection, category: str | None, quantities: list[int | None]
) -> None:
    connection.execute("INSERT INTO date_dim (d_date_sk, d_month_seq) VALUES (1, 1200)")
    connection.execute("INSERT INTO item (i_item_sk, i_category) VALUES (1, ?)", [category])
    connection.execute("INSERT INTO store (s_store_sk) VALUES (1)")
    connection.executemany(
        "INSERT INTO store_sales "
        "(ss_sold_date_sk, ss_item_sk, ss_ticket_number, ss_store_sk, ss_quantity, ss_sales_price) "
        "VALUES (1, 1, 1, 1, ?, 1)",
        [(quantity,) for quantity in quantities],
    )
    total = Decimal(sum(quantity for quantity in quantities if quantity is not None))
    grand_total = (None,) * 8 + (total, 1)
    category_total = (category,) + (None,) * 7 + (total, 1)
    expected = [grand_total] + [category_total] * 8
    for adapted in (False, True):
        rows = cast(list[Row], connection.execute(_query("67", adapted=adapted)).fetchall())
        assert Counter(rows) == Counter(expected)
        assert rows == expected


def test_native_rollup_preserves_empty_grand_total(connection: duckdb.DuckDBPyConnection) -> None:
    assert connection.execute(_query("67", adapted=True)).fetchall() == [(None,) * 9 + (1,)]
