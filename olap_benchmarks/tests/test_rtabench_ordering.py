from pathlib import Path

import duckdb
import pytest

from ..settings import REPO_ROOT

QUERIES = REPO_ROOT / "olap_benchmarks/suites/rtabench/queries"


@pytest.mark.parametrize(
    "query",
    sorted(path for path in QUERIES.rglob("*.sql") if path.name.startswith(("0005_", "0006_", "0010_", "0011_"))),
    ids=lambda path: str(path.relative_to(QUERIES)),
)
def test_event_queries_resolve_timestamp_ties_before_limits(query: Path) -> None:
    if query.name.startswith(("0005_", "0006_")):
        rows = [(order, counter) for order in range(12, 0, -1) for counter in (2, 1)]
        timestamp = "2024-03-01 00:00:00"
        expected = [(order, counter) for order in range(1, 6) for counter in (1, 2)]
    elif query.name.startswith("0010_"):
        rows = [(333, counter) for counter in (1, 2, 3)]
        timestamp = "2024-08-01 00:00:00"
        expected = [(333, 3)]
    else:
        rows = [(512, counter) for counter in (3, 2, 1)]
        timestamp = "2024-01-01 00:00:00"
        expected = [(512, counter) for counter in (1, 2, 3)]

    with duckdb.connect() as connection:
        connection.execute("""
            CREATE TABLE order_events (
                order_id INT, counter INT, event_created TIMESTAMP, event_type VARCHAR,
                satisfaction FLOAT, processor VARCHAR, backup_processor VARCHAR, event_payload VARCHAR
            )
        """)
        connection.executemany(
            "INSERT INTO order_events VALUES (?, ?, ?, 'Created', 0.1, 'ron', 'backup', '{}')",
            [(order, counter, timestamp) for order, counter in rows],
        )

        actual = connection.execute(query.read_text()).fetchall()

    assert [(row[0], row[1]) for row in actual] == expected


@pytest.mark.parametrize(
    "query",
    sorted(QUERIES.rglob("0017_top_selling_month_product.sql")),
    ids=lambda path: str(path.relative_to(QUERIES)),
)
def test_top_selling_products_keep_exact_totals_and_resolve_name_and_amount_ties(query: Path) -> None:
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE products (product_id INT, name VARCHAR)")
        connection.execute("CREATE TABLE order_items (order_id INT, product_id INT, amount INT)")
        connection.execute("CREATE TABLE order_events (order_id INT, event_created TIMESTAMP, event_type VARCHAR)")
        connection.executemany("INSERT INTO products VALUES (?, 'same')", [(product,) for product in range(12, 0, -1)])
        connection.executemany(
            "INSERT INTO order_items VALUES (?, ?, 1)", [(product, product) for product in range(12, 0, -1)]
        )
        connection.executemany(
            "INSERT INTO order_events VALUES (?, '2024-01-01', 'Delivered')",
            [(product,) for product in range(12, 0, -1)],
        )

        actual = connection.execute(query.read_text()).fetchall()

    assert actual == [(product, "same", 1) for product in range(1, 11)]
    assert all(type(row[2]) is int for row in actual)


@pytest.mark.parametrize("engine", ["doris", "starrocks", "questdb"])
def test_country_state_rollups_keep_nulls_last_and_preserve_duplicate_grouping_rows(engine: str) -> None:
    with duckdb.connect() as connection:
        connection.execute("""
            CREATE TABLE customers (customer_id INT, country VARCHAR, state VARCHAR);
            INSERT INTO customers VALUES (1, 'A', 'B'), (2, 'B', 'A'), (3, NULL, 'Y'), (4, 'C', NULL);
            CREATE TABLE orders (order_id INT, customer_id INT, created_at TIMESTAMP);
            INSERT INTO orders VALUES (1, 1, '2024-12-25'), (2, 2, '2024-12-25'),
                (3, 3, '2024-12-25'), (4, 4, '2024-12-25');
            CREATE TABLE order_items (order_id INT, product_id INT, amount INT);
            INSERT INTO order_items VALUES (1, 1, 1), (2, 1, 1), (3, 1, 1), (4, 1, 1);
            CREATE TABLE products (product_id INT, price INT);
            INSERT INTO products VALUES (1, 5);
        """)
        query_name = "0022_sales_volume_by_country_state.sql"
        expected = connection.execute((QUERIES / query_name).read_text()).fetchall()
        actual = connection.execute((QUERIES / engine / query_name).read_text()).fetchall()

    assert expected == [
        ("A", "B", 5),
        ("A", None, 5),
        ("B", "A", 5),
        ("B", None, 5),
        ("C", None, 5),
        ("C", None, 5),
        (None, "Y", 5),
        (None, None, 5),
        (None, None, 20),
    ]
    assert actual == expected


@pytest.mark.parametrize("override", ["", "monetdb", "questdb"])
def test_out_of_stock_products_keeps_product_keys_and_excludes_shipped_orders(override: str) -> None:
    query = (
        REPO_ROOT / "olap_benchmarks/suites/rtabench/queries" / override / "0019_out_of_stock_products.sql"
    ).read_text()
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders (order_id INT, created_at TIMESTAMP)")
        connection.execute("CREATE TABLE order_items (order_id INT, product_id INT, amount INT)")
        connection.execute("CREATE TABLE products (product_id INT, name VARCHAR, stock INT)")
        connection.execute("CREATE TABLE order_events (order_id INT, event_type VARCHAR)")
        connection.execute(
            "INSERT INTO orders VALUES (1, '2024-12-02'), (2, '2024-12-03'), (3, '2024-12-01'), (4, '2024-12-04')"
        )
        connection.execute("INSERT INTO order_items VALUES (1, 1, 2), (1, 2, 3), (2, 1, 5), (3, 1, 90), (4, 3, 4)")
        connection.execute("INSERT INTO products VALUES (1, 'same', 100), (2, 'same', 100), (3, 'other', 100)")
        connection.execute("INSERT INTO order_events VALUES (2, 'Shipped'), (1, 'Created'), (NULL, 'Shipped')")

        assert connection.execute(query).fetchall() == [("same", 100, 2), ("same", 100, 3), ("other", 100, 4)]
