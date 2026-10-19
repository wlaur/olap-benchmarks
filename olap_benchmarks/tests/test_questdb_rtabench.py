from decimal import Decimal

import duckdb
import pytest

from ..settings import REPO_ROOT

QUERIES = REPO_ROOT / "olap_benchmarks/suites/rtabench/queries"


@pytest.mark.parametrize(
    ("query_name", "created_at"),
    [
        ("0018_customer_month_value", "2024-01-05"),
        ("0022_sales_volume_by_country_state", "2024-12-25"),
        ("0024_top_customer_by_revenue", "2024-12-02"),
    ],
)
@pytest.mark.parametrize("price", [Decimal("0.29"), Decimal("-0.29"), Decimal("99999999.99"), None])
def test_decimal_revenue_aggregates_exact_cents_before_converting_to_double(
    query_name: str, created_at: str, price: Decimal | None
) -> None:
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE customers (customer_id INT, name VARCHAR, country VARCHAR, state VARCHAR)")
        connection.execute("INSERT INTO customers VALUES (1, 'customer', 'country', 'state')")
        connection.execute("CREATE TABLE orders (order_id INT, customer_id INT, created_at TIMESTAMP)")
        connection.execute("INSERT INTO orders VALUES (1, 1, ?)", [created_at])
        connection.execute("CREATE TABLE order_items (order_id INT, product_id INT, amount INT)")
        connection.execute("INSERT INTO order_items VALUES (1, 1, 3)")
        connection.execute("CREATE TABLE products (product_id INT, price DECIMAL(10,2))")
        connection.execute("INSERT INTO products VALUES (1, ?)", [price])
        expected = connection.execute((QUERIES / f"{query_name}.sql").read_text()).fetchall()
        connection.execute("ALTER TABLE products ALTER price TYPE DOUBLE")
        actual = connection.execute((QUERIES / "questdb" / f"{query_name}.sql").read_text()).fetchall()

    assert actual == [(row[0], row[1], None if row[2] is None else float(row[2])) for row in expected]


def test_backup_conditions_exclude_null_even_when_null_is_not_equal_to_empty_string() -> None:
    query_name = "0013_satisfaction_with_without_backup.sql"
    canonical = (QUERIES / query_name).read_text()
    native = (
        (QUERIES / "questdb" / query_name)
        .read_text()
        .replace("backup_processor <> ''", "(backup_processor IS NULL OR backup_processor <> '')")
    )
    with duckdb.connect() as connection:
        connection.execute(
            "CREATE TABLE order_events (order_id INT, event_created TIMESTAMP, "
            "satisfaction FLOAT, backup_processor VARCHAR)"
        )
        connection.execute(
            "INSERT INTO order_events VALUES (112, '2024-01-01', 10, NULL), "
            "(112, '2024-01-02', 20, ''), (112, '2024-01-03', 30, 'backup')"
        )
        expected = connection.execute(canonical).fetchall()
        actual = connection.execute(native).fetchall()

    assert expected[0][1:] == (1, 1, 30.0, 10.0)
    assert actual == expected
