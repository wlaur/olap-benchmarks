import duckdb
import pytest

from ..settings import REPO_ROOT


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
