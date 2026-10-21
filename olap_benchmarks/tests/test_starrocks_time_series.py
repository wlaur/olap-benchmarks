from statistics import stdev

import duckdb
import pytest

from ..dbs.starrocks import StarRocks


@pytest.mark.parametrize(
    ("table", "column"),
    [("tall", "process_4"), ("wide", "process_545"), ("large", "process_364")],
)
@pytest.mark.parametrize("values", [(1.0, 2.0, 3.0), (1.0,), ()])
def test_full_aggregate_uses_sample_standard_deviation(table: str, column: str, values: tuple[float, ...]) -> None:
    suite = StarRocks().benchmarks["time_series"]
    with duckdb.connect() as connection:
        connection.execute("CREATE MACRO stddev(x) AS stddev_pop(x)")
        connection.execute(f"CREATE TABLE data_{table} ({column} FLOAT)")
        for value in (*values, None):
            connection.execute(f"INSERT INTO data_{table} VALUES (?)", [value])
        actual = connection.sql(suite.load_query(f"{table}_07_full_aggregate")).pl()

    assert actual.item(0, "cnt") == len(values)
    if len(values) > 1:
        assert actual.item(0, "stddev_val") == pytest.approx(stdev(values))
    else:
        assert actual.item(0, "stddev_val") is None
