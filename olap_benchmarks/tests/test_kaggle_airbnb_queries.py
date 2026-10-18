import json
from typing import Any, cast

import duckdb
import pytest

from ..settings import REPO_ROOT

QUERIES = REPO_ROOT / "olap_benchmarks/suites/kaggle_airbnb/queries"


@pytest.mark.parametrize("engine", ["postgres", "timescaledb", "questdb"])
@pytest.mark.parametrize("query", ["04_join_three_tables_array_agg", "05_join_three_tables_row_number"])
def test_preaggregated_airbnb_joins_preserve_duplicates_nulls_and_missing_dimensions(engine: str, query: str) -> None:
    con = cast(duckdb.DuckDBPyConnection, cast(Any, duckdb).connect(":memory:"))
    try:
        con.execute("""
            create table calendar (
                listing_id bigint, price varchar, date date, minimum_nights bigint, maximum_nights bigint
            );
            insert into calendar values
                (1, '$1,234.50', '2025-01-01', 1, 5), (1, '$900.00', '2025-01-02', 2, 6),
                (2, 'empty', '2025-01-01', 1, 4), (3, '$20.00', '2025-01-01', 3, 4),
                (4, null, '2025-01-01', 1, 1), (5, '$5.00', '2025-01-01', 1, 1);
            create table listings (id bigint, name varchar, host_name varchar, host_id bigint);
            insert into listings values (1, 'first', 'host', 7), (1, 'first', 'host', 7),
                (2, 'second', 'host', 7), (5, 'fifth', null, null);
            create table listings_detailed (
                id bigint, source varchar, property_type varchar, room_type varchar,
                has_availability boolean, availability_30 bigint, availability_60 bigint,
                availability_90 bigint, availability_365 bigint
            );
            insert into listings_detailed values (1, 'source', 'house', 'room', true, 2, 3, 4, 5),
                (2, 'source', 'house', 'room', false, 0, 0, 0, 0);
            create table reviews_detailed (id bigint, listing_id bigint, reviewer_id bigint, comments varchar);
            insert into reviews_detailed values (10, 1, 20, 'hello'), (11, 1, 20, 'longer comment'),
                (11, 1, 30, null), (12, 1, null, 'åäö'), (13, 2, null, null), (14, 3, 99, 'orphan'),
                (15, 1, 9007199254740993, 'precise');
        """)
        expected = con.execute((QUERIES / f"{query}.sql").read_text()).fetchall()
        actual = con.execute((QUERIES / engine / f"{query}.sql").read_text()).fetchall()
        if engine == "questdb" and query.startswith("04_"):
            actual = [(*row[:-1], json.loads(row[-1]) if row[-1] is not None else [None]) for row in actual]
    finally:
        con.close()

    assert len(expected) == 5
    assert actual == expected
