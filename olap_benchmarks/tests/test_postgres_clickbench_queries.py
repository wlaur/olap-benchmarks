import re
from datetime import datetime

import duckdb
import pytest

from ..dbs.postgres import Postgres
from ..dbs.timescaledb import TimescaleDB
from ..settings import REPO_ROOT


@pytest.mark.parametrize("db_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize("query_name", ["Q22", "Q29", "Q35"])
def test_clickbench_aggregate_aliases_preserve_answers(
    db_type: type[Postgres] | type[TimescaleDB], query_name: str
) -> None:
    query = db_type().benchmarks["clickbench"].load_query(query_name)
    with duckdb.connect() as connection:
        connection.execute("""
            CREATE TABLE hits (
                SearchPhrase VARCHAR, URL VARCHAR, Title VARCHAR,
                UserID BIGINT, ResolutionWidth INTEGER, ClientIP BIGINT
            );
            INSERT INTO hits VALUES
                ('alpha', 'https://a.example', 'Google Zebra', 1, 10, 10),
                ('alpha', 'https://b.example', 'Google Apple', 2, 20, 10),
                ('beta', 'https://c.example', 'Google Beta', 3, 30, 4294967295),
                ('', 'https://d.example', 'Google ignored', 4, 40, 4294967295),
                ('alpha', 'https://www.google.com', 'Google excluded', 5, NULL, 20),
                ('null', NULL, NULL, NULL, NULL, NULL)
        """)
        result = connection.execute(query)
        assert result.description is not None
        columns = [column[0] for column in result.description]
        assert len(columns) == len(set(columns))
        if query_name == "Q22":
            assert columns[1:3] == ["min_url", "min_title"]
            assert result.fetchall() == [
                ("alpha", "https://a.example", "Google Apple", 2, 2),
                ("beta", "https://c.example", "Google Beta", 1, 1),
            ]
        elif query_name == "Q29":
            assert columns == [f"resolution_width_sum_{index}" for index in range(90)]
            assert result.fetchall() == [tuple(100 + 4 * index for index in range(90))]
        else:
            assert columns[1:4] == ["clientip_minus_1", "clientip_minus_2", "clientip_minus_3"]
            assert result.fetchall() == [
                (10, 9, 8, 7, 2),
                (4294967295, 4294967294, 4294967293, 4294967292, 2),
                (20, 19, 18, 17, 1),
                (None, None, None, None, 1),
            ]


@pytest.mark.parametrize("db_type", [Postgres, TimescaleDB])
def test_clickbench_url_minimum_uses_byte_order(db_type: type[Postgres] | type[TimescaleDB]) -> None:
    query = db_type().benchmarks["clickbench"].load_query("Q21")
    assert 'MIN(URL COLLATE "C")' in query
    with duckdb.connect() as connection:
        connection.execute("""
            CREATE TABLE hits AS
            SELECT * FROM (VALUES
                ('phrase', 'https://google.example/Zebra'),
                ('phrase', 'https://google.example/alpha'),
                ('', 'https://google.example/A'),
                ('phrase', 'https://other.example/A')
            ) AS input(SearchPhrase, URL)
        """)
        assert connection.execute(query).fetchall() == [("phrase", "https://google.example/Zebra", 2)]


@pytest.mark.parametrize("db_type", [Postgres, TimescaleDB])
def test_clickbench_raw_rows_preserve_canonical_column_order(db_type: type[Postgres] | type[TimescaleDB]) -> None:
    directory = REPO_ROOT / "olap_benchmarks/suites/clickbench/schemas"

    def schema_columns(engine: str) -> list[str]:
        schema = (directory / f"{engine}.sql").read_text().split("CREATE TABLE hits", 1)[1]
        return re.findall(r"^\s+(\w+)\s+\w+", schema, re.MULTILINE)

    canonical = schema_columns("duckdb")
    physical = schema_columns("postgres")
    assert len(canonical) == len(physical) == 105
    assert canonical != physical and set(canonical) == set(physical)
    values = {name: str(index) for index, name in enumerate(canonical)}
    values.update(URL="'https://google.example'", EventTime="TIMESTAMP '2013-07-15 12:00:00'")
    projection = ", ".join(f"{values[name]} AS {name}" for name in physical)
    query = db_type().benchmarks["clickbench"].load_query("Q23")
    with duckdb.connect() as connection:
        connection.execute(f"CREATE TABLE hits AS SELECT {projection}")
        result = connection.execute(query)
        assert result.description is not None
        assert [column[0] for column in result.description] == canonical
        expected = tuple(
            "https://google.example" if name == "URL" else datetime(2013, 7, 15, 12) if name == "EventTime" else index
            for index, name in enumerate(canonical)
        )
        assert result.fetchall() == [expected]
