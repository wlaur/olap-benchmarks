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


@pytest.mark.parametrize("db_type", [Postgres, TimescaleDB])
def test_clickbench_phrase_order_uses_byte_order(db_type: type[Postgres] | type[TimescaleDB]) -> None:
    query = db_type().benchmarks["clickbench"].load_query("Q25")
    assert 'ORDER BY SearchPhrase COLLATE "C"' in query
    with duckdb.connect() as connection:
        connection.execute(
            "CREATE TABLE hits AS SELECT * FROM (VALUES ('alpha'), ('Zebra'), (''), ('Ärger')) AS t(SearchPhrase)"
        )
        assert connection.execute(query).fetchall() == [("Zebra",), ("alpha",), ("Ärger",)]


@pytest.mark.parametrize("db_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize(("query_name", "column"), [("Q27", "URL"), ("Q28", "Referer")])
def test_clickbench_average_lengths_count_bytes(
    db_type: type[Postgres] | type[TimescaleDB], query_name: str, column: str
) -> None:
    query = db_type().benchmarks["clickbench"].load_query(query_name)
    assert f"AVG(octet_length({column}))" in query and "AVG(length(" not in query
    if query_name == "Q27":
        with duckdb.connect() as connection:
            connection.execute(
                "CREATE TABLE hits AS SELECT 7 AS CounterID, 'жж' AS URL FROM range(100001)"
                " UNION ALL SELECT 8, 'ab' FROM range(100001)"
            )
            rows = connection.execute(query.replace("octet_length(", "strlen(")).fetchall()
            assert rows == [(7, 4.0, 100001), (8, 2.0, 100001)]


@pytest.mark.parametrize("db_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize("query_name", ["Q38", "Q39"])
def test_clickbench_offset_ties_use_byte_order(db_type: type[Postgres] | type[TimescaleDB], query_name: str) -> None:
    query = db_type().benchmarks["clickbench"].load_query(query_name)
    keys = ["URL"] if query_name == "Q38" else ["END", "URL"]
    assert all(f'{key} COLLATE "C" ASC' in query for key in keys)
    with duckdb.connect() as connection:
        connection.execute("""
            CREATE TABLE hits AS
            SELECT
                62 AS CounterID, DATE '2013-07-15' AS EventDate, 0 AS IsRefresh, 1 AS IsLink, 0 AS IsDownload,
                0 AS TraficSourceID, 0 AS SearchEngineID, 0 AS AdvEngineID, '' AS Referer,
                CASE WHEN i % 2 = 0 THEN 'a' ELSE 'B' END || lpad(i::VARCHAR, 4, '0') AS URL
            FROM range(1004) AS t(i)
        """)
        urls = [row[-2] for row in connection.execute(query).fetchall()]
        assert urls == ["a0996", "a0998", "a1000", "a1002"]
