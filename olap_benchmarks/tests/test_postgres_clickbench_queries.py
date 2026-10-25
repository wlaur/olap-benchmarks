import re
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from typing import cast

import duckdb
import polars as pl
import pytest
from sqlalchemy import Connection

from ..dbs.postgres import Postgres, PostgresFetchMethod
from ..dbs.timescaledb import TimescaleDB
from ..results.hashing import build_answer_metadata
from ..settings import REPO_ROOT


@pytest.fixture
def q18_decimal_answer() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "userid": [1313338681122956954] * 10,
            "m": [Decimal(minute) for minute in (31, 28, 29, 33, 27, 32, 30, 34, 26, 10)],
            "searchphrase": [""] * 10,
            "count": [589, 578, 572, 567, 557, 554, 552, 546, 540, 539],
        },
        schema_overrides={"m": pl.Decimal(38, 10)},
    )


def _mock_fetch_frame(frame: pl.DataFrame, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    queries: list[str] = []

    def read(query: str, _uri: str) -> pl.DataFrame:
        queries.append(query)
        return frame.clone()

    class Result:
        def keys(self) -> list[str]:
            return frame.columns

        def fetchall(self) -> list[tuple[object, ...]]:
            return frame.rows()

    class FakeConnection:
        def execute(self, query: object) -> Result:
            queries.append(str(query))
            return Result()

    def connect(_self: Postgres, reconnect: bool = False) -> Connection:
        return cast(Connection, FakeConnection())

    monkeypatch.setattr(pl, "read_database_uri", read)
    monkeypatch.setattr(Postgres, "connect", connect)
    return queries


@pytest.mark.parametrize("db_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize("method", ["connectorx", "python"])
def test_clickbench_q18_client_schema_reproduces_sealed_and_peer_hashes(
    db_type: type[Postgres] | type[TimescaleDB],
    method: PostgresFetchMethod,
    q18_decimal_answer: pl.DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = db_type()
    suite = database.benchmarks["clickbench"]
    raw = q18_decimal_answer
    assert raw.schema["m"] == pl.Decimal(38, 10)
    assert build_answer_metadata(raw)["answer_hash"] == "blake2b128:7aae3990077bd97f5d5562be7a989683"
    queries = _mock_fetch_frame(raw, monkeypatch)
    query = suite.load_query("Q18")
    kwargs = suite.query_fetch_kwargs("Q18")
    assert kwargs == {"schema": {"m": pl.Int64}}

    actual = database.fetch(query, method=method, **kwargs)

    assert queries == [query.strip().removesuffix(";")]
    assert actual.schema == {**raw.schema, "m": pl.Int64}
    assert actual.rows() == raw.rows()
    assert actual.drop("m").equals(raw.drop("m"))
    assert build_answer_metadata(actual)["answer_hash"] == "blake2b128:fb1308d0569108347f102270ada59b03"
    assert build_answer_metadata(actual)["answer_hash_version"] == "canonical-v11"
    assert build_answer_metadata(raw)["answer_hash"] == "blake2b128:7aae3990077bd97f5d5562be7a989683"


@pytest.mark.parametrize("db_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize("method", ["connectorx", "python"])
@pytest.mark.parametrize("value", [Decimal("59.5"), Decimal("0.0000000001"), Decimal("-0.5"), 59.5])
def test_clickbench_q18_rejects_fractional_minutes_before_casting(
    db_type: type[Postgres] | type[TimescaleDB],
    method: PostgresFetchMethod,
    value: Decimal | float,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = db_type()
    suite = database.benchmarks["clickbench"]
    raw = pl.DataFrame({"m": [value]})
    _mock_fetch_frame(raw, monkeypatch)

    with pytest.raises(ValueError, match="Column m contains non-integral values"):
        database.fetch(suite.load_query("Q18"), method=method, **suite.query_fetch_kwargs("Q18"))

    assert raw.item() == value


@pytest.mark.parametrize("db_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize("method", ["connectorx", "python"])
def test_clickbench_q18_preserves_nulls_and_integral_values_without_clamping(
    db_type: type[Postgres] | type[TimescaleDB], method: PostgresFetchMethod, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = db_type()
    suite = database.benchmarks["clickbench"]
    raw = pl.DataFrame(
        {"m": [Decimal(0), None, Decimal(59), Decimal(-1), Decimal(60)]},
        schema_overrides={"m": pl.Decimal(38, 10)},
    )
    _mock_fetch_frame(raw, monkeypatch)

    actual = database.fetch(suite.load_query("Q18"), method=method, **suite.query_fetch_kwargs("Q18"))

    assert actual.schema == {"m": pl.Int64}
    assert actual.get_column("m").to_list() == [0, None, 59, -1, 60]


@pytest.mark.parametrize("db_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize("method", ["connectorx", "python"])
def test_clickbench_q18_partial_schema_preserves_empty_result_columns(
    db_type: type[Postgres] | type[TimescaleDB],
    method: PostgresFetchMethod,
    q18_decimal_answer: pl.DataFrame,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = db_type()
    suite = database.benchmarks["clickbench"]
    raw = q18_decimal_answer.head(0)
    _mock_fetch_frame(raw, monkeypatch)

    actual = database.fetch(suite.load_query("Q18"), method=method, **suite.query_fetch_kwargs("Q18"))

    assert actual.columns == raw.columns
    assert actual.shape == (0, 4)
    assert actual.schema["m"] == pl.Int64


@pytest.mark.parametrize("db_type", [Postgres, TimescaleDB])
def test_clickbench_q18_override_preserves_sql_and_other_query_fetches(
    db_type: type[Postgres] | type[TimescaleDB], monkeypatch: pytest.MonkeyPatch
) -> None:
    database = db_type()
    suite = database.benchmarks["clickbench"]
    assert sha256(suite.load_query("Q18").encode()).hexdigest() == (
        "c9e33b31c1ce1dc970a00642717176b6730dc9cff46d9a1665dd86c6faad72d4"
    )
    sql_path = REPO_ROOT / f"olap_benchmarks/suites/clickbench/queries/{database.name}.sql"
    assert (
        sha256(sql_path.read_bytes()).hexdigest() == "1f149020fb3ff3875ae8b68dc75d892be24fe1bd2ccf8210cd4dc5fe256d3703"
    )
    assert all(suite.query_fetch_kwargs(name) == {} for name in suite.planned_query_names() if name != "Q18")
    assert database.benchmarks["tpc_h"].query_fetch_kwargs("Q18") == {"method": "python"}
    raw = pl.DataFrame({"m": [Decimal("59.5")]})
    _mock_fetch_frame(raw, monkeypatch)

    assert database.fetch(suite.load_query("Q17"), **suite.query_fetch_kwargs("Q17")).equals(raw)


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
