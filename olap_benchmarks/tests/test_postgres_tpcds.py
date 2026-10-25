import json
from collections import Counter
from collections.abc import Iterator
from decimal import Decimal
from fractions import Fraction
from hashlib import sha256
from pathlib import Path
from typing import cast
from urllib.parse import parse_qs, urlsplit

import duckdb
import polars as pl
import pytest
from pydantic import JsonValue
from sqlalchemy import Connection

from ..dbs import postgres as postgres_module
from ..dbs.postgres import Postgres, PostgresTpcDs, PostgresTpcH
from ..dbs.timescaledb import TimescaleDB
from ..settings import Operation, get_settings
from ..suites.tpc_ds.config import TPCDS_QUERY_NAMES
from ..suites.tpc_h.config import TPCH_QUERY_NAMES


@pytest.mark.parametrize("database_type", [Postgres, TimescaleDB])
def test_postgres_family_tpc_suites_select_faithful_python_fetch_and_record_it(database_type: type[Postgres]) -> None:
    database = database_type()
    database._current_suite = "tpc_ds"
    database._current_suite_scale_factor = 1
    suite = database.benchmarks["tpc_ds"]

    assert isinstance(suite, PostgresTpcDs)
    assert suite.planned_query_names() == TPCDS_QUERY_NAMES
    assert suite.fetch_kwargs == {"method": "python"}
    assert all(suite.query_fetch_kwargs(query) == {"method": "python"} for query in TPCDS_QUERY_NAMES)
    assert database.run_options["select_fetch_method"] == "python"
    assert database.db_driver is None

    database._current_suite = "tpc_h"
    database._current_suite_scale_factor = 10
    tpc_h = database.benchmarks["tpc_h"]
    assert isinstance(tpc_h, PostgresTpcH)
    assert tpc_h.planned_query_names() == TPCH_QUERY_NAMES
    assert tpc_h.fetch_kwargs == {"method": "python"}
    assert all(tpc_h.query_fetch_kwargs(query) == {"method": "python"} for query in TPCH_QUERY_NAMES)
    assert database.run_options["select_fetch_method"] == "python"
    assert database.db_driver is None

    database._current_suite = "kaggle_airbnb"
    database._current_suite_scale_factor = 1
    assert database.run_options["select_fetch_method"] == "connectorx"


@pytest.mark.parametrize("database_type", [Postgres, TimescaleDB])
def test_postgres_family_tpcds_shares_only_the_explicit_query_overrides(database_type: type[Postgres]) -> None:
    suite = database_type().benchmarks["tpc_ds"]
    for query in TPCDS_QUERY_NAMES:
        path = suite.query_path(query)
        assert path is not None
        if query in ("31", "46", "49", "65", "72", "78", "85"):
            assert path == suite.queries_directory / "postgres" / f"{query}.sql"
        elif query in ("01", "04"):
            assert path == suite.queries_directory / database_type().name / f"{query}.sql"
        else:
            assert path == suite.queries_directory / f"{query}.sql"

    for query, text_keys in {
        "46": ["c_last_name", "c_first_name", "ca_city", "bought_city"],
        "72": ["i_item_desc", "w_warehouse_name"],
    }.items():
        canonical = (suite.queries_directory / f"{query}.sql").read_text()
        override = suite.load_query(query)
        assert override.rsplit("ORDER BY", 1)[0] == canonical.rsplit("ORDER BY", 1)[0]
        for key in text_keys:
            assert f'{key} COLLATE "C" NULLS FIRST' in override
        assert override.endswith("LIMIT 100;\n")


def _without_sql_locations(value: JsonValue) -> JsonValue:
    if isinstance(value, list):
        return [_without_sql_locations(item) for item in value]
    if isinstance(value, dict):
        return {
            key: _without_sql_locations(item)
            for key, item in value.items()
            if key not in ("query_location", "query_location_length")
        }
    return value


def _sql_tree(connection: duckdb.DuckDBPyConnection, sql: str) -> dict[str, JsonValue]:
    row = connection.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()
    assert row is not None
    parsed = cast(dict[str, JsonValue], json.loads(str(row[0])))
    assert parsed["error"] is False
    statements = cast(list[dict[str, JsonValue]], parsed["statements"])
    assert len(statements) == 1
    node = cast(dict[str, JsonValue], _without_sql_locations(statements[0]["node"]))
    assert node["type"] == "SELECT_NODE"
    return node


@pytest.mark.parametrize("database_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize(
    ("query", "text_sort_indices", "sort_count", "canonical_sha256", "candidate_sha256"),
    [
        (
            "31",
            (0,),
            1,
            "d1d1ee84af35662f8e0b766c0c7bbc38c1277c243a87433ee10c1e63c30c4d8c",
            "bc24b32a90c24664424e9cec95ea34974b544c5c192cfdc4875d80cb352a4160",
        ),
        (
            "65",
            (0, 1),
            2,
            "a2f14a96bb6280a725a18e84a94b99fda0a299e0c1aacbdb56a07b40d364ceb3",
            "82150b95ee4de318010fc68956317cbcc856cfcc5f73c0ff2db869c88a8efbcc",
        ),
        (
            "85",
            (0,),
            4,
            "09f2031d466106cfe8a42db595bf2184d0db3144f1d323b74556b02911bde766",
            "526af09fbdce11d928ff75dc9a4902b3a3f049a2e6064ddfb921e4d8ed5e5da6",
        ),
    ],
)
def test_postgres_family_tpcds_collation_only_wraps_approved_outer_sort_keys(
    database_type: type[Postgres],
    query: str,
    text_sort_indices: tuple[int, ...],
    sort_count: int,
    canonical_sha256: str,
    candidate_sha256: str,
) -> None:
    suite = database_type().benchmarks["tpc_ds"]
    canonical = (suite.queries_directory / f"{query}.sql").read_text()
    override = suite.load_query(query)
    assert sha256(canonical.encode()).hexdigest() == canonical_sha256
    assert sha256(override.encode()).hexdigest() == candidate_sha256
    assert override.replace(' COLLATE "C"', "") == canonical

    with duckdb.connect(":memory:", config={"threads": 1}) as connection:
        original = _sql_tree(connection, canonical)
        adapted = _sql_tree(connection, override)
    modifiers = cast(list[dict[str, JsonValue]], adapted["modifiers"])
    order_modifiers = [modifier for modifier in modifiers if modifier["type"] == "ORDER_MODIFIER"]
    assert len(order_modifiers) == 1
    orders = cast(list[dict[str, JsonValue]], order_modifiers[0]["orders"])
    assert len(orders) == sort_count
    for index in text_sort_indices:
        expression = cast(dict[str, JsonValue], orders[index]["expression"])
        assert expression["class"] == "COLLATE"
        assert expression["collation"] == "C"
        orders[index]["expression"] = expression["child"]
    assert adapted == original


def _sql_nodes(value: JsonValue) -> Iterator[dict[str, JsonValue]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _sql_nodes(child)
    elif isinstance(value, list):
        for child in value:
            yield from _sql_nodes(child)


@pytest.mark.parametrize("database_type", [Postgres, TimescaleDB])
def test_postgres_family_q49_only_converts_all_twelve_decimal_ratio_operands(database_type: type[Postgres]) -> None:
    suite = database_type().benchmarks["tpc_ds"]
    canonical = (suite.queries_directory / "49.sql").read_text()
    candidate = suite.load_query("49")
    assert suite.query_path("49") == suite.queries_directory / "postgres" / "49.sql"
    assert sha256(canonical.encode()).hexdigest() == "2a4a143b28d1dda71b3113b04d4d2e264e95fa9fbe314bbfd7b13c2c060e174e"
    assert sha256(candidate.encode()).hexdigest() == "4d408183a0884a31edbb341f8ff1ab0f159ed00615f5044e52fb787fd4c3797b"
    with duckdb.connect(":memory:", config={"threads": 1}) as connection:
        original = _sql_tree(connection, canonical)
        adapted = _sql_tree(connection, candidate)
    decimal_operands = [node for node in _sql_nodes(original) if node.get("class") == "CAST"]
    assert len(decimal_operands) == 12
    double_casts = [
        node
        for node in _sql_nodes(adapted)
        if node.get("class") == "CAST"
        and isinstance(type_expr := node.get("type_expr"), dict)
        and type_expr.get("type_name") == "DOUBLE"
    ]
    assert len(double_casts) == 12
    converted_operands: list[dict[str, JsonValue]] = []
    for node in double_casts:
        assert node["try_cast"] is False
        assert node["alias"] == ""
        child = cast(dict[str, JsonValue], node["child"])
        assert child["class"] == "CAST"
        assert cast(dict[str, JsonValue], child["type_expr"])["type_name"] == "DECIMAL"
        assert cast(dict[str, JsonValue], child["child"])["function_name"] == "sum"
        converted_operands.append(child)
        node.clear()
        node.update(child)
    assert Counter(json.dumps(node, sort_keys=True) for node in converted_operands) == Counter(
        json.dumps(node, sort_keys=True) for node in decimal_operands
    )
    assert adapted == original


@pytest.mark.parametrize(
    ("engine", "expected_sha256"),
    [
        ("", "2a4a143b28d1dda71b3113b04d4d2e264e95fa9fbe314bbfd7b13c2c060e174e"),
        ("clickhouse", "fa14a4eb7a0ba07e49343ed5a463959496f6a3affe83751f2dcd6e5b439ab67b"),
        ("doris", "6d308bf144bbda11f4d757bc14419cf202dd3c9a90f391a3e9c008fe7716a32a"),
        ("monetdb", "748434baf46e81a6600af0614100c7a0b8b904aa3f458943050f60d6e958a9ef"),
        ("starrocks", "221df208bb03df804318a44c9b7e11faa924a56dab0740acff3fe2f89dd19ef8"),
    ],
)
def test_q49_candidate_keeps_base_and_other_engine_sql_unchanged(engine: str, expected_sha256: str) -> None:
    path = Postgres().benchmarks["tpc_ds"].queries_directory / engine / "49.sql"
    assert sha256(path.read_bytes()).hexdigest() == expected_sha256


def test_q49_exact_aggregate_operands_split_a_rational_tie_after_binary64_conversion() -> None:
    operands = [
        (Decimal("12824.8000"), Decimal("12985.1100")),
        (Decimal("16503.2000"), Decimal("16709.4900")),
        (Decimal("11832.8000"), Decimal("11980.7100")),
    ]
    exact = [Fraction(numerator) / Fraction(denominator) for numerator, denominator in operands]
    assert exact == [Fraction(80, 81)] * 3
    decimal = [numerator / denominator for numerator, denominator in operands]
    assert len(set(decimal)) == 1
    binary = [float(numerator) / float(denominator) for numerator, denominator in operands]
    assert [value.hex() for value in binary] == [
        "0x1.f9add3c0ca458p-1",
        "0x1.f9add3c0ca458p-1",
        "0x1.f9add3c0ca459p-1",
    ]
    assert [1 + sum(other < value for other in decimal) for value in decimal] == [1, 1, 1]
    assert [1 + sum(other < value for other in binary) for value in binary] == [1, 1, 3]


def test_timescale_specific_tpcds_query_has_priority_over_the_shared_postgres_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for database_name in ("postgres", "timescaledb"):
        directory = tmp_path / database_name
        directory.mkdir()
        (directory / "46.sql").write_text(database_name)

    def queries_directory(_self: PostgresTpcDs[Postgres]) -> Path:
        return tmp_path

    monkeypatch.setattr(PostgresTpcDs, "queries_directory", property(queries_directory))
    suite = TimescaleDB().benchmarks["tpc_ds"]

    assert suite.load_query("46") == "timescaledb"


@pytest.mark.parametrize("database_type", [Postgres, TimescaleDB])
def test_postgres_python_fetch_preserves_small_decimal_precision_and_exact_integer_scale(
    database_type: type[Postgres], monkeypatch: pytest.MonkeyPatch
) -> None:
    ratio = Decimal("0.0702677249135792468")
    count = Decimal(9007199254740993)

    class Result:
        def keys(self) -> list[str]:
            return ["ratio", "count"]

        def fetchall(self) -> list[tuple[Decimal, Decimal]]:
            return [(ratio, count)]

    class PythonConnection:
        def execute(self, _query: object) -> Result:
            return Result()

    def connect(_self: Postgres, reconnect: bool = False) -> Connection:
        assert not reconnect
        return cast(Connection, PythonConnection())

    monkeypatch.setattr(Postgres, "connect", connect)
    database = database_type()
    suite = database.benchmarks["tpc_ds"]

    frame = database.fetch("SELECT decimal_values", **suite.fetch_kwargs)

    assert frame.item(0, "ratio") == ratio
    assert frame.item(0, "count") == count
    ratio_type = frame.schema["ratio"]
    count_type = frame.schema["count"]
    assert isinstance(ratio_type, pl.Decimal) and ratio_type.scale == 19
    assert isinstance(count_type, pl.Decimal) and count_type.scale == 0


@pytest.mark.parametrize("database_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize(("operation", "seconds"), [("select", 45), ("select", None), ("populate", 45)])
def test_postgres_python_connection_has_startup_timeout_and_preserves_options_without_extra_sql(
    database_type: type[Postgres], operation: Operation, seconds: int | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "query_timeout_seconds", seconds)
    database = database_type(
        connection_string="postgresql://user:pass+word@localhost/postgres?options=-cwork_mem%3D64MB"
    )
    database._current_operation = operation
    uris: list[str] = []

    class PythonConnection:
        def __init__(self) -> None:
            self.info: dict[str, object] = {}

        def execute(self, _query: object) -> None:
            raise AssertionError("Connection initialization must not issue SET")

    connection = PythonConnection()

    class Engine:
        def connect(self) -> Connection:
            return cast(Connection, connection)

    def create_engine(uri: str) -> Engine:
        uris.append(uri)
        return Engine()

    monkeypatch.setattr(postgres_module, "create_engine", create_engine)

    assert database.connect() is database.connect()
    assert len(uris) == 1
    parsed = urlsplit(uris[0])
    expected = "-cwork_mem=64MB"
    if operation == "select" and seconds is not None:
        expected += " -cstatement_timeout=45000"
    assert parse_qs(parsed.query)["options"] == [expected]
    assert "+" not in parsed.query
    assert parsed.password == "pass+word"
