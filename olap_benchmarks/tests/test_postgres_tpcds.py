from decimal import Decimal
from pathlib import Path
from typing import cast
from urllib.parse import parse_qs, urlsplit

import polars as pl
import pytest
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
        if query in ("46", "72", "78"):
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
