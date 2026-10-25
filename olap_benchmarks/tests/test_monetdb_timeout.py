from typing import cast
from unittest.mock import Mock

import pyarrow as pa
import pytest
from sqlalchemy import Connection, Engine

from ..dbs import monetdb
from ..dbs.monetdb import MonetDB, adbc
from ..settings import Operation, get_settings


@pytest.fixture
def connection_factory(monkeypatch: pytest.MonkeyPatch) -> Mock:
    factory = Mock()

    def create_engine(*args: object, **kwargs: object) -> Mock:
        engine = Mock(spec=Engine)
        connection = Mock(spec=Connection)
        connection.info = {}
        connection.engine = engine
        engine.connect.return_value = connection
        return engine

    factory.side_effect = create_engine
    monkeypatch.setattr(monetdb, "create_engine", factory)
    return factory


@pytest.mark.parametrize(
    ("operation", "seconds", "expected"),
    [
        ("select", 300, "CALL sys.setquerytimeout(300)"),
        ("select", None, None),
        ("populate", 300, None),
        ("mutate", 300, None),
        ("concurrent", 300, None),
        ("analytical", 300, None),
        (None, 300, None),
    ],
)
def test_select_timeout_is_set_once_on_the_arrow_fetch_session(
    operation: Operation | None,
    seconds: int | None,
    expected: str | None,
    monkeypatch: pytest.MonkeyPatch,
    connection_factory: Mock,
) -> None:
    monkeypatch.setattr(get_settings(), "query_timeout_seconds", seconds)
    database = MonetDB()
    database._current_operation = operation
    connection = cast(Mock, database.connect())

    def fetch_arrow_table(actual_connection: Connection, query: str) -> pa.Table:
        assert actual_connection is connection
        assert query == "SELECT 1"
        assert [str(call.args[0]) for call in connection.execute.call_args_list] == ([expected] if expected else [])
        return pa.table({"value": [1]})

    monkeypatch.setattr(adbc, "fetch_arrow_table", fetch_arrow_table)
    for _ in range(3):
        assert database.fetch("SELECT 1").item() == 1

    assert connection_factory.call_count == 1
    assert database.run_options["native_query_timeout_seconds"] == (seconds if operation == "select" else None)
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()
    if expected:
        connection.execute.return_value.close.assert_called_once()


def test_session_cached_before_select_gets_timeout_before_runtime_capture(
    monkeypatch: pytest.MonkeyPatch, connection_factory: Mock
) -> None:
    monkeypatch.setattr(get_settings(), "query_timeout_seconds", 300)
    database = MonetDB()
    connection = cast(Mock, database.connect())
    connection.execute.assert_not_called()
    database._current_operation = "select"
    baseline = Mock()
    capture = Mock(side_effect=database.connect)
    monkeypatch.setattr(MonetDB, "_record_session_baseline", baseline)
    monkeypatch.setattr(MonetDB, "_record_runtime_settings", capture)

    database.before_benchmark_operation()

    baseline.assert_called_once_with("session_baseline_before", expected_sessions=1)
    assert database.connect() is connection
    assert connection_factory.call_count == 1
    assert str(connection.execute.call_args.args[0]) == "CALL sys.setquerytimeout(300)"
    connection.execute.assert_called_once()


def test_reconnect_reapplies_timeout_and_closes_old_session(
    monkeypatch: pytest.MonkeyPatch, connection_factory: Mock
) -> None:
    monkeypatch.setattr(get_settings(), "query_timeout_seconds", 300)
    database = MonetDB()
    database._current_operation = "select"
    previous = cast(Mock, database.connect())

    current = cast(Mock, database.connect(reconnect=True))

    assert current is not previous
    previous.close.assert_called_once()
    previous.engine.dispose.assert_called_once()
    assert str(current.execute.call_args.args[0]) == "CALL sys.setquerytimeout(300)"
    current.execute.assert_called_once()
    assert connection_factory.call_count == 2
    baseline = Mock()
    monkeypatch.setattr(MonetDB, "_record_session_baseline", baseline)

    database.after_benchmark_operation()

    current.close.assert_called_once()
    current.engine.dispose.assert_called_once()
    assert database._connection is None
    baseline.assert_called_once_with("session_baseline_after", expected_sessions=0)


@pytest.mark.parametrize(("operation", "seconds"), [("populate", 300), ("mutate", 300), ("select", None)])
def test_reused_session_does_not_keep_select_cap(
    operation: Operation, seconds: int | None, monkeypatch: pytest.MonkeyPatch, connection_factory: Mock
) -> None:
    monkeypatch.setattr(get_settings(), "query_timeout_seconds", 300)
    database = MonetDB()
    database._current_operation = "select"
    connection = cast(Mock, database.connect())
    database._current_operation = operation
    monkeypatch.setattr(get_settings(), "query_timeout_seconds", seconds)

    assert database.connect() is connection
    assert database.connect() is connection

    assert [str(call.args[0]) for call in connection.execute.call_args_list] == [
        "CALL sys.setquerytimeout(300)",
        "CALL sys.setquerytimeout(0)",
    ]
    assert connection_factory.call_count == 1
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()


@pytest.mark.parametrize("error", [RuntimeError("setting rejected"), KeyboardInterrupt()])
def test_configuration_failure_discards_session_and_propagates(
    error: BaseException, monkeypatch: pytest.MonkeyPatch, connection_factory: Mock
) -> None:
    monkeypatch.setattr(get_settings(), "query_timeout_seconds", 300)
    database = MonetDB()
    connection = cast(Mock, database.connect())
    connection.execute.side_effect = error
    database._current_operation = "select"
    fetch = Mock()
    monkeypatch.setattr(adbc, "fetch_arrow_table", fetch)

    with pytest.raises(type(error)) as raised:
        database.fetch("SELECT 1")

    assert raised.value is error
    assert database._connection is None
    connection.close.assert_called_once()
    connection.engine.dispose.assert_called_once()
    fetch.assert_not_called()
    current = cast(Mock, database.connect())
    assert current is not connection
    assert str(current.execute.call_args.args[0]) == "CALL sys.setquerytimeout(300)"
    assert connection_factory.call_count == 2
