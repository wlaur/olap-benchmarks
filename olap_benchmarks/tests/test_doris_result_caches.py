from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, cast

import polars as pl
import pytest
from pymysql.constants import FIELD_TYPE
from sqlalchemy import Engine

from ..dbs import doris
from ..dbs.doris import DORIS_CACHE_INIT_SQL, DORIS_CACHE_SETTINGS_SQL, Doris, DorisFetchMethod

_CACHE_NAMES = (
    "enable_sql_cache",
    "enable_query_cache",
)


def _settings() -> dict[str, object]:
    return {
        **dict.fromkeys(_CACHE_NAMES, "false"),
        "enable_condition_cache": "true",
        "enable_inverted_index_query_cache": "true",
        "enable_page_cache": "true",
        "enable_file_cache": "true",
        "enable_segment_cache": "true",
        "enable_inverted_index_searcher_cache": "true",
        "version": "5.7.99",
        "version_comment": "Doris version doris-4.1.5-release",
    }


@dataclass
class _Cursor:
    description: tuple[tuple[str, int], ...]


@dataclass
class _Result:
    settings: dict[str, object]
    cursor: _Cursor | None = None

    def mappings(self) -> _Result:
        return self

    def one(self) -> dict[str, object]:
        return self.settings

    def keys(self) -> list[str]:
        return list(self.settings)

    def fetchall(self) -> list[tuple[object, ...]]:
        return [tuple(self.settings.values())]


@dataclass
class _Connection:
    engine: _Engine
    info: dict[str, object] = field(default_factory=dict[str, object])
    queries: list[str] = field(default_factory=list[str])
    closed: bool = False

    def execute(self, query: object) -> _Result:
        sql = str(query)
        self.queries.append(sql)
        settings: dict[str, object] = self.engine.settings if sql == DORIS_CACHE_SETTINGS_SQL else {"one": 1}
        cursor = _Cursor(tuple((name, FIELD_TYPE.LONGLONG) for name in settings))
        return _Result(settings, cursor)

    def close(self) -> None:
        self.closed = True


@dataclass
class _Engine:
    settings: dict[str, object] = field(default_factory=_settings)
    connections: list[_Connection] = field(default_factory=list[_Connection])
    disposed: bool = False

    def connect(self) -> _Connection:
        connection = _Connection(self)
        self.connections.append(connection)
        return connection

    def dispose(self) -> None:
        self.disposed = True


def _install_engines(monkeypatch: pytest.MonkeyPatch, engines: list[_Engine]) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def create_engine(uri: str, **kwargs: object) -> Engine:
        calls.append({"uri": uri, **kwargs})
        return cast(Engine, engines[len(calls) - 1])

    monkeypatch.setattr(doris, "create_engine", create_engine)
    return calls


@pytest.mark.parametrize("method", [None, "python"])
def test_python_fetch_initializes_and_verifies_reused_and_replacement_sessions(
    monkeypatch: pytest.MonkeyPatch, method: DorisFetchMethod | None
) -> None:
    engines = [_Engine(), _Engine()]
    calls = _install_engines(monkeypatch, engines)
    database = Doris()

    for _ in range(2):
        df = database.fetch("select 1 as one;") if method is None else database.fetch("select 1 as one;", method=method)
        assert df.to_dict(as_series=False) == {"one": [1]}
    assert len(calls) == 1
    assert engines[0].connections[0].queries == [DORIS_CACHE_SETTINGS_SQL, "select 1 as one", "select 1 as one"]

    database.connect(reconnect=True)
    assert len(calls) == 2
    assert engines[0].disposed
    assert engines[0].connections[0].closed
    for call in calls:
        assert call["connect_args"] == {"init_command": "SET " + ", ".join(f"{name} = false" for name in _CACHE_NAMES)}
    assert engines[1].connections[0].queries == [DORIS_CACHE_SETTINGS_SQL]


@pytest.mark.parametrize("name", _CACHE_NAMES)
@pytest.mark.parametrize("value", ["true", True, 1, None, "unexpected"])
def test_python_connection_rejects_enabled_missing_or_unknown_cache_settings(
    monkeypatch: pytest.MonkeyPatch, name: str, value: object
) -> None:
    engine = _Engine()
    engine.settings[name] = value
    _install_engines(monkeypatch, [engine])
    database = Doris()

    with pytest.raises(RuntimeError, match=name):
        database.fetch("select 1 as one")

    assert database._connection is None
    assert engine.disposed
    assert engine.connections[0].closed
    assert engine.connections[0].queries == [DORIS_CACHE_SETTINGS_SQL]


@pytest.mark.parametrize("value", [False, 0, "false", "FALSE", "OFF", "0"])
def test_python_connection_accepts_disabled_server_boolean_representations(
    monkeypatch: pytest.MonkeyPatch, value: object
) -> None:
    engine = _Engine()
    engine.settings.update(dict.fromkeys(_CACHE_NAMES, value))
    _install_engines(monkeypatch, [engine])
    assert isinstance(Doris().connect(), _Connection)


def test_python_connection_rejects_an_absent_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _Engine()
    del engine.settings["enable_query_cache"]
    _install_engines(monkeypatch, [engine])
    with pytest.raises(RuntimeError, match="enable_query_cache"):
        Doris().connect()
    assert engine.disposed


def test_python_initialization_errors_dispose_the_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    class RejectingEngine(_Engine):
        def connect(self) -> _Connection:
            raise RuntimeError("Unknown system variable")

    engine = RejectingEngine()
    _install_engines(monkeypatch, [engine])
    with pytest.raises(RuntimeError, match="Unknown system variable"):
        Doris().connect()
    assert engine.disposed


def test_connectorx_initializes_each_fetch_and_preserves_uri_and_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, list[str] | None]] = []

    def read_database_uri(query: str, uri: str, *, pre_execution_query: list[str] | None = None) -> pl.DataFrame:
        calls.append((query, uri, pre_execution_query))
        return pl.DataFrame({"one": [1]})

    monkeypatch.setattr(pl, "read_database_uri", read_database_uri)
    database = Doris(connection_string="mysql+pymysql://reader:password@server:1234/db?prefer_socket=false")
    for _ in range(2):
        result = database.fetch("select 1 as one;", schema={"one": pl.Int32}, method="connectorx")
        assert result.schema == {"one": pl.Int32}
    assert (
        calls
        == [("select 1 as one", "mysql://reader:password@server:1234/db?prefer_socket=false", [DORIS_CACHE_INIT_SQL])]
        * 2
    )


def test_connectorx_initialization_errors_are_not_retried_without_cache_controls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def read_database_uri(query: str, uri: str, *, pre_execution_query: list[str] | None = None) -> pl.DataFrame:
        nonlocal calls
        calls += 1
        assert pre_execution_query == [DORIS_CACHE_INIT_SQL]
        raise RuntimeError("Unknown system variable")

    monkeypatch.setattr(pl, "read_database_uri", read_database_uri)
    with pytest.raises(RuntimeError, match="Unknown system variable"):
        Doris().fetch("select 1 as one", method="connectorx")
    assert calls == 1


class _RecordingDoris(Doris):
    steps: list[dict[str, Any]] = []

    def _start_step(self, *args: object, **kwargs: object) -> int:
        self.steps.append({"args": args, "kwargs": kwargs})
        return len(self.steps)

    def _finish_step(self, step_id: int, *args: object, **kwargs: object) -> None:
        self.steps[step_id - 1].update(kwargs)


@pytest.mark.parametrize("bad_method", [None, "python", "connectorx"])
def test_operation_gates_record_effective_settings_and_reject_drift_on_both_fetch_paths(
    monkeypatch: pytest.MonkeyPatch, bad_method: DorisFetchMethod | None
) -> None:
    calls: list[DorisFetchMethod] = []

    def fetch(
        self: Doris,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
        method: DorisFetchMethod = "python",
    ) -> pl.DataFrame:
        assert query == DORIS_CACHE_SETTINGS_SQL
        calls.append(method)
        settings = _settings()
        if method == bad_method:
            settings["enable_sql_cache"] = "true"
        return pl.DataFrame({name: [value] for name, value in settings.items()})

    monkeypatch.setattr(_RecordingDoris, "fetch", fetch)
    database = _RecordingDoris()
    for hook in (database.before_benchmark_operation, database.after_benchmark_operation):
        if bad_method is None:
            hook()
        else:
            with pytest.raises(RuntimeError, match="enable_sql_cache"):
                hook()

    assert [step["args"] for step in database.steps] == [
        ("phase", "doris_cache_settings_before"),
        ("phase", "doris_cache_settings_after"),
    ]
    expected_methods = ["python"] if bad_method == "python" else ["python", "connectorx"]
    assert calls == expected_methods * 2
    for step in database.steps:
        assert step["status"] == ("completed" if bad_method is None else "failed")
        for method in expected_methods:
            settings = step["metadata"]["sessions"][method]
            assert settings["version_comment"] == "Doris version doris-4.1.5-release"
            assert settings["enable_page_cache"] == "true"
            assert settings["enable_sql_cache"] == ("true" if method == bad_method else "false")


def test_run_options_declare_cache_policy_and_preserve_data_and_predicate_caches() -> None:
    database = Doris()
    assert database.run_options["result_cache_policy"] == "disabled-v1"
    assert database.run_options["session_settings"] == dict.fromkeys(_CACHE_NAMES, False)
    assert database.run_options["connectorx_pre_execution_query"] == [DORIS_CACHE_INIT_SQL]
    for name in (
        "enable_condition_cache",
        "enable_inverted_index_query_cache",
        "enable_page_cache",
        "enable_file_cache",
        "enable_segment_cache",
        "enable_inverted_index_searcher_cache",
    ):
        assert name not in DORIS_CACHE_INIT_SQL
        assert name in DORIS_CACHE_SETTINGS_SQL
