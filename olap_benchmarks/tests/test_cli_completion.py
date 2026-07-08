from __future__ import annotations

import json
import logging
from collections.abc import Callable

import pytest
from cyclopts.exceptions import CoercionError

from .. import __main__
from ..__main__ import app
from ..settings import DatabaseArg, DatabaseName, SuiteArg, SuiteName
from ..suites import ManualPreparationRequired


def test_cli_registers_install_completion_command() -> None:
    assert app.name == ("olap",)
    assert "--install-completion" in app._commands
    assert app.generate_completion(shell="zsh").splitlines()[0] == "#compdef olap"


def test_runs_lists_filtered_runs(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rows: list[dict[str, object]] = [
        {
            "id": 7,
            "suite": "time_series",
            "db": "timescaledb",
            "operation": "run",
            "status": "failed",
            "started_at": "2026-03-12T00:00:00",
            "finished_at": "2026-03-12T00:00:01",
            "error_type": "RuntimeError",
        }
    ]

    def fake_list_runs(
        revision: str = "default",
        status: str | None = None,
        suite: str | None = None,
        db: str | None = None,
    ) -> list[dict[str, object]]:
        assert revision == "candidate"
        assert status == "failed"
        assert suite == "time_series"
        assert db == "timescaledb"
        return rows

    monkeypatch.setattr(__main__, "list_runs", fake_list_runs)

    __main__.runs(status="failed", revision="candidate", suite="time_series", db="timescaledb")

    assert capsys.readouterr().out == f"{json.dumps(rows, indent=2)}\n"


def test_delete_cmd_deletes_orphaned_runs_by_status(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_delete_runs_by_status(status: str, revision: str = "default") -> int:
        assert status == "orphaned"
        assert revision == "candidate"
        return 2

    def confirm_yes(_prompt: str) -> str:
        return "yes"

    monkeypatch.setattr(__main__, "delete_runs_by_status", fake_delete_runs_by_status)
    monkeypatch.setattr("builtins.input", confirm_yes)

    __main__.delete_cmd(status="orphaned", revision="candidate")

    assert capsys.readouterr().out == "Deleted 2 run(s) and their associated steps and metrics.\n"


def test_delete_cmd_rejects_completed_status() -> None:
    with pytest.raises(CoercionError):
        app(["results", "delete", "--status", "completed"], exit_on_error=False)


def test_delete_cmd_aborts_without_yes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_delete_runs_by_status(_status: str, _revision: str = "default") -> int:
        raise AssertionError("delete_runs_by_status should not be called")

    def confirm_no(_prompt: str) -> str:
        return "no"

    monkeypatch.setattr(__main__, "delete_runs_by_status", unexpected_delete_runs_by_status)
    monkeypatch.setattr("builtins.input", confirm_no)

    with pytest.raises(SystemExit, match="Aborted."):
        __main__.delete_cmd(status="failed")


def test_delete_cmd_force_skips_confirmation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_delete_runs_by_status(status: str, revision: str = "default") -> int:
        assert status == "failed"
        return 1

    def unexpected_input(_prompt: str) -> str:
        raise AssertionError("input should not be called")

    monkeypatch.setattr(__main__, "delete_runs_by_status", fake_delete_runs_by_status)
    monkeypatch.setattr("builtins.input", unexpected_input)

    __main__.delete_cmd(status="failed", force=True)

    assert capsys.readouterr().out == "Deleted 1 run(s) and their associated steps and metrics.\n"


def test_prepare_all_skips_manual_preparation_suites(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    prepared: list[tuple[SuiteName, int]] = []

    def fake_resolve_suites(_suite: SuiteArg) -> list[SuiteName]:
        return ["rtabench", "clickbench", "time_series"]

    def fake_get_suite_preparer(suite_name: SuiteName, scale_factor: int) -> Callable[[], None]:
        def prepare_suite() -> None:
            if suite_name == "clickbench":
                raise ManualPreparationRequired("download hits.parquet")
            prepared.append((suite_name, scale_factor))

        return prepare_suite

    monkeypatch.setattr(__main__, "resolve_suites", fake_resolve_suites)
    monkeypatch.setattr(__main__, "get_suite_preparer", fake_get_suite_preparer)

    with caplog.at_level(logging.WARNING):
        __main__.prepare(suite="all")

    assert prepared == [("rtabench", 1), ("time_series", 1), ("time_series", 10)]
    assert "Skipping clickbench: download hits.parquet" in caplog.text


def test_prepare_explicit_manual_preparation_suite_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_resolve_suites(_suite: SuiteArg) -> list[SuiteName]:
        return ["clickbench"]

    def fake_get_suite_preparer(_suite_name: SuiteName, _scale_factor: int) -> Callable[[], None]:
        def prepare_suite() -> None:
            raise ManualPreparationRequired("download hits.parquet")

        return prepare_suite

    monkeypatch.setattr(__main__, "resolve_suites", fake_resolve_suites)
    monkeypatch.setattr(__main__, "get_suite_preparer", fake_get_suite_preparer)

    with pytest.raises(SystemExit, match="download hits.parquet"):
        __main__.prepare(suite="clickbench")


def test_benchmark_marks_interrupted_runs_failed_after_writer_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummySuite:
        supported_operations = ("populate", "mutate", "select")

    class DummyWriter:
        def __init__(self) -> None:
            self.queue = object()
            self.result_queue = object()
            self.closed = False

        def close(self) -> None:
            self.closed = True

    class DummyDatabase:
        def __init__(self) -> None:
            self._current_suite = None
            self.benchmarks = {"time_series": DummySuite()}

        def set_queues(self, _queue: object, _result_queue: object) -> None:
            return None

        def benchmark(self, _suite: str, _operation: str, scale_factor: int | None = None) -> None:
            assert scale_factor == 1
            raise KeyboardInterrupt

    writer = DummyWriter()
    db_instance = DummyDatabase()
    failed_revisions: list[str] = []

    def fake_resolve_suites(_suite: object) -> list[str]:
        return ["time_series"]

    def fake_resolve_dbs(_db: object) -> list[str]:
        return ["timescaledb"]

    def fake_check_input_data(_suite_name: str, scale_factor: int) -> None:
        assert scale_factor == 1
        return None

    def fake_start_writer_process(revision: str = "default") -> DummyWriter:
        return writer

    def fake_get_databases() -> dict[str, DummyDatabase]:
        return {"timescaledb": db_instance}

    def fake_start_db(_db: object) -> None:
        return None

    def fake_stop_db(_db: object) -> None:
        return None

    def fake_mark_running_runs_failed(revision: str = "default") -> int:
        failed_revisions.append(revision)
        return 1

    monkeypatch.setattr(__main__, "resolve_suites", fake_resolve_suites)
    monkeypatch.setattr(__main__, "resolve_dbs", fake_resolve_dbs)
    monkeypatch.setattr(__main__, "_check_input_data", fake_check_input_data)
    monkeypatch.setattr(__main__, "start_writer_process", fake_start_writer_process)
    monkeypatch.setattr(__main__, "get_databases", fake_get_databases)
    monkeypatch.setattr(__main__, "_start_db", fake_start_db)
    monkeypatch.setattr(__main__, "_stop_db", fake_stop_db)
    monkeypatch.setattr(__main__, "mark_running_runs_failed", fake_mark_running_runs_failed)

    with pytest.raises(KeyboardInterrupt):
        __main__.benchmark(db="timescaledb", suite="time_series", revision="candidate")

    assert writer.closed is True
    assert failed_revisions == ["candidate"]


def test_benchmark_all_uses_suite_supported_operations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyClickbenchSuite:
        supported_operations = ("populate", "select")

    class DummyWriter:
        def __init__(self) -> None:
            self.queue = object()
            self.result_queue = object()

        def close(self) -> None:
            return None

    class DummyDatabase:
        def __init__(self) -> None:
            self._current_suite = None
            self.benchmarks = {"clickbench": DummyClickbenchSuite()}
            self.operations: list[tuple[str, str, int | None]] = []

        def set_queues(self, _queue: object, _result_queue: object) -> None:
            return None

        def benchmark(self, suite: str, operation: str, scale_factor: int | None = None) -> None:
            self.operations.append((suite, operation, scale_factor))

    writer = DummyWriter()
    db_instance = DummyDatabase()

    def fake_resolve_suites(_suite: SuiteArg) -> list[SuiteName]:
        return ["clickbench"]

    def fake_resolve_dbs(_db: DatabaseArg) -> list[DatabaseName]:
        return ["clickhouse"]

    def fake_check_input_data(_suite_name: SuiteName, scale_factor: int) -> None:
        assert scale_factor == 1
        return None

    def fake_start_writer_process(revision: str = "default") -> DummyWriter:
        return writer

    def fake_get_databases() -> dict[DatabaseName, DummyDatabase]:
        return {"clickhouse": db_instance}

    def fake_start_db(_db: DummyDatabase) -> None:
        return None

    def fake_stop_db(_db: DummyDatabase) -> None:
        return None

    validated: list[tuple[str, str, int]] = []

    def fake_assert_latest_query_row_counts(
        revision: str,
        system: str,
        suite: SuiteName,
        suite_scale_factor: int,
    ) -> None:
        assert revision == "default"
        assert system == __main__.SETTINGS.system
        validated.append((system, suite, suite_scale_factor))

    monkeypatch.setattr(__main__, "resolve_suites", fake_resolve_suites)
    monkeypatch.setattr(__main__, "resolve_dbs", fake_resolve_dbs)
    monkeypatch.setattr(__main__, "_check_input_data", fake_check_input_data)
    monkeypatch.setattr(__main__, "start_writer_process", fake_start_writer_process)
    monkeypatch.setattr(__main__, "get_databases", fake_get_databases)
    monkeypatch.setattr(__main__, "_start_db", fake_start_db)
    monkeypatch.setattr(__main__, "_stop_db", fake_stop_db)
    monkeypatch.setattr(__main__, "assert_latest_query_row_counts", fake_assert_latest_query_row_counts)

    __main__.benchmark(db="clickhouse", suite="clickbench", operation="all")

    assert db_instance.operations == [("clickbench", "populate", 1), ("clickbench", "select", 1)]
    assert validated == [(__main__.SETTINGS.system, "clickbench", 1)]


def test_benchmark_all_fans_out_time_series_scale_factors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyTimeSeriesSuite:
        supported_operations = ("select",)

    class DummyWriter:
        def __init__(self) -> None:
            self.queue = object()
            self.result_queue = object()

        def close(self) -> None:
            return None

    class DummyDatabase:
        def __init__(self) -> None:
            self._current_suite = None
            self._current_suite_scale_factor = None
            self.benchmarks = {"time_series": DummyTimeSeriesSuite()}
            self.operations: list[tuple[str, str, int | None]] = []

        def set_queues(self, _queue: object, _result_queue: object) -> None:
            return None

        def benchmark(self, suite: str, operation: str, scale_factor: int | None = None) -> None:
            self.operations.append((suite, operation, scale_factor))

    writer = DummyWriter()
    db_instance = DummyDatabase()
    checked_inputs: list[tuple[SuiteName, int]] = []
    validated: list[tuple[SuiteName, int]] = []

    def fake_resolve_suites(_suite: SuiteArg) -> list[SuiteName]:
        return ["time_series"]

    def fake_resolve_dbs(_db: DatabaseArg) -> list[DatabaseName]:
        return ["duckdb"]

    def fake_check_input_data(suite_name: SuiteName, scale_factor: int) -> None:
        checked_inputs.append((suite_name, scale_factor))

    def fake_start_writer_process(revision: str = "default") -> DummyWriter:
        return writer

    def fake_get_databases() -> dict[DatabaseName, DummyDatabase]:
        return {"duckdb": db_instance}

    def fake_start_db(_db: DummyDatabase) -> None:
        return None

    def fake_stop_db(_db: DummyDatabase) -> None:
        return None

    def fake_assert_latest_query_row_counts(
        revision: str,
        system: str,
        suite: SuiteName,
        suite_scale_factor: int,
    ) -> None:
        assert revision == "default"
        assert system == __main__.SETTINGS.system
        validated.append((suite, suite_scale_factor))

    monkeypatch.setattr(__main__, "resolve_suites", fake_resolve_suites)
    monkeypatch.setattr(__main__, "resolve_dbs", fake_resolve_dbs)
    monkeypatch.setattr(__main__, "_check_input_data", fake_check_input_data)
    monkeypatch.setattr(__main__, "start_writer_process", fake_start_writer_process)
    monkeypatch.setattr(__main__, "get_databases", fake_get_databases)
    monkeypatch.setattr(__main__, "_start_db", fake_start_db)
    monkeypatch.setattr(__main__, "_stop_db", fake_stop_db)
    monkeypatch.setattr(__main__, "assert_latest_query_row_counts", fake_assert_latest_query_row_counts)

    __main__.benchmark(db="duckdb", suite="all", operation="all")

    assert checked_inputs == [("time_series", 1), ("time_series", 10)]
    assert db_instance.operations == [("time_series", "select", 1), ("time_series", "select", 10)]
    assert validated == [("time_series", 1), ("time_series", 10)]
