from __future__ import annotations

import json

import pytest
from cyclopts.exceptions import CoercionError

from .. import __main__
from ..__main__ import app
from ..settings import DatabaseArg, DatabaseName, SuiteArg, SuiteName


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

        def benchmark(self, _suite: str, _operation: str) -> None:
            raise KeyboardInterrupt

    writer = DummyWriter()
    db_instance = DummyDatabase()
    failed_revisions: list[str] = []

    def fake_resolve_suites(_suite: object) -> list[str]:
        return ["time_series"]

    def fake_resolve_dbs(_db: object) -> list[str]:
        return ["timescaledb"]

    def fake_check_input_data(_suite_name: str) -> None:
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
            self.operations: list[tuple[str, str]] = []

        def set_queues(self, _queue: object, _result_queue: object) -> None:
            return None

        def benchmark(self, suite: str, operation: str) -> None:
            self.operations.append((suite, operation))

    writer = DummyWriter()
    db_instance = DummyDatabase()

    def fake_resolve_suites(_suite: SuiteArg) -> list[SuiteName]:
        return ["clickbench"]

    def fake_resolve_dbs(_db: DatabaseArg) -> list[DatabaseName]:
        return ["clickhouse"]

    def fake_check_input_data(_suite_name: SuiteName) -> None:
        return None

    def fake_start_writer_process(revision: str = "default") -> DummyWriter:
        return writer

    def fake_get_databases() -> dict[DatabaseName, DummyDatabase]:
        return {"clickhouse": db_instance}

    def fake_start_db(_db: DummyDatabase) -> None:
        return None

    def fake_stop_db(_db: DummyDatabase) -> None:
        return None

    monkeypatch.setattr(__main__, "resolve_suites", fake_resolve_suites)
    monkeypatch.setattr(__main__, "resolve_dbs", fake_resolve_dbs)
    monkeypatch.setattr(__main__, "_check_input_data", fake_check_input_data)
    monkeypatch.setattr(__main__, "start_writer_process", fake_start_writer_process)
    monkeypatch.setattr(__main__, "get_databases", fake_get_databases)
    monkeypatch.setattr(__main__, "_start_db", fake_start_db)
    monkeypatch.setattr(__main__, "_stop_db", fake_stop_db)

    __main__.benchmark(db="clickhouse", suite="clickbench", operation="all")

    assert db_instance.operations == [("clickbench", "populate"), ("clickbench", "select")]
