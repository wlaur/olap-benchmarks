from __future__ import annotations

import json

import pytest

from .. import __main__
from ..__main__ import app


def test_cli_registers_install_completion_command() -> None:
    assert app.name == ("olap",)
    assert "--install-completion" in app._commands
    assert app.generate_completion(shell="zsh").splitlines()[0] == "#compdef olap"


def test_failed_runs_shortcut_lists_failed_runs(
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

    __main__.failed_runs(revision="candidate", suite="time_series", db="timescaledb")

    assert capsys.readouterr().out == f"{json.dumps(rows, indent=2)}\n"


def test_delete_cmd_deletes_aborted_runs_by_status(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_delete_runs_by_status(status: str, revision: str = "default") -> int:
        assert status == "aborted"
        assert revision == "candidate"
        return 2

    def confirm_yes(_prompt: str) -> str:
        return "yes"

    monkeypatch.setattr(__main__, "delete_runs_by_status", fake_delete_runs_by_status)
    monkeypatch.setattr("builtins.input", confirm_yes)

    __main__.delete_cmd(status="aborted", revision="candidate")

    assert capsys.readouterr().out == "Deleted 2 run(s) and their associated steps and metrics.\n"


def test_delete_cmd_rejects_completed_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_delete_runs_by_status(status: str, revision: str = "default") -> int:
        del status, revision
        raise AssertionError("delete_runs_by_status should not be called")

    monkeypatch.setattr(__main__, "delete_runs_by_status", unexpected_delete_runs_by_status)

    with pytest.raises(SystemExit, match="only supports 'failed' or 'aborted'"):
        __main__.delete_cmd(status="completed")


def test_delete_cmd_aborts_without_yes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_delete_runs_by_status(status: str, revision: str = "default") -> int:
        del status, revision
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
        del revision
        return 1

    def unexpected_input(_prompt: str) -> str:
        raise AssertionError("input should not be called")

    monkeypatch.setattr(__main__, "delete_runs_by_status", fake_delete_runs_by_status)
    monkeypatch.setattr("builtins.input", unexpected_input)

    __main__.delete_cmd(status="failed", force=True)

    assert capsys.readouterr().out == "Deleted 1 run(s) and their associated steps and metrics.\n"


def test_benchmark_marks_interrupted_runs_aborted_after_writer_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

        def set_queues(self, queue: object, result_queue: object) -> None:
            del queue, result_queue

        def benchmark(self, suite: str, operation: str) -> None:
            del suite, operation
            raise KeyboardInterrupt

    writer = DummyWriter()
    db_instance = DummyDatabase()
    aborted_revisions: list[str] = []

    def fake_resolve_suites(suite: object) -> list[str]:
        del suite
        return ["time_series"]

    def fake_resolve_dbs(db: object) -> list[str]:
        del db
        return ["timescaledb"]

    def fake_check_input_data(suite_name: str) -> None:
        del suite_name

    def fake_start_writer_process(revision: str = "default") -> DummyWriter:
        del revision
        return writer

    def fake_get_dbs() -> dict[str, DummyDatabase]:
        return {"timescaledb": db_instance}

    def fake_start_db(db: object) -> None:
        del db

    def fake_stop_db(db: object) -> None:
        del db

    def fake_abort_running_runs(revision: str = "default") -> int:
        aborted_revisions.append(revision)
        return 1

    monkeypatch.setattr(__main__, "resolve_suites", fake_resolve_suites)
    monkeypatch.setattr(__main__, "resolve_dbs", fake_resolve_dbs)
    monkeypatch.setattr(__main__, "_check_input_data", fake_check_input_data)
    monkeypatch.setattr(__main__, "start_writer_process", fake_start_writer_process)
    monkeypatch.setattr(__main__, "_get_dbs", fake_get_dbs)
    monkeypatch.setattr(__main__, "_start_db", fake_start_db)
    monkeypatch.setattr(__main__, "_stop_db", fake_stop_db)
    monkeypatch.setattr(__main__, "abort_running_runs", fake_abort_running_runs)

    with pytest.raises(KeyboardInterrupt):
        __main__.benchmark(db="timescaledb", suite="time_series", revision="candidate")

    assert writer.closed is True
    assert aborted_revisions == ["candidate"]
