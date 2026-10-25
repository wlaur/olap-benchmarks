from collections.abc import Mapping
from typing import Any

import polars as pl
import pytest

from ..dbs.monetdb import (
    MONETDB_RUNTIME_ENVIRONMENT_SQL,
    MONETDB_RUNTIME_SESSION_SQL,
    MonetDB,
)
from ..settings import get_settings


class RecordingMonetDB(MonetDB):
    steps: list[dict[str, Any]] = []
    queries: list[str] = []
    baselines: list[tuple[str, int]] = []
    environment: dict[str, str] = {
        "gdk_nr_threads": "10",
        "gdk_mem_maxsize": "21002390077",
        "gdk_vm_maxsize": "4398046511104",
        "monet_version": "11.55.9",
    }
    session: dict[str, object] = {
        "optimizer": "default_pipe",
        "workerlimit": 0,
        "memorylimit": 0,
        "querytimeout": 0,
        "sessiontimeout": 0,
        "user_max_workers": 0,
        "user_max_memory": 0,
    }
    session_rows: int = 1
    fetch_error: str | None = None

    def _start_step(self, *args: object, **kwargs: object) -> int:
        self.steps.append({"args": args, **kwargs})
        return len(self.steps)

    def _finish_step(self, step_id: int, *args: object, **kwargs: object) -> None:
        self.steps[step_id - 1].update(kwargs)

    def _record_session_baseline(self, step_name: str, expected_sessions: int) -> None:
        self.baselines.append((step_name, expected_sessions))

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
    ) -> pl.DataFrame:
        self.queries.append(query)
        if self.fetch_error:
            raise RuntimeError(self.fetch_error)
        if query == MONETDB_RUNTIME_ENVIRONMENT_SQL:
            return pl.DataFrame({"name": list(self.environment), "value": list(self.environment.values())})
        assert query == MONETDB_RUNTIME_SESSION_SQL
        return pl.DataFrame([self.session] * self.session_rows)


def test_operation_records_effective_server_and_current_session_settings() -> None:
    database = RecordingMonetDB()

    database.before_benchmark_operation()

    assert database.baselines == [("session_baseline_before", 1)]
    assert database.queries == [MONETDB_RUNTIME_ENVIRONMENT_SQL, MONETDB_RUNTIME_SESSION_SQL]
    assert database.steps == [
        {
            "args": ("phase", "monetdb_runtime_settings"),
            "status": "completed",
            "step_type": "phase",
            "metadata": {
                "environment": database.environment,
                "session": database.session,
                "session_units": {"querytimeout": "seconds"},
            },
        }
    ]
    assert "sys.current_sessionid()" in MONETDB_RUNTIME_SESSION_SQL


def test_runtime_capture_preserves_restrictive_settings_for_diagnosis() -> None:
    database = RecordingMonetDB(
        environment={
            "gdk_nr_threads": "2",
            "gdk_mem_maxsize": "6000000",
            "gdk_vm_maxsize": "9000000",
            "monet_version": "11.55.9",
        },
        session={"optimizer": "sequential_pipe", "workerlimit": 1, "memorylimit": 1024},
    )

    database.before_benchmark_operation()

    assert database.steps[0]["metadata"] == {
        "environment": database.environment,
        "session": database.session,
        "session_units": {"querytimeout": "seconds"},
    }


@pytest.mark.parametrize("seconds", [300, None])
def test_runtime_capture_records_native_timeout_seconds(seconds: int | None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(get_settings(), "query_timeout_seconds", seconds)
    database = RecordingMonetDB()
    database._current_operation = "select"
    database.session["querytimeout"] = seconds or 0

    database.before_benchmark_operation()

    metadata = database.steps[0]["metadata"]
    assert metadata["session"]["querytimeout"] == (seconds or 0)
    assert metadata["session_units"]["querytimeout"] == "seconds"


@pytest.mark.parametrize("observed", [0, 300_000, 300_000_000])
def test_runtime_capture_rejects_an_ineffective_select_timeout(observed: int, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(get_settings(), "query_timeout_seconds", 300)
    database = RecordingMonetDB()
    database._current_operation = "select"
    database.session["querytimeout"] = observed

    with pytest.raises(RuntimeError, match="MonetDB query timeout mismatch: requested 300 seconds"):
        database.before_benchmark_operation()

    assert database.steps[0]["status"] == "failed"


@pytest.mark.parametrize("session_rows", [0, 2])
def test_runtime_capture_requires_exactly_the_current_session(session_rows: int) -> None:
    database = RecordingMonetDB(session_rows=session_rows)

    with pytest.raises(RuntimeError, match="Expected one current MonetDB session"):
        database.before_benchmark_operation()

    assert database.steps[0]["status"] == "failed"


def test_runtime_capture_rejects_missing_environment_values() -> None:
    database = RecordingMonetDB(environment={"monet_version": "11.55.9"})

    with pytest.raises(RuntimeError, match="missing settings: gdk_mem_maxsize, gdk_nr_threads, gdk_vm_maxsize"):
        database.before_benchmark_operation()

    assert database.steps[0]["status"] == "failed"


def test_runtime_capture_records_and_propagates_driver_errors() -> None:
    database = RecordingMonetDB(fetch_error="connection lost")

    with pytest.raises(RuntimeError, match="connection lost"):
        database.before_benchmark_operation()

    assert database.steps[0]["status"] == "failed"
    assert database.steps[0]["error_type"] == "RuntimeError"
    assert database.steps[0]["error_message"] == "connection lost"
