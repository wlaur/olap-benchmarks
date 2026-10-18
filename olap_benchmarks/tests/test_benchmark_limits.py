from __future__ import annotations

import os
import signal
from collections.abc import Mapping
from contextlib import suppress
from multiprocessing import Queue, get_context
from multiprocessing.process import BaseProcess
from pathlib import Path
from time import sleep
from types import SimpleNamespace
from typing import Literal, NamedTuple, cast

import polars as pl
import psutil
import pytest
from pydantic import ValidationError

from .. import dbs as dbs_module
from .. import limits, operation_runner
from .. import suites as suites_module
from ..dbs.duckdb import DuckDB
from ..metrics.storage import Storage, WriterMessage, start_writer_process
from ..settings import Settings
from ..suites.time_series_metrics import config as metrics_config
from ..suites.time_series_ranges import config as ranges_config
from .test_benchmark_profile import ProfileDatabase, ProfileSuite, RecordingStorage


class _DiskUsage(NamedTuple):
    total: int
    used: int
    free: int


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    for name in ("inputs", "temporary", "results"):
        (tmp_path / name).mkdir()
    settings = Settings.model_construct(
        input_data_directory=tmp_path / "inputs",
        database_directory=tmp_path / "database" / "new",
        temporary_directory=tmp_path / "temporary",
        results_directory=tmp_path / "results",
        system="test",
    )
    for module in (limits, operation_runner, suites_module, metrics_config, ranges_config, dbs_module):
        monkeypatch.setattr(module, "get_settings", lambda: settings)
    return settings


def test_disk_reserve_checks_existing_ancestors_of_every_output_directory(
    settings: Settings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checked: list[Path] = []

    def usage(path: Path) -> _DiskUsage:
        assert path.is_dir()
        checked.append(path)
        return _DiskUsage(100 * 1024**3, 60 * 1024**3, 40 * 1024**3)

    monkeypatch.setattr(limits, "disk_usage", usage)

    limits.check_disk_space()

    assert set(checked) == {
        tmp_path,
        settings.input_data_directory,
        settings.temporary_directory,
        settings.results_directory,
    }


@pytest.mark.parametrize("directory", ["database", "inputs", "temporary", "results"])
def test_disk_reserve_rejects_a_full_output_filesystem(
    settings: Settings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, directory: str
) -> None:
    low_path = tmp_path if directory == "database" else tmp_path / directory

    def usage(path: Path) -> _DiskUsage:
        return _DiskUsage(100 * 1024**3, 0, 39 * 1024**3 if path == low_path else 100 * 1024**3)

    monkeypatch.setattr(limits, "disk_usage", usage)

    with pytest.raises(OSError, match="Disk reserve reached.*require 40 GiB"):
        limits.check_disk_space()


@pytest.mark.parametrize("reserve_gb", [10, 40])
def test_polars_spill_budget_preserves_both_campaign_and_minimum_headroom(
    settings: Settings, monkeypatch: pytest.MonkeyPatch, reserve_gb: int
) -> None:
    settings.minimum_free_disk_gb = reserve_gb
    for name in ("POLARS_TEMP_DIR", "POLARS_OOC_SPILL_DIR", "POLARS_OOC_DISK_BUDGET_MB"):
        monkeypatch.delenv(name, raising=False)

    def usage(_path: Path) -> _DiskUsage:
        return _DiskUsage(0, 0, 100 * 1024**3)

    monkeypatch.setattr(operation_runner.shutil, "disk_usage", usage)

    operation_runner.configure_polars_spill()

    spill = settings.temporary_directory / "polars"
    assert spill.is_dir()
    assert os.environ["POLARS_TEMP_DIR"] == spill.as_posix()
    assert os.environ["POLARS_OOC_SPILL_DIR"] == spill.as_posix()
    assert int(os.environ["POLARS_OOC_DISK_BUDGET_MB"]) == (100 - max(30, reserve_gb)) * 1024


def test_polars_spill_rejects_a_budget_that_would_consume_the_reserve(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    def usage(_path: Path) -> _DiskUsage:
        return _DiskUsage(0, 0, 40 * 1024**3)

    monkeypatch.setattr(operation_runner.shutil, "disk_usage", usage)

    with pytest.raises(OSError, match="Not enough free disk for Polars spill"):
        operation_runner.configure_polars_spill()


@pytest.mark.parametrize("expire", [False, True])
def test_query_deadline_restores_signal_handler_and_disarms_alarm(expire: bool) -> None:
    previous = signal.getsignal(signal.SIGALRM)
    try:
        with limits.query_deadline(30):
            assert signal.getitimer(signal.ITIMER_REAL)[0] > 0
            if expire:
                with pytest.raises(TimeoutError, match="Query exceeded 30 seconds"):
                    signal.raise_signal(signal.SIGALRM)
            else:
                raise ValueError("query failed")
    except ValueError:
        assert not expire

    assert signal.getsignal(signal.SIGALRM) == previous
    assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)


def test_disabled_query_deadline_does_not_change_an_existing_alarm() -> None:
    previous = signal.getsignal(signal.SIGALRM)
    signal.setitimer(signal.ITIMER_REAL, 30)
    try:
        with limits.query_deadline(None):
            assert signal.getsignal(signal.SIGALRM) == previous
            assert signal.getitimer(signal.ITIMER_REAL)[0] > 0
        assert signal.getitimer(signal.ITIMER_REAL)[0] > 0
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)


def test_query_watchdog_expires_only_an_active_overdue_query(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "query.json"
    monkeypatch.setattr(limits, "monotonic", lambda: 100.0)
    assert limits.expired_query(path, 1) is None
    limits.start_query_watchdog(path, "slow")
    assert limits.expired_query(path, 1) is None
    monkeypatch.setattr(limits, "monotonic", lambda: 101.1)
    assert limits.expired_query(path, 1) == "slow"
    assert limits.expired_query(path, None) is None
    path.unlink()
    assert limits.expired_query(path, 1) is None


def test_query_timeout_records_failure_and_clears_active_step(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings.query_timeout_seconds = 30
    storage = RecordingStorage()
    database = ProfileDatabase()
    database._result_storage = cast(Storage, storage)
    database._run_id = 1
    database._current_operation = "select"

    def fetch(
        _self: ProfileDatabase,
        _query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
    ) -> pl.DataFrame:
        signal.raise_signal(signal.SIGALRM)
        raise AssertionError("query deadline did not interrupt fetch")

    monkeypatch.setattr(ProfileDatabase, "fetch", fetch)

    with pytest.raises(TimeoutError, match="Query exceeded 30 seconds"):
        database.execute_query_iteration("slow", 1, "select 'slow'")

    assert database.active_step_id is None
    assert storage.steps[1]["status"] == "failed"
    assert storage.steps[1]["error_type"] == "TimeoutError"
    assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)


def test_query_filters_record_skips_without_removing_queries_from_the_plan(settings: Settings) -> None:
    settings.query_include = ("a", "b")
    settings.query_exclude = ("b",)
    storage = RecordingStorage()
    database = ProfileDatabase(abort_at=("", 0))
    database._result_storage = cast(Storage, storage)
    database._run_id = 1
    database._current_operation = "select"
    suite = ProfileSuite(db=database)

    suite.run_select_queries()

    assert tuple(suite.planned_query_names()) == ("a", "b", "c")
    assert database.iteration_counter == {"a": database.profile.total_iterations}
    assert len(storage.steps) == 3 * database.profile.total_iterations
    assert {step["result_status"] for step in storage.steps.values() if step["query_name"] == "a"} == {"ok"}
    for name, reason in (("b", "query_exclude"), ("c", "query_include")):
        skipped = [step for step in storage.steps.values() if step["query_name"] == name]
        assert {step["result_status"] for step in skipped} == {"skipped"}
        assert all(reason in str(step["metadata"]) for step in skipped)


@pytest.mark.parametrize("suite_name", ["time_series_metrics", "time_series_ranges"])
def test_table_filters_limit_load_and_verification_but_preserve_query_plan(settings: Settings, suite_name: str) -> None:
    settings.tables = ("wide100", "eav100?")
    if suite_name == "time_series_metrics":
        suite = metrics_config.TimeSeriesMetrics(db=DuckDB())
        expected_queries = metrics_config.QUERY_NAMES
    else:
        suite = ranges_config.TimeSeriesRanges(db=DuckDB())
        expected_queries = ranges_config.QUERY_NAMES

    assert set(suite.dataset_files()) == {"wide100", "eav1000"}
    assert set(suite.expected_table_row_counts()) == {"wide100", "eav1000"}
    assert suite.planned_query_names() == expected_queries
    for query in expected_queries:
        skip = suite.query_skip(query)
        if query.startswith(("wide100_", "eav1000_")):
            assert skip is None
        else:
            assert skip == ("skipped", "table excluded by campaign tables")


@pytest.mark.parametrize(
    "field",
    ["minimum_free_disk_gb", "operation_timeout_seconds", "query_timeout_seconds", "client_memory_limit_gb"],
)
@pytest.mark.parametrize("value", [0, -1])
def test_resource_limits_must_be_positive(settings: Settings, field: str, value: int) -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate({**settings.model_dump(), field: value})


class _StuckProcess:
    pid: int = 12345
    exitcode: int | None = None

    def __init__(self, **_kwargs: object) -> None:
        self.started = False
        self.killed = False
        self.joined: list[float | None] = []

    def start(self) -> None:
        self.started = True

    def is_alive(self) -> bool:
        return self.started and not self.killed

    def join(self, timeout: float | None = None) -> None:
        self.joined.append(timeout)

    def kill(self) -> None:
        self.killed = True


class _MemoryInfo(NamedTuple):
    rss: int


class _MemoryProcess:
    def __init__(self, _pid: int) -> None:
        pass

    def memory_info(self) -> _MemoryInfo:
        return _MemoryInfo(3 * 1024**3)


@pytest.mark.parametrize("resource", ["time", "memory", "disk", "query"])
def test_operation_resource_limit_escalates_after_interrupt_and_kills_its_process_group(
    settings: Settings, monkeypatch: pytest.MonkeyPatch, resource: Literal["time", "memory", "disk", "query"]
) -> None:
    settings.operation_timeout_seconds = 10 if resource == "time" else None
    settings.client_memory_limit_gb = 2 if resource == "memory" else None
    process = _StuckProcess()
    signals: list[tuple[int, int]] = []
    group_signals: list[tuple[int, int]] = []
    clock = iter((0.0, 11.0, 22.0, 33.0))
    disk_checks = 0

    def new_process(**_kwargs: object) -> _StuckProcess:
        return process

    def context(method: str) -> SimpleNamespace:
        assert method == "spawn"
        return SimpleNamespace(Process=new_process)

    def send_signal(pid: int, signum: int) -> None:
        signals.append((pid, signum))

    def send_group_signal(pgid: int, signum: int) -> None:
        group_signals.append((pgid, signum))
        if signum == signal.SIGKILL:
            process.kill()

    def disk_space() -> None:
        nonlocal disk_checks
        disk_checks += 1
        if disk_checks > 1 and resource == "disk":
            raise OSError("Disk reserve reached during operation")

    def query_expired(_path: Path, _timeout: int | None) -> str | None:
        return "slow" if resource == "query" else None

    monkeypatch.setattr(operation_runner, "get_context", context)
    monkeypatch.setattr(operation_runner, "monotonic", lambda: next(clock))
    monkeypatch.setattr(operation_runner, "check_disk_space", disk_space)
    monkeypatch.setattr(operation_runner, "configure_polars_spill", lambda: None)
    monkeypatch.setattr(operation_runner, "expired_query", query_expired)
    monkeypatch.setattr(operation_runner.os, "kill", send_signal)
    monkeypatch.setattr(operation_runner.os, "killpg", send_group_signal)
    monkeypatch.setattr(operation_runner.psutil, "Process", _MemoryProcess)
    expected_error, message = {
        "time": (TimeoutError, "exceeded operation time budget"),
        "memory": (MemoryError, "client exceeded 2 GiB RSS"),
        "disk": (OSError, "Disk reserve reached during operation"),
        "query": (TimeoutError, "Query slow exceeded"),
    }[resource]

    with pytest.raises(expected_error, match=message):
        operation_runner.run_operation(
            DuckDB(),
            "clickbench",
            "select",
            1,
            cast("Queue[WriterMessage]", object()),
            cast("Queue[object]", object()),
        )

    assert signals == [(process.pid, signal.SIGINT)]
    assert group_signals == [(process.pid, signal.SIGTERM), (process.pid, signal.SIGKILL)]
    assert process.killed
    assert not process.is_alive()
    assert process.joined == [1, 5, 5]


@pytest.mark.parametrize("before_first_poll", [False, True])
def test_abrupt_operation_exit_still_kills_the_process_group(
    settings: Settings, monkeypatch: pytest.MonkeyPatch, before_first_poll: bool
) -> None:
    class CrashedProcess(_StuckProcess):
        def start(self) -> None:
            super().start()
            if before_first_poll:
                self.killed = True
                self.exitcode = -signal.SIGSEGV

        def join(self, timeout: float | None = None) -> None:
            super().join(timeout)
            self.killed = True
            self.exitcode = -signal.SIGSEGV

    process = CrashedProcess()
    group_signals: list[tuple[int, int]] = []
    clock = iter((0.0, 11.0))

    def new_process(**_kwargs: object) -> CrashedProcess:
        return process

    def context(method: str) -> SimpleNamespace:
        assert method == "spawn"
        return SimpleNamespace(Process=new_process)

    def send_group_signal(pgid: int, signum: int) -> None:
        group_signals.append((pgid, signum))

    monkeypatch.setattr(operation_runner, "get_context", context)
    monkeypatch.setattr(operation_runner, "monotonic", lambda: next(clock))
    monkeypatch.setattr(operation_runner, "check_disk_space", lambda: None)
    monkeypatch.setattr(operation_runner, "configure_polars_spill", lambda: None)
    monkeypatch.setattr(operation_runner.os, "killpg", send_group_signal)
    settings.operation_timeout_seconds = None

    with pytest.raises(RuntimeError, match="exit code -11"):
        operation_runner.run_operation(
            DuckDB(),
            "clickbench",
            "select",
            1,
            cast("Queue[WriterMessage]", object()),
            cast("Queue[object]", object()),
        )

    assert group_signals == [(process.pid, signal.SIGTERM), (process.pid, signal.SIGKILL)]
    assert process.joined == ([5] if before_first_poll else [1, 5])


def test_operation_creates_its_session_before_database_setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    database = ProfileDatabase()

    def databases() -> dict[str, ProfileDatabase]:
        events.append("database")
        return {"duckdb": database}

    def benchmark(_self: ProfileDatabase, _suite: str, _operation: str, *, scale_factor: int) -> None:
        events.append("benchmark")

    monkeypatch.setattr(operation_runner.os, "setsid", lambda: events.append("session"))
    monkeypatch.setattr(operation_runner, "get_databases", databases)
    monkeypatch.setattr(ProfileDatabase, "benchmark", benchmark)

    operation_runner._operation_entrypoint(
        "duckdb",
        "clickbench",
        "select",
        1,
        cast("Queue[WriterMessage]", object()),
        cast("Queue[object]", object()),
        None,
        False,
        tmp_path / "watchdog.json",
    )

    assert events == ["session", "database", "benchmark"]


def _crash_with_orphaned_helper(path: Path) -> None:
    os.setsid()
    helper = get_context("spawn").Process(target=sleep, args=(60,))
    helper.start()
    path.write_text(str(helper.pid))
    os._exit(17)


def test_abrupt_exit_cleanup_stops_a_spawned_helper_and_preserves_the_writer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    results_directory = tmp_path / "results"
    results_directory.mkdir()
    monkeypatch.setenv("OLAP_BENCHMARKS_RESULTS_DIRECTORY", str(results_directory))
    writer = start_writer_process(revision="operation-cleanup")
    storage = Storage(writer.queue, writer.result_queue, process=writer.process)
    helper_path = tmp_path / "helper.pid"
    process = get_context("spawn").Process(target=_crash_with_orphaned_helper, args=(helper_path,))

    try:
        assert storage.debug("before cleanup") > 0
        process.start()
        process.join(timeout=15)
        assert process.exitcode == 17
        helper = psutil.Process(int(helper_path.read_text()))

        operation_runner._stop_operation(process)

        helper.wait(timeout=5)
        assert writer.process.is_alive()
        assert storage.debug("after cleanup") > 0
    finally:
        if process.pid is not None:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            if process.is_alive():
                process.kill()
            process.join(timeout=5)
        writer.close()


@pytest.mark.parametrize("denied_signal", [0, signal.SIGTERM, signal.SIGKILL])
def test_operation_cleanup_permission_errors_still_kill_and_join_the_child(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, denied_signal: int
) -> None:
    process = _StuckProcess()
    process.start()
    group_signals: list[tuple[int, int]] = []
    clock = iter((0.0, 0.0, 4.0))

    def send_group_signal(pgid: int, signum: int) -> None:
        group_signals.append((pgid, signum))
        if signum == denied_signal:
            raise PermissionError("group permission denied")

    def ignore_signal(_pid: int, _signum: int) -> None:
        pass

    monkeypatch.setattr(operation_runner.os, "kill", ignore_signal)
    monkeypatch.setattr(operation_runner.os, "killpg", send_group_signal)
    monkeypatch.setattr(operation_runner, "monotonic", lambda: next(clock))

    def ignore_sleep(_seconds: float) -> None:
        pass

    monkeypatch.setattr(operation_runner, "sleep", ignore_sleep)

    operation_runner._stop_operation(cast(BaseProcess, process))

    assert group_signals[0] == (process.pid, signal.SIGTERM)
    assert group_signals[-1] == (process.pid, signal.SIGKILL)
    assert process.killed
    assert process.joined[-1] == 5
    assert "Permission denied" in caplog.text


def test_operation_cleanup_does_not_confuse_an_unprobeable_zombie_group_with_success(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    process = _StuckProcess()
    process.started = True
    process.killed = True
    process.exitcode = 17
    group_signals: list[tuple[int, int]] = []

    def send_group_signal(pgid: int, signum: int) -> None:
        group_signals.append((pgid, signum))
        if signum in (0, signal.SIGKILL):
            raise PermissionError("zombie-only group cannot be signalled")

    monkeypatch.setattr(operation_runner.os, "killpg", send_group_signal)
    monkeypatch.setattr(operation_runner, "monotonic", lambda: 0.0)

    operation_runner._stop_operation(cast(BaseProcess, process))

    assert group_signals == [(process.pid, signal.SIGTERM), (process.pid, 0), (process.pid, signal.SIGKILL)]
    assert process.joined == [0, 5]
    assert "probing operation process group" in caplog.text
    assert "killing operation process group" in caplog.text
    assert "cleanup is unconfirmed" in caplog.text


def test_operation_cleanup_always_joins_after_an_unexpected_group_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    process = _StuckProcess()
    process.start()

    def fail_group_signal(_pgid: int, _signum: int) -> None:
        raise RuntimeError("unexpected group failure")

    def ignore_signal(_pid: int, _signum: int) -> None:
        pass

    monkeypatch.setattr(operation_runner.os, "kill", ignore_signal)
    monkeypatch.setattr(operation_runner.os, "killpg", fail_group_signal)

    with pytest.raises(RuntimeError, match="unexpected group failure"):
        operation_runner._stop_operation(cast(BaseProcess, process))

    assert process.killed
    assert process.joined == [5, 5]


def test_operation_cleanup_always_joins_and_reports_a_child_it_cannot_kill(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    class UnkillableProcess(_StuckProcess):
        def kill(self) -> None:
            raise PermissionError("child permission denied")

    process = UnkillableProcess()
    process.start()

    def missing_group(_pgid: int, _signum: int) -> None:
        raise ProcessLookupError("group already gone")

    def ignore_signal(_pid: int, _signum: int) -> None:
        pass

    monkeypatch.setattr(operation_runner.os, "kill", ignore_signal)
    monkeypatch.setattr(operation_runner.os, "killpg", missing_group)

    operation_runner._stop_operation(cast(BaseProcess, process))

    assert process.joined == [5, 5]
    assert "Permission denied killing operation process" in caplog.text
    assert "remains alive after cleanup" in caplog.text


def test_operation_preserves_its_original_failure_when_cleanup_raises(
    settings: Settings, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    process = _StuckProcess()
    settings.operation_timeout_seconds = None
    settings.client_memory_limit_gb = None

    def new_process(**_kwargs: object) -> _StuckProcess:
        return process

    def context(method: str) -> SimpleNamespace:
        assert method == "spawn"
        return SimpleNamespace(Process=new_process)

    def fail_cleanup(_process: BaseProcess) -> None:
        raise RuntimeError("unexpected cleanup failure")

    monkeypatch.setattr(operation_runner, "get_context", context)
    monkeypatch.setattr(operation_runner, "check_disk_space", lambda: None)
    monkeypatch.setattr(operation_runner, "configure_polars_spill", lambda: None)

    def query_expired(_path: Path, _timeout: int | None) -> str:
        return "slow"

    monkeypatch.setattr(operation_runner, "expired_query", query_expired)
    monkeypatch.setattr(operation_runner, "_stop_operation", fail_cleanup)

    with pytest.raises(TimeoutError, match="Query slow exceeded"):
        operation_runner.run_operation(
            DuckDB(),
            "clickbench",
            "select",
            1,
            cast("Queue[WriterMessage]", object()),
            cast("Queue[object]", object()),
        )

    assert "Failed to clean up benchmark operation process" in caplog.text
    assert "unexpected cleanup failure" in caplog.text
