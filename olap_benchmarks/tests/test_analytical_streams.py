from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Generator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from multiprocessing import Queue
from pathlib import Path
from threading import Barrier, Event, Lock, Thread
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import polars as pl
import pytest

from .. import __main__ as cli
from .. import analytical_streams as streams
from .. import dbs, operation_runner, suites
from ..dbs import Database, get_databases
from ..dbs.clickhouse import Clickhouse
from ..metrics import measure
from ..metrics.storage import Storage, WriterMessage, WriterProcessHandle
from ..run_metadata import sha256_text
from ..settings import SUITE_SPECS, DatabaseName, Settings
from .test_benchmark_profile import ProfileDatabase, ProfileSuite, RecordingStorage


class StreamStorage(RecordingStorage):
    def __init__(self) -> None:
        super().__init__()
        self.lock = Lock()

    def start_step(self, **kwargs: object) -> int:
        with self.lock:
            return super().start_step(**kwargs)

    def finish_step(self, step_id: int, **kwargs: object) -> None:
        with self.lock:
            super().finish_step(step_id, **kwargs)


def _unit_budget(_database: Database) -> dict[str, object]:
    return {"test": True}


def _unit_runtime(_database: Database) -> dict[str, object]:
    return {"test_runtime": True}


def _record(events: list[str], name: str) -> Callable[..., None]:
    def record(*_args: object, **_kwargs: object) -> None:
        events.append(name)

    return record


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    settings = Settings.model_construct(
        input_data_directory=tmp_path / "inputs",
        database_directory=tmp_path / "database",
        temporary_directory=tmp_path / "temporary",
        results_directory=tmp_path / "results",
        system="analytical-unit-test",
        benchmark_threads=10,
        benchmark_memory_gb=24,
        client_memory_limit_gb=28,
        operation_timeout_seconds=120,
        query_timeout_seconds=30,
    )
    for module in (streams, dbs, suites, cli, operation_runner):
        monkeypatch.setattr(module, "get_settings", lambda: settings)
    return settings


@pytest.fixture
def database(settings: Settings) -> ProfileDatabase:
    _ = settings
    database = ProfileDatabase(abort_at=("never", 0))
    database._current_suite = "clickbench"
    database._current_suite_scale_factor = 1
    database._current_operation = "analytical"
    database._run_id = 1
    database._result_storage = cast(Storage, StreamStorage())
    return database


def test_balanced_schedule_is_seeded_reproducible_and_rotated() -> None:
    names = tuple(f"q{i}" for i in range(22))
    order = streams.balanced_query_order(names, 42, 0)
    assert order == streams.balanced_query_order(names, 42, 0)
    assert set(order) == set(names)
    assert order != streams.balanced_query_order(names, 43, 0)
    assert streams.balanced_query_order(names, 42, 4) == order[4:] + order[:4]
    counts = Counter(order[i % len(order)] for i in range(109))
    assert max(counts.values()) - min(counts.values()) == 1


@pytest.mark.parametrize("names", [(), ("a", "a")])
def test_schedule_rejects_empty_or_duplicate_manifest(names: tuple[str, ...]) -> None:
    with pytest.raises(ValueError, match="nonempty and unique"):
        streams.balanced_query_order(names, 0, 0)


@pytest.mark.parametrize("n", [0, 3, 5, 99, 100, 999, 1_000])
def test_percentiles_expose_sample_count_and_suppress_sparse_tails(n: int) -> None:
    summary = streams.latency_summary([float(i) for i in range(1, n + 1)])
    assert summary["n"] == n
    for percentile, minimum in streams.PERCENTILE_MINIMUM_N.items():
        value = cast(dict[str, object], summary[f"p{percentile}"])
        assert value["minimum_n"] == minimum
        assert value["status"] == ("available" if n >= minimum else "insufficient_samples")
        if n < minimum:
            assert value["value_ms"] is None
    if n == 1_000:
        assert cast(dict[str, object], summary["p99"])["value_ms"] == 990
        assert cast(dict[str, object], summary["p95"])["value_ms"] == 950


def test_summary_counts_failures_without_using_their_latencies() -> None:
    samples = [
        streams.RequestSample("a", "measured", "completed", 10),
        streams.RequestSample("a", "measured", "error", 1_000),
        streams.RequestSample("a", "measured", "timeout", 2_000),
    ]
    summary = streams.summarize_requests(samples, 2)
    assert {key: summary[key] for key in ("offered", "completed", "errors", "timeouts")} == {
        "offered": 3,
        "completed": 1,
        "errors": 1,
        "timeouts": 1,
    }
    assert summary["throughput_per_second"] == 0.5
    assert cast(dict[str, object], summary["latency_ms"])["n"] == 1


@pytest.mark.parametrize("clients,duration", [(2, 1), (4, 0), (4, -1), (4, float("nan")), (4, float("inf"))])
def test_invalid_configuration_is_rejected(clients: int, duration: float) -> None:
    with pytest.raises(ValueError):
        streams.AnalyticalConfig(clients, duration)


@pytest.mark.parametrize(
    "setting", ["benchmark_threads", "benchmark_memory_gb", "operation_timeout_seconds", "client_memory_limit_gb"]
)
def test_explicit_budgets_are_required_before_engine_start(
    database: ProfileDatabase,
    settings: Settings,
    setting: str,
) -> None:
    setattr(settings, setting, None)
    with pytest.raises(ValueError, match="require"):
        streams.validate_analytical_config(database, streams.AnalyticalConfig())


@pytest.mark.parametrize(
    "setting,value",
    [("benchmark_threads", 40), ("benchmark_memory_gb", 96), ("client_memory_limit_gb", 112)],
)
@pytest.mark.parametrize("database_name", ["duckdb", "polars", "postgres", "clickhouse"])
def test_resource_budgets_cannot_grow_with_client_count(
    database: ProfileDatabase, settings: Settings, setting: str, value: int, database_name: str
) -> None:
    database.name = cast(DatabaseName, database_name)
    setattr(settings, setting, value)
    with pytest.raises(ValueError, match="require"):
        streams.validate_analytical_config(database, streams.AnalyticalConfig(4))


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0
        self.lock = Lock()

    def __call__(self) -> float:
        with self.lock:
            return self.now

    def advance(self, seconds: float) -> None:
        with self.lock:
            self.now += seconds


@pytest.mark.parametrize("clients", [1, 4, 16])
@pytest.mark.parametrize("cancel_during_fetch", [False, True])
def test_synchronized_duration_stops_issuing_and_drains_inflight(
    database: ProfileDatabase,
    monkeypatch: pytest.MonkeyPatch,
    clients: int,
    cancel_during_fetch: bool,
) -> None:
    clock = _Clock()
    cancel = Event()
    warmups: list[tuple[int, str]] = []
    offered: list[tuple[int, str]] = []
    lock = Lock()
    config = streams.AnalyticalConfig(clients, 5, 8)
    queries = (streams.AnalyticalQuery("a", "select 'a'", {}), streams.AnalyticalQuery("b", "select 'b'", {}))
    metadata = streams.analytical_metadata(config, queries, {})

    def all_inflight() -> None:
        if cancel_during_fetch:
            cancel.set()
        clock.advance(8)

    inflight = Barrier(clients, action=all_inflight)

    def request(
        worker: Database,
        query: streams.AnalyticalQuery,
        client_id: int,
        sequence: int,
        phase: streams.RequestPhase,
        watchdog: Path | None,
        **_kwargs: object,
    ) -> streams.RequestSample:
        _ = worker, sequence, watchdog
        with lock:
            if phase == "warmup":
                warmups.append((client_id, query.name))
            else:
                assert len(warmups) == clients * len(queries)
                offered.append((client_id, query.name))
        if phase == "measured":
            inflight.wait(timeout=5)
        return streams.RequestSample(query.name, phase, "completed", 8_000 if phase == "measured" else 1)

    monkeypatch.setattr(streams, "read_effective_budget", _unit_budget)
    monkeypatch.setattr(streams, "read_worker_runtime", _unit_runtime)
    monkeypatch.setattr(streams, "perf_counter", clock)
    monkeypatch.setattr(streams, "execute_analytical_request", request)
    if cancel_during_fetch:
        with pytest.raises(streams.AnalyticalWorkloadError):
            streams.run_analytical_streams(database, config, queries, metadata, cancel=cancel)
    else:
        streams.run_analytical_streams(database, config, queries, metadata, cancel=cancel)
    assert len(offered) == clients
    assert metadata["actual_elapsed_seconds"] == 8
    assert metadata["drain_seconds"] == 3
    assert metadata["cancelled"] is cancel_during_fetch
    assert metadata["overall"]["completed"] == clients
    assert metadata["overall"]["throughput_per_second"] == clients / 8
    assert metadata["warmup_summary"]["completed"] == clients * 2
    assert metadata["measurement_started_at"] is not None
    assert metadata["measurement_finished_at"] is not None


def test_worker_initialization_failure_aborts_warmup_without_deadlock(
    database: ProfileDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @contextmanager
    def broken_worker(_database: Database) -> Generator[Database]:
        raise RuntimeError("cannot connect")
        yield _database

    queries = (streams.AnalyticalQuery("a", "select 'a'", {}),)
    metadata = streams.analytical_metadata(streams.AnalyticalConfig(4), queries, {})
    monkeypatch.setattr(streams, "read_effective_budget", _unit_budget)
    monkeypatch.setattr(streams, "analytical_worker", broken_worker)
    with pytest.raises(streams.AnalyticalWorkloadError, match="warmup"):
        streams.run_analytical_streams(database, streams.AnalyticalConfig(4), queries, metadata)
    assert metadata["measurement_started_at"] is None
    assert metadata["overall"]["offered"] == 0
    assert len(metadata["worker_failures"]) == 4
    assert all("cannot connect" in message for message in metadata["worker_failures"])


def test_pre_cancelled_workload_never_executes_queries(
    database: ProfileDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancel = Event()
    cancel.set()
    queries = (streams.AnalyticalQuery("a", "select 'a'", {}),)
    metadata = streams.analytical_metadata(streams.AnalyticalConfig(4), queries, {})
    request = Mock(side_effect=AssertionError("must not fetch"))
    monkeypatch.setattr(streams, "read_effective_budget", _unit_budget)
    monkeypatch.setattr(streams, "execute_analytical_request", request)
    with pytest.raises(streams.AnalyticalWorkloadError):
        streams.run_analytical_streams(database, streams.AnalyticalConfig(4), queries, metadata, cancel=cancel)
    request.assert_not_called()
    assert metadata["warmup_summary"]["offered"] == 0


def test_warmup_request_failure_closes_every_worker_and_never_starts_measurement(
    database: ProfileDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    warmed = Barrier(4)
    closed: list[Database] = []
    lock = Lock()

    @contextmanager
    def worker_context(db: Database) -> Generator[Database]:
        with real_worker(db) as worker:
            try:
                yield worker
            finally:
                with lock:
                    closed.append(worker)

    def request(
        _worker: Database,
        query: streams.AnalyticalQuery,
        client_id: int,
        _sequence: int,
        phase: streams.RequestPhase,
        _watchdog: Path | None,
        **_kwargs: object,
    ) -> streams.RequestSample:
        assert phase == "warmup"
        warmed.wait(timeout=5)
        return streams.RequestSample(query.name, phase, "error" if client_id == 0 else "completed", 1)

    real_worker = streams.analytical_worker
    config = streams.AnalyticalConfig(4)
    queries = (streams.AnalyticalQuery("a", "select 'a'", {}),)
    metadata = streams.analytical_metadata(config, queries, {})
    monkeypatch.setattr(streams, "analytical_worker", worker_context)
    monkeypatch.setattr(streams, "read_effective_budget", _unit_budget)
    monkeypatch.setattr(streams, "read_worker_runtime", _unit_runtime)
    monkeypatch.setattr(streams, "execute_analytical_request", request)
    with pytest.raises(streams.AnalyticalWorkloadError, match="warmup"):
        streams.run_analytical_streams(database, config, queries, metadata)
    assert len(closed) == 4
    assert metadata["measurement_started_at"] is None
    assert metadata["warmup_summary"]["errors"] == 1
    assert metadata["warmup_summary"]["completed"] == 3
    assert metadata["overall"]["offered"] == 0
    assert metadata["worker_failures"] == []
    assert metadata["worker_runtime"] == {str(i): {"test_runtime": True} for i in range(4)}


def test_warmup_cancellation_releases_barrier_waiters_before_draining(
    database: ProfileDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    cancel = Event()
    aborted = Event()
    others_warmed = Barrier(4)

    class WatchedBarrier(Barrier):
        def abort(self) -> None:
            super().abort()
            aborted.set()

    def request(
        _worker: Database,
        query: streams.AnalyticalQuery,
        client_id: int,
        _sequence: int,
        phase: streams.RequestPhase,
        _watchdog: Path | None,
        **_kwargs: object,
    ) -> streams.RequestSample:
        assert phase == "warmup"
        others_warmed.wait(timeout=5)
        if client_id == 0:
            cancel.set()
            assert aborted.wait(timeout=5), "main thread did not release warmup waiters on cancellation"
        return streams.RequestSample(query.name, phase, "completed", 1)

    config = streams.AnalyticalConfig(4)
    queries = (streams.AnalyticalQuery("a", "select 'a'", {}),)
    metadata = streams.analytical_metadata(config, queries, {})
    monkeypatch.setattr(streams, "Barrier", WatchedBarrier)
    monkeypatch.setattr(streams, "read_effective_budget", _unit_budget)
    monkeypatch.setattr(streams, "read_worker_runtime", _unit_runtime)
    monkeypatch.setattr(streams, "execute_analytical_request", request)
    with pytest.raises(streams.AnalyticalWorkloadError, match="warmup"):
        streams.run_analytical_streams(database, config, queries, metadata, cancel=cancel)
    assert metadata["warmup_summary"]["completed"] == 4
    assert metadata["worker_failures"] == []
    assert metadata["measurement_started_at"] is None


def test_measured_throughput_excludes_session_cleanup_time(
    database: ProfileDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _Clock()

    @contextmanager
    def worker_context(db: Database) -> Generator[Database]:
        with real_worker(db) as worker:
            try:
                yield worker
            finally:
                clock.advance(100)

    def request(
        _worker: Database,
        query: streams.AnalyticalQuery,
        _client_id: int,
        _sequence: int,
        phase: streams.RequestPhase,
        _watchdog: Path | None,
        **_kwargs: object,
    ) -> streams.RequestSample:
        if phase == "measured":
            clock.advance(8)
        return streams.RequestSample(query.name, phase, "completed", 1)

    real_worker = streams.analytical_worker
    config = streams.AnalyticalConfig(duration_seconds=5)
    queries = (streams.AnalyticalQuery("a", "select 'a'", {}),)
    metadata = streams.analytical_metadata(config, queries, {})
    monkeypatch.setattr(streams, "analytical_worker", worker_context)
    monkeypatch.setattr(streams, "read_effective_budget", _unit_budget)
    monkeypatch.setattr(streams, "read_worker_runtime", _unit_runtime)
    monkeypatch.setattr(streams, "execute_analytical_request", request)
    monkeypatch.setattr(streams, "perf_counter", clock)
    streams.run_analytical_streams(database, config, queries, metadata)
    assert clock.now == 208
    assert metadata["actual_elapsed_seconds"] == 8
    assert metadata["overall"]["throughput_per_second"] == 1 / 8


def test_worker_start_failure_releases_already_started_threads(
    database: ProfileDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    started: list[Thread] = []

    class StartFailure(Thread):
        def start(self) -> None:
            if self.name == "analytical-1":
                raise RuntimeError("thread start failed")
            super().start()
            started.append(self)

    config = streams.AnalyticalConfig(4)
    queries = (streams.AnalyticalQuery("a", "select 'a'", {}),)
    metadata = streams.analytical_metadata(config, queries, {})
    monkeypatch.setattr(streams, "Thread", StartFailure)
    monkeypatch.setattr(streams, "read_effective_budget", _unit_budget)
    monkeypatch.setattr(streams, "read_worker_runtime", _unit_runtime)
    with pytest.raises(RuntimeError, match="thread start failed"):
        streams.run_analytical_streams(database, config, queries, metadata)
    assert len(started) == 1
    assert not started[0].is_alive()
    assert metadata["cancelled"] is True


def test_request_fetch_boundary_hashing_watchdog_and_separate_roles(
    database: ProfileDatabase,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    clock = _Clock()
    watchdog = tmp_path / "client-3.json"
    kwargs: dict[str, Any] = {"schema": {"value": pl.String}}
    query = streams.AnalyticalQuery("a", "select 'a'", kwargs)
    frame = pl.DataFrame({"value": ["a"]})

    def fetch(worker: ProfileDatabase, sql: str, schema: Mapping[str, pl.DataType]) -> pl.DataFrame:
        assert sql == query.query
        assert worker.current_query_name == "a"
        assert worker.active_step_id is not None
        assert watchdog.is_file()
        cast(dict[str, object], schema)["mutated"] = True
        clock.advance(0.25)
        return frame

    real_answer_metadata = streams.build_answer_metadata

    def answer_metadata(result: pl.DataFrame) -> dict[str, Any]:
        assert result is frame
        assert not watchdog.exists()
        clock.advance(9)
        return real_answer_metadata(result)

    monkeypatch.setattr(streams, "perf_counter", clock)
    monkeypatch.setattr(ProfileDatabase, "fetch", fetch)
    monkeypatch.setattr(streams, "build_answer_metadata", answer_metadata)
    sample = streams.execute_analytical_request(database, query, 3, 11, "measured", watchdog)
    assert sample is not None
    assert sample.duration_ms == 250
    assert sample.status == "completed"
    assert kwargs == {"schema": {"value": pl.String}}
    assert database.current_query_name is None
    assert database.active_step_id is None
    step = cast(StreamStorage, database.result_storage).steps[1]
    assert step["iteration_role"] is None
    assert step["iteration"] is None
    assert step["row_count"] == 1
    evidence = cast(dict[str, Any], step["metadata"])
    assert evidence["duration_ms"] == 250
    assert evidence["request_id"] == "client-3:measured:11"
    assert evidence["query_sha256"] == sha256_text(query.query)
    assert evidence["answer_hash"]


@pytest.mark.parametrize("cancelled", [False, True])
def test_request_rechecks_issuance_guard_after_writer_delay(
    database: ProfileDatabase, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, cancelled: bool
) -> None:
    clock = _Clock()
    cancel = Event()
    storage = cast(StreamStorage, database.result_storage)
    start_step = storage.start_step

    def delayed_start(**kwargs: object) -> int:
        step = start_step(**kwargs)
        if cancelled:
            cancel.set()
        else:
            clock.advance(6)
        return step

    fetch = Mock(side_effect=AssertionError("must stop before issuing fetch"))
    watchdog = tmp_path / "client-0.json"
    monkeypatch.setattr(storage, "start_step", delayed_start)
    monkeypatch.setattr(streams, "perf_counter", clock)
    monkeypatch.setattr(ProfileDatabase, "fetch", fetch)
    result = streams.execute_analytical_request(
        database,
        streams.AnalyticalQuery("a", "select 'a'", {}),
        0,
        0,
        "measured",
        watchdog,
        deadline=105,
        cancel=cancel,
    )
    assert result is None
    fetch.assert_not_called()
    assert not watchdog.exists()
    assert database.active_step_id is None
    assert database.current_query_name is None
    assert storage.steps[1]["result_status"] == "skipped"
    assert cast(dict[str, object], storage.steps[1]["metadata"])["request_status"] == "not_issued"


class _PostgresTimeout(Exception):
    orig: SimpleNamespace = SimpleNamespace(pgcode="57014")


@pytest.mark.parametrize("failure", [ValueError("broken query"), TimeoutError("native deadline"), _PostgresTimeout()])
def test_request_errors_preserve_evidence_and_clear_worker_state(
    database: ProfileDatabase,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: Exception,
) -> None:
    def fetch(_self: ProfileDatabase, _query: str) -> pl.DataFrame:
        raise failure

    monkeypatch.setattr(ProfileDatabase, "fetch", fetch)
    watchdog = tmp_path / "client-0.json"
    sample = streams.execute_analytical_request(
        database, streams.AnalyticalQuery("a", "select 'a'", {}), 0, 0, "measured", watchdog
    )
    assert sample is not None
    assert sample.status == ("timeout" if streams._is_timeout(failure) else "error")
    assert not watchdog.exists()
    assert database.current_query_name is None
    assert database.active_step_id is None
    step = cast(StreamStorage, database.result_storage).steps[1]
    assert step["status"] == "failed"
    assert step["error_type"] == type(failure).__name__
    assert step["result_status"] == sample.status
    assert cast(dict[str, object], step["metadata"])["request_status"] == sample.status


def test_interrupted_request_finishes_its_step_and_reraises(
    database: ProfileDatabase, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(ProfileDatabase, "fetch", Mock(side_effect=KeyboardInterrupt))
    watchdog = tmp_path / "client-0.json"
    with pytest.raises(KeyboardInterrupt):
        streams.execute_analytical_request(
            database, streams.AnalyticalQuery("a", "select 'a'", {}), 0, 0, "warmup", watchdog
        )
    assert not watchdog.exists()
    assert database.current_query_name is None
    assert database.active_step_id is None
    step = cast(StreamStorage, database.result_storage).steps[1]
    assert step["status"] == "failed"
    assert step["error_type"] == "KeyboardInterrupt"


def test_workers_get_independent_database_and_query_state_with_one_shared_storage(database: ProfileDatabase) -> None:
    with streams.analytical_worker(database) as first, streams.analytical_worker(database) as second:
        assert first is not second and first is not database
        assert first.result_storage is second.result_storage is database.result_storage
        assert first._active_step_ids_by_thread is not second._active_step_ids_by_thread
        assert first._query_hashes is not second._query_hashes
        with first.query_context("a"), second.query_context("b"):
            assert first.current_query_name == "a"
            assert second.current_query_name == "b"
            assert database.current_query_name is None


def test_clickhouse_worker_does_not_share_parent_native_clients(database: ProfileDatabase) -> None:
    parent = Clickhouse()
    parent._current_suite = "clickbench"
    parent._current_suite_scale_factor = 1
    parent._result_storage = database.result_storage
    parent._run_id = 1
    connection = Mock()
    native_client = Mock()
    parent._connection = connection
    parent._clickhouse_client = native_client
    with streams.analytical_worker(parent) as worker:
        assert isinstance(worker, Clickhouse)
        assert worker._connection is None
        assert worker._clickhouse_client is None
    connection.close.assert_not_called()
    native_client.close.assert_not_called()


def test_concurrent_worker_step_logging_preserves_unique_ids_and_context(database: ProfileDatabase) -> None:
    def client(client_id: int) -> None:
        with streams.analytical_worker(database) as worker:
            for sequence in range(20):
                name = "a" if sequence % 2 else "b"
                result = streams.execute_analytical_request(
                    worker,
                    streams.AnalyticalQuery(name, f"select '{name}'", {}),
                    client_id,
                    sequence,
                    "measured",
                    None,
                )
                assert result is not None
                assert result.status == "completed"

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(client, range(4)))
    steps = cast(StreamStorage, database.result_storage).steps
    assert len(steps) == 80
    assert len({cast(dict[str, Any], step["metadata"])["request_id"] for step in steps.values()}) == 80
    assert all(step["status"] == "completed" and step["iteration_role"] is None for step in steps.values())
    assert database.current_query_name is None


def test_postgres_worker_uses_one_readonly_session_with_native_deadline(
    database: ProfileDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database.name = "postgres"
    connection = Mock()
    connect = Mock(return_value=connection)
    monkeypatch.setattr(ProfileDatabase, "connect", connect)
    with streams.analytical_worker(database):
        statements = [str(call.args[0]) for call in connection.execute.call_args_list]
        assert statements == ["SET default_transaction_read_only = on", "SET statement_timeout = 30000"]
        connect.assert_called_once()
        connection.commit.assert_called_once()


@pytest.mark.parametrize("name", ["postgres", "clickhouse"])
def test_worker_cleanup_runs_after_setup_or_body_failure(
    database: ProfileDatabase, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    database.name = cast(DatabaseName, name)
    close = Mock()
    reset = Mock()
    connect = Mock(side_effect=RuntimeError("setup failed"))
    monkeypatch.setattr(ProfileDatabase, "connect", connect)
    monkeypatch.setattr(ProfileDatabase, "close_connection", close)
    monkeypatch.setattr(ProfileDatabase, "reset_session", reset)
    with pytest.raises(RuntimeError, match="setup failed|body failed"), streams.analytical_worker(database):
        raise RuntimeError("body failed")
    close.assert_called_once()
    assert reset.call_count == (1 if name == "clickhouse" else 0)


@pytest.mark.parametrize("timeout_seconds", [None, 30])
@pytest.mark.parametrize(
    "wrong_setting", [None, "default_transaction_read_only", "transaction_read_only", "statement_timeout"]
)
def test_postgres_runtime_is_read_back_and_validated_on_the_worker_session(
    database: ProfileDatabase,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    timeout_seconds: int | None,
    wrong_setting: str | None,
) -> None:
    database.name = "postgres"
    settings.query_timeout_seconds = timeout_seconds
    values = {
        "default_transaction_read_only": "on",
        "transaction_read_only": "on",
        "statement_timeout": str((timeout_seconds or 0) * 1_000),
    }
    if wrong_setting is not None:
        values[wrong_setting] = "1" if wrong_setting == "statement_timeout" else "off"
    rows = [{"name": name, "setting": value, "unit": None, "source": "test"} for name, value in values.items()]
    result = Mock()
    result.mappings.return_value = rows
    result.scalar_one.return_value = 123
    connection = Mock()
    connection.execute.return_value = result
    monkeypatch.setattr(ProfileDatabase, "connect", Mock(return_value=connection))
    if wrong_setting is not None:
        with pytest.raises(ValueError, match="read-only|statement_timeout"):
            streams.read_worker_runtime(database)
    else:
        runtime = streams.read_worker_runtime(database)
        assert runtime["backend_pid"] == 123
        assert cast(dict[str, Any], runtime["settings"])["statement_timeout"]["setting"] == values["statement_timeout"]
        assert "not instrumented" in cast(str, runtime["cache_attestation"])


@pytest.mark.parametrize("automatic_threads", [False, True])
@pytest.mark.parametrize("wrong_setting", [None, "use_query_cache", "readonly", "max_threads", "max_memory_usage"])
def test_clickhouse_runtime_attests_effective_request_settings(
    monkeypatch: pytest.MonkeyPatch, wrong_setting: str | None, automatic_threads: bool
) -> None:
    actual = {name: str(value) for name, value in streams.CLICKHOUSE_REQUEST_SETTINGS.items()}
    if wrong_setting is not None:
        actual[wrong_setting] = "-1"
    table_values = {**actual, "max_threads": "auto(10)"} if automatic_threads else actual
    frame = pl.DataFrame(
        {"name": list(actual), "value": list(table_values.values()), "effective_value": list(actual.values())}
    )
    fetch = Mock(return_value=frame)
    monkeypatch.setattr(Clickhouse, "fetch", fetch)
    if wrong_setting is not None:
        with pytest.raises(ValueError, match=f"setting {wrong_setting}"):
            streams.read_worker_runtime(Clickhouse())
    else:
        runtime = streams.read_worker_runtime(Clickhouse())
        assert runtime["settings"] == actual
        assert runtime["settings_table"] == table_values
        assert "query-log cache hits are not instrumented" in cast(str, runtime["cache_attestation"])
    assert fetch.call_args.kwargs["settings"] == streams.CLICKHOUSE_REQUEST_SETTINGS
    assert "getSetting('max_threads')" in fetch.call_args.args[0]


def test_query_manifest_preserves_filters_scale_and_driver_selection(
    database: ProfileDatabase,
    settings: Settings,
) -> None:
    settings.query_include = ("a", "b")
    settings.query_exclude = ("b",)
    database.name = "postgres"
    benchmark = ProfileSuite(db=database, name="clickbench", scale_factor=1)
    queries, excluded = streams.prepare_analytical_queries(benchmark)
    assert [query.name for query in queries] == ["a"]
    assert queries[0].fetch_kwargs == {"method": "python"}
    assert set(excluded) == {"b", "c"}
    metadata = streams.analytical_metadata(streams.AnalyticalConfig(16, 7, 19), queries, excluded)
    assert metadata["query_manifest"]["files"] == {"a": sha256_text("select 'a'")}
    assert metadata["clients"] == 16
    assert metadata["workload_seed"] == 19
    assert metadata["result_cache"]["engine_attestation"].startswith("not inferred")


def test_clickhouse_disables_answer_cache_without_changing_predicate_caches(database: ProfileDatabase) -> None:
    database.name = "clickhouse"
    queries, _ = streams.prepare_analytical_queries(ProfileSuite(db=database, name="clickbench"))
    assert all(query.fetch_kwargs["settings"] == streams.CLICKHOUSE_REQUEST_SETTINGS for query in queries)


def test_container_budget_is_read_back_once_for_all_clients(
    database: ProfileDatabase,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database.name = "postgres"
    monkeypatch.setattr(ProfileDatabase, "metric_container_names", property(lambda _db: ("unit-server",)))
    host_config = {"NanoCpus": 10_000_000_000, "Memory": 24 * 1024**3, "MemorySwap": 24 * 1024**3}
    docker = Mock()
    docker.containers.get.return_value = SimpleNamespace(attrs={"HostConfig": host_config})
    monkeypatch.setattr(measure, "get_docker_client", lambda: docker)
    budget = streams.read_effective_budget(database)
    assert budget["multiplied_by_clients"] is False
    assert budget["configured_engine_memory_bytes"] == 24 * 1024**3
    assert budget["client_rss_guard_bytes"] == 28 * 1024**3
    assert "cgroup charge including page cache" in cast(str, budget["scope"])
    assert cast(dict[str, Any], budget["effective"])["unit-server"]["NanoCpus"] == 10_000_000_000
    host_config["NanoCpus"] = 16_000_000_000
    with pytest.raises(ValueError, match="do not match"):
        streams.read_effective_budget(database)
    settings.benchmark_threads = 16
    host_config["Memory"] = 0
    with pytest.raises(ValueError, match="finite"):
        streams.read_effective_budget(database)


def test_polars_budget_exposes_rss_guard_without_claiming_an_engine_memory_cap(
    database: ProfileDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database.name = "polars"
    monkeypatch.setattr(pl, "thread_pool_size", lambda: 10)
    budget = streams.read_effective_budget(database)
    assert cast(dict[str, object], budget["effective"]) == {"thread_pool_size": 10, "engine_memory_limit_bytes": None}
    monkeypatch.setattr(pl, "thread_pool_size", lambda: 16)
    with pytest.raises(ValueError, match="shared pool"):
        streams.read_effective_budget(database)


@pytest.mark.parametrize("incomplete", [False, True])
def test_benchmark_hook_preserves_provenance_sampler_and_distinct_track(
    database: ProfileDatabase,
    monkeypatch: pytest.MonkeyPatch,
    incomplete: bool,
) -> None:
    storage = cast(StreamStorage, database.result_storage)
    finished: list[datetime] = []
    captured: dict[str, Any] = {}
    checked: list[bool] = []

    def verify_store(_suite: ProfileSuite) -> None:
        if incomplete:
            raise RuntimeError("partially populated store")
        checked.append(True)

    def metadata(**kwargs: object) -> dict[str, Any]:
        captured.update(kwargs)
        return {"repository": {"commit": "unit", "dirty": False}, "options": {"unit": "preserved"}}

    def run(
        _db: Database,
        _config: streams.AnalyticalConfig,
        _queries: Sequence[streams.AnalyticalQuery],
        evidence: dict[str, Any],
    ) -> None:
        assert checked == [True]
        evidence["overall"] = {"offered": 10, "completed": 10}

    monkeypatch.setattr(ProfileDatabase, "create_result_storage", Mock(return_value=storage))
    monkeypatch.setattr(dbs, "build_system_metadata", lambda: {"unit": True})
    monkeypatch.setattr(dbs, "build_run_metadata", metadata)
    monkeypatch.setattr(dbs, "start_metric_sampler", Mock(return_value=SimpleNamespace(finish=finished.append)))
    monkeypatch.setattr(streams, "run_analytical_streams", run)
    monkeypatch.setattr(ProfileSuite, "verify_populated_data", verify_store)
    database._analytical_config = streams.AnalyticalConfig(4, 5, 8)
    if incomplete:
        with pytest.raises(RuntimeError, match="partially populated store"):
            database.benchmark("clickbench", "analytical", 1)
        assert storage.run["status"] == "failed"
        assert "overall" not in cast(dict[str, Any], storage.run["metadata_update"])["analytical"]
        assert len(finished) == 1
        return
    database.benchmark("clickbench", "analytical", 1)
    assert database.restarts == 0
    assert storage.run["operation"] == "analytical"
    assert storage.run["benchmark_profile"] == "analytical-streams-v1"
    assert storage.run["status"] == "completed"
    assert captured["plan"] is None
    assert captured["profile"].metadata()["measured_iterations"] is None
    assert cast(dict[str, Any], storage.run["metadata"])["repository"]["commit"] == "unit"
    assert cast(dict[str, Any], storage.run["metadata_update"])["analytical"]["overall"]["completed"] == 10
    assert len(finished) == 1


def test_failed_streams_preserve_summary_and_finish_sampler_and_run(
    database: ProfileDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = cast(StreamStorage, database.result_storage)
    finished: list[datetime] = []

    def run(
        _db: Database,
        _config: streams.AnalyticalConfig,
        _queries: Sequence[streams.AnalyticalQuery],
        evidence: dict[str, Any],
    ) -> None:
        evidence["warmup_summary"] = {"errors": 1}
        raise streams.AnalyticalWorkloadError("warmup failed")

    monkeypatch.setattr(ProfileDatabase, "create_result_storage", Mock(return_value=storage))
    monkeypatch.setattr(dbs, "build_system_metadata", Mock(return_value={}))
    monkeypatch.setattr(dbs, "build_run_metadata", Mock(return_value={}))
    monkeypatch.setattr(dbs, "start_metric_sampler", Mock(return_value=SimpleNamespace(finish=finished.append)))
    monkeypatch.setattr(streams, "run_analytical_streams", run)
    database._analytical_config = streams.AnalyticalConfig(4, 5)
    with pytest.raises(streams.AnalyticalWorkloadError, match="warmup failed"):
        database.benchmark("clickbench", "analytical", 1)
    assert storage.run["status"] == "failed"
    assert storage.run["error_type"] == "AnalyticalWorkloadError"
    assert cast(dict[str, Any], storage.run["metadata_update"])["analytical"]["warmup_summary"]["errors"] == 1
    assert len(finished) == 1


def test_supported_scope_uses_only_existing_canonical_registrations(settings: Settings) -> None:
    _ = settings
    supported: set[tuple[str, str]] = set()
    for name in streams.ANALYTICAL_DATABASES:
        database = get_databases()[name]
        for suite in ("tpc_h", "tpc_ds", "clickbench"):
            if suite not in database.suite_registry():
                continue
            queries, _ = streams.prepare_analytical_queries(database.benchmarks[suite])
            assert queries
            supported.add((name, suite))
    assert supported == {
        (name, suite)
        for name in ("duckdb", "polars", "postgres", "clickhouse")
        for suite in ("tpc_h", "tpc_ds", "clickbench")
        if (name, suite) != ("polars", "tpc_ds")
    }


def test_analytical_stays_out_of_legacy_suite_operations_and_matrix() -> None:
    from ..matrix import expand_matrix

    for database in get_databases().values():
        for benchmark in database.suite_registry().values():
            assert "analytical" not in benchmark.supported_operations
    assert all("analytical" not in spec.operations for spec in SUITE_SPECS.values())
    assert all(cell.operation != "analytical" for cell in expand_matrix())
    assert SUITE_SPECS["time_series"].operations == ("populate", "select", "mutate", "concurrent")
    assert SUITE_SPECS["chat_threads"].operations == ("populate", "select", "mutate", "concurrent")


def test_cli_setup_is_serial_and_stops_before_cleanup(
    database: ProfileDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    writer = cast(
        WriterProcessHandle,
        SimpleNamespace(queue=object(), result_queue=object(), close=lambda: events.append("writer-close")),
    )
    monkeypatch.setattr(cli, "get_databases", lambda: {"duckdb": database})
    monkeypatch.setattr(cli, "_check_and_hash_input_data", _record(events, "inputs"))
    monkeypatch.setattr(cli, "build_repository_provenance", lambda: events.append("provenance"))
    monkeypatch.setattr(cli, "start_writer_process", Mock(return_value=writer))
    monkeypatch.setattr(cli, "_start_db", _record(events, "start"))
    stop = Mock(side_effect=_record(events, "stop"))
    monkeypatch.setattr(cli, "_stop_db", stop)
    monkeypatch.setattr(cli, "_cleanup_db_files", _record(events, "cleanup"))

    def run_operation(_db: Database, _suite: str, operation: str, *_args: object, **_kwargs: object) -> None:
        events.append(operation)

    monkeypatch.setattr(cli, "run_operation", run_operation)
    cli.analytical("duckdb", "clickbench", clients=4, duration_seconds=5, setup=True, cleanup=True)
    assert events == ["inputs", "provenance", "start", "populate", "analytical", "stop", "cleanup", "writer-close"]
    assert stop.call_args.kwargs["check"] is True


def test_cli_failure_stops_engine_and_marks_interrupted_run(
    database: ProfileDatabase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    writer = cast(
        WriterProcessHandle,
        SimpleNamespace(queue=object(), result_queue=object(), close=lambda: events.append("writer-close")),
    )

    def fail(*_args: object, **_kwargs: object) -> None:
        events.append("operation")
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "get_databases", lambda: {"duckdb": database})
    monkeypatch.setattr(cli, "_check_and_hash_input_data", Mock())
    monkeypatch.setattr(cli, "build_repository_provenance", lambda: None)
    monkeypatch.setattr(cli, "start_writer_process", Mock(return_value=writer))
    monkeypatch.setattr(cli, "_start_db", _record(events, "start"))
    monkeypatch.setattr(cli, "_stop_db", _record(events, "stop"))
    monkeypatch.setattr(cli, "run_operation", fail)
    monkeypatch.setattr(cli, "mark_running_runs_failed", _record(events, "mark-failed"))
    with pytest.raises(KeyboardInterrupt):
        cli.analytical("duckdb", "clickbench", duration_seconds=5)
    assert events == ["start", "operation", "stop", "writer-close", "mark-failed"]


def test_cli_preserves_operation_error_when_shutdown_and_writer_cleanup_fail(
    database: ProfileDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = KeyboardInterrupt("cancelled")
    writer = cast(
        WriterProcessHandle,
        SimpleNamespace(queue=object(), result_queue=object(), close=Mock(side_effect=RuntimeError("writer failed"))),
    )
    mark_failed = Mock()
    cleanup = Mock()
    monkeypatch.setattr(cli, "get_databases", lambda: {"duckdb": database})
    monkeypatch.setattr(cli, "_check_and_hash_input_data", Mock())
    monkeypatch.setattr(cli, "build_repository_provenance", lambda: None)
    monkeypatch.setattr(cli, "start_writer_process", Mock(return_value=writer))
    monkeypatch.setattr(cli, "_start_db", Mock())
    monkeypatch.setattr(cli, "_stop_db", Mock(side_effect=RuntimeError("shutdown unconfirmed")))
    monkeypatch.setattr(cli, "_cleanup_db_files", cleanup)
    monkeypatch.setattr(cli, "run_operation", Mock(side_effect=original))
    monkeypatch.setattr(cli, "mark_running_runs_failed", mark_failed)
    with pytest.raises(KeyboardInterrupt) as exc:
        cli.analytical("duckdb", "clickbench", duration_seconds=5, cleanup=True)
    assert exc.value is original
    assert any("shutdown unconfirmed" in note for note in original.__notes__)
    assert any("writer failed" in note for note in original.__notes__)
    cleanup.assert_not_called()
    mark_failed.assert_called_once()


def test_operation_parent_checks_independent_worker_watchdogs_and_removes_them(
    database: ProfileDatabase, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    from .test_benchmark_limits import _StuckProcess

    process = _StuckProcess()
    watchdog_paths: list[Path] = []
    watched: list[str] = []
    kwargs_seen: dict[str, Any] = {}

    def stop_process(_process: _StuckProcess) -> None:
        process.kill()

    stop = Mock(side_effect=stop_process)

    def new_process(**kwargs: object) -> _StuckProcess:
        kwargs_seen.update(kwargs)
        directory = cast(Path, cast(tuple[object, ...], kwargs["args"])[8])
        watchdog_paths.append(directory)
        (directory / "client-0.json").write_text("{}")
        (directory / "client-3.json").write_text("{}")
        (directory / "client-1.part").write_text("incomplete")
        return process

    def expired(path: Path, timeout: int | None) -> str | None:
        assert timeout == 30
        watched.append(path.name)
        return "client-3:warmup:0" if path.name == "client-3.json" else None

    settings.temporary_directory.mkdir()
    monkeypatch.setattr(operation_runner, "get_context", Mock(return_value=SimpleNamespace(Process=new_process)))
    monkeypatch.setattr(operation_runner, "check_disk_space", Mock())
    monkeypatch.setattr(operation_runner, "configure_polars_spill", Mock())
    monkeypatch.setattr(operation_runner, "expired_query", expired)
    monkeypatch.setattr(operation_runner, "_stop_operation", stop)
    with pytest.raises(TimeoutError, match="client-3:warmup:0"):
        operation_runner.run_operation(
            database,
            "clickbench",
            "analytical",
            1,
            cast("Queue[WriterMessage]", object()),
            cast("Queue[object]", object()),
            analytical=streams.AnalyticalConfig(4, 5),
        )
    assert "client-3.json" in watched
    assert all(name.endswith(".json") for name in watched)
    assert len(watchdog_paths) == 1
    assert not watchdog_paths[0].exists()
    assert kwargs_seen["args"][9] == streams.AnalyticalConfig(4, 5)
    stop.assert_called_once_with(process)


def test_operation_process_creation_failure_removes_watchdog_directory(
    database: ProfileDatabase, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings.temporary_directory.mkdir()
    monkeypatch.setattr(operation_runner, "check_disk_space", Mock())
    monkeypatch.setattr(operation_runner, "configure_polars_spill", Mock())
    monkeypatch.setattr(operation_runner, "get_context", Mock(side_effect=RuntimeError("spawn failed")))
    with pytest.raises(RuntimeError, match="spawn failed"):
        operation_runner.run_operation(
            database,
            "clickbench",
            "analytical",
            1,
            cast("Queue[WriterMessage]", object()),
            cast("Queue[object]", object()),
            analytical=streams.AnalyticalConfig(4, 5),
        )
    assert list(settings.temporary_directory.iterdir()) == []


def test_checked_stop_reports_unconfirmed_server_cleanup(
    database: ProfileDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ProfileDatabase, "stop_commands", property(lambda _db: ("unit-stop",)))
    monkeypatch.setattr(cli, "run_shell", Mock(return_value=1))
    with pytest.raises(RuntimeError, match="shutdown is unconfirmed"):
        cli._stop_db(database, check=True)


def test_cli_help_keeps_explicit_command_and_legacy_benchmark_options(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        cli.app(["analytical", "--help"])
    assert exc.value.code == 0
    help_text = capsys.readouterr().out
    assert "--clients" in help_text and "--duration-seconds" in help_text and "--setup" in help_text
    with pytest.raises(SystemExit) as exc:
        cli.app(["benchmark", "--help"])
    assert exc.value.code == 0
    assert "analytical" not in capsys.readouterr().out
