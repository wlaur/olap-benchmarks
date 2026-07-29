from __future__ import annotations

import logging
import multiprocessing
import os
import time
from datetime import datetime
from multiprocessing import Queue
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..metrics.storage import Storage, WriterMessage, WriterProcessHandle, start_writer_process
from ..results import get_results_engine
from ..results.models import QueryExecution, Run, RunStep, SystemSnapshot
from ..settings import setup_stdout_logging

_LOGGER = logging.getLogger(__name__)


def _subprocess_func(q: Queue[WriterMessage], rq: Queue[object]) -> None:
    setup_stdout_logging()
    id = Storage(q, rq).debug(f"in proc {os.getpid()}")
    _LOGGER.info(f"Inserted id {id} from subprocess {os.getpid()}")


def _exit_immediately() -> None:
    raise SystemExit(17)


def _exit_during_request(queue: Queue[WriterMessage]) -> None:
    queue.get(timeout=5)
    raise SystemExit(18)


def _block_during_request(queue: Queue[WriterMessage]) -> None:
    queue.get(timeout=5)
    time.sleep(60)


def test_storage_fails_if_writer_dies_before_first_request() -> None:
    queue: Queue[WriterMessage] = Queue()
    result_queue: Queue[object] = Queue()
    process = multiprocessing.Process(target=_exit_immediately)
    process.start()
    process.join(timeout=5)

    with pytest.raises(RuntimeError, match="code 17"):
        Storage(queue, result_queue, process=process).debug()

    queue.close()
    result_queue.close()


def test_storage_fails_if_writer_dies_during_request() -> None:
    queue: Queue[WriterMessage] = Queue()
    result_queue: Queue[object] = Queue()
    process = multiprocessing.Process(target=_exit_during_request, args=(queue,))
    process.start()

    with pytest.raises(RuntimeError, match="code 18"):
        Storage(queue, result_queue, process=process, response_timeout_seconds=5).debug()

    process.join(timeout=5)
    queue.close()
    result_queue.close()


def test_writer_close_terminates_a_blocked_database_request() -> None:
    queue: Queue[WriterMessage] = Queue()
    result_queue: Queue[object] = Queue()
    process = multiprocessing.Process(target=_block_during_request, args=(queue,))
    process.start()
    handle = WriterProcessHandle(process=process, queue=queue, result_queue=result_queue)
    queue.put({"type": "debug", "args": ["blocked"]})

    handle.close(timeout_seconds=0.1)

    assert not process.is_alive()


def test_result_concurrency(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, nproc: int = 10) -> None:
    setup_stdout_logging()

    monkeypatch.setenv("OLAP_BENCHMARKS_RESULTS_DIRECTORY", str(tmp_path))

    writer = start_writer_process()
    s = Storage(writer.queue, writer.result_queue)

    first = s.debug("first")
    _LOGGER.warning(f"Inserted first id {first}")
    assert first is not None

    processes: list[multiprocessing.Process] = []

    for idx in range(nproc):
        p = multiprocessing.Process(
            target=_subprocess_func,
            args=(writer.queue, writer.result_queue),
            daemon=True,
        )
        p.start()

        if idx % 10 == 0:
            mid = s.debug("mid")
            _LOGGER.warning(f"Inserted mid id {mid}")
            assert mid is not None

        processes.append(p)

    for p in processes:
        p.join()
        assert p.exitcode == 0

    last = s.debug("last")
    _LOGGER.warning(f"Inserted last id {last}")
    assert last is not None

    writer.close()


def test_writer_persists_query_execution_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OLAP_BENCHMARKS_RESULTS_DIRECTORY", str(tmp_path))

    writer = start_writer_process()
    storage = Storage(writer.queue, writer.result_queue)

    storage.insert_query_execution(
        run_id=123,
        run_step_id=456,
        query="select 1",
        start_time=datetime(2026, 1, 1, 12, 0, 0),
        end_time=datetime(2026, 1, 1, 12, 0, 1),
    )
    writer.close()

    engine = get_results_engine(read_only=False, db_path=tmp_path / "default.db")

    try:
        with Session(engine) as session:
            rows = session.scalars(select(QueryExecution)).all()
            assert len(rows) == 1
            assert rows[0].run_id == 123
            assert rows[0].run_step_id == 456
            assert rows[0].query == "select 1"
    finally:
        engine.dispose()


def test_writer_persists_system_snapshot_run_metadata_and_step_status_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OLAP_BENCHMARKS_RESULTS_DIRECTORY", str(tmp_path))

    writer = start_writer_process()
    storage = Storage(writer.queue, writer.result_queue)

    run_id = storage.insert_run(
        suite="time_series",
        suite_scale_factor=1,
        db="duckdb",
        db_version="test",
        db_driver=None,
        operation="select",
        system="test",
        started_at=datetime(2026, 1, 1, 12, 0, 0),
        system_metadata={
            "host": {
                "os": "TestOS",
                "os_release": "1",
                "machine": "test-machine",
                "processor": "test-cpu",
                "cpu_count_logical": 8,
                "memory_total_mb": 16_384,
            },
            "python": {"version": "3.14"},
        },
        metadata={"execution": {"mode": "in_process"}},
    )
    second_run_id = storage.insert_run(
        suite="time_series",
        suite_scale_factor=1,
        db="clickhouse",
        db_version="test",
        db_driver=None,
        operation="select",
        system="test",
        started_at=datetime(2026, 1, 1, 12, 0, 3),
        system_metadata={
            "python": {"version": "3.14"},
            "host": {
                "memory_total_mb": 16_384,
                "cpu_count_logical": 8,
                "processor": "test-cpu",
                "machine": "test-machine",
                "os_release": "1",
                "os": "TestOS",
            },
        },
        metadata={"execution": {"mode": "container"}},
    )
    step_id = storage.start_step(
        run_id=run_id,
        step_type="query",
        step_name="query",
        query_name="q1",
        iteration=1,
        started_at=datetime(2026, 1, 1, 12, 0, 1),
        result_status=None,
        iteration_role="first_run",
    )
    storage.finish_step(
        step_id=step_id,
        finished_at=datetime(2026, 1, 1, 12, 0, 2),
        status="completed",
        result_status="ok",
        row_count=1,
    )
    writer.close()

    engine = get_results_engine(read_only=False, db_path=tmp_path / "default.db")

    try:
        with Session(engine) as session:
            run = session.get_one(Run, run_id)
            second_run = session.get_one(Run, second_run_id)
            step = session.get_one(RunStep, step_id)
            assert run.system_snapshot_id is not None
            assert second_run.system_snapshot_id == run.system_snapshot_id
            snapshot = session.get_one(SystemSnapshot, run.system_snapshot_id)

            assert run.metadata_json == {"execution": {"mode": "in_process"}}
            assert snapshot.system == "test"
            assert snapshot.os == "TestOS"
            assert snapshot.machine == "test-machine"
            assert snapshot.cpu_count_logical == 8
            assert snapshot.memory_total_mb == 16_384
            assert snapshot.metadata_json == {
                "host": {
                    "cpu_count_logical": 8,
                    "machine": "test-machine",
                    "memory_total_mb": 16_384,
                    "os": "TestOS",
                    "os_release": "1",
                    "processor": "test-cpu",
                },
                "python": {"version": "3.14"},
            }
            assert step.result_status == "ok"
            assert step.iteration_role == "first_run"
            assert step.row_count == 1
    finally:
        engine.dispose()
