from __future__ import annotations

import logging
import multiprocessing
import os
from datetime import datetime
from multiprocessing import Queue
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..metrics.storage import Storage, WriterMessage, start_writer_process
from ..results import get_results_engine
from ..results.models import QueryExecution
from ..settings import setup_stdout_logging

_LOGGER = logging.getLogger(__name__)


def _subprocess_func(q: Queue[WriterMessage], rq: Queue[object]) -> None:
    setup_stdout_logging()
    id = Storage(q, rq).debug(f"in proc {os.getpid()}")
    _LOGGER.info(f"Inserted id {id} from subprocess {os.getpid()}")


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
