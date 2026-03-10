from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Process, Queue
from queue import Empty
from typing import Any, Literal, TypedDict, cast

from sqlalchemy import update
from sqlalchemy.orm import Session

from ..results import get_results_engine
from ..results_models import DebugEntry, Run, RunMetric, RunStep
from ..results_schema import ensure_results_schema
from ..settings import DatabaseName, Operation, SuiteName, setup_stdout_logging

_LOGGER = logging.getLogger(__name__)

RunStatus = Literal["running", "completed", "failed", "aborted"]
StepType = Literal["phase", "query"]

MessageType = Literal[
    "insert_run",
    "finish_run",
    "insert_metric",
    "start_step",
    "finish_step",
    "debug",
    "shutdown",
]


class WriterMessage(TypedDict):
    type: MessageType
    args: list[Any]


def writer_loop(queue: Queue[WriterMessage], result_queue: Queue[object], revision: str = "default") -> None:
    setup_stdout_logging()

    engine = get_results_engine(read_only=False, revision=revision)
    ensure_results_schema(engine)

    with Session(engine) as session:
        while True:
            try:
                msg = queue.get()
            except EOFError:
                engine.dispose()
                return

            match msg["type"]:
                case "debug":
                    row = DebugEntry(content=cast(str, msg["args"][0]))
                    session.add(row)
                    session.commit()
                    result_queue.put(row.id)

                case "insert_run":
                    row = Run(
                        suite=cast(str, msg["args"][0]),
                        db=cast(str, msg["args"][1]),
                        db_version=cast(str, msg["args"][2]),
                        operation=cast(str, msg["args"][3]),
                        system=cast(str, msg["args"][4]),
                        status=cast(str, msg["args"][5]),
                        started_at=cast(datetime, msg["args"][6]),
                    )
                    session.add(row)
                    session.commit()
                    result_queue.put(row.id)

                case "finish_run":
                    session.execute(
                        update(Run)
                        .where(Run.id == cast(int, msg["args"][4]))
                        .values(
                            finished_at=cast(datetime, msg["args"][0]),
                            status=cast(str, msg["args"][1]),
                            error_type=cast(str | None, msg["args"][2]),
                            error_message=cast(str | None, msg["args"][3]),
                        )
                    )
                    session.commit()

                case "insert_metric":
                    row = RunMetric(
                        run_id=cast(int, msg["args"][0]),
                        time=cast(datetime, msg["args"][1]),
                        cpu_percent=cast(float, msg["args"][2]),
                        mem_mb=cast(int, msg["args"][3]),
                        disk_mb=cast(int, msg["args"][4]),
                    )
                    session.add(row)
                    session.commit()

                case "start_step":
                    row = RunStep(
                        run_id=cast(int, msg["args"][0]),
                        step_type=cast(str, msg["args"][1]),
                        step_name=cast(str, msg["args"][2]),
                        query_name=cast(str | None, msg["args"][3]),
                        iteration=cast(int | None, msg["args"][4]),
                        table_name=cast(str | None, msg["args"][5]),
                        started_at=cast(datetime, msg["args"][6]),
                        status=cast(str, msg["args"][7]),
                        metadata_json=cast(dict[str, Any] | None, msg["args"][8]),
                    )
                    session.add(row)
                    session.commit()
                    result_queue.put(row.id)

                case "finish_step":
                    update_values: dict[str, Any] = {
                        "finished_at": cast(datetime, msg["args"][0]),
                        "status": cast(str, msg["args"][1]),
                        "row_count": cast(int | None, msg["args"][2]),
                        "error_type": cast(str | None, msg["args"][3]),
                        "error_message": cast(str | None, msg["args"][4]),
                    }

                    metadata_value = cast(dict[str, Any] | None, msg["args"][5])
                    if metadata_value is not None:
                        update_values["metadata_json"] = metadata_value

                    session.execute(
                        update(RunStep).where(RunStep.id == cast(int, msg["args"][6])).values(**update_values)
                    )
                    session.commit()

                case "shutdown":
                    session.commit()
                    result_queue.put("ok")
                    engine.dispose()
                    return

                case _:
                    raise ValueError(f"Unknown message type: {msg['type']}")


@dataclass
class WriterProcessHandle:
    process: Process
    queue: Queue[WriterMessage]
    result_queue: Queue[object]
    _closed: bool = False

    def close(self, timeout_seconds: float = 10.0) -> None:
        if self._closed:
            return

        try:
            Storage(self.queue, self.result_queue).shutdown(timeout_seconds=timeout_seconds)
        except Exception as exc:
            _LOGGER.warning(f"Writer shutdown handshake failed: {exc}")

        self.process.join(timeout=timeout_seconds)

        if self.process.is_alive():
            _LOGGER.warning("Writer process did not exit cleanly, terminating")
            self.process.terminate()
            self.process.join(timeout=timeout_seconds)

        self.queue.close()
        self.result_queue.close()

        self._closed = True


def start_writer_process(revision: str = "default") -> WriterProcessHandle:
    queue: Queue[WriterMessage] = Queue()
    result_queue: Queue[object] = Queue()

    writer_process = Process(target=writer_loop, args=(queue, result_queue, revision), daemon=False)
    writer_process.start()

    return WriterProcessHandle(process=writer_process, queue=queue, result_queue=result_queue)


class Storage:
    def __init__(self, queue: Queue[WriterMessage], result_queue: Queue[object]) -> None:
        self.queue = queue
        self.result_queue = result_queue

    def put(self, type: MessageType, args: list[Any]) -> None:
        self.queue.put({"type": type, "args": args})

    def insert_run(
        self,
        suite: SuiteName,
        db: DatabaseName,
        db_version: str,
        operation: Operation,
        system: str,
        started_at: datetime,
    ) -> int:
        self.put("insert_run", [suite, db, db_version, operation, system, "running", started_at])
        return cast(int, self.result_queue.get())

    def finish_run(
        self,
        run_id: int,
        finished_at: datetime,
        status: RunStatus,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        self.put("finish_run", [finished_at, status, error_type, error_message, run_id])

    def start_step(
        self,
        run_id: int,
        step_type: StepType,
        step_name: str,
        started_at: datetime,
        query_name: str | None = None,
        iteration: int | None = None,
        table_name: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        self.put(
            "start_step",
            [run_id, step_type, step_name, query_name, iteration, table_name, started_at, "running", metadata],
        )
        return cast(int, self.result_queue.get())

    def finish_step(
        self,
        step_id: int,
        finished_at: datetime,
        status: RunStatus,
        row_count: int | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.put(
            "finish_step",
            [finished_at, status, row_count, error_type, error_message, metadata, step_id],
        )

    def insert_metric(self, run_id: int, time: datetime, cpu_percent: float, mem_mb: int, disk_mb: int) -> None:
        self.put("insert_metric", [run_id, time, cpu_percent, mem_mb, disk_mb])

    def debug(self, content: str | None = None) -> int:
        if content is None:
            content = uuid.uuid4().hex

        self.put("debug", [content])
        return cast(int, self.result_queue.get())

    def shutdown(self, timeout_seconds: float = 10.0) -> None:
        self.put("shutdown", [])

        try:
            ack = self.result_queue.get(timeout=timeout_seconds)
        except Empty as exc:
            raise TimeoutError("Timed out waiting for writer shutdown acknowledgement") from exc

        if ack != "ok":
            raise RuntimeError(f"Unexpected writer shutdown acknowledgement: {ack}")
