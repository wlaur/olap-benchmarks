from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import Process, Queue
from queue import Empty
from threading import Lock
from typing import Any, Literal, TypedDict, cast

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from ..results import get_results_engine
from ..results.models import DebugEntry, QueryExecution, Run, RunMetric, RunStep, SystemSnapshot
from ..results.schema import ensure_results_schema
from ..run_metadata import IterationRole, StepResultStatus
from ..settings import DatabaseName, Operation, Revision, SuiteName, setup_stdout_logging

_LOGGER = logging.getLogger(__name__)

RunStatus = Literal["running", "completed", "failed"]
StepType = Literal["phase", "query", "mutation"]

MessageType = Literal[
    "insert_run",
    "finish_run",
    "insert_metric",
    "insert_query_execution",
    "start_step",
    "finish_step",
    "debug",
    "shutdown",
]


class WriterMessage(TypedDict):
    type: MessageType
    args: list[Any]


def _get_or_create_system_snapshot(
    session: Session,
    system: str,
    metadata: dict[str, Any] | None,
) -> int | None:
    if metadata is None:
        return None

    canonical_metadata = cast(dict[str, Any], json.loads(json.dumps(metadata, sort_keys=True)))
    existing_id = session.scalar(
        select(SystemSnapshot.id)
        .where(SystemSnapshot.system == system)
        .where(SystemSnapshot.metadata_json == canonical_metadata)
        .limit(1)
    )
    if existing_id is not None:
        return existing_id

    host_value = canonical_metadata.get("host")
    host = cast(dict[str, Any], host_value) if isinstance(host_value, dict) else {}
    snapshot = SystemSnapshot(
        system=system,
        os=cast(str | None, host.get("os")),
        os_release=cast(str | None, host.get("os_release")),
        machine=cast(str | None, host.get("machine")),
        processor=cast(str | None, host.get("processor")),
        cpu_count_logical=cast(int | None, host.get("cpu_count_logical")),
        memory_total_mb=cast(int | None, host.get("memory_total_mb")),
        metadata_json=canonical_metadata,
    )
    session.add(snapshot)
    session.flush()
    return snapshot.id


def writer_loop(queue: Queue[WriterMessage], result_queue: Queue[object], revision: Revision = "default") -> None:
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
                    system = cast(str, msg["args"][5])
                    system_snapshot_id = _get_or_create_system_snapshot(
                        session,
                        system,
                        cast(dict[str, Any] | None, msg["args"][8]),
                    )
                    row = Run(
                        suite=cast(str, msg["args"][0]),
                        suite_scale_factor=cast(int, msg["args"][1]),
                        db=cast(str, msg["args"][2]),
                        db_version=cast(str, msg["args"][3]),
                        operation=cast(str, msg["args"][4]),
                        system=system,
                        system_snapshot_id=system_snapshot_id,
                        status=cast(str, msg["args"][6]),
                        started_at=cast(datetime, msg["args"][7]),
                        metadata_json=cast(dict[str, Any] | None, msg["args"][9]),
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
                        client_mem_mb=cast(int, msg["args"][4]),
                        client_uss_mb=cast(int, msg["args"][5]),
                        disk_mb=cast(int, msg["args"][6]),
                    )
                    session.add(row)
                    session.commit()

                case "insert_query_execution":
                    row = QueryExecution(
                        run_id=cast(int, msg["args"][0]),
                        run_step_id=cast(int | None, msg["args"][1]),
                        query=cast(str, msg["args"][2]),
                        start_time=cast(datetime, msg["args"][3]),
                        end_time=cast(datetime, msg["args"][4]),
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
                        result_status=cast(str | None, msg["args"][8]),
                        iteration_role=cast(str | None, msg["args"][9]),
                        metadata_json=cast(dict[str, Any] | None, msg["args"][10]),
                    )
                    session.add(row)
                    session.commit()
                    result_queue.put(row.id)

                case "finish_step":
                    update_values: dict[str, Any] = {
                        "finished_at": cast(datetime, msg["args"][0]),
                        "status": cast(str, msg["args"][1]),
                        "result_status": cast(str | None, msg["args"][2]),
                        "row_count": cast(int | None, msg["args"][3]),
                        "error_type": cast(str | None, msg["args"][4]),
                        "error_message": cast(str | None, msg["args"][5]),
                    }

                    metadata_value = cast(dict[str, Any] | None, msg["args"][6])
                    if metadata_value is not None:
                        update_values["metadata_json"] = metadata_value

                    session.execute(
                        update(RunStep).where(RunStep.id == cast(int, msg["args"][7])).values(**update_values)
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


def start_writer_process(revision: Revision = "default") -> WriterProcessHandle:
    queue: Queue[WriterMessage] = Queue()
    result_queue: Queue[object] = Queue()

    writer_process = Process(target=writer_loop, args=(queue, result_queue, revision), daemon=False)
    writer_process.start()

    return WriterProcessHandle(process=writer_process, queue=queue, result_queue=result_queue)


class Storage:
    def __init__(self, queue: Queue[WriterMessage], result_queue: Queue[object]) -> None:
        self.queue = queue
        self.result_queue = result_queue
        self._response_lock = Lock()

    def put(self, type: MessageType, args: list[Any]) -> None:
        self.queue.put({"type": type, "args": args})

    def put_and_get_id(self, type: MessageType, args: list[Any]) -> int:
        with self._response_lock:
            self.put(type, args)
            return cast(int, self.result_queue.get())

    def insert_run(
        self,
        suite: SuiteName,
        suite_scale_factor: int,
        db: DatabaseName,
        db_version: str,
        operation: Operation,
        system: str,
        started_at: datetime,
        system_metadata: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        return self.put_and_get_id(
            "insert_run",
            [
                suite,
                suite_scale_factor,
                db,
                db_version,
                operation,
                system,
                "running",
                started_at,
                system_metadata,
                metadata,
            ],
        )

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
        result_status: StepResultStatus | None = None,
        iteration_role: IterationRole | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        return self.put_and_get_id(
            "start_step",
            [
                run_id,
                step_type,
                step_name,
                query_name,
                iteration,
                table_name,
                started_at,
                "running",
                result_status,
                iteration_role,
                metadata,
            ],
        )

    def finish_step(
        self,
        step_id: int,
        finished_at: datetime,
        status: RunStatus,
        result_status: StepResultStatus | None = None,
        row_count: int | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.put(
            "finish_step",
            [finished_at, status, result_status, row_count, error_type, error_message, metadata, step_id],
        )

    def insert_metric(
        self,
        run_id: int,
        time: datetime,
        cpu_percent: float,
        mem_mb: int,
        client_mem_mb: int,
        client_uss_mb: int,
        disk_mb: int,
    ) -> None:
        self.put(
            "insert_metric",
            [run_id, time, cpu_percent, mem_mb, client_mem_mb, client_uss_mb, disk_mb],
        )

    def insert_query_execution(
        self,
        run_id: int,
        query: str,
        start_time: datetime,
        end_time: datetime,
        run_step_id: int | None = None,
    ) -> None:
        self.put("insert_query_execution", [run_id, run_step_id, query, start_time, end_time])

    def debug(self, content: str | None = None) -> int:
        if content is None:
            content = uuid.uuid4().hex

        return self.put_and_get_id("debug", [content])

    def shutdown(self, timeout_seconds: float = 10.0) -> None:
        self.put("shutdown", [])

        try:
            ack = self.result_queue.get(timeout=timeout_seconds)
        except Empty as exc:
            raise TimeoutError("Timed out waiting for writer shutdown acknowledgement") from exc

        if ack != "ok":
            raise RuntimeError(f"Unexpected writer shutdown acknowledgement: {ack}")
