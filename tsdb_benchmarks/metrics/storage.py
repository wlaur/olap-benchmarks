import logging
from datetime import datetime
from multiprocessing import Manager, Process
from queue import Queue
from typing import Any, Literal, TypedDict, cast

import duckdb

from ..settings import REPO_ROOT, SETTINGS, DatabaseName, Operation, SuiteName, setup_stdout_logging

_LOGGER = logging.getLogger(__name__)

EventType = Literal["start", "end"]

MessageType = Literal[
    "insert_benchmark",
    "finish_benchmark",
    "insert_metric",
    "insert_event",
]


class WriterMessage(TypedDict, total=False):
    type: MessageType
    args: list[Any]


def writer_loop(queue: Queue, result_queue: Queue) -> None:
    setup_stdout_logging()
    db_path = SETTINGS.results_directory / "results.db"

    _LOGGER.info(f"Trying to connect to results database at {db_path}")
    conn = duckdb.connect(db_path)
    _LOGGER.info(f"Connected to results database at {db_path}")

    with (REPO_ROOT / "tsdb_benchmarks/metrics/schema.sql").open() as f:
        conn.execute(f.read())

    while True:
        try:
            msg = cast(WriterMessage, queue.get())
        except EOFError:
            return

        match msg["type"]:
            case "insert_benchmark":
                result = conn.execute(
                    """
                    insert into benchmark (suite, db, operation, started_at, notes)
                    values (?, ?, ?, ?, ?)
                    returning id
                    """,
                    msg["args"],
                ).fetchone()

                result_queue.put(result[0] if result else None)

            case "finish_benchmark":
                conn.execute("update benchmark set finished_at = ? where id = ?", msg["args"])

            case "insert_metric":
                conn.execute(
                    """
                    insert into metric (
                        benchmark_id, time, cpu_percent, mem_mb, disk_mb
                    )
                    values (?, ?, ?, ?, ?)
                    """,
                    msg["args"],
                )

            case "insert_event":
                conn.execute(
                    """
                    insert into event (
                        benchmark_id, time, name, type
                    )
                    values (?, ?, ?, ?)
                    """,
                    msg["args"],
                )

            case _:
                raise ValueError(f"Unknown message type: {msg['type']}")

        _LOGGER.info(f"Wrote message with type {msg['type']}")


def start_writer_process() -> tuple[Queue, Queue]:
    manager = Manager()
    queue = manager.Queue()
    result_queue = manager.Queue()

    writer_process: Process = Process(target=writer_loop, args=(queue, result_queue))
    writer_process.start()

    return queue, result_queue


class Storage:
    def __init__(self, queue: Queue, result_queue: Queue) -> None:
        self.queue = queue
        self.result_queue = result_queue

    def put(self, type: MessageType, args: list[Any]) -> None:
        self.queue.put({"type": type, "args": args})

    def insert_benchmark(
        self, suite: SuiteName, db: DatabaseName, operation: Operation, started_at: datetime, notes: str | None = None
    ) -> int:
        self.put("insert_benchmark", [suite, db, operation, started_at, notes])
        return self.result_queue.get()

    def finish_benchmark(self, benchmark_id: int, finished_at: datetime) -> None:
        self.put("finish_benchmark", [finished_at, benchmark_id])

    def insert_metric(self, benchmark_id: int, time: datetime, cpu_percent: float, mem_mb: int, disk_mb: int) -> None:
        self.put("insert_metric", [benchmark_id, time, cpu_percent, mem_mb, disk_mb])

    def insert_event(self, benchmark_id: int, time: datetime, name: str, type: EventType) -> None:
        self.put("insert_event", [benchmark_id, time, name, type])
