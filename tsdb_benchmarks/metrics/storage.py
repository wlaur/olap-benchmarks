from datetime import datetime
from multiprocessing import Process, Queue
from typing import Any, Literal, TypedDict, cast

import duckdb

from ..settings import REPO_ROOT, SETTINGS, DatabaseName, Operation, SuiteName

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
    result_queue: Queue


class Storage:
    def __init__(self) -> None:
        self.db_path = SETTINGS.results_directory / "results.db"
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._queue: Queue = Queue()
        self._writer_process: Process = Process(target=self.writer_loop, args=(self._queue,))
        self._writer_process.start()

    def writer_loop(self, queue: Queue) -> None:
        conn = duckdb.connect(self.db_path)
        with (REPO_ROOT / "tsdb_benchmarks/metrics/schema.sql").open() as f:
            conn.execute(f.read())

        while True:
            msg = cast(WriterMessage, queue.get())

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
                    if msg.get("result_queue"):
                        msg["result_queue"].put(result[0] if result else None)

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

    def put(self, type: MessageType, args: list[Any], result_queue: Queue | None = None) -> None:
        self._queue.put(
            {
                "type": "insert_benchmark",
                "args": args,
                "result_queue": result_queue,
            }
        )

    def insert_benchmark(
        self, suite: SuiteName, db: DatabaseName, operation: Operation, started_at: datetime, notes: str | None = None
    ) -> int:
        result_queue: Queue = Queue()
        self.put("insert_benchmark", [suite, db, operation, started_at, notes], result_queue)
        return result_queue.get()

    def finish_benchmark(self, benchmark_id: int, finished_at: datetime) -> None:
        self.put("finish_benchmark", [finished_at, benchmark_id])

    def insert_metric(self, benchmark_id: int, time: datetime, cpu_percent: float, mem_mb: int, disk_mb: int) -> None:
        self.put("insert_metric", [benchmark_id, time, cpu_percent, mem_mb, disk_mb])

    def insert_event(self, benchmark_id: int, time: datetime, name: str, type: EventType) -> None:
        self.put("insert_event", [benchmark_id, time, name, type])
