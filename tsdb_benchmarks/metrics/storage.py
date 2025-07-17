from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from time import perf_counter, sleep
from typing import Literal

import duckdb

from ..settings import REPO_ROOT, SETTINGS, DatabaseName, Operation

EventType = Literal["start", "end"]


class Storage:
    def __init__(self) -> None:
        self.db_path = SETTINGS.results_directory / "results.db"
        self._connection: duckdb.DuckDBPyConnection | None = None

        with self.connect():
            self.init_schema()

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._connection is None:
            raise RuntimeError("Connection is not open, use with storage.db() as con: ...")

        return self._connection

    @contextmanager
    def connect(self, timeout: float = 30.0, wait_interval_seconds: float = 0.1) -> Iterator[None]:
        # hacky way of ensure the db can be opened (after waiting) even if other processes are writing
        start = perf_counter()
        while True:
            try:
                self._connection = duckdb.connect(self.db_path)
                break
            except duckdb.Error as e:
                if "lock" in str(e).lower() and ((perf_counter() - start) < timeout):
                    sleep(wait_interval_seconds)
                else:
                    raise

        try:
            yield
        finally:
            self._connection.close()
            self._connection = None

    def init_schema(self) -> None:
        with (REPO_ROOT / "tsdb_benchmarks/metrics/schema.sql").open() as f:
            sql = f.read()

        self.conn.execute(sql)

    def insert_benchmark(
        self, name: DatabaseName, operation: Operation, started_at: datetime, notes: str | None = None
    ) -> int:
        result = self.conn.execute(
            """
            insert into benchmark (name, operation, started_at, notes)
            values (?, ?, ?, ?)
            returning id
            """,
            [name, operation, started_at, notes],
        ).fetchone()
        assert result is not None
        return result[0]

    def finish_benchmark(self, benchmark_id: int, finished_at: datetime) -> None:
        self.conn.execute(
            """
            update benchmark
            set finished_at = ?
            where id = ?
            """,
            [finished_at, benchmark_id],
        )

    def insert_metric(self, benchmark_id: int, time: datetime, cpu_percent: float, mem_mb: int, disk_mb: int) -> None:
        self.conn.execute(
            """
            insert into metric (
                benchmark_id, time, cpu_percent, mem_mb, disk_mb
            )
            values (?, ?, ?, ?, ?)
            """,
            [benchmark_id, time, cpu_percent, mem_mb, disk_mb],
        )

    def insert_event(self, benchmark_id: int, time: datetime, name: str, type: EventType) -> None:
        self.conn.execute(
            """
            insert into event (
                benchmark_id, time, name, type
            )
            values (?, ?, ?, ?)
            """,
            [benchmark_id, time, name, type],
        )
