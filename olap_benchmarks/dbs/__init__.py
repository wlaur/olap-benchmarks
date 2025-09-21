from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from queue import Queue
from time import perf_counter, sleep
from typing import TYPE_CHECKING

import polars as pl
from pydantic import BaseModel
from sqlalchemy import Connection, text

from ..metrics.sampler import start_metric_sampler
from ..metrics.storage import EventType, Storage
from ..settings import REPO_ROOT, SETTINGS, DatabaseName, Operation, SuiteName, TableName

if TYPE_CHECKING:
    from ..suites import BenchmarkSuite

_LOGGER = logging.getLogger(__name__)


class QueryContext(BaseModel):
    suite: SuiteName
    query_name: str


class Database(BaseModel, ABC):
    name: DatabaseName
    version: str

    connection_string: str

    context: QueryContext | None = None

    _connection: Connection | None = None
    _result_storage: Storage | None = None
    _benchmark_id: int | None = None

    _queue: Queue | None = None
    _result_queue: Queue | None = None

    def set_queues(self, queue: Queue, result_queue: Queue) -> None:
        self._queue = queue
        self._result_queue = result_queue

    def create_result_storage(self) -> Storage:
        assert self._queue is not None and self._result_queue is not None
        return Storage(self._queue, self._result_queue)

    @property
    def result_storage(self) -> Storage:
        if self._result_storage is None:
            raise ValueError("self._result_storage is not set")

        return self._result_storage

    @property
    def benchmark_id(self) -> int:
        if self._benchmark_id is None:
            raise ValueError("self._benchmark_id is not set")

        return self._benchmark_id

    @property
    @abstractmethod
    def start(self) -> str: ...

    @property
    def stop(self) -> str:
        return f"docker stop {self.name}-benchmark"

    @property
    def restart(self) -> str:
        return f"docker restart {self.name}-benchmark"

    def event(self, name: str, type: EventType) -> None:
        self.result_storage.insert_event(self.benchmark_id, datetime.now(UTC).replace(tzinfo=None), name, type)
        _LOGGER.info(f"Registered event {name}:{type}")

    @contextmanager
    def event_context(self, name: str) -> Iterator[None]:
        self.event(name, "start")
        yield
        self.event(name, "end")

    @contextmanager
    def query_context(self, suite: SuiteName, query_name: str) -> Iterator[None]:
        self.context = QueryContext(suite=suite, query_name=query_name)

        yield

        self.context = None

    def restart_event(self) -> None:
        with self.event_context("restart"):
            _LOGGER.info(f"Restarting service {self.name}")
            os.system(self.restart)
            _LOGGER.info(f"Restarted service {self.name}")
            self.wait_until_accessible()

    def execute_schema_file(self, fpath: Path) -> None:
        with (fpath).open() as f:
            statements = f.read()

        for stmt in statements.split(";"):
            stmt = stmt.strip()

            if not stmt or all(line.strip().startswith("--") for line in stmt.splitlines()):
                continue

            # ensure the connection used when initializing the schema is not reused
            # if we use e.g. ALTER DATABASE, it's important that subsequent queries use a new connection
            con = self.connect(reconnect=True)
            con.execute(text(stmt))
            con.commit()

    def initialize_schema(self, suite: SuiteName) -> None:
        fpath = REPO_ROOT / f"olap_benchmarks/suites/{suite}/schemas/{self.name}.sql"

        if not fpath.is_file():
            _LOGGER.info(f"Schema definition for {self.name}:{suite} does not exist, skipping...")
            return

        with self.event_context("schema"):
            self.execute_schema_file(fpath)

        self.connect(reconnect=True)
        _LOGGER.info(f"Initialized schema for {self.name}:{suite}")

    @abstractmethod
    def connect(self, reconnect: bool = False) -> Connection: ...

    def rollback(self) -> None:
        if self._connection is None:
            return
        self._connection.rollback()

    def wait_until_accessible(self, timeout_seconds: float = 900.0, interval_seconds: float = 0.1) -> None:
        _LOGGER.info(f"Waiting for database {self.name}...")

        deadline = perf_counter() + timeout_seconds

        while perf_counter() < deadline:
            try:
                self.connect(reconnect=True)
                self.fetch("select 1")
                _LOGGER.info(f"Database {self.name} is ready to accept connections")
                return
            except NotImplementedError:
                raise
            except Exception as e:
                _LOGGER.debug(f"Database not ready yet: {e}")
                sleep(interval_seconds)

        raise TimeoutError(f"Timed out waiting for database {self.name} to become ready")

    @abstractmethod
    def fetch(
        self, query: str, schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None
    ) -> pl.DataFrame: ...

    @abstractmethod
    def insert(
        self,
        df: pl.DataFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None: ...

    @abstractmethod
    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None: ...

    @property
    def rtabench(self) -> BenchmarkSuite:
        from ..suites.rtabench.config import RTABench

        return RTABench(db=self)

    @property
    def clickbench(self) -> BenchmarkSuite:
        from ..suites.clickbench.config import Clickbench

        return Clickbench(db=self)

    @property
    def time_series(self) -> BenchmarkSuite:
        from ..suites.time_series.config import TimeSeries

        return TimeSeries(db=self)

    @property
    def kaggle_airbnb(self) -> BenchmarkSuite:
        from ..suites.kaggle_airbnb.config import KaggleAirbnb

        return KaggleAirbnb(db=self)

    @property
    def benchmarks(self) -> dict[SuiteName, BenchmarkSuite]:
        return {
            "rtabench": self.rtabench,
            "time_series": self.time_series,
            "clickbench": self.clickbench,
            "kaggle_airbnb": self.kaggle_airbnb,
        }

    def benchmark(self, suite: SuiteName, operation: Operation) -> None:
        benchmark = self.benchmarks.get(suite)

        if benchmark is None:
            raise ValueError(f"Invalid benchmark suite: '{suite}'")

        match operation:
            case "populate":
                benchmark_func = benchmark.populate
            case "run":
                benchmark_func = benchmark.run
            case _:
                raise ValueError(f"Invalid operation '{operation}'")

        self._result_storage = self.create_result_storage()

        self._benchmark_id, process, stop_event = start_metric_sampler(
            suite,
            self.name,
            operation,
            self._result_storage,
            interval_seconds=None,  # docker stats takes ~1 sec, no need to wait here
            notes=f"{self.version} | {SETTINGS.system}",
        )

        t0 = perf_counter()

        _LOGGER.info(
            f"Starting benchmark with ID {self._benchmark_id} (database: {self.name}, suite: {suite}, "
            f"operation: {operation})"
        )

        try:
            with self.event_context(operation):
                benchmark_func()
        finally:
            stop_event.set()
            process.join()

        _LOGGER.info(f"Finished benchmark with ID {self._benchmark_id} in {perf_counter() - t0:_.2f} seconds")
