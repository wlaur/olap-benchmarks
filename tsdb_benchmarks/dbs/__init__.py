import logging
import os
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from queue import Queue
from time import perf_counter
from typing import Any

import polars as pl
from pydantic import BaseModel
from sqlalchemy import Connection, text

from ..metrics.sampler import start_metric_sampler
from ..metrics.storage import EventType, Storage
from ..settings import REPO_ROOT, SETTINGS, DatabaseName, Operation, SuiteName, TableName
from ..suites.rtabench.config import RTABENCH_QUERY_NAMES, RTABENCH_SCHEMAS

_LOGGER = logging.getLogger(__name__)


RTABENCH_QUERIES_DIRECTORY = REPO_ROOT / "tsdb_benchmarks/suites/rtabench/queries"


class Database(BaseModel, ABC):
    name: DatabaseName

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

    def setup(self) -> None:
        pass

    def event(self, name: str, type: EventType) -> None:
        self.result_storage.insert_event(self.benchmark_id, datetime.now(UTC).replace(tzinfo=None), name, type)
        _LOGGER.info(f"Registered event {name}:{type}")

    @contextmanager
    def event_context(self, name: str) -> Iterator[None]:
        self.event(name, "start")

        yield

        self.event(name, "end")

    def populate_rtabench(self, restart: bool = True) -> None:
        with (REPO_ROOT / f"tsdb_benchmarks/suites/rtabench/schemas/{self.name}.sql").open() as f:
            sql = f.read()

        con = self.connect()

        with self.event_context("schema"):
            for stmt in sql.split(";"):
                if not stmt.strip():
                    continue

                con.execute(text(stmt))

            con.commit()

        _LOGGER.info(f"Created RTABench tables for {self.name}")

        for table_name in RTABENCH_SCHEMAS:
            df = pl.read_parquet(SETTINGS.input_data_directory / f"rtabench/{table_name}.parquet")

            with self.event_context(f"insert_{table_name}"):
                self.insert(df, table_name)
                _LOGGER.info(f"Inserted {table_name} for {self.name}")

        if restart:
            with self.event_context("restart"):
                # restart db to ensure data is not kept in-memory by the db, and also
                # ensure that WAL is processed etc...
                os.system(self.restart)
                self.fetch("select 1")

    @property
    def rtabench_fetch_kwargs(self) -> dict[str, Any]:
        return {}

    def load_rtabench_query(self, query_name: str) -> str:
        with (RTABENCH_QUERIES_DIRECTORY / f"{self.name}/{query_name}.sql").open() as f:
            return f.read()

    def run_rtabench(self) -> None:
        self.event("queries", "start")

        t0 = perf_counter()
        for idx, (query_name, iterations) in enumerate(RTABENCH_QUERY_NAMES.items()):
            query = self.load_rtabench_query(query_name)

            for it in range(1, iterations + 1):
                with self.event_context(f"query_{query_name}_iteration_{it}"):
                    t1 = perf_counter()
                    df = self.fetch(query, **self.rtabench_fetch_kwargs)
                    t = perf_counter() - t1

                    _LOGGER.info(
                        f"Executed {query_name} ({idx + 1:_}/{len(RTABENCH_QUERY_NAMES):_}) "
                        f"iteration {it:_}/{iterations:_} "
                        f"in {1_000 * (t):_.2f} ms\ndf={df}"
                    )

        self.event("queries", "end")

        _LOGGER.info(
            f"Executed {len(RTABENCH_QUERY_NAMES):_} queries (with repetitions) in {perf_counter() - t0:_.2f} seconds"
        )

    def populate_time_series(self) -> None:
        raise NotImplementedError

    def run_time_series(self) -> None:
        raise NotImplementedError

    def benchmark(self, suite: SuiteName, operation: Operation) -> None:
        self._result_storage = self.create_result_storage()

        operations: dict[SuiteName, dict[Operation, Callable[[], None]]] = {
            "rtabench": {
                "populate": self.populate_rtabench,
                "run": self.run_rtabench,
            },
            "time_series": {
                "populate": self.populate_time_series,
                "run": self.run_time_series,
            },
        }

        self._benchmark_id, process, stop_event = start_metric_sampler(
            suite,
            self.name,
            operation,
            self._result_storage,
            interval_seconds=None,  # docker stats takes ~1 sec, no need to wait here
        )

        t0 = perf_counter()
        _LOGGER.info(
            f"Starting benchmark with ID {self._benchmark_id} (database: {self.name}, suite: {suite}, "
            f"operation: {operation})"
        )

        try:
            operations[suite][operation]()
        finally:
            stop_event.set()
            process.join()

        _LOGGER.info(f"Finished benchmark with ID {self._benchmark_id} in {perf_counter() - t0:_.2f} seconds")

    @abstractmethod
    def connect(self, reconnect: bool = False) -> Connection: ...

    def rollback(self) -> None:
        if self._connection is None:
            return
        self._connection.rollback()

    @abstractmethod
    def fetch(
        self, query: str, schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None
    ) -> pl.DataFrame: ...

    @abstractmethod
    def insert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str] | None = None) -> None: ...

    @abstractmethod
    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None: ...
