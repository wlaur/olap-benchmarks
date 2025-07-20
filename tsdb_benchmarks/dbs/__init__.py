import logging
import os
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from queue import Queue
from time import perf_counter, sleep
from typing import Any

import polars as pl
from pydantic import BaseModel
from sqlalchemy import Connection, text

from ..metrics.sampler import start_metric_sampler
from ..metrics.storage import EventType, Storage
from ..settings import REPO_ROOT, SETTINGS, DatabaseName, Operation, SuiteName, TableName
from ..suites.rtabench.config import RTABENCH_QUERY_NAMES, RTABENCH_SCHEMAS
from ..suites.time_series.config import get_time_series_input_files

_LOGGER = logging.getLogger(__name__)


RTABENCH_QUERIES_DIRECTORY = REPO_ROOT / "tsdb_benchmarks/suites/rtabench/queries"


class Database(BaseModel, ABC):
    name: DatabaseName
    connection_string: str

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

    def restart_event(self) -> None:
        with self.event_context("restart"):
            _LOGGER.info(f"Restarting service {self.name}")
            os.system(self.restart)
            _LOGGER.info(f"Restarted service {self.name}")
            self.wait_until_accessible()

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

        _LOGGER.info(f"Created rtabench tables for {self.name}")

        for table_name in RTABENCH_SCHEMAS:
            df = pl.read_parquet(SETTINGS.input_data_directory / f"rtabench/{table_name}.parquet")

            with self.event_context(f"insert_{table_name}"):
                self.insert(df, table_name)
                _LOGGER.info(f"Inserted {table_name} for {self.name}")

        # restart db to ensure data is not kept in-memory by the db, and also
        # ensure that WAL is processed etc...
        if restart:
            self.restart_event()

    @property
    def rtabench_fetch_kwargs(self) -> dict[str, Any]:
        return {}

    def load_rtabench_query(self, query_name: str) -> str:
        with (RTABENCH_QUERIES_DIRECTORY / f"{self.name}/{query_name}.sql").open() as f:
            return f.read()

    def run_rtabench(self) -> None:
        t0 = perf_counter()
        for idx, (query_name, iterations) in enumerate(RTABENCH_QUERY_NAMES.items()):
            query = self.load_rtabench_query(query_name)

            for it in range(1, iterations + 1):
                with self.event_context(f"query_{query_name}_iteration_{it}"):
                    t1 = perf_counter()
                    df = self.fetch(query, **self.rtabench_fetch_kwargs)
                    t = perf_counter() - t1

                # time delta t will not match time at end - time at start exactly, but within a couple of milliseconds
                # there is a small overhead when the event is sent to the queue
                # (the actual write to result db happens later)
                _LOGGER.info(
                    f"Executed {query_name} ({idx + 1:_}/{len(RTABENCH_QUERY_NAMES):_}) "
                    f"iteration {it:_}/{iterations:_} "
                    f"in {1_000 * (t):_.2f} ms\ndf={df}"
                )

        _LOGGER.info(
            f"Executed {len(RTABENCH_QUERY_NAMES):_} queries (with repetitions) in {perf_counter() - t0:_.2f} seconds"
        )

    def populate_time_series(self, restart: bool = True) -> None:
        for table_name, fpath in get_time_series_input_files().items():
            # do not use primary key for time series data (e.g. Clickhouse does not enforce unique primary key)
            primary_key = None
            not_null = "time" if "_wide" in table_name else ["time", "id"]

            df = pl.read_parquet(fpath)

            with self.event_context(f"insert_{table_name}"):
                self.insert(df, table_name, primary_key=primary_key, not_null=not_null)
                _LOGGER.info(f"Inserted {table_name} for {self.name}")

        # restart db to ensure data is not kept in-memory by the db, and also
        # ensure that WAL is processed etc...
        if restart:
            self.restart_event()

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
            with self.event_context(operation):
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

    def wait_until_accessible(self, timeout_seconds: float = 900.0, interval_seconds: float = 0.1) -> None:
        _LOGGER.info(f"Waiting for database {self.name}...")

        deadline = perf_counter() + timeout_seconds

        while perf_counter() < deadline:
            try:
                self.connect(reconnect=True)
                self.fetch("select 1")
                _LOGGER.info(f"Database {self.name} is ready to accept connections")
                return
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
