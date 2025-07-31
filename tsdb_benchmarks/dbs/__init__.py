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
from ..suites.clickbench.config import ITERATIONS as CLICKBENCH_ITERATIONS
from ..suites.clickbench.config import load_clickbench_dataset
from ..suites.rtabench.config import RTABENCH_QUERY_NAMES, RTABENCH_SCHEMAS
from ..suites.time_series.config import TIME_SERIES_QUERY_NAMES, get_time_series_input_files

_LOGGER = logging.getLogger(__name__)


RTABENCH_QUERIES_DIRECTORY = REPO_ROOT / "tsdb_benchmarks/suites/rtabench/queries"
TIME_SERIES_QUERIES_DIRECTORY = REPO_ROOT / "tsdb_benchmarks/suites/time_series/queries"


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

    def initialize_schema(self, suite: SuiteName) -> None:
        fpath = REPO_ROOT / f"tsdb_benchmarks/suites/{suite}/schemas/{self.name}.sql"

        if not fpath.is_file():
            _LOGGER.info(f"Schema definition for {self.name}:{suite} does not exist, skipping...")
            return

        with (fpath).open() as f:
            statements = f.read()

        with self.event_context("schema"):
            for stmt in statements.split(";"):
                stmt = stmt.strip()

                if not stmt or all(line.strip().startswith("--") for line in stmt.splitlines()):
                    continue

                # ensure the connection used when initializing the schema is not reused
                # if we use e.g. ALTER DATABASE, it's important that subsequent queries use a new connection
                con = self.connect(reconnect=True)
                con.execute(text(stmt))
                con.commit()

        self.connect(reconnect=True)
        _LOGGER.info(f"Initialized schema for {self.name}:{suite}")

    @property
    def rtabench_populate_kwargs(self) -> dict[str, Any]:
        return {}

    def populate_rtabench(self, restart: bool = True) -> None:
        self.initialize_schema("rtabench")

        for table_name in RTABENCH_SCHEMAS:
            df = pl.read_parquet(SETTINGS.input_data_directory / f"rtabench/{table_name}.parquet")

            with self.event_context(f"insert_{table_name}"):
                self.insert(df, table_name, **self.rtabench_populate_kwargs)
                _LOGGER.info(f"Inserted {table_name} for {self.name}")

        _LOGGER.info(f"Inserted all rtabench tables for {self.name}")

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

    def get_time_series_primary_key(self, table_name: TableName) -> str | list[str] | None:
        # do not use primary key for time series data (e.g. Clickhouse does not enforce unique primary key)
        return None

    def get_time_series_not_null(self, table_name: TableName) -> str | list[str] | None:
        return ["time", "id"] if "_eav" in table_name else "time"

    @property
    def time_series_populate_kwargs(self) -> dict[str, Any]:
        return {}

    def populate_time_series(self, restart: bool = True) -> None:
        self.initialize_schema("time_series")

        for table_name, fpath in get_time_series_input_files().items():
            primary_key = self.get_time_series_primary_key(table_name)
            not_null = self.get_time_series_not_null(table_name)

            df = pl.read_parquet(fpath)

            with self.event_context(f"insert_{table_name}"):
                self.insert(
                    df, table_name, primary_key=primary_key, not_null=not_null, **self.time_series_populate_kwargs
                )
                _LOGGER.info(f"Inserted {table_name} for {self.name}")

        _LOGGER.info(f"Inserted all time_series tables for {self.name}")

        # restart db to ensure data is not kept in-memory by the db, and also
        # ensure that WAL is processed etc...
        if restart:
            self.restart_event()

    def load_time_series_query(self, query_name: str) -> str:
        db_specific = TIME_SERIES_QUERIES_DIRECTORY / f"{self.name}/{query_name}.sql"
        common = TIME_SERIES_QUERIES_DIRECTORY / f"{query_name}.sql"

        sql_source = db_specific if db_specific.is_file() else common

        with (sql_source).open() as f:
            return f.read()

    @property
    def time_series_fetch_kwargs(self) -> dict[str, Any]:
        return {}

    def run_time_series(self) -> None:
        t0 = perf_counter()
        for idx, (query_name, iterations) in enumerate(TIME_SERIES_QUERY_NAMES.items()):
            query = self.load_time_series_query(query_name)

            for it in range(1, iterations + 1):
                with self.event_context(f"query_{query_name}_iteration_{it}"):
                    t1 = perf_counter()
                    df = self.fetch(query, **self.time_series_fetch_kwargs)
                    t = perf_counter() - t1

                _LOGGER.info(
                    f"Executed {query_name} ({idx + 1:_}/{len(TIME_SERIES_QUERY_NAMES):_}) "
                    f"iteration {it:_}/{iterations:_} "
                    f"in {1_000 * (t):_.2f} ms\ndf (head 100)={df.head(100)}"
                )

        _LOGGER.info(
            f"Executed {len(RTABENCH_QUERY_NAMES):_} queries (with repetitions) in {perf_counter() - t0:_.2f} seconds"
        )

    @property
    def clickbench_populate_kwargs(self) -> dict[str, Any]:
        return {}

    def populate_clickbench(self, restart: bool = True) -> None:
        self.initialize_schema("clickbench")

        # this is an expensive operation, would be better to avoid reading with polars
        # for the databases that can ingest directly from parquet
        # on the other hand, the purpose of these benchmarks is to measure in-memory polars df
        # to and from the database, so this is appropriate,
        # although not directly comparable with the insert times from the official clickbench resultsg
        df = load_clickbench_dataset()
        _LOGGER.info(f"Loaded clickbench dataset with shape ({df.shape[0]:_}, {df.shape[1]:_})")

        with self.event_context("insert_hits"):
            self.insert(df, "hits", **self.clickbench_populate_kwargs)

        _LOGGER.info(f"Inserted clickbench table for {self.name}")

        # restart db to ensure data is not kept in-memory by the db, and also
        # ensure that WAL is processed etc...
        if restart:
            self.restart_event()

    @property
    def clickbench_fetch_kwargs(self) -> dict[str, Any]:
        return {}

    def run_clickbench(self) -> None:
        t0 = perf_counter()
        iterations = CLICKBENCH_ITERATIONS

        # NOTE: clickbench query files should not be formatted, need to have one query per line
        with (REPO_ROOT / f"tsdb_benchmarks/suites/clickbench/queries/{self.name}.sql").open() as f:
            queries = f.readlines()

        for idx, query in enumerate(queries):
            query_name = f"Q{idx}"

            for it in range(1, iterations + 1):
                with self.event_context(f"query_{query_name}_iteration_{it}"):
                    t1 = perf_counter()
                    df = self.fetch(query, **self.clickbench_fetch_kwargs)
                    t = perf_counter() - t1

                _LOGGER.info(
                    f"Executed {query_name} ({idx + 1:_}/{len(queries):_}) "
                    f"iteration {it:_}/{iterations:_} "
                    f"in {1_000 * (t):_.2f} ms\ndf={df}"
                )

        _LOGGER.info(f"Executed {len(queries):_} queries (with repetitions) in {perf_counter() - t0:_.2f} seconds")

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
            "clickbench": {
                "populate": self.populate_clickbench,
                "run": self.run_clickbench,
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
