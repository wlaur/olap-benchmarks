from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from multiprocessing import Queue
from pathlib import Path
from time import perf_counter, sleep
from typing import TYPE_CHECKING, Any, Literal, cast, get_args

import polars as pl
from pydantic import BaseModel
from sqlalchemy import Connection, text

from ..metrics.sampler import start_metric_sampler
from ..metrics.storage import RunStatus, Storage, WriterMessage
from ..settings import REPO_ROOT, SETTINGS, DatabaseName, Operation, SuiteName, TableName

if TYPE_CHECKING:
    from ..suites import BenchmarkSuite
    from ..suites.clickbench.config import Clickbench
    from ..suites.kaggle_airbnb.config import KaggleAirbnb
    from ..suites.rtabench.config import RTABench
    from ..suites.time_series.config import TimeSeries

_LOGGER = logging.getLogger(__name__)
LiteralStepType = Literal["phase", "query"]


def _status_from_exception(exc: BaseException) -> RunStatus:
    if isinstance(exc, KeyboardInterrupt):
        return "aborted"
    return "failed"


class QueryContext(BaseModel):
    suite: SuiteName
    query_name: str


class Database(BaseModel, ABC):
    name: DatabaseName
    version: str

    connection_string: str

    context: QueryContext | None = None
    _current_suite: SuiteName | None = None

    _connection: Connection | None = None
    _result_storage: Storage | None = None
    _run_id: int | None = None

    _queue: Queue[WriterMessage] | None = None
    _result_queue: Queue[object] | None = None

    @property
    def current_suite(self) -> SuiteName:
        if self._current_suite is None:
            raise ValueError("current_suite is not set")
        return self._current_suite

    @property
    def database_directory(self) -> Path:
        directory = SETTINGS.database_directory / self.name / self.current_suite
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def set_queues(self, queue: Queue[WriterMessage], result_queue: Queue[object]) -> None:
        self._queue = queue
        self._result_queue = result_queue

    def create_result_storage(self) -> Storage:
        if self._queue is None or self._result_queue is None:
            raise ValueError("Result queues are not set")

        return Storage(self._queue, self._result_queue)

    @property
    def result_storage(self) -> Storage:
        if self._result_storage is None:
            raise ValueError("self._result_storage is not set")

        return self._result_storage

    @property
    def run_id(self) -> int:
        if self._run_id is None:
            raise ValueError("self._run_id is not set")

        return self._run_id

    @property
    @abstractmethod
    def start(self) -> str | None: ...

    @property
    def stop(self) -> str | None:
        return f"docker stop {self.name}-benchmark"

    @property
    def restart(self) -> str | None:
        return f"docker restart {self.name}-benchmark"

    def _start_step(
        self,
        step_type: LiteralStepType,
        step_name: str,
        query_name: str | None = None,
        iteration: int | None = None,
        table_name: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        return self.result_storage.start_step(
            run_id=self.run_id,
            step_type=step_type,
            step_name=step_name,
            query_name=query_name,
            iteration=iteration,
            table_name=table_name,
            started_at=datetime.now(UTC).replace(tzinfo=None),
            metadata=metadata,
        )

    def _finish_step(
        self,
        step_id: int,
        status: RunStatus,
        row_count: int | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.result_storage.finish_step(
            step_id=step_id,
            finished_at=datetime.now(UTC).replace(tzinfo=None),
            status=status,
            row_count=row_count,
            error_type=error_type,
            error_message=error_message,
            metadata=metadata,
        )

    @contextmanager
    def phase_context(self, phase_name: str, table_name: str | None = None) -> Iterator[None]:
        step_id = self._start_step("phase", phase_name, table_name=table_name)

        try:
            yield
        except BaseException as exc:
            self._finish_step(
                step_id=step_id,
                status=_status_from_exception(exc),
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise

        self._finish_step(step_id=step_id, status="completed")

    @contextmanager
    def event_context(self, name: str) -> Iterator[None]:
        # Backwards-compatible alias for existing suite/database implementations.
        with self.phase_context(name):
            yield

    @contextmanager
    def query_context(self, suite: SuiteName, query_name: str) -> Iterator[None]:
        self.context = QueryContext(suite=suite, query_name=query_name)

        try:
            yield
        finally:
            self.context = None

    def start_query_step(self, query_name: str, iteration: int) -> int:
        return self._start_step(
            step_type="query",
            step_name="query",
            query_name=query_name,
            iteration=iteration,
        )

    def finish_query_step(
        self,
        step_id: int,
        status: RunStatus,
        row_count: int | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        duration_ms: float | None = None,
    ) -> None:
        metadata = {"duration_ms": duration_ms} if duration_ms is not None else None

        self._finish_step(
            step_id=step_id,
            status=status,
            row_count=row_count,
            error_type=error_type,
            error_message=error_message,
            metadata=metadata,
        )

    def execute_query_iteration(
        self,
        query_name: str,
        iteration: int,
        query: str,
        fetch_kwargs: Mapping[str, Any] | None = None,
    ) -> tuple[pl.DataFrame, float]:
        step_id = self.start_query_step(query_name=query_name, iteration=iteration)

        kwargs = dict(fetch_kwargs or {})

        try:
            t0 = perf_counter()
            df = self.fetch(query, **kwargs)
            duration_seconds = perf_counter() - t0
        except BaseException as exc:
            self.finish_query_step(
                step_id=step_id,
                status=_status_from_exception(exc),
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise

        self.finish_query_step(
            step_id=step_id,
            status="completed",
            row_count=df.shape[0],
            duration_ms=1_000 * duration_seconds,
        )

        return df, duration_seconds

    def restart_event(self) -> None:
        cmd = self.restart
        if cmd is None:
            return

        with self.phase_context("restart"):
            _LOGGER.info(f"Restarting service {self.name}")
            os.system(cmd)
            _LOGGER.info(f"Restarted service {self.name}")
            self.wait_until_accessible()

    def execute_schema_file(self, fpath: Path) -> None:
        with fpath.open() as f:
            statements = f.read()

        for stmt in statements.split(";"):
            stmt = stmt.strip()

            if not stmt or all(line.strip().startswith("--") for line in stmt.splitlines()):
                continue

            # ensure the connection used when initializing the schema is not reused
            # if we use e.g. alter database, it's important that subsequent queries use a new connection
            con = self.connect(reconnect=True)
            con.execute(text(stmt))
            con.commit()

    def initialize_schema(self, suite: SuiteName) -> None:
        fpath = REPO_ROOT / f"olap_benchmarks/suites/{suite}/schemas/{self.name}.sql"

        if not fpath.is_file():
            _LOGGER.info(f"Schema definition for {self.name}:{suite} does not exist, skipping...")
            return

        with self.phase_context("schema"):
            self.execute_schema_file(fpath)

        self.connect(reconnect=True)
        _LOGGER.info(f"Initialized schema for {self.name}:{suite}")

    @abstractmethod
    def connect(self, reconnect: bool = False) -> Connection: ...

    def rollback(self) -> None:
        if self._connection is None:
            return
        self._connection.rollback()

    def wait_until_accessible(self, timeout_seconds: float = 120.0, interval_seconds: float = 1.0) -> None:
        _LOGGER.info(f"Waiting for database {self.name} (timeout: {timeout_seconds:.0f}s)...")

        deadline = perf_counter() + timeout_seconds
        attempts = 0

        while perf_counter() < deadline:
            attempts += 1
            try:
                self.connect(reconnect=True)
                self.fetch("select 1")
                _LOGGER.info(f"Database {self.name} is ready (after {attempts} attempt(s))")
                return
            except NotImplementedError:
                raise
            except Exception as e:
                elapsed = perf_counter() + timeout_seconds - deadline
                _LOGGER.info(f"Database {self.name} not ready ({elapsed:.0f}s elapsed): {e}")
                sleep(interval_seconds)

        raise TimeoutError(f"Timed out after {timeout_seconds:.0f}s waiting for database {self.name}")

    @abstractmethod
    def fetch(
        self, query: str, schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None
    ) -> pl.DataFrame: ...

    def get_row_count(self, table: TableName) -> int:
        df = self.fetch(f"select count(*) as row_count from {table}", schema={"row_count": pl.Int64})

        if df.shape != (1, 1):
            raise RuntimeError(f"Expected a single row-count result for {table}, got shape={df.shape}")

        return int(df.item(0, 0))

    @abstractmethod
    def get_table_names(self) -> set[TableName]: ...

    @abstractmethod
    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None: ...

    @abstractmethod
    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None: ...

    @property
    def rtabench(self) -> RTABench[Any]:
        from ..suites.rtabench.config import RTABench

        return RTABench(db=self)

    @property
    def clickbench(self) -> Clickbench[Any]:
        from ..suites.clickbench.config import Clickbench

        return Clickbench(db=self)

    @property
    def time_series(self) -> TimeSeries[Any]:
        from ..suites.time_series.config import TimeSeries

        return TimeSeries(db=self)

    @property
    def kaggle_airbnb(self) -> KaggleAirbnb[Any]:
        from ..suites.kaggle_airbnb.config import KaggleAirbnb

        return KaggleAirbnb(db=self)

    @property
    def benchmarks(self) -> dict[SuiteName, BenchmarkSuite[Any]]:
        return cast(
            "dict[SuiteName, BenchmarkSuite[Any]]",
            {suite_name: getattr(self, suite_name) for suite_name in get_args(SuiteName)},
        )

    def benchmark(self, suite: SuiteName, operation: Operation) -> None:
        self._current_suite = suite
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

        started_at = datetime.now(UTC).replace(tzinfo=None)
        self._run_id = self.result_storage.insert_run(
            suite=suite,
            db=self.name,
            db_version=self.version,
            operation=operation,
            system=SETTINGS.system,
            started_at=started_at,
        )

        metric_process, stop_event = start_metric_sampler(
            db=self.name,
            suite=suite,
            run_id=self.run_id,
            storage=self.result_storage,
            interval_seconds=None,  # docker stats takes ~1 sec, no need to wait here
        )

        status: RunStatus = "completed"
        error_type: str | None = None
        error_message: str | None = None

        t0 = perf_counter()

        _LOGGER.info(
            f"Starting benchmark run {self.run_id} (database: {self.name}, suite: {suite}, operation: {operation})"
        )

        try:
            with self.phase_context(operation):
                benchmark_func()
        except BaseException as exc:
            status = _status_from_exception(exc)
            error_type = type(exc).__name__
            error_message = str(exc)
            raise
        finally:
            stop_event.set()
            metric_process.join()

            finished_at = datetime.now(UTC).replace(tzinfo=None)
            self.result_storage.finish_run(
                run_id=self.run_id,
                finished_at=finished_at,
                status=status,
                error_type=error_type,
                error_message=error_message,
            )

        _LOGGER.info(f"Finished benchmark run {self.run_id} with status={status} in {perf_counter() - t0:_.2f} seconds")
