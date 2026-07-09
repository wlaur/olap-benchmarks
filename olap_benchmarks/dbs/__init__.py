from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from functools import lru_cache
from multiprocessing import Queue
from pathlib import Path
from threading import get_ident
from time import perf_counter, sleep
from typing import TYPE_CHECKING, Any, ClassVar, Literal, get_args

import polars as pl
from pydantic import BaseModel, PrivateAttr
from sqlalchemy import Connection, text

from ..metrics.sampler import start_metric_sampler
from ..metrics.storage import RunStatus, Storage, WriterMessage
from ..results.hashing import build_answer_metadata
from ..run_metadata import (
    ExecutionMode,
    StepResultStatus,
    build_run_metadata,
    classify_iteration_role,
    classify_step_result_status,
)
from ..settings import (
    REPO_ROOT,
    SETTINGS,
    DatabaseName,
    Operation,
    SuiteName,
    TableName,
    format_suite_data_directory_name,
    get_suite_scale_factor,
    resolve_suite_scale_factor,
)
from ..utils import run_shell
from .utils import tracked_commit

if TYPE_CHECKING:
    from ..suites import BenchmarkSuite

_LOGGER = logging.getLogger(__name__)
LiteralStepType = Literal["phase", "query", "mutation"]


def _new_active_step_stacks() -> dict[int, list[int]]:
    return {}


class Database(BaseModel, ABC):
    name: DatabaseName
    version: str

    connection_string: str
    DISABLED_MUTATION_STEPS: ClassVar[Mapping[SuiteName, frozenset[str]]] = {}
    container_image: ClassVar[str | None] = None

    current_query_name: str | None = None
    _current_suite: SuiteName | None = None
    _current_suite_scale_factor: int | None = None

    _connection: Connection | None = None
    _result_storage: Storage | None = None
    _run_id: int | None = None
    _last_start_command: str | None = None

    _queue: Queue[WriterMessage] | None = None
    _result_queue: Queue[object] | None = None
    _active_step_ids_by_thread: dict[int, list[int]] = PrivateAttr(default_factory=_new_active_step_stacks)

    @property
    def current_suite(self) -> SuiteName:
        if self._current_suite is None:
            raise ValueError("current_suite is not set")
        return self._current_suite

    @property
    def current_suite_scale_factor(self) -> int:
        if self._current_suite is None:
            raise ValueError("current_suite is not set")
        if self._current_suite_scale_factor is None:
            raise ValueError("current_suite_scale_factor is not set")
        return resolve_suite_scale_factor(self._current_suite, self._current_suite_scale_factor)

    @property
    def database_directory(self) -> Path:
        directory = (
            SETTINGS.database_directory
            / self.name
            / format_suite_data_directory_name(self.current_suite, self.current_suite_scale_factor)
        )
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
    def active_step_id(self) -> int | None:
        stack = self._active_step_ids_by_thread.get(get_ident())
        if not stack:
            return None
        return stack[-1]

    @property
    @abstractmethod
    def start(self) -> str | None: ...

    @property
    def execution_mode(self) -> ExecutionMode:
        if not self.container_images:
            return "in_process"
        return "container"

    @property
    def container_images(self) -> Mapping[str, str]:
        if self.container_image is None:
            return {}
        return {self.name: self.container_image}

    @property
    def metric_container_names(self) -> tuple[str, ...]:
        if not self.container_images:
            return ()
        return (f"{self.name}-benchmark",)

    @property
    def start_commands(self) -> tuple[str, ...]:
        command = self.start
        return () if command is None else (command,)

    @property
    def stop_commands(self) -> tuple[str, ...]:
        command = self.stop
        return () if command is None else (command,)

    @property
    def restart_commands(self) -> tuple[str, ...]:
        command = self.restart
        return () if command is None else (command,)

    def docker_run_command(
        self,
        image: str,
        ports: Mapping[str, str] | None = None,
        mounts: Mapping[str, str] | None = None,
        env: Mapping[str, str] | None = None,
        args: Sequence[str] = (),
        platform: str | None = None,
        name: str | None = None,
        network: str | None = None,
        ip: str | None = None,
    ) -> str:
        parts = ["docker run"]
        if platform is not None:
            parts.extend(["--platform", platform])
        parts.append(f"--name {name or f'{self.name}-benchmark'} --rm -d")
        if network is not None:
            parts.extend(["--network", network])
        if ip is not None:
            parts.extend(["--ip", ip])
        parts.extend(f"-p {host}:{container}" for host, container in (ports or {}).items())
        parts.extend(f"-v {src}:{dst}" for src, dst in (mounts or {}).items())
        parts.extend(f"-e {key}={value}" for key, value in (env or {}).items())
        parts.append(image)
        parts.extend(args)
        return " ".join(parts)

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
        result_status: StepResultStatus | None = None,
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
            result_status=result_status,
            iteration_role=classify_iteration_role(iteration) if step_type in ("query", "mutation") else None,
            metadata=metadata,
        )

    def _finish_step(
        self,
        step_id: int,
        status: RunStatus,
        step_type: LiteralStepType,
        row_count: int | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        result_status: StepResultStatus | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.result_storage.finish_step(
            step_id=step_id,
            finished_at=datetime.now(UTC).replace(tzinfo=None),
            status=status,
            result_status=result_status
            if result_status is not None
            else classify_step_result_status(step_type=step_type, status=status, error_type=error_type),
            row_count=row_count,
            error_type=error_type,
            error_message=error_message,
            metadata=metadata,
        )

    def _push_active_step(self, step_id: int) -> None:
        thread_id = get_ident()
        stack = self._active_step_ids_by_thread.get(thread_id)
        if stack is None:
            stack = []
            self._active_step_ids_by_thread[thread_id] = stack
        stack.append(step_id)

    def _pop_active_step(self, step_id: int) -> None:
        thread_id = get_ident()
        stack = self._active_step_ids_by_thread.get(thread_id)
        if not stack:
            return

        if stack[-1] == step_id:
            stack.pop()
            if not stack:
                del self._active_step_ids_by_thread[thread_id]
            return

        remaining = [active_step_id for active_step_id in stack if active_step_id != step_id]
        if remaining:
            self._active_step_ids_by_thread[thread_id] = remaining
        else:
            del self._active_step_ids_by_thread[thread_id]

    def bind_query_recorder(self, connection: Connection) -> Connection:
        connection.info["olap_query_recorder"] = self.record_query_execution
        return connection

    def execute(self, statement: str, commit: bool = True, autocommit: bool = False, reconnect: bool = False) -> None:
        """Execute a single statement with query-execution recording.

        `autocommit` reconnects and runs the statement outside a transaction
        (needed for e.g. VACUUM). `commit` is ignored in that case.
        """
        con = self.connect(reconnect=reconnect or autocommit)

        if autocommit:
            con = con.execution_options(isolation_level="AUTOCOMMIT")

        with self.record_query_execution(statement):
            con.execute(text(statement))

        if commit and not autocommit:
            tracked_commit(con)

    @contextmanager
    def record_query_execution(self, query: str) -> Generator[None]:
        stripped_query = query.strip()

        if not stripped_query or self._result_storage is None or self._run_id is None:
            yield
            return

        started_at = datetime.now(UTC).replace(tzinfo=None)

        try:
            yield
        finally:
            finished_at = datetime.now(UTC).replace(tzinfo=None)
            self.result_storage.insert_query_execution(
                run_id=self.run_id,
                run_step_id=self.active_step_id,
                query=stripped_query,
                start_time=started_at,
                end_time=finished_at,
            )

    @contextmanager
    def phase_context(self, phase_name: str, table_name: str | None = None) -> Generator[None]:
        step_id = self._start_step("phase", phase_name, table_name=table_name)
        self._push_active_step(step_id)

        try:
            yield
        except BaseException as exc:
            self._finish_step(
                step_id=step_id,
                status="failed",
                step_type="phase",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise
        finally:
            self._pop_active_step(step_id)

        self._finish_step(step_id=step_id, status="completed", step_type="phase")

    @contextmanager
    def query_context(self, query_name: str) -> Generator[None]:
        self.current_query_name = query_name

        try:
            yield
        finally:
            self.current_query_name = None

    def _finish_timed_step(
        self,
        step_id: int,
        status: RunStatus,
        step_type: LiteralStepType,
        row_count: int | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        duration_ms: float | None = None,
        result_status: StepResultStatus | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        step_metadata = dict(metadata or {})
        if duration_ms is not None:
            step_metadata["duration_ms"] = duration_ms

        self._finish_step(
            step_id=step_id,
            status=status,
            step_type=step_type,
            row_count=row_count,
            error_type=error_type,
            error_message=error_message,
            result_status=result_status,
            metadata=step_metadata or None,
        )

    def is_mutation_step_enabled(self, suite: SuiteName, step_name: str) -> bool:
        return step_name not in self.DISABLED_MUTATION_STEPS.get(suite, frozenset())

    def get_runtime_version(self) -> str | None:
        return None

    def is_runtime_version_expected(self, runtime_version: str) -> bool:
        return self.version in runtime_version

    def verify_runtime_version(self) -> None:
        runtime_version = self.get_runtime_version()
        if runtime_version is None:
            return

        if not self.is_runtime_version_expected(runtime_version):
            raise RuntimeError(
                f"{self.name} runtime version does not match pinned version: "
                f"expected {self.version!r}, got {runtime_version!r}"
            )

    @contextmanager
    def mutation_context(
        self,
        query_name: str,
        iteration: int,
        table_name: str | None = None,
    ) -> Generator[None]:
        step_id = self._start_step(
            "mutation", "mutation", query_name=query_name, iteration=iteration, table_name=table_name
        )
        self._push_active_step(step_id)
        try:
            t0 = perf_counter()
            yield
            duration_seconds = perf_counter() - t0
        except BaseException as exc:
            self._finish_timed_step(
                step_id=step_id,
                status="failed",
                step_type="mutation",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise
        finally:
            self._pop_active_step(step_id)

        self._finish_timed_step(
            step_id=step_id,
            status="completed",
            step_type="mutation",
            duration_ms=1_000 * duration_seconds,
        )

    def record_skipped_mutation_step(self, query_name: str, iteration: int, table_name: str, reason: str) -> None:
        metadata = {"skip_reason": reason, "duration_ms": 0.0}
        step_id = self._start_step(
            "mutation",
            "mutation",
            query_name=query_name,
            iteration=iteration,
            table_name=table_name,
            result_status="skipped",
            metadata=metadata,
        )
        self._finish_step(
            step_id=step_id,
            status="completed",
            step_type="mutation",
            result_status="skipped",
            metadata=metadata,
        )

    def record_skipped_query_step(
        self,
        query_name: str,
        iteration: int,
        reason: str,
        result_status: Literal["skipped", "unsupported"] = "skipped",
    ) -> None:
        metadata = {"skip_reason": reason, "duration_ms": 0.0}
        step_id = self._start_step(
            "query",
            "query",
            query_name=query_name,
            iteration=iteration,
            result_status=result_status,
            metadata=metadata,
        )
        self._finish_step(
            step_id=step_id,
            status="completed",
            step_type="query",
            result_status=result_status,
            metadata=metadata,
        )

    def execute_query_iteration(
        self,
        query_name: str,
        iteration: int,
        query: str,
        fetch_kwargs: Mapping[str, Any] | None = None,
    ) -> tuple[pl.DataFrame, float]:
        step_id = self._start_step("query", "query", query_name=query_name, iteration=iteration)
        self._push_active_step(step_id)

        kwargs = dict(fetch_kwargs or {})

        try:
            t0 = perf_counter()
            df = self.fetch(query, **kwargs)
            duration_seconds = perf_counter() - t0
        except BaseException as exc:
            self._finish_timed_step(
                step_id=step_id,
                status="failed",
                step_type="query",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise
        finally:
            self._pop_active_step(step_id)

        self._finish_timed_step(
            step_id=step_id,
            status="completed",
            step_type="query",
            row_count=df.shape[0],
            duration_ms=1_000 * duration_seconds,
            metadata=build_answer_metadata(df),
        )

        return df, duration_seconds

    def restart_event(self) -> None:
        commands = self.restart_commands
        if not commands:
            return

        with self.phase_context("restart"):
            _LOGGER.info(f"Restarting service {self.name}")
            for command in commands:
                rc = run_shell(command)
                if rc != 0:
                    raise RuntimeError(f"Restart command for {self.name} exited with code {rc}: {command}")
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
            self.execute(stmt, reconnect=True)

    def initialize_schema(self, suite_directory: str) -> None:
        fpath = REPO_ROOT / f"olap_benchmarks/suites/{suite_directory}/schemas/{self.name}.sql"

        if not fpath.is_file():
            _LOGGER.info(f"Schema definition for {self.name}:{suite_directory} does not exist, skipping...")
            return

        with self.phase_context("schema"):
            self.execute_schema_file(fpath)

        self.connect(reconnect=True)
        _LOGGER.info(f"Initialized schema for {self.name}:{suite_directory}")

    @abstractmethod
    def connect(self, reconnect: bool = False) -> Connection: ...

    def rollback(self) -> None:
        if self._connection is None:
            return
        self._connection.rollback()

    def wait_until_accessible(self, timeout_seconds: float = 300.0, interval_seconds: float = 1.0) -> None:
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

    @abstractmethod
    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None: ...

    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        from ..suites.clickbench.config import Clickbench
        from ..suites.kaggle_airbnb.config import KaggleAirbnb
        from ..suites.rtabench.config import RTABench
        from ..suites.time_series.config import TimeSeries
        from ..suites.tpc_ds.config import TpcDs
        from ..suites.tpc_h.config import TpcH

        return {
            "rtabench": RTABench,
            "clickbench": Clickbench,
            "time_series": TimeSeries,
            "kaggle_airbnb": KaggleAirbnb,
            "tpc_h": TpcH,
            "tpc_ds": TpcDs,
        }

    @property
    def benchmarks(self) -> dict[SuiteName, BenchmarkSuite[Any]]:
        return {
            suite_name: suite_class(
                db=self,
                name=suite_name,
                scale_factor=(
                    self.current_suite_scale_factor
                    if self._current_suite == suite_name
                    else get_suite_scale_factor(suite_name)
                ),
            )
            for suite_name, suite_class in self.suite_registry().items()
        }

    def benchmark(self, suite: SuiteName, operation: Operation, scale_factor: int | None = None) -> None:
        self._current_suite = suite
        self._current_suite_scale_factor = resolve_suite_scale_factor(suite, scale_factor)
        self._active_step_ids_by_thread = {}
        benchmark = self.benchmarks.get(suite)

        if benchmark is None:
            raise ValueError(f"Invalid benchmark suite: '{suite}'")

        if operation not in benchmark.supported_operations:
            supported_operations = ", ".join(benchmark.supported_operations)
            raise ValueError(
                f"{type(benchmark).__name__} does not support operation '{operation}'. "
                f"Supported operations: {supported_operations}"
            )

        match operation:
            case "populate":
                benchmark_func = benchmark.populate
            case "select":
                benchmark_func = benchmark.select
            case "mutate":
                benchmark_func = benchmark.mutate
            case "concurrent":
                benchmark_func = benchmark.concurrent
            case _:
                raise ValueError(f"Invalid operation '{operation}'")

        if operation == "populate" and not benchmark.should_populate():
            _LOGGER.info(
                f"Skipping populate run recording for {suite} scale factor {benchmark.scale_factor} on {self.name}"
            )
            return

        self.verify_runtime_version()

        self._result_storage = self.create_result_storage()

        started_at = datetime.now(UTC).replace(tzinfo=None)
        self._run_id = self.result_storage.insert_run(
            suite=suite,
            suite_scale_factor=benchmark.scale_factor,
            db=self.name,
            db_version=self.version,
            operation=operation,
            system=SETTINGS.system,
            started_at=started_at,
            metadata=build_run_metadata(
                execution_mode=self.execution_mode,
                container_image=self.container_image,
                container_images=self.container_images,
                start_command=self._last_start_command,
            ),
        )

        metric_process, stop_event = start_metric_sampler(
            db=self.name,
            suite=suite,
            suite_scale_factor=benchmark.scale_factor,
            container_names=self.metric_container_names,
            run_id=self.run_id,
            storage=self.result_storage,
            interval_seconds=None,  # docker stats takes ~1 sec, no need to wait here
        )

        status: RunStatus = "completed"
        error_type: str | None = None
        error_message: str | None = None

        t0 = perf_counter()

        _LOGGER.info(
            f"Starting benchmark run {self.run_id} "
            f"(database: {self.name}, suite: {suite}, scale_factor: {benchmark.scale_factor}, "
            f"operation: {operation})"
        )

        try:
            with self.phase_context(operation):
                benchmark_func()
        except BaseException as exc:
            status = "failed"
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


@lru_cache(maxsize=1)
def get_databases() -> dict[DatabaseName, Database]:
    from .clickhouse import Clickhouse
    from .doris import Doris
    from .duckdb import DuckDB
    from .monetdb import MonetDB
    from .polars import Polars
    from .postgres import Postgres
    from .questdb import QuestDB
    from .starrocks import StarRocks
    from .timescaledb import TimescaleDB

    databases: dict[DatabaseName, Database] = {
        "monetdb": MonetDB(),
        "clickhouse": Clickhouse(),
        "timescaledb": TimescaleDB(),
        "duckdb": DuckDB(),
        "polars": Polars(),
        "questdb": QuestDB(),
        "postgres": Postgres(),
        "starrocks": StarRocks(),
        "doris": Doris(),
    }

    assert set(databases) == set(get_args(DatabaseName))
    return databases
