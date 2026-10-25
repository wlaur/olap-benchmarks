import logging
from collections.abc import Mapping
from os import getpid
from pathlib import Path
from time import monotonic, sleep
from typing import Any, ClassVar
from urllib.parse import urlencode

import polars as pl
from pydantic import Field
from sqlalchemy import Connection, create_engine, text

from ...settings import (
    DatabaseName,
    SuiteName,
    TableName,
    effective_settings,
    format_suite_data_directory_name,
    get_settings,
    host_port,
)
from ...suites import BenchmarkSuite
from ...suites.chat_threads.config import ChatThreads
from ...suites.time_series.config import TimeSeries
from ...suites.time_series_metrics.config import TimeSeriesMetrics
from ...suites.time_series_ranges.config import TimeSeriesRanges
from .. import Database, ParquetEpochColumns
from ..utils import tracked_commit
from .adbc import (
    delete_adbc,
    fetch_adbc,
    insert_adbc,
    insert_parquet_adbc,
    upsert_adbc,
)
from .settings import get_monetdb_settings

_LOGGER = logging.getLogger(__name__)


MONETDB_APPLICATION = "olap-benchmarks"
MONETDB_CONTAINER_PORT = "50000"
MONETDB_RUNTIME_ENVIRONMENT_NAMES = ("gdk_nr_threads", "gdk_mem_maxsize", "gdk_vm_maxsize", "monet_version")
MONETDB_RUNTIME_ENVIRONMENT_SQL = (
    "SELECT name, value FROM sys.env() WHERE name IN ("
    + ", ".join(f"'{name}'" for name in MONETDB_RUNTIME_ENVIRONMENT_NAMES)
    + ")"
)
MONETDB_RUNTIME_SESSION_SQL = (
    "SELECT s.optimizer, s.workerlimit, s.memorylimit, s.querytimeout, s.sessiontimeout, "
    "u.max_workers AS user_max_workers, u.max_memory AS user_max_memory "
    "FROM sys.sessions s JOIN sys.users u ON u.name = s.username "
    "WHERE s.sessionid = sys.current_sessionid()"
)


def _monetdb_base_uri() -> str:
    return f"monetdb+adbc://monetdb:monetdb@localhost:{host_port('monetdb')}/benchmark"


def _monetdb_observer_uri() -> str:
    return f"{_monetdb_base_uri()}?client_application=olap-benchmarks-observer"


def _monetdb_connection_string() -> str:
    settings = get_monetdb_settings()
    parameters = {"client_application": MONETDB_APPLICATION}
    if settings.write_window_bytes is not None:
        parameters["write_window_bytes"] = str(settings.write_window_bytes)
    if settings.wire_compression != "auto":
        parameters["wire_compression"] = settings.wire_compression
    if settings.constrained_append != "auto":
        parameters["constrained_append"] = settings.constrained_append
    return f"{_monetdb_base_uri()}?{urlencode(parameters)}"


class MonetDBTimeSeries(TimeSeries["MonetDB"]):
    def finish_parquet_table(self, table_name: TableName) -> None:
        self.db.analyze_table(table_name, ["time"])


class MonetDBTimeSeriesMetrics(TimeSeriesMetrics["MonetDB"]):
    def finish_populate(self) -> None:
        for table in self.expected_table_row_counts():
            self.db.analyze_table(table, ["metric_name", "time"] if table.startswith("eav") else ["time"])


class MonetDBTimeSeriesRanges(TimeSeriesRanges["MonetDB"], MonetDBTimeSeriesMetrics):
    pass


class MonetDB(Database):
    name: DatabaseName = "monetdb"
    supports_arm64_containers: ClassVar[bool] = True
    wal_path_patterns: ClassVar[tuple[str, ...]] = ("benchmark/sql_logs",)

    @property
    def db_driver(self) -> str:
        return "adbc"

    @property
    def run_package_names(self) -> tuple[str, ...]:
        return ("olap-benchmarks", "adbc-driver-monetdb", "sqlalchemy-monetdb-adbc")

    @property
    def run_options(self) -> Mapping[str, object]:
        return {
            **super().run_options,
            "client_application": MONETDB_APPLICATION,
            "session_tracking": "client_application+clientpid",
            "native_query_timeout_seconds": self._select_query_timeout_seconds,
        }

    @property
    def _select_query_timeout_seconds(self) -> int | None:
        return get_settings().query_timeout_seconds if self._current_operation == "select" else None

    @property
    def benchmark_settings(self) -> Mapping[str, object]:
        return {**super().benchmark_settings, **effective_settings(get_monetdb_settings())}

    connection_string: str = Field(default_factory=_monetdb_connection_string)

    @property
    def database_directory(self) -> Path:
        directory = (
            get_settings().database_directory
            / self.name
            / format_suite_data_directory_name(self.current_suite, self.current_suite_scale_factor)
        )
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    @property
    def start(self) -> str:
        return self.docker_run_command(
            self.container_images["server"],
            ports={str(host_port("monetdb")): MONETDB_CONTAINER_PORT},
            mounts={self.database_directory.as_posix(): "/var/monetdb5/dbfarm"},
            env={"MDB_DB_ADMIN_PASS": "monetdb", "MDB_CREATE_DBS": "benchmark"},
        )

    def connect(self, reconnect: bool = False) -> Connection:
        if reconnect:
            self.close_connection()

        if self._connection is None:
            engine = create_engine(
                self.connection_string,
                # avoid crash "ImportError: sys.meta_path is None, Python is likely shutting down"
                # not clear why this happens
                pool_reset_on_return=None,
            )
            try:
                self._connection = engine.connect()
            except BaseException:
                engine.dispose()
                raise

        try:
            connection = self.bind_query_recorder(self._connection)
            timeout = self._select_query_timeout_seconds
            if connection.info.get("olap_query_timeout_seconds") != timeout:
                connection.execute(text(f"CALL sys.setquerytimeout({timeout or 0})")).close()
                connection.info["olap_query_timeout_seconds"] = timeout
        except BaseException:
            self.close_connection()
            raise

        return connection

    def _session_state(self) -> tuple[int, int]:
        engine = create_engine(_monetdb_observer_uri(), pool_reset_on_return=None)
        try:
            with engine.connect() as observer:
                session_count = observer.execute(
                    text(
                        "SELECT COUNT(*) FROM sys.sessions "
                        f"WHERE clientpid = {getpid()} AND application = '{MONETDB_APPLICATION}'"
                    )
                ).scalar_one()
                staging_count = observer.execute(
                    text("SELECT COUNT(*) FROM sys.tables WHERE name LIKE 'adbc_ingest_stage_%'")
                ).scalar_one()
                return int(session_count), int(staging_count)
        finally:
            engine.dispose()

    def _wait_for_session_state(self, expected_sessions: int, timeout_seconds: float = 5.0) -> tuple[int, int]:
        deadline = monotonic() + timeout_seconds
        state = self._session_state()
        while state != (expected_sessions, 0) and monotonic() < deadline:
            sleep(0.05)
            state = self._session_state()
        if state != (expected_sessions, 0):
            raise RuntimeError(
                "MonetDB operation did not return to its session baseline: "
                f"expected tagged_sessions={expected_sessions}, temporary_ingest_tables=0; "
                f"observed tagged_sessions={state[0]}, temporary_ingest_tables={state[1]}"
            )
        return state

    def _record_session_baseline(self, step_name: str, expected_sessions: int) -> None:
        step_id = self._start_step("phase", step_name)
        try:
            sessions, temporary_tables = self._wait_for_session_state(expected_sessions)
        except BaseException as exc:
            self._finish_step(
                step_id,
                status="failed",
                step_type="phase",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise
        self._finish_step(
            step_id,
            status="completed",
            step_type="phase",
            row_count=sessions,
            metadata={
                "tagged_sessions": sessions,
                "temporary_ingest_tables": temporary_tables,
            },
        )

    def before_benchmark_operation(self) -> None:
        self._record_session_baseline("session_baseline_before", expected_sessions=1)
        self._record_runtime_settings()

    def _record_runtime_settings(self) -> None:
        step_id = self._start_step("phase", "monetdb_runtime_settings")
        try:
            environment = {
                str(name): str(value) for name, value in self.fetch(MONETDB_RUNTIME_ENVIRONMENT_SQL).iter_rows()
            }
            missing = set(MONETDB_RUNTIME_ENVIRONMENT_NAMES) - environment.keys()
            if missing:
                raise RuntimeError(f"MonetDB runtime environment is missing settings: {', '.join(sorted(missing))}")
            session = self.fetch(MONETDB_RUNTIME_SESSION_SQL)
            if session.height != 1:
                raise RuntimeError(f"Expected one current MonetDB session, observed {session.height}")
            timeout = self._select_query_timeout_seconds
            if timeout is not None and session.item(0, "querytimeout") != timeout:
                raise RuntimeError(
                    f"MonetDB query timeout mismatch: requested {timeout} seconds, "
                    f"observed {session.item(0, 'querytimeout')} seconds"
                )
        except BaseException as exc:
            self._finish_step(
                step_id,
                status="failed",
                step_type="phase",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise
        self._finish_step(
            step_id,
            status="completed",
            step_type="phase",
            metadata={
                "environment": environment,
                "session": session.row(0, named=True),
                "session_units": {"querytimeout": "seconds"},
            },
        )

    def after_benchmark_operation(self) -> None:
        self.close_connection()
        self._record_session_baseline("session_baseline_after", expected_sessions=0)

    def get_runtime_version(self) -> str:
        df = self.fetch(
            "select value as version from sys.env() where name = 'monet_version'",
            schema={"version": pl.String},
        )
        return str(df.item(0, 0))

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
    ) -> pl.DataFrame:
        return fetch_adbc(query, self.connect(), schema)

    def get_table_names(self) -> set[TableName]:
        df = self.fetch(
            "select name as table_name from sys.tables where system = false",
            schema={"table_name": pl.String},
        )
        return set(df.get_column("table_name").to_list())

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        exists = self._table_exists(table)

        try:
            insert_adbc(
                df,
                table,
                self.connect(),
                primary_key,
                not_null,
                create=not exists,
            )
        except Exception as error:
            self._rollback_after_failed_ingest(error, "insert")
            raise

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        upsert_adbc(df, table, self.connect(), primary_key=primary_key)

    def insert_parquet(
        self,
        path: Path,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
        *,
        epoch_columns: ParquetEpochColumns | None = None,
    ) -> None:
        exists = self._table_exists(table)

        try:
            insert_parquet_adbc(
                path,
                table,
                self.connect(),
                primary_key,
                not_null,
                create=not exists,
                epoch_columns=epoch_columns,
            )
        except Exception as error:
            self._rollback_after_failed_ingest(error, "Parquet insert")
            raise

    def _rollback_after_failed_ingest(self, error: Exception, operation: str) -> None:
        try:
            self.rollback()
        except Exception as rollback_error:
            error.add_note(f"rollback after failed {operation} also failed: {rollback_error}")

    def _table_exists(self, table: TableName) -> bool:
        statement = "SELECT count(*) FROM sys.tables WHERE name = :table_name"
        with self.record_query_execution(statement):
            result = self.connect().execute(text(statement), {"table_name": table})
        return bool(result.scalar())

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        delete_adbc(table, self.connect(), primary_key=primary_key, keys=keys)

    def analyze_table(self, table: TableName, columns: list[str] | None = None) -> None:
        # ANALYZE refreshes column statistics (min/max, sortedness, uniqueness)
        # used by the optimiser. After a bulk ingest the catalog otherwise
        # reports defaults, which silently disables some predicate-pushdown
        # optimisations on time-bounded queries. MonetDB's ANALYZE always
        # requires a schema-qualified name -- unqualified is parsed as
        # ANALYZE <schema>.
        #
        # On wide tables (1500+ columns) analysing every column is expensive
        # (~3 min for data_large) and most of those columns hold random
        # process values that don't benefit query planning. Passing a column
        # list restricts the work to the columns that actually drive plans
        # (typically the time column).
        cols_clause = " (" + ", ".join(f'"{c}"' for c in columns) + ")" if columns else ""
        statement = f'ANALYZE sys."{table}"{cols_clause}'
        con = self.connect(reconnect=True)
        with self.record_query_execution(statement):
            con.execute(text(statement))
        tracked_commit(con)
        _LOGGER.info(f"Analyzed table {table}{' columns ' + ', '.join(columns) if columns else ''}")

    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        return {
            **super().suite_registry(),
            "time_series": MonetDBTimeSeries,
            "time_series_metrics": MonetDBTimeSeriesMetrics,
            "time_series_ranges": MonetDBTimeSeriesRanges,
            "chat_threads": ChatThreads,
        }
