import logging
from collections.abc import Mapping
from dataclasses import dataclass
from os import getpid
from pathlib import Path
from time import monotonic, sleep
from typing import Any, ClassVar
from urllib.parse import urlencode

import polars as pl
from pydantic import Field
from sqlalchemy import Connection, create_engine, text

from ...settings import (
    SETTINGS,
    DatabaseName,
    Operation,
    SuiteName,
    TableName,
    format_suite_data_directory_name,
    host_port,
)
from ...suites import BenchmarkSuite
from ...suites.chat_threads.config import ChatThreads
from ...suites.time_series.config import TimeSeries
from .. import Database, ParquetEpochColumns
from ..utils import tracked_commit
from .adbc import (
    delete_adbc,
    fetch_adbc,
    insert_adbc,
    insert_parquet_adbc,
    upsert_adbc,
)
from .settings import SETTINGS as MONETDB_SETTINGS

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MonetDBRelease:
    label: str
    runtime_version: str
    arm64_image_revision: int

    @property
    def amd64_container_image(self) -> str:
        return f"monetdb/monetdb:{self.label}"

    @property
    def arm64_container_image(self) -> str:
        return f"wlaur/monetdb-container:{self.runtime_version}-{self.arm64_image_revision}"


MONETDB_RELEASE = MonetDBRelease(label="Dec2025-SP3", runtime_version="11.55.7", arm64_image_revision=2)
MONETDB_APPLICATION = "olap-benchmarks"
MONETDB_CONTAINER_PORT = "50000"
MONETDB_HOST_PORT = host_port("monetdb")

MONETDB_CONNECTION_STRING = f"monetdb+adbc://monetdb:monetdb@localhost:{MONETDB_HOST_PORT}/benchmark"
MONETDB_OBSERVER_URI = f"{MONETDB_CONNECTION_STRING}?client_application=olap-benchmarks-observer"


def _monetdb_connection_string() -> str:
    parameters = {"client_application": MONETDB_APPLICATION}
    if MONETDB_SETTINGS.write_window_bytes is not None:
        parameters["write_window_bytes"] = str(MONETDB_SETTINGS.write_window_bytes)
    if MONETDB_SETTINGS.wire_compression != "auto":
        parameters["wire_compression"] = MONETDB_SETTINGS.wire_compression
    if MONETDB_SETTINGS.constrained_append != "auto":
        parameters["constrained_append"] = MONETDB_SETTINGS.constrained_append
    return f"{MONETDB_CONNECTION_STRING}?{urlencode(parameters)}"


_ROLLING_AVG_WINDOW_BUG = (
    "MonetDB 11.55.7 returns wrong values from avg() over a ROWS frame wider than 16 rows, "
    "from the 17th row onward; see MONETDB_ISSUE.md. A 60-row moving average cannot be expressed "
    "correctly, so the query is not measured rather than publishing a runtime for a wrong answer."
)


class MonetDBChatThreads(ChatThreads["MonetDB"]):
    # The concurrent workload terminates mserver5 mid-query: several readers plus an ADBC writer
    # against the wide JSON column, three attempts out of three, on MonetDB 11.55.7 with
    # adbc-driver-monetdb 0.12.0. Clients see "IO: unexpected end of file" and the container is
    # gone afterwards. Nothing on our side fixes it, and retrying only costs the run 11 minutes.
    # populate, select and mutate all complete, so the rest of the suite is still measured.
    # See MONETDB_ISSUE.md.
    supported_operations: ClassVar[tuple[Operation, ...]] = ("populate", "select", "mutate")


class MonetDBTimeSeries(TimeSeries["MonetDB"]):
    UNSUPPORTED_QUERIES: ClassVar[dict[DatabaseName, dict[str, str]]] = {
        "monetdb": {
            "large_13_rolling_avg": _ROLLING_AVG_WINDOW_BUG,
            "tall_13_rolling_avg": _ROLLING_AVG_WINDOW_BUG,
            "wide_13_rolling_avg": _ROLLING_AVG_WINDOW_BUG,
        }
    }

    def finish_parquet_table(self, table_name: TableName) -> None:
        self.db.analyze_table(table_name, ["time"])


class MonetDB(Database):
    name: DatabaseName = "monetdb"
    version: str = MONETDB_RELEASE.label
    container_image: ClassVar[str | None] = MONETDB_RELEASE.amd64_container_image
    arm64_container_image: ClassVar[str | None] = MONETDB_RELEASE.arm64_container_image
    supports_arm64_containers: ClassVar[bool] = True
    expected_runtime_version: ClassVar[str | None] = MONETDB_RELEASE.runtime_version

    @property
    def db_driver(self) -> str:
        return "adbc"

    @property
    def run_package_names(self) -> tuple[str, ...]:
        return ("olap-benchmarks", "adbc-driver-monetdb", "sqlalchemy-monetdb-adbc")

    @property
    def run_options(self) -> Mapping[str, object]:
        return {
            "client_application": MONETDB_APPLICATION,
            "session_tracking": "client_application+clientpid",
            "write_window_bytes": MONETDB_SETTINGS.write_window_bytes,
            "wire_compression": MONETDB_SETTINGS.wire_compression,
            "constrained_append": MONETDB_SETTINGS.constrained_append,
        }

    connection_string: str = Field(default_factory=_monetdb_connection_string)

    @property
    def database_directory(self) -> Path:
        directory = (
            SETTINGS.database_directory
            / self.name
            / format_suite_data_directory_name(self.current_suite, self.current_suite_scale_factor)
        )
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    @property
    def start(self) -> str:
        image = self.resolved_container_image
        if image is None:
            raise RuntimeError("MonetDB container image is not configured")

        return self.docker_run_command(
            image,
            ports={str(MONETDB_HOST_PORT): MONETDB_CONTAINER_PORT},
            mounts={self.database_directory.as_posix(): "/var/monetdb5/dbfarm"},
            env={"MDB_DB_ADMIN_PASS": "monetdb", "MDB_CREATE_DBS": "benchmark"},
        )

    def connect(self, reconnect: bool = False) -> Connection:
        if reconnect:
            self.close_connection()

        if self._connection is not None:
            return self._connection

        engine = create_engine(
            self.connection_string,
            # avoid crash "ImportError: sys.meta_path is None, Python is likely shutting down"
            # not clear why this happens
            pool_reset_on_return=None,
        )

        self._connection = self.bind_query_recorder(engine.connect())

        return self._connection

    def _session_state(self) -> tuple[int, int]:
        engine = create_engine(MONETDB_OBSERVER_URI, pool_reset_on_return=None)
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
            "chat_threads": MonetDBChatThreads,
        }
