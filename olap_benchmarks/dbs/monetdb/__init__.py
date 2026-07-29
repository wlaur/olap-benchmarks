import logging
from collections.abc import Mapping
from dataclasses import dataclass
from os import getpid
from pathlib import Path
from time import monotonic, sleep
from typing import Any, ClassVar, Literal, cast
from urllib.parse import urlencode

import polars as pl
import pymonetdb
from pydantic import Field
from sqlalchemy import Connection, create_engine, text

from ...settings import SETTINGS, DatabaseName, SuiteName, TableName, format_suite_data_directory_name
from ...suites import BenchmarkSuite
from ...suites.kaggle_airbnb.config import KaggleAirbnb
from ...suites.time_series.config import TimeSeries
from .. import Database, ParquetEpochColumns, apply_parquet_epoch_columns
from ..utils import tracked_commit
from . import insert as _insert_mod
from .adbc import (
    delete_adbc,
    fetch_adbc,
    insert_adbc,
    insert_parquet_adbc,
    upsert_adbc,
)
from .fetch import fetch_binary, fetch_pymonetdb
from .insert import (
    DEFAULT_LAZY_WRITE,
    LazyWrite,
    insert,
    staged_write_for_column_count,
    upsert,
)
from .settings import SETTINGS as MONETDB_SETTINGS
from .utils import get_pymonetdb_connection

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
MONETDB_OBSERVER_URI = "monetdb://monetdb:monetdb@localhost:50000/benchmark?client_application=olap-benchmarks-observer"

MONETDB_CONNECTION_STRINGS = {
    "staged": "monetdb+pymonetdb://monetdb:monetdb@localhost:50000/benchmark",
    "adbc": "monetdb+adbc://monetdb:monetdb@localhost:50000/benchmark",
}


def _monetdb_connection_string() -> str:
    base = MONETDB_CONNECTION_STRINGS[MONETDB_SETTINGS.driver]
    parameters = {"client_application": MONETDB_APPLICATION}
    if MONETDB_SETTINGS.driver == "staged":
        return base
    if MONETDB_SETTINGS.write_window_bytes is not None:
        parameters["write_window_bytes"] = str(MONETDB_SETTINGS.write_window_bytes)
    if MONETDB_SETTINGS.wire_compression != "auto":
        parameters["wire_compression"] = MONETDB_SETTINGS.wire_compression
    if MONETDB_SETTINGS.constrained_append != "auto":
        parameters["constrained_append"] = MONETDB_SETTINGS.constrained_append
    return f"{base}?{urlencode(parameters)}" if parameters else base


class MonetDBTimeSeries(TimeSeries["MonetDB"]):
    def finish_parquet_table(self, table_name: TableName) -> None:
        self.db.analyze_table(table_name, ["time"])

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        assert self.db.current_query_name is not None

        if MONETDB_SETTINGS.driver == "adbc":
            return {"method": "adbc"}
        if "batch_export" in self.db.current_query_name:
            return {"method": "binary"}

        return {"method": "pymonetdb"}


class MonetDBKaggleAirbnb(KaggleAirbnb["MonetDB"]):
    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        assert self.db.current_query_name is not None

        if MONETDB_SETTINGS.driver == "adbc":
            return {"method": "adbc"}
        pymonetdb_queries = [
            "01_calendar_count",
        ]

        if any(n in self.db.current_query_name for n in pymonetdb_queries):
            return {"method": "pymonetdb"}

        return {"method": "binary"}


class MonetDB(Database):
    name: DatabaseName = "monetdb"
    version: str = MONETDB_RELEASE.label
    container_image: ClassVar[str | None] = MONETDB_RELEASE.amd64_container_image
    arm64_container_image: ClassVar[str | None] = MONETDB_RELEASE.arm64_container_image
    supports_arm64_containers: ClassVar[bool] = True
    expected_runtime_version: ClassVar[str | None] = MONETDB_RELEASE.runtime_version

    @property
    def db_driver(self) -> str:
        return MONETDB_SETTINGS.driver

    @property
    def run_package_names(self) -> tuple[str, ...]:
        if MONETDB_SETTINGS.driver == "adbc":
            return ("olap-benchmarks", "adbc-driver-monetdb", "sqlalchemy-monetdb-adbc")
        return ("olap-benchmarks", "pymonetdb", "sqlalchemy-monetdb")

    @property
    def run_options(self) -> Mapping[str, object]:
        return {
            "driver": MONETDB_SETTINGS.driver,
            "client_application": MONETDB_APPLICATION if MONETDB_SETTINGS.driver == "adbc" else None,
            "session_tracking": (
                "client_application+clientpid" if MONETDB_SETTINGS.driver == "adbc" else "pymonetdb-client+clientpid"
            ),
            "client_file_transfer": MONETDB_SETTINGS.client_file_transfer,
            "default_fetch_method": (
                "adbc" if MONETDB_SETTINGS.driver == "adbc" else MONETDB_SETTINGS.default_fetch_method
            ),
            "write_window_bytes": MONETDB_SETTINGS.write_window_bytes,
            "wire_compression": MONETDB_SETTINGS.wire_compression if MONETDB_SETTINGS.driver == "adbc" else None,
            "constrained_append": (MONETDB_SETTINGS.constrained_append if MONETDB_SETTINGS.driver == "adbc" else None),
            "staged_parquet_policy": (
                "column_groups_of_10_at_512_columns_or_wider;row_batches_of_500000_otherwise"
                if MONETDB_SETTINGS.driver == "staged"
                else None
            ),
        }

    connection_string: str = Field(default_factory=_monetdb_connection_string)

    @property
    def database_directory(self) -> Path:
        directory = (
            SETTINGS.database_directory
            / self.name
            / format_suite_data_directory_name(self.current_suite, self.current_suite_scale_factor)
            / MONETDB_SETTINGS.driver
        )
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    @property
    def metric_directories(self) -> tuple[Path, ...]:
        return (self.database_directory, SETTINGS.temporary_directory / "monetdb")

    @property
    def start(self) -> str:
        (SETTINGS.temporary_directory / "monetdb/data").mkdir(exist_ok=True, parents=True)

        mounts = {self.database_directory.as_posix(): "/var/monetdb5/dbfarm"}
        if not MONETDB_SETTINGS.client_file_transfer:
            mounts[f"{SETTINGS.temporary_directory.as_posix()}/monetdb/data"] = "/data"

        image = self.resolved_container_image
        if image is None:
            raise RuntimeError("MonetDB container image is not configured")

        return self.docker_run_command(
            image,
            ports={"50000": "50000"},
            mounts=mounts,
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
        observer = pymonetdb.connect(MONETDB_OBSERVER_URI)
        try:
            cursor = cast(Any, observer.cursor())
            cursor.execute(
                "SELECT COUNT(*) FROM sys.sessions "
                f"WHERE clientpid = {getpid()} AND (application = '{MONETDB_APPLICATION}' "
                "OR (application = '-' AND client LIKE 'pymonetdb %'))"
            )
            session_row = cast(tuple[Any, ...] | None, cursor.fetchone())
            assert session_row is not None
            cursor.execute("SELECT COUNT(*) FROM sys.tables WHERE name LIKE 'adbc_ingest_stage_%'")
            staging_row = cast(tuple[Any, ...] | None, cursor.fetchone())
            assert staging_row is not None
            return int(session_row[0]), int(staging_row[0])
        finally:
            observer.close()

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
            method="adbc" if MONETDB_SETTINGS.driver == "adbc" else "pymonetdb",
        )
        return str(df.item(0, 0))

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
        method: Literal["binary", "pymonetdb", "adbc"] | None = None,
    ) -> pl.DataFrame:
        if MONETDB_SETTINGS.driver == "adbc":
            if method not in (None, "adbc"):
                raise ValueError(f"Fetch method '{method}' is unavailable with the ADBC connection")
            method = "adbc"
        else:
            method = method or MONETDB_SETTINGS.default_fetch_method

        _LOGGER.info(f"Fetching with {method=}")

        if method == "binary":
            return fetch_binary(query, self.connect(), schema)
        elif method == "pymonetdb":
            df = fetch_pymonetdb(query, self.connect())
        elif method == "adbc":
            return fetch_adbc(query, self.connect(), schema)
        else:
            raise ValueError(f"Invalid method: '{method}'")

        if schema is not None:
            df = df.cast(cast(pl.Schema, schema))

        return df

    def rollback(self) -> None:
        if self._connection is None:
            return

        super().rollback()
        if MONETDB_SETTINGS.driver == "staged":
            get_pymonetdb_connection(self._connection).rollback()

    def get_table_names(self) -> set[TableName]:
        df = self.fetch(
            "select name as table_name from sys.tables where system = false",
            schema={"table_name": pl.String},
            method="adbc" if MONETDB_SETTINGS.driver == "adbc" else "pymonetdb",
        )
        return set(df.get_column("table_name").to_list())

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
        lazy_write: LazyWrite = DEFAULT_LAZY_WRITE,
    ) -> None:
        exists = self._table_exists(table)

        try:
            if MONETDB_SETTINGS.driver == "adbc":
                return insert_adbc(
                    df,
                    table,
                    self.connect(),
                    primary_key,
                    not_null,
                    create=not exists,
                )
            return insert(
                df,
                table,
                self.connect(),
                primary_key,
                not_null,
                create=not exists,
                lazy_write=lazy_write,
            )
        except Exception as error:
            self._rollback_after_failed_ingest(error, "insert")
            raise

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        if MONETDB_SETTINGS.driver == "adbc":
            return upsert_adbc(df, table, self.connect(), primary_key=primary_key)
        return upsert(df, table, self.connect(), primary_key=primary_key)

    def insert_parquet(
        self,
        path: Path,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
        *,
        epoch_columns: ParquetEpochColumns | None = None,
    ) -> None:
        if MONETDB_SETTINGS.driver != "adbc":
            frame = apply_parquet_epoch_columns(pl.scan_parquet(path), epoch_columns)
            return self.insert(
                frame,
                table,
                primary_key=primary_key,
                not_null=not_null,
                lazy_write=staged_write_for_column_count(len(frame.collect_schema())),
            )

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
        if MONETDB_SETTINGS.driver == "adbc":
            return delete_adbc(table, self.connect(), primary_key=primary_key, keys=keys)
        return _insert_mod.delete(table, self.connect(), primary_key=primary_key, keys=keys)

    def analyze_table(self, table: TableName, columns: list[str] | None = None) -> None:
        # ANALYZE refreshes column statistics (min/max, sortedness, uniqueness)
        # used by the optimiser. After a bulk binary copy the catalog otherwise
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
            "kaggle_airbnb": MonetDBKaggleAirbnb,
        }
