import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, ClassVar, Literal, cast

import polars as pl
from sqlalchemy import Connection, create_engine, text

from ...settings import SETTINGS, DatabaseName, SuiteName, TableName
from ...suites import BenchmarkSuite
from ...suites.kaggle_airbnb.config import KaggleAirbnb
from ...suites.time_series.config import TimeSeries, get_time_series_input_files
from .. import Database
from ..utils import tracked_commit
from . import insert as _insert_mod
from .adbc import fetch_adbc, insert_adbc, upsert_adbc
from .fetch import fetch_binary, fetch_pymonetdb
from .insert import (
    DEFAULT_LAZY_WRITE,
    ColumnGroupWrite,
    LazyWrite,
    MonetDBInsertKwargs,
    insert,
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

MONETDB_CONNECTION_STRING = "monetdb://monetdb:monetdb@localhost:50000/benchmark"


class MonetDBTimeSeries(TimeSeries["MonetDB"]):
    def insert_table(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table_name: TableName,
        primary_key: str | list[str] | None,
        not_null: str | list[str] | None,
    ) -> None:
        kwargs: MonetDBInsertKwargs = {"lazy_write": ColumnGroupWrite(group_size=10)}
        self.db.insert(df, table_name, primary_key=primary_key, not_null=not_null, **kwargs)

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        assert self.db.current_query_name is not None

        if "batch_export" in self.db.current_query_name:
            return {"method": "binary"}

        return {"method": "pymonetdb"}

    def populate(self, restart: bool = True) -> None:
        # Skip the parent class's restart so we can slot ANALYZE in between
        # the inserts and the final restart event.
        super().populate(restart=False)

        # MonetDB does not auto-collect column statistics on the load path; the
        # optimizer falls back to defaults until ANALYZE has been run, which
        # leaves predicate pushdown on the time column off the table.
        # We only analyse the time column -- value columns hold random
        # process readings whose stats don't help any of the benchmark queries
        # and a full ANALYZE on the 1500-column wide tables would otherwise
        # add ~3 min for ~250 ms of total query speed-up.
        with self.db.phase_context("analyze"):
            for table_name in get_time_series_input_files(self.scale_factor):
                self.db.analyze_table(table_name, columns=["time"])

        if restart:
            self.db.restart_event()


class MonetDBKaggleAirbnb(KaggleAirbnb["MonetDB"]):
    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        assert self.db.current_query_name is not None

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

    connection_string: str = MONETDB_CONNECTION_STRING

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

    def get_runtime_version(self) -> str:
        df = self.fetch(
            "select value as version from sys.env() where name = 'monet_version'",
            schema={"version": pl.String},
            method="adbc",
        )
        return str(df.item(0, 0))

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
        method: Literal["adbc", "binary", "pymonetdb"] | None = None,
    ) -> pl.DataFrame:
        method = method or MONETDB_SETTINGS.default_fetch_method

        _LOGGER.info(f"Fetching with {method=}")

        if method == "adbc":
            return fetch_adbc(query, self.connect(), schema)
        elif method == "binary":
            return fetch_binary(query, self.connect(), schema)
        elif method == "pymonetdb":
            df = fetch_pymonetdb(query, self.connect())
        else:
            raise ValueError(f"Invalid method: '{method}'")

        if schema is not None:
            df = df.cast(cast(pl.Schema, schema))

        return df

    def rollback(self) -> None:
        if self._connection is None:
            return

        super().rollback()
        if MONETDB_SETTINGS.write_method == "staged":
            get_pymonetdb_connection(self._connection).rollback()

    def get_table_names(self) -> set[TableName]:
        df = self.fetch(
            "select name as table_name from sys.tables where system = false",
            schema={"table_name": pl.String},
            method="adbc",
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
        statement = f"SELECT count(*) FROM sys.tables WHERE name = '{table}'"
        with self.record_query_execution(statement):
            result = self.connect().execute(
                text("SELECT count(*) FROM sys.tables WHERE name = :table_name"),
                {"table_name": table},
            )
        exists = bool(result.scalar())

        try:
            if MONETDB_SETTINGS.write_method == "adbc":
                return insert_adbc(df, table, self.connect(), primary_key, not_null, create=not exists)
            return insert(
                df,
                table,
                self.connect(),
                primary_key,
                not_null,
                create=not exists,
                lazy_write=lazy_write,
            )
        except Exception:
            self.rollback()
            raise

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        if MONETDB_SETTINGS.write_method == "adbc":
            return upsert_adbc(df, table, self.connect(), primary_key=primary_key)
        return upsert(df, table, self.connect(), primary_key=primary_key)

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
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
