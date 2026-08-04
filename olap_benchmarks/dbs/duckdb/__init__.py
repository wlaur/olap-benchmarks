import logging
import uuid
from collections.abc import Mapping
from importlib.metadata import version as package_version
from pathlib import Path
from shutil import rmtree
from typing import Any, cast

import polars as pl
from duckdb import DuckDBPyConnection
from duckdb import __version__ as duckdb_version_runtime
from sqlalchemy import Connection, create_engine

from ...results.duckdb_sqlalchemy import patch_duckdb_sqlalchemy_compat
from ...settings import SETTINGS, DatabaseName, SuiteName, TableName
from ...suites import BenchmarkSuite
from ...suites.chat_threads.config import ChatThreads
from ...suites.jsonbench.config import JSONBench, get_jsonbench_input_files, write_jsonbench_input_file
from .. import Database
from ..utils import normalize_columns, require_columns, tracked_commit

_LOGGER = logging.getLogger(__name__)

VERSION = package_version("duckdb")

assert duckdb_version_runtime == VERSION

(SETTINGS.temporary_directory / "duckdb/data").mkdir(exist_ok=True, parents=True)


POLARS_DUCKDB_TYPE_MAP: dict[pl.DataType | type[pl.DataType], str] = {
    pl.Int8: "TINYINT",
    pl.Int16: "SMALLINT",
    pl.Int32: "INTEGER",
    pl.Int64: "BIGINT",
    pl.UInt8: "UTINYINT",
    pl.UInt16: "USMALLINT",
    pl.UInt32: "UINTEGER",
    pl.UInt64: "UBIGINT",
    pl.Float32: "FLOAT",
    pl.Float64: "DOUBLE",
    pl.Boolean: "BOOLEAN",
    pl.String: "TEXT",
    pl.Date: "DATE",
    pl.Datetime: "TIMESTAMP",
    pl.Time: "TIME",
    pl.Duration: "BIGINT",
    pl.Object: "BLOB",
    pl.Struct: "JSON",
}


def get_duckdb_connection(connection: Connection) -> DuckDBPyConnection:
    return cast(DuckDBPyConnection, connection._dbapi_connection)


def polars_dtype_to_duckdb(dtype: pl.DataType) -> str:
    for pl_type, duck_type in POLARS_DUCKDB_TYPE_MAP.items():
        if dtype == pl_type:
            return duck_type
    raise ValueError(f"Unsupported Polars dtype: {dtype}")


def duckdb_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class DuckDBJSONBench(JSONBench["DuckDB"]):
    def _stage_input_files(self) -> Path:
        temp_dir = SETTINGS.temporary_directory / "duckdb/data"
        staging_dir = temp_dir / self.data_directory_name
        if staging_dir.exists():
            rmtree(staging_dir)
        staging_dir.mkdir(parents=True)

        for input_file in get_jsonbench_input_files(self.scale_factor):
            staged_file = staging_dir / input_file.name.removesuffix(".gz")
            write_jsonbench_input_file(input_file, staged_file)

        return staging_dir

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        staging_dir = self._stage_input_files()
        try:
            input_files = ", ".join(
                duckdb_string_literal(fpath.as_posix()) for fpath in sorted(staging_dir.glob("file_*.json"))
            )

            with self.db.phase_context("insert", table_name="bluesky"):
                self.db.execute(
                    f"""
                    CREATE TABLE bluesky AS
                    SELECT json AS j
                    FROM read_ndjson_objects([{input_files}])
                    """
                )
        finally:
            rmtree(staging_dir)

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        if restart:
            self.db.restart_event()


class DuckDB(Database):
    name: DatabaseName = "duckdb"
    version: str = VERSION

    connection_string: str = ""

    # in-process, no docker commands necessary
    @property
    def start(self) -> None:
        return None

    @property
    def stop(self) -> None:
        return None

    @property
    def restart(self) -> None:
        return None

    def connect(self, reconnect: bool = False) -> Connection:
        if reconnect:
            self.close_connection()

        if self._connection is not None and not reconnect:
            return self._connection

        patch_duckdb_sqlalchemy_compat()
        connection_string = f"duckdb:///{self.database_directory.as_posix()}/duck.db"
        engine = create_engine(connection_string)
        self._connection = self.bind_query_recorder(engine.connect())

        # the suites store naive timestamps and mean UTC by them; without this DuckDB would
        # interpret and render them in the host's local zone, making answers machine-dependent
        get_duckdb_connection(self._connection).execute("SET TimeZone='UTC'")

        return self._connection

    def fetch(self, query: str, schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None) -> pl.DataFrame:
        con = get_duckdb_connection(self.connect())
        with self.record_query_execution(query):
            con.execute(query)
            df = con.pl()

        if schema is not None:
            df = df.cast(cast(pl.Schema, schema))

        return df

    def get_table_names(self) -> set[TableName]:
        df = self.fetch(
            "select table_name from information_schema.tables "
            "where table_schema = 'main' and table_type = 'BASE TABLE'",
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
        connection = self.connect()
        con = get_duckdb_connection(connection)

        result = cast(
            tuple[int] | None,
            cast(Any, con)
            .execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table.lower()}'")
            .fetchone(),
        )

        assert result is not None
        table_exists = result[0] > 0

        schema = df.schema if isinstance(df, pl.DataFrame) else df.collect_schema()

        if not table_exists:
            not_null_cols = set(normalize_columns(not_null))
            primary_keys = normalize_columns(primary_key)

            col_defs: list[str] = []
            for name, dtype in schema.items():
                duck_type = polars_dtype_to_duckdb(dtype)
                constraints: list[str] = []
                if name in not_null_cols:
                    constraints.append("not null")

                col_def = f'"{name}" {duck_type} {" ".join(constraints)}'
                col_defs.append(col_def)

            pk_clause = f", primary key ({', '.join(f'"{pk}"' for pk in primary_keys)})" if primary_keys else ""
            ddl = f"create table {table} (\n  " + ",\n  ".join(col_defs) + pk_clause + "\n)"
            self.execute(ddl, commit=False)

        if isinstance(df, pl.LazyFrame):
            fpath = SETTINGS.temporary_directory / "duckdb/data" / f"{uuid.uuid4().hex}.parquet"
            df.sink_parquet(fpath)
            _LOGGER.info("Inserting from staged Parquet file via sink_parquet")

            try:
                self.execute(f"insert into {table} select * from '{fpath.as_posix()}'", commit=False)
            finally:
                fpath.unlink()
        else:
            con.register("source", df)
            _LOGGER.info(f"Inserting from in-memory dataset with shape ({df.shape[0]:_}, {df.shape[1]:_})")

            self.execute(f"insert into {table} select * from source", commit=False)

        tracked_commit(connection)

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        # DuckDB 1.5.0 crashes when creating UNIQUE/PRIMARY KEY constraints on
        # TIMESTAMP columns in persistent databases beyond ~15K rows.
        # Work around by using DELETE + INSERT instead of ON CONFLICT.
        primary_keys = require_columns(primary_key)

        for pk in primary_keys:
            if pk not in df.columns:
                raise ValueError(f"Primary key column '{pk}' not found in DataFrame columns")

        con = get_duckdb_connection(self.connect())

        con.register("upsert_source", df)

        pk_cols = ", ".join(f'"{pk}"' for pk in primary_keys)
        self.execute(f"DELETE FROM {table} WHERE ({pk_cols}) IN (SELECT {pk_cols} FROM upsert_source)", commit=False)
        self.execute(f"INSERT INTO {table} SELECT * FROM upsert_source")

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        primary_keys = require_columns(primary_key)

        con = get_duckdb_connection(self.connect())
        con.register("delete_keys", keys)

        pk_cols = ", ".join(f'"{pk}"' for pk in primary_keys)
        self.execute(f"DELETE FROM {table} WHERE ({pk_cols}) IN (SELECT {pk_cols} FROM delete_keys)")

    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        return {
            **super().suite_registry(),
            "jsonbench": DuckDBJSONBench,
            "chat_threads": ChatThreads,
        }
