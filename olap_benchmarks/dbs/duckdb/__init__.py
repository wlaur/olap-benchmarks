import logging
import uuid
from collections.abc import Mapping
from pathlib import Path
from shutil import rmtree
from typing import Any, ClassVar, cast

import polars as pl
from duckdb import DuckDBPyConnection, build_info
from duckdb import __version__ as duckdb_version_runtime
from pydantic import PrivateAttr
from sqlalchemy import Connection, create_engine

from ...metrics.measure import get_paths_size_mb
from ...settings import DatabaseName, Operation, SuiteName, TableName, get_settings
from ...suites import BenchmarkSuite
from ...suites.chat_threads.config import ChatThreads
from ...suites.jsonbench.config import JSONBench, get_jsonbench_input_files, write_jsonbench_input_file
from ...suites.kaggle_airbnb.config import KaggleAirbnb
from ...suites.rtabench.config import RTABench
from .. import Database, ParquetEpochColumns, apply_parquet_epoch_columns
from ..utils import normalize_columns, require_columns, tracked_commit
from .storage import Column, Storage, StorageMode, identifier, literal

_LOGGER = logging.getLogger(__name__)


def _staging_directory() -> Path:
    directory = get_settings().temporary_directory / "duckdb/data"
    directory.mkdir(exist_ok=True, parents=True)
    return directory


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
    if isinstance(dtype, pl.Decimal):
        return f"DECIMAL({dtype.precision}, {dtype.scale})"
    for pl_type, duck_type in POLARS_DUCKDB_TYPE_MAP.items():
        if dtype == pl_type:
            return duck_type
    raise ValueError(f"Unsupported Polars dtype: {dtype}")


def duckdb_string_literal(value: str) -> str:
    return literal(value)


DATABASE_FILE_NAME = "duck.db"


class DuckDBAirbnb(KaggleAirbnb["DuckDB"]):
    def populate_table(self, table: TableName, path: Path) -> None:
        if self.db.storage_mode == "parquet":
            self.db.insert_parquet(path, table, **self.populate_kwargs)
        else:
            super().populate_table(table, path)
            self.db.storage.record_native(table, [path])


class DuckDBRTABench(RTABench["DuckDB"]):
    def populate_table(self, table: TableName, path: Path) -> None:
        if self.db.storage_mode == "parquet":
            self.db.insert_parquet(path, table, **self.populate_kwargs)
        else:
            super().populate_table(table, path)
            self.db.storage.record_native(table, [path])


class DuckDBJSONBench(JSONBench["DuckDB"]):
    def _stage_input_files(self) -> Path:
        staging_dir = _staging_directory() / self.data_directory_name
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

        self.db.storage.building()
        staging_dir = self._stage_input_files()
        try:
            input_files = ", ".join(
                duckdb_string_literal(fpath.as_posix()) for fpath in sorted(staging_dir.glob("file_*.json"))
            )

            with self.db.phase_context("insert", table_name="bluesky"):
                query = f"SELECT json AS j FROM read_ndjson_objects([{input_files}])"
                if self.db.storage_mode == "native":
                    self.db.execute(f"CREATE TABLE bluesky AS {query}")
                    self.db.storage.record_native("bluesky", list(get_jsonbench_input_files(self.scale_factor)))
                else:
                    path = self.db.database_directory / "bluesky.parquet"
                    partial = path.with_suffix(".parquet.part")
                    try:
                        self.db.execute(
                            f"COPY (SELECT CAST(j AS VARCHAR) AS j FROM ({query})) "
                            f"TO {literal(partial.as_posix())} (FORMAT PARQUET)"
                        )
                        partial.replace(path)
                        self.db.storage.register(
                            "bluesky",
                            path,
                            [Column(name="j", dtype="JSON", not_null=False, primary_key=False)],
                            'CAST("j" AS JSON) AS "j"',
                        )
                    finally:
                        partial.unlink(missing_ok=True)
        finally:
            rmtree(staging_dir)

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        if restart:
            self.db.restart_event()


class DuckDB(Database):
    name: DatabaseName = "duckdb"
    wal_path_patterns: ClassVar[tuple[str, ...]] = (f"{DATABASE_FILE_NAME}.wal",)

    connection_string: str = ""
    _runtime_before: dict[str, object] = PrivateAttr(default_factory=dict)
    _runtime_after: dict[str, object] = PrivateAttr(default_factory=dict)
    _recorded_options: dict[str, object] = PrivateAttr(default_factory=dict)

    @property
    def storage_mode(self) -> StorageMode:
        return get_settings().duckdb_storage_mode

    @property
    def storage(self) -> Storage:
        return Storage(self)

    @property
    def storage_contract_scope(self) -> str:
        if self.storage_mode == "parquet" and self.current_suite in ("time_series", "chat_threads"):
            return "select-only"
        return "full-suite"

    @property
    def storage_options(self) -> dict[str, object]:
        return {
            **self.storage.metadata(),
            "contract_scope": self.storage_contract_scope,
            "constraint_policy": "validated on populate; Parquet views do not enforce DML constraints"
            if self.storage_mode == "parquet"
            else "native enforced constraints",
        }

    @property
    def connection_config(self) -> dict[str, str]:
        settings = get_settings()
        temporary = settings.temporary_directory / "duckdb" / self.database_directory.name / self.storage_mode / "spill"
        temporary.mkdir(parents=True, exist_ok=True)
        config = {
            "threads": str(settings.benchmark_threads or 10),
            "async_threads": str(settings.duckdb_async_threads),
            "read_ahead_depth": str(settings.duckdb_read_ahead_depth),
            "TimeZone": "UTC",
            "temp_directory": temporary.resolve().as_posix(),
            "storage_compatibility_version": "v2.0.0",
        }
        if settings.benchmark_memory_gb is not None:
            config["memory_limit"] = f"{settings.benchmark_memory_gb}GiB"
        return config

    @property
    def metric_directories(self) -> tuple[Path, ...]:
        return (self.database_directory.resolve(), *self.storage.external_files())

    @property
    def run_options(self) -> Mapping[str, object]:
        if self._current_suite is None:
            return super().run_options
        memory_gb = get_settings().benchmark_memory_gb
        external = self.storage.external_files()
        options: dict[str, object] = {
            **super().run_options,
            "memory_limit_bytes": memory_gb * 1024**3 if memory_gb is not None else None,
            "duckdb_storage": self.storage_options,
            "duckdb_runtime": {"requested": self.connection_config, "before": self.runtime_metadata()},
            "storage_accounting": {
                "version": 1,
                "additional_imported_store_path": self.database_directory.resolve().as_posix(),
                "required_referenced_storage_paths": [path.as_posix() for path in external],
                "required_referenced_storage_mb_before_operation": get_paths_size_mb(external),
                "scope": "disk_mb includes the owned store and required external Parquet exactly once. "
                "The referenced baseline is already included, not an amount to add again. "
                "Temporary staging/spill and host-retained native input sources are excluded.",
            },
        }
        self._recorded_options = options
        return options

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

    @property
    def run_package_names(self) -> tuple[str, ...]:
        return (*super().run_package_names, "duckdb", "duckdb-engine", "polars", "pyarrow")

    def get_runtime_version(self) -> str:
        return duckdb_version_runtime

    def connect(self, reconnect: bool = False) -> Connection:
        if reconnect:
            self.close_connection()

        if self._connection is not None and not reconnect:
            return self._connection

        connection_string = f"duckdb:///{self.database_directory.as_posix()}/{DATABASE_FILE_NAME}"
        engine = create_engine(connection_string, connect_args={"config": self.connection_config})
        self._connection = self.bind_query_recorder(engine.connect())
        return self._connection

    def raw_connection(self) -> DuckDBPyConnection:
        return get_duckdb_connection(self.connect())

    def runtime_build(self) -> list[str]:
        rows = cast(list[tuple[str, str, str]], self.raw_connection().execute("PRAGMA version").fetchall())
        return list(rows[0])

    def database_tags(self) -> dict[str, str]:
        rows = cast(
            list[tuple[dict[str, str]]],
            self.raw_connection()
            .execute("SELECT tags FROM duckdb_databases() WHERE database_name=current_database()")
            .fetchall(),
        )
        return rows[0][0]

    def runtime_metadata(self, *, validate: bool = True) -> dict[str, object]:
        rows = cast(
            list[tuple[str, str | None]],
            self.raw_connection()
            .execute(
                "SELECT name, value FROM duckdb_settings() WHERE name IN "
                "('threads', 'async_threads', 'read_ahead_depth', 'TimeZone', 'memory_limit', "
                "'temp_directory', 'max_temp_directory_size', 'storage_compatibility_version', "
                "'preserve_insertion_order', 'fsync_mode') "
                "OR name LIKE '%cache%' OR name LIKE '%prefetch%' ORDER BY name"
            )
            .fetchall(),
        )
        actual = dict(rows)
        databases = cast(
            list[tuple[str, str, dict[str, str]]],
            self.raw_connection()
            .execute("SELECT database_name, path, tags FROM duckdb_databases() WHERE database_name=current_database()")
            .fetchall(),
        )
        metadata: dict[str, object] = {
            "build_info": build_info(),
            "pragma_version": self.runtime_build(),
            "settings": actual,
            "databases": [{"name": name, "path": path, "tags": tags} for name, path, tags in databases],
        }
        if validate:
            self.verify_runtime_configuration(metadata)
        return metadata

    def verify_runtime_configuration(self, metadata: Mapping[str, object]) -> None:
        actual = cast(dict[str, str], metadata["settings"])
        for name, requested in self.connection_config.items():
            expected = f"{get_settings().benchmark_memory_gb}.0 GiB" if name == "memory_limit" else requested
            if actual.get(name) != expected:
                raise RuntimeError(f"DuckDB setting {name}: requested {requested!r}, effective {actual.get(name)!r}")

    def reset_session(self) -> None:
        self.connect(reconnect=True)
        self.runtime_metadata()

    def initialize_schema(self, suite_directory: str) -> None:
        self.storage.building()
        super().initialize_schema(suite_directory)

    def verify_storage_readiness(self) -> None:
        self.storage.verify()

    def finalize_populate(self) -> None:
        self.storage.finalize()

    def benchmark(self, suite: SuiteName, operation: Operation, scale_factor: int | None = None) -> None:
        if self.storage_mode == "parquet" and operation in ("mutate", "concurrent"):
            raise ValueError("DuckDB Parquet mode supports only populate/select; use native for writes")
        super().benchmark(suite, operation, scale_factor)

    def before_benchmark_operation(self) -> None:
        self._runtime_after = {}
        self._runtime_before = self.runtime_metadata(validate=False)
        self.verify_runtime_configuration(self._runtime_before)
        if self._current_operation != "populate":
            manifest = self.storage.verify()
            if self._current_operation in ("mutate", "concurrent"):
                self._require_native_writes()
                manifest.dirty = True
                self.storage.write(manifest)

    def after_benchmark_operation(self) -> None:
        self._runtime_after = self.runtime_metadata(validate=False)
        self.verify_runtime_configuration(self._runtime_after)

    def _executed_query_metadata(self) -> dict[str, Any]:
        return {
            **(super()._executed_query_metadata() or {}),
            "options": {
                **self._recorded_options,
                "duckdb_storage": self.storage_options,
                "duckdb_runtime": {
                    "requested": self.connection_config,
                    "before": self._runtime_before,
                    "after": self._runtime_after,
                },
            },
        }

    def _require_native_writes(self) -> None:
        if self.storage_mode != "native":
            raise ValueError("DuckDB Parquet views are read-only; use native mode for writes")

    def fetch(self, query: str, schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None) -> pl.DataFrame:
        con = get_duckdb_connection(self.connect())
        with self.record_query_execution(query):
            df = con.sql(query).pl()

        if schema is not None:
            df = df.cast(cast(pl.Schema, schema))

        return df

    def get_table_names(self) -> set[TableName]:
        df = self.fetch(
            "select table_name from information_schema.tables "
            "where table_catalog=current_database() and table_schema = 'main' "
            "and table_type IN ('BASE TABLE', 'VIEW')",
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
        self._require_native_writes()
        if self._current_operation == "populate":
            self.storage.building()
        connection = self.connect()
        con = get_duckdb_connection(connection)

        rows = cast(
            list[tuple[int]],
            con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema='main' "
                f"AND table_catalog=current_database() AND table_name={literal(table)}"
            ).fetchall(),
        )
        table_exists = rows[0][0] > 0

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

                col_def = f"{identifier(name)} {duck_type} {' '.join(constraints)}"
                col_defs.append(col_def)

            pk_clause = f", primary key ({', '.join(identifier(pk) for pk in primary_keys)})" if primary_keys else ""
            ddl = f"create table {identifier(table)} (\n  " + ",\n  ".join(col_defs) + pk_clause + "\n)"
            self.execute(ddl, commit=False)

        if isinstance(df, pl.LazyFrame):
            fpath = _staging_directory() / f"{uuid.uuid4().hex}.parquet"
            df.sink_parquet(fpath)
            _LOGGER.info("Inserting from staged Parquet file via sink_parquet")

            try:
                self.execute(f"insert into {identifier(table)} select * from {literal(fpath.as_posix())}", commit=False)
            finally:
                fpath.unlink()
        else:
            con.register("source", df)
            _LOGGER.info(f"Inserting from in-memory dataset with shape ({df.shape[0]:_}, {df.shape[1]:_})")

            self.execute(f"insert into {table} select * from source", commit=False)

        tracked_commit(connection)
        if self._current_operation == "populate":
            self.storage.record_native(table)

    def insert_parquet(
        self,
        path: Path,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
        *,
        epoch_columns: ParquetEpochColumns | None = None,
    ) -> None:
        self.storage.building()
        if self.storage_mode == "native":
            super().insert_parquet(path, table, primary_key, not_null, epoch_columns=epoch_columns)
            self.storage.record_native(table, [path])
            return
        if table in self.get_table_names():
            columns = self.storage.columns(table)
        else:
            schema = apply_parquet_epoch_columns(pl.scan_parquet(path), epoch_columns).collect_schema()
            required = normalize_columns(not_null)
            keys = normalize_columns(primary_key)
            columns = [
                Column(
                    name=name,
                    dtype=polars_dtype_to_duckdb(dtype),
                    not_null=name in required or name in keys,
                    primary_key=name in keys,
                )
                for name, dtype in schema.items()
            ]
            # Normalize aliases through DuckDB, as for schema-defined tables.
            types = ", ".join(f"CAST(NULL AS {col.dtype}) AS {identifier(col.name)}" for col in columns)
            rows = cast(
                list[tuple[str, str, object, object, object, object]],
                self.raw_connection().execute(f"DESCRIBE SELECT {types}").fetchall(),
            )
            for col, row in zip(columns, rows, strict=True):
                col.dtype = row[1]
        projection: list[str] = []
        for col in columns:
            expression = identifier(col.name)
            unit = (epoch_columns or {}).get(col.name)
            if unit == "day":
                expression = f"DATE '1970-01-01' + CAST({expression} AS INTEGER)"
            elif unit is not None:
                factor = {"s": 1_000_000, "ms": 1000, "us": 1, "ns": None}[unit]
                if factor is None:
                    expression = f"make_timestamp_ns(CAST({expression} AS BIGINT))"
                else:
                    expression = f"make_timestamp(CAST({expression} AS BIGINT) * {factor})"
            projection.append(f"CAST({expression} AS {col.dtype}) AS {identifier(col.name)}")
        self.storage.register(table, path, columns, ", ".join(projection))

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        self._require_native_writes()
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
        self._require_native_writes()
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
            "kaggle_airbnb": DuckDBAirbnb,
            "rtabench": DuckDBRTABench,
        }
