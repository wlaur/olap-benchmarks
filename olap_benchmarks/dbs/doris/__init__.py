import json
import logging
import subprocess
import uuid
from collections.abc import Mapping
from pathlib import Path
from time import perf_counter, sleep
from typing import Any, ClassVar, Literal, cast

import polars as pl
from sqlalchemy import Connection, create_engine, text
from sqlalchemy.engine import make_url

from ...settings import SETTINGS, DatabaseName, SuiteName, TableName
from ...suites import BenchmarkSuite
from ...suites.clickbench.config import Clickbench
from ...suites.jsonbench.config import JSONBench, get_jsonbench_input_files, iter_jsonbench_input_lines
from .. import Database
from ..utils import normalize_columns

_LOGGER = logging.getLogger(__name__)

VERSION = "4.1.3"

DORIS_FE_IMAGE = f"apache/doris:fe-{VERSION}"
DORIS_BE_IMAGE = f"apache/doris:be-{VERSION}"
DORIS_NETWORK = "doris-benchmark"
DORIS_NETWORK_SUBNET = "172.28.40.0/24"
DORIS_FE_IP = "172.28.40.2"
DORIS_BE_IP = "172.28.40.3"
DORIS_FE_CONTAINER = "doris-fe-benchmark"
DORIS_BE_CONTAINER = "doris-be-benchmark"
DORIS_FE_SERVERS = f"fe1:{DORIS_FE_IP}:9010"

DORIS_HOST = "localhost"
DORIS_QUERY_PORT = 9030
DORIS_HTTP_PORT = 8030
DORIS_USER = "root"
DORIS_PASSWORD = ""
DORIS_DATABASE = "benchmark"
DORIS_CONNECTION_STRING = f"mysql+pymysql://{DORIS_USER}@{DORIS_HOST}:{DORIS_QUERY_PORT}/{DORIS_DATABASE}"

DorisFetchMethod = Literal["connectorx", "python"]

POLARS_DORIS_TYPE_MAP: dict[pl.DataType | type[pl.DataType], str] = {
    pl.Int8: "TINYINT",
    pl.Int16: "SMALLINT",
    pl.Int32: "INT",
    pl.Int64: "BIGINT",
    pl.UInt8: "SMALLINT",
    pl.UInt16: "INT",
    pl.UInt32: "BIGINT",
    pl.UInt64: "LARGEINT",
    pl.Float32: "FLOAT",
    pl.Float64: "DOUBLE",
    pl.Boolean: "BOOLEAN",
    pl.String: "STRING",
    pl.Date: "DATE",
}


def get_doris_type(dtype: pl.DataType | type[pl.DataType]) -> str:
    if dtype == pl.Datetime:
        return "DATETIME(3)"

    if isinstance(dtype, pl.Decimal):
        precision = dtype.precision or 38
        scale = dtype.scale or 0
        return f"DECIMAL({precision}, {scale})"

    sql_type = POLARS_DORIS_TYPE_MAP.get(dtype)
    if sql_type is None:
        raise ValueError(f"Unsupported Polars dtype for Doris: {dtype}")
    return sql_type


class DorisClickbench(Clickbench["Doris"]):
    @property
    def populate_kwargs(self) -> dict[str, Any]:
        return {"partitions": 100}


class DorisJSONBench(JSONBench["Doris"]):
    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {"method": "python"}

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        with self.db.phase_context("schema", table_name="bluesky"):
            self.db.execute(
                """
                CREATE TABLE bluesky (
                    id BIGINT NOT NULL,
                    data JSON NOT NULL
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 16
                PROPERTIES ("replication_num" = "1")
                """
            )

        next_id = 1
        with self.db.phase_context("insert", table_name="bluesky"):
            for input_file in get_jsonbench_input_files(self.scale_factor):
                staged_file, next_id = self._write_stream_load_input_file(input_file, next_id)
                try:
                    self.db.stream_load_file(
                        "bluesky",
                        staged_file,
                        {
                            "format": "csv",
                            "column_separator": "\t",
                            "columns": "id,data",
                            "strict_mode": "true",
                            "max_filter_ratio": "0.00001",
                        },
                    )
                finally:
                    staged_file.unlink(missing_ok=True)

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        if restart:
            self.db.restart_event()

    def _write_stream_load_input_file(self, input_file: Path, start_id: int) -> tuple[Path, int]:
        staged_file = self.db.staging_directory / f"bluesky_{uuid.uuid4().hex}.tsv"
        row_id = start_id

        with staged_file.open("w", encoding="utf-8") as out:
            for line in iter_jsonbench_input_lines(input_file):
                out.write(f"{row_id}\t{line}")
                row_id += 1

        return staged_file, row_id


class Doris(Database):
    name: DatabaseName = "doris"
    version: str = VERSION
    container_image: ClassVar[str | None] = None

    connection_string: str = DORIS_CONNECTION_STRING

    @property
    def container_images(self) -> Mapping[str, str]:
        return {"fe": DORIS_FE_IMAGE, "be": DORIS_BE_IMAGE}

    @property
    def metric_container_names(self) -> tuple[str, ...]:
        return (DORIS_FE_CONTAINER, DORIS_BE_CONTAINER)

    @property
    def start(self) -> str | None:
        return None

    @property
    def start_commands(self) -> tuple[str, ...]:
        meta_dir = self.database_directory / "fe-meta"
        fe_log_dir = self.database_directory / "fe-log"
        storage_dir = self.database_directory / "be-storage"
        be_log_dir = self.database_directory / "be-log"
        self.staging_directory.mkdir(parents=True, exist_ok=True)

        for directory in (meta_dir, fe_log_dir, storage_dir, be_log_dir):
            directory.mkdir(parents=True, exist_ok=True)

        return (
            f"docker network create --subnet {DORIS_NETWORK_SUBNET} {DORIS_NETWORK}",
            self.docker_run_command(
                DORIS_FE_IMAGE,
                name=DORIS_FE_CONTAINER,
                network=DORIS_NETWORK,
                ip=DORIS_FE_IP,
                ports={"9030": "9030", "8030": "8030"},
                mounts={
                    meta_dir.as_posix(): "/opt/apache-doris/fe/doris-meta",
                    fe_log_dir.as_posix(): "/opt/apache-doris/fe/log",
                },
                env={"FE_SERVERS": DORIS_FE_SERVERS, "FE_ID": "1"},
            ),
            self.docker_run_command(
                DORIS_BE_IMAGE,
                name=DORIS_BE_CONTAINER,
                network=DORIS_NETWORK,
                ip=DORIS_BE_IP,
                ports={"8040": "8040"},
                mounts={
                    storage_dir.as_posix(): "/opt/apache-doris/be/storage",
                    be_log_dir.as_posix(): "/opt/apache-doris/be/log",
                },
                env={"FE_SERVERS": DORIS_FE_SERVERS, "BE_ADDR": f"{DORIS_BE_IP}:9050"},
            ),
        )

    @property
    def stop_commands(self) -> tuple[str, ...]:
        return (
            f"docker stop {DORIS_BE_CONTAINER}",
            f"docker stop {DORIS_FE_CONTAINER}",
            f"docker network rm {DORIS_NETWORK}",
        )

    @property
    def restart_commands(self) -> tuple[str, ...]:
        return (f"docker restart {DORIS_FE_CONTAINER}", f"docker restart {DORIS_BE_CONTAINER}")

    @property
    def staging_directory(self) -> Path:
        directory = SETTINGS.temporary_directory / "doris/data"
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def get_runtime_version(self) -> str:
        df = self.fetch("select @@version_comment as version", schema={"version": pl.String}, method="python")
        return str(df.item(0, 0))

    def wait_until_accessible(self, timeout_seconds: float = 300.0, interval_seconds: float = 2.0) -> None:
        _LOGGER.info(f"Waiting for database {self.name} (timeout: {timeout_seconds:.0f}s)...")
        deadline = perf_counter() + timeout_seconds
        attempts = 0
        bootstrap_uri = f"mysql+pymysql://{DORIS_USER}@{DORIS_HOST}:{DORIS_QUERY_PORT}/"

        while perf_counter() < deadline:
            attempts += 1
            try:
                engine = create_engine(bootstrap_uri)
                try:
                    with engine.connect() as con:
                        con.execute(text(f"CREATE DATABASE IF NOT EXISTS {DORIS_DATABASE}"))
                finally:
                    engine.dispose()

                self.connect(reconnect=True)
                self.fetch("select 1 as one", schema={"one": pl.Int64}, method="python")
                self.fetch("select host, alive from backends()", method="python")

                self.execute("DROP TABLE IF EXISTS _probe")
                self.execute(
                    """
                    CREATE TABLE _probe (
                        id INT NOT NULL
                    )
                    DUPLICATE KEY(id)
                    DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                    """
                )
                self.execute("DROP TABLE _probe")

                _LOGGER.info(f"Database {self.name} is ready (after {attempts} attempt(s))")
                return
            except NotImplementedError:
                raise
            except Exception as e:
                elapsed = perf_counter() + timeout_seconds - deadline
                _LOGGER.info(f"Database {self.name} not ready ({elapsed:.0f}s elapsed): {e}")
                sleep(interval_seconds)

        raise TimeoutError(f"Timed out after {timeout_seconds:.0f}s waiting for database {self.name}")

    def connect(self, reconnect: bool = False) -> Connection:
        if reconnect:
            self.close_connection()

        if self._connection is not None:
            return self._connection

        engine = create_engine(self.connection_string)
        self._connection = self.bind_query_recorder(engine.connect())
        return self._connection

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
        method: DorisFetchMethod = "python",
    ) -> pl.DataFrame:
        if method == "connectorx":
            return self.fetch_connectorx(query, schema)
        if method == "python":
            return self.fetch_python(query, schema)

        raise ValueError(f"Unknown method: {method}")

    def _connectorx_uri(self) -> str:
        url = make_url(self.connection_string).set(drivername="mysql")
        return url.render_as_string(hide_password=False)

    def fetch_connectorx(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
    ) -> pl.DataFrame:
        sql = query.strip().removesuffix(";")
        with self.record_query_execution(sql):
            df = pl.read_database_uri(sql, self._connectorx_uri())

        if schema is not None:
            df = df.cast(cast(pl.Schema, schema))

        return df

    def fetch_python(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
    ) -> pl.DataFrame:
        sql = query.strip().removesuffix(";").replace(":", r"\:")
        with self.record_query_execution(query):
            result = self.connect().execute(text(sql))

        columns = list(result.keys())
        rows = result.fetchall()

        if not rows:
            if schema:
                return pl.DataFrame(schema=cast(pl.Schema, schema))
            return pl.DataFrame({col: [] for col in columns})

        df = pl.DataFrame({col: [row[idx] for row in rows] for idx, col in enumerate(columns)})

        if schema is not None:
            df = df.cast(cast(pl.Schema, schema))

        return df

    def get_table_names(self) -> set[TableName]:
        df = self.fetch(
            f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{DORIS_DATABASE}'",
            schema={"table_name": pl.String},
            method="python",
        )
        return set(df.get_column("table_name").to_list())

    def _generate_create_table_sql(
        self,
        schema: pl.Schema,
        table: TableName,
        primary_key: str | list[str] | None,
        not_null: str | list[str] | None,
    ) -> str:
        not_null_columns = normalize_columns(not_null)
        primary_key_columns = normalize_columns(primary_key)
        key_columns = primary_key_columns or [schema.names()[0]]

        for column in key_columns:
            if column not in not_null_columns:
                not_null_columns = [*not_null_columns, column]

        ordered_columns = [column for column in key_columns if column in schema] + [
            column for column in schema if column not in key_columns
        ]

        columns: list[str] = []
        for name in ordered_columns:
            doris_type = get_doris_type(schema[name])
            null_clause = "NOT NULL" if name in not_null_columns else "NULL"
            columns.append(f"`{name}` {doris_type} {null_clause}")

        columns_sql = ",\n  ".join(columns)
        key_cols = ", ".join(f"`{column}`" for column in key_columns)
        return (
            f"CREATE TABLE `{table}` (\n  {columns_sql}\n)\n"
            f"DUPLICATE KEY({key_cols})\n"
            f"DISTRIBUTED BY HASH({key_cols}) BUCKETS 16\n"
            f'PROPERTIES ("replication_num" = "1")'
        )

    def create_table(
        self,
        schema: pl.Schema,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        self.execute(self._generate_create_table_sql(schema, table, primary_key, not_null))
        _LOGGER.info(f"Created table {table} with {len(schema):_} columns")

    def stream_load_file(self, table: TableName, file_path: Path, headers: Mapping[str, str]) -> None:
        url = f"http://{DORIS_HOST}:{DORIS_HTTP_PORT}/api/{DORIS_DATABASE}/{table}/_stream_load"
        command = [
            "curl",
            "-sS",
            "--location-trusted",
            "-u",
            f"{DORIS_USER}:{DORIS_PASSWORD}",
            "-H",
            "Expect:100-continue",
            "-H",
            f"label:{table}_{uuid.uuid4().hex}",
        ]
        for key, value in headers.items():
            command.extend(["-H", f"{key}:{value}"])
        command.extend(["-T", file_path.as_posix(), "-XPUT", url])

        label = f"STREAM LOAD {table} FROM {file_path.name}"
        with self.record_query_execution(label):
            result = subprocess.run(command, capture_output=True, text=True, check=False)

        if result.returncode != 0:
            raise RuntimeError(f"Doris stream load failed for {file_path.name}: {result.stderr.strip()}")

        response = json.loads(result.stdout)
        if response.get("Status") != "Success":
            raise RuntimeError(f"Doris stream load failed for {file_path.name}: {response}")

        _LOGGER.info(f"Stream-loaded {file_path.name} into {table}: {response}")

    def _write_single_parquet(self, df: pl.DataFrame | pl.LazyFrame, table: TableName) -> Path:
        parquet_path = self.staging_directory / f"{table}_{uuid.uuid4().hex}.parquet"
        if isinstance(df, pl.LazyFrame):
            df.sink_parquet(parquet_path)
        else:
            df.write_parquet(parquet_path)
        return parquet_path

    def _write_partitioned_parquet_lazy(self, df: pl.LazyFrame, table: TableName, partitions: int) -> list[Path]:
        row_count = int(df.select(pl.len()).collect().item(0, 0))
        chunk_size = max(1, row_count // partitions)
        parquet_paths: list[Path] = []

        total_rows = 0
        for idx, batch in enumerate(df.collect_batches(chunk_size=chunk_size)):
            total_rows += batch.shape[0]
            parquet_path = self.staging_directory / f"{table}_{uuid.uuid4().hex}_{idx}.parquet"
            batch.write_parquet(parquet_path)
            parquet_paths.append(parquet_path)

        if total_rows != row_count:
            raise RuntimeError(f"Partitioned Parquet staging wrote {total_rows:_} rows, expected {row_count:_}")

        return parquet_paths

    def _write_partitioned_parquet(self, df: pl.DataFrame, table: TableName, partitions: int) -> list[Path]:
        chunk_size = max(1, df.shape[0] // partitions)
        parquet_paths: list[Path] = []

        for idx, offset in enumerate(range(0, df.shape[0], chunk_size)):
            parquet_path = self.staging_directory / f"{table}_{uuid.uuid4().hex}_{idx}.parquet"
            df.slice(offset, chunk_size).write_parquet(parquet_path)
            parquet_paths.append(parquet_path)

        return parquet_paths

    def _write_parquet_files(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        partitions: int | None,
    ) -> list[Path]:
        if partitions is None:
            return [self._write_single_parquet(df, table)]
        if isinstance(df, pl.LazyFrame):
            return self._write_partitioned_parquet_lazy(df, table, partitions)
        return self._write_partitioned_parquet(df, table, partitions)

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
        partitions: int | None = None,
    ) -> None:
        schema = df.schema if isinstance(df, pl.DataFrame) else df.collect_schema()

        if table not in self.get_table_names():
            self.create_table(schema, table, primary_key, not_null)

        parquet_paths = self._write_parquet_files(df, table, partitions)
        columns = ",".join(schema.names())
        try:
            for parquet_path in parquet_paths:
                self.stream_load_file(
                    table,
                    parquet_path,
                    {
                        "format": "parquet",
                        "columns": columns,
                        "strict_mode": "true",
                        "max_filter_ratio": "0.00001",
                    },
                )
                sleep(0.1)
        finally:
            for parquet_path in parquet_paths:
                parquet_path.unlink(missing_ok=True)

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        raise NotImplementedError("Doris upsert is not wired for the initial ClickBench/JSONBench scope")

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        raise NotImplementedError("Doris delete is not wired for the initial ClickBench/JSONBench scope")

    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        return {
            "clickbench": DorisClickbench,
            "jsonbench": DorisJSONBench,
        }
