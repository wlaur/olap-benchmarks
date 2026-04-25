import json
import logging
import uuid
from collections.abc import Mapping
from pathlib import Path
from time import perf_counter, sleep
from typing import Any, cast

import httpx
import polars as pl
from sqlalchemy import Connection, create_engine, text

from ...settings import SETTINGS, DatabaseName, TableName
from .. import Database
from ..utils import tracked_commit

_LOGGER = logging.getLogger(__name__)

VERSION = "4.1.0"

DOCKER_IMAGE = f"starrocks/allin1-ubuntu:{VERSION}"

STARROCKS_HOST = "localhost"
STARROCKS_QUERY_PORT = 9030
STARROCKS_HTTP_PORT = 8030
STARROCKS_USER = "root"
STARROCKS_PASSWORD = ""
STARROCKS_DATABASE = "benchmark"

STARROCKS_CONNECTION_STRING = (
    f"mysql+pymysql://{STARROCKS_USER}@{STARROCKS_HOST}:{STARROCKS_QUERY_PORT}/{STARROCKS_DATABASE}"
)


POLARS_STARROCKS_TYPE_MAP: dict[pl.DataType | type[pl.DataType], str] = {
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


def get_starrocks_type(dtype: pl.DataType | type[pl.DataType]) -> str:
    if dtype == pl.Datetime:
        return "DATETIME"

    sql_type = POLARS_STARROCKS_TYPE_MAP.get(dtype)
    if sql_type is None:
        raise ValueError(f"Unsupported Polars dtype for StarRocks: {dtype}")
    return sql_type


class StarRocks(Database):
    name: DatabaseName = "starrocks"
    version: str = VERSION

    connection_string: str = STARROCKS_CONNECTION_STRING

    @property
    def start(self) -> str:
        # allin1-ubuntu 4.x ships FE+BE under /data/deploy/starrocks. Mount
        # only the two state dirs the image's IMPORTANT NOTICE calls out;
        # mounting the parent hides the pre-populated configs and BE crashes.
        meta_dir = self.database_directory / "fe-meta"
        storage_dir = self.database_directory / "be-storage"
        staging_dir = SETTINGS.temporary_directory / "starrocks/data"

        for d in (meta_dir, storage_dir, staging_dir):
            d.mkdir(parents=True, exist_ok=True)

        parts = [
            f"docker run --platform linux/amd64 --name {self.name}-benchmark --rm -d",
            "-p 9030:9030 -p 8030:8030 -p 8040:8040",
            f"-v {meta_dir.as_posix()}:/data/deploy/starrocks/fe/meta",
            f"-v {storage_dir.as_posix()}:/data/deploy/starrocks/be/storage",
            f"-v {staging_dir.as_posix()}:/staging",
            DOCKER_IMAGE,
        ]
        return " ".join(parts)

    def wait_until_accessible(self, timeout_seconds: float = 240.0, interval_seconds: float = 2.0) -> None:
        # FE + BE startup takes ~30-60s the first time and we also need to
        # ensure the benchmark database exists before SQLAlchemy can connect.
        _LOGGER.info(f"Waiting for database {self.name} (timeout: {timeout_seconds:.0f}s)...")
        deadline = perf_counter() + timeout_seconds
        attempts = 0

        while perf_counter() < deadline:
            attempts += 1
            try:
                # connect to no specific db first to issue CREATE DATABASE
                bootstrap_uri = f"mysql+pymysql://{STARROCKS_USER}@{STARROCKS_HOST}:{STARROCKS_QUERY_PORT}/"
                engine = create_engine(bootstrap_uri)
                with engine.connect() as con:
                    con.execute(text(f"CREATE DATABASE IF NOT EXISTS {STARROCKS_DATABASE}"))
                engine.dispose()
                # now check the actual db is reachable
                self.connect(reconnect=True)
                self.fetch("select 1 as one", schema={"one": pl.Int64})
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
            self._connection = None

        if self._connection is not None:
            return self._connection

        engine = create_engine(self.connection_string)
        self._connection = self.bind_query_recorder(engine.connect())
        return self._connection

    def fetch(
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
            f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{STARROCKS_DATABASE}'",
            schema={"table_name": pl.String},
        )
        return set(df.get_column("table_name").to_list())

    def _generate_create_table_sql(
        self,
        schema: pl.Schema,
        table: TableName,
        primary_key: str | list[str] | None,
        not_null: str | list[str] | None,
    ) -> str:
        if not_null is None:
            not_null = []
        if isinstance(not_null, str):
            not_null = [not_null]

        columns: list[str] = []
        for name, dtype in schema.items():
            sr_type = get_starrocks_type(dtype)
            null_clause = "NOT NULL" if name in not_null else "NULL"
            columns.append(f"`{name}` {sr_type} {null_clause}")

        columns_sql = ",\n  ".join(columns)

        # DUPLICATE KEY tables are append-only columnar (no PK enforcement).
        # PRIMARY KEY tables support UPDATE/DELETE but require declaring the key
        # columns up front. For now use DUPLICATE KEY everywhere; suite-level
        # subclasses override CREATE TABLE for tables that need mutate.
        if isinstance(primary_key, str):
            key_cols = f"`{primary_key}`"
        elif isinstance(primary_key, list) and primary_key:
            key_cols = ", ".join(f"`{c}`" for c in primary_key)
        else:
            # fall back to first column as the duplicate key (StarRocks requires one)
            key_cols = f"`{next(iter(schema.keys()))}`"

        return (
            f"CREATE TABLE `{table}` (\n  {columns_sql}\n)\n"
            f"DUPLICATE KEY ({key_cols})\n"
            f"DISTRIBUTED BY HASH ({key_cols}) BUCKETS 4\n"
            f"PROPERTIES ('replication_num' = '1')"
        )

    def create_table(
        self,
        schema: pl.Schema,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        con = self.connect()
        statement = self._generate_create_table_sql(schema, table, primary_key, not_null)
        with self.record_query_execution(statement):
            con.execute(text(statement))
        tracked_commit(con)
        _LOGGER.info(f"Created table {table} with {len(schema):_} columns")

    def _stream_load(self, table: TableName, parquet_path: Path) -> dict[str, Any]:
        # StarRocks STREAM LOAD: HTTP PUT to FE, FE redirects to BE which
        # ingests the parquet body.
        url = f"http://{STARROCKS_HOST}:{STARROCKS_HTTP_PORT}/api/{STARROCKS_DATABASE}/{table}/_stream_load"
        headers = {
            "format": "parquet",
            "label": f"load_{table}_{uuid.uuid4().hex}",
            "Expect": "100-continue",
        }
        size = parquet_path.stat().st_size
        _LOGGER.info(f"STREAM LOAD {parquet_path.name} ({size / 1e6:.1f} MB) → {table}")
        with parquet_path.open("rb") as f:
            response = httpx.put(
                url,
                content=f.read(),
                headers=headers,
                auth=(STARROCKS_USER, STARROCKS_PASSWORD),
                follow_redirects=True,
                timeout=None,
            )
        response.raise_for_status()
        result = cast(dict[str, Any], response.json())
        if result.get("Status") not in ("Success", "Publish Timeout"):
            raise RuntimeError(f"STREAM LOAD failed for {table}: {json.dumps(result)}")
        return result

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        schema = df.schema if isinstance(df, pl.DataFrame) else df.collect_schema()

        if table not in self.get_table_names():
            self.create_table(schema, table, primary_key, not_null)

        staging = SETTINGS.temporary_directory / "starrocks/data"
        staging.mkdir(parents=True, exist_ok=True)
        parquet_path = staging / f"{table}_{uuid.uuid4().hex}.parquet"

        if isinstance(df, pl.LazyFrame):
            df.sink_parquet(parquet_path)
        else:
            df.write_parquet(parquet_path)

        try:
            with self.record_query_execution(f"STREAM LOAD {table}"):
                self._stream_load(table, parquet_path)
        finally:
            parquet_path.unlink(missing_ok=True)

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        # Implemented per-suite where needed: PRIMARY KEY tables support
        # UPSERT via STREAM LOAD with `partial_update` header. For DUPLICATE
        # tables there's no native upsert; suites do delete + insert instead.
        raise NotImplementedError("StarRocks upsert is implemented per suite")

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        primary_keys = [primary_key] if isinstance(primary_key, str) else primary_key
        if not primary_keys:
            raise ValueError("primary_key must be a non-empty string or list of strings")
        if len(primary_keys) != 1:
            raise NotImplementedError("StarRocks delete supports a single-column primary key only")

        pk = primary_keys[0]
        if pk not in keys.columns:
            raise ValueError(f"Primary key column '{pk}' not found in keys DataFrame")

        # Stage keys as a temp table, then DELETE FROM ... WHERE pk IN (subquery).
        staging_table = f"_staging_del_{table}_{uuid.uuid4().hex[:8]}"
        self.insert(keys.select(pk).unique(), staging_table, primary_key=pk, not_null=pk)

        try:
            con = self.connect()
            statement = f"DELETE FROM `{table}` WHERE `{pk}` IN (SELECT `{pk}` FROM `{staging_table}`)"
            with self.record_query_execution(statement):
                con.execute(text(statement))
            tracked_commit(con)
        finally:
            con = self.connect()
            statement = f"DROP TABLE IF EXISTS `{staging_table}`"
            with self.record_query_execution(statement):
                con.execute(text(statement))
            tracked_commit(con)
