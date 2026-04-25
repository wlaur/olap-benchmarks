import logging
import os
import shutil
import uuid
from collections.abc import Mapping
from pathlib import Path
from time import perf_counter, sleep
from typing import cast

import polars as pl
from sqlalchemy import Connection, create_engine, text

from ...settings import SETTINGS, DatabaseName, TableName
from ...suites.clickbench.config import Clickbench
from ...suites.time_series.config import TimeSeries
from .. import Database
from ..utils import tracked_commit

_LOGGER = logging.getLogger(__name__)

# Pinned to 4.0.9 because the 4.1.0 BE binary segfaults on startup in both
# the linux/amd64 (under OrbStack QEMU/Rosetta) and linux/arm64 images on
# Apple Silicon -- fe-ubuntu/be-ubuntu split images crash the same way.
# 4.0.9 is the latest 4.0.x patch (2026-04-20) and is one week older than
# 4.1.0; the LTS-style 4.0 line is fine for benchmarking. When the bench is
# re-run on a real x86_64 Linux host, bump VERSION to whichever 4.x is
# current and switch the platform flag below back to linux/amd64.
VERSION = "4.0.9"

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

    if isinstance(dtype, pl.Decimal):
        precision = dtype.precision or 38
        scale = dtype.scale or 0
        return f"DECIMAL({precision}, {scale})"

    sql_type = POLARS_STARROCKS_TYPE_MAP.get(dtype)
    if sql_type is None:
        raise ValueError(f"Unsupported Polars dtype for StarRocks: {dtype}")
    return sql_type


_CLICKBENCH_TIMESTAMP_COLS = ("EventTime", "ClientEventTime", "LocalEventTime")
_CLICKBENCH_DATE_COLS = ("EventDate",)


class StarRocksClickbench(Clickbench["StarRocks"]):
    """Stream the 14 GB hits.parquet straight into StarRocks via FILES().

    The base Clickbench.populate would scan-and-collect the parquet into a
    polars DataFrame and re-stage it; for 100 M rows that's both slow and
    memory-hungry. Instead we hardlink the source parquet into the BE-mounted
    staging dir and use INSERT INTO ... SELECT ... FROM FILES(...) with
    inline casts for the int-encoded timestamp / date columns.
    """

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        self.db.initialize_schema("clickbench")

        staging = SETTINGS.temporary_directory / "starrocks/data"
        staging.mkdir(parents=True, exist_ok=True)
        src = SETTINGS.input_data_directory / "clickbench/hits.parquet"
        dst = staging / "hits.parquet"
        if dst.exists() or dst.is_symlink():
            dst.unlink()
        try:
            os.link(src, dst)
        except OSError:
            shutil.copy2(src, dst)

        # Source parquet has EventTime/ClientEventTime/LocalEventTime as
        # int64 epoch-seconds and EventDate as uint16 days-since-epoch. Cast
        # inline in the INSERT so we don't rewrite the file.
        con = self.db.connect()
        col_names = list(con.execute(text("DESCRIBE hits")).fetchall())
        select_parts: list[str] = []
        for row in col_names:
            name = row[0]
            if name in _CLICKBENCH_TIMESTAMP_COLS:
                select_parts.append(f"from_unixtime(`{name}`) AS `{name}`")
            elif name in _CLICKBENCH_DATE_COLS:
                select_parts.append(f"date_add('1970-01-01', INTERVAL `{name}` DAY) AS `{name}`")
            else:
                select_parts.append(f"`{name}`")
        select_list = ", ".join(select_parts)
        sql = (
            f"INSERT INTO hits SELECT {select_list} "
            f"FROM FILES('path' = 'file:///staging/hits.parquet', 'format' = 'parquet')"
        )

        with self.db.phase_context("insert", table_name="hits"):
            with self.db.record_query_execution(sql):
                con.execute(text(sql))
            tracked_commit(con)
            _LOGGER.info("Inserted clickbench table for starrocks")

        dst.unlink()

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        if restart:
            self.db.restart_event()


class StarRocksTimeSeries(TimeSeries["StarRocks"]):
    # Use PRIMARY KEY tables so the mutate phase (UPDATE/DELETE/UPSERT) works.
    # DUPLICATE KEY tables (the default for read-only suites) reject DELETE
    # with subqueries, which the mutate path relies on.
    def get_primary_key(self, table_name: TableName) -> str | list[str] | None:  # noqa: ARG002
        return "time"


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

        # See VERSION comment above for why this is arm64. On a real x86_64
        # host, switch to linux/amd64 to match the rest of the suite.
        parts = [
            f"docker run --platform linux/arm64 --name {self.name}-benchmark --rm -d",
            "-p 9030:9030 -p 8030:8030 -p 8040:8040",
            f"-v {meta_dir.as_posix()}:/data/deploy/starrocks/fe/meta",
            f"-v {storage_dir.as_posix()}:/data/deploy/starrocks/be/storage",
            f"-v {staging_dir.as_posix()}:/staging",
            DOCKER_IMAGE,
        ]
        return " ".join(parts)

    def wait_until_accessible(self, timeout_seconds: float = 240.0, interval_seconds: float = 2.0) -> None:
        # FE + BE startup takes ~30-60s the first time. Also need to bootstrap
        # the benchmark database. Wait until BE is registered AND alive --
        # otherwise CREATE TABLE later fails with "no available BE".
        _LOGGER.info(f"Waiting for database {self.name} (timeout: {timeout_seconds:.0f}s)...")
        deadline = perf_counter() + timeout_seconds
        attempts = 0
        bootstrap_uri = f"mysql+pymysql://{STARROCKS_USER}@{STARROCKS_HOST}:{STARROCKS_QUERY_PORT}/"

        while perf_counter() < deadline:
            attempts += 1
            try:
                engine = create_engine(bootstrap_uri)
                try:
                    with engine.connect() as con:
                        con.execute(text(f"CREATE DATABASE IF NOT EXISTS {STARROCKS_DATABASE}"))
                finally:
                    engine.dispose()

                # SHOW BACKENDS reporting Alive=true is not sufficient: the FE
                # tracks additional per-BE readiness flags (disk capacity
                # report, tablet report) and CREATE TABLE fails with
                # "backends without enough disk space" until all of them
                # arrive. Easiest robust check: actually try a probe CREATE
                # TABLE and drop it.
                probe_uri = (
                    f"mysql+pymysql://{STARROCKS_USER}@{STARROCKS_HOST}:{STARROCKS_QUERY_PORT}/{STARROCKS_DATABASE}"
                )
                engine = create_engine(probe_uri)
                try:
                    with engine.connect() as con:
                        con.execute(text("DROP TABLE IF EXISTS _probe"))
                        con.execute(
                            text(
                                "CREATE TABLE _probe (id INT NOT NULL) "
                                "PRIMARY KEY (id) DISTRIBUTED BY HASH (id) BUCKETS 1 "
                                "PROPERTIES ('replication_num' = '1')"
                            )
                        )
                        con.execute(text("DROP TABLE _probe"))
                finally:
                    engine.dispose()

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

        # PRIMARY KEY columns must be NOT NULL and must come first in the
        # column list. Reorder so they appear up front when a PK is given.
        pk_list: list[str]
        if isinstance(primary_key, str):
            pk_list = [primary_key]
        elif isinstance(primary_key, list) and primary_key:
            pk_list = list(primary_key)
        else:
            pk_list = []

        if pk_list:
            for c in pk_list:
                if c not in not_null:
                    not_null = [*not_null, c]
            ordered = [c for c in pk_list if c in schema] + [c for c in schema if c not in pk_list]
        else:
            ordered = list(schema.names())

        columns: list[str] = []
        for name in ordered:
            dtype = schema[name]
            sr_type = get_starrocks_type(dtype)
            null_clause = "NOT NULL" if name in not_null else "NULL"
            columns.append(f"`{name}` {sr_type} {null_clause}")

        columns_sql = ",\n  ".join(columns)

        # PRIMARY KEY tables support UPDATE/DELETE/UPSERT (used by the
        # time_series mutate suite); DUPLICATE KEY tables are append-only
        # columnar and cheaper for read-heavy ClickBench/RTABench shapes.
        if pk_list:
            key_clause = "PRIMARY KEY"
            key_cols = ", ".join(f"`{c}`" for c in pk_list)
        else:
            key_clause = "DUPLICATE KEY"
            key_cols = f"`{ordered[0]}`"

        return (
            f"CREATE TABLE `{table}` (\n  {columns_sql}\n)\n"
            f"{key_clause} ({key_cols})\n"
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

    def _ingest_parquet(self, table: TableName, parquet_path: Path, columns: list[str]) -> None:
        # StarRocks STREAM LOAD only supports CSV/JSON; for parquet we use the
        # FILES table function. The BE reads the parquet from its own
        # filesystem -- we mount {temp}/starrocks/data -> /staging in the
        # container, so the file path inside the BE is /staging/<name>.
        in_container_path = f"/staging/{parquet_path.name}"
        col_list = ", ".join(f"`{c}`" for c in columns)
        sql = (
            f"INSERT INTO `{table}` ({col_list}) "
            f"SELECT {col_list} FROM FILES("
            f'"path" = "file://{in_container_path}", '
            f'"format" = "parquet"'
            f")"
        )
        size = parquet_path.stat().st_size
        _LOGGER.info(f"INSERT FROM FILES {parquet_path.name} ({size / 1e6:.1f} MB) → {table}")
        con = self.connect()
        with self.record_query_execution(sql):
            con.execute(text(sql))
        tracked_commit(con)

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

        # Eagerly materialize then write -- sink_parquet has shown empty/truncated
        # output on some Apple Silicon arm64 + large LazyFrame combinations,
        # which surfaces as "Parquet magic bytes not found" inside StarRocks.
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        df.write_parquet(parquet_path)
        # Same intermittent host -> container bind-mount visibility issue
        # questdb hits: the container can briefly see a 0-length file even
        # after write_parquet has returned. 100 ms is enough in practice.
        sleep(0.1)

        try:
            self._ingest_parquet(table, parquet_path, list(schema.names()))
        finally:
            parquet_path.unlink(missing_ok=True)

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:  # noqa: ARG002
        # PRIMARY KEY tables (used by the time_series suite) auto-upsert on
        # INSERT: matching keys overwrite, new ones append. So we just go
        # through the regular insert path. DUPLICATE KEY tables cannot
        # upsert, so suites that need upsert must use PRIMARY KEY tables.
        self.insert(df, table)

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

    @property
    def time_series(self) -> StarRocksTimeSeries:
        return StarRocksTimeSeries(db=self)

    @property
    def clickbench(self) -> StarRocksClickbench:
        return StarRocksClickbench(db=self)
