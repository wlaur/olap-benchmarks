import logging
import subprocess
import uuid
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar, cast

import connectorx
import polars as pl
from sqlalchemy import Connection, create_engine, text

from ...settings import REPO_ROOT, SETTINGS, DatabaseName, TableName
from ...suites.clickbench.config import Clickbench
from ...suites.rtabench.config import RTABench
from ...suites.time_series.config import (
    TIME_SERIES_DATASET_SIZES,
    MutateStep,
    TimeSeries,
    get_time_series_input_files,
    get_time_series_table_name,
)
from .. import Database
from ..postgres import generate_create_table_sql, table_exists
from ..utils import tracked_commit

_LOGGER = logging.getLogger(__name__)

VERSION = "2.25.0"

DOCKER_IMAGE = f"timescale/timescaledb:{VERSION}-pg18"
TIMESCALEDB_CONNECTION_STRING = "postgresql://postgres:password@localhost:5432/postgres"


class TimescaleRTABench(RTABench["TimescaleDB"]):
    def compress_tables(self) -> None:
        conn = self.db.connect()

        statement = "SELECT show_chunks('order_events')"
        with self.db.record_query_execution(statement):
            result = conn.execute(text(statement))
        chunks = [row[0] for row in result.fetchall()]

        if not chunks:
            raise RuntimeError

        _LOGGER.info(f"Found {len(chunks)} chunks to compress.")

        for idx, chunk in enumerate(chunks):
            _LOGGER.info(f"Compressing chunk {idx + 1:_}/{len(chunks):_}: {chunk}")
            statement = f"select compress_chunk('{chunk}'::regclass)"
            with self.db.record_query_execution(statement):
                conn.execute(text(statement))

        tracked_commit(conn)

        con = self.db.connect(reconnect=True)
        statement = "vacuum freeze analyze orders"
        with self.db.record_query_execution(statement):
            con.execution_options(isolation_level="AUTOCOMMIT").execute(text(statement))

        con = self.db.connect(reconnect=True)
        statement = "vacuum freeze analyze order_events"
        with self.db.record_query_execution(statement):
            con.execution_options(isolation_level="AUTOCOMMIT").execute(text(statement))

    def populate(self, restart: bool = True) -> None:
        super().populate(restart=False)

        with self.db.phase_context("compress"):
            self.compress_tables()

        if restart:
            self.db.restart_event()


class TimescaleClickbench(Clickbench["TimescaleDB"]):
    def compress_table(self) -> None:
        con = self.db.connect()

        statement = "SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('hits') i"
        with self.db.record_query_execution(statement):
            con.execute(text(statement))

        tracked_commit(con)

        con = self.db.connect(reconnect=True)
        statement = "vacuum freeze analyze hits"
        with self.db.record_query_execution(statement):
            con.execution_options(isolation_level="AUTOCOMMIT").execute(text(statement))

    def populate(self, restart: bool = True) -> None:
        super().populate(restart=False)

        with self.db.phase_context("compress"):
            self.compress_table()

        if restart:
            self.db.restart_event()


class TimescaleTimeSeries(TimeSeries["TimescaleDB"]):
    EAV_TABLES: ClassVar[frozenset[TableName]] = frozenset({"data_wide", "data_large"})

    PRE_INSERT_SCHEMA_FILES: ClassVar[dict[str, str]] = {
        "data_tall": "tall_pre_insert.sql",
        "data_wide": "wide_pre_insert.sql",
        "data_large": "large_pre_insert.sql",
    }

    # Chunk_time_interval per table -- must match the create_hypertable() calls
    # in the *_pre_insert.sql files. EAV inserts are batched at this granularity
    # so each just-completed chunk can be compressed before the next batch
    # starts; otherwise data_large accumulates ~200 GB of uncompressed EAV
    # heap before any compression runs.
    EAV_CHUNK_INTERVAL_DAYS: ClassVar[Mapping[str, int]] = {
        "data_wide": 1,
        "data_large": 14,
    }

    def expected_table_row_counts(self) -> Mapping[TableName, int]:
        counts: dict[TableName, int] = {}
        for size, (n_rows, n_cols) in TIME_SERIES_DATASET_SIZES.items():
            table_name = get_time_series_table_name(size)
            counts[table_name] = n_rows * n_cols if table_name in self.EAV_TABLES else n_rows
        return counts

    def get_primary_key(self, table_name: TableName) -> str | list[str] | None:
        _ = table_name
        return None

    def get_not_null(self, table_name: TableName) -> str | list[str] | None:
        if table_name in self.EAV_TABLES:
            return ["time", "metric_name"]
        return "time"

    @staticmethod
    def _wide_to_eav(df: pl.DataFrame) -> pl.DataFrame:
        metric_cols = [c for c in df.columns if c != "time"]
        bool_cols = [c for c, t in df.schema.items() if c != "time" and t == pl.Boolean]
        casted = df.with_columns([pl.col(c).cast(pl.Float32) for c in bool_cols]) if bool_cols else df
        return casted.unpivot(
            index="time",
            on=metric_cols,
            variable_name="metric_name",
            value_name="value",
        )

    def _eav_create_table(self, table_name: TableName) -> None:
        con = self.db.connect()
        statement = (
            f'CREATE TABLE "{table_name}" ("time" TIMESTAMP NOT NULL, "metric_name" TEXT NOT NULL, "value" REAL)'
        )
        with self.db.record_query_execution(statement):
            con.execute(text(statement))
        tracked_commit(con)
        _LOGGER.info(f"Created EAV table {table_name}")

    def _compress_completed_chunks(self, table_name: TableName, before: datetime) -> None:
        """Compress all uncompressed chunks of `table_name` whose range_end is
        at or before `before`. Called after each EAV batch insert so the
        uncompressed footprint never grows past the chunk currently being
        written."""
        con = self.db.connect(reconnect=True)
        before_str = before.strftime("%Y-%m-%d %H:%M:%S")
        list_sql = (
            "SELECT format('%I.%I', chunk_schema, chunk_name) "
            "FROM timescaledb_information.chunks "
            f"WHERE hypertable_name = '{table_name}' AND NOT is_compressed "
            f"AND range_end <= '{before_str}' "
            "ORDER BY range_end"
        )
        with self.db.record_query_execution(list_sql):
            rows = con.execute(text(list_sql)).fetchall()
        for (chunk_name,) in rows:
            stmt = f"SELECT compress_chunk('{chunk_name}'::regclass, if_not_compressed => true)"
            with self.db.record_query_execution(stmt):
                con.execute(text(stmt))
            tracked_commit(con)
        if rows:
            _LOGGER.info(f"Compressed {len(rows)} chunk(s) of {table_name} (range_end <= {before_str})")

    def _eav_chunked_insert(self, table_name: TableName, fpath: Path) -> None:
        """Insert EAV in time-aligned batches matching the hypertable's
        chunk_time_interval, compressing just-completed chunks after each
        batch. This caps peak uncompressed disk to ~1 chunk."""
        schema = cast(pl.Schema, pl.read_parquet_schema(fpath))
        _ = [c for c in schema if c != "time"]  # noqa: F841 -- documents schema shape

        days = self.EAV_CHUNK_INTERVAL_DAYS[table_name]
        interval = timedelta(days=days)

        bounds = pl.scan_parquet(fpath).select(pl.min("time").alias("mn"), pl.max("time").alias("mx")).collect()
        if bounds.height == 0 or bounds.row(0)[0] is None:
            return
        min_t = cast(datetime, bounds.row(0)[0])
        max_t = cast(datetime, bounds.row(0)[1])

        # Align cur to TimescaleDB chunk boundaries (chunks start at
        # floor((t - epoch) / interval) * interval).
        epoch = datetime(1970, 1, 1)
        secs = interval.total_seconds()
        aligned = int((min_t - epoch).total_seconds() // secs) * secs
        cur = epoch + timedelta(seconds=aligned)

        batch_idx = 0
        while cur <= max_t:
            end = cur + interval
            batch_wide = pl.scan_parquet(fpath).filter((pl.col("time") >= cur) & (pl.col("time") < end)).collect()
            if batch_wide.height > 0:
                batch_idx += 1
                batch_eav = self._wide_to_eav(batch_wide)
                self.db.insert(batch_eav, table_name)
                _LOGGER.info(
                    f"Inserted EAV batch {batch_idx} for {table_name}: "
                    f"{cur} → {end} (wide={batch_wide.shape[0]:_}, eav={batch_eav.shape[0]:_})"
                )
                self._compress_completed_chunks(table_name, end)
            cur = end

    def compress_tables(self) -> None:
        for table_name in get_time_series_input_files():
            con = self.db.connect(reconnect=True)
            statement = f"SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('{table_name}') i"
            with self.db.record_query_execution(statement):
                con.execute(text(statement))
            tracked_commit(con)
            _LOGGER.info(f"Compressed table {table_name}")

            con = self.db.connect(reconnect=True)
            statement = f"vacuum freeze analyze {table_name}"
            with self.db.record_query_execution(statement):
                con.execution_options(isolation_level="AUTOCOMMIT").execute(text(statement))
            _LOGGER.info(f"Vacuumed table {table_name}")

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        input_files = get_time_series_input_files()

        # 1. Create empty tables (regular wide for tall, EAV for wide/large).
        for table_name, fpath in input_files.items():
            if table_name in self.EAV_TABLES:
                self._eav_create_table(table_name)
            else:
                schema = cast(pl.Schema, pl.read_parquet_schema(fpath))
                self.db.create_table(schema, table_name, primary_key=None, not_null="time")

        # 2. Convert each table to a hypertable + columnstore config (empty
        #    table → no migrate_data needed, much faster than retro-conversion).
        for table_name, sql_file in self.PRE_INSERT_SCHEMA_FILES.items():
            with self.db.phase_context("create_hypertable", table_name=table_name):
                self.db.execute_schema_file(
                    REPO_ROOT / "olap_benchmarks/suites/time_series/schemas/timescaledb" / sql_file
                )

        # 3. Ingest data.
        for table_name, fpath in input_files.items():
            with self.db.phase_context("insert", table_name=table_name):
                if table_name in self.EAV_TABLES:
                    self._eav_chunked_insert(table_name, fpath)
                else:
                    df = pl.scan_parquet(fpath)
                    self.db.insert(df, table_name, primary_key=None, not_null="time")
            _LOGGER.info(f"Inserted {table_name} for {self.name}")

        # 4. Compress all chunks.
        with self.db.phase_context("compress"):
            self.compress_tables()

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        if restart:
            self.db.restart_event()

    def _generate_insert_data(self, step: MutateStep, seed: int) -> pl.DataFrame:
        df_wide = super()._generate_insert_data(step, seed)
        if step.table in self.EAV_TABLES:
            return self._wide_to_eav(df_wide)
        return df_wide

    def _generate_upsert_data(self, step: MutateStep, seed: int) -> pl.DataFrame:
        df_wide = super()._generate_upsert_data(step, seed)
        if step.table in self.EAV_TABLES:
            return self._wide_to_eav(df_wide)
        return df_wide

    def _apply_upsert(self, step: MutateStep, df: pl.DataFrame) -> None:
        # None of the time_series tables here have a unique constraint on
        # `time` (data_tall is wide with no PK; data_wide/data_large are EAV
        # with no (time, metric_name) unique). ON CONFLICT therefore cannot
        # bind, so we emulate upsert with DELETE-by-time + INSERT for every
        # table.
        keys = df.select("time").unique()
        self.db.delete(step.table, primary_key="time", keys=keys)
        primary_key = None if step.table in self.EAV_TABLES else self.get_primary_key(step.table)
        not_null = self.get_not_null(step.table)
        self.db.insert(df, step.table, primary_key=primary_key, not_null=not_null)


class TimescaleDB(Database):
    name: DatabaseName = "timescaledb"
    version: str = VERSION

    connection_string: str = TIMESCALEDB_CONNECTION_STRING

    @property
    def start(self) -> str:
        (SETTINGS.temporary_directory / "timescaledb/data").mkdir(exist_ok=True, parents=True)

        # macOS Finder writes .DS_Store into bind-mounted dirs as soon as it's
        # browsed; PG's initdb refuses to run on a non-empty PGDATA, so scrub
        # macOS metadata before handing the dir over.
        for junk in self.database_directory.glob(".DS_Store"):
            junk.unlink(missing_ok=True)
        for junk in self.database_directory.glob("._*"):
            junk.unlink(missing_ok=True)

        parts = [
            f"docker run --platform linux/amd64 --name {self.name}-benchmark --rm -d -p 5432:5432",
            f"-v {self.database_directory.as_posix()}:/var/lib/postgresql/data/",
            "-e POSTGRES_PASSWORD=password",
            "-e PGDATA=/var/lib/postgresql/data/",
            DOCKER_IMAGE,
        ]

        return " ".join(parts)

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
        # fetch_python is fastest for small result sets
        # fetch_connectorx might be better for large results and simple queries
        # (e.g. "select time, col_23 from data order by time")
        # fetch_polars is slightly slower than fetch_connectorx

        # schemas do not match exactly between these (i32 vs i64 for example)
        return self.fetch_python(query, schema)

    def get_table_names(self) -> set[TableName]:
        df = self.fetch(
            "select table_name from information_schema.tables "
            "where table_schema = 'public' and table_type = 'BASE TABLE'",
            schema={"table_name": pl.String},
        )
        return set(df.get_column("table_name").to_list())

    def fetch_python(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
    ) -> pl.DataFrame:
        # escape literal ":" to avoid SQLAlchemy interpreting bind params
        # bind params are not supported in this method
        sql = query.strip().removesuffix(";").replace(":", r"\:")
        with self.record_query_execution(query):
            result = self.connect().execute(text(sql))

        columns = result.keys()
        rows = result.fetchall()

        if not rows:
            if schema:
                return pl.DataFrame(schema=cast(pl.Schema, schema))
            return pl.DataFrame({col: [] for col in columns})

        df = pl.DataFrame({col: [row[idx] for row in rows] for idx, col in enumerate(columns)})

        if schema is not None:
            df = df.cast(cast(pl.Schema, schema))

        return df

    def fetch_connectorx(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
    ) -> pl.DataFrame:
        with self.record_query_execution(query):
            df = cast(
                pl.DataFrame,
                cast(Any, connectorx).read_sql(
                    TIMESCALEDB_CONNECTION_STRING, query.strip().removesuffix(";"), return_type="polars"
                ),
            )

        if schema is not None:
            df = df.cast(cast(pl.Schema, schema))

        return df

    def fetch_polars(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
    ) -> pl.DataFrame:
        # (maybe) emits a separate "select ... limit 1" query to determine the output schema
        # avoid doing this for complex queries with small result sizes
        # not clear if postgres actually does this, could check source if this is important to know
        # engine="adbc" is slower that "connectorx"
        with self.record_query_execution(query):
            df = pl.read_database_uri(
                query.strip().removesuffix(";"),
                TIMESCALEDB_CONNECTION_STRING,
                engine="connectorx",
            )

        if schema is not None:
            df = df.cast(cast(pl.Schema, schema))

        return df

    def create_table(
        self,
        schema: pl.Schema,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        con = self.connect()

        create_sql = generate_create_table_sql(table, schema, primary_key, not_null)
        with self.record_query_execution(create_sql):
            con.execute(text(create_sql))
        tracked_commit(con)
        _LOGGER.info(f"Created table {table} with {len(schema):_} columns")

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        con = self.connect()

        schema = df.schema if isinstance(df, pl.DataFrame) else df.collect_schema()

        if not table_exists(con, table):
            self.create_table(schema, table, primary_key, not_null)

        temp_dir = SETTINGS.temporary_directory / "timescaledb/data"

        temp_file = temp_dir / f"{table}_{uuid.uuid4().hex}.csv"
        temp_file_str = temp_file.resolve().as_posix()

        if isinstance(df, pl.LazyFrame):
            df.sink_csv(temp_file)
            _LOGGER.info("Inserting from staged CSV using timescaledb-parallel-copy")
        else:
            df.write_csv(temp_file)
            _LOGGER.info(
                f"Inserting dataset with shape ({df.shape[0]:_}, {df.shape[1]:_}) using timescaledb-parallel-copy"
            )

        db_host = "localhost"
        db_name = "postgres"
        db_user = "postgres"
        db_password = "password"
        db_port = "5432"

        connection_string = f"host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_password}"

        # install timescaledb-parallel-copy first
        # on macos: brew tap timescale/tap && brew install timescaledb-tools
        command = [
            "timescaledb-parallel-copy",
            "--connection",
            connection_string,
            "--table",
            table,
            "--file",
            temp_file_str,
            "--workers",
            # possible that using more workers could speed things up, but this is not linear
            "12",
            "--batch-size",
            "50000",
            "--skip-header",
        ]
        copy_sql = f"COPY {table} FROM '{temp_file_str}' WITH (FORMAT csv, HEADER true)"

        try:
            with self.record_query_execution(copy_sql):
                subprocess.run(command, capture_output=True, text=True, check=True)
        finally:
            temp_file.unlink()

    def _copy_csv_to_table(self, con: Connection, table: str, csv_path: Path) -> None:
        raw_conn = con.connection.dbapi_connection
        assert raw_conn is not None
        cursor = raw_conn.cursor()
        copy_sql = f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true)"
        with open(csv_path) as f, self.record_query_execution(copy_sql):
            cursor.copy_expert(copy_sql, f)

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        primary_keys = [primary_key] if isinstance(primary_key, str) else primary_key

        if not primary_keys:
            raise ValueError("primary_key must be a non-empty string or list of strings")

        for pk in primary_keys:
            if pk not in df.columns:
                raise ValueError(f"Primary key column '{pk}' not found in DataFrame columns")

        con = self.connect()
        pk_cols = ", ".join(f'"{pk}"' for pk in primary_keys)

        staging_table = f"_staging_{table}_{uuid.uuid4().hex[:8]}"
        statement = f"CREATE TEMP TABLE {staging_table} (LIKE {table} INCLUDING DEFAULTS)"
        with self.record_query_execution(statement):
            con.execute(text(statement))

        temp_dir = SETTINGS.temporary_directory / "timescaledb/data"
        temp_file = temp_dir / f"{table}_upsert_{uuid.uuid4().hex}.csv"

        try:
            df.write_csv(temp_file)
            self._copy_csv_to_table(con, staging_table, temp_file)
        finally:
            temp_file.unlink(missing_ok=True)

        statement = "SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0"
        with self.record_query_execution(statement):
            con.execute(text(statement))
        all_columns = ", ".join(f'"{col}"' for col in df.columns)
        non_key_columns = [col for col in df.columns if col not in primary_keys]

        if non_key_columns:
            set_clause = ", ".join(f'"{col}" = EXCLUDED."{col}"' for col in non_key_columns)
            on_conflict_clause = f"ON CONFLICT ({pk_cols}) DO UPDATE SET {set_clause}"
        else:
            on_conflict_clause = f"ON CONFLICT ({pk_cols}) DO NOTHING"

        statement = (
            f"INSERT INTO {table} ({all_columns}) SELECT {all_columns} FROM {staging_table} {on_conflict_clause}"
        )
        with self.record_query_execution(statement):
            con.execute(text(statement))

        statement = f"DROP TABLE {staging_table}"
        with self.record_query_execution(statement):
            con.execute(text(statement))
        tracked_commit(con)

        _LOGGER.info(f"Upserted {df.shape[0]:_} rows into {table}")

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        primary_keys = [primary_key] if isinstance(primary_key, str) else primary_key

        if not primary_keys:
            raise ValueError("primary_key must be a non-empty string or list of strings")

        con = self.connect()

        staging_table = f"_staging_del_{table}_{uuid.uuid4().hex[:8]}"
        pk_cols = ", ".join(f'"{pk}"' for pk in primary_keys)

        statement = f"CREATE TEMP TABLE {staging_table} AS SELECT {pk_cols} FROM {table} WHERE false"
        with self.record_query_execution(statement):
            con.execute(text(statement))

        temp_dir = SETTINGS.temporary_directory / "timescaledb/data"
        temp_file = temp_dir / f"{table}_delete_{uuid.uuid4().hex}.csv"

        try:
            keys.select(primary_keys).write_csv(temp_file)
            self._copy_csv_to_table(con, staging_table, temp_file)
        finally:
            temp_file.unlink(missing_ok=True)

        statement = "SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0"
        with self.record_query_execution(statement):
            con.execute(text(statement))
        sql = f"DELETE FROM {table} WHERE ({pk_cols}) IN (SELECT {pk_cols} FROM {staging_table})"
        with self.record_query_execution(sql):
            con.execute(text(sql))
        statement = f"DROP TABLE {staging_table}"
        with self.record_query_execution(statement):
            con.execute(text(statement))
        tracked_commit(con)

        _LOGGER.info(f"Deleted rows from {table} by primary key")

    @property
    def rtabench(self) -> TimescaleRTABench:
        return TimescaleRTABench(db=self)

    @property
    def clickbench(self) -> TimescaleClickbench:
        return TimescaleClickbench(db=self)

    @property
    def time_series(self) -> TimescaleTimeSeries:
        return TimescaleTimeSeries(db=self)
