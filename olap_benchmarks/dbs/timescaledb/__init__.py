import logging
import subprocess
import uuid
from collections.abc import Mapping
from typing import Any, ClassVar, cast

import connectorx
import polars as pl
from sqlalchemy import Connection, create_engine, text

from ...settings import REPO_ROOT, SETTINGS, DatabaseName, TableName
from ...suites.clickbench.config import Clickbench
from ...suites.rtabench.config import RTABench
from ...suites.time_series.config import TimeSeries, get_time_series_input_files
from .. import Database
from ..postgres import generate_create_table_sql, table_exists

_LOGGER = logging.getLogger(__name__)

VERSION = "2.25.0"

DOCKER_IMAGE = f"timescale/timescaledb:{VERSION}-pg18"
TIMESCALEDB_CONNECTION_STRING = "postgresql://postgres:password@localhost:5432/postgres"


class TimescaleRTABench(RTABench["TimescaleDB"]):
    def compress_tables(self) -> None:
        conn = self.db.connect()

        result = conn.execute(text("SELECT show_chunks('order_events')"))
        chunks = [row[0] for row in result.fetchall()]

        if not chunks:
            raise RuntimeError

        _LOGGER.info(f"Found {len(chunks)} chunks to compress.")

        for idx, chunk in enumerate(chunks):
            _LOGGER.info(f"Compressing chunk {idx + 1:_}/{len(chunks):_}: {chunk}")
            conn.execute(text(f"select compress_chunk('{chunk}'::regclass)"))

        conn.commit()

        con = self.db.connect(reconnect=True)
        con.execution_options(isolation_level="AUTOCOMMIT").execute(text("vacuum freeze analyze orders"))

        con = self.db.connect(reconnect=True)
        con.execution_options(isolation_level="AUTOCOMMIT").execute(text("vacuum freeze analyze order_events"))

    def populate(self, restart: bool = True) -> None:
        super().populate(restart=False)

        with self.db.phase_context("compress"):
            self.compress_tables()

        if restart:
            self.db.restart_event()


class TimescaleClickbench(Clickbench["TimescaleDB"]):
    def compress_table(self) -> None:
        con = self.db.connect()

        con.execute(text("SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('hits') i"))

        con.commit()

        con = self.db.connect(reconnect=True)
        con.execution_options(isolation_level="AUTOCOMMIT").execute(text("vacuum freeze analyze hits"))

    def populate(self, restart: bool = True) -> None:
        super().populate(restart=False)

        with self.db.phase_context("compress"):
            self.compress_table()

        if restart:
            self.db.restart_event()


class TimescaleTimeSeries(TimeSeries["TimescaleDB"]):
    POST_INSERT_SCHEMA_FILES: ClassVar[dict[str, str]] = {
        "data_tall": "tall_post_insert.sql",
        "data_wide": "wide_post_insert.sql",
        "data_large": "large_post_insert.sql",
        "data_wide_eav": "wide_eav_post_insert.sql",
    }

    # wide columnar tables can exceed PostgreSQL's max tuple size for compressed rows
    # on TimescaleDB 2.25.0 / PG 18.
    SKIP_COMPRESS: ClassVar[set[str]] = {"data_large", "data_wide"}

    def compress_tables(self) -> None:
        for table_name in get_time_series_input_files():
            if table_name in self.SKIP_COMPRESS:
                _LOGGER.warning(f"Skipping compression for {table_name} (exceeds PG max tuple size)")
            else:
                con = self.db.connect(reconnect=True)
                con.execute(
                    text(f"SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('{table_name}') i")
                )
                con.commit()
                _LOGGER.info(f"Compressed table {table_name}")

            con = self.db.connect(reconnect=True)
            con.execution_options(isolation_level="AUTOCOMMIT").execute(text(f"vacuum freeze analyze {table_name}"))

            _LOGGER.info(f"Vacuumed table {table_name}")

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        input_files = get_time_series_input_files()

        for table_name, fpath in input_files.items():
            primary_key = self.get_primary_key(table_name)
            not_null = self.get_not_null(table_name)

            schema = cast(pl.Schema, pl.read_parquet_schema(fpath))

            self.db.create_table(schema, table_name, primary_key, not_null)

        for table_name, fpath in input_files.items():
            primary_key = self.get_primary_key(table_name)
            not_null = self.get_not_null(table_name)

            df = pl.scan_parquet(fpath)

            _LOGGER.info(f"Streaming {table_name} from parquet to staged CSV")

            with self.db.phase_context("insert", table_name=table_name):
                self.db.insert(df, table_name, primary_key=primary_key, not_null=not_null, **self.populate_kwargs)
                _LOGGER.info(f"Inserted {table_name} for {self.name}")

        for table_name, schema_file in self.POST_INSERT_SCHEMA_FILES.items():
            with self.db.phase_context("create_hypertable", table_name=table_name):
                self.db.execute_schema_file(
                    REPO_ROOT / "olap_benchmarks/suites/time_series/schemas/timescaledb" / schema_file
                )

        _LOGGER.info(f"Inserted all time_series tables for {self.name}")

        with self.db.phase_context("compress"):
            self.compress_tables()

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        if restart:
            self.db.restart_event()


class TimescaleDB(Database):
    name: DatabaseName = "timescaledb"
    version: str = VERSION

    connection_string: str = TIMESCALEDB_CONNECTION_STRING

    @property
    def start(self) -> str:
        (SETTINGS.temporary_directory / "timescaledb/data").mkdir(exist_ok=True, parents=True)

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
        self._connection = engine.connect()

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
        result = self.connect().execute(text(query.strip().removesuffix(";").replace(":", r"\:")))

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
        df = pl.read_database_uri(query.strip().removesuffix(";"), TIMESCALEDB_CONNECTION_STRING, engine="connectorx")

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
        con.execute(text(create_sql))
        con.commit()
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

        try:
            subprocess.run(command, capture_output=True, text=True, check=True)
        finally:
            temp_file.unlink()

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        raise NotImplementedError

    @property
    def rtabench(self) -> TimescaleRTABench:
        return TimescaleRTABench(db=self)

    @property
    def clickbench(self) -> TimescaleClickbench:
        return TimescaleClickbench(db=self)

    @property
    def time_series(self) -> TimescaleTimeSeries:
        return TimescaleTimeSeries(db=self)
