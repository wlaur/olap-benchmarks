import logging
import subprocess
import uuid
from collections.abc import Mapping
from typing import Literal, cast

import connectorx
import polars as pl
from sqlalchemy import Connection, create_engine, text

from ...settings import REPO_ROOT, SETTINGS, TableName
from ...suites.time_series.config import get_time_series_input_files
from .. import Database

_LOGGER = logging.getLogger(__name__)

DOCKER_IMAGE = "timescale/timescaledb:2.21.1-pg16"
TIMESCALEDB_CONNECTION_STRING = "postgresql://postgres:password@localhost:5432/postgres"


def polars_to_postgres_type(dtype: pl.DataType) -> str:
    if dtype == pl.Int64:
        return "BIGINT"
    elif dtype == pl.Int16:
        return "SMALLINT"
    elif dtype == pl.Int32:
        return "INTEGER"
    elif dtype == pl.Float64:
        return "DOUBLE PRECISION"
    elif dtype == pl.Float32:
        return "REAL"
    elif dtype == pl.Boolean:
        return "BOOLEAN"
    elif dtype == pl.Utf8:
        return "TEXT"
    elif dtype == pl.Date:
        return "DATE"
    elif isinstance(dtype, pl.Datetime):
        return "TIMESTAMP WITHOUT TIME ZONE"
    else:
        _LOGGER.warning(f"Falling back to type JSONB for Polars dtype {dtype}")
        return "JSONB"


def generate_create_table_sql(
    table: str, schema: pl.Schema, primary_key: str | list[str] | None = None, not_null: str | list[str] | None = None
) -> str:
    if not_null is None:
        not_null = []

    if isinstance(not_null, str):
        not_null = [not_null]

    columns: list[str] = []
    for name, dtype in schema.items():
        pg_type = polars_to_postgres_type(dtype)
        columns.append(f'"{name}" {pg_type} {"not null" if name in not_null else ""}')

    if primary_key:
        if isinstance(primary_key, str):
            pk = f'primary key ("{primary_key}")'
        else:
            pk_cols = ", ".join(f'"{col}"' for col in primary_key)
            pk = f"primary key ({pk_cols})"

        columns.append(pk)

    columns_sql = ",\n  ".join(columns)
    return f'create table "{table}" (\n  {columns_sql}\n);'


def table_exists(connection: Connection, table: str) -> bool:
    dbapi_con = connection._dbapi_connection
    assert dbapi_con is not None
    cursor = dbapi_con.cursor()
    cursor.execute("SELECT to_regclass(%s);", (f'public."{table}"',))
    result = cursor.fetchone()
    return result[0] is not None  # type: ignore[index]


class TimescaleDB(Database):
    name: Literal["timescaledb"] = "timescaledb"
    connection_string: str = TIMESCALEDB_CONNECTION_STRING

    @property
    def start(self) -> str:
        (SETTINGS.database_directory / "timescaledb").mkdir(exist_ok=True)
        (SETTINGS.temporary_directory / "timescaledb/data").mkdir(exist_ok=True, parents=True)

        parts = [
            f"docker run --platform linux/amd64 --name {self.name}-benchmark --rm -d -p 5432:5432",
            f"-v {SETTINGS.database_directory.as_posix()}/timescaledb:/var/lib/postgresql/data/",
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
                return pl.DataFrame(schema=schema)
            return pl.DataFrame({col: [] for col in columns})

        df = pl.DataFrame({col: [row[idx] for row in rows] for idx, col in enumerate(columns)})

        if schema is not None:
            df = df.cast(schema)  # type: ignore[arg-type]

        return df

    def fetch_connectorx(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
    ) -> pl.DataFrame:
        df = connectorx.read_sql(TIMESCALEDB_CONNECTION_STRING, query.strip().removesuffix(";"), return_type="polars")

        if schema is not None:
            df = df.cast(schema)  # type: ignore[arg-type]

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
            df = df.cast(schema)  # type: ignore[arg-type]

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
        df: pl.DataFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        con = self.connect()

        if not table_exists(con, table):
            self.create_table(df.schema, table, primary_key, not_null)

        temp_dir = SETTINGS.temporary_directory / "timescaledb/data"

        temp_file = temp_dir / f"{table}_{uuid.uuid4().hex}.csv"
        temp_file_str = temp_file.resolve().as_posix()

        df.write_csv(temp_file)

        _LOGGER.info(f"Inserting dataset with shape ({df.shape[0]:_}, {df.shape[1]:_}) using timescaledb-parallel-copy")

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

    def compress_rtabench_tables(self) -> None:
        conn = self.connect()

        result = conn.execute(text("SELECT show_chunks('order_events')"))
        chunks = [row[0] for row in result.fetchall()]

        if not chunks:
            raise RuntimeError

        _LOGGER.info(f"Found {len(chunks)} chunks to compress.")

        for idx, chunk in enumerate(chunks):
            _LOGGER.info(f"Compressing chunk {idx + 1:_}/{len(chunks):_}: {chunk}")
            conn.execute(text(f"select compress_chunk('{chunk}'::regclass)"))

        conn.commit()

        con = self.connect(reconnect=True)
        con.execution_options(isolation_level="AUTOCOMMIT").execute(text("vacuum freeze analyze orders"))

        con = self.connect(reconnect=True)
        con.execution_options(isolation_level="AUTOCOMMIT").execute(text("vacuum freeze analyze order_events"))

    def populate_rtabench(self, restart: bool = True) -> None:
        super().populate_rtabench(restart=False)

        with self.event_context("compress"):
            self.compress_rtabench_tables()

        if restart:
            self.restart_event()

    def compress_clickbench_table(self) -> None:
        con = self.connect()

        con.execute(text("SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('hits') i"))

        con.commit()

        con = self.connect(reconnect=True)
        con.execution_options(isolation_level="AUTOCOMMIT").execute(text("vacuum freeze analyze hits"))

    def populate_clickbench(self, restart: bool = True) -> None:
        super().populate_clickbench(restart=False)

        with self.event_context("compress"):
            self.compress_clickbench_table()

        if restart:
            self.restart_event()

    def compress_time_series_tables(self) -> None:
        for table_name in get_time_series_input_files():
            con = self.connect()

            con.execute(text(f"SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('{table_name}') i"))

            con.commit()

            con = self.connect(reconnect=True)
            con.execution_options(isolation_level="AUTOCOMMIT").execute(text(f"vacuum freeze analyze {table_name}"))

            _LOGGER.info(f"Compressed and vacuumed table {table_name}")

    def populate_time_series(self, restart: bool = True) -> None:
        # need to define parts of the schema and insert data in a specific order

        self.execute_schema_file(REPO_ROOT / "tsdb_benchmarks/suites/time_series/schemas/timescaledb/eav.sql")

        input_files = get_time_series_input_files()

        # wide tables are created dynamically, eav tables already exist at this point
        for table_name, fpath in input_files.items():
            if "eav" in table_name:
                continue

            primary_key = self.get_time_series_primary_key(table_name)
            not_null = self.get_time_series_not_null(table_name)

            schema = cast(pl.Schema, pl.read_parquet_schema(fpath))

            self.create_table(schema, table_name, primary_key, not_null)

        self.execute_schema_file(REPO_ROOT / "tsdb_benchmarks/suites/time_series/schemas/timescaledb/wide.sql")

        for table_name, fpath in input_files.items():
            df = pl.read_parquet(fpath)

            with self.event_context(f"insert_{table_name}"):
                self.insert(
                    df, table_name, primary_key=primary_key, not_null=not_null, **self.time_series_populate_kwargs
                )
                _LOGGER.info(f"Inserted {table_name} for {self.name}")

        _LOGGER.info(f"Inserted all time_series tables for {self.name}")

        with self.event_context("compress"):
            self.compress_time_series_tables()

        if restart:
            self.restart_event()
