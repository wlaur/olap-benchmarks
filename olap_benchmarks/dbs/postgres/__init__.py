import logging
import subprocess
import uuid
from collections.abc import Mapping
from typing import Literal

import connectorx
import polars as pl
from sqlalchemy import Connection, create_engine, text

from ...settings import SETTINGS, TableName
from ...suites.rtabench.config import RTABench
from ...suites.time_series.config import TimeSeries
from .. import Database

_LOGGER = logging.getLogger(__name__)

VERSION = "16.9"

DOCKER_IMAGE = f"postgres:{VERSION}"
POSTGRES_CONNECTION_STRING = "postgresql://postgres:password@localhost:5433/postgres"


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


class PostgresRTABench(RTABench):
    def index_tables(self) -> None:
        con = self.db.connect()

        con.execute(text("CREATE INDEX orders_customer_id_index ON orders (customer_id);"))
        con.execute(text("CREATE INDEX order_events_order_id_index ON order_events (order_id);"))
        con.execute(text("CREATE INDEX order_events_event_type_index ON order_events (event_type);"))

        con.commit()

    def populate(self, restart: bool = True) -> None:
        super().populate(restart=False)

        with self.db.event_context("index"):
            self.index_tables()

        if restart:
            self.db.restart_event()


class PostgresTimeSeries(TimeSeries):
    def index_tables(self) -> None:
        con = self.db.connect()

        # too expensive to create normal b-tree index on (id, time) or (time, id), use brin (block range index) instead
        con.execute(text("CREATE INDEX data_small_eav_index ON data_small_eav using brin(id)"))
        _LOGGER.info("Indexed data_small_eav")

        con.execute(text("CREATE INDEX data_large_eav_index ON data_large_eav using brin(id)"))
        _LOGGER.info("Indexed data_large_eav")

        con.execute(text("CREATE INDEX data_small_wide_index ON data_small_wide (time)"))
        _LOGGER.info("Indexed data_small_wide")

        con.execute(text("CREATE INDEX data_large_wide_index ON data_large_wide (time)"))
        _LOGGER.info("Indexed data_large_wide")

        con.commit()

    def populate_time_series(self, restart: bool = True) -> None:
        super().populate(restart=False)

        with self.db.event_context("index"):
            self.index_tables()

        if restart:
            self.db.restart_event()

    def include_query(self, query_name: str) -> bool:
        # too slow
        if "latest_time_range" in query_name and "eav" in query_name:
            _LOGGER.warning(f"Skipping query '{query_name}'")
            return False

        return True


class Postgres(Database):
    name: Literal["postgres"] = "postgres"
    version: str = VERSION

    connection_string: str = POSTGRES_CONNECTION_STRING

    @property
    def start(self) -> str:
        (SETTINGS.database_directory / "postgres").mkdir(exist_ok=True)
        (SETTINGS.temporary_directory / "postgres/data").mkdir(exist_ok=True, parents=True)

        parts = [
            f"docker run --platform linux/amd64 --name {self.name}-benchmark --rm -d -p 5433:5432",
            f"-v {SETTINGS.database_directory.as_posix()}/postgres:/var/lib/postgresql/data/",
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
        df = connectorx.read_sql(POSTGRES_CONNECTION_STRING, query.strip().removesuffix(";"), return_type="polars")

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
        df = pl.read_database_uri(query.strip().removesuffix(";"), POSTGRES_CONNECTION_STRING, engine="connectorx")

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
        # timescale-parallel-copy also works with normal postgres, and should be faster than \copy or similar
        con = self.connect()

        if not table_exists(con, table):
            self.create_table(df.schema, table, primary_key, not_null)

        temp_dir = SETTINGS.temporary_directory / "postgres/data"

        temp_file = temp_dir / f"{table}_{uuid.uuid4().hex}.csv"
        temp_file_str = temp_file.resolve().as_posix()

        df.write_csv(temp_file)

        _LOGGER.info(f"Inserting dataset with shape ({df.shape[0]:_}, {df.shape[1]:_}) using timescaledb-parallel-copy")

        db_host = "localhost"
        db_name = "postgres"
        db_user = "postgres"
        db_password = "password"
        db_port = "5433"

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
    def rtabench(self) -> PostgresRTABench:
        return PostgresRTABench(db=self)

    @property
    def time_series(self) -> PostgresTimeSeries:
        return PostgresTimeSeries(db=self)
