import logging
import subprocess
import uuid
from collections.abc import Mapping
from pathlib import Path
from textwrap import dedent
from typing import Any, ClassVar, cast

import connectorx
import polars as pl
from sqlalchemy import Connection, create_engine, text

from ...settings import SETTINGS, DatabaseName, TableName
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
from ..utils import tracked_commit

_LOGGER = logging.getLogger(__name__)

VERSION = "18.3"

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
    return result is not None and result[0] is not None


class PostgresRTABench(RTABench["Postgres"]):
    def index_tables(self) -> None:
        con = self.db.connect()

        for statement in (
            "CREATE INDEX orders_customer_id_index ON orders (customer_id);",
            "CREATE INDEX order_events_order_id_index ON order_events (order_id);",
            "CREATE INDEX order_events_event_type_index ON order_events (event_type);",
        ):
            with self.db.record_query_execution(statement):
                con.execute(text(statement))

        tracked_commit(con)

    def populate(self, restart: bool = True) -> None:
        super().populate(restart=False)

        with self.db.phase_context("index"):
            self.index_tables()

        if restart:
            self.db.restart_event()


class PostgresClickbench(Clickbench["Postgres"]):
    def index_table(self) -> None:
        statements = dedent("""
                CREATE INDEX adveng on hits (advengineid);
                CREATE INDEX regid  on hits (RegionID);
                CREATE INDEX cid on hits (counterid);
                CREATE INDEX eventtime on hits (eventtime);
                CREATE INDEX eventdate on hits (eventdate);
                CREATE INDEX mobile on hits (mobilephonemodel);
                CREATE INDEX refresh on hits (isrefresh, dontcounthits);
                CREATE INDEX resolutionwidth on hits (resolutionwidth);
                CREATE INDEX search on hits (searchphrase);
                CREATE INDEX userid on hits (userid);
                CREATE INDEX useridsearch on hits (userid, searchphrase);
                CREATE INDEX widcip on hits (watchid, clientip);
                CREATE INDEX mobileuser on hits (MobilePhoneModel,UserID);
                CREATE INDEX regionuser on hits (RegionID,UserID);
                CREATE INDEX mobile2 on hits (mobilephonemodel) WHERE mobilephonemodel <> ''::text;
                CREATE INDEX search2 on hits (searchphrase) WHERE searchphrase <> ''::text;

        """)

        con = self.db.connect()

        for n in statements.strip().split(";"):
            n = n.strip()

            if not n:
                continue

            with self.db.record_query_execution(n):
                con.execute(text(n))

            _LOGGER.info(f"Executed {n}")
            tracked_commit(con)

        con = self.db.connect(reconnect=True)
        statement = "CREATE EXTENSION IF NOT EXISTS pg_trgm"
        with self.db.record_query_execution(statement):
            con.execute(text(statement))
        tracked_commit(con)

        statement = "CREATE INDEX trgm_idx_title ON hits USING gin (title gin_trgm_ops);"
        with self.db.record_query_execution(statement):
            con.execute(text(statement))
        tracked_commit(con)
        _LOGGER.info("Created index trgm_idx_title")

        statement = "CREATE INDEX trgm_idx_url ON hits USING gin (url gin_trgm_ops);"
        with self.db.record_query_execution(statement):
            con.execute(text(statement))
        tracked_commit(con)
        _LOGGER.info("Created index trgm_idx_url")

        _LOGGER.info("Generated indexes for table hits")

        con = self.db.connect(reconnect=True)
        statement = "VACUUM ANALYZE hits"
        with self.db.record_query_execution(statement):
            con.execution_options(isolation_level="AUTOCOMMIT").execute(text(statement))

        _LOGGER.info("Ran vacuum analyze for table hits")

    def populate(self, restart: bool = True) -> None:
        self.db.initialize_schema("clickbench")

        # need to reorder columns
        df = self.load_dataset()

        columns_ordered = [
            "WatchID",
            "UserID",
            "FUniqID",
            "ParamPrice",
            "RefererHash",
            "URLHash",
            "EventTime",
            "ClientEventTime",
            "LocalEventTime",
            "EventDate",
            "CounterID",
            "ClientIP",
            "RegionID",
            "RefererRegionID",
            "URLRegionID",
            "IPNetworkID",
            "SilverlightVersion3",
            "CodeVersion",
            "HID",
            "RemoteIP",
            "WindowName",
            "OpenerName",
            "SendTiming",
            "DNSTiming",
            "ConnectTiming",
            "ResponseStartTiming",
            "ResponseEndTiming",
            "FetchTiming",
            "CLID",
            "JavaEnable",
            "GoodEvent",
            "CounterClass",
            "OS",
            "UserAgent",
            "IsRefresh",
            "RefererCategoryID",
            "URLCategoryID",
            "ResolutionWidth",
            "ResolutionHeight",
            "ResolutionDepth",
            "FlashMajor",
            "FlashMinor",
            "TraficSourceID",
            "SearchEngineID",
            "NetMajor",
            "NetMinor",
            "UserAgentMajor",
            "CookieEnable",
            "JavascriptEnable",
            "IsMobile",
            "MobilePhone",
            "AdvEngineID",
            "IsArtifical",
            "WindowClientWidth",
            "WindowClientHeight",
            "ClientTimeZone",
            "SilverlightVersion1",
            "SilverlightVersion2",
            "SilverlightVersion4",
            "IsLink",
            "IsDownload",
            "IsNotBounce",
            "IsOldCounter",
            "IsEvent",
            "IsParameter",
            "DontCountHits",
            "WithHash",
            "Age",
            "Sex",
            "Income",
            "Interests",
            "Robotness",
            "HistoryLength",
            "HTTPError",
            "SocialSourceNetworkID",
            "HasGCLID",
            "ParamCurrencyID",
            "Title",
            "URL",
            "Referer",
            "FlashMinor2",
            "BrowserLanguage",
            "BrowserCountry",
            "SocialNetwork",
            "SocialAction",
            "MobilePhoneModel",
            "Params",
            "SearchPhrase",
            "PageCharset",
            "OriginalURL",
            "SocialSourcePage",
            "ParamOrderID",
            "ParamCurrency",
            "OpenstatServiceName",
            "OpenstatCampaignID",
            "OpenstatAdID",
            "OpenstatSourceID",
            "UTMSource",
            "UTMMedium",
            "UTMCampaign",
            "UTMContent",
            "UTMTerm",
            "FromTag",
            "UserAgentMinor",
            "HitColor",
        ]

        df = df.select(columns_ordered)

        _LOGGER.info("Loaded clickbench dataset (lazy)")

        with self.db.phase_context("insert", table_name="hits"):
            self.db.insert(df, "hits", **self.populate_kwargs)

        _LOGGER.info(f"Inserted clickbench table for {self.name}")

        with self.db.phase_context("index"):
            self.index_table()

        # restart db to ensure data is not kept in-memory by the db, and also
        # ensure that WAL is processed etc...
        if restart:
            self.db.restart_event()


class PostgresTimeSeries(TimeSeries["Postgres"]):
    # Wide telemetry tables (1500+ cols) blow past PG's 8160-byte tuple limit, so
    # the row-store engines (Postgres, TimescaleDB) use the idiomatic EAV layout
    # for them. Tall (10 cols) stays wide.
    EAV_TABLES: ClassVar[frozenset[TableName]] = frozenset({"data_wide", "data_large"})
    # ~6 GB CSV per chunk (≈30 bytes/row). Tune downward if disk gets tight.
    EAV_CHUNK_TARGET_ROWS: ClassVar[int] = 200_000_000

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

    def index_tables(self) -> None:
        con = self.db.connect()

        for table_name in get_time_series_input_files():
            if table_name in self.EAV_TABLES:
                statement = f'CREATE INDEX {table_name}_metric_time_index ON "{table_name}" ("metric_name", "time")'
            else:
                statement = f'CREATE INDEX {table_name}_time_index ON "{table_name}" ("time")'

            with self.db.record_query_execution(statement):
                con.execute(text(statement))
            _LOGGER.info(f"Indexed {table_name}")

        tracked_commit(con)

    def _eav_create_table(self, table_name: TableName) -> None:
        con = self.db.connect()
        statement = (
            f'CREATE TABLE "{table_name}" ("time" TIMESTAMP NOT NULL, "metric_name" TEXT NOT NULL, "value" REAL)'
        )
        with self.db.record_query_execution(statement):
            con.execute(text(statement))
        tracked_commit(con)
        _LOGGER.info(f"Created EAV table {table_name}")

    def _eav_chunk_rows(self, n_metric_cols: int) -> int:
        return max(1_000, self.EAV_CHUNK_TARGET_ROWS // max(1, n_metric_cols))

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

    def _eav_chunked_insert(self, table_name: TableName, fpath: Path) -> None:
        schema = cast(pl.Schema, pl.read_parquet_schema(fpath))
        metric_cols = [c for c in schema if c != "time"]
        chunk_rows = self._eav_chunk_rows(len(metric_cols))
        total = self.parquet_row_count(fpath)

        offset = 0
        chunk_idx = 0
        while offset < total:
            chunk_idx += 1
            df_wide = pl.scan_parquet(fpath).slice(offset, chunk_rows).collect()
            df_eav = self._wide_to_eav(df_wide)
            self.db.insert(df_eav, table_name)
            _LOGGER.info(
                f"Inserted EAV chunk {chunk_idx} for {table_name}: "
                f"wide_rows={offset + df_wide.shape[0]:_}/{total:_}, "
                f"eav_rows={df_eav.shape[0]:_}"
            )
            offset += chunk_rows

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        self.db.initialize_schema("time_series")

        for table_name, fpath in get_time_series_input_files().items():
            if table_name in self.EAV_TABLES:
                with self.db.phase_context("create_eav_table", table_name=table_name):
                    self._eav_create_table(table_name)

                with self.db.phase_context("insert", table_name=table_name):
                    self._eav_chunked_insert(table_name, fpath)
                    _LOGGER.info(f"Inserted {table_name} for {self.name}")
            else:
                primary_key = self.get_primary_key(table_name)
                not_null = self.get_not_null(table_name)
                df = pl.scan_parquet(fpath)

                with self.db.phase_context("insert", table_name=table_name):
                    self.insert_table(df, table_name, primary_key, not_null)
                    _LOGGER.info(f"Inserted {table_name} for {self.name}")

        with self.db.phase_context("index"):
            self.index_tables()

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


class Postgres(Database):
    name: DatabaseName = "postgres"
    version: str = VERSION

    connection_string: str = POSTGRES_CONNECTION_STRING

    @property
    def start(self) -> str:
        host_pgdata = self.database_directory / "pgdata"
        host_pgdata.mkdir(parents=True, exist_ok=True)
        host_pgdata.chmod(0o777)  # macOS bind-friendly

        parts = [
            "docker run --platform linux/amd64",
            f"--name {self.name}-benchmark",
            "--rm -d -p 5433:5432",
            "--user 0:0",
            f"--mount type=bind,src={host_pgdata.as_posix()},dst=/var/lib/postgresql/pgdata",
            "-e PGDATA=/var/lib/postgresql/pgdata",
            "-e POSTGRES_PASSWORD=password",
            DOCKER_IMAGE,  # e.g. postgres:18
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
                    POSTGRES_CONNECTION_STRING, query.strip().removesuffix(";"), return_type="polars"
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
            df = pl.read_database_uri(query.strip().removesuffix(";"), POSTGRES_CONNECTION_STRING, engine="connectorx")

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
        # timescale-parallel-copy also works with normal postgres, and should be faster than \copy or similar
        con = self.connect()

        schema = df.schema if isinstance(df, pl.DataFrame) else df.collect_schema()

        if not table_exists(con, table):
            self.create_table(schema, table, primary_key, not_null)

        temp_dir = SETTINGS.temporary_directory / "postgres/data"

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

        temp_dir = SETTINGS.temporary_directory / "postgres/data"
        temp_file = temp_dir / f"{table}_upsert_{uuid.uuid4().hex}.csv"

        try:
            df.write_csv(temp_file)
            self._copy_csv_to_table(con, staging_table, temp_file)
        finally:
            temp_file.unlink(missing_ok=True)

        delete_sql = f"DELETE FROM {table} WHERE ({pk_cols}) IN (SELECT {pk_cols} FROM {staging_table})"
        with self.record_query_execution(delete_sql):
            con.execute(text(delete_sql))

        all_columns = ", ".join(f'"{col}"' for col in df.columns)
        insert_sql = f"INSERT INTO {table} ({all_columns}) SELECT {all_columns} FROM {staging_table}"
        with self.record_query_execution(insert_sql):
            con.execute(text(insert_sql))

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

        temp_dir = SETTINGS.temporary_directory / "postgres/data"
        temp_file = temp_dir / f"{table}_delete_{uuid.uuid4().hex}.csv"

        try:
            keys.select(primary_keys).write_csv(temp_file)
            self._copy_csv_to_table(con, staging_table, temp_file)
        finally:
            temp_file.unlink(missing_ok=True)

        sql = f"DELETE FROM {table} WHERE ({pk_cols}) IN (SELECT {pk_cols} FROM {staging_table})"
        with self.record_query_execution(sql):
            con.execute(text(sql))

        statement = f"DROP TABLE {staging_table}"
        with self.record_query_execution(statement):
            con.execute(text(statement))
        tracked_commit(con)

        _LOGGER.info(f"Deleted rows from {table} by primary key")

    @property
    def rtabench(self) -> PostgresRTABench:
        return PostgresRTABench(db=self)

    @property
    def clickbench(self) -> PostgresClickbench:
        return PostgresClickbench(db=self)

    @property
    def time_series(self) -> PostgresTimeSeries:
        return PostgresTimeSeries(db=self)
