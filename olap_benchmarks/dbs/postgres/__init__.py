import logging
import subprocess
import uuid
from collections.abc import Mapping
from pathlib import Path
from textwrap import dedent
from typing import Any, ClassVar, Literal, cast

import polars as pl
from sqlalchemy import Connection, create_engine, text
from sqlalchemy.engine import make_url

from ...run_metadata import StepResultStatus
from ...settings import SETTINGS, DatabaseName, SuiteName, TableName, host_port
from ...suites import BenchmarkSuite
from ...suites.chat_threads.config import ChatThreads
from ...suites.clickbench.config import Clickbench
from ...suites.jsonbench.config import JSONBench, get_jsonbench_input_files, iter_jsonbench_input_lines
from ...suites.rtabench.config import RTABench
from ...suites.time_series.config import (
    MutateStep,
    TimeSeries,
    get_time_series_dataset_sizes,
    get_time_series_input_files,
    get_time_series_table_name,
)
from .. import Database
from ..utils import iter_parquet_frames, require_columns, tracked_commit

_LOGGER = logging.getLogger(__name__)

VERSION = "18.3"

DOCKER_IMAGE = f"postgres:{VERSION}"
POSTGRES_HOST_PORT = host_port("postgres")

POSTGRES_CONNECTION_STRING = f"postgresql://postgres:password@localhost:{POSTGRES_HOST_PORT}/postgres"
PostgresFetchMethod = Literal["connectorx", "python"]


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
        # TimescaleDB / Postgres best-practice is TIMESTAMPTZ (it warns about
        # TIMESTAMP on hypertables). Polars naive datetimes are interpreted
        # against the cluster's `timezone` GUC, which the docker entrypoints
        # below pin to UTC so the round-trip is loss-less.
        return "TIMESTAMPTZ"
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
        for statement in (
            "CREATE INDEX orders_customer_id_index ON orders (customer_id);",
            "CREATE INDEX order_events_order_id_index ON order_events (order_id);",
            "CREATE INDEX order_events_event_type_index ON order_events (event_type);",
        ):
            self.db.execute(statement, commit=False)

        tracked_commit(self.db.connect())

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

        for n in statements.strip().split(";"):
            n = n.strip()

            if not n:
                continue

            self.db.execute(n)
            _LOGGER.info(f"Executed {n}")

        self.db.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm", reconnect=True)

        self.db.execute("CREATE INDEX trgm_idx_title ON hits USING gin (title gin_trgm_ops);")
        _LOGGER.info("Created index trgm_idx_title")

        self.db.execute("CREATE INDEX trgm_idx_url ON hits USING gin (url gin_trgm_ops);")
        _LOGGER.info("Created index trgm_idx_url")

        _LOGGER.info("Generated indexes for table hits")

        self.db.execute("VACUUM ANALYZE hits", autocommit=True)
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


class PostgresTimeSeries[DBT: "Postgres"](TimeSeries[DBT]):
    # Wide telemetry tables (1500+ cols) blow past PG's 8160-byte tuple limit, so
    # the row-store engines (Postgres, TimescaleDB) use the idiomatic EAV layout
    # for them. Tall (10 cols) stays wide.
    EAV_TABLES: ClassVar[frozenset[TableName]] = frozenset({"data_wide", "data_large"})

    # data_large EAV is ≈ 6 billion rows × ~50 bytes ≈ 300 GB heap on plain
    # Postgres (no columnar compression) plus a ~70 GB (metric_name, time)
    # btree index, so the populate would not fit into the per-(db, suite) disk
    # budget on this host. Skipping it here also disables the corresponding
    # large_* select queries and *_data_large_* mutation steps. The EAV insert
    # code path is still wired up so re-enabling is just emptying this set.
    SKIP_TABLES: ClassVar[frozenset[TableName]] = frozenset({"data_large"})
    UNSUPPORTED_EAV_QUERIES: ClassVar[frozenset[str]] = frozenset({"large_23_batch_export"})

    # pyarrow row-batch size for the single-pass parquet stream. 100 k wide
    # rows × 1500 cols × 4 bytes ≈ 600 MB peak Arrow buffer per batch. Each
    # flush stages ~5 GB of CSV before it's COPYed into the heap.
    PARQUET_STREAM_BATCH_ROWS: ClassVar[int] = 100_000

    def expected_table_row_counts(self) -> Mapping[TableName, int]:
        counts: dict[TableName, int] = {}
        for size, (n_rows, n_cols) in get_time_series_dataset_sizes(self.scale_factor).items():
            table_name = get_time_series_table_name(size)
            if table_name in self.SKIP_TABLES:
                continue
            counts[table_name] = n_rows * n_cols if table_name in self.EAV_TABLES else n_rows
        return counts

    def get_primary_key(self, table_name: TableName) -> str | list[str] | None:
        _ = table_name
        return None

    def get_not_null(self, table_name: TableName) -> str | list[str] | None:
        if table_name in self.EAV_TABLES:
            return ["time", "metric_name"]
        return "time"

    def include_query(self, query_name: str) -> bool:
        # Queries against tables we never populated must be skipped.
        for table in self.SKIP_TABLES:
            size = table.removeprefix("data_")
            if query_name.startswith(f"{size}_"):
                return False
        return True

    def query_skip(self, query_name: str) -> tuple[StepResultStatus, str] | None:
        skipped = super().query_skip(query_name)
        if skipped is not None:
            return skipped

        if query_name in self.UNSUPPORTED_EAV_QUERIES:
            return ("unsupported", "query requires wide-row export shape, but this database stores data_large as EAV")

        return None

    def index_tables(self) -> None:
        for table_name in get_time_series_input_files(self.scale_factor):
            if table_name in self.SKIP_TABLES:
                continue
            if table_name in self.EAV_TABLES:
                statement = f'CREATE INDEX {table_name}_metric_time_index ON "{table_name}" ("metric_name", "time")'
            else:
                statement = f'CREATE INDEX {table_name}_time_index ON "{table_name}" ("time")'

            self.db.execute(statement, commit=False)
            _LOGGER.info(f"Indexed {table_name}")

        tracked_commit(self.db.connect())

    def _eav_create_table(self, table_name: TableName) -> None:
        self.db.execute(
            f'CREATE TABLE "{table_name}" ("time" TIMESTAMPTZ NOT NULL, "metric_name" TEXT NOT NULL, "value" REAL)'
        )
        _LOGGER.info(f"Created EAV table {table_name}")

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

    def _eav_streaming_insert(self, table_name: TableName, fpath: Path) -> None:
        """Single-pass stream of `fpath` → unpivot → COPY. Replaces the previous
        offset-based loop that called `pl.scan_parquet().slice(offset, n)` per
        batch and re-decoded the parquet from the start each time."""
        frames = iter_parquet_frames(fpath, self.PARQUET_STREAM_BATCH_ROWS)
        for batch_idx, df_wide in enumerate(frames, start=1):
            df_eav = self._wide_to_eav(df_wide)
            self.db.insert(df_eav, table_name)
            _LOGGER.info(
                f"Inserted EAV batch {batch_idx} for {table_name}: wide={df_wide.shape[0]:_}, eav={df_eav.shape[0]:_}"
            )

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        self.db.initialize_schema("time_series")

        for table_name, fpath in get_time_series_input_files(self.scale_factor).items():
            if table_name in self.SKIP_TABLES:
                _LOGGER.info(f"Skipping {table_name} for {self.name} (would not fit in disk budget)")
                continue
            if table_name in self.EAV_TABLES:
                with self.db.phase_context("create_eav_table", table_name=table_name):
                    self._eav_create_table(table_name)

                with self.db.phase_context("insert", table_name=table_name):
                    self._eav_streaming_insert(table_name, fpath)
                    _LOGGER.info(f"Inserted {table_name} for {self.name}")
            else:
                primary_key = self.get_primary_key(table_name)
                not_null = self.get_not_null(table_name)

                with self.db.phase_context("insert", table_name=table_name):
                    self.db.insert_parquet(
                        fpath,
                        table_name,
                        primary_key=primary_key,
                        not_null=not_null,
                    )
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


class PostgresJSONBench(JSONBench["Postgres"]):
    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        with self.db.phase_context("schema", table_name="bluesky"):
            self.db.execute(
                """
                CREATE TABLE bluesky (
                    data JSONB COMPRESSION lz4 NOT NULL
                )
                """
            )

        with self.db.phase_context("insert", table_name="bluesky"):
            for input_file in get_jsonbench_input_files(self.scale_factor):
                self._copy_json_file(input_file)

        with self.db.phase_context("index", table_name="bluesky"):
            self.db.execute(
                """
                CREATE INDEX idx_bluesky
                ON bluesky (
                    (data ->> 'kind'),
                    (data -> 'commit' ->> 'operation'),
                    (data -> 'commit' ->> 'collection'),
                    (data ->> 'did'),
                    (TO_TIMESTAMP((data ->> 'time_us')::BIGINT / 1000000.0))
                )
                """
            )

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        if restart:
            self.db.restart_event()

    def _copy_json_file(self, input_file: Path) -> None:
        con = self.db.connect()
        raw_conn = con.connection.dbapi_connection
        assert raw_conn is not None
        cursor = raw_conn.cursor()
        temp_file = self.db._staging_directory / f"bluesky_{uuid.uuid4().hex}.json"
        copy_sql = "COPY bluesky FROM STDIN WITH (FORMAT csv, QUOTE E'\\x01', DELIMITER E'\\x02', ESCAPE E'\\x01')"

        try:
            with temp_file.open("w", encoding="utf-8") as out:
                out.writelines(iter_jsonbench_input_lines(input_file))

            with temp_file.open(encoding="utf-8") as f, self.db.record_query_execution(copy_sql):
                cursor.copy_expert(copy_sql, f)

            tracked_commit(con)
            _LOGGER.info(f"Copied JSONBench file {input_file.name} into bluesky")
        finally:
            cursor.close()
            temp_file.unlink(missing_ok=True)


class Postgres(Database):
    name: DatabaseName = "postgres"
    version: str = VERSION
    container_image: ClassVar[str | None] = DOCKER_IMAGE
    supports_arm64_containers: ClassVar[bool] = True

    connection_string: str = POSTGRES_CONNECTION_STRING

    # Mirrors PostgresTimeSeries.SKIP_TABLES: data_large EAV does not fit in
    # the per-(db, suite) disk budget on plain Postgres, so its mutate steps
    # have to be disabled too.
    DISABLED_MUTATION_STEPS: ClassVar[Mapping[SuiteName, frozenset[str]]] = {
        "time_series": frozenset(
            f"{action}_data_large_{count}" for action in ("insert", "upsert", "delete") for count in (1, 100, 10_000)
        )
    }

    @property
    def start(self) -> str:
        host_pgdata = self.database_directory / "pgdata"
        host_pgdata.mkdir(parents=True, exist_ok=True)
        host_pgdata.chmod(0o777)  # macOS bind-friendly

        parts = [
            "docker run",
            f"--platform {self.container_platform}",
            f"--name {self.name}-benchmark",
            f"--rm -d -p {POSTGRES_HOST_PORT}:5432",
            "--user 0:0",
            f"--mount type=bind,src={host_pgdata.as_posix()},dst=/var/lib/postgresql/pgdata",
            "-e PGDATA=/var/lib/postgresql/pgdata",
            "-e POSTGRES_PASSWORD=password",
            # Pin cluster timezone to UTC so TIMESTAMPTZ values written from
            # naive Polars datetimes round-trip without offset surprises.
            "-e TZ=UTC",
            "-e PGTZ=UTC",
            DOCKER_IMAGE,  # e.g. postgres:18
            # Settings tuned for the EAV bulk-load workload. Stock PG18 ships
            # with shared_buffers=128MB and max_wal_size=1GB, which forces a
            # checkpoint storm during multi-hundred-million-row inserts.
            "-c shared_buffers=8GB",
            "-c effective_cache_size=24GB",
            "-c work_mem=4GB",
            "-c maintenance_work_mem=4GB",
            "-c max_wal_size=16GB",
            "-c min_wal_size=2GB",
            "-c wal_compression=off",
            "-c synchronous_commit=off",
            "-c checkpoint_timeout=30min",
            "-c max_parallel_maintenance_workers=4",
            "-c max_parallel_workers_per_gather=4",
            "-c max_parallel_workers=8",
        ]
        return " ".join(parts)

    def get_runtime_version(self) -> str:
        df = self.fetch("select current_setting('server_version') as version", schema={"version": pl.String})
        return str(df.item(0, 0))

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
        method: PostgresFetchMethod = "connectorx",
    ) -> pl.DataFrame:
        if method == "connectorx":
            return self.fetch_connectorx(query, schema)
        if method == "python":
            return self.fetch_python(query, schema)

        raise ValueError(f"Unknown method: {method}")

    def fetch_connectorx(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
    ) -> pl.DataFrame:
        sql = query.strip().removesuffix(";")
        with self.record_query_execution(sql):
            df = pl.read_database_uri(sql, self.connection_string)

        if schema is not None:
            df = df.cast(cast(pl.Schema, schema))

        return df

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

    @property
    def _staging_directory(self) -> Path:
        directory = SETTINGS.temporary_directory / self.name / "data"
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _dml_session_setup(self, con: Connection) -> None:
        """Hook for session settings needed before DML statements."""

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

        temp_file = self._staging_directory / f"{table}_{uuid.uuid4().hex}.csv"
        temp_file_str = temp_file.resolve().as_posix()

        if isinstance(df, pl.LazyFrame):
            df.sink_csv(temp_file)
            _LOGGER.info("Inserting from staged CSV using timescaledb-parallel-copy")
        else:
            df.write_csv(temp_file)
            _LOGGER.info(
                f"Inserting dataset with shape ({df.shape[0]:_}, {df.shape[1]:_}) using timescaledb-parallel-copy"
            )

        url = make_url(self.connection_string)
        connection_string = (
            f"host={url.host} port={url.port} dbname={url.database} user={url.username} password={url.password}"
        )

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

    def _copy_dataframe(self, con: Connection, table: str, df: pl.DataFrame, label: str) -> None:
        temp_file = self._staging_directory / f"{label}_{uuid.uuid4().hex}.csv"

        try:
            df.write_csv(temp_file)
            self._copy_csv_to_table(con, table, temp_file)
        finally:
            temp_file.unlink(missing_ok=True)

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        primary_keys = require_columns(primary_key)

        for pk in primary_keys:
            if pk not in df.columns:
                raise ValueError(f"Primary key column '{pk}' not found in DataFrame columns")

        con = self.connect()
        pk_cols = ", ".join(f'"{pk}"' for pk in primary_keys)

        staging_table = f"_staging_{table}_{uuid.uuid4().hex[:8]}"
        self.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {table} INCLUDING DEFAULTS)", commit=False)
        self._copy_dataframe(con, staging_table, df, f"{table}_upsert")
        self._dml_session_setup(con)

        self.execute(f"DELETE FROM {table} WHERE ({pk_cols}) IN (SELECT {pk_cols} FROM {staging_table})", commit=False)

        all_columns = ", ".join(f'"{col}"' for col in df.columns)
        self.execute(f"INSERT INTO {table} ({all_columns}) SELECT {all_columns} FROM {staging_table}", commit=False)

        self.execute(f"DROP TABLE {staging_table}", commit=False)
        tracked_commit(con)

        _LOGGER.info(f"Upserted {df.shape[0]:_} rows into {table}")

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        primary_keys = require_columns(primary_key)

        con = self.connect()

        staging_table = f"_staging_del_{table}_{uuid.uuid4().hex[:8]}"
        pk_cols = ", ".join(f'"{pk}"' for pk in primary_keys)

        self.execute(f"CREATE TEMP TABLE {staging_table} AS SELECT {pk_cols} FROM {table} WHERE false", commit=False)
        self._copy_dataframe(con, staging_table, keys.select(primary_keys), f"{table}_delete")
        self._dml_session_setup(con)

        self.execute(f"DELETE FROM {table} WHERE ({pk_cols}) IN (SELECT {pk_cols} FROM {staging_table})", commit=False)
        self.execute(f"DROP TABLE {staging_table}", commit=False)
        tracked_commit(con)

        _LOGGER.info(f"Deleted rows from {table} by primary key")

    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        return {
            **super().suite_registry(),
            "rtabench": PostgresRTABench,
            "clickbench": PostgresClickbench,
            "jsonbench": PostgresJSONBench,
            "time_series": PostgresTimeSeries,
            "chat_threads": ChatThreads,
        }
