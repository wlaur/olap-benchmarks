import logging
import re
import uuid
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from pathlib import Path
from textwrap import dedent
from typing import Any, ClassVar, Literal, cast

import polars as pl
from pydantic import Field
from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.engine import make_url

from ...settings import DatabaseName, SuiteName, TableName, get_settings, host_port
from ...suites import BenchmarkSuite
from ...suites.chat_threads.config import ChatThreads
from ...suites.clickbench.config import Clickbench
from ...suites.join_order import schema as job_schema
from ...suites.jsonbench.config import JSONBench, get_jsonbench_input_files, iter_jsonbench_input_lines
from ...suites.rtabench.config import RTABench
from ...suites.time_series.config import (
    MutateStep,
    TimeSeries,
    get_time_series_dataset_sizes,
    get_time_series_table_name,
)
from ...suites.time_series_metrics.config import TimeSeriesMetrics, metric_name
from ...suites.tpc_ds.config import TpcDs
from ...suites.tpc_h.config import TpcH
from .. import Database
from ..utils import iter_parquet_frames, require_columns, tracked_commit

_LOGGER = logging.getLogger(__name__)


def _postgres_connection_string() -> str:
    return f"postgresql://postgres:password@localhost:{host_port('postgres')}/postgres"


PostgresFetchMethod = Literal["connectorx", "python"]

COPY_BATCH_ROWS = 1_000_000
COPY_BATCH_MAX_CELLS = 64_000_000
JOB_INDEX_POLICY = "upstream-fkindexes-v1"
JOB_INDEX_SHA256 = "727909bbbb9b0ae2b7d5739cfcd066992fc4bf1e984b29d87bcc700c4e2583cc"
JOB_INDEX_COUNT = 23
JOB_INDEX_READBACK_SQL = """
SELECT t.relname AS table_name, i.relname AS index_name,
       x.indisprimary, x.indisvalid, x.indisready,
       pg_get_indexdef(i.oid) AS definition,
       pg_get_indexdef(i.oid, 1, true) AS column_name,
       pg_relation_size(i.oid) AS bytes
FROM pg_index x
JOIN pg_class i ON i.oid = x.indexrelid
JOIN pg_class t ON t.oid = x.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = 'public'
ORDER BY t.relname, i.relname
""".strip()


def _job_indexes() -> dict[str, tuple[str, str, str]]:
    source = (job_schema.DIRECTORY / "upstream/fkindexes.sql").read_bytes()
    if sha256(source).hexdigest() != JOB_INDEX_SHA256:
        raise ValueError("JOB fkindexes.sql differs from its upstream SHA-256 pin")
    indexes: dict[str, tuple[str, str, str]] = {}
    for statement in source.decode("utf-8").splitlines():
        match = re.fullmatch(r"create index ([a-z_]+) on ([a-z_]+)\(([a-z_]+)\);", statement)
        if (
            match is None
            or match[1] in indexes
            or match[2] not in job_schema.JOB_SCHEMA
            or match[3] not in {column.name for column in job_schema.JOB_SCHEMA[match[2]]}
        ):
            raise ValueError("Unexpected pinned JOB foreign-key index definition")
        indexes[match[1]] = (match[2], match[3], statement)
    if len(indexes) != JOB_INDEX_COUNT:
        raise ValueError("Expected exactly 23 pinned JOB foreign-key indexes")
    return indexes


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

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        _LOGGER.info(f"Inserted clickbench table for {self.name}")

        with self.db.phase_context("index"):
            self.index_table()

        # restart db to ensure data is not kept in-memory by the db, and also
        # ensure that WAL is processed etc...
        if restart:
            self.db.restart_event()


class PostgresTpcH[DBT: "Postgres"](TpcH[DBT]):
    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {"method": "python"}


class PostgresTpcDs[DBT: "Postgres"](TpcDs[DBT]):
    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {"method": "python"}

    def query_path(self, query_name: str) -> Path | None:
        for db_name in (self.db.name, "postgres"):
            override = self.queries_directory / db_name / f"{query_name}.sql"
            if override.is_file():
                return override
        return super().query_path(query_name)


class PostgresTimeSeriesMetrics[DBT: "Postgres"](TimeSeriesMetrics[DBT]):
    def load_query(self, query_name: str) -> str:
        if query_name != "wide1000_12_all_metrics":
            return super().load_query(query_name)
        values = ", ".join(f"CAST({metric_name(index)} AS DOUBLE PRECISION)" for index in range(1, 1001))
        return (
            "SELECT COUNT(v) AS cnt, COALESCE(SUM(v), 0.0) AS total\n"
            "FROM wide1000\n"
            f"CROSS JOIN LATERAL UNNEST(ARRAY[{values}]) AS m(v)\n"
        )


class PostgresTimeSeries[DBT: "Postgres"](TimeSeries[DBT]):
    # Shared EAV layout for wide/large telemetry; tall stays wide. The original
    # Float32 rows can fit PostgreSQL's heap. TimescaleDB compression still hits
    # the 8160-byte tuple limit: 785/1500-metric fixtures produce compressed
    # tuples of 14216/27088 bytes on 2.30.2/PG18. Successful COPY is not evidence
    # that the wide layout can be compressed; check compress_chunk before changing this.
    EAV_TABLES: ClassVar[frozenset[TableName]] = frozenset({"data_wide", "data_large"})

    # pyarrow row-batch size for the single-pass parquet stream. 100 k wide
    # rows × 1500 cols × 4 bytes ≈ 600 MB peak Arrow buffer per batch. Each
    # flush stages ~5 GB of CSV before it's COPYed into the heap.
    PARQUET_STREAM_BATCH_ROWS: ClassVar[int] = 100_000

    def expected_table_row_counts(self) -> Mapping[TableName, int]:
        selected = self.dataset_files()
        counts: dict[TableName, int] = {}
        for size, (n_rows, n_cols) in get_time_series_dataset_sizes(self.scale_factor).items():
            table_name = get_time_series_table_name(size)
            if table_name not in selected:
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

    def index_tables(self) -> None:
        for table_name in self.dataset_files():
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

        for table_name, fpath in self.dataset_files().items():
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
    supports_arm64_containers: ClassVar[bool] = True
    wal_path_patterns: ClassVar[tuple[str, ...]] = ("pgdata/pg_wal",)

    connection_string: str = Field(default_factory=_postgres_connection_string)

    @property
    def run_options(self) -> Mapping[str, object]:
        method = (
            self.benchmarks[self.current_suite].fetch_kwargs.get("method", "connectorx")
            if self._current_suite is not None
            else "connectorx"
        )
        options: dict[str, object] = {
            **super().run_options,
            "select_fetch_method": method,
            "generic_insert_method": "native-csv-copy-v1",
            "copy_workers": self.parallel_copy_workers,
            "copy_batch_rows": COPY_BATCH_ROWS,
            "copy_batch_max_cells": COPY_BATCH_MAX_CELLS,
        }
        if self._uses_job_indexes:
            options.update(
                job_index_policy=JOB_INDEX_POLICY,
                job_upstream_index_sha256=JOB_INDEX_SHA256,
                job_intended_secondary_index_count=JOB_INDEX_COUNT,
                job_index_readback_phase="job_index_readiness",
            )
        return options

    @property
    def _uses_job_indexes(self) -> bool:
        return self.name == "postgres" and self._current_suite == "join_order"

    @property
    def server_settings(self) -> Mapping[str, str]:
        settings = get_settings()
        values = {
            "shared_buffers": "8GB",
            "effective_cache_size": "24GB",
            "work_mem": "4GB",
            "maintenance_work_mem": "4GB",
            "max_wal_size": "16GB",
            "min_wal_size": "2GB",
            "wal_compression": "off",
            "synchronous_commit": "off",
            "checkpoint_timeout": "30min",
            "max_parallel_maintenance_workers": "4",
            "max_parallel_workers_per_gather": "4",
            "max_parallel_workers": "8",
        }
        threads = settings.benchmark_threads
        if threads is not None:
            values.update(
                max_parallel_maintenance_workers=str(min(4, threads)),
                max_parallel_workers_per_gather=str(max(0, threads - 1)),
                max_parallel_workers=str(threads),
                max_worker_processes=str(max(8, threads + 4)),
            )
        if settings.benchmark_memory_gb is not None:
            memory_mb = settings.benchmark_memory_gb * 1024
            parallel_workers = threads or 8
            values.update(
                shared_buffers=f"{max(128, memory_mb // 4)}MB",
                effective_cache_size=f"{memory_mb * 3 // 4}MB",
                work_mem=f"{max(16, min(256, memory_mb // (16 * (parallel_workers + 1))))}MB",
                maintenance_work_mem=f"{max(64, min(4096, memory_mb // 4))}MB",
            )
        return values

    def finalize_populate(self) -> None:
        if self._uses_job_indexes and not self._verify_job_indexes(allow_primary_only=True):
            for table_name, _column, statement in _job_indexes().values():
                with self.phase_context("index", table_name=table_name):
                    self.execute(statement)
        for table_name in sorted(self.get_table_names()):
            quoted_table = table_name.replace('"', '""')
            with self.phase_context("analyze", table_name=table_name):
                self.execute(f'ANALYZE public."{quoted_table}"')
        if self._uses_job_indexes:
            self.verify_storage_readiness()

    def verify_storage_readiness(self) -> None:
        if self._uses_job_indexes:
            self._verify_job_indexes()

    def _verify_job_indexes(self, *, allow_primary_only: bool = False) -> bool:
        evidence: dict[str, Any] = {
            "index_policy": JOB_INDEX_POLICY,
            "upstream_index_sha256": JOB_INDEX_SHA256,
            "intended_secondary_index_count": JOB_INDEX_COUNT,
            "ready": False,
        }
        step_id = (
            self._start_step("phase", "job_index_readiness")
            if self._result_storage is not None and self._run_id is not None
            else None
        )
        if step_id is not None:
            self._push_active_step(step_id)
        try:
            indexes = _job_indexes()
            rows = cast(list[dict[str, object]], self.fetch(JOB_INDEX_READBACK_SQL, method="python").to_dicts())
            evidence["observed_indexes"] = rows
            secondary_count = sum(row["indisprimary"] is not True for row in rows)
            evidence["observed_secondary_index_count"] = secondary_count
            expected: dict[str, tuple[str, str, str, bool]] = {
                f"{table}_pkey": (
                    table,
                    "id",
                    f"CREATE UNIQUE INDEX {table}_pkey ON public.{table} USING btree (id)",
                    True,
                )
                for table in job_schema.JOB_TABLES
            }
            if secondary_count or not allow_primary_only:
                expected.update(
                    {
                        name: (table, column, f"CREATE INDEX {name} ON public.{table} USING btree ({column})", False)
                        for name, (table, column, _statement) in indexes.items()
                    }
                )
            if len(rows) != len(expected) or {str(row["index_name"]) for row in rows} != set(expected):
                raise RuntimeError("PostgreSQL JOB has missing or unexpected indexes; recreate storage and populate")
            for row in rows:
                table, column, definition, primary = expected[str(row["index_name"])]
                if (
                    row["table_name"] != table
                    or row["column_name"] != column
                    or row["definition"] != definition
                    or row["indisprimary"] is not primary
                    or row["indisvalid"] is not True
                    or row["indisready"] is not True
                ):
                    raise RuntimeError(f"PostgreSQL JOB index is invalid or differs from the pinned layout: {row}")
            ready = secondary_count == JOB_INDEX_COUNT
            evidence["ready"] = ready
            evidence["observed_layout"] = JOB_INDEX_POLICY if ready else "primary-only"
        except BaseException as exc:
            if step_id is not None:
                self._finish_step(
                    step_id,
                    status="failed",
                    step_type="phase",
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    metadata=evidence,
                )
            raise
        finally:
            if step_id is not None:
                self._pop_active_step(step_id)
        if step_id is not None:
            self._finish_step(step_id, status="completed", step_type="phase", metadata=evidence)
        return ready

    @property
    def parallel_copy_workers(self) -> int:
        return min(12, get_settings().benchmark_threads or 12)

    @property
    def start(self) -> str:
        host_pgdata = self.database_directory / "pgdata"
        host_pgdata.mkdir(parents=True, exist_ok=True)
        host_pgdata.chmod(0o777)  # macOS bind-friendly

        return self.docker_run_command(
            self.container_images["server"],
            ports={str(host_port("postgres")): "5432"},
            mounts={host_pgdata.as_posix(): "/var/lib/postgresql/pgdata"},
            env={
                "PGDATA": "/var/lib/postgresql/pgdata",
                "POSTGRES_PASSWORD": "password",
                "TZ": "UTC",
                "PGTZ": "UTC",
            },
            args=tuple(part for name, value in self.server_settings.items() for part in ("-c", f"{name}={value}")),
        )

    def get_runtime_version(self) -> str:
        df = self.fetch("select current_setting('server_version') as version", schema={"version": pl.String})
        return str(df.item(0, 0))

    def _query_connection_string(self) -> str:
        timeout = get_settings().query_timeout_seconds if self._current_operation == "select" else None
        connection_string = self.connection_string
        if timeout is not None:
            url = make_url(connection_string)
            options = url.query.get("options", "")
            if isinstance(options, tuple):
                options = " ".join(options)
            url = url.update_query_dict({"options": f"{options} -cstatement_timeout={timeout * 1000}".strip()})
            prefix, _, parameters = url.render_as_string(hide_password=False).partition("?")
            connection_string = f"{prefix}?{parameters.replace('+', '%20')}"
        return connection_string

    def connect(self, reconnect: bool = False) -> Connection:
        if reconnect:
            self.close_connection()

        if self._connection is not None:
            return self._connection

        engine = create_engine(self._query_connection_string())
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
            df = pl.read_database_uri(sql, self._query_connection_string())

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
        directory = get_settings().temporary_directory / self.name / "data"
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
        con = self.connect()
        schema = df.schema if isinstance(df, pl.DataFrame) else df.collect_schema()
        if not table_exists(con, table):
            self.create_table(schema, table, primary_key, not_null)

        batch_rows = min(COPY_BATCH_ROWS, max(1, COPY_BATCH_MAX_CELLS // max(1, len(schema))))
        batches = (
            df.collect_batches(chunk_size=batch_rows, maintain_order=True)
            if isinstance(df, pl.LazyFrame)
            else df.iter_slices(batch_rows)
        )
        parent_step = self.active_step_id
        with ThreadPoolExecutor(max_workers=self.parallel_copy_workers) as workers:
            for batch_index, batch in enumerate(batches, start=1):
                partition_rows = max(1, (batch.height + self.parallel_copy_workers - 1) // self.parallel_copy_workers)
                futures = [
                    workers.submit(self._copy_insert_partition, con.engine, table, partition, parent_step)
                    for partition in batch.iter_slices(partition_rows)
                ]
                for future in futures:
                    future.result()
                _LOGGER.info(f"Inserted native COPY batch {batch_index:_} into {table} with {batch.height:_} rows")

    def _copy_insert_partition(
        self, engine: Engine, table: TableName, frame: pl.DataFrame, parent_step: int | None
    ) -> None:
        if parent_step is not None:
            self._push_active_step(parent_step)
        try:
            with engine.connect() as connection:
                self.bind_query_recorder(connection)
                connection.begin()
                self._copy_dataframe(connection, table, frame, table, columns=None)
                tracked_commit(connection)
        finally:
            if parent_step is not None:
                self._pop_active_step(parent_step)

    def _copy_csv_to_table(self, con: Connection, table: str, csv_path: Path, columns: list[str] | None) -> None:
        raw_conn = con.connection.dbapi_connection
        assert raw_conn is not None
        cursor = raw_conn.cursor()
        column_sql = " (" + ", ".join(f'"{column}"' for column in columns) + ")" if columns is not None else ""
        copy_sql = f"COPY {table}{column_sql} FROM STDIN WITH (FORMAT csv, HEADER true)"
        with open(csv_path, encoding="utf-8", newline="") as f, self.record_query_execution(copy_sql):
            cursor.copy_expert(copy_sql, f)

    def _copy_dataframe(
        self, con: Connection, table: str, df: pl.DataFrame, label: str, *, columns: list[str] | None
    ) -> None:
        temp_file = self._staging_directory / f"{label}_{uuid.uuid4().hex}.csv"

        try:
            df.write_csv(temp_file)
            self._copy_csv_to_table(con, table, temp_file, columns)
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
        self._copy_dataframe(con, staging_table, df, f"{table}_upsert", columns=df.columns)
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
        self._copy_dataframe(con, staging_table, keys.select(primary_keys), f"{table}_delete", columns=primary_keys)
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
            "time_series_metrics": PostgresTimeSeriesMetrics,
            "tpc_ds": PostgresTpcDs,
            "tpc_h": PostgresTpcH,
            "chat_threads": ChatThreads,
        }
