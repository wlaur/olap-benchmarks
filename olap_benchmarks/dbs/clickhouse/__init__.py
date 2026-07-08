import logging
import uuid
from collections.abc import Mapping
from math import ceil
from pathlib import Path
from shutil import copy2, rmtree
from time import perf_counter, sleep
from typing import Any, ClassVar, cast
from urllib.parse import urlparse

import clickhouse_connect
import clickhouse_connect.driver
import clickhouse_connect.driver.client
import polars as pl
from clickhouse_connect.driver.client import Client as ClickhouseClient
from sqlalchemy import Connection, create_engine

from ...settings import SETTINGS, DatabaseName, SuiteName, TableName
from ...suites import BenchmarkSuite
from ...suites.clickbench.config import Clickbench
from ...suites.jsonbench.config import JSONBench, get_jsonbench_input_files
from ...suites.rtabench.config import RTABench
from ...suites.time_series.config import TimeSeries
from ...suites.tpc_ds.config import TpcDs
from .. import Database
from ..utils import normalize_columns, require_columns

_LOGGER = logging.getLogger(__name__)

VERSION = "26.6.1.1193"

DOCKER_IMAGE = f"clickhouse:{VERSION}-jammy"

CLICKHOUSE_CONNECTION_STRING = "clickhouse://user:password@localhost:18123/default"

POLARS_CLICKHOUSE_TYPE_MAP: dict[pl.DataType | type[pl.DataType], str] = {
    pl.Int8: "Int8",
    pl.Int16: "Int16",
    pl.Int32: "Int32",
    pl.Int64: "Int64",
    pl.UInt8: "UInt8",
    pl.UInt16: "UInt16",
    pl.UInt32: "UInt32",
    pl.UInt64: "UInt64",
    pl.Float32: "Float32",
    pl.Float64: "Float64",
    pl.Boolean: "UInt8",
    pl.String: "String",
    pl.Struct: "JSON",
    pl.Date: "Date",
}


def get_clickhouse_type(dtype: pl.DataType | type[pl.DataType], nullable: bool = False) -> str:
    if dtype == pl.Datetime:
        # DateTime64(3) preserves the ms-precision of the source parquet
        # (datetime[ms]); plain DateTime would silently truncate to seconds.
        # query_arrow returns DateTime64 as a typed timestamp[ms,tz=UTC] so
        # the fetch path no longer needs the from_epoch round-trip dance.
        # NOTE: timestamp is never nullable (overrides parameter not_null to the insert method)
        return "DateTime64(3, 'UTC')"

    sql_type = POLARS_CLICKHOUSE_TYPE_MAP.get(dtype)

    if sql_type is None:
        raise ValueError(f"Unsupported Polars dtype: {dtype}")

    if nullable:
        return f"Nullable({sql_type})"
    else:
        return sql_type


def get_clickhouse_client() -> ClickhouseClient:
    parsed_sqlalchemy_connection_string = urlparse(CLICKHOUSE_CONNECTION_STRING)

    return cast(
        ClickhouseClient,
        cast(Any, clickhouse_connect).get_client(
            host=parsed_sqlalchemy_connection_string.hostname,
            port=parsed_sqlalchemy_connection_string.port or 18123,
            username=parsed_sqlalchemy_connection_string.username,
            password=parsed_sqlalchemy_connection_string.password or "no-password",
            database="default",
        ),
    )


class ClickHouseRTABench(RTABench["Clickhouse"]):
    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {"time_columns": ["hour", "day"]}


class ClickhouseClickbench(Clickbench["Clickhouse"]):
    @property
    def populate_kwargs(self) -> dict[str, Any]:
        # same number of partitions as the official clickbench insert
        return {"partitions": 100}

    def optimize_clickbench_table(self) -> None:
        # not 100% clear if this is necessary, but seems to force cleaning up inactive parts
        self.db.run_sql("optimize table hits")

    def populate(self, restart: bool = True) -> None:
        super().populate(restart=False)

        with self.db.phase_context("optimize"):
            self.optimize_clickbench_table()

        if restart:
            self.db.restart_event()


class ClickhouseTpcDs(TpcDs["Clickhouse"]):
    # Query-level session settings from the official ClickHouse TPC-DS kit
    # (tests/benchmarks/tpc-ds/settings.json): standard-SQL NULL semantics for
    # outer joins and ROLLUP grouping sets, and DISTINCT set-operation
    # defaults. Without these, several queries return wrong results rather
    # than fail.
    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {
            "settings": {
                "group_by_use_nulls": 1,
                "join_use_nulls": 1,
                "intersect_default_mode": "DISTINCT",
                "union_default_mode": "DISTINCT",
                "joined_subquery_requires_alias": 0,
            }
        }


class ClickhouseJSONBench(JSONBench["Clickhouse"]):
    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {"time_columns": ["first_post_ts"]}

    def _stage_input_files(self) -> Path:
        temp_dir = SETTINGS.temporary_directory / "clickhouse/data"
        staging_dir = temp_dir / self.data_directory_name
        if staging_dir.exists():
            rmtree(staging_dir)
        staging_dir.mkdir(parents=True)

        for input_file in get_jsonbench_input_files(self.scale_factor):
            staged_file = staging_dir / input_file.name
            try:
                staged_file.hardlink_to(input_file)
            except OSError:
                copy2(input_file, staged_file)

        return staging_dir

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        ddl = """
            CREATE TABLE bluesky
            (
                `data` JSON(
                    max_dynamic_paths = 0,
                    kind LowCardinality(String),
                    commit.operation LowCardinality(String),
                    commit.collection LowCardinality(String),
                    did String,
                    time_us UInt64) CODEC(ZSTD(1))
            )
            ORDER BY (
                data.kind,
                data.commit.operation,
                data.commit.collection,
                data.did,
                fromUnixTimestamp64Micro(data.time_us))
            SETTINGS object_serialization_version = 'v3',
                     dynamic_serialization_version = 'v3',
                     object_shared_data_serialization_version = 'advanced',
                     object_shared_data_serialization_version_for_zero_level_parts = 'map_with_buckets'
        """

        with self.db.phase_context("schema", table_name="bluesky"):
            self.db.run_sql(ddl)

        staging_dir = self._stage_input_files()
        try:
            with self.db.phase_context("insert", table_name="bluesky"):
                self.db.run_sql(
                    f"""
                    INSERT INTO bluesky
                    SELECT *
                    FROM file('{staging_dir.name}/file_*.json.gz', 'JSONAsObject')
                    SETTINGS min_insert_block_size_rows = 1000000,
                             min_insert_block_size_bytes = 0
                    """
                )
        finally:
            rmtree(staging_dir)

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        if restart:
            self.db.restart_event()


class ClickhouseTimeseries(TimeSeries["Clickhouse"]):
    # Columnar codecs applied per dtype. DoubleDelta is the textbook codec for
    # monotonically increasing timestamps with regular intervals (one row per
    # minute in this suite), and Gorilla compresses correlated float
    # time-series an order of magnitude better than the LZ4 default. Wrapping
    # both with ZSTD(1) gives an additional ~2x for free at negligible
    # decompression cost. Together they compress this workload ~2.5x.
    TIME_CODEC: ClassVar[str] = "CODEC(DoubleDelta, ZSTD(1))"
    FLOAT_CODEC: ClassVar[str] = "CODEC(Gorilla, ZSTD(1))"
    # No PARTITION BY: monthly partitioning is the canonical recipe for
    # time-series in ClickHouse, but on this workload it tripled populate time
    # (each batch insert touches ~92 partitions for data_large, creating one
    # part per partition per block and forcing background merges). The
    # benchmark queries are mostly full-table aggregates, so partition pruning
    # gives little back. ORDER BY (time) is enough for time-bounded queries
    # to use the primary index.
    PARTITION_EXPR: ClassVar[str] = ""

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {"time_columns": ["time", "time_", "max(time)", "hr", "d"]}

    def _column_codec(self, name: str, dtype: pl.DataType | type[pl.DataType]) -> str:
        if name == "time":
            return self.TIME_CODEC
        if dtype in (pl.Float32, pl.Float64):
            return self.FLOAT_CODEC
        return ""

    def _create_time_series_table(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table_name: TableName,
        primary_key: str | list[str] | None,
        not_null: list[str],
    ) -> None:
        schema = df.schema if isinstance(df, pl.DataFrame) else df.collect_schema()

        columns_def: list[str] = []
        for name, dtype in schema.items():
            sql_type = get_clickhouse_type(dtype, nullable=name not in not_null)
            codec = self._column_codec(name, dtype)
            columns_def.append(f"`{name}` {sql_type}{(' ' + codec) if codec else ''}")

        order_by = self.db.get_order_by_columns(df, primary_key, not_null)
        order_by_clause = f"ORDER BY ({order_by})" if order_by is not None else ""

        partition_clause = f"PARTITION BY {self.PARTITION_EXPR}" if self.PARTITION_EXPR else ""

        sql = f"""
            CREATE TABLE {table_name} (
                {", ".join(columns_def)}
            )
            ENGINE = MergeTree
            {partition_clause}
            {order_by_clause}
            -- Aggressive part GC for the benchmark; default is 480 s.
            SETTINGS old_parts_lifetime = 5
        """
        self.db.run_sql(sql)
        _LOGGER.info(f"Created time_series table {table_name} with per-column codecs")

    def insert_table(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table_name: TableName,
        primary_key: str | list[str] | None,
        not_null: str | list[str] | None,
    ) -> None:
        normalized = normalize_columns(not_null)

        if table_name not in self.db.get_table_names():
            self._create_time_series_table(df, table_name, primary_key, normalized)

        # Falls through to db.insert which detects the table already exists
        # and skips the CTAS path, going straight to INSERT INTO ... SELECT
        # FROM file(...).
        self.db.insert(df, table_name, primary_key=primary_key, not_null=normalized, **self.populate_kwargs)


class Clickhouse(Database):
    name: DatabaseName = "clickhouse"
    version: str = VERSION
    container_image: ClassVar[str | None] = DOCKER_IMAGE

    connection_string: str = CLICKHOUSE_CONNECTION_STRING

    _clickhouse_client: clickhouse_connect.driver.client.Client | None = None

    @property
    def start(self) -> str:
        (SETTINGS.temporary_directory / "clickhouse/data").mkdir(exist_ok=True, parents=True)

        return self.docker_run_command(
            DOCKER_IMAGE,
            ports={"18123": "8123", "19000": "9000"},
            mounts={
                self.database_directory.as_posix(): "/var/lib/clickhouse",
                f"{SETTINGS.temporary_directory.as_posix()}/clickhouse/data": "/var/lib/clickhouse/user_files",
            },
            env={
                # does not seem to be able to create a new dt "benchmark", use the default name "default" instead
                "CLICKHOUSE_DB": "default",
                "CLICKHOUSE_PASSWORD": "password",
                "CLICKHOUSE_USER": "user",
                "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
            },
        )

    def connect(self, reconnect: bool = False) -> Connection:
        if reconnect:
            self._connection = None

        if self._connection is not None:
            return self._connection

        engine = create_engine(self.connection_string)
        self._connection = self.bind_query_recorder(engine.connect())

        return self._connection

    def get_client(self) -> clickhouse_connect.driver.client.Client:
        if self._clickhouse_client is not None:
            return self._clickhouse_client

        self._clickhouse_client = get_clickhouse_client()
        return self._clickhouse_client

    def get_runtime_version(self) -> str:
        df = self.fetch("select version() as version", schema={"version": pl.String})
        return str(df.item(0, 0))

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
        time_columns: str | list[str] | None = None,
        settings: Mapping[str, Any] | None = None,
    ) -> pl.DataFrame:
        query = query.strip().removesuffix(";")

        with self.record_query_execution(query):
            df = cast(
                pl.DataFrame,
                cast(Any, pl).from_arrow(cast(Any, self.get_client()).query_arrow(query, settings=settings)),
            )

        if schema is not None:
            df = df.cast(cast(pl.Schema, schema))

        if time_columns is None:
            time_columns = []

        if isinstance(time_columns, str):
            time_columns = [time_columns]

        if "time" not in time_columns:
            time_columns.append("time")

        # query_arrow returns:
        #   * DateTime64 / date_trunc()        -> timestamp[ms, tz=UTC] (typed)
        #   * DateTime / toStartOfHour() etc.  -> uint32 (epoch seconds)
        # Normalise both shapes to the naive ms-precision Datetime that the
        # rest of the benchmark assumes.
        for n in time_columns:
            if n not in df.columns:
                continue
            dtype = df.schema[n]
            if isinstance(dtype, pl.Datetime):
                df = df.with_columns(pl.col(n).cast(pl.Datetime("ms")).dt.replace_time_zone(None))
            elif dtype.is_integer():
                df = df.with_columns(pl.from_epoch(n, "s").cast(pl.Datetime("ms")))

        return df

    def get_table_names(self) -> set[TableName]:
        df = self.fetch(
            "select name as table_name from system.tables where database = currentDatabase()",
            schema={"table_name": pl.String},
        )
        return set(df.get_column("table_name").to_list())

    def run_sql(self, statement: str, settings: dict[str, Any] | None = None) -> None:
        # Transient errors that can fire under heavy back-to-back DELETE/INSERT
        # mutation traffic and that ClickHouse itself documents as retry-safe:
        #   1001  -- generic "try again"
        #   341   -- mutation UNFINISHED
        #   424   -- CANNOT_LINK (part vanished mid-mutation, surfaces as
        #            'Cannot link ... .cmrk2 ... No such file or directory')
        # All three can show up as the textual code in the exception message.
        retryable_codes = ("error code 1001", "code: 341", "code: 424", "CANNOT_LINK", "UNFINISHED")
        retries = 10
        for retry in range(retries):
            try:
                with self.record_query_execution(statement):
                    cast(Any, self.get_client()).command(statement, settings=settings)
                return
            except Exception as e:
                msg = str(e)
                if any(token in msg for token in retryable_codes):
                    backoff = min(2.0, 0.1 * (1 << retry))
                    _LOGGER.warning(
                        f"Retrying ClickHouse statement after retryable error "
                        f"({retry + 1:_}/{retries:_}, sleep={backoff:.1f}s): {msg.splitlines()[0][:200]}"
                    )
                    sleep(backoff)
                    continue
                # might happen if the parquet file is not fully written when clickhouse tries to read it
                if "error code 636" in msg:
                    raise

                raise

        raise RuntimeError(f"ClickHouse statement failed after {retries} retries: {statement[:200]}")

    def _build_key_filter(self, input_file_string: str, primary_keys: list[str]) -> str:
        if len(primary_keys) == 1:
            col = primary_keys[0]
            return f"{col} in (select distinct {col} from file('{input_file_string}', parquet))"

        key_tuple = ", ".join(primary_keys)
        return f"({key_tuple}) in (select distinct {key_tuple} from file('{input_file_string}', parquet))"

    def get_order_by_columns(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        primary_key: str | list[str] | None,
        not_null: list[str],
    ) -> str | None:
        # special case for time_series benchmark
        if primary_key is None and len(not_null):
            if set(not_null) == {"id", "time"}:
                order_by = "id, time"
            elif set(not_null) == {"time"}:
                order_by = "time"
            else:
                order_by = None
        elif primary_key is None:
            columns = df.columns if isinstance(df, pl.DataFrame) else list(df.collect_schema().names())
            order_by = columns[0]
        elif isinstance(primary_key, str):
            order_by = primary_key
        else:
            order_by = ", ".join(primary_key)

        return order_by

    def _write_single_parquet(self, parent: Path, df: pl.DataFrame | pl.LazyFrame) -> Path:
        temp_file = parent / f"{uuid.uuid4().hex}.parquet"

        if isinstance(df, pl.LazyFrame):
            df.sink_parquet(temp_file)
            _LOGGER.info("Wrote single Parquet file via sink_parquet")
        else:
            df.write_parquet(temp_file)
            _LOGGER.info(f"Wrote single Parquet file with shape ({df.shape[0]:_}, {df.shape[1]:_})")

        return temp_file

    def _write_partitioned_parquet(self, parent: Path, df: pl.DataFrame, partitions: int) -> Path:
        subdir = parent / uuid.uuid4().hex
        subdir.mkdir(parents=True, exist_ok=False)

        chunk_size = len(df) // partitions

        for idx in range(partitions):
            start = idx * chunk_size
            end = (idx + 1) * chunk_size if idx < partitions - 1 else len(df)
            df_partition = df.slice(start, end - start)
            df_partition.write_parquet(subdir / f"partition_{idx}.parquet")

            _LOGGER.info(
                f"Wrote Parquet file for partition {idx + 1:_}/{partitions:_} "
                f"with shape ({df_partition.shape[0]:_}, {df_partition.shape[1]:_})"
            )

        return subdir

    def _write_partitioned_parquet_lazy(self, parent: Path, df: pl.LazyFrame, partitions: int) -> Path:
        subdir = parent / uuid.uuid4().hex
        subdir.mkdir(parents=True, exist_ok=False)

        row_count = int(df.select(pl.len()).collect().item(0, 0))
        chunk_size = max(1, ceil(row_count / partitions))
        total_rows = 0

        for idx, batch in enumerate(df.collect_batches(chunk_size=chunk_size)):
            batch_rows = batch.shape[0]
            total_rows += batch_rows
            batch.write_parquet(subdir / f"partition_{idx}.parquet")

            _LOGGER.info(
                f"Wrote Parquet file for partition {idx + 1:_}/{partitions:_} "
                f"with shape ({batch_rows:_}, {batch.shape[1]:_})"
            )

        if total_rows != row_count:
            raise RuntimeError(f"Partitioned Parquet staging wrote {total_rows:_} rows, expected {row_count:_}")

        return subdir

    def _wait_for_parquet_readable(self, input_file: str, timeout_seconds: float = 10.0) -> None:
        deadline = perf_counter() + timeout_seconds

        while perf_counter() < deadline:
            try:
                statement = f"DESCRIBE file('{input_file}', Parquet)"
                with self.record_query_execution(statement):
                    cast(Any, self.get_client()).command(statement)
                return
            except Exception:
                sleep(0.1)

        raise TimeoutError(f"Timed out after {timeout_seconds:.0f}s waiting for ClickHouse to read {input_file}")

    def _cleanup_temporary_parquet(self, p: Path) -> None:
        if p.is_dir():
            rmtree(p)
        elif p.is_file():
            p.unlink()
        else:
            raise RuntimeError(f"Invalid value for {p = }")

    def _write_temporary_parquet(
        self, df: pl.DataFrame | pl.LazyFrame, temp_dir: Path, partitions: int | None
    ) -> tuple[Path, str]:
        # inserting very large Parquet files in a single chunk causes OOM-related issues,
        # e.g. for Clickbench (7.4 GB Parquet)
        # better to insert as partitioned files instead (using wildcard file('*.parquet'))
        if partitions is None:
            temp_parquet_path = self._write_single_parquet(temp_dir, df)
            input_file_string = temp_parquet_path.relative_to(temp_dir).as_posix()
        else:
            if isinstance(df, pl.LazyFrame):
                temp_parquet_path = self._write_partitioned_parquet_lazy(temp_dir, df, partitions)
            else:
                temp_parquet_path = self._write_partitioned_parquet(temp_dir, df, partitions)
            input_file_string = temp_parquet_path.relative_to(temp_dir).as_posix() + "/*.parquet"

        self._wait_for_parquet_readable(input_file_string)
        return temp_parquet_path, input_file_string

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
        partitions: int | None = None,
    ) -> None:
        not_null = normalize_columns(not_null)

        schema = df.schema if isinstance(df, pl.DataFrame) else df.collect_schema()
        columns = list(schema.names())

        client = self.get_client()
        temp_dir = SETTINGS.temporary_directory / "clickhouse/data"
        temp_parquet_path, input_file_string = self._write_temporary_parquet(df, temp_dir, partitions)

        try:
            exists_result = cast(Any, client).query_df(f"EXISTS TABLE {table}")
            table_exists = bool(exists_result["result"][0])

            if not table_exists:
                columns_def: list[str] = []
                for name, dtype in schema.items():
                    sql_type = get_clickhouse_type(dtype, nullable=name not in not_null)

                    columns_def.append(f"`{name}` {sql_type}")

                column_list = ", ".join(f"`{col}`" for col in columns if col != "time")

                # time is read as epoch integer by default
                time_col_def = "toDateTime(time) AS time," if "time" in columns else ""

                order_by = self.get_order_by_columns(df, primary_key, not_null)
                order_by_clause = f"order by ({order_by})" if order_by is not None else ""

                sql = f"""
                    create table {table} (
                        {", ".join(columns_def)}
                    )
                    engine = MergeTree
                    -- an order by clause is equivalent to a primary key (pk is not unique)
                    -- the primary key clause can be omitted (can be used to limit indexes to only one of the sort keys)
                    {order_by_clause}
                    -- ensure temporary data is cleaned up almost immediately
                    settings old_parts_lifetime = 5, allow_nullable_key = 1
                    as select
                        {time_col_def}
                        {column_list}
                    from file('{input_file_string}', Parquet)
                """
            else:
                sql = f"""
                    insert into {table}
                    select * from file('{input_file_string}', Parquet)
                """

            _LOGGER.info("Running insert query...")
            self.run_sql(sql)
            _LOGGER.info("Finished insert query")

        finally:
            self._cleanup_temporary_parquet(temp_parquet_path)

    def upsert(
        self,
        df: pl.DataFrame,
        table: TableName,
        primary_key: str | list[str],
        partitions: int | None = None,
    ) -> None:
        temp_dir = SETTINGS.temporary_directory / "clickhouse/data"
        temp_parquet_path, input_file_string = self._write_temporary_parquet(df, temp_dir, partitions)

        try:
            pk_list = normalize_columns(primary_key)

            where_clause = self._build_key_filter(input_file_string, pk_list)
            delete_sql = f"delete from {table} where {where_clause}"
            self.run_sql(delete_sql, settings={"mutations_sync": 1})

            sql = f"""
                insert into {table}
                select * from file('{input_file_string}', parquet)
            """

            self.run_sql(sql)

        finally:
            self._cleanup_temporary_parquet(temp_parquet_path)

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        primary_keys = require_columns(primary_key)

        temp_dir = SETTINGS.temporary_directory / "clickhouse/data"
        temp_parquet_path, input_file_string = self._write_temporary_parquet(keys, temp_dir, None)

        try:
            where_clause = self._build_key_filter(input_file_string, primary_keys)
            delete_sql = f"delete from {table} where {where_clause}"
            self.run_sql(delete_sql, settings={"mutations_sync": 1})
        finally:
            self._cleanup_temporary_parquet(temp_parquet_path)

    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        return {
            **super().suite_registry(),
            "rtabench": ClickHouseRTABench,
            "clickbench": ClickhouseClickbench,
            "jsonbench": ClickhouseJSONBench,
            "time_series": ClickhouseTimeseries,
            "tpc_ds": ClickhouseTpcDs,
        }
