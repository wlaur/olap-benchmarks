import logging
import uuid
from collections.abc import Mapping
from pathlib import Path
from shutil import rmtree
from time import sleep
from typing import Any, Literal, cast
from urllib.parse import urlparse

import clickhouse_connect
import clickhouse_connect.driver
import clickhouse_connect.driver.client
import polars as pl
from clickhouse_connect.driver.client import Client as ClickhouseClient
from sqlalchemy import Connection, create_engine

from ...settings import SETTINGS, TableName
from .. import Database

_LOGGER = logging.getLogger(__name__)

DOCKER_IMAGE = "clickhouse:25.6.3.116-jammy"

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
        # NOTE: timestamp is never nullable (overrides parameter not_null to the insert method)
        return "DateTime('UTC')"

    sql_type = POLARS_CLICKHOUSE_TYPE_MAP.get(dtype)

    if sql_type is None:
        raise ValueError(f"Unsupported Polars dtype: {dtype}")

    if nullable:
        return f"Nullable({sql_type})"
    else:
        return sql_type


def get_clickhouse_client() -> ClickhouseClient:
    parsed_sqlalchemy_connection_string = urlparse(CLICKHOUSE_CONNECTION_STRING)

    return clickhouse_connect.get_client(
        host=parsed_sqlalchemy_connection_string.hostname,
        port=parsed_sqlalchemy_connection_string.port or 18123,
        username=parsed_sqlalchemy_connection_string.username,
        password=parsed_sqlalchemy_connection_string.password or "no-password",
        database="default",
    )


class Clickhouse(Database):
    name: Literal["clickhouse"] = "clickhouse"
    connection_string: str = CLICKHOUSE_CONNECTION_STRING

    _clickhouse_client: clickhouse_connect.driver.client.Client | None = None

    @property
    def start(self) -> str:
        (SETTINGS.database_directory / "clickhouse").mkdir(exist_ok=True)
        (SETTINGS.temporary_directory / "clickhouse/data").mkdir(exist_ok=True, parents=True)

        parts = [
            f"docker run --platform linux/amd64 --name {self.name}-benchmark --rm -d -p 18123:8123 -p 19000:9000",
            f"-v {SETTINGS.database_directory.as_posix()}/clickhouse:/var/lib/clickhouse",
            f"-v {SETTINGS.temporary_directory.as_posix()}/clickhouse/data:/var/lib/clickhouse/user_files",
            # does not seem to be able to create a new dt "benchmark", use the default name "default" instead
            "-e CLICKHOUSE_DB=default",
            "-e CLICKHOUSE_PASSWORD=password",
            "-e CLICKHOUSE_USER=user",
            "-e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
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

    def get_client(self) -> clickhouse_connect.driver.client.Client:
        if self._clickhouse_client is not None:
            return self._clickhouse_client

        self._clickhouse_client = get_clickhouse_client()
        return self._clickhouse_client

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
        time_columns: str | list[str] | None = None,
    ) -> pl.DataFrame:
        query = query.strip().removesuffix(";")

        # query_arrow converts datetime to epoch second
        df = cast(pl.DataFrame, pl.from_arrow(self.get_client().query_arrow(query)))

        if schema is not None:
            df = df.cast(schema)  # type: ignore[arg-type]

        if time_columns is None:
            time_columns = []

        if isinstance(time_columns, str):
            time_columns = [time_columns]

        if "time" not in time_columns:
            time_columns.append("time")

        for n in time_columns:
            if n in df.columns:
                df = df.with_columns(pl.from_epoch(n, "s").cast(pl.Datetime("ms")))

        return df

    def run_sql(self, statement: str) -> None:
        retries = 10
        for retry in range(retries):
            try:
                self.get_client().command(statement)
                return
            except Exception as e:
                if "error code 1001" in str(e):
                    _LOGGER.warning(f"Could not execute statement: '{e}', retrying {retry + 1:_}/{retries:_}")
                    sleep(0.1)
                    continue
                # might happen if the parquet file is not fully written when clickhouse tries to read it
                if "error code 636" in str(e):
                    raise e

                raise

    def _get_order_by_columns(
        self,
        df: pl.DataFrame,
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
            order_by = df.columns[0]
        elif isinstance(primary_key, str):
            order_by = primary_key
        else:
            order_by = ", ".join(primary_key)

        return order_by

    def _write_single_parquet(self, parent: Path, df: pl.DataFrame) -> Path:
        temp_file = parent / f"{uuid.uuid4().hex}.parquet"
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

    def _cleanup_temporary_parquet(self, p: Path) -> None:
        if p.is_dir():
            rmtree(p)
        elif p.is_file():
            p.unlink()
        else:
            raise RuntimeError(f"Invalid value for {p = }")

    def _write_temporary_parquet(self, df: pl.DataFrame, temp_dir: Path, partitions: int | None) -> tuple[Path, str]:
        # inserting very large Parquet files in a single chunk causes OOM-related issues,
        # e.g. for Clickbench (7.4 GB Parquet)
        # better to insert as partitioned files instead (using wildcard file('*.parquet'))
        if partitions is None:
            temp_parquet_path = self._write_single_parquet(temp_dir, df)
            input_file_string = temp_parquet_path.relative_to(temp_dir).as_posix()
        else:
            temp_parquet_path = self._write_partitioned_parquet(temp_dir, df, partitions)
            input_file_string = temp_parquet_path.relative_to(temp_dir).as_posix() + "/*.parquet"

        return temp_parquet_path, input_file_string

    def insert(
        self,
        df: pl.DataFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
        partitions: int | None = None,
    ) -> None:
        if not_null is None:
            not_null = []

        if isinstance(not_null, str):
            not_null = [not_null]

        client = self.get_client()
        temp_dir = SETTINGS.temporary_directory / "clickhouse/data"
        temp_parquet_path, input_file_string = self._write_temporary_parquet(df, temp_dir, partitions)

        try:
            exists_result = client.query_df(f"EXISTS TABLE {table}")
            table_exists = bool(exists_result["result"][0])

            if not table_exists:
                columns_def: list[str] = []
                for name, dtype in df.schema.items():
                    sql_type = get_clickhouse_type(dtype, nullable=name not in not_null)

                    columns_def.append(f"`{name}` {sql_type}")

                column_list = ", ".join(f"`{col}`" for col in df.columns if col != "time")

                # time is read as epoch integer by default
                time_col_def = "toDateTime(time) AS time," if "time" in df.columns else ""

                order_by = self._get_order_by_columns(df, primary_key, not_null)
                order_by_clause = f"order by ({order_by})" if order_by is not None else ""

                sql = f"""
                    create table {table} (
                        {", ".join(columns_def)}
                    )
                    engine = MergeTree
                    -- an order by clause is equivalent to a primary key (pk is not unique)
                    -- the primary key clause can be omitted (can be used to limit indexes to only one of the sort keys)
                    {order_by_clause}
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
            pk_list = [primary_key] if isinstance(primary_key, str) else primary_key

            where_clause = " and ".join(
                f"{col} in (select distinct {col} from file('{input_file_string}', parquet))" for col in pk_list
            )

            delete_sql = f"delete from {table} where {where_clause}"
            self.run_sql(delete_sql)

            sql = f"""
                insert into {table}
                select * from file('{input_file_string}', parquet)
            """

            self.run_sql(sql)

        finally:
            self._cleanup_temporary_parquet(temp_parquet_path)

    @property
    def rtabench_fetch_kwargs(self) -> dict[str, Any]:
        return {"time_columns": ["hour", "day"]}

    @property
    def time_series_fetch_kwargs(self) -> dict[str, Any]:
        return {"time_columns": ["time"]}

    @property
    def clickbench_populate_kwargs(self) -> dict[str, Any]:
        # same number of partitions as the official clickbench insert
        return {"partitions": 100}

    def optimize_clickbench_table(self) -> None:
        # not 100% clear if this is necessary, but seems to force cleaning up inactive parts
        self.run_sql("optimize table hits")

    def populate_clickbench(self, restart: bool = True) -> None:
        super().populate_clickbench(restart=False)

        with self.event_context("optimize"):
            self.optimize_clickbench_table()

        if restart:
            self.restart_event()
