import uuid
from collections.abc import Mapping
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

    _clickhouse_client: clickhouse_connect.driver.client.Client | None = None

    @property
    def start(self) -> str:
        (SETTINGS.database_directory / "clickhouse").mkdir(exist_ok=True)
        (SETTINGS.temporary_directory / "clickhouse/data").mkdir(exist_ok=True)

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

        engine = create_engine(CLICKHOUSE_CONNECTION_STRING)
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

    def insert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str] | None = None) -> None:
        client = self.get_client()
        temp_dir = SETTINGS.temporary_directory / "clickhouse/data"

        temp_file = temp_dir / f"{table}_{uuid.uuid4().hex}.parquet"
        relative_path = temp_file.relative_to(temp_dir).as_posix()
        df.write_parquet(temp_file)

        try:
            exists_result = client.query_df(f"EXISTS TABLE {table}")
            table_exists = bool(exists_result["result"][0])

            if primary_key is None and "time" in df.columns:
                order_by = "time"
            elif primary_key is None:
                order_by = df.columns[0]
            elif isinstance(primary_key, str):
                order_by = primary_key
            else:
                order_by = ", ".join(primary_key)

            if not table_exists:
                columns_def: list[str] = []
                for name, dtype in df.schema.items():
                    sql_type = get_clickhouse_type(dtype, nullable=True)

                    columns_def.append(f"`{name}` {sql_type}")

                column_list = ", ".join(f"`{col}`" for col in df.columns if col != "time")

                # time is read as epoch integer by default
                time_col_def = "toDateTime(time) AS time," if "time" in df.columns else ""

                sql = f"""
                    CREATE TABLE {table} (
                        {", ".join(columns_def)}
                    )
                    ENGINE = MergeTree
                    ORDER BY ({order_by})
                    AS SELECT
                        {time_col_def}
                        {column_list}
                    FROM file('{relative_path}', Parquet)
                """
            else:
                sql = f"""
                    INSERT INTO {table}
                    SELECT * FROM file('{relative_path}', Parquet)
                """

            client.command(sql)

        finally:
            temp_file.unlink()

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        client = self.get_client()
        temp_dir = SETTINGS.temporary_directory / "clickhouse/data"

        temp_file = temp_dir / f"{table}_{uuid.uuid4().hex}.parquet"
        relative_path = temp_file.relative_to(temp_dir).as_posix()
        df.write_parquet(temp_file)

        try:
            pk_list = [primary_key] if isinstance(primary_key, str) else primary_key

            where_clause = " and ".join(
                f"{col} in (select distinct {col} from file('{relative_path}', parquet))" for col in pk_list
            )

            delete_sql = f"delete from {table} where {where_clause}"
            client.command(delete_sql)

            sql = f"""
                insert into {table}
                select * from file('{relative_path}', parquet)
            """

            client.command(sql)

        finally:
            temp_file.unlink()

    @property
    def rtabench_fetch_kwargs(self) -> dict[str, Any]:
        return {"time_columns": ["hour", "day"]}
