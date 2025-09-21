import logging
import uuid
from collections.abc import Mapping
from typing import Any, Literal, cast

import polars as pl
from duckdb import DuckDBPyConnection  # type: ignore[import-untyped]
from duckdb import __version__ as duckdb_version_runtime
from sqlalchemy import Connection, create_engine

from ...settings import SETTINGS, TableName
from ...suites.clickbench.config import Clickbench
from .. import Database

_LOGGER = logging.getLogger(__name__)

VERSION = "1.3.2"

assert duckdb_version_runtime == VERSION

(SETTINGS.database_directory / "duckdb").mkdir(exist_ok=True)
(SETTINGS.temporary_directory / "duckdb/data").mkdir(exist_ok=True, parents=True)

DUCKDB_CONNECTION_STRING = f"duckdb:///{SETTINGS.database_directory.as_posix()}/duckdb/duck.db"


POLARS_DUCKDB_TYPE_MAP: dict[pl.DataType | type[pl.DataType], str] = {
    pl.Int8: "TINYINT",
    pl.Int16: "SMALLINT",
    pl.Int32: "INTEGER",
    pl.Int64: "BIGINT",
    pl.UInt8: "UTINYINT",
    pl.UInt16: "USMALLINT",
    pl.UInt32: "UINTEGER",
    pl.UInt64: "UBIGINT",
    pl.Float32: "FLOAT",
    pl.Float64: "DOUBLE",
    pl.Boolean: "BOOLEAN",
    pl.String: "TEXT",
    pl.Date: "DATE",
    pl.Datetime: "TIMESTAMP",
    pl.Time: "TIME",
    pl.Duration: "BIGINT",
    pl.Object: "BLOB",
    pl.Struct: "JSON",
}


def get_duckdb_connection(connection: Connection) -> DuckDBPyConnection:
    return cast(DuckDBPyConnection, connection._dbapi_connection)


def polars_dtype_to_duckdb(dtype: pl.DataType) -> str:
    for pl_type, duck_type in POLARS_DUCKDB_TYPE_MAP.items():
        if dtype == pl_type:
            return duck_type
    raise ValueError(f"Unsupported Polars dtype: {dtype}")


class DuckDBClickbench(Clickbench):
    @property
    def populate_kwargs(self) -> dict[str, Any]:
        # uses > 40g memory otherwise
        return {"in_memory": False}


class DuckDB(Database):
    name: Literal["duckdb"] = "duckdb"
    version: str = VERSION

    connection_string: str = DUCKDB_CONNECTION_STRING

    # in-process, no docker commands necessary
    @property
    def start(self) -> str:
        return ""

    @property
    def stop(self) -> str:
        return ""

    @property
    def restart(self) -> str:
        return ""

    def connect(self, reconnect: bool = False) -> Connection:
        if self._connection is not None:
            return self._connection

        engine = create_engine(self.connection_string)
        self._connection = engine.connect()

        return self._connection

    def fetch(self, query: str, schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None) -> pl.DataFrame:
        con = get_duckdb_connection(self.connect())
        con.execute(query)
        df = con.pl()

        if schema is not None:
            df = df.cast(schema)  # type: ignore[arg-type]

        return df

    def insert(
        self,
        df: pl.DataFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
        in_memory: bool = True,
    ) -> None:
        con = get_duckdb_connection(self.connect())

        result = con.execute(
            f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table.lower()}'"
        ).fetchone()

        assert result is not None
        table_exists = cast(int, result[0]) > 0

        if not table_exists:
            not_null_cols = {not_null} if isinstance(not_null, str) else set(not_null or [])
            primary_keys = [primary_key] if isinstance(primary_key, str) else (primary_key or [])

            col_defs: list[str] = []
            for name, dtype in df.schema.items():
                duck_type = polars_dtype_to_duckdb(dtype)
                constraints = []
                if name in not_null_cols:
                    constraints.append("not null")

                col_def = f'"{name}" {duck_type} {" ".join(constraints)}'
                col_defs.append(col_def)

            pk_clause = f", primary key ({', '.join(f'"{pk}"' for pk in primary_keys)})" if primary_keys else ""
            ddl = f"create table {table} (\n  " + ",\n  ".join(col_defs) + pk_clause + "\n)"
            con.execute(ddl)

        if in_memory:
            con.register("source", df)
            _LOGGER.info(f"Inserting from in-memory dataset with shape ({df.shape[0]:_}, {df.shape[1]:_})")

            con.execute(f"insert into {table} select * from source")
        else:
            fpath = SETTINGS.temporary_directory / "duckdb/data" / f"{uuid.uuid4().hex}.parquet"
            df.write_parquet(fpath)
            _LOGGER.info(f"Inserting from Parquet dataset with shape ({df.shape[0]:_}, {df.shape[1]:_})")

            try:
                con.execute(f"insert into {table} select * from '{fpath.as_posix()}'")
            finally:
                fpath.unlink()

        con.commit()

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        primary_keys = [primary_key] if isinstance(primary_key, str) else primary_key

        if not primary_keys:
            raise ValueError("primary_key must be a non-empty string or list of strings")

        for pk in primary_keys:
            if pk not in df.columns:
                raise ValueError(f"Primary key column '{pk}' not found in DataFrame columns")

        con = get_duckdb_connection(self.connect())

        con.register("source", df)

        non_key_columns = [col for col in df.columns if col not in primary_keys]

        if not non_key_columns:
            conflict_target = ", ".join(f'"{col}"' for col in primary_keys)
            sql = f"""
                insert into {table}
                select * from source
                on conflict ({conflict_target}) do nothing
            """
            con.execute(sql)
            con.commit()
            return

        set_clause = ", ".join(f'"{col}" = excluded."{col}"' for col in non_key_columns)

        conflict_target = ", ".join(f'"{col}"' for col in primary_keys)

        sql = f"""
            insert into {table}
            select * from source
            on conflict ({conflict_target}) do update set {set_clause}
        """

        con.execute(sql)
        con.commit()

    @property
    def clickbench(self) -> DuckDBClickbench:
        return DuckDBClickbench(db=self)
