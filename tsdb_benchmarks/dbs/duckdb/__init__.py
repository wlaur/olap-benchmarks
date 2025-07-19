from collections.abc import Mapping
from typing import Literal, cast

import polars as pl
from duckdb import DuckDBPyConnection
from sqlalchemy import Connection, create_engine

from ...settings import SETTINGS, TableName
from .. import Database

(SETTINGS.database_directory / "duckdb").mkdir(exist_ok=True)
DUCKDB_CONNECTION_STRING = f"duckdb:///{SETTINGS.database_directory.as_posix()}/duckdb/duck.db"


def get_duckdb_connection(connection: Connection) -> DuckDBPyConnection:
    return cast(DuckDBPyConnection, connection._dbapi_connection)


class DuckDB(Database):
    name: Literal["duckdb"] = "duckdb"
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

    def insert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str] | None = None) -> None:
        con = get_duckdb_connection(self.connect())

        result = con.execute(
            f"select count(*) from information_schema.tables where table_name = '{table.lower()}'"
        ).fetchone()

        assert result is not None
        count = cast(int, result[0])

        con.register("source", df)

        if count > 0:
            con.execute(f'insert into "{table}" select * from source')
            con.commit()

            # assume the primary key is correctly set if the table already exists
            return

        con.execute(f'create table "{table}" as select * from source')
        con.commit()

        if primary_key is None:
            return

        primary_keys = [primary_key] if isinstance(primary_key, str) else primary_key

        for pk in primary_keys:
            if pk not in df.columns:
                raise ValueError(f"Primary key column '{pk}' not found in dataset")

        pk_clause = ", ".join(f'"{pk}"' for pk in primary_keys)
        con.execute(f'alter table "{table}" add primary key ({pk_clause})')
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
                insert into "{table}"
                select * from source
                on conflict ({conflict_target}) do nothing
            """
            con.execute(sql)
            con.commit()
            return

        set_clause = ", ".join(f'"{col}" = excluded."{col}"' for col in non_key_columns)

        conflict_target = ", ".join(f'"{col}"' for col in primary_keys)

        sql = f"""
            insert into "{table}"
            select * from source
            on conflict ({conflict_target}) do update set {set_clause}
        """

        con.execute(sql)
        con.commit()
