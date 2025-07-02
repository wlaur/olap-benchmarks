from collections.abc import Mapping

import polars as pl
from sqlalchemy import Connection, create_engine

from ..database import Database
from ..settings import SETTINGS, TableName
from .fetch import fetch_binary, fetch_pymonetdb
from .insert import insert, upsert


class MonetDB(Database):
    name: str = "monetdb"
    start: str = (
        f"docker run --platform linux/amd64 --name monetdb-benchmark --rm -d -p 50000:50000 "
        f"-v {SETTINGS.database_directory.as_posix()}/monetdb:/var/monetdb5/dbfarm "
        "-e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=benchmark "
        "monetdb/monetdb:Mar2025"
    )
    stop: str = "docker kill monetdb-benchmark"

    def connect(self, reconnect: bool = False) -> Connection:
        if reconnect:
            self._connection = None

        if self._connection is not None:
            return self._connection

        engine = create_engine("monetdb://monetdb:monetdb@localhost:50000/benchmark")
        self._connection = engine.connect()

        return self._connection

    def fetch(self, query: str, schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None) -> pl.DataFrame:
        return fetch_binary(query, self.connect(), schema)

    def fetch_pymonetdb(self, query: str) -> pl.DataFrame:
        return fetch_pymonetdb(query, self.connect())

    def insert(self, df: pl.DataFrame, table: TableName):
        return insert(df, table, self.connect())

    def upsert(self, df: pl.DataFrame, table: TableName):
        return upsert(df, table, self.connect())
