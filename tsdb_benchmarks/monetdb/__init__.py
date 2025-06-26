import polars as pl
from sqlalchemy import Connection, create_engine

from ..database import Database
from ..settings import SETTINGS, TableName
from .fetch import fetch_binary
from .insert import insert, upsert


class MonetDB(Database):
    name = "monetdb"
    start = (
        f"docker run --name monetdb-benchmark --rm -d -p 50000:50000 "
        f"-v {SETTINGS.database_directory.as_posix()}/monetdb:/var/monetdb5/dbfarm monetdb/monetdb:Mar2025"
    )
    stop = "docker kill monetdb-benchmark"

    def connect(self) -> Connection:
        engine = create_engine("monetdb://monetdb:monetdb@localhost:50000/benchmark")
        return engine.connect()

    def fetch(self, query: str) -> pl.DataFrame:
        return fetch_binary(query)

    def insert(self, df: pl.DataFrame, table: TableName):
        return insert(df, table)

    def upsert(self, df: pl.DataFrame, table: TableName):
        return upsert(df, table)
