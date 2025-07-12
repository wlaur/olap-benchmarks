from collections.abc import Mapping

import polars as pl
from sqlalchemy import Connection, create_engine

from ...database import Database
from ...settings import SETTINGS, TableName
from .fetch import fetch_binary, fetch_pymonetdb
from .insert import insert, upsert
from .settings import SETTINGS as MONETDB_SETTINGS

# does not seem to be SP1 (is actually Mar2025)
# MONETDB_IMAGE = "monetdb/monetdb:Mar2025-SP1"

# built from https://github.com/MonetDBSolutions/monetdb-docker
# with
# docker build -t monetdb-local:Mar2025-SP1 -f ubuntu.dockerfile \
# --platform linux/amd64 --build-arg BRANCH=Mar2025_SP1_release .
MONETDB_IMAGE = "monetdb-local:Mar2025-SP1"


def get_start_command() -> str:
    parts = [
        "docker run --platform linux/amd64 --name monetdb-benchmark --rm -d -p 50000:50000",
        f"-v {SETTINGS.database_directory.as_posix()}/monetdb:/var/monetdb5/dbfarm",
        f"-v {SETTINGS.temporary_directory.as_posix()}/monetdb/data:/data"
        if not MONETDB_SETTINGS.client_file_transfer
        else "",
        "-e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=benchmark",
        MONETDB_IMAGE,
    ]

    return " ".join(parts)


class MonetDB(Database):
    name: str = "monetdb"

    start: str = get_start_command()
    stop: str = "docker kill monetdb-benchmark"
    restart: str = "docker restart monetdb-benchmark"

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

    def insert(self, df: pl.DataFrame, table: TableName) -> None:
        return insert(df, table, self.connect())

    def upsert(self, df: pl.DataFrame, table: TableName) -> None:
        return upsert(df, table, self.connect())
