import logging
from collections.abc import Mapping
from typing import Any, Literal

import polars as pl
from sqlalchemy import Connection, create_engine, text

from ...settings import SETTINGS, TableName
from ...suites.time_series.config import TimeSeries
from .. import Database
from .fetch import fetch_binary, fetch_pymonetdb
from .insert import insert, upsert
from .settings import SETTINGS as MONETDB_SETTINGS

_LOGGER = logging.getLogger(__name__)

VERSION = "Mar2025-SP1"

# does not seem to be SP1 (is actually Mar2025)
# MONETDB_IMAGE = "monetdb/monetdb:Mar2025-SP1"

# built from https://github.com/MonetDBSolutions/monetdb-docker
# with
# docker build -t monetdb-local:Mar2025-SP1 -f ubuntu.dockerfile \
# --platform linux/amd64 --build-arg BRANCH=Mar2025_SP1_release .
DOCKER_IMAGE = f"monetdb-local:{VERSION}"

MONETDB_CONNECTION_STRING = "monetdb://monetdb:monetdb@localhost:50000/benchmark"


class MonetDBTimeSeries(TimeSeries):
    def get_not_null(self, table_name: TableName) -> str | list[str] | None:
        # terrible insert performance if primary key or not null constraints are used for eav tables
        return None if "_eav" in table_name else "time"

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        assert self.db.context is not None

        if "batch_export" in self.db.context.query_name:
            return {"method": "binary"}

        return {}


class MonetDB(Database):
    name: Literal["monetdb"] = "monetdb"
    version: str = VERSION

    connection_string: str = MONETDB_CONNECTION_STRING

    @property
    def start(self) -> str:
        (SETTINGS.database_directory / "monetdb").mkdir(exist_ok=True)
        (SETTINGS.temporary_directory / "monetdb/data").mkdir(exist_ok=True, parents=True)

        parts = [
            f"docker run --platform linux/amd64 --name {self.name}-benchmark --rm -d -p 50000:50000",
            f"-v {SETTINGS.database_directory.as_posix()}/monetdb:/var/monetdb5/dbfarm",
            f"-v {SETTINGS.temporary_directory.as_posix()}/monetdb/data:/data"
            if not MONETDB_SETTINGS.client_file_transfer
            else "",
            "-e MDB_DB_ADMIN_PASS=monetdb -e MDB_CREATE_DBS=benchmark",
            DOCKER_IMAGE,
        ]

        return " ".join(parts)

    def connect(self, reconnect: bool = False) -> Connection:
        if reconnect:
            self._connection = None

        if self._connection is not None:
            return self._connection

        engine = create_engine(
            self.connection_string,
            # avoid crash "ImportError: sys.meta_path is None, Python is likely shutting down"
            # not clear why this happens
            pool_reset_on_return=None,
        )

        self._connection = engine.connect()

        return self._connection

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
        method: Literal["binary", "pymonetdb"] | None = None,
    ) -> pl.DataFrame:
        method = method or MONETDB_SETTINGS.default_fetch_method

        _LOGGER.info(f"Fetching with {method=}")

        if method == "binary":
            return fetch_binary(query, self.connect(), schema)
        elif method == "pymonetdb":
            return fetch_pymonetdb(query, self.connect())
        else:
            raise ValueError(f"Invalid method: '{method}'")

    def insert(
        self,
        df: pl.DataFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        result = self.connect().execute(
            text("SELECT count(*) FROM sys.tables WHERE name = :table_name"), {"table_name": table}
        )
        exists = bool(result.scalar())

        return insert(df, table, self.connect(), primary_key, not_null, create=not exists)

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        return upsert(df, table, self.connect(), primary_key=primary_key)

    @property
    def time_series(self) -> MonetDBTimeSeries:
        return MonetDBTimeSeries(db=self)
