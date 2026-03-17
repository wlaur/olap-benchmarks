import logging
from collections.abc import Mapping
from typing import Any, Literal, cast

import polars as pl
from sqlalchemy import Connection, create_engine, text

from ...settings import SETTINGS, DatabaseName, TableName
from ...suites.kaggle_airbnb.config import KaggleAirbnb
from ...suites.time_series.config import TimeSeries
from .. import Database
from .fetch import fetch_binary, fetch_pymonetdb
from .insert import (
    DEFAULT_LAZY_WRITE,
    ColumnGroupWrite,
    LazyWrite,
    MonetDBInsertKwargs,
    insert,
    upsert,
)
from .insert import delete as monetdb_delete
from .settings import SETTINGS as MONETDB_SETTINGS
from .utils import get_pymonetdb_connection

_LOGGER = logging.getLogger(__name__)

LOCAL_IMAGE = False

if LOCAL_IMAGE:
    # built from https://github.com/MonetDBSolutions/monetdb-docker
    # docker build -t monetdb-local:Mar2025-11 -f ubuntu.dockerfile --platform linux/amd64 --build-arg BRANCH=Mar2025_11 . # noqa: E501
    # NOTE: getting 401 error from https://www.monetdb.org/hg/MonetDB/archive/${BRANCH}.tar.bz2
    # need to modify Dockerfile to use https://github.com/MonetDB/MonetDB/archive/refs/tags/${BRANCH}.tar.gz instead
    # change in monetdb-docker/ubuntu.Dockerfile:
    # RUN curl -L -o MonetDB.tar.gz https://github.com/MonetDB/MonetDB/archive/refs/tags/${BRANCH}.tar.gz
    # RUN tar zxf MonetDB.tar.gz

    # TODO: fails with "#main-thread: log_read_types_file: ERROR: unknown type in log file 'mbr'"
    # when starting a db created with Mar2025-SP1
    # (this is an unreleased version, will probably be fixed before SP2 is released)
    _version = "Mar2025-11"
    _docker_image = f"monetdb-local:{_version}"
else:
    _version = "Dec2025-SP1"
    _docker_image = f"monetdb/monetdb:{_version}"

VERSION = _version
DOCKER_IMAGE = _docker_image

MONETDB_CONNECTION_STRING = "monetdb://monetdb:monetdb@localhost:50000/benchmark"


class MonetDBTimeSeries(TimeSeries["MonetDB"]):
    def insert_table(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table_name: TableName,
        primary_key: str | list[str] | None,
        not_null: str | list[str] | None,
    ) -> None:
        kwargs: MonetDBInsertKwargs = {"lazy_write": ColumnGroupWrite(group_size=10)}
        self.db.insert(df, table_name, primary_key=primary_key, not_null=not_null, **kwargs)

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        assert self.db.context is not None

        if "batch_export" in self.db.context.query_name:
            return {"method": "binary"}

        return {"method": "pymonetdb"}


class MonetDBKaggleAirbnb(KaggleAirbnb["MonetDB"]):
    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        assert self.db.context is not None

        pymonetdb_queries = [
            "01_calendar_count",
        ]

        if any(n in self.db.context.query_name for n in pymonetdb_queries):
            return {"method": "pymonetdb"}

        return {"method": "binary"}


class MonetDB(Database):
    name: DatabaseName = "monetdb"
    version: str = VERSION

    connection_string: str = MONETDB_CONNECTION_STRING

    @property
    def start(self) -> str:
        (SETTINGS.temporary_directory / "monetdb/data").mkdir(exist_ok=True, parents=True)

        parts = [
            f"docker run --platform linux/amd64 --name {self.name}-benchmark --rm -d -p 50000:50000",
            f"-v {self.database_directory.as_posix()}:/var/monetdb5/dbfarm",
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
            df = fetch_pymonetdb(query, self.connect())
        else:
            raise ValueError(f"Invalid method: '{method}'")

        if schema is not None:
            df = df.cast(cast(pl.Schema, schema))

        return df

    def rollback(self) -> None:
        if self._connection is None:
            return

        # MonetDB reads and writes may use the raw DBAPI cursor directly, bypassing
        # SQLAlchemy's transaction bookkeeping. Clear both SQLAlchemy's view and the
        # underlying MonetDB transaction state.
        super().rollback()
        get_pymonetdb_connection(self._connection).rollback()

    def get_table_names(self) -> set[TableName]:
        df = self.fetch(
            "select name as table_name from sys.tables where system = false",
            schema={"table_name": pl.String},
            method="pymonetdb",
        )
        return set(df.get_column("table_name").to_list())

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
        lazy_write: LazyWrite = DEFAULT_LAZY_WRITE,
    ) -> None:
        result = self.connect().execute(
            text("SELECT count(*) FROM sys.tables WHERE name = :table_name"),
            {"table_name": table},
        )
        exists = bool(result.scalar())

        try:
            return insert(df, table, self.connect(), primary_key, not_null, create=not exists, lazy_write=lazy_write)
        except Exception:
            self.rollback()
            raise

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        return upsert(df, table, self.connect(), primary_key=primary_key)

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        return monetdb_delete(table, self.connect(), primary_key=primary_key, keys=keys)

    @property
    def time_series(self) -> MonetDBTimeSeries:
        return MonetDBTimeSeries(db=self)

    @property
    def kaggle_airbnb(self) -> MonetDBKaggleAirbnb:
        return MonetDBKaggleAirbnb(db=self)
