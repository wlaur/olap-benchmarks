import logging
import uuid
from collections.abc import Mapping
from typing import Literal

import polars as pl
from sqlalchemy import Connection, create_engine, text

from ...settings import SETTINGS, TableName
from .. import Database

_LOGGER = logging.getLogger(__name__)

DOCKER_IMAGE = "questdb/questdb:9.0.1-rhel"


class QuestDB(Database):
    name: Literal["questdb"] = "questdb"
    connection_string: str = "questdb://admin:quest@localhost:8812/qdb"

    @property
    def start(self) -> str:
        (SETTINGS.database_directory / "questdb").mkdir(exist_ok=True)
        (SETTINGS.temporary_directory / "questdb/data").mkdir(exist_ok=True, parents=True)

        parts = [
            f"docker run --platform linux/amd64 --name {self.name}-benchmark --rm -d -p 9000:9000 -p 8812:8812",
            f"-v {SETTINGS.database_directory.as_posix()}/questdb:/var/lib/questdb",
            f"-v {SETTINGS.temporary_directory.as_posix()}/questdb/data:/import",
            "-e QDB_CAIRO_SQL_COPY_ROOT=/import",
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

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
        method: Literal["connectorx", "python"] = "connectorx",
    ) -> pl.DataFrame:
        if method == "python":
            df = pl.DataFrame(
                self.connect().execute(text(query.strip().removesuffix(";"))).fetchall(), infer_schema_length=None
            )

        elif method == "connectorx":
            uri = "redshift" + self.connection_string.removeprefix("questdb")
            df = pl.read_database_uri(query, uri)

        else:
            raise ValueError(f"Unknown method:'{method}'")

        if schema is not None:
            df = df.cast(schema)  # type: ignore[arg-type]

        return df

    def insert(
        self,
        df: pl.DataFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        parquet_fname = f"{table}_{uuid.uuid4().hex}.parquet"

        parquet_fpath = SETTINGS.temporary_directory / "questdb/data" / parquet_fname
        df.write_parquet(parquet_fpath)

        # QuestDB does not support primary keys
        # TODO: look into DEDUP KEYS
        # TODO: support not_null

        try:
            con = self.connect()

            tables = [n[0] for n in con.execute(text("show tables")).fetchall()]

            if table in tables:
                statement = f"""
                    insert into {table}
                    select * from read_parquet('{parquet_fname}')
                    """
            else:
                statement = f"""
                    create table {table} as (
                        select * from read_parquet('{parquet_fname}')
                    )
                    """

            con.execute(text(statement))
            con.commit()

        finally:
            parquet_fpath.unlink()

        _LOGGER.info(f"Inserted table {table}")

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        raise NotImplementedError
