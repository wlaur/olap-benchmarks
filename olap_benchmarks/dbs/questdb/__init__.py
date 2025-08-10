import logging
import uuid
from collections.abc import Mapping
from time import sleep
from typing import Literal

import polars as pl
from questdb.ingress import Protocol, Sender  # type: ignore[import-untyped]
from sqlalchemy import Connection, create_engine, text

from ...settings import SETTINGS, TableName
from ...suites.clickbench.config import Clickbench
from .. import Database

_LOGGER = logging.getLogger(__name__)

VERSION = "9.0.1"

DOCKER_IMAGE = f"questdb/questdb:{VERSION}-rhel"


class QuestDBClickbench(Clickbench):
    db: "QuestDB"

    def populate(self, restart: bool = True) -> None:
        # NOTE: inserts directly from source Parquet file to avoid OOM issues
        # insert time is not comparable with other databases that insert from an in memory dataframe
        # on the other hand, this is more in line with how inserts would actually be done with QuestDB

        self.db.initialize_schema("clickbench")

        fpath = SETTINGS.temporary_directory / "questdb/data/hits.parquet"
        input_fpath = SETTINGS.input_data_directory / "clickbench/hits.parquet"

        timestamp_columns = ["EventTime", "ClientEventTime", "LocalEventTime"]
        date_columns = ["EventDate"]

        (
            pl.scan_parquet(input_fpath)
            .with_columns(pl.from_epoch(n, "s").cast(pl.Datetime("ms")).alias(n) for n in timestamp_columns)
            .with_columns(pl.col(n).cast(pl.Date).alias(n) for n in date_columns)
            .with_columns(pl.selectors.decimal().cast(pl.Float64))
            .with_columns(pl.selectors.date().cast(pl.Datetime("us")))
            .with_columns(pl.selectors.datetime().cast(pl.Datetime("us")))
            .sink_parquet(fpath)
        )

        # 99997497 rows
        count: int = pl.scan_parquet(input_fpath).select(pl.len()).collect().item(0, 0)

        _LOGGER.info(f"Wrote temporary hits.parquet with {count:_} rows to {fpath}")

        with self.db.event_context("insert_hits"):
            con = self.db.connect()

            con.execute(
                text(f"""
                    insert into hits
                    select * from read_parquet('{fpath.name}')
                """)
            )
            con.commit()
            _LOGGER.info(f"Inserted clickbench table for {self.name}")

            self.db.wait_until_count("hits", count)

        fpath.unlink()

        if restart:
            self.db.restart_event()


class QuestDB(Database):
    name: Literal["questdb"] = "questdb"
    version: str = VERSION

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

        engine = create_engine(self.connection_string, pool_reset_on_return=None)
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

    def get_count(self, table: TableName) -> int:
        ret = self.connect().execute(text(f"select count(*) from {table}")).fetchone()
        assert ret is not None
        c: int = ret[0]
        assert isinstance(c, int)

        return c

    def wait_until_count(self, table: TableName, count: int, interval_seconds: float = 10.0) -> None:
        _LOGGER.info(f"Waiting until table '{table}' contains {count:_} rows...")

        while True:
            c = self.get_count(table)

            if c == count:
                _LOGGER.info(f"Table {table} contains all {count:_} rows")
                return

            _LOGGER.info(f"Table {table} contains {c:_}/{count:_} rows, waiting...")

            sleep(interval_seconds)

    def insert(
        self,
        df: pl.DataFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
        batch_size: int | None = None,
        method: Literal["sender", "parquet"] = "parquet",
    ) -> None:
        df = (
            df.with_columns(pl.selectors.decimal().cast(pl.Float64))
            .with_columns(pl.selectors.date().cast(pl.Datetime("us")))
            .with_columns(pl.selectors.datetime().cast(pl.Datetime("us")))
        )

        if method == "sender":
            # much slower than read_parquet (serializes of http or similar)
            self.insert_sender(df, table, primary_key, not_null, batch_size)
        elif method == "parquet":
            self.insert_parquet(df, table, primary_key, not_null)
        else:
            raise ValueError(f"Unknown method: '{method}'")

    def insert_sender(
        self,
        df: pl.DataFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
        batch_size: int | None = None,
    ) -> None:
        # QuestDB does not support primary keys
        # TODO: look into DEDUP KEYS
        # TODO: support not_null

        if batch_size is None:
            batch_size = 1_000_000 // len(df.columns)

        df_pd = df.with_columns(pl.selectors.datetime().cast(pl.Datetime("ns"))).to_pandas(
            use_pyarrow_extension_array=False
        )
        num_rows = len(df_pd)
        num_batches = (num_rows + batch_size - 1) // batch_size
        _LOGGER.info(
            f"Inserting {num_rows:_} rows into '{table}' in {num_batches:_} batches (batch size: {batch_size:_})"
        )

        try:
            initial_count = self.get_count(table)
        except Exception:
            initial_count = 0

        with Sender(Protocol.Http, "localhost", 9000) as sender:
            for i, start in enumerate(range(0, num_rows, batch_size)):
                end = min(start + batch_size, num_rows)
                batch_df = df_pd.iloc[start:end]

                sender.dataframe(batch_df, table_name=table, at="time")

                if i % 10 == 0 or i == num_batches - 1:
                    _LOGGER.info(f"Batch {i + 1:_}/{num_batches:_}: inserted rows {start:_} to {end - 1:_}")

                sender.flush()

        self.wait_until_count(table, initial_count + len(df))

        _LOGGER.info(f"Finished inserting into '{table}' ({num_rows:_} rows total)")

    def insert_parquet(
        self,
        df: pl.DataFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        parquet_fname = f"{table}_{uuid.uuid4().hex}.parquet"

        parquet_fpath = SETTINGS.temporary_directory / "questdb/data" / parquet_fname
        df.write_parquet(parquet_fpath)

        # TODO: issue with parquet file not being accessible by questdb even if it is completely written
        # intermittent issue, sleeping for 100 ms seems to fix it
        sleep(0.1)

        try:
            con = self.connect()

            tables = [n[0] for n in con.execute(text("show tables")).fetchall()]

            if table in tables:
                initial_count = self.get_count(table)
                statement = f"""
                    insert into {table}
                    select * from read_parquet('{parquet_fname}')
                    """
            else:
                initial_count = 0
                statement = f"""
                    create table {table} as (
                        select * from read_parquet('{parquet_fname}')
                    )
                    """

            con.execute(text(statement))
            con.commit()
            self.wait_until_count(table, initial_count + len(df))

        finally:
            parquet_fpath.unlink()

        _LOGGER.info(f"Inserted table {table}")

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        raise NotImplementedError

    @property
    def clickbench(self) -> QuestDBClickbench:
        return QuestDBClickbench(db=self)
