import logging
import uuid
from collections.abc import Mapping
from time import sleep
from typing import Any, ClassVar, Literal, cast

import polars as pl
from questdb.ingress import Protocol, Sender
from sqlalchemy import Connection, create_engine, text

from ...settings import SETTINGS, DatabaseName, SuiteName, TableName
from ...suites import BenchmarkSuite
from ...suites.clickbench.config import (
    CLICKBENCH_DATE_COLUMNS,
    CLICKBENCH_TIMESTAMP_COLUMNS,
    Clickbench,
)
from .. import Database

_LOGGER = logging.getLogger(__name__)

VERSION = "9.4.3"

DOCKER_IMAGE = f"questdb/questdb:{VERSION}"


def _build_clickbench_insert(
    con: Connection,
    parquet_name: str,
    timestamp_columns: tuple[str, ...],
    date_columns: tuple[str, ...],
) -> str:
    rows = con.execute(text(f"select * from read_parquet('{parquet_name}') limit 0")).keys()
    parts: list[str] = []
    for name in rows:
        if name in timestamp_columns:
            parts.append(f"cast({name} * 1000000L as timestamp) as {name}")
        elif name in date_columns:
            parts.append(f"cast(cast({name} as long) * 86400000000L as timestamp) as {name}")
        else:
            parts.append(name)
    return f"insert into hits select {', '.join(parts)} from read_parquet('{parquet_name}')"


class QuestDBClickbench(Clickbench["QuestDB"]):
    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        # NOTE: inserts directly from source Parquet file to avoid OOM issues
        # insert time is not comparable with other databases that insert from an in memory dataframe
        # on the other hand, this is more in line with how inserts would actually be done with QuestDB

        self.db.initialize_schema("clickbench")

        fpath = SETTINGS.temporary_directory / "questdb/data/hits.parquet"
        input_fpath = SETTINGS.input_data_directory / "clickbench/hits.parquet"

        # Convert the epoch-integer columns inline in the INSERT instead of
        # rewriting the casts into the staged parquet.

        # Pre-sort the source parquet by EventTime on the host before handing it to
        # QuestDB. With a designated-timestamp partitioned table, sorted input lets
        # the WAL applier append into partitions instead of paying an O3 merge per
        # segment. Doing the sort inside QuestDB's INSERT … SELECT runs single-
        # threaded and stalls before any rows reach the WAL writer. EventTime is
        # the int64 epoch-seconds column, which has the same order as the cast
        # microsecond timestamp.
        fpath.parent.mkdir(parents=True, exist_ok=True)
        if fpath.exists() or fpath.is_symlink():
            fpath.unlink()

        with self.db.phase_context("sort_input", table_name="hits"):
            pl.scan_parquet(input_fpath).sort("EventTime").sink_parquet(fpath)

        # 99997497 rows
        count: int = pl.scan_parquet(fpath).select(pl.len()).collect().item(0, 0)
        _LOGGER.info(f"Sorted source hits.parquet ({count:_} rows) to {fpath}")

        with self.db.phase_context("insert", table_name="hits"):
            statement = _build_clickbench_insert(
                self.db.connect(), fpath.name, CLICKBENCH_TIMESTAMP_COLUMNS, CLICKBENCH_DATE_COLUMNS
            )
            self.db.execute(statement)
            _LOGGER.info(f"Inserted clickbench table for {self.name}")

            self.db.wait_until_count("hits", count)

        fpath.unlink()

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        if restart:
            self.db.restart_event()


class QuestDB(Database):
    name: DatabaseName = "questdb"
    version: str = VERSION
    container_image: ClassVar[str | None] = DOCKER_IMAGE
    supports_arm64_containers: ClassVar[bool] = True

    connection_string: str = "questdb://admin:quest@localhost:8812/qdb"

    # No row-level DELETE in QuestDB 9.4.3 (`DELETE FROM t WHERE id = 1` is
    # rejected as "unexpected token [FROM]"); only TRUNCATE TABLE and ALTER
    # TABLE DROP PARTITION are available, neither matches the row-targeted
    # mutate workload. No row-level upsert either -- the QuestDB-native path
    # is DEDUP UPSERT KEYS at CREATE TABLE, which would mean a schema shape
    # different from what every other DB here uses.
    #
    # Not emulating delete via CTAS-then-rename: that would measure table-
    # rewrite throughput rather than delete throughput.
    #
    # Insert stays enabled.
    DISABLED_MUTATION_STEPS: ClassVar[Mapping[SuiteName, frozenset[str]]] = {
        "time_series": frozenset(
            f"{action}_{table}_{count}"
            for action in ("upsert", "delete")
            for table in ("data_tall", "data_wide", "data_large")
            for count in (1, 100, 10_000)
        )
    }

    @property
    def start(self) -> str:
        (SETTINGS.temporary_directory / "questdb/data").mkdir(exist_ok=True, parents=True)

        return self.docker_run_command(
            DOCKER_IMAGE,
            ports={"9000": "9000", "8812": "8812"},
            mounts={
                self.database_directory.as_posix(): "/var/lib/questdb",
                f"{SETTINGS.temporary_directory.as_posix()}/questdb/data": "/import",
            },
            env={"QDB_CAIRO_SQL_COPY_ROOT": "/import"},
        )

    def connect(self, reconnect: bool = False) -> Connection:
        if reconnect:
            self.close_connection()

        if self._connection is not None:
            return self._connection

        engine = create_engine(self.connection_string, pool_reset_on_return=None)
        self._connection = self.bind_query_recorder(engine.connect())

        return self._connection

    def get_runtime_version(self) -> str:
        df = self.fetch("select version() as version", schema={"version": pl.String}, method="python")
        return str(df.item(0, 0))

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
        method: Literal["connectorx", "python"] = "connectorx",
    ) -> pl.DataFrame:
        if method == "python":
            with self.record_query_execution(query):
                result = self.connect().execute(text(query.strip().removesuffix(";")))
                columns = list(result.keys())
                rows = result.fetchall()
            df = pl.DataFrame(
                rows,
                schema=columns,
                orient="row",
                infer_schema_length=None,
            )

        elif method == "connectorx":
            uri = "redshift" + self.connection_string.removeprefix("questdb")
            with self.record_query_execution(query):
                df = pl.read_database_uri(query, uri)

        else:
            raise ValueError(f"Unknown method:'{method}'")

        if schema is not None:
            df = df.cast(cast(pl.Schema, schema))

        return df

    def get_table_names(self) -> set[TableName]:
        df = self.fetch(
            "select table_name from information_schema.tables "
            "where table_schema = 'public' and table_type = 'BASE TABLE'",
            schema={"table_name": pl.String},
            method="python",
        )
        return set(df.get_column("table_name").to_list())

    def get_count(self, table: TableName) -> int:
        ret = self.connect().execute(text(f"select count(*) from {table}")).fetchone()
        assert ret is not None
        c: int = ret[0]
        assert isinstance(c, int)

        return c

    def get_row_count(self, table: TableName) -> int:
        return self.get_count(table)

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
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
        batch_size: int | None = None,
        method: Literal["sender", "parquet"] = "parquet",
    ) -> None:
        cast_expr = (
            pl.selectors.decimal().cast(pl.Float64),
            pl.selectors.date().cast(pl.Datetime("us")),
            pl.selectors.datetime().cast(pl.Datetime("us")),
        )

        if isinstance(df, pl.LazyFrame):
            if method == "parquet":
                self._insert_frame_via_parquet(df.with_columns(*cast_expr), table)
                return

            _LOGGER.warning("QuestDB LazyFrame insert with sender method requires collecting to DataFrame")
            df = df.collect().with_columns(*cast_expr)
        else:
            df = df.with_columns(*cast_expr)

        if method == "sender":
            # much slower than read_parquet (serializes of http or similar)
            self.insert_sender(df, table, primary_key, not_null, batch_size)
        elif method == "parquet":
            self._insert_frame_via_parquet(df, table)
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

                cast(Any, sender).flush()

        self.wait_until_count(table, initial_count + len(df))

        _LOGGER.info(f"Finished inserting into '{table}' ({num_rows:_} rows total)")

    def _insert_frame_via_parquet(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
    ) -> None:
        parquet_fname = f"{table}_{uuid.uuid4().hex}.parquet"
        parquet_fpath = SETTINGS.temporary_directory / "questdb/data" / parquet_fname

        if isinstance(df, pl.LazyFrame):
            df.sink_parquet(parquet_fpath)
            row_count: int = pl.scan_parquet(parquet_fpath).select(pl.len()).collect().item(0, 0)
        else:
            df.write_parquet(parquet_fpath)
            row_count = len(df)

        # TODO: issue with parquet file not being accessible by questdb even if it is completely written
        # intermittent issue, sleeping for 100 ms seems to fix it
        sleep(0.1)

        try:
            con = self.connect()

            with self.record_query_execution("show tables"):
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

            self.execute(statement)
            self.wait_until_count(table, initial_count + row_count)

        finally:
            parquet_fpath.unlink()

        _LOGGER.info(f"Inserted table {table} ({row_count:_} rows)")

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        # QuestDB has no DELETE statement, so there's no fast in-place upsert
        # path. Production users either rely on DEDUP keys (declared at
        # CREATE TABLE) or accept duplicates. Skipped via DISABLED_MUTATION_STEPS.
        raise NotImplementedError("QuestDB does not support row-level upsert")

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        # QuestDB has no DELETE statement (only TRUNCATE TABLE / ALTER TABLE
        # DROP PARTITION). Skipped via DISABLED_MUTATION_STEPS.
        raise NotImplementedError("QuestDB does not support row-level delete")

    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        registry: dict[SuiteName, type[BenchmarkSuite[Any]]] = {
            **super().suite_registry(),
            "clickbench": QuestDBClickbench,
        }

        # TPC-H and TPC-DS need correlated subqueries and EXISTS/NOT EXISTS
        # (TPC-DS additionally ROLLUP/GROUPING and INTERSECT/EXCEPT), which
        # QuestDB SQL does not support
        for tpc_suite in ("tpc_h", "tpc_ds"):
            registry.pop(tpc_suite, None)

        return registry
