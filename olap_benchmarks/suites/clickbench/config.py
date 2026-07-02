# Based on ClickBench by ClickHouse
# https://github.com/ClickHouse/ClickBench

import logging
from time import perf_counter
from typing import Any

import polars as pl

from ...dbs import Database
from ...settings import REPO_ROOT, SETTINGS, SuiteName, TableName
from .. import BenchmarkSuite

_LOGGER = logging.getLogger(__name__)

ITERATIONS = 5


def prepare_data() -> None:
    (SETTINGS.input_data_directory / "clickbench").mkdir(exist_ok=True, parents=True)

    # TODO: download https://datasets.clickhouse.com/hits_compatible/hits.parquet and move to data/input/clickbench
    raise NotImplementedError


class Clickbench[DBT: Database](BenchmarkSuite[DBT]):
    name: SuiteName = "clickbench"

    def expected_table_row_counts(self) -> dict[TableName, int]:
        return {"hits": self.parquet_row_count(SETTINGS.input_data_directory / "clickbench/hits.parquet")}

    def load_dataset(self) -> pl.LazyFrame:
        # parquet file stores these as integers, the schema expects correct dtypes
        timestamp_columns = ["EventTime", "ClientEventTime", "LocalEventTime"]
        date_columns = ["EventDate"]

        return (
            pl.scan_parquet(SETTINGS.input_data_directory / "clickbench/hits.parquet")
            .with_columns(pl.from_epoch(n, "s").cast(pl.Datetime("ms")).alias(n) for n in timestamp_columns)
            .with_columns(pl.col(n).cast(pl.Date).alias(n) for n in date_columns)
        )

    @property
    def populate_kwargs(self) -> dict[str, Any]:
        return {}

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        self.db.initialize_schema("clickbench")

        # this is an expensive operation, would be better to avoid reading with polars
        # for the databases that can ingest directly from parquet
        # on the other hand, the purpose of these benchmarks is to measure in-memory polars df
        # to and from the database, so this is appropriate,
        # although not directly comparable with the insert times from the official clickbench results
        df = self.load_dataset()
        _LOGGER.info("Loaded clickbench dataset (lazy)")

        with self.db.phase_context("insert", table_name="hits"):
            self.db.insert(df, "hits", **self.populate_kwargs)

        _LOGGER.info(f"Inserted clickbench table for {self.name}")

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        # restart db to ensure data is not kept in-memory by the db, and also
        # ensure that WAL is processed etc...
        if restart:
            self.db.restart_event()

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {}

    def include_query(self, query_name: str) -> bool:
        return True

    def select(self) -> None:
        t0 = perf_counter()

        # NOTE: clickbench query files should not be formatted, need to have one query per line
        with (REPO_ROOT / f"olap_benchmarks/suites/clickbench/queries/{self.db.name}.sql").open() as f:
            queries = f.readlines()

        for idx, query in enumerate(queries):
            query_name = f"Q{idx}"

            if not self.include_query(query_name):
                continue

            with self.db.query_context(query_name):
                for it in range(1, ITERATIONS + 1):
                    df, t = self.db.execute_query_iteration(
                        query_name=query_name,
                        iteration=it,
                        query=query,
                        fetch_kwargs=self.fetch_kwargs,
                    )

                    _LOGGER.info(
                        f"Executed {query_name} ({idx + 1:_}/{len(queries):_}) "
                        f"iteration {it:_}/{ITERATIONS:_} "
                        f"in {1_000 * (t):_.2f} ms\ndf={df}"
                    )

        _LOGGER.info(f"Executed {len(queries):_} queries (with repetitions) in {perf_counter() - t0:_.2f} seconds")
