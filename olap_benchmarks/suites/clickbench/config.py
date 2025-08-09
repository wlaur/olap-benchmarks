import logging
from time import perf_counter
from typing import Any, Literal

import polars as pl

from ...settings import REPO_ROOT, SETTINGS
from .. import BenchmarkSuite

_LOGGER = logging.getLogger(__name__)

ITERATIONS = 3


def download_clickbench() -> None:
    (SETTINGS.input_data_directory / "clickbench").mkdir(exist_ok=True, parents=True)

    # TODO: download https://datasets.clickhouse.com/hits_compatible/hits.parquet and move to data/input/clickbench
    raise NotImplementedError


class Clickbench(BenchmarkSuite):
    name: Literal["clickbench"] = "clickbench"

    def load_dataset(self) -> pl.DataFrame:
        # parquet file stores these as integers, the schema expects correct dtypes
        timestamp_columns = ["EventTime", "ClientEventTime", "LocalEventTime"]
        date_columns = ["EventDate"]

        return (
            pl.scan_parquet(SETTINGS.input_data_directory / "clickbench/hits.parquet")
            .with_columns(pl.from_epoch(n, "s").cast(pl.Datetime("ms")).alias(n) for n in timestamp_columns)
            .with_columns(pl.col(n).cast(pl.Date).alias(n) for n in date_columns)
        ).collect()

    @property
    def populate_kwargs(self) -> dict[str, Any]:
        return {}

    def populate(self, restart: bool = True) -> None:
        self.db.initialize_schema("clickbench")

        # this is an expensive operation, would be better to avoid reading with polars
        # for the databases that can ingest directly from parquet
        # on the other hand, the purpose of these benchmarks is to measure in-memory polars df
        # to and from the database, so this is appropriate,
        # although not directly comparable with the insert times from the official clickbench results
        df = self.load_dataset()
        _LOGGER.info(f"Loaded clickbench dataset with shape ({df.shape[0]:_}, {df.shape[1]:_})")

        with self.db.event_context("insert_hits"):
            self.db.insert(df, "hits", **self.populate_kwargs)

        _LOGGER.info(f"Inserted clickbench table for {self.name}")

        # restart db to ensure data is not kept in-memory by the db, and also
        # ensure that WAL is processed etc...
        if restart:
            self.db.restart_event()

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {}

    def include_query(self, query_name: str) -> bool:
        return True

    def run(self) -> None:
        t0 = perf_counter()

        # NOTE: clickbench query files should not be formatted, need to have one query per line
        with (REPO_ROOT / f"olap_benchmarks/suites/clickbench/queries/{self.db.name}.sql").open() as f:
            queries = f.readlines()

        for idx, query in enumerate(queries):
            query_name = f"Q{idx}"

            if not self.include_query(query_name):
                continue

            with self.db.query_context("clickbench", query_name):
                for it in range(1, ITERATIONS + 1):
                    with self.db.event_context(f"query_{query_name}_iteration_{it}"):
                        t1 = perf_counter()
                        df = self.db.fetch(query, **self.fetch_kwargs)
                        t = perf_counter() - t1

                    _LOGGER.info(
                        f"Executed {query_name} ({idx + 1:_}/{len(queries):_}) "
                        f"iteration {it:_}/{ITERATIONS:_} "
                        f"in {1_000 * (t):_.2f} ms\ndf={df}"
                    )

        _LOGGER.info(f"Executed {len(queries):_} queries (with repetitions) in {perf_counter() - t0:_.2f} seconds")
