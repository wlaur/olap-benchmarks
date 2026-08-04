# Based on ClickBench by ClickHouse
# https://github.com/ClickHouse/ClickBench

import logging
from time import perf_counter
from typing import Any

import polars as pl

from ...dbs import Database
from ...settings import REPO_ROOT, SETTINGS, SuiteName, TableName
from .. import BenchmarkSuite
from ..download import download_file

_LOGGER = logging.getLogger(__name__)

ITERATIONS = 5
CLICKBENCH_QUERY_COUNT = 43
CLICKBENCH_DATASET_URL = "https://datasets.clickhouse.com/hits_compatible/hits.parquet"


def prepare_data() -> None:
    destination = SETTINGS.input_data_directory / "clickbench" / "hits.parquet"
    download_file(CLICKBENCH_DATASET_URL, destination)


# The source parquet stores these as epoch integers; every engine's populate
# path must cast them to real timestamp/date columns.
CLICKBENCH_TIMESTAMP_COLUMNS = ("EventTime", "ClientEventTime", "LocalEventTime")
CLICKBENCH_DATE_COLUMNS = ("EventDate",)


class Clickbench[DBT: Database](BenchmarkSuite[DBT]):
    name: SuiteName = "clickbench"

    def expected_table_row_counts(self) -> dict[TableName, int]:
        return {"hits": self.parquet_row_count(SETTINGS.input_data_directory / "clickbench/hits.parquet")}

    def load_dataset(self) -> pl.LazyFrame:
        # parquet file stores these as integers, the schema expects correct dtypes
        return (
            pl.scan_parquet(SETTINGS.input_data_directory / "clickbench/hits.parquet")
            .with_columns(pl.from_epoch(n, "s").cast(pl.Datetime("ms")).alias(n) for n in CLICKBENCH_TIMESTAMP_COLUMNS)
            .with_columns(pl.col(n).cast(pl.Date).alias(n) for n in CLICKBENCH_DATE_COLUMNS)
        )

    @property
    def populate_kwargs(self) -> dict[str, Any]:
        return {}

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        self.db.initialize_schema("clickbench")

        with self.db.phase_context("insert", table_name="hits"):
            if self.populate_kwargs:
                self.db.insert(self.load_dataset(), "hits", **self.populate_kwargs)
            else:
                self.db.insert_parquet(
                    SETTINGS.input_data_directory / "clickbench/hits.parquet",
                    "hits",
                    epoch_columns={
                        **dict.fromkeys(CLICKBENCH_TIMESTAMP_COLUMNS, "s"),
                        **dict.fromkeys(CLICKBENCH_DATE_COLUMNS, "day"),
                    },
                )

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

    def load_queries(self) -> list[str]:
        with (REPO_ROOT / f"olap_benchmarks/suites/clickbench/queries/{self.db.name}.sql").open() as f:
            queries = f.readlines()

        if len(queries) != CLICKBENCH_QUERY_COUNT:
            raise RuntimeError(f"Expected {CLICKBENCH_QUERY_COUNT} ClickBench queries for {self.db.name}")
        return queries

    def select(self) -> None:
        t0 = perf_counter()
        queries = self.load_queries()

        failed_queries = 0
        for idx, query in enumerate(queries):
            query_name = f"Q{idx}"
            progress_label = f"({idx + 1:_}/{len(queries):_})"

            if not self.include_query(query_name):
                self.record_skipped_query_steps(
                    query_name,
                    ITERATIONS,
                    result_status="skipped",
                    reason="query excluded by suite/database",
                )
                continue

            ok = self.execute_query_with_isolation(
                query_name=query_name,
                iterations=ITERATIONS,
                query_loader=lambda query=query: query,
                fetch_kwargs_factory=lambda: self.fetch_kwargs,
                progress_label=progress_label,
                log_success=lambda it, df, t, query_name=query_name, progress_label=progress_label: _LOGGER.info(
                    f"Executed {query_name} {progress_label} "
                    f"iteration {it:_}/{ITERATIONS:_} "
                    f"in {1_000 * (t):_.2f} ms\ndf={df}"
                ),
            )
            if not ok:
                failed_queries += 1

        self.assert_no_failed_queries(failed_queries, len(queries))

        _LOGGER.info(f"Executed {len(queries):_} queries (with repetitions) in {perf_counter() - t0:_.2f} seconds")
