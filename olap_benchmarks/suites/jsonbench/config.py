import logging
from pathlib import Path
from time import perf_counter
from urllib.request import urlretrieve

import polars as pl

from ...dbs import Database
from ...settings import (
    REPO_ROOT,
    SETTINGS,
    SuiteName,
    TableName,
    format_suite_data_directory_name,
    resolve_suite_scale_factor,
)
from .. import BenchmarkSuite

_LOGGER = logging.getLogger(__name__)

JSONBENCH_QUERIES_DIRECTORY = REPO_ROOT / "olap_benchmarks/suites/jsonbench/queries"
JSONBENCH_DATASET_BASE_URL = "https://clickhouse-public-datasets.s3.amazonaws.com/bluesky"
JSONBENCH_FILE_COUNTS = {10: 10}

JSONBENCH_QUERY_NAMES = {
    "01_events_by_collection": 5,
    "02_create_events_by_collection": 5,
    "03_create_events_by_hour": 5,
    "04_first_post_users": 5,
    "05_longest_post_activity": 5,
}


def get_jsonbench_input_directory(scale_factor: int) -> Path:
    return SETTINGS.input_data_directory / format_suite_data_directory_name("jsonbench", scale_factor)


def get_jsonbench_input_files(scale_factor: int) -> list[Path]:
    file_count = JSONBENCH_FILE_COUNTS[scale_factor]
    input_directory = get_jsonbench_input_directory(scale_factor)
    return [input_directory / f"file_{idx:04d}.json.gz" for idx in range(1, file_count + 1)]


def prepare_data(scale_factor: int) -> None:
    scale_factor = resolve_suite_scale_factor("jsonbench", scale_factor)
    input_directory = get_jsonbench_input_directory(scale_factor)
    input_directory.mkdir(parents=True, exist_ok=True)

    for fpath in get_jsonbench_input_files(scale_factor):
        if fpath.is_file():
            _LOGGER.info(f"Reusing JSONBench file {fpath.name}")
            continue

        url = f"{JSONBENCH_DATASET_BASE_URL}/{fpath.name}"
        _LOGGER.info(f"Downloading {url} to {fpath}")
        urlretrieve(url, fpath)


class JSONBench[DBT: Database](BenchmarkSuite[DBT]):
    name: SuiteName = "jsonbench"

    def expected_table_row_counts(self) -> dict[TableName, int]:
        return {"bluesky": self.scale_factor * 1_000_000}

    def populate(self) -> None:
        raise NotImplementedError(f"{type(self).__name__} must implement JSONBench populate")

    def load_jsonbench_query(self, query_name: str) -> str:
        db_specific = JSONBENCH_QUERIES_DIRECTORY / f"{self.db.name}/{query_name}.sql"
        common = JSONBENCH_QUERIES_DIRECTORY / f"{query_name}.sql"
        sql_source = db_specific if db_specific.is_file() else common
        return sql_source.read_text()

    @property
    def fetch_kwargs(self) -> dict[str, object]:
        return {}

    def include_query(self, query_name: str) -> bool:
        db_specific = JSONBENCH_QUERIES_DIRECTORY / f"{self.db.name}/{query_name}.sql"
        common = JSONBENCH_QUERIES_DIRECTORY / f"{query_name}.sql"
        return db_specific.is_file() or common.is_file()

    def select(self) -> None:
        t0 = perf_counter()
        failed_queries = 0
        for idx, (query_name, iterations) in enumerate(JSONBENCH_QUERY_NAMES.items()):
            progress_label = f"({idx + 1:_}/{len(JSONBENCH_QUERY_NAMES):_})"
            if not self.include_query(query_name):
                self.record_skipped_query_steps(
                    query_name,
                    iterations,
                    result_status="unsupported",
                    reason="query file is not defined for suite/database",
                )
                continue

            def log_success(
                it: int,
                df: pl.DataFrame,
                t: float,
                *,
                query_name: str = query_name,
                progress_label: str = progress_label,
                iterations: int = iterations,
            ) -> None:
                _LOGGER.info(
                    f"Executed {query_name} {progress_label} "
                    f"iteration {it:_}/{iterations:_} "
                    f"in {1_000 * (t):_.2f} ms, shape=({df.shape[0]:_}, {df.shape[1]:_})"
                )

            ok = self.execute_query_with_isolation(
                query_name=query_name,
                iterations=iterations,
                query_loader=lambda query_name=query_name: self.load_jsonbench_query(query_name),
                fetch_kwargs=self.fetch_kwargs,
                progress_label=progress_label,
                log_success=log_success,
            )
            if not ok:
                failed_queries += 1

        if failed_queries:
            _LOGGER.warning(
                f"JSONBench select completed on {self.db.name} with {failed_queries:_} failed "
                f"{'queries' if failed_queries != 1 else 'query'}"
            )

        _LOGGER.info(
            f"Executed {len(JSONBENCH_QUERY_NAMES):_} queries (with repetitions) in {perf_counter() - t0:_.2f} seconds"
        )
