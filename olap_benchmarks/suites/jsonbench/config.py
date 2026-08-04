import json
import logging
from collections.abc import Iterator
from gzip import open as gzip_open
from pathlib import Path
from time import perf_counter

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
from ..download import download_file

_LOGGER = logging.getLogger(__name__)

JSONBENCH_QUERIES_DIRECTORY = REPO_ROOT / "olap_benchmarks/suites/jsonbench/queries"
JSONBENCH_DATASET_BASE_URL = "https://clickhouse-public-datasets.s3.amazonaws.com/bluesky"
JSONBENCH_FILE_COUNTS = {10: 10}
JSONBENCH_EMPTY_OBJECT_LINE = "{}\n"

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


def _looks_like_json_object_line(line: str) -> bool:
    stripped = line.rstrip("\r\n")
    return stripped.startswith("{") and stripped.endswith("}")


def _ensure_line_ending(line: str) -> str:
    if line.endswith("\n"):
        return line
    return f"{line}\n"


def _validate_json_object_line(line: str, input_file: Path, line_number: int) -> str:
    try:
        value = json.loads(line)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSONBench row in {input_file.name} near line {line_number:_}: {exc}") from exc

    if not isinstance(value, dict):
        # a non-object row is malformed input, the same failure class as the decode error
        # above, not a caller passing the wrong type
        message = f"Invalid JSONBench row in {input_file.name} near line {line_number:_}: expected object"
        raise RuntimeError(message)  # noqa: TRY004

    return _ensure_line_ending(line)


def iter_jsonbench_input_lines(input_file: Path) -> Iterator[str]:
    pending_line: str | None = None
    pending_line_number: int | None = None

    with gzip_open(input_file, "rt", encoding="utf-8") as source:
        for line_number, raw_line in enumerate(source, 1):
            line = raw_line.replace("\\u0000", "")
            if pending_line is not None:
                repaired_line = f"{pending_line}\\n{line}"
                yield _validate_json_object_line(repaired_line, input_file, pending_line_number or line_number)
                yield JSONBENCH_EMPTY_OBJECT_LINE
                pending_line = None
                pending_line_number = None
                continue

            if _looks_like_json_object_line(line):
                yield _ensure_line_ending(line)
                continue

            pending_line = line.rstrip("\r\n")
            pending_line_number = line_number

    if pending_line_number is not None:
        raise RuntimeError(
            f"Invalid JSONBench row in {input_file.name} at line {pending_line_number:_}: unrepaired fragment"
        )


def write_jsonbench_input_file(input_file: Path, output_file: Path) -> None:
    with output_file.open("w", encoding="utf-8") as out:
        out.writelines(iter_jsonbench_input_lines(input_file))


def prepare_data(scale_factor: int) -> None:
    scale_factor = resolve_suite_scale_factor("jsonbench", scale_factor)
    input_directory = get_jsonbench_input_directory(scale_factor)
    input_directory.mkdir(parents=True, exist_ok=True)

    for fpath in get_jsonbench_input_files(scale_factor):
        url = f"{JSONBENCH_DATASET_BASE_URL}/{fpath.name}"
        download_file(url, fpath)


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
                fetch_kwargs_factory=lambda: self.fetch_kwargs,
                progress_label=progress_label,
                log_success=log_success,
            )
            if not ok:
                failed_queries += 1

        self.assert_no_failed_queries(failed_queries, len(JSONBENCH_QUERY_NAMES))

        _LOGGER.info(
            f"Executed {len(JSONBENCH_QUERY_NAMES):_} queries (with repetitions) in {perf_counter() - t0:_.2f} seconds"
        )
