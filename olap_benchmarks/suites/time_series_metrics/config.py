from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import ClassVar

import numpy as np
import polars as pl
import pyarrow.parquet as pq

from ...dbs import Database
from ...settings import REPO_ROOT, Operation, SuiteName, TableName, get_settings
from .. import BenchmarkSuite

METRIC_COUNTS = (100, 1_000, 2_000, 5_000)
TIMESTAMP_COUNT = 14 * 24 * 60
GENERATION_BATCH_ROWS = 480
LAYOUTS = ("wide", "eav")
QUERIES_DIRECTORY = REPO_ROOT / "olap_benchmarks/suites/time_series_metrics/queries"
QUERY_NAMES = tuple(path.stem for path in sorted(QUERIES_DIRECTORY.glob("*.sql")))


def metric_name(index: int) -> str:
    return f"metric_{index:04d}"


def table_name(layout: str, metrics: int) -> str:
    return f"{layout}{metrics}"


def input_files() -> dict[TableName, Path]:
    directory = get_settings().input_data_directory / "time_series_metrics"
    return {
        table_name(layout, count): directory / f"{table_name(layout, count)}.parquet"
        for layout in LAYOUTS
        for count in METRIC_COUNTS
    }


def generate_batch(
    start: int,
    stop: int,
    *,
    rows: int = TIMESTAMP_COUNT,
    metrics: int = max(METRIC_COUNTS),
    interval_minutes: int = 1,
    end: datetime = datetime(2025, 1, 1),
) -> pl.DataFrame:
    positions = np.arange(start, stop)
    beginning = end - timedelta(minutes=(rows - 1) * interval_minutes)
    data: dict[str, pl.Series] = {
        "time": pl.datetime_range(
            beginning + timedelta(minutes=start * interval_minutes),
            beginning + timedelta(minutes=(stop - 1) * interval_minutes),
            interval=f"{interval_minutes}m",
            time_unit="ms",
            eager=True,
        ).alias("time")
    }
    for index in range(1, metrics + 1):
        values = (index % 100 + 10 * np.sin(positions / 120 + index) + ((positions * (index + 1)) % 97) / 97).astype(
            np.float32
        )
        values[(positions + index * 11) % 997 < 31] = np.nan
        data[metric_name(index)] = pl.Series(metric_name(index), values, nan_to_null=True)
    return pl.DataFrame(data)


def to_eav(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.unpivot(index="time", variable_name="metric_name", value_name="value").sort("time", "metric_name")


def write_datasets(
    directory: Path,
    *,
    rows: int = TIMESTAMP_COUNT,
    counts: tuple[int, ...] = METRIC_COUNTS,
    interval_minutes: int = 1,
    end: datetime = datetime(2025, 1, 1),
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    writers: dict[str, pq.ParquetWriter] = {}
    partials: dict[str, Path] = {}
    try:
        for start in range(0, rows, GENERATION_BATCH_ROWS):
            batch = generate_batch(
                start,
                min(start + GENERATION_BATCH_ROWS, rows),
                rows=rows,
                metrics=max(counts),
                interval_minutes=interval_minutes,
                end=end,
            )
            for count in counts:
                wide = batch.select("time", *[metric_name(index) for index in range(1, count + 1)])
                for layout in LAYOUTS:
                    name = table_name(layout, count)
                    frame = wide if layout == "wide" else to_eav(wide)
                    arrow = frame.to_arrow()
                    if name not in writers:
                        partials[name] = directory / f".{name}.parquet.part"
                        writers[name] = pq.ParquetWriter(partials[name], arrow.schema, compression="zstd")
                    writers[name].write_table(arrow, row_group_size=GENERATION_BATCH_ROWS * count)
        for writer in writers.values():
            writer.close()
        writers.clear()
        for name, partial in partials.items():
            partial.replace(directory / f"{name}.parquet")
    finally:
        for writer in writers.values():
            writer.close()
        for partial in partials.values():
            partial.unlink(missing_ok=True)


def prepare_data() -> None:
    files = input_files()
    missing_counts = tuple(
        count for count in METRIC_COUNTS if any(not files[table_name(layout, count)].is_file() for layout in LAYOUTS)
    )
    if not missing_counts:
        return
    write_datasets(next(iter(files.values())).parent, counts=missing_counts)


class TimeSeriesMetrics[DBT: Database](BenchmarkSuite[DBT]):
    name: SuiteName = "time_series_metrics"
    supported_operations: ClassVar[tuple[Operation, ...]] = ("populate", "select")

    def expected_table_row_counts(self) -> Mapping[TableName, int]:
        return {
            table_name(layout, count): TIMESTAMP_COUNT * (count if layout == "eav" else 1)
            for layout in LAYOUTS
            for count in METRIC_COUNTS
        }

    def planned_query_names(self) -> tuple[str, ...]:
        return QUERY_NAMES

    def dataset_files(self) -> Mapping[TableName, Path]:
        return input_files()

    def load_dataset(self) -> pl.LazyFrame:
        raise NotImplementedError("Time Series Metrics loads each table separately")

    def populate_table(self, table: str, path: Path) -> None:
        self.db.insert_parquet(path, table, not_null=["time", "metric_name"] if table.startswith("eav") else "time")

    def finish_populate(self) -> None:
        pass

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return
        for table, path in self.dataset_files().items():
            with self.db.phase_context("insert", table_name=table):
                self.populate_table(table, path)
        self.finish_populate()
        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()
        if restart:
            self.db.restart_event()
