from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

from ...dbs import Database
from ...settings import REPO_ROOT, SuiteName, TableName, get_settings
from ..time_series_metrics.config import (
    LAYOUTS,
    TimeSeriesMetrics,
    metric_name,
    table_name,
    write_datasets,
)

HISTORY_DAYS = 100
METRIC_COUNTS = (100, 1_000, 2_000, 4_094)
INTERVAL_MINUTES = 10
TIMESTAMP_COUNT = HISTORY_DAYS * 24 * 60 // INTERVAL_MINUTES
END = datetime(2025, 1, 1)
QUERIES_DIRECTORY = REPO_ROOT / "olap_benchmarks/suites/time_series_ranges/queries"


@dataclass(frozen=True)
class RangeQuery:
    days: int
    selected_metrics: int
    operation: Literal["raw", "avg", "calc"]

    @property
    def suffix(self) -> str:
        return f"{self.operation}_{self.selected_metrics}_metrics_{self.days}d"

    @property
    def start(self) -> datetime:
        return END - timedelta(days=self.days)

    def columns(self, metrics: int) -> tuple[str, ...]:
        return tuple(
            metric_name(1 + index * (metrics - 1) // (self.selected_metrics - 1))
            for index in range(self.selected_metrics)
        )


RANGE_QUERIES = tuple(
    RangeQuery(days, selected, operation)
    for days in (1, 10, 100)
    for selected in (5, 10)
    for operation in ("raw", "avg")
) + tuple(RangeQuery(days, 10, "calc") for days in (1, 10, 100))
QUERY_NAMES = tuple(
    f"{table_name(layout, count)}_{number:02d}_{query.suffix}"
    for layout in LAYOUTS
    for count in METRIC_COUNTS
    for number, query in enumerate(RANGE_QUERIES, 1)
)


def input_files() -> dict[TableName, Path]:
    directory = get_settings().input_data_directory / "time_series_ranges"
    return {
        table_name(layout, count): directory / f"{table_name(layout, count)}.parquet"
        for layout in LAYOUTS
        for count in METRIC_COUNTS
    }


def prepare_data() -> None:
    files = input_files()
    missing_counts = tuple(
        count for count in METRIC_COUNTS if any(not files[table_name(layout, count)].is_file() for layout in LAYOUTS)
    )
    if missing_counts:
        write_datasets(
            next(iter(files.values())).parent,
            rows=TIMESTAMP_COUNT,
            counts=missing_counts,
            interval_minutes=INTERVAL_MINUTES,
            end=END - timedelta(minutes=INTERVAL_MINUTES),
        )


class TimeSeriesRanges[DBT: Database](TimeSeriesMetrics[DBT]):
    name: SuiteName = "time_series_ranges"

    def expected_table_row_counts(self) -> Mapping[TableName, int]:
        return {
            table_name(layout, count): TIMESTAMP_COUNT * (count if layout == "eav" else 1)
            for layout in LAYOUTS
            for count in METRIC_COUNTS
            if self.table_selected(table_name(layout, count))
        }

    def planned_query_names(self) -> tuple[str, ...]:
        return QUERY_NAMES

    def dataset_files(self) -> Mapping[TableName, Path]:
        return {table: path for table, path in input_files().items() if self.table_selected(table)}
