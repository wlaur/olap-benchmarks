import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any, Literal, get_args

import numpy as np
import polars as pl

from ...dbs import Database
from ...settings import REPO_ROOT, SETTINGS, SuiteName, TableName
from .. import BenchmarkSuite

_LOGGER = logging.getLogger(__name__)

TIME_SERIES_QUERIES_DIRECTORY = REPO_ROOT / "olap_benchmarks/suites/time_series/queries"


TIME_SERIES_QUERY_NAMES = {
    n.stem: 5 for n in sorted((REPO_ROOT / "olap_benchmarks/suites/time_series/queries").glob("*.sql"))
}


DatasetSize = Literal[
    "tall",
    "wide",
    "large",
]


TIME_SERIES_DATASET_SIZES: dict[DatasetSize, tuple[int, int]] = {
    "tall": (2_000_000, 10),
    "wide": (200_000, 1_500),
    "large": (4_000_000, 1_500),
}

TIME_SERIES_EAV_TABLE_NAME: TableName = "data_wide_eav"


assert set(TIME_SERIES_DATASET_SIZES) == set(get_args(DatasetSize))


@dataclass(frozen=True)
class TimeSeriesColumnCounts:
    binary: int
    ratio: int
    deviation: int
    process: int


def get_time_series_table_name(size: DatasetSize) -> TableName:
    return f"data_{size}"


def get_time_series_column_counts(n_cols: int) -> TimeSeriesColumnCounts:
    return TimeSeriesColumnCounts(
        binary=max(1, int(0.05 * n_cols)),
        ratio=max(1, int(0.05 * n_cols)),
        deviation=max(1, int(0.05 * n_cols)),
        process=n_cols - 3 * max(1, int(0.05 * n_cols)),
    )


def get_time_series_schemas() -> Mapping[TableName, Mapping[str, pl.DataType | type[pl.DataType]]]:
    schemas = {
        get_time_series_table_name(size): pl.read_parquet_schema(get_dataset_path(size))
        for size in TIME_SERIES_DATASET_SIZES
    }
    schemas[TIME_SERIES_EAV_TABLE_NAME] = pl.read_parquet_schema(get_eav_dataset_path())
    return schemas


def get_time_series_input_files() -> dict[TableName, Path]:
    files = {get_time_series_table_name(size): get_dataset_path(size) for size in TIME_SERIES_DATASET_SIZES}
    files[TIME_SERIES_EAV_TABLE_NAME] = get_eav_dataset_path()
    return files


def get_time_series_expected_row_counts() -> dict[TableName, int]:
    counts = {get_time_series_table_name(size): rows for size, (rows, _cols) in TIME_SERIES_DATASET_SIZES.items()}
    wide_rows, wide_cols = TIME_SERIES_DATASET_SIZES["wide"]
    counts[TIME_SERIES_EAV_TABLE_NAME] = wide_rows * wide_cols
    return counts


def generate_time_series_data(n_rows: int, n_cols: int, seed: int = 1) -> pl.DataFrame:
    downtime_duration_minutes = n_rows // 15
    rng = np.random.default_rng(seed)

    end = datetime(2025, 1, 1)
    start = end - timedelta(minutes=n_rows - 1)
    time = pl.datetime_range(start, end, interval="1m", eager=True, time_unit="ms")

    counts = get_time_series_column_counts(n_cols)

    base_signals: list[np.ndarray] = []

    for _ in range(max(1, counts.process // 3)):
        t = np.linspace(0, 4 * np.pi, n_rows)
        trend = np.linspace(0, 2, n_rows) * rng.uniform(-1, 1)
        seasonal = np.sin(t / 4) * rng.uniform(0.5, 2.0)
        noise = rng.normal(0, 0.1, n_rows)

        baseline = rng.integers(-10, 1000)
        base = baseline + trend + seasonal + noise + rng.uniform(10, 40)

        base_signals.append(base)

    columns_data: dict[str, np.ndarray] = {}
    col_idx = 1

    for idx in range(counts.binary):
        prob = rng.uniform(0.3, 0.7)
        binary_data = rng.choice([0, 1], size=n_rows, p=[1 - prob, prob])
        for j in range(1, n_rows):
            if rng.random() < 0.8:
                binary_data[j] = binary_data[j - 1]
        columns_data[f"binary_{idx + 1}"] = binary_data.astype(bool)
        col_idx += 1

    for idx in range(counts.ratio):
        data = rng.beta(2, 2, n_rows)
        columns_data[f"ratio_{idx + 1}"] = data.astype(np.float32)
        col_idx += 1

    for idx in range(counts.deviation):
        data = rng.normal(0, 30, n_rows)
        data = np.clip(data, -100, 100)
        columns_data[f"deviation_{idx + 1}"] = data.astype(np.float32)
        col_idx += 1

    for idx in range(counts.process):
        base_idx = idx % len(base_signals)
        base_signal = base_signals[base_idx]

        correlation_coeff = rng.uniform(0.3, 0.9)
        noise_level = rng.uniform(0.1, 0.5)
        offset = rng.uniform(-20, 20)
        scale = rng.uniform(0.5, 2.0)

        correlated_data = (
            offset + scale * correlation_coeff * base_signal + rng.normal(0, noise_level * np.std(base_signal), n_rows)
        )

        columns_data[f"process_{idx + 1}"] = correlated_data.astype(np.float32)
        col_idx += 1

    df = pl.DataFrame({"time": time})

    for col_name, col_data in columns_data.items():
        df = df.with_columns(pl.Series(col_name, col_data))

    df = _add_downtime_periods(df, n_rows, downtime_duration_minutes, rng)

    cols = [n for n in df.columns if n != "time"]

    rng.shuffle(cols)

    df = df.select("time", *cols)

    return df


def _add_downtime_periods(
    df: pl.DataFrame, n_rows: int, downtime_duration_minutes: int, rng: np.random.Generator
) -> pl.DataFrame:
    downtime_rows = downtime_duration_minutes

    total_available = n_rows - 2 * downtime_rows
    if total_available < 5 * downtime_rows:
        downtime_rows = max(1, total_available // 10)

    early_section = total_available // 4
    late_section = total_available // 4
    middle_gap = total_available - early_section - late_section - 3 * downtime_rows

    downtime_starts = [
        downtime_rows,
        downtime_rows + early_section // 2,
        downtime_rows + early_section + middle_gap // 3,
        downtime_rows + early_section + middle_gap * 2 // 3,
        n_rows - late_section - downtime_rows,
    ]

    data_columns = [col for col in df.columns if col != "time"]

    for start_idx in downtime_starts:
        end_idx = min(start_idx + downtime_rows, n_rows)

        mask = pl.int_range(0, n_rows).is_between(start_idx, end_idx - 1)

        for col in data_columns:
            if col.startswith("binary_"):
                df = df.with_columns(pl.when(mask).then(False).otherwise(pl.col(col)).alias(col))
            elif col.startswith("process_") and rng.random() < 0.7:
                df = df.with_columns(pl.when(mask).then(None).otherwise(pl.col(col)).alias(col))
            elif col.startswith("process_") and rng.random() < 0.9:
                constant_val = rng.choice([0.0, -1.0, 1.0], p=[0.8, 0.1, 0.1])
                df = df.with_columns(pl.when(mask).then(constant_val).otherwise(pl.col(col)).alias(col))
            elif col.startswith("ratio_"):
                # ratios might drop to very low values during downtime
                low_val = rng.uniform(0.0, 0.1)
                df = df.with_columns(pl.when(mask).then(low_val).otherwise(pl.col(col)).alias(col))
            elif col.startswith("deviation_"):
                # deviations might spike or go to zero during downtime
                deviation_val = rng.choice([0.0, 50.0, -50.0], p=[0.6, 0.2, 0.2])
                df = df.with_columns(pl.when(mask).then(deviation_val).otherwise(pl.col(col)).alias(col))

    return df


def get_dataset_path(size: DatasetSize) -> Path:
    return SETTINGS.input_data_directory / "time_series" / f"{get_time_series_table_name(size)}.parquet"


def get_eav_dataset_path() -> Path:
    return SETTINGS.input_data_directory / "time_series" / f"{TIME_SERIES_EAV_TABLE_NAME}.parquet"


def get_eav_metric_id(column_name: str, n_cols: int) -> int:
    prefix, raw_index = column_name.rsplit("_", 1)
    index = int(raw_index)
    counts = get_time_series_column_counts(n_cols)

    if prefix == "binary":
        return index
    if prefix == "ratio":
        return counts.binary + index
    if prefix == "deviation":
        return counts.binary + counts.ratio + index
    if prefix == "process":
        return counts.binary + counts.ratio + counts.deviation + index

    raise ValueError(f"Unknown time-series column prefix for EAV conversion: {column_name}")


def convert_wide_to_eav(source_path: Path, target_path: Path, n_cols: int) -> None:
    value_columns = [column for column in pl.read_parquet(source_path, n_rows=0).columns if column != "time"]

    id_lookup = pl.DataFrame(
        {
            "metric_name": value_columns,
            "id": [get_eav_metric_id(column, n_cols) for column in value_columns],
        }
    )

    eav = (
        pl.scan_parquet(source_path)
        .with_columns([pl.col(column).cast(pl.Float32).alias(column) for column in value_columns])
        .unpivot(on=value_columns, index="time", variable_name="metric_name", value_name="value")
        .join(id_lookup.lazy(), on="metric_name", how="inner")
        .select("time", pl.col("id").cast(pl.Int32), pl.col("value").cast(pl.Float32))
    )

    eav.sink_parquet(target_path)
    _LOGGER.info(f"Wrote EAV dataset {target_path.name}")


def prepare_data(overwrite: bool = False) -> None:
    output_directory = SETTINGS.input_data_directory / "time_series"
    output_directory.mkdir(exist_ok=True, parents=True)

    for size, (rows, cols) in TIME_SERIES_DATASET_SIZES.items():
        fpath = get_dataset_path(size)

        if fpath.is_file() and not overwrite:
            _LOGGER.info(f"Reusing dataset {fpath.name}")
        else:
            df = generate_time_series_data(rows, cols)
            df.write_parquet(fpath)

            _LOGGER.info(f"Wrote dataset {fpath.name}")

        if size != "wide":
            continue

        eav_path = get_eav_dataset_path()
        if eav_path.is_file() and not overwrite:
            _LOGGER.info(f"Reusing dataset {eav_path.name}")
            continue

        convert_wide_to_eav(fpath, eav_path, cols)


class TimeSeries[DBT: Database](BenchmarkSuite[DBT]):
    name: SuiteName = "time_series"

    def expected_table_row_counts(self) -> Mapping[TableName, int]:
        return get_time_series_expected_row_counts()

    def get_primary_key(self, table_name: TableName) -> str | list[str] | None:
        # do not use primary key for time series data (e.g. Clickhouse does not enforce unique primary key)
        return None

    def get_not_null(self, table_name: TableName) -> str | list[str] | None:
        if table_name == TIME_SERIES_EAV_TABLE_NAME:
            return ["time", "id"]
        return "time"

    @property
    def populate_kwargs(self) -> dict[str, Any]:
        return {}

    def insert_table(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table_name: TableName,
        primary_key: str | list[str] | None,
        not_null: str | list[str] | None,
    ) -> None:
        self.db.insert(df, table_name, primary_key=primary_key, not_null=not_null, **self.populate_kwargs)

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        self.db.initialize_schema("time_series")

        for table_name, fpath in get_time_series_input_files().items():
            primary_key = self.get_primary_key(table_name)
            not_null = self.get_not_null(table_name)

            df = pl.scan_parquet(fpath)

            with self.db.phase_context("insert", table_name=table_name):
                self.insert_table(df, table_name, primary_key, not_null)
                _LOGGER.info(f"Inserted {table_name} for {self.name}")

        _LOGGER.info(f"Inserted all time_series tables for {self.name}")

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        # restart db to ensure data is not kept in-memory by the db, and also
        # ensure that WAL is processed etc...
        if restart:
            self.db.restart_event()

    def load_time_series_query(self, query_name: str) -> str:
        db_specific = TIME_SERIES_QUERIES_DIRECTORY / f"{self.db.name}/{query_name}.sql"
        common = TIME_SERIES_QUERIES_DIRECTORY / f"{query_name}.sql"

        sql_source = db_specific if db_specific.is_file() else common

        with (sql_source).open() as f:
            return f.read()

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {}

    def include_query(self, query_name: str) -> bool:
        return True

    def run(self) -> None:
        t0 = perf_counter()
        for idx, (query_name, iterations) in enumerate(TIME_SERIES_QUERY_NAMES.items()):
            if not self.include_query(query_name):
                continue

            with self.db.query_context("time_series", query_name):
                query = self.load_time_series_query(query_name)

                for it in range(1, iterations + 1):
                    df, t = self.db.execute_query_iteration(
                        query_name=query_name,
                        iteration=it,
                        query=query,
                        fetch_kwargs=self.fetch_kwargs,
                    )

                    _LOGGER.info(
                        f"Executed {query_name} ({idx + 1:_}/{len(TIME_SERIES_QUERY_NAMES):_}) "
                        f"iteration {it:_}/{iterations:_} "
                        f"in {1_000 * (t):_.2f} ms, shape=({df.shape[0]:_}, {df.shape[1]:_})\n"
                        f"df (head 100)={df.head(100)}"
                    )

        _LOGGER.info(
            f"Executed {len(TIME_SERIES_QUERY_NAMES):_} queries "
            f"(with repetitions) in {perf_counter() - t0:_.2f} seconds"
        )
