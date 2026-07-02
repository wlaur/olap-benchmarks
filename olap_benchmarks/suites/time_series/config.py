import logging
import shutil
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

assert set(TIME_SERIES_DATASET_SIZES) == set(get_args(DatasetSize))

MutateAction = Literal["insert", "upsert", "delete"]

MUTATE_ROW_COUNTS = [1, 100, 10_000]
MUTATE_ACTIONS: list[MutateAction] = ["insert", "upsert", "delete"]
MUTATE_TABLES: list[TableName] = ["data_tall", "data_wide", "data_large"]
MUTATE_ITERATIONS = 3


@dataclass(frozen=True)
class MutateStep:
    action: MutateAction
    table: TableName
    row_count: int

    @property
    def name(self) -> str:
        return f"{self.action}_{self.table}_{self.row_count}"


TIME_SERIES_MUTATE_STEPS: list[MutateStep] = [
    MutateStep(action=action, table=table, row_count=row_count)
    for action in MUTATE_ACTIONS
    for table in MUTATE_TABLES
    for row_count in MUTATE_ROW_COUNTS
]


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


def get_time_series_input_files() -> dict[TableName, Path]:
    return {get_time_series_table_name(size): get_dataset_path(size) for size in TIME_SERIES_DATASET_SIZES}


def get_time_series_expected_row_counts() -> dict[TableName, int]:
    return {get_time_series_table_name(size): rows for size, (rows, _cols) in TIME_SERIES_DATASET_SIZES.items()}


@dataclass(frozen=True)
class BaseSignalSpec:
    trend_scale: float
    seasonal_scale: float
    baseline: int
    offset: float


@dataclass(frozen=True)
class ProcessSignalSpec:
    base_idx: int
    correlation_coeff: float
    noise_level: float
    offset: float
    scale: float


@dataclass(frozen=True)
class DowntimeWindow:
    start_idx: int
    end_idx: int


@dataclass(frozen=True)
class TimeSeriesGenerationSpec:
    n_rows: int
    n_cols: int
    seed: int
    counts: TimeSeriesColumnCounts
    column_order: list[str]
    binary_probs: list[float]
    base_specs: list[BaseSignalSpec]
    process_specs: list[ProcessSignalSpec]
    downtime_windows: list[DowntimeWindow]


def _make_rng(seed: int, *parts: int) -> np.random.Generator:
    return np.random.default_rng(np.random.SeedSequence([seed, *parts]))


def _build_downtime_windows(n_rows: int, downtime_duration_minutes: int) -> list[DowntimeWindow]:
    downtime_rows = downtime_duration_minutes
    total_available = n_rows - 2 * downtime_rows

    if total_available < 5 * downtime_rows:
        downtime_rows = max(1, total_available // 10)

    early_section = total_available // 4
    late_section = total_available // 4
    middle_gap = total_available - early_section - late_section - 3 * downtime_rows

    return [
        DowntimeWindow(start_idx=downtime_rows, end_idx=min(2 * downtime_rows, n_rows)),
        DowntimeWindow(
            start_idx=downtime_rows + early_section // 2,
            end_idx=min(downtime_rows + early_section // 2 + downtime_rows, n_rows),
        ),
        DowntimeWindow(
            start_idx=downtime_rows + early_section + middle_gap // 3,
            end_idx=min(downtime_rows + early_section + middle_gap // 3 + downtime_rows, n_rows),
        ),
        DowntimeWindow(
            start_idx=downtime_rows + early_section + middle_gap * 2 // 3,
            end_idx=min(downtime_rows + early_section + middle_gap * 2 // 3 + downtime_rows, n_rows),
        ),
        DowntimeWindow(
            start_idx=n_rows - late_section - downtime_rows,
            end_idx=min(n_rows - late_section, n_rows),
        ),
    ]


def build_time_series_generation_spec(n_rows: int, n_cols: int, seed: int = 1) -> TimeSeriesGenerationSpec:
    rng = np.random.default_rng(seed)
    counts = get_time_series_column_counts(n_cols)
    column_order = [
        *[f"binary_{idx + 1}" for idx in range(counts.binary)],
        *[f"ratio_{idx + 1}" for idx in range(counts.ratio)],
        *[f"deviation_{idx + 1}" for idx in range(counts.deviation)],
        *[f"process_{idx + 1}" for idx in range(counts.process)],
    ]
    rng.shuffle(column_order)

    base_specs = [
        BaseSignalSpec(
            trend_scale=float(rng.uniform(-1, 1)),
            seasonal_scale=float(rng.uniform(0.5, 2.0)),
            baseline=int(rng.integers(-10, 1000)),
            offset=float(rng.uniform(10, 40)),
        )
        for _ in range(max(1, counts.process // 3))
    ]
    process_specs = [
        ProcessSignalSpec(
            base_idx=idx % len(base_specs),
            correlation_coeff=float(rng.uniform(0.3, 0.9)),
            noise_level=float(rng.uniform(0.1, 0.5)),
            offset=float(rng.uniform(-20, 20)),
            scale=float(rng.uniform(0.5, 2.0)),
        )
        for idx in range(counts.process)
    ]

    return TimeSeriesGenerationSpec(
        n_rows=n_rows,
        n_cols=n_cols,
        seed=seed,
        counts=counts,
        column_order=column_order,
        binary_probs=[float(rng.uniform(0.3, 0.7)) for _ in range(counts.binary)],
        base_specs=base_specs,
        process_specs=process_specs,
        downtime_windows=_build_downtime_windows(n_rows, n_rows // 15),
    )


def generate_time_series_data(n_rows: int, n_cols: int, seed: int = 1) -> pl.DataFrame:
    return generate_time_series_data_batch(
        build_time_series_generation_spec(n_rows, n_cols, seed),
        0,
        n_rows,
        batch_index=0,
    )


def _generate_time_values(batch_start: int, batch_end: int, total_rows: int) -> pl.Series:
    end = datetime(2025, 1, 1)
    start = end - timedelta(minutes=total_rows - 1)
    return pl.datetime_range(
        start + timedelta(minutes=batch_start),
        start + timedelta(minutes=batch_end - 1),
        interval="1m",
        eager=True,
        time_unit="ms",
    ).alias("time")


def _apply_downtime_to_batch(
    values: np.ndarray,
    column_name: str,
    column_index: int,
    batch_start: int,
    batch_end: int,
    spec: TimeSeriesGenerationSpec,
) -> np.ndarray:
    adjusted = values.copy()

    for window_index, window in enumerate(spec.downtime_windows):
        overlap_start = max(batch_start, window.start_idx)
        overlap_end = min(batch_end, window.end_idx)
        if overlap_start >= overlap_end:
            continue

        local_start = overlap_start - batch_start
        local_end = overlap_end - batch_start

        if column_name.startswith("binary_"):
            adjusted[local_start:local_end] = False
        elif column_name.startswith("process_"):
            rng = _make_rng(spec.seed, 41, column_index, window_index)
            decision = float(rng.random())
            if decision < 0.7:
                adjusted[local_start:local_end] = np.nan
            elif decision < 0.9:
                adjusted[local_start:local_end] = float(rng.choice([0.0, -1.0, 1.0], p=[0.8, 0.1, 0.1]))
        elif column_name.startswith("ratio_"):
            ratio_rng = _make_rng(spec.seed, 43, column_index, window_index)
            adjusted[local_start:local_end] = float(ratio_rng.uniform(0.0, 0.1))
        elif column_name.startswith("deviation_"):
            adjusted[local_start:local_end] = float(
                _make_rng(spec.seed, 47, column_index, window_index).choice([0.0, 50.0, -50.0], p=[0.6, 0.2, 0.2])
            )

    return adjusted


def _generate_base_signal_batch(
    spec: TimeSeriesGenerationSpec,
    base_index: int,
    batch_start: int,
    batch_end: int,
    batch_index: int,
) -> np.ndarray:
    positions = np.arange(batch_start, batch_end, dtype=np.float64)
    denominator = max(1, spec.n_rows - 1)
    base_spec = spec.base_specs[base_index]
    noise = _make_rng(spec.seed, 19, base_index, batch_index).normal(0, 0.1, batch_end - batch_start)

    return (
        base_spec.baseline
        + (positions / denominator) * 2 * base_spec.trend_scale
        + np.sin((positions * (4 * np.pi / denominator)) / 4) * base_spec.seasonal_scale
        + noise
        + base_spec.offset
    ).astype(np.float32)


def generate_time_series_data_batch(
    spec: TimeSeriesGenerationSpec,
    batch_start: int,
    batch_end: int,
    batch_index: int,
) -> pl.DataFrame:
    batch_rows = batch_end - batch_start
    base_signals = {
        base_index: _generate_base_signal_batch(spec, base_index, batch_start, batch_end, batch_index)
        for base_index in range(len(spec.base_specs))
    }

    data: dict[str, pl.Series] = {"time": _generate_time_values(batch_start, batch_end, spec.n_rows)}

    for column_name in spec.column_order:
        prefix, raw_index = column_name.rsplit("_", 1)
        column_index = int(raw_index) - 1

        if prefix == "binary":
            rng = _make_rng(spec.seed, 11, column_index, batch_index)
            values = rng.choice(
                [False, True],
                size=batch_rows,
                p=[1 - spec.binary_probs[column_index], spec.binary_probs[column_index]],
            )
            for idx in range(1, batch_rows):
                if float(rng.random()) < 0.8:
                    values[idx] = values[idx - 1]
            adjusted = _apply_downtime_to_batch(values, column_name, column_index, batch_start, batch_end, spec)
            data[column_name] = pl.Series(column_name, adjusted)
            continue

        if prefix == "ratio":
            values = _make_rng(spec.seed, 23, column_index, batch_index).beta(2, 2, batch_rows).astype(np.float32)
        elif prefix == "deviation":
            values = _make_rng(spec.seed, 29, column_index, batch_index).normal(0, 30, batch_rows).astype(np.float32)
            values = np.clip(values, -100, 100)
        elif prefix == "process":
            process_spec = spec.process_specs[column_index]
            base_signal = base_signals[process_spec.base_idx]
            noise_scale = process_spec.noise_level * max(float(np.std(base_signal)), 1e-6)
            values = (
                process_spec.offset
                + process_spec.scale * process_spec.correlation_coeff * base_signal
                + _make_rng(spec.seed, 31, column_index, batch_index).normal(0, noise_scale, batch_rows)
            ).astype(np.float32)
        else:
            raise ValueError(f"Unsupported time-series column prefix: {column_name}")

        adjusted = _apply_downtime_to_batch(values, column_name, column_index, batch_start, batch_end, spec)
        data[column_name] = pl.Series(column_name, adjusted, nan_to_null=True)

    return pl.DataFrame(data)


def get_time_series_batch_rows(n_cols: int, target_cells: int = 20_000_000) -> int:
    return max(10_000, min(250_000, target_cells // max(1, n_cols)))


def write_time_series_dataset(
    fpath: Path,
    n_rows: int,
    n_cols: int,
    seed: int = 1,
) -> None:
    spec = build_time_series_generation_spec(n_rows, n_cols, seed)
    batch_rows = get_time_series_batch_rows(n_cols)
    temp_dir = fpath.parent / f".{fpath.stem}_parts"
    batch_count = 0

    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=False)

    try:
        for batch_number, batch_start in enumerate(range(0, n_rows, batch_rows), start=1):
            batch_count = batch_number
            batch_end = min(batch_start + batch_rows, n_rows)
            batch_path = temp_dir / f"part_{batch_number:05d}.parquet"
            generate_time_series_data_batch(spec, batch_start, batch_end, batch_index=batch_number - 1).write_parquet(
                batch_path
            )
            _LOGGER.info(
                f"Wrote {fpath.stem} partition {batch_number:_} "
                f"({batch_end - batch_start:_} rows, {batch_end:_}/{n_rows:_})"
            )

        pl.scan_parquet(str(temp_dir / "*.parquet")).sink_parquet(fpath)
        _LOGGER.info(f"Stitched dataset {fpath.name} from {batch_count:_} partition(s)")
    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)


def get_dataset_path(size: DatasetSize) -> Path:
    return SETTINGS.input_data_directory / "time_series" / f"{get_time_series_table_name(size)}.parquet"


def prepare_data(overwrite: bool = False) -> None:
    output_directory = SETTINGS.input_data_directory / "time_series"
    output_directory.mkdir(exist_ok=True, parents=True)

    for size, (rows, cols) in TIME_SERIES_DATASET_SIZES.items():
        fpath = get_dataset_path(size)

        if fpath.is_file() and not overwrite:
            _LOGGER.info(f"Reusing dataset {fpath.name}")
        else:
            write_time_series_dataset(fpath, rows, cols)


class TimeSeries[DBT: Database](BenchmarkSuite[DBT]):
    supported_operations = ("populate", "mutate", "select")
    name: SuiteName = "time_series"

    def get_mutate_steps(self) -> tuple[list[MutateStep], list[MutateStep]]:
        enabled_steps: list[MutateStep] = []
        skipped_steps: list[MutateStep] = []

        for step in TIME_SERIES_MUTATE_STEPS:
            if self.db.is_mutation_step_enabled(self.name, step.name):
                enabled_steps.append(step)
            else:
                skipped_steps.append(step)

        return enabled_steps, skipped_steps

    def expected_table_row_counts(self) -> Mapping[TableName, int]:
        return get_time_series_expected_row_counts()

    def get_primary_key(self, table_name: TableName) -> str | list[str] | None:
        # do not use primary key for time series data (e.g. Clickhouse does not enforce unique primary key)
        return None

    def get_not_null(self, table_name: TableName) -> str | list[str] | None:
        _ = table_name
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

    def select(self) -> None:
        t0 = perf_counter()
        for idx, (query_name, iterations) in enumerate(TIME_SERIES_QUERY_NAMES.items()):
            if not self.include_query(query_name):
                continue

            with self.db.query_context(query_name):
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

    def _get_mutate_primary_key(self, table_name: TableName) -> str | list[str]:
        _ = table_name
        return "time"

    def _get_mutate_dataset_size(self, table_name: TableName) -> DatasetSize | None:
        for size in TIME_SERIES_DATASET_SIZES:
            if get_time_series_table_name(size) == table_name:
                return size
        return None

    def _get_table_column_order(self, table_name: TableName) -> list[str]:
        size = self._get_mutate_dataset_size(table_name)
        if size is None:
            raise ValueError(f"Unsupported time-series table: {table_name}")

        n_rows, n_cols = TIME_SERIES_DATASET_SIZES[size]
        spec = build_time_series_generation_spec(n_rows, n_cols, seed=1)
        return ["time", *spec.column_order]

    def _generate_insert_data(self, step: MutateStep, seed: int) -> pl.DataFrame:
        size = self._get_mutate_dataset_size(step.table)
        if size is None:
            raise ValueError(f"Unsupported time-series table: {step.table}")

        _rows, n_cols = TIME_SERIES_DATASET_SIZES[size]
        df = generate_time_series_data(step.row_count, n_cols, seed=seed)
        start_time = datetime(2025, 1, 1) + timedelta(minutes=seed * 100_000)
        return df.select(self._get_table_column_order(step.table)).with_columns(
            pl.datetime_range(
                start_time,
                start_time + timedelta(minutes=step.row_count - 1),
                interval="1m",
                eager=True,
                time_unit="ms",
            ).alias("time")
        )

    def _generate_upsert_data(self, step: MutateStep, seed: int) -> pl.DataFrame:
        size = self._get_mutate_dataset_size(step.table)
        if size is None:
            raise ValueError(f"Unsupported time-series table: {step.table}")

        n_rows, n_cols = TIME_SERIES_DATASET_SIZES[size]
        df = generate_time_series_data(step.row_count, n_cols, seed=seed + 1000)
        rng = np.random.default_rng(seed)
        end = datetime(2025, 1, 1)
        start = end - timedelta(minutes=n_rows - 1)
        offsets = sorted(rng.choice(n_rows, size=step.row_count, replace=False))
        times = pl.Series(
            "time",
            [start + timedelta(minutes=int(o)) for o in offsets],
            dtype=pl.Datetime("ms"),
        )
        return df.select(self._get_table_column_order(step.table)).with_columns(times)

    def _generate_delete_keys(self, step: MutateStep, seed: int) -> pl.DataFrame:
        size = self._get_mutate_dataset_size(step.table)
        if size is None:
            raise ValueError(f"Unsupported time-series table: {step.table}")

        n_rows, _n_cols = TIME_SERIES_DATASET_SIZES[size]

        rng = np.random.default_rng(seed)
        end = datetime(2025, 1, 1)
        start = end - timedelta(minutes=n_rows - 1)
        offsets = sorted(rng.choice(n_rows, size=step.row_count, replace=False))
        times = pl.Series(
            "time",
            [start + timedelta(minutes=int(o)) for o in offsets],
            dtype=pl.Datetime("ms"),
        )
        return pl.DataFrame({"time": times})

    def _apply_insert(self, step: MutateStep, df: pl.DataFrame) -> None:
        pk = self._get_mutate_primary_key(step.table)
        self.db.insert(df, step.table, primary_key=pk if isinstance(pk, list) else None)

    def _apply_upsert(self, step: MutateStep, df: pl.DataFrame) -> None:
        pk = self._get_mutate_primary_key(step.table)
        self.db.upsert(df, step.table, primary_key=pk)

    def _apply_delete(self, step: MutateStep, keys: pl.DataFrame) -> None:
        pk = self._get_mutate_primary_key(step.table)
        self.db.delete(step.table, primary_key=pk, keys=keys)

    def mutate(self) -> None:
        t0 = perf_counter()
        steps, skipped_steps = self.get_mutate_steps()

        if skipped_steps:
            skipped_names = ", ".join(step.name for step in skipped_steps)
            _LOGGER.info(f"Skipping {len(skipped_steps):_} mutation steps for {self.db.name}: {skipped_names}")

        if not steps:
            _LOGGER.info(f"No mutation steps enabled for {self.name} on {self.db.name}")
            return

        for step_idx, step in enumerate(steps):
            for iteration in range(1, MUTATE_ITERATIONS + 1):
                seed = step_idx * 1000 + iteration

                with self.db.mutation_context(
                    query_name=step.name,
                    iteration=iteration,
                    table_name=step.table,
                ):
                    match step.action:
                        case "insert":
                            self._apply_insert(step, self._generate_insert_data(step, seed))
                        case "upsert":
                            self._apply_upsert(step, self._generate_upsert_data(step, seed))
                        case "delete":
                            self._apply_delete(step, self._generate_delete_keys(step, seed))

                _LOGGER.info(
                    f"Executed {step.name} ({step_idx + 1:_}/{len(steps):_}) "
                    f"iteration {iteration:_}/{MUTATE_ITERATIONS:_}"
                )

        _LOGGER.info(f"Executed {len(steps):_} mutation steps (with repetitions) in {perf_counter() - t0:_.2f} seconds")
