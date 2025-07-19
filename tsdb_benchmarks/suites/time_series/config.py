import logging
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

import numpy as np
import polars as pl

from ...settings import REPO_ROOT

_LOGGER = logging.getLogger(__name__)


METHOD: Literal["process"] = "process"

# TODO: EAV tables take too long and use too much disk space
DatasetSize = Literal[
    "small",
    "medium",
    # "large",
    # "huge",
]


TIME_SERIES_DATASET_SIZES: dict[DatasetSize, tuple[int, int]] = {
    "small": (200_000, 100),
    "medium": (2_000_000, 100),
    # "large": (2_000_000, 500),
    # "huge": (2_000_000, 1_000),
}


TIME_SERIES_QUERY_NAMES = {
    "0001_select_timestamp": 10,
    "0002_select_timestamps": 10,
}

EAV_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "time": pl.Datetime("ms"),
    "id": pl.Int16,
    "value": pl.Float32,
}


def get_time_series_schemas() -> Mapping[str, Mapping[str, pl.DataType | type[pl.DataType]]]:
    return {f"process_{size}_eav": EAV_SCHEMA for size in TIME_SERIES_DATASET_SIZES} | {
        f"process_{size}_wide": pl.read_parquet_schema(
            REPO_ROOT / "data/input/time_series" / get_dataset_name("process", "wide", rows, cols)
        )
        for size, (rows, cols) in TIME_SERIES_DATASET_SIZES.items()
    }


def get_time_series_input_files() -> dict[str, Path]:
    return {
        f"process_{size}_wide": REPO_ROOT / "data/input/time_series" / get_dataset_name("process", "wide", rows, cols)
        for size, (rows, cols) in TIME_SERIES_DATASET_SIZES.items()
    } | {
        f"process_{size}_eav": REPO_ROOT / "data/input/time_series" / get_dataset_name("process", "eav", rows, cols)
        for size, (rows, cols) in TIME_SERIES_DATASET_SIZES.items()
    }


def generate_random_time_series_data(n_rows: int, n_cols: int) -> pl.DataFrame:
    end = datetime(2025, 1, 1)
    start = end - timedelta(minutes=n_rows - 1)

    time = pl.datetime_range(start, end, interval="1m", eager=True, time_unit="ms")

    df = pl.DataFrame({"time": time})

    data = np.random.rand(n_rows, n_cols)

    df = pl.concat(
        [df, pl.from_numpy(data, schema={f"col_{n}": pl.Float32 for n in range(1, n_cols + 1)}, orient="row")],
        how="horizontal",
    )

    return df


def generate_time_series_data(n_rows: int, n_cols: int, seed: int = 1) -> pl.DataFrame:
    downtime_duration_minutes = n_rows // 15
    rng = np.random.default_rng(seed)

    end = datetime(2025, 1, 1)
    start = end - timedelta(minutes=n_rows - 1)
    time = pl.datetime_range(start, end, interval="1m", eager=True, time_unit="ms")

    fraction_binary = 0.05
    fraction_0_1 = 0.05
    fraction_neg_100_pos_100 = 0.05

    n_binary = max(1, int(fraction_binary * n_cols))
    n_bounded_0_1 = max(1, int(fraction_0_1 * n_cols))
    n_bounded_neg100_100 = max(1, int(fraction_neg_100_pos_100 * n_cols))
    n_correlated = n_cols - n_binary - n_bounded_0_1 - n_bounded_neg100_100

    base_signals: list[np.ndarray] = []

    for _ in range(max(1, n_correlated // 3)):
        t = np.linspace(0, 4 * np.pi, n_rows)
        trend = np.linspace(0, 2, n_rows) * rng.uniform(-1, 1)
        seasonal = np.sin(t / 4) * rng.uniform(0.5, 2.0)
        noise = rng.normal(0, 0.1, n_rows)

        baseline = rng.integers(-10, 1000)
        base = baseline + trend + seasonal + noise + rng.uniform(10, 40)

        base_signals.append(base)

    columns_data: dict[str, np.ndarray] = {}
    col_idx = 1

    for idx in range(n_binary):
        prob = rng.uniform(0.3, 0.7)
        binary_data = rng.choice([0, 1], size=n_rows, p=[1 - prob, prob])
        for j in range(1, n_rows):
            if rng.random() < 0.8:
                binary_data[j] = binary_data[j - 1]
        columns_data[f"binary_{idx + 1}"] = binary_data.astype(bool)
        col_idx += 1

    for idx in range(n_bounded_0_1):
        data = rng.beta(2, 2, n_rows)
        columns_data[f"ratio_{idx + 1}"] = data.astype(np.float32)
        col_idx += 1

    for idx in range(n_bounded_neg100_100):
        data = rng.normal(0, 30, n_rows)
        data = np.clip(data, -100, 100)
        columns_data[f"deviation_{idx + 1}"] = data.astype(np.float32)
        col_idx += 1

    for idx in range(n_correlated):
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


def write_eav_dataset(fpath: Path, overwrite: bool = False) -> None:
    eav_fpath = fpath.with_name(fpath.name.replace("_wide_", "_eav_"))

    if eav_fpath.is_file() and not overwrite:
        return

    df = pl.scan_parquet(fpath)

    columns = df.collect_schema().names()

    assert columns[0] == "time"

    columns.pop(0)

    col_id_map = {n: str(idx) for idx, n in enumerate(columns)}
    df = df.rename(col_id_map)

    # boolean is converted to float32 (0.0 and 1.0)
    df.unpivot(index="time", variable_name="id", value_name="value").sort("id", "time").with_columns(
        pl.col.id.cast(pl.Int16), pl.col.value.cast(pl.Float32)
    ).sink_parquet(eav_fpath)

    _LOGGER.info(f"Wrote EAV dataset {eav_fpath.name}")


def get_dataset_name(
    method: Literal["process", "random"], orientation: Literal["wide", "eav"], rows: int, cols: int
) -> str:
    return f"{method}_{orientation}_{rows / 1e6:.1f}M_{cols / 1e3:.1f}k.parquet"


def generate_time_series_datasets(overwrite: bool = False) -> None:
    output_directory = REPO_ROOT / "data/input/time_series"

    fpaths: list[Path] = []

    # don't exceed 1_600 columns (max for postgres/timescaledb)
    # TODO: make a separate benchmark for very wide data (only use EAV for timescaledb)
    for rows, cols in TIME_SERIES_DATASET_SIZES.values():
        fpath = output_directory / get_dataset_name(METHOD, "wide", rows, cols)
        fpaths.append(fpath)

        if fpath.is_file() and not overwrite:
            continue

        if METHOD == "random":
            df = generate_random_time_series_data(rows, cols)
        else:
            df = generate_time_series_data(rows, cols)

        df.write_parquet(fpath)

        _LOGGER.info(f"Wrote dataset {fpath.name}")

    for fpath in fpaths:
        write_eav_dataset(fpath, overwrite)
