import logging
from datetime import datetime, timedelta

import numpy as np
import polars as pl

from ...settings import REPO_ROOT

_LOGGER = logging.getLogger(__name__)


def generate_time_series_data(n_rows: int, n_cols: int) -> pl.DataFrame:
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


def generate_time_series_datasets() -> None:
    output_directory = REPO_ROOT / "data/input/time_series"

    for rows, cols in [(200_000, 500), (1_000_000, 2_000)]:
        df = generate_time_series_data(rows, cols)
        fname = f"data_{rows / 1e6:.1f}M_{cols / 1e3:.1f}k.parquet"
        df.write_parquet(output_directory / fname)
        _LOGGER.info(f"Wrote dataset {fname}")
