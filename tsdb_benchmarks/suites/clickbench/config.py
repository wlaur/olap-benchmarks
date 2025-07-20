import polars as pl

from ...settings import SETTINGS

ITERATIONS = 3


def download_clickbench() -> None:
    # TODO: download https://datasets.clickhouse.com/hits_compatible/hits.parquet and move to data/input/clickbench
    raise NotImplementedError


def load_clickbench_dataset() -> pl.DataFrame:
    # parquet file stores these as integers, the schema expects correct dtypes
    timestamp_columns = ["EventTime", "ClientEventTime", "LocalEventTime"]
    date_columns = ["EventDate"]

    return (
        pl.scan_parquet(SETTINGS.input_data_directory / "clickbench/hits.parquet")
        .with_columns(pl.from_epoch(n, "s").cast(pl.Datetime("ms")).alias(n) for n in timestamp_columns)
        .with_columns(pl.col(n).cast(pl.Date).alias(n) for n in date_columns)
    ).collect()
