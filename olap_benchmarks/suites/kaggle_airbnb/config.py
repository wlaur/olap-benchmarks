# Based on "Testing query speed for DuckDB vs ClickHouse vs StarRocks databases" by Vitaliy
# https://medium.com/@marvin_data/testing-query-speed-for-duckdb-vs-clickhouse-vs-starrocks-databases-fecc6614d1ef

import logging
import shutil
from gzip import open as gzip_open
from pathlib import Path
from typing import Any

import polars as pl

from ...dbs import Database
from ...settings import SuiteName, TableName, get_settings
from .. import BenchmarkSuite
from ..download import download_file

_LOGGER = logging.getLogger(__name__)
INSIDE_AIRBNB_SNAPSHOT_URL = "https://data.insideairbnb.com/united-states/tx/austin/2024-12-14"
INSIDE_AIRBNB_FILES = {
    "calendar": "data/calendar.csv.gz",
    "listings_detailed": "data/listings.csv.gz",
    "listings": "visualisations/listings.csv",
    "neighbourhoods": "visualisations/neighbourhoods.csv",
    "reviews_detailed": "data/reviews.csv.gz",
    "reviews": "visualisations/reviews.csv",
}


KAGGLE_AIRBNB_TABLES = [
    "neighbourhoods",
    "listings",
    "listings_detailed",
    "calendar",
    "reviews",
    "reviews_detailed",
]

KAGGLE_AIRBNB_QUERY_NAMES = (
    "01_calendar_count",
    "02_join_one_table",
    "03_join_two_tables",
    "04_join_three_tables_array_agg",
    "05_join_three_tables_row_number",
)


def _download_csv(data_dir: Path, table_name: str, source: str) -> Path:
    csv_path = data_dir / f"{table_name}.csv"
    if csv_path.is_file():
        return csv_path

    url = f"{INSIDE_AIRBNB_SNAPSHOT_URL}/{source}"
    if not source.endswith(".gz"):
        download_file(url, csv_path)
        return csv_path

    compressed_path = data_dir / f"{table_name}.csv.gz"
    download_file(url, compressed_path)
    partial_path = csv_path.with_name(f"{csv_path.name}.part")
    with gzip_open(compressed_path, "rb") as compressed, partial_path.open("wb") as output:
        shutil.copyfileobj(compressed, output)
    partial_path.replace(csv_path)
    return csv_path


def prepare_data() -> None:
    data_dir = get_settings().input_data_directory / "kaggle_airbnb"
    for table_name, source in INSIDE_AIRBNB_FILES.items():
        _download_csv(data_dir, table_name, source)

    pl.read_csv(data_dir / "calendar.csv").with_columns(
        *[pl.col(col).cast(pl.Date).alias(col) for col in ["date"]],
        *[pl.when(pl.col(col) == "t").then(True).otherwise(False).alias(col) for col in ["available"]],
    ).write_parquet(data_dir / "calendar.parquet")

    pl.read_csv(data_dir / "listings_detailed.csv").with_columns(
        pl.col("bathrooms").cast(pl.String),
        *[
            pl.col(col).cast(pl.Date).alias(col)
            for col in [
                "last_scraped",
                "host_since",
                "calendar_updated",
                "calendar_last_scraped",
                "first_review",
                "last_review",
            ]
        ],
        *[
            pl.when(pl.col(col) == "t").then(True).otherwise(False).alias(col)
            for col in [
                "host_is_superhost",
                "host_has_profile_pic",
                "host_identity_verified",
                "instant_bookable",
                "has_availability",
            ]
        ],
    ).write_parquet(data_dir / "listings_detailed.parquet")

    pl.read_csv(data_dir / "listings.csv").with_columns(
        *[pl.col(col).cast(pl.Date).alias(col) for col in ["last_review"]],
    ).write_parquet(data_dir / "listings.parquet")

    pl.read_csv(data_dir / "neighbourhoods.csv").write_parquet(data_dir / "neighbourhoods.parquet")

    pl.read_csv(data_dir / "reviews_detailed.csv").with_columns(
        *[pl.col(col).cast(pl.Date).alias(col) for col in ["date"]],
    ).write_parquet(data_dir / "reviews_detailed.parquet")

    pl.read_csv(data_dir / "reviews.csv").with_columns(
        *[pl.col(col).cast(pl.Date).alias(col) for col in ["date"]],
    ).write_parquet(data_dir / "reviews.parquet")

    assert all((data_dir / f"{n}.parquet").is_file() for n in KAGGLE_AIRBNB_TABLES)


class KaggleAirbnb[DBT: Database](BenchmarkSuite[DBT]):
    name: SuiteName = "kaggle_airbnb"

    def expected_table_row_counts(self) -> dict[TableName, int]:
        return {
            table_name: self.parquet_row_count(
                get_settings().input_data_directory / f"kaggle_airbnb/{table_name}.parquet"
            )
            for table_name in KAGGLE_AIRBNB_TABLES
        }

    @property
    def populate_kwargs(self) -> dict[str, Any]:
        return {}

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        self.db.initialize_schema("kaggle_airbnb")

        for table_name in KAGGLE_AIRBNB_TABLES:
            df = pl.scan_parquet(get_settings().input_data_directory / f"kaggle_airbnb/{table_name}.parquet")

            with self.db.phase_context("insert", table_name=table_name):
                self.db.insert(df, table_name, **self.populate_kwargs)
                _LOGGER.info(f"Inserted {table_name} for {self.name}")

        _LOGGER.info(f"Inserted all kaggle_airbnb tables for {self.name}")

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        # restart db to ensure data is not kept in-memory by the db, and also
        # ensure that WAL is processed etc...
        if restart:
            self.db.restart_event()

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {}

    def planned_query_names(self) -> tuple[str, ...]:
        return KAGGLE_AIRBNB_QUERY_NAMES
