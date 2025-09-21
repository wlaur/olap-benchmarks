import logging
from time import perf_counter
from typing import Any, Literal

import polars as pl

from ...settings import REPO_ROOT, SETTINGS
from .. import BenchmarkSuite

_LOGGER = logging.getLogger(__name__)
KAGGLE_AIRBNB_QUERIES_DIRECTORY = REPO_ROOT / "olap_benchmarks/suites/kaggle_airbnb/queries"


KAGGLE_AIRBNB_TABLES = [
    "calendar",
    "listings_detailed",
    "listings",
    "neighbourhoods",
    "reviews_detailed",
    "reviews",
]

KAGGLE_AIRBNB_QUERY_NAMES = {
    "01_calendar_count": 10,
    "02_join_one_table": 1,
    "03_join_two_tables": 1,
    "04_join_three_tables_array_agg": 1,
    "05_join_three_tables_row_number": 1,
}


def convert_kaggle_airbnb_data_to_parquet() -> None:
    data_dir = SETTINGS.input_data_directory / "kaggle_airbnb"
    # download and unzip *.csv.zip files from
    # https://www.kaggle.com/datasets/konradb/inside-airbnb-usa (subdirectory Austin)
    # need to login to Kaggle to be able to download
    table_name = "calendar"

    pl.read_csv(data_dir / f"{table_name}.csv").with_columns(
        *[pl.col(col).cast(pl.Date).alias(col) for col in ["date"]],
        *[pl.when(pl.col(col) == "t").then(True).otherwise(False).alias(col) for col in ["available"]],
    ).write_parquet(data_dir / f"{table_name}.parquet")

    table_name = "listings_detailed"

    pl.read_csv(data_dir / f"{table_name}.csv").with_columns(
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
    ).write_parquet(data_dir / f"{table_name}.parquet")

    table_name = "listings"

    pl.read_csv(data_dir / f"{table_name}.csv").with_columns(
        *[pl.col(col).cast(pl.Date).alias(col) for col in ["last_review"]],
    ).write_parquet(data_dir / f"{table_name}.parquet")

    table_name = "neighbourhoods"

    pl.read_csv(data_dir / f"{table_name}.csv").write_parquet(data_dir / f"{table_name}.parquet")

    table_name = "reviews_detailed"

    pl.read_csv(data_dir / f"{table_name}.csv").with_columns(
        *[pl.col(col).cast(pl.Date).alias(col) for col in ["date"]],
    ).write_parquet(data_dir / f"{table_name}.parquet")

    table_name = "reviews"

    pl.read_csv(data_dir / f"{table_name}.csv").with_columns(
        *[pl.col(col).cast(pl.Date).alias(col) for col in ["date"]],
    ).write_parquet(data_dir / f"{table_name}.parquet")

    assert all((data_dir / f"{n}.parquet").is_file() for n in KAGGLE_AIRBNB_TABLES)


class KaggleAirbnb(BenchmarkSuite):
    name: Literal["kaggle_airbnb"] = "kaggle_airbnb"

    @property
    def populate_kwargs(self) -> dict[str, Any]:
        return {}

    def populate(self, restart: bool = True) -> None:
        self.db.initialize_schema("kaggle_airbnb")

        for table_name in KAGGLE_AIRBNB_TABLES:
            df = pl.read_parquet(SETTINGS.input_data_directory / f"kaggle_airbnb/{table_name}.parquet")

            with self.db.event_context(f"insert_{table_name}"):
                self.db.insert(df, table_name, **self.populate_kwargs)
                _LOGGER.info(f"Inserted {table_name} for {self.name}")

        _LOGGER.info(f"Inserted all kaggle_airbnb tables for {self.name}")

        # restart db to ensure data is not kept in-memory by the db, and also
        # ensure that WAL is processed etc...
        if restart:
            self.db.restart_event()

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {}

    def load_kaggle_airbnb_query(self, query_name: str) -> str:
        db_specific = KAGGLE_AIRBNB_QUERIES_DIRECTORY / f"{self.db.name}/{query_name}.sql"
        common = KAGGLE_AIRBNB_QUERIES_DIRECTORY / f"{query_name}.sql"

        sql_source = db_specific if db_specific.is_file() else common

        with (sql_source).open() as f:
            return f.read()

    def include_query(self, query_name: str) -> bool:
        return True

    def run(self) -> None:
        t0 = perf_counter()
        for idx, (query_name, iterations) in enumerate(KAGGLE_AIRBNB_QUERY_NAMES.items()):
            if not self.include_query(query_name):
                continue

            with self.db.query_context(self.name, query_name):
                query = self.load_kaggle_airbnb_query(query_name)

                for it in range(1, iterations + 1):
                    with self.db.event_context(f"query_{query_name}_iteration_{it}"):
                        t1 = perf_counter()
                        df = self.db.fetch(query, **self.fetch_kwargs)
                        t = perf_counter() - t1

                    _LOGGER.info(
                        f"Executed {query_name} ({idx + 1:_}/{len(KAGGLE_AIRBNB_QUERY_NAMES):_}) "
                        f"iteration {it:_}/{iterations:_} "
                        f"in {1_000 * (t):_.2f} ms\ndf={df}"
                    )

        _LOGGER.info(
            f"Executed {len(KAGGLE_AIRBNB_QUERY_NAMES):_} queries (with repetitions) "
            f"in {perf_counter() - t0:_.2f} seconds"
        )
