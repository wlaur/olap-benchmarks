# Based on RTABench by Timescale
# https://github.com/timescale/rtabench

import logging
import shutil
from gzip import open as gzip_open
from typing import Any

import polars as pl

from ...dbs import Database
from ...run_metadata import StepResultStatus
from ...settings import REPO_ROOT, SETTINGS, SuiteName, TableName
from .. import BenchmarkSuite
from ..download import download_file

RTABENCH_QUERIES_DIRECTORY = REPO_ROOT / "olap_benchmarks/suites/rtabench/queries"

_LOGGER = logging.getLogger(__name__)

RTABENCH_QUERY_NAMES = (
    "0000_terminal_hourly_stats",
    "0001_count_orders_from_terminal",
    "0002_global_agg",
    "0003_exists_order_delivered_from_terminal",
    "0004_count_delayed_orders_per_day",
    "0005_search_events_for_processor",
    "0006_order_events_without_backups",
    "0007_last_order_event_for_order",
    "0008_most_week_delayed_order",
    "0009_departed_orders_count",
    "0010_last_event_for_an_order",
    "0011_events_for_an_order",
    "0012_max_satisfaction_for_order_per_day",
    "0013_satisfaction_with_without_backup",
    "0014_sum_prod_stock_price_per_category",
    "0015_exists_order_delivered_for_customer",
    "0016_customers_with_most_orders",
    "0017_top_selling_month_product",
    "0018_customer_month_value",
    "0019_out_of_stock_products",
    "0020_customers_outstanding",
    "0021_sales_volume_by_country",
    "0022_sales_volume_by_country_state",
    "0023_top_sales_volume_product_from_terminal",
    "0024_top_customer_by_revenue",
    "0025_product_category_performance",
    "0026_average_order_value",
    "0027_country_category_performance",
    "0028_sales_volume_by_age_group",
    "0029_top_product_in_age_group",
    "0030_customers_with_most_orders_delivered",
    "1000_terminal_hourly_stats",
    "1004_count_delayed_orders_per_day",
    "1008_most_week_delayed_order",
    "1012_max_satisfaction_for_order_per_week",
    "1013_satisfaction_with_without_backup",
    "1017_top_selling_month_product",
    "1023_top_sales_volume_product_from_terminal",
    "1025_product_category_performance",
    "1027_country_category_performance",
    "1030_customers_with_most_orders_delivered",
)

RTABENCH_SCHEMAS: dict[str, dict[str, pl.DataType | type[pl.DataType]]] = {
    "customers": {
        "customer_id": pl.Int32,
        "name": pl.String,
        "birthday": pl.Datetime("ms"),
        "email": pl.String,
        "address": pl.String,
        "city": pl.String,
        "zip": pl.String,
        "state": pl.String,
        "country": pl.String,
    },
    "products": {
        "product_id": pl.Int32,
        "name": pl.String,
        "description": pl.String,
        "category": pl.String,
        "price": pl.Decimal(10, 2),
        "stock": pl.Int32,
    },
    "orders": {
        "order_id": pl.Int32,
        "customer_id": pl.Int32,
        "created_at": pl.Datetime("ms"),
    },
    "order_items": {
        "order_id": pl.Int32,
        "product_id": pl.Int32,
        "amount": pl.Int32,
    },
    "order_events": {
        "order_id": pl.Int32,
        "counter": pl.Int32,
        "event_created": pl.Datetime("ms"),
        "event_type": pl.String,
        "satisfaction": pl.Float32,
        "processor": pl.String,
        "backup_processor": pl.String,
        "event_payload": pl.String,
    },
}


def prepare_data() -> None:
    output_directory = SETTINGS.input_data_directory / "rtabench"
    output_directory.mkdir(exist_ok=True, parents=True)

    for name, schema in RTABENCH_SCHEMAS.items():
        parquet_path = output_directory / f"{name}.parquet"
        if parquet_path.is_file():
            _LOGGER.info("Reusing %s", parquet_path)
            continue

        compressed_path = output_directory / f"{name}.csv.gz"
        csv_path = output_directory / f"{name}.csv"
        download_file(f"https://rtadatasets.timescale.com/{compressed_path.name}", compressed_path)

        if not csv_path.is_file():
            partial_csv_path = csv_path.with_name(f"{csv_path.name}.part")
            with gzip_open(compressed_path, "rb") as compressed, partial_csv_path.open("wb") as output:
                shutil.copyfileobj(compressed, output)
            partial_csv_path.replace(csv_path)

        partial_parquet_path = parquet_path.with_name(f"{parquet_path.name}.part")
        pl.read_csv(csv_path, has_header=False, schema=schema).write_parquet(partial_parquet_path)
        partial_parquet_path.replace(parquet_path)
        csv_path.unlink()
        compressed_path.unlink()
        _LOGGER.info("Converted %s to Parquet", csv_path)


class RTABench[DBT: Database](BenchmarkSuite[DBT]):
    name: SuiteName = "rtabench"

    def expected_table_row_counts(self) -> dict[TableName, int]:
        return {
            table_name: self.parquet_row_count(SETTINGS.input_data_directory / f"rtabench/{table_name}.parquet")
            for table_name in RTABENCH_SCHEMAS
        }

    @property
    def populate_kwargs(self) -> dict[str, Any]:
        return {}

    @property
    def populate_table_order(self) -> list[TableName]:
        return list(RTABENCH_SCHEMAS)

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        self.db.initialize_schema("rtabench")

        for table_name in self.populate_table_order:
            df = pl.scan_parquet(SETTINGS.input_data_directory / f"rtabench/{table_name}.parquet")

            with self.db.phase_context("insert", table_name=table_name):
                self.db.insert(df, table_name, **self.populate_kwargs)
                _LOGGER.info(f"Inserted {table_name} for {self.name}")

        _LOGGER.info(f"Inserted all rtabench tables for {self.name}")

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        # restart db to ensure data is not kept in-memory by the db, and also
        # ensure that WAL is processed etc...
        if restart:
            self.db.restart_event()

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {}

    def load_rtabench_query(self, query_name: str) -> str:
        with (RTABENCH_QUERIES_DIRECTORY / f"{self.db.name}/{query_name}.sql").open() as f:
            return f.read()

    def include_query(self, query_name: str) -> bool:
        return (RTABENCH_QUERIES_DIRECTORY / f"{self.db.name}/{query_name}.sql").is_file()

    def planned_query_names(self) -> tuple[str, ...]:
        return RTABENCH_QUERY_NAMES

    def load_query(self, query_name: str) -> str:
        return self.load_rtabench_query(query_name)

    def query_skip(self, query_name: str) -> tuple[StepResultStatus, str] | None:
        if self.include_query(query_name):
            return None
        return ("unsupported", "query file is not defined for suite/database")
