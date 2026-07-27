# Based on RTABench by Timescale
# https://github.com/timescale/rtabench

import logging
import shutil
from gzip import open as gzip_open
from time import perf_counter
from typing import Any

import polars as pl

from ...dbs import Database
from ...settings import REPO_ROOT, SETTINGS, SuiteName, TableName
from .. import BenchmarkSuite
from ..download import download_file

RTABENCH_QUERIES_DIRECTORY = REPO_ROOT / "olap_benchmarks/suites/rtabench/queries"

_LOGGER = logging.getLogger(__name__)

RTABENCH_QUERY_NAMES = {
    "0000_terminal_hourly_stats": 3,
    "0001_count_orders_from_terminal": 5,
    "0002_global_agg": 5,
    "0003_exists_order_delivered_from_terminal": 5,
    "0004_count_delayed_orders_per_day": 5,
    "0005_search_events_for_processor": 5,
    "0006_order_events_without_backups": 5,
    "0007_last_order_event_for_order": 5,
    "0008_most_week_delayed_order": 5,
    "0009_departed_orders_count": 5,
    "0010_last_event_for_an_order": 5,
    "0011_events_for_an_order": 5,
    "0012_max_satisfaction_for_order_per_day": 5,
    "0013_satisfaction_with_without_backup": 5,
    "0014_sum_prod_stock_price_per_category": 5,
    "0015_exists_order_delivered_for_customer": 5,
    "0016_customers_with_most_orders": 5,
    "0017_top_selling_month_product": 5,
    "0018_customer_month_value": 5,
    "0019_out_of_stock_products": 5,
    "0020_customers_outstanding": 5,
    "0021_sales_volume_by_country": 5,
    "0022_sales_volume_by_country_state": 5,
    "0023_top_sales_volume_product_from_terminal": 2,
    "0024_top_customer_by_revenue": 5,
    "0025_product_category_performance": 5,
    "0026_average_order_value": 5,
    "0027_country_category_performance": 5,
    "0028_sales_volume_by_age_group": 5,
    "0029_top_product_in_age_group": 5,
    "0030_customers_with_most_orders_delivered": 5,
    "1000_terminal_hourly_stats": 3,
    "1004_count_delayed_orders_per_day": 5,
    "1008_most_week_delayed_order": 5,
    "1012_max_satisfaction_for_order_per_week": 5,
    "1013_satisfaction_with_without_backup": 5,
    "1017_top_selling_month_product": 5,
    "1023_top_sales_volume_product_from_terminal": 2,
    "1025_product_category_performance": 5,
    "1027_country_category_performance": 5,
    "1030_customers_with_most_orders_delivered": 5,
}

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

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        self.db.initialize_schema("rtabench")

        for table_name in RTABENCH_SCHEMAS:
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

    def select(self) -> None:
        t0 = perf_counter()
        failed_queries = 0
        for idx, (query_name, iterations) in enumerate(RTABENCH_QUERY_NAMES.items()):
            progress_label = f"({idx + 1:_}/{len(RTABENCH_QUERY_NAMES):_})"
            if not self.include_query(query_name):
                self.record_skipped_query_steps(
                    query_name,
                    iterations,
                    result_status="unsupported",
                    reason="query file is not defined for suite/database",
                )
                continue

            def log_success(
                it: int,
                df: pl.DataFrame,
                t: float,
                *,
                query_name: str = query_name,
                progress_label: str = progress_label,
                iterations: int = iterations,
            ) -> None:
                _LOGGER.info(
                    f"Executed {query_name} {progress_label} "
                    f"iteration {it:_}/{iterations:_} "
                    f"in {1_000 * (t):_.2f} ms\ndf={df}"
                )

            ok = self.execute_query_with_isolation(
                query_name=query_name,
                iterations=iterations,
                query_loader=lambda query_name=query_name: self.load_rtabench_query(query_name),
                fetch_kwargs_factory=lambda: self.fetch_kwargs,
                progress_label=progress_label,
                log_success=log_success,
            )
            if not ok:
                failed_queries += 1

        if failed_queries:
            _LOGGER.warning(
                f"RTABench select completed on {self.db.name} with {failed_queries:_} failed "
                f"{'queries' if failed_queries != 1 else 'query'}"
            )

        _LOGGER.info(
            f"Executed {len(RTABENCH_QUERY_NAMES):_} queries (with repetitions) in {perf_counter() - t0:_.2f} seconds"
        )
