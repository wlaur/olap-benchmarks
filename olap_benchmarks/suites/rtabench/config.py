import asyncio
import logging
import os
from pathlib import Path
from time import perf_counter
from typing import Any, Literal

import httpx
import polars as pl

from ...settings import REPO_ROOT, SETTINGS
from .. import BenchmarkSuite

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


async def download_file(client: httpx.AsyncClient, url: str, dest_path: Path) -> None:
    if dest_path.exists():
        raise ValueError(f"Already exists: {dest_path.name}")

    _LOGGER.info(f"Downloading {url} to {dest_path}")

    response = await client.get(url, follow_redirects=True)
    response.raise_for_status()
    dest_path.write_bytes(response.content)

    os.system(f"cd {dest_path.parent.as_posix()} && gzip -d {dest_path.name}")

    _LOGGER.info(f"Downloaded and extracted {dest_path.name}")


async def download_rtabench_data_async(output_directory: Path) -> None:
    output_directory.mkdir(parents=True, exist_ok=True)

    urls = [f"https://rtadatasets.timescale.com/{name}.csv.gz" for name in RTABENCH_SCHEMAS]

    async with httpx.AsyncClient() as client:
        tasks = []
        for url in urls:
            filename = url.split("/")[-1]
            dest_path = output_directory / filename
            tasks.append(download_file(client, url, dest_path))

        await asyncio.gather(*tasks)


def convert_rtabench_data_to_parquet(data_dir: Path) -> None:
    for name, schema in RTABENCH_SCHEMAS.items():
        fname = data_dir / f"{name}.csv"
        df = pl.read_csv(fname, has_header=False, schema=schema)
        df.write_parquet(fname.with_suffix(".parquet"))
        fname.unlink()

        _LOGGER.info(f"Converted {fname} to Parquet")


def download_rtabench_data() -> None:
    output_directory = REPO_ROOT / "data/input/rtabench"
    output_directory.mkdir(exist_ok=True, parents=True)

    asyncio.run(download_rtabench_data_async(output_directory))

    # convert to Parquet, size goes from 22 GB to 3.8 GB
    # benchmark assumes Parquet inputs for all batch data
    convert_rtabench_data_to_parquet(output_directory)


class RTABench(BenchmarkSuite):
    name: Literal["rtabench"] = "rtabench"

    @property
    def populate_kwargs(self) -> dict[str, Any]:
        return {}

    def populate(self, restart: bool = True) -> None:
        self.db.initialize_schema("rtabench")

        for table_name in RTABENCH_SCHEMAS:
            df = pl.read_parquet(SETTINGS.input_data_directory / f"rtabench/{table_name}.parquet")

            with self.db.event_context(f"insert_{table_name}"):
                self.db.insert(df, table_name, **self.populate_kwargs)
                _LOGGER.info(f"Inserted {table_name} for {self.name}")

        _LOGGER.info(f"Inserted all rtabench tables for {self.name}")

        # restart db to ensure data is not kept in-memory by the db, and also
        # ensure that WAL is processed etc...
        if restart:
            self.db.restart_event()

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {}

    def load_rtabench_query(self, query_name: str) -> str:
        with (RTABENCH_QUERIES_DIRECTORY / f"{self.name}/{query_name}.sql").open() as f:
            return f.read()

    def include_query(self, query_name: str) -> bool:
        return True

    def run(self) -> None:
        t0 = perf_counter()
        for idx, (query_name, iterations) in enumerate(RTABENCH_QUERY_NAMES.items()):
            if not self.include_query(query_name):
                continue

            with self.db.query_context(self.name, query_name):
                query = self.load_rtabench_query(query_name)

                for it in range(1, iterations + 1):
                    with self.db.event_context(f"query_{query_name}_iteration_{it}"):
                        t1 = perf_counter()
                        df = self.db.fetch(query, **self.fetch_kwargs)
                        t = perf_counter() - t1

                    # time delta t will not match time at end - time at start exactly,
                    # but within a couple of milliseconds
                    # there is a small overhead when the event is sent to the queue
                    # (the actual write to result db happens later)
                    _LOGGER.info(
                        f"Executed {query_name} ({idx + 1:_}/{len(RTABENCH_QUERY_NAMES):_}) "
                        f"iteration {it:_}/{iterations:_} "
                        f"in {1_000 * (t):_.2f} ms\ndf={df}"
                    )

        _LOGGER.info(
            f"Executed {len(RTABENCH_QUERY_NAMES):_} queries (with repetitions) in {perf_counter() - t0:_.2f} seconds"
        )
