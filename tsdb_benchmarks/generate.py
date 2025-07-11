import asyncio
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import numpy as np
import polars as pl

from .settings import REPO_ROOT

_LOGGER = logging.getLogger(__name__)


def generate_data(n_rows: int, n_cols: int) -> pl.DataFrame:
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


async def download_file(client: httpx.AsyncClient, url: str, dest_path: Path) -> None:
    if dest_path.exists():
        raise ValueError(f"Already exists: {dest_path.name}")

    _LOGGER.info(f"Downloading {url} to {dest_path}")

    response = await client.get(url, follow_redirects=True)
    response.raise_for_status()
    dest_path.write_bytes(response.content)

    os.system(f"cd {dest_path.parent.as_posix()} && gzip -d {dest_path.name}")

    _LOGGER.info(f"Downloaded and extracted {dest_path.name}")


async def download_rtabench_data_async(target: Path) -> None:
    target.mkdir(parents=True, exist_ok=True)

    URLS = [
        "https://rtadatasets.timescale.com/customers.csv.gz",
        "https://rtadatasets.timescale.com/products.csv.gz",
        "https://rtadatasets.timescale.com/orders.csv.gz",
        "https://rtadatasets.timescale.com/order_items.csv.gz",
        "https://rtadatasets.timescale.com/order_events.csv.gz",
    ]

    async with httpx.AsyncClient() as client:
        tasks = []
        for url in URLS:
            filename = url.split("/")[-1]
            dest_path = target / filename
            tasks.append(download_file(client, url, dest_path))

        await asyncio.gather(*tasks)


def convert_rtabench_data_to_parquet(data_dir: Path) -> None:
    schemas: dict[str, dict[str, pl.DataType | type[pl.DataType]]] = {
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

    for name, schema in schemas.items():
        fname = data_dir / f"{name}.csv"
        df = pl.read_csv(fname, has_header=False, schema=schema)
        df.write_parquet(fname.with_suffix(".parquet"))
        fname.unlink()

        _LOGGER.info(f"Converted {fname} to Parquet")


def download_rtabench_data() -> None:
    target = REPO_ROOT / "data/input/rtabench"

    asyncio.run(download_rtabench_data_async(target))

    # convert to Parquet, size goes from 22 GB to 3.8 GB
    # benchmark assumes Parquet inputs for all batch data
    convert_rtabench_data_to_parquet(target)
