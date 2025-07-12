import asyncio
import logging
import os
from pathlib import Path

import httpx
import polars as pl

from ...settings import REPO_ROOT

_LOGGER = logging.getLogger(__name__)

RTA_BENCH_SCHEMAS: dict[str, dict[str, pl.DataType | type[pl.DataType]]] = {
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

    urls = [f"https://rtadatasets.timescale.com/{name}.csv.gz" for name in RTA_BENCH_SCHEMAS]

    async with httpx.AsyncClient() as client:
        tasks = []
        for url in urls:
            filename = url.split("/")[-1]
            dest_path = output_directory / filename
            tasks.append(download_file(client, url, dest_path))

        await asyncio.gather(*tasks)


def convert_rtabench_data_to_parquet(data_dir: Path) -> None:
    for name, schema in RTA_BENCH_SCHEMAS.items():
        fname = data_dir / f"{name}.csv"
        df = pl.read_csv(fname, has_header=False, schema=schema)
        df.write_parquet(fname.with_suffix(".parquet"))
        fname.unlink()

        _LOGGER.info(f"Converted {fname} to Parquet")


def download_rtabench_data() -> None:
    output_directory = REPO_ROOT / "data/input/rtabench"

    asyncio.run(download_rtabench_data_async(output_directory))

    # convert to Parquet, size goes from 22 GB to 3.8 GB
    # benchmark assumes Parquet inputs for all batch data
    convert_rtabench_data_to_parquet(output_directory)
