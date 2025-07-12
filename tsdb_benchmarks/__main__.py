import logging
import os
from typing import Literal

from fire import Fire  # type: ignore[import-untyped]

from .settings import DatabaseName, setup_stdout_logging
from .suites.rtabench.generate import download_rtabench_data
from .suites.time_series.generate import generate_datasets

setup_stdout_logging()

_LOGGER = logging.getLogger(__name__)


def benchmark(name: DatabaseName) -> None:
    pass


def run(name: DatabaseName, command: Literal["start", "stop"]) -> None:
    match name:
        case "monetdb":
            from .dbs.monetdb import MonetDB

            cmd = getattr(MonetDB(), command)
            _LOGGER.info(f"Running command {command}: {cmd}")
            os.system(cmd)

        case "clickhouse":
            from .dbs.clickhouse import Clickhouse

            cmd = getattr(Clickhouse(), command)
            _LOGGER.info(f"Running command {command}: {cmd}")
            os.system(cmd)

        case _:
            raise ValueError(f"Unknown database name: '{name}'")


def generate(dataset: str) -> None:
    match dataset:
        case "rtabench":
            download_rtabench_data()
        case "time_series":
            generate_datasets()
        case _:
            raise ValueError(f"Unknown dataset: '{dataset}'")


if __name__ == "__main__":
    Fire(
        {
            "benchmark": benchmark,
            "run": run,
            "generate": generate,
        }
    )
