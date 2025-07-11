import logging
import os
from typing import Literal

from fire import Fire  # type: ignore[import-untyped]

from .generate import download_rtabench_data
from .settings import DatabaseName, setup_stdout_logging

setup_stdout_logging()

_LOGGER = logging.getLogger(__name__)


def benchmark(name: DatabaseName) -> None:
    pass


def run(name: DatabaseName, command: Literal["start", "stop"]) -> None:
    match name:
        case "monetdb":
            from .monetdb import MonetDB

            cmd = getattr(MonetDB(), command)
            _LOGGER.info(f"Running command {command}: {cmd}")
            os.system(cmd)

        case _:
            raise ValueError(f"Unknown database name: '{name}'")


def query(name: DatabaseName, query: str) -> None:
    match name:
        case "monetdb":
            from .monetdb import MonetDB

            _LOGGER.info(MonetDB().fetch(query))

        case _:
            raise ValueError(f"Unknown database name: '{name}'")


def download(dataset: str) -> None:
    match dataset:
        case "rtabench":
            download_rtabench_data()
        case _:
            raise ValueError(f"Unknown dataset: '{dataset}'")


if __name__ == "__main__":
    Fire(
        {
            "benchmark": benchmark,
            "run": run,
            "query": query,
            "download": download,
        }
    )
