import logging
import os
from typing import Literal

from fire import Fire  # type: ignore[import-untyped]

from .dbs import Database
from .dbs.clickhouse import Clickhouse
from .dbs.duckdb import DuckDB
from .dbs.monetdb import MonetDB
from .dbs.timescaledb import TimescaleDB
from .metrics.storage import start_writer_process
from .settings import DatabaseName, SuiteName, setup_stdout_logging
from .suites.rtabench.generate import download_rtabench_data
from .suites.time_series.generate import generate_time_series_datasets

DBS: dict[DatabaseName, Database] = {
    "monetdb": MonetDB(),
    "clickhouse": Clickhouse(),
    "timescaledb": TimescaleDB(),
    "duckdb": DuckDB(),
}

setup_stdout_logging()

_LOGGER = logging.getLogger(__name__)


def benchmark(name: DatabaseName, suite: SuiteName, operation: Literal["run", "populate"]) -> None:
    queue, result_queue = start_writer_process()
    db = DBS[name]

    db.set_queues(queue, result_queue)

    db.benchmark(suite, operation)


def run(name: DatabaseName, command: Literal["start", "stop", "restart", "setup", "create"]) -> None:
    db = DBS[name]

    match command:
        case "create":
            run(name, "start")
            run(name, "setup")

        case "start" | "stop" | "restart":
            cmd: str = getattr(db, command)
            _LOGGER.info(f"Running command {command}: {cmd}")
            os.system(cmd)

        case "setup":
            db.setup()

        case _:
            raise ValueError(f"Unknown command: '{command}'")


def generate(suite: str) -> None:
    match suite:
        case "rtabench":
            download_rtabench_data()
        case "time_series":
            generate_time_series_datasets()
        case _:
            raise ValueError(f"Unknown suite: '{suite}'")


if __name__ == "__main__":
    Fire(
        {
            "benchmark": benchmark,
            "run": run,
            "generate": generate,
        }
    )
