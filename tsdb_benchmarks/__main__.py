import logging
import os
from typing import Literal

from fire import Fire  # type: ignore[import-untyped]
from setproctitle import setproctitle

from .dbs import Database
from .dbs.clickhouse import Clickhouse
from .dbs.duckdb import DuckDB
from .dbs.monetdb import MonetDB
from .dbs.timescaledb import TimescaleDB
from .metrics.storage import start_writer_process
from .settings import MAIN_PROCESS_TITLE, DatabaseName, SuiteName, setup_stdout_logging
from .suites.clickbench.config import download_clickbench
from .suites.rtabench.config import download_rtabench_data
from .suites.time_series.config import generate_time_series_datasets

setproctitle(MAIN_PROCESS_TITLE)

DBS: dict[DatabaseName, Database] = {
    "monetdb": MonetDB(),
    "clickhouse": Clickhouse(),
    "timescaledb": TimescaleDB(),
    "duckdb": DuckDB(),
}

setup_stdout_logging()

_LOGGER = logging.getLogger(__name__)


def benchmark(db: DatabaseName, suite: SuiteName, operation: Literal["run", "populate", "both"]) -> None:
    _, queue, result_queue = start_writer_process()
    db_instance = DBS[db]

    db_instance.set_queues(queue, result_queue)

    if operation == "both":
        db_instance.benchmark(suite, "populate")
        db_instance.benchmark(suite, "run")
    else:
        db_instance.benchmark(suite, operation)


def run(db: DatabaseName, command: Literal["start", "stop", "restart", "create"]) -> None:
    db_instance = DBS[db]

    match command:
        case "start" | "stop" | "restart":
            cmd: str = getattr(db_instance, command)
            _LOGGER.info(f"Running command {command}: {cmd}")
            os.system(cmd)

            if command in ("start", "restart"):
                db_instance.wait_until_accessible()

        case _:
            raise ValueError(f"Unknown command: '{command}'")


if __name__ == "__main__":
    Fire(
        {
            "benchmark": benchmark,
            "run": run,
            "download_rtabench": download_rtabench_data,
            "generate_time_series": generate_time_series_datasets,
            "download_clickbench": download_clickbench,
        }
    )
