import logging
import os
from typing import Any, Literal, cast, get_args

import fire
from setproctitle import setproctitle

from .dbs import Database
from .dbs.clickhouse import Clickhouse
from .dbs.duckdb import DuckDB
from .dbs.monetdb import MonetDB
from .dbs.postgres import Postgres
from .dbs.questdb import QuestDB
from .dbs.timescaledb import TimescaleDB
from .metrics.storage import start_writer_process
from .results import config, export_site_data
from .settings import MAIN_PROCESS_TITLE, DatabaseName, SuiteName, setup_stdout_logging
from .suites.clickbench.config import download_clickbench
from .suites.kaggle_airbnb.config import convert_kaggle_airbnb_data_to_parquet
from .suites.rtabench.config import download_rtabench_data
from .suites.time_series.config import generate_time_series_datasets

setproctitle(MAIN_PROCESS_TITLE)

DBS: dict[DatabaseName, Database] = {
    "monetdb": MonetDB(),
    "clickhouse": Clickhouse(),
    "timescaledb": TimescaleDB(),
    "duckdb": DuckDB(),
    "questdb": QuestDB(),
    "postgres": Postgres(),
}

assert set(DBS) == set(get_args(DatabaseName))

setup_stdout_logging()

_LOGGER = logging.getLogger(__name__)


def benchmark(db: DatabaseName, suite: SuiteName, operation: Literal["run", "populate", "both"]) -> None:
    writer = start_writer_process()

    try:
        db_instance = DBS[db]
        db_instance.set_queues(writer.queue, writer.result_queue)

        if operation == "both":
            db_instance.benchmark(suite, "populate")
            db_instance.benchmark(suite, "run")
        else:
            db_instance.benchmark(suite, operation)
    finally:
        writer.close()


def run(db: DatabaseName, command: Literal["start", "stop", "restart"]) -> None:
    db_instance = DBS[db]

    match command:
        case "start" | "stop" | "restart":
            cmd: str = getattr(db_instance, command)
            _LOGGER.info(f"Running command {command}: {cmd}")

            if cmd:
                os.system(cmd)

            if command in ("start", "restart"):
                db_instance.wait_until_accessible()

        case _:
            raise ValueError(f"Unknown command: '{command}'")


if __name__ == "__main__":
    cast(Any, fire).Fire(
        {
            "benchmark": benchmark,
            "run": run,
            "config": config,
            "export_site_data": export_site_data,
            "download_rtabench": download_rtabench_data,
            "generate_time_series": generate_time_series_datasets,
            "download_clickbench": download_clickbench,
            "convert_kaggle_airbnb": convert_kaggle_airbnb_data_to_parquet,
        }
    )
