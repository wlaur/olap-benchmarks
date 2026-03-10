import logging
import os
from collections.abc import Callable
from typing import Literal, get_args

import cyclopts
from setproctitle import setproctitle

from .dbs import Database
from .dbs.clickhouse import Clickhouse
from .dbs.duckdb import DuckDB
from .dbs.monetdb import MonetDB
from .dbs.postgres import Postgres
from .dbs.questdb import QuestDB
from .dbs.timescaledb import TimescaleDB
from .metrics.storage import start_writer_process
from .results import config as show_config
from .results import export_site_data, list_revisions, query_results
from .settings import (
    MAIN_PROCESS_TITLE,
    DatabaseArg,
    DatabaseName,
    SuiteArg,
    SuiteName,
    resolve_dbs,
    resolve_suites,
    setup_stdout_logging,
)

setproctitle(MAIN_PROCESS_TITLE)
setup_stdout_logging()

_LOGGER = logging.getLogger(__name__)

DBS: dict[DatabaseName, Database] = {
    "monetdb": MonetDB(),
    "clickhouse": Clickhouse(),
    "timescaledb": TimescaleDB(),
    "duckdb": DuckDB(),
    "questdb": QuestDB(),
    "postgres": Postgres(),
}

assert set(DBS) == set(get_args(DatabaseName))

SUITE_PREPARERS: dict[SuiteName, Callable[[], None]] = {}


def _get_suite_preparer(suite: SuiteName) -> Callable[[], None]:
    if suite not in SUITE_PREPARERS:
        from .suites.clickbench.config import prepare_data as clickbench_prepare
        from .suites.kaggle_airbnb.config import prepare_data as kaggle_prepare
        from .suites.rtabench.config import prepare_data as rtabench_prepare
        from .suites.time_series.config import prepare_data as timeseries_prepare

        SUITE_PREPARERS["rtabench"] = rtabench_prepare
        SUITE_PREPARERS["clickbench"] = clickbench_prepare
        SUITE_PREPARERS["time_series"] = timeseries_prepare
        SUITE_PREPARERS["kaggle_airbnb"] = kaggle_prepare

    return SUITE_PREPARERS[suite]


app = cyclopts.App(name="olap-benchmarks")


@app.command
def benchmark(
    db: DatabaseArg,
    suite: SuiteArg,
    operation: Literal["run", "populate", "both"] = "both",
    revision: str = "default",
) -> None:
    writer = start_writer_process(revision=revision)

    try:
        for db_name in resolve_dbs(db):
            for suite_name in resolve_suites(suite):
                _LOGGER.info(f"Benchmarking {suite_name} on {db_name} ({operation})")
                db_instance = DBS[db_name]
                db_instance.set_queues(writer.queue, writer.result_queue)

                if operation == "both":
                    db_instance.benchmark(suite_name, "populate")
                    db_instance.benchmark(suite_name, "run")
                else:
                    db_instance.benchmark(suite_name, operation)
    finally:
        writer.close()


@app.command
def data(suite: SuiteArg) -> None:
    for suite_name in resolve_suites(suite):
        _LOGGER.info(f"Preparing data for {suite_name}")
        _get_suite_preparer(suite_name)()


@app.command
def docker(db: DatabaseArg, command: Literal["start", "stop", "restart"]) -> None:
    for db_name in resolve_dbs(db):
        db_instance = DBS[db_name]
        cmd: str = getattr(db_instance, command)
        _LOGGER.info(f"Running {command} for {db_name}: {cmd}")

        if cmd:
            os.system(cmd)

        if command in ("start", "restart"):
            db_instance.wait_until_accessible()


@app.command
def config(as_json: bool = False) -> None:
    show_config(as_json)


@app.command
def revisions() -> None:
    for name in list_revisions():
        print(name)


@app.command
def query(sql: str, revision: str = "default") -> None:
    query_results(sql, revision=revision)


@app.command
def export(
    output_directory: str | None = None,
    source_database: str | None = None,
    revision: str = "default",
) -> None:
    export_site_data(output_directory, source_database, revision=revision)


if __name__ == "__main__":
    app()
