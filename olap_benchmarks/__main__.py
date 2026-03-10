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
    SETTINGS,
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


def _start_db(db_instance: Database) -> None:
    cmd = db_instance.start
    if cmd is not None:
        _LOGGER.info(f"Starting {db_instance.name}: {cmd}")
        os.system(cmd)
        db_instance.wait_until_accessible()


def _stop_db(db_instance: Database) -> None:
    cmd = db_instance.stop
    if cmd is not None:
        _LOGGER.info(f"Stopping {db_instance.name}: {cmd}")
        os.system(cmd)


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
                db_instance._current_suite = suite_name
                db_instance.set_queues(writer.queue, writer.result_queue)

                _start_db(db_instance)

                try:
                    if operation == "both":
                        db_instance.benchmark(suite_name, "populate")
                        db_instance.benchmark(suite_name, "run")
                    else:
                        db_instance.benchmark(suite_name, operation)
                finally:
                    _stop_db(db_instance)
    finally:
        writer.close()


@app.command
def data(suite: SuiteArg) -> None:
    for suite_name in resolve_suites(suite):
        _LOGGER.info(f"Preparing data for {suite_name}")
        _get_suite_preparer(suite_name)()


@app.command
def docker(db: DatabaseArg, suite: SuiteArg, command: Literal["start", "stop", "restart"]) -> None:
    """Manually manage database containers (for debugging)."""
    for db_name in resolve_dbs(db):
        for suite_name in resolve_suites(suite):
            db_instance = DBS[db_name]
            db_instance._current_suite = suite_name

            match command:
                case "start":
                    _start_db(db_instance)
                case "stop":
                    _stop_db(db_instance)
                case "restart":
                    _stop_db(db_instance)
                    _start_db(db_instance)


@app.command
def config(as_json: bool = False) -> None:
    show_config(as_json)


@app.command
def revisions() -> None:
    names = list_revisions()
    if not names:
        print(f"No result databases in {SETTINGS.results_directory}")
        return
    for name in names:
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
