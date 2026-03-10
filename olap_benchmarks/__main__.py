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
from .results import delete_runs, delete_runs_by_status, export_site_data, list_revisions, list_runs, query_results
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


app = cyclopts.App(name="olap-benchmarks", help="OLAP database benchmarking tool.")


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
def prepare(suite: SuiteArg) -> None:
    """Generate input data files for a benchmark suite (e.g. Parquet files)."""
    for suite_name in resolve_suites(suite):
        _LOGGER.info(f"Preparing data for {suite_name}")
        _get_suite_preparer(suite_name)()


def _check_input_data(suite_name: SuiteName) -> None:
    input_dir = SETTINGS.input_data_directory / suite_name

    if not input_dir.is_dir() or not any(input_dir.iterdir()):
        raise SystemExit(
            f"No input data found for suite '{suite_name}' at {input_dir}\nRun 'olap prepare {suite_name}' first."
        )


@app.command
def benchmark(
    db: DatabaseArg,
    suite: SuiteArg,
    operation: Literal["run", "populate", "both"] = "both",
    revision: str = "default",
) -> None:
    """Run a benchmark suite against a database. Starts and stops the database container automatically."""
    suite_names = resolve_suites(suite)
    for suite_name in suite_names:
        _check_input_data(suite_name)

    writer = start_writer_process(revision=revision)

    try:
        for db_name in resolve_dbs(db):
            for suite_name in suite_names:
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
def docker(db: DatabaseArg, suite: SuiteArg, command: Literal["start", "stop", "restart"]) -> None:
    """Manually start, stop, or restart a database container."""
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


results_app = cyclopts.App(name="results", help="Inspect and manage the results database.")
app.command(results_app)


@results_app.command
def runs(
    status: str | None = None,
    suite: str | None = None,
    db: str | None = None,
    revision: str = "default",
) -> None:
    """List benchmark runs, optionally filtered by status, suite, or db."""
    import json

    rows = list_runs(revision=revision, status=status, suite=suite, db=db)
    if not rows:
        print("No runs found.")
        return

    print(json.dumps(rows, indent=2))


@results_app.command(name="delete")
def delete_cmd(
    run_id: list[int] | None = None,
    status: str | None = None,
    revision: str = "default",
) -> None:
    """Delete runs (and their steps/metrics) by run ID or status (e.g. 'failed', 'aborted')."""
    if run_id and status:
        raise SystemExit("Specify either --run-id or --status, not both.")
    if not run_id and not status:
        raise SystemExit("Specify --run-id or --status to select runs to delete.")

    if run_id:
        count = delete_runs(run_id, revision=revision)
    else:
        assert status is not None
        count = delete_runs_by_status(status, revision=revision)

    print(f"Deleted {count} run(s) and their associated steps and metrics.")


@results_app.command
def query(sql: str, revision: str = "default") -> None:
    """Run a SQL query against the results database."""
    query_results(sql, revision=revision)


@results_app.command
def revisions() -> None:
    """List available result database revisions."""
    names = list_revisions()
    if not names:
        print(f"No result databases in {SETTINGS.results_directory}")
        return
    for name in names:
        print(name)


@app.command
def config(as_json: bool = False) -> None:
    """Show current configuration from .env."""
    show_config(as_json)


@app.command
def export(
    output_directory: str | None = None,
    source_database: str | None = None,
    revision: str = "default",
) -> None:
    """Export results to JSON for the site."""
    export_site_data(output_directory, source_database, revision=revision)


if __name__ == "__main__":
    app()
