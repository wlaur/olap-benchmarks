import logging
import os
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, cast, get_args

import cyclopts
from setproctitle import setproctitle

from .metrics.storage import start_writer_process
from .results import (
    abort_running_runs,
    delete_runs,
    delete_runs_by_status,
    list_revisions,
    list_runs,
    migrate_results,
    query_results,
    rename_database,
)
from .results import config as show_config
from .results import publish as publish_results
from .settings import (
    MAIN_PROCESS_TITLE,
    SETTINGS,
    DatabaseArg,
    DatabaseName,
    Revision,
    SuiteArg,
    SuiteName,
    resolve_dbs,
    resolve_suites,
    setup_stdout_logging,
)

if TYPE_CHECKING:
    from .dbs import Database

setproctitle(MAIN_PROCESS_TITLE)
setup_stdout_logging()

_LOGGER = logging.getLogger(__name__)

_dbs: dict[DatabaseName, "Database"] | None = None

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


app = cyclopts.App(name="olap", help="OLAP database benchmarking tool.")
cast(Any, app).register_install_completion_command()


def _get_dbs() -> dict[DatabaseName, "Database"]:
    global _dbs

    if _dbs is None:
        from .dbs.clickhouse import Clickhouse
        from .dbs.duckdb import DuckDB
        from .dbs.monetdb import MonetDB
        from .dbs.postgres import Postgres
        from .dbs.questdb import QuestDB
        from .dbs.timescaledb import TimescaleDB

        _dbs = {
            "monetdb": MonetDB(),
            "clickhouse": Clickhouse(),
            "timescaledb": TimescaleDB(),
            "duckdb": DuckDB(),
            "questdb": QuestDB(),
            "postgres": Postgres(),
        }

        assert set(_dbs) == set(get_args(DatabaseName))

    return _dbs


def _start_db(db_instance: "Database") -> None:
    cmd = db_instance.start
    if cmd is not None:
        _stop_db(db_instance)
        _LOGGER.info(f"Starting {db_instance.name}: {cmd}")
        os.system(cmd)
        db_instance.wait_until_accessible()


def _stop_db(db_instance: "Database") -> None:
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
    operation: Literal["populate", "select", "mutate", "all"] = "all",
    revision: Revision = "default",
) -> None:
    """Run a benchmark suite against a database. Starts and stops the database container automatically."""
    suite_names = resolve_suites(suite)
    for suite_name in suite_names:
        _check_input_data(suite_name)

    writer = start_writer_process(revision=revision)
    interrupted = False

    try:
        for db_name in resolve_dbs(db):
            for suite_name in suite_names:
                _LOGGER.info(f"Benchmarking {suite_name} on {db_name} ({operation})")
                db_instance = _get_dbs()[db_name]
                db_instance._current_suite = suite_name
                db_instance.set_queues(writer.queue, writer.result_queue)

                _start_db(db_instance)

                try:
                    if operation == "all":
                        db_instance.benchmark(suite_name, "populate")
                        db_instance.benchmark(suite_name, "select")
                        try:
                            db_instance.benchmark(suite_name, "mutate")
                        except NotImplementedError:
                            _LOGGER.info(f"Skipping mutate for {suite_name} on {db_name} (not supported)")
                    else:
                        db_instance.benchmark(suite_name, operation)
                finally:
                    _stop_db(db_instance)
    except KeyboardInterrupt:
        interrupted = True
        raise
    finally:
        writer.close()
        if interrupted:
            aborted_runs = abort_running_runs(revision=revision)
            if aborted_runs:
                _LOGGER.warning(f"Marked {aborted_runs} interrupted run(s) as aborted")


@app.command
def docker(db: DatabaseArg, suite: SuiteArg, command: Literal["start", "stop", "restart"]) -> None:
    """Manually start, stop, or restart a database container."""
    for db_name in resolve_dbs(db):
        for suite_name in resolve_suites(suite):
            db_instance = _get_dbs()[db_name]
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


def _print_runs(rows: list[dict[str, object]]) -> None:
    import json

    if not rows:
        print("No runs found.")
        return

    print(json.dumps(rows, indent=2))


def _confirm_delete(description: str, force: bool = False) -> None:
    if force:
        return

    confirmation = input(f"Type 'yes' to {description}: ").strip()
    if confirmation != "yes":
        raise SystemExit("Aborted.")


def _validate_delete_status(status: str) -> None:
    if status not in {"failed", "aborted"}:
        raise SystemExit("`results delete --status` only supports 'failed' or 'aborted'.")


@results_app.command
def runs(
    status: str | None = None,
    suite: str | None = None,
    db: str | None = None,
    revision: Revision = "default",
) -> None:
    """List benchmark runs, optionally filtered by status, suite, or db."""
    rows = list_runs(revision=revision, status=status, suite=suite, db=db)
    _print_runs(rows)


@results_app.command(name="failed")
def failed_runs(
    suite: str | None = None,
    db: str | None = None,
    revision: Revision = "default",
) -> None:
    """List failed benchmark runs, optionally filtered by suite or db."""
    rows = list_runs(revision=revision, status="failed", suite=suite, db=db)
    _print_runs(rows)


@results_app.command(name="delete")
def delete_cmd(
    run_id: list[int] | None = None,
    status: str | None = None,
    revision: Revision = "default",
    force: bool = False,
) -> None:
    """Delete runs (and their steps/metrics) by run ID or status (e.g. 'failed', 'aborted')."""
    if run_id and status:
        raise SystemExit("Specify either --run-id or --status, not both.")
    if not run_id and not status:
        raise SystemExit("Specify --run-id or --status to select runs to delete.")

    if run_id:
        _confirm_delete(f"delete {len(run_id)} run(s)", force=force)
        count = delete_runs(run_id, revision=revision)
    else:
        assert status is not None
        _validate_delete_status(status)
        _confirm_delete(f"delete all '{status}' run(s)", force=force)
        count = delete_runs_by_status(status, revision=revision)

    print(f"Deleted {count} run(s) and their associated steps and metrics.")


@results_app.command
def query(sql: str, revision: Revision = "default") -> None:
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


@results_app.command
def migrate(revision: Revision = "default") -> None:
    """Apply Alembic migrations through head to a results database revision."""
    db_path = migrate_results(revision=revision)
    print(f"Migrated revision '{revision}' to Alembic head at {db_path}")


@results_app.command(name="rename-db")
def rename_database_cmd(old_name: str, new_name: str, revision: Revision = "default") -> None:
    """Rename a database name stored in a results database revision."""
    renamed_runs = rename_database(old_name, new_name, revision=revision)
    print(f"Renamed database '{old_name}' to '{new_name}' in {renamed_runs} run(s).")


@app.command
def publish(revision: Revision = "default") -> None:
    """Copy a results database to site/public/data for the webpage, with a manifest."""
    output_dir = publish_results(revision=revision)
    print(f"Published revision '{revision}' to {output_dir}")


@app.command
def config(as_json: bool = False) -> None:
    """Show current configuration from .env."""
    show_config(as_json)


if __name__ == "__main__":
    app()
