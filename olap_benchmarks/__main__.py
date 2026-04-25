import logging
import os
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal, cast, get_args

import cyclopts
from setproctitle import setproctitle

from .metrics.storage import start_writer_process
from .results import (
    config as show_config,
)
from .results import (
    delete_runs,
    delete_runs_by_status,
    list_revisions,
    list_runs,
    mark_running_runs_failed,
    migrate_results,
    query_results,
    rename_database,
)
from .results import (
    publish as publish_results,
)
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
        from .dbs.starrocks import StarRocks
        from .dbs.timescaledb import TimescaleDB

        _dbs = {
            "monetdb": MonetDB(),
            "clickhouse": Clickhouse(),
            "timescaledb": TimescaleDB(),
            "duckdb": DuckDB(),
            "questdb": QuestDB(),
            "postgres": Postgres(),
            "starrocks": StarRocks(),
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


def _cleanup_db_files(db_name: DatabaseName, suite_name: SuiteName) -> None:
    """Remove persistent db files and temp files for a (db, suite) combination.

    Docker (incl. OrbStack on macOS) writes data files as root inside the
    container, which surfaces on the host as files the current user cannot
    delete without `sudo`. Run a throwaway alpine container with the host
    paths mounted so `rm -rf` runs as root and clears them.
    """
    db_subdir = SETTINGS.database_directory / db_name / suite_name
    temp_subdir = SETTINGS.temporary_directory / db_name

    rm_paths: list[str] = []
    mounts: list[str] = []

    if db_subdir.exists():
        mounts.append(f"-v {SETTINGS.database_directory.as_posix()}:/dbs")
        rm_paths.append(f"/dbs/{db_name}/{suite_name}")

    if temp_subdir.exists():
        mounts.append(f"-v {SETTINGS.temporary_directory.as_posix()}:/temp")
        rm_paths.append(f"/temp/{db_name}")

    if not rm_paths:
        return

    cmd = f"docker run --rm {' '.join(mounts)} alpine sh -c 'rm -rf {' '.join(rm_paths)}'"
    _LOGGER.info(f"Cleaning up files for {db_name}:{suite_name}: {cmd}")
    rc = os.system(cmd)
    if rc != 0:
        _LOGGER.warning(f"Cleanup for {db_name}:{suite_name} exited with code {rc}")


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
    cleanup: bool = False,
    omit: list[DatabaseName] | None = None,
) -> None:
    """Run a benchmark suite against a database. Starts and stops the database container automatically.

    If `--cleanup` is set, persistent db files (under OLAP_BENCHMARKS_DATABASE_DIRECTORY)
    and temp files (under OLAP_BENCHMARKS_TEMPORARY_DIRECTORY) for the (db, suite)
    combination are deleted after the run. Cleanup runs inside a throwaway docker
    container so it can remove root-owned files written by the db container.

    If `--omit` is set, the listed databases are skipped. Useful with `db=all`
    to run every database except some (e.g. `--omit questdb`).
    """
    suite_names = resolve_suites(suite)
    for suite_name in suite_names:
        _check_input_data(suite_name)

    omitted = set(omit or [])

    writer = start_writer_process(revision=revision)
    interrupted = False

    try:
        for db_name in resolve_dbs(db):
            if db_name in omitted:
                _LOGGER.info(f"Omitting database {db_name}")
                continue
            for suite_name in suite_names:
                _LOGGER.info(f"Benchmarking {suite_name} on {db_name} ({operation})")
                db_instance = _get_dbs()[db_name]
                db_instance._current_suite = suite_name
                db_instance.set_queues(writer.queue, writer.result_queue)

                _start_db(db_instance)

                try:
                    if operation == "all":
                        for suite_operation in db_instance.benchmarks[suite_name].supported_operations:
                            db_instance.benchmark(suite_name, suite_operation)
                    else:
                        db_instance.benchmark(suite_name, operation)
                finally:
                    _stop_db(db_instance)
                    if cleanup:
                        _cleanup_db_files(db_name, suite_name)
    except KeyboardInterrupt:
        interrupted = True
        raise
    finally:
        writer.close()
        if interrupted:
            failed_runs = mark_running_runs_failed(revision=revision)
            if failed_runs:
                _LOGGER.warning(f"Marked {failed_runs} interrupted run(s) as failed")


@app.command(name="benchmark-all")
def benchmark_all(
    revision: Revision = "default",
    cleanup: bool = False,
    omit: list[DatabaseName] | None = None,
) -> None:
    """Run every supported operation of every suite against every database.

    Equivalent to `olap benchmark all all` (with `operation=all`) and `cleanup`
    forwarded. With `--cleanup`, persistent db files and temp files for each
    (db, suite) combination are deleted after the run, so each combo starts
    from a fresh state. Default is no cleanup.

    Use `--omit <db>` (repeatable) to skip specific databases, e.g.
    `olap benchmark-all --revision release --omit questdb`.
    """
    benchmark(db="all", suite="all", operation="all", revision=revision, cleanup=cleanup, omit=omit)


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
    if status not in {"failed", "orphaned"}:
        raise SystemExit("`results delete --status` only supports 'failed' or 'orphaned'.")


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
    """Delete runs (and their steps/metrics) by run ID or status (e.g. 'failed', 'orphaned')."""
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
        description = "delete all failed and orphaned run(s)" if status == "failed" else "delete all orphaned run(s)"
        _confirm_delete(description, force=force)
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
