import json
import logging
from typing import TYPE_CHECKING, Any, Literal, cast

import cyclopts
from setproctitle import setproctitle

from .dbs import get_databases
from .metrics.storage import start_writer_process
from .operation_runner import run_operation
from .results import (
    PUBLISHED_DATABASE_RELEASE_TAG,
    compact_results,
    delete_runs,
    delete_runs_by_status,
    list_revisions,
    list_runs,
    mark_running_runs_failed,
    migrate_results,
    query_results,
    rename_database,
    upload_published_database,
)
from .results import (
    config as show_config,
)
from .results import (
    publish as publish_results,
)
from .results.resource_usage import (
    describe_run_resource_usage,
    load_run_resource_usage,
)
from .results.validation import (
    AnswerHashValidationError,
    RowCountValidationError,
    assert_latest_query_answer_hashes,
    assert_latest_query_row_counts,
    format_answer_hash_mismatches,
    format_row_count_mismatches,
    validate_latest_query_answer_hashes,
    validate_latest_query_row_counts,
)
from .settings import (
    MAIN_PROCESS_TITLE,
    ROW_STORE_DATABASES,
    ROW_STORE_OPTIONAL_SUITE_NAMES,
    SETTINGS,
    DatabaseArg,
    DatabaseName,
    Operation,
    Revision,
    SuiteArg,
    SuiteName,
    format_suite_data_directory_name,
    resolve_dbs,
    resolve_suite_scale_factors,
    resolve_suites,
    setup_stdout_logging,
)
from .suites import ManualPreparationRequired, get_suite_preparer
from .utils import run_shell

if TYPE_CHECKING:
    from .dbs import Database

setproctitle(MAIN_PROCESS_TITLE)
setup_stdout_logging()

_LOGGER = logging.getLogger(__name__)

app = cyclopts.App(name="olap", help="OLAP database benchmarking tool.")
# the method itself is typed but takes **kwargs, which strict pyright rejects
cast(Any, app).register_install_completion_command()

results_app = cyclopts.App(name="results", help="Inspect and manage the results database.")
app.command(results_app)


def _start_db(db_instance: "Database") -> None:
    commands = db_instance.start_commands
    db_instance._last_start_command = "\n".join(commands) if commands else None
    if not commands:
        return

    platform_warning = db_instance.container_platform_warning
    if platform_warning is not None:
        _LOGGER.warning(platform_warning)

    _stop_db(db_instance)
    for command in commands:
        _LOGGER.info(f"Starting {db_instance.name}: {command}")
        rc = run_shell(command)
        if rc != 0:
            raise RuntimeError(f"Start command for {db_instance.name} exited with code {rc}: {command}")

    db_instance.wait_until_accessible()


def _stop_db(db_instance: "Database") -> None:
    db_instance.close_connection()
    for command in db_instance.stop_commands:
        _LOGGER.info(f"Stopping {db_instance.name}: {command}")
        run_shell(command)


def _cleanup_db_files(db_name: DatabaseName, suite_name: SuiteName, scale_factor: int) -> None:
    """Remove persistent db files and temp files for a (db, suite) combination.

    Docker (incl. OrbStack on macOS) writes data files as root inside the
    container, which surfaces on the host as files the current user cannot
    delete without `sudo`. Run a throwaway alpine container with the host
    paths mounted so `rm -rf` runs as root and clears them.
    """
    data_directory_name = format_suite_data_directory_name(suite_name, scale_factor)
    db_subdir = SETTINGS.database_directory / db_name / data_directory_name
    temp_subdir = SETTINGS.temporary_directory / db_name

    rm_paths: list[str] = []
    mounts: list[str] = []

    if db_subdir.exists():
        mounts.append(f"-v {SETTINGS.database_directory.as_posix()}:/dbs")
        rm_paths.append(f"/dbs/{db_name}/{data_directory_name}")

    if temp_subdir.exists():
        mounts.append(f"-v {SETTINGS.temporary_directory.as_posix()}:/temp")
        rm_paths.append(f"/temp/{db_name}")

    if not rm_paths:
        return

    cmd = f"docker run --rm {' '.join(mounts)} alpine sh -c 'rm -rf {' '.join(rm_paths)}'"
    _LOGGER.info(f"Cleaning up files for {db_name}:{suite_name}: {cmd}")
    rc = run_shell(cmd)
    if rc != 0:
        _LOGGER.warning(f"Cleanup for {db_name}:{suite_name} exited with code {rc}")


def _check_input_data(suite_name: SuiteName, scale_factor: int) -> None:
    input_dir = SETTINGS.input_data_directory / format_suite_data_directory_name(suite_name, scale_factor)

    if not input_dir.is_dir() or not any(input_dir.iterdir()):
        raise SystemExit(
            f"No input data found for suite '{suite_name}' scale factor {scale_factor} at {input_dir}\n"
            f"Run 'olap prepare {suite_name} --scale-factor {scale_factor}' first."
        )


def _resolve_suite_scale_factor_pairs(suite: SuiteArg, scale_factor: int | None) -> list[tuple[SuiteName, int]]:
    suite_scale_factor_pairs: list[tuple[SuiteName, int]] = []
    for suite_name in resolve_suites(suite):
        for resolved_scale_factor in resolve_suite_scale_factors(
            suite_name,
            scale_factor,
            include_all_supported=suite == "all",
            allow_fixed_default=suite == "all",
        ):
            suite_scale_factor_pairs.append((suite_name, resolved_scale_factor))
    return suite_scale_factor_pairs


def _should_skip_default_benchmark_pair(db_name: DatabaseName, suite_name: SuiteName, suite_arg: SuiteArg) -> bool:
    return suite_arg == "all" and db_name in ROW_STORE_DATABASES and suite_name in ROW_STORE_OPTIONAL_SUITE_NAMES


def _resolve_benchmark_plan(
    db: DatabaseArg,
    suite: SuiteArg,
    scale_factor: int | None,
    omit: list[DatabaseName] | None = None,
) -> list[tuple[DatabaseName, SuiteName, int]]:
    omitted = set(omit or [])
    suite_scale_factor_pairs = _resolve_suite_scale_factor_pairs(suite, scale_factor)
    plan: list[tuple[DatabaseName, SuiteName, int]] = []

    for db_name in resolve_dbs(db):
        if db_name in omitted:
            continue

        for suite_name, resolved_scale_factor in suite_scale_factor_pairs:
            if _should_skip_default_benchmark_pair(db_name, suite_name, suite):
                continue
            plan.append((db_name, suite_name, resolved_scale_factor))

    return plan


def _filter_supported_benchmark_plan(
    plan: list[tuple[DatabaseName, SuiteName, int]],
) -> list[tuple[DatabaseName, SuiteName, int]]:
    databases = get_databases()
    supported_plan: list[tuple[DatabaseName, SuiteName, int]] = []

    for db_name, suite_name, resolved_scale_factor in plan:
        if suite_name not in databases[db_name].benchmarks:
            _LOGGER.info(f"Skipping {suite_name} on {db_name}; suite is not registered for this database")
            continue
        supported_plan.append((db_name, suite_name, resolved_scale_factor))

    return supported_plan


def _unique_suite_scale_factor_pairs(
    plan: list[tuple[DatabaseName, SuiteName, int]],
) -> list[tuple[SuiteName, int]]:
    seen: set[tuple[SuiteName, int]] = set()
    pairs: list[tuple[SuiteName, int]] = []

    for _, suite_name, resolved_scale_factor in plan:
        pair = (suite_name, resolved_scale_factor)
        if pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)

    return pairs


@app.command
def prepare(suite: SuiteArg, scale_factor: int | None = None) -> None:
    """Generate input data files for a benchmark suite (e.g. Parquet files)."""
    for suite_name, resolved_scale_factor in _resolve_suite_scale_factor_pairs(suite, scale_factor):
        _LOGGER.info(f"Preparing data for {suite_name} scale factor {resolved_scale_factor}")
        try:
            get_suite_preparer(suite_name, resolved_scale_factor)()
        except ManualPreparationRequired as exc:
            if suite != "all":
                raise SystemExit(str(exc)) from exc
            _LOGGER.warning(f"Skipping {suite_name}: {exc}")


@app.command
def benchmark(
    db: DatabaseArg,
    suite: SuiteArg,
    operation: Literal["populate", "select", "mutate", "concurrent", "all"] = "all",
    revision: Revision = "default",
    cleanup: bool = False,
    omit: list[DatabaseName] | None = None,
    scale_factor: int | None = None,
) -> None:
    """Run a benchmark suite against a database. Starts and stops the database container automatically.

    `db` and `suite` accept `all`, so `olap benchmark all all` runs every
    supported operation of every suite against every database.

    If `--cleanup` is set, persistent db files (under OLAP_BENCHMARKS_DATABASE_DIRECTORY)
    and temp files (under OLAP_BENCHMARKS_TEMPORARY_DIRECTORY) for the (db, suite)
    combination are deleted after the run. Cleanup runs inside a throwaway docker
    container so it can remove root-owned files written by the db container.

    If `--omit` is set, the listed databases are skipped. Useful with `db=all`
    to run every database except some (e.g. `--omit questdb`).

    `--scale-factor` selects the suite scale factor. With `suite=all`, fixed-size
    suites keep their default scale factor and scalable suites use the requested
    factor; without `--scale-factor`, suites with configured fan-out run every
    configured factor. With `suite=all`, row-store databases skip optional
    TPC-H/TPC-DS runs by default; select those suites explicitly to include them.
    """
    plan = _filter_supported_benchmark_plan(_resolve_benchmark_plan(db, suite, scale_factor, omit))
    suite_scale_factor_pairs = _unique_suite_scale_factor_pairs(plan)

    if not plan:
        raise SystemExit("No benchmark runs selected.")

    for suite_name, resolved_scale_factor in suite_scale_factor_pairs:
        _check_input_data(suite_name, resolved_scale_factor)

    writer = start_writer_process(revision=revision)
    interrupted = False

    try:
        for db_name, suite_name, resolved_scale_factor in plan:
            db_instance = get_databases()[db_name]

            _LOGGER.info(f"Benchmarking {suite_name} scale factor {resolved_scale_factor} on {db_name} ({operation})")
            db_instance._current_suite = suite_name
            db_instance._current_suite_scale_factor = resolved_scale_factor
            db_instance.set_queues(writer.queue, writer.result_queue)
            db_instance._writer_process = getattr(writer, "process", None)

            _start_db(db_instance)

            try:
                operations = (
                    db_instance.benchmarks[suite_name].supported_operations if operation == "all" else (operation,)
                )
                # each operation runs in its own process so that its peak client RSS is its own,
                # not inherited from a preceding operation (see operation_runner)
                for suite_operation in operations:
                    run_operation(
                        db_instance,
                        suite_name,
                        suite_operation,
                        resolved_scale_factor,
                        writer.queue,
                        writer.result_queue,
                    )
            finally:
                _stop_db(db_instance)
                if cleanup:
                    _cleanup_db_files(db_name, suite_name, resolved_scale_factor)
    except KeyboardInterrupt:
        interrupted = True
        raise
    finally:
        writer.close()
        if interrupted:
            failed_runs = mark_running_runs_failed(revision=revision)
            if failed_runs:
                _LOGGER.warning(f"Marked {failed_runs} interrupted run(s) as failed")

    if operation in ("select", "all"):
        try:
            for suite_name, resolved_scale_factor in suite_scale_factor_pairs:
                assert_latest_query_row_counts(
                    revision=revision,
                    system=SETTINGS.system,
                    suite=suite_name,
                    suite_scale_factor=resolved_scale_factor,
                    mark_wrong_results=True,
                )
                assert_latest_query_answer_hashes(
                    revision=revision,
                    system=SETTINGS.system,
                    suite=suite_name,
                    suite_scale_factor=resolved_scale_factor,
                    mark_wrong_results=True,
                )
        except (AnswerHashValidationError, RowCountValidationError) as exc:
            raise SystemExit(str(exc)) from exc


@app.command
def docker(
    db: DatabaseArg,
    suite: SuiteArg,
    command: Literal["start", "stop", "restart"],
    scale_factor: int | None = None,
) -> None:
    """Manually start, stop, or restart a database container."""
    for db_name in resolve_dbs(db):
        for suite_name in resolve_suites(suite):
            for resolved_scale_factor in resolve_suite_scale_factors(
                suite_name,
                scale_factor,
                allow_fixed_default=suite == "all",
            ):
                db_instance = get_databases()[db_name]
                db_instance._current_suite = suite_name
                db_instance._current_suite_scale_factor = resolved_scale_factor

                match command:
                    case "start":
                        _start_db(db_instance)
                    case "stop":
                        _stop_db(db_instance)
                    case "restart":
                        _stop_db(db_instance)
                        _start_db(db_instance)


@app.command
def publish(revision: Revision = "default", merge: bool = False, upload: bool = False) -> None:
    """Copy a results database to site/public/data for the webpage, with a manifest.

    With `--merge`, runs from the revision are merged into the already published
    results.db instead of replacing the whole file: runs matching an existing
    (system, db, db_version, suite, suite_scale_factor, operation, started_at) are replaced, new
    runs are added, and everything else in the published file is kept.

    With `--upload`, the published results.db is uploaded to the `data` GitHub release, which is
    where the site build fetches it from. The database is not committed to the repository.
    """
    output_dir, merge_stats = publish_results(revision=revision, merge=merge)

    if merge_stats is not None:
        print(
            f"Merged revision '{revision}' into {output_dir}: "
            f"{merge_stats.runs_added} run(s) added, {merge_stats.runs_replaced} replaced"
        )
    else:
        print(f"Published revision '{revision}' to {output_dir}")

    if upload:
        upload_published_database()
        print(f"Uploaded results.db to the '{PUBLISHED_DATABASE_RELEASE_TAG}' release")


@app.command
def config(as_json: bool = False) -> None:
    """Show current configuration from .env."""
    show_config(as_json)


def _confirm(description: str, force: bool = False) -> None:
    if force:
        return

    confirmation = input(f"Type 'yes' to {description}: ").strip()
    if confirmation != "yes":
        raise SystemExit("Aborted.")


@results_app.command
def runs(
    status: str | None = None,
    suite: str | None = None,
    db: str | None = None,
    db_driver: str | None = None,
    revision: Revision = "default",
) -> None:
    """List benchmark runs, optionally filtered by status, suite, database, or driver."""
    rows = list_runs(revision=revision, status=status, suite=suite, db=db, db_driver=db_driver)

    if not rows:
        print("No runs found.")
        return

    print(json.dumps(rows, indent=2))


@results_app.command(name="delete")
def delete_cmd(
    run_id: list[int] | None = None,
    status: Literal["failed", "orphaned"] | None = None,
    revision: Revision = "default",
    force: bool = False,
) -> None:
    """Delete runs (and their steps/metrics) by run ID or status.

    `--status failed` deletes failed and orphaned runs, `--status orphaned`
    only runs still marked as running.
    """
    if run_id and status:
        raise SystemExit("Specify either --run-id or --status, not both.")
    if not run_id and not status:
        raise SystemExit("Specify --run-id or --status to select runs to delete.")

    if run_id:
        _confirm(f"delete {len(run_id)} run(s)", force=force)
        count = delete_runs(run_id, revision=revision)
    else:
        assert status is not None
        description = "delete all failed and orphaned run(s)" if status == "failed" else "delete all orphaned run(s)"
        _confirm(description, force=force)
        count = delete_runs_by_status(status, revision=revision)

    print(f"Deleted {count} run(s) and their associated steps and metrics.")


@results_app.command
def query(sql: str, revision: Revision = "default") -> None:
    """Run a SQL query against the results database."""
    query_results(sql, revision=revision)


@results_app.command
def resources(
    revision: Revision = "default",
    system: str | None = None,
    db: DatabaseName | None = None,
    suite: SuiteName | None = None,
    operation: Operation | None = None,
    status: Literal["running", "completed", "failed"] | None = None,
) -> None:
    """Report per-run peak resource usage with server and client memory kept apart.

    `comparable_peak_memory_mb` is the figure to compare across engines: server memory
    for containerised engines and client memory for in-process engines such as DuckDB
    and Polars, whose `server_mem_mb` is always 0 because they have no server. Server
    and client peaks are never summed; they occur at different times.
    """
    rows = load_run_resource_usage(
        revision=revision,
        system=system,
        db=db,
        suite=suite,
        operation=operation,
        status=status,
    )

    if not rows:
        print("No runs found.")
        return

    print(json.dumps([describe_run_resource_usage(row) for row in rows], indent=2))


@results_app.command(name="validate-row-counts")
def validate_row_counts(
    revision: Revision = "default",
    system: str | None = None,
    suite: SuiteName | None = None,
    scale_factor: int | None = None,
) -> None:
    """Validate query row counts across latest completed select runs."""
    mismatches = validate_latest_query_row_counts(
        revision=revision,
        system=system,
        suite=suite,
        suite_scale_factor=scale_factor,
    )
    if mismatches:
        raise SystemExit(format_row_count_mismatches(mismatches))
    print(format_row_count_mismatches(mismatches))


@results_app.command(name="validate-answer-hashes")
def validate_answer_hashes(
    revision: Revision = "default",
    system: str | None = None,
    suite: SuiteName | None = None,
    scale_factor: int | None = None,
) -> None:
    """Validate query answer hashes across latest completed select runs."""
    mismatches = validate_latest_query_answer_hashes(
        revision=revision,
        system=system,
        suite=suite,
        suite_scale_factor=scale_factor,
    )
    if mismatches:
        raise SystemExit(format_answer_hash_mismatches(mismatches))
    print(format_answer_hash_mismatches(mismatches))


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
def compact(revision: Revision = "default") -> None:
    """Rewrite a results database into a fresh file to reclaim unused space."""
    db_path, size_before, size_after = compact_results(revision=revision)
    print(f"Compacted {db_path}: {size_before / 1e6:.1f} MB -> {size_after / 1e6:.1f} MB")


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


if __name__ == "__main__":
    app()
