import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

import cyclopts
from setproctitle import setproctitle

from .dbs import get_databases
from .inputs import drop_page_cache, fingerprint_input_directory
from .limits import check_disk_space
from .matrix import MatrixCell, current_drivers, current_pins, expand_matrix, published_databases
from .metrics.storage import WriterProcessHandle, start_writer_process
from .operation_runner import run_operation
from .results import (
    PUBLISHED_DATABASE_RELEASE_TAG,
    SHARED_REVISION_RELEASE_TAG,
    compact_results,
    delete_runs,
    delete_runs_by_status,
    fetch_results_revision,
    find_superseded_runs,
    get_results_db_path,
    list_revisions,
    list_runs,
    mark_running_runs_failed,
    migrate_results,
    query_results,
    rename_database,
    trigger_pages_deploy,
    upload_published_database,
    upload_results_revision,
)
from .results import (
    config as show_config,
)
from .results import (
    publish as publish_results,
)
from .results.cells import CellStatus, format_matrix_status, format_table, load_runs, matrix_status
from .results.publishing import PublishValidationError
from .results.resource_usage import (
    describe_run_resource_usage,
    load_run_resource_usage,
)
from .results.validation import ValidationReport, clear_wrong_result_marks, validate_results
from .run_metadata import build_repository_provenance
from .settings import (
    MAIN_PROCESS_TITLE,
    ROW_STORE_DATABASES,
    SUITE_SPECS,
    ContainerPlatform,
    DatabaseArg,
    DatabaseName,
    Operation,
    Revision,
    SuiteArg,
    SuiteName,
    format_suite_data_directory_name,
    get_settings,
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

matrix_app = cyclopts.App(name="matrix", help="Inspect and run the declared benchmark matrix.")
app.command(matrix_app)


def _start_db(db_instance: "Database") -> None:
    check_disk_space()
    commands = db_instance.start_commands
    db_instance._last_start_command = "\n".join(commands) if commands else None
    if not commands:
        return

    platform_warning = db_instance.container_platform_warning
    if platform_warning is not None:
        _LOGGER.warning(platform_warning)

    # pulls any image without a registry digest before a container starts on a locally built one
    db_instance.resolve_container_images()
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
    db_subdir = get_settings().database_directory / db_name / data_directory_name
    temp_subdir = get_settings().temporary_directory / db_name

    rm_paths: list[str] = []
    mounts: list[str] = []

    if db_subdir.exists():
        mounts.append(f"-v {get_settings().database_directory.as_posix()}:/dbs")
        rm_paths.append(f"/dbs/{db_name}/{data_directory_name}")

    if temp_subdir.exists():
        mounts.append(f"-v {get_settings().temporary_directory.as_posix()}:/temp")
        rm_paths.append(f"/temp/{db_name}")

    if not rm_paths:
        return

    cmd = f"docker run --rm {' '.join(mounts)} alpine sh -c 'rm -rf {' '.join(rm_paths)}'"
    _LOGGER.info(f"Cleaning up files for {db_name}:{suite_name}: {cmd}")
    rc = run_shell(cmd)
    if rc != 0:
        _LOGGER.warning(f"Cleanup for {db_name}:{suite_name} exited with code {rc}")


def _check_and_hash_input_data(suite_name: SuiteName, scale_factor: int) -> None:
    input_dir = get_settings().input_data_directory / format_suite_data_directory_name(suite_name, scale_factor)

    if not input_dir.is_dir() or not any(input_dir.iterdir()):
        raise SystemExit(
            f"No input data found for suite '{suite_name}' scale factor {scale_factor} at {input_dir}\n"
            f"Run 'olap prepare {suite_name} --scale-factor {scale_factor}' first."
        )

    # hashed before any container starts; each operation's run metadata reads the cached hashes
    _LOGGER.info(f"Fingerprinting inputs of {suite_name} scale factor {scale_factor}")
    fingerprint_input_directory(input_dir)


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
    return suite_arg == "all" and db_name in ROW_STORE_DATABASES and SUITE_SPECS[suite_name].row_store_optional


def _resolve_benchmark_plan(
    db: DatabaseArg,
    suite: SuiteArg,
    scale_factor: int | None,
    omit: list[DatabaseName] | None = None,
    include_experimental: bool = False,
) -> list[tuple[DatabaseName, SuiteName, int]]:
    omitted = set(omit or [])
    published = set(published_databases())
    suite_scale_factor_pairs = _resolve_suite_scale_factor_pairs(suite, scale_factor)
    plan: list[tuple[DatabaseName, SuiteName, int]] = []

    for db_name in resolve_dbs(db):
        if db_name in omitted:
            continue
        if db == "all" and not include_experimental and db_name not in published:
            _LOGGER.info(f"Skipping experimental {db_name}; name it or pass --include-experimental")
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


@dataclass(frozen=True, slots=True)
class BenchmarkGroup:
    db: DatabaseName
    suite: SuiteName
    scale_factor: int
    operations: tuple[Operation, ...]

    @property
    def label(self) -> str:
        return f"{self.db}:{self.suite}:sf{self.scale_factor} ({', '.join(self.operations)})"


@dataclass(frozen=True, slots=True)
class GroupOutcome:
    group: BenchmarkGroup
    error: str | None


def _run_benchmark_group(
    group: BenchmarkGroup, writer: WriterProcessHandle, cleanup: bool, drop_caches: bool = False
) -> None:
    db_instance = get_databases()[group.db]
    db_instance._page_cache_dropped = drop_page_cache() if drop_caches else False

    _LOGGER.info(f"Benchmarking {group.label}")
    db_instance._current_suite = group.suite
    db_instance._current_suite_scale_factor = group.scale_factor
    db_instance.set_queues(writer.queue, writer.result_queue)
    db_instance._writer_process = writer.process

    try:
        _start_db(db_instance)
        # readiness probing opens a connection here, but each operation runs in its own
        # process and opens its own. Leaving this one open would hold a second server session
        # for the whole run, which for MonetDB keeps pending changes alive and forces large
        # ingests onto the ordinary write-ahead-log path.
        db_instance.close_connection()

        # each operation runs in its own process so that its peak client RSS is its own,
        # not inherited from a preceding operation (see operation_runner)
        for suite_operation in group.operations:
            run_operation(
                db_instance,
                group.suite,
                suite_operation,
                group.scale_factor,
                writer.queue,
                writer.result_queue,
            )
    finally:
        _stop_db(db_instance)
        if cleanup:
            _cleanup_db_files(group.db, group.suite, group.scale_factor)


def run_benchmark_groups(
    groups: list[BenchmarkGroup],
    *,
    revision: Revision,
    cleanup: bool,
    keep_going: bool,
    drop_caches: bool = False,
) -> list[GroupOutcome]:
    inputs: dict[tuple[SuiteName, int], None] = {(group.suite, group.scale_factor): None for group in groups}
    for suite_name, scale_factor in inputs:
        _check_and_hash_input_data(suite_name, scale_factor)
    # fails before any container starts when the harness is not running from a git checkout
    build_repository_provenance()

    writer = start_writer_process(revision=revision)
    interrupted = False
    outcomes: list[GroupOutcome] = []

    try:
        for group in groups:
            try:
                _run_benchmark_group(group, writer, cleanup, drop_caches)
            except Exception as exc:
                _LOGGER.exception(f"{group.label} failed")
                outcomes.append(GroupOutcome(group, f"{type(exc).__name__}: {exc}"))
                if not keep_going:
                    break
            else:
                outcomes.append(GroupOutcome(group, None))
    except KeyboardInterrupt:
        interrupted = True
        raise
    finally:
        writer.close()
        if interrupted:
            failed_runs = mark_running_runs_failed(revision=revision)
            if failed_runs:
                _LOGGER.warning(f"Marked {failed_runs} interrupted run(s) as failed")
        elif any(outcome.error is not None for outcome in outcomes):
            error = next(outcome.error for outcome in reversed(outcomes) if outcome.error is not None)
            error_type, _, error_message = error.partition(": ")
            mark_running_runs_failed(revision=revision, error_type=error_type, error_message=error_message)

    return outcomes


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
    drop_caches: bool = False,
    include_experimental: bool = False,
) -> None:
    """Run a benchmark suite against a database. Starts and stops the database container automatically.

    `db` and `suite` accept `all`, so `olap benchmark all all` runs every
    supported operation of every suite against every published database. Experimental
    (unpublished) engines run only when named or with `--include-experimental`.

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

    Inputs are hashed before any container starts. `--drop-caches` then drops the operating-system
    page cache before each container starts (Linux only, needs root; logged and skipped otherwise),
    so populate reads its inputs cold.
    """
    plan = _filter_supported_benchmark_plan(
        _resolve_benchmark_plan(db, suite, scale_factor, omit, include_experimental)
    )

    if not plan:
        raise SystemExit("No benchmark runs selected.")

    databases = get_databases()
    groups = [
        BenchmarkGroup(
            db_name,
            suite_name,
            resolved_scale_factor,
            databases[db_name].benchmarks[suite_name].supported_operations if operation == "all" else (operation,),
        )
        for db_name, suite_name, resolved_scale_factor in plan
    ]
    outcomes = run_benchmark_groups(
        groups, revision=revision, cleanup=cleanup, keep_going=False, drop_caches=drop_caches
    )
    failed = [outcome for outcome in outcomes if outcome.error is not None]
    if failed:
        raise SystemExit(f"{failed[0].group.label} failed: {failed[0].error}")

    if operation in ("select", "all"):
        pins = current_pins()
        for suite_name, resolved_scale_factor in _unique_suite_scale_factor_pairs(plan):
            report = validate_results(
                revision=revision,
                pins=pins,
                system=get_settings().system,
                suite=suite_name,
                suite_scale_factor=resolved_scale_factor,
            )
            if not report.ok:
                _LOGGER.warning(
                    f"{report.format()}\nRun 'olap results validate --mark' to record wrong results once every "
                    "engine of the campaign has run."
                )


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
def publish(
    revision: Revision = "default",
    merge: bool = False,
    upload: bool = False,
    campaign: bool = False,
    report_url: str | None = None,
) -> None:
    """Publish a results revision to site/public/data for the webpage, with a manifest.

    Only the latest run of each cell of the declared matrix (`olap matrix status`)
    is published. The set must be complete and coherent: every cell completed with answer hashes,
    no wrong results after validation, current version pins only, recorded repository commits with
    clean trees, and one system snapshot. A full replacement requires one commit. Otherwise nothing
    is written and the offending cells are listed. The published file is compacted, and its SHA-256
    and size go into manifest.json.

    With `--merge`, runs from the revision are merged into the already published
    results.db instead of replacing the whole file: runs of a cell (system, db, db_version,
    db_driver, suite, suite_scale_factor, operation) the revision contains replace the published
    runs of that cell, and everything else in the published file is kept. Different commits are
    accepted for a merge, with each run's original provenance preserved.

    With `--campaign --report-url URL`, replace the dataset with all registered engine/suite/scale
    combinations. Every combination must have a nonempty completed select cohort with one warmup
    and three measured iterations. Explicitly skipped queries and failed or missing non-select
    operations are allowed and disclosed. Measurements may span clean commits; their original
    provenance is retained. Invalid diagnostics are excluded. The linked report must explain
    methodology, corrections and resource limits. This mode cannot be combined with `--merge`.

    With `--upload`, the published results.db is uploaded to the `data` GitHub release, which is
    where the site build fetches it from, and the Pages workflow is triggered so the site serves
    the new file. The database is not committed to the repository.
    """
    try:
        output_dir, merge_stats = publish_results(
            revision=revision, merge=merge, campaign=campaign, report_url=report_url
        )
    except (PublishValidationError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

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
        trigger_pages_deploy()
        print("Triggered the Pages deployment")


@app.command
def versions(check: bool = False, outdated: bool = False) -> None:
    """Show the engine version pins from versions.toml with their images.

    Without flags, lists each engine's pinned version and images, whether the image is used on this
    host's platform, whether it is present locally and its local registry digest. `--check` pulls
    every image used on this host (so it has a registry digest), verifies the other platforms' images
    exist in the registry, and exits non-zero if any is missing or does not match a pinned digest.
    `--outdated` compares each pin with the newest upstream release (Docker Hub tags, PyPI).
    """
    import httpx

    from .images import DockerImageStore
    from .version_report import (
        CHECK_HEADER,
        INVENTORY_HEADER,
        OUTDATED_HEADER,
        check_rows,
        inventory_rows,
        outdated_rows,
    )
    from .versions import load_versions

    pins = load_versions()
    failed = False

    if outdated:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        with httpx.Client(timeout=20, follow_redirects=True) as client:
            print(format_table(OUTDATED_HEADER, outdated_rows(pins, client)))

    if check or not outdated:
        platforms: dict[DatabaseName, ContainerPlatform | None] = {
            database.name: database.container_platform for database in get_databases().values()
        }
        store = DockerImageStore()
        if outdated:
            print()
        if check:
            rows, ok = check_rows(pins, platforms, store)
            print(format_table(CHECK_HEADER, rows))
            failed = not ok
        else:
            print(format_table(INVENTORY_HEADER, inventory_rows(pins, platforms, store)))

    if failed:
        raise SystemExit(1)


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
    (cgroup anon + shmem, page cache excluded) for containerised engines and client
    memory for in-process engines such as DuckDB and Polars, which are reported as
    `resource_scope: client_only` with null server figures. Server and client peaks
    are never summed; they occur at different times.

    Disk is reported as peak and final for the whole store, its data and its WAL.

    `mean_combined_cpu_percent` and `cpu_core_seconds` weight each sample by the gap to
    the next one, so they stay comparable across runs whose sampling rates differ.
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


@results_app.command
def validate(
    revision: Revision = "default",
    system: str | None = None,
    suite: SuiteName | None = None,
    scale_factor: int | None = None,
    mark: bool = False,
    clear: bool = False,
) -> None:
    """Validate answers of the latest completed select run per cell of the current version pins.

    Compares row counts and answer hashes across engines and flags queries whose answer hash
    differs between iterations of one run. Runs of versions other than the current pins are
    ignored.

    `--mark` clears every earlier wrong-result mark in scope and then marks the consensus
    outliers of this comparison, so repeating it gives the same result and a corrected rerun
    heals its old marks. `--clear` only removes the marks. Exits non-zero on any finding.
    """
    if mark and clear:
        raise SystemExit("Specify either --mark or --clear, not both.")

    if clear:
        cleared = clear_wrong_result_marks(
            revision=revision, system=system, suite=suite, suite_scale_factor=scale_factor
        )
        print(f"Cleared {cleared} wrong-result mark(s).")
        return

    report = validate_results(
        revision=revision,
        pins=current_pins(),
        system=system,
        suite=suite,
        suite_scale_factor=scale_factor,
        mark=mark,
    )
    _exit_on_findings(report)


def _exit_on_findings(report: ValidationReport) -> None:
    if not report.ok:
        raise SystemExit(report.format())
    print(report.format())


@results_app.command
def prune(
    superseded: bool = False,
    stale_versions: bool = False,
    revision: Revision = "default",
    dry_run: bool = False,
    force: bool = False,
) -> None:
    """Delete runs that no longer describe a current benchmark cell.

    `--superseded` keeps the latest completed run per cell (system, db, db_version, db_driver,
    suite, scale factor, operation) and deletes the runs of that cell that started before it.
    Cells without a completed run, later failed attempts and running runs are kept.
    `--stale-versions` also deletes every run of a db_version other than the current pin.
    """
    if not superseded and not stale_versions:
        raise SystemExit("Specify --superseded and/or --stale-versions.")

    doomed = find_superseded_runs(revision=revision, stale_versions=stale_versions)
    if not superseded:
        pins = current_pins()
        doomed = [run for run in doomed if run.db in pins and run.db_version != pins[run.db]]

    if not doomed:
        print("Nothing to prune.")
        return

    print(f"{'Would delete' if dry_run else 'Deleting'} {len(doomed)} run(s):")
    for run in doomed:
        print(f"  {run.label}")

    if dry_run:
        return

    _confirm(f"delete {len(doomed)} run(s)", force=force)
    count = delete_runs([run.id for run in doomed], revision=revision)
    print(f"Deleted {count} run(s) and their associated steps and metrics.")


@results_app.command
def revisions() -> None:
    """List available result database revisions."""
    names = list_revisions()
    if not names:
        print(f"No result databases in {get_settings().results_directory}")
        return
    for name in names:
        print(name)


@results_app.command
def upload(revision: Revision = "default") -> None:
    """Upload a results revision to the `runs` release so another host can merge it.

    Revision databases are binary and effectively immutable (use one revision name per host and
    run), so they are shared as release assets rather than committed to the repository.
    """
    db_path = upload_results_revision(revision)
    print(f"Uploaded {db_path.name} to the '{SHARED_REVISION_RELEASE_TAG}' release")


@results_app.command
def fetch(revision: Revision = "default") -> None:
    """Download a results revision from the `runs` release into the configured results directory."""
    db_path = fetch_results_revision(revision)
    print(f"Downloaded {db_path}")


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


def _matrix_statuses(cells: list[MatrixCell], revision: Revision) -> list[CellStatus]:
    db_path = get_results_db_path(revision)
    runs = load_runs(db_path) if db_path.is_file() else []
    databases = get_databases()
    query_names = {
        cell: tuple(
            databases[cell.db]
            .suite_registry()[cell.suite](db=databases[cell.db], name=cell.suite, scale_factor=cell.scale_factor)
            .planned_query_names()
        )
        for cell in cells
        if cell.operation == "select"
    }
    return matrix_status(
        runs,
        cells,
        system=get_settings().system,
        pins=current_pins(),
        drivers=current_drivers(),
        current_query_names=query_names,
    )


def _matrix_cells(
    db: list[DatabaseName] | None, include_experimental: bool, suites: list[SuiteName] | None = None
) -> list[MatrixCell]:
    cells = expand_matrix(dbs=db, published_only=db is None and not include_experimental)
    return cells if suites is None else [cell for cell in cells if cell.suite in suites]


@matrix_app.command(name="status")
def matrix_status_cmd(
    db: list[DatabaseName] | None = None,
    suite: list[SuiteName] | None = None,
    revision: Revision = "default",
    include_experimental: bool = False,
    check: bool = False,
) -> None:
    """Show each declared cell against its latest run in a revision.

    A cell is `ok` when its latest run completed on the current version pin with clean recorded
    repository provenance, `stale` when its version, profile or provenance is invalid,
    `failed` when the latest run did not complete and `missing` without any run. The `unsupported`
    column counts the select queries the engine declares unsupported in query_registry. With
    `--check`, exits non-zero unless every cell is ok. Experimental (unpublished) engines are
    included when named with `--db` or with `--include-experimental`.
    """
    statuses = _matrix_statuses(_matrix_cells(db, include_experimental, suite), revision)
    print(format_matrix_status(statuses))

    counts: dict[str, int] = {}
    for status in statuses:
        counts[status.state] = counts.get(status.state, 0) + 1
    print(", ".join(f"{count} {state}" for state, count in sorted(counts.items())))

    if check and any(status.state != "ok" for status in statuses):
        raise SystemExit(1)


@matrix_app.command(name="run")
def matrix_run(
    db: list[DatabaseName] | None = None,
    suite: list[SuiteName] | None = None,
    revision: Revision = "default",
    missing_only: bool = False,
    keep_going: bool = False,
    include_experimental: bool = False,
    drop_caches: bool = False,
) -> None:
    """Run the declared matrix through the benchmark machinery, then validate once.

    Cells are grouped by (db, suite, scale factor) and each group runs its declared operations in
    order on one container, followed by cleanup of the database files. With `--missing-only`, only
    groups with a cell that is not ok are run, and they are run in full because the operations
    share the populated data. Without `--keep-going` the run stops at the first failed group.

    Wrong-result marking runs once at the end (as `olap results validate --mark`). Prints a summary
    and exits non-zero when any group failed. `--drop-caches` drops the page cache before each
    container starts, as for `olap benchmark`. Experimental (unpublished) engines run only when
    named with `--db` or with `--include-experimental`.
    """
    cells = _matrix_cells(db, include_experimental, suite)
    selected = cells
    if missing_only:
        pending = {
            (status.cell.db, status.cell.suite, status.cell.scale_factor)
            for status in _matrix_statuses(cells, revision)
            if status.state != "ok"
        }
        selected = [cell for cell in cells if (cell.db, cell.suite, cell.scale_factor) in pending]

    operations: dict[tuple[DatabaseName, SuiteName, int], list[Operation]] = {}
    for cell in selected:
        operations.setdefault((cell.db, cell.suite, cell.scale_factor), []).append(cell.operation)
    groups = [BenchmarkGroup(db_name, suite, scale, tuple(ops)) for (db_name, suite, scale), ops in operations.items()]

    if not groups:
        print("Every selected cell is ok; nothing to run.")
        return

    outcomes = run_benchmark_groups(
        groups, revision=revision, cleanup=True, keep_going=keep_going, drop_caches=drop_caches
    )

    report = validate_results(revision=revision, pins=current_pins(), system=get_settings().system, mark=True)
    print(report.format())
    print()
    print(format_matrix_status(_matrix_statuses(selected, revision)))
    print()
    finished = {outcome.group: outcome for outcome in outcomes}
    print(
        format_table(
            ("group", "outcome"),
            (
                (
                    group.label,
                    "not run" if group not in finished else (finished[group].error or "ok"),
                )
                for group in groups
            ),
        )
    )

    if any(outcome.error is not None for outcome in outcomes):
        raise SystemExit(1)


if __name__ == "__main__":
    app()
