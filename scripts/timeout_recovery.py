from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import signal
import subprocess
from collections.abc import Generator, Sequence
from contextlib import contextmanager, suppress
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import Any, Literal, NoReturn, TypedDict, cast
from uuid import uuid4

import duckdb
import psutil
from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter

from olap_benchmarks.dbs import get_databases
from olap_benchmarks.inputs import fingerprint_input_directory
from olap_benchmarks.matrix import current_pins
from olap_benchmarks.results import mark_running_runs_failed
from olap_benchmarks.results.analytical import analytical_run_problems
from olap_benchmarks.results.cells import StoredRun, load_runs
from olap_benchmarks.results.validation import validate_results
from olap_benchmarks.run_metadata import digest_hashes, hash_schema_files, sha256_text
from olap_benchmarks.settings import (
    REPO_ROOT,
    DatabaseName,
    SuiteName,
    format_suite_data_directory_name,
    get_settings,
)

DEFAULT_PLAN = REPO_ROOT / "docs/campaigns/2026-10-timeout-recovery-plan.json"


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Budgets(Model):
    system: str
    benchmark_threads: int
    benchmark_memory_gb: int
    client_memory_limit_gb: int
    minimum_free_disk_gb: int
    query_timeout_seconds: int
    operation_timeout_seconds: int


class InputFile(Model):
    path: str
    bytes: int
    sha256: str


class InputManifest(Model):
    algorithm: Literal["sha256"]
    manifest_sha256: str
    files: list[InputFile]


class Cell(Model):
    key: str
    db: DatabaseName
    suite: SuiteName
    scale_factor: int
    source_run_id: int
    reason: str
    query_names: list[str]
    sql_sha256: dict[str, str]
    registry_exclusions: dict[str, list[str]]
    query_exclude: list[str]
    input_key: str
    schema_hashes: dict[str, JsonValue] = Field(alias="schema")


class Analytical(Model):
    cell_key: str
    source_run_ids: list[int]
    clients: list[int]
    duration_seconds: float
    seed: int
    manifest: dict[str, JsonValue]
    warmup: dict[str, JsonValue]
    recorded_budget: dict[str, JsonValue]
    original_operation_timeout_seconds: int
    original_query_timeout_seconds: int
    source_budget_note: str


class Plan(Model):
    version: Literal[1]
    status: Literal["prepared-not-started"]
    workspace: Path
    branch: str
    revision: str
    source_database: Path
    source_bytes: int
    source_sha256: str
    input_directory: Path
    settings: Budgets
    pins: dict[str, str]
    provenance: dict[str, JsonValue]
    inputs: dict[str, InputManifest]
    cells: list[Cell]
    analytical: Analytical


class Step(Model):
    step_type: str
    status: str
    query_name: str | None
    iteration_role: str | None
    result_status: str | None
    error_type: str | None
    error_message: str | None


class RunAudit(Model):
    id: int
    operation: str
    status: str
    finished_at: str | None
    error_type: str | None
    error_message: str | None
    metadata: dict[str, JsonValue]
    steps: list[Step]


class ResumeRun(Model):
    id: int
    status: str
    note: str | None


class Acknowledgment(Model):
    reason: str
    recorded_at: str
    run_id: int


class Attempt(Model):
    id: str
    cell_key: str
    started_at: str
    status: Literal["running", "blocked", "completed", "accepted_failed_select", "accepted_failed_populate"] = "running"
    phase: str = "fresh_store"
    runs: dict[str, int] = Field(default_factory=dict)
    error: str | None = None
    cleanup_confirmed: bool = False
    validation_ok: bool = False
    completed_operations: list[str] = Field(default_factory=lambda: list[str]())
    acknowledgment: Acknowledgment | None = None


class Journal(Model):
    plan_sha256: str
    commit: str
    revision: str
    attempts: list[Attempt] = Field(default_factory=lambda: list[Attempt]())


class RecoveryBlocked(RuntimeError):
    pass


class RecoverySummary(TypedDict):
    status: Literal["measurements_complete", "attempted_with_failures", "blocked_or_not_attempted"]
    settled_cells: list[str]
    completed_measurement_cells: list[str]
    acknowledged_failed_populations: list[str]
    acknowledged_failed_selects: list[str]
    select_not_attempted_cells: list[str]
    remaining_cells: list[str]
    analytical_completed_clients: list[int]
    analytical_remaining_clients: list[int]
    analytical_prerequisite_failed: bool


def sha256_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def atomic_write(path: Path, text: str) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def save_journal(path: Path, journal: Journal) -> None:
    atomic_write(path, journal.model_dump_json(indent=2) + "\n")


def checked_output(command: Sequence[str], *, env: dict[str, str] | None = None) -> str:
    return subprocess.check_output(command, cwd=REPO_ROOT, env=env, text=True).strip()


def require_owned_path(path: Path, root: Path) -> None:
    if not path.is_relative_to(root) or path == root or path.resolve() != path:
        raise RecoveryBlocked(f"Refusing a path outside the owned tree or through a symlink: {path}")


def environment(plan: Plan, cell: Cell | None = None, note: str = "") -> dict[str, str]:
    values: dict[str, str] = dict(os.environ)
    configured = {
        **plan.settings.model_dump(),
        "input_data_directory": str(plan.input_directory),
        "results_directory": str(plan.workspace / "results"),
        "database_directory": str(plan.workspace / "data/dbs"),
        "temporary_directory": str(plan.workspace / "data/temp"),
        "query_include": "[]",
        "query_exclude": json.dumps(cell.query_exclude if cell else []),
        "tables": "[]",
        "run_note": note,
    }
    values.update({f"OLAP_BENCHMARKS_{key.upper()}": str(value) for key, value in configured.items()})
    values["POLARS_MAX_THREADS"] = "10"
    return values


def no_benchmark_containers() -> None:
    names = checked_output(["docker", "ps", "-a", "--filter", "name=benchmark", "--format", "{{.Names}}"])
    if names:
        raise RecoveryBlocked(
            f"Benchmark containers still exist; refusing to stop or delete another owner's store: {names}"
        )


def check_disk(plan: Plan) -> None:
    free = shutil.disk_usage(plan.workspace).free
    minimum = plan.settings.minimum_free_disk_gb * 1024**3
    if free < minimum:
        raise RecoveryBlocked(f"Only {free / 1024**3:.1f} GiB free; reserve is {minimum / 1024**3:.0f} GiB")


def verify_manifests(plan: Plan) -> None:
    if current_pins() != plan.pins:
        raise RecoveryBlocked("Engine pins changed from the frozen plan")
    for cell in plan.cells:
        database = get_databases()[cell.db]
        suite = database.suite_registry()[cell.suite](db=database, name=cell.suite, scale_factor=cell.scale_factor)
        names = list(suite.planned_query_names())
        exclusions = {name: list(skip) for name in names if (skip := suite.query_skip(name)) is not None}
        hashes = {name: sha256_text(suite.load_query(name)) for name in names if name not in exclusions}
        if (names, exclusions, hashes) != (cell.query_names, cell.registry_exclusions, cell.sql_sha256):
            raise RecoveryBlocked(f"Canonical SQL or registry exclusions changed: {cell.key}")
        if hash_schema_files(cell.suite, cell.db) != cell.schema_hashes:
            raise RecoveryBlocked(f"Physical schema changed: {cell.key}")
        if cell.key == plan.analytical.cell_key and digest_hashes(hashes) != plan.analytical.manifest:
            raise RecoveryBlocked("Analytical SQL differs from original runs670/671/672")
        directory = plan.input_directory / format_suite_data_directory_name(cell.suite, cell.scale_factor)
        sizes = {p.relative_to(directory).as_posix(): p.stat().st_size for p in directory.rglob("*") if p.is_file()}
        if sizes != {f.path: f.bytes for f in plan.inputs[cell.input_key].files}:
            raise RecoveryBlocked(f"Input files/sizes differ from original run{cell.source_run_id}: {directory}")


def preflight(plan: Plan, *, measurement: bool) -> str:
    if plan.workspace != REPO_ROOT or Path.cwd().resolve() != plan.workspace:
        raise RecoveryBlocked("Run from the exact worktree recorded in the plan")
    if Path(plan.revision).name != plan.revision or not plan.revision.startswith("2026-10-timeout-recovery"):
        raise RecoveryBlocked("Recovery revision must be a new, local recovery revision")
    for relative in ("data/dbs", "data/temp", "results", str(plan.source_database)):
        require_owned_path(plan.workspace / relative, plan.workspace)
    if checked_output(["git", "branch", "--show-current"]) != plan.branch:
        raise RecoveryBlocked("Wrong branch")
    if measurement and checked_output(["git", "status", "--porcelain"]):
        raise RecoveryBlocked("Measurement requires clean committed provenance, including untracked files")
    source = plan.workspace / plan.source_database
    if source.stat().st_size != plan.source_bytes or sha256_file(source) != plan.source_sha256:
        raise RecoveryBlocked("Frozen selected asset size or SHA256 differs")
    os.environ.update(environment(plan))
    get_settings.cache_clear()
    verify_manifests(plan)
    check_disk(plan)
    no_benchmark_containers()
    return checked_output(["git", "rev-parse", "HEAD"])


@contextmanager
def exclusive_lock(path: Path) -> Generator[None]:
    with path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RecoveryBlocked("Another recovery runner holds the lock") from exc
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def execute(command: list[str], env: dict[str, str], log: Path) -> int:
    with log.open("w") as stream:
        stream.write(json.dumps({"command": command, "started_at": datetime.now(UTC).isoformat()}) + "\n")
        stream.flush()
        process = subprocess.Popen(
            command, cwd=REPO_ROOT, env=env, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True
        )
        try:
            return process.wait()
        except BaseException:
            descendants: list[psutil.Process] = []
            with suppress(psutil.NoSuchProcess):
                descendants = psutil.Process(process.pid).children(recursive=True)
            if process.poll() is None:
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGINT)
                try:
                    process.wait(timeout=180)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            alive = [child for child in descendants if child.is_running()]
            for child in alive:
                with suppress(psutil.NoSuchProcess):
                    child.terminate()
            _, alive = psutil.wait_procs(alive, timeout=10)
            for child in alive:
                with suppress(psutil.NoSuchProcess):
                    child.kill()
            psutil.wait_procs(alive, timeout=10)
            raise


def olap_command(*arguments: str) -> list[str]:
    return [str(REPO_ROOT / ".venv/bin/olap"), *arguments]


def sql_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def export_json(plan: Plan, sql: str, destination: Path, *, source: bool = False) -> None:
    env = environment(plan)
    revision = plan.revision
    if source:
        env["OLAP_BENCHMARKS_RESULTS_DIRECTORY"] = str((plan.workspace / plan.source_database).parent)
        revision = plan.source_database.stem
    query = f"COPY ({sql}) TO {sql_literal(str(destination))} (FORMAT JSON, ARRAY true)"
    checked_output(olap_command("results", "query", "--revision", revision, query), env=env)


def audit(plan: Plan, note: str, destination: Path) -> RunAudit:
    export_json(
        plan,
        "SELECT r.id,r.operation,r.status,r.finished_at,r.error_type,r.error_message,r.metadata, "
        "(SELECT list({'step_type':s.step_type,'status':s.status,'query_name':s.query_name,"
        "'iteration_role':s.iteration_role,'result_status':s.result_status,"
        "'error_type':s.error_type,'error_message':s.error_message} ORDER BY s.id) "
        "FROM run_step s WHERE s.run_id=r.id) AS steps FROM run r "
        f"WHERE json_extract_string(r.metadata,'$.options.note')={sql_literal(note)}",
        destination,
    )
    return read_audit(destination)


def read_audit(path: Path) -> RunAudit:
    runs = TypeAdapter(list[RunAudit]).validate_json(path.read_text())
    if len(runs) != 1:
        raise RecoveryBlocked(f"Expected exactly one recorded run in {path}, found {len(runs)}")
    return runs[0]


def require_complete(run: RunAudit, operation: str) -> None:
    if run.operation != operation or run.status != "completed" or not run.steps:
        raise RecoveryBlocked(f"run{run.id} {operation}: {run.status}; {run.error_type}: {run.error_message}")
    if any(step.status != "completed" for step in run.steps):
        raise RecoveryBlocked(f"run{run.id} contains failed or unfinished steps")


def require_select_coverage(run: RunAudit, cell: Cell) -> None:
    plan_metadata = run.metadata["plan"]
    if not isinstance(plan_metadata, dict) or plan_metadata["query_names"] != cell.query_names:
        raise RecoveryBlocked(f"run{run.id} did not retain the full canonical query plan")
    expected = set(cell.query_names) - set(cell.registry_exclusions) - set(cell.query_exclude)
    for name in expected:
        measured = [
            step
            for step in run.steps
            if step.query_name == name and step.iteration_role == "measured" and step.result_status == "ok"
        ]
        if len(measured) != 3:
            raise RecoveryBlocked(f"run{run.id} {name}: expected three successful measured iterations")


def store_paths(plan: Plan, cell: Cell) -> list[Path]:
    paths = [
        plan.workspace / "data/dbs" / cell.db / format_suite_data_directory_name(cell.suite, cell.scale_factor),
        plan.workspace / "data/temp" / cell.db,
    ]
    for path in paths:
        require_owned_path(path, plan.workspace / "data")
    return paths


def clean_store(plan: Plan, cell: Cell, log: Path) -> None:
    no_benchmark_containers()
    paths = store_paths(plan, cell)
    existing = [path for path in paths if path.exists()]
    if not existing:
        return
    command = ["docker", "run", "--rm"]
    for index, path in enumerate(existing):
        command += ["--mount", f"type=bind,src={path.parent},dst=/owned{index}"]
    command += ["alpine", "rm", "-rf", "--", *[f"/owned{i}/{p.name}" for i, p in enumerate(existing)]]
    if execute(command, environment(plan), log) or any(path.exists() for path in existing):
        raise RecoveryBlocked(f"Cleanup was not confirmed for {cell.key}")


def validate_answers(plan: Plan, cell: Cell, destination: Path) -> bool:
    report = validate_results(
        db_path=plan.workspace / "results" / f"{plan.revision}.db",
        pins=plan.pins,
        system=plan.settings.system,
        suite=cell.suite,
        suite_scale_factor=cell.scale_factor,
        include_failed=True,
        mark=False,
    )
    destination.write_text(json.dumps(asdict(report), indent=2) + "\n")
    destination.with_suffix(".txt").write_text(report.format() + "\n")
    return report.ok


def validate_analytical_answers(plan: Plan, cell: Cell, run_id: int, destination: Path) -> None:
    database = plan.workspace / "results" / f"{plan.revision}.db"
    connection: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(database), read_only=True)
    try:
        runs = load_runs(database, connection)
        references: dict[tuple[str, str | None], StoredRun] = {}
        for run in runs:
            if (
                run.operation == "select"
                and run.suite == cell.suite
                and run.scale_factor == cell.scale_factor
                and run.system == plan.settings.system
                and run.status in ("completed", "failed")
                and run.db_version == plan.pins.get(run.db)
            ):
                references[run.db, run.db_driver] = run
        selected = [*references.values(), *[run for run in runs if run.id == run_id]]
        problems = analytical_run_problems(connection, selected)
    finally:
        connection.close()
    destination.write_text(json.dumps(problems, indent=2) + "\n")
    if problems:
        raise RecoveryBlocked(f"Analytical evidence/answer validation has findings: {destination}")


def require_run_provenance(plan: Plan, cell: Cell, journal: Journal, recorded: RunAudit) -> None:
    repository = recorded.metadata["repository"]
    if not isinstance(repository, dict) or repository["commit"] != journal.commit or repository["dirty"] is not False:
        raise RecoveryBlocked(f"run{recorded.id} does not have the required clean committed provenance")
    if recorded.metadata["input"] != plan.inputs[cell.input_key].model_dump():
        raise RecoveryBlocked(f"run{recorded.id} input manifest changed")
    settings = recorded.metadata["settings"]
    if not isinstance(settings, dict):
        raise RecoveryBlocked(f"run{recorded.id} settings are missing")
    expected = {
        f"OLAP_BENCHMARKS_{key.upper()}": value for key, value in plan.settings.model_dump().items() if key != "system"
    }
    expected.update(
        OLAP_BENCHMARKS_QUERY_INCLUDE=[], OLAP_BENCHMARKS_QUERY_EXCLUDE=cell.query_exclude, OLAP_BENCHMARKS_TABLES=[]
    )
    if any(settings[key] != value for key, value in expected.items()):
        raise RecoveryBlocked(f"run{recorded.id} recorded different settings or filters")


def operation_command(plan: Plan, cell: Cell, operation: str, clients: int | None = None) -> list[str]:
    common = ["--revision", plan.revision, "--scale-factor", str(cell.scale_factor)]
    if clients is None:
        return olap_command("benchmark", cell.db, cell.suite, operation, *common)
    return olap_command(
        "analytical",
        cell.db,
        cell.suite,
        "--clients",
        str(clients),
        "--duration-seconds",
        str(plan.analytical.duration_seconds),
        "--seed",
        str(plan.analytical.seed),
        *common,
    )


def operation_note(plan: Plan, cell: Cell, attempt: Attempt, phase: str) -> str:
    return f"{plan.revision}/{attempt.id}/{phase}; source run{cell.source_run_id}; frozen timeout recovery plan"


def run_operation_step(
    plan: Plan, cell: Cell, attempt: Attempt, journal: Journal, state: Path, directory: Path, clients: int | None = None
) -> RunAudit:
    phase = attempt.phase
    note = operation_note(plan, cell, attempt, phase)
    save_journal(state, journal)
    check_disk(plan)
    no_benchmark_containers()
    if checked_output(["git", "rev-parse", "HEAD"]) != journal.commit or checked_output(
        ["git", "status", "--porcelain"]
    ):
        raise RecoveryBlocked("Checkout changed during the campaign; refusing further measurements")
    operation = "analytical" if clients is not None else phase
    print(f"{datetime.now(UTC).isoformat()} {cell.key} {phase}; log: {directory / (phase + '.log')}", flush=True)
    code = execute(
        operation_command(plan, cell, operation, clients), environment(plan, cell, note), directory / f"{phase}.log"
    )
    no_benchmark_containers()
    recorded = audit(plan, note, directory / f"{phase}.json")
    attempt.runs[phase] = recorded.id
    save_journal(state, journal)
    require_run_provenance(plan, cell, journal, recorded)
    if operation == "select":
        attempt.validation_ok = validate_answers(plan, cell, directory / "validation.json")
        save_journal(state, journal)
    require_complete(recorded, operation)
    if code:
        raise RecoveryBlocked(f"Harness exit{code} after run{recorded.id}; see {phase}.log")
    if operation == "select":
        require_select_coverage(recorded, cell)
        if not attempt.validation_ok:
            raise RecoveryBlocked("Answer validation has findings; inspect validation.json; no marks were changed")
    if operation == "analytical":
        validate_analytical_answers(plan, cell, recorded.id, directory / f"{phase}-validation.json")
    return recorded


def run_cell(plan: Plan, cell: Cell, journal: Journal, state: Path, directory: Path) -> None:
    attempt = Attempt(id=uuid4().hex, cell_key=cell.key, started_at=datetime.now(UTC).isoformat())
    journal.attempts.append(attempt)
    directory = directory / attempt.id
    directory.mkdir()
    save_journal(state, journal)
    try:
        clean_store(plan, cell, directory / "initial-cleanup.log")
        input_directory = plan.input_directory / format_suite_data_directory_name(cell.suite, cell.scale_factor)
        actual = fingerprint_input_directory(input_directory)
        if actual != plan.inputs[cell.input_key].model_dump():
            raise RecoveryBlocked(f"Input SHA256 manifest changed: {cell.key}")
        for phase in ("populate", "select"):
            attempt.phase = phase
            run_operation_step(plan, cell, attempt, journal, state, directory)
            attempt.completed_operations.append(phase)
        if cell.key == plan.analytical.cell_key:
            for clients in plan.analytical.clients:
                attempt.phase = f"analytical-{clients}"
                run_operation_step(plan, cell, attempt, journal, state, directory, clients)
                attempt.completed_operations.append(attempt.phase)
        attempt.status = "completed"
    except BaseException as exc:
        attempt.status = "blocked"
        attempt.error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        try:
            clean_store(plan, cell, directory / "final-cleanup.log")
            attempt.cleanup_confirmed = True
        except BaseException as exc:
            attempt.status = "blocked"
            attempt.error = f"{attempt.error or ''}; cleanup: {type(exc).__name__}: {exc}"
            raise
        finally:
            save_journal(state, journal)


def settled_cells(journal: Journal) -> set[str]:
    latest = {attempt.cell_key: attempt for attempt in journal.attempts}
    return {
        attempt.cell_key
        for attempt in latest.values()
        if attempt.status in ("completed", "accepted_failed_select", "accepted_failed_populate")
        and attempt.cleanup_confirmed
    }


def pending_cells(plan: Plan, journal: Journal) -> list[Cell]:
    return [cell for cell in plan.cells if cell.key not in settled_cells(journal)]


def verify_resume_evidence(plan: Plan, journal: Journal, directory: Path) -> None:
    finished = [attempt for attempt in journal.attempts if attempt.cell_key in settled_cells(journal)]
    expected: dict[int, tuple[str, str | None]] = {}
    for attempt in finished:
        if attempt.status not in ("completed", "accepted_failed_select", "accepted_failed_populate"):
            continue
        failed_populate = attempt.status == "accepted_failed_populate"
        required = {"populate"} if failed_populate else {"populate", "select"}
        if failed_populate and (
            set(attempt.runs) != required
            or attempt.completed_operations
            or attempt.acknowledgment is None
            or attempt.acknowledgment.run_id != attempt.runs.get("populate")
        ):
            raise RecoveryBlocked(f"Acknowledged population failure has contradictory evidence: {attempt.cell_key}")
        if not failed_populate and attempt.cell_key == plan.analytical.cell_key:
            required.update(f"analytical-{clients}" for clients in plan.analytical.clients)
        if not required <= attempt.runs.keys():
            raise RecoveryBlocked(f"Settled journal entry lacks required operation evidence: {attempt.cell_key}")
        for phase, run_id in attempt.runs.items():
            status = None if phase == "select" and attempt.status == "accepted_failed_select" else "completed"
            if failed_populate:
                status = "failed"
            expected[run_id] = (f"{plan.revision}/{attempt.id}/{phase};", status)
    if not expected:
        return
    destination = directory / f"resume-evidence-{uuid4().hex}.json"
    export_json(
        plan,
        "SELECT id,status,json_extract_string(metadata,'$.options.note') AS note FROM run WHERE id IN ("
        + ",".join(str(run_id) for run_id in expected)
        + ")",
        destination,
    )
    records = TypeAdapter(list[ResumeRun]).validate_json(destination.read_text())
    if {run.id for run in records} != set(expected):
        raise RecoveryBlocked("Settled journal entries are missing from recovery results")
    for run in records:
        prefix, status = expected[run.id]
        if (
            run.note is None
            or not run.note.startswith(prefix)
            or run.status not in ("completed", "failed")
            or (status is not None and run.status != status)
        ):
            raise RecoveryBlocked(f"Recovery results disagree with journal for run{run.id}")


def accept_failed_select(journal: Journal, key: str) -> None:
    candidates = [attempt for attempt in journal.attempts if attempt.cell_key == key]
    if not candidates:
        raise RecoveryBlocked(f"No attempted cell to acknowledge: {key}")
    attempt = candidates[-1]
    if not (
        attempt.status == "blocked"
        and attempt.phase == "select"
        and {"populate", "select"} <= attempt.runs.keys()
        and attempt.cleanup_confirmed
        and attempt.validation_ok
    ):
        raise RecoveryBlocked(
            "Only a recorded SELECT attempt after complete population, validation and cleanup qualifies"
        )
    attempt.status = "accepted_failed_select"


def accept_failed_populate(
    plan: Plan, journal: Journal, key: str, directory: Path, reason: str, *, resume: bool
) -> None:
    if not resume or not reason.strip():
        raise RecoveryBlocked("Population acknowledgment requires --resume and a nonempty review reason")
    cell = next((cell for cell in plan.cells if cell.key == key), None)
    attempts = [attempt for attempt in journal.attempts if attempt.cell_key == key]
    if cell is None or not attempts:
        raise RecoveryBlocked(f"No planned population attempt to acknowledge: {key}")
    attempt = attempts[-1]
    if not (
        attempt.status == "blocked"
        and attempt.phase == "populate"
        and set(attempt.runs) == {"populate"}
        and not attempt.completed_operations
        and attempt.cleanup_confirmed
    ):
        raise RecoveryBlocked("Only a recorded failed population with confirmed cleanup and no dependents qualifies")
    no_benchmark_containers()
    check_disk(plan)
    if any(path.exists() for path in store_paths(plan, cell)):
        raise RecoveryBlocked("Population cleanup is no longer confirmed: owned store or scratch data remains")
    attempt_directory = directory / attempt.id
    archived = read_audit(attempt_directory / "populate.json")
    current = audit(
        plan,
        operation_note(plan, cell, attempt, "populate"),
        attempt_directory / f"populate-review-{uuid4().hex}.json",
    )
    for recorded in (archived, current):
        if not (
            recorded.id == attempt.runs["populate"]
            and recorded.operation == "populate"
            and recorded.status == "failed"
            and recorded.finished_at is not None
            and recorded.error_type
        ):
            raise RecoveryBlocked("Acknowledgment requires matching terminal failed population evidence")
        require_run_provenance(plan, cell, journal, recorded)
    if archived != current:
        raise RecoveryBlocked("Saved population failure evidence disagrees with the results revision")
    attempt.acknowledgment = Acknowledgment(
        reason=reason.strip(), recorded_at=datetime.now(UTC).isoformat(), run_id=current.id
    )
    attempt.status = "accepted_failed_populate"


def recovery_summary(plan: Plan, journal: Journal) -> RecoverySummary:
    latest = {attempt.cell_key: attempt for attempt in journal.attempts}
    settled = settled_cells(journal)
    measured = sorted(key for key, attempt in latest.items() if attempt.status == "completed")
    failed_populations = sorted(key for key, attempt in latest.items() if attempt.status == "accepted_failed_populate")
    failed_selects = sorted(key for key, attempt in latest.items() if attempt.status == "accepted_failed_select")
    analytical = latest.get(plan.analytical.cell_key)
    analytical_completed = [
        clients
        for clients in plan.analytical.clients
        if analytical is not None and f"analytical-{clients}" in analytical.completed_operations
    ]
    remaining = [clients for clients in plan.analytical.clients if clients not in analytical_completed]
    return {
        "status": (
            "measurements_complete"
            if len(measured) == len(plan.cells) and not remaining
            else "attempted_with_failures"
            if len(settled) == len(plan.cells)
            else "blocked_or_not_attempted"
        ),
        "settled_cells": sorted(settled),
        "completed_measurement_cells": measured,
        "acknowledged_failed_populations": failed_populations,
        "acknowledged_failed_selects": failed_selects,
        "select_not_attempted_cells": [
            cell.key for cell in plan.cells if cell.key not in latest or "select" not in latest[cell.key].runs
        ],
        "remaining_cells": [cell.key for cell in pending_cells(plan, journal)],
        "analytical_completed_clients": analytical_completed,
        "analytical_remaining_clients": remaining,
        "analytical_prerequisite_failed": analytical is not None
        and analytical.phase == "populate"
        and analytical.status in ("blocked", "accepted_failed_populate"),
    }


def initialize_journal(plan: Plan, plan_path: Path, state: Path, commit: str, *, resume: bool) -> Journal:
    database = plan.workspace / "results" / f"{plan.revision}.db"
    digest = sha256_file(plan_path)
    if state.exists():
        if not resume:
            raise RecoveryBlocked("Journal already exists; use --resume")
        journal = Journal.model_validate_json(state.read_text())
        if (journal.plan_sha256, journal.commit, journal.revision) != (digest, commit, plan.revision):
            raise RecoveryBlocked("Resume requires the same frozen plan, revision and committed checkout")
        if not database.is_file():
            raise RecoveryBlocked("Journal exists but recovery results are missing")
        mark_running_runs_failed(
            db_path=database, error_type="RecoveryInterrupted", error_message="Previous recovery process interrupted"
        )
        for attempt in journal.attempts:
            if attempt.status == "running":
                attempt.status = "blocked"
                attempt.error = "Interrupted; resume will freshly populate the entire cell"
        save_journal(state, journal)
        return journal
    if resume or database.exists():
        raise RecoveryBlocked("Refusing to adopt a results database without its matching journal")
    source_state = state.parent / "source-running-runs.json"
    export_json(plan, "SELECT id FROM run WHERE status='running'", source_state, source=True)
    if json.loads(source_state.read_text()):
        raise RecoveryBlocked("Source contains running runs; refusing to mutate historical evidence on resume")
    staging = database.with_suffix(".seed")
    with staging.open("xb") as target, (plan.workspace / plan.source_database).open("rb") as source:
        shutil.copyfileobj(source, target)
        target.flush()
        os.fsync(target.fileno())
    if sha256_file(staging) != plan.source_sha256:
        raise RecoveryBlocked("Seed copy failed its SHA256 check")
    staging.replace(database)
    journal = Journal(plan_sha256=digest, commit=commit, revision=plan.revision)
    save_journal(state, journal)
    return journal


def stop_requested(signum: int, _frame: FrameType | None) -> NoReturn:
    raise KeyboardInterrupt(f"Stop requested by signal {signum}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare/check or explicitly run the frozen serial timeout recovery")
    parser.add_argument("action", choices=("check", "run"), nargs="?", default="check")
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--accept-failed-select", action="append", default=[], metavar="DB:SUITE:sfN")
    parser.add_argument("--accept-failed-populate", action="append", default=[], metavar="DB:SUITE:sfN")
    parser.add_argument(
        "--acknowledgment-reason", default="", help="Required review reason for population acknowledgment"
    )
    arguments = parser.parse_args()
    accepted_selects: list[str] = arguments.accept_failed_select
    accepted_populations: list[str] = arguments.accept_failed_populate
    reason: str = arguments.acknowledgment_reason
    if (accepted_selects or accepted_populations) and (arguments.action != "run" or not arguments.resume):
        parser.error("Acknowledging failures requires run --resume and review of saved evidence")
    if accepted_populations and not reason.strip():
        parser.error("--accept-failed-populate requires --acknowledgment-reason")
    if set(accepted_populations) & set(accepted_selects):
        parser.error("A cell cannot acknowledge both population and SELECT failure")
    signal.signal(signal.SIGINT, stop_requested)
    signal.signal(signal.SIGTERM, stop_requested)
    plan_path: Path = arguments.plan
    plan = Plan.model_validate_json(plan_path.read_text())
    commit = preflight(plan, measurement=arguments.action == "run")
    if arguments.action == "check":
        print(
            f"Prepared only: {len(plan.cells)} cells + {len(plan.analytical.clients)} analytical runs; no measurements."
        )
        print("Source/SQL/pins/input sizes verified. Full input hashes checked before each population at START.")
        return
    directory = plan.workspace / "data/temp/timeout-recovery"
    directory.mkdir(parents=True, exist_ok=True)
    state = directory / "state.json"
    with exclusive_lock(directory / "runner.lock"):
        journal = initialize_journal(plan, plan_path, state, commit, resume=arguments.resume)
        for key in accepted_populations:
            accept_failed_populate(plan, journal, key, directory, reason, resume=arguments.resume)
        for key in accepted_selects:
            if key == plan.analytical.cell_key:
                raise RecoveryBlocked("PostgreSQL SF10 SELECT must succeed before the required analytical recovery")
            accept_failed_select(journal, key)
        verify_resume_evidence(plan, journal, directory)
        save_journal(state, journal)
        try:
            for cell in pending_cells(plan, journal):
                run_cell(plan, cell, journal, state, directory)
        finally:
            summary = recovery_summary(plan, journal)
            atomic_write(directory / "summary.json", json.dumps(summary, indent=2) + "\n")
            print(json.dumps(summary, indent=2), flush=True)
            print(f"Review recovery status in {directory / 'summary.json'}; nothing was published.", flush=True)


if __name__ == "__main__":
    main()
