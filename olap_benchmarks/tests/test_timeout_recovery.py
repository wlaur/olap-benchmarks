from __future__ import annotations

import json
import signal
import subprocess
import sys
from pathlib import Path
from time import monotonic, sleep

import psutil
import pytest

from scripts import timeout_recovery as recovery


@pytest.fixture
def plan(tmp_path: Path) -> recovery.Plan:
    original = recovery.Plan.model_validate_json(recovery.DEFAULT_PLAN.read_text())
    return original.model_copy(update={"workspace": tmp_path})


def journal() -> recovery.Journal:
    return recovery.Journal(plan_sha256="plan", commit="commit", revision="recovery")


def test_plan_covers_all_17_cells_and_preserves_analytical_contract() -> None:
    plan = recovery.Plan.model_validate_json(recovery.DEFAULT_PLAN.read_text())
    assert {cell.source_run_id for cell in plan.cells} == {
        511,
        525,
        522,
        397,
        365,
        658,
        372,
        458,
        316,
        282,
        494,
        404,
        387,
        381,
        368,
        660,
        465,
    }
    assert [cell.query_exclude for cell in plan.cells if cell.query_exclude] == [["15_part_type_distribution"]]
    assert len(next(cell for cell in plan.cells if cell.source_run_id == 522).query_names) == 23
    assert plan.analytical.source_run_ids == [670, 671, 672]
    assert plan.analytical.clients == [1, 4, 16]
    assert (plan.analytical.duration_seconds, plan.analytical.seed) == (60, 0)
    assert plan.analytical.manifest["digest"] == "1aceb1b1f21ad379edda0976a55c5ea8c126eb15ca04f92e85d346025ed57022"
    assert plan.settings.query_timeout_seconds == 300
    assert plan.settings.operation_timeout_seconds == 14400


def test_environment_clears_inherited_filters_and_uses_only_owned_stores(
    plan: recovery.Plan, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OLAP_BENCHMARKS_QUERY_INCLUDE", '["Q1"]')
    monkeypatch.setenv("OLAP_BENCHMARKS_TABLES", '["orders"]')
    monkeypatch.setenv("OLAP_BENCHMARKS_QUERY_EXCLUDE", '["Q32"]')
    cell = next(cell for cell in plan.cells if cell.source_run_id == 522)
    env = recovery.environment(plan, cell)
    assert env["OLAP_BENCHMARKS_QUERY_INCLUDE"] == "[]"
    assert env["OLAP_BENCHMARKS_TABLES"] == "[]"
    assert env["OLAP_BENCHMARKS_QUERY_EXCLUDE"] == '["15_part_type_distribution"]'
    assert env["OLAP_BENCHMARKS_DATABASE_DIRECTORY"] == str(plan.workspace / "data/dbs")
    assert env["OLAP_BENCHMARKS_INPUT_DATA_DIRECTORY"] == str(plan.input_directory)


def test_cleanup_rejects_symlink_to_another_worktree(tmp_path: Path) -> None:
    owned = tmp_path / "owned"
    owned.mkdir()
    other = tmp_path / "other"
    other.mkdir()
    alias = owned / "data"
    alias.symlink_to(other, target_is_directory=True)
    with pytest.raises(recovery.RecoveryBlocked, match="symlink"):
        recovery.require_owned_path(alias / "postgres", owned)
    assert other.is_dir()


@pytest.mark.parametrize("failure_phase", ["populate", "select", "analytical-4", None])
def test_failure_boundaries_and_serial_order(
    plan: recovery.Plan, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_phase: str | None
) -> None:
    cell = next(cell for cell in plan.cells if cell.key == plan.analytical.cell_key)
    state = tmp_path / "state.json"
    saved = journal()
    phases: list[str] = []
    cleanups: list[str] = []

    def clean(_plan: recovery.Plan, _cell: recovery.Cell, log: Path) -> None:
        cleanups.append(log.name)

    def fingerprint(_path: Path) -> dict[str, object]:
        return plan.inputs[cell.input_key].model_dump()

    def operation(
        _plan: recovery.Plan,
        _cell: recovery.Cell,
        attempt: recovery.Attempt,
        _journal: recovery.Journal,
        _state: Path,
        _directory: Path,
        clients: int | None = None,
    ) -> None:
        phases.append(attempt.phase)
        attempt.runs[attempt.phase] = len(phases)
        if attempt.phase == failure_phase:
            raise recovery.RecoveryBlocked("simulated resource boundary")
        if attempt.phase == "select":
            attempt.validation_ok = True
        if clients is not None:
            assert "select" in phases

    monkeypatch.setattr(recovery, "clean_store", clean)
    monkeypatch.setattr(recovery, "fingerprint_input_directory", fingerprint)
    monkeypatch.setattr(recovery, "run_operation_step", operation)
    if failure_phase is None:
        recovery.run_cell(plan, cell, saved, state, tmp_path)
        assert phases == ["populate", "select", "analytical-1", "analytical-4", "analytical-16"]
        assert recovery.settled_cells(saved) == {cell.key}
    else:
        with pytest.raises(recovery.RecoveryBlocked, match="resource boundary"):
            recovery.run_cell(plan, cell, saved, state, tmp_path)
        assert phases[-1] == failure_phase
        assert not recovery.settled_cells(saved)
    assert cleanups == ["initial-cleanup.log", "final-cleanup.log"]
    assert recovery.Journal.model_validate_json(state.read_text()).attempts[-1].cleanup_confirmed


def test_unconfirmed_cleanup_never_marks_cell_complete(
    plan: recovery.Plan, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cell = plan.cells[0]

    def clean(_plan: recovery.Plan, _cell: recovery.Cell, _log: Path) -> None:
        raise recovery.RecoveryBlocked("container still exists")

    monkeypatch.setattr(recovery, "clean_store", clean)
    saved = journal()
    with pytest.raises(recovery.RecoveryBlocked, match="container still exists"):
        recovery.run_cell(plan, cell, saved, tmp_path / "state.json", tmp_path)
    assert saved.attempts[-1].status == "blocked"
    assert not saved.attempts[-1].cleanup_confirmed
    assert not recovery.settled_cells(saved)


def test_resume_marks_only_new_revision_and_repopulates_interrupted_cell(
    plan: recovery.Plan, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state.json"
    result = tmp_path / "results" / f"{plan.revision}.db"
    result.parent.mkdir()
    result.touch()
    saved = recovery.Journal(
        plan_sha256=recovery.sha256_file(recovery.DEFAULT_PLAN), commit="same", revision=plan.revision
    )
    saved.attempts = [
        recovery.Attempt(id="finished", cell_key="first", started_at="now", status="completed", cleanup_confirmed=True),
        recovery.Attempt(id="interrupted", cell_key="second", started_at="now", phase="select", runs={"populate": 900}),
    ]
    recovery.save_journal(state, saved)
    mutations: list[object] = []

    def mark(**kwargs: object) -> int:
        mutations.append(kwargs["db_path"])
        return 1

    monkeypatch.setattr(recovery, "mark_running_runs_failed", mark)
    resumed = recovery.initialize_journal(plan, recovery.DEFAULT_PLAN, state, "same", resume=True)
    assert mutations == [result]
    assert recovery.settled_cells(resumed) == {"first"}
    assert resumed.attempts[-1].status == "blocked"
    with pytest.raises(recovery.RecoveryBlocked, match="same frozen plan"):
        recovery.initialize_journal(plan, recovery.DEFAULT_PLAN, state, "changed", resume=True)
    assert mutations == [result]


def test_seed_preserves_original_and_refuses_untracked_existing_revision(
    plan: recovery.Plan, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = tmp_path / plan.source_database
    original.parent.mkdir(parents=True)
    original.write_bytes(b"immutable original evidence")
    (tmp_path / "results").mkdir()
    plan.source_sha256 = recovery.sha256_file(original)
    plan.source_bytes = original.stat().st_size

    def export(_plan: recovery.Plan, _sql: str, destination: Path, *, source: bool = False) -> None:
        assert source
        destination.write_text("[]")

    monkeypatch.setattr(recovery, "export_json", export)
    state = tmp_path / "state.json"
    recovery.initialize_journal(plan, recovery.DEFAULT_PLAN, state, "clean", resume=False)
    assert original.read_bytes() == b"immutable original evidence"
    assert (tmp_path / "results" / f"{plan.revision}.db").read_bytes() == original.read_bytes()
    state.unlink()
    with pytest.raises(recovery.RecoveryBlocked, match="without its matching journal"):
        recovery.initialize_journal(plan, recovery.DEFAULT_PLAN, state, "clean", resume=False)


def test_failed_population_cannot_be_acknowledged_as_a_select_attempt() -> None:
    saved = journal()
    attempt = recovery.Attempt(id="a", cell_key="cell", started_at="now", status="blocked", phase="populate")
    saved.attempts.append(attempt)
    with pytest.raises(recovery.RecoveryBlocked, match="Only a recorded SELECT"):
        recovery.accept_failed_select(saved, "cell")
    attempt.phase = "select"
    attempt.runs = {"populate": 1000, "select": 1001}
    attempt.cleanup_confirmed = True
    attempt.validation_ok = True
    recovery.accept_failed_select(saved, "cell")
    assert attempt.status == "accepted_failed_select"
    assert recovery.settled_cells(saved) == {"cell"}


def population_failure(
    plan: recovery.Plan, cell: recovery.Cell, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[recovery.Journal, recovery.RunAudit]:
    saved = journal()
    attempt = recovery.Attempt(
        id="failed-population",
        cell_key=cell.key,
        started_at="now",
        status="blocked",
        phase="populate",
        runs={"populate": 900},
        error="Insufficient disk space",
        cleanup_confirmed=True,
    )
    saved.attempts.append(attempt)
    settings = {
        f"OLAP_BENCHMARKS_{key.upper()}": value for key, value in plan.settings.model_dump().items() if key != "system"
    }
    settings.update(
        OLAP_BENCHMARKS_QUERY_INCLUDE=[], OLAP_BENCHMARKS_QUERY_EXCLUDE=cell.query_exclude, OLAP_BENCHMARKS_TABLES=[]
    )
    recorded = recovery.RunAudit(
        id=900,
        operation="populate",
        status="failed",
        finished_at="2026-10-25T10:00:00",
        error_type="RuntimeError",
        error_message="Insufficient disk space",
        steps=[],
        metadata={
            "repository": {"commit": saved.commit, "dirty": False},
            "input": plan.inputs[cell.input_key].model_dump(),
            "settings": settings,
        },
    )
    directory = tmp_path / attempt.id
    directory.mkdir()
    (directory / "populate.json").write_text(json.dumps([recorded.model_dump()]))

    def audit(_plan: recovery.Plan, note: str, destination: Path) -> recovery.RunAudit:
        assert note == recovery.operation_note(plan, cell, attempt, "populate")
        destination.write_text(json.dumps([recorded.model_dump()]))
        return recorded

    def disk(_plan: recovery.Plan) -> None:
        pass

    monkeypatch.setattr(recovery, "audit", audit)
    monkeypatch.setattr(recovery, "no_benchmark_containers", lambda: None)
    monkeypatch.setattr(recovery, "check_disk", disk)
    return saved, recorded


@pytest.mark.parametrize("source_run_id", [458, 372])
def test_reviewed_population_failure_allows_only_independent_remaining_cells(
    plan: recovery.Plan, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source_run_id: int
) -> None:
    cell = next(cell for cell in plan.cells if cell.source_run_id == source_run_id)
    independent = plan.cells[0]
    plan.cells = [cell, independent]
    saved, recorded = population_failure(plan, cell, tmp_path, monkeypatch)
    before = recorded.model_dump_json()
    recovery.accept_failed_populate(
        plan, saved, cell.key, tmp_path, "Reviewed disk failure; reserve restored", resume=True
    )
    attempt = saved.attempts[0]
    assert attempt.status == "accepted_failed_populate"
    assert attempt.acknowledgment is not None
    assert attempt.acknowledgment.run_id == 900
    assert attempt.acknowledgment.reason == "Reviewed disk failure; reserve restored"
    assert recorded.model_dump_json() == before
    assert not attempt.completed_operations
    assert attempt.error == "Insufficient disk space"
    assert recovery.pending_cells(plan, saved) == [independent]

    def export(_plan: recovery.Plan, _sql: str, destination: Path, *, source: bool = False) -> None:
        assert not source
        destination.write_text(
            json.dumps(
                [
                    {
                        "id": recorded.id,
                        "status": recorded.status,
                        "note": recovery.operation_note(plan, cell, attempt, "populate"),
                    }
                ]
            )
        )

    monkeypatch.setattr(recovery, "export_json", export)
    recovery.verify_resume_evidence(plan, saved, tmp_path)
    recorded.status = "completed"
    with pytest.raises(recovery.RecoveryBlocked, match="disagree with journal"):
        recovery.verify_resume_evidence(plan, saved, tmp_path)
    recorded.status = "failed"
    phases: list[tuple[str, str]] = []

    def operation(
        _plan: recovery.Plan,
        current: recovery.Cell,
        active: recovery.Attempt,
        _journal: recovery.Journal,
        _state: Path,
        _directory: Path,
        clients: int | None = None,
    ) -> None:
        assert clients is None
        phases.append((current.key, active.phase))
        active.runs[active.phase] = 900 + len(phases)

    def clean(_plan: recovery.Plan, _cell: recovery.Cell, _log: Path) -> None:
        pass

    def fingerprint(_path: Path) -> dict[str, object]:
        return plan.inputs[independent.input_key].model_dump()

    monkeypatch.setattr(recovery, "clean_store", clean)
    monkeypatch.setattr(recovery, "fingerprint_input_directory", fingerprint)
    monkeypatch.setattr(recovery, "run_operation_step", operation)
    for remaining in recovery.pending_cells(plan, saved):
        recovery.run_cell(plan, remaining, saved, tmp_path / "state.json", tmp_path)
    assert phases == [(independent.key, "populate"), (independent.key, "select")]
    summary = recovery.recovery_summary(plan, saved)
    assert summary["status"] == "attempted_with_failures"
    assert summary["acknowledged_failed_populations"] == [cell.key]
    assert summary["completed_measurement_cells"] == [independent.key]
    assert summary["select_not_attempted_cells"] == [cell.key]
    assert summary["analytical_completed_clients"] == []
    assert summary["analytical_remaining_clients"] == [1, 4, 16]
    assert summary["analytical_prerequisite_failed"] is (source_run_id == 372)


@pytest.mark.parametrize(
    "problem",
    [
        "no_resume",
        "no_reason",
        "running",
        "completed",
        "no_terminal_time",
        "wrong_run",
        "wrong_commit",
        "not_cleaned",
        "remaining_store",
        "low_disk",
        "active_container",
        "dependent_run",
        "changed_evidence",
    ],
)
def test_population_acknowledgment_refuses_unreviewable_or_unsafe_evidence(
    plan: recovery.Plan, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, problem: str
) -> None:
    cell = next(cell for cell in plan.cells if cell.source_run_id == 458)
    saved, recorded = population_failure(plan, cell, tmp_path, monkeypatch)

    def blocked(*_args: object) -> None:
        raise recovery.RecoveryBlocked("Safety check failed")

    match problem:
        case "running" | "completed":
            recorded.status = problem
        case "no_terminal_time":
            recorded.finished_at = None
        case "wrong_run":
            recorded.id = 901
        case "wrong_commit":
            recorded.metadata["repository"] = {"commit": "other", "dirty": False}
        case "not_cleaned":
            saved.attempts[0].cleanup_confirmed = False
        case "remaining_store":
            recovery.store_paths(plan, cell)[0].mkdir(parents=True)
        case "low_disk":
            monkeypatch.setattr(recovery, "check_disk", blocked)
        case "active_container":
            monkeypatch.setattr(recovery, "no_benchmark_containers", blocked)
        case "dependent_run":
            saved.attempts[0].runs["select"] = 901
        case "changed_evidence":
            recorded.error_message = "Different failure"
        case _:
            pass
    with pytest.raises(recovery.RecoveryBlocked):
        recovery.accept_failed_populate(
            plan,
            saved,
            cell.key,
            tmp_path,
            "" if problem == "no_reason" else "Reviewed evidence",
            resume=problem != "no_resume",
        )
    assert saved.attempts[0].status == "blocked"
    assert saved.attempts[0].acknowledgment is None
    assert not recovery.settled_cells(saved)


@pytest.mark.parametrize(
    "arguments",
    [
        ["run", "--accept-failed-populate", "postgres:tpc_h:sf50"],
        ["check", "--resume", "--accept-failed-populate", "postgres:tpc_h:sf50"],
        ["run", "--resume", "--accept-failed-populate", "postgres:tpc_h:sf50"],
    ],
)
def test_invalid_acknowledgment_cli_does_not_initialize_campaign(
    monkeypatch: pytest.MonkeyPatch, arguments: list[str]
) -> None:
    def unexpected(*_args: object, **_kwargs: object) -> str:
        pytest.fail("Invalid acknowledgment reached preflight")

    monkeypatch.setattr(sys, "argv", ["timeout_recovery.py", *arguments])
    monkeypatch.setattr(recovery, "preflight", unexpected)
    with pytest.raises(SystemExit) as exc:
        recovery.main()
    assert exc.value.code == 2


def test_resume_does_not_trust_completed_journal_without_matching_results(
    plan: recovery.Plan, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    saved = journal()
    saved.attempts = [
        recovery.Attempt(
            id="done",
            cell_key=plan.cells[0].key,
            started_at="now",
            status="completed",
            cleanup_confirmed=True,
            runs={"populate": 900, "select": 901},
        )
    ]

    def export(_plan: recovery.Plan, _sql: str, destination: Path, *, source: bool = False) -> None:
        assert not source
        destination.write_text("[]")

    monkeypatch.setattr(recovery, "export_json", export)
    with pytest.raises(recovery.RecoveryBlocked, match="missing from recovery results"):
        recovery.verify_resume_evidence(plan, saved, tmp_path)


def test_completed_run_with_partial_measurements_cannot_unlock_analytical(plan: recovery.Plan) -> None:
    cell = plan.cells[0].model_copy(update={"query_names": ["Q1"], "query_exclude": [], "registry_exclusions": {}})
    step = recovery.Step(
        step_type="query",
        status="completed",
        query_name="Q1",
        iteration_role="measured",
        result_status="ok",
        error_type=None,
        error_message=None,
    )
    recorded = recovery.RunAudit(
        id=900,
        operation="select",
        status="completed",
        finished_at="2026-10-25T10:00:00",
        error_type=None,
        error_message=None,
        metadata={"plan": {"query_names": ["Q1"]}},
        steps=[step, step],
    )
    with pytest.raises(recovery.RecoveryBlocked, match="three successful measured"):
        recovery.require_select_coverage(recorded, cell)
    recorded.steps.append(step)
    recovery.require_select_coverage(recorded, cell)
    recorded.steps.append(step.model_copy(update={"step_type": "phase", "status": "failed"}))
    with pytest.raises(recovery.RecoveryBlocked, match="failed or unfinished"):
        recovery.require_complete(recorded, "select")


def test_command_preserves_full_analytical_workload(plan: recovery.Plan) -> None:
    cell = next(cell for cell in plan.cells if cell.key == plan.analytical.cell_key)
    command = recovery.operation_command(plan, cell, "analytical", 16)
    assert command[1:4] == ["analytical", "postgres", "tpc_h"]
    assert command[command.index("--clients") + 1] == "16"
    assert command[command.index("--duration-seconds") + 1] == "60.0"
    assert command[command.index("--seed") + 1] == "0"
    assert "--setup" not in command


def test_interrupt_joins_the_operation_process(tmp_path: Path) -> None:
    child_pid = tmp_path / "child.pid"
    child_code = (
        "import os,time; from pathlib import Path; "
        f"Path({str(child_pid)!r}).write_text(str(os.getpid())); time.sleep(60)"
    )
    wrapper = (
        "import os,signal; from pathlib import Path; from scripts.timeout_recovery import execute,stop_requested; "
        "signal.signal(signal.SIGINT,stop_requested); "
        f"execute({[sys.executable, '-c', child_code]!r},dict(os.environ),Path({str(tmp_path / 'child.log')!r}))"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", wrapper], cwd=recovery.REPO_ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    try:
        deadline = monotonic() + 10
        while not child_pid.exists() and monotonic() < deadline:
            sleep(0.05)
        assert child_pid.exists(), "Executor did not start the test child"
        pid = int(child_pid.read_text())
        process.send_signal(signal.SIGINT)
        assert process.wait(timeout=15) != 0
        assert not psutil.pid_exists(pid)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        if child_pid.exists():
            pid = int(child_pid.read_text())
            if psutil.pid_exists(pid):
                psutil.Process(pid).kill()
