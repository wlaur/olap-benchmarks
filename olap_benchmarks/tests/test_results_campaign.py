from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from .. import results as results_module
from ..matrix import MatrixCell, registered_operations
from ..profile import BENCHMARK_PROFILE
from ..results import get_results_engine, migrate_results
from ..results.campaign import _current_query_names, campaign_cells, select_campaign_runs, validate_campaign_runs
from ..results.models import Run, RunStep, SystemSnapshot
from ..results.publishing import PublishValidationError, validate_publishable_runs
from ..results.validation import validate_results
from ..settings import SUITE_SPECS, get_settings
from ..suites.jsonbench.config import JSONBENCH_QUERY_NAMES

SELECT = MatrixCell("duckdb", "jsonbench", 10, "select")
POPULATE = MatrixCell("duckdb", "jsonbench", 10, "populate")
CELLS = [POPULATE, SELECT]
PINS = {"duckdb": "1.0", "polars": "2.0"}
DRIVERS: dict[str, str | None] = {"duckdb": None, "polars": None}


def _add_run(
    session: Session,
    cell: MatrixCell = SELECT,
    *,
    start: int = 0,
    status: str = "completed",
    commit: str = "clean-commit",
    measured: tuple[str, ...] = JSONBENCH_QUERY_NAMES,
    plan: tuple[str, ...] = JSONBENCH_QUERY_NAMES,
    iterations: tuple[int, ...] = (1, 2, 3, 4),
    plan_iterations: int = 4,
    recorded: tuple[str, ...] | None = None,
) -> int:
    time = datetime(2026, 10, 19) + timedelta(seconds=start)
    run = Run(
        suite=cell.suite,
        suite_scale_factor=cell.scale_factor,
        db=cell.db,
        db_version=PINS[cell.db],
        db_driver=DRIVERS[cell.db],
        operation=cell.operation,
        system="test",
        system_snapshot_id=1,
        benchmark_profile=BENCHMARK_PROFILE.id,
        status=status,
        started_at=time,
        finished_at=None if status == "running" else time,
        error_type="TimeoutError" if status == "failed" else None,
        error_message="Operation budget exceeded" if status == "failed" else None,
        metadata_json={
            "repository": {"commit": commit, "dirty": False},
            "plan": {"query_names": list(plan), "iterations": plan_iterations},
            "execution": {"mode": "in_process"},
        },
    )
    session.add(run)
    session.flush()
    if cell.operation == "select":
        for query in plan if recorded is None else recorded:
            for iteration in iterations:
                successful = query in measured
                session.add(
                    RunStep(
                        run_id=run.id,
                        step_type="query",
                        step_name="query",
                        query_name=query,
                        iteration=iteration,
                        iteration_role=BENCHMARK_PROFILE.iteration_role(iteration),
                        started_at=time,
                        finished_at=time,
                        status="completed",
                        result_status="ok" if successful else "skipped",
                        row_count=1 if successful else None,
                        metadata_json={"answer_hash": "hash"}
                        if successful
                        else {"skip_reason": "Excluded after reaching the operation budget"},
                    )
                )
    return run.id


def _results_db(tmp_path: Path, add: Callable[[Session], object]) -> Path:
    path = migrate_results(db_path=tmp_path / "campaign.db")
    engine = get_results_engine(db_path=path, read_only=False)
    try:
        with Session(engine) as session:
            session.add(SystemSnapshot(id=1, system="test"))
            add(session)
            session.commit()
    finally:
        engine.dispose()
    return path


def _select(path: Path, cells: list[MatrixCell] = CELLS) -> set[int]:
    return select_campaign_runs(path, cells, pins=PINS, drivers=DRIVERS)


def _validate(path: Path) -> None:
    validate_campaign_runs(path, CELLS, pins=PINS, drivers=DRIVERS)


def _validate_publication(path: Path, cells: list[MatrixCell] = CELLS, *, campaign: bool = True) -> None:
    validate_publishable_runs(path, cells, pins=PINS, drivers=DRIVERS, campaign=campaign, allow_mixed_commits=campaign)


def _query_step(session: Session, run_id: int, iteration: int = 1) -> RunStep:
    session.flush()
    return session.scalars(
        select(RunStep).where(
            RunStep.run_id == run_id, RunStep.query_name == JSONBENCH_QUERY_NAMES[0], RunStep.iteration == iteration
        )
    ).one()


def _fail_step(session: Session, run_id: int, result_status: str = "error", iteration: int = 1) -> None:
    step = _query_step(session, run_id, iteration)
    step.status = "failed"
    step.result_status = result_status
    step.row_count = None
    step.error_type = "TimeoutError" if result_status == "timeout" else "QueryError"
    step.error_message = "Query did not finish"
    step.metadata_json = {"duration_ms": 12.5}


def test_campaign_inventory_covers_every_registration_scale_and_operation() -> None:
    cells = campaign_cells()
    selects = [cell for cell in cells if cell.operation == "select"]

    assert len({cell.db for cell in selects}) == 11
    assert len({cell.suite for cell in selects}) == 14
    assert len(cells) == len(set(cells))
    assert set(cells) == {
        MatrixCell(db, suite, scale, operation)
        for db, suites in registered_operations().items()
        for suite, operations in suites.items()
        for scale in SUITE_SPECS[suite].scale_factors
        for operation in operations
    }
    assert MatrixCell("polars", "tpc_h", 50, "select") in cells
    assert MatrixCell("postgres", "time_series_metrics", 1, "select") in cells


def test_latest_failed_select_supersedes_completed_cohort(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _add_run(session, POPULATE)
        _add_run(session, start=1)
        _add_run(session, start=2, status="failed")

    assert _select(_results_db(tmp_path, add)) == {1, 3}


@pytest.mark.parametrize("status", ["completed", "failed"])
@pytest.mark.parametrize("invalid", ["version", "driver", "profile", "dirty", "commit", "invalidation"])
def test_selection_excludes_ineligible_newer_cohorts(tmp_path: Path, invalid: str, status: str) -> None:
    def add(session: Session) -> None:
        _add_run(session, start=1)
        invalid_id = _add_run(session, start=2, status=status)
        run = session.get(Run, invalid_id)
        assert run is not None
        if invalid == "version":
            run.db_version = "old"
        elif invalid == "driver":
            run.db_driver = "different"
        elif invalid == "profile":
            run.benchmark_profile = "legacy"
        elif invalid == "dirty":
            run.metadata_json = {**(run.metadata_json or {}), "repository": {"commit": "dirty", "dirty": True}}
        elif invalid == "commit":
            run.metadata_json = {**(run.metadata_json or {}), "repository": {"dirty": False}}
        else:
            run.metadata_json = {**(run.metadata_json or {}), "invalidation": {"reason": "Incomplete source table"}}

    assert _select(_results_db(tmp_path, add)) == {1}


def test_failed_nonselect_is_retained_as_an_explicit_partial_attempt(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _add_run(session, POPULATE, status="failed")
        _add_run(session, start=1)

    path = _results_db(tmp_path, add)
    assert _select(path) == {1, 2}
    _validate(path)


def test_latest_failed_nonselect_supersedes_an_earlier_completed_attempt(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _add_run(session, POPULATE)
        _add_run(session, POPULATE, start=1, status="failed")
        _add_run(session, start=2)

    assert _select(_results_db(tmp_path, add)) == {2, 3}


def test_unattempted_nonselect_does_not_prevent_a_measured_partial(tmp_path: Path) -> None:
    path = _results_db(tmp_path, lambda session: _add_run(session))

    assert _select(path) == {1}
    _validate(path)


def test_mixed_clean_commits_and_explained_query_skips_are_preserved(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _add_run(session, POPULATE, commit="populate-commit")
        _add_run(session, commit="select-commit", measured=JSONBENCH_QUERY_NAMES[:1])

    path = _results_db(tmp_path, add)
    assert _select(path) == {1, 2}
    _validate(path)


def test_every_select_cell_must_have_a_terminal_attempt(tmp_path: Path) -> None:
    path = _results_db(tmp_path, lambda session: _add_run(session))
    cells = [*CELLS, MatrixCell("polars", "jsonbench", 10, "select")]

    with pytest.raises(PublishValidationError, match="polars:jsonbench:sf10:select"):
        _select(path, cells)


def test_failed_only_select_retains_its_terminal_status(tmp_path: Path) -> None:
    path = _results_db(tmp_path, lambda session: _add_run(session, status="failed"))

    assert _select(path) == {1}
    _validate(path)
    _validate_publication(path)


@pytest.mark.parametrize("result_status", ["error", "timeout"])
def test_failed_query_results_and_recorded_skips_are_retained(tmp_path: Path, result_status: str) -> None:
    def add(session: Session) -> None:
        run_id = _add_run(session, status="failed")
        _fail_step(session, run_id, result_status, iteration=2)
        for iteration in (3, 4):
            step = _query_step(session, run_id, iteration)
            step.result_status = "skipped"
            step.row_count = None
            step.metadata_json = {"skip_reason": "Query aborted after a recorded failure"}

    path = _results_db(tmp_path, add)
    before = path.read_bytes()
    assert _select(path) == {1}
    _validate_publication(path)
    assert path.read_bytes() == before


@pytest.mark.parametrize("recorded", [(), JSONBENCH_QUERY_NAMES[:1]])
def test_operation_abort_keeps_unstarted_positions_absent(tmp_path: Path, recorded: tuple[str, ...]) -> None:
    def add(session: Session) -> None:
        run_id = _add_run(session, status="failed", recorded=recorded, iterations=(1, 2))
        if recorded:
            _fail_step(session, run_id, "timeout", iteration=2)

    path = _results_db(tmp_path, add)
    assert _select(path) == {1}
    _validate_publication(path)
    engine = get_results_engine(db_path=path)
    try:
        with Session(engine) as session:
            steps = session.scalars(select(RunStep)).all()
            assert len(steps) == len(recorded) * 2
            assert all(step.result_status != "skipped" for step in steps)
            run = session.get(Run, 1)
            assert run is not None and run.status == "failed"
            assert run.metadata_json is not None
            assert run.metadata_json["plan"]["query_names"] == list(JSONBENCH_QUERY_NAMES)
    finally:
        engine.dispose()


@pytest.mark.parametrize("field", ["error_type", "error_message"])
def test_failed_run_requires_explicit_error(tmp_path: Path, field: str) -> None:
    def add(session: Session) -> None:
        run_id = _add_run(session, status="failed", recorded=())
        run = session.get(Run, run_id)
        assert run is not None
        setattr(run, field, None)

    with pytest.raises(PublishValidationError, match="requires an explicit run error"):
        _select(_results_db(tmp_path, add))


def test_failed_interrupted_step_can_retain_an_unknown_result_status(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        run_id = _add_run(session, status="failed", recorded=JSONBENCH_QUERY_NAMES[:1], iterations=(1,))
        _fail_step(session, run_id)
        _query_step(session, run_id).result_status = None

    path = _results_db(tmp_path, add)
    assert _select(path) == {1}
    _validate_publication(path)


def test_matrix_publication_still_requires_completed_selects(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _add_run(session, POPULATE)
        _add_run(session, status="failed")

    with pytest.raises(PublishValidationError, match="latest run did not complete"):
        _validate_publication(_results_db(tmp_path, add), campaign=False)


def test_all_skipped_select_cannot_claim_measured_coverage(tmp_path: Path) -> None:
    path = _results_db(tmp_path, lambda session: _add_run(session, measured=()))

    with pytest.raises(PublishValidationError, match="no fully measured four-iteration query"):
        _select(path)


@pytest.mark.parametrize("status", ["completed", "failed"])
@pytest.mark.parametrize("problem", ["empty", "truncated", "iterations", "reordered"])
def test_recorded_query_plan_must_be_the_full_four_iteration_plan(tmp_path: Path, problem: str, status: str) -> None:
    def add(session: Session) -> None:
        _add_run(
            session,
            plan=()
            if problem == "empty"
            else JSONBENCH_QUERY_NAMES[:1]
            if problem == "truncated"
            else JSONBENCH_QUERY_NAMES[::-1]
            if problem == "reordered"
            else JSONBENCH_QUERY_NAMES,
            plan_iterations=3 if problem == "iterations" else 4,
            status=status,
        )

    with pytest.raises(PublishValidationError, match="recorded query plan differs"):
        _select(_results_db(tmp_path, add))


@pytest.mark.parametrize("iterations", [4.2, "4"])
def test_recorded_plan_iteration_count_must_be_an_exact_integer(tmp_path: Path, iterations: float | str) -> None:
    def add(session: Session) -> None:
        run_id = _add_run(session, status="failed", recorded=())
        run = session.get(Run, run_id)
        assert run is not None and run.metadata_json is not None
        run.metadata_json = {
            **run.metadata_json,
            "plan": {"query_names": list(JSONBENCH_QUERY_NAMES), "iterations": iterations},
        }

    with pytest.raises(PublishValidationError, match="recorded query plan differs"):
        _select(_results_db(tmp_path, add))


def test_incomplete_four_iteration_query_is_rejected(tmp_path: Path) -> None:
    path = _results_db(tmp_path, lambda session: _add_run(session, iterations=(1, 2, 3)))

    with pytest.raises(PublishValidationError, match="every planned query exactly four times"):
        _select(path)


@pytest.mark.parametrize("problem", ["duplicate", "wrong_role", "unfinished", "failed", "mixed_skip"])
def test_malformed_query_iterations_are_rejected(tmp_path: Path, problem: str) -> None:
    def add(session: Session) -> None:
        run_id = _add_run(session)
        session.flush()
        step = session.scalars(select(RunStep).where(RunStep.run_id == run_id).order_by(RunStep.id).limit(1)).one()
        if problem == "duplicate":
            step.iteration = 2
        elif problem == "wrong_role":
            step.iteration_role = "measured"
        elif problem == "unfinished":
            step.finished_at = None
        elif problem == "failed":
            step.status = "failed"
            step.result_status = "timeout"
        else:
            step.result_status = "skipped"
            step.metadata_json = {"skip_reason": "Query stopped"}

    with pytest.raises(PublishValidationError, match="invalid campaign cohorts"):
        _select(_results_db(tmp_path, add))


@pytest.mark.parametrize(
    "problem",
    [
        "duplicate",
        "wrong_role",
        "unfinished",
        "running",
        "out_of_plan",
        "out_of_range",
        "wrong_result",
        "missing_hash",
        "missing_rows",
        "unexplained_skip",
        "failed_ok",
        "completed_timeout",
        "success_with_error",
        "failure_without_error",
    ],
)
def test_failed_runs_do_not_relax_recorded_iteration_validation(tmp_path: Path, problem: str) -> None:
    def add(session: Session) -> None:
        run_id = _add_run(session, status="failed")
        step = _query_step(session, run_id)
        if problem == "duplicate":
            step.iteration = 2
        elif problem == "wrong_role":
            step.iteration_role = "measured"
        elif problem == "unfinished":
            step.finished_at = None
        elif problem == "running":
            step.status = "running"
        elif problem == "out_of_plan":
            step.query_name = "unplanned"
        elif problem == "out_of_range":
            step.iteration = 5
        elif problem == "wrong_result":
            step.result_status = "wrong_result"
        elif problem == "missing_hash":
            step.metadata_json = {}
        elif problem == "missing_rows":
            step.row_count = None
        elif problem == "unexplained_skip":
            step.result_status = "skipped"
        elif problem == "failed_ok":
            step.status = "failed"
        elif problem == "completed_timeout":
            step.result_status = "timeout"
        elif problem == "success_with_error":
            step.error_type = "QueryError"
        else:
            step.status = "failed"
            step.result_status = "error"

    with pytest.raises(PublishValidationError, match="invalid campaign cohorts"):
        _select(_results_db(tmp_path, add))


@pytest.mark.parametrize("conflict", ["hash", "row_count", "iterations"])
@pytest.mark.parametrize("other_status", ["completed", "failed"])
def test_failed_run_successes_still_undergo_answer_validation(tmp_path: Path, conflict: str, other_status: str) -> None:
    other = MatrixCell("polars", "jsonbench", 10, "select")

    def add(session: Session) -> None:
        _add_run(session, status=other_status)
        run_id = _add_run(session, other, status="failed")
        if conflict == "iterations":
            step = _query_step(session, run_id, iteration=2)
            step.metadata_json = {"answer_hash": "conflicting"}
        else:
            for step in session.scalars(select(RunStep).where(RunStep.run_id == run_id)).all():
                if conflict == "hash":
                    step.metadata_json = {"answer_hash": "conflicting"}
                else:
                    step.row_count = 2

    path = _results_db(tmp_path, add)
    assert _select(path, [SELECT, other]) == {1, 2}
    assert validate_results(db_path=path, pins=PINS).ok
    with pytest.raises(PublishValidationError, match="Answer validation failed") as exc:
        _validate_publication(path, [SELECT, other])
    assert "latest terminal select attempts" in str(exc.value)
    assert "latest completed select runs" not in str(exc.value)


def test_completed_run_cannot_record_an_operation_error(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        run_id = _add_run(session)
        run = session.get(Run, run_id)
        assert run is not None
        run.error_type = "QueryError"
        run.error_message = "Operation failed"

    with pytest.raises(PublishValidationError, match="completed select records a run error"):
        _select(_results_db(tmp_path, add))


@pytest.mark.parametrize("status", ["completed", "failed"])
def test_terminal_run_without_finish_time_is_rejected(tmp_path: Path, status: str) -> None:
    def add(session: Session) -> None:
        _add_run(session)
        run_id = _add_run(session, start=1, status=status)
        run = session.get(Run, run_id)
        assert run is not None
        run.finished_at = None

    with pytest.raises(PublishValidationError, match="unfinished campaign runs"):
        _select(_results_db(tmp_path, add))


def test_selected_cohorts_must_share_one_system(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _add_run(session, POPULATE)
        run_id = _add_run(session, status="failed")
        run = session.get(Run, run_id)
        assert run is not None
        run.system = "another-system"

    with pytest.raises(PublishValidationError, match="exactly one system"):
        _select(_results_db(tmp_path, add))


def test_older_unfinished_attempt_does_not_hide_a_newer_completed_cohort(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _add_run(session, status="running")
        _add_run(session, start=1)

    assert _select(_results_db(tmp_path, add)) == {2}


def test_newer_running_attempt_blocks_publication(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _add_run(session)
        _add_run(session, start=1, status="running")

    with pytest.raises(PublishValidationError, match="unfinished campaign runs"):
        _select(_results_db(tmp_path, add))


def test_staged_database_rejects_superseded_or_ineligible_rows(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _add_run(session)
        _add_run(session, start=1)

    path = _results_db(tmp_path, add)
    assert _select(path) == {2}
    with pytest.raises(PublishValidationError, match="outside the selected current campaign"):
        _validate(path)


@pytest.mark.parametrize("latest_status", ["completed", "failed"])
def test_campaign_publication_replaces_prior_data_and_preserves_source_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, latest_status: str
) -> None:
    latest_commit = "failed-select" if latest_status == "failed" else "completed-select"

    def add(session: Session) -> None:
        _add_run(session, POPULATE, commit="superseded-load")
        _add_run(session, start=1, commit="superseded-select")
        _add_run(session, POPULATE, start=2, status="failed", commit="load-commit")
        _add_run(session, start=3, measured=JSONBENCH_QUERY_NAMES[:1], commit="select-commit")
        run_id = _add_run(
            session,
            start=4,
            status=latest_status,
            commit=latest_commit,
            measured=JSONBENCH_QUERY_NAMES[:1],
            recorded=JSONBENCH_QUERY_NAMES[:1] if latest_status == "failed" else None,
            iterations=(1, 2) if latest_status == "failed" else (1, 2, 3, 4),
        )
        if latest_status == "failed":
            _fail_step(session, run_id, "timeout", iteration=2)
        invalid_id = _add_run(session, start=5, commit="invalid-select")
        invalid = session.get(Run, invalid_id)
        assert invalid is not None
        invalid.metadata_json = {**(invalid.metadata_json or {}), "invalidation": {"reason": "Incomplete input"}}
        stale_id = _add_run(session, start=6, commit="stale-select")
        stale = session.get(Run, stale_id)
        assert stale is not None
        stale.db_version = "old"

    source = _results_db(tmp_path / "source", add)
    previous = _results_db(
        tmp_path / "previous", lambda session: _add_run(session, MatrixCell("duckdb", "tpc_h", 10, "populate"))
    )
    published = tmp_path / "published"
    published.mkdir()
    shutil.copy2(previous, published / "results.db")
    source_digest = hashlib.sha256(source.read_bytes()).hexdigest()
    monkeypatch.setattr(get_settings(), "results_directory", source.parent)
    monkeypatch.setattr(results_module, "PUBLISHED_DATA_DIRECTORY", published)
    monkeypatch.setattr(results_module, "campaign_cells", lambda: CELLS)
    monkeypatch.setattr(results_module, "current_pins", lambda: PINS)
    monkeypatch.setattr(results_module, "current_drivers", lambda: DRIVERS)
    report_url = "https://example.com/campaign-report"

    output, merge_stats = results_module.publish(revision="campaign", campaign=True, report_url=report_url)

    assert output == published
    assert merge_stats is None
    assert hashlib.sha256(source.read_bytes()).hexdigest() == source_digest
    manifest = json.loads((published / "manifest.json").read_text())
    assert manifest["merged"] is False
    assert manifest["source"] == source.as_posix()
    assert manifest["publication"] == {
        "kind": "campaign",
        "title": "Benchmark campaign: campaign",
        "report_url": report_url,
        "select_cohorts": 1,
        "completed_select_cohorts": int(latest_status == "completed"),
        "failed_select_cohorts": int(latest_status == "failed"),
        "partial_select_cohorts": 1,
        "source_commits": sorted(["load-commit", latest_commit]),
    }
    assert manifest["table_counts"]["run"] == 2
    assert manifest["results_db_sha256"] == hashlib.sha256((published / "results.db").read_bytes()).hexdigest()
    engine = get_results_engine(db_path=published / "results.db", read_only=True)
    try:
        with Session(engine) as session:
            runs = session.scalars(select(Run).order_by(Run.id)).all()
            assert [(run.id, run.suite, run.operation, run.status) for run in runs] == [
                (3, "jsonbench", "populate", "failed"),
                (5, "jsonbench", "select", latest_status),
            ]
            assert runs[0].error_type == "TimeoutError"
            assert runs[0].error_message == "Operation budget exceeded"
            assert runs[0].metadata_json is not None
            assert runs[0].metadata_json["repository"] == {"commit": "load-commit", "dirty": False}
            assert runs[1].metadata_json is not None
            assert runs[1].metadata_json["repository"] == {"commit": latest_commit, "dirty": False}
            assert runs[1].metadata_json["plan"]["query_names"] == list(JSONBENCH_QUERY_NAMES)
            steps = session.scalars(select(RunStep).where(RunStep.run_id == 5).order_by(RunStep.id)).all()
            if latest_status == "failed":
                assert [(step.iteration, step.status, step.result_status) for step in steps] == [
                    (1, "completed", "ok"),
                    (2, "failed", "timeout"),
                ]
                assert steps[0].metadata_json == {"answer_hash": "hash"}
                assert steps[1].metadata_json == {"duration_ms": 12.5}
                assert steps[1].error_type == "TimeoutError"
                assert runs[1].error_type == "TimeoutError"
                assert runs[1].error_message == "Operation budget exceeded"
    finally:
        engine.dispose()
    comparison = json.loads((published / "comparison.json").read_text())
    family = next(family for family in comparison["families"] if family["suite"] == "jsonbench")
    assert family["coverage_query_names"] == list(JSONBENCH_QUERY_NAMES)
    assert any(item["db"] == "duckdb" and item["suite"] == "jsonbench" for item in comparison["registrations"])
    assert not (published / "results.db.staging").exists()


def test_publication_metadata_distinguishes_failed_and_partial_cohorts(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _add_run(session)
        _add_run(session, start=1, measured=JSONBENCH_QUERY_NAMES[:1])
        _add_run(session, start=2, status="failed", recorded=())
        _add_run(session, start=3, status="failed")

    path = _results_db(tmp_path, add)
    metadata = results_module._publication_metadata(path, "test", "https://example.com/report")
    assert metadata["select_cohorts"] == 4
    assert metadata["completed_select_cohorts"] == 2
    assert metadata["failed_select_cohorts"] == 2
    assert metadata["partial_select_cohorts"] == 3


def test_conflicting_answers_in_failed_runs_do_not_replace_published_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    other = MatrixCell("polars", "jsonbench", 10, "select")

    def add(session: Session) -> None:
        _add_run(session)
        run_id = _add_run(session, other, status="failed")
        step = _query_step(session, run_id, iteration=2)
        step.metadata_json = {"answer_hash": "conflicting"}

    source = _results_db(tmp_path / "source", add)
    before = source.read_bytes()
    published = tmp_path / "published"
    published.mkdir()
    for name in ("results.db", "manifest.json", "comparison.json"):
        (published / name).write_text("previous publication")
    monkeypatch.setattr(get_settings(), "results_directory", source.parent)
    monkeypatch.setattr(results_module, "PUBLISHED_DATA_DIRECTORY", published)
    monkeypatch.setattr(results_module, "campaign_cells", lambda: [SELECT, other])
    monkeypatch.setattr(results_module, "current_pins", lambda: PINS)
    monkeypatch.setattr(results_module, "current_drivers", lambda: DRIVERS)

    with pytest.raises(PublishValidationError, match="Answer validation failed"):
        results_module.publish(revision="campaign", campaign=True, report_url="https://example.com/report")

    assert source.read_bytes() == before
    for name in ("results.db", "manifest.json", "comparison.json"):
        assert (published / name).read_text() == "previous publication"
    assert not (published / "results.db.staging").exists()


@pytest.mark.parametrize("status", ["completed", "failed"])
@pytest.mark.parametrize("image_digest", [None, "monetdb/monetdb@sha256:" + "a" * 64])
def test_failed_monetdb_select_keeps_container_provenance_without_claiming_clean_session_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, status: str, image_digest: str | None
) -> None:
    cell = MatrixCell("monetdb", "rtabench", 1, "select")
    names = _current_query_names(cell)
    monkeypatch.setitem(PINS, "monetdb", "SP4")
    monkeypatch.setitem(DRIVERS, "monetdb", "adbc")

    def add(session: Session) -> None:
        run_id = _add_run(
            session, cell, status=status, plan=names, measured=names, recorded=() if status == "failed" else names
        )
        run = session.get(Run, run_id)
        assert run is not None and run.metadata_json is not None
        run.metadata_json = {
            **run.metadata_json,
            "execution": {
                "mode": "container",
                "container_images": {"server": "monetdb/monetdb:SP4"},
                "container_image_digests": {"server": image_digest},
            },
        }

    path = _results_db(tmp_path, add)
    if image_digest is None:
        with pytest.raises(PublishValidationError, match="without a registry digest"):
            _validate_publication(path, [cell])
    elif status == "completed":
        with pytest.raises(PublishValidationError, match="lack clean session baselines"):
            _validate_publication(path, [cell])
    else:
        _validate_publication(path, [cell])
