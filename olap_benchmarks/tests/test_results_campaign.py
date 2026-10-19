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
from ..results.campaign import campaign_cells, select_campaign_runs, validate_campaign_runs
from ..results.models import Run, RunStep, SystemSnapshot
from ..results.publishing import PublishValidationError
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
        for query in plan:
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


def test_campaign_inventory_covers_every_registration_scale_and_operation() -> None:
    cells = campaign_cells()
    selects = [cell for cell in cells if cell.operation == "select"]

    assert len(selects) == 99
    assert len({cell.db for cell in selects}) == 11
    assert len({cell.suite for cell in selects}) == 10
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


def test_latest_failed_select_does_not_hide_completed_cohort(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _add_run(session, POPULATE)
        _add_run(session, start=1)
        _add_run(session, start=2, status="failed")

    assert _select(_results_db(tmp_path, add)) == {1, 2}


@pytest.mark.parametrize("invalid", ["version", "driver", "profile", "dirty", "commit", "invalidation"])
def test_selection_excludes_ineligible_newer_cohorts(tmp_path: Path, invalid: str) -> None:
    def add(session: Session) -> None:
        _add_run(session, start=1)
        invalid_id = _add_run(session, start=2)
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


def test_every_select_cell_must_have_a_completed_cohort(tmp_path: Path) -> None:
    path = _results_db(tmp_path, lambda session: _add_run(session))
    cells = [*CELLS, MatrixCell("polars", "jsonbench", 10, "select")]

    with pytest.raises(PublishValidationError, match="polars:jsonbench:sf10:select"):
        _select(path, cells)


def test_failed_only_select_is_not_a_completed_partial(tmp_path: Path) -> None:
    path = _results_db(tmp_path, lambda session: _add_run(session, status="failed"))

    with pytest.raises(PublishValidationError, match="without a completed select"):
        _select(path)


def test_all_skipped_select_cannot_claim_measured_coverage(tmp_path: Path) -> None:
    path = _results_db(tmp_path, lambda session: _add_run(session, measured=()))

    with pytest.raises(PublishValidationError, match="no fully measured four-iteration query"):
        _select(path)


@pytest.mark.parametrize("problem", ["empty", "truncated", "iterations"])
def test_recorded_query_plan_must_be_the_full_four_iteration_plan(tmp_path: Path, problem: str) -> None:
    def add(session: Session) -> None:
        _add_run(
            session,
            plan=()
            if problem == "empty"
            else JSONBENCH_QUERY_NAMES[:1]
            if problem == "truncated"
            else JSONBENCH_QUERY_NAMES,
            plan_iterations=3 if problem == "iterations" else 4,
        )

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


def test_campaign_publication_replaces_prior_data_and_preserves_source_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def add(session: Session) -> None:
        _add_run(session, POPULATE, commit="superseded-load")
        _add_run(session, start=1, commit="superseded-select")
        _add_run(session, POPULATE, start=2, status="failed", commit="load-commit")
        _add_run(session, start=3, measured=JSONBENCH_QUERY_NAMES[:1], commit="select-commit")
        _add_run(session, start=4, status="failed", commit="failed-select")
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
        "partial_select_cohorts": 1,
        "source_commits": ["load-commit", "select-commit"],
    }
    assert manifest["table_counts"]["run"] == 2
    assert manifest["results_db_sha256"] == hashlib.sha256((published / "results.db").read_bytes()).hexdigest()
    engine = get_results_engine(db_path=published / "results.db", read_only=True)
    try:
        with Session(engine) as session:
            runs = session.scalars(select(Run).order_by(Run.id)).all()
            assert [(run.id, run.suite, run.operation, run.status) for run in runs] == [
                (3, "jsonbench", "populate", "failed"),
                (4, "jsonbench", "select", "completed"),
            ]
            assert runs[0].error_type == "TimeoutError"
            assert runs[0].error_message == "Operation budget exceeded"
            assert runs[0].metadata_json is not None
            assert runs[0].metadata_json["repository"] == {"commit": "load-commit", "dirty": False}
            assert runs[1].metadata_json is not None
            assert runs[1].metadata_json["repository"] == {"commit": "select-commit", "dirty": False}
            assert runs[1].metadata_json["plan"]["query_names"] == list(JSONBENCH_QUERY_NAMES)
    finally:
        engine.dispose()
    assert not (published / "results.db.staging").exists()
