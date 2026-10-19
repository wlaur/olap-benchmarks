from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from ..results import get_results_engine, migrate_results, validation
from ..results.models import Run, RunStep
from ..results.validation import (
    AnswerHashMismatch,
    RowCountMismatch,
    clear_wrong_result_marks,
    validate_results,
)


def validate_latest_query_row_counts(db_path: Path) -> list[RowCountMismatch]:
    return validate_results(db_path=db_path).row_count_mismatches


def validate_latest_query_answer_hashes(db_path: Path) -> list[AnswerHashMismatch]:
    return validate_results(db_path=db_path).answer_hash_mismatches


def _create_results_db(db_path: Path) -> None:
    migrate_results(db_path=db_path)


def _insert_select_run(
    db_path: Path,
    db: str,
    row_count: int,
    *,
    suite: str = "time_series",
    suite_scale_factor: int = 1,
    db_version: str = "1.0",
    db_driver: str | None = None,
    system: str = "test-system",
    query_name: str = "q1",
    iteration: int = 1,
    result_status: str | None = "ok",
    answer_hash: str | None = None,
    started_at: datetime = datetime(2026, 1, 1, 12, 0, 0),
    finished_at: datetime = datetime(2026, 1, 1, 12, 0, 1),
) -> None:
    engine = get_results_engine(read_only=False, db_path=db_path)
    try:
        with Session(engine) as session:
            run = Run(
                benchmark_profile="resident-hot-v1",
                suite=suite,
                suite_scale_factor=suite_scale_factor,
                db=db,
                db_version=db_version,
                db_driver=db_driver,
                operation="select",
                system=system,
                status="completed",
                started_at=started_at,
                finished_at=finished_at,
            )
            session.add(run)
            session.flush()
            session.add(
                RunStep(
                    run_id=run.id,
                    step_type="query",
                    step_name="query",
                    query_name=query_name,
                    iteration=iteration,
                    started_at=started_at,
                    finished_at=finished_at,
                    status="completed",
                    result_status=result_status,
                    row_count=row_count,
                    metadata_json={"answer_hash": answer_hash} if answer_hash is not None else None,
                )
            )
            session.commit()
    finally:
        engine.dispose()


def test_answer_hash_validation_accepts_matching_latest_runs(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10, answer_hash="hash-a")
    _insert_select_run(db_path, "clickhouse", 10, answer_hash="hash-a")

    assert validate_latest_query_answer_hashes(db_path=db_path) == []
    assert validate_results(db_path=db_path).ok


def test_validation_excludes_explicitly_invalidated_experiments(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10, answer_hash="hash-a")
    _insert_select_run(db_path, "clickhouse", 10, answer_hash="hash-a")
    _insert_select_run(db_path, "postgres", 12, answer_hash="incomplete-input")
    assert not validate_results(db_path=db_path).ok

    engine = get_results_engine(read_only=False, db_path=db_path)
    try:
        with Session(engine) as session:
            run = session.query(Run).filter(Run.db == "postgres").one()
            run.metadata_json = {"invalidation": {"reason": "Required input table was incomplete"}}
            session.commit()
    finally:
        engine.dispose()

    assert validate_results(db_path=db_path, mark=True).ok


def test_answer_hash_validation_ignores_row_count_mismatches(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10, answer_hash="hash-a")
    _insert_select_run(db_path, "clickhouse", 12, answer_hash="hash-b")

    assert validate_latest_query_answer_hashes(db_path=db_path) == []


def test_validation_compares_variants_of_the_same_database(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "monetdb", 10, db_driver="adbc", answer_hash="hash-a")
    _insert_select_run(db_path, "monetdb", 11, db_driver="native", answer_hash="hash-b")

    row_mismatches = validate_latest_query_row_counts(db_path=db_path)

    assert len(row_mismatches) == 1
    assert [(row.db_driver, row.row_count) for row in row_mismatches[0].observations] == [
        ("adbc", 10),
        ("native", 11),
    ]


def test_answer_hash_validation_compares_variants_of_the_same_database(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "monetdb", 10, db_driver="adbc", answer_hash="hash-a")
    _insert_select_run(db_path, "monetdb", 10, db_driver="native", answer_hash="hash-b")

    hash_mismatches = validate_latest_query_answer_hashes(db_path=db_path)

    assert len(hash_mismatches) == 1
    assert [(row.db_driver, row.answer_hash) for row in hash_mismatches[0].observations] == [
        ("adbc", "hash-a"),
        ("native", "hash-b"),
    ]


def test_answer_hash_validation_marks_consensus_outlier_wrong_result(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10, answer_hash="hash-a")
    _insert_select_run(db_path, "clickhouse", 10, answer_hash="hash-a")
    _insert_select_run(db_path, "timescaledb", 10, answer_hash="hash-b")

    report = validate_results(db_path=db_path, mark=True)
    assert "timescaledb" in report.format()
    assert report.marks_applied == 1

    engine = get_results_engine(read_only=True, db_path=db_path)
    try:
        with Session(engine) as session:
            statuses = session.query(Run.db, RunStep.result_status, RunStep.error_type).join(
                RunStep, RunStep.run_id == Run.id
            )
            assert statuses.order_by(Run.db).all() == [
                ("clickhouse", "ok", None),
                ("duckdb", "ok", None),
                ("timescaledb", "wrong_result", "AnswerHashMismatch"),
            ]
    finally:
        engine.dispose()


def test_row_count_validation_accepts_matching_latest_runs(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10)
    _insert_select_run(db_path, "clickhouse", 10)

    assert validate_latest_query_row_counts(db_path=db_path) == []
    assert validate_results(db_path=db_path).ok


def test_row_count_validation_reports_cross_engine_mismatch(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10)
    _insert_select_run(db_path, "clickhouse", 12)

    mismatches = validate_latest_query_row_counts(db_path=db_path)

    assert len(mismatches) == 1
    mismatch = mismatches[0]
    assert mismatch.system == "test-system"
    assert mismatch.suite == "time_series"
    assert mismatch.suite_scale_factor == 1
    assert mismatch.query_name == "q1"
    assert mismatch.iteration == 1
    assert [(row.db, row.row_count) for row in mismatch.observations] == [
        ("duckdb", 10),
        ("clickhouse", 12),
    ]
    assert "q1 iteration 1" in validate_results(db_path=db_path).format()


def test_row_count_validation_does_not_compare_across_scale_factors(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10, suite_scale_factor=1)
    _insert_select_run(db_path, "clickhouse", 12, suite_scale_factor=10)

    assert validate_latest_query_row_counts(db_path=db_path) == []


def test_row_count_validation_does_not_compare_across_systems(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10, system="laptop")
    _insert_select_run(db_path, "clickhouse", 12, system="workstation")

    assert validate_latest_query_row_counts(db_path=db_path) == []


def test_row_count_validation_uses_latest_completed_run_per_database(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10, finished_at=datetime(2026, 1, 1, 12, 0, 1))
    _insert_select_run(
        db_path,
        "clickhouse",
        12,
        started_at=datetime(2026, 1, 1, 12, 1, 0),
        finished_at=datetime(2026, 1, 1, 12, 1, 1),
    )
    _insert_select_run(
        db_path,
        "clickhouse",
        10,
        started_at=datetime(2026, 1, 1, 12, 2, 0),
        finished_at=datetime(2026, 1, 1, 12, 2, 1),
    )

    assert validate_latest_query_row_counts(db_path=db_path) == []


def test_row_count_validation_uses_latest_version_per_database(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(
        db_path,
        "duckdb",
        12,
        db_version="old",
        started_at=datetime(2026, 1, 1, 12, 0, 0),
        finished_at=datetime(2026, 1, 1, 12, 0, 1),
    )
    _insert_select_run(
        db_path,
        "duckdb",
        10,
        db_version="new",
        started_at=datetime(2026, 1, 1, 12, 1, 0),
        finished_at=datetime(2026, 1, 1, 12, 1, 1),
    )
    _insert_select_run(db_path, "clickhouse", 10)

    assert validate_latest_query_row_counts(db_path=db_path) == []


def test_row_count_validation_reports_latest_version_mismatch(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(
        db_path,
        "duckdb",
        10,
        db_version="old",
        started_at=datetime(2026, 1, 1, 12, 0, 0),
        finished_at=datetime(2026, 1, 1, 12, 0, 1),
    )
    _insert_select_run(
        db_path,
        "duckdb",
        12,
        db_version="new",
        started_at=datetime(2026, 1, 1, 12, 1, 0),
        finished_at=datetime(2026, 1, 1, 12, 1, 1),
    )
    _insert_select_run(db_path, "clickhouse", 10)

    mismatches = validate_latest_query_row_counts(db_path=db_path)

    assert len(mismatches) == 1
    assert [(row.db, row.db_version, row.row_count) for row in mismatches[0].observations] == [
        ("clickhouse", "1.0", 10),
        ("duckdb", "new", 12),
    ]


def test_row_count_validation_marks_consensus_outlier_wrong_result(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10)
    _insert_select_run(db_path, "clickhouse", 10)
    _insert_select_run(db_path, "timescaledb", 12)

    report = validate_results(db_path=db_path, mark=True)
    assert "timescaledb" in report.format()

    engine = get_results_engine(read_only=True, db_path=db_path)
    try:
        with Session(engine) as session:
            statuses = session.query(Run.db, RunStep.result_status, RunStep.error_type).join(
                RunStep, RunStep.run_id == Run.id
            )
            assert statuses.order_by(Run.db).all() == [
                ("clickhouse", "ok", None),
                ("duckdb", "ok", None),
                ("timescaledb", "wrong_result", "RowCountMismatch"),
            ]
    finally:
        engine.dispose()


def test_row_count_validation_does_not_mark_ambiguous_mismatch(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10)
    _insert_select_run(db_path, "clickhouse", 12)

    report = validate_results(db_path=db_path, mark=True)
    assert "q1 iteration 1" in report.format()
    assert report.marks_applied == 0

    engine = get_results_engine(read_only=True, db_path=db_path)
    try:
        with Session(engine) as session:
            statuses = session.query(Run.db, RunStep.result_status, RunStep.error_type).join(
                RunStep, RunStep.run_id == Run.id
            )
            assert statuses.order_by(Run.db).all() == [
                ("clickhouse", "ok", None),
                ("duckdb", "ok", None),
            ]
    finally:
        engine.dispose()


def _step_statuses(db_path: Path) -> list[tuple[str, str, str | None, str | None]]:
    engine = get_results_engine(read_only=True, db_path=db_path)
    try:
        with Session(engine) as session:
            rows = (
                session.query(Run.db, Run.db_version, RunStep.result_status, RunStep.error_type)
                .join(RunStep, RunStep.run_id == Run.id)
                .order_by(Run.db, Run.started_at)
                .all()
            )
            return [(str(db), str(version), status, error_type) for db, version, status, error_type in rows]
    finally:
        engine.dispose()


def test_marking_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10, answer_hash="hash-a")
    _insert_select_run(db_path, "clickhouse", 10, answer_hash="hash-a")
    _insert_select_run(db_path, "timescaledb", 10, answer_hash="hash-b")

    first = validate_results(db_path=db_path, mark=True)
    after_first = _step_statuses(db_path)
    second = validate_results(db_path=db_path, mark=True)

    assert (first.marks_cleared, first.marks_applied) == (0, 1)
    assert (second.marks_cleared, second.marks_applied) == (1, 1)
    assert _step_statuses(db_path) == after_first
    assert ("timescaledb", "1.0", "wrong_result", "AnswerHashMismatch") in after_first


def test_marks_heal_after_a_corrected_rerun(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10, answer_hash="hash-a")
    _insert_select_run(db_path, "clickhouse", 10, answer_hash="hash-a")
    _insert_select_run(db_path, "timescaledb", 10, answer_hash="hash-b")
    validate_results(db_path=db_path, mark=True)

    _insert_select_run(
        db_path,
        "timescaledb",
        10,
        answer_hash="hash-a",
        started_at=datetime(2026, 1, 2, 12, 0, 0),
        finished_at=datetime(2026, 1, 2, 12, 0, 1),
    )
    report = validate_results(db_path=db_path, mark=True)

    assert report.ok
    assert all(status == "ok" for _, _, status, _ in _step_statuses(db_path))


def test_marking_ignores_runs_of_versions_that_are_no_longer_pinned(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10, answer_hash="hash-a", db_version="new")
    _insert_select_run(db_path, "clickhouse", 10, answer_hash="hash-a", db_version="new")
    # a stale run recorded after the pinned one must neither be compared nor tip the consensus
    _insert_select_run(
        db_path,
        "clickhouse",
        10,
        answer_hash="hash-b",
        db_version="old",
        started_at=datetime(2026, 1, 2, 12, 0, 0),
        finished_at=datetime(2026, 1, 2, 12, 0, 1),
    )
    _insert_select_run(db_path, "timescaledb", 10, answer_hash="hash-b", db_version="new")

    unpinned = validate_results(db_path=db_path)
    report = validate_results(
        db_path=db_path, pins={"duckdb": "new", "clickhouse": "new", "timescaledb": "new"}, mark=True
    )

    assert [o.db_version for o in unpinned.answer_hash_mismatches[0].observations].count("old") == 1
    assert report.marks_applied == 1
    assert ("timescaledb", "new", "wrong_result", "AnswerHashMismatch") in _step_statuses(db_path)
    assert ("clickhouse", "old", "ok", None) in _step_statuses(db_path)


def test_clear_restores_ok(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10)
    _insert_select_run(db_path, "clickhouse", 10)
    _insert_select_run(db_path, "timescaledb", 12)
    validate_results(db_path=db_path, mark=True)

    assert clear_wrong_result_marks(db_path=db_path) == 1
    assert all(status == "ok" and error is None for _, _, status, error in _step_statuses(db_path))


def test_iteration_inconsistencies_are_reported(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    engine = get_results_engine(read_only=False, db_path=db_path)
    try:
        with Session(engine) as session:
            run = Run(
                benchmark_profile="resident-hot-v1",
                suite="rtabench",
                suite_scale_factor=1,
                db="clickhouse",
                db_version="1.0",
                operation="select",
                system="test-system",
                status="completed",
                started_at=datetime(2026, 1, 1),
                finished_at=datetime(2026, 1, 1, 0, 1),
            )
            session.add(run)
            session.flush()
            for query_name, iteration, answer_hash in (
                ("stable", 1, "h1"),
                ("stable", 2, "h1"),
                ("flaky", 1, "h1"),
                ("flaky", 2, "h2"),
                ("flaky", 3, "h1"),
            ):
                session.add(
                    RunStep(
                        run_id=run.id,
                        step_type="query",
                        step_name="query",
                        query_name=query_name,
                        iteration=iteration,
                        started_at=datetime(2026, 1, 1),
                        finished_at=datetime(2026, 1, 1),
                        status="completed",
                        result_status="ok",
                        row_count=1,
                        metadata_json={"answer_hash": answer_hash},
                    )
                )
            session.commit()
    finally:
        engine.dispose()

    report = validate_results(db_path=db_path)

    assert not report.ok
    assert [(item.query_name, item.answer_hashes) for item in report.iteration_inconsistencies] == [
        ("flaky", ("h1", "h2"))
    ]
    assert "non-deterministic" in report.format()


def test_answer_comparison_exclusions_skip_a_query_or_one_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    # one suite per query, because a run holds the steps of one suite
    for suite in ("rtabench", "tpc_ds", "tpc_h"):
        for db, answer_hash in (("duckdb", "hash-a"), ("clickhouse", "hash-b"), ("monetdb", "hash-a")):
            _insert_select_run(db_path, db, 10, suite=suite, query_name="q1", answer_hash=answer_hash)
    monkeypatch.setattr(validation, "ANSWER_COMPARISON_EXCLUSIONS", {})
    monkeypatch.setitem(validation.ANSWER_COMPARISON_EXCLUSIONS, ("rtabench", "q1", None), "test")
    monkeypatch.setitem(validation.ANSWER_COMPARISON_EXCLUSIONS, ("tpc_ds", "q1", "clickhouse"), "test")

    mismatches = validate_latest_query_answer_hashes(db_path=db_path)

    assert [mismatch.suite for mismatch in mismatches] == ["tpc_h"]
