from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from ..results import get_results_engine, migrate_results
from ..results.models import Run, RunStep
from ..results.validation import (
    AnswerHashValidationError,
    RowCountValidationError,
    assert_latest_query_answer_hashes,
    assert_latest_query_row_counts,
    validate_latest_query_answer_hashes,
    validate_latest_query_row_counts,
)


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
    assert_latest_query_answer_hashes(db_path=db_path)


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
    _insert_select_run(db_path, "monetdb", 11, db_driver="staged", answer_hash="hash-b")

    row_mismatches = validate_latest_query_row_counts(db_path=db_path)

    assert len(row_mismatches) == 1
    assert [(row.db_driver, row.row_count) for row in row_mismatches[0].observations] == [
        ("adbc", 10),
        ("staged", 11),
    ]


def test_answer_hash_validation_compares_variants_of_the_same_database(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "monetdb", 10, db_driver="adbc", answer_hash="hash-a")
    _insert_select_run(db_path, "monetdb", 10, db_driver="staged", answer_hash="hash-b")

    hash_mismatches = validate_latest_query_answer_hashes(db_path=db_path)

    assert len(hash_mismatches) == 1
    assert [(row.db_driver, row.answer_hash) for row in hash_mismatches[0].observations] == [
        ("adbc", "hash-a"),
        ("staged", "hash-b"),
    ]


def test_answer_hash_validation_marks_consensus_outlier_wrong_result(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _create_results_db(db_path)
    _insert_select_run(db_path, "duckdb", 10, answer_hash="hash-a")
    _insert_select_run(db_path, "clickhouse", 10, answer_hash="hash-a")
    _insert_select_run(db_path, "timescaledb", 10, answer_hash="hash-b")

    with pytest.raises(AnswerHashValidationError, match="timescaledb"):
        assert_latest_query_answer_hashes(db_path=db_path, mark_wrong_results=True)

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
    assert_latest_query_row_counts(db_path=db_path)


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
    with pytest.raises(RowCountValidationError, match="q1 iteration 1"):
        assert_latest_query_row_counts(db_path=db_path)


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

    with pytest.raises(RowCountValidationError, match="timescaledb"):
        assert_latest_query_row_counts(db_path=db_path, mark_wrong_results=True)

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

    with pytest.raises(RowCountValidationError, match="q1 iteration 1"):
        assert_latest_query_row_counts(db_path=db_path, mark_wrong_results=True)

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
