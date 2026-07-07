from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from ..results import get_results_engine, migrate_results
from ..results.models import Run, RunStep
from ..results.validation import (
    RowCountValidationError,
    assert_latest_query_row_counts,
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
    system: str = "test-system",
    query_name: str = "q1",
    iteration: int = 1,
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
                    row_count=row_count,
                )
            )
            session.commit()
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
