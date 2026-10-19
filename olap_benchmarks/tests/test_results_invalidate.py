from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from ..results import get_results_engine, invalidate_runs, migrate_results
from ..results.models import QueryExecution, Run, RunMetric, RunStep

STARTED = datetime(2026, 10, 19, 9)
FINISHED = STARTED + timedelta(seconds=10)


@pytest.fixture
def results_db(tmp_path: Path) -> Iterator[tuple[Path, Engine]]:
    path = migrate_results(db_path=tmp_path / "results.db")
    engine = get_results_engine(read_only=False, db_path=path)
    try:
        with Session(engine) as session:
            for run_id, status in enumerate(("completed", "failed", "running", "completed"), start=1):
                session.add(
                    Run(
                        id=run_id,
                        suite="tpc_h",
                        suite_scale_factor=50,
                        db="duckdb",
                        db_version="test",
                        operation="select",
                        system="test",
                        benchmark_profile="resident-hot-v1",
                        status=status,
                        started_at=STARTED + timedelta(seconds=run_id),
                        finished_at=FINISHED if run_id < 3 else None,
                        error_type="QueryTimeout" if status == "failed" else None,
                        error_message="query deadline exceeded" if status == "failed" else None,
                        metadata_json={"source": "diagnostic", "nested": {"original": True}},
                    )
                )
            session.add(
                RunStep(
                    id=1,
                    run_id=1,
                    step_type="query",
                    step_name="query",
                    query_name="07_volume_shipping",
                    iteration=1,
                    started_at=STARTED,
                    finished_at=FINISHED,
                    status="completed",
                    result_status="ok",
                    iteration_role="warmup",
                    row_count=4,
                    metadata_json={"duration_ms": 10000.0, "answer_hash": "diagnostic-hash"},
                )
            )
            session.add(RunMetric(id=1, run_id=1, time=STARTED, client_mem_mb=42, disk_mb=100))
            session.add(
                QueryExecution(
                    id=1,
                    run_id=1,
                    run_step_id=1,
                    query="SELECT * FROM diagnostic_input",
                    start_time=STARTED,
                    end_time=FINISHED,
                )
            )
            session.commit()
        yield path, engine
    finally:
        engine.dispose()


def test_invalidation_preserves_original_failures_and_diagnostic_records(results_db: tuple[Path, Engine]) -> None:
    path, engine = results_db
    with engine.connect() as connection:
        before = [
            connection.execute(select(model.__table__)).mappings().all()
            for model in (RunStep, RunMetric, QueryExecution)
        ]
    before_time = datetime.now(UTC)

    assert invalidate_runs([1, 2, 1], "Input population was incomplete", db_path=path) == 2

    with Session(engine) as session:
        for run_id, original_status in ((1, "completed"), (2, "failed")):
            run = session.get(Run, run_id)
            assert run is not None
            assert run.status == "failed"
            assert run.error_type == ("QueryTimeout" if run_id == 2 else "InvalidBenchmark")
            assert run.error_message == (
                "query deadline exceeded" if run_id == 2 else "Input population was incomplete"
            )
            assert run.started_at == STARTED + timedelta(seconds=run_id)
            assert run.finished_at == FINISHED
            metadata = run.metadata_json
            assert metadata is not None
            assert metadata["source"] == "diagnostic"
            assert metadata["nested"] == {"original": True}
            invalidation = metadata["invalidation"]
            assert invalidation["original_status"] == original_status
            assert invalidation["reason"] == "Input population was incomplete"
            assert before_time <= datetime.fromisoformat(invalidation["time"]) <= datetime.now(UTC)
            assert invalidation["error_type"] == ("QueryTimeout" if run_id == 2 else None)
            assert invalidation["error_message"] == ("query deadline exceeded" if run_id == 2 else None)
    with engine.connect() as connection:
        after = [
            connection.execute(select(model.__table__)).mappings().all()
            for model in (RunStep, RunMetric, QueryExecution)
        ]
    assert after == before


@pytest.mark.parametrize(
    ("run_ids", "reason", "message"),
    [
        ([1, 999], "Invalid input", "Run IDs not found: 999"),
        ([1, 3], "Invalid input", "Only finished runs can be invalidated: 3"),
        ([1, 4], "Invalid input", "Only finished runs can be invalidated: 4"),
        ([], "Invalid input", "at least one run ID"),
        ([1], " \n", "reason is required"),
    ],
)
def test_invalid_selection_changes_nothing(
    results_db: tuple[Path, Engine], run_ids: list[int], reason: str, message: str
) -> None:
    path, engine = results_db
    with engine.connect() as connection:
        before = connection.execute(select(Run.__table__)).mappings().all()

    with pytest.raises(ValueError, match=message):
        invalidate_runs(run_ids, reason, db_path=path)

    with engine.connect() as connection:
        assert connection.execute(select(Run.__table__)).mappings().all() == before


@pytest.mark.parametrize("run_id", [1, 2])
def test_repeated_invalidation_retains_first_provenance(results_db: tuple[Path, Engine], run_id: int) -> None:
    path, engine = results_db
    invalidate_runs([run_id], "Incomplete input", db_path=path)
    with Session(engine) as session:
        run = session.get(Run, run_id)
        assert run is not None and run.metadata_json is not None
        original_invalidation = run.metadata_json["invalidation"]
        original_error = (run.error_type, run.error_message)

    invalidate_runs([run_id], "Incomplete input confirmed", db_path=path)

    with Session(engine) as session:
        run = session.get(Run, run_id)
        assert run is not None and run.metadata_json is not None
        assert run.metadata_json["invalidation"] == original_invalidation
        assert (run.error_type, run.error_message) == original_error


def test_invalidation_accepts_missing_metadata(results_db: tuple[Path, Engine]) -> None:
    path, engine = results_db
    with Session(engine) as session:
        run = session.get(Run, 1)
        assert run is not None
        run.metadata_json = None
        session.commit()

    invalidate_runs([1], "Diagnostic experiment", db_path=path)

    with Session(engine) as session:
        run = session.get(Run, 1)
        assert run is not None and run.metadata_json is not None
        assert run.metadata_json["invalidation"]["original_status"] == "completed"
