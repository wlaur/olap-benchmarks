from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..results import (
    delete_runs_by_status,
    get_results_engine,
    get_results_head_revision,
    mark_running_runs_failed,
    migrate_results,
    rename_database,
)
from ..results.models import Run, RunMetric, RunStep
from ..results.schema import ensure_results_schema


def _insert_run(session: Session, suite: str, db: str = "monetdb") -> Run:
    run = Run(
        suite=suite,
        db=db,
        db_version="test",
        operation="populate",
        system="test",
        status="running",
        started_at=datetime.now(),
    )
    session.add(run)
    session.commit()
    return run


def test_run_update_succeeds_with_related_rows_after_migration(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    migrate_results(db_path=db_path)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        ensure_results_schema(engine)

        with Session(engine) as session:
            run = _insert_run(session, suite="time_series")

            session.add(
                RunStep(
                    run_id=run.id,
                    step_type="phase",
                    step_name="populate",
                    started_at=datetime.now(),
                    status="running",
                )
            )
            session.add(
                RunMetric(
                    run_id=run.id,
                    time=datetime.now(),
                    cpu_percent=0.0,
                    mem_mb=0,
                    disk_mb=0,
                )
            )
            session.commit()

            run.status = "completed"
            run.finished_at = datetime.now()
            session.commit()

            assert session.get(Run, run.id) is not None
            assert session.get(Run, run.id).status == "completed"  # pyright: ignore[reportOptionalMemberAccess]

            with engine.begin() as connection:
                head_revision = connection.exec_driver_sql("select version_num from alembic_version").scalar_one()
            assert head_revision == get_results_head_revision()
    finally:
        engine.dispose()


def test_ensure_results_schema_initializes_new_db_with_alembic_head(tmp_path: Path) -> None:
    engine = get_results_engine(read_only=False, db_path=tmp_path / "results.db")

    try:
        ensure_results_schema(engine)

        with engine.begin() as connection:
            head_revision = connection.exec_driver_sql("select version_num from alembic_version").scalar_one()
            assert head_revision == get_results_head_revision()
    finally:
        engine.dispose()


def test_rename_database_updates_run_db_only(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    migrate_results(db_path=db_path)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        ensure_results_schema(engine)

        with Session(engine) as session:
            renamed_run = _insert_run(session, suite="time_series", db="timescaledb")
            renamed_run_id = renamed_run.id
            _insert_run(session, suite="clickbench", db="timescaledb")
            untouched_db = "monetdb"
            untouched_suite = "rtabench"
            _insert_run(session, suite=untouched_suite, db=untouched_db)

            session.add(
                RunStep(
                    run_id=renamed_run.id,
                    step_type="query",
                    step_name="query",
                    query_name="001_max_time_small_wide",
                    started_at=datetime.now(),
                    status="completed",
                )
            )
            session.commit()

        renamed_runs = rename_database("timescaledb", "timescale", db_path=db_path)
        assert renamed_runs == 2

        with Session(engine) as session:
            renamed_dbs = session.scalars(select(Run.db).order_by(Run.id)).all()
            assert renamed_dbs == ["timescale", "timescale", untouched_db]

            suites = session.scalars(select(Run.suite).order_by(Run.id)).all()
            assert suites == ["time_series", "clickbench", untouched_suite]

            query_name = session.scalar(select(RunStep.query_name).where(RunStep.run_id == renamed_run_id))
            assert query_name == "001_max_time_small_wide"
    finally:
        engine.dispose()


def test_rename_database_errors_when_old_name_missing(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    migrate_results(db_path=db_path)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        ensure_results_schema(engine)

        with Session(engine) as session:
            _insert_run(session, suite="time_series")

        with pytest.raises(SystemExit, match="Database 'timescaledb' not found"):
            rename_database("timescaledb", "timescale", db_path=db_path)
    finally:
        engine.dispose()


def test_rename_database_errors_when_new_name_exists(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    migrate_results(db_path=db_path)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        ensure_results_schema(engine)

        with Session(engine) as session:
            _insert_run(session, suite="time_series", db="timescaledb")
            _insert_run(session, suite="clickbench", db="monetdb")

        with pytest.raises(SystemExit, match="Database 'monetdb' already exists"):
            rename_database("timescaledb", "monetdb", db_path=db_path)
    finally:
        engine.dispose()


def test_mark_running_runs_failed_marks_runs_and_steps_failed(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    migrate_results(db_path=db_path)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        ensure_results_schema(engine)

        with Session(engine) as session:
            running_run = _insert_run(session, suite="time_series", db="timescaledb")
            running_run_id = running_run.id
            completed_run = _insert_run(session, suite="time_series", db="monetdb")
            completed_run_id = completed_run.id
            completed_run.status = "completed"
            completed_run.finished_at = datetime.now()

            session.add(
                RunStep(
                    run_id=running_run.id,
                    step_type="phase",
                    step_name="populate",
                    started_at=datetime.now(),
                    status="running",
                )
            )
            session.add(
                RunStep(
                    run_id=completed_run.id,
                    step_type="phase",
                    step_name="populate",
                    started_at=datetime.now(),
                    finished_at=datetime.now(),
                    status="completed",
                )
            )
            session.commit()

        failed_runs = mark_running_runs_failed(db_path=db_path)
        assert failed_runs == 1

        with Session(engine) as session:
            statuses = session.execute(select(Run.id, Run.status, Run.error_type).order_by(Run.id)).all()
            assert statuses[0] == (running_run_id, "failed", "KeyboardInterrupt")
            assert statuses[1] == (completed_run_id, "completed", None)

            step_statuses = session.execute(
                select(RunStep.run_id, RunStep.status, RunStep.error_type).order_by(RunStep.run_id),
            ).all()
            assert step_statuses[0] == (running_run_id, "failed", "KeyboardInterrupt")
            assert step_statuses[1] == (completed_run_id, "completed", None)
    finally:
        engine.dispose()


def test_delete_runs_by_status_failed_deletes_failed_and_running_runs(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    migrate_results(db_path=db_path)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        ensure_results_schema(engine)

        with Session(engine) as session:
            failed_run = _insert_run(session, suite="time_series", db="timescaledb")
            failed_run.status = "failed"
            failed_run.finished_at = datetime.now()

            running_run = _insert_run(session, suite="time_series", db="monetdb")
            completed_run = _insert_run(session, suite="clickbench", db="duckdb")
            completed_run.status = "completed"
            completed_run.finished_at = datetime.now()

            session.add_all(
                [
                    RunStep(
                        run_id=failed_run.id,
                        step_type="phase",
                        step_name="populate",
                        started_at=datetime.now(),
                        finished_at=datetime.now(),
                        status="failed",
                    ),
                    RunStep(
                        run_id=running_run.id,
                        step_type="phase",
                        step_name="populate",
                        started_at=datetime.now(),
                        status="running",
                    ),
                    RunStep(
                        run_id=completed_run.id,
                        step_type="phase",
                        step_name="populate",
                        started_at=datetime.now(),
                        finished_at=datetime.now(),
                        status="completed",
                    ),
                    RunMetric(run_id=failed_run.id, time=datetime.now(), cpu_percent=0.0, mem_mb=0, disk_mb=0),
                    RunMetric(run_id=running_run.id, time=datetime.now(), cpu_percent=0.0, mem_mb=0, disk_mb=0),
                    RunMetric(run_id=completed_run.id, time=datetime.now(), cpu_percent=0.0, mem_mb=0, disk_mb=0),
                ]
            )
            session.commit()

        deleted_runs = delete_runs_by_status("failed", db_path=db_path)
        assert deleted_runs == 2

        with Session(engine) as session:
            remaining_run_statuses = session.scalars(select(Run.status).order_by(Run.id)).all()
            assert remaining_run_statuses == ["completed"]

            remaining_step_statuses = session.scalars(select(RunStep.status).order_by(RunStep.id)).all()
            assert remaining_step_statuses == ["completed"]

            remaining_metric_run_ids = session.scalars(select(RunMetric.run_id).order_by(RunMetric.id)).all()
            assert len(remaining_metric_run_ids) == 1
    finally:
        engine.dispose()


def test_delete_runs_by_status_orphaned_deletes_running_runs_only(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    migrate_results(db_path=db_path)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        ensure_results_schema(engine)

        with Session(engine) as session:
            running_run = _insert_run(session, suite="time_series", db="timescaledb")

            failed_run = _insert_run(session, suite="time_series", db="monetdb")
            failed_run.status = "failed"
            failed_run.finished_at = datetime.now()

            session.add_all(
                [
                    RunStep(
                        run_id=running_run.id,
                        step_type="phase",
                        step_name="populate",
                        started_at=datetime.now(),
                        status="running",
                    ),
                    RunStep(
                        run_id=failed_run.id,
                        step_type="phase",
                        step_name="populate",
                        started_at=datetime.now(),
                        finished_at=datetime.now(),
                        status="failed",
                    ),
                    RunMetric(run_id=running_run.id, time=datetime.now(), cpu_percent=0.0, mem_mb=0, disk_mb=0),
                    RunMetric(run_id=failed_run.id, time=datetime.now(), cpu_percent=0.0, mem_mb=0, disk_mb=0),
                ]
            )
            session.commit()

        deleted_runs = delete_runs_by_status("orphaned", db_path=db_path)
        assert deleted_runs == 1

        with Session(engine) as session:
            remaining_run_statuses = session.scalars(select(Run.status).order_by(Run.id)).all()
            assert remaining_run_statuses == ["failed"]

            remaining_step_statuses = session.scalars(select(RunStep.status).order_by(RunStep.id)).all()
            assert remaining_step_statuses == ["failed"]

            remaining_metric_run_ids = session.scalars(select(RunMetric.run_id).order_by(RunMetric.id)).all()
            assert len(remaining_metric_run_ids) == 1
    finally:
        engine.dispose()
