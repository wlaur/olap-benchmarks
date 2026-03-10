from __future__ import annotations

from datetime import datetime
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..results import get_results_engine, get_results_head_revision, migrate_results
from ..results_models import Run, RunMetric, RunStep
from ..results_schema import ensure_results_schema


def test_run_update_succeeds_with_related_rows_after_migration(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    migrate_results(db_path=db_path)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        ensure_results_schema(engine)

        with Session(engine) as session:
            run = Run(
                suite="time_series",
                db="monetdb",
                db_version="test",
                operation="populate",
                system="test",
                status="running",
                started_at=datetime.now(),
            )
            session.add(run)
            session.commit()

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

        with Session(engine) as session:
            head_revision = session.execute(text("select version_num from alembic_version")).scalar_one()
            assert head_revision == get_results_head_revision()
    finally:
        engine.dispose()
