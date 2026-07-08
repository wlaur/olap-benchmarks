from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, cast

import duckdb
import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..results import (
    delete_runs_by_status,
    get_results_engine,
    get_results_head_revision,
    mark_running_runs_failed,
    migrate_results,
    rename_database,
)
from ..results.models import QueryExecution, Run, RunMetric, RunStep
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
            session.add(
                QueryExecution(
                    run_id=run.id,
                    run_step_id=None,
                    query="select 1",
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                )
            )
            session.commit()

            run.status = "completed"
            run.finished_at = datetime.now()
            session.commit()

            assert session.get(Run, run.id) is not None
            assert session.get(Run, run.id).status == "completed"  # pyright: ignore[reportOptionalMemberAccess]
            assert session.get(Run, run.id).suite_scale_factor == 1  # pyright: ignore[reportOptionalMemberAccess]

            with engine.begin() as connection:
                head_revision = connection.exec_driver_sql("select version_num from alembic_version").scalar_one()
                assert head_revision == get_results_head_revision()

                scale_column = next(
                    row
                    for row in connection.exec_driver_sql("pragma table_info('run')").mappings()
                    if row["name"] == "suite_scale_factor"
                )
                assert scale_column["notnull"] is True
    finally:
        engine.dispose()


def test_run_natural_key_is_unique_but_includes_scale_factor(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    migrate_results(db_path=db_path)
    engine = get_results_engine(read_only=False, db_path=db_path)
    started_at = datetime(2026, 1, 1, 12, 0, 0)

    try:
        with Session(engine) as session:
            session.add(
                Run(
                    suite="time_series",
                    suite_scale_factor=1,
                    db="duckdb",
                    db_version="test",
                    operation="select",
                    system="test",
                    status="completed",
                    started_at=started_at,
                    finished_at=started_at,
                )
            )
            session.commit()

            session.add(
                Run(
                    suite="time_series",
                    suite_scale_factor=1,
                    db="duckdb",
                    db_version="test",
                    operation="select",
                    system="test",
                    status="completed",
                    started_at=started_at,
                    finished_at=started_at,
                )
            )
            with pytest.raises(IntegrityError):
                session.commit()

            session.rollback()
            session.add(
                Run(
                    suite="time_series",
                    suite_scale_factor=2,
                    db="duckdb",
                    db_version="test",
                    operation="select",
                    system="test",
                    status="completed",
                    started_at=started_at,
                    finished_at=started_at,
                )
            )
            session.commit()

            assert session.scalars(select(Run).order_by(Run.suite_scale_factor)).all()[1].suite_scale_factor == 2
    finally:
        engine.dispose()


def test_suite_scale_factor_migration_normalizes_legacy_tpc_suite_names(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    migrate_results(db_path=db_path, target_revision="2f5d7f0e8a21")

    con = cast(Any, duckdb).connect(str(db_path))
    try:
        con.execute(
            """
            insert into run (
                suite, db, db_version, operation, system, status, started_at
            ) values
                ('tpch', 'duckdb', 'test', 'select', 'test', 'completed', timestamp '2026-01-01 00:00:00'),
                ('tpch_sf10', 'duckdb', 'test', 'select', 'test', 'completed', timestamp '2026-01-02 00:00:00'),
                ('tpch_sf50', 'duckdb', 'test', 'select', 'test', 'completed', timestamp '2026-01-03 00:00:00'),
                ('tpcds', 'duckdb', 'test', 'select', 'test', 'completed', timestamp '2026-01-04 00:00:00'),
                ('tpcds_sf1', 'duckdb', 'test', 'select', 'test', 'completed', timestamp '2026-01-05 00:00:00')
            """
        )
        con.close()

        migrate_results(db_path=db_path)

        con = cast(Any, duckdb).connect(str(db_path), read_only=True)
        rows = con.execute("select suite, suite_scale_factor from run order by started_at").fetchall()
        assert rows == [
            ("tpc_h", 10),
            ("tpc_h", 10),
            ("tpc_h", 50),
            ("tpc_ds", 1),
            ("tpc_ds", 1),
        ]

        scale_column = next(
            row for row in con.execute("pragma table_info('run')").fetchall() if row[1] == "suite_scale_factor"
        )
        assert scale_column[3] is True
    finally:
        con.close()


def test_methodology_metadata_migration_backfills_step_fields(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    migrate_results(db_path=db_path, target_revision="4d9c2b7e6f10")

    con = cast(Any, duckdb).connect(str(db_path))
    try:
        con.execute(
            """
            insert into run (
                suite, suite_scale_factor, db, db_version, operation, system, status, started_at
            ) values
                ('time_series', 1, 'duckdb', 'test', 'select', 'test', 'completed', timestamp '2026-01-01 00:00:00')
            """
        )
        con.execute(
            """
            insert into run_step (
                run_id, step_type, step_name, query_name, iteration, started_at, status
            ) values
                (1, 'query', 'query', 'q1', 1, timestamp '2026-01-01 00:00:01', 'completed'),
                (1, 'query', 'query', 'q2', 2, timestamp '2026-01-01 00:00:02', 'failed'),
                (1, 'phase', 'restart', null, null, timestamp '2026-01-01 00:00:03', 'completed')
            """
        )
        con.close()

        migrate_results(db_path=db_path)

        con = cast(Any, duckdb).connect(str(db_path), read_only=True)
        run_columns = {row[1] for row in con.execute("pragma table_info('run')").fetchall()}
        step_columns = {row[1] for row in con.execute("pragma table_info('run_step')").fetchall()}
        rows = con.execute(
            """
            select query_name, result_status, iteration_role
            from run_step
            order by id
            """
        ).fetchall()

        assert "metadata" in run_columns
        assert {"result_status", "iteration_role"}.issubset(step_columns)
        assert rows == [
            ("q1", "ok", "first_run"),
            ("q2", "error", "warm"),
            (None, None, None),
        ]
    finally:
        con.close()


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
            failed_run_id = failed_run.id
            failed_run.status = "failed"
            failed_run.finished_at = datetime.now()

            running_run = _insert_run(session, suite="time_series", db="monetdb")
            running_run_id = running_run.id
            completed_run = _insert_run(session, suite="clickbench", db="duckdb")
            completed_run_id = completed_run.id
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
                    QueryExecution(
                        run_id=failed_run_id,
                        run_step_id=None,
                        query="select 1",
                        start_time=datetime.now(),
                        end_time=datetime.now(),
                    ),
                    QueryExecution(
                        run_id=running_run_id,
                        run_step_id=None,
                        query="select 2",
                        start_time=datetime.now(),
                        end_time=datetime.now(),
                    ),
                    QueryExecution(
                        run_id=completed_run_id,
                        run_step_id=None,
                        query="select 3",
                        start_time=datetime.now(),
                        end_time=datetime.now(),
                    ),
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

            remaining_query_run_ids = session.scalars(select(QueryExecution.run_id).order_by(QueryExecution.id)).all()
            assert remaining_query_run_ids == [completed_run_id]
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
            running_run_id = running_run.id

            failed_run = _insert_run(session, suite="time_series", db="monetdb")
            failed_run_id = failed_run.id
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
                    QueryExecution(
                        run_id=running_run_id,
                        run_step_id=None,
                        query="select 1",
                        start_time=datetime.now(),
                        end_time=datetime.now(),
                    ),
                    QueryExecution(
                        run_id=failed_run_id,
                        run_step_id=None,
                        query="select 2",
                        start_time=datetime.now(),
                        end_time=datetime.now(),
                    ),
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

            remaining_query_run_ids = session.scalars(select(QueryExecution.run_id).order_by(QueryExecution.id)).all()
            assert remaining_query_run_ids == [failed_run_id]
    finally:
        engine.dispose()
