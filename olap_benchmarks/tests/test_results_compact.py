from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, cast

import duckdb
from sqlalchemy.orm import Session

from ..results import compact_results, get_results_engine, migrate_results
from ..results.models import QueryExecution, Run, RunMetric, RunStep


def _insert_run_tree(session: Session, db: str, started_at: datetime) -> int:
    run = Run(
        suite="rtabench",
        suite_scale_factor=1,
        db=db,
        db_version="1.0",
        operation="select",
        system="test-system",
        status="completed",
        started_at=started_at,
        metadata_json={"source": db},
    )
    session.add(run)
    session.commit()

    step = RunStep(
        run_id=run.id,
        step_type="query",
        step_name="query",
        query_name="q1",
        started_at=started_at,
        status="completed",
        result_status="ok",
        iteration_role="first_run",
    )
    session.add(step)
    session.commit()

    session.add(RunMetric(run_id=run.id, time=started_at, cpu_percent=1.0, mem_mb=10, disk_mb=100))
    session.add(
        QueryExecution(
            run_id=run.id,
            run_step_id=step.id,
            query="select 1",
            start_time=started_at,
            end_time=started_at,
        )
    )
    session.commit()

    return run.id


def _populate(db_path: Path, db: str, started_at: datetime) -> int:
    engine = get_results_engine(read_only=False, db_path=db_path)
    try:
        with Session(engine) as session:
            return _insert_run_tree(session, db=db, started_at=started_at)
    finally:
        engine.dispose()


def _query(db_path: Path, sql: str) -> list[tuple[Any, ...]]:
    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(db_path), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_compact_preserves_data_and_sequences(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    migrate_results(db_path=db_path)

    _populate(db_path, db="monetdb", started_at=datetime(2026, 1, 1))
    last_id = _populate(db_path, db="duckdb", started_at=datetime(2026, 1, 2))

    counts_before = {
        table: _query(db_path, f"select count(*) from {table}")[0][0]
        for table in ("run", "run_step", "run_metric", "query_execution")
    }
    revision_before = _query(db_path, "select version_num from alembic_version")[0][0]

    compacted_path, _, _ = compact_results(db_path=db_path)

    assert compacted_path == db_path.resolve()
    assert not db_path.with_name(f"{db_path.name}.compact").exists()

    for table, count in counts_before.items():
        assert _query(db_path, f"select count(*) from {table}")[0][0] == count

    assert _query(db_path, "select version_num from alembic_version")[0][0] == revision_before
    assert _query(db_path, "select db from run order by id")[0][0] == "monetdb"

    new_id = _populate(db_path, db="clickhouse", started_at=datetime(2026, 1, 3))
    assert new_id == last_id + 1


def test_compact_reclaims_space(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    migrate_results(db_path=db_path)
    _populate(db_path, db="monetdb", started_at=datetime(2026, 1, 1))

    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(db_path))
    try:
        con.execute("create table bloat as select range from range(2_000_000)")
        con.execute("drop table bloat")
        con.execute("checkpoint")
    finally:
        con.close()

    _, size_before, size_after = compact_results(db_path=db_path)

    assert size_after < size_before
    assert _query(db_path, "select count(*) from run")[0][0] == 1
