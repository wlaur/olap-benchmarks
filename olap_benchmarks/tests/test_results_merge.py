from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, cast

import duckdb
import pytest
from sqlalchemy.orm import Session

from ..results import get_results_engine, get_results_head_revision, migrate_results
from ..results.merge import merge_results
from ..results.models import QueryExecution, Run, RunMetric, RunStep, SystemSnapshot


def _create_results_db(db_path: Path) -> None:
    migrate_results(db_path=db_path)


def _insert_run_tree(
    session: Session,
    suite: str = "rtabench",
    suite_scale_factor: int = 1,
    db: str = "monetdb",
    db_version: str = "1.0",
    status: str = "completed",
    started_at: datetime | None = None,
    query: str = "select 1",
    snapshot_metadata: dict[str, Any] | None = None,
) -> int:
    snapshot_id: int | None = None
    if snapshot_metadata is not None:
        snapshot = SystemSnapshot(
            system="test-system",
            os="TestOS",
            machine="test-machine",
            cpu_count_logical=8,
            memory_total_mb=16_384,
            metadata_json=snapshot_metadata,
        )
        session.add(snapshot)
        session.flush()
        snapshot_id = snapshot.id

    run = Run(
        suite=suite,
        suite_scale_factor=suite_scale_factor,
        db=db,
        db_version=db_version,
        operation="select",
        system="test-system",
        system_snapshot_id=snapshot_id,
        status=status,
        started_at=started_at or datetime(2026, 1, 1),
        metadata_json={"source": db},
    )
    session.add(run)
    session.commit()

    step = RunStep(
        run_id=run.id,
        step_type="query",
        step_name="query",
        query_name="q1",
        started_at=datetime(2026, 1, 1),
        status=status,
        result_status="ok" if status == "completed" else "error",
        iteration_role="first_run",
    )
    session.add(step)
    session.commit()

    session.add(
        RunMetric(
            run_id=run.id,
            time=datetime(2026, 1, 1),
            cpu_percent=1.0,
            mem_mb=10,
            client_mem_mb=11,
            disk_mb=100,
        )
    )
    session.add(
        QueryExecution(
            run_id=run.id,
            run_step_id=step.id,
            query=query,
            start_time=datetime(2026, 1, 1),
            end_time=datetime(2026, 1, 1),
        )
    )
    session.commit()

    return run.id


def _populate(
    db_path: Path,
    suite: str = "rtabench",
    suite_scale_factor: int = 1,
    db: str = "monetdb",
    status: str = "completed",
    started_at: datetime | None = None,
    query: str = "select 1",
    snapshot_metadata: dict[str, Any] | None = None,
) -> int:
    engine = get_results_engine(read_only=False, db_path=db_path)
    try:
        with Session(engine) as session:
            return _insert_run_tree(
                session,
                suite=suite,
                suite_scale_factor=suite_scale_factor,
                db=db,
                status=status,
                started_at=started_at,
                query=query,
                snapshot_metadata=snapshot_metadata,
            )
    finally:
        engine.dispose()


def _query(db_path: Path, sql: str) -> list[tuple[Any, ...]]:
    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(db_path), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_merge_into_empty_destination(tmp_path: Path) -> None:
    source, dest = tmp_path / "source.db", tmp_path / "dest.db"
    _create_results_db(source)
    _create_results_db(dest)

    _populate(source, db="monetdb")
    _populate(source, db="duckdb", started_at=datetime(2026, 1, 2))

    stats = merge_results(source, dest, head_revision=get_results_head_revision())

    assert stats.runs_added == 2
    assert stats.runs_replaced == 0
    assert stats.run_steps_added == 2
    assert stats.run_metrics_added == 2
    assert stats.query_executions_added == 2

    assert _query(dest, "select count(*) from run")[0][0] == 2


def test_merge_remaps_colliding_ids(tmp_path: Path) -> None:
    source, dest = tmp_path / "source.db", tmp_path / "dest.db"
    _create_results_db(source)
    _create_results_db(dest)

    _populate(dest, db="clickhouse", query="dest query")
    source_run_id = _populate(source, db="monetdb", query="source query")

    assert _query(dest, "select id from run")[0][0] == source_run_id  # both files allocated id 1

    stats = merge_results(source, dest, head_revision=get_results_head_revision())

    assert stats.runs_added == 1
    assert stats.runs_replaced == 0

    rows = _query(dest, "select id, db from run order by id")
    assert rows == [(1, "clickhouse"), (2, "monetdb")]

    # children follow the remapped run id and step id
    step_rows = _query(dest, "select run_id from run_step where id = 2")
    assert step_rows == [(2,)]
    qe_rows = _query(dest, "select run_id, run_step_id from query_execution where query = 'source query'")
    assert qe_rows == [(2, 2)]
    assert _query(dest, "select run_id from run_metric order by id") == [(1,), (2,)]


def test_merge_replaces_matching_runs(tmp_path: Path) -> None:
    source, dest = tmp_path / "source.db", tmp_path / "dest.db"
    _create_results_db(source)
    _create_results_db(dest)

    started_at = datetime(2026, 3, 1, 12, 0, 0)
    _populate(dest, db="monetdb", started_at=started_at, query="old query")
    _populate(dest, db="duckdb", started_at=datetime(2026, 3, 2))
    _populate(source, db="monetdb", started_at=started_at, query="new query")

    stats = merge_results(source, dest, head_revision=get_results_head_revision())

    assert stats.runs_added == 0
    assert stats.runs_replaced == 1

    assert _query(dest, "select count(*) from run")[0][0] == 2
    assert _query(dest, "select count(*) from query_execution where query = 'old query'")[0][0] == 0
    assert _query(dest, "select count(*) from query_execution where query = 'new query'")[0][0] == 1

    # merging the same source again is idempotent
    stats = merge_results(source, dest, head_revision=get_results_head_revision())
    assert stats.runs_added == 0
    assert stats.runs_replaced == 1
    assert _query(dest, "select count(*) from run")[0][0] == 2
    assert _query(dest, "select count(*) from run_step")[0][0] == 2
    assert _query(dest, "select count(*) from run_metric")[0][0] == 2
    assert _query(dest, "select count(*) from query_execution")[0][0] == 2


def test_merge_preserves_run_metadata_and_step_status_fields(tmp_path: Path) -> None:
    source, dest = tmp_path / "source.db", tmp_path / "dest.db"
    _create_results_db(source)
    _create_results_db(dest)

    _populate(source, db="monetdb")

    merge_results(source, dest, head_revision=get_results_head_revision())

    assert _query(dest, """select "metadata"->>'source' from run""") == [("monetdb",)]
    assert _query(dest, "select result_status, iteration_role from run_step") == [("ok", "first_run")]
    assert _query(dest, "select mem_mb, client_mem_mb from run_metric") == [(10, 11)]


def test_merge_remaps_and_deduplicates_system_snapshots(tmp_path: Path) -> None:
    source, dest = tmp_path / "source.db", tmp_path / "dest.db"
    _create_results_db(source)
    _create_results_db(dest)
    metadata = {"host": {"os": "TestOS", "machine": "test-machine"}}

    _populate(dest, db="clickhouse", snapshot_metadata=metadata)
    _populate(source, db="monetdb", snapshot_metadata=metadata)

    stats = merge_results(source, dest, head_revision=get_results_head_revision())

    assert stats.system_snapshots_added == 0
    assert _query(dest, "select count(*) from system_snapshot") == [(1,)]
    assert _query(dest, "select count(distinct system_snapshot_id) from run") == [(1,)]


def test_merge_keeps_different_suite_scale_factors(tmp_path: Path) -> None:
    source, dest = tmp_path / "source.db", tmp_path / "dest.db"
    _create_results_db(source)
    _create_results_db(dest)

    started_at = datetime(2026, 3, 1, 12, 0, 0)
    _populate(dest, suite="tpc_h", suite_scale_factor=10, db="duckdb", started_at=started_at, query="sf10")
    _populate(source, suite="tpc_h", suite_scale_factor=50, db="duckdb", started_at=started_at, query="sf50")

    stats = merge_results(source, dest, head_revision=get_results_head_revision())

    assert stats.runs_added == 1
    assert stats.runs_replaced == 0
    assert _query(dest, "select suite, suite_scale_factor from run order by suite_scale_factor") == [
        ("tpc_h", 10),
        ("tpc_h", 50),
    ]


def test_merge_skips_running_orphans(tmp_path: Path) -> None:
    source, dest = tmp_path / "source.db", tmp_path / "dest.db"
    _create_results_db(source)
    _create_results_db(dest)

    _populate(source, db="monetdb", status="running")
    _populate(source, db="duckdb", started_at=datetime(2026, 1, 2))

    stats = merge_results(source, dest, head_revision=get_results_head_revision())

    assert stats.runs_added == 1
    assert _query(dest, "select db from run") == [("duckdb",)]


def test_merge_syncs_sequences(tmp_path: Path) -> None:
    source, dest = tmp_path / "source.db", tmp_path / "dest.db"
    _create_results_db(source)
    _create_results_db(dest)

    _populate(dest, db="clickhouse")
    _populate(source, db="monetdb")
    _populate(source, db="duckdb", started_at=datetime(2026, 1, 2))

    merge_results(source, dest, head_revision=get_results_head_revision())

    # a fresh insert through the ORM (sequence default) must not collide
    new_run_id = _populate(dest, db="postgres", started_at=datetime(2026, 1, 3))
    assert new_run_id == 4


def test_merge_rejects_schema_revision_mismatch(tmp_path: Path) -> None:
    source, dest = tmp_path / "source.db", tmp_path / "dest.db"
    _create_results_db(source)
    _create_results_db(dest)

    with pytest.raises(RuntimeError, match="schema revision"):
        merge_results(source, dest, head_revision="nonexistent")
