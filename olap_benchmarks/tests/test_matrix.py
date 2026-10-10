from __future__ import annotations

from datetime import datetime

import pytest

from ..matrix import MATRIX, EnginePlan, MatrixCell, SuitePlan, expand_matrix, published_databases
from ..results.cells import StoredRun, matrix_status, superseded_runs
from ..settings import DatabaseName, Operation, SuiteName


def test_every_declared_cell_is_registered_by_its_engine() -> None:
    cells = expand_matrix()

    assert len(set(cells)) == len(cells)
    assert {cell.db for cell in cells} == set(MATRIX)


def test_published_matrix_content() -> None:
    cells = set(expand_matrix(published_only=True))

    assert published_databases() == ["clickhouse", "duckdb", "monetdb", "polars"]
    for db in ("clickhouse", "duckdb"):
        assert MatrixCell(db, "chat_threads", 1, "concurrent") in cells
        assert MatrixCell(db, "jsonbench", 10, "select") in cells
        assert MatrixCell(db, "time_series", 1, "mutate") in cells
    assert MatrixCell("monetdb", "chat_threads", 1, "mutate") in cells
    assert MatrixCell("monetdb", "chat_threads", 1, "concurrent") not in cells
    assert not any(cell.db == "monetdb" and cell.suite == "jsonbench" for cell in cells)
    assert {(cell.suite, cell.scale_factor) for cell in cells if cell.db == "polars"} == {
        ("tpc_h", 10),
        ("jsonbench", 10),
        ("clickbench", 1),
    }
    assert {cell.operation for cell in cells if cell.suite == "clickbench"} == {"populate", "select"}


def test_declaring_an_unregistered_suite_or_operation_fails() -> None:
    registry: dict[DatabaseName, dict[SuiteName, tuple[Operation, ...]]] = {"duckdb": {"tpc_h": ("populate", "select")}}

    with pytest.raises(ValueError, match="does not register"):
        expand_matrix({"duckdb": EnginePlan(True, (SuitePlan("rtabench", 1),))}, registry=registry)
    with pytest.raises(ValueError, match="unsupported tpc_h operation"):
        expand_matrix({"duckdb": EnginePlan(True, (SuitePlan("tpc_h", 10, ("mutate",)),))}, registry=registry)


def _run(
    run_id: int,
    cell: MatrixCell,
    *,
    version: str = "1.0",
    status: str = "completed",
    day: int = 1,
    commit: str | None = "c1",
) -> StoredRun:
    return StoredRun(
        id=run_id,
        system="test",
        db=cell.db,
        db_version=version,
        db_driver=None,
        suite=cell.suite,
        scale_factor=cell.scale_factor,
        operation=cell.operation,
        status=status,
        started_at=datetime(2026, 10, day),
        commit=commit,
        dirty=False,
    )


POPULATE = MatrixCell("duckdb", "tpc_h", 10, "populate")
SELECT = MatrixCell("duckdb", "tpc_h", 10, "select")
CLICKBENCH = MatrixCell("duckdb", "clickbench", 1, "select")
RTABENCH = MatrixCell("duckdb", "rtabench", 1, "select")
JSONBENCH = MatrixCell("duckdb", "jsonbench", 10, "select")


def test_matrix_status_classifies_cells() -> None:
    runs = [
        _run(1, POPULATE, day=1),
        _run(2, POPULATE, day=2, commit="c2"),
        _run(3, SELECT, day=2, commit="c2"),
        _run(4, CLICKBENCH, day=1, commit="c1"),
        _run(5, RTABENCH, day=1, version="0.9", commit="c2"),
        _run(6, JSONBENCH, day=1, commit="c2"),
        _run(7, JSONBENCH, day=3, status="failed", commit="c2"),
    ]
    cells = [POPULATE, SELECT, CLICKBENCH, RTABENCH, JSONBENCH, MatrixCell("duckdb", "tpc_ds", 1, "select")]

    statuses = matrix_status(runs, cells, system="test", pins={"duckdb": "1.0"}, drivers={"duckdb": None})

    assert [(status.cell.suite, status.cell.operation, status.state) for status in statuses] == [
        ("tpc_h", "populate", "ok"),
        ("tpc_h", "select", "ok"),
        ("clickbench", "select", "stale"),
        ("rtabench", "select", "stale"),
        ("jsonbench", "select", "failed"),
        ("tpc_ds", "select", "missing"),
    ]
    assert statuses[2].detail.startswith("commit c1")
    assert statuses[3].detail == "version 0.9, pinned 1.0"


def test_superseded_runs_keep_the_latest_completed_run_and_later_attempts() -> None:
    runs = [
        _run(1, SELECT, day=1),
        _run(2, SELECT, day=2, status="failed"),
        _run(3, SELECT, day=3),
        _run(4, SELECT, day=4, status="failed"),
        _run(5, POPULATE, day=1, status="failed"),
        _run(6, CLICKBENCH, day=1, version="0.9"),
        _run(7, CLICKBENCH, day=2, version="0.9"),
    ]
    pins = {"duckdb": "1.0"}

    assert [run.id for run in superseded_runs(runs, pins=pins, stale_versions=False)] == [1, 2, 6]
    assert [run.id for run in superseded_runs(runs, pins=pins, stale_versions=True)] == [1, 2, 6, 7]
