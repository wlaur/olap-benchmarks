from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from .. import __main__
from ..__main__ import BenchmarkGroup, GroupOutcome
from ..matrix import MatrixCell
from ..results.cells import CellStatus, StoredRun
from ..results.validation import ValidationReport

CELLS = [
    MatrixCell("duckdb", "tpc_h", 10, "populate"),
    MatrixCell("duckdb", "tpc_h", 10, "select"),
    MatrixCell("duckdb", "clickbench", 1, "populate"),
    MatrixCell("duckdb", "clickbench", 1, "select"),
]


def _status(cell: MatrixCell, state: str) -> CellStatus:
    run = StoredRun(
        id=1,
        system="test",
        db=cell.db,
        db_version="1.0",
        db_driver=None,
        suite=cell.suite,
        scale_factor=cell.scale_factor,
        operation=cell.operation,
        status="completed",
        started_at=datetime(2026, 10, 1),
        commit="c1",
        dirty=False,
    )
    return CellStatus(cell, "ok" if state == "ok" else "missing", run if state == "ok" else None, "")


def _patch(
    monkeypatch: pytest.MonkeyPatch,
    states: dict[MatrixCell, str],
    error: str | None = None,
) -> tuple[list[list[BenchmarkGroup]], list[dict[str, object]]]:
    runs: list[list[BenchmarkGroup]] = []
    validations: list[dict[str, object]] = []

    def fake_expand_matrix(dbs: object = None, published_only: bool = False) -> list[MatrixCell]:
        return CELLS

    def fake_statuses(cells: list[MatrixCell], _revision: str) -> list[CellStatus]:
        return [_status(cell, states[cell]) for cell in cells]

    def fake_run_groups(
        groups: list[BenchmarkGroup], *, revision: str, cleanup: bool, keep_going: bool
    ) -> list[GroupOutcome]:
        assert cleanup
        runs.append(groups)
        return [GroupOutcome(group, error) for group in groups]

    def fake_validate(**kwargs: object) -> ValidationReport:
        validations.append(kwargs)
        return ValidationReport([], [], [])

    def fake_pins() -> dict[str, str]:
        return {"duckdb": "1.0"}

    monkeypatch.setattr(__main__, "expand_matrix", fake_expand_matrix)
    monkeypatch.setattr(__main__, "_matrix_statuses", fake_statuses)
    monkeypatch.setattr(__main__, "run_benchmark_groups", fake_run_groups)
    monkeypatch.setattr(__main__, "validate_results", fake_validate)
    monkeypatch.setattr(__main__, "current_pins", fake_pins)
    return runs, validations


def test_matrix_run_missing_only_reruns_whole_groups_and_marks_once(monkeypatch: pytest.MonkeyPatch) -> None:
    states = {CELLS[0]: "ok", CELLS[1]: "missing", CELLS[2]: "ok", CELLS[3]: "ok"}
    runs, validations = _patch(monkeypatch, states)

    __main__.matrix_run(missing_only=True)

    assert runs == [[BenchmarkGroup("duckdb", "tpc_h", 10, ("populate", "select"))]]
    assert len(validations) == 1
    assert validations[0]["mark"] is True


def test_matrix_run_exits_non_zero_when_a_group_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    states = dict.fromkeys(CELLS, "missing")
    runs, validations = _patch(monkeypatch, states, error="RuntimeError: boom")

    with pytest.raises(SystemExit):
        __main__.matrix_run()

    assert [group.suite for group in runs[0]] == ["tpc_h", "clickbench"]
    assert len(validations) == 1


def test_matrix_status_check_fails_unless_every_cell_is_ok(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _patch(monkeypatch, {CELLS[0]: "ok", CELLS[1]: "missing", CELLS[2]: "ok", CELLS[3]: "ok"})

    __main__.matrix_status_cmd()
    assert "1 missing, 3 ok" in capsys.readouterr().out

    with pytest.raises(SystemExit):
        __main__.matrix_status_cmd(check=True)


def test_matrix_status_of_a_new_revision_is_all_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def no_revision(_revision: str) -> Path:
        return tmp_path / "absent.db"

    def fake_pins() -> dict[str, str]:
        return {"duckdb": "1.0"}

    def fake_drivers() -> dict[str, str | None]:
        return {"duckdb": None}

    monkeypatch.setattr(__main__, "get_results_db_path", no_revision)
    monkeypatch.setattr(__main__, "current_pins", fake_pins)
    monkeypatch.setattr(__main__, "current_drivers", fake_drivers)

    assert {status.state for status in __main__._matrix_statuses(CELLS, "fresh")} == {"missing"}
