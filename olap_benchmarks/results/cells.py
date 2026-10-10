from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, cast

import duckdb

from ..matrix import MatrixCell

CellState = Literal["ok", "missing", "failed", "stale"]


@dataclass(frozen=True, slots=True)
class StoredRun:
    id: int
    system: str
    db: str
    db_version: str
    db_driver: str | None
    suite: str
    scale_factor: int
    operation: str
    status: str
    started_at: datetime
    commit: str | None
    dirty: bool | None

    @property
    def label(self) -> str:
        driver = f"/{self.db_driver}" if self.db_driver is not None else ""
        return (
            f"run {self.id} {self.db}{driver} {self.db_version} {self.suite}:sf{self.scale_factor}:{self.operation}"
            f" ({self.status}, {self.started_at:%Y-%m-%d %H:%M})"
        )


@dataclass(frozen=True, slots=True)
class CellStatus:
    cell: MatrixCell
    state: CellState
    run: StoredRun | None
    detail: str


def load_runs(path: Path, con: duckdb.DuckDBPyConnection | None = None) -> list[StoredRun]:
    owned = con is None
    connection: duckdb.DuckDBPyConnection = con or cast(Any, duckdb).connect(str(path), read_only=True)
    try:
        rows = connection.execute(
            """
            select
              id, system, db, db_version, db_driver, suite, suite_scale_factor, operation, status, started_at,
              json_extract_string("metadata", '$.repository.commit'),
              cast(json_extract("metadata", '$.repository.dirty') as boolean)
            from run
            order by started_at, id
            """
        ).fetchall()
    finally:
        if owned:
            connection.close()

    return [
        StoredRun(
            id=int(row[0]),
            system=str(row[1]),
            db=str(row[2]),
            db_version=str(row[3]),
            db_driver=cast(str | None, row[4]),
            suite=str(row[5]),
            scale_factor=int(row[6]),
            operation=str(row[7]),
            status=str(row[8]),
            started_at=cast(datetime, row[9]),
            commit=cast(str | None, row[10]),
            dirty=cast(bool | None, row[11]),
        )
        for row in rows
    ]


def newest_commit(runs: Iterable[StoredRun]) -> str | None:
    newest: StoredRun | None = None
    for run in runs:
        if run.commit is not None and (newest is None or (run.started_at, run.id) > (newest.started_at, newest.id)):
            newest = run
    return newest.commit if newest is not None else None


def latest_run_per_cell(
    runs: Iterable[StoredRun],
    cells: Iterable[MatrixCell],
    *,
    system: str,
    drivers: Mapping[str, str | None],
) -> dict[MatrixCell, StoredRun | None]:
    latest: dict[tuple[str, str, int, str], StoredRun] = {}
    for run in runs:
        if run.system != system or run.db_driver != drivers.get(run.db):
            continue
        key = (run.db, run.suite, run.scale_factor, run.operation)
        current = latest.get(key)
        if current is None or (run.started_at, run.id) > (current.started_at, current.id):
            latest[key] = run

    return {cell: latest.get((cell.db, cell.suite, cell.scale_factor, cell.operation)) for cell in cells}


def matrix_status(
    runs: list[StoredRun],
    cells: Iterable[MatrixCell],
    *,
    system: str,
    pins: Mapping[str, str],
    drivers: Mapping[str, str | None],
) -> list[CellStatus]:
    commit = newest_commit(run for run in runs if run.system == system)
    statuses: list[CellStatus] = []

    for cell, run in latest_run_per_cell(runs, cells, system=system, drivers=drivers).items():
        if run is None:
            statuses.append(CellStatus(cell, "missing", None, "no run"))
        elif run.status != "completed":
            statuses.append(CellStatus(cell, "failed", run, f"latest run is {run.status}"))
        elif run.db_version != pins[cell.db]:
            statuses.append(CellStatus(cell, "stale", run, f"version {run.db_version}, pinned {pins[cell.db]}"))
        elif run.commit != commit:
            statuses.append(CellStatus(cell, "stale", run, f"commit {_short(run.commit)}, newest {_short(commit)}"))
        else:
            statuses.append(CellStatus(cell, "ok", run, ""))

    return statuses


def superseded_runs(runs: Iterable[StoredRun], *, pins: Mapping[str, str], stale_versions: bool) -> list[StoredRun]:
    by_cell: dict[tuple[str, str, str, str | None, str, int, str], list[StoredRun]] = {}
    for run in runs:
        key = (run.system, run.db, run.db_version, run.db_driver, run.suite, run.scale_factor, run.operation)
        by_cell.setdefault(key, []).append(run)

    doomed: list[StoredRun] = []
    for cell_runs in by_cell.values():
        completed = [run for run in cell_runs if run.status == "completed"]
        if stale_versions and cell_runs[0].db in pins and cell_runs[0].db_version != pins[cell_runs[0].db]:
            doomed.extend(run for run in cell_runs if run.status != "running")
            continue
        if not completed:
            continue
        keep = max(completed, key=lambda run: (run.started_at, run.id))
        doomed.extend(
            run
            for run in cell_runs
            if run.status != "running" and run.id != keep.id and (run.started_at, run.id) < (keep.started_at, keep.id)
        )

    return sorted(doomed, key=lambda run: run.id)


def _short(commit: str | None) -> str:
    return commit[:10] if commit is not None else "none"


def format_table(header: tuple[str, ...], rows: Iterable[tuple[object, ...]]) -> str:
    body = [tuple("" if value is None else str(value) for value in row) for row in rows]
    widths = [max([len(title), *(len(row[index]) for row in body)]) for index, title in enumerate(header)]
    lines = ["  ".join(title.ljust(width) for title, width in zip(header, widths, strict=True)).rstrip()]
    lines.append("  ".join("-" * width for width in widths))
    lines.extend(
        "  ".join(value.ljust(width) for value, width in zip(row, widths, strict=True)).rstrip() for row in body
    )
    return "\n".join(lines)


def format_matrix_status(statuses: Iterable[CellStatus]) -> str:
    return format_table(
        ("db", "suite", "sf", "operation", "state", "run", "version", "commit", "detail"),
        (
            (
                status.cell.db,
                status.cell.suite,
                status.cell.scale_factor,
                status.cell.operation,
                status.state,
                status.run.id if status.run is not None else None,
                status.run.db_version if status.run is not None else None,
                _short(status.run.commit) if status.run is not None else None,
                status.detail,
            )
            for status in statuses
        ),
    )
