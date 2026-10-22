from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import duckdb

from ..matrix import MatrixCell, registered_operations
from ..profile import BENCHMARK_PROFILE
from ..settings import SUITE_SPECS
from .cells import StoredRun, latest_run_per_cell, load_runs
from .publishing import PublishValidationError, single_system


def campaign_cells() -> list[MatrixCell]:
    return sorted(
        MatrixCell(db, suite, scale_factor, operation)
        for db, suites in registered_operations().items()
        for suite, operations in suites.items()
        for scale_factor in SUITE_SPECS[suite].scale_factors
        for operation in operations
    )


def _current_query_names(cell: MatrixCell) -> tuple[str, ...]:
    from ..dbs import get_databases

    database = get_databases()[cell.db]
    suite = database.suite_registry()[cell.suite](db=database, name=cell.suite, scale_factor=cell.scale_factor)
    return tuple(suite.planned_query_names())


@dataclass(frozen=True)
class _QueryStep:
    name: str | None
    iteration: int | None
    status: str
    result_status: str | None
    iteration_role: str | None
    finished: bool
    row_count: int | None
    answer_hash: str | None
    hash_skipped_reason: str | None
    skip_reason: str | None
    error_type: str | None
    error_message: str | None

    def has_valid_result(self) -> bool:
        if self.status == "failed":
            return (
                self.result_status in (None, "error", "timeout")
                and bool(self.error_type and self.error_type.strip())
                and self.error_message is not None
            )
        if self.status != "completed" or self.error_type is not None:
            return False
        if self.result_status == "ok":
            return (
                self.row_count is not None
                and self.row_count >= 0
                and self.error_message is None
                and bool(
                    (self.answer_hash and self.answer_hash.strip())
                    or (self.hash_skipped_reason and self.hash_skipped_reason.strip())
                )
            )
        return self.result_status in ("skipped", "unsupported") and bool(self.skip_reason and self.skip_reason.strip())


def _select_problems(con: duckdb.DuckDBPyConnection, selected: Mapping[MatrixCell, StoredRun]) -> list[str]:
    selects = {cell: run for cell, run in selected.items() if cell.operation == "select"}
    ids = [run.id for run in selects.values()]
    plans = {
        int(row[0]): (cast(int | None, row[1]), cast(str | None, row[2]), cast(str | None, row[3]))
        for row in con.execute(
            """
            select id, case when json_type(metadata, '$.plan.iterations') in ('BIGINT', 'UBIGINT')
                then try_cast(json_extract(metadata, '$.plan.iterations') as integer) end,
              error_type, error_message
            from run where id = any(?)
            """,
            [ids],
        ).fetchall()
    }
    steps: dict[int, list[_QueryStep]] = {}
    for row in con.execute(
        """
        select run_id, query_name, iteration, status, result_status, iteration_role, finished_at is not null,
          row_count, json_extract_string(metadata, '$.answer_hash'),
          json_extract_string(metadata, '$.answer_hash_skipped_reason'),
          json_extract_string(metadata, '$.skip_reason'), error_type, error_message
        from run_step where run_id = any(?) and step_type = 'query'
        """,
        [ids],
    ).fetchall():
        steps.setdefault(int(row[0]), []).append(
            _QueryStep(
                name=cast(str | None, row[1]),
                iteration=cast(int | None, row[2]),
                status=str(row[3]),
                result_status=cast(str | None, row[4]),
                iteration_role=cast(str | None, row[5]),
                finished=bool(row[6]),
                row_count=cast(int | None, row[7]),
                answer_hash=cast(str | None, row[8]),
                hash_skipped_reason=cast(str | None, row[9]),
                skip_reason=cast(str | None, row[10]),
                error_type=cast(str | None, row[11]),
                error_message=cast(str | None, row[12]),
            )
        )

    problems: list[str] = []
    for cell, run in selects.items():
        names = _current_query_names(cell)
        iterations, error_type, error_message = plans[run.id]
        if not names or run.query_names != names or iterations != BENCHMARK_PROFILE.total_iterations:
            problems.append(f"{run.label}: recorded query plan differs from the complete current four-iteration plan")
            continue
        if run.status == "failed" and (not error_type or not error_type.strip() or error_message is None):
            problems.append(f"{run.label}: failed select requires an explicit run error")
        if run.status == "completed" and (error_type is not None or error_message is not None):
            problems.append(f"{run.label}: completed select records a run error")

        query_steps = steps.get(run.id, [])
        expected: Counter[tuple[str | None, int | None]] = Counter(
            (name, iteration) for name in names for iteration in range(1, BENCHMARK_PROFILE.total_iterations + 1)
        )
        observed = Counter((step.name, step.iteration) for step in query_steps)
        if observed - expected or (run.status == "completed" and observed != expected):
            problems.append(
                f"{run.label}: query steps must cover planned positions at most once; "
                "completed selects must cover every planned query exactly four times"
            )
            continue

        if any(
            not step.finished
            or not step.has_valid_result()
            or (run.status == "completed" and step.status != "completed")
            or step.iteration_role != BENCHMARK_PROFILE.iteration_role(cast(int, step.iteration))
            for step in query_steps
        ):
            problems.append(f"{run.label}: query iterations have invalid outcomes, unfinished steps or profile roles")
            continue

        if run.status == "failed":
            continue

        statuses = {name: {step.result_status for step in query_steps if step.name == name} for name in names}
        if any(len(results) != 1 for results in statuses.values()):
            problems.append(f"{run.label}: query iterations mix measured results with skipped or unsupported steps")
        if not any(results == {"ok"} for results in statuses.values()):
            problems.append(f"{run.label}: completed select contains no fully measured four-iteration query")

    return problems


def select_campaign_runs(
    db_path: Path,
    cells: Sequence[MatrixCell],
    *,
    pins: Mapping[str, str],
    drivers: Mapping[str, str | None],
) -> set[int]:
    if not any(cell.operation == "select" for cell in cells):
        raise PublishValidationError("A campaign must declare at least one select cell")
    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(db_path), read_only=True)
    try:
        declared = {(cell.db, cell.suite, cell.scale_factor, cell.operation) for cell in cells}
        invalidated = {
            int(row[0])
            for row in con.execute(
                "select id from run where json_extract(\"metadata\", '$.invalidation') is not null"
            ).fetchall()
        }
        finished = {int(row[0]) for row in con.execute("select id from run where finished_at is not null").fetchall()}
        runs = load_runs(db_path, con)
        eligible = [
            run
            for run in runs
            if (run.db, run.suite, run.scale_factor, run.operation) in declared
            and run.db in pins
            and run.db_version == pins[run.db]
            and run.db in drivers
            and run.db_driver == drivers[run.db]
            and run.benchmark_profile == BENCHMARK_PROFILE.id
            and run.commit is not None
            and bool(run.commit.strip())
            and run.dirty is False
            and run.id not in invalidated
        ]
        candidates = [run for run in eligible if run.id in finished and run.status in ("completed", "failed")]
        if not candidates:
            missing = [cell.label for cell in cells if cell.operation == "select"]
            raise PublishValidationError("Campaign cells without a terminal select attempt:\n- " + "\n- ".join(missing))
        system = single_system(candidates)
        latest = latest_run_per_cell(candidates, cells, system=system, drivers=drivers)
        latest_by_key = {
            (cell.db, cell.suite, cell.scale_factor, cell.operation): run
            for cell, run in latest.items()
            if run is not None
        }
        unfinished: list[str] = []
        for run in eligible:
            if run.status != "running" and run.id in finished:
                continue
            previous = latest_by_key.get((run.db, run.suite, run.scale_factor, run.operation))
            if previous is None or (run.started_at, run.id) > (previous.started_at, previous.id):
                unfinished.append(run.label)
        if unfinished:
            raise PublishValidationError("Refusing to publish unfinished campaign runs:\n- " + "\n- ".join(unfinished))
        missing = [cell.label for cell, run in latest.items() if cell.operation == "select" and run is None]
        if missing:
            raise PublishValidationError("Campaign cells without a terminal select attempt:\n- " + "\n- ".join(missing))
        selected = {cell: run for cell, run in latest.items() if run is not None}
        problems = _select_problems(con, selected)
        if problems:
            raise PublishValidationError("Refusing to publish invalid campaign cohorts:\n- " + "\n- ".join(problems))
        from .analytical import analytical_run_problems, select_analytical_runs

        analytical_ids = select_analytical_runs(con, runs, cells, pins=pins, drivers=drivers)
        selected_ids = {run.id for run in selected.values()} | analytical_ids
        selected_runs = [run for run in runs if run.id in selected_ids]
        single_system(selected_runs)
        problems = analytical_run_problems(con, selected_runs)
        if problems:
            raise PublishValidationError("Refusing to publish invalid analytical evidence:\n- " + "\n- ".join(problems))
        return selected_ids
    finally:
        con.close()


def validate_campaign_runs(
    db_path: Path,
    cells: Sequence[MatrixCell],
    *,
    pins: Mapping[str, str],
    drivers: Mapping[str, str | None],
) -> None:
    selected = select_campaign_runs(db_path, cells, pins=pins, drivers=drivers)
    extra = [run.label for run in load_runs(db_path) if run.id not in selected]
    if extra:
        raise PublishValidationError("Runs outside the selected current campaign:\n- " + "\n- ".join(extra))
