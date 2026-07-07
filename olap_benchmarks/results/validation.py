from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import duckdb

from ..settings import SETTINGS, Revision, SuiteName


@dataclass(frozen=True)
class RowCountObservation:
    db: str
    db_version: str
    row_count: int


@dataclass(frozen=True)
class RowCountMismatch:
    system: str
    suite: str
    suite_scale_factor: int
    query_name: str
    iteration: int
    observations: tuple[RowCountObservation, ...]


class RowCountValidationError(RuntimeError):
    pass


def _resolve_results_path(revision: Revision, db_path: Path | None) -> Path:
    path = db_path or SETTINGS.results_directory / f"{revision}.db"
    if not path.is_file():
        raise FileNotFoundError(f"Results database does not exist: {path}")
    return path


def validate_latest_query_row_counts(
    revision: Revision = "default",
    db_path: Path | None = None,
    system: str | None = None,
    suite: SuiteName | None = None,
    suite_scale_factor: int | None = None,
) -> list[RowCountMismatch]:
    path = _resolve_results_path(revision, db_path)

    where_clauses = [
        "r.operation = 'select'",
        "r.status = 'completed'",
        "r.finished_at is not null",
    ]
    params: list[object] = []

    if system is not None:
        where_clauses.append("r.system = ?")
        params.append(system)
    if suite is not None:
        where_clauses.append("r.suite = ?")
        params.append(suite)
    if suite_scale_factor is not None:
        where_clauses.append("r.suite_scale_factor = ?")
        params.append(suite_scale_factor)

    where_sql = "\n          and ".join(where_clauses)
    sql = f"""
        with scoped_runs as (
          select
            r.id as run_id,
            r.system,
            r.suite,
            r.suite_scale_factor,
            r.db,
            r.db_version,
            r.finished_at
          from run r
          where {where_sql}
        ),
        latest_runs as (
          select
            *,
            row_number() over (
              partition by system, suite, suite_scale_factor, db, db_version
              order by finished_at desc, run_id desc
            ) as run_rank
          from scoped_runs
        ),
        query_counts as (
          select
            lr.system,
            lr.suite,
            lr.suite_scale_factor,
            s.query_name,
            s.iteration,
            lr.db,
            lr.db_version,
            s.row_count
          from latest_runs lr
          join run_step s on s.run_id = lr.run_id
          where lr.run_rank = 1
            and s.step_type = 'query'
            and s.status = 'completed'
            and s.query_name is not null
            and s.iteration is not null
            and s.row_count is not null
        ),
        mismatches as (
          select
            system,
            suite,
            suite_scale_factor,
            query_name,
            iteration
          from query_counts
          group by system, suite, suite_scale_factor, query_name, iteration
          having count(distinct row_count) > 1
             and count(distinct db || chr(31) || db_version) > 1
        )
        select
          qc.system,
          qc.suite,
          qc.suite_scale_factor,
          qc.query_name,
          qc.iteration,
          qc.db,
          qc.db_version,
          qc.row_count
        from query_counts qc
        join mismatches m
          on qc.system = m.system
         and qc.suite = m.suite
         and qc.suite_scale_factor = m.suite_scale_factor
         and qc.query_name = m.query_name
         and qc.iteration = m.iteration
        order by
          qc.system,
          qc.suite,
          qc.suite_scale_factor,
          qc.query_name,
          qc.iteration,
          qc.row_count,
          qc.db,
          qc.db_version
    """

    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(path), read_only=True)
    try:
        rows = con.execute(sql, params).fetchall()
    finally:
        con.close()

    grouped: dict[tuple[str, str, int, str, int], list[RowCountObservation]] = {}
    for row in rows:
        key = (
            str(row[0]),
            str(row[1]),
            int(row[2]),
            str(row[3]),
            int(row[4]),
        )
        observations = grouped.setdefault(key, [])
        observations.append(
            RowCountObservation(
                db=str(row[5]),
                db_version=str(row[6]),
                row_count=int(row[7]),
            )
        )

    return [
        RowCountMismatch(
            system=system,
            suite=suite_name,
            suite_scale_factor=scale_factor,
            query_name=query_name,
            iteration=iteration,
            observations=tuple(observations),
        )
        for (system, suite_name, scale_factor, query_name, iteration), observations in grouped.items()
    ]


def format_row_count_mismatches(mismatches: list[RowCountMismatch], limit: int = 20) -> str:
    if not mismatches:
        return "Row counts match across latest completed select runs."

    lines = ["Row-count mismatches across latest completed select runs:"]
    for mismatch in mismatches[:limit]:
        values = ", ".join(
            f"{observation.db} {observation.db_version}: {observation.row_count}"
            for observation in mismatch.observations
        )
        lines.append(
            "- "
            f"{mismatch.system} {mismatch.suite} sf{mismatch.suite_scale_factor} "
            f"{mismatch.query_name} iteration {mismatch.iteration}: {values}"
        )

    omitted = len(mismatches) - limit
    if omitted > 0:
        lines.append(f"... and {omitted} more mismatch(es)")

    return "\n".join(lines)


def assert_latest_query_row_counts(
    revision: Revision = "default",
    db_path: Path | None = None,
    system: str | None = None,
    suite: SuiteName | None = None,
    suite_scale_factor: int | None = None,
) -> None:
    mismatches = validate_latest_query_row_counts(
        revision=revision,
        db_path=db_path,
        system=system,
        suite=suite,
        suite_scale_factor=suite_scale_factor,
    )
    if mismatches:
        raise RowCountValidationError(format_row_count_mismatches(mismatches))
