from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import duckdb

from ..settings import Revision, SuiteName, get_settings

# Queries whose answers are legitimately not comparable across engines, mapped to why. These still
# run and are still timed; only the cross-engine answer comparison is skipped, because the engines
# disagree by dialect rather than because one of them is wrong. Keep this list short: an entry here
# is a claim that no engine is at fault, which is a much stronger claim than a failing query.
ANSWER_COMPARISON_EXCLUSIONS: dict[SuiteName, dict[str, str]] = {
    "clickbench": {
        "Q28": (
            "DuckDB and ClickHouse apply different regex semantics to the roughly 1031 Referer "
            "values containing a newline: RE2's default leaves '.' not matching a newline and '$' "
            "meaning end of text, which DuckDB follows and ClickHouse does not. Neither is wrong "
            "and no rewrite reconciles them without changing what the query asks."
        )
    },
}


def _excluded_query_names(suite: SuiteName | None) -> dict[SuiteName, dict[str, str]]:
    if suite is None:
        return ANSWER_COMPARISON_EXCLUSIONS
    return {suite: ANSWER_COMPARISON_EXCLUSIONS[suite]} if suite in ANSWER_COMPARISON_EXCLUSIONS else {}


@dataclass(frozen=True)
class RowCountObservation:
    db: str
    db_version: str
    row_count: int
    db_driver: str | None = None
    run_step_id: int | None = None


@dataclass(frozen=True)
class RowCountMismatch:
    system: str
    suite: str
    suite_scale_factor: int
    query_name: str
    iteration: int
    observations: tuple[RowCountObservation, ...]


@dataclass(frozen=True)
class AnswerHashObservation:
    db: str
    db_version: str
    row_count: int
    answer_hash: str
    db_driver: str | None = None
    run_step_id: int | None = None


@dataclass(frozen=True)
class AnswerHashMismatch:
    system: str
    suite: str
    suite_scale_factor: int
    query_name: str
    iteration: int
    observations: tuple[AnswerHashObservation, ...]


@dataclass(frozen=True)
class IterationInconsistency:
    run_id: int
    system: str
    db: str
    db_version: str
    db_driver: str | None
    suite: str
    suite_scale_factor: int
    query_name: str
    answer_hashes: tuple[str, ...]


@dataclass(frozen=True)
class ValidationReport:
    row_count_mismatches: list[RowCountMismatch]
    answer_hash_mismatches: list[AnswerHashMismatch]
    iteration_inconsistencies: list[IterationInconsistency]
    marks_cleared: int = 0
    marks_applied: int = 0

    @property
    def ok(self) -> bool:
        return not (self.row_count_mismatches or self.answer_hash_mismatches or self.iteration_inconsistencies)

    def format(self) -> str:
        sections = [
            format_row_count_mismatches(self.row_count_mismatches),
            format_answer_hash_mismatches(self.answer_hash_mismatches),
            format_iteration_inconsistencies(self.iteration_inconsistencies),
        ]
        if self.marks_cleared or self.marks_applied:
            sections.append(
                f"Wrong-result marks: {self.marks_cleared} cleared, {self.marks_applied} applied from the current "
                "comparison."
            )
        return "\n".join(sections)


@dataclass(frozen=True)
class _Scope:
    pins: Mapping[str, str] | None
    system: str | None
    suite: SuiteName | None
    suite_scale_factor: int | None

    def run_filter(self, alias: str = "r") -> tuple[str, list[object]]:
        clauses: list[str] = []
        params: list[object] = []
        if self.system is not None:
            clauses.append(f"{alias}.system = ?")
            params.append(self.system)
        if self.suite is not None:
            clauses.append(f"{alias}.suite = ?")
            params.append(self.suite)
        if self.suite_scale_factor is not None:
            clauses.append(f"{alias}.suite_scale_factor = ?")
            params.append(self.suite_scale_factor)
        return " and ".join(clauses) or "true", params

    def latest_runs_cte(self) -> tuple[str, list[object]]:
        where_sql, params = self.run_filter()
        pin_sql = "true"
        if self.pins is not None:
            pin_sql = "false"
            if self.pins:
                placeholders = ", ".join("(?, ?)" for _ in self.pins)
                pin_sql = f"(r.db, r.db_version) in (values {placeholders})"
                for db, version in self.pins.items():
                    params.extend((db, version))
        exclusion_clauses: list[str] = []
        for excluded_suite, queries in _excluded_query_names(self.suite).items():
            placeholders = ", ".join("?" for _ in queries)
            exclusion_clauses.append(f"and not (lr.suite = ? and s.query_name in ({placeholders}))")
            params.append(excluded_suite)
            params.extend(queries)
        exclusion_sql = "\n            ".join(exclusion_clauses)

        # a cell is (system, suite, scale factor, db, driver); restricting to the pinned versions first makes
        # the latest run per cell the latest run of the version the harness would benchmark today
        sql = f"""
        with latest_runs as (
          select
            r.id as run_id, r.system, r.suite, r.suite_scale_factor, r.db, r.db_version, r.db_driver,
            row_number() over (
              partition by r.system, r.suite, r.suite_scale_factor, r.db, r.db_driver
              order by r.finished_at desc, r.id desc
            ) as run_rank
          from run r
          where r.operation = 'select'
            and r.status = 'completed'
            and r.finished_at is not null
            and {where_sql}
            and {pin_sql}
        ),
        query_steps as (
          select
            lr.run_id, lr.system, lr.suite, lr.suite_scale_factor, lr.db, lr.db_version, lr.db_driver,
            s.id as run_step_id, s.query_name, s.iteration, s.row_count,
            json_extract_string(s."metadata", '$.answer_hash') as answer_hash,
            json_extract_string(s."metadata", '$.answer_hash_version') as answer_hash_version
          from latest_runs lr
          join run_step s on s.run_id = lr.run_id
          where lr.run_rank = 1
            and s.step_type = 'query'
            and s.status = 'completed'
            and s.result_status in ('ok', 'wrong_result')
            and s.query_name is not null
            and s.iteration is not null
            and s.row_count is not null
            {exclusion_sql}
        )
        """
        return sql, params


def _database_label(db: str, db_version: str, db_driver: str | None) -> str:
    driver = f"/{db_driver}" if db_driver is not None else ""
    return f"{db}{driver} {db_version}"


def _resolve_results_path(revision: Revision, db_path: Path | None) -> Path:
    path = db_path or get_settings().results_directory / f"{revision}.db"
    if not path.is_file():
        raise FileNotFoundError(f"Results database does not exist: {path}")
    return path


def _connect(path: Path, read_only: bool) -> duckdb.DuckDBPyConnection:
    return cast(Any, duckdb).connect(str(path), read_only=read_only)


def _load_row_count_mismatches(con: duckdb.DuckDBPyConnection, scope: _Scope) -> list[RowCountMismatch]:
    cte, params = scope.latest_runs_cte()
    rows = con.execute(
        f"""
        {cte},
        mismatches as (
          select system, suite, suite_scale_factor, query_name, iteration
          from query_steps
          group by system, suite, suite_scale_factor, query_name, iteration
          having count(distinct row_count) > 1
             and count(distinct db || chr(31) || db_version || chr(31) || coalesce(db_driver, '')) > 1
        )
        select
          q.system, q.suite, q.suite_scale_factor, q.query_name, q.iteration,
          q.run_step_id, q.db, q.db_version, q.db_driver, q.row_count
        from query_steps q
        join mismatches m using (system, suite, suite_scale_factor, query_name, iteration)
        order by
          q.system, q.suite, q.suite_scale_factor, q.query_name, q.iteration, q.row_count, q.db, q.db_version,
          q.db_driver
        """,
        params,
    ).fetchall()

    grouped: dict[tuple[str, str, int, str, int], list[RowCountObservation]] = {}
    for row in rows:
        key = (str(row[0]), str(row[1]), int(row[2]), str(row[3]), int(row[4]))
        grouped.setdefault(key, []).append(
            RowCountObservation(
                db=str(row[6]),
                db_version=str(row[7]),
                db_driver=cast(str | None, row[8]),
                row_count=int(row[9]),
                run_step_id=int(row[5]),
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


def _load_answer_hash_mismatches(con: duckdb.DuckDBPyConnection, scope: _Scope) -> list[AnswerHashMismatch]:
    cte, params = scope.latest_runs_cte()
    rows = con.execute(
        f"""
        {cte},
        query_answers as (
          select * from query_steps where answer_hash is not null
        ),
        mismatches as (
          select system, suite, suite_scale_factor, query_name, iteration
          from query_answers
          group by system, suite, suite_scale_factor, query_name, iteration
          -- hashes are only comparable within one canonicalisation version; a version bump
          -- otherwise reports every stored result as a mismatch against freshly measured ones
          having count(distinct answer_hash) > 1
             and count(distinct coalesce(answer_hash_version, '')) = 1
             and count(distinct row_count) = 1
             and count(distinct db || chr(31) || db_version || chr(31) || coalesce(db_driver, '')) > 1
        )
        select
          q.system, q.suite, q.suite_scale_factor, q.query_name, q.iteration,
          q.run_step_id, q.db, q.db_version, q.db_driver, q.row_count, q.answer_hash
        from query_answers q
        join mismatches m using (system, suite, suite_scale_factor, query_name, iteration)
        order by
          q.system, q.suite, q.suite_scale_factor, q.query_name, q.iteration, q.answer_hash, q.db, q.db_version,
          q.db_driver
        """,
        params,
    ).fetchall()

    grouped: dict[tuple[str, str, int, str, int], list[AnswerHashObservation]] = {}
    for row in rows:
        key = (str(row[0]), str(row[1]), int(row[2]), str(row[3]), int(row[4]))
        grouped.setdefault(key, []).append(
            AnswerHashObservation(
                db=str(row[6]),
                db_version=str(row[7]),
                db_driver=cast(str | None, row[8]),
                row_count=int(row[9]),
                answer_hash=str(row[10]),
                run_step_id=int(row[5]),
            )
        )

    return [
        AnswerHashMismatch(
            system=system,
            suite=suite_name,
            suite_scale_factor=scale_factor,
            query_name=query_name,
            iteration=iteration,
            observations=tuple(observations),
        )
        for (system, suite_name, scale_factor, query_name, iteration), observations in grouped.items()
    ]


def _load_iteration_inconsistencies(con: duckdb.DuckDBPyConnection, scope: _Scope) -> list[IterationInconsistency]:
    cte, params = scope.latest_runs_cte()
    rows = con.execute(
        f"""
        {cte}
        select
          run_id, system, db, db_version, db_driver, suite, suite_scale_factor, query_name,
          list(distinct answer_hash order by answer_hash) as answer_hashes
        from query_steps
        where answer_hash is not null
        group by run_id, system, db, db_version, db_driver, suite, suite_scale_factor, query_name
        having count(distinct answer_hash) > 1 and count(distinct coalesce(answer_hash_version, '')) = 1
        order by system, suite, suite_scale_factor, query_name, db, db_driver, run_id
        """,
        params,
    ).fetchall()

    return [
        IterationInconsistency(
            run_id=int(row[0]),
            system=str(row[1]),
            db=str(row[2]),
            db_version=str(row[3]),
            db_driver=cast(str | None, row[4]),
            suite=str(row[5]),
            suite_scale_factor=int(row[6]),
            query_name=str(row[7]),
            answer_hashes=tuple(str(value) for value in cast(list[object], row[8])),
        )
        for row in rows
    ]


def _consensus[T](values: list[T]) -> T | None:
    counts = Counter(values)
    if not counts:
        return None

    highest_count = max(counts.values())
    consensus_values = [value for value, count in counts.items() if count == highest_count]
    if len(consensus_values) != 1 or highest_count == 1:
        return None

    return consensus_values[0]


def _row_count_marks(mismatches: list[RowCountMismatch]) -> dict[int, tuple[str, str]]:
    marks: dict[int, tuple[str, str]] = {}
    for mismatch in mismatches:
        expected = _consensus([observation.row_count for observation in mismatch.observations])
        if expected is None:
            continue
        values = ", ".join(
            f"{_database_label(o.db, o.db_version, o.db_driver)}: {o.row_count}" for o in mismatch.observations
        )
        message = (
            "Row count differs from the consensus of the latest completed select runs: "
            f"{mismatch.system} {mismatch.suite} sf{mismatch.suite_scale_factor} "
            f"{mismatch.query_name} iteration {mismatch.iteration}; expected {expected}; observed {values}"
        )
        for observation in mismatch.observations:
            if observation.run_step_id is not None and observation.row_count != expected:
                marks[observation.run_step_id] = (ROW_COUNT_MISMATCH, message)
    return marks


def _answer_hash_marks(mismatches: list[AnswerHashMismatch]) -> dict[int, tuple[str, str]]:
    marks: dict[int, tuple[str, str]] = {}
    for mismatch in mismatches:
        expected = _consensus([observation.answer_hash for observation in mismatch.observations])
        if expected is None:
            continue
        values = ", ".join(
            f"{_database_label(o.db, o.db_version, o.db_driver)}: {o.answer_hash}" for o in mismatch.observations
        )
        message = (
            "Answer hash differs from the consensus of the latest completed select runs: "
            f"{mismatch.system} {mismatch.suite} sf{mismatch.suite_scale_factor} "
            f"{mismatch.query_name} iteration {mismatch.iteration}; expected {expected}; observed {values}"
        )
        for observation in mismatch.observations:
            if observation.run_step_id is not None and observation.answer_hash != expected:
                marks[observation.run_step_id] = (ANSWER_HASH_MISMATCH, message)
    return marks


ROW_COUNT_MISMATCH = "RowCountMismatch"
ANSWER_HASH_MISMATCH = "AnswerHashMismatch"


def _clear_marks(con: duckdb.DuckDBPyConnection, scope: _Scope) -> int:
    where_sql, params = scope.run_filter()
    # marks only ever replace 'ok' (comparisons read ok and wrong_result steps only), so clearing restores 'ok'
    row = con.execute(
        f"""
        update run_step
        set result_status = 'ok', error_type = null, error_message = null
        where result_status = 'wrong_result'
          and error_type in ('{ROW_COUNT_MISMATCH}', '{ANSWER_HASH_MISMATCH}')
          and run_id in (select r.id from run r where {where_sql})
        """,
        params,
    ).fetchone()
    return int(row[0]) if row is not None else 0


def _apply_marks(con: duckdb.DuckDBPyConnection, marks: dict[int, tuple[str, str]]) -> None:
    for run_step_id, (error_type, message) in sorted(marks.items()):
        con.execute(
            """
            update run_step
            set result_status = 'wrong_result', error_type = ?, error_message = ?
            where id = ?
            """,
            [error_type, message, run_step_id],
        )


def validate_results(
    revision: Revision = "default",
    db_path: Path | None = None,
    *,
    pins: Mapping[str, str] | None = None,
    system: str | None = None,
    suite: SuiteName | None = None,
    suite_scale_factor: int | None = None,
    mark: bool = False,
) -> ValidationReport:
    path = _resolve_results_path(revision, db_path)
    scope = _Scope(pins=pins, system=system, suite=suite, suite_scale_factor=suite_scale_factor)
    con = _connect(path, read_only=not mark)
    try:
        if not mark:
            return ValidationReport(
                row_count_mismatches=_load_row_count_mismatches(con, scope),
                answer_hash_mismatches=_load_answer_hash_mismatches(con, scope),
                iteration_inconsistencies=_load_iteration_inconsistencies(con, scope),
            )

        con.execute("begin")
        try:
            cleared = _clear_marks(con, scope)
            row_count_mismatches = _load_row_count_mismatches(con, scope)
            answer_hash_mismatches = _load_answer_hash_mismatches(con, scope)
            marks = {**_answer_hash_marks(answer_hash_mismatches), **_row_count_marks(row_count_mismatches)}
            _apply_marks(con, marks)
            report = ValidationReport(
                row_count_mismatches=row_count_mismatches,
                answer_hash_mismatches=answer_hash_mismatches,
                iteration_inconsistencies=_load_iteration_inconsistencies(con, scope),
                marks_cleared=cleared,
                marks_applied=len(marks),
            )
            con.execute("commit")
        except BaseException:
            con.execute("rollback")
            raise
        return report
    finally:
        con.close()


def clear_wrong_result_marks(
    revision: Revision = "default",
    db_path: Path | None = None,
    *,
    system: str | None = None,
    suite: SuiteName | None = None,
    suite_scale_factor: int | None = None,
) -> int:
    path = _resolve_results_path(revision, db_path)
    con = _connect(path, read_only=False)
    try:
        return _clear_marks(con, _Scope(pins=None, system=system, suite=suite, suite_scale_factor=suite_scale_factor))
    finally:
        con.close()


def format_row_count_mismatches(mismatches: list[RowCountMismatch], limit: int = 20) -> str:
    if not mismatches:
        return "Row counts match across latest completed select runs."

    lines = ["Row-count mismatches across latest completed select runs:"]
    for mismatch in mismatches[:limit]:
        values = ", ".join(
            f"{_database_label(o.db, o.db_version, o.db_driver)}: {o.row_count}" for o in mismatch.observations
        )
        lines.append(
            f"- {mismatch.system} {mismatch.suite} sf{mismatch.suite_scale_factor} "
            f"{mismatch.query_name} iteration {mismatch.iteration}: {values}"
        )

    omitted = len(mismatches) - limit
    if omitted > 0:
        lines.append(f"... and {omitted} more mismatch(es)")

    return "\n".join(lines)


def format_answer_hash_mismatches(mismatches: list[AnswerHashMismatch], limit: int = 20) -> str:
    if not mismatches:
        return "Answer hashes match across latest completed select runs."

    lines = ["Answer-hash mismatches across latest completed select runs:"]
    for mismatch in mismatches[:limit]:
        values = ", ".join(
            f"{_database_label(o.db, o.db_version, o.db_driver)}: {o.answer_hash}" for o in mismatch.observations
        )
        lines.append(
            f"- {mismatch.system} {mismatch.suite} sf{mismatch.suite_scale_factor} "
            f"{mismatch.query_name} iteration {mismatch.iteration}: {values}"
        )

    omitted = len(mismatches) - limit
    if omitted > 0:
        lines.append(f"... and {omitted} more mismatch(es)")

    return "\n".join(lines)


def format_iteration_inconsistencies(inconsistencies: list[IterationInconsistency], limit: int = 20) -> str:
    if not inconsistencies:
        return "Answer hashes are stable across iterations of each run."

    lines = ["Queries whose answer hash differs between iterations of one run (non-deterministic):"]
    for item in inconsistencies[:limit]:
        lines.append(
            f"- run {item.run_id} {_database_label(item.db, item.db_version, item.db_driver)} "
            f"{item.system} {item.suite} sf{item.suite_scale_factor} {item.query_name}: "
            f"{len(item.answer_hashes)} distinct hashes"
        )

    omitted = len(inconsistencies) - limit
    if omitted > 0:
        lines.append(f"... and {omitted} more")

    return "\n".join(lines)
