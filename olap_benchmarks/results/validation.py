from __future__ import annotations

from collections import Counter
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
    run_step_id: int | None = None


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


@dataclass(frozen=True)
class AnswerHashObservation:
    db: str
    db_version: str
    row_count: int
    answer_hash: str
    run_step_id: int | None = None


@dataclass(frozen=True)
class AnswerHashMismatch:
    system: str
    suite: str
    suite_scale_factor: int
    query_name: str
    iteration: int
    observations: tuple[AnswerHashObservation, ...]


class AnswerHashValidationError(RuntimeError):
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

    return _load_latest_query_row_count_mismatches(
        path=path,
        system=system,
        suite=suite,
        suite_scale_factor=suite_scale_factor,
    )


def _load_latest_query_row_count_mismatches(
    *,
    path: Path,
    system: str | None,
    suite: SuiteName | None,
    suite_scale_factor: int | None,
) -> list[RowCountMismatch]:
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
              partition by system, suite, suite_scale_factor, db
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
            s.id as run_step_id,
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
          qc.run_step_id,
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
                db=str(row[6]),
                db_version=str(row[7]),
                row_count=int(row[8]),
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


def _consensus_row_count(mismatch: RowCountMismatch) -> int | None:
    counts = Counter(observation.row_count for observation in mismatch.observations)
    if not counts:
        return None

    highest_count = max(counts.values())
    consensus_values = [row_count for row_count, count in counts.items() if count == highest_count]
    if len(consensus_values) != 1 or highest_count == 1:
        return None

    return consensus_values[0]


def _format_wrong_result_message(mismatch: RowCountMismatch, expected_row_count: int) -> str:
    values = ", ".join(
        f"{observation.db} {observation.db_version}: {observation.row_count}" for observation in mismatch.observations
    )
    return (
        "Row count differs from latest completed select-run consensus: "
        f"{mismatch.system} {mismatch.suite} sf{mismatch.suite_scale_factor} "
        f"{mismatch.query_name} iteration {mismatch.iteration}; "
        f"expected {expected_row_count}; observed {values}"
    )


def mark_latest_query_row_count_mismatches_as_wrong_results(
    revision: Revision = "default",
    db_path: Path | None = None,
    system: str | None = None,
    suite: SuiteName | None = None,
    suite_scale_factor: int | None = None,
) -> list[RowCountMismatch]:
    path = _resolve_results_path(revision, db_path)
    mismatches = _load_latest_query_row_count_mismatches(
        path=path,
        system=system,
        suite=suite,
        suite_scale_factor=suite_scale_factor,
    )
    updates: list[tuple[int, str]] = []

    for mismatch in mismatches:
        expected_row_count = _consensus_row_count(mismatch)
        if expected_row_count is None:
            continue

        message = _format_wrong_result_message(mismatch, expected_row_count)
        updates.extend(
            (observation.run_step_id, message)
            for observation in mismatch.observations
            if observation.run_step_id is not None and observation.row_count != expected_row_count
        )

    if not updates:
        return mismatches

    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(path), read_only=False)
    try:
        for run_step_id, message in updates:
            con.execute(
                """
                update run_step
                set
                  result_status = 'wrong_result',
                  error_type = coalesce(error_type, 'RowCountMismatch'),
                  error_message = coalesce(error_message, ?)
                where id = ?
                """,
                [message, run_step_id],
            )
    finally:
        con.close()

    return mismatches


def validate_latest_query_answer_hashes(
    revision: Revision = "default",
    db_path: Path | None = None,
    system: str | None = None,
    suite: SuiteName | None = None,
    suite_scale_factor: int | None = None,
) -> list[AnswerHashMismatch]:
    path = _resolve_results_path(revision, db_path)

    return _load_latest_query_answer_hash_mismatches(
        path=path,
        system=system,
        suite=suite,
        suite_scale_factor=suite_scale_factor,
    )


def _load_latest_query_answer_hash_mismatches(
    *,
    path: Path,
    system: str | None,
    suite: SuiteName | None,
    suite_scale_factor: int | None,
) -> list[AnswerHashMismatch]:
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
              partition by system, suite, suite_scale_factor, db
              order by finished_at desc, run_id desc
            ) as run_rank
          from scoped_runs
        ),
        query_answers as (
          select
            lr.system,
            lr.suite,
            lr.suite_scale_factor,
            s.query_name,
            s.iteration,
            s.id as run_step_id,
            lr.db,
            lr.db_version,
            s.row_count,
            json_extract_string(s."metadata", '$.answer_hash') as answer_hash
          from latest_runs lr
          join run_step s on s.run_id = lr.run_id
          where lr.run_rank = 1
            and s.step_type = 'query'
            and s.status = 'completed'
            and s.query_name is not null
            and s.iteration is not null
            and s.row_count is not null
            and s."metadata" is not null
            and json_extract_string(s."metadata", '$.answer_hash') is not null
        ),
        mismatches as (
          select
            system,
            suite,
            suite_scale_factor,
            query_name,
            iteration
          from query_answers
          group by system, suite, suite_scale_factor, query_name, iteration
          having count(distinct answer_hash) > 1
             and count(distinct row_count) = 1
             and count(distinct db || chr(31) || db_version) > 1
        )
        select
          qa.system,
          qa.suite,
          qa.suite_scale_factor,
          qa.query_name,
          qa.iteration,
          qa.run_step_id,
          qa.db,
          qa.db_version,
          qa.row_count,
          qa.answer_hash
        from query_answers qa
        join mismatches m
          on qa.system = m.system
         and qa.suite = m.suite
         and qa.suite_scale_factor = m.suite_scale_factor
         and qa.query_name = m.query_name
         and qa.iteration = m.iteration
        order by
          qa.system,
          qa.suite,
          qa.suite_scale_factor,
          qa.query_name,
          qa.iteration,
          qa.answer_hash,
          qa.db,
          qa.db_version
    """

    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(path), read_only=True)
    try:
        rows = con.execute(sql, params).fetchall()
    finally:
        con.close()

    grouped: dict[tuple[str, str, int, str, int], list[AnswerHashObservation]] = {}
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
            AnswerHashObservation(
                db=str(row[6]),
                db_version=str(row[7]),
                row_count=int(row[8]),
                answer_hash=str(row[9]),
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


def _consensus_answer_hash(mismatch: AnswerHashMismatch) -> str | None:
    counts = Counter(observation.answer_hash for observation in mismatch.observations)
    if not counts:
        return None

    highest_count = max(counts.values())
    consensus_values = [answer_hash for answer_hash, count in counts.items() if count == highest_count]
    if len(consensus_values) != 1 or highest_count == 1:
        return None

    return consensus_values[0]


def _format_answer_hash_wrong_result_message(mismatch: AnswerHashMismatch, expected_answer_hash: str) -> str:
    values = ", ".join(
        f"{observation.db} {observation.db_version}: {observation.answer_hash}" for observation in mismatch.observations
    )
    return (
        "Answer hash differs from latest completed select-run consensus: "
        f"{mismatch.system} {mismatch.suite} sf{mismatch.suite_scale_factor} "
        f"{mismatch.query_name} iteration {mismatch.iteration}; "
        f"expected {expected_answer_hash}; observed {values}"
    )


def mark_latest_query_answer_hash_mismatches_as_wrong_results(
    revision: Revision = "default",
    db_path: Path | None = None,
    system: str | None = None,
    suite: SuiteName | None = None,
    suite_scale_factor: int | None = None,
) -> list[AnswerHashMismatch]:
    path = _resolve_results_path(revision, db_path)
    mismatches = _load_latest_query_answer_hash_mismatches(
        path=path,
        system=system,
        suite=suite,
        suite_scale_factor=suite_scale_factor,
    )
    updates: list[tuple[int, str]] = []

    for mismatch in mismatches:
        expected_answer_hash = _consensus_answer_hash(mismatch)
        if expected_answer_hash is None:
            continue

        message = _format_answer_hash_wrong_result_message(mismatch, expected_answer_hash)
        updates.extend(
            (observation.run_step_id, message)
            for observation in mismatch.observations
            if observation.run_step_id is not None and observation.answer_hash != expected_answer_hash
        )

    if not updates:
        return mismatches

    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(path), read_only=False)
    try:
        for run_step_id, message in updates:
            con.execute(
                """
                update run_step
                set
                  result_status = 'wrong_result',
                  error_type = coalesce(error_type, 'AnswerHashMismatch'),
                  error_message = coalesce(error_message, ?)
                where id = ?
                """,
                [message, run_step_id],
            )
    finally:
        con.close()

    return mismatches


def format_answer_hash_mismatches(mismatches: list[AnswerHashMismatch], limit: int = 20) -> str:
    if not mismatches:
        return "Answer hashes match across latest completed select runs."

    lines = ["Answer-hash mismatches across latest completed select runs:"]
    for mismatch in mismatches[:limit]:
        values = ", ".join(
            f"{observation.db} {observation.db_version}: {observation.answer_hash}"
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


def assert_latest_query_answer_hashes(
    revision: Revision = "default",
    db_path: Path | None = None,
    system: str | None = None,
    suite: SuiteName | None = None,
    suite_scale_factor: int | None = None,
    mark_wrong_results: bool = False,
) -> None:
    validate = (
        mark_latest_query_answer_hash_mismatches_as_wrong_results
        if mark_wrong_results
        else validate_latest_query_answer_hashes
    )
    mismatches = validate(
        revision=revision,
        db_path=db_path,
        system=system,
        suite=suite,
        suite_scale_factor=suite_scale_factor,
    )
    if mismatches:
        raise AnswerHashValidationError(format_answer_hash_mismatches(mismatches))


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
    mark_wrong_results: bool = False,
) -> None:
    validate = (
        mark_latest_query_row_count_mismatches_as_wrong_results
        if mark_wrong_results
        else validate_latest_query_row_counts
    )
    mismatches = validate(
        revision=revision,
        db_path=db_path,
        system=system,
        suite=suite,
        suite_scale_factor=suite_scale_factor,
    )
    if mismatches:
        raise RowCountValidationError(format_row_count_mismatches(mismatches))
