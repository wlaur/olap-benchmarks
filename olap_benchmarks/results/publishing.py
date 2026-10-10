from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, cast

import duckdb

from ..matrix import MatrixCell
from ..profile import BENCHMARK_PROFILE
from .cells import StoredRun, latest_run_per_cell, load_runs
from .validation import validate_results

MAX_LISTED = 30


class PublishValidationError(RuntimeError):
    pass


def _listing(title: str, items: Sequence[str]) -> str:
    lines = [f"{title} ({len(items)}):"]
    lines.extend(f"    {item}" for item in items[:MAX_LISTED])
    if len(items) > MAX_LISTED:
        lines.append(f"    ... and {len(items) - MAX_LISTED} more")
    return "\n".join(lines)


def publishable_run_ids(
    runs: Iterable[StoredRun],
    cells: Iterable[MatrixCell],
    *,
    system: str,
    drivers: Mapping[str, str | None],
) -> set[int]:
    latest = latest_run_per_cell(runs, cells, system=system, drivers=drivers)
    return {run.id for run in latest.values() if run is not None}


def single_system(runs: Iterable[StoredRun]) -> str:
    systems = sorted({run.system for run in runs})
    if len(systems) != 1:
        raise PublishValidationError(f"A published set must come from exactly one system, found: {systems or 'none'}")
    return systems[0]


def _fetch_ints(con: duckdb.DuckDBPyConnection, sql: str) -> list[int]:
    return [int(row[0]) for row in con.execute(sql).fetchall()]


def _query_step_problems(con: duckdb.DuckDBPyConnection) -> list[str]:
    problems: list[str] = []

    runs_without_queries = con.execute(
        """
        select r.id, r.db, r.suite
        from run r
        where r.status = 'completed'
          and r.operation = 'select'
          and not exists (select 1 from run_step s where s.run_id = r.id and s.step_type = 'query')
        order by r.id
        """
    ).fetchall()
    if runs_without_queries:
        problems.append(
            _listing(
                "Select runs without query steps",
                [f"run {row[0]} {row[1]}:{row[2]}" for row in runs_without_queries],
            )
        )

    invalid_steps = con.execute(
        """
        select s.id, r.id, r.db, r.suite, s.query_name, s.iteration, s.status, s.result_status
        from run_step s
        join run r on r.id = s.run_id
        where r.status = 'completed'
          and r.operation = 'select'
          and s.step_type = 'query'
          and s.result_status is distinct from 'wrong_result'
          and (
            s.status <> 'completed'
            or s.result_status not in ('ok', 'skipped', 'unsupported')
            or (
              s.result_status = 'ok'
              and (
                s.row_count is null
                or (
                  json_extract_string(s."metadata", '$.answer_hash') is null
                  and json_extract_string(s."metadata", '$.answer_hash_skipped_reason') is null
                )
              )
            )
            or (
              s.result_status in ('skipped', 'unsupported')
              and json_extract_string(s."metadata", '$.skip_reason') is null
            )
          )
        order by s.id
        """
    ).fetchall()
    if invalid_steps:
        problems.append(
            _listing(
                "Query steps without a correctness result (failed, unhashed or unexplained skips)",
                [
                    f"step {row[0]} (run {row[1]} {row[2]}:{row[3]} {row[4]} iteration {row[5]}: "
                    f"status={row[6]}, result_status={row[7]})"
                    for row in invalid_steps
                ],
            )
        )

    wrong_results = con.execute(
        """
        select r.db, r.suite, s.query_name, count(*)
        from run_step s
        join run r on r.id = s.run_id
        where s.result_status = 'wrong_result'
        group by all
        order by all
        """
    ).fetchall()
    if wrong_results:
        problems.append(
            _listing(
                "Queries marked wrong_result",
                [f"{row[0]}:{row[1]} {row[2]} ({row[3]} iteration(s))" for row in wrong_results],
            )
        )

    return problems


def _monetdb_problems(con: duckdb.DuckDBPyConnection) -> list[str]:
    problems: list[str] = []
    drivers = sorted(
        {
            str(row[0]) if row[0] is not None else "none"
            for row in con.execute("select distinct db_driver from run where db = 'monetdb'").fetchall()
        }
    )
    unexpected_drivers = [driver for driver in drivers if driver != "adbc"]
    if unexpected_drivers:
        problems.append(f"Unknown MonetDB db_driver values: {unexpected_drivers}")

    missing_baselines = _fetch_ints(
        con,
        """
        select r.id
        from run r
        where r.db = 'monetdb'
          and r.status in ('completed', 'failed')
          and (
            (
              select count(*)
              from run_step s
              where s.run_id = r.id
                and s.step_name = 'session_baseline_before'
                and s.status = 'completed'
                and s.row_count = 1
                and coalesce(cast(json_extract(s."metadata", '$.temporary_ingest_tables') as integer), -1) = 0
            ) <> 1
            or
            (
              select count(*)
              from run_step s
              where s.run_id = r.id
                and s.step_name = 'session_baseline_after'
                and s.status = 'completed'
                and s.row_count = 0
                and coalesce(cast(json_extract(s."metadata", '$.temporary_ingest_tables') as integer), -1) = 0
            ) <> 1
          )
        order by r.id
        """,
    )
    if missing_baselines:
        problems.append(f"MonetDB runs lack clean session baselines: {missing_baselines}")

    return problems


def _image_digest_problems(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute(
        """
        select id, db, suite, suite_scale_factor, operation, json_extract("metadata", '$.execution')
        from run
        where json_extract_string("metadata", '$.execution.mode') is distinct from 'in_process'
        order by id
        """
    ).fetchall()
    missing: list[str] = []
    digests_by_image: dict[tuple[str, str], set[str]] = {}
    for run_id, db, suite, scale_factor, operation, execution_json in rows:
        execution = cast(dict[str, Any], json.loads(execution_json)) if execution_json is not None else {}
        images = cast(dict[str, str] | None, execution.get("container_images")) or {}
        digests = cast(dict[str, str | None] | None, execution.get("container_image_digests")) or {}
        label = f"run {run_id} {db} {suite}:sf{scale_factor}:{operation}"
        if not images or any(not digests.get(role) for role in images):
            missing.append(label)
            continue
        for role in images:
            digests_by_image.setdefault((str(db), role), set()).add(str(digests[role]))

    problems: list[str] = []
    if missing:
        problems.append(_listing("Container runs without a registry digest for every image", missing))
    mixed = [
        f"{db} {role}: {', '.join(sorted(digests))}"
        for (db, role), digests in sorted(digests_by_image.items())
        if len(digests) > 1
    ]
    if mixed:
        problems.append(_listing("Engines whose runs used different images", mixed))
    return problems


def validate_publishable_runs(
    db_path: Path,
    cells: Sequence[MatrixCell],
    *,
    pins: Mapping[str, str],
    drivers: Mapping[str, str | None],
) -> None:
    problems: list[str] = []
    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(db_path), read_only=True)

    try:
        runs = load_runs(db_path, con)
        system = single_system(runs)
        latest = latest_run_per_cell(runs, cells, system=system, drivers=drivers)

        missing = [cell.label for cell, run in latest.items() if run is None]
        if missing:
            problems.append(_listing("Declared cells without a run", missing))

        not_completed = [run.label for run in latest.values() if run is not None and run.status != "completed"]
        if not_completed:
            problems.append(_listing("Declared cells whose latest run did not complete", not_completed))

        declared_ids = {run.id for run in latest.values() if run is not None}
        extra = [run.label for run in runs if run.id not in declared_ids]
        if extra:
            problems.append(
                _listing(
                    "Runs outside the declared matrix or superseded by a later run of the same cell "
                    "(see `olap results prune --superseded`)",
                    extra,
                )
            )

        stale = [run.label for run in runs if run.db_version != pins.get(run.db)]
        if stale:
            problems.append(_listing("Runs of a version other than the current pin", stale))

        commits = sorted({run.commit or "none" for run in runs})
        if len(commits) != 1 or commits == ["none"]:
            by_commit = [
                f"{commit}: {sum(1 for run in runs if (run.commit or 'none') == commit)} run(s)" for commit in commits
            ]
            problems.append(_listing("A published set must come from exactly one repository commit", by_commit))

        profiles = sorted({run.benchmark_profile for run in runs})
        if profiles != [BENCHMARK_PROFILE.id]:
            counts = Counter(run.benchmark_profile for run in runs)
            listed = [f"{profile}: {counts[profile]} run(s)" for profile in profiles]
            title = f"A published set must follow exactly one benchmark profile, {BENCHMARK_PROFILE.id}"
            problems.append(_listing(title, listed))

        dirty = [run.label for run in runs if run.dirty is not False]
        if dirty:
            problems.append(_listing("Runs from a dirty or unrecorded working tree", dirty))

        snapshots = sorted(
            {
                str(row[0]) if row[0] is not None else "none"
                for row in con.execute("select distinct system_snapshot_id from run").fetchall()
            }
        )
        if len(snapshots) != 1 or snapshots == ["none"]:
            problems.append(f"A published set must share exactly one system snapshot, found: {snapshots}")

        problems.extend(_image_digest_problems(con))
        problems.extend(_query_step_problems(con))
        problems.extend(_monetdb_problems(con))
    finally:
        con.close()

    report = validate_results(db_path=db_path, pins=pins)
    if not report.ok:
        problems.append(f"Answer validation failed:\n{report.format()}")

    if problems:
        raise PublishValidationError("Refusing to publish:\n- " + "\n- ".join(problems))
