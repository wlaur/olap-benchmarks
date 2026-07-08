from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import duckdb

_LOGGER = logging.getLogger(__name__)

# Run rows have no stable id across database files (each file starts its
# sequences at 1), so runs are matched by this natural key when merging.
RUN_NATURAL_KEY = ("system", "db", "db_version", "suite", "suite_scale_factor", "operation", "started_at")

MERGE_TABLES = ("run", "run_step", "run_metric", "query_execution")


@dataclass(frozen=True)
class MergeStats:
    runs_added: int
    runs_replaced: int
    run_steps_added: int
    run_metrics_added: int
    query_executions_added: int


def _fetch_scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = cast(Any, con).execute(sql).fetchone()
    assert row is not None
    return int(row[0])


def _alembic_revision(con: duckdb.DuckDBPyConnection, catalog: str) -> str:
    try:
        row = cast(Any, con).execute(f"select version_num from {catalog}.alembic_version").fetchone()
    except duckdb.CatalogException as exc:
        raise RuntimeError(f"Database has no alembic_version table ({catalog})") from exc

    if row is None:
        raise RuntimeError(f"Database has an empty alembic_version table ({catalog})")

    return str(row[0])


def _require_same_schema_revision(con: duckdb.DuckDBPyConnection, head: str) -> None:
    for catalog, label in (("main", "destination"), ("src", "source")):
        revision = _alembic_revision(con, catalog)
        if revision != head:
            raise RuntimeError(
                f"The {label} database is at schema revision '{revision}' but the current head is '{head}'. "
                "Run `olap results migrate` first."
            )


def merge_results(source_db_path: Path, dest_db_path: Path, head_revision: str) -> MergeStats:
    """Merge all runs from `source_db_path` into `dest_db_path`.

    Runs are matched on RUN_NATURAL_KEY: matching destination runs are replaced
    (the whole run subtree), unmatched source runs are added. Ids are remapped
    since both files allocate ids from 1. Orphaned 'running' runs are skipped.
    """
    con = cast(Any, duckdb).connect(str(dest_db_path))

    try:
        con.execute(f"ATTACH '{source_db_path.as_posix()}' AS src (READ_ONLY)")
        _require_same_schema_revision(con, head_revision)

        con.execute("BEGIN")
        try:
            stats = _merge_attached(con)
            _sync_sequences(con)
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()

    _LOGGER.info(
        f"Merged {source_db_path.name} into {dest_db_path}: "
        f"{stats.runs_added} run(s) added, {stats.runs_replaced} replaced"
    )
    return stats


def _merge_attached(con: duckdb.DuckDBPyConnection) -> MergeStats:
    key_join = " and ".join(f"d.{column} = s.{column}" for column in RUN_NATURAL_KEY)

    con.execute(
        f"""
        create or replace temp table matched_run as
        select d.id as dest_id, s.id as src_id
        from run d join src.run s on {key_join}
        where s.status <> 'running'
        """
    )
    runs_replaced = _fetch_scalar(con, "select count(*) from matched_run")

    con.execute("delete from query_execution where run_id in (select dest_id from matched_run)")
    con.execute("delete from run_metric where run_id in (select dest_id from matched_run)")
    con.execute("delete from run_step where run_id in (select dest_id from matched_run)")

    run_offset = _fetch_scalar(con, "select coalesce(max(id), 0) from run")
    step_offset = _fetch_scalar(con, "select coalesce(max(id), 0) from run_step")
    metric_offset = _fetch_scalar(con, "select coalesce(max(id), 0) from run_metric")
    query_execution_offset = _fetch_scalar(con, "select coalesce(max(id), 0) from query_execution")

    con.execute(
        """
        create or replace temp table unmatched_source_run as
        select s.id
        from src.run s
        left join matched_run m on s.id = m.src_id
        where s.status <> 'running' and m.src_id is null
        """
    )
    runs_added = _fetch_scalar(con, "select count(*) from unmatched_source_run")

    con.execute(
        """
        update run as d
        set
            suite = s.suite,
            suite_scale_factor = s.suite_scale_factor,
            db = s.db,
            db_version = s.db_version,
            operation = s.operation,
            system = s.system,
            status = s.status,
            started_at = s.started_at,
            finished_at = s.finished_at,
            error_type = s.error_type,
            error_message = s.error_message,
            "metadata" = s."metadata"
        from src.run s
        join matched_run m on s.id = m.src_id
        where d.id = m.dest_id
        """
    )

    con.execute(
        f"""
        create or replace temp table run_map as
        select src_id, dest_id as new_id from matched_run
        union all
        select s.id as src_id, {run_offset} + row_number() over (order by s.id) as new_id
        from unmatched_source_run s
        """
    )
    con.execute(
        f"""
        create or replace temp table step_map as
        select s.id as src_id, {step_offset} + row_number() over (order by s.id) as new_id
        from src.run_step s join run_map m on s.run_id = m.src_id
        """
    )

    con.execute(
        """
        insert into run (id, suite, suite_scale_factor, db, db_version, operation, system, status,
                         started_at, finished_at, error_type, error_message, "metadata")
        select m.new_id, s.suite, s.suite_scale_factor, s.db, s.db_version, s.operation, s.system, s.status,
               s.started_at, s.finished_at, s.error_type, s.error_message, s."metadata"
        from src.run s
        join run_map m on s.id = m.src_id
        join unmatched_source_run u on s.id = u.id
        """
    )

    con.execute(
        """
        insert into run_step (id, run_id, step_type, step_name, query_name, iteration, table_name,
                              started_at, finished_at, status, result_status, iteration_role,
                              row_count, error_type, error_message, "metadata")
        select sm.new_id, rm.new_id, s.step_type, s.step_name, s.query_name, s.iteration, s.table_name,
               s.started_at, s.finished_at, s.status, s.result_status, s.iteration_role,
               s.row_count, s.error_type, s.error_message, s."metadata"
        from src.run_step s
        join run_map rm on s.run_id = rm.src_id
        join step_map sm on s.id = sm.src_id
        """
    )
    run_steps_added = _fetch_scalar(con, "select count(*) from step_map")

    con.execute(
        f"""
        insert into run_metric (id, run_id, time, cpu_percent, mem_mb, disk_mb)
        select {metric_offset} + row_number() over (order by s.id),
               rm.new_id, s.time, s.cpu_percent, s.mem_mb, s.disk_mb
        from src.run_metric s join run_map rm on s.run_id = rm.src_id
        """
    )
    run_metrics_added = _fetch_scalar(con, f"select count(*) from run_metric where id > {metric_offset}")

    con.execute(
        f"""
        insert into query_execution (id, run_id, run_step_id, query, start_time, end_time)
        select {query_execution_offset} + row_number() over (order by s.id),
               rm.new_id, sm.new_id, s.query, s.start_time, s.end_time
        from src.query_execution s
        join run_map rm on s.run_id = rm.src_id
        left join step_map sm on s.run_step_id = sm.src_id
        """
    )
    query_executions_added = _fetch_scalar(
        con, f"select count(*) from query_execution where id > {query_execution_offset}"
    )

    return MergeStats(
        runs_added=runs_added,
        runs_replaced=runs_replaced,
        run_steps_added=run_steps_added,
        run_metrics_added=run_metrics_added,
        query_executions_added=query_executions_added,
    )


def _sync_sequences(con: duckdb.DuckDBPyConnection) -> None:
    # Inserted rows carry explicit ids, which does not advance the sequences
    # backing the id defaults. Recreate them past the current max so the file
    # stays usable as a write target.
    for sequence, table in (
        ("seq_run", "run"),
        ("seq_run_step", "run_step"),
        ("seq_run_metric", "run_metric"),
        ("seq_query_execution", "query_execution"),
    ):
        next_value = _fetch_scalar(con, f"select coalesce(max(id), 0) + 1 from {table}")
        con.execute(f"create or replace sequence {sequence} start {next_value}")
