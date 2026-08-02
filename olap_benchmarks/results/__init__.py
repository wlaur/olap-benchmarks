from __future__ import annotations

import json
import logging
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import duckdb
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, delete, func, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from alembic import command

from ..settings import (
    ALL_SUITE_SCALE_FACTORS,
    DEFAULT_SUITE_SCALE_FACTORS,
    REPO_ROOT,
    SCALE_FACTOR_SUITES,
    SETTINGS,
    SUITE_DISPLAY_ORDER,
    SUITE_LABELS,
    SUITE_NAV_LABELS,
    SUITE_OPERATIONS,
    SUITE_PUBLIC_ROLES,
    SUITE_QUERY_NAME_PARSERS,
    Revision,
)
from .duckdb_sqlalchemy import patch_duckdb_sqlalchemy_compat
from .merge import MergeStats, merge_results
from .models import QueryExecution, Run, RunMetric, RunStep, SystemSnapshot
from .schema import ensure_results_schema

_LOGGER = logging.getLogger(__name__)


def _serialize_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def get_results_db_path(revision: Revision = "default") -> Path:
    return SETTINGS.results_directory / f"{revision}.db"


def get_alembic_config(db_path: Path | None = None) -> Config:
    config = Config((REPO_ROOT / "alembic.ini").as_posix())
    config.set_main_option("script_location", (REPO_ROOT / "alembic").as_posix())

    if db_path is not None:
        config.attributes["db_path"] = db_path.expanduser().resolve()

    return config


def get_results_head_revision() -> str:
    script = ScriptDirectory.from_config(get_alembic_config())
    head = script.get_current_head()

    if head is None:
        raise RuntimeError("Alembic has no head revision configured for the results schema")

    return head


def stamp_results(revision: Revision = "default", db_path: Path | None = None, target_revision: str = "head") -> Path:
    path = (db_path or get_results_db_path(revision)).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    command.stamp(get_alembic_config(path), target_revision)
    return path


def migrate_results(revision: Revision = "default", db_path: Path | None = None, target_revision: str = "head") -> Path:
    path = (db_path or get_results_db_path(revision)).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    command.upgrade(get_alembic_config(path), target_revision)
    return path


def get_results_engine(read_only: bool = True, db_path: Path | None = None, revision: Revision = "default") -> Engine:
    path = db_path or get_results_db_path(revision)
    patch_duckdb_sqlalchemy_compat()

    if read_only:
        if not path.is_file():
            raise FileNotFoundError(f"Results database does not exist: {path}")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)

    return create_engine(
        f"duckdb:///{path}",
        connect_args={"read_only": read_only},
        json_serializer=_serialize_json,
    )


def _require_revision(revision: str) -> Path:
    db_path = get_results_db_path(revision)

    if not db_path.is_file():
        available = list_revisions()
        if available:
            raise SystemExit(f"Revision '{revision}' not found. Available: {', '.join(available)}")
        raise SystemExit(f"Revision '{revision}' not found. No result databases exist in {SETTINGS.results_directory}")

    return db_path


def list_revisions() -> list[str]:
    results_dir = SETTINGS.results_directory
    return sorted(p.stem for p in results_dir.glob("*.db"))


def query_results(sql: str, revision: Revision = "default") -> None:
    db_path = _require_revision(revision)

    with tempfile.TemporaryDirectory(dir=SETTINGS.temporary_directory) as tmpdir:
        snapshot_path = Path(tmpdir) / db_path.name
        shutil.copy2(db_path, snapshot_path)

        _duckdb = cast(Any, duckdb)
        con: duckdb.DuckDBPyConnection = _duckdb.connect(str(snapshot_path), read_only=True)

        try:
            result = con.sql(sql)
            result.show()
        finally:
            con.close()


def compact_results(revision: Revision = "default", db_path: Path | None = None) -> tuple[Path, int, int]:
    path = (db_path or _require_revision(revision)).expanduser().resolve()
    size_before = path.stat().st_size

    tmp_path = path.with_name(f"{path.name}.compact")

    try:
        tmp_path.unlink(missing_ok=True)
        con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect()

        try:
            con.execute(f"ATTACH '{path.as_posix()}' AS src (READ_ONLY)")
            con.execute(f"ATTACH '{tmp_path.as_posix()}' AS dst")
            # copies tables, views, and sequences including their current state
            con.execute("COPY FROM DATABASE src TO dst")
        finally:
            con.close()

        tmp_path.replace(path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise

    size_after = path.stat().st_size
    _LOGGER.info(f"Compacted {path.name}: {size_before / 1e6:.1f} MB -> {size_after / 1e6:.1f} MB")
    return path, size_before, size_after


def _published_table_counts(db_path: Path) -> dict[str, int]:
    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(db_path), read_only=True)

    try:
        counts: dict[str, int] = {}
        for table in ("system_snapshot", "run", "run_step", "run_metric", "query_execution"):
            row = con.execute(f"select count(*) from {table}").fetchone()
            assert row is not None
            counts[table] = int(row[0])
        return counts
    finally:
        con.close()


def _validate_publishable_runs(db_path: Path) -> None:
    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(db_path), read_only=True)
    try:
        running_row = con.execute("select count(*) from run where status = 'running'").fetchone()
        assert running_row is not None
        running = int(running_row[0])
        if running:
            raise RuntimeError(f"Results database contains {running} running run(s)")

        drivers = {
            str(row[0])
            for row in con.execute(
                "select distinct db_driver from run where db = 'monetdb' and db_driver is not null"
            ).fetchall()
        }
        if not drivers:
            return
        unexpected_drivers = sorted(drivers - {"adbc"})
        if unexpected_drivers:
            raise RuntimeError(f"Unknown MonetDB db_driver values: {unexpected_drivers}")

        from ..dbs.monetdb.manifest import monetdb_benchmark_manifest

        expected = {(cell.suite, cell.scale_factor, cell.operation) for cell in monetdb_benchmark_manifest()}
        completed_cells = {
            (str(suite), int(scale_factor), str(operation))
            for suite, scale_factor, operation in con.execute(
                """
                select suite, suite_scale_factor, operation
                from run
                where db = 'monetdb' and status = 'completed' and db_driver = 'adbc'
                group by suite, suite_scale_factor, operation
                """
            ).fetchall()
        }
        missing = sorted(expected - completed_cells)
        if missing:
            formatted = ", ".join(f"{suite}:sf{scale}:{operation}" for suite, scale, operation in missing)
            raise RuntimeError(f"MonetDB release matrix lacks completed ADBC runs: {formatted}")

        missing_baselines = [
            int(row[0])
            for row in con.execute(
                """
                select r.id
                from run r
                where r.db = 'monetdb'
                  and r.db_driver is not null
                  and r.status in ('completed', 'failed')
                  and (
                    (
                      select count(*)
                      from run_step s
                      where s.run_id = r.id
                        and s.step_name = 'session_baseline_before'
                        and s.status = 'completed'
                        and s.row_count = 1
                        and coalesce(
                          cast(json_extract(s."metadata", '$.temporary_ingest_tables') as integer),
                          -1
                        ) = 0
                    ) <> 1
                    or
                    (
                      select count(*)
                      from run_step s
                      where s.run_id = r.id
                        and s.step_name = 'session_baseline_after'
                        and s.status = 'completed'
                        and s.row_count = 0
                        and coalesce(
                          cast(json_extract(s."metadata", '$.temporary_ingest_tables') as integer),
                          -1
                        ) = 0
                    ) <> 1
                  )
                order by r.id
                """
            ).fetchall()
        ]
        if missing_baselines:
            raise RuntimeError(f"MonetDB runs lack clean session baselines: {missing_baselines}")

        invalid_query_steps = [
            int(row[0])
            for row in con.execute(
                """
                select s.id
                from run_step s
                join run r on r.id = s.run_id
                where r.db = 'monetdb'
                  and r.db_driver is not null
                  and r.status = 'completed'
                  and r.operation = 'select'
                  and s.step_type = 'query'
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
        ]
        missing_query_runs = [
            int(row[0])
            for row in con.execute(
                """
                select r.id
                from run r
                where r.db = 'monetdb'
                  and r.db_driver is not null
                  and r.status = 'completed'
                  and r.operation = 'select'
                  and not exists (
                    select 1 from run_step s where s.run_id = r.id and s.step_type = 'query'
                  )
                order by r.id
                """
            ).fetchall()
        ]
        if invalid_query_steps or missing_query_runs:
            raise RuntimeError(
                "MonetDB select runs lack correctness results: "
                f"runs_without_queries={missing_query_runs}, invalid_query_steps={invalid_query_steps}"
            )
    finally:
        con.close()


def publish(revision: Revision = "default", merge: bool = False) -> tuple[Path, MergeStats | None]:
    source_db_path = _require_revision(revision)
    output_dir = REPO_ROOT / "site" / "public" / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_db_path = output_dir / "results.db"
    manifest_path = output_dir / "manifest.json"

    engine = get_results_engine(read_only=True, db_path=source_db_path)

    try:
        ensure_results_schema(engine, allow_create=False)
    finally:
        engine.dispose()
    _validate_publishable_runs(source_db_path)

    merge_stats: MergeStats | None = None

    if merge and output_db_path.is_file():
        merge_stats = merge_results(source_db_path, output_db_path, head_revision=get_results_head_revision())
    else:
        if merge:
            _LOGGER.info(f"No published database at {output_db_path}, copying instead of merging")

        if output_db_path.is_file():
            output_db_path.unlink()

        shutil.copy2(source_db_path, output_db_path)

    manifest = {
        "published_at": datetime.now(UTC).isoformat(),
        "revision": revision,
        "source": source_db_path.as_posix(),
        "merged": merge_stats is not None,
        "schema_revision": get_results_head_revision(),
        "table_counts": _published_table_counts(output_db_path),
    }

    manifest_path.write_text(f"{json.dumps(manifest, indent=2)}\n")

    queries_manifest = _build_queries_manifest()
    queries_path = output_dir / "queries.json"
    queries_path.write_text(f"{json.dumps(queries_manifest, indent=2)}\n")

    suites_manifest = _build_suites_manifest()
    suites_path = output_dir / "suites.json"
    suites_path.write_text(f"{json.dumps(suites_manifest, indent=2)}\n")

    return output_dir, merge_stats


PUBLISHED_DATABASE_RELEASE_TAG = "data"


def upload_published_database() -> None:
    """Replace the `data` release asset with the locally published results database.

    The database is not committed: it is large and rewritten on every publish, so git would keep
    every version forever. The site build fetches this asset instead (`bun run fetch-data`).
    """
    output_db_path = REPO_ROOT / "site" / "public" / "data" / "results.db"
    if not output_db_path.is_file():
        raise FileNotFoundError(f"No published database at {output_db_path}; run 'olap publish' first")

    if shutil.which("gh") is None:
        raise RuntimeError(
            "gh not found on PATH; install the GitHub CLI or upload the asset manually to the "
            f"'{PUBLISHED_DATABASE_RELEASE_TAG}' release"
        )

    command = [
        "gh",
        "release",
        "upload",
        PUBLISHED_DATABASE_RELEASE_TAG,
        output_db_path.as_posix(),
        "--clobber",
    ]
    _LOGGER.info(f"Uploading {output_db_path.name} to the '{PUBLISHED_DATABASE_RELEASE_TAG}' release")
    subprocess.run(command, check=True)


def _build_suites_manifest() -> dict[str, list[dict[str, object]]]:
    return {
        "suites": [
            {
                "id": suite,
                "title": SUITE_LABELS[suite],
                "nav_label": SUITE_NAV_LABELS[suite],
                "queries_key": suite,
                "default_scale_factor": DEFAULT_SUITE_SCALE_FACTORS[suite],
                "supported_scale_factors": list(ALL_SUITE_SCALE_FACTORS[suite]),
                "scale_factor_supported": suite in SCALE_FACTOR_SUITES,
                "operations": list(SUITE_OPERATIONS[suite]),
                "query_name_parser": SUITE_QUERY_NAME_PARSERS[suite],
                "public_role": SUITE_PUBLIC_ROLES[suite],
            }
            for suite in SUITE_DISPLAY_ORDER
        ]
    }


def _build_queries_manifest() -> dict[str, dict[str, dict[str, str | None | dict[str, str]]]]:
    suites_root = REPO_ROOT / "olap_benchmarks" / "suites"
    manifest: dict[str, dict[str, dict[str, str | None | dict[str, str]]]] = {}

    for suite_dir in sorted(suites_root.iterdir()):
        queries_dir = suite_dir / "queries"
        if not queries_dir.is_dir():
            continue

        suite_queries: dict[str, dict[str, str | None | dict[str, str]]] = {}

        if suite_dir.name == "clickbench":
            # Clickbench: each {db}.sql file contains one query per line
            for db_file in sorted(queries_dir.glob("*.sql")):
                db_name = db_file.stem
                for idx, line in enumerate(db_file.read_text().splitlines()):
                    line = line.strip()
                    if not line:
                        continue
                    qname = f"Q{idx}"
                    if qname not in suite_queries:
                        suite_queries[qname] = {"sql": None, "db_overrides": {}}
                    overrides = suite_queries[qname]["db_overrides"]
                    assert isinstance(overrides, dict)
                    overrides[db_name] = line
        else:
            # Generic: common queries in queries/*.sql, db overrides in queries/{db}/*.sql
            for sql_file in sorted(queries_dir.glob("*.sql")):
                qname = sql_file.stem
                suite_queries[qname] = {"sql": sql_file.read_text(), "db_overrides": {}}

            for db_dir in sorted(queries_dir.iterdir()):
                if not db_dir.is_dir():
                    continue
                db_name = db_dir.name
                for sql_file in sorted(db_dir.glob("*.sql")):
                    qname = sql_file.stem
                    if qname not in suite_queries:
                        suite_queries[qname] = {"sql": None, "db_overrides": {}}
                    overrides = suite_queries[qname]["db_overrides"]
                    assert isinstance(overrides, dict)
                    overrides[db_name] = sql_file.read_text()

        manifest[suite_dir.name] = suite_queries

    return manifest


def list_runs(
    revision: Revision = "default",
    status: str | None = None,
    suite: str | None = None,
    db: str | None = None,
    db_driver: str | None = None,
) -> list[dict[str, object]]:
    db_path = _require_revision(revision)
    engine = get_results_engine(read_only=True, db_path=db_path)

    try:
        with Session(engine) as session:
            stmt = select(Run).order_by(Run.started_at.desc())

            if status is not None:
                stmt = stmt.where(Run.status == status)
            if suite is not None:
                stmt = stmt.where(Run.suite == suite)
            if db is not None:
                stmt = stmt.where(Run.db == db)
            if db_driver is not None:
                stmt = stmt.where(Run.db_driver == db_driver)

            runs = session.scalars(stmt).all()

            return [
                {
                    "id": run.id,
                    "suite": run.suite,
                    "suite_scale_factor": run.suite_scale_factor,
                    "db": run.db,
                    "db_driver": run.db_driver,
                    "operation": run.operation,
                    "status": run.status,
                    "started_at": run.started_at.isoformat(),
                    "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                    "error_type": run.error_type,
                }
                for run in runs
            ]
    finally:
        engine.dispose()


def _delete_run_subtrees(session: Session, run_ids: list[int]) -> None:
    snapshot_ids = list(
        session.scalars(
            select(Run.system_snapshot_id).where(Run.id.in_(run_ids)).where(Run.system_snapshot_id.is_not(None))
        ).all()
    )
    session.execute(delete(QueryExecution).where(QueryExecution.run_id.in_(run_ids)))
    session.execute(delete(RunMetric).where(RunMetric.run_id.in_(run_ids)))
    session.execute(delete(RunStep).where(RunStep.run_id.in_(run_ids)))
    session.execute(delete(Run).where(Run.id.in_(run_ids)))
    if snapshot_ids:
        snapshot_is_referenced = select(Run.id).where(Run.system_snapshot_id == SystemSnapshot.id).exists()
        session.execute(
            delete(SystemSnapshot).where(SystemSnapshot.id.in_(snapshot_ids)).where(~snapshot_is_referenced)
        )
    session.commit()


def delete_runs(run_ids: list[int], revision: Revision = "default", db_path: Path | None = None) -> int:
    db_path = db_path or _require_revision(revision)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        with Session(engine) as session:
            _delete_run_subtrees(session, run_ids)
            return len(run_ids)
    finally:
        engine.dispose()


def delete_runs_by_status(status: str, revision: Revision = "default", db_path: Path | None = None) -> int:
    db_path = db_path or _require_revision(revision)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        with Session(engine) as session:
            if status == "failed":
                run_ids = list(session.scalars(select(Run.id).where(Run.status.in_(("failed", "running")))).all())
            elif status == "orphaned":
                run_ids = list(session.scalars(select(Run.id).where(Run.status == "running")).all())
            else:
                raise ValueError(f"Unsupported delete status: {status}")

            if not run_ids:
                return 0

            _delete_run_subtrees(session, run_ids)
            return len(run_ids)
    finally:
        engine.dispose()


def rename_database(
    old_name: str,
    new_name: str,
    revision: Revision = "default",
    db_path: Path | None = None,
) -> int:
    db_path = db_path or _require_revision(revision)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        with Session(engine) as session:
            if old_name == new_name:
                raise SystemExit("Old and new database names must differ.")

            existing_names = sorted(session.scalars(select(Run.db).distinct()).all())

            if old_name not in existing_names:
                available_names = ", ".join(existing_names) if existing_names else "none"
                raise SystemExit(
                    f"Database '{old_name}' not found in revision '{revision}'. Available databases: {available_names}."
                )

            if new_name in existing_names:
                raise SystemExit(f"Database '{new_name}' already exists in revision '{revision}'.")

            renamed_runs = int(session.scalar(select(func.count()).select_from(Run).where(Run.db == old_name)) or 0)
            session.execute(update(Run).where(Run.db == old_name).values(db=new_name))
            session.commit()
            return renamed_runs
    finally:
        engine.dispose()


def mark_running_runs_failed(
    revision: Revision = "default",
    db_path: Path | None = None,
    error_type: str = "KeyboardInterrupt",
    error_message: str = "Interrupted by user",
) -> int:
    db_path = db_path or _require_revision(revision)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        with Session(engine) as session:
            run_ids = list(session.scalars(select(Run.id).where(Run.status == "running")).all())

            if not run_ids:
                return 0

            finished_at = datetime.now(UTC).replace(tzinfo=None)
            session.execute(
                update(RunStep)
                .where(RunStep.run_id.in_(run_ids))
                .where(RunStep.status == "running")
                .values(
                    finished_at=finished_at,
                    status="failed",
                    error_type=error_type,
                    error_message=error_message,
                )
            )
            session.execute(
                update(Run)
                .where(Run.id.in_(run_ids))
                .where(Run.status == "running")
                .values(
                    finished_at=finished_at,
                    status="failed",
                    error_type=error_type,
                    error_message=error_message,
                )
            )
            session.commit()
            return len(run_ids)
    finally:
        engine.dispose()


def config(as_json: bool = False) -> None:
    settings_dict = {
        "input_data_directory": str(SETTINGS.input_data_directory),
        "results_directory": str(SETTINGS.results_directory),
        "database_directory": str(SETTINGS.database_directory),
        "temporary_directory": str(SETTINGS.temporary_directory),
        "system": SETTINGS.system,
    }

    if as_json:
        print(json.dumps(settings_dict, indent=2))
        return

    print("\nCurrent Configuration (from .env)")
    print("=" * 60)
    print(f"  OLAP_BENCHMARKS_INPUT_DATA_DIRECTORY:  {SETTINGS.input_data_directory}")
    print(f"  OLAP_BENCHMARKS_RESULTS_DIRECTORY:     {SETTINGS.results_directory}")
    print(f"  OLAP_BENCHMARKS_DATABASE_DIRECTORY:    {SETTINGS.database_directory}")
    print(f"  OLAP_BENCHMARKS_TEMPORARY_DIRECTORY:   {SETTINGS.temporary_directory}")
    print(f"  OLAP_BENCHMARKS_SYSTEM:                {SETTINGS.system}")
    print()
