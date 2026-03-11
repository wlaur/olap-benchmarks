from __future__ import annotations

import json
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import duckdb
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, delete, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from alembic import command

from ..settings import REPO_ROOT, SETTINGS, Revision
from .duckdb_sqlalchemy import patch_duckdb_sqlalchemy_compat
from .models import Run, RunMetric, RunStep
from .schema import ensure_results_schema


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


def publish(revision: Revision = "default") -> Path:
    source_db_path = _require_revision(revision)
    output_dir = REPO_ROOT / "site" / "public" / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_db_path = output_dir / "results.duckdb"
    manifest_path = output_dir / "manifest.json"

    engine = get_results_engine(read_only=True, db_path=source_db_path)

    try:
        ensure_results_schema(engine, allow_create=False)

        with Session(engine) as session:
            table_counts = {
                "run": int(session.scalar(select(func.count()).select_from(Run)) or 0),
                "run_step": int(session.scalar(select(func.count()).select_from(RunStep)) or 0),
                "run_metric": int(session.scalar(select(func.count()).select_from(RunMetric)) or 0),
            }
    finally:
        engine.dispose()

    if output_db_path.is_file():
        output_db_path.unlink()

    shutil.copy2(source_db_path, output_db_path)

    manifest = {
        "published_at": datetime.now(UTC).isoformat(),
        "revision": revision,
        "source": source_db_path.as_posix(),
        "schema_revision": get_results_head_revision(),
        "table_counts": table_counts,
    }

    manifest_path.write_text(json.dumps(manifest, indent=2))

    queries_manifest = _build_queries_manifest()
    queries_path = output_dir / "queries.json"
    queries_path.write_text(json.dumps(queries_manifest, indent=2))

    return output_dir


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

            runs = session.scalars(stmt).all()

            return [
                {
                    "id": run.id,
                    "suite": run.suite,
                    "db": run.db,
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


def delete_runs(run_ids: list[int], revision: Revision = "default") -> int:
    db_path = _require_revision(revision)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        with Session(engine) as session:
            count = len(run_ids)
            session.execute(delete(RunMetric).where(RunMetric.run_id.in_(run_ids)))
            session.execute(delete(RunStep).where(RunStep.run_id.in_(run_ids)))
            session.execute(delete(Run).where(Run.id.in_(run_ids)))
            session.commit()
            return count
    finally:
        engine.dispose()


def delete_runs_by_status(status: str, revision: Revision = "default") -> int:
    db_path = _require_revision(revision)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        with Session(engine) as session:
            run_ids = list(session.scalars(select(Run.id).where(Run.status == status)).all())

            if not run_ids:
                return 0

            session.execute(delete(RunMetric).where(RunMetric.run_id.in_(run_ids)))
            session.execute(delete(RunStep).where(RunStep.run_id.in_(run_ids)))
            session.execute(delete(Run).where(Run.id.in_(run_ids)))
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
