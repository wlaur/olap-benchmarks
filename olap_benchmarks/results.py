from __future__ import annotations

import json
import shutil
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import duckdb
from sqlalchemy import create_engine, delete, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from .duckdb_sqlalchemy import patch_duckdb_sqlalchemy_compat
from .results_models import Run, RunMetric, RunStep
from .results_schema import SCHEMA_VERSION, ensure_results_schema
from .settings import REPO_ROOT, SETTINGS


def get_results_db_path(revision: str = "default") -> Path:
    return SETTINGS.results_directory / f"{revision}.db"


def get_results_engine(read_only: bool = True, db_path: Path | None = None, revision: str = "default") -> Engine:
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


def _duration_ms(started_at: datetime, finished_at: datetime) -> float:
    return 1000 * (finished_at - started_at).total_seconds()


def _round_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 3)


def _build_run_summary(session: Session) -> list[dict[str, object]]:
    rows = session.execute(
        select(
            Run.suite,
            Run.db,
            Run.operation,
            Run.status,
            Run.started_at,
            Run.finished_at,
        ).where(Run.finished_at.is_not(None))
    ).all()

    grouped: dict[tuple[str, str, str, str], list[float]] = defaultdict(list)

    for suite, db, operation, status, started_at, finished_at in rows:
        assert isinstance(suite, str)
        assert isinstance(db, str)
        assert isinstance(operation, str)
        assert isinstance(status, str)
        assert isinstance(started_at, datetime)
        assert isinstance(finished_at, datetime)

        grouped[(suite, db, operation, status)].append(_duration_ms(started_at, finished_at))

    result: list[dict[str, object]] = []

    for (suite, db, operation, status), durations in sorted(grouped.items()):
        avg = sum(durations) / len(durations)
        result.append(
            {
                "suite": suite,
                "db": db,
                "operation": operation,
                "status": status,
                "runs": len(durations),
                "avg_duration_ms": _round_or_none(avg),
                "min_duration_ms": _round_or_none(min(durations)),
                "max_duration_ms": _round_or_none(max(durations)),
            }
        )

    return result


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None

    sorted_values = sorted(values)
    idx = (len(sorted_values) - 1) * p

    lower = int(idx)
    upper = min(lower + 1, len(sorted_values) - 1)

    if lower == upper:
        return sorted_values[lower]

    frac = idx - lower
    return sorted_values[lower] * (1 - frac) + sorted_values[upper] * frac


def _build_query_summary(session: Session) -> list[dict[str, object]]:
    rows = session.execute(
        select(
            Run.suite,
            Run.db,
            RunStep.query_name,
            RunStep.started_at,
            RunStep.finished_at,
            RunStep.row_count,
        )
        .join(Run, Run.id == RunStep.run_id)
        .where(RunStep.step_type == "query")
        .where(RunStep.status == "completed")
        .where(RunStep.query_name.is_not(None))
        .where(RunStep.finished_at.is_not(None))
    ).all()

    grouped_durations: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    grouped_rows: dict[tuple[str, str, str], list[int]] = defaultdict(list)

    for suite, db, query_name, started_at, finished_at, row_count in rows:
        assert isinstance(suite, str)
        assert isinstance(db, str)
        assert isinstance(query_name, str)
        assert isinstance(started_at, datetime)
        assert isinstance(finished_at, datetime)

        key = (suite, db, query_name)
        grouped_durations[key].append(_duration_ms(started_at, finished_at))

        if isinstance(row_count, int):
            grouped_rows[key].append(row_count)

    result: list[dict[str, object]] = []

    for (suite, db, query_name), durations in sorted(grouped_durations.items()):
        rows_for_key = grouped_rows.get((suite, db, query_name), [])
        avg_row_count = (sum(rows_for_key) / len(rows_for_key)) if rows_for_key else None

        result.append(
            {
                "suite": suite,
                "db": db,
                "query_name": query_name,
                "samples": len(durations),
                "avg_duration_ms": _round_or_none(sum(durations) / len(durations)),
                "p50_duration_ms": _round_or_none(_percentile(durations, 0.5)),
                "min_duration_ms": _round_or_none(min(durations)),
                "max_duration_ms": _round_or_none(max(durations)),
                "avg_row_count": _round_or_none(avg_row_count),
            }
        )

    return result


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


def query_results(sql: str, revision: str = "default") -> None:
    db_path = _require_revision(revision)

    _duckdb = cast(Any, duckdb)
    con: duckdb.DuckDBPyConnection = _duckdb.connect(str(db_path), read_only=True)

    try:
        result = con.sql(sql)
        result.show()
    finally:
        con.close()


def export_site_data(
    output_directory: str | None = None,
    source_database: str | None = None,
    revision: str = "default",
) -> None:
    source_db_path = Path(source_database).resolve() if source_database else _require_revision(revision)

    if output_directory is None:
        output_dir = REPO_ROOT / "site" / "public" / "data"
    else:
        output_dir = Path(output_directory).resolve()

    output_dir.mkdir(parents=True, exist_ok=True)

    output_db_path = output_dir / "results.duckdb"
    manifest_path = output_dir / "results_manifest.json"
    summary_path = output_dir / "summary.json"

    source_engine = get_results_engine(read_only=True, db_path=source_db_path)

    try:
        ensure_results_schema(source_engine, reset_if_mismatch=False, allow_create=False)

        with Session(source_engine) as session:
            table_counts = {
                "run": int(session.scalar(select(func.count()).select_from(Run)) or 0),
                "run_step": int(session.scalar(select(func.count()).select_from(RunStep)) or 0),
                "run_metric": int(session.scalar(select(func.count()).select_from(RunMetric)) or 0),
            }

            run_summary = _build_run_summary(session)
            query_summary = _build_query_summary(session)

        manifest = {
            "generated_at": datetime.now(UTC).isoformat(),
            "source_database": source_db_path.as_posix(),
            "schema_version": SCHEMA_VERSION,
            "table_counts": table_counts,
        }

        summary = {
            "generated_at": manifest["generated_at"],
            "run_summary": run_summary,
            "query_summary": query_summary,
        }
    finally:
        source_engine.dispose()

    if output_db_path.is_file():
        output_db_path.unlink()

    shutil.copy2(source_db_path, output_db_path)

    manifest_path.write_text(json.dumps(manifest, indent=2))
    summary_path.write_text(json.dumps(summary, indent=2))

    print(f"Exported DuckDB artifact to {output_db_path}")
    print(f"Exported manifest to {manifest_path}")
    print(f"Exported summary to {summary_path}")


def list_runs(
    revision: str = "default",
    status: str | None = None,
    suite: str | None = None,
    db: str | None = None,
) -> list[dict[str, object]]:
    engine = get_results_engine(read_only=True, revision=revision)

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


def delete_runs(run_ids: list[int], revision: str = "default") -> int:
    engine = get_results_engine(read_only=False, revision=revision)

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


def delete_runs_by_status(status: str, revision: str = "default") -> int:
    engine = get_results_engine(read_only=False, revision=revision)

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
