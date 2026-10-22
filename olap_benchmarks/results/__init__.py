from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import subprocess
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

import duckdb
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, delete, func, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from alembic import command

from ..matrix import MatrixCell, current_drivers, current_pins, expand_matrix
from ..settings import (
    REPO_ROOT,
    SUITE_SPECS,
    Revision,
    get_settings,
)
from .campaign import campaign_cells, select_campaign_runs
from .cells import StoredRun, load_runs, superseded_runs
from .comparison import write_comparison_manifest
from .merge import MergeStats, merge_results
from .models import QueryExecution, Run, RunMetric, RunStep, SystemSnapshot
from .publishing import publishable_run_ids, single_system, validate_publishable_runs
from .schema import ensure_results_schema
from .validation import validate_results

_LOGGER = logging.getLogger(__name__)

PUBLISHED_DATA_DIRECTORY = REPO_ROOT / "site" / "public" / "data"
PUBLISHED_STORAGE_VERSION: Literal["v1.4.0"] = "v1.4.0"


def _serialize_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def get_results_db_path(revision: Revision = "default") -> Path:
    return get_settings().results_directory / f"{revision}.db"


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
        raise SystemExit(
            f"Revision '{revision}' not found. No result databases exist in {get_settings().results_directory}"
        )

    return db_path


def list_revisions() -> list[str]:
    results_dir = get_settings().results_directory
    return sorted(p.stem for p in results_dir.glob("*.db"))


def query_results(sql: str, revision: Revision = "default") -> None:
    db_path = _require_revision(revision)

    with tempfile.TemporaryDirectory(dir=get_settings().temporary_directory) as tmpdir:
        snapshot_path = Path(tmpdir) / db_path.name
        shutil.copy2(db_path, snapshot_path)

        _duckdb = cast(Any, duckdb)
        con: duckdb.DuckDBPyConnection = _duckdb.connect(str(snapshot_path), read_only=True)

        try:
            result = con.sql(sql)
            result.show()
        finally:
            con.close()


def compact_results(
    revision: Revision = "default",
    db_path: Path | None = None,
    *,
    storage_version: Literal["v1.4.0", "v2.0.0"] = "v2.0.0",
) -> tuple[Path, int, int]:
    path = (db_path or _require_revision(revision)).expanduser().resolve()
    size_before = path.stat().st_size

    tmp_path = path.with_name(f"{path.name}.compact")

    try:
        tmp_path.unlink(missing_ok=True)
        con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect()

        try:
            con.execute(f"ATTACH '{path.as_posix()}' AS src (READ_ONLY)")
            con.execute(f"ATTACH '{tmp_path.as_posix()}' AS dst (STORAGE_VERSION '{storage_version}')")
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


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _restrict_to_publishable_runs(
    db_path: Path,
    cells: list[MatrixCell],
    drivers: Mapping[str, str | None],
    *,
    pins: Mapping[str, str],
    campaign: bool = False,
) -> int:
    runs = load_runs(db_path)
    keep = (
        select_campaign_runs(db_path, cells, pins=pins, drivers=drivers)
        if campaign
        else publishable_run_ids(runs, cells, system=single_system(runs), drivers=drivers)
    )
    dropped = [run.id for run in runs if run.id not in keep]
    if dropped:
        delete_runs(dropped, db_path=db_path)
    return len(dropped)


def _publication_metadata(db_path: Path, revision: str, report_url: str | None) -> dict[str, object]:
    if report_url is None:
        return {"kind": "matrix"}
    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(db_path), read_only=True)
    try:
        row = con.execute(
            """
            select count(*), count(*) filter (where r.status = 'completed'),
              count(*) filter (where r.status = 'failed'),
              count(*) filter (where r.status = 'failed' or exists (
                select 1 from run_step s where s.run_id = r.id
                  and s.step_type = 'query' and s.result_status <> 'ok'
            ))
            from run r where r.operation = 'select'
            """
        ).fetchone()
        assert row is not None
        commits = sorted({run.commit for run in load_runs(db_path, con) if run.commit is not None})
        metadata: dict[str, object] = {
            "kind": "campaign",
            "title": f"Benchmark campaign: {revision}",
            "report_url": report_url,
            "select_cohorts": int(row[0]),
            "completed_select_cohorts": int(row[1]),
            "failed_select_cohorts": int(row[2]),
            "partial_select_cohorts": int(row[3]),
            "source_commits": commits,
        }
        analytical = con.execute("select count(*) from run where operation = 'analytical'").fetchone()
        assert analytical is not None
        if analytical[0]:
            metadata.update(analytical_runs=int(analytical[0]), analytical_track="analytical-streams-v1")
        return metadata
    finally:
        con.close()


def publish(
    revision: Revision = "default", merge: bool = False, campaign: bool = False, report_url: str | None = None
) -> tuple[Path, MergeStats | None]:
    if campaign and (merge or not report_url):
        raise ValueError("--campaign requires --report-url and replaces the published dataset; --merge is not allowed")
    if not campaign and report_url is not None:
        raise ValueError("--report-url requires --campaign")
    source_db_path = _require_revision(revision)
    output_dir = PUBLISHED_DATA_DIRECTORY
    output_dir.mkdir(parents=True, exist_ok=True)

    output_db_path = output_dir / "results.db"
    manifest_path = output_dir / "manifest.json"
    staging_path = output_dir / "results.db.staging"

    engine = get_results_engine(read_only=True, db_path=source_db_path)

    try:
        ensure_results_schema(engine, allow_create=False)
    finally:
        engine.dispose()

    if merge and not output_db_path.is_file():
        # the published database is no longer committed, so a fresh clone has no local copy.
        # Quietly copying instead of merging would drop every previously published run, and
        # --upload would then overwrite the release asset with that truncated dataset.
        raise SystemExit(
            f"--merge needs the currently published database at {output_db_path}, which is missing.\n"
            "Fetch it first ('cd site && bun run fetch-data'), or rerun without --merge to "
            "deliberately replace everything that is published."
        )

    if merge:
        if not manifest_path.is_file():
            raise ValueError("--merge requires the currently published manifest.json alongside results.db")
        publication = json.loads(manifest_path.read_text())["publication"]
        if publication["kind"] != "matrix":
            raise ValueError(
                "--merge cannot preserve a campaign publication's expanded inventory and coverage disclosures. "
                "Publish a complete replacement with --campaign --report-url instead."
            )

    cells = campaign_cells() if campaign else expand_matrix(published_only=True)
    pins = current_pins()
    drivers = current_drivers()
    merge_stats: MergeStats | None = None

    staging_path.unlink(missing_ok=True)
    try:
        if merge:
            shutil.copy2(output_db_path, staging_path)
            merge_stats = merge_results(source_db_path, staging_path, head_revision=get_results_head_revision())
        else:
            shutil.copy2(source_db_path, staging_path)

        dropped = _restrict_to_publishable_runs(staging_path, cells, drivers, pins=pins, campaign=campaign)
        if dropped:
            _LOGGER.info(f"Left out {dropped} run(s) outside the selected publication cohorts")

        validate_results(db_path=staging_path, pins=pins, mark=True, include_failed=campaign)
        validate_publishable_runs(
            staging_path, cells, pins=pins, drivers=drivers, allow_mixed_commits=merge or campaign, campaign=campaign
        )
        compact_results(db_path=staging_path, storage_version=PUBLISHED_STORAGE_VERSION)
        staging_path.replace(output_db_path)
    finally:
        staging_path.unlink(missing_ok=True)

    manifest = {
        "published_at": datetime.now(UTC).isoformat(),
        "revision": revision,
        "source": source_db_path.as_posix(),
        "merged": merge_stats is not None,
        "publication": _publication_metadata(output_db_path, revision, report_url),
        "schema_revision": get_results_head_revision(),
        "results_db_format": {
            "storage_compatibility_version": PUBLISHED_STORAGE_VERSION,
            "minimum_reader_engine_version": PUBLISHED_STORAGE_VERSION,
            "writer_engine_version": duckdb.build_info()["duckdb_version"],
        },
        "results_db_sha256": _file_sha256(output_db_path),
        "results_db_bytes": output_db_path.stat().st_size,
        "table_counts": _published_table_counts(output_db_path),
    }

    manifest_path.write_text(f"{json.dumps(manifest, indent=2)}\n")

    queries_manifest = _build_queries_manifest()
    queries_path = output_dir / "queries.json"
    queries_path.write_text(f"{json.dumps(queries_manifest, indent=2)}\n")

    suites_manifest = _build_suites_manifest()
    suites_path = output_dir / "suites.json"
    suites_path.write_text(f"{json.dumps(suites_manifest, indent=2)}\n")

    write_comparison_manifest(output_dir)

    return output_dir, merge_stats


PUBLISHED_DATABASE_RELEASE_TAG = "data"


SHARED_REVISION_RELEASE_TAG = "runs"


def _require_gh(tag: str) -> None:
    """Fail early and clearly when the GitHub CLI is missing or not logged in.

    Only the upload/fetch commands need it; the site build downloads the published asset over
    plain HTTPS, so CI does not depend on gh.
    """
    if shutil.which("gh") is None:
        raise RuntimeError(
            "gh not found on PATH; install the GitHub CLI (https://cli.github.com) to manage the "
            f"'{tag}' release assets, or upload and download them manually"
        )

    status = subprocess.run(["gh", "auth", "status"], capture_output=True, text=True, check=False)
    if status.returncode != 0:
        raise RuntimeError(f"gh is installed but not authenticated; run 'gh auth login' to manage the '{tag}' release")


def _upload_release_asset(tag: str, path: Path) -> None:
    _require_gh(tag)
    _LOGGER.info(f"Uploading {path.name} to the '{tag}' release")
    subprocess.run(["gh", "release", "upload", tag, path.as_posix(), "--clobber"], check=True)


def upload_published_database() -> str:
    output_db_path = PUBLISHED_DATA_DIRECTORY / "results.db"
    if not output_db_path.is_file():
        raise FileNotFoundError(f"No published database at {output_db_path}; run 'olap publish' first")

    manifest = json.loads((PUBLISHED_DATA_DIRECTORY / "manifest.json").read_text())
    if not isinstance(manifest, dict):
        raise TypeError("manifest.json must contain a publication manifest")
    manifest_values = cast(dict[str, object], manifest)
    expected_sha256 = manifest_values.get("results_db_sha256")
    expected_bytes = manifest_values.get("results_db_bytes")
    if not isinstance(expected_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None:
        raise ValueError("manifest.json has no valid results_db_sha256; run 'olap publish' first")
    if type(expected_bytes) is not int or expected_bytes <= 0:
        raise ValueError("manifest.json has no valid results_db_bytes; run 'olap publish' first")
    if output_db_path.stat().st_size != expected_bytes:
        raise ValueError("Published results.db size does not match manifest.json; run 'olap publish' first")
    actual_sha256 = _file_sha256(output_db_path)
    if actual_sha256 != expected_sha256:
        raise ValueError("Published results.db SHA-256 does not match manifest.json; run 'olap publish' first")

    asset_name = f"results-{actual_sha256}.db"
    with tempfile.TemporaryDirectory() as temporary_directory:
        asset_path = Path(temporary_directory) / asset_name
        asset_path.symlink_to(output_db_path.resolve())
        _upload_release_asset(PUBLISHED_DATABASE_RELEASE_TAG, asset_path)
    return asset_name


def upload_results_revision(revision: Revision) -> Path:
    """Upload a results revision to the `runs` release so another host can merge it.

    Revision databases are binary and effectively immutable (use one revision name per host and
    run), so they are shared as release assets rather than committed.
    """
    db_path = _require_revision(revision)
    _upload_release_asset(SHARED_REVISION_RELEASE_TAG, db_path)
    return db_path


def fetch_results_revision(revision: Revision) -> Path:
    """Download a results revision from the `runs` release into the configured results directory."""
    _require_gh(SHARED_REVISION_RELEASE_TAG)
    destination = get_results_db_path(revision)
    destination.parent.mkdir(parents=True, exist_ok=True)

    _LOGGER.info(f"Downloading {destination.name} from the '{SHARED_REVISION_RELEASE_TAG}' release")
    subprocess.run(
        [
            "gh",
            "release",
            "download",
            SHARED_REVISION_RELEASE_TAG,
            "--pattern",
            destination.name,
            "--dir",
            destination.parent.as_posix(),
            "--clobber",
        ],
        check=True,
    )
    return destination


def _build_suites_manifest() -> dict[str, list[dict[str, object]]]:
    return {
        "suites": [
            {
                "id": suite,
                "title": spec.label,
                "nav_label": spec.nav_label,
                "queries_key": suite,
                "default_scale_factor": spec.default_scale_factor,
                "supported_scale_factors": list(spec.scale_factors),
                "scale_factor_supported": spec.scalable,
                "operations": list(spec.operations),
                "query_name_parser": spec.query_name_parser,
                "public_role": spec.public_role,
            }
            for suite, spec in SUITE_SPECS.items()
        ]
    }


def _build_queries_manifest() -> dict[str, dict[str, dict[str, str | None | dict[str, str]]]]:
    from ..suites.parquet_analytics.config import queries_for_publication

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

    manifest["parquet_analytics"] = queries_for_publication()
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
                    "benchmark_profile": run.benchmark_profile,
                    "system_snapshot_id": run.system_snapshot_id,
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


def invalidate_runs(
    run_ids: list[int],
    reason: str,
    revision: Revision = "default",
    db_path: Path | None = None,
) -> int:
    if not run_ids:
        raise ValueError("Specify at least one run ID to invalidate.")
    if not reason.strip():
        raise ValueError("An invalidation reason is required.")

    path = db_path or _require_revision(revision)
    if not path.is_file():
        raise FileNotFoundError(f"Results database does not exist: {path}")
    engine = get_results_engine(read_only=False, db_path=path)

    try:
        with Session(engine) as session:
            selected_ids = set(run_ids)
            runs = list(session.scalars(select(Run).where(Run.id.in_(selected_ids))).all())
            missing = selected_ids - {run.id for run in runs}
            if missing:
                raise ValueError(f"Run IDs not found: {', '.join(map(str, sorted(missing)))}")
            unfinished = sorted(run.id for run in runs if run.status == "running" or run.finished_at is None)
            if unfinished:
                raise ValueError(f"Only finished runs can be invalidated: {', '.join(map(str, unfinished))}")

            invalidated_at = datetime.now(UTC).isoformat()
            for run in runs:
                metadata = dict(run.metadata_json or {})
                metadata.setdefault(
                    "invalidation",
                    {
                        "reason": reason,
                        "time": invalidated_at,
                        "original_status": run.status,
                        "error_type": run.error_type,
                        "error_message": run.error_message,
                    },
                )
                run.metadata_json = metadata
                if run.status == "completed":
                    run.status = "failed"
                    run.error_type = "InvalidBenchmark"
                    run.error_message = reason
            session.commit()
            return len(runs)
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


def find_superseded_runs(
    revision: Revision = "default", db_path: Path | None = None, *, stale_versions: bool = False
) -> list[StoredRun]:
    path = db_path or _require_revision(revision)
    return superseded_runs(load_runs(path), pins=current_pins(), stale_versions=stale_versions)


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


def _record_unstarted_planned_steps(session: Session, run_id: int, finished_at: datetime, reason: str) -> int:
    metadata = session.scalar(select(Run.metadata_json).where(Run.id == run_id)) or {}
    plan = cast(dict[str, Any] | None, metadata.get("plan"))
    profile = cast(dict[str, Any] | None, metadata.get("benchmark_profile"))
    if not plan or not profile:
        return 0

    query_names = cast(list[str], plan["query_names"])
    iterations = int(plan["iterations"])
    warmup_iterations = int(profile["warmup_iterations"])
    started = {
        (str(query_name), int(iteration))
        for query_name, iteration in session.execute(
            select(RunStep.query_name, RunStep.iteration)
            .where(RunStep.run_id == run_id)
            .where(RunStep.step_type == "query")
            .where(RunStep.query_name.is_not(None))
            .where(RunStep.iteration.is_not(None))
        ).all()
    }

    recorded = 0
    for query_name in query_names:
        for iteration in range(1, iterations + 1):
            if (query_name, iteration) in started:
                continue
            session.add(
                RunStep(
                    run_id=run_id,
                    step_type="query",
                    step_name="query",
                    query_name=query_name,
                    iteration=iteration,
                    started_at=finished_at,
                    finished_at=finished_at,
                    status="completed",
                    result_status="skipped",
                    iteration_role="warmup" if iteration <= warmup_iterations else "measured",
                    error_message=reason,
                    metadata_json={"skip_reason": reason, "duration_ms": 0.0},
                )
            )
            recorded += 1
    return recorded


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
            for run_id in run_ids:
                _record_unstarted_planned_steps(
                    session, run_id, finished_at, f"never started: the run was interrupted ({error_message})"
                )
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
        "input_data_directory": str(get_settings().input_data_directory),
        "results_directory": str(get_settings().results_directory),
        "database_directory": str(get_settings().database_directory),
        "temporary_directory": str(get_settings().temporary_directory),
        "system": get_settings().system,
    }

    if as_json:
        print(json.dumps(settings_dict, indent=2))
        return

    print("\nCurrent Configuration (from .env)")
    print("=" * 60)
    print(f"  OLAP_BENCHMARKS_INPUT_DATA_DIRECTORY:  {get_settings().input_data_directory}")
    print(f"  OLAP_BENCHMARKS_RESULTS_DIRECTORY:     {get_settings().results_directory}")
    print(f"  OLAP_BENCHMARKS_DATABASE_DIRECTORY:    {get_settings().database_directory}")
    print(f"  OLAP_BENCHMARKS_TEMPORARY_DIRECTORY:   {get_settings().temporary_directory}")
    print(f"  OLAP_BENCHMARKS_SYSTEM:                {get_settings().system}")
    print()
